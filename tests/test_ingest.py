"""批量接入的端到端测试：幂等、原子性、并发判定与回执语义。"""
from __future__ import annotations

import threading
import time
import uuid
from datetime import datetime, timedelta, timezone

import psycopg
import pytest

API = "/readings/batches"


@pytest.fixture(autouse=True)
def _clean_ingest_fixtures(pg_dsn):
    """本文件使用 2030 年起的专用时刻；用例前后清理，避免跨次运行残留。"""
    conn = psycopg.connect(pg_dsn, autocommit=True)
    try:
        conn.execute(
            "DELETE FROM readings WHERE read_time >= TIMESTAMPTZ '2030-01-01 00:00:00+00' "
            "AND read_time < TIMESTAMPTZ '2031-01-01 00:00:00+00'"
        )
        conn.execute("DELETE FROM ingestion_receipts WHERE batch_id LIKE 'test-%'")
        yield
        conn.execute(
            "DELETE FROM readings WHERE read_time >= TIMESTAMPTZ '2030-01-01 00:00:00+00' "
            "AND read_time < TIMESTAMPTZ '2031-01-01 00:00:00+00'"
        )
        conn.execute("DELETE FROM ingestion_receipts WHERE batch_id LIKE 'test-%'")
    finally:
        conn.close()


def _batch(*readings: tuple[int, datetime, int], batch_id: str | None = None) -> dict:
    return {
        "batch_id": batch_id or f"test-{uuid.uuid4().hex}",
        "readings": [
            {
                "meter_id": meter_id,
                "read_time": read_time.isoformat(),
                "wh": wh,
            }
            for meter_id, read_time, wh in readings
        ],
    }


def _t(offset_seconds: int = 0) -> datetime:
    # 每个测试用独立时刻，避免与 init.sql 及历史测试数据撞键
    base = datetime(2030, 1, 1, tzinfo=timezone.utc)
    return base + timedelta(seconds=offset_seconds)


def _reading_count(db_conn: psycopg.Connection, meter_id: int, read_time: datetime) -> int:
    row = db_conn.execute(
        "SELECT count(*) FROM readings WHERE meter_id = %s AND read_time = %s",
        (meter_id, read_time),
    ).fetchone()
    return row[0]


def _receipt(db_conn: psycopg.Connection, batch_id: str):
    return db_conn.execute(
        "SELECT inserted, existing FROM ingestion_receipts WHERE batch_id = %s",
        (batch_id,),
    ).fetchone()


def test_new_batch_returns_inserted_and_existing(client, db_conn) -> None:
    existing_time = _t(100)
    new_time = _t(200)
    db_conn.execute(
        "INSERT INTO readings VALUES (1001, %s, 5000) "
        "ON CONFLICT (meter_id, read_time) DO UPDATE SET wh = EXCLUDED.wh",
        (existing_time,),
    )

    batch = _batch(
        (1001, existing_time, 5000),   # 同键同值已存在
        (1001, new_time, 5100),        # 新增
        (1002, new_time, 600),         # 新增
    )
    resp = client.post(API, json=batch)
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body == {
        "batch_id": batch["batch_id"],
        "inserted": 2,
        "existing": 1,
    }
    assert "Idempotent-Replay" not in resp.headers

    assert _reading_count(db_conn, 1001, new_time) == 1
    assert _reading_count(db_conn, 1002, new_time) == 1
    assert _receipt(db_conn, batch["batch_id"]) == (2, 1)


def test_replay_returns_original_receipt_even_after_direct_sql_correction(
    client, db_conn
) -> None:
    t = _t(300)
    batch = _batch((1001, t, 7000))
    first = client.post(API, json=batch)
    assert first.status_code == 201
    assert first.json() == {
        "batch_id": batch["batch_id"],
        "inserted": 1,
        "existing": 0,
    }
    assert "Idempotent-Replay" not in first.headers

    # 直连 SQL 更正读数
    db_conn.execute(
        "UPDATE readings SET wh = 7777 WHERE meter_id = 1001 AND read_time = %s", (t,)
    )

    replay = client.post(API, json=batch)
    assert replay.status_code == 201
    # 只返回原回执（响应体与首次完全一致），不与更正后的值比较、也不计入 existing
    assert replay.json() == first.json()
    assert replay.headers.get("Idempotent-Replay") == "true"
    # 不得回写
    wh = db_conn.execute(
        "SELECT wh FROM readings WHERE meter_id = 1001 AND read_time = %s", (t,)
    ).fetchone()[0]
    assert wh == 7777


def test_replay_is_order_insensitive(client, db_conn) -> None:
    t1, t2 = _t(400), _t(401)
    batch_id = f"test-{uuid.uuid4().hex}"
    a = _batch((1001, t1, 1), (1002, t2, 2), batch_id=batch_id)
    b = {"batch_id": batch_id, "readings": list(reversed(a["readings"]))}
    assert client.post(API, json=a).json()["inserted"] == 2
    replay = client.post(API, json=b)
    assert replay.status_code == 201
    assert replay.headers.get("Idempotent-Replay") == "true"
    assert replay.json()["inserted"] == 2


def test_same_batch_id_different_content_rejected(client, db_conn) -> None:
    t = _t(500)
    batch_id = f"test-{uuid.uuid4().hex}"
    good = _batch((1001, t, 8000), batch_id=batch_id)
    assert client.post(API, json=good).status_code == 201

    changed = _batch((1001, t, 8001), batch_id=batch_id)
    resp = client.post(API, json=changed)
    assert resp.status_code == 409
    assert "内容不同" in resp.json()["detail"]["message"]
    # 原读数与原回执均不变
    assert db_conn.execute(
        "SELECT wh FROM readings WHERE meter_id = 1001 AND read_time = %s", (t,)
    ).fetchone()[0] == 8000
    assert _receipt(db_conn, batch_id) == (1, 0)


def test_duplicate_key_within_batch_rejected(client, db_conn) -> None:
    t = _t(600)
    batch = {
        "batch_id": f"test-{uuid.uuid4().hex}",
        "readings": [
            {"meter_id": 1001, "read_time": t.isoformat(), "wh": 1},
            # 同一时刻的另一种时区写法，仍是同一个键
            {
                "meter_id": 1001,
                "read_time": t.astimezone(timezone(timedelta(hours=8))).isoformat(),
                "wh": 2,
            },
        ],
    }
    resp = client.post(API, json=batch)
    assert resp.status_code == 422
    assert _receipt(db_conn, batch["batch_id"]) is None


def test_sizes_out_of_range_rejected(client) -> None:
    empty = {"batch_id": "x", "readings": []}
    assert client.post(API, json=empty).status_code == 422

    too_many = _batch(*[(1001, _t(700 + i), i) for i in range(501)])
    assert client.post(API, json=too_many).status_code == 422


def test_naive_read_time_rejected(client) -> None:
    batch = {
        "batch_id": f"test-{uuid.uuid4().hex}",
        "readings": [{"meter_id": 1001, "read_time": "2030-01-01T00:00:00", "wh": 1}],
    }
    assert client.post(API, json=batch).status_code == 422


def test_blank_batch_id_rejected(client) -> None:
    batch = {
        "batch_id": "   ",
        "readings": [{"meter_id": 1001, "read_time": _t(750).isoformat(), "wh": 1}],
    }
    assert client.post(API, json=batch).status_code == 422


def test_timezone_equivalent_retry_is_same_content(client, db_conn) -> None:
    t = _t(760)
    batch_id = f"test-{uuid.uuid4().hex}"
    first = {
        "batch_id": batch_id,
        "readings": [{"meter_id": 1002, "read_time": t.isoformat(), "wh": 321}],
    }
    # 同一 UTC 时刻的 +08:00 写法：内容相同，应命中重放
    retry = {
        "batch_id": batch_id,
        "readings": [
            {
                "meter_id": 1002,
                "read_time": t.astimezone(timezone(timedelta(hours=8))).isoformat(),
                "wh": 321,
            }
        ],
    }
    assert client.post(API, json=first).json()["inserted"] == 1
    replay = client.post(API, json=retry)
    assert replay.status_code == 201
    assert replay.headers.get("Idempotent-Replay") == "true"


def test_batch_of_500_accepted(client, db_conn) -> None:
    batch = _batch(*[(3002, _t(2000 + i), i) for i in range(500)])
    resp = client.post(API, json=batch)
    assert resp.status_code == 201, resp.text
    assert resp.json()["inserted"] == 500


def test_same_key_different_value_rejects_whole_batch(client, db_conn) -> None:
    t_ok, t_conflict = _t(800), _t(801)
    db_conn.execute(
        "INSERT INTO readings VALUES (1002, %s, 9000) "
        "ON CONFLICT (meter_id, read_time) DO UPDATE SET wh = EXCLUDED.wh",
        (t_conflict,),
    )

    batch = _batch(
        (1001, t_ok, 100),        # 本可新增
        (1002, t_conflict, 9001), # 与库中实际值不同
    )
    resp = client.post(API, json=batch)
    assert resp.status_code == 409
    assert "实际值" in resp.json()["detail"]["message"]

    # 整批失败：可新增的那条也不得留下，回执不得存在
    assert _reading_count(db_conn, 1001, t_ok) == 0
    assert db_conn.execute(
        "SELECT wh FROM readings WHERE meter_id = 1002 AND read_time = %s", (t_conflict,)
    ).fetchone()[0] == 9000
    assert _receipt(db_conn, batch["batch_id"]) is None


def test_invalid_meter_rejects_whole_batch(client, db_conn) -> None:
    t_ok, t_bad = _t(900), _t(901)
    batch = _batch((1001, t_ok, 1), (999999, t_bad, 2))
    resp = client.post(API, json=batch)
    assert resp.status_code == 422
    assert "表号不存在" in resp.json()["detail"]["message"]
    assert _reading_count(db_conn, 1001, t_ok) == 0
    assert _receipt(db_conn, batch["batch_id"]) is None


def test_concurrent_uncommitted_direct_sql_is_judged_by_actual_committed_value(
    client, db_conn
) -> None:
    """另一个事务先写入同键但未提交：入仓等待其提交后按实际值判定。"""
    t = _t(1000)
    batch_id_same = f"test-{uuid.uuid4().hex}"
    batch_id_diff = f"test-{uuid.uuid4().hex}"

    db_conn.execute("BEGIN")
    db_conn.execute("INSERT INTO readings VALUES (2001, %s, 12345)", (t,))
    try:
        results: dict[str, object] = {}

        def post_same() -> None:
            results["same"] = client.post(
                API, json=_batch((2001, t, 12345), batch_id=batch_id_same)
            )

        def post_diff() -> None:
            results["diff"] = client.post(
                API, json=_batch((2001, t, 999), batch_id=batch_id_diff)
            )

        t1 = threading.Thread(target=post_same)
        t2 = threading.Thread(target=post_diff)
        t1.start()
        t2.start()
        time.sleep(0.8)  # 两个请求都卡在并发插入等待上
        db_conn.execute("COMMIT")
        t1.join(timeout=15)
        t2.join(timeout=15)
    finally:
        if not db_conn.closed:
            db_conn.rollback()

    same_resp = results["same"]
    diff_resp = results["diff"]
    assert same_resp.status_code == 201, same_resp.text
    assert same_resp.json()["inserted"] == 0
    assert same_resp.json()["existing"] == 1
    assert diff_resp.status_code == 409
    assert _receipt(db_conn, batch_id_same) == (0, 1)
    assert _receipt(db_conn, batch_id_diff) is None
    rows = db_conn.execute(
        "SELECT count(*), min(wh) FROM readings WHERE meter_id = 2001 AND read_time = %s",
        (t,),
    ).fetchone()
    assert rows == (1, 12345)


def test_concurrent_different_batches_same_key_same_value(client, db_conn) -> None:
    """两个不同批次并发写同一个新键、值相同：一个新增，另一个计为已存在。"""
    t = _t(1050)
    payloads = [
        _batch((2001, t, 4242), batch_id=f"test-{uuid.uuid4().hex}"),
        _batch((2001, t, 4242), batch_id=f"test-{uuid.uuid4().hex}"),
    ]
    responses: list = []

    def post(payload) -> None:
        responses.append(client.post(API, json=payload))

    threads = [threading.Thread(target=post, args=(p,)) for p in payloads]
    for th in threads:
        th.start()
    for th in threads:
        th.join(timeout=20)

    assert len(responses) == 2
    assert all(r.status_code == 201 for r in responses), [r.text for r in responses]
    bodies = sorted(r.json()["inserted"] for r in responses)
    assert bodies == [0, 1]  # 一个 inserted=1，另一个 inserted=0/existing=1
    existing_sum = sum(r.json()["existing"] for r in responses)
    assert existing_sum == 1
    assert _reading_count(db_conn, 2001, t) == 1


def test_concurrent_different_batches_same_key_different_value(client, db_conn) -> None:
    """两个不同批次并发写同一个新键但值不同：恰好一个成功，另一个整批失败。"""
    t = _t(1060)
    payloads = [
        _batch((2002, t, 111), batch_id=f"test-{uuid.uuid4().hex}"),
        _batch((2002, t, 222), batch_id=f"test-{uuid.uuid4().hex}"),
    ]
    responses: list = []

    def post(payload) -> None:
        responses.append((payload, client.post(API, json=payload)))

    threads = [threading.Thread(target=post, args=(p,)) for p in payloads]
    for th in threads:
        th.start()
    for th in threads:
        th.join(timeout=20)

    assert len(responses) == 2
    codes = sorted(r.status_code for _p, r in responses)
    assert codes == [201, 409]
    winner = next(p for p, r in responses if r.status_code == 201)
    loser = next(p for p, r in responses if r.status_code == 409)
    winner_wh = winner["readings"][0]["wh"]
    assert db_conn.execute(
        "SELECT wh FROM readings WHERE meter_id = 2002 AND read_time = %s", (t,)
    ).fetchone()[0] == winner_wh
    # 失败批次的回执不得残留
    assert _receipt(db_conn, loser["batch_id"]) is None
    assert _receipt(db_conn, winner["batch_id"]) is not None


def test_concurrent_same_batch_id_only_applies_once(client, db_conn) -> None:
    t = _t(1100)
    batch = _batch((3001, t, 555))
    responses: list = []

    def post() -> None:
        responses.append(client.post(API, json=batch))

    threads = [threading.Thread(target=post) for _ in range(5)]
    for th in threads:
        th.start()
    for th in threads:
        th.join(timeout=15)

    assert len(responses) == 5
    assert all(r.status_code == 201 for r in responses)
    bodies = [r.json() for r in responses]
    assert sum(1 for r in responses if r.headers.get("Idempotent-Replay") == "true") == 4
    assert all(b["inserted"] == 1 and b["existing"] == 0 for b in bodies)
    assert _reading_count(db_conn, 3001, t) == 1
    assert _receipt(db_conn, batch["batch_id"]) == (1, 0)


def test_ingested_readings_visible_to_reconciliation_sql(client) -> None:
    t = _t(1200)
    assert client.post(API, json=_batch((3002, t, 4321))).status_code == 201

    sql = (
        "SELECT meter_id::bigint, wh::bigint FROM readings "
        "WHERE read_time = TIMESTAMPTZ '2030-01-01 00:20:00+00'"
    )
    job = client.post("/jobs", json={"left_sql": sql, "right_sql": sql}).json()
    deadline = time.time() + 30
    while time.time() < deadline:
        out = client.get(f"/jobs/{job['job_id']}").json()
        if out["status"] in ("succeeded", "failed", "cancelled", "timeout"):
            break
        time.sleep(0.1)
    assert out["status"] == "succeeded"
    assert out["result"]["identical"] is True
    assert out["result"]["left_row_total"] == 1
