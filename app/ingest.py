"""采集员补传读数的批量接入。

语义（与需求逐条对应）：

1. 一个批次提交 ``batch_id`` 与 1～500 条 ``(meter_id, read_time, wh)``；
   批内同一 ``(meter_id, read_time)`` 只允许出现一次（由请求模型拒绝）。
2. 入仓后返回本次真正新增的条数与“同键同值、先前已存在”的条数。
3. 幂等：同一 ``batch_id`` + 相同内容的重试，直接返回**首次落库的回执**，
   不再触碰 ``readings``——即使两次请求之间有人用直连 SQL 改过读数，
   也绝不回写；同一 ``batch_id`` 携带不同内容则拒绝（409）。
4. 并发：同一批次用事务级 advisory lock 串行化；读数行用
   ``SELECT ... FOR UPDATE`` 按主键顺序加锁，按**入仓时实际读到的值**判定：
   同键同值计为 existing，同键异值整批失败。
5. 原子性：读数写入与回执写入在同一事务内提交；任何一步失败都整体回滚，
   不会留下半份读数或半份回执。``readings`` 表结构不变，原有核对 SQL
   照常读取新入仓的数据。
"""
from __future__ import annotations

import hashlib
import json
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone

import psycopg

from .config import settings
from .models import ReadingIn

# 读数行 + 回执；容器首次初始化时由 sql/init.sql 创建，
# 已有数据卷/测试库则由 ensure_ingest_schema() 幂等补建。
INGEST_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS ingestion_receipts (
    batch_id     text PRIMARY KEY,
    content_hash text NOT NULL,
    inserted     integer NOT NULL CHECK (inserted >= 0),
    existing     integer NOT NULL CHECK (existing >= 0),
    created_at   timestamptz NOT NULL DEFAULT now()
)
"""

_schema_lock = threading.Lock()
_schema_ready_for: str | None = None


class IngestError(Exception):
    """批量接入的可预期失败；http_status 决定 HTTP 状态码。"""

    http_status = 400

    def __init__(self, message: str):
        super().__init__(message)
        self.message = message


class BatchContentConflict(IngestError):
    """同一批次号已存在，但本次内容不同。"""

    http_status = 409


class ReadingValueConflict(IngestError):
    """同键读数在库中的实际值与本次提交不同；整批拒绝。"""

    http_status = 409


class InvalidMeterError(IngestError):
    """批次中包含不存在的表号；整批拒绝。"""

    http_status = 422


class IngestBusyError(IngestError):
    """并发锁等待超时/死锁，整批未生效，可安全重试。"""

    http_status = 409


class IngestTransientError(IngestError):
    """数据库暂时不可用/超时，整批未生效，可安全重试。"""

    http_status = 503


@dataclass(frozen=True)
class IngestOutcome:
    batch_id: str
    inserted: int
    existing: int
    replayed: bool = False


def ensure_ingest_schema() -> None:
    """幂等补建回执表（供已有数据卷/测试库使用）。

    多 worker/多实例同时首次调用时，用 advisory lock 串行化，避免
    ``CREATE TABLE IF NOT EXISTS`` 在系统目录上互相竞争。
    """
    global _schema_ready_for
    if _schema_ready_for == settings.pg_dsn:
        return
    with _schema_lock:
        if _schema_ready_for == settings.pg_dsn:
            return
        conn = psycopg.connect(
            settings.pg_dsn,
            connect_timeout=settings.connect_timeout,
            autocommit=False,
            application_name="recon-ingest-schema",
        )
        try:
            # 防止 schema 补建本身无限等待：多实例同时首次启动时，
            # 未抢到锁的一方有界重试，等建表方提交后即可看到表。
            deadline = time.monotonic() + settings.ingest_lock_timeout_ms / 1000.0
            while True:
                acquired = conn.execute(
                    "SELECT pg_try_advisory_xact_lock(hashtextextended(%s::text, 0))",
                    ("recon:ingestion_receipts",),
                ).fetchone()[0]
                if acquired:
                    conn.execute(INGEST_SCHEMA_SQL)
                    conn.commit()
                    break
                conn.rollback()
                if time.monotonic() >= deadline:
                    raise IngestTransientError(
                        "回执表初始化等待超时，请重试"
                    )
                time.sleep(0.05)
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()
        _schema_ready_for = settings.pg_dsn


def _utc_iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds")


def content_fingerprint(readings: list[ReadingIn]) -> str:
    """内容指纹：与提交顺序无关，时刻统一按 UTC 规范化。

    只覆盖读数内容（batch_id 是回执主键，另行保存）。
    """
    canonical = sorted(
        (
            {
                "meter_id": r.meter_id,
                "read_time": _utc_iso(r.read_time),
                "wh": r.wh,
            }
            for r in readings
        ),
        key=lambda item: (item["meter_id"], item["read_time"], item["wh"]),
    )
    payload = json.dumps(
        canonical, ensure_ascii=False, separators=(",", ":"), sort_keys=True
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _open_ingest_connection() -> psycopg.Connection:
    conn = psycopg.connect(
        settings.pg_dsn,
        connect_timeout=settings.connect_timeout,
        autocommit=False,  # 读写同一事务；提交/回滚由 ingest_batch 显式控制
        application_name="recon-ingest",
    )
    # SET LOCAL 只作用于本次事务。均为内部 int 配置，内联安全。
    conn.execute(
        f"SET LOCAL statement_timeout = {int(settings.statement_timeout_ms)}"
    )
    conn.execute(
        f"SET LOCAL lock_timeout = {int(settings.ingest_lock_timeout_ms)}"
    )
    conn.execute(
        f"SET LOCAL idle_in_transaction_session_timeout = "
        f"{int(settings.idle_in_tx_timeout_ms)}"
    )
    return conn


def ingest_batch(batch_id: str, readings: list[ReadingIn]) -> IngestOutcome:
    """把一批读数原子地写入并留下回执；失败时整批回滚。"""
    ensure_ingest_schema()
    fingerprint = content_fingerprint(readings)
    conn = _open_ingest_connection()
    try:
        outcome = _ingest_in_transaction(conn, batch_id, readings, fingerprint)
        conn.commit()
        return outcome
    except IngestError:
        conn.rollback()
        raise
    except psycopg.errors.ForeignKeyViolation as exc:
        # 表号在事务进行中被并发删除等极端情况；仍然整批失败
        conn.rollback()
        raise InvalidMeterError("读数中包含不存在的表号（meters）") from exc
    except (psycopg.errors.LockNotAvailable, psycopg.errors.DeadlockDetected) as exc:
        conn.rollback()
        raise IngestBusyError("并发锁等待超时，本批未生效，请重试") from exc
    except (
        psycopg.errors.QueryCanceled,      # statement_timeout / 用户取消
        psycopg.OperationalError,          # 连接中断等
        psycopg.errors.AdminShutdown,
    ) as exc:
        conn.rollback()
        raise IngestTransientError(
            f"数据库暂时不可用（{exc.__class__.__name__}），本批未生效，请重试"
        ) from exc
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()


def _ingest_in_transaction(
    conn: psycopg.Connection,
    batch_id: str,
    readings: list[ReadingIn],
    fingerprint: str,
) -> IngestOutcome:
    # 同一批次的并发重试串行化：事务结束自动释放。
    # 用 try + 短间隔轮询而非阻塞等待，使等待上限严格由
    # ingest_lock_timeout_ms 控制（受限环境中信号唤醒可能迟滞）。
    deadline = time.monotonic() + settings.ingest_lock_timeout_ms / 1000.0
    while True:
        acquired = conn.execute(
            "SELECT pg_try_advisory_xact_lock(hashtextextended(%s::text, 0))",
            (batch_id,),
        ).fetchone()[0]
        if acquired:
            break
        if time.monotonic() >= deadline:
            raise IngestBusyError(
                "同一批次号正被其他请求处理，等待超时，本次未生效，请重试"
            )
        time.sleep(0.02)

    receipt = conn.execute(
        "SELECT content_hash, inserted, existing "
        "FROM ingestion_receipts WHERE batch_id = %s FOR UPDATE",
        (batch_id,),
    ).fetchone()
    if receipt is not None:
        stored_hash, inserted, existing = receipt
        if stored_hash != fingerprint:
            raise BatchContentConflict(
                f"批次号 {batch_id!r} 已存在且内容不同，拒绝接入"
            )
        # 命中幂等重放：不碰 readings，原样返回首次回执。
        return IngestOutcome(
            batch_id=batch_id,
            inserted=inserted,
            existing=existing,
            replayed=True,
        )

    inserted, existing = _apply_readings(conn, readings)
    # 回执与读数在同一事务内提交：任何失败都会在这里一同回滚。
    conn.execute(
        "INSERT INTO ingestion_receipts (batch_id, content_hash, inserted, existing) "
        "VALUES (%s, %s, %s, %s)",
        (batch_id, fingerprint, inserted, existing),
    )
    return IngestOutcome(
        batch_id=batch_id, inserted=inserted, existing=existing, replayed=False
    )


def _apply_readings(
    conn: psycopg.Connection, readings: list[ReadingIn]
) -> tuple[int, int]:
    """在已开启的事务内校验表号并写入读数，返回 (inserted, existing)。"""
    wanted: dict[tuple[int, datetime], int] = {}
    for reading in readings:
        key = (reading.meter_id, reading.read_time.astimezone(timezone.utc))
        # 请求模型已保证批内键唯一；这里再兜底一次，绝不静默覆盖
        if key in wanted:
            raise IngestError("同一表号和时刻在批内只能出现一次")
        wanted[key] = reading.wh

    meter_ids = sorted({key[0] for key in wanted})
    known = {
        row[0]
        for row in conn.execute(
            "SELECT meter_id FROM meters WHERE meter_id = ANY(%s::bigint[])",
            (meter_ids,),
        )
    }
    missing_meters = [m for m in meter_ids if m not in known]
    if missing_meters:
        raise InvalidMeterError(
            "以下表号不存在，整批未接入：" + ", ".join(map(str, missing_meters[:20]))
        )

    # 按 (meter_id, read_time) 固定顺序加锁，避免多个批次交叉加锁死锁。
    locked = _select_keys_for_update(conn, list(wanted))

    existing = 0
    pending: dict[tuple[int, datetime], int] = {}
    for key, wh in wanted.items():
        if key in locked:
            if locked[key] != wh:
                raise ReadingValueConflict(
                    f"表号 {key[0]} 在 {_utc_iso(key[1])} 的库中实际值为 "
                    f"{locked[key]}，与提交值 {wh} 不同；整批未接入"
                )
            existing += 1
        else:
            pending[key] = wh

    # 逐轮处理“加锁时还不存在、随后被其他批次/直连 SQL 并发写入”的键：
    # ON CONFLICT DO NOTHING 会等待并发事务结束，再按最终提交值判定。
    inserted = 0
    for _attempt in range(3):
        if not pending:
            break
        inserted_keys = _insert_missing(conn, pending)
        inserted += len(inserted_keys)
        for key in inserted_keys:
            pending.pop(key, None)
        if not pending:
            break

        locked = _select_keys_for_update(conn, list(pending))
        for key in list(pending):
            if key not in locked:
                continue  # 并发删除等：下一轮再尝试插入
            if locked[key] != pending[key]:
                raise ReadingValueConflict(
                    f"表号 {key[0]} 在 {_utc_iso(key[1])} 的并发写入值与提交值不同；"
                    f"整批未接入"
                )
            existing += 1
            pending.pop(key)

    if pending:
        raise IngestBusyError("并发写入持续冲突，无法确定入仓时实际值，本批未生效")

    return inserted, existing


def _select_keys_for_update(
    conn: psycopg.Connection, keys: list[tuple[int, datetime]]
) -> dict[tuple[int, datetime], int]:
    """按主键顺序锁定并读取当前已提交值（READ COMMITTED 下即最新值）。"""
    if not keys:
        return {}
    meter_ids = [key[0] for key in keys]
    read_times = [key[1] for key in keys]
    rows = conn.execute(
        "SELECT r.meter_id, r.read_time, r.wh FROM readings r "
        "JOIN unnest(%s::bigint[], %s::timestamptz[]) "
        "  AS k(meter_id, read_time) USING (meter_id, read_time) "
        "ORDER BY r.meter_id, r.read_time "
        "FOR UPDATE OF r",
        (meter_ids, read_times),
    ).fetchall()
    return {(row[0], row[1].astimezone(timezone.utc)): row[2] for row in rows}


def _insert_missing(
    conn: psycopg.Connection, pending: dict[tuple[int, datetime], int]
) -> set[tuple[int, datetime]]:
    """插入当前不存在的键；返回真正由本事务插入的键集合。"""
    items = sorted(pending.items())
    meter_ids = [key[0] for key, _wh in items]
    read_times = [key[1] for key, _wh in items]
    values = [wh for _key, wh in items]
    rows = conn.execute(
        "INSERT INTO readings (meter_id, read_time, wh) "
        "SELECT * FROM unnest(%s::bigint[], %s::timestamptz[], %s::bigint[]) "
        "ON CONFLICT (meter_id, read_time) DO NOTHING "
        "RETURNING meter_id, read_time",
        (meter_ids, read_times, values),
    ).fetchall()
    return {(row[0], row[1].astimezone(timezone.utc)) for row in rows}
