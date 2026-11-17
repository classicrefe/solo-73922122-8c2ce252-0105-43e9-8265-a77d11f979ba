"""API 请求 / 响应模型。"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator, model_validator

# PostgreSQL bigint 范围；超出范围在请求校验阶段拒绝
_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1

JobStatus = Literal[
    "pending",      # 已受理，等待运行名额
    "running",      # 运行中
    "cancelling",   # 已收到取消请求，正在中止本轮 SQL
    "succeeded",    # 终态：核对完成（可能一致，也可能有差异）
    "failed",       # 终态：任一侧失败
    "cancelled",    # 终态：用户取消
    "timeout",      # 终态：超过单轮时限
]

TERMINAL_STATUSES = {"succeeded", "failed", "cancelled", "timeout"}
ACTIVE_STATUSES = {"pending", "running", "cancelling"}


class CreateJobRequest(BaseModel):
    left_sql: str = Field(..., min_length=1, description="左侧只读 SELECT（如改写后的日结 SQL）")
    right_sql: str = Field(..., min_length=1, description="右侧只读 SELECT（基准 SQL）")
    name: str | None = Field(None, max_length=128, description="可选备注名")


class DiffRow(BaseModel):
    # 与结果集列顺序对齐；NULL 用 null 表示，空串为 ""，二者不同
    values: list[Any]
    left_count: int
    right_count: int


class ReconcileResult(BaseModel):
    identical: bool
    columns: list[str]
    column_types: list[str]          # bigint | text
    left_row_total: int
    right_row_total: int
    matched_group_count: int         # 行内容与次数都相同的去重行数
    diff_group_total: int            # 存在差异的去重行种类总数（不受 20 条展示上限影响）
    diffs_shown: int
    diffs: list[DiffRow]             # 最多 20 种


class JobOut(BaseModel):
    job_id: str
    name: str | None
    status: JobStatus
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    deadline: datetime | None
    cancel_requested: bool
    error: str | None
    result: ReconcileResult | None

    @staticmethod
    def utcnow() -> datetime:
        return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# 采集员补传：批量接入
# ---------------------------------------------------------------------------

def _utc_iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds")


class ReadingIn(BaseModel):
    meter_id: int = Field(..., ge=_INT64_MIN, le=_INT64_MAX, description="表号（bigint）")
    read_time: datetime = Field(..., description="读数时刻（UTC）")
    wh: int = Field(..., ge=_INT64_MIN, le=_INT64_MAX, description="累计电量（Wh，bigint）")

    @field_validator("read_time")
    @classmethod
    def _require_aware_utc(cls, value: datetime) -> datetime:
        # 必须带时区：不带时区的时刻无法无歧义地定为 UTC。
        # 带时区偏移的会先换算成 UTC 再参与键判定。
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("read_time 必须带时区（UTC，如 2026-11-15T08:00:00Z）")
        return value.astimezone(timezone.utc)


class IngestBatchRequest(BaseModel):
    batch_id: str = Field(..., min_length=1, max_length=128, description="批次号，幂等键")
    readings: list[ReadingIn] = Field(
        ..., min_length=1, max_length=500, description="1～500 条读数"
    )

    @field_validator("batch_id")
    @classmethod
    def _non_blank_batch_id(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("batch_id 不能为空白字符")
        return value

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "batch_id": "collector-20261115-001",
                    "readings": [
                        {
                            "meter_id": 1001,
                            "read_time": "2026-11-15T12:00:00Z",
                            "wh": 1480,
                        },
                        {
                            "meter_id": 1002,
                            "read_time": "2026-11-15T12:00:00Z",
                            "wh": 770,
                        },
                    ],
                }
            ]
        }
    }

    @model_validator(mode="after")
    def _unique_keys(self) -> "IngestBatchRequest":
        seen: set[tuple[int, datetime]] = set()
        for reading in self.readings:
            key = (reading.meter_id, reading.read_time)
            if key in seen:
                raise ValueError(
                    f"同一表号和时刻在批内只能出现一次：meter_id={key[0]}，"
                    f"read_time={_utc_iso(key[1])}"
                )
            seen.add(key)
        return self


class IngestBatchResponse(BaseModel):
    """批量接入回执。幂等重放时返回的就是首次落库的原回执，字段完全一致。"""

    batch_id: str
    inserted: int          # 本次真正写入的读数条数
    existing: int          # 同键同值、先前已在库中的条数（不计入本次写入）

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "batch_id": "collector-20261115-001",
                    "inserted": 2,
                    "existing": 0,
                }
            ]
        }
    }
