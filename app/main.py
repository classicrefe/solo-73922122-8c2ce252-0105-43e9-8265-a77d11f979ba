"""FastAPI 应用入口与 HTTP 路由。"""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Response
from fastapi.responses import JSONResponse

from . import db as dbmod
from . import ingest as ingestmod
from .manager import SlotBusyError, manager
from .models import (
    CreateJobRequest,
    IngestBatchRequest,
    IngestBatchResponse,
    JobOut,
)
from .sqlvalidation import SqlValidationError


@asynccontextmanager
async def lifespan(_app: FastAPI):
    """启动时幂等补建回执表；失败不阻止服务启动（首个接入请求会重试）。"""
    try:
        ingestmod.ensure_ingest_schema()
    except Exception:  # noqa: BLE001 - 数据库可能尚未就绪，接入请求会再次尝试
        pass
    yield


app = FastAPI(
    title="电表日结 SQL 核对 API",
    description=(
        "提交两条只读 SELECT，基于同一已提交快照并行读取，"
        "按行内容与出现次数做有序多重集核对（忽略行序，NULL≠空串），"
        "差异最多展示 20 种。单轮 30 秒，全局同时只允许一个任务。\n\n"
        "另提供采集员补传的批量接入：POST /readings/batches，"
        "按批次号幂等、整批原子写入 readings，回执与读数同事务提交。"
    ),
    version="1.1.0",
    lifespan=lifespan,
)


@app.get("/health", tags=["运维"])
def health() -> dict:
    """存活探针；不触碰数据库。"""
    return {"status": "ok"}


@app.post(
    "/readings/batches",
    response_model=IngestBatchResponse,
    status_code=201,
    tags=["读数接入"],
    responses={
        201: {
            "description": (
                "已接入；若为同批次同内容的幂等重放，响应体与首次完全一致，"
                "并带响应头 Idempotent-Replay: true"
            )
        },
        409: {"description": "批次号已存在但内容不同，或同键读数与库中值不同"},
        422: {"description": "请求结构非法（含批内重复表号+时刻），或表号不存在"},
        503: {"description": "数据库暂时不可用，整批未生效，可重试"},
    },
)
def create_readings_batch(
    payload: IngestBatchRequest, response: Response
) -> IngestBatchResponse:
    """采集员补传读数：按批次号幂等，整批原子落库并返回回执。

    - 批内同一表号 + 时刻只能出现一次（否则 422）；
    - 返回本次新增条数与同键同值已存在条数；
    - 同批次同内容重试返回**首次回执原文**（响应头 Idempotent-Replay: true），
      不再触碰 readings，即使读数后来被直连 SQL 更正也不回写；
    - 同批次异内容 409；同键异值 409、无效表号 422，读数与回执均不留半份。
    """
    try:
        outcome = ingestmod.ingest_batch(payload.batch_id, payload.readings)
    except ingestmod.IngestError as exc:
        raise HTTPException(
            status_code=exc.http_status, detail={"message": exc.message}
        ) from exc
    if outcome.replayed:
        response.headers["Idempotent-Replay"] = "true"
    return IngestBatchResponse(
        batch_id=outcome.batch_id,
        inserted=outcome.inserted,
        existing=outcome.existing,
    )


@app.get("/jobs", response_model=list[JobOut], tags=["核对任务"])
def list_jobs() -> list[JobOut]:
    """列出近期任务（含终态），状态查询始终可用。"""
    return manager.list_jobs()


@app.post("/jobs", response_model=JobOut, status_code=201, tags=["核对任务"])
def create_job(payload: CreateJobRequest) -> JobOut:
    try:
        job = manager.create_job(payload.left_sql, payload.right_sql, payload.name)
    except SqlValidationError as exc:
        raise HTTPException(status_code=400, detail=f"SQL 校验失败：{exc}") from exc
    except SlotBusyError as exc:
        raise HTTPException(
            status_code=409,
            detail={
                "message": "当前已有核对任务在运行，一次只允许一组",
                "active_job_id": exc.active_job_id,
            },
        ) from exc
    return job.snapshot()


@app.get("/jobs/{job_id}", response_model=JobOut, tags=["核对任务"])
def get_job(job_id: str) -> JobOut:
    job = manager.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="任务不存在")
    return job.snapshot()


@app.post("/jobs/{job_id}/cancel", response_model=JobOut, tags=["核对任务"])
def cancel_job(job_id: str) -> JobOut:
    try:
        job = manager.request_cancel(job_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="任务不存在") from exc
    return job.snapshot()


@app.exception_handler(dbmod.DbError)
def _db_error_handler(_request, exc: dbmod.DbError) -> JSONResponse:
    return JSONResponse(status_code=422, detail={"message": str(exc)})
