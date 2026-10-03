"""FastAPI 入口：以 HTTP 方式驱动本地事件时间流处理运行时。"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from etsr.engine import EngineConfig, StreamRuntime
from etsr.errors import (
    BufferOverloadError,
    CheckpointRegressionError,
    OffsetGapError,
)
from etsr.model import Event


class EventIn(BaseModel):
    offset: int = Field(ge=0)
    event_time_ms: int
    key: str
    value: float
    event_id: str | None = None


class BatchIn(BaseModel):
    events: list[EventIn]


class MarkerIn(BaseModel):
    watermark_ms: int


def _to_event(item: EventIn) -> Event:
    return Event(
        offset=item.offset,
        event_time_ms=item.event_time_ms,
        key=item.key,
        value=item.value,
        event_id=item.event_id,
    )


def create_app(state_dir: str | None = None, config: EngineConfig | None = None) -> FastAPI:
    """构造 FastAPI 应用（测试中可注入独立 state_dir / 配置）。"""

    state_dir = state_dir or os.environ.get("ETSR_STATE_DIR", ".runtime_state")
    config = config or EngineConfig(state_dir=state_dir)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> Any:
        runtime = StreamRuntime(config)
        runtime.start()  # 启动即从最新检查点恢复，位点回退会在恢复路径被拒绝
        app.state.runtime = runtime
        yield
        runtime.stop()

    app = FastAPI(title="event-time-stream-runtime", lifespan=lifespan)

    def rt() -> StreamRuntime:
        return app.state.runtime  # type: ignore[no-any-return]

    @app.get("/health")
    def health() -> dict[str, Any]:
        return {"status": "ok"}

    @app.get("/state")
    def state() -> dict[str, Any]:
        return rt().explain()

    @app.post("/events")
    def ingest_event(event: EventIn) -> dict[str, Any]:
        try:
            report = rt().ingest_event(_to_event(event))
        except BufferOverloadError as exc:
            # 503 + 稳定 reason：过载，调用方退避重试。
            raise HTTPException(status_code=503, detail={"reason": exc.reason, "message": str(exc)})
        except OffsetGapError as exc:
            raise HTTPException(status_code=409, detail={"reason": exc.reason, "message": str(exc)})
        return _report_dict(report)

    @app.post("/events/batch")
    def ingest_batch(batch: BatchIn) -> dict[str, Any]:
        report = rt().ingest_batch([_to_event(e) for e in batch.events])
        return _report_dict(report)

    @app.post("/watermark")
    def advance_watermark(marker: MarkerIn) -> dict[str, Any]:
        report = rt().advance_watermark(marker.watermark_ms)
        return _report_dict(report)

    @app.post("/checkpoint")
    def checkpoint() -> dict[str, Any]:
        cp = rt().checkpoint()
        return {
            "checkpoint_id": cp.checkpoint_id,
            "next_offset": cp.next_offset,
            "watermark_ms": cp.watermark_ms,
            "pending_windows": len(cp.windows),
            "finalized_windows": len(cp.finalized_results),
        }

    @app.post("/restore/{checkpoint_id}")
    def restore(checkpoint_id: int) -> dict[str, Any]:
        try:
            cp = rt().restore_from(checkpoint_id)
        except CheckpointRegressionError as exc:
            raise HTTPException(status_code=409, detail={"reason": exc.reason, "message": str(exc)})
        return {
            "checkpoint_id": cp.checkpoint_id,
            "next_offset": cp.next_offset,
            "watermark_ms": cp.watermark_ms,
        }

    return app


def _report_dict(report: Any) -> dict[str, Any]:
    return {
        "accepted": report.accepted,
        "duplicates": report.duplicates,
        "late": report.late,
        "rejected": report.rejected,
        "corrected": report.corrected,
        "watermark_ms": report.watermark_ms,
        "windows_finalized": report.windows_finalized,
        "rejected_items": [
            {"kind": item.kind, "offset": item.offset, "detail": item.detail}
            for item in report.rejected_items
        ],
    }


app = create_app()
