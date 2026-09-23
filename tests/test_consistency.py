"""跨入口一致性：直接 API、HTTP 入口、崩溃后重放必须得到相同聚合结论。"""

from __future__ import annotations

import logging

import pytest
from fastapi.testclient import TestClient

from etsr.engine import EngineConfig, StreamRuntime
from etsr.source import LocalEventSource
from main import create_app

logger = logging.getLogger("etsr.test")


def _fingerprint_from_results(results: list) -> list[tuple]:
    return [
        (r.key, r.window_start_ms, r.window_end_ms, r.value, r.count,
         tuple(r.included_event_ids))
        for r in results
    ]


def test_python_api_matches_http_entry(
    tmp_path: pytest.TempPathFactory,
    out_of_order_events: list,
) -> None:
    # 入口 1：直接 Python API
    api_rt = StreamRuntime(
        EngineConfig(window_size_ms=1000, state_dir=str(tmp_path / "api"))  # type: ignore[operator]
    )
    api_rt.start()
    api_rt.run_source(LocalEventSource(out_of_order_events))
    api_rt.advance_watermark(10_000)
    expected = _fingerprint_from_results(api_rt.results())

    # 入口 2：HTTP（逐条投递 + 水位线标记）
    with TestClient(create_app(str(tmp_path / "http"))) as client:  # type: ignore[operator]
        for event in out_of_order_events:
            resp = client.post(
                "/events",
                json={
                    "offset": event.offset,
                    "event_time_ms": event.event_time_ms,
                    "key": event.key,
                    "value": event.value,
                    "event_id": event.event_id,
                },
            )
            assert resp.status_code == 200
        client.post("/watermark", json={"watermark_ms": 10_000})
        state = client.get("/state").json()

    http_fp = [
        (
            item["key"], item["window"][0], item["window"][1],
            float(item["value"]), item["count"], tuple(item["included_event_ids"]),
        )
        for item in state["finalized"]
    ]
    assert http_fp == expected
    logger.info(
        "api/http consistent | windows=%d watermark_ms=%s",
        len(http_fp), state["watermark_ms"],
    )


def test_http_state_is_explainable(tmp_path: pytest.TempPathFactory) -> None:
    with TestClient(create_app(str(tmp_path / "http"))) as client:  # type: ignore[operator]
        client.post("/events", json={"offset": 0, "event_time_ms": 200, "key": "a", "value": 1})
        client.post("/events", json={"offset": 1, "event_time_ms": 100, "key": "a", "value": 4})
        pending = client.get("/state").json()["pending_windows"]
        assert pending[0]["value"] == 5.0  # 关闭前可更新、可解释
        client.post("/watermark", json={"watermark_ms": 1000})
        client.post("/events", json={"offset": 2, "event_time_ms": 50, "key": "a", "value": 99})
        state = client.get("/state").json()
        assert state["finalized"][0]["value"] == 5.0
        assert state["late_events"][0]["reason"] == "LATE_AFTER_WINDOW_CLOSE"
        assert state["late_events"][0]["late_by_ms"] == 950


def test_restart_across_http_processes_replays_and_converges(
    tmp_path: pytest.TempPathFactory,
    out_of_order_events: list,
) -> None:
    """模拟两个 HTTP 进程：崩溃前 checkpoint，新进程从同一 state_dir 自动恢复。"""

    state_dir = str(tmp_path / "httpcrash")  # type: ignore[operator]
    with TestClient(create_app(state_dir)) as client:
        for event in out_of_order_events[:4]:
            client.post(
                "/events",
                json={
                    "offset": event.offset,
                    "event_time_ms": event.event_time_ms,
                    "key": event.key,
                    "value": event.value,
                },
            )
        client.post("/checkpoint")  # 崩溃前显式快照
        crashed = client.get("/state").json()
        logger.info("before crash | next_offset=%s wm=%s", crashed["next_offset"], crashed["watermark_ms"])

    # 新进程：lifespan 启动时从最新检查点恢复；继续投递 offset>=4 的事件。
    with TestClient(create_app(state_dir)) as client:
        state0 = client.get("/state").json()
        assert state0["next_offset"] == 4
        for event in out_of_order_events[4:]:
            client.post(
                "/events",
                json={
                    "offset": event.offset,
                    "event_time_ms": event.event_time_ms,
                    "key": event.key,
                    "value": event.value,
                },
            )
        # 重复投递崩溃前已确认的事件，不得重复计入。
        dup = client.post(
            "/events",
            json={"offset": 0, "event_time_ms": out_of_order_events[0].event_time_ms,
                  "key": "a", "value": 1},
        ).json()
        assert dup["duplicates"] == 1
        client.post("/watermark", json={"watermark_ms": 10_000})
        recovered_state = client.get("/state").json()

    # 与无故障参考运行对比。
    ref = StreamRuntime(
        EngineConfig(window_size_ms=1000, state_dir=str(tmp_path / "ref"))  # type: ignore[operator]
    )
    ref.start()
    ref.run_source(LocalEventSource(out_of_order_events))
    ref.advance_watermark(10_000)
    expected = _fingerprint_from_results(ref.results())
    recovered_fp = [
        (
            item["key"], item["window"][0], item["window"][1],
            float(item["value"]), item["count"], tuple(item["included_event_ids"]),
        )
        for item in recovered_state["finalized"]
    ]
    assert recovered_fp == expected
    logger.info(
        "crash-recovery over http consistent | windows=%d wm=%s",
        len(recovered_fp), recovered_state["watermark_ms"],
    )
