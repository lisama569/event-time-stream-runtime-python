"""HTTP 入口的过载 503、批量拒绝与检查点/恢复接口。"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from etsr.engine import EngineConfig
from main import create_app

from conftest import make_event


def test_http_single_event_overload_returns_503(
    tmp_path: pytest.TempPathFactory,
) -> None:
    cfg = EngineConfig(
        window_size_ms=1000,
        buffer_capacity=2,
        process_budget_per_cycle=1,
        state_dir=str(tmp_path / "s"),  # type: ignore[operator]
    )
    with TestClient(create_app(config=cfg)) as client:
        # 先直接占满有界缓冲（不触发处理），制造“输入速率>处理能力”。
        client.app.state.runtime.enqueue(make_event(0, 0, "a", 1.0))
        client.app.state.runtime.enqueue(make_event(1, 10, "a", 1.0))
        resp = client.post(
            "/events", json={"offset": 2, "event_time_ms": 20, "key": "a", "value": 1.0}
        )
        assert resp.status_code == 503
        assert resp.json()["detail"]["reason"] == "OVERLOADED_BUFFER_FULL"


def test_http_checkpoint_and_stale_restore(
    tmp_path: pytest.TempPathFactory,
) -> None:
    with TestClient(create_app(str(tmp_path / "s"))) as client:  # type: ignore[operator]
        client.post("/events", json={"offset": 0, "event_time_ms": 100, "key": "a", "value": 1})
        cp1 = client.post("/checkpoint").json()
        assert cp1["checkpoint_id"] == 1 and cp1["next_offset"] == 1
        client.post("/events", json={"offset": 1, "event_time_ms": 200, "key": "a", "value": 2})
        cp2 = client.post("/checkpoint").json()
        assert cp2["checkpoint_id"] == 2 and cp2["next_offset"] == 2
        resp = client.post("/restore/1")
        assert resp.status_code == 409
        assert resp.json()["detail"]["reason"] == "STALE_CHECKPOINT"
        ok = client.post("/restore/2")
        assert ok.status_code == 200 and ok.json()["next_offset"] == 2


def test_http_batch_overload_report(
    tmp_path: pytest.TempPathFactory,
) -> None:
    cfg = EngineConfig(
        window_size_ms=1000,
        buffer_capacity=2,
        process_budget_per_cycle=10,
        state_dir=str(tmp_path / "s"),  # type: ignore[operator]
    )
    with TestClient(create_app(config=cfg)) as client:
        resp = client.post(
            "/events/batch",
            json={"events": [
                {"offset": i, "event_time_ms": i * 10, "key": "a", "value": 1.0}
                for i in range(5)
            ]},
        )
        body = resp.json()
        assert body["accepted"] == 2
        assert body["rejected"] == 3
        assert [item["offset"] for item in body["rejected_items"]] == [2, 3, 4]
