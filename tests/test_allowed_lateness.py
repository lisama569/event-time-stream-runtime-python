"""迟到容忍 + 结论修正：容忍边界、修正可见性、状态释放、重启重放一致性。"""

from __future__ import annotations

import logging

import pytest
from fastapi.testclient import TestClient

from etsr.engine import EngineConfig, StreamRuntime
from main import create_app

from conftest import make_event

logger = logging.getLogger("etsr.test")


def _runtime(tmp_path: pytest.TempPathFactory, name: str = "s", **kw: object) -> StreamRuntime:
    config = EngineConfig(window_size_ms=1000, state_dir=str(tmp_path / name), **kw)  # type: ignore[operator, arg-type]
    rt = StreamRuntime(config)
    rt.start()
    return rt


def _fingerprint(rt: StreamRuntime) -> dict[str, object]:
    return {
        "results": [
            (r.key, r.window_start_ms, r.window_end_ms, r.value, r.count,
             tuple(r.included_event_ids), r.revision)
            for r in rt.results()
        ],
        "corrections": [r.to_dict() for r in rt.corrections()],
        "late": [
            (x.event_offset, x.window_start_ms, x.reason)
            for x in rt.late_events()
        ],
        "watermark_ms": rt.watermark_ms,
    }


def test_late_event_within_tolerance_corrects_conclusion(
    tmp_path: pytest.TempPathFactory,
) -> None:
    rt = _runtime(tmp_path, allowed_lateness_ms=500)
    rt.ingest_event(make_event(0, 100, "a", 2.0))
    rt.ingest_event(make_event(1, 900, "a", 3.0))
    rt.advance_watermark(1000)  # 关闭 [0,1000)，首版结论 5.0
    assert rt.results()[0].value == 5.0
    assert rt.results()[0].revision == 0

    # 容忍期内的迟到事件：修正结论而不是走旁路。
    report = rt.ingest_event(make_event(2, 400, "a", 100.0))
    assert report.corrected == 1
    assert report.late == 0 and report.accepted == 0

    final = rt.results()[0]
    assert final.value == 105.0 and final.count == 3
    assert final.revision == 1 and final.corrected
    assert final.included_event_ids == [0, 1, 2]

    corrections = rt.corrections()
    assert len(corrections) == 1
    assert corrections[0].revision == 1
    assert corrections[0].value == 105.0
    assert corrections[0].triggering_offsets == [2]  # 参与本次修正的事件
    assert rt.late_events() == []  # 容忍期内不产生迟到旁路
    logger.info(
        "corrected | event_time_ms=400 watermark_ms=%s revision=%d value=%s",
        rt.watermark_ms, final.revision, final.value,
    )


def test_correction_does_not_disturb_watermark(
    tmp_path: pytest.TempPathFactory,
) -> None:
    rt = _runtime(tmp_path, allowed_lateness_ms=500)
    rt.ingest_event(make_event(0, 100, "a", 1.0))
    rt.advance_watermark(1000)
    wm_before = rt.watermark_ms
    rt.ingest_event(make_event(1, 200, "a", 5.0))  # 容忍期内修正
    assert rt.watermark_ms == wm_before == 1000  # 水位线不被迟到事件带乱


def test_tolerance_boundary_exactly_within_and_exactly_beyond(
    tmp_path: pytest.TempPathFactory,
) -> None:
    """容忍期 = [window_end, window_end + allowed_lateness)：边界两侧行为分明。"""

    rt = _runtime(tmp_path, allowed_lateness_ms=500)
    rt.ingest_event(make_event(0, 100, "a", 1.0))
    rt.advance_watermark(1000)  # 关闭 [0,1000)

    # 刚好仍在容忍范围内：watermark = end + lateness - 1 = 1499。
    rt.advance_watermark(1499)
    assert len(rt.correctable_windows()) == 1  # 状态仍保留
    rep_in = rt.ingest_event(make_event(1, 300, "a", 9.0))
    assert rep_in.corrected == 1 and rep_in.late == 0
    assert rt.results()[0].value == 10.0 and rt.results()[0].revision == 1

    # 刚好超出容忍范围：watermark = end + lateness = 1500，状态被释放。
    rt.advance_watermark(1500)
    assert rt.correctable_windows() == []
    rep_out = rt.ingest_event(make_event(2, 400, "a", 1000.0))
    assert rep_out.late == 1 and rep_out.corrected == 0
    late = rt.late_events()
    assert len(late) == 1
    assert late[0].reason == "LATE_BEYOND_ALLOWED_LATENESS"
    # 已给出的修正结论不被超容忍事件污染。
    assert rt.results()[0].value == 10.0 and rt.results()[0].revision == 1
    logger.info(
        "boundary | within: wm=1499 corrected; beyond: wm=1500 reason=%s",
        late[0].reason,
    )


def test_two_late_classes_are_distinguishable(
    tmp_path: pytest.TempPathFactory,
) -> None:
    """容忍期内修正 vs 超容忍旁路：两类结果走不同通道、reason 可区分。"""

    rt = _runtime(tmp_path, allowed_lateness_ms=500)
    rt.ingest_event(make_event(0, 100, "a", 1.0))
    rt.advance_watermark(1000)
    rt.ingest_event(make_event(1, 200, "a", 2.0))  # 容忍期内 -> 修正
    rt.advance_watermark(2000)
    rt.ingest_event(make_event(2, 300, "a", 4.0))  # 超容忍 -> 旁路

    assert [c.revision for c in rt.corrections()] == [1]
    assert [x.reason for x in rt.late_events()] == ["LATE_BEYOND_ALLOWED_LATENESS"]
    assert rt.results()[0].value == 3.0  # 只包含修正，不含超容忍事件


def test_correctable_state_is_released_after_tolerance(
    tmp_path: pytest.TempPathFactory,
) -> None:
    """修正状态有上界：水位线持续推进时，可修正窗口数量恒有界。"""

    rt = _runtime(tmp_path, allowed_lateness_ms=500)
    offset = 0
    max_correctable = 0
    for i in range(20):
        rt.ingest_event(make_event(offset, i * 1000 + 100, "a", 1.0))
        offset += 1
        rt.advance_watermark((i + 1) * 1000)
        max_correctable = max(max_correctable, len(rt.correctable_windows()))
    # 容忍 500ms < 窗口 1000ms：任一时刻至多 1 个窗口处于可修正状态。
    assert max_correctable <= 1
    rt.advance_watermark(20_500)  # 越过最后一个窗口的容忍期
    assert rt.correctable_windows() == []  # 最终全部释放
    assert len(rt.results()) == 20
    logger.info(
        "state bounded | max_correctable=%d finalized=%d watermark_ms=%s",
        max_correctable, len(rt.results()), rt.watermark_ms,
    )


def test_duplicate_redelivery_does_not_double_correct(
    tmp_path: pytest.TempPathFactory,
) -> None:
    rt = _runtime(tmp_path, allowed_lateness_ms=500)
    rt.ingest_event(make_event(0, 100, "a", 1.0))
    rt.advance_watermark(1000)
    rt.ingest_event(make_event(1, 200, "a", 5.0))  # 修正 -> 6.0

    dup = rt.ingest_event(make_event(1, 200, "a", 5.0))  # 同一条再投一次
    assert dup.duplicates == 1 and dup.corrected == 0
    assert rt.results()[0].value == 6.0
    assert rt.results()[0].revision == 1  # 同一次修正不被算两遍
    assert len(rt.corrections()) == 1


def test_restart_replay_correction_matches_failure_free(
    tmp_path: pytest.TempPathFactory,
) -> None:
    """崩溃重启 + 重复投递后，修正结论与全程无故障运行完全一致。"""

    kw: dict[str, object] = {"allowed_lateness_ms": 500}

    # 无故障参考运行。
    ref = _runtime(tmp_path, "ref", **kw)
    ref.ingest_event(make_event(0, 100, "a", 2.0))
    ref.ingest_event(make_event(1, 900, "a", 3.0))
    ref.advance_watermark(1000)
    ref.ingest_event(make_event(2, 400, "a", 100.0))  # 容忍期内修正
    ref.ingest_event(make_event(3, 2500, "a", 10.0))  # 推进水位线并释放旧状态
    ref.ingest_event(make_event(4, 600, "a", 50.0))   # 超容忍 -> 旁路
    ref.advance_watermark(3000)
    expected = _fingerprint(ref)

    # 故障运行：修正前崩溃，重启重放，并重复投递已确认事件。
    crashed = _runtime(tmp_path, "crash", **kw)
    crashed.ingest_event(make_event(0, 100, "a", 2.0))
    crashed.ingest_event(make_event(1, 900, "a", 3.0))
    crashed.advance_watermark(1000)
    cp = crashed.stop()  # 崩溃前检查点
    logger.info(
        "crash | checkpoint_id=%d next_offset=%d watermark_ms=%s",
        cp.checkpoint_id, cp.next_offset, cp.watermark_ms,
    )

    restored = _runtime(tmp_path, "crash", **kw)  # 同一 state_dir，自动恢复
    assert restored.next_offset == 2
    dup = restored.ingest_event(make_event(1, 900, "a", 3.0))  # 重放已确认段
    assert dup.duplicates == 1
    restored.ingest_event(make_event(2, 400, "a", 100.0))  # 恢复后修正
    cp2 = restored.checkpoint()  # 修正后再崩溃一次
    logger.info(
        "crash again | checkpoint_id=%d next_offset=%d watermark_ms=%s",
        cp2.checkpoint_id, cp2.next_offset, cp2.watermark_ms,
    )

    restored2 = _runtime(tmp_path, "crash", **kw)
    dup2 = restored2.ingest_event(make_event(2, 400, "a", 100.0))  # 修正事件重投
    assert dup2.duplicates == 1
    restored2.ingest_event(make_event(3, 2500, "a", 10.0))
    restored2.ingest_event(make_event(4, 600, "a", 50.0))
    restored2.advance_watermark(3000)

    assert _fingerprint(restored2) == expected
    logger.info(
        "restart consistent | revisions=%s corrections=%d watermark_ms=%s",
        [r.revision for r in restored2.results()],
        len(restored2.corrections()),
        restored2.watermark_ms,
    )


def test_disabled_by_default_matches_legacy_behavior(
    tmp_path: pytest.TempPathFactory,
) -> None:
    """不开启容忍（默认 allowed_lateness_ms=0）：表现与原有语义完全一致。"""

    rt = _runtime(tmp_path)  # 默认配置
    assert rt.explain()["allowed_lateness_ms"] == 0
    rt.ingest_event(make_event(0, 100, "a", 2.0))
    rt.advance_watermark(1000)
    report = rt.ingest_event(make_event(1, 400, "a", 100.0))
    assert report.late == 1 and report.corrected == 0
    assert rt.late_events()[0].reason == "LATE_AFTER_WINDOW_CLOSE"
    assert rt.results()[0].value == 2.0 and rt.results()[0].revision == 0
    assert rt.correctable_windows() == [] and rt.corrections() == []


def test_http_entry_sees_same_corrections_as_api(
    tmp_path: pytest.TempPathFactory,
) -> None:
    """HTTP 入口与直接调用看到的修正结论一致。"""

    config = EngineConfig(
        window_size_ms=1000,
        allowed_lateness_ms=500,
        state_dir=str(tmp_path / "http"),  # type: ignore[operator]
    )
    with TestClient(create_app(config=config)) as client:
        client.post("/events", json={"offset": 0, "event_time_ms": 100, "key": "a", "value": 2})
        client.post("/events", json={"offset": 1, "event_time_ms": 900, "key": "a", "value": 3})
        client.post("/watermark", json={"watermark_ms": 1000})
        resp = client.post(
            "/events", json={"offset": 2, "event_time_ms": 400, "key": "a", "value": 100}
        ).json()
        assert resp["corrected"] == 1 and resp["late"] == 0
        state = client.get("/state").json()

    finalized = state["finalized"][0]
    assert finalized["value"] == 105.0
    assert finalized["revision"] == 1 and finalized["corrected"] is True
    assert state["corrections"][0]["triggering_offsets"] == [2]
    assert state["corrections"][0]["value"] == 105.0
    assert state["allowed_lateness_ms"] == 500

    # 与直接 API 运行的指纹一致。
    api = _runtime(tmp_path, "api", allowed_lateness_ms=500)
    api.ingest_event(make_event(0, 100, "a", 2.0))
    api.ingest_event(make_event(1, 900, "a", 3.0))
    api.advance_watermark(1000)
    api.ingest_event(make_event(2, 400, "a", 100.0))
    assert finalized["value"] == api.results()[0].value
    assert finalized["revision"] == api.results()[0].revision
    assert state["corrections"][0]["value"] == api.corrections()[0].value
    logger.info(
        "http/api consistent | revision=%d value=%s watermark_ms=%s",
        finalized["revision"], finalized["value"], state["watermark_ms"],
    )
