"""迟到容忍 + 结论修正：容忍边界、修正可见性、状态释放与重启重放一致性。"""

from __future__ import annotations

import logging

import pytest
from fastapi.testclient import TestClient

from etsr.engine import EngineConfig, StreamRuntime
from main import create_app

from conftest import make_event

logger = logging.getLogger("etsr.test")

WINDOW = 1000
LATENESS = 500  # 窗口 [0,1000) 的可修正期到 watermark >= 1500 为止


def _config(state_dir: str, **kw: object) -> EngineConfig:
    return EngineConfig(
        window_size_ms=WINDOW,
        allowed_lateness_ms=LATENESS,
        state_dir=state_dir,
        **kw,  # type: ignore[arg-type]
    )


def _fingerprint(rt: StreamRuntime) -> dict[str, object]:
    """与检查点 id 无关的结论指纹：崩溃恢复前后必须完全一致。"""

    return {
        "results": [
            (
                r.key, r.window_start_ms, r.window_end_ms, r.value, r.count,
                tuple(r.included_event_ids), r.revision, tuple(r.corrected_event_ids),
                r.watermark_at_close_ms,
            )
            for r in rt.results()
        ],
        "late": [
            (x.event_offset, x.reason, x.late_by_ms) for x in rt.late_events()
        ],
        "watermark_ms": rt.watermark_ms,
    }


def test_late_event_within_tolerance_corrects_finalized_result(tmp_path) -> None:
    rt = StreamRuntime(_config(str(tmp_path / "s")))
    rt.start()
    rt.ingest_event(make_event(0, 100, "a", 2.0))
    rt.ingest_event(make_event(1, 900, "a", 3.0))
    rt.advance_watermark(1000)  # 关闭 [0,1000)，初始结论 value=5.0 revision=0
    assert rt.results()[0].value == 5.0
    assert rt.results()[0].revision == 0

    # 容忍范围内（wm=1000 < 1000+500）的迟到事件 -> 修正结论，不进旁路。
    report = rt.ingest_event(make_event(2, 400, "a", 100.0))
    assert report.corrected == 1
    assert report.late == 0 and report.accepted == 0
    assert rt.late_events() == []

    final = rt.results()[0]
    assert final.value == 105.0 and final.count == 3
    assert final.revision == 1                      # 能看出被修正过
    assert final.corrected_event_ids == [2]         # 能看出谁参与了修正
    assert final.included_event_ids == [0, 1, 2]    # 修正后是多少、包含哪些事件
    assert final.watermark_at_close_ms == 1000      # 保留最初关闭时的水位线

    # 可修正状态在视图中可见，且标出容忍截止点。
    correctable = rt.explain()["correctable_windows"]
    assert len(correctable) == 1
    assert correctable[0]["correctable_until_ms"] == 1500
    logger.info(
        "corrected | window=[%d,%d) revision=%d value=%s watermark_ms=%s",
        final.window_start_ms, final.window_end_ms, final.revision,
        final.value, rt.watermark_ms,
    )


def test_tolerance_boundary_just_inside_and_just_outside(tmp_path) -> None:
    rt = StreamRuntime(_config(str(tmp_path / "s")))
    rt.start()
    rt.ingest_event(make_event(0, 100, "a", 1.0))
    rt.advance_watermark(1000)  # 关闭 [0,1000)

    # 刚好在容忍范围内：水位线推进到 1499（< 1000+500），状态仍未释放。
    rt.advance_watermark(1499)
    assert len(rt.explain()["correctable_windows"]) == 1
    rep_in = rt.ingest_event(make_event(1, 400, "a", 10.0))
    assert rep_in.corrected == 1
    assert rt.results()[0].value == 11.0 and rt.results()[0].revision == 1

    # 刚好超出：水位线到达 1500 == window_end + allowed_lateness -> 状态释放。
    rt.advance_watermark(1500)
    assert rt.explain()["correctable_windows"] == []
    rep_out = rt.ingest_event(make_event(2, 500, "a", 100.0))
    assert rep_out.late == 1 and rep_out.corrected == 0
    late = rt.late_events()
    assert len(late) == 1
    assert late[0].reason == "LATE_BEYOND_ALLOWED_LATENESS"  # 与默认迟到可区分
    assert late[0].event_offset == 2
    # 结论不被超出容忍的事件污染：保持修正后的 11.0。
    assert rt.results()[0].value == 11.0 and rt.results()[0].revision == 1
    logger.info(
        "boundary | inside corrected at wm=1499, outside side-output at wm=1500 "
        "reason=%s late_by_ms=%d",
        late[0].reason, late[0].late_by_ms,
    )


def test_multiple_corrections_increment_revision(tmp_path) -> None:
    rt = StreamRuntime(_config(str(tmp_path / "s")))
    rt.start()
    rt.ingest_event(make_event(0, 100, "a", 1.0))
    rt.advance_watermark(1000)
    rt.ingest_event(make_event(1, 200, "a", 10.0))
    rt.ingest_event(make_event(2, 300, "a", 100.0))
    final = rt.results()[0]
    assert final.revision == 2
    assert final.value == 111.0 and final.count == 3
    assert final.corrected_event_ids == [1, 2]


def test_redelivered_correcting_event_is_not_applied_twice(tmp_path) -> None:
    rt = StreamRuntime(_config(str(tmp_path / "s")))
    rt.start()
    rt.ingest_event(make_event(0, 100, "a", 1.0))
    rt.advance_watermark(1000)
    rt.ingest_event(make_event(1, 200, "a", 10.0))  # 修正 -> revision=1

    dup = rt.ingest_event(make_event(1, 200, "a", 10.0))  # 同一条再投一次
    assert dup.duplicates == 1 and dup.corrected == 0
    final = rt.results()[0]
    assert final.revision == 1            # 同一次修正不能算两遍
    assert final.value == 11.0 and final.count == 2
    assert final.corrected_event_ids == [1]
    # 水位线与窗口语义未被重复投递带乱。
    assert rt.watermark_ms == 1000
    assert rt.next_offset == 2


def test_correction_state_is_released_and_memory_bounded(tmp_path) -> None:
    """持续输入下，可修正状态只保留容忍期内的少数窗口，不随历史堆积。"""

    rt = StreamRuntime(_config(str(tmp_path / "s")))
    rt.start()
    offset = 0
    max_correctable = 0
    # 20 个窗口，每个窗口一条事件；水位线随事件时间持续推进。
    for i in range(20):
        rt.ingest_event(make_event(offset, i * WINDOW + 1, "a", 1.0))
        offset += 1
        max_correctable = max(
            max_correctable, len(rt.explain()["correctable_windows"])
        )
    # 任一时刻至多 1 个窗口处于容忍期内（window_end <= wm < window_end + 500）。
    assert max_correctable <= 1
    # 历史再长也不堆积：最后只有尾部窗口可能仍在容忍期。
    assert len(rt.explain()["correctable_windows"]) <= 1
    assert len(rt.results()) >= 19
    logger.info(
        "bounded correction state | max_correctable=%d finalized=%d watermark_ms=%s",
        max_correctable, len(rt.results()), rt.watermark_ms,
    )


def test_restart_replay_correction_matches_failure_free_run(tmp_path) -> None:
    """崩溃重启 + 重复投递后，修正结论与全程无故障运行完全一致。"""

    def drive(rt: StreamRuntime, events: list, marks: list[int]) -> None:
        for event, mark in zip(events, marks):
            rt.ingest_event(event)
            if mark is not None:
                rt.advance_watermark(mark)

    timeline = [
        (make_event(0, 100, "a", 1.0), None),
        (make_event(1, 900, "a", 2.0), 1000),   # 关闭 [0,1000)，结论 3.0
        (make_event(2, 400, "a", 10.0), None),  # 容忍内修正 -> 13.0 rev1
        (make_event(3, 500, "a", 100.0), None),  # 再次修正 -> 113.0 rev2
        (make_event(4, 2000, "a", 5.0), None),  # wm->2000，[0,1000) 状态释放
        (make_event(5, 600, "a", 7.0), None),   # 超出容忍 -> 旁路
        (make_event(6, 2500, "a", 6.0), 3000),  # 关闭 [2000,3000)
    ]
    events = [e for e, _ in timeline]
    marks = [m for _, m in timeline]

    # 参考运行：全程无故障。
    ref = StreamRuntime(_config(str(tmp_path / "ref")))
    ref.start()
    drive(ref, events, marks)
    expected = _fingerprint(ref)

    # 故障运行：处理到 offset=2（含一次修正）后检查点并“崩溃”。
    crashed = StreamRuntime(_config(str(tmp_path / "crash")))
    crashed.start()
    drive(crashed, events[:3], marks[:3])
    cp = crashed.checkpoint()
    logger.info(
        "crash | checkpoint_id=%d next_offset=%d watermark_ms=%s",
        cp.checkpoint_id, cp.next_offset, cp.watermark_ms,
    )
    del crashed

    # 重启：从检查点恢复，重放剩余事件，并重复投递崩溃前已确认的修正事件。
    resumed = StreamRuntime(_config(str(tmp_path / "crash")))
    resumed.start()
    assert resumed.next_offset == 3
    assert len(resumed.explain()["correctable_windows"]) == 1  # 可修正状态随检查点恢复
    dup = resumed.ingest_event(events[2])  # at-least-once 重投修正事件
    assert dup.duplicates == 1 and dup.corrected == 0
    drive(resumed, events[3:], marks[3:])

    assert _fingerprint(resumed) == expected
    # 重复投递被单独记账，不影响结论。
    assert resumed.explain()["duplicate_offsets"] == [2]
    final = resumed.results()[0]
    assert (final.value, final.revision) == (113.0, 2)
    assert final.corrected_event_ids == [2, 3]
    assert resumed.late_events()[0].reason == "LATE_BEYOND_ALLOWED_LATENESS"
    logger.info(
        "restart consistent | revision=%d value=%s corrected_by=%s watermark_ms=%s",
        final.revision, final.value, final.corrected_event_ids, resumed.watermark_ms,
    )


def test_correction_visible_consistently_via_python_and_http(tmp_path) -> None:
    """修正前后，直接调用与 HTTP 入口看到的结论一致。"""

    cfg = _config(str(tmp_path / "http"))
    with TestClient(create_app(config=cfg)) as client:
        client.post("/events", json={"offset": 0, "event_time_ms": 100, "key": "a", "value": 2.0})
        client.post("/watermark", json={"watermark_ms": 1000})
        before = client.get("/state").json()["finalized"][0]
        assert before["value"] == 2.0 and before["revision"] == 0

        resp = client.post(
            "/events", json={"offset": 1, "event_time_ms": 400, "key": "a", "value": 40.0}
        )
        assert resp.status_code == 200
        assert resp.json()["corrected"] == 1 and resp.json()["late"] == 0
        state = client.get("/state").json()

    # 同样的序列走直接调用，结论必须一致。
    rt = StreamRuntime(_config(str(tmp_path / "api")))
    rt.start()
    rt.ingest_event(make_event(0, 100, "a", 2.0))
    rt.advance_watermark(1000)
    rt.ingest_event(make_event(1, 400, "a", 40.0))
    api_final = rt.results()[0]

    http_final = state["finalized"][0]
    assert http_final["value"] == api_final.value == 42.0
    assert http_final["revision"] == api_final.revision == 1
    assert http_final["corrected_event_ids"] == api_final.corrected_event_ids == [1]
    assert state["correctable_windows"][0]["correctable_until_ms"] == 1500
    assert state["late_events"] == []


def test_default_config_without_tolerance_is_unchanged(tmp_path) -> None:
    """不开启容忍（默认 allowed_lateness_ms=0）时行为与原来完全一致。"""

    rt = StreamRuntime(
        EngineConfig(window_size_ms=WINDOW, state_dir=str(tmp_path / "s"))
    )
    rt.start()
    rt.ingest_event(make_event(0, 100, "a", 5.0))
    rt.advance_watermark(1000)
    report = rt.ingest_event(make_event(1, 400, "a", 100.0))
    assert report.late == 1 and report.corrected == 0
    assert rt.late_events()[0].reason == "LATE_AFTER_WINDOW_CLOSE"
    final = rt.results()[0]
    assert final.value == 5.0 and final.revision == 0
    assert rt.explain()["correctable_windows"] == []
