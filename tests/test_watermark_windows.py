"""水位线推进与事件时间窗口聚合语义测试。"""

from __future__ import annotations

import logging

import pytest

from etsr.engine import EngineConfig, StreamRuntime
from etsr.watermark import WatermarkGenerator
from etsr.windows import TumblingWindowAssigner

from conftest import make_event

logger = logging.getLogger("etsr.test")


def test_window_assigner_tumbling_boundaries() -> None:
    assigner = TumblingWindowAssigner(1000)
    assert assigner.assign(0) == (0, 1000)
    assert assigner.assign(999) == (0, 1000)
    assert assigner.assign(1000) == (1000, 2000)
    assert assigner.assign(2500) == (2000, 3000)


def test_watermark_is_monotonic_and_never_regresses() -> None:
    wm = WatermarkGenerator(max_out_of_orderness_ms=500)
    assert wm.current_ms is None
    assert wm.observe(2000) == 1500
    logger.info("watermark after observe(2000)=%s", wm.current_ms)

    # 乱序旧事件不能拉低水位线。
    assert wm.observe(100) == 1500
    assert wm.observe(1999) == 1500
    # 显式心跳回退被忽略。
    assert wm.advance(10) == 1500
    assert wm.advance(5000) == 5000

    # 恢复后重放旧数据同样不能倒退。
    restored = WatermarkGenerator.restore(5000, max_out_of_orderness_ms=500)
    assert restored.observe(100) == 5000
    logger.info("restored watermark held at %s", restored.current_ms)


def test_out_of_order_window_is_updatable_until_close(
    tmp_path: pytest.TempPathFactory,
) -> None:
    rt = StreamRuntime(
        EngineConfig(window_size_ms=1000, state_dir=str(tmp_path / "s"))  # type: ignore[operator]
    )
    rt.start()

    rt.ingest_event(make_event(0, 900, "a", 3.0))
    rt.ingest_event(make_event(1, 100, "a", 7.0))

    pending = rt.pending_windows()
    assert len(pending) == 1
    # 关闭前结果可更新：乱序事件已并入，中间值即 10.0。
    assert pending[0]["value"] == 10.0
    assert pending[0]["count"] == 2
    assert pending[0]["included_event_ids"] == [0, 1]
    logger.info(
        "pending window before close | window=[%s,%s) partial_value=%s watermark_ms=%s",
        pending[0]["window_start_ms"], pending[0]["window_end_ms"],
        pending[0]["value"], rt.watermark_ms,
    )
    assert rt.results() == []  # 未关闭不得提前输出

    rt.advance_watermark(1000)
    results = rt.results()
    assert len(results) == 1
    assert results[0].window_start_ms == 0
    assert results[0].value == 10.0
    assert results[0].watermark_at_close_ms == 1000
    logger.info(
        "window finalized | start=%s end=%s value=%s watermark_at_close=%s",
        results[0].window_start_ms, results[0].window_end_ms,
        results[0].value, results[0].watermark_at_close_ms,
    )


def test_multiple_windows_close_in_order(
    tmp_path: pytest.TempPathFactory,
    out_of_order_events: list,
) -> None:
    rt = StreamRuntime(
        EngineConfig(window_size_ms=1000, state_dir=str(tmp_path / "s"))  # type: ignore[operator]
    )
    rt.start()
    rt.run_source(_source(out_of_order_events[:8]))
    rt.advance_watermark(10_000)

    by_window = {}
    for r in rt.results():
        by_window[(r.key, r.window_start_ms)] = r
    # offset 3(t=900) 在水位线 1500 后到达、offset 6(t=300) 在水位线 2500 后到达，
    # 属于迟到（由 test_late_events 覆盖），不计入此处结论。
    assert by_window[("a", 0)].value == 3.0 and by_window[("a", 0)].count == 2
    assert by_window[("b", 1000)].value == 14.0 and by_window[("b", 1000)].count == 2
    assert by_window[("a", 2000)].value == 20.0
    assert by_window[("b", 3000)].value == 30.0
    # 输出按窗口起点有序。
    assert [r.window_start_ms for r in rt.results()] == sorted(
        r.window_start_ms for r in rt.results()
    )


def _source(events: list):
    from etsr.source import LocalEventSource

    return LocalEventSource(events)


def test_allowed_lateness_keeps_reordering_in_window(
    tmp_path: pytest.TempPathFactory,
) -> None:
    """max_out_of_orderness_ms 容忍度内，水位线滞后，迟到一点的旧事件仍计入同窗口。"""

    rt = StreamRuntime(
        EngineConfig(
            window_size_ms=1000,
            max_out_of_orderness_ms=500,
            state_dir=str(tmp_path / "s"),  # type: ignore[operator]
        )
    )
    rt.start()
    rt.ingest_event(make_event(0, 1500, "a", 1.0))  # wm = 1000
    rt.ingest_event(make_event(1, 1050, "a", 4.0))  # 窗口 [1000,2000)，未关
    rt.ingest_event(make_event(2, 1000, "a", 2.0))  # 恰好窗口起点
    pending = {(w["key"], w["window_start_ms"]): w for w in rt.pending_windows()}
    assert pending[("a", 1000)]["value"] == 7.0
    rt.advance_watermark(2000)
    final = {r.window_start_ms: r for r in rt.results()}
    assert final[1000].value == 7.0 and final[1000].count == 3
