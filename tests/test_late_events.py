"""迟到事件与重复投递语义：迟到走旁路、不静默丢弃、不重复计入。"""

from __future__ import annotations

import logging

import pytest

from etsr.engine import EngineConfig, StreamRuntime

from conftest import make_event

logger = logging.getLogger("etsr.test")


def test_late_event_after_window_close_goes_to_side_output(
    tmp_path: pytest.TempPathFactory,
) -> None:
    rt = StreamRuntime(
        EngineConfig(window_size_ms=1000, state_dir=str(tmp_path / "s"))  # type: ignore[operator]
    )
    rt.start()
    rt.ingest_event(make_event(0, 100, "a", 2.0))
    rt.ingest_event(make_event(1, 900, "a", 3.0))
    rt.advance_watermark(1000)  # 关闭 [0,1000)
    assert rt.results()[0].value == 5.0

    # 窗口关闭后的迟到事件。
    report = rt.ingest_event(make_event(2, 400, "a", 100.0))
    assert report.late == 1
    assert report.accepted == 0

    late = rt.late_events()
    assert len(late) == 1
    assert late[0].event_offset == 2
    assert late[0].window_start_ms == 0 and late[0].window_end_ms == 1000
    assert late[0].watermark_ms == 1000
    assert late[0].late_by_ms == 600
    assert late[0].reason == "LATE_AFTER_WINDOW_CLOSE"
    logger.info(
        "late event | event_time_ms=%d watermark_ms=%d late_by_ms=%d reason=%s",
        late[0].event_time_ms, late[0].watermark_ms, late[0].late_by_ms, late[0].reason,
    )

    # 最终结果不被迟到事件污染：值仍是 5.0，计数仍是 2。
    final = rt.results()[0]
    assert final.value == 5.0 and final.count == 2


def test_event_before_watermark_but_window_open_is_still_accepted(
    tmp_path: pytest.TempPathFactory,
) -> None:
    """水位线由同窗口事件推进时，更旧但窗口未关的乱序事件仍可更新结果。"""

    rt = StreamRuntime(
        EngineConfig(window_size_ms=1000, state_dir=str(tmp_path / "s"))  # type: ignore[operator]
    )
    rt.start()
    rt.ingest_event(make_event(0, 900, "a", 1.0))  # wm -> 900
    rt.ingest_event(make_event(1, 100, "a", 4.0))  # event_time<wm，但窗口仍开
    pending = rt.pending_windows()
    assert pending[0]["value"] == 5.0 and pending[0]["count"] == 2
    rt.advance_watermark(1000)
    assert rt.results()[0].value == 5.0
    assert rt.late_events() == []


def test_duplicate_delivery_is_idempotent(
    tmp_path: pytest.TempPathFactory,
) -> None:
    """at-least-once 重复投递：同 offset 再来一次只计数为 duplicate，结果不变。"""

    rt = StreamRuntime(
        EngineConfig(window_size_ms=1000, state_dir=str(tmp_path / "s"))  # type: ignore[operator]
    )
    rt.start()
    rt.ingest_event(make_event(0, 100, "a", 2.0))
    rt.ingest_event(make_event(1, 200, "a", 3.0))

    dup_report = rt.ingest_event(make_event(0, 100, "a", 2.0))
    assert dup_report.duplicates == 1
    assert dup_report.accepted == 0

    rt.advance_watermark(1000)
    final = rt.results()[0]
    assert final.value == 5.0 and final.count == 2
    assert final.included_event_ids == [0, 1]
    assert rt.explain()["duplicate_offsets"] == [0]
    logger.info(
        "duplicate handled | watermark_ms=%s result_value=%s duplicates=%s",
        rt.watermark_ms, final.value, rt.explain()["duplicate_offsets"],
    )


def test_late_and_duplicate_reasons_are_distinguishable(
    tmp_path: pytest.TempPathFactory,
) -> None:
    rt = StreamRuntime(
        EngineConfig(window_size_ms=1000, state_dir=str(tmp_path / "s"))  # type: ignore[operator]
    )
    rt.start()
    rt.ingest_event(make_event(0, 100, "a", 1.0))
    rt.advance_watermark(1000)

    late_rep = rt.ingest_event(make_event(1, 500, "a", 1.0))
    dup_rep = rt.ingest_event(make_event(0, 100, "a", 1.0))
    assert (late_rep.late, late_rep.duplicates) == (1, 0)
    assert (dup_rep.late, dup_rep.duplicates) == (0, 1)
    assert rt.late_events()[0].reason == "LATE_AFTER_WINDOW_CLOSE"
