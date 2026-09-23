"""背压与有界资源：缓冲有界、过载原因可区分、退避后零丢失。"""

from __future__ import annotations

import logging

import pytest

from etsr.engine import EngineConfig, StreamRuntime
from etsr.errors import BufferOverloadError
from etsr.source import LocalEventSource

from conftest import make_event

logger = logging.getLogger("etsr.test")


def test_buffer_is_bounded_and_overload_is_distinguishable(
    tmp_path: pytest.TempPathFactory,
) -> None:
    rt = StreamRuntime(
        EngineConfig(
            window_size_ms=1000,
            buffer_capacity=2,
            process_budget_per_cycle=1,
            state_dir=str(tmp_path / "s"),  # type: ignore[operator]
        )
    )
    rt.start()
    rt.enqueue(make_event(0, 0, "a", 1.0))
    rt.enqueue(make_event(1, 10, "a", 1.0))
    with pytest.raises(BufferOverloadError) as exc:
        rt.enqueue(make_event(2, 20, "a", 1.0))
    assert exc.value.reason == "OVERLOADED_BUFFER_FULL"
    assert rt.buffer_in_flight <= 2  # 内存有界
    logger.info(
        "overload signaled | reason=%s in_flight=%d capacity=%d",
        exc.value.reason, rt.buffer_in_flight, 2,
    )


def test_batch_partial_overload_reports_rejected_tail(
    tmp_path: pytest.TempPathFactory,
) -> None:
    rt = StreamRuntime(
        EngineConfig(window_size_ms=1000, buffer_capacity=3, state_dir=str(tmp_path / "s"))  # type: ignore[operator]
    )
    rt.start()
    report = rt.ingest_batch([make_event(i, i * 10, "a", 1.0) for i in range(5)])
    assert report.accepted == 3
    assert report.rejected == 2
    assert [item.offset for item in report.rejected_items] == [3, 4]
    assert all(item.detail.startswith("OVERLOADED_BUFFER_FULL") for item in report.rejected_items)
    # 过载与迟到/重复的失败原因彼此可区分。
    assert {item.detail.split(":")[0] for item in report.rejected_items} == {
        "OVERLOADED_BUFFER_FULL"
    }


def test_backpressure_retry_loses_no_event_and_keeps_correct_results(
    tmp_path: pytest.TempPathFactory,
) -> None:
    """输入速率 4/周期、处理能力 1/周期、容量 2：触发背压但保序重试后结果正确。"""

    events = [make_event(i, i * 30, "a", float(i + 1)) for i in range(20)]

    ref = StreamRuntime(
        EngineConfig(window_size_ms=1000, state_dir=str(tmp_path / "ref"))  # type: ignore[operator]
    )
    ref.start()
    ref.run_source(LocalEventSource(events))
    ref.advance_watermark(10_000)

    slow = StreamRuntime(
        EngineConfig(
            window_size_ms=1000,
            buffer_capacity=2,
            process_budget_per_cycle=1,
            source_pull_per_cycle=4,
            state_dir=str(tmp_path / "slow"),  # type: ignore[operator]
        )
    )
    slow.start()
    report = slow.run_source(LocalEventSource(events))
    slow.advance_watermark(10_000)

    assert report.rejected > 0  # 确实发生了背压暂缓
    assert slow.buffer_in_flight == 0
    assert [
        (r.window_start_ms, r.value, r.count, tuple(r.included_event_ids))
        for r in slow.results()
    ] == [
        (r.window_start_ms, r.value, r.count, tuple(r.included_event_ids))
        for r in ref.results()
    ]
    total_count = sum(r.count for r in slow.results())
    assert total_count == 20  # 零丢失
    logger.info(
        "backpressure recovered | withheld_events=%d final_watermark_ms=%s total_count=%d",
        report.rejected, slow.watermark_ms, total_count,
    )


def test_in_flight_memory_never_exceeds_capacity(
    tmp_path: pytest.TempPathFactory,
) -> None:
    capacity = 3
    rt = StreamRuntime(
        EngineConfig(
            window_size_ms=1000,
            buffer_capacity=capacity,
            process_budget_per_cycle=2,
            source_pull_per_cycle=5,
            state_dir=str(tmp_path / "s"),  # type: ignore[operator]
        )
    )
    rt.start()
    events = [make_event(i, i, "a", 1.0) for i in range(30)]
    source = LocalEventSource(events)
    while source.has_next():
        burst = [source.next_event() for _ in range(5) if source.has_next()]
        pending = burst
        while pending:
            pending = rt.enqueue_batch(pending)
            assert rt.buffer_in_flight <= capacity
            rt.process_cycle()
    while rt.buffer_in_flight:
        rt.process_cycle()
        assert rt.buffer_in_flight <= capacity
