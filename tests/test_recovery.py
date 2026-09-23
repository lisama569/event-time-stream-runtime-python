"""重启重放、位点回退与检查点损坏测试。"""

import pathlib

import pytest

from stream_runtime import (
    CheckpointCorruptionError,
    CheckpointStore,
    Event,
    LocalEventSource,
    OffsetRegressionError,
    StreamRuntime,
)
from stream_runtime.source import InjectedSourceError


def make_events():
    return [
        Event(0, 1000, "a", 1.0),
        Event(1, 2500, "a", 2.0),
        Event(2, 1800, "b", 3.0),   # 乱序
        Event(3, 5000, "a", 4.0),
        Event(4, 1200, "a", 9.0),   # 迟到
        Event(5, 6200, "b", 5.0),
    ]


def make_runtime(tmp_path: pathlib.Path, events, **kw):
    params = dict(window_size_ms=1000, allowed_lateness_ms=1000, buffer_capacity=8)
    params.update(kw)
    return StreamRuntime(
        LocalEventSource(events), CheckpointStore(tmp_path / "ckpt.json"), **params
    )


def test_restart_replay_matches_no_failure_run(tmp_path):
    """崩溃后从检查点恢复并重放，结果与无故障运行完全一致。"""
    events = make_events()

    # 无故障基线
    baseline = make_runtime(tmp_path / "baseline", events)
    baseline.run()

    # 故障运行：读 3 条后崩溃
    crash_dir = tmp_path / "crash"
    source = LocalEventSource(events)
    source.inject_failure(fail_after=3)
    rt = StreamRuntime(
        source, CheckpointStore(crash_dir / "ckpt.json"),
        window_size_ms=1000, allowed_lateness_ms=1000, buffer_capacity=8,
    )
    with pytest.raises(InjectedSourceError):
        rt.run()

    # 重启：新运行时从检查点恢复，源从检查点位点重放
    rt2 = StreamRuntime(
        LocalEventSource(events), CheckpointStore(crash_dir / "ckpt.json"),
        window_size_ms=1000, allowed_lateness_ms=1000, buffer_capacity=8,
    )
    assert rt2.restore() is True
    rt2.run()

    assert rt2.results == baseline.results
    assert [(le.event.offset, le.watermark) for le in rt2.late_events] == [
        (le.event.offset, le.watermark) for le in baseline.late_events
    ]
    assert rt2.watermark == baseline.watermark


def test_duplicate_delivery_after_restore_is_consistent(tmp_path):
    """重复投递（从检查点位点重放同一批事件）不产生重复输出。"""
    events = make_events()
    rt = make_runtime(tmp_path / "d", events)
    rt.run(max_events=3)          # 处理一部分后停止（模拟宕机前）
    ckpt_offset = rt.source.offset

    # 重启 + 从检查点位点重放（at-least-once 重复投递）
    rt2 = make_runtime(tmp_path / "d", events)
    rt2.restore()
    assert rt2.source.offset == ckpt_offset
    rt2.run()

    baseline = make_runtime(tmp_path / "base", events)
    baseline.run()
    assert rt2.results == baseline.results


def test_offset_regression_rejected(tmp_path):
    """位点回退必须被明确拒绝。"""
    events = make_events()
    rt = make_runtime(tmp_path, events)
    rt.run(max_events=4)
    checkpointed = rt.source.offset
    assert checkpointed > 0

    rt2 = make_runtime(tmp_path, events)
    with pytest.raises(OffsetRegressionError) as exc_info:
        rt2.restore(requested_offset=checkpointed - 1)
    assert exc_info.value.requested == checkpointed - 1
    assert exc_info.value.checkpointed == checkpointed


def test_corrupted_checkpoint_is_distinguishable(tmp_path):
    events = make_events()
    rt = make_runtime(tmp_path, events)
    rt.run(max_events=2)
    (tmp_path / "ckpt.json").write_text("{broken json", encoding="utf-8")

    rt2 = make_runtime(tmp_path, events)
    with pytest.raises(CheckpointCorruptionError):
        rt2.restore()


def test_restore_logs_checkpoint_info(tmp_path, pipeline_logs):
    events = make_events()
    rt = make_runtime(tmp_path, events)
    rt.run(max_events=2)

    rt2 = make_runtime(tmp_path, events)
    rt2.restore()
    assert "restored from checkpoint" in pipeline_logs.text
    assert "watermark=" in pipeline_logs.text
