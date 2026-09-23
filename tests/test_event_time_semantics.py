"""乱序、迟到与水位线语义测试。"""

import pathlib

from stream_runtime import (
    CheckpointStore,
    Event,
    LocalEventSource,
    OverloadError,
    StreamRuntime,
)


def make_runtime(tmp_path: pathlib.Path, events, **kw):
    params = dict(window_size_ms=1000, allowed_lateness_ms=1000, buffer_capacity=8)
    params.update(kw)
    return StreamRuntime(
        LocalEventSource(events), CheckpointStore(tmp_path / "ckpt.json"), **params
    )


def test_out_of_order_events_update_open_window(tmp_path):
    """乱序事件在窗口关闭前到达，结果与按事件时间顺序到达一致。"""
    shuffled = [
        Event(0, 2500, "a", 2.0),
        Event(1, 1100, "a", 1.0),   # 乱序：属于 [1000,2000)
        Event(2, 1900, "a", 3.0),   # 乱序：同一窗口
        Event(3, 4200, "a", 4.0),   # 推进水位线关闭前面窗口
    ]
    ordered = sorted(shuffled, key=lambda e: e.event_time)
    ordered = [Event(i, e.event_time, e.key, e.value) for i, e in enumerate(ordered)]

    rt1 = make_runtime(tmp_path / "a", shuffled)
    rt1.run()
    rt2 = make_runtime(tmp_path / "b", ordered)
    rt2.run()

    assert rt1.results == rt2.results
    by_window = {(r.window_start, r.key): (r.count, r.total) for r in rt1.results}
    assert by_window[(1000, "a")] == (2, 4.0)
    assert by_window[(2000, "a")] == (1, 2.0)


def test_late_event_not_dropped_not_double_counted(tmp_path):
    """窗口关闭后到达的迟到事件：进入侧输出，不静默丢弃、不重复计入。"""
    events = [
        Event(0, 1000, "a", 1.0),
        Event(1, 5000, "a", 2.0),   # wm=4000，关闭 [1000,2000)
        Event(2, 1500, "a", 9.0),   # 迟到
    ]
    rt = make_runtime(tmp_path, events)
    rt.run()

    closed = {(r.window_start, r.key): (r.count, r.total) for r in rt.results}
    assert closed[(1000, "a")] == (1, 1.0)  # 迟到值 9.0 未重复计入
    assert len(rt.late_events) == 1          # 也未静默丢弃
    late = rt.late_events[0]
    assert late.event.offset == 2 and late.watermark == 4000
    assert "window closed" in late.reason


def test_watermark_never_regresses(tmp_path):
    events = [
        Event(0, 9000, "a", 1.0),
        Event(1, 1000, "a", 1.0),   # 大幅乱序
        Event(2, 5000, "a", 1.0),
    ]
    rt = make_runtime(tmp_path, events)
    rt.run()
    assert rt.watermark == 8000
    # 水位线历史单调：通过日志验证
    # （observe 的单调性由 WatermarkTracker 单元保证，这里验证终态）


def test_backpressure_overload_is_distinguishable(tmp_path):
    """缓冲有界：溢出抛 OverloadError，可与处理错误区分。"""
    rt = make_runtime(tmp_path, [], buffer_capacity=2)
    rt.submit(Event(0, 100, "a", 1.0))
    rt.submit(Event(1, 200, "a", 1.0))
    try:
        rt.submit(Event(2, 300, "a", 1.0))
        raised = None
    except OverloadError as exc:
        raised = exc
    assert raised is not None and raised.capacity == 2
    assert len(rt._buffer) == 2  # 有界，不无界增长


def test_logs_contain_event_time_watermark_checkpoint(tmp_path, pipeline_logs):
    events = [Event(0, 1000, "a", 1.0), Event(1, 5000, "a", 2.0)]
    rt = make_runtime(tmp_path, events)
    rt.run()
    text = pipeline_logs.text
    assert "event_time=1000" in text
    assert "watermark=4000" in text
    assert "checkpoint saved" in text and "offset=" in text
