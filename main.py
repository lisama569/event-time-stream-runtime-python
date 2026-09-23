"""演示入口：乱序 + 迟到 + 崩溃恢复 的完整场景，各阶段结论可对照解释。"""

from __future__ import annotations

import logging
import tempfile
from pathlib import Path

from stream_runtime import CheckpointStore, Event, LocalEventSource, StreamRuntime
from stream_runtime.source import InjectedSourceError

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

EVENTS = [
    Event(0, 1000, "a", 1.0),
    Event(1, 2500, "a", 2.0),
    Event(2, 1800, "b", 3.0),   # 乱序：窗口未关闭，正常计入
    Event(3, 5000, "a", 4.0),
    Event(4, 1200, "a", 9.0),   # 迟到：窗口已关闭，进入侧输出
    Event(5, 6200, "b", 5.0),
]

PARAMS = dict(window_size_ms=1000, allowed_lateness_ms=1000, buffer_capacity=8)


def summarize(title: str, rt: StreamRuntime) -> None:
    print(f"\n== {title} ==")
    for r in rt.results:
        print(f"  window=[{r.window_start},{r.window_end}) key={r.key} "
              f"count={r.count} total={r.total}")
    for le in rt.late_events:
        print(f"  LATE offset={le.event.offset} event_time={le.event.event_time} "
              f"watermark={le.watermark} ({le.reason})")
    print(f"  final watermark={rt.watermark}")


def main() -> None:
    workdir = Path(tempfile.mkdtemp(prefix="stream-runtime-demo-"))

    # 1) 无故障基线
    baseline = StreamRuntime(
        LocalEventSource(EVENTS), CheckpointStore(workdir / "baseline.json"), **PARAMS
    )
    baseline.run()
    summarize("baseline (no failure)", baseline)

    # 2) 故障运行：读 3 条后崩溃，重启后从检查点恢复重放
    ckpt = CheckpointStore(workdir / "crash.json")
    crashing = LocalEventSource(EVENTS)
    crashing.inject_failure(fail_after=3)
    rt = StreamRuntime(crashing, ckpt, **PARAMS)
    try:
        rt.run()
    except InjectedSourceError as exc:
        print(f"\n!! injected crash: {exc}")

    recovered = StreamRuntime(LocalEventSource(EVENTS), ckpt, **PARAMS)
    recovered.restore()
    recovered.run()
    summarize("recovered (crash + replay)", recovered)

    # 3) 一致性断言：两条路径结论必须一致
    assert recovered.results == baseline.results
    assert [le.event.offset for le in recovered.late_events] == [
        le.event.offset for le in baseline.late_events
    ]
    print("\nOK: recovered results identical to baseline")


if __name__ == "__main__":
    main()
