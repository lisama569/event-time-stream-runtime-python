"""端到端演示：无故障运行 vs 崩溃后从检查点重放，聚合结论必须一致。

运行：uv run python scripts/demo.py
"""

from __future__ import annotations

import logging
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from etsr.engine import EngineConfig, StreamRuntime  # noqa: E402
from etsr.model import Event  # noqa: E402
from etsr.source import LocalEventSource  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger("etsr.demo")


def build_events() -> list[Event]:
    # offset 连续，事件时间故意乱序，并夹杂一条将在窗口关闭后到达的迟到事件。
    rows = [
        (0, 200, "a", 1.0),
        (1, 100, "a", 2.0),
        (2, 1500, "b", 10.0),
        (3, 900, "a", 3.0),
        (4, 1200, "b", 4.0),
        (5, 2500, "a", 20.0),
        (6, 300, "a", 5.0),
        (7, 3000, "b", 30.0),
        (8, 400, "a", 99.0),   # 水位线推高后到达 -> 迟到旁路
    ]
    return [Event(offset=i, event_time_ms=t, key=k, value=v, event_id=f"evt-{i}")
            for i, t, k, v in rows]


def fingerprint(rt: StreamRuntime) -> list[tuple]:
    return [
        (r.key, r.window_start_ms, r.window_end_ms, r.value, r.count,
         tuple(r.included_event_ids))
        for r in rt.results()
    ]


def run_failure_free(events: list[Event], state_dir: str) -> tuple[list[tuple], list]:
    rt = StreamRuntime(EngineConfig(window_size_ms=1000, state_dir=state_dir))
    rt.start()
    rt.run_source(LocalEventSource(events))
    rt.advance_watermark(10_000)
    rt.stop()
    return fingerprint(rt), rt.late_events()


def run_with_crash_and_replay(events: list[Event], state_dir: str) -> tuple[list[tuple], list]:
    # 第一次"进程"：处理前 5 条后崩溃，stop 留下一致性检查点（next_offset=5）。
    rt1 = StreamRuntime(EngineConfig(window_size_ms=1000, state_dir=state_dir))
    rt1.start()
    rt1.run_source(LocalEventSource(events), max_events=5)
    cp = rt1.stop()
    logger.info("进程崩溃，最后检查点 id=%d next_offset=%d", cp.checkpoint_id, cp.next_offset)

    # 第二次"进程"：新实例从同一目录自动恢复，源从位点 5 重放剩余事件。
    rt2 = StreamRuntime(EngineConfig(window_size_ms=1000, state_dir=state_dir))
    rt2.start()
    source = LocalEventSource(events)
    rt2.run_source(source.replay_from(5))
    rt2.advance_watermark(10_000)

    # 崩溃边界重复投递：再把 0..4 投递一遍（at-least-once），必须幂等。
    rt2.run_source(LocalEventSource(events).replay_from(0))
    rt2.stop()
    return fingerprint(rt2), rt2.late_events()


def main() -> None:
    events = build_events()
    with tempfile.TemporaryDirectory() as tmp:
        ok_fp, ok_late = run_failure_free(events, str(Path(tmp) / "clean"))
        rec_fp, rec_late = run_with_crash_and_replay(events, str(Path(tmp) / "crash"))

    print("\n无故障运行窗口结论:")
    for key, start, end, value, count, ids in ok_fp:
        print(f"  key={key} window=[{start},{end}) SUM={value} count={count} offsets={ids}")
    print(f"  迟到旁路: {[(x.event_offset, x.late_by_ms) for x in ok_late]}")

    print("\n崩溃恢复运行窗口结论:")
    for key, start, end, value, count, ids in rec_fp:
        print(f"  key={key} window=[{start},{end}) SUM={value} count={count} offsets={ids}")
    print(f"  迟到旁路: {[(x.event_offset, x.late_by_ms) for x in rec_late]}")

    assert ok_fp == rec_fp, "恢复后结论与无故障运行不一致"
    print("\n✅ 崩溃恢复 + 重复投递后，聚合结论与无故障运行完全一致。")


if __name__ == "__main__":
    main()
