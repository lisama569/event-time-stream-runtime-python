"""流处理运行时：有界缓冲、背压、水位线推进、迟到路由、检查点。"""

from __future__ import annotations

import logging
from collections import deque
from dataclasses import asdict

from .errors import OffsetRegressionError, OverloadError
from .events import Event, LateEvent, WindowResult
from .source import LocalEventSource
from .state import CheckpointStore
from .watermark import WatermarkTracker
from .windows import WindowStore, assign_window

logger = logging.getLogger("stream_runtime.pipeline")


class StreamRuntime:
    """事件时间窗口聚合运行时。

    语义保证：
    - 水位线单调不倒退；窗口关闭前乱序事件正常计入，关闭后迟到事件路由到
      late_events 侧输出，既不静默丢弃也不重复计入。
    - 检查点保存位点与聚合状态（含已输出结果）；恢复后结果与无故障运行一致。
    - 位点回退（恢复位点 < 检查点位点）抛 OffsetRegressionError。
    - 输入缓冲有界，溢出抛 OverloadError（背压反馈可区分）。
    """

    def __init__(
        self,
        source: LocalEventSource,
        checkpoint_store: CheckpointStore,
        *,
        window_size_ms: int,
        allowed_lateness_ms: int,
        buffer_capacity: int,
        checkpoint_interval: int = 1,
    ) -> None:
        if buffer_capacity <= 0:
            raise ValueError("buffer_capacity must be > 0")
        if checkpoint_interval <= 0:
            raise ValueError("checkpoint_interval must be > 0")
        self.source = source
        self.checkpoint_store = checkpoint_store
        self.watermarks = WatermarkTracker(allowed_lateness_ms)
        self.windows = WindowStore(window_size_ms)
        self.buffer_capacity = buffer_capacity
        self.checkpoint_interval = checkpoint_interval
        self._buffer: deque[Event] = deque()
        self._results: list[WindowResult] = []
        self._late_events: list[LateEvent] = []
        self._processed = 0

    @property
    def results(self) -> list[WindowResult]:
        return self._results

    @property
    def late_events(self) -> list[LateEvent]:
        return self._late_events

    @property
    def watermark(self) -> int:
        return self.watermarks.watermark

    @property
    def processed(self) -> int:
        return self._processed

    # ---- 输入（背压） ----

    def submit(self, event: Event) -> None:
        """向有界缓冲提交事件；缓冲满抛 OverloadError（可区分的过载反馈）。"""
        if len(self._buffer) >= self.buffer_capacity:
            raise OverloadError(self.buffer_capacity)
        self._buffer.append(event)

    # ---- 检查点与恢复 ----

    def checkpoint(self) -> None:
        snap = {
            "offset": self.source.offset,
            "processed": self._processed,
            "watermark": self.watermarks.snapshot(),
            "windows": self.windows.snapshot(),
            "results": [asdict(r) for r in self._results],
            "late_events": [
                {"event": asdict(le.event), "watermark": le.watermark, "reason": le.reason}
                for le in self._late_events
            ],
        }
        self.checkpoint_store.save(snap)
        logger.info(
            "checkpoint saved: offset=%d processed=%d watermark=%d open_windows=%d",
            snap["offset"],
            snap["processed"],
            self.watermarks.watermark,
            len(snap["windows"]["open"]),
        )

    def restore(self, requested_offset: int | None = None) -> bool:
        """从检查点恢复；位点回退抛 OffsetRegressionError。返回是否恢复到了检查点。"""
        data = self.checkpoint_store.load()
        if data is None:
            if requested_offset is not None:
                self.source.seek(requested_offset)
            return False
        checkpointed = int(data["offset"])
        if requested_offset is not None:
            if requested_offset < checkpointed:
                raise OffsetRegressionError(requested_offset, checkpointed)
            if requested_offset > checkpointed:
                raise ValueError(
                    f"cannot skip ahead of checkpoint: requested={requested_offset} "
                    f"> checkpointed={checkpointed}"
                )
        self.watermarks.restore(data["watermark"])
        self.windows.restore(data["windows"])
        self._processed = int(data.get("processed", 0))
        self._results = [WindowResult(**r) for r in data.get("results", [])]
        self._late_events = [
            LateEvent(event=Event(**le["event"]), watermark=le["watermark"], reason=le["reason"])
            for le in data.get("late_events", [])
        ]
        self.source.seek(checkpointed)
        logger.info(
            "restored from checkpoint: offset=%d processed=%d watermark=%d",
            checkpointed,
            self._processed,
            self.watermarks.watermark,
        )
        return True

    # ---- 运行 ----

    def run(self, max_events: int | None = None) -> None:
        """从源拉取事件进入有界缓冲并处理，直到源耗尽或达到 max_events。"""
        pulled = 0
        while True:
            # 每轮至多拉取一条进入有界缓冲，再处理一条：拉取与处理交替，
            # 源故障时已处理事件的检查点不会丢失。
            if len(self._buffer) < self.buffer_capacity and (
                max_events is None or pulled < max_events
            ):
                event = self.source.read()
                if event is not None:
                    self._buffer.append(event)
                    pulled += 1
            if not self._buffer:
                break
            event = self._buffer.popleft()
            self._process(event)
            if self._processed % self.checkpoint_interval == 0:
                self.checkpoint()
            if max_events is not None and pulled >= max_events and not self._buffer:
                break
        self.checkpoint()

    def _process(self, event: Event) -> None:
        wm = self.watermarks.observe(event.event_time)
        start, end = assign_window(event.event_time, self.windows.window_size_ms)
        if end <= wm:
            # 窗口已关闭：路由到迟到侧输出，不静默丢弃、不重复计入。
            self._late_events.append(LateEvent(event=event, watermark=wm))
            logger.info(
                "late event: offset=%d event_time=%d window=[%d,%d) watermark=%d",
                event.offset, event.event_time, start, end, wm,
            )
        else:
            self.windows.add(event)
            logger.info(
                "event accepted: offset=%d event_time=%d window=[%d,%d) watermark=%d",
                event.offset, event.event_time, start, end, wm,
            )
        closed = self.windows.close_windows(wm)
        for r in closed:
            logger.info(
                "window closed: [%d,%d) key=%s count=%d total=%s watermark=%d",
                r.window_start, r.window_end, r.key, r.count, r.total, wm,
            )
        self._results.extend(closed)
        self._processed += 1
