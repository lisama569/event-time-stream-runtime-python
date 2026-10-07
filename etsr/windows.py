"""窗口分配与窗口聚合状态（SUM / COUNT / MIN / MAX / AVG）。"""

from __future__ import annotations

from typing import Any

from etsr.model import (
    Event,
    WindowResult,
    accumulate,
    aggregate_value,
    new_accumulator,
)


class TumblingWindowAssigner:
    """事件时间滚动窗口：[floor(t/size)*size, floor(t/size)*size + size)。"""

    def __init__(self, size_ms: int) -> None:
        if size_ms <= 0:
            raise ValueError("window size must be positive")
        self.size_ms = int(size_ms)

    def assign(self, event_time_ms: int) -> tuple[int, int]:
        start = (int(event_time_ms) // self.size_ms) * self.size_ms
        return start, start + self.size_ms


class WindowAggregationState:
    """单个 (key, window) 的可更新聚合状态。

    窗口关闭前可反复并入乱序事件；按源位点去重，保证重启重放/重复投递
    不会重复计入。
    """

    def __init__(
        self,
        key: str,
        window_start_ms: int,
        window_end_ms: int,
        aggregate: str = "SUM",
    ) -> None:
        self.key = key
        self.window_start_ms = int(window_start_ms)
        self.window_end_ms = int(window_end_ms)
        self.aggregate = aggregate
        self._acc: dict[str, Any] = new_accumulator()
        self._offsets: set[int] = set()
        self.included_event_ids: list[int] = []
        self.first_event_time_ms: int | None = None
        self.last_event_time_ms: int | None = None
        self.updated_at_checkpoints: list[int] = []
        # 结论版本号：0=首次定稿；迟到容忍期内每次修正 +1。
        self.revision = 0
        # 窗口关闭那一刻的水位线：关闭时定格，之后的迟到修正不得改写。
        self.closed_watermark_ms: int | None = None

    @property
    def count(self) -> int:
        return self._acc["count"]

    def contains_offset(self, offset: int) -> bool:
        return int(offset) in self._offsets

    def add(self, event: Event, checkpoint_id: int) -> bool:
        """并入一条事件；重复位点返回 False（幂等，不重复计入）。"""

        offset = int(event.offset)
        if offset in self._offsets:
            return False
        self._offsets.add(offset)
        self.included_event_ids.append(offset)
        accumulate(self._acc, float(event.value))
        et = int(event.event_time_ms)
        if self.first_event_time_ms is None or et < self.first_event_time_ms:
            self.first_event_time_ms = et
        if self.last_event_time_ms is None or et > self.last_event_time_ms:
            self.last_event_time_ms = et
        if not self.updated_at_checkpoints or self.updated_at_checkpoints[-1] != checkpoint_id:
            self.updated_at_checkpoints.append(checkpoint_id)
        return True

    def preliminary_value(self) -> float:
        """窗口关闭前的中间聚合值（乱序期间可更新）。"""

        return aggregate_value(self.aggregate, self._acc)

    def finalize(self, watermark_ms: int, checkpoint_id: int | None) -> WindowResult:
        """窗口关闭，产出不可变的最终聚合结论。"""

        return WindowResult(
            key=self.key,
            window_start_ms=self.window_start_ms,
            window_end_ms=self.window_end_ms,
            aggregate=self.aggregate,
            value=aggregate_value(self.aggregate, self._acc),
            count=self._acc["count"],
            included_event_ids=list(self.included_event_ids),
            first_event_time_ms=self.first_event_time_ms,
            last_event_time_ms=self.last_event_time_ms,
            watermark_at_close_ms=int(watermark_ms) if watermark_ms is not None else None,
            checkpoint_id=checkpoint_id,
            updated_at_checkpoints=list(self.updated_at_checkpoints),
            revision=self.revision,
        )

    def snapshot(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "window_start_ms": self.window_start_ms,
            "window_end_ms": self.window_end_ms,
            "aggregate": self.aggregate,
            "acc": self._acc,
            "included_event_ids": list(self.included_event_ids),
            "first_event_time_ms": self.first_event_time_ms,
            "last_event_time_ms": self.last_event_time_ms,
            "updated_at_checkpoints": list(self.updated_at_checkpoints),
            "revision": self.revision,
            "closed_watermark_ms": self.closed_watermark_ms,
        }

    @classmethod
    def restore(cls, state: dict[str, Any]) -> "WindowAggregationState":
        win = cls(
            key=state["key"],
            window_start_ms=state["window_start_ms"],
            window_end_ms=state["window_end_ms"],
            aggregate=state.get("aggregate", "SUM"),
        )
        win._acc = dict(state["acc"])
        win.included_event_ids = list(state["included_event_ids"])
        win._offsets = set(win.included_event_ids)
        win.first_event_time_ms = state.get("first_event_time_ms")
        win.last_event_time_ms = state.get("last_event_time_ms")
        win.updated_at_checkpoints = list(state.get("updated_at_checkpoints", []))
        win.revision = int(state.get("revision", 0))
        closed_wm = state.get("closed_watermark_ms")
        win.closed_watermark_ms = int(closed_wm) if closed_wm is not None else None
        return win
