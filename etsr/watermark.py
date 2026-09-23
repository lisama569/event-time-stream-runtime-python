"""水位线生成器：基于已观察事件时间减去乱序容忍，单调不减、绝不倒退。"""

from __future__ import annotations


class WatermarkGenerator:
    """周期性标记时间水位线（BoundedOutOfOrderness 语义）。

    wm(t) = max(已观察事件时间) - max_out_of_orderness_ms
    另支持心跳标记显式推进。任何更新只能令水位线增大。
    尚未观察到任何事件时 current_ms 为 None（等价于 -∞）。
    """

    def __init__(self, max_out_of_orderness_ms: int = 0) -> None:
        self._allowed = int(max_out_of_orderness_ms)
        self._max_event_time_ms: int | None = None
        self._watermark_ms: int | None = None

    @property
    def current_ms(self) -> int | None:
        return self._watermark_ms

    @property
    def max_event_time_ms(self) -> int | None:
        return self._max_event_time_ms

    def observe(self, event_time_ms: int) -> int | None:
        """观察一条事件的事件时间，返回更新后的水位线（单调不减）。"""

        if self._max_event_time_ms is None or event_time_ms > self._max_event_time_ms:
            self._max_event_time_ms = event_time_ms
        candidate = self._max_event_time_ms - self._allowed
        return self._advance_to(candidate)

    def advance(self, timestamp_ms: int) -> int | None:
        """由心跳/标记显式推进水位线；传入更小的值不会使其倒退。"""

        return self._advance_to(timestamp_ms)

    def _advance_to(self, candidate: int) -> int | None:
        # 水位线绝不倒退：仅当候选值更大时更新。
        if self._watermark_ms is None or candidate > self._watermark_ms:
            self._watermark_ms = candidate
        return self._watermark_ms

    def snapshot(self) -> int | None:
        return self._watermark_ms

    @classmethod
    def restore(
        cls, watermark_ms: int | None, max_out_of_orderness_ms: int = 0
    ) -> "WatermarkGenerator":
        gen = cls(max_out_of_orderness_ms)
        if watermark_ms is not None:
            gen._watermark_ms = int(watermark_ms)
            # 反推最大事件时间下界，保证恢复后水位线不会因重放而倒退。
            gen._max_event_time_ms = int(watermark_ms) + gen._allowed
        return gen
