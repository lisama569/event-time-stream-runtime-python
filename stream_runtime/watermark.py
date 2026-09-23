"""水位线跟踪：基于最大观测事件时间减允许乱序，单调不倒退。"""

from __future__ import annotations


class WatermarkTracker:
    """跟踪事件时间进度，产出单调递增的水位线。

    watermark = max_observed_event_time - allowed_lateness，且永不倒退。
    """

    def __init__(self, allowed_lateness_ms: int) -> None:
        if allowed_lateness_ms < 0:
            raise ValueError("allowed_lateness_ms must be >= 0")
        self.allowed_lateness_ms = allowed_lateness_ms
        self._max_event_time: int | None = None
        self._watermark: int = -(2**63)

    @property
    def watermark(self) -> int:
        return self._watermark

    @property
    def max_event_time(self) -> int | None:
        return self._max_event_time

    def observe(self, event_time: int) -> int:
        """观测一个事件时间，返回当前水位线（不倒退）。"""
        if self._max_event_time is None or event_time > self._max_event_time:
            self._max_event_time = event_time
        candidate = self._max_event_time - self.allowed_lateness_ms
        if candidate > self._watermark:
            self._watermark = candidate
        return self._watermark

    def snapshot(self) -> dict:
        return {
            "allowed_lateness_ms": self.allowed_lateness_ms,
            "max_event_time": self._max_event_time,
            "watermark": self._watermark,
        }

    def restore(self, snap: dict) -> None:
        if snap["allowed_lateness_ms"] != self.allowed_lateness_ms:
            raise ValueError("allowed_lateness_ms mismatch with checkpoint")
        self._max_event_time = snap["max_event_time"]
        self._watermark = snap["watermark"]
