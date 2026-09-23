"""事件时间滚动窗口分配与聚合状态。"""

from __future__ import annotations

from dataclasses import dataclass

from .events import Event, WindowResult


@dataclass(slots=True)
class WindowAggregate:
    count: int = 0
    total: float = 0.0

    def add(self, value: float) -> None:
        self.count += 1
        self.total += value


def assign_window(event_time: int, window_size_ms: int) -> tuple[int, int]:
    """返回事件所属的 [start, end) 滚动窗口。"""
    start = (event_time // window_size_ms) * window_size_ms
    return start, start + window_size_ms


class WindowStore:
    """按 (window_start, key) 维护活跃窗口聚合；窗口关闭后输出并清理。"""

    def __init__(self, window_size_ms: int) -> None:
        if window_size_ms <= 0:
            raise ValueError("window_size_ms must be > 0")
        self.window_size_ms = window_size_ms
        self._open: dict[tuple[int, str], WindowAggregate] = {}

    def add(self, event: Event) -> None:
        start, _ = assign_window(event.event_time, self.window_size_ms)
        key = (start, event.key)
        agg = self._open.setdefault(key, WindowAggregate())
        agg.add(event.value)

    def close_windows(self, watermark: int) -> list[WindowResult]:
        """关闭所有 window_end <= watermark 的窗口，返回结果并清理状态。"""
        closed: list[WindowResult] = []
        remaining: dict[tuple[int, str], WindowAggregate] = {}
        for (start, key), agg in self._open.items():
            end = start + self.window_size_ms
            if end <= watermark:
                closed.append(
                    WindowResult(
                        window_start=start,
                        window_end=end,
                        key=key,
                        count=agg.count,
                        total=agg.total,
                    )
                )
            else:
                remaining[(start, key)] = agg
        self._open = remaining
        closed.sort(key=lambda r: (r.window_start, r.key))
        return closed

    def snapshot(self) -> dict:
        return {
            "window_size_ms": self.window_size_ms,
            "open": [
                {"start": start, "key": key, "count": agg.count, "total": agg.total}
                for (start, key), agg in sorted(self._open.items())
            ],
        }

    def restore(self, snap: dict) -> None:
        if snap["window_size_ms"] != self.window_size_ms:
            raise ValueError("window_size_ms mismatch with checkpoint")
        self._open = {
            (entry["start"], entry["key"]): WindowAggregate(
                count=entry["count"], total=entry["total"]
            )
            for entry in snap["open"]
        }
