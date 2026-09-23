"""本地事件源：模拟真实流的可重放读取，支持故障注入与重复投递。"""

from __future__ import annotations

from collections.abc import Iterator

from .events import Event


class InjectedSourceError(Exception):
    """故障注入：模拟进程崩溃 / 源读取失败。"""


class LocalEventSource:
    """基于内存事件列表的确定性源。

    - 位点 = 下一条待读事件的下标（单调递增）。
    - 支持 seek(offset) 从指定位点重放（用于恢复与重复投递模拟）。
    - 支持注入处理中断（fail_after）模拟进程崩溃。
    """

    def __init__(self, events: list[Event]) -> None:
        self._events = sorted(events, key=lambda e: e.offset)
        self._pos = 0
        self._fail_after: int | None = None

    @property
    def offset(self) -> int:
        return self._pos

    def seek(self, offset: int) -> None:
        if offset < 0 or offset > len(self._events):
            raise ValueError(f"offset {offset} out of range [0, {len(self._events)}]")
        self._pos = offset

    def inject_failure(self, fail_after: int | None) -> None:
        """设置读取 fail_after 条后抛 InjectedSourceError 模拟故障。"""
        self._fail_after = fail_after

    def read(self) -> Event | None:
        """读取下一条事件；到达末尾返回 None。"""
        if self._pos >= len(self._events):
            return None
        if self._fail_after is not None:
            if self._fail_after <= 0:
                self._fail_after = None  # 故障只触发一次，重试可继续
                raise InjectedSourceError(f"injected failure at offset {self._pos}")
            self._fail_after -= 1
        event = self._events[self._pos]
        self._pos += 1
        return event

    def __iter__(self) -> Iterator[Event]:
        return self

    def __next__(self) -> Event:
        event = self.read()
        if event is None:
            raise StopIteration
        return event
