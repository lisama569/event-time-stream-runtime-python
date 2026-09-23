"""本地事件源：内存事件序列 + 位点，模拟乱序投递与重启重放。"""

from __future__ import annotations

from collections.abc import Iterable

from etsr.errors import OffsetGapError, OffsetRegressionError
from etsr.model import Event


class LocalEventSource:
    """按连续 offset 枚举事件的本地源。

    事件可按事件时间乱序追加，但源位点 offset 必须连续递增；
    若追加出现跳号，说明上游丢数据，立即以 OffsetGapError 拒绝。
    position 为下一条待读取 offset，重启后 seek 到检查点位点即可重放。
    """

    def __init__(self, events: Iterable[Event] | None = None) -> None:
        self._events: list[Event] = []
        self._by_offset: dict[int, int] = {}
        self._next_append_offset = 0
        self._position = 0
        for event in events or []:
            self.append(event)

    def append(self, event: Event) -> None:
        offset = int(event.offset)
        if offset < self._next_append_offset:
            raise OffsetRegressionError(
                f"append offset {offset} already passed {self._next_append_offset - 1}"
            )
        if offset > self._next_append_offset:
            raise OffsetGapError(
                f"append gap: expected {self._next_append_offset}, got {offset}"
            )
        self._by_offset[offset] = len(self._events)
        self._events.append(event)
        self._next_append_offset = offset + 1

    def seek(self, offset: int) -> None:
        """显式设置读取位点。

        位点回退到当前位置之前会被明确拒绝（OffsetRegressionError），
        重启恢复应使用 replay_from 从检查点位点重建源，而不是在活源上倒退。
        """

        if offset < 0 or offset > self._next_append_offset:
            raise OffsetGapError(f"seek {offset} out of range [0,{self._next_append_offset}]")
        if offset < self._position:
            raise OffsetRegressionError(
                f"refuse to rewind source position {self._position} -> {offset}"
            )
        self._position = int(offset)

    @property
    def position(self) -> int:
        return self._position

    @property
    def end_offset(self) -> int:
        return self._next_append_offset

    def has_next(self) -> bool:
        return self._position < len(self._events)

    def next_event(self) -> Event | None:
        if not self.has_next():
            return None
        event = self._events[self._position]
        self._position += 1
        return event

    def replay_from(self, offset: int) -> "LocalEventSource":
        """以相同事件数据构造从指定位点开始的重放视图（用于崩溃恢复）。"""

        if offset < 0 or offset > self._next_append_offset:
            raise OffsetGapError(f"replay offset {offset} out of range")
        view = LocalEventSource(self._events)
        view.seek(offset)
        return view

    def all_events(self) -> list[Event]:
        return list(self._events)
