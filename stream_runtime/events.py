"""事件与结果类型定义。"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class Event:
    """一条输入事件。

    offset: 在源中的位点（单调递增，用于检查点与重放）。
    event_time: 事件时间（毫秒）。
    key: 聚合键。
    value: 聚合值。
    """

    offset: int
    event_time: int
    key: str
    value: float


@dataclass(frozen=True, slots=True)
class WindowResult:
    """一个窗口的聚合输出。"""

    window_start: int
    window_end: int
    key: str
    count: int
    total: float
    # late=True 表示该结果包含窗口关闭后到达的迟到事件（单独标注，不静默合并）。
    late: bool = False


@dataclass(slots=True)
class LateEvent:
    """被路由到迟到侧输出的记录（不静默丢弃，也不重复计入已关闭窗口）。"""

    event: Event
    watermark: int
    reason: str = "event_time <= watermark (window closed)"
