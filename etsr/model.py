"""核心数据模型：事件、窗口聚合结果、迟到记录、投递报告。"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

SUPPORTED_AGGREGATES = ("SUM", "COUNT", "MIN", "MAX", "AVG")


@dataclass(frozen=True, slots=True)
class Event:
    """一条输入事件。

    offset: 单调递增的源位点（检查点与去重依据）。
    event_time_ms: 事件时间（毫秒），窗口与水位线的唯一时间语义来源。
    key: 聚合键。
    value: 参与聚合的数值。
    event_id: 业务幂等键（可选，重复投递时用于解释）。
    """

    offset: int
    event_time_ms: int
    key: str
    value: float
    event_id: str | None = None


@dataclass(frozen=True, slots=True)
class WindowResult:
    """一个 (key, 窗口) 的最终聚合结论（窗口关闭后不可再变）。"""

    key: str
    window_start_ms: int
    window_end_ms: int
    aggregate: str
    value: float
    count: int
    included_event_ids: list[int] = field(default_factory=list)
    first_event_time_ms: int | None = None
    last_event_time_ms: int | None = None
    watermark_at_close_ms: int | None = None
    checkpoint_id: int | None = None
    updated_at_checkpoints: list[int] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "window_start_ms": self.window_start_ms,
            "window_end_ms": self.window_end_ms,
            "aggregate": self.aggregate,
            "value": self.value,
            "count": self.count,
            "included_event_ids": list(self.included_event_ids),
            "first_event_time_ms": self.first_event_time_ms,
            "last_event_time_ms": self.last_event_time_ms,
            "watermark_at_close_ms": self.watermark_at_close_ms,
            "checkpoint_id": self.checkpoint_id,
            "updated_at_checkpoints": list(self.updated_at_checkpoints),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "WindowResult":
        return cls(
            key=data["key"],
            window_start_ms=int(data["window_start_ms"]),
            window_end_ms=int(data["window_end_ms"]),
            aggregate=data.get("aggregate", "SUM"),
            value=float(data["value"]),
            count=int(data["count"]),
            included_event_ids=[int(x) for x in data.get("included_event_ids", [])],
            first_event_time_ms=data.get("first_event_time_ms"),
            last_event_time_ms=data.get("last_event_time_ms"),
            watermark_at_close_ms=data.get("watermark_at_close_ms"),
            checkpoint_id=data.get("checkpoint_id"),
            updated_at_checkpoints=[int(x) for x in data.get("updated_at_checkpoints", [])],
        )


@dataclass(frozen=True, slots=True)
class LateRecord:
    """窗口关闭后才到达的迟到事件（不静默丢弃，进入旁路输出）。"""

    event_offset: int
    event_id: str | None
    key: str
    event_time_ms: int
    window_start_ms: int
    window_end_ms: int
    watermark_ms: int
    late_by_ms: int
    reason: str = "LATE_AFTER_WINDOW_CLOSE"


@dataclass(frozen=True, slots=True)
class RejectedItem:
    """因过载被拒绝的条目（调用方可凭 offset/事件重试）。"""

    kind: str
    offset: int | None
    detail: str


@dataclass(frozen=True, slots=True)
class IngestionReport:
    """一次投递的可解释结果。"""

    accepted: int = 0
    duplicates: int = 0
    late: int = 0
    rejected: int = 0
    watermark_ms: int = 0
    windows_finalized: int = 0
    rejected_items: list[RejectedItem] = field(default_factory=list)


def new_accumulator() -> dict[str, Any]:
    """所有聚合共用一个累加器，切换聚合类型不丢信息。"""

    return {"sum": 0.0, "count": 0, "min": None, "max": None}


def accumulate(acc: dict[str, Any], value: float) -> dict[str, Any]:
    acc["sum"] += value
    acc["count"] += 1
    acc["min"] = value if acc["min"] is None else min(acc["min"], value)
    acc["max"] = value if acc["max"] is None else max(acc["max"], value)
    return acc


def aggregate_value(aggregate: str, acc: dict[str, Any]) -> float:
    """从累加器读取指定聚合的最终值。"""

    if aggregate not in SUPPORTED_AGGREGATES:
        raise ValueError(f"unsupported aggregate: {aggregate}")
    count = acc["count"]
    if aggregate == "SUM":
        return float(acc["sum"])
    if aggregate == "COUNT":
        return float(count)
    if aggregate == "MIN":
        return float(acc["min"]) if count else 0.0
    if aggregate == "MAX":
        return float(acc["max"]) if count else 0.0
    return acc["sum"] / count if count else 0.0
