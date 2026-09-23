"""运行时错误类型：所有失败原因必须可区分。"""

from __future__ import annotations


class StreamRuntimeError(Exception):
    """所有流运行时错误的基类。"""


class OffsetRegressionError(StreamRuntimeError):
    """位点回退：试图从比检查点位点更早的位点继续消费。"""

    def __init__(self, requested: int, checkpointed: int) -> None:
        self.requested = requested
        self.checkpointed = checkpointed
        super().__init__(
            f"offset regression rejected: requested={requested} < checkpointed={checkpointed}"
        )


class OverloadError(StreamRuntimeError):
    """背压：有界缓冲已满，输入被拒绝（可区分于处理错误）。"""

    def __init__(self, capacity: int) -> None:
        self.capacity = capacity
        super().__init__(f"input buffer full: capacity={capacity}")


class CheckpointCorruptionError(StreamRuntimeError):
    """检查点数据损坏或不完整。"""
