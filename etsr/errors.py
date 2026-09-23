"""可区分的运行时错误类型（失败原因可解释）。"""


class StreamRuntimeError(Exception):
    """所有运行时错误的基类，携带稳定的 reason 码。"""

    reason = "STREAM_RUNTIME_ERROR"


class BufferOverloadError(StreamRuntimeError):
    """有界缓冲已满，调用方必须退避重试（背压反馈）。"""

    reason = "OVERLOADED_BUFFER_FULL"


class OffsetRegressionError(StreamRuntimeError):
    """位点试图回退到已提交位置之前，被明确拒绝。"""

    reason = "OFFSET_REGRESSION"


class OffsetGapError(StreamRuntimeError):
    """位点出现跳跃（中间事件缺失），拒绝在缺数据的情况下推进。"""

    reason = "OFFSET_GAP"


class CheckpointRegressionError(StreamRuntimeError):
    """试图恢复比当前更新检查点更旧的检查点。"""

    reason = "STALE_CHECKPOINT"


class CheckpointCorruptError(StreamRuntimeError):
    """检查点内容损坏或无法解析。"""

    reason = "CHECKPOINT_CORRUPT"
