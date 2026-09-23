"""有界输入缓冲：容量受限，满时以明确的过载错误反馈背压。"""

from __future__ import annotations

from collections import deque

from etsr.errors import BufferOverloadError


class BoundedBuffer:
    """固定容量 FIFO 缓冲。

    - put 在容量不足时抛 BufferOverloadError（reason=OVERLOADED_BUFFER_FULL），
      调用方据此退避重试，内存使用恒有上界。
    - 不做静默丢弃：任何拒绝都必须由调用方感知并体现在投递报告中。
    """

    def __init__(self, capacity: int) -> None:
        if capacity <= 0:
            raise ValueError("buffer capacity must be positive")
        self.capacity = int(capacity)
        self._items: deque[object] = deque()
        self.total_put = 0
        self.total_rejected = 0

    def put(self, item: object) -> None:
        if len(self._items) >= self.capacity:
            self.total_rejected += 1
            raise BufferOverloadError(
                f"buffer full: capacity={self.capacity}, in-flight={len(self._items)}"
            )
        self._items.append(item)
        self.total_put += 1

    def try_put_batch(self, items: list[object]) -> tuple[list[object], list[object]]:
        """批量放入；返回 (已接收, 因过载被拒绝)，拒绝从尾部开始。"""

        accepted: list[object] = []
        for item in items:
            if len(self._items) >= self.capacity:
                remaining = items[len(accepted):]
                self.total_rejected += len(remaining)
                return accepted, remaining
            self._items.append(item)
            self.total_put += 1
            accepted.append(item)
        return accepted, []

    def get(self) -> object | None:
        if not self._items:
            return None
        return self._items.popleft()

    def drain(self, max_items: int) -> list[object]:
        n = min(max_items, len(self._items))
        return [self._items.popleft() for _ in range(n)]

    def __len__(self) -> int:
        return len(self._items)

    @property
    def available(self) -> int:
        return self.capacity - len(self._items)
