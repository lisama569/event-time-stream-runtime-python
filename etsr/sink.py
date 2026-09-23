"""本地结果收集器：最终窗口结果、迟到旁路输出与重复事件统计。"""

from __future__ import annotations

from etsr.model import LateRecord, WindowResult


def _result_key(result: WindowResult) -> str:
    return f"{result.key}|{result.window_start_ms}|{result.window_end_ms}"


class ResultCollector:
    """收集运行产物。

    - results：窗口关闭后的不可变聚合，按 (key, 窗口) 幂等保存；
    - late：窗口关闭后到达的迟到事件旁路（绝不静默丢弃）；
    - duplicates：重复投递/重放命中的源位点（绝不重复计入）。
    """

    def __init__(self) -> None:
        self._results: dict[str, WindowResult] = {}
        self._late: list[LateRecord] = []
        self._duplicates: list[int] = []
        self.total_finalized = 0
        self.total_late = 0
        self.total_duplicates = 0

    def collect(self, result: WindowResult) -> None:
        key = _result_key(result)
        if key in self._results:
            # 同一窗口重复输出属于状态损坏信号，正常恢复路径不会触发。
            raise ValueError(f"window already finalized: {key}")
        self._results[key] = result
        self.total_finalized += 1

    def side_output(self, record: LateRecord) -> None:
        self._late.append(record)
        self.total_late += 1

    def record_duplicate(self, offset: int) -> None:
        self._duplicates.append(int(offset))
        self.total_duplicates += 1

    def contains_window(self, key: str, window_start_ms: int) -> bool:
        start = int(window_start_ms)
        return any(
            r.key == key and r.window_start_ms == start
            for r in self._results.values()
        )

    def restore_result(self, result: WindowResult) -> None:
        """从检查点回填已输出结果（幂等）。"""

        self._results.setdefault(_result_key(result), result)

    def restore_duplicates(self, offsets: list[int]) -> None:
        for offset in offsets:
            if offset not in self._duplicates:
                self._duplicates.append(int(offset))
        self.total_duplicates = len(self._duplicates)

    def results_sorted(self) -> list[WindowResult]:
        return sorted(
            self._results.values(),
            key=lambda r: (r.window_start_ms, r.key),
        )

    def late_events(self) -> list[LateRecord]:
        return list(self._late)

    def duplicate_offsets(self) -> list[int]:
        return list(self._duplicates)
