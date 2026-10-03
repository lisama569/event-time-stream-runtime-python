"""本地结果收集器：最终窗口结果、迟到修正、迟到旁路输出与重复事件统计。"""

from __future__ import annotations

from etsr.model import CorrectionRecord, LateRecord, WindowResult


def _result_key(result: WindowResult) -> str:
    return f"{result.key}|{result.window_start_ms}|{result.window_end_ms}"


class ResultCollector:
    """收集运行产物。

    - results：窗口关闭后的聚合结论，按 (key, 窗口) 幂等保存；开启迟到容忍后，
      容忍期内的修正会以更高 revision 覆盖旧结论（以最新 revision 为准）；
    - corrections：每次迟到修正的流水（窗口、版本、修正后结论、参与事件位点）；
    - late：超出容忍范围（或未开启容忍时窗口关闭后）到达的迟到事件旁路；
    - duplicates：重复投递/重放命中的源位点（绝不重复计入）。
    """

    def __init__(self) -> None:
        self._results: dict[str, WindowResult] = {}
        self._corrections: list[CorrectionRecord] = []
        self._late: list[LateRecord] = []
        self._duplicates: list[int] = []
        self.total_finalized = 0
        self.total_corrected = 0
        self.total_late = 0
        self.total_duplicates = 0

    def collect(self, result: WindowResult) -> None:
        key = _result_key(result)
        if key in self._results:
            # 同一窗口重复输出属于状态损坏信号，正常恢复路径不会触发。
            raise ValueError(f"window already finalized: {key}")
        self._results[key] = result
        self.total_finalized += 1

    def apply_correction(self, result: WindowResult, record: CorrectionRecord) -> None:
        """用修正后的结论覆盖窗口旧结论（revision 必须严格递增）。"""

        key = _result_key(result)
        previous = self._results.get(key)
        if previous is not None and result.revision <= previous.revision:
            raise ValueError(
                f"correction revision not increasing for {key}: "
                f"{result.revision} <= {previous.revision}"
            )
        self._results[key] = result
        self._corrections.append(record)
        self.total_corrected += 1

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
        """从检查点回填已输出结果（幂等；保留更高 revision 的结论）。"""

        key = _result_key(result)
        existing = self._results.get(key)
        if existing is None or result.revision > existing.revision:
            self._results[key] = result

    def restore_corrections(self, records: list[CorrectionRecord]) -> None:
        """从检查点回填修正流水（幂等，按 (窗口, revision) 去重）。"""

        seen = {
            (r.key, r.window_start_ms, r.window_end_ms, r.revision)
            for r in self._corrections
        }
        for record in records:
            token = (record.key, record.window_start_ms, record.window_end_ms, record.revision)
            if token not in seen:
                seen.add(token)
                self._corrections.append(record)
        self.total_corrected = len(self._corrections)

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

    def corrections(self) -> list[CorrectionRecord]:
        return list(self._corrections)

    def late_events(self) -> list[LateRecord]:
        return list(self._late)

    def duplicate_offsets(self) -> list[int]:
        return list(self._duplicates)
