"""本地结果收集器：最终窗口结果、迟到修正、迟到旁路输出与重复事件统计。"""

from __future__ import annotations

from etsr.history import HistoryArchive
from etsr.model import CorrectionRecord, LateRecord, WindowResult


def _result_key(result: WindowResult) -> str:
    return f"{result.key}|{result.window_start_ms}|{result.window_end_ms}"


def _window_token(key: str, window_start_ms: int, window_end_ms: int) -> str:
    return f"{key}|{int(window_start_ms)}|{int(window_end_ms)}"


class ResultCollector:
    """收集运行产物。

    - results：窗口关闭后的聚合结论，按 (key, 窗口) 幂等保存；开启迟到容忍后，
      容忍期内的修正会以更高 revision 覆盖旧结论（以最新 revision 为准）；
    - corrections：每次迟到修正的流水（窗口、版本、修正后结论、参与事件位点）；
    - late：超出容忍范围（或未开启容忍时窗口关闭后）到达的迟到事件旁路；
    - duplicates：重复投递/重放命中的源位点（绝不重复计入）。

    活动状态只保留"仍处在容忍期内、还可能被再修正"的窗口；窗口彻底过了容忍期
    即被封存（seal）进追加式历史归档，活动内存与检查点体积均有上界。封存后的
    结论与修正流水仍可随时查询（results_sorted/corrections 会合并归档视图），
    历史信息不丢。
    """

    def __init__(self, archive: HistoryArchive | None = None) -> None:
        self._results: dict[str, WindowResult] = {}
        self._corrections: list[CorrectionRecord] = []
        self._late: list[LateRecord] = []
        self._duplicates: list[int] = []
        self._archive = archive
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
        """窗口是否已输出结论（只查活动状态即可）。

        已过容忍期被封存的窗口必然满足 watermark >= window_end，迟到判定会走
        水位线分支得到同样的分类，无需为此扫描历史归档。
        """

        start = int(window_start_ms)
        return any(
            r.key == key and r.window_start_ms == start
            for r in self._results.values()
        )

    # ------------------------------------------------------------------ 封存
    def seal_window(self, key: str, window_start_ms: int, window_end_ms: int) -> bool:
        """把彻底过了容忍期的窗口数据从活动状态封存进历史归档。

        先追加落盘（fsync）再移除活动状态：崩溃时要么归档已有、要么活动状态
        仍在（随检查点恢复后重新封存），两边都不会丢；重复封存在读取侧去重。
        """

        if self._archive is None:
            return False
        token = _window_token(key, window_start_ms, window_end_ms)
        result = self._results.get(token)
        if result is None:
            return False  # 已封存或从未输出：幂等，不重复追加
        corrections = [
            r
            for r in self._corrections
            if _window_token(r.key, r.window_start_ms, r.window_end_ms) == token
        ]
        self._archive.append_result(result)
        for record in corrections:
            self._archive.append_correction(record)
        del self._results[token]
        self._corrections = [
            r
            for r in self._corrections
            if _window_token(r.key, r.window_start_ms, r.window_end_ms) != token
        ]
        return True

    # ------------------------------------------------------------------ 恢复
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

    # ------------------------------------------------------------------ 视图
    def live_results(self) -> list[WindowResult]:
        """仍处在容忍期内的活动结论（检查点只持久化这部分，体积有上界）。"""

        return list(self._results.values())

    def live_corrections(self) -> list[CorrectionRecord]:
        """仍处在容忍期内窗口的修正流水（检查点只持久化这部分）。"""

        return list(self._corrections)

    def results_sorted(self) -> list[WindowResult]:
        """全部窗口结论：活动状态 + 已封存历史，按 (窗口起点, key) 排序。"""

        merged: dict[str, WindowResult] = {}
        if self._archive is not None:
            for result in self._archive.results():
                merged[_result_key(result)] = result
        for key, result in self._results.items():
            existing = merged.get(key)
            # 崩溃重放可能让同一窗口既在归档又在活动状态：取更高 revision。
            if existing is None or result.revision >= existing.revision:
                merged[key] = result
        return sorted(merged.values(), key=lambda r: (r.window_start_ms, r.key))

    def corrections(self) -> list[CorrectionRecord]:
        """全部修正流水：已封存历史（按追加序）+ 活动状态，按 (窗口, revision) 去重。"""

        merged: list[CorrectionRecord] = []
        seen: set[tuple[str, int, int, int]] = set()
        sources: list[list[CorrectionRecord]] = []
        if self._archive is not None:
            sources.append(self._archive.corrections())
        sources.append(self._corrections)
        for source in sources:
            for record in source:
                token = (
                    record.key,
                    record.window_start_ms,
                    record.window_end_ms,
                    record.revision,
                )
                if token in seen:
                    continue
                seen.add(token)
                merged.append(record)
        return merged

    def late_events(self) -> list[LateRecord]:
        return list(self._late)

    def duplicate_offsets(self) -> list[int]:
        return list(self._duplicates)
