"""本地结果收集器：最终窗口结果、迟到修正、迟到旁路输出与重复事件统计。"""

from __future__ import annotations

from etsr.model import CorrectionRecord, LateRecord, WindowResult


def _result_key(result: WindowResult) -> str:
    return f"{result.key}|{result.window_start_ms}|{result.window_end_ms}"


def _merge_offsets(left: list[int], right: list[int]) -> list[int]:
    """保序合并两个位点列表（去重），用于修正摘要的参与事件并集。"""

    merged = list(left)
    seen = set(left)
    for offset in right:
        if offset not in seen:
            seen.add(offset)
            merged.append(offset)
    return merged


class ResultCollector:
    """收集运行产物。

    - results：窗口关闭后的聚合结论，按 (key, 窗口) 幂等保存；开启迟到容忍后，
      容忍期内的修正会以更高 revision 覆盖旧结论（以最新 revision 为准）；
    - corrections：迟到修正的**按窗口汇总**（每个被修正窗口一条：当前版本、
      修正后结论、参与历次修正的事件位点、最近一次修正时的水位线）。
      体积与"被修正的窗口数"成正比，而不是与"修正次数"成正比——
      长跑下来修正审计信息不丢，但内存与检查点不随迟到数据量线性膨胀；
    - late：超出容忍范围（或未开启容忍时窗口关闭后）到达的迟到事件旁路；
    - duplicates：重复投递/重放命中的源位点（绝不重复计入）。
    """

    def __init__(self) -> None:
        self._results: dict[str, WindowResult] = {}
        self._corrections: dict[tuple[str, int, int], CorrectionRecord] = {}
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
        """用修正后的结论覆盖窗口旧结论（revision 必须严格递增）。

        修正审计按窗口汇总：同一窗口的再次修正合并进该窗口的摘要记录
        （版本/结论取最新，参与事件位点按到达顺序累积），不逐次堆记录。
        """

        key = _result_key(result)
        previous = self._results.get(key)
        if previous is not None and result.revision <= previous.revision:
            raise ValueError(
                f"correction revision not increasing for {key}: "
                f"{result.revision} <= {previous.revision}"
            )
        self._results[key] = result
        token = (record.key, record.window_start_ms, record.window_end_ms)
        existing = self._corrections.get(token)
        if existing is not None:
            record = CorrectionRecord(
                key=record.key,
                window_start_ms=record.window_start_ms,
                window_end_ms=record.window_end_ms,
                revision=record.revision,
                value=record.value,
                count=record.count,
                triggering_offsets=[
                    *existing.triggering_offsets, *record.triggering_offsets
                ],
                watermark_ms=record.watermark_ms,
            )
        self._corrections[token] = record
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
        """从检查点回填修正摘要（幂等，按窗口合并：保留最高版本，位点取并集）。

        兼容旧格式检查点（每次修正一条记录）：合并后自然收敛为按窗口摘要。
        """

        for record in records:
            token = (record.key, record.window_start_ms, record.window_end_ms)
            existing = self._corrections.get(token)
            if existing is None or record.revision > existing.revision:
                merged = record
                if existing is not None:
                    merged = CorrectionRecord(
                        key=record.key,
                        window_start_ms=record.window_start_ms,
                        window_end_ms=record.window_end_ms,
                        revision=record.revision,
                        value=record.value,
                        count=record.count,
                        triggering_offsets=_merge_offsets(
                            existing.triggering_offsets, record.triggering_offsets
                        ),
                        watermark_ms=record.watermark_ms,
                    )
                self._corrections[token] = merged
            elif record.revision == existing.revision:
                self._corrections[token] = CorrectionRecord(
                    key=existing.key,
                    window_start_ms=existing.window_start_ms,
                    window_end_ms=existing.window_end_ms,
                    revision=existing.revision,
                    value=existing.value,
                    count=existing.count,
                    triggering_offsets=_merge_offsets(
                        existing.triggering_offsets, record.triggering_offsets
                    ),
                    watermark_ms=existing.watermark_ms,
                )
        # 修正事件总次数 = 各窗口版本号之和（每窗口 revision 从 1 起每次修正 +1）。
        self.total_corrected = sum(r.revision for r in self._corrections.values())

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
        """按窗口汇总的修正审计（按窗口起始时间、键排序，每条一个被修正窗口）。"""

        return [
            self._corrections[token]
            for token in sorted(self._corrections, key=lambda t: (t[1], t[0]))
        ]

    def late_events(self) -> list[LateRecord]:
        return list(self._late)

    def duplicate_offsets(self) -> list[int]:
        return list(self._duplicates)
