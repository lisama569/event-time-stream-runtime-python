"""流处理运行时核心：事件时间窗口聚合 + 水位线 + 检查点 + 背压。"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from etsr.buffer import BoundedBuffer
from etsr.checkpoint import Checkpoint, LocalCheckpointStore
from etsr.errors import CheckpointRegressionError, OffsetGapError
from etsr.model import (
    Event,
    IngestionReport,
    LateRecord,
    RejectedItem,
    WindowResult,
)
from etsr.sink import ResultCollector
from etsr.source import LocalEventSource
from etsr.watermark import WatermarkGenerator
from etsr.windows import TumblingWindowAssigner, WindowAggregationState

logger = logging.getLogger("etsr")


def _window_key(key: str, window_start_ms: int) -> str:
    return f"{key}|{int(window_start_ms)}"


@dataclass(frozen=True, slots=True)
class EngineConfig:
    window_size_ms: int = 1000
    max_out_of_orderness_ms: int = 0
    aggregate: str = "SUM"
    buffer_capacity: int = 1024
    state_dir: str = ".runtime_state"
    auto_checkpoint_events: int = 0  # 0 表示仅在显式调用时做检查点
    process_budget_per_cycle: int = 0  # 每个处理周期最多处理事件数，0 表示不限速（全部排空）
    source_pull_per_cycle: int = 0  # 每周期从源拉取的突发大小，0 表示等于处理预算
    # 迟到容忍时长（事件时间域，从窗口结束时间起算）。0 表示不开启（默认，
    # 行为与此前完全一致）；>0 时窗口关闭后仍保留可修正状态直到
    # watermark >= window_end + allowed_lateness_ms，期间迟到的同窗口事件
    # 会修正已给出的结论，超出容忍范围的迟到仍走旁路输出。
    allowed_lateness_ms: int = 0


class StreamRuntime:
    """单进程本地流处理运行时。

    处理顺序（每条事件）：
    1. 位点校验：跳号拒绝，重复位点幂等去重；
    2. 迟到判定：event_time < 当前水位线（或窗口已关闭）->
       容忍范围内修正已给出的结论，否则迟到旁路；
    3. 观察事件时间推进水位线（单调不减）；
    4. 并入窗口（窗口关闭前可更新），关闭所有到期窗口并输出最终结果；
       开启迟到容忍时关闭后的窗口状态保留至容忍期结束再释放；
    5. 计数达到阈值时自动一致性检查点。
    """

    def __init__(self, config: EngineConfig | None = None) -> None:
        self.config = config or EngineConfig()
        self._assigner = TumblingWindowAssigner(self.config.window_size_ms)
        self._watermark = WatermarkGenerator(self.config.max_out_of_orderness_ms)
        self._buffer = BoundedBuffer(self.config.buffer_capacity)
        self._store = LocalCheckpointStore(self.config.state_dir)
        self._sink = ResultCollector()
        self._pending: dict[str, WindowAggregationState] = {}
        # 已关闭但仍在迟到容忍范围内的窗口状态；仅容忍开启时非空，随水位线推进释放。
        self._correctable: dict[str, WindowAggregationState] = {}
        self._next_offset = 0
        self._checkpoint_id = 0
        self._events_since_checkpoint = 0
        self._started = True  # 无检查点时即可用；start() 会尝试恢复

    # ------------------------------------------------------------------ 生命周期
    def start(self) -> Checkpoint | None:
        """从最新检查点恢复；无检查点时从位点 0 全新开始。"""

        latest = self._store.load_latest()
        if latest is not None:
            self._restore(latest)
        logger.info(
            "runtime started | watermark_ms=%s next_offset=%d checkpoint_id=%d",
            self.watermark_ms, self._next_offset, self._checkpoint_id,
        )
        return latest

    def restore_from(self, checkpoint_id: int) -> Checkpoint:
        """从指定检查点恢复；比当前更旧的检查点被明确拒绝。"""

        target = self._store.load(checkpoint_id)
        if target is None:
            raise CheckpointRegressionError(f"checkpoint id={checkpoint_id} not found")
        self._store.reject_stale_restore(checkpoint_id, self._checkpoint_id)
        self._restore(target)
        return target

    def _restore(self, cp: Checkpoint) -> None:
        self._pending = {
            key: WindowAggregationState.restore(state)
            for key, state in cp.windows.items()
        }
        self._correctable = {
            key: WindowAggregationState.restore(state)
            for key, state in cp.correctable_windows.items()
        }
        self._watermark = WatermarkGenerator.restore(
            cp.watermark_ms, self.config.max_out_of_orderness_ms
        )
        self._next_offset = int(cp.next_offset)
        self._checkpoint_id = int(cp.checkpoint_id)
        self._events_since_checkpoint = 0
        self._sink = ResultCollector()
        # 回填崩溃前已输出窗口，保证恢复后最终结论完整且重放不重复计入。
        for result_data in cp.finalized_results:
            self._sink.restore_result(WindowResult.from_dict(result_data))
        self._sink.restore_duplicates(cp.duplicate_offsets)
        logger.info(
            "checkpoint restored | id=%d next_offset=%d watermark_ms=%s "
            "pending_windows=%d finalized_windows=%d correctable_windows=%d",
            cp.checkpoint_id, cp.next_offset, cp.watermark_ms,
            len(cp.windows), len(cp.finalized_results), len(cp.correctable_windows),
        )

    def checkpoint(self) -> Checkpoint:
        """对 (位点, 水位线, 全部窗口状态) 做一致性快照并原子落盘。"""

        self._checkpoint_id += 1
        cp = Checkpoint(
            checkpoint_id=self._checkpoint_id,
            next_offset=self._next_offset,
            watermark_ms=self._watermark.current_ms,
            windows={key: win.snapshot() for key, win in self._pending.items()},
            finalized_results=[r.to_dict() for r in self._sink.results_sorted()],
            duplicate_offsets=self._sink.duplicate_offsets(),
            correctable_windows={
                key: win.snapshot() for key, win in self._correctable.items()
            },
        )
        self._store.save(cp)
        self._events_since_checkpoint = 0
        logger.info(
            "checkpoint saved | id=%d next_offset=%d watermark_ms=%s pending=%d "
            "finalized=%d correctable=%d",
            cp.checkpoint_id, cp.next_offset, cp.watermark_ms,
            len(cp.windows), len(cp.finalized_results), len(cp.correctable_windows),
        )
        return cp

    def stop(self) -> Checkpoint:
        """停机前强制一次检查点，保证可断点续算。"""

        return self.checkpoint()

    # ------------------------------------------------------------------ 数据面
    def enqueue(self, event: Event) -> None:
        """仅入队一条事件；缓冲满立即抛 BufferOverloadError（背压反馈）。"""

        self._buffer.put(event)

    def enqueue_batch(self, events: list[Event]) -> list[Event]:
        """批量入队；返回因过载被拒绝的尾部（不静默丢弃）。"""

        _, rejected = self._buffer.try_put_batch(list(events))
        return rejected  # type: ignore[return-value]

    def process_cycle(self, max_events: int | None = None) -> IngestionReport:
        """运行一个处理周期：从有界缓冲取出至多 max_events 条并处理。

        max_events 缺省取 config.process_budget_per_cycle；
        处理速率低于输入速率时，缓冲积压直至容量上限，随后入端得到过载反馈，
        内存占用恒有上界。
        """

        if max_events is None:
            budget = self.config.process_budget_per_cycle
            limit = budget if budget and budget > 0 else max(1, len(self._buffer))
        else:
            limit = max_events
        items = self._buffer.drain(limit)
        accepted = duplicates = late = corrected = 0
        for raw in items:
            outcome = self._process(raw)
            accepted += outcome["accepted"]
            duplicates += outcome["duplicates"]
            late += outcome["late"]
            corrected += outcome["corrected"]
        finalized_now = self._close_due_windows()
        if self._auto_due():
            self.checkpoint()
        return IngestionReport(
            accepted=accepted,
            duplicates=duplicates,
            late=late,
            corrected=corrected,
            watermark_ms=self._watermark.current_ms or 0,
            windows_finalized=finalized_now,
        )

    def ingest_event(self, event: Event) -> IngestionReport:
        """投递单条事件并立即处理（便利接口；限速场景请用 enqueue + process_cycle）。"""

        self.enqueue(event)
        return self.process_cycle()

    def ingest_batch(self, events: list[Event]) -> IngestionReport:
        """批量投递：先全部尝试入队，再按处理预算处理。

        部分过载时尾部事件被拒绝并在报告中逐条交代（reason 可区分），
        已入队事件按正常语义处理，绝不错误推进。
        """

        rejected = self.enqueue_batch(events)
        report = self.process_cycle()
        if rejected:
            object.__setattr__(
                report,
                "rejected_items",
                [
                    RejectedItem(
                        kind="event",
                        offset=getattr(item, "offset", None),
                        detail="OVERLOADED_BUFFER_FULL: retry after backoff",
                    )
                    for item in rejected
                ],
            )
            object.__setattr__(report, "rejected", report.rejected + len(rejected))
        return report

    def run_source(self, source: LocalEventSource, max_events: int | None = None) -> IngestionReport:
        """从本地源拉取（含重启重放视图）直到结束或达上限。

        每个周期先处理一个预算批次，再拉取等量新事件入队；当处理跟不上
        输入时由有界缓冲触发 BufferOverloadError，而不是内存无界增长。
        """

        budget = self.config.process_budget_per_cycle
        per_cycle = self.config.source_pull_per_cycle or budget or self.config.buffer_capacity
        drain_budget = budget if budget and budget > 0 else None
        total = IngestionReport(watermark_ms=self.watermark_ms or 0)
        pulled = 0
        withheld_total = 0
        while source.has_next() and (max_events is None or pulled < max_events):
            incoming: list[Event] = []
            while len(incoming) < per_cycle and source.has_next() and (
                max_events is None or pulled < max_events
            ):
                event = source.next_event()
                if event is None:
                    break
                incoming.append(event)
                pulled += 1
            rejected_events: list[Event] = incoming
            # 过载退避：处理腾出空间后，按原顺序重试被拒后缀，
            # 既不丢数据也不形成位点缺口；缓冲占用恒不超过容量。
            while rejected_events:
                rejected_events = self.enqueue_batch(rejected_events)
                report = self.process_cycle()
                if rejected_events:
                    withheld_total += len(rejected_events)
                    logger.warning(
                        "backpressure | %d events withheld, draining before retry",
                        len(rejected_events),
                    )
                total = self._merge_reports(total, report)
        # 源已耗尽但缓冲可能仍有残留（限速场景），排空它们。
        while len(self._buffer) > 0 and max_events is None:
            total = self._merge_reports(total, self.process_cycle())
        # rejected 在此表示曾因过载被暂缓、随后退避重试成功的事件次数。
        object.__setattr__(total, "rejected", withheld_total)
        return total

    def advance_watermark(self, timestamp_ms: int) -> IngestionReport:
        """心跳标记显式推进水位线，并关闭输出到期窗口。"""

        before = self._watermark.current_ms
        new_wm = self._watermark.advance(int(timestamp_ms))
        finalized = self._close_due_windows()
        logger.info(
            "watermark marker | %s -> %s | windows_finalized=%d",
            before, new_wm, finalized,
        )
        report = IngestionReport(
            watermark_ms=new_wm if new_wm is not None else 0,
            windows_finalized=finalized,
        )
        if self._auto_due():
            self.checkpoint()
        return report

    # ------------------------------------------------------------------ 内部处理
    def _process(self, event: Event) -> dict[str, int]:
        offset = int(event.offset)

        # 1) 位点校验：跳号明确拒绝，绝不带着缺口继续。
        if offset > self._next_offset:
            raise OffsetGapError(
                f"offset gap: expected {self._next_offset}, got {offset}"
            )

        # 2) 重复投递 / 重放：小于下一位点的事件幂等跳过，绝不重复计入。
        if offset < self._next_offset:
            self._sink.record_duplicate(offset)
            logger.info("duplicate event | offset=%d ignored (idempotent)", offset)
            return {"accepted": 0, "duplicates": 1, "late": 0, "corrected": 0}

        start, end = self._assigner.assign(event.event_time_ms)
        wkey = _window_key(event.key, start)
        wm_before = self._watermark.current_ms

        # 3) 迟到判定：水位线已越过窗口结束（或窗口早已输出）。
        already_closed = self._sink.contains_window(event.key, start)
        is_late_by_wm = wm_before is not None and int(event.event_time_ms) < wm_before
        if already_closed or (is_late_by_wm and wm_before is not None and wm_before >= end):
            # 3a) 开启迟到容忍且窗口的可修正状态仍在容忍范围内 -> 修正已给出的结论。
            if self.config.allowed_lateness_ms > 0 and wkey in self._correctable:
                return self._apply_correction(event, wkey, wm_before)
            # 3b) 其余迟到走旁路，不静默丢弃；reason 区分“未开容忍”与“超出容忍”。
            reason = (
                "LATE_BEYOND_ALLOWED_LATENESS"
                if self.config.allowed_lateness_ms > 0
                else "LATE_AFTER_WINDOW_CLOSE"
            )
            late_by = (wm_before - int(event.event_time_ms)) if wm_before is not None else 0
            record = LateRecord(
                event_offset=offset,
                event_id=event.event_id,
                key=event.key,
                event_time_ms=int(event.event_time_ms),
                window_start_ms=start,
                window_end_ms=end,
                watermark_ms=wm_before if wm_before is not None else -1,
                late_by_ms=max(0, late_by),
                reason=reason,
            )
            self._sink.side_output(record)
            self._next_offset = offset + 1
            logger.info(
                "late event | offset=%d event_time_ms=%d window=[%d,%d) watermark_ms=%s "
                "reason=%s -> side-output",
                offset, event.event_time_ms, start, end, wm_before, reason,
            )
            return {"accepted": 0, "duplicates": 0, "late": 1, "corrected": 0}

        # 4) 观察事件时间（单调推进），并入窗口（关闭前可更新）。
        new_wm = self._watermark.observe(int(event.event_time_ms))
        window = self._pending.get(wkey)
        if window is None:
            window = WindowAggregationState(
                event.key, start, end, self.config.aggregate
            )
            self._pending[wkey] = window
        window.add(event, self._checkpoint_id)
        self._next_offset = offset + 1
        self._events_since_checkpoint += 1
        logger.info(
            "event | offset=%d event_time_ms=%d key=%s value=%s window=[%d,%d) watermark_ms=%s",
            offset, event.event_time_ms, event.key, event.value, start, end, new_wm,
        )
        return {"accepted": 1, "duplicates": 0, "late": 0, "corrected": 0}

    def _apply_correction(
        self, event: Event, wkey: str, wm_before: int | None
    ) -> dict[str, int]:
        """容忍范围内的迟到事件：并入保留的窗口状态并产出修正后的结论。

        位点去重在进入本路径前已完成（offset == next_offset），窗口状态内部
        再按位点幂等一次，因此同一条事件重复投递不会把同一次修正算两遍。
        """

        offset = int(event.offset)
        state = self._correctable[wkey]
        previous = self._sink.get_result(state.key, state.window_start_ms, state.window_end_ms)
        if previous is None:  # 状态与结论必须成对存在，否则属于状态损坏。
            raise ValueError(f"correctable window missing finalized result: {wkey}")
        added = state.add(event, self._checkpoint_id)
        self._next_offset = offset + 1
        if not added:
            logger.info("correction skipped | offset=%d already included (idempotent)", offset)
            return {"accepted": 0, "duplicates": 1, "late": 0, "corrected": 0}
        wm_now = self._watermark.current_ms
        base = state.finalize(wm_now if wm_now is not None else 0, self._checkpoint_id)
        corrected_result = WindowResult(
            key=base.key,
            window_start_ms=base.window_start_ms,
            window_end_ms=base.window_end_ms,
            aggregate=base.aggregate,
            value=base.value,
            count=base.count,
            included_event_ids=base.included_event_ids,
            first_event_time_ms=base.first_event_time_ms,
            last_event_time_ms=base.last_event_time_ms,
            watermark_at_close_ms=previous.watermark_at_close_ms,
            checkpoint_id=base.checkpoint_id,
            updated_at_checkpoints=base.updated_at_checkpoints,
            revision=previous.revision + 1,
            corrected_event_ids=previous.corrected_event_ids + [offset],
        )
        self._sink.correct(corrected_result)
        self._events_since_checkpoint += 1
        logger.info(
            "window corrected | key=%s window=[%d,%d) revision=%d value=%s count=%d "
            "by_offset=%d event_time_ms=%d watermark_ms=%s",
            state.key, state.window_start_ms, state.window_end_ms,
            corrected_result.revision, corrected_result.value, corrected_result.count,
            offset, event.event_time_ms, wm_now,
        )
        return {"accepted": 0, "duplicates": 0, "late": 0, "corrected": 1}

    def _close_due_windows(self) -> int:
        wm = self._watermark.current_ms
        if wm is None:
            return 0
        due = sorted(
            (key, win) for key, win in self._pending.items()
            if win.window_end_ms <= wm
        )
        for key, win in due:
            result = win.finalize(wm, self._checkpoint_id)
            self._sink.collect(result)
            del self._pending[key]
            # 开启迟到容忍：关闭不等于遗忘，状态保留到容忍期结束，供修正使用。
            if self.config.allowed_lateness_ms > 0:
                self._correctable[key] = win
            logger.info(
                "window closed | key=%s window=[%d,%d) aggregate=%s value=%s count=%d "
                "watermark_ms=%d included=%s correctable=%s",
                win.key, win.window_start_ms, win.window_end_ms,
                win.aggregate, result.value, result.count, wm,
                result.included_event_ids,
                self.config.allowed_lateness_ms > 0,
            )
        self._purge_expired_corrections(wm)
        return len(due)

    def _purge_expired_corrections(self, wm: int) -> int:
        """释放已超出容忍范围的可修正状态（状态有上界，不随历史无限堆积）。

        释放判定在事件时间域进行：watermark >= window_end + allowed_lateness_ms。
        释放之后到达的更晚数据走旁路（LATE_BEYOND_ALLOWED_LATENESS），
        既不悄悄丢弃，也不会拿过期状态给出错误结论。
        """

        lateness = self.config.allowed_lateness_ms
        if lateness <= 0 or not self._correctable:
            return 0
        expired = sorted(
            key for key, win in self._correctable.items()
            if win.window_end_ms + lateness <= wm
        )
        for key in expired:
            win = self._correctable.pop(key)
            logger.info(
                "correction state purged | key=%s window=[%d,%d) "
                "allowed_lateness_ms=%d watermark_ms=%d",
                win.key, win.window_start_ms, win.window_end_ms, lateness, wm,
            )
        return len(expired)

    def _auto_due(self) -> bool:
        n = self.config.auto_checkpoint_events
        return n > 0 and self._events_since_checkpoint >= n

    @staticmethod
    def _merge_reports(total: IngestionReport, part: IngestionReport) -> IngestionReport:
        return IngestionReport(
            accepted=total.accepted + part.accepted,
            duplicates=total.duplicates + part.duplicates,
            late=total.late + part.late,
            rejected=total.rejected + part.rejected,
            corrected=total.corrected + part.corrected,
            watermark_ms=part.watermark_ms or total.watermark_ms,
            windows_finalized=total.windows_finalized + part.windows_finalized,
            rejected_items=total.rejected_items + part.rejected_items,
        )

    # ------------------------------------------------------------------ 只读视图
    @property
    def watermark_ms(self) -> int | None:
        return self._watermark.current_ms

    @property
    def next_offset(self) -> int:
        return self._next_offset

    @property
    def checkpoint_id(self) -> int:
        return self._checkpoint_id

    @property
    def buffer_in_flight(self) -> int:
        return len(self._buffer)

    def pending_windows(self) -> list[dict[str, object]]:
        """窗口关闭前的可更新中间聚合（解释乱序期间结果如何演变）。"""

        return [
            {
                "key": win.key,
                "window_start_ms": win.window_start_ms,
                "window_end_ms": win.window_end_ms,
                "aggregate": win.aggregate,
                "value": win.preliminary_value(),
                "count": win.count,
                "included_event_ids": list(win.included_event_ids),
            }
            for win in sorted(self._pending.values(), key=lambda w: (w.window_start_ms, w.key))
        ]

    def results(self) -> list[WindowResult]:
        return self._sink.results_sorted()

    def late_events(self) -> list[LateRecord]:
        return self._sink.late_events()

    def explain(self) -> dict[str, object]:
        return {
            "watermark_ms": self.watermark_ms,
            "next_offset": self._next_offset,
            "checkpoint_id": self._checkpoint_id,
            "buffer_in_flight": len(self._buffer),
            "buffer_capacity": self.config.buffer_capacity,
            "window_size_ms": self.config.window_size_ms,
            "max_out_of_orderness_ms": self.config.max_out_of_orderness_ms,
            "allowed_lateness_ms": self.config.allowed_lateness_ms,
            "aggregate": self.config.aggregate,
            "pending_windows": self.pending_windows(),
            "correctable_windows": [
                {
                    "key": win.key,
                    "window_start_ms": win.window_start_ms,
                    "window_end_ms": win.window_end_ms,
                    "aggregate": win.aggregate,
                    "count": win.count,
                    "correctable_until_ms": win.window_end_ms
                    + self.config.allowed_lateness_ms,
                }
                for win in sorted(
                    self._correctable.values(),
                    key=lambda w: (w.window_start_ms, w.key),
                )
            ],
            "finalized": [
                {
                    "key": r.key,
                    "window": [r.window_start_ms, r.window_end_ms],
                    "value": r.value,
                    "count": r.count,
                    "included_event_ids": r.included_event_ids,
                    "watermark_at_close_ms": r.watermark_at_close_ms,
                    "checkpoint_id": r.checkpoint_id,
                    "revision": r.revision,
                    "corrected_event_ids": r.corrected_event_ids,
                }
                for r in self.results()
            ],
            "late_events": [
                {
                    "offset": x.event_offset,
                    "key": x.key,
                    "event_time_ms": x.event_time_ms,
                    "window": [x.window_start_ms, x.window_end_ms],
                    "watermark_ms": x.watermark_ms,
                    "late_by_ms": x.late_by_ms,
                    "reason": x.reason,
                }
                for x in self.late_events()
            ],
            "duplicate_offsets": self._sink.duplicate_offsets(),
        }
