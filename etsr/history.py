"""封存历史归档：已过容忍期窗口的结论与修正流水的追加式本地持久化。

设计动机：开启迟到容忍后，若把全部历史结论与修正流水都塞进每次检查点，
检查点文件与内存会随历史线性膨胀。这里把"彻底过了容忍期、不再可能被修正"
的窗口数据封存（seal）进追加式归档文件 history.jsonl：

- 每个窗口只在其容忍期届满时被追加一次，之后检查点不再重复携带；
- 归档只追加不重写，崩溃时最后一行可能残缺，读取时容忍并忽略；
- 崩溃重放可能让同一窗口被重复封存，读取时按窗口/版本去重（取最高 revision），
  保证重启后看到的结论与全程无故障运行完全一致。
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Iterator

from etsr.model import CorrectionRecord, WindowResult

HISTORY_FILE_NAME = "history.jsonl"


class HistoryArchive:
    """state_dir 下的追加式历史归档（已封存、不可再变的窗口历史）。"""

    def __init__(self, state_dir: str | Path) -> None:
        self._path = Path(state_dir) / HISTORY_FILE_NAME

    @property
    def path(self) -> Path:
        return self._path

    def append_result(self, result: WindowResult) -> None:
        self._append({"type": "result", "result": result.to_dict()})

    def append_correction(self, record: CorrectionRecord) -> None:
        self._append({"type": "correction", "correction": record.to_dict()})

    def _append(self, record: dict[str, Any]) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps(record, ensure_ascii=False) + "\n"
        # 追加 + fsync：先落盘成功，调用方才允许从活动状态中移除对应数据，
        # 崩溃不会出现"两边都没有"的丢失窗口。
        with self._path.open("a", encoding="utf-8") as fh:
            fh.write(line)
            fh.flush()
            os.fsync(fh.fileno())

    def _iter_records(self) -> Iterator[dict[str, Any]]:
        if not self._path.exists():
            return
        with self._path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    # 崩溃可能留下写了一半的最后一行：忽略它，对应数据会随
                    # 检查点恢复 + 重放重新封存，绝不静默丢失结论。
                    continue
                if isinstance(record, dict) and "type" in record:
                    yield record

    def results(self) -> list[WindowResult]:
        """已封存的窗口结论；同一窗口重复封存时取最高 revision。"""

        by_window: dict[tuple[str, int, int], WindowResult] = {}
        for record in self._iter_records():
            if record.get("type") != "result":
                continue
            result = WindowResult.from_dict(record["result"])
            token = (result.key, result.window_start_ms, result.window_end_ms)
            existing = by_window.get(token)
            if existing is None or result.revision >= existing.revision:
                by_window[token] = result
        return list(by_window.values())

    def corrections(self) -> list[CorrectionRecord]:
        """已封存的修正流水；按 (窗口, revision) 去重，保持追加顺序。"""

        seen: set[tuple[str, int, int, int]] = set()
        records: list[CorrectionRecord] = []
        for record in self._iter_records():
            if record.get("type") != "correction":
                continue
            correction = CorrectionRecord.from_dict(record["correction"])
            token = (
                correction.key,
                correction.window_start_ms,
                correction.window_end_ms,
                correction.revision,
            )
            if token in seen:
                continue
            seen.add(token)
            records.append(correction)
        return records
