"""本地文件检查点存储：位点 + 水位线 + 窗口聚合状态的原子保存与恢复。"""

from __future__ import annotations

import json
import os
from pathlib import Path

from .errors import CheckpointCorruptionError, OffsetRegressionError

_REQUIRED_KEYS = {"offset", "watermark", "windows"}


class CheckpointStore:
    """以 JSON 文件原子落盘（写临时文件后 rename）。"""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)

    def save(self, checkpoint: dict) -> None:
        missing = _REQUIRED_KEYS - checkpoint.keys()
        if missing:
            raise ValueError(f"checkpoint missing keys: {sorted(missing)}")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text(json.dumps(checkpoint, indent=2, sort_keys=True), encoding="utf-8")
        os.replace(tmp, self.path)  # 原子替换，避免半写状态

    def load(self) -> dict | None:
        """无检查点返回 None；损坏时抛 CheckpointCorruptionError。"""
        if not self.path.exists():
            return None
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise CheckpointCorruptionError(f"unreadable checkpoint {self.path}: {exc}") from exc
        if not isinstance(data, dict) or not _REQUIRED_KEYS <= data.keys():
            raise CheckpointCorruptionError(
                f"checkpoint {self.path} missing keys: {sorted(_REQUIRED_KEYS - data.keys())}"
            )
        return data

    def validate_offset(self, requested_offset: int) -> int:
        """恢复位点校验：回退（requested < checkpointed）必须抛 OffsetRegressionError。"""
        data = self.load()
        if data is None:
            return requested_offset
        checkpointed = int(data["offset"])
        if requested_offset < checkpointed:
            raise OffsetRegressionError(requested_offset, checkpointed)
        return requested_offset
