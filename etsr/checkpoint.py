"""检查点：位点 + 水位线 + 窗口聚合状态的本地持久化与单调恢复。"""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from etsr.errors import CheckpointCorruptError, CheckpointRegressionError

CHECKPOINT_FORMAT_VERSION = 1


@dataclass(frozen=True, slots=True)
class Checkpoint:
    """一次一致性快照。

    包含：检查点 id、下一条待处理位点 next_offset、水位线、
    全部尚未关闭窗口的聚合状态、已输出窗口键、被识别为重复的位点。
    """

    checkpoint_id: int
    next_offset: int
    watermark_ms: int | None
    windows: dict[str, dict[str, Any]] = field(default_factory=dict)
    finalized_results: list[dict[str, Any]] = field(default_factory=list)
    duplicate_offsets: list[int] = field(default_factory=list)
    # 已关闭但仍在迟到容忍范围内、可被修正的窗口聚合状态（有上界：随水位线推进释放）。
    correctable_windows: dict[str, dict[str, Any]] = field(default_factory=dict)


def checkpoint_to_dict(checkpoint: Checkpoint) -> dict[str, Any]:
    return {
        "format_version": CHECKPOINT_FORMAT_VERSION,
        "checkpoint_id": checkpoint.checkpoint_id,
        "next_offset": checkpoint.next_offset,
        "watermark_ms": checkpoint.watermark_ms,
        "windows": checkpoint.windows,
        "finalized_results": list(checkpoint.finalized_results),
        "duplicate_offsets": list(checkpoint.duplicate_offsets),
        "correctable_windows": checkpoint.correctable_windows,
    }


def checkpoint_from_dict(data: dict[str, Any]) -> Checkpoint:
    try:
        if int(data.get("format_version", -1)) != CHECKPOINT_FORMAT_VERSION:
            raise CheckpointCorruptError("unsupported checkpoint format version")
        return Checkpoint(
            checkpoint_id=int(data["checkpoint_id"]),
            next_offset=int(data["next_offset"]),
            watermark_ms=data.get("watermark_ms"),
            windows=dict(data.get("windows", {})),
            finalized_results=list(data.get("finalized_results", [])),
            duplicate_offsets=[int(x) for x in data.get("duplicate_offsets", [])],
            # 旧版本检查点没有该字段，缺省为空（等价于未开启迟到容忍）。
            correctable_windows=dict(data.get("correctable_windows", {})),
        )
    except CheckpointCorruptError:
        raise
    except (KeyError, TypeError, ValueError) as exc:
        raise CheckpointCorruptError(f"malformed checkpoint: {exc}") from exc


class LocalCheckpointStore:
    """检查点目录下的原子 JSON 持久化，id 单调递增。"""

    PREFIX = "checkpoint-"
    SUFFIX = ".json"

    def __init__(self, state_dir: str | Path) -> None:
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)

    def _path(self, checkpoint_id: int) -> Path:
        return self.state_dir / f"{self.PREFIX}{checkpoint_id:010d}{self.SUFFIX}"

    def save(self, checkpoint: Checkpoint) -> Path:
        path = self._path(checkpoint.checkpoint_id)
        if path.exists():
            raise CheckpointRegressionError(
                f"refuse to overwrite checkpoint id={checkpoint.checkpoint_id}"
            )
        payload = json.dumps(checkpoint_to_dict(checkpoint), ensure_ascii=False, indent=2)
        # 原子落盘：先写临时文件再 rename，崩溃不会留下半个检查点。
        fd, tmp_name = tempfile.mkstemp(dir=self.state_dir, suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                fh.write(payload)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp_name, path)
        except BaseException:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)
            raise
        return path

    def _read_path(self, path: Path) -> Checkpoint:
        try:
            with path.open("r", encoding="utf-8") as fh:
                return checkpoint_from_dict(json.load(fh))
        except CheckpointCorruptError:
            raise
        except (OSError, json.JSONDecodeError) as exc:
            raise CheckpointCorruptError(f"cannot read {path}: {exc}") from exc

    def load(self, checkpoint_id: int) -> Checkpoint | None:
        path = self._path(checkpoint_id)
        if not path.exists():
            return None
        return self._read_path(path)

    def load_latest(self) -> Checkpoint | None:
        ids = self.list_ids()
        return self.load(ids[-1]) if ids else None

    def list_ids(self) -> list[int]:
        ids = []
        for path in self.state_dir.glob(f"{self.PREFIX}*{self.SUFFIX}"):
            token = path.name[len(self.PREFIX):-len(self.SUFFIX)]
            if token.isdigit():
                ids.append(int(token))
        return sorted(ids)

    def reject_stale_restore(self, candidate_id: int, current_id: int) -> None:
        """恢复目标必须不旧于当前检查点；位点/检查点回退明确拒绝。"""

        if candidate_id < current_id:
            raise CheckpointRegressionError(
                f"refuse stale restore: current checkpoint id={current_id}, "
                f"requested id={candidate_id}"
            )
