"""检查点与断点续算：恢复一致性、位点/检查点回退拒绝、位点缺口拒绝。"""

from __future__ import annotations

import logging

import pytest

from etsr.engine import EngineConfig, StreamRuntime
from etsr.errors import (
    CheckpointCorruptError,
    CheckpointRegressionError,
    OffsetGapError,
)
from etsr.source import LocalEventSource

from conftest import make_event

logger = logging.getLogger("etsr.test")


def _reference_run(events: list, state_dir: str) -> list[tuple]:
    rt = StreamRuntime(EngineConfig(window_size_ms=1000, state_dir=state_dir + "/ref"))
    rt.start()
    rt.run_source(LocalEventSource(events))
    rt.advance_watermark(10_000)
    return _fingerprint(rt)


def _fingerprint(rt: StreamRuntime) -> list[tuple]:
    return [
        (r.key, r.window_start_ms, r.window_end_ms, r.value, r.count,
         tuple(r.included_event_ids))
        for r in rt.results()
    ]


def test_checkpoint_contains_offset_watermark_and_windows(
    tmp_path: pytest.TempPathFactory,
) -> None:
    rt = StreamRuntime(
        EngineConfig(window_size_ms=1000, state_dir=str(tmp_path / "s"))  # type: ignore[operator]
    )
    rt.start()
    rt.ingest_event(make_event(0, 100, "a", 1.0))
    rt.ingest_event(make_event(1, 900, "a", 2.0))
    cp = rt.checkpoint()
    assert cp.next_offset == 2
    assert cp.watermark_ms == 900
    assert "a|0" in cp.windows
    logger.info(
        "checkpoint | id=%d next_offset=%d watermark_ms=%s pending=%s",
        cp.checkpoint_id, cp.next_offset, cp.watermark_ms, list(cp.windows),
    )


def test_restart_replay_matches_failure_free_run(
    tmp_path: pytest.TempPathFactory,
    out_of_order_events: list,
) -> None:
    state_dir = str(tmp_path / "s")  # type: ignore[operator]
    reference = _reference_run(out_of_order_events, state_dir)

    # 第一次运行：处理 4 条后模拟崩溃，stop 前留下一致性检查点。
    rt1 = StreamRuntime(EngineConfig(window_size_ms=1000, state_dir=state_dir + "/crash"))
    rt1.start()
    rt1.run_source(LocalEventSource(out_of_order_events), max_events=4)
    crashed_cp = rt1.stop()
    assert crashed_cp.next_offset == 4
    logger.info("crash simulated at next_offset=%d", crashed_cp.next_offset)

    # 第二次运行（新实例=新进程）：自动从最新检查点恢复，源从位点 4 重放。
    rt2 = StreamRuntime(EngineConfig(window_size_ms=1000, state_dir=state_dir + "/crash"))
    restored = rt2.start()
    assert restored is not None and restored.next_offset == 4
    rt2.run_source(LocalEventSource(out_of_order_events).replay_from(4))
    rt2.advance_watermark(10_000)

    recovered = _fingerprint(rt2)
    assert recovered == reference
    logger.info(
        "recovery consistent | checkpoint_id=%d watermark_ms=%s windows=%d",
        rt2.checkpoint_id, rt2.watermark_ms, len(recovered),
    )


def test_restart_with_duplicate_redelivery_stays_consistent(
    tmp_path: pytest.TempPathFactory,
    out_of_order_events: list,
) -> None:
    """崩溃边界 at-least-once：已提交段被重复投递，结果必须与无故障一致。"""

    state_dir = str(tmp_path / "s") + "/dup"
    reference = _reference_run(out_of_order_events, state_dir)

    rt1 = StreamRuntime(EngineConfig(window_size_ms=1000, state_dir=state_dir))
    rt1.start()
    rt1.run_source(LocalEventSource(out_of_order_events), max_events=5)
    rt1.stop()  # next_offset=5

    rt2 = StreamRuntime(EngineConfig(window_size_ms=1000, state_dir=state_dir))
    rt2.start()
    # 源从更旧的位点 0 重放（重复投递已确认段 0..4），再续 5..。
    rt2.run_source(LocalEventSource(out_of_order_events).replay_from(0))
    rt2.advance_watermark(10_000)
    assert _fingerprint(rt2) == reference
    # 0..4 全部识别为重复，offset 5 起为正常处理。
    assert set(rt2.explain()["duplicate_offsets"]) == {0, 1, 2, 3, 4}


def test_restoring_older_checkpoint_is_explicitly_rejected(
    tmp_path: pytest.TempPathFactory,
) -> None:
    state_dir = str(tmp_path / "s")  # type: ignore[operator]
    rt = StreamRuntime(EngineConfig(window_size_ms=1000, state_dir=state_dir))
    rt.start()
    rt.ingest_event(make_event(0, 100, "a", 1.0))
    rt.checkpoint()  # id=1
    rt.ingest_event(make_event(1, 200, "a", 1.0))
    rt.checkpoint()  # id=2
    assert rt.checkpoint_id == 2

    with pytest.raises(CheckpointRegressionError) as exc:
        rt.restore_from(1)
    assert exc.value.reason == "STALE_CHECKPOINT"
    logger.info("stale restore rejected: %s", exc.value)


def test_offset_gap_is_rejected_instead_of_continuing(
    tmp_path: pytest.TempPathFactory,
) -> None:
    state_dir = str(tmp_path / "s")  # type: ignore[operator]
    rt = StreamRuntime(EngineConfig(window_size_ms=1000, state_dir=state_dir))
    rt.start()
    rt.ingest_event(make_event(0, 100, "a", 1.0))
    rt.ingest_event(make_event(1, 200, "a", 1.0))
    # 跳过 offset 2 直接来 3：必须明确拒绝，而不是从中间继续。
    with pytest.raises(OffsetGapError) as exc:
        rt.ingest_event(make_event(3, 300, "a", 1.0))
    assert exc.value.reason == "OFFSET_GAP"
    # 位点未前进、水位线与既有结果不受影响。
    assert rt.next_offset == 2


def test_corrupt_checkpoint_is_detected(
    tmp_path: pytest.TempPathFactory,
) -> None:
    state_dir = str(tmp_path / "s")  # type: ignore[operator]
    rt = StreamRuntime(EngineConfig(window_size_ms=1000, state_dir=state_dir))
    rt.start()
    rt.checkpoint()
    (tmp_path / "s" / "checkpoint-0000000001.json").write_text("{not-json", encoding="utf-8")  # type: ignore[operator]
    with pytest.raises(CheckpointCorruptError):
        StreamRuntime(EngineConfig(window_size_ms=1000, state_dir=state_dir)).start()


def test_auto_checkpoint_during_run(
    tmp_path: pytest.TempPathFactory,
    out_of_order_events: list,
) -> None:
    state_dir = str(tmp_path / "auto")  # type: ignore[operator]
    rt = StreamRuntime(
        EngineConfig(
            window_size_ms=1000,
            auto_checkpoint_events=3,
            state_dir=state_dir,
        )
    )
    rt.start()
    rt.run_source(LocalEventSource(out_of_order_events), max_events=4)
    assert rt.checkpoint_id >= 1  # 每处理 3 条自动快照
    # 用自动快照恢复，水位线与位点衔接正确
    rt2 = StreamRuntime(
        EngineConfig(window_size_ms=1000, auto_checkpoint_events=3, state_dir=state_dir)
    )
    rt2.start()
    assert rt2.next_offset == 4


def test_window_finalized_records_checkpoint_id(
    tmp_path: pytest.TempPathFactory,
) -> None:
    state_dir = str(tmp_path / "cid")  # type: ignore[operator]
    rt = StreamRuntime(EngineConfig(window_size_ms=1000, state_dir=state_dir))
    rt.start()
    rt.ingest_event(make_event(0, 100, "a", 1.0))
    rt.checkpoint()  # id=1
    rt.advance_watermark(1000)
    result = rt.results()[0]
    assert result.checkpoint_id == 1  # 窗口在哪个检查点时代关闭，可解释可追溯
