"""长运行状态有界：检查点收敛、容忍边界释放、关闭时水位线语义、崩溃一致性。"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from etsr.engine import EngineConfig, StreamRuntime
from main import create_app

from conftest import make_event

logger = logging.getLogger("etsr.test")


def _runtime(tmp_path: pytest.TempPathFactory, name: str = "s", **kw: object) -> StreamRuntime:
    config = EngineConfig(window_size_ms=1000, state_dir=str(tmp_path / name), **kw)  # type: ignore[operator, arg-type]
    rt = StreamRuntime(config)
    rt.start()
    return rt


def _checkpoint_payload(rt: StreamRuntime) -> dict[str, object]:
    path = Path(rt.config.state_dir) / f"checkpoint-{rt.checkpoint_id:010d}.json"
    return json.loads(path.read_text(encoding="utf-8"))  # type: ignore[no-any-return]


def _fingerprint(rt: StreamRuntime) -> dict[str, object]:
    return {
        "results": [
            (r.key, r.window_start_ms, r.window_end_ms, r.value, r.count,
             tuple(r.included_event_ids), r.revision, r.watermark_at_close_ms)
            for r in rt.results()
        ],
        "corrections": [r.to_dict() for r in rt.corrections()],
        "late": [
            (x.event_offset, x.window_start_ms, x.reason)
            for x in rt.late_events()
        ],
        "watermark_ms": rt.watermark_ms,
    }


def test_checkpoint_and_memory_stay_bounded_over_long_run(
    tmp_path: pytest.TempPathFactory,
) -> None:
    """长时间运行 + 每窗口都有容忍期内修正：检查点文件与活动状态收敛，历史仍可查。"""

    rt = _runtime(tmp_path, allowed_lateness_ms=500)
    offset = 0
    max_correctable = 0
    for i in range(30):
        rt.ingest_event(make_event(offset, i * 1000 + 100, "a", 1.0))
        offset += 1
        rt.advance_watermark((i + 1) * 1000)  # 关闭窗口 i
        rt.ingest_event(make_event(offset, i * 1000 + 200, "a", 1.0))  # 容忍期内修正
        offset += 1
        rt.checkpoint()
        max_correctable = max(max_correctable, len(rt.correctable_windows()))

    payload = _checkpoint_payload(rt)
    # 容忍 500ms < 窗口 1000ms：任一时刻至多 1 个窗口处于可修正状态。
    assert max_correctable <= 1
    # 最后一个检查点只携带活动状态，不随 30 个窗口的历史膨胀。
    assert len(payload["finalized_results"]) <= 1  # type: ignore[arg-type]
    assert len(payload["corrections"]) <= 1  # type: ignore[arg-type]
    assert len(payload["correctable_windows"]) <= 1  # type: ignore[arg-type]
    path = Path(rt.config.state_dir) / f"checkpoint-{rt.checkpoint_id:010d}.json"
    assert path.stat().st_size < 2048  # 收敛到恒定小体积

    # 历史结论与修正流水一条不丢，仍可随时查询。
    assert len(rt.results()) == 30
    assert len(rt.corrections()) == 30
    assert rt.results()[0].included_event_ids == [0, 1]
    assert rt.corrections()[0].triggering_offsets == [1]
    logger.info(
        "bounded | checkpoints=%d last_cp_bytes=%d correctable=%d results=%d "
        "corrections=%d watermark_ms=%s",
        rt.checkpoint_id, path.stat().st_size, max_correctable,
        len(rt.results()), len(rt.corrections()), rt.watermark_ms,
    )


def test_state_released_exactly_at_tolerance_boundary(
    tmp_path: pytest.TempPathFactory,
) -> None:
    """容忍边界两侧：end+lateness-1 仍保留且进检查点，end+lateness 即释放且出检查点。"""

    rt = _runtime(tmp_path, allowed_lateness_ms=500)
    rt.ingest_event(make_event(0, 100, "a", 1.0))
    rt.advance_watermark(1000)  # 关闭 [0,1000)

    # 边界内侧：watermark = 1000 + 500 - 1 = 1499，状态保留。
    rt.advance_watermark(1499)
    assert len(rt.correctable_windows()) == 1
    rt.checkpoint()
    payload = _checkpoint_payload(rt)
    assert len(payload["correctable_windows"]) == 1  # type: ignore[arg-type]
    rep = rt.ingest_event(make_event(1, 300, "a", 9.0))
    assert rep.corrected == 1

    # 边界上：watermark = 1000 + 500 = 1500，状态释放并封存。
    rt.advance_watermark(1500)
    assert rt.correctable_windows() == []
    rt.checkpoint()
    payload = _checkpoint_payload(rt)
    assert payload["correctable_windows"] == {}
    assert payload["finalized_results"] == []  # 已封存，不再进检查点
    assert payload["corrections"] == []

    # 释放后才到的更晚数据：可区分的旁路结果，不丢、也不影响已给出的结论。
    rep = rt.ingest_event(make_event(2, 400, "a", 1000.0))
    assert rep.late == 1 and rep.corrected == 0
    assert rt.late_events()[-1].reason == "LATE_BEYOND_ALLOWED_LATENESS"
    final = rt.results()[0]
    assert final.value == 10.0 and final.revision == 1
    # 封存后修正信息依然可查：窗口、版本、修正后的值、参与事件。
    corrections = rt.corrections()
    assert len(corrections) == 1
    assert corrections[0].value == 10.0 and corrections[0].triggering_offsets == [1]
    logger.info(
        "boundary release | event_time_ms=400 watermark_ms=%s checkpoint_id=%d "
        "reason=%s sealed_value=%s",
        rt.watermark_ms, rt.checkpoint_id,
        rt.late_events()[-1].reason, final.value,
    )


def test_correction_never_rewrites_watermark_at_close(
    tmp_path: pytest.TempPathFactory,
) -> None:
    """窗口关闭那一刻的水位线恒定：修正只改结论，直接调用与 HTTP 看到的一致。"""

    rt = _runtime(tmp_path, allowed_lateness_ms=500)
    rt.ingest_event(make_event(0, 100, "a", 2.0))
    rt.advance_watermark(1000)  # 关闭 [0,1000)，关闭时水位线 1000
    assert rt.results()[0].watermark_at_close_ms == 1000

    rt.advance_watermark(1400)  # 水位线继续往前走
    rt.ingest_event(make_event(1, 200, "a", 5.0))  # 容忍期内第一次修正
    rt.advance_watermark(1490)
    rt.ingest_event(make_event(2, 300, "a", 1.0))  # 第二次修正
    final = rt.results()[0]
    assert final.revision == 2 and final.value == 8.0
    assert final.watermark_at_close_ms == 1000  # 不被修正那一刻的水位线改写

    # 崩溃重启后再修正，关闭时水位线依然保持。
    cp = rt.checkpoint()
    logger.info(
        "checkpoint before crash | id=%d watermark_ms=%s",
        cp.checkpoint_id, cp.watermark_ms,
    )
    rt2 = _runtime(tmp_path, "s", allowed_lateness_ms=500)
    rt2.ingest_event(make_event(3, 350, "a", 0.5))
    assert rt2.results()[0].revision == 3
    assert rt2.results()[0].watermark_at_close_ms == 1000

    # HTTP 入口看到的与直接调用一致。
    config = EngineConfig(
        window_size_ms=1000,
        allowed_lateness_ms=500,
        state_dir=str(tmp_path / "http"),  # type: ignore[operator]
    )
    with TestClient(create_app(config=config)) as client:
        client.post("/events", json={"offset": 0, "event_time_ms": 100, "key": "a", "value": 2})
        client.post("/watermark", json={"watermark_ms": 1000})
        client.post("/watermark", json={"watermark_ms": 1400})
        client.post("/events", json={"offset": 1, "event_time_ms": 200, "key": "a", "value": 5})
        state = client.get("/state").json()
    assert state["finalized"][0]["watermark_at_close_ms"] == 1000
    assert state["finalized"][0]["revision"] == 1
    logger.info(
        "watermark_at_close stable | api=%s http=%s revision=%d",
        rt2.results()[0].watermark_at_close_ms,
        state["finalized"][0]["watermark_at_close_ms"],
        rt2.results()[0].revision,
    )


def test_crash_restart_with_duplicates_matches_failure_free_after_sealing(
    tmp_path: pytest.TempPathFactory,
) -> None:
    """封存发生后崩溃重启 + 重复投递：最终结论逐窗口、逐值、逐计数、逐纳入事件一致。"""

    kw: dict[str, object] = {"allowed_lateness_ms": 500}

    def drive(rt: StreamRuntime, start_offset: int = 0) -> None:
        offset = start_offset
        for i in range(6):
            rt.ingest_event(make_event(offset, i * 1000 + 100, "a", float(i + 1)))
            offset += 1
            rt.advance_watermark((i + 1) * 1000)
            rt.ingest_event(make_event(offset, i * 1000 + 200, "a", 1.0))  # 容忍期内修正
            offset += 1
        rt.advance_watermark(7000)  # 全部窗口越过容忍期，尽数封存

    # 无故障参考运行。
    ref = _runtime(tmp_path, "ref", **kw)
    drive(ref)
    expected = _fingerprint(ref)
    assert len(ref.results()) == 6 and len(ref.corrections()) == 6

    # 故障运行：处理到一半做检查点并"崩溃"，恢复后重复投递已确认段再续跑。
    crashed = _runtime(tmp_path, "crash", **kw)
    offset = 0
    for i in range(3):  # 前 3 个窗口，含修正与封存
        crashed.ingest_event(make_event(offset, i * 1000 + 100, "a", float(i + 1)))
        offset += 1
        crashed.advance_watermark((i + 1) * 1000)
        crashed.ingest_event(make_event(offset, i * 1000 + 200, "a", 1.0))
        offset += 1
    cp = crashed.stop()  # 崩溃前检查点：窗口 0、1 已封存，窗口 2 仍在容忍期
    logger.info(
        "crash | checkpoint_id=%d next_offset=%d watermark_ms=%s",
        cp.checkpoint_id, cp.next_offset, cp.watermark_ms,
    )

    restored = _runtime(tmp_path, "crash", **kw)
    assert restored.next_offset == offset
    # 重复投递已确认段（含普通事件与修正事件）：不得把同一次修正算两遍。
    for dup_offset in range(offset):
        event_time = (dup_offset // 2) * 1000 + (100 if dup_offset % 2 == 0 else 200)
        value = float(dup_offset // 2 + 1) if dup_offset % 2 == 0 else 1.0
        rep = restored.ingest_event(make_event(dup_offset, event_time, "a", value))
        assert rep.duplicates == 1 and rep.corrected == 0
    assert len(restored.corrections()) == 3  # 修正次数不因重放翻倍

    # 继续剩余窗口，随后全部越过容忍期。
    for i in range(3, 6):
        restored.ingest_event(make_event(offset, i * 1000 + 100, "a", float(i + 1)))
        offset += 1
        restored.advance_watermark((i + 1) * 1000)
        restored.ingest_event(make_event(offset, i * 1000 + 200, "a", 1.0))
        offset += 1
    restored.advance_watermark(7000)

    assert _fingerprint(restored) == expected
    logger.info(
        "restart consistent after sealing | windows=%d corrections=%d watermark_ms=%s "
        "checkpoint_id=%d",
        len(restored.results()), len(restored.corrections()),
        restored.watermark_ms, restored.checkpoint_id,
    )


def test_crash_between_seal_and_checkpoint_recovers_consistently(
    tmp_path: pytest.TempPathFactory,
) -> None:
    """封存已落归档但尚未进任何检查点时崩溃：从封存前的旧检查点恢复 + 重放，
    重复封存被去重，最终结论与无故障运行完全一致。"""

    kw: dict[str, object] = {"allowed_lateness_ms": 500}

    def emit_window(rt: StreamRuntime, offset: int, i: int) -> int:
        rt.ingest_event(make_event(offset, i * 1000 + 100, "a", float(i + 1)))
        offset += 1
        rt.advance_watermark((i + 1) * 1000)
        rt.ingest_event(make_event(offset, i * 1000 + 200, "a", 1.0))  # 容忍期内修正
        return offset + 1

    # 无故障参考运行：窗口 0..3。
    ref = _runtime(tmp_path, "ref", **kw)
    offset = 0
    for i in range(4):
        offset = emit_window(ref, offset, i)
    ref.advance_watermark(5000)
    expected = _fingerprint(ref)

    # 故障运行：窗口 0、1 -> 检查点 -> 窗口 2、3（窗口 0、1 在此期间被封存）-> 崩溃。
    crashed = _runtime(tmp_path, "crash", **kw)
    offset = 0
    for i in range(2):
        offset = emit_window(crashed, offset, i)
    cp = crashed.checkpoint()
    for i in range(2, 4):
        offset = emit_window(crashed, offset, i)
    crashed.advance_watermark(5000)  # 窗口 0、1 已封存进归档，但不再落新检查点
    logger.info(
        "crash after seal | checkpoint_id=%d next_offset=%d watermark_ms=%s",
        cp.checkpoint_id, cp.next_offset, cp.watermark_ms,
    )
    del crashed  # 模拟进程崩溃：丢弃内存态，只留旧检查点 + 归档

    # 从封存前的旧检查点恢复：位点回退，重放窗口 2、3 并重复投递已确认段。
    restored = _runtime(tmp_path, "crash", **kw)
    assert restored.next_offset == 4
    for dup_offset in range(4):
        event_time = (dup_offset // 2) * 1000 + (100 if dup_offset % 2 == 0 else 200)
        value = float(dup_offset // 2 + 1) if dup_offset % 2 == 0 else 1.0
        rep = restored.ingest_event(make_event(dup_offset, event_time, "a", value))
        assert rep.duplicates == 1 and rep.corrected == 0
    offset = 4
    for i in range(2, 4):
        offset = emit_window(restored, offset, i)
    restored.advance_watermark(5000)

    assert _fingerprint(restored) == expected
    assert len(restored.corrections()) == 4  # 重复封存/重放不产生重复修正流水
    logger.info(
        "recover from pre-seal checkpoint consistent | windows=%d corrections=%d "
        "watermark_ms=%s",
        len(restored.results()), len(restored.corrections()), restored.watermark_ms,
    )
