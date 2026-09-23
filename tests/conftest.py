"""pytest 公共夹具：事件时间 / 水位线 / 检查点日志与乱序事件构造。"""

from __future__ import annotations

import logging
import sys
from collections.abc import Iterator

import pytest

from etsr.model import Event

# 单测日志必须打印事件时间、水位线与检查点信息。
LOG_FORMAT = (
    "%(asctime)s %(levelname)s [%(name)s] %(message)s "
    "| event_time/watermark/checkpoint 见消息体"
)


def _configure_logging() -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(LOG_FORMAT))
    root = logging.getLogger("etsr")
    root.addHandler(handler)
    root.setLevel(logging.INFO)
    root.propagate = False


_configure_logging()


def make_event(offset: int, event_time_ms: int, key: str = "a", value: float = 1.0) -> Event:
    return Event(
        offset=offset,
        event_time_ms=event_time_ms,
        key=key,
        value=float(value),
        event_id=f"evt-{offset}",
    )


@pytest.fixture
def out_of_order_events() -> list[Event]:
    """事件时间乱序、含迟到、跨多个滚动窗口的一组事件（offset 连续）。"""

    # (offset, event_time, key, value)
    rows = [
        (0, 200, "a", 1.0),
        (1, 100, "a", 2.0),   # 乱序旧事件
        (2, 1500, "b", 10.0),  # 更晚窗口先到
        (3, 900, "a", 3.0),    # 回到窗口 [0,1000)
        (4, 1200, "b", 4.0),   # b 窗口内乱序
        (5, 2500, "a", 20.0),
        (6, 300, "a", 5.0),    # 水位线越过 1000 前到达 -> 仍计入
        (7, 3000, "b", 30.0),
        (8, 400, "a", 99.0),   # 水位线推进后到达 -> 迟到旁路
    ]
    return [make_event(*row) for row in rows]


@pytest.fixture
def tmp_state_dir(tmp_path: pytest.TempPathFactory) -> Iterator[str]:
    state_dir = tmp_path / "state"  # type: ignore[attr-defined]
    yield str(state_dir)
