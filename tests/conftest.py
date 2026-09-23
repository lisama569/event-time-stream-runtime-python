import logging

import pytest


@pytest.fixture(autouse=True)
def pipeline_logs(caplog):
    """单测日志中打印事件时间、水位线与检查点信息。"""
    with caplog.at_level(logging.INFO, logger="stream_runtime.pipeline"):
        yield caplog
