"""下游业务处理协议与默认模拟实现。

下游内部抛出的任何异常（含超时）都只在网关内部传播，
由编排层统一转换为 DOWNSTREAM_ERROR / DOWNSTREAM_TIMEOUT，
响应中不附带内部异常类型、堆栈等细节。
"""

from __future__ import annotations

import itertools
from typing import Protocol


class DownstreamError(Exception):
    """下游内部异常，网关不得把其细节泄漏给调用方。"""


class DownstreamTimeout(Exception):
    """下游处理超时。"""


class DownstreamService(Protocol):
    def handle(self, payload: dict) -> dict:
        """处理业务并返回版本无关的内部结果。"""
        ...


class SimulatedDownstream:
    """默认模拟下游：为下单类写请求生成版本无关的内部结果。"""

    def __init__(self) -> None:
        self._counter = itertools.count(1)

    def handle(self, payload: dict) -> dict:
        item = str(payload.get("item", "unknown"))
        qty = int(payload.get("qty", 1))
        return {
            "order_id": f"ord-{next(self._counter):06d}",
            "item": item,
            "qty": qty,
            "status": "created",
        }
