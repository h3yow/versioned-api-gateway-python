"""场景二：版本协商与版本化响应结构。

覆盖：版本缺失/未知/已下线三类可区分拒绝；废弃版本可用但带告警；
同一内部结果在不同版本下渲染为不同结构（新增版本不破坏既有版本）。
使用 FakeClock 固定时间，结果完全确定可复现。
"""

from __future__ import annotations

import pytest

from gateway.core import GatewayRequest
from gateway.errors import ErrorCode, GatewayError
from gateway.quota import Caller, CallerTier

from .fakes import FakeDownstream

CALLER = Caller(caller_id="user-v", tier=CallerTier.PRIVILEGED)
PAYLOAD = {"item": "book", "qty": 2}
KEY = "version-key"


def _post(gateway, version, key=KEY):
    return gateway.handle(
        CALLER,
        GatewayRequest("orders", version, PAYLOAD, idempotency_key=key),
    )


def test_missing_version_rejected(make_gateway):
    gateway, _ = make_gateway()
    with pytest.raises(GatewayError) as ei:
        _post(gateway, None, key="v-missing")
    assert ei.value.code is ErrorCode.VERSION_MISSING


def test_unknown_version_rejected(make_gateway):
    gateway, _ = make_gateway()
    with pytest.raises(GatewayError) as ei:
        _post(gateway, "1999-01-01", key="v-unknown")
    assert ei.value.code is ErrorCode.VERSION_UNKNOWN


def test_sunset_version_rejected_with_successor_hint(make_gateway):
    gateway, _ = make_gateway()
    with pytest.raises(GatewayError) as ei:
        _post(gateway, "2024-01-01", key="v-sunset")
    assert ei.value.code is ErrorCode.VERSION_SUNSET
    assert "2025-01-01" in ei.value.message


def test_deprecated_version_usable_with_warning(make_gateway):
    gateway, _ = make_gateway()
    resp = _post(gateway, "2025-01-01", key="v-deprecated")
    assert resp.status == "deprecated"
    assert "warning" in resp.body
    assert "2026-01-01" in resp.body["warning"]


def test_distinct_version_renderings_are_stable(make_gateway):
    gateway, ds = make_gateway()

    # 共用同一幂等键：下游只执行一次，两个版本渲染同一内部结果。
    old = _post(gateway, "2025-01-01", key="v-shared")
    new = _post(gateway, "2026-01-01", key="v-shared")

    # 旧版平铺、数量为字符串；新版嵌套 data、数量为数值。
    assert set(old.body) == {"orderId", "item", "qty", "state", "warning"}
    assert old.body["qty"] == "2"
    assert set(new.body) == {"data"}
    assert new.body["data"]["qty"] == 2

    # 同一内部结果，订单号渲染后一致（证明只是结构不同）。
    assert old.body["orderId"] == new.body["data"]["order_id"]
    assert ds.calls == 1
