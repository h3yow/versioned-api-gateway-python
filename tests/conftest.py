"""共享测试夹具：以可控时钟装配网关，并确保单测打印判定日志。"""

from __future__ import annotations

import logging

import pytest

from gateway.clock import FakeClock
from gateway.core import Gateway
from gateway.idempotency import IdempotencyStore
from gateway.quota import DEFAULT_POLICIES, QuotaManager

from .fakes import FakeDownstream


@pytest.fixture(autouse=True)
def _gateway_logs(caplog: pytest.LogCaptureFixture) -> None:
    """让每个单测都以 INFO 级别捕获/打印网关判定日志。"""
    caplog.set_level(logging.INFO, logger="gateway")


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock(start=1000.0)


@pytest.fixture
def make_gateway(clock: FakeClock):
    """工厂夹具：注入指定下游，返回 (gateway, downstream)。"""

    def _factory(downstream: FakeDownstream | None = None) -> tuple[Gateway, FakeDownstream]:
        ds = downstream if downstream is not None else FakeDownstream()
        gw = Gateway(
            clock=clock,
            quota_manager=QuotaManager(DEFAULT_POLICIES, clock),
            idempotency_store=IdempotencyStore(clock, ttl_seconds=60.0),
            downstream=ds,
        )
        return gw, ds

    return _factory
