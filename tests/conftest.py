import logging

import pytest

from gateway.clock import FakeClock
from gateway.idempotency import IdempotencyStore
from gateway.quota import CallerTier, QuotaLimiter
from gateway.service import Gateway
from gateway.versioning import VersionInfo, VersionRegistry, VersionStatus


@pytest.fixture(autouse=True)
def gateway_logs(caplog):
    """Surface gateway decision logs (caller/version/idempotency key/verdict)."""
    with caplog.at_level(logging.INFO, logger="gateway"):
        yield


@pytest.fixture
def clock():
    return FakeClock(start=1_000.0)


@pytest.fixture
def versions():
    return VersionRegistry(
        [
            VersionInfo("v1", VersionStatus.SUNSET),
            VersionInfo("v2", VersionStatus.DEPRECATED),
            VersionInfo("v3", VersionStatus.ACTIVE),
        ]
    )


@pytest.fixture
def make_gateway(clock, versions):
    """Factory: build a Gateway with controllable handlers and quota limits."""

    def build(handlers=None, limits=None, window_seconds=60.0, idem_ttl=300.0, renderers=None):
        if handlers is None:
            handlers = {
                "v2": lambda v, p: {"api_version": v, "echo": p},
                "v3": lambda v, p: {"api_version": v, "data": {"attributes": p}},
            }
        if limits is None:
            limits = {
                CallerTier.ANONYMOUS: 5,
                CallerTier.STANDARD: 50,
                CallerTier.PRIVILEGED: 500,
            }
        quota = QuotaLimiter(clock, limits, window_seconds)
        idem = IdempotencyStore(clock, idem_ttl)
        return Gateway(versions, quota, idem, handlers, clock, renderers=renderers)

    return build
