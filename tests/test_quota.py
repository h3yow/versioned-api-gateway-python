"""Scenario: quota exhaustion and per-tier isolation, deterministic clock."""

import threading

from gateway.errors import ErrorCode
from gateway.quota import CallerTier
from gateway.service import GatewayRequest


def req(caller, tier, key=None):
    return GatewayRequest(caller, tier, "v3", key, {})


def test_quota_exhaustion_returns_retry_hint(make_gateway, clock):
    gw = make_gateway(limits={CallerTier.ANONYMOUS: 3})
    for i in range(3):
        r = gw.handle(req("anon", CallerTier.ANONYMOUS))
        assert r.ok, f"request {i} should be allowed"
    r = gw.handle(req("anon", CallerTier.ANONYMOUS))
    assert not r.ok and r.status == 429
    assert r.error.code is ErrorCode.QUOTA_EXCEEDED
    # FakeClock starts at t=1000; the current 60s window ends at t=1020.
    assert r.error.retry_after_seconds == 20.0
    assert "Retry after" in r.error.message


def test_quota_window_resets_with_clock(make_gateway, clock):
    gw = make_gateway(limits={CallerTier.ANONYMOUS: 1})
    assert gw.handle(req("anon", CallerTier.ANONYMOUS)).ok
    assert not gw.handle(req("anon", CallerTier.ANONYMOUS)).ok
    clock.advance(61.0)
    assert gw.handle(req("anon", CallerTier.ANONYMOUS)).ok


def test_tiers_have_independent_quotas(make_gateway):
    gw = make_gateway(
        limits={
            CallerTier.ANONYMOUS: 1,
            CallerTier.STANDARD: 2,
            CallerTier.PRIVILEGED: 4,
        }
    )
    # Same caller id, different tiers: independent buckets.
    assert gw.handle(req("c", CallerTier.ANONYMOUS)).ok
    assert not gw.handle(req("c", CallerTier.ANONYMOUS)).ok
    assert gw.handle(req("c", CallerTier.STANDARD)).ok
    assert gw.handle(req("c", CallerTier.STANDARD)).ok
    assert not gw.handle(req("c", CallerTier.STANDARD)).ok
    for _ in range(4):
        assert gw.handle(req("c", CallerTier.PRIVILEGED)).ok
    assert not gw.handle(req("c", CallerTier.PRIVILEGED)).ok


def test_concurrent_requests_counted_exactly(make_gateway):
    limit = 25
    gw = make_gateway(limits={CallerTier.STANDARD: limit})
    results = []
    barrier = threading.Barrier(100)

    def hit():
        barrier.wait()
        results.append(gw.handle(req("c", CallerTier.STANDARD)).ok)

    threads = [threading.Thread(target=hit) for _ in range(100)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    # No lost updates and no over-counting under concurrency.
    assert sum(results) == limit
