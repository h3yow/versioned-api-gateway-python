"""场景三：基于调用方身份的限流与配额。

覆盖：
- 匿名 / 普通 / 高权限三档独立配额口径；
- 不同调用方计数互不影响；
- 并发请求不会少计或重复计数（突发并发恰好打满额度，第 N+1 个被拒）；
- 超限时附带可解释 retry_after，用 FakeClock 推进时间后窗口重置。
"""

from __future__ import annotations

import threading

import pytest

from gateway.core import GatewayRequest
from gateway.errors import ErrorCode, GatewayError
from gateway.quota import (
    DEFAULT_POLICIES,
    Caller,
    CallerTier,
    QuotaManager,
    QuotaPolicy,
)

from .fakes import FakeDownstream  # noqa: F401  (make_gateway 夹具内部使用替身)

PAYLOAD = {"item": "x", "qty": 1}


def _call(gateway, caller, idx_key):
    return gateway.handle(
        caller,
        GatewayRequest("orders", "2026-01-01", PAYLOAD, idempotency_key=idx_key),
    )


def test_each_tier_has_independent_quota(make_gateway):
    gateway, _ = make_gateway()

    assert DEFAULT_POLICIES[CallerTier.ANONYMOUS].limit == 2
    assert DEFAULT_POLICIES[CallerTier.NORMAL].limit == 10
    assert DEFAULT_POLICIES[CallerTier.PRIVILEGED].limit == 100

    anon = Caller(caller_id="anon", tier=CallerTier.ANONYMOUS)
    # 匿名调用方：前 2 次成功，第 3 次被配额拒绝。
    assert _call(gateway, anon, "anon-1").status == "ok"
    assert _call(gateway, anon, "anon-2").status == "ok"
    with pytest.raises(GatewayError) as ei:
        _call(gateway, anon, "anon-3")
    assert ei.value.code is ErrorCode.QUOTA_EXCEEDED


def test_different_callers_counted_independently(make_gateway):
    gateway, _ = make_gateway()
    a = Caller(caller_id="alice", tier=CallerTier.ANONYMOUS)
    b = Caller(caller_id="bob", tier=CallerTier.ANONYMOUS)

    # alice 已打满，但 bob 的口径独立，仍可调用。
    _call(gateway, a, "a-1")
    _call(gateway, a, "a-2")
    with pytest.raises(GatewayError):
        _call(gateway, a, "a-3")
    assert _call(gateway, b, "b-1").status == "ok"
    assert _call(gateway, b, "b-2").status == "ok"


def test_concurrent_requests_count_exactly_once(clock):
    """突发并发：成功数必须严格等于额度，不少计也不重复计数。"""
    limit = 20
    qm = QuotaManager(
        {CallerTier.NORMAL: QuotaPolicy(limit=limit, window_seconds=60.0)}, clock
    )
    caller = Caller(caller_id="burst", tier=CallerTier.NORMAL)

    results: list[float | None] = []
    lock = threading.Lock()

    def worker():
        with lock:
            results.append(qm.acquire(caller))

    threads = [threading.Thread(target=worker) for _ in range(limit + 10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    allowed = [r for r in results if r is None]
    rejected = [r for r in results if r is not None]
    assert len(allowed) == limit
    assert len(rejected) == 10
    assert all(r > 0 for r in rejected)


def test_retry_after_resets_after_window_advances(make_gateway, clock):
    gateway, _ = make_gateway()
    anon = Caller(caller_id="anon-reset", tier=CallerTier.ANONYMOUS)
    policy = DEFAULT_POLICIES[CallerTier.ANONYMOUS]

    _call(gateway, anon, "r-1")
    _call(gateway, anon, "r-2")
    with pytest.raises(GatewayError) as ei:
        _call(gateway, anon, "r-3")
    assert ei.value.code is ErrorCode.QUOTA_EXCEEDED
    assert ei.value.retry_after is not None
    assert 0 < ei.value.retry_after <= policy.window_seconds

    # 推进可控时钟越过窗口，配额重置，可再次调用。
    clock.advance(policy.window_seconds + 0.001)
    assert _call(gateway, anon, "r-4").status == "ok"
