"""Scenario: concurrent duplicate submissions, replay, and payload conflict."""

import threading
import time

from gateway.errors import ErrorCode
from gateway.quota import CallerTier
from gateway.service import GatewayRequest


def req(key, payload, caller="c", tier=CallerTier.STANDARD):
    return GatewayRequest(caller, tier, "v3", key, payload)


def test_replay_returns_first_result(make_gateway):
    executions = []

    def handler(v, p):
        executions.append(p)
        return {"n": len(executions)}

    gw = make_gateway(handlers={"v3": handler})
    r1 = gw.handle(req("k1", {"a": 1}))
    r2 = gw.handle(req("k1", {"a": 1}))
    assert r1.ok and not r1.replayed
    assert r2.ok and r2.replayed
    assert r2.body == r1.body
    assert len(executions) == 1


def test_concurrent_duplicates_execute_once(make_gateway):
    executions = []
    release = threading.Event()

    def handler(v, p):
        executions.append(p)
        release.wait(timeout=5.0)  # hold the leader so others must wait
        return {"done": True}

    gw = make_gateway(handlers={"v3": handler})
    n = 16
    barrier = threading.Barrier(n + 1)  # n workers + main thread
    responses = []

    def submit():
        barrier.wait()
        responses.append(gw.handle(req("k-conc", {"a": 1})))

    threads = [threading.Thread(target=submit) for _ in range(n)]
    for t in threads:
        t.start()
    barrier.wait()  # release all workers simultaneously
    # Wait until the leader is inside the downstream handler, then let it finish.
    deadline = time.monotonic() + 5.0
    while not executions and time.monotonic() < deadline:
        time.sleep(0.001)
    assert executions, "leader never reached downstream"
    release.set()
    for t in threads:
        t.join()

    assert len(executions) == 1, "downstream must run exactly once"
    assert all(r.ok for r in responses)
    assert sum(1 for r in responses if r.replayed) == n - 1
    assert len({id(r.body) for r in responses}) >= 1
    assert all(r.body == {"done": True} for r in responses)


def test_same_key_different_payload_rejected(make_gateway):
    gw = make_gateway()
    assert gw.handle(req("k2", {"a": 1})).ok
    r = gw.handle(req("k2", {"a": 2}))
    assert not r.ok and r.status == 409
    assert r.error.code is ErrorCode.IDEMPOTENCY_CONFLICT
    # Original result is not silently overwritten.
    again = gw.handle(req("k2", {"a": 1}))
    assert again.ok and again.replayed


def test_conflict_also_applies_while_in_flight(make_gateway):
    release = threading.Event()

    def handler(v, p):
        release.wait(timeout=5.0)
        return {"ok": True}

    gw = make_gateway(handlers={"v3": handler})
    t = threading.Thread(target=lambda: gw.handle(req("k3", {"a": 1})))
    t.start()
    try:
        r = gw.handle(req("k3", {"a": 999}))
        assert not r.ok and r.error.code is ErrorCode.IDEMPOTENCY_CONFLICT
    finally:
        release.set()
        t.join()


def test_failed_attempt_allows_same_payload_retry(make_gateway):
    calls = []

    def flaky(v, p):
        calls.append(p)
        if len(calls) == 1:
            raise RuntimeError("transient")
        return {"recovered": True}

    gw = make_gateway(handlers={"v3": flaky})
    r1 = gw.handle(req("k4", {"a": 1}))
    assert not r1.ok and r1.error.code is ErrorCode.DOWNSTREAM_ERROR
    r2 = gw.handle(req("k4", {"a": 1}))
    assert r2.ok and r2.body == {"recovered": True}
    assert len(calls) == 2
