"""Scenario: the first attempt of a concurrent batch fails.

When the leader's downstream call fails (error or timeout), every follower
that was waiting on the same in-flight attempt must converge on the identical
failure conclusion — same error type, same retry hint — and downstream must
not be triggered a second time for that batch.
"""

import threading
import time

from gateway.errors import ErrorCode
from gateway.quota import CallerTier
from gateway.service import DownstreamTimeout, GatewayRequest


def req(key, payload, caller="c"):
    return GatewayRequest(caller, CallerTier.STANDARD, "v3", key, payload)


def run_batch(gw, key, payload, n=8):
    barrier = threading.Barrier(n + 1)
    responses = []

    def submit():
        barrier.wait()
        responses.append(gw.handle(req(key, payload)))

    threads = [threading.Thread(target=submit) for _ in range(n)]
    for t in threads:
        t.start()
    barrier.wait()
    return threads, responses


def join(threads):
    for t in threads:
        t.join()


def test_concurrent_batch_shares_downstream_error(make_gateway):
    executions = []
    gate = threading.Event()

    def failing(v, p):
        executions.append(p)
        gate.wait(timeout=5.0)  # hold the leader so followers pile up on WAIT
        raise RuntimeError("db connection refused")

    gw = make_gateway(handlers={"v3": failing})
    threads, responses = run_batch(gw, "k-fail", {"a": 1})
    deadline = time.monotonic() + 5.0
    while not executions and time.monotonic() < deadline:
        time.sleep(0.001)
    assert executions
    gate.set()
    join(threads)

    assert len(executions) == 1, "downstream must be triggered exactly once for the batch"
    assert len(responses) == 8
    for r in responses:
        assert not r.ok
        assert r.status == 502
        assert r.error.code is ErrorCode.DOWNSTREAM_ERROR
    # Identical conclusion for the whole batch.
    conclusions = {(r.error.code, r.error.message, r.error.retry_after_seconds) for r in responses}
    assert len(conclusions) == 1


def test_concurrent_batch_shares_downstream_timeout(make_gateway):
    executions = []
    gate = threading.Event()

    def timing_out(v, p):
        executions.append(p)
        gate.wait(timeout=5.0)
        raise DownstreamTimeout("slow db")

    gw = make_gateway(handlers={"v3": timing_out})
    threads, responses = run_batch(gw, "k-timeout", {"a": 1})
    deadline = time.monotonic() + 5.0
    while not executions and time.monotonic() < deadline:
        time.sleep(0.001)
    assert executions
    gate.set()
    join(threads)

    assert len(executions) == 1
    for r in responses:
        assert not r.ok
        assert r.status == 504
        assert r.error.code is ErrorCode.DOWNSTREAM_TIMEOUT
        # Same retry hint for everyone.
        assert r.error.retry_after_seconds == 1.0


def test_retry_after_failed_batch_still_allowed(make_gateway):
    executions = []
    gate = threading.Event()

    def flaky(v, p):
        executions.append(p)
        if len(executions) == 1:
            gate.wait(timeout=5.0)
            raise RuntimeError("transient")
        return {"recovered": True}

    gw = make_gateway(handlers={"v3": flaky})
    threads, responses = run_batch(gw, "k-retry", {"a": 1})
    deadline = time.monotonic() + 5.0
    while not executions and time.monotonic() < deadline:
        time.sleep(0.001)
    assert executions
    gate.set()
    join(threads)

    assert len(executions) == 1
    assert all(not r.ok and r.error.code is ErrorCode.DOWNSTREAM_ERROR for r in responses)

    # A *later* request (not part of the failed batch) may take over and retry.
    r = gw.handle(req("k-retry", {"a": 1}))
    assert r.ok and r.body == {"recovered": True}
    assert len(executions) == 2


def test_concurrent_cross_version_batch_shares_failure(make_gateway):
    executions = []
    gate = threading.Event()

    def failing(v, p):
        executions.append(p)
        gate.wait(timeout=5.0)
        raise DownstreamTimeout("slow db")

    renderers = {
        "v2": lambda c: {"api_version": "v2", "echo": c},
        "v3": lambda c: {"api_version": "v3", "data": {"attributes": c}},
    }
    gw = make_gateway(
        handlers={"v2": failing, "v3": failing},
        renderers=renderers,
    )
    versions = ["v2", "v3", "v2", "v3"]
    barrier = threading.Barrier(len(versions) + 1)
    responses = []

    def submit(v):
        barrier.wait()
        responses.append(gw.handle(GatewayRequest("c", CallerTier.STANDARD, v, "k-xf", {"a": 1})))

    threads = [threading.Thread(target=submit, args=(v,)) for v in versions]
    for t in threads:
        t.start()
    barrier.wait()
    deadline = time.monotonic() + 5.0
    while not executions and time.monotonic() < deadline:
        time.sleep(0.001)
    assert executions
    gate.set()
    join(threads)

    assert len(executions) == 1
    assert all(not r.ok and r.error.code is ErrorCode.DOWNSTREAM_TIMEOUT for r in responses)
