"""Scenario: downstream exceptions/timeouts become unified, sanitized errors."""

import threading

from gateway.errors import ErrorCode
from gateway.quota import CallerTier
from gateway.service import DownstreamTimeout, GatewayRequest


def req(key=None, caller="c"):
    return GatewayRequest(caller, CallerTier.STANDARD, "v3", key, {})


def test_exception_mapped_to_502_without_leaking_internals(make_gateway, caplog):
    secret = "postgres://user:password@internal-host/db"

    def boom(v, p):
        raise RuntimeError(f"connection failed: {secret}")

    gw = make_gateway(handlers={"v3": boom})
    r = gw.handle(req())
    assert not r.ok and r.status == 502
    assert r.error.code is ErrorCode.DOWNSTREAM_ERROR
    # No internal details leak into the externally visible result.
    assert secret not in r.error.message
    assert secret not in str(r.body)
    assert "RuntimeError" not in str(r.body)


def test_timeout_mapped_to_504_with_retry_hint(make_gateway):
    def slow(v, p):
        raise DownstreamTimeout()

    gw = make_gateway(handlers={"v3": slow})
    r = gw.handle(req())
    assert not r.ok and r.status == 504
    assert r.error.code is ErrorCode.DOWNSTREAM_TIMEOUT
    assert r.error.retry_after_seconds is not None


def test_error_and_timeout_are_distinguishable(make_gateway):
    def boom(v, p):
        raise ValueError("bad")

    def slow(v, p):
        raise DownstreamTimeout()

    r_err = make_gateway(handlers={"v3": boom}).handle(req())
    r_to = make_gateway(handlers={"v3": slow}).handle(req())
    assert r_err.error.code is not r_to.error.code
    assert r_err.status != r_to.status


def test_concurrent_waiters_observe_leader_failure(make_gateway):
    """Followers waiting on a failed leader must not hang or get a fake success."""
    release = threading.Event()

    def boom(v, p):
        release.wait(timeout=5.0)
        raise RuntimeError("nope")

    gw = make_gateway(handlers={"v3": boom})
    n = 8
    barrier = threading.Barrier(n + 1)
    responses = []

    def submit():
        barrier.wait()
        responses.append(gw.handle(req(key="k-fail")))

    threads = [threading.Thread(target=submit) for _ in range(n)]
    for t in threads:
        t.start()
    barrier.wait()
    import time

    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        time.sleep(0.005)
        # let the leader get into the handler before releasing
        if release.is_set():
            break
        release.set()  # idempotent: allow leader to proceed and fail
        break
    for t in threads:
        t.join(timeout=15.0)
    assert not any(t.is_alive() for t in threads), "waiters must not hang"
    # Leader failed; followers either saw the failure or retried and failed too.
    assert all(not r.ok for r in responses)
    assert all(r.error.code is ErrorCode.DOWNSTREAM_ERROR for r in responses)
