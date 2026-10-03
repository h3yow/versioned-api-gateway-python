"""Scenario: the first attempt in a concurrent batch fails.

If the leader of a concurrent same-key batch errors or times out, every
member of the batch must converge on that *same* failure: identical error
type and retry hint, downstream triggered exactly once. No member is let
through to trigger a second downstream call.
"""

import threading
import time

from gateway.errors import ErrorCode
from gateway.quota import CallerTier
from gateway.service import DownstreamTimeout, GatewayRequest


def req(key, version="v3", caller=None, i=None):
    return GatewayRequest(caller or f"c-{i}", CallerTier.STANDARD, version, key, {"a": 1})


def run_batch(gw, key, n, handler_block, versions=None):
    """Fire n same-key requests concurrently; return responses + downstream calls."""
    calls = []
    release = threading.Event()
    versions = versions or ["v3"] * n
    barrier = threading.Barrier(n + 1)
    responses = []

    def handler(v, p):
        calls.append((v, p))
        handler_block(release)

    gw._handlers = {ver: handler for ver in set(versions)}  # rebind
    threads = []

    def submit(i):
        barrier.wait()
        responses.append((versions[i], gw.handle(req(key, versions[i], i=i))))

    threads = [threading.Thread(target=submit, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    barrier.wait()
    # Wait until the leader has entered downstream.
    deadline = time.monotonic() + 5.0
    while not calls and time.monotonic() < deadline:
        time.sleep(0.001)
    assert calls, "leader never reached downstream"
    release.set()
    for t in threads:
        t.join(timeout=15.0)
    assert not any(t.is_alive() for t in threads), "waiters must not hang"
    return responses, calls


def test_batch_leader_runtime_error_converges(make_gateway):
    gw = make_gateway(handlers={"v3": lambda v, p: {}})

    def block(release):
        release.wait(timeout=5.0)
        raise RuntimeError("downstream blew up")

    responses, calls = run_batch(gw, "k-fail", n=12, handler_block=block)

    assert len(calls) == 1, "a failed batch must not trigger a second downstream call"
    assert len(responses) == 12
    assert all(not r.ok for ver, r in responses)
    assert all(r.status == 502 for ver, r in responses)
    assert all(r.error.code is ErrorCode.DOWNSTREAM_ERROR for ver, r in responses)
    # Same retry hint / message across the whole batch.
    assert len({r.error.message for ver, r in responses}) == 1
    assert len({r.error.retry_after_seconds for ver, r in responses}) == 1
    leaders = [r for ver, r in responses if not r.replayed]
    assert len(leaders) == 12  # failures are never marked as replayed successes


def test_batch_leader_timeout_converges_to_504(make_gateway):
    gw = make_gateway(handlers={"v3": lambda v, p: {}})

    def block(release):
        release.wait(timeout=5.0)
        raise DownstreamTimeout()

    responses, calls = run_batch(gw, "k-timeout", n=8, handler_block=block)

    assert len(calls) == 1
    assert all(not r.ok and r.status == 504 for ver, r in responses)
    assert all(r.error.code is ErrorCode.DOWNSTREAM_TIMEOUT for ver, r in responses)
    assert {r.error.retry_after_seconds for ver, r in responses} == {1.0}


def test_failure_converges_across_versions_too(make_gateway):
    gw = make_gateway(
        handlers={"v2": lambda v, p: {}, "v3": lambda v, p: {}},
        renderers={
            "v2": lambda v, e: {"api_version": v, "echo": e},
            "v3": lambda v, e: {"api_version": v, "data": {"attributes": e}},
        },
    )

    def block(release):
        release.wait(timeout=5.0)
        raise RuntimeError("nope")

    versions = ["v2", "v3", "v3", "v2"]
    responses, calls = run_batch(gw, "k-xv", n=4, handler_block=block, versions=versions)

    assert len(calls) == 1
    assert all(r.error.code is ErrorCode.DOWNSTREAM_ERROR for ver, r in responses)
    # Error envelope is uniform regardless of the declared version.
    assert all(r.body["error"]["code"] == "DOWNSTREAM_ERROR" for ver, r in responses)


def test_after_converged_failure_a_later_retry_may_execute(make_gateway):
    """Convergence applies within one concurrent batch; a *later* retry (after
    the leader has already failed) still takes over as a fresh leader."""
    gw = make_gateway(handlers={"v3": lambda v, p: {}})

    def fail_block(release):
        release.wait(timeout=5.0)
        raise RuntimeError("nope")

    responses, calls = run_batch(gw, "k-later", n=6, handler_block=fail_block)
    assert len(calls) == 1
    assert all(not r.ok for ver, r in responses)

    # Sequential retry after the batch has settled: allowed to re-execute.
    ok = []

    def recover(v, p):
        ok.append(p)
        return {"recovered": True}

    gw._handlers = {"v3": recover}
    retry = gw.handle(req("k-later"))
    assert retry.ok and retry.body == {"recovered": True}
    assert len(ok) == 1
