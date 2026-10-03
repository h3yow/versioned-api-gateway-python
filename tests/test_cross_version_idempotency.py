"""Scenario: one idempotency key shared across API versions.

During a client upgrade, old and new versions submit the same idempotency
key (sequentially or concurrently). The downstream write must execute exactly
once, every caller must receive the response shape of the version *it*
declared, and all shapes must point at the same business result.
"""

import threading
import time

import pytest

from gateway.errors import ErrorCode
from gateway.quota import CallerTier
from gateway.service import GatewayRequest


def render_v2(canonical):
    return {"api_version": "v2", "echo": canonical}


def render_v3(canonical):
    return {"api_version": "v3", "data": {"attributes": canonical}, "meta": {"schema": "v3"}}


RENDERERS = {"v2": render_v2, "v3": render_v3}


def make_executor(executions, gate=None):
    def execute(version, payload):
        executions.append(payload)
        if gate is not None:
            gate.wait(timeout=5.0)
        return {"order_id": "ord-42", "item": payload["item"], "qty": payload["qty"]}

    return execute


def req(version, key, payload, caller="c"):
    return GatewayRequest(caller, CallerTier.STANDARD, version, key, payload)


@pytest.fixture
def gateway_factory(make_gateway):
    def build(executions, gate=None):
        handlers = {"v2": make_executor(executions, gate), "v3": make_executor(executions, gate)}
        return make_gateway(handlers=handlers, renderers=RENDERERS)

    return build


def test_v2_writes_v3_replays_in_v3_shape(gateway_factory):
    executions = []
    gw = gateway_factory(executions)

    r1 = gw.handle(req("v2", "k-xv", {"item": "book", "qty": 1}))
    assert r1.ok and not r1.replayed
    assert r1.body == {"api_version": "v2", "echo": {"order_id": "ord-42", "item": "book", "qty": 1}}

    r2 = gw.handle(req("v3", "k-xv", {"item": "book", "qty": 1}))
    assert r2.ok and r2.replayed
    # Shape follows the caller's declared version, not the leader's.
    assert r2.body == {
        "api_version": "v3",
        "data": {"attributes": {"order_id": "ord-42", "item": "book", "qty": 1}},
        "meta": {"schema": "v3"},
    }
    # Same business result underneath both shapes; downstream ran once.
    assert r2.body["data"]["attributes"]["order_id"] == r1.body["echo"]["order_id"]
    assert len(executions) == 1


def test_v3_writes_v2_replays_in_v2_shape(gateway_factory):
    executions = []
    gw = gateway_factory(executions)

    r1 = gw.handle(req("v3", "k-xv2", {"item": "pen", "qty": 2}))
    r2 = gw.handle(req("v2", "k-xv2", {"item": "pen", "qty": 2}))
    assert r2.ok and r2.replayed
    assert r2.body["api_version"] == "v2"
    assert r2.body["echo"]["order_id"] == r1.body["data"]["attributes"]["order_id"]
    assert len(executions) == 1


def test_concurrent_cross_version_batch_converges(gateway_factory):
    executions = []
    gate = threading.Event()
    gw = gateway_factory(executions, gate=gate)

    versions = ["v2", "v3", "v3", "v2", "v3", "v2"]
    barrier = threading.Barrier(len(versions) + 1)
    responses = []

    def submit(v):
        barrier.wait()
        responses.append((v, gw.handle(req(v, "k-conc-xv", {"item": "book", "qty": 1}))))

    threads = [threading.Thread(target=submit, args=(v,)) for v in versions]
    for t in threads:
        t.start()
    barrier.wait()
    deadline = time.monotonic() + 5.0
    while not executions and time.monotonic() < deadline:
        time.sleep(0.001)
    assert executions, "leader never reached downstream"
    gate.set()
    for t in threads:
        t.join()

    assert len(executions) == 1, "downstream must run exactly once across versions"
    assert all(r.ok for _, r in responses)
    order_ids = set()
    for v, r in responses:
        if v == "v2":
            assert r.body["api_version"] == "v2"
            order_ids.add(r.body["echo"]["order_id"])
        else:
            assert r.body["api_version"] == "v3"
            order_ids.add(r.body["data"]["attributes"]["order_id"])
    assert order_ids == {"ord-42"}, "all versions must point at the same business result"


def test_cross_version_conflict_still_rejected(gateway_factory):
    executions = []
    gw = gateway_factory(executions)

    assert gw.handle(req("v2", "k-xv3", {"item": "book", "qty": 1})).ok
    r = gw.handle(req("v3", "k-xv3", {"item": "book", "qty": 999}))
    assert not r.ok and r.status == 409
    assert r.error.code is ErrorCode.IDEMPOTENCY_CONFLICT
    assert len(executions) == 1
