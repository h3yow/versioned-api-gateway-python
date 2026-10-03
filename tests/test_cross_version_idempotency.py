"""Scenario: the same idempotency key submitted across client versions.

During client upgrades, old and new versions may submit the same key in
sequence or concurrently. The downstream write must happen once, every
version must see its own declared response shape, and all views must point at
the same business key / canonical content.
"""

import threading

from gateway.errors import ErrorCode
from gateway.quota import CallerTier
from gateway.service import GatewayRequest, WriteResult


def req(key, payload, version, caller="c"):
    return GatewayRequest(caller, CallerTier.STANDARD, version, key, payload)


def cross_version_gw(make_gateway, handler):
    # Both versions share ONE write handler producing the same canonical
    # business result; renderers give each version its declared shape.
    return make_gateway(
        handlers={"v2": handler, "v3": handler},
        renderers={
            "v2": lambda v, e: {"api_version": v, "echo": e},
            "v3": lambda v, e: {"api_version": v, "data": {"attributes": e}},
        },
    )


def make_handler(calls):
    def handler(v, p):
        calls.append((v, p))
        return WriteResult(
            business_key="order-1",
            entity={"order_id": "order-1", "item": p["item"], "qty": p["qty"]},
        )

    return handler


def test_cross_version_sequential_replay_renders_each_version(make_gateway):
    calls = []
    gw = cross_version_gw(make_gateway, make_handler(calls))
    payload = {"item": "book", "qty": 1}

    r3 = gw.handle(req("k", payload, "v3"))
    r2 = gw.handle(req("k", payload, "v2"))
    r3_again = gw.handle(req("k", payload, "v3"))

    assert r3.ok and not r3.replayed
    assert r2.ok and r2.replayed
    assert r3_again.ok and r3_again.replayed
    # Downstream ran exactly once.
    assert calls == [("v3", payload)]
    # Each caller gets its own declared shape ...
    assert r3.body == {
        "api_version": "v3",
        "data": {"attributes": {"order_id": "order-1", "item": "book", "qty": 1}},
    }
    assert r2.body == {
        "api_version": "v2",
        "echo": {"order_id": "order-1", "item": "book", "qty": 1},
    }
    # ... yet both views point at the same business result.
    assert r3.body["data"]["attributes"]["order_id"] == r2.body["echo"]["order_id"]
    assert r3_again.body == r3.body


def test_leader_can_be_old_version_follower_new_version(make_gateway):
    """Old v2 client arrives first; the new v3 client replays in v3 shape."""
    calls = []
    gw = cross_version_gw(make_gateway, make_handler(calls))
    payload = {"item": "pen", "qty": 2}

    r2 = gw.handle(req("k", payload, "v2"))
    r3 = gw.handle(req("k", payload, "v3"))

    assert r2.ok and not r2.replayed
    assert r3.ok and r3.replayed
    assert calls == [("v2", payload)]
    assert r2.body["echo"]["order_id"] == "order-1"
    assert r3.body["data"]["attributes"]["order_id"] == "order-1"
    # Deprecated flag follows the responding caller's own version.
    assert r2.deprecated and not r3.deprecated


def test_cross_version_concurrent_execute_once_and_views_align(make_gateway):
    calls = []
    release = threading.Event()

    def handler(v, p):
        calls.append(v)
        release.wait(timeout=5.0)
        return WriteResult(business_key="order-X", entity={"order_id": "order-X", **p})

    gw = cross_version_gw(make_gateway, handler)
    payload = {"item": "mug", "qty": 1}
    versions = ["v2", "v3", "v3", "v2", "v3"]
    n = len(versions)
    barrier = threading.Barrier(n + 1)
    responses = []

    def submit(version):
        barrier.wait()
        responses.append((version, gw.handle(req("ck", payload, version))))

    threads = [threading.Thread(target=submit, args=(ver,)) for ver in versions]
    for t in threads:
        t.start()
    barrier.wait()
    release.set()
    for t in threads:
        t.join()

    assert len(calls) == 1, "downstream write happens once across versions"
    leaders = [r for ver, r in responses if not r.replayed]
    assert len(leaders) == 1
    assert all(r.ok for ver, r in responses)
    for ver, r in responses:
        entity = r.body["echo"] if ver == "v2" else r.body["data"]["attributes"]
        assert entity["order_id"] == "order-X"
        assert entity["item"] == "mug"
        assert r.body["api_version"] == ver


def test_cross_version_different_payload_is_conflict_even_from_other_version(
    make_gateway,
):
    """Same key, different business content — never silently merged."""
    calls = []
    gw = cross_version_gw(make_gateway, make_handler(calls))

    assert gw.handle(req("k", {"item": "a", "qty": 1}, "v3")).ok
    conflict = gw.handle(req("k", {"item": "b", "qty": 1}, "v2"))
    assert not conflict.ok and conflict.status == 409
    assert conflict.error.code is ErrorCode.IDEMPOTENCY_CONFLICT
    assert len(calls) == 1
