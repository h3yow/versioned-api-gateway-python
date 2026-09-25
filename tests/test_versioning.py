"""Scenario: version mismatch / negotiation rejections are distinguishable."""

from gateway.errors import ErrorCode
from gateway.quota import CallerTier
from gateway.service import GatewayRequest


def req(version, **kw):
    return GatewayRequest("caller-t", CallerTier.STANDARD, version, None, {}, **kw)


def test_missing_version_rejected_400(make_gateway):
    gw = make_gateway()
    r = gw.handle(req(None))
    assert not r.ok and r.status == 400
    assert r.error.code is ErrorCode.VERSION_MISSING


def test_unknown_version_rejected_404(make_gateway):
    gw = make_gateway()
    r = gw.handle(req("v99"))
    assert not r.ok and r.status == 404
    assert r.error.code is ErrorCode.VERSION_UNKNOWN


def test_sunset_version_rejected_410(make_gateway):
    gw = make_gateway()
    r = gw.handle(req("v1"))
    assert not r.ok and r.status == 410
    assert r.error.code is ErrorCode.VERSION_SUNSET


def test_each_version_gets_its_own_shape(make_gateway):
    gw = make_gateway()
    r2 = gw.handle(req("v2"))
    r3 = gw.handle(req("v3"))
    assert r2.ok and r2.body == {"api_version": "v2", "echo": {}}
    assert r3.ok and r3.body == {"api_version": "v3", "data": {"attributes": {}}}
    # v3 added without breaking v2's response.
    assert r2.deprecated and not r3.deprecated
