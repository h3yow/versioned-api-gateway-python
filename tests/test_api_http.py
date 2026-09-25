"""HTTP 端到端：验证状态码、错误码与 Retry-After 头的对外映射。"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from gateway.api import create_app
from gateway.clock import FakeClock
from gateway.core import Gateway
from gateway.downstream import SimulatedDownstream
from gateway.idempotency import IdempotencyStore
from gateway.quota import DEFAULT_POLICIES, CallerTier, QuotaManager


@pytest.fixture
def client(clock):
    gw = Gateway(
        clock=clock,
        quota_manager=QuotaManager(DEFAULT_POLICIES, clock),
        idempotency_store=IdempotencyStore(clock),
        downstream=SimulatedDownstream(),
    )
    return TestClient(create_app(gw))


def test_success_and_idempotent_replay_over_http(client):
    headers = {"X-API-Version": "2026-01-01", "Idempotency-Key": "http-1"}
    r1 = client.post("/orders", json={"item": "book", "qty": 1}, headers=headers)
    r2 = client.post("/orders", json={"item": "book", "qty": 1}, headers=headers)
    assert r1.status_code == 200
    assert r2.status_code == 200
    assert r1.json()["body"] == r2.json()["body"]
    assert "重放" in r2.json()["reason"]


def test_missing_version_returns_400(client):
    r = client.post("/orders", json={"item": "book", "qty": 1},
                    headers={"Idempotency-Key": "http-2"})
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "version_missing"


def test_sunset_version_returns_410(client):
    r = client.post("/orders", json={"item": "book"},
                    headers={"X-API-Version": "2024-01-01", "Idempotency-Key": "http-3"})
    assert r.status_code == 410
    assert r.json()["error"]["code"] == "version_sunset"


def test_idempotency_conflict_returns_409(client):
    h = {"X-API-Version": "2026-01-01", "Idempotency-Key": "http-4"}
    client.post("/orders", json={"item": "book", "qty": 1}, headers=h)
    r = client.post("/orders", json={"item": "pen", "qty": 1}, headers=h)
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "idempotency_conflict"


def test_anonymous_quota_returns_429_with_retry_after(client):
    # 不带 X-Caller-Id 即匿名，匿名额度为每窗口 2 次。
    h = {"X-API-Version": "2026-01-01"}
    assert client.post("/orders", json={"item": "a"},
                       headers={**h, "Idempotency-Key": "q1"}).status_code == 200
    assert client.post("/orders", json={"item": "b"},
                       headers={**h, "Idempotency-Key": "q2"}).status_code == 200
    r = client.post("/orders", json={"item": "c"},
                    headers={**h, "Idempotency-Key": "q3"})
    assert r.status_code == 429
    assert r.json()["error"]["code"] == "quota_exceeded"
    assert int(r.headers["Retry-After"]) > 0
