"""HTTP adapter: exposes the gateway core over FastAPI.

Headers:
    X-API-Version:     client-declared API version (required)
    X-Caller-Id:       caller identity (defaults to "anonymous")
    X-Caller-Tier:     anonymous | standard | privileged (default: anonymous)
    X-Idempotency-Key: idempotency key for write requests (POST)
"""

import hashlib
import json
import logging

import uvicorn
from fastapi import FastAPI, Request, Response

from gateway.clock import SystemClock
from gateway.idempotency import IdempotencyStore
from gateway.quota import CallerTier, QuotaLimiter
from gateway.service import Gateway, GatewayRequest
from gateway.versioning import VersionRegistry, VersionInfo, VersionStatus

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")

clock = SystemClock()

versions = VersionRegistry(
    [
        VersionInfo("v1", VersionStatus.SUNSET),
        VersionInfo("v2", VersionStatus.DEPRECATED),
        VersionInfo("v3", VersionStatus.ACTIVE),
    ]
)

quota = QuotaLimiter(
    clock,
    limits={
        CallerTier.ANONYMOUS: 10,
        CallerTier.STANDARD: 100,
        CallerTier.PRIVILEGED: 1000,
    },
    window_seconds=60.0,
    # Capacity bound: stale-window buckets are reclaimed under pressure, so
    # resident memory tracks the callers active in the current window.
    max_buckets=10_000,
)

idempotency = IdempotencyStore(
    clock,
    ttl_seconds=3600.0,
    # Capacity bound: expired/abandoned records are reclaimed under pressure.
    max_records=10_000,
    inflight_timeout_seconds=300.0,
)


def execute_order(version: str, payload: dict) -> dict:
    """Downstream write, executed exactly once per idempotency key.

    Returns the canonical business result; the per-version response shape is
    produced by the renderers below, so a replay always matches the shape of
    the version the replaying caller declared.
    """
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    order_id = "ord-" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:12]
    return {"order_id": order_id, "item": payload.get("item"), "qty": payload.get("qty")}


def render_v2(canonical: dict) -> dict:
    # v2 legacy shape: flat echo.
    return {"api_version": "v2", "echo": canonical}


def render_v3(canonical: dict) -> dict:
    # v3 shape: structured envelope; additive change, v2 untouched.
    return {"api_version": "v3", "data": {"attributes": canonical}, "meta": {"schema": "v3"}}


gateway = Gateway(
    versions,
    quota,
    idempotency,
    handlers={"v2": execute_order, "v3": execute_order},
    clock=clock,
    renderers={"v2": render_v2, "v3": render_v3},
)

app = FastAPI(title="versioned-api-gateway")


def _to_request(req: Request, payload: dict) -> GatewayRequest:
    tier_raw = req.headers.get("x-caller-tier", CallerTier.ANONYMOUS.value)
    try:
        tier = CallerTier(tier_raw)
    except ValueError:
        tier = CallerTier.ANONYMOUS
    return GatewayRequest(
        caller_id=req.headers.get("x-caller-id", "anonymous"),
        tier=tier,
        version=req.headers.get("x-api-version"),
        idempotency_key=req.headers.get("x-idempotency-key"),
        payload=payload,
    )


def _to_http(resp, out: Response) -> dict:
    out.status_code = resp.status
    if resp.deprecated:
        out.headers["Deprecation"] = "true"
    if resp.error and resp.error.retry_after_seconds is not None:
        out.headers["Retry-After"] = str(int(resp.error.retry_after_seconds))
    if resp.replayed:
        out.headers["X-Idempotent-Replay"] = "true"
    return resp.body


@app.post("/orders")
async def create_order(req: Request, out: Response) -> dict:
    payload = await req.json()
    return _to_http(gateway.handle(_to_request(req, payload)), out)


@app.get("/orders")
async def list_orders(req: Request, out: Response) -> dict:
    return _to_http(gateway.handle(_to_request(req, {})), out)


@app.get("/")
def read_root():
    return {"Hello": "World"}


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=0)
