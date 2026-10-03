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
from gateway.service import Gateway, GatewayRequest, WriteResult
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
    max_buckets=100_000,
)

# Records older than the TTL (or idle longer than it) are auto-reclaimed;
# max_records is a hard resident-memory safety valve on top of TTL expiry.
idempotency = IdempotencyStore(clock, ttl_seconds=3600.0, max_records=1_000_000)


def _create_order(version: str, payload: dict) -> WriteResult:
    """One version-neutral business write, regardless of client version."""
    order_id = "ord-" + hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()[:16]
    canonical = {
        "order_id": order_id,
        "item": payload.get("item"),
        "qty": payload.get("qty", 1),
    }
    return WriteResult(business_key=order_id, entity=canonical)


def render_v2(version: str, entity: dict) -> dict:
    # v2 legacy shape: flat echo of the canonical business entity.
    return {"api_version": version, "echo": entity}


def render_v3(version: str, entity: dict) -> dict:
    # v3 shape: structured envelope over the same canonical business entity.
    return {
        "api_version": version,
        "data": {"attributes": entity},
        "meta": {"schema": "v3"},
    }


gateway = Gateway(
    versions,
    quota,
    idempotency,
    handlers={"v2": _create_order, "v3": _create_order},
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
