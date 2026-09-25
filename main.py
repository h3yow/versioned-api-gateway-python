"""HTTP adapter: exposes the gateway core over FastAPI.

Headers:
    X-API-Version:     client-declared API version (required)
    X-Caller-Id:       caller identity (defaults to "anonymous")
    X-Caller-Tier:     anonymous | standard | privileged (default: anonymous)
    X-Idempotency-Key: idempotency key for write requests (POST)
"""

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
)

idempotency = IdempotencyStore(clock, ttl_seconds=3600.0)


def handle_v2(version: str, payload: dict) -> dict:
    # v2 legacy shape: flat echo.
    return {"api_version": version, "echo": payload}


def handle_v3(version: str, payload: dict) -> dict:
    # v3 shape: structured envelope; additive change, v2 untouched.
    return {"api_version": version, "data": {"attributes": payload}, "meta": {"schema": "v3"}}


gateway = Gateway(
    versions,
    quota,
    idempotency,
    handlers={"v2": handle_v2, "v3": handle_v3},
    clock=clock,
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
