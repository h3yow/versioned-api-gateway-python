"""FastAPI 接入层：把 HTTP 请求映射为网关调用，并统一错误响应。"""

from __future__ import annotations

from fastapi import FastAPI, Header, Request
from fastapi.responses import JSONResponse

from .clock import SystemClock
from .core import Gateway, GatewayRequest
from .downstream import SimulatedDownstream
from .errors import ErrorCode, GatewayError
from .idempotency import IdempotencyStore
from .quota import DEFAULT_POLICIES, Caller, CallerTier, QuotaManager

# 错误码 -> HTTP 状态码
_HTTP_STATUS: dict[ErrorCode, int] = {
    ErrorCode.VERSION_MISSING: 400,
    ErrorCode.VERSION_UNKNOWN: 400,
    ErrorCode.VERSION_SUNSET: 410,
    ErrorCode.QUOTA_EXCEEDED: 429,
    ErrorCode.IDEMPOTENCY_CONFLICT: 409,
    ErrorCode.DOWNSTREAM_ERROR: 502,
    ErrorCode.DOWNSTREAM_TIMEOUT: 504,
}


def build_default_gateway() -> Gateway:
    """用系统时钟与默认策略装配网关。"""
    clock = SystemClock()
    return Gateway(
        clock=clock,
        quota_manager=QuotaManager(DEFAULT_POLICIES, clock),
        idempotency_store=IdempotencyStore(clock),
        downstream=SimulatedDownstream(),
    )


def create_app(gateway: Gateway | None = None) -> FastAPI:
    """构造 FastAPI 应用并挂在网关上。"""
    app = FastAPI(title="versioned-api-gateway")
    gw = gateway if gateway is not None else build_default_gateway()

    @app.exception_handler(GatewayError)
    async def _on_gateway_error(_: Request, exc: GatewayError) -> JSONResponse:
        headers: dict[str, str] = {}
        if exc.retry_after is not None:
            # Retry-After 单位为秒，向上取整为整数秒。
            seconds = int(exc.retry_after) + (1 if exc.retry_after % 1 else 0)
            headers["Retry-After"] = str(seconds)
        return JSONResponse(
            status_code=_HTTP_STATUS[exc.code],
            content={"error": {"code": exc.code.value, "message": exc.message}},
            headers=headers,
        )

    @app.post("/orders")
    async def create_order(
        request: Request,
        x_api_version: str | None = Header(default=None),
        x_caller_id: str | None = Header(default=None),
        x_caller_tier: str | None = Header(default=None),
        idempotency_key: str | None = Header(default=None),
    ) -> dict:
        body = await request.json()

        # 调用方身份：无 caller-id 视为匿名；tier 仅在提供合法值时采用。
        if x_caller_id:
            tier = CallerTier.NORMAL
            if x_caller_tier:
                try:
                    tier = CallerTier(x_caller_tier)
                except ValueError:
                    tier = CallerTier.NORMAL
            caller = Caller(caller_id=x_caller_id, tier=tier)
        else:
            caller = Caller(caller_id="anonymous", tier=CallerTier.ANONYMOUS)

        result = gw.handle(
            caller,
            GatewayRequest(
                capability="orders",
                version=x_api_version,
                payload=body if isinstance(body, dict) else {},
                idempotency_key=idempotency_key,
                method="POST",
            ),
        )
        return {
            "status": result.status,
            "version": result.version,
            "body": result.body,
            "reason": result.reason,
        }

    return app
