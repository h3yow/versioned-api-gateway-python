"""Gateway orchestration: version negotiation -> quota -> idempotency -> downstream."""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from typing import Any, Callable

from .clock import Clock
from .errors import ErrorCode, GatewayError
from .idempotency import BeginResult, IdempotencyRecord, IdempotencyStore, RecordState
from .quota import CallerTier, QuotaLimiter
from .versioning import VersionRegistry

logger = logging.getLogger("gateway")


@dataclass(frozen=True)
class GatewayRequest:
    caller_id: str
    tier: CallerTier
    version: str | None
    idempotency_key: str | None
    payload: dict[str, Any]


@dataclass(frozen=True)
class GatewayResponse:
    ok: bool
    status: int
    body: dict[str, Any]
    error: GatewayError | None = None
    deprecated: bool = False
    replayed: bool = False


# Downstream handler: (version, payload) -> canonical business result.
# The canonical result is what gets stored under the idempotency key; each
# request's response body is then rendered from it for that request's version.
DownstreamHandler = Callable[[str, dict[str, Any]], dict[str, Any]]

# Renderer: canonical result -> version-specific response body. Versions
# without a registered renderer use the identity renderer (canonical as-is),
# which preserves the historical behaviour for single-version deployments.
CanonicalRenderer = Callable[[dict[str, Any]], dict[str, Any]]


class DownstreamTimeout(Exception):
    """Raised by downstream adapters to signal a timeout."""


def _fingerprint(payload: dict[str, Any]) -> str:
    """Stable fingerprint of a payload for idempotency conflict detection."""
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class Gateway:
    def __init__(
        self,
        versions: VersionRegistry,
        quota: QuotaLimiter,
        idempotency: IdempotencyStore,
        handlers: dict[str, DownstreamHandler],
        clock: Clock,
        wait_timeout_seconds: float = 10.0,
        renderers: dict[str, CanonicalRenderer] | None = None,
    ) -> None:
        self._versions = versions
        self._quota = quota
        self._idempotency = idempotency
        self._handlers = handlers
        self._clock = clock
        self._wait_timeout = wait_timeout_seconds
        self._renderers = renderers or {}

    def handle(self, request: GatewayRequest) -> GatewayResponse:
        """Process one request through the gateway pipeline.

        Pipeline order: version negotiation -> quota -> idempotency -> downstream.
        Every decision is logged with caller, version, idempotency key and the
        final verdict rationale.
        """
        # 1. Version negotiation.
        negotiated = self._versions.negotiate(request.version)
        if isinstance(negotiated, GatewayError):
            return self._reject(request, negotiated, http_status=self._status_for(negotiated))

        version = negotiated.info.name
        handler = self._handlers.get(version)
        if handler is None:
            err = GatewayError(
                code=ErrorCode.VERSION_UNKNOWN,
                message=f"No handler registered for version '{version}'.",
            )
            return self._reject(request, err, http_status=404)

        # 2. Quota (consumed once per logical request, before idempotency so
        #    replays of the same submission are not double-counted here: each
        #    HTTP call is a distinct request and pays its own quota unit).
        decision = self._quota.check_and_consume(request.tier, request.caller_id)
        if not decision.allowed:
            err = GatewayError(
                code=ErrorCode.QUOTA_EXCEEDED,
                message=(
                    f"Quota exceeded for tier '{request.tier.value}'. "
                    f"Retry after {decision.retry_after_seconds:.0f}s."
                ),
                retry_after_seconds=decision.retry_after_seconds,
            )
            return self._reject(request, err, http_status=429)

        # 3. Idempotency (write requests carry an idempotency key).
        if request.idempotency_key is not None:
            return self._handle_idempotent(request, version, handler, negotiated.deprecated)

        # 4. Plain read/no-key request: straight to downstream.
        return self._invoke(request, version, handler, negotiated.deprecated)

    # -- internals ---------------------------------------------------------

    def _handle_idempotent(
        self,
        request: GatewayRequest,
        version: str,
        handler: DownstreamHandler,
        deprecated: bool,
    ) -> GatewayResponse:
        fingerprint = _fingerprint(request.payload)
        while True:
            outcome = self._idempotency.begin(request.idempotency_key, fingerprint)
            if outcome.kind is BeginResult.CONFLICT:
                return self._reject(request, outcome.error, http_status=409)
            if outcome.kind is BeginResult.REPLAY:
                return self._replay_success(request, version, outcome.record, deprecated)
            if outcome.kind is BeginResult.WAIT:
                record = outcome.record
                record.done.wait(timeout=self._wait_timeout)
                if not record.done.is_set():
                    continue  # leader still running; keep waiting
                # Converge on the outcome of the exact attempt this request
                # waited on: the whole concurrent batch shares one downstream
                # execution and therefore one conclusion, success or failure.
                if record.state is RecordState.COMPLETED:
                    return self._replay_success(request, version, record, deprecated)
                if record.state is RecordState.FAILED:
                    return self._replay_failure(request, version, record)
                continue
            # LEADER: execute downstream exactly once for this key.
            canonical, error_response = self._execute(request, version, handler)
            if error_response is not None:
                self._idempotency.fail(outcome.record, error_response.error)
                return error_response
            self._idempotency.complete(outcome.record, canonical)
            logger.info(
                "caller=%s version=%s idem=%s verdict=executed basis=downstream-ok",
                request.caller_id, version, request.idempotency_key,
            )
            return GatewayResponse(
                ok=True,
                status=200,
                body=self._render(version, canonical),
                deprecated=deprecated,
            )

    def _replay_success(
        self,
        request: GatewayRequest,
        version: str,
        record: IdempotencyRecord,
        deprecated: bool,
    ) -> GatewayResponse:
        logger.info(
            "caller=%s version=%s idem=%s verdict=replay basis=completed-record",
            request.caller_id, version, request.idempotency_key,
        )
        return GatewayResponse(
            ok=True,
            status=200,
            body=self._render(version, record.result),
            deprecated=deprecated,
            replayed=True,
        )

    def _replay_failure(
        self,
        request: GatewayRequest,
        version: str,
        record: IdempotencyRecord,
    ) -> GatewayResponse:
        """Replay the recorded failure conclusion to a same-batch follower.

        The follower gets the identical error (code, message, retry hint) the
        leader produced; downstream is not triggered again for this batch.
        """
        error = record.error
        status = 504 if error.code is ErrorCode.DOWNSTREAM_TIMEOUT else 502
        logger.info(
            "caller=%s version=%s idem=%s verdict=replay basis=failed-record",
            request.caller_id, version, request.idempotency_key,
        )
        return GatewayResponse(
            ok=False,
            status=status,
            body={"error": {"code": error.code.value, "message": error.message}},
            error=error,
        )

    def _render(self, version: str, canonical: dict[str, Any]) -> dict[str, Any]:
        """Render the canonical business result for a specific API version."""
        renderer = self._renderers.get(version)
        if renderer is None:
            return dict(canonical)
        return renderer(canonical)

    def _execute(
        self,
        request: GatewayRequest,
        version: str,
        handler: DownstreamHandler,
    ) -> tuple[dict[str, Any] | None, GatewayResponse | None]:
        """Run the downstream handler once.

        Returns (canonical result, None) on success, or (None, error response)
        on failure. Internal exception details never leak to the caller.
        """
        try:
            return handler(version, request.payload), None
        except DownstreamTimeout:
            err = GatewayError(
                code=ErrorCode.DOWNSTREAM_TIMEOUT,
                message="Downstream dependency timed out. Safe to retry with the same idempotency key.",
                retry_after_seconds=1.0,
            )
            return None, self._reject(request, err, http_status=504)
        except Exception:
            # Deliberately no exception details leak to the caller.
            logger.exception("downstream failure caller=%s version=%s", request.caller_id, version)
            err = GatewayError(
                code=ErrorCode.DOWNSTREAM_ERROR,
                message="Downstream dependency failed. Safe to retry with the same idempotency key.",
            )
            return None, self._reject(request, err, http_status=502)

    def _invoke(
        self,
        request: GatewayRequest,
        version: str,
        handler: DownstreamHandler,
        deprecated: bool,
    ) -> GatewayResponse:
        canonical, error_response = self._execute(request, version, handler)
        if error_response is not None:
            return error_response
        logger.info(
            "caller=%s version=%s idem=%s verdict=executed basis=downstream-ok",
            request.caller_id, version, request.idempotency_key,
        )
        return GatewayResponse(
            ok=True,
            status=200,
            body=self._render(version, canonical),
            deprecated=deprecated,
        )

    def _reject(self, request: GatewayRequest, error: GatewayError, http_status: int) -> GatewayResponse:
        logger.info(
            "caller=%s version=%s idem=%s verdict=rejected basis=%s",
            request.caller_id, request.version, request.idempotency_key, error.code.value,
        )
        return GatewayResponse(
            ok=False,
            status=http_status,
            body={"error": {"code": error.code.value, "message": error.message}},
            error=error,
        )

    @staticmethod
    def _status_for(error: GatewayError) -> int:
        return {
            ErrorCode.VERSION_MISSING: 400,
            ErrorCode.VERSION_UNKNOWN: 404,
            ErrorCode.VERSION_SUNSET: 410,
        }[error.code]
