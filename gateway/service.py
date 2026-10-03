"""Gateway orchestration: version negotiation -> quota -> idempotency -> downstream.

Cross-version idempotency model:
    The idempotency record stores one *version-neutral business result*
    (business key + canonical entity). Every client declaring the same key +
    same payload converges on that single business result; when a result is
    served back it is rendered through the **requesting** client's version, so
    each caller sees its own declared shape while all views reference the same
    write. A leader's failure converges the same way: the whole concurrent
    batch observes the identical failure code and retry hint, and the
    downstream is triggered at most once per batch.
"""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from typing import Any, Callable

from .clock import Clock
from .errors import ErrorCode, GatewayError
from .idempotency import BeginResult, IdempotencyStore, RecordState, TerminalSnapshot
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


@dataclass(frozen=True)
class _DownstreamOutcome:
    """Raw downstream verdict before version rendering / HTTP wrapping."""

    ok: bool
    error: GatewayError | None = None
    result: Any = None  # canonical WriteResult | raw body dict, only when ok


# Downstream handler: (version, payload) -> canonical WriteResult | raw body dict.
DownstreamHandler = Callable[[str, dict[str, Any]], Any]
# Version renderer: (target version, canonical entity) -> response body dict.
VersionRenderer = Callable[[str, dict[str, Any]], dict[str, Any]]


@dataclass(frozen=True)
class WriteResult:
    """Version-neutral outcome of one downstream write.

    `business_key` is the stable identity of the business record; `entity` is
    the canonical content all version views are rendered from.
    """

    business_key: str
    entity: dict[str, Any]


class DownstreamTimeout(Exception):
    """Raised by downstream adapters to signal a timeout."""


def _fingerprint(payload: dict[str, Any]) -> str:
    """Stable fingerprint of a payload for idempotency conflict detection."""
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _canonical(result: Any) -> tuple[dict[str, Any], str | None]:
    """Split a handler result into (canonical entity, business key)."""
    if isinstance(result, WriteResult):
        return result.entity, result.business_key
    return result, None


class Gateway:
    def __init__(
        self,
        versions: VersionRegistry,
        quota: QuotaLimiter,
        idempotency: IdempotencyStore,
        handlers: dict[str, DownstreamHandler],
        clock: Clock,
        wait_timeout_seconds: float = 10.0,
        renderers: dict[str, VersionRenderer] | None = None,
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

        # 2. Quota: each HTTP call pays one quota unit, including replays.
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
        outcome = self._invoke(request, version, handler, negotiated.deprecated)
        if outcome.ok:
            entity, _ = _canonical(outcome.result)
            return GatewayResponse(
                ok=True,
                status=200,
                body=self._render(version, entity),
                deprecated=negotiated.deprecated,
            )
        return self._reject(
            request, outcome.error, http_status=self._status_for(outcome.error)
        )

    # -- internals ---------------------------------------------------------

    def _handle_idempotent(
        self,
        request: GatewayRequest,
        version: str,
        handler: DownstreamHandler,
        deprecated: bool,
    ) -> GatewayResponse:
        fingerprint = _fingerprint(request.payload)
        outcome = self._idempotency.begin(request.idempotency_key, fingerprint)

        if outcome.kind is BeginResult.CONFLICT:
            return self._reject(request, outcome.error, http_status=409)

        if outcome.kind is BeginResult.REPLAY:
            return self._replay_completed(
                request, version, deprecated, self._idempotency.snapshot(outcome.record)
            )

        if outcome.kind is BeginResult.WAIT:
            record = outcome.record
            settled = record.done.wait(timeout=self._wait_timeout)
            if not settled:
                # Leader still has not returned; do not trigger a second call.
                err = GatewayError(
                    code=ErrorCode.DOWNSTREAM_TIMEOUT,
                    message="Downstream dependency timed out. Safe to retry with the same idempotency key.",
                    retry_after_seconds=1.0,
                )
                return self._reject(request, err, http_status=504)
            snapshot = self._idempotency.snapshot(record)
            if snapshot.state is RecordState.COMPLETED:
                return self._replay_completed(request, version, deprecated, snapshot)
            # The leader of THIS concurrent batch failed: converge on its exact
            # failure instead of being let through to hit downstream again.
            return self._replay_failure(request, version, snapshot)

        # LEADER: the only caller in this batch that touches the downstream.
        leader_record = outcome.record
        outcome = self._invoke(request, version, handler, deprecated)
        if outcome.ok:
            entity, business_key = _canonical(outcome.result)
            self._idempotency.complete(leader_record, entity, business_key)
            logger.info(
                "caller=%s version=%s idem=%s verdict=executed basis=downstream-ok business_key=%s",
                request.caller_id, version, request.idempotency_key, business_key or "-",
            )
            return GatewayResponse(
                ok=True,
                status=200,
                body=self._render(version, entity),
                deprecated=deprecated,
            )
        self._idempotency.fail(leader_record, outcome.error)
        return self._reject(request, outcome.error, http_status=self._status_for(outcome.error))

    def _replay_completed(
        self,
        request: GatewayRequest,
        version: str,
        deprecated: bool,
        snapshot: TerminalSnapshot,
    ) -> GatewayResponse:
        # Render the shared canonical result in THIS caller's declared version.
        body = self._render(version, snapshot.result)
        logger.info(
            "caller=%s version=%s idem=%s verdict=replay basis=completed-record business_key=%s",
            request.caller_id, version, request.idempotency_key, snapshot.business_key or "-",
        )
        return GatewayResponse(
            ok=True,
            status=200,
            body=body,
            deprecated=deprecated,
            replayed=True,
        )

    def _replay_failure(
        self,
        request: GatewayRequest,
        version: str,
        snapshot: TerminalSnapshot,
    ) -> GatewayResponse:
        error = snapshot.error
        logger.info(
            "caller=%s version=%s idem=%s verdict=rejected basis=%s convergence=leader-failure",
            request.caller_id, version, request.idempotency_key, error.code.value,
        )
        return GatewayResponse(
            ok=False,
            status=self._status_for(error),
            body={"error": {"code": error.code.value, "message": error.message}},
            error=error,
        )

    def _render(self, version: str, entity: object) -> dict[str, Any]:
        renderer = self._renderers.get(version)
        if renderer is None:
            # Backward-compatible default: the canonical body is already shaped.
            return dict(entity)  # type: ignore[arg-type]
        return renderer(version, entity)  # type: ignore[arg-type]

    def _invoke(
        self,
        request: GatewayRequest,
        version: str,
        handler: DownstreamHandler,
        deprecated: bool,
    ) -> _DownstreamOutcome:
        """Call the downstream once; return a raw verdict (no rendering)."""
        del deprecated  # presentation is applied by the caller
        try:
            return _DownstreamOutcome(ok=True, result=handler(version, request.payload))
        except DownstreamTimeout:
            return _DownstreamOutcome(
                ok=False,
                error=GatewayError(
                    code=ErrorCode.DOWNSTREAM_TIMEOUT,
                    message="Downstream dependency timed out. Safe to retry with the same idempotency key.",
                    retry_after_seconds=1.0,
                ),
            )
        except Exception:
            # Deliberately no exception details leak to the caller.
            logger.exception("downstream failure caller=%s version=%s", request.caller_id, version)
            return _DownstreamOutcome(
                ok=False,
                error=GatewayError(
                    code=ErrorCode.DOWNSTREAM_ERROR,
                    message="Downstream dependency failed. Safe to retry with the same idempotency key.",
                ),
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
            ErrorCode.QUOTA_EXCEEDED: 429,
            ErrorCode.IDEMPOTENCY_CONFLICT: 409,
            ErrorCode.DOWNSTREAM_ERROR: 502,
            ErrorCode.DOWNSTREAM_TIMEOUT: 504,
        }[error.code]
