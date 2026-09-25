"""Gateway orchestration: version negotiation -> quota -> idempotency -> downstream."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Callable

from .clock import Clock
from .errors import GatewayError
from .idempotency import IdempotencyStore
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


# Downstream handler: (version, payload) -> response body dict.
DownstreamHandler = Callable[[str, dict[str, Any]], dict[str, Any]]


class DownstreamTimeout(Exception):
    """Raised by downstream adapters to signal a timeout."""


class Gateway:
    def __init__(
        self,
        versions: VersionRegistry,
        quota: QuotaLimiter,
        idempotency: IdempotencyStore,
        handlers: dict[str, DownstreamHandler],
        clock: Clock,
    ) -> None:
        self._versions = versions
        self._quota = quota
        self._idempotency = idempotency
        self._handlers = handlers
        self._clock = clock

    def handle(self, request: GatewayRequest) -> GatewayResponse:
        raise NotImplementedError
