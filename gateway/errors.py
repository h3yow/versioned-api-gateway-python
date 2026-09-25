"""Unified, distinguishable error results for the gateway."""

from __future__ import annotations

import enum
from dataclasses import dataclass


class ErrorCode(enum.Enum):
    """Stable machine-readable rejection/failure reasons."""

    VERSION_MISSING = "VERSION_MISSING"
    VERSION_UNKNOWN = "VERSION_UNKNOWN"
    VERSION_SUNSET = "VERSION_SUNSET"
    QUOTA_EXCEEDED = "QUOTA_EXCEEDED"
    IDEMPOTENCY_CONFLICT = "IDEMPOTENCY_CONFLICT"
    DOWNSTREAM_ERROR = "DOWNSTREAM_ERROR"
    DOWNSTREAM_TIMEOUT = "DOWNSTREAM_TIMEOUT"


@dataclass(frozen=True)
class GatewayError:
    """An error result safe to expose to external callers (no internals)."""

    code: ErrorCode
    message: str
    retry_after_seconds: float | None = None
