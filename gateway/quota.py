"""Per-caller-tier quota / rate limiting."""

from __future__ import annotations

import enum
import threading
from dataclasses import dataclass

from .clock import Clock
from .errors import GatewayError


class CallerTier(enum.Enum):
    ANONYMOUS = "anonymous"
    STANDARD = "standard"
    PRIVILEGED = "privileged"


@dataclass(frozen=True)
class QuotaDecision:
    allowed: bool
    remaining: int
    retry_after_seconds: float | None


class QuotaLimiter:
    """Fixed-window quota limiter, thread-safe, per (tier, caller) bucket."""

    def __init__(self, clock: Clock, limits: dict[CallerTier, int], window_seconds: float) -> None:
        self._clock = clock
        self._limits = limits
        self._window = window_seconds
        self._lock = threading.Lock()
        self._buckets: dict[tuple[CallerTier, str], tuple[int, int]] = {}

    def check_and_consume(self, tier: CallerTier, caller_id: str) -> QuotaDecision:
        raise NotImplementedError
