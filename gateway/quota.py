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
        """Atomically check the caller's window quota and consume one unit.

        The check and the increment happen under a single lock so concurrent
        requests can neither under-count (lost update) nor over-count.
        """
        limit = self._limits[tier]
        now = self._clock.now()
        window_idx = int(now // self._window)
        key = (tier, caller_id)
        with self._lock:
            cur_window, used = self._buckets.get(key, (window_idx, 0))
            if cur_window != window_idx:
                cur_window, used = window_idx, 0
            if used >= limit:
                retry_after = (cur_window + 1) * self._window - now
                return QuotaDecision(
                    allowed=False,
                    remaining=0,
                    retry_after_seconds=max(retry_after, 0.0),
                )
            used += 1
            self._buckets[key] = (cur_window, used)
            return QuotaDecision(allowed=True, remaining=limit - used, retry_after_seconds=None)
