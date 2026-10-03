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
    """Fixed-window quota limiter, thread-safe, per (tier, caller) bucket.

    Reclamation: buckets from past windows are decision-neutral (a stale
    bucket would be reset on next access anyway), so they are safe to evict.
    When `max_buckets` is set, stale buckets are swept once the bucket count
    reaches the cap, bounding resident memory by the number of callers active
    in the current window. `sweep()` is also exposed for periodic/external
    housekeeping.
    """

    def __init__(
        self,
        clock: Clock,
        limits: dict[CallerTier, int],
        window_seconds: float,
        max_buckets: int | None = None,
    ) -> None:
        self._clock = clock
        self._limits = limits
        self._window = window_seconds
        self._max_buckets = max_buckets
        self._lock = threading.Lock()
        self._buckets: dict[tuple[CallerTier, str], tuple[int, int]] = {}

    def __len__(self) -> int:
        with self._lock:
            return len(self._buckets)

    def sweep(self) -> int:
        """Drop buckets from past windows; returns how many were removed."""
        now = self._clock.now()
        with self._lock:
            return self._sweep_locked(now)

    def _sweep_locked(self, now: float) -> int:
        window_idx = int(now // self._window)
        doomed = [key for key, (bucket_window, _) in self._buckets.items() if bucket_window != window_idx]
        for key in doomed:
            del self._buckets[key]
        return len(doomed)

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
            if self._max_buckets is not None and len(self._buckets) >= self._max_buckets:
                # Memory pressure: reclaim stale-window buckets first. Only
                # current-window buckets (this window's active callers) remain.
                self._sweep_locked(now)
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
