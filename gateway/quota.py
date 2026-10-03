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

    Buckets belong to a fixed window: once the clock moves past it they are
    dead state and get reclaimed (opportunistic, time-throttled full sweep plus
    immediate per-key reuse). `max_buckets` is a hard safety valve on resident
    memory for very large caller populations within one window.
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
        self._max = max_buckets
        self._lock = threading.Lock()
        self._buckets: dict[tuple[CallerTier, str], tuple[int, int]] = {}
        self._last_sweep = 0.0

    def __len__(self) -> int:
        with self._lock:
            return len(self._buckets)

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
            self._maybe_sweep(window_idx, now)
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
            if self._max is not None and len(self._buckets) > self._max:
                self._enforce_capacity_locked(window_idx)
            return QuotaDecision(allowed=True, remaining=limit - used, retry_after_seconds=None)

    def purge_stale(self) -> int:
        """Force removal of buckets not belonging to the current window."""
        now = self._clock.now()
        window_idx = int(now // self._window)
        with self._lock:
            return self._purge_stale_locked(window_idx)

    # -- internals ---------------------------------------------------------

    def _maybe_sweep(self, window_idx: int, now: float) -> None:
        # One full sweep per window transition is enough: every bucket of an
        # older window is stale until the caller reuses its key.
        if now - self._last_sweep >= self._window:
            self._purge_stale_locked(window_idx)
            self._last_sweep = now

    def _purge_stale_locked(self, window_idx: int) -> int:
        stale = [key for key, (win, _) in self._buckets.items() if win != window_idx]
        for key in stale:
            del self._buckets[key]
        return len(stale)

    def _enforce_capacity_locked(self, window_idx: int) -> None:
        """Hard safety valve, independent of window rolls.

        Only current-window buckets are reachable here (stale ones are swept
        first). Buckets of callers that have already exhausted their window
        quota are dropped first; in normal operation max_buckets is sized so
        this valve never fires. It exists solely to guarantee a bounded
        resident footprint when distinct-caller traffic exceeds the cap.
        """
        self._purge_stale_locked(window_idx)
        overflow = len(self._buckets) - self._max
        if overflow <= 0:
            return
        candidates = sorted(self._buckets, key=lambda k: self._buckets[k][1], reverse=True)
        for key in candidates[:overflow]:
            del self._buckets[key]
