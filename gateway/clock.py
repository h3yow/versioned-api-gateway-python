"""Clock abstraction for deterministic time control in tests."""

from __future__ import annotations

import threading
import time
from typing import Protocol


class Clock(Protocol):
    """Monotonic clock used for quota windows and idempotency expiry."""

    def now(self) -> float:
        """Return current time in seconds (monotonic semantics)."""
        ...


class SystemClock:
    """Clock backed by time.monotonic()."""

    def now(self) -> float:
        return time.monotonic()


class FakeClock:
    """Manually advanced clock for deterministic tests."""

    def __init__(self, start: float = 0.0) -> None:
        self._t = start
        self._lock = threading.Lock()

    def now(self) -> float:
        with self._lock:
            return self._t

    def advance(self, seconds: float) -> None:
        with self._lock:
            self._t += seconds
