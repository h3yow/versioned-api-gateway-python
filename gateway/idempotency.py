"""Idempotency store with concurrent de-duplication for write requests.

Retention / capacity:
    - Terminal records (completed/failed) expire `ttl_seconds` after their last
      access and are then safe to reclaim; replaying a record slides its expiry.
    - IN_FLIGHT records are never reclaimed while a downstream call is running.
    - Expired records are swept opportunistically (time-throttled) and an
      optional `max_records` provides a hard resident-memory safety valve.
"""

from __future__ import annotations

import enum
import threading
from dataclasses import dataclass, field

from .clock import Clock
from .errors import ErrorCode, GatewayError


class RecordState(enum.Enum):
    IN_FLIGHT = "in_flight"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class IdempotencyRecord:
    key: str
    payload_fingerprint: str
    state: RecordState
    business_key: str | None = None
    result: object | None = None
    error: GatewayError | None = None
    expires_at: float = 0.0
    last_accessed: float = 0.0
    done: threading.Event = field(default_factory=threading.Event)


class BeginResult(enum.Enum):
    LEADER = "leader"      # caller should execute the downstream work
    REPLAY = "replay"      # a completed result already exists
    CONFLICT = "conflict"  # same key, different payload
    WAIT = "wait"          # another request with same key is in flight


@dataclass(frozen=True)
class BeginOutcome:
    kind: BeginResult
    record: IdempotencyRecord | None = None
    error: GatewayError | None = None


@dataclass(frozen=True)
class TerminalSnapshot:
    """Immutable view of a record after its leader has settled."""

    state: RecordState
    business_key: str | None
    result: object | None
    error: GatewayError | None


class IdempotencyStore:
    """Thread-safe in-memory idempotency store with TTL and bounded size."""

    def __init__(
        self,
        clock: Clock,
        ttl_seconds: float,
        max_records: int | None = None,
    ) -> None:
        self._clock = clock
        self._ttl = ttl_seconds
        self._max = max_records
        # Expensive full sweeps run at most once per sweep interval of clock
        # time; per-key lazy expiry on every begin() stays immediate.
        self._sweep_interval = max(ttl_seconds / 10.0, 1e-6)
        self._lock = threading.Lock()
        self._records: dict[str, IdempotencyRecord] = {}
        self._last_sweep = 0.0

    def __len__(self) -> int:
        with self._lock:
            return len(self._records)

    def begin(self, key: str, payload_fingerprint: str) -> BeginOutcome:
        """Register intent to execute work for `key`.

        Exactly one concurrent caller becomes LEADER; others either REPLAY a
        completed result, WAIT for the in-flight leader, or get CONFLICT when
        the same key carries a different payload fingerprint.
        """
        now = self._clock.now()
        with self._lock:
            self._maybe_sweep(now)
            record = self._records.get(key)
            if record is not None and record.state is not RecordState.IN_FLIGHT and now >= record.expires_at:
                # Expired terminal record: drop it so the key can be reused.
                del self._records[key]
                record = None

            if record is None:
                self._enforce_capacity_locked(now)
                record = IdempotencyRecord(
                    key=key,
                    payload_fingerprint=payload_fingerprint,
                    state=RecordState.IN_FLIGHT,
                    last_accessed=now,
                )
                self._records[key] = record
                return BeginOutcome(kind=BeginResult.LEADER, record=record)

            if record.payload_fingerprint != payload_fingerprint:
                return BeginOutcome(
                    kind=BeginResult.CONFLICT,
                    error=GatewayError(
                        code=ErrorCode.IDEMPOTENCY_CONFLICT,
                        message=(
                            f"Idempotency key '{key}' was already used with a "
                            "different payload; refusing to overwrite."
                        ),
                    ),
                )

            record.last_accessed = now
            if record.state is RecordState.IN_FLIGHT:
                return BeginOutcome(kind=BeginResult.WAIT, record=record)
            if record.state is RecordState.COMPLETED:
                # Sliding expiry: a key callers still rely on for de-dup stays.
                record.expires_at = max(record.expires_at, now + self._ttl)
                return BeginOutcome(kind=BeginResult.REPLAY, record=record)
            # FAILED before this caller arrived: this is a fresh retry, not a
            # member of the failed concurrent batch; let it take over.
            fresh = IdempotencyRecord(
                key=key,
                payload_fingerprint=payload_fingerprint,
                state=RecordState.IN_FLIGHT,
                last_accessed=now,
            )
            self._records[key] = fresh
            return BeginOutcome(kind=BeginResult.LEADER, record=fresh)

    def complete(
        self,
        record: IdempotencyRecord,
        result: object,
        business_key: str | None = None,
    ) -> None:
        with self._lock:
            record.state = RecordState.COMPLETED
            record.business_key = business_key
            record.result = result
            record.error = None
            now = self._clock.now()
            record.last_accessed = now
            record.expires_at = now + self._ttl
            record.done.set()

    def fail(self, record: IdempotencyRecord, error: GatewayError) -> None:
        with self._lock:
            record.state = RecordState.FAILED
            record.error = error
            now = self._clock.now()
            record.last_accessed = now
            record.expires_at = now + self._ttl
            record.done.set()

    def snapshot(self, record: IdempotencyRecord) -> TerminalSnapshot:
        """Read a record's settled outcome (call after its ``done`` event)."""
        with self._lock:
            return TerminalSnapshot(
                state=record.state,
                business_key=record.business_key,
                result=record.result,
                error=record.error,
            )

    def purge_expired(self) -> int:
        """Force a sweep; return the number of reclaimed records."""
        with self._lock:
            return self._purge_expired_locked(self._clock.now())

    # -- internals ---------------------------------------------------------

    def _maybe_sweep(self, now: float) -> None:
        if now - self._last_sweep >= self._sweep_interval:
            self._purge_expired_locked(now)
            self._last_sweep = now

    def _purge_expired_locked(self, now: float) -> int:
        # Only terminal records past their expiry are reclaimable. IN_FLIGHT
        # records represent a live downstream call and are always protected.
        stale = [
            key
            for key, rec in self._records.items()
            if rec.state is not RecordState.IN_FLIGHT and now >= rec.expires_at
        ]
        for key in stale:
            del self._records[key]
        return len(stale)

    def _enforce_capacity_locked(self, now: float) -> None:
        if self._max is None or len(self._records) < self._max:
            return
        self._purge_expired_locked(now)
        # Safety valve: drop the terminal record closest to its expiry. It is
        # only reached when live (within-TTL) records exceed max_records; size
        # max_records so this never fires in normal operation.
        while len(self._records) >= self._max:
            victim_key = min(
                (
                    key
                    for key, rec in self._records.items()
                    if rec.state is not RecordState.IN_FLIGHT
                ),
                key=lambda k: self._records[k].expires_at,
                default=None,
            )
            if victim_key is None:
                # All records are in-flight; their count is bounded by request
                # concurrency, so allow the insertion rather than block writes.
                return
            del self._records[victim_key]
