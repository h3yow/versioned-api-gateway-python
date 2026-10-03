"""Idempotency store with concurrent de-duplication for write requests."""

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
    result: object | None = None
    error: GatewayError | None = None
    created_at: float = 0.0
    expires_at: float = 0.0
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


class IdempotencyStore:
    """Thread-safe in-memory idempotency store with TTL and bounded capacity.

    Reclamation policy (never changes decisions returned to callers):
    - terminal records (COMPLETED/FAILED) are dropped once expired (TTL);
    - IN_FLIGHT records are never evicted while plausibly active; only when
      `inflight_timeout_seconds` is set and the record is older than that
      (leader crashed/abandoned) may it be reclaimed;
    - `max_records` bounds resident memory: on overflow the store sweeps
      reclaimable records. Records still valid for de-duplication are kept
      even beyond the cap (the cap then degrades to the live working set).
    """

    def __init__(
        self,
        clock: Clock,
        ttl_seconds: float,
        max_records: int | None = None,
        inflight_timeout_seconds: float | None = None,
    ) -> None:
        self._clock = clock
        self._ttl = ttl_seconds
        self._max_records = max_records
        self._inflight_timeout = inflight_timeout_seconds
        self._lock = threading.Lock()
        self._records: dict[str, IdempotencyRecord] = {}

    def __len__(self) -> int:
        with self._lock:
            return len(self._records)

    def sweep(self) -> int:
        """Drop reclaimable records; returns how many were removed."""
        now = self._clock.now()
        with self._lock:
            return self._sweep_locked(now)

    def _sweep_locked(self, now: float) -> int:
        doomed = [
            key
            for key, record in self._records.items()
            if self._reclaimable(record, now)
        ]
        for key in doomed:
            del self._records[key]
        return len(doomed)

    def _reclaimable(self, record: IdempotencyRecord, now: float) -> bool:
        if record.state is RecordState.IN_FLIGHT:
            # Still being processed: only reclaim if abandoned past the
            # configured in-flight timeout.
            return (
                self._inflight_timeout is not None
                and now - record.created_at >= self._inflight_timeout
            )
        return now >= record.expires_at

    def begin(self, key: str, payload_fingerprint: str) -> BeginOutcome:
        """Register intent to execute work for `key`.

        Exactly one concurrent caller becomes LEADER; others either REPLAY a
        completed result, WAIT for the in-flight leader, or get CONFLICT when
        the same key carries a different payload fingerprint.
        """
        now = self._clock.now()
        with self._lock:
            if self._max_records is not None and len(self._records) >= self._max_records:
                # Housekeeping under memory pressure: reclaim expired/abandoned
                # records before deciding. Valid records are never evicted.
                self._sweep_locked(now)

            record = self._records.get(key)
            if record is not None and self._reclaimable(record, now):
                # Expired terminal record (or abandoned in-flight one): drop it
                # so the key can be reused.
                del self._records[key]
                record = None

            if record is None:
                record = IdempotencyRecord(
                    key=key,
                    payload_fingerprint=payload_fingerprint,
                    state=RecordState.IN_FLIGHT,
                    created_at=now,
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

            if record.state is RecordState.IN_FLIGHT:
                return BeginOutcome(kind=BeginResult.WAIT, record=record)
            if record.state is RecordState.COMPLETED:
                return BeginOutcome(kind=BeginResult.REPLAY, record=record)
            # FAILED: the previous attempt did not produce a result; allow this
            # caller to take over as the new leader with a fresh record.
            fresh = IdempotencyRecord(
                key=key,
                payload_fingerprint=payload_fingerprint,
                state=RecordState.IN_FLIGHT,
                created_at=now,
            )
            self._records[key] = fresh
            return BeginOutcome(kind=BeginResult.LEADER, record=fresh)

    def complete(self, record: IdempotencyRecord, result: object) -> None:
        with self._lock:
            record.state = RecordState.COMPLETED
            record.result = result
            record.expires_at = self._clock.now() + self._ttl
            record.done.set()

    def fail(self, record: IdempotencyRecord, error: GatewayError) -> None:
        with self._lock:
            record.state = RecordState.FAILED
            record.error = error
            record.expires_at = self._clock.now() + self._ttl
            record.done.set()
