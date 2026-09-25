"""Idempotency store with concurrent de-duplication for write requests."""

from __future__ import annotations

import enum
import threading
from dataclasses import dataclass, field

from .clock import Clock
from .errors import GatewayError


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
    """Thread-safe in-memory idempotency store with TTL."""

    def __init__(self, clock: Clock, ttl_seconds: float) -> None:
        self._clock = clock
        self._ttl = ttl_seconds
        self._lock = threading.Lock()
        self._records: dict[str, IdempotencyRecord] = {}

    def begin(self, key: str, payload_fingerprint: str) -> BeginOutcome:
        raise NotImplementedError

    def complete(self, record: IdempotencyRecord, result: object) -> None:
        raise NotImplementedError

    def fail(self, record: IdempotencyRecord, error: GatewayError) -> None:
        raise NotImplementedError
