"""Scenario: long-running service reclaims state with a bounded footprint.

- Expired / long-idle idempotency records are auto-released.
- IN_FLIGHT records and records still inside their validity window are never
  reclaimed, so verdicts never change.
- Past-window quota buckets are released; the current window keeps counting.
- A hard cap (max_records / max_buckets) bounds resident memory regardless of
  the volume of historical identifiers.

All time movement uses the controllable FakeClock; no real sleeps.
"""

from gateway.clock import FakeClock
from gateway.idempotency import IdempotencyStore, RecordState
from gateway.quota import CallerTier, QuotaLimiter


# ---- idempotency store ----------------------------------------------------

def test_completed_record_expires_after_ttl():
    clock = FakeClock(0.0)
    store = IdempotencyStore(clock, ttl_seconds=10.0)

    o = store.begin("k", "fp")
    assert o.kind.name == "LEADER"
    store.complete(o.record, {"x": 1}, business_key="bk")
    assert len(store) == 1

    clock.advance(9.0)
    # Still valid: replay works and the record stays.
    again = store.begin("k", "fp")
    assert again.kind.name == "REPLAY"
    assert len(store) == 1

    clock.advance(11.0)  # 20s since last access, past its slid 10s TTL
    after = store.begin("k", "fp")
    assert after.kind.name == "LEADER"  # expired -> key reusable
    assert len(store) == 1              # old record was replaced, not leaked


def test_in_flight_record_is_never_reclaimed():
    clock = FakeClock(0.0)
    store = IdempotencyStore(clock, ttl_seconds=5.0)

    o = store.begin("k", "fp")
    clock.advance(60.0)  # far beyond any TTL
    reclaimed = store.purge_expired()
    assert reclaimed == 0
    assert len(store) == 1
    assert o.record.state is RecordState.IN_FLIGHT
    # Concurrent peers can still wait on it.
    peer = store.begin("k", "fp")
    assert peer.kind.name == "WAIT"


def test_replay_slides_expiry_for_active_keys():
    clock = FakeClock(0.0)
    store = IdempotencyStore(clock, ttl_seconds=10.0)
    o = store.begin("k", "fp")
    store.complete(o.record, {"x": 1})

    clock.advance(9.0)
    assert store.begin("k", "fp").kind.name == "REPLAY"  # touch -> slides
    clock.advance(9.0)
    assert store.begin("k", "fp").kind.name == "REPLAY"  # still protected
    clock.advance(11.0)
    assert store.begin("k", "fp").kind.name == "LEADER"


def test_eviction_does_not_change_verdict_within_window():
    clock = FakeClock(0.0)
    store = IdempotencyStore(clock, ttl_seconds=100.0)
    for i in range(50):
        o = store.begin(f"k{i}", "fp")
        store.complete(o.record, {"i": i}, business_key=f"bk{i}")
    assert len(store) == 50
    # Clock inside TTL: nothing is reclaimed, all replays still resolve.
    clock.advance(1.0)
    assert store.purge_expired() == 0
    for i in range(50):
        assert store.begin(f"k{i}", "fp").kind.name == "REPLAY"
    # After TTL everything is reclaimable.
    clock.advance(200.0)
    store.purge_expired()
    assert len(store) == 0


def test_max_records_hard_cap_bounds_resident_size():
    clock = FakeClock(0.0)
    store = IdempotencyStore(clock, ttl_seconds=10.0, max_records=5)
    for i in range(5):
        o = store.begin(f"k{i}", "fp")
        store.complete(o.record, {"i": i})
    assert len(store) == 5
    # Expired records get reclaimed first, no safety-valve eviction needed.
    clock.advance(20.0)
    o = store.begin("k-new", "fp")
    store.complete(o.record, {"i": 99})
    assert len(store) <= 5
    assert "k-new" in store._records

    # Safety valve: all records live, cap still holds resident memory.
    fresh_clock = FakeClock(0.0)
    capped = IdempotencyStore(fresh_clock, ttl_seconds=1000.0, max_records=5)
    for i in range(12):
        o = capped.begin(f"j{i}", "fp")
        capped.complete(o.record, {"i": i})
    assert len(capped) == 5


def test_idle_terminal_records_are_reclaimed_but_busy_ones_kept():
    clock = FakeClock(0.0)
    store = IdempotencyStore(clock, ttl_seconds=10.0)
    hot = store.begin("hot", "fp")
    store.complete(hot.record, {"v": 1})
    cold = store.begin("cold", "fp")
    store.complete(cold.record, {"v": 2})

    clock.advance(9.0)
    store.begin("hot", "fp")  # hot key still used -> slides
    clock.advance(9.0)        # cold is 18s idle, hot only 9s
    reclaimed = store.purge_expired()
    assert reclaimed == 1
    assert "cold" not in store._records
    assert store.begin("hot", "fp").kind.name == "REPLAY"


# ---- quota limiter --------------------------------------------------------

def _limiter(clock, limit=3, window=60.0, max_buckets=None):
    return QuotaLimiter(
        clock,
        {CallerTier.STANDARD: limit},
        window_seconds=window,
        max_buckets=max_buckets,
    )


def test_quota_buckets_released_after_window_rolls():
    clock = FakeClock(1_000.0)
    limiter = _limiter(clock)
    for i in range(20):
        limiter.check_and_consume(CallerTier.STANDARD, f"caller-{i}")
    assert len(limiter) == 20

    clock.advance(61.0)  # into the next fixed window
    # Any traffic triggers the throttled sweep of previous-window buckets.
    limiter.check_and_consume(CallerTier.STANDARD, "caller-new")
    assert len(limiter) == 1
    assert limiter.check_and_consume(CallerTier.STANDARD, "caller-new").allowed


def test_sweep_keeps_current_window_decisions_intact():
    clock = FakeClock(1_000.0)
    limiter = _limiter(clock, limit=2)
    assert limiter.check_and_consume(CallerTier.STANDARD, "a").allowed
    clock.advance(19.0)  # still inside window 16 (1000..1019.999...)
    assert len(limiter) == 1
    assert limiter.check_and_consume(CallerTier.STANDARD, "a").allowed
    third = limiter.check_and_consume(CallerTier.STANDARD, "a")
    assert not third.allowed and third.retry_after_seconds == 1.0


def test_max_buckets_caps_resident_buckets():
    clock = FakeClock(1_000.0)
    limiter = _limiter(clock, limit=1000, max_buckets=10)
    for i in range(100):
        limiter.check_and_consume(CallerTier.STANDARD, f"caller-{i}")
    assert len(limiter) == 10
