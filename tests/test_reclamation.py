"""Scenario: long-running service reclaims idempotency/quota state.

Expired or abandoned records are released automatically so resident memory
stays bounded, while in-flight and still-valid records are never evicted and
caller-visible decisions never change. All time control uses FakeClock.
"""

from gateway.clock import FakeClock
from gateway.idempotency import BeginResult, IdempotencyStore, RecordState
from gateway.quota import CallerTier, QuotaLimiter

TIER = CallerTier.STANDARD


def complete(store, key, fp="fp"):
    outcome = store.begin(key, fp)
    assert outcome.kind is BeginResult.LEADER
    store.complete(outcome.record, {"ok": True})
    return outcome.record


class TestIdempotencyReclamation:
    def test_expired_terminal_records_are_swept(self):
        clock = FakeClock(start=1_000.0)
        store = IdempotencyStore(clock, ttl_seconds=60.0)
        for i in range(5):
            complete(store, f"k{i}")
        assert len(store) == 5

        clock.advance(61.0)
        removed = store.sweep()
        assert removed == 5
        assert len(store) == 0

    def test_valid_records_are_never_evicted(self):
        clock = FakeClock(start=1_000.0)
        store = IdempotencyStore(clock, ttl_seconds=60.0, max_records=2)
        for i in range(3):
            complete(store, f"k{i}")
        # Over the cap, but every record is still valid for de-duplication:
        # nothing may be reclaimed and decisions must not change.
        assert len(store) == 3
        outcome = store.begin("k0", "fp")
        assert outcome.kind is BeginResult.REPLAY

    def test_in_flight_record_is_not_swept(self):
        clock = FakeClock(start=1_000.0)
        store = IdempotencyStore(clock, ttl_seconds=60.0, inflight_timeout_seconds=300.0)
        outcome = store.begin("k-live", "fp")
        assert outcome.kind is BeginResult.LEADER

        clock.advance(120.0)  # past any terminal TTL, still within in-flight budget
        assert store.sweep() == 0
        assert len(store) == 1
        assert outcome.record.state is RecordState.IN_FLIGHT

    def test_abandoned_in_flight_record_is_reclaimed(self):
        clock = FakeClock(start=1_000.0)
        store = IdempotencyStore(clock, ttl_seconds=60.0, inflight_timeout_seconds=300.0)
        store.begin("k-stuck", "fp")

        clock.advance(301.0)  # leader presumed crashed
        assert store.sweep() == 1
        assert len(store) == 0
        # Key is reusable afterwards.
        assert store.begin("k-stuck", "fp").kind is BeginResult.LEADER

    def test_sweep_does_not_change_caller_visible_decisions(self):
        clock = FakeClock(start=1_000.0)
        store = IdempotencyStore(clock, ttl_seconds=60.0)
        complete(store, "k1")
        clock.advance(10.0)
        store.sweep()  # nothing expired yet
        assert store.begin("k1", "fp").kind is BeginResult.REPLAY

        clock.advance(51.0)  # now past TTL
        store.sweep()
        assert store.begin("k1", "fp").kind is BeginResult.LEADER

    def test_resident_memory_stays_bounded_over_long_run(self):
        clock = FakeClock(start=0.0)
        store = IdempotencyStore(clock, ttl_seconds=60.0, max_records=100)
        for i in range(1_000):
            complete(store, f"k{i}")
            clock.advance(1.0)  # simulate a long-running service
            # Bounded by the cap plus the not-yet-expired working set.
            assert len(store) <= 100
        clock.advance(61.0)
        store.sweep()
        assert len(store) == 0


class TestQuotaReclamation:
    def test_stale_window_buckets_are_swept(self):
        clock = FakeClock(start=1_000.0)
        quota = QuotaLimiter(clock, {TIER: 5}, window_seconds=60.0)
        for i in range(10):
            quota.check_and_consume(TIER, f"caller-{i}")
        assert len(quota) == 10

        clock.advance(60.0)  # next window
        removed = quota.sweep()
        assert removed == 10
        assert len(quota) == 0

    def test_current_window_buckets_are_kept(self):
        clock = FakeClock(start=1_000.0)
        quota = QuotaLimiter(clock, {TIER: 5}, window_seconds=60.0)
        quota.check_and_consume(TIER, "active")
        assert quota.sweep() == 0
        assert len(quota) == 1

    def test_sweep_does_not_change_quota_decisions(self):
        clock = FakeClock(start=1_000.0)
        quota = QuotaLimiter(clock, {TIER: 2}, window_seconds=60.0)
        assert quota.check_and_consume(TIER, "c").allowed
        assert quota.check_and_consume(TIER, "c").allowed
        assert not quota.check_and_consume(TIER, "c").allowed  # window exhausted

        clock.advance(60.0)
        quota.sweep()  # drop the stale bucket
        decision = quota.check_and_consume(TIER, "c")
        assert decision.allowed  # fresh window, full quota again

    def test_bucket_count_bounded_under_pressure(self):
        clock = FakeClock(start=1_000.0)
        quota = QuotaLimiter(clock, {TIER: 100}, window_seconds=60.0, max_buckets=50)
        for i in range(500):
            quota.check_and_consume(TIER, f"caller-{i}")
            clock.advance(1.0)
            # Stale buckets are reclaimed once the cap is reached; the count
            # never exceeds the cap plus the current window's active callers.
            assert len(quota) <= 60
