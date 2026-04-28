"""
Unit tests for the sharding / routing logic in producer.py.

These tests require NO running services — no Redis, no Postgres.
They validate the mathematical properties of get_shard() in isolation.

Industry pattern: test pure functions exhaustively in unit tests so that
integration tests can focus on I/O behaviour rather than routing correctness.
"""

import hashlib
import pytest

# Import the function under test directly.
# Keeping it importable (not buried in if __name__ == "__main__") is itself
# a testability best practice.
from producer import get_shard, NUM_SHARDS


class TestShardRange:
    """get_shard() must always return a valid shard index."""

    def test_result_within_range(self):
        # Boundary check: shard index must be 0 … NUM_SHARDS-1 inclusive.
        for i in range(1000):
            shard = get_shard(f"ORD-{i:06d}")
            assert 0 <= shard < NUM_SHARDS, (
                f"shard {shard} out of range [0, {NUM_SHARDS}) for ORD-{i:06d}"
            )

    def test_single_shard_always_zero(self, monkeypatch):
        """With one shard, every order routes to shard 0."""
        # monkeypatch temporarily overrides a module-level name for this test
        # only — it is restored automatically when the test finishes.
        monkeypatch.setattr("producer.NUM_SHARDS", 1)
        for order_id in ["ORD-A", "ORD-B", "ORD-C", "ORD-99999"]:
            assert get_shard(order_id) == 0


class TestShardDeterminism:
    """Same order_id must always map to the same shard."""

    def test_same_id_same_shard_repeated_calls(self):
        # Call 1000 times — must be identical every time.
        order_id = "ORD-STABLE-001"
        first = get_shard(order_id)
        for _ in range(999):
            assert get_shard(order_id) == first

    def test_different_ids_may_differ(self):
        # Sanity check: the function is not a constant.
        # With 4 shards and SHA-1, the chance all 100 IDs land on the same
        # shard by coincidence is (1/4)^99 ≈ 0 — safe to assert.
        shards = {get_shard(f"ORD-{i}") for i in range(100)}
        assert len(shards) > 1, "get_shard appears to return a constant — routing is broken"

    def test_stable_hash_not_pythons_builtin(self):
        """
        Confirm the implementation matches hashlib.sha1, not hash().

        Python's hash() is randomised per-process (PYTHONHASHSEED).
        This test would be flaky if the code used hash() because the expected
        value would differ between test runs.
        """
        order_id = "ORD-HASH-CHECK"
        expected = int(hashlib.sha1(order_id.encode()).hexdigest(), 16) % NUM_SHARDS
        assert get_shard(order_id) == expected


class TestShardDistribution:
    """Orders should spread roughly evenly across shards."""

    def test_all_shards_used_across_large_sample(self, monkeypatch):
        """
        With 10,000 orders, every shard should receive at least some messages.

        A good hash function distributes keys uniformly. If any shard gets zero
        messages it means either the sample is too small or the hash is biased.
        """
        monkeypatch.setattr("producer.NUM_SHARDS", 4)
        counts = [0] * 4
        for i in range(10_000):
            counts[get_shard(f"ORD-{i}")] += 1
        assert all(c > 0 for c in counts), f"Uneven distribution: {counts}"

    def test_distribution_within_40_percent_of_ideal(self, monkeypatch):
        """
        Each shard should receive between 60 % and 140 % of the ideal share.

        Ideal share = total / num_shards.  A ±40 % tolerance is generous —
        a good hash typically stays within ±5 % for n >= 10,000.
        """
        monkeypatch.setattr("producer.NUM_SHARDS", 4)
        n = 10_000
        counts = [0] * 4
        for i in range(n):
            counts[get_shard(f"ORD-{i}")] += 1

        ideal = n / 4
        for shard, count in enumerate(counts):
            assert 0.60 * ideal <= count <= 1.40 * ideal, (
                f"Shard {shard} got {count} orders; expected ~{ideal:.0f} (±40%)"
            )
