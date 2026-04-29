"""
Unit tests for the Ack Aggregator — consumer-side buffering and
producer-side /ack/status + /ack/summary endpoints.

The Ack Aggregator solves a fundamental async pipeline problem:
  POST /orders returns in <1 ms (Redis write confirmed).
  The consumer may persist the row to Postgres seconds later.
  Callers need a way to confirm end-to-end completion.

Consumer side:
  After each successful DB commit, order_ids are buffered per shard.
  When ACK_BATCH_SIZE is reached or ACK_FLUSH_INTERVAL elapses, the
  buffer is flushed to a Redis Hash orders:acks:{shard} via HSET.

Producer side:
  GET /ack/status?ids=…   queries HMGET on the correct shard per order_id.
  GET /ack/summary         queries HLEN on every shard's ack hash.

Tests cover:
  1. Buffer populated after DB success.
  2. hset called when buffer reaches ACK_BATCH_SIZE.
  3. hset called when ACK_FLUSH_INTERVAL elapses (time-based flush).
  4. Buffer stays empty when DB insert fails (no ack for unwritten orders).
  5. hset not called when ack buffer is below threshold and interval not elapsed.
  6. /ack/status returns "acked" for orders present in the hash.
  7. /ack/status returns "pending" for orders absent from the hash.
  8. /ack/status groups ids by shard (one HMGET per shard, not per id).
  9. /ack/status rejects requests over ACK_MAX_IDS limit.
  10. /ack/status returns 400 when ids param is missing or empty.
  11. /ack/summary returns total acked count and per-shard breakdown.
"""

import json
import time
from unittest.mock import AsyncMock, MagicMock, patch, call
import pytest
from httpx import AsyncClient, ASGITransport

import consumer as cons_module
from consumer import flush_batch, _ack_flush_shard


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_entries(orders: list[dict], base_msg_id: str = "1714") -> list[tuple[str, dict]]:
    """Build flush_batch entries from a list of order dicts."""
    return [
        (f"{base_msg_id}-{i}", {"data": json.dumps(o), "type": "order_created"})
        for i, o in enumerate(orders)
    ]


# ---------------------------------------------------------------------------
# Consumer ack buffer tests
# ---------------------------------------------------------------------------

class TestAckBuffer:

    @pytest.mark.asyncio
    async def test_buffer_populated_after_db_success(
        self, mock_db_pool, mock_redis, sample_order
    ):
        """
        After a successful DB commit, the order_id must appear in
        _ack_buffers[shard_idx] — ready for the next flush.

        We set ACK_BATCH_SIZE=9999 and stamp _ack_last_flush[0] to now
        so neither the size nor the time threshold triggers a flush.
        """
        pool, _ = mock_db_pool
        entries = _make_entries([sample_order])

        cons_module._ack_last_flush[0] = time.monotonic()  # suppress time-based flush

        with patch.object(cons_module, "ACK_BATCH_SIZE", 9999), \
             patch.object(cons_module, "ACK_FLUSH_INTERVAL", 9999.0):
            await flush_batch(pool, mock_redis, "orders:stream:0", 0, entries)

        buf = cons_module._ack_buffers.get(0, [])
        assert sample_order["order_id"] in buf

    @pytest.mark.asyncio
    async def test_hset_called_when_batch_size_reached(
        self, mock_db_pool, mock_redis, sample_order
    ):
        """
        When buffer length reaches ACK_BATCH_SIZE, hset must be called
        immediately and the buffer must be cleared afterward.

        ACK_BATCH_SIZE=1 means every single successful insert triggers a flush.
        """
        pool, _ = mock_db_pool
        entries = _make_entries([sample_order])

        with patch.object(cons_module, "ACK_BATCH_SIZE", 1), \
             patch.object(cons_module, "ACK_FLUSH_INTERVAL", 9999.0):
            await flush_batch(pool, mock_redis, "orders:stream:0", 0, entries)

        mock_redis.hset.assert_called_once()
        call_kwargs = mock_redis.hset.call_args
        mapping = call_kwargs[1]["mapping"]
        assert sample_order["order_id"] in mapping

        # Buffer must be cleared after flush
        assert cons_module._ack_buffers.get(0, []) == []

    @pytest.mark.asyncio
    async def test_hset_called_by_interval(
        self, mock_db_pool, mock_redis, sample_order
    ):
        """
        When ACK_FLUSH_INTERVAL has elapsed (regardless of buffer size),
        hset is called.  We simulate elapsed time by setting _ack_last_flush
        to a value far in the past.
        """
        pool, _ = mock_db_pool
        entries = _make_entries([sample_order])

        cons_module._ack_last_flush[0] = 0.0   # pretend last flush was at t=0

        with patch.object(cons_module, "ACK_BATCH_SIZE", 9999), \
             patch.object(cons_module, "ACK_FLUSH_INTERVAL", 0.0):
            await flush_batch(pool, mock_redis, "orders:stream:0", 0, entries)

        mock_redis.hset.assert_called_once()

    @pytest.mark.asyncio
    async def test_buffer_not_flushed_below_threshold(
        self, mock_db_pool, mock_redis, sample_order
    ):
        """
        When buffer < ACK_BATCH_SIZE AND interval has not elapsed,
        hset must NOT be called — accumulation is still ongoing.
        """
        pool, _ = mock_db_pool
        entries = _make_entries([sample_order])

        cons_module._ack_last_flush[0] = time.monotonic()  # just flushed

        with patch.object(cons_module, "ACK_BATCH_SIZE", 9999), \
             patch.object(cons_module, "ACK_FLUSH_INTERVAL", 9999.0):
            await flush_batch(pool, mock_redis, "orders:stream:0", 0, entries)

        mock_redis.hset.assert_not_called()

    @pytest.mark.asyncio
    async def test_buffer_not_populated_when_db_fails(
        self, mock_db_pool, mock_redis, sample_stream_fields
    ):
        """
        If executemany raises, the batch returns early — no order_ids must
        be added to the ack buffer.  Unwritten orders must never appear as
        acked in the Redis hash.
        """
        pool, conn = mock_db_pool
        conn.executemany.side_effect = Exception("Postgres down")

        await flush_batch(pool, mock_redis, "orders:stream:0", 0,
                          [("id-1", sample_stream_fields)])

        buf = cons_module._ack_buffers.get(0, [])
        assert buf == []
        mock_redis.hset.assert_not_called()

    @pytest.mark.asyncio
    async def test_hset_written_to_correct_shard_key(
        self, mock_db_pool, mock_redis, sample_order
    ):
        """
        The ack hash key must include the shard index so each shard's
        acks live on the correct Redis instance.
        """
        pool, _ = mock_db_pool

        with patch.object(cons_module, "ACK_BATCH_SIZE", 1), \
             patch.object(cons_module, "ACK_FLUSH_INTERVAL", 9999.0):
            await flush_batch(pool, mock_redis, "orders:stream:2", 2,
                              _make_entries([sample_order]))

        key = mock_redis.hset.call_args[0][0]
        assert key == "orders:acks:2"

    @pytest.mark.asyncio
    async def test_expire_set_on_ack_key(
        self, mock_db_pool, mock_redis, sample_order
    ):
        """
        After hset, expire must be called on the same key so the ack hash
        is auto-cleaned after ACK_KEY_TTL seconds — no manual maintenance needed.
        """
        pool, _ = mock_db_pool

        with patch.object(cons_module, "ACK_BATCH_SIZE", 1), \
             patch.object(cons_module, "ACK_FLUSH_INTERVAL", 9999.0):
            await flush_batch(pool, mock_redis, "orders:stream:0", 0,
                              _make_entries([sample_order]))

        mock_redis.expire.assert_called_once()
        expire_key = mock_redis.expire.call_args[0][0]
        assert expire_key == "orders:acks:0"

    @pytest.mark.asyncio
    async def test_multiple_orders_all_in_ack_mapping(
        self, mock_db_pool, mock_redis
    ):
        """
        All order_ids in a batch must appear in the mapping passed to hset,
        not just the first one.  One HSET with N fields saves N-1 round trips.
        """
        pool, _ = mock_db_pool
        orders = [
            {"order_id": f"ORD-{i}", "customer_id": "C1",
             "amount": 9.99, "items": ["X"]}
            for i in range(5)
        ]

        with patch.object(cons_module, "ACK_BATCH_SIZE", 1), \
             patch.object(cons_module, "ACK_FLUSH_INTERVAL", 9999.0):
            await flush_batch(pool, mock_redis, "orders:stream:0", 0,
                              _make_entries(orders))

        mapping = mock_redis.hset.call_args[1]["mapping"]
        for o in orders:
            assert o["order_id"] in mapping, f"{o['order_id']} missing from ack mapping"


# ---------------------------------------------------------------------------
# Heartbeat / idle-stream flush tests
# ---------------------------------------------------------------------------

class TestAckHeartbeat:
    """
    The heartbeat fixes a specific bug: when the Redis stream drains after a
    load test, xreadgroup returns empty and flush_batch is never called again.
    Any orders in a partial last batch (< ACK_BATCH_SIZE) would stay pending
    forever.  _ack_flush_shard() is the extracted flush logic called by both
    flush_batch (threshold-triggered) and _ack_heartbeat (timer-triggered).
    """

    @pytest.mark.asyncio
    async def test_flush_shard_writes_residual_buffer(self, mock_redis):
        """
        Pre-populate buffer below ACK_BATCH_SIZE (simulating stream idle after
        load test).  _ack_flush_shard must write those orders to Redis even
        though no new flush_batch call triggered the threshold.
        """
        cons_module._ack_buffers[0] = ["ORD-STUCK-1", "ORD-STUCK-2", "ORD-STUCK-3"]
        cons_module._ack_last_flush[0] = time.monotonic()  # recent — would suppress interval flush

        count = await _ack_flush_shard(0, mock_redis)

        assert count == 3
        mock_redis.hset.assert_called_once()
        mapping = mock_redis.hset.call_args[1]["mapping"]
        assert "ORD-STUCK-1" in mapping
        assert "ORD-STUCK-2" in mapping
        assert "ORD-STUCK-3" in mapping

    @pytest.mark.asyncio
    async def test_flush_shard_clears_buffer_after_write(self, mock_redis):
        """Buffer must be empty after _ack_flush_shard — no double-ack on next call."""
        cons_module._ack_buffers[0] = ["ORD-A", "ORD-B"]

        await _ack_flush_shard(0, mock_redis)

        assert cons_module._ack_buffers.get(0, []) == []

    @pytest.mark.asyncio
    async def test_flush_shard_noop_when_buffer_empty(self, mock_redis):
        """No-op and returns 0 when buffer is empty — heartbeat running on idle shard."""
        cons_module._ack_buffers[0] = []

        count = await _ack_flush_shard(0, mock_redis)

        assert count == 0
        mock_redis.hset.assert_not_called()

    @pytest.mark.asyncio
    async def test_flush_shard_calls_expire(self, mock_redis):
        """expire must be called after hset so the ack hash auto-cleans."""
        cons_module._ack_buffers[0] = ["ORD-X"]

        await _ack_flush_shard(0, mock_redis)

        mock_redis.expire.assert_called_once()
        assert mock_redis.expire.call_args[0][0] == "orders:acks:0"

    @pytest.mark.asyncio
    async def test_idle_stream_orders_eventually_acked(
        self, mock_db_pool, mock_redis
    ):
        """
        End-to-end: send a partial batch (< ACK_BATCH_SIZE) via flush_batch,
        confirm orders are NOT yet in Redis (below threshold, interval suppressed),
        then call _ack_flush_shard directly (as the heartbeat would) and confirm
        hset is finally called.

        This reproduces the 1044-timeout scenario: 261 orders per shard stuck
        in the last partial batch after the stream drains.
        """
        pool, _ = mock_db_pool
        orders = [
            {"order_id": f"ORD-PARTIAL-{i}", "customer_id": "C1",
             "amount": 9.99, "items": ["X"]}
            for i in range(3)   # 3 < ACK_BATCH_SIZE=9999
        ]
        cons_module._ack_last_flush[0] = time.monotonic()  # suppress interval flush

        with patch.object(cons_module, "ACK_BATCH_SIZE", 9999), \
             patch.object(cons_module, "ACK_FLUSH_INTERVAL", 9999.0):
            await flush_batch(pool, mock_redis, "orders:stream:0", 0,
                              _make_entries(orders))

        # Threshold not hit, interval suppressed — hset NOT called yet
        mock_redis.hset.assert_not_called()
        assert len(cons_module._ack_buffers.get(0, [])) == 3

        # Stream goes idle; heartbeat fires _ack_flush_shard
        await _ack_flush_shard(0, mock_redis)

        mock_redis.hset.assert_called_once()
        mapping = mock_redis.hset.call_args[1]["mapping"]
        for o in orders:
            assert o["order_id"] in mapping
        assert cons_module._ack_buffers.get(0, []) == []


# ---------------------------------------------------------------------------
# Producer /ack/status endpoint tests
# ---------------------------------------------------------------------------

class TestAckStatusEndpoint:

    @pytest.fixture
    async def prod_client(self, mock_redis):
        """FastAPI test client with one mocked Redis shard."""
        import producer as prod_module
        prod_module.clients = [mock_redis]
        async with AsyncClient(
            transport=ASGITransport(app=prod_module.app),
            base_url="http://test",
        ) as ac:
            yield ac, mock_redis

    @pytest.mark.asyncio
    async def test_ack_status_returns_acked(self, prod_client, sample_order):
        """
        When HMGET returns a non-None value the order is "acked" — it reached
        Postgres and was confirmed by the consumer.
        """
        ac, mock_redis = prod_client
        # hmget returns [timestamp_string] → order is present in hash
        mock_redis.hmget.return_value = ["1714000000.0"]

        resp = await ac.get("/ack/status", params={"ids": sample_order["order_id"]})
        assert resp.status_code == 200
        data = resp.json()
        assert data["acked"] == 1
        assert data["pending"] == 0
        assert data["pct_complete"] == 100.0
        assert data["orders"][sample_order["order_id"]] == "acked"

    @pytest.mark.asyncio
    async def test_ack_status_returns_pending(self, prod_client, sample_order):
        """
        When HMGET returns None the order is still "pending" — the consumer
        has not yet written it to the ack hash.
        """
        ac, mock_redis = prod_client
        mock_redis.hmget.return_value = [None]

        resp = await ac.get("/ack/status", params={"ids": sample_order["order_id"]})
        assert resp.status_code == 200
        data = resp.json()
        assert data["acked"] == 0
        assert data["pending"] == 1
        assert data["pct_complete"] == 0.0
        assert data["orders"][sample_order["order_id"]] == "pending"

    @pytest.mark.asyncio
    async def test_ack_status_mixed_acked_and_pending(self, prod_client):
        """
        A batch query returns the correct acked/pending split.
        The pct_complete field lets the load test show a progress bar.
        """
        import producer as prod_module
        with patch.object(prod_module, "NUM_SHARDS", 1):
            prod_module.clients = [prod_client[1]]
            prod_client[1].hmget.return_value = ["ts", None, "ts"]

            resp = await prod_client[0].get(
                "/ack/status",
                params={"ids": "ORD-A,ORD-B,ORD-C"},
            )

        data = resp.json()
        assert data["total"] == 3
        assert data["acked"] == 2
        assert data["pending"] == 1
        assert data["pct_complete"] == pytest.approx(66.67, abs=0.01)

    @pytest.mark.asyncio
    async def test_ack_status_missing_ids_returns_400(self, prod_client):
        """ids param is required — missing it must return 400, not 500."""
        ac, _ = prod_client
        resp = await ac.get("/ack/status")
        assert resp.status_code == 422   # FastAPI Query(...) raises 422 for missing required param

    @pytest.mark.asyncio
    async def test_ack_status_empty_ids_returns_400(self, prod_client):
        """An ids value of only whitespace/commas must return 400."""
        ac, _ = prod_client
        resp = await ac.get("/ack/status", params={"ids": "  ,  "})
        assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_ack_status_over_limit_returns_400(self, prod_client):
        """
        Requests with more than ACK_MAX_IDS ids must be rejected — prevents
        a single request from issuing an unbounded HMGET against Redis.

        We patch ACK_MAX_IDS to 3 so the test doesn't need to build a
        10 000-item URL string that would hit HTTP length limits.
        """
        import producer as prod_module
        ac, _ = prod_client
        with patch.object(prod_module, "ACK_MAX_IDS", 3):
            ids = ",".join(f"ORD-{i}" for i in range(4))   # 4 > 3
            resp = await ac.get("/ack/status", params={"ids": ids})
        assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_ack_status_one_hmget_per_shard(self, prod_client):
        """
        With NUM_SHARDS=1 all ids land on shard 0 — hmget must be called
        exactly once regardless of how many ids are in the request.
        This is the batching invariant: O(shards) Redis calls, not O(ids).
        """
        import producer as prod_module
        with patch.object(prod_module, "NUM_SHARDS", 1):
            prod_module.clients = [prod_client[1]]
            prod_client[1].hmget.return_value = ["ts", "ts", None]

            await prod_client[0].get(
                "/ack/status",
                params={"ids": "ORD-1,ORD-2,ORD-3"},
            )

        assert prod_client[1].hmget.call_count == 1


# ---------------------------------------------------------------------------
# Producer /ack/summary endpoint tests
# ---------------------------------------------------------------------------

class TestAckSummaryEndpoint:

    @pytest.fixture
    async def prod_client(self, mock_redis):
        import producer as prod_module
        prod_module.clients = [mock_redis]
        async with AsyncClient(
            transport=ASGITransport(app=prod_module.app),
            base_url="http://test",
        ) as ac:
            yield ac, mock_redis

    @pytest.mark.asyncio
    async def test_summary_returns_total_and_by_shard(self, prod_client):
        """
        /ack/summary must return total_acked and a by_shard list.
        Uses HLEN on each shard's ack hash — O(1) per shard.
        """
        ac, mock_redis = prod_client
        mock_redis.hlen.return_value = 12345

        resp = await ac.get("/ack/summary")
        assert resp.status_code == 200
        data = resp.json()
        assert "total_acked" in data
        assert "by_shard" in data
        assert isinstance(data["by_shard"], list)

    @pytest.mark.asyncio
    async def test_summary_total_is_sum_of_shards(self, prod_client):
        """total_acked must equal the sum of each shard's acked count."""
        ac, mock_redis = prod_client
        mock_redis.hlen.return_value = 1000   # each of the 1 mocked shards has 1000

        resp = await ac.get("/ack/summary")
        data = resp.json()
        shard_total = sum(s["acked"] for s in data["by_shard"])
        assert data["total_acked"] == shard_total

    @pytest.mark.asyncio
    async def test_summary_calls_hlen_not_hgetall(self, prod_client):
        """
        Summary must use HLEN (O(1)) not HGETALL (O(N)) — fetching every
        field in a multi-million-row hash would be catastrophically slow.
        """
        ac, mock_redis = prod_client
        mock_redis.hlen.return_value = 0

        await ac.get("/ack/summary")

        mock_redis.hlen.assert_called()
        mock_redis.hgetall.assert_not_called()
