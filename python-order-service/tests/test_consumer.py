"""
Unit tests for the consumer's batch-processing logic.

All external dependencies (Redis, Postgres) are mocked so these tests run
without any infrastructure. The goal is to verify:
  1. All rows in a batch are inserted with correct column values.
  2. XACK is called once with ALL message IDs after a successful insert.
  3. XACK is NOT called if the DB insert fails (at-least-once guarantee).
  4. Malformed messages in a batch are skipped; good messages still insert.
  5. Table creation is idempotent.
  6. Consumer group creation handles the "already exists" case.

Industry pattern: unit-test the processing kernel (flush_batch) in isolation;
leave the full xreadgroup → flush → xack loop to integration tests that use
real services.
"""

import json
from unittest.mock import AsyncMock, MagicMock, call
import pytest

from consumer import flush_batch, create_table, consume_shard


# ---------------------------------------------------------------------------
# flush_batch tests
# ---------------------------------------------------------------------------

class TestFlushBatch:

    @pytest.mark.asyncio
    async def test_inserts_all_rows_in_batch(
        self, mock_db_pool, mock_redis, sample_order
    ):
        """
        executemany must receive one tuple per message in the batch.

        Sending N rows in one executemany call is the core of the batching
        optimisation — one DB round trip instead of N.
        """
        pool, conn = mock_db_pool
        entries = [
            ("1714-1", {"data": json.dumps({**sample_order, "order_id": "ORD-1"}), "type": "order_created"}),
            ("1714-2", {"data": json.dumps({**sample_order, "order_id": "ORD-2"}), "type": "order_created"}),
            ("1714-3", {"data": json.dumps({**sample_order, "order_id": "ORD-3"}), "type": "order_created"}),
        ]

        await flush_batch(pool, mock_redis, "orders:stream:0", 0, entries)

        conn.executemany.assert_called_once()
        records = conn.executemany.call_args[0][1]
        assert len(records) == 3
        assert records[0][0] == "ORD-1"
        assert records[1][0] == "ORD-2"
        assert records[2][0] == "ORD-3"

    @pytest.mark.asyncio
    async def test_inserts_correct_column_values(
        self, mock_db_pool, mock_redis, sample_stream_fields, sample_order
    ):
        """
        Each tuple in the executemany batch must have the right column order:
        (order_id, customer_id, amount, items_json, msg_id, shard_idx).

        Swapping any two fields would silently corrupt the data — this test
        pins the parameter positions so a future refactor can't reorder them.
        """
        pool, conn = mock_db_pool
        msg_id = "1714000000000-0"

        await flush_batch(pool, mock_redis, "orders:stream:0", 0, [(msg_id, sample_stream_fields)])

        records = conn.executemany.call_args[0][1]
        row = records[0]
        assert row[0] == sample_order["order_id"]
        assert row[1] == sample_order["customer_id"]
        assert row[2] == float(sample_order["amount"])
        assert json.loads(row[3]) == sample_order["items"]
        assert row[4] == msg_id
        assert row[5] == 0  # shard index

    @pytest.mark.asyncio
    async def test_xack_called_once_with_all_ids(
        self, mock_db_pool, mock_redis, sample_stream_fields
    ):
        """
        XACK must be called exactly once with ALL message IDs as varargs —
        not once per message.

        Sending one XACK command with N IDs saves N-1 Redis round trips
        compared to individual XACK calls.
        """
        pool, _ = mock_db_pool
        entries = [
            ("id-1", sample_stream_fields),
            ("id-2", sample_stream_fields),
            ("id-3", sample_stream_fields),
        ]

        await flush_batch(pool, mock_redis, "orders:stream:1", 1, entries)

        mock_redis.xack.assert_called_once_with(
            "orders:stream:1", "order_processors", "id-1", "id-2", "id-3"
        )

    @pytest.mark.asyncio
    async def test_xack_not_called_when_insert_fails(
        self, mock_db_pool, mock_redis, sample_stream_fields
    ):
        """
        If executemany raises, XACK must NOT be called.

        This is the at-least-once guarantee: messages stay in the Redis
        Pending Entries List (PEL) and are re-delivered on the next consumer
        restart. Without this, a DB outage permanently drops orders.
        """
        pool, conn = mock_db_pool
        conn.executemany.side_effect = Exception("Postgres connection lost")

        # flush_batch swallows the exception internally (logged, not re-raised)
        # because the consumer loop must survive transient DB failures
        await flush_batch(pool, mock_redis, "orders:stream:0", 0, [("id-1", sample_stream_fields)])

        mock_redis.xack.assert_not_called()

    @pytest.mark.asyncio
    async def test_malformed_message_skipped_good_messages_still_insert(
        self, mock_db_pool, mock_redis, sample_stream_fields
    ):
        """
        If one message in a batch has invalid JSON, that message is skipped.
        The rest of the batch must still be inserted and XACK'd.

        This prevents one bad producer message from blocking the entire shard.
        """
        pool, conn = mock_db_pool
        entries = [
            ("good-1", sample_stream_fields),
            ("bad-1", {"data": "NOT JSON {{{", "type": "order_created"}),
            ("good-2", sample_stream_fields),
        ]

        await flush_batch(pool, mock_redis, "orders:stream:0", 0, entries)

        records = conn.executemany.call_args[0][1]
        assert len(records) == 2  # bad message excluded

        # Only the good IDs are XACK'd
        xack_ids = set(mock_redis.xack.call_args[0][2:])
        assert xack_ids == {"good-1", "good-2"}
        assert "bad-1" not in xack_ids

    @pytest.mark.asyncio
    async def test_empty_batch_does_nothing(self, mock_db_pool, mock_redis):
        """
        An empty batch (no messages returned by xreadgroup) must not touch
        Postgres or Redis at all.
        """
        pool, conn = mock_db_pool
        await flush_batch(pool, mock_redis, "orders:stream:0", 0, [])
        conn.executemany.assert_not_called()
        mock_redis.xack.assert_not_called()

    @pytest.mark.asyncio
    async def test_shard_index_stored_per_shard(
        self, mock_db_pool, mock_redis, sample_stream_fields
    ):
        """The shard column in each row must match the shard that consumed it."""
        pool, conn = mock_db_pool

        for shard in range(4):
            conn.reset_mock()
            await flush_batch(pool, mock_redis, f"orders:stream:{shard}", shard,
                              [("id-0", sample_stream_fields)])
            row = conn.executemany.call_args[0][1][0]
            assert row[5] == shard


# ---------------------------------------------------------------------------
# create_table tests
# ---------------------------------------------------------------------------

class TestCreateTable:

    @pytest.mark.asyncio
    async def test_executes_create_table_statement(self, mock_db_pool):
        pool, conn = mock_db_pool
        await create_table(pool)
        conn.execute.assert_called_once()
        sql = conn.execute.call_args[0][0]
        assert "CREATE TABLE IF NOT EXISTS orders" in sql

    @pytest.mark.asyncio
    async def test_idempotent_called_twice(self, mock_db_pool):
        """
        Calling create_table() twice must not raise.
        In practice Postgres handles IF NOT EXISTS; this confirms the Python
        code itself has no state that breaks on a second call.
        """
        pool, conn = mock_db_pool
        await create_table(pool)
        await create_table(pool)
        assert conn.execute.call_count == 2

    @pytest.mark.asyncio
    async def test_table_has_required_columns(self, mock_db_pool):
        pool, conn = mock_db_pool
        await create_table(pool)
        sql = conn.execute.call_args[0][0]
        for col in ["order_id", "customer_id", "amount", "items", "shard", "created_at"]:
            assert col in sql, f"Column '{col}' missing from CREATE TABLE"


# ---------------------------------------------------------------------------
# consume_shard loop tests
# ---------------------------------------------------------------------------

class TestConsumeShard:

    @pytest.mark.asyncio
    async def test_busygroup_error_is_swallowed(self, mock_db_pool):
        """
        Redis raises BUSYGROUP when the consumer group already exists.
        consume_shard must continue — not crash on restart.
        """
        import asyncio
        pool, _ = mock_db_pool
        client = AsyncMock()
        client.xgroup_create.side_effect = Exception("BUSYGROUP Consumer Group name already exists")

        call_count = 0

        async def xreadgroup_once(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count > 1:
                raise asyncio.CancelledError()
            return None

        client.xreadgroup.side_effect = xreadgroup_once

        with pytest.raises(asyncio.CancelledError):
            await consume_shard(0, client, pool)

        assert call_count >= 1, "xreadgroup was never called after BUSYGROUP error"

    @pytest.mark.asyncio
    async def test_batch_messages_dispatched_to_flush(
        self, mock_db_pool, sample_stream_fields
    ):
        """
        When xreadgroup returns messages, flush_batch must be called with
        the full entry list — not one message at a time.
        """
        import asyncio
        pool, conn = mock_db_pool
        client = AsyncMock()
        client.xgroup_create.return_value = "OK"

        entries = [("id-1", sample_stream_fields), ("id-2", sample_stream_fields)]
        call_count = 0

        async def xreadgroup_once(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return [("orders:stream:0", entries)]
            raise asyncio.CancelledError()

        client.xreadgroup.side_effect = xreadgroup_once

        with pytest.raises(asyncio.CancelledError):
            await consume_shard(0, client, pool)

        # Both rows inserted in one executemany call (not two separate calls)
        conn.executemany.assert_called_once()
        records = conn.executemany.call_args[0][1]
        assert len(records) == 2

        # Both IDs XACK'd in one call
        client.xack.assert_called_once()
        xack_ids = set(client.xack.call_args[0][2:])
        assert xack_ids == {"id-1", "id-2"}
