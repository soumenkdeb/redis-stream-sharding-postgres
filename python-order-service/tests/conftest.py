"""
Shared pytest fixtures used across all test modules.

Fixtures are functions that set up (and optionally tear down) test
dependencies. pytest discovers them automatically when they live in
conftest.py — no import needed in the test files.

Scope controls how often the fixture runs:
  "function" (default) — fresh instance for every test
  "session"            — one instance for the whole test run
"""

import json
from unittest.mock import AsyncMock, MagicMock
import pytest
import pytest_asyncio


# ---------------------------------------------------------------------------
# Redis mock
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_redis():
    """
    Async mock of a redis.asyncio.Redis client.

    AsyncMock makes every method awaitable by default, which is required
    because the production code calls `await client.xadd(...)` etc.

    Individual tests override specific methods (e.g. xadd, xreadgroup)
    to control what the fake Redis returns.
    """
    client = AsyncMock()
    # xadd normally returns a bytes message ID like b"1714000000000-0".
    # Return a plain string here since decode_responses=True is set in prod.
    client.xadd.return_value = "1714000000000-0"
    return client


# ---------------------------------------------------------------------------
# asyncpg pool mock
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_db_pool():
    """
    Mock of an asyncpg connection pool.

    asyncpg uses an async context manager pattern:
        async with pool.acquire() as conn:
            await conn.execute(...)

    MagicMock supports __aenter__ / __aexit__ (async context manager protocol)
    when configured correctly. AsyncMock on acquire() lets us `await` it, and
    __aenter__ returns the fake connection.
    """
    pool = MagicMock()
    conn = AsyncMock()

    # acquire() is used as `async with pool.acquire() as conn`
    acquire_ctx = AsyncMock()
    acquire_ctx.__aenter__ = AsyncMock(return_value=conn)
    acquire_ctx.__aexit__ = AsyncMock(return_value=False)
    pool.acquire.return_value = acquire_ctx

    return pool, conn


# ---------------------------------------------------------------------------
# Sample order payload
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_order():
    """Reusable valid order dict — mirrors what the producer receives via HTTP."""
    return {
        "order_id": "ORD-TEST-001",
        "customer_id": "CUST-42",
        "amount": 299.99,
        "items": ["Laptop", "Mouse"],
    }


@pytest.fixture
def sample_stream_fields(sample_order):
    """
    Redis stream fields dict as the consumer would receive them.

    The producer serialises the order into the 'data' key as JSON.
    The consumer calls json.loads(fields["data"]) to recover the order.
    """
    return {
        "type": "order_created",
        "data": json.dumps(sample_order),
        "timestamp": "1714000000.123",
    }
