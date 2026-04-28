"""
HTTP-layer tests for the FastAPI producer (POST /orders).

Uses httpx.AsyncClient with ASGITransport to call the app in-process —
no network socket opened, no uvicorn process needed.

Redis is mocked so these tests run without any infrastructure.

Industry pattern: test the HTTP contract (status, shape, headers) separately
from the business logic. If a field disappears from the response or a status
code changes, this layer catches it immediately.
"""

import json
from unittest.mock import AsyncMock, patch
import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport

import producer as prod_module
from producer import app, get_shard


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture
async def client(mock_redis):
    """
    Async HTTP client wired directly to the FastAPI app via ASGI transport.

    ASGITransport bypasses the network entirely — requests go straight into
    the app's request handler. This makes tests fast and deterministic.

    We patch producer.clients (the module-level list of Redis connections)
    with our mock so no real Redis connection is attempted.
    """
    prod_module.clients = [mock_redis]  # one shard in tests
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as ac:
        yield ac, mock_redis


# ---------------------------------------------------------------------------
# Response contract tests
# ---------------------------------------------------------------------------

class TestPostOrderSuccess:

    @pytest.mark.asyncio
    async def test_returns_200(self, client, sample_order):
        ac, _ = client
        response = await ac.post("/orders", json=sample_order)
        assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_response_contains_required_fields(self, client, sample_order):
        """
        The response JSON must contain status, message_id, and shard.

        Consumers of this API (dashboards, monitoring) depend on these fields.
        Adding this test means a future refactor cannot accidentally remove one.
        """
        ac, _ = client
        body = (await ac.post("/orders", json=sample_order)).json()
        assert "status" in body
        assert "message_id" in body
        assert "shard" in body

    @pytest.mark.asyncio
    async def test_status_is_queued(self, client, sample_order):
        ac, _ = client
        body = (await ac.post("/orders", json=sample_order)).json()
        assert body["status"] == "queued"

    @pytest.mark.asyncio
    async def test_message_id_matches_redis_return(self, client, sample_order):
        """
        The message_id in the response must be whatever Redis xadd returned.

        This verifies the controller passes the Redis-generated ID back to
        the caller — important for tracing an order through the pipeline.
        """
        ac, mock_redis = client
        mock_redis.xadd.return_value = "9999000000000-7"
        body = (await ac.post("/orders", json=sample_order)).json()
        assert body["message_id"] == "9999000000000-7"


# ---------------------------------------------------------------------------
# Shard routing tests
# ---------------------------------------------------------------------------

class TestShardRouting:

    @pytest.mark.asyncio
    async def test_shard_field_is_integer(self, client, sample_order):
        ac, _ = client
        body = (await ac.post("/orders", json=sample_order)).json()
        assert isinstance(body["shard"], int)

    @pytest.mark.asyncio
    async def test_shard_matches_get_shard_function(self, client, sample_order):
        """
        The shard in the response must equal get_shard(order_id).

        This ties the HTTP response to the routing logic so both stay in sync.
        """
        ac, mock_redis = client
        # Force one shard so get_shard always returns 0 regardless of hash
        with patch.object(prod_module, "NUM_SHARDS", 1):
            prod_module.clients = [mock_redis]
            body = (await ac.post("/orders", json=sample_order)).json()
        assert body["shard"] == 0

    @pytest.mark.asyncio
    async def test_xadd_called_on_correct_stream(self, client, sample_order):
        """
        Confirm the message lands on the stream for the computed shard, not a
        hardcoded one. This is the core routing invariant.
        """
        ac, mock_redis = client
        await ac.post("/orders", json=sample_order)

        # xadd should have been called once
        mock_redis.xadd.assert_called_once()
        call_kwargs = mock_redis.xadd.call_args

        # First positional arg is the stream key
        stream_key = call_kwargs[0][0]
        expected_shard = get_shard(sample_order["order_id"])
        assert stream_key == f"orders:stream:{expected_shard}"

    @pytest.mark.asyncio
    async def test_xadd_message_contains_data_field(self, client, sample_order):
        """
        The stream message must have a 'data' field containing the JSON order.
        The consumer depends on this field to reconstruct the Order object.
        """
        ac, mock_redis = client
        await ac.post("/orders", json=sample_order)

        fields = mock_redis.xadd.call_args[1]["fields"]
        assert "data" in fields
        parsed = json.loads(fields["data"])
        assert parsed["order_id"] == sample_order["order_id"]
        assert parsed["customer_id"] == sample_order["customer_id"]

    @pytest.mark.asyncio
    async def test_xadd_message_contains_type_field(self, client, sample_order):
        ac, mock_redis = client
        await ac.post("/orders", json=sample_order)
        fields = mock_redis.xadd.call_args[1]["fields"]
        assert fields["type"] == "order_created"


# ---------------------------------------------------------------------------
# Input validation tests
# ---------------------------------------------------------------------------

class TestInputValidation:

    @pytest.mark.asyncio
    async def test_missing_order_id_returns_422(self, client):
        """
        Pydantic validates the request body before the handler runs.
        A missing required field must return 422 Unprocessable Entity —
        never a 500 Internal Server Error.
        """
        ac, _ = client
        response = await ac.post("/orders", json={
            "customer_id": "CUST-1",
            "amount": 99.99,
            "items": ["Laptop"],
            # order_id missing
        })
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_missing_amount_returns_422(self, client):
        ac, _ = client
        response = await ac.post("/orders", json={
            "order_id": "ORD-1",
            "customer_id": "CUST-1",
            "items": ["Laptop"],
        })
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_invalid_amount_type_returns_422(self, client):
        ac, _ = client
        response = await ac.post("/orders", json={
            "order_id": "ORD-1",
            "customer_id": "CUST-1",
            "amount": "not-a-number",
            "items": ["Laptop"],
        })
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_empty_body_returns_422(self, client):
        ac, _ = client
        response = await ac.post("/orders", json={})
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_empty_items_list_accepted(self, client):
        """
        An empty items list is structurally valid — business rules might
        reject it, but the HTTP layer should not. Test documents this boundary.
        """
        ac, _ = client
        response = await ac.post("/orders", json={
            "order_id": "ORD-EMPTY-ITEMS",
            "customer_id": "CUST-1",
            "amount": 0.0,
            "items": [],
        })
        assert response.status_code == 200
