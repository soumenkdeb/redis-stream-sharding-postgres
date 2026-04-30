import asyncio
import hashlib
import json
import logging
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel
import redis.asyncio as redis
from dotenv import load_dotenv
import os
from prometheus_client import (
    Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST,
)

from resilience import CircuitBreaker, CircuitBreakerOpenError, with_resilience

load_dotenv()

# ── structured JSON logging ───────────────────────────────────────────────────

class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        return json.dumps({
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        })

_handler = logging.StreamHandler()
_handler.setFormatter(_JsonFormatter())
logging.basicConfig(handlers=[_handler], level=logging.INFO, force=True)
log = logging.getLogger("producer")

# ── Prometheus metrics ────────────────────────────────────────────────────────

ORDERS_PUBLISHED = Counter(
    "orders_published_total",
    "Orders successfully written to Redis stream",
    ["shard"],
)
ORDERS_FAILED = Counter(
    "orders_failed_total",
    "Orders that failed after all retries or circuit open",
    ["shard", "reason"],   # reason: circuit_open | retries_exhausted
)
REQUEST_LATENCY = Histogram(
    "http_request_duration_seconds",
    "End-to-end POST /orders latency",
    ["status_code"],
    buckets=[.001, .005, .01, .025, .05, .1, .25, .5, 1.0, 2.5],
)
CB_STATE = Gauge(
    "producer_circuit_breaker_state",
    "Producer circuit breaker state: 0=closed 1=half_open 2=open",
    ["name"],
)

REDIS_URLS  = os.getenv("REDIS_URLS", "redis://:123456@localhost:6379").split(",")
NUM_SHARDS  = int(os.getenv("NUM_SHARDS", len(REDIS_URLS)))
STREAM_BASE = "orders:stream"
ACK_KEY_BASE = "orders:acks"
ACK_MAX_IDS  = int(os.getenv("ACK_MAX_IDS", "10000"))  # guard against huge requests

# One async Redis client per shard, populated at startup.
clients: list[redis.Redis] = []

# One circuit breaker per shard so a failure on shard-2 does not block shard-0.
# failure_threshold=3 : circuit opens after 3 total-retry-exhaustion events
# recovery_timeout=30 : circuit stays open 30 s before allowing a probe
redis_breakers: list[CircuitBreaker] = []


class Order(BaseModel):
    order_id: str
    customer_id: str
    amount: float
    items: list[str]


def get_shard(order_id: str) -> int:
    """
    Stable shard routing via SHA-1.
    hashlib.sha1 is consistent across processes/restarts unlike Python's hash().
    """
    return int(hashlib.sha1(order_id.encode()).hexdigest(), 16) % NUM_SHARDS


_CB_STATE_MAP = {"closed": 0, "half_open": 1, "open": 2}


@asynccontextmanager
async def lifespan(app: FastAPI):
    global clients, redis_breakers
    clients = [redis.from_url(url.strip(), decode_responses=True) for url in REDIS_URLS]
    redis_breakers = [
        CircuitBreaker(f"redis-shard-{i}", failure_threshold=3, recovery_timeout=30)
        for i in range(NUM_SHARDS)
    ]
    log.info("Producer started — shards=%d", NUM_SHARDS)
    yield
    for c in clients:
        await c.aclose()


app = FastAPI(title="Order Producer", lifespan=lifespan)


@app.get("/health")
async def health():
    """Circuit breaker states per shard — useful for liveness/readiness probes."""
    states = {cb.name: cb.state for cb in redis_breakers}
    healthy = all(s != "open" for s in states.values())
    return JSONResponse(
        status_code=200 if healthy else 503,
        content={"status": "ok" if healthy else "degraded", "circuit_breakers": states},
    )


@app.get("/metrics")
async def metrics():
    """Prometheus scrape endpoint."""
    for cb in redis_breakers:
        CB_STATE.labels(name=cb.name).set(_CB_STATE_MAP.get(cb.state, 0))
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/ack/status")
async def ack_status(
    ids: str = Query(..., description="Comma-separated order_ids (max 10 000)"),
):
    """
    Check which orders have been durably saved to Postgres and ack-flushed.

    Routes each order_id to its shard via get_shard(), queries HMGET on
    orders:acks:{shard}, and returns an acked/pending map.  The consumer
    writes to that hash after every successful DB commit.

    Use this endpoint from load tests or monitoring to track end-to-end
    processing completion without querying Postgres directly.
    """
    if not clients:
        return JSONResponse(status_code=503, content={"error": "service not ready"})
    order_ids = [i.strip() for i in ids.split(",") if i.strip()]
    if not order_ids:
        return JSONResponse(status_code=400, content={"error": "ids param required"})
    if len(order_ids) > ACK_MAX_IDS:
        return JSONResponse(status_code=400, content={"error": f"max {ACK_MAX_IDS} ids per request"})

    # Group by shard so we issue one HMGET per shard, not one per order_id
    shard_groups: dict[int, list[str]] = {}
    for oid in order_ids:
        shard_groups.setdefault(get_shard(oid), []).append(oid)

    result: dict[str, str] = {}
    for shard_idx, oids in shard_groups.items():
        ack_key = f"{ACK_KEY_BASE}:{shard_idx}"
        values = await clients[shard_idx].hmget(ack_key, *oids)
        for oid, val in zip(oids, values):
            result[oid] = "acked" if val is not None else "pending"

    acked_count = sum(1 for v in result.values() if v == "acked")
    total = len(result)
    return {
        "total": total,
        "acked": acked_count,
        "pending": total - acked_count,
        "pct_complete": round(acked_count / total * 100, 2) if total else 0.0,
        "orders": result,
    }


@app.get("/ack/summary")
async def ack_summary():
    """
    Return total acked order count per shard (HLEN on each ack hash).
    Useful as a quick dashboard metric — does not require listing order_ids.
    """
    if not clients:
        return JSONResponse(status_code=503, content={"error": "service not ready"})
    by_shard = []
    for i, client in enumerate(clients):
        count = await client.hlen(f"{ACK_KEY_BASE}:{i}")
        by_shard.append({"shard": i, "acked": count})
    total_acked = sum(s["acked"] for s in by_shard)
    return {"total_acked": total_acked, "by_shard": by_shard}


@app.post("/orders")
async def create_order(order: Order, request: Request):
    shard_idx  = get_shard(order.order_id)
    stream_key = f"{STREAM_BASE}:{shard_idx}"
    message = {
        "type":      "order_created",
        "data":      order.model_dump_json(),
        "timestamp": str(asyncio.get_running_loop().time()),
    }

    t0 = time.perf_counter()
    try:
        msg_id = await with_resilience(
            lambda: clients[shard_idx].xadd(
                stream_key,
                fields=message,
                maxlen=1_000_000,
                approximate=True,
            ),
            circuit_breaker=redis_breakers[shard_idx],
            max_retries=3,
            delay=1.0,
            backoff=2.0,
            timeout=10.0,
            label=f"xadd-shard-{shard_idx}",
        )
    except CircuitBreakerOpenError as e:
        ORDERS_FAILED.labels(shard=shard_idx, reason="circuit_open").inc()
        REQUEST_LATENCY.labels(status_code="503").observe(time.perf_counter() - t0)
        log.warning("circuit open shard=%d order_id=%s", shard_idx, order.order_id)
        return JSONResponse(status_code=503, content={
            "error": "service_unavailable",
            "detail": str(e),
        })
    except Exception as e:
        ORDERS_FAILED.labels(shard=shard_idx, reason="retries_exhausted").inc()
        REQUEST_LATENCY.labels(status_code="502").observe(time.perf_counter() - t0)
        log.error("xadd failed shard=%d order_id=%s err=%s", shard_idx, order.order_id, e)
        return JSONResponse(status_code=502, content={
            "error": "upstream_failed",
            "detail": str(e),
        })

    ORDERS_PUBLISHED.labels(shard=shard_idx).inc()
    REQUEST_LATENCY.labels(status_code="200").observe(time.perf_counter() - t0)
    log.debug("queued order_id=%s shard=%d msg_id=%s", order.order_id, shard_idx, msg_id)
    return {"status": "queued", "message_id": msg_id, "shard": shard_idx}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
