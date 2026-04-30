import asyncio
import json
import logging
import time
from dotenv import load_dotenv
import redis.asyncio as redis_async
import asyncpg
import os
from prometheus_client import Counter, Histogram, Gauge, start_http_server

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
log = logging.getLogger("consumer")

# ── Prometheus metrics ────────────────────────────────────────────────────────

ROWS_INSERTED = Counter(
    "consumer_rows_inserted_total",
    "Rows successfully written to PostgreSQL",
    ["shard"],
)
BATCHES_FAILED = Counter(
    "consumer_batches_failed_total",
    "Batches that could not be persisted (DB or circuit open)",
    ["shard", "reason"],
)
XACK_FAILED = Counter(
    "consumer_xack_failed_total",
    "XACK calls that failed after retries (messages may be re-delivered)",
    ["shard"],
)
BATCH_LATENCY = Histogram(
    "consumer_batch_duration_seconds",
    "Time to INSERT + XACK one batch",
    ["shard"],
    buckets=[.001, .005, .01, .025, .05, .1, .25, .5, 1.0, 2.5, 5.0],
)
CB_STATE = Gauge(
    "consumer_circuit_breaker_state",
    "Consumer circuit breaker state: 0=closed 1=half_open 2=open",
    ["name"],
)

ACKS_FLUSHED = Counter(
    "consumer_acks_flushed_total",
    "Order IDs written to the ack hash after DB commit",
    ["shard"],
)

METRICS_PORT = int(os.getenv("METRICS_PORT", "9090"))
_CB_STATE_MAP = {"closed": 0, "half_open": 1, "open": 2}

REDIS_URLS   = os.getenv("REDIS_URLS", "redis://:123456@localhost:6379").split(",")
NUM_SHARDS   = int(os.getenv("NUM_SHARDS", len(REDIS_URLS)))
POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://postgres:postgres123@localhost:5432/orders_db")
BATCH_SIZE        = int(os.getenv("BATCH_SIZE", "500"))
ACK_BATCH_SIZE    = int(os.getenv("ACK_BATCH_SIZE", "500"))    # flush acks after N order_ids
ACK_FLUSH_INTERVAL = float(os.getenv("ACK_FLUSH_INTERVAL", "5.0"))  # or after N seconds
ACK_KEY_TTL       = int(os.getenv("ACK_KEY_TTL", str(86400)))  # ack hash expiry (24 h)
ACK_KEY_BASE      = "orders:acks"

# Per-shard ack buffers — asyncio is single-threaded so no locks needed.
# Keyed by shard_idx; populated lazily via setdefault().
_ack_buffers: dict[int, list[str]] = {}
_ack_last_flush: dict[int, float]  = {}  # shard_idx → monotonic timestamp of last flush

STREAM_BASE   = "orders:stream"
GROUP_NAME    = "order_processors"
CONSUMER_NAME = os.getenv("CONSUMER_NAME", "py-consumer-1")

# Separate circuit breakers for DB and Redis so a Postgres outage does not
# affect Redis health tracking and vice versa.
db_breaker    = CircuitBreaker("postgres",      failure_threshold=3, recovery_timeout=30)
# One Redis breaker per shard
redis_breakers: list[CircuitBreaker] = []


async def create_table(db_pool: asyncpg.Pool) -> None:
    """CREATE TABLE IF NOT EXISTS is idempotent — safe on every startup."""
    async with db_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                id          BIGSERIAL PRIMARY KEY,
                order_id    TEXT NOT NULL,
                customer_id TEXT NOT NULL,
                amount      NUMERIC(12, 2) NOT NULL,
                items       JSONB NOT NULL,
                msg_id      TEXT,
                shard       INT,
                created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)


async def _ack_flush_shard(shard_idx: int, client: redis_async.Redis) -> int:
    """
    Unconditionally flush _ack_buffers[shard_idx] to Redis Hash.
    Returns the number of order_ids written (0 if buffer was empty).
    Called from flush_batch (threshold-triggered) and _ack_heartbeat (timer-triggered).
    """
    buf = _ack_buffers.get(shard_idx, [])
    if not buf:
        return 0
    ack_key = f"{ACK_KEY_BASE}:{shard_idx}"
    ts = str(time.time())
    await client.hset(ack_key, mapping={oid: ts for oid in buf})
    await client.expire(ack_key, ACK_KEY_TTL)
    ACKS_FLUSHED.labels(shard=shard_idx).inc(len(buf))
    log.info("ack flush shard=%d count=%d", shard_idx, len(buf))
    n = len(buf)
    buf.clear()
    _ack_last_flush[shard_idx] = time.monotonic()
    return n


async def _ack_heartbeat(shard_idx: int, client: redis_async.Redis) -> None:
    """
    Background timer that flushes any residual ack buffer when the stream is
    idle.  Without this, the last partial batch (<ACK_BATCH_SIZE orders) would
    sit in memory indefinitely — flush_batch is never called again once
    xreadgroup returns empty.
    """
    while True:
        await asyncio.sleep(ACK_FLUSH_INTERVAL)
        await _ack_flush_shard(shard_idx, client)


async def flush_batch(
    db_pool: asyncpg.Pool,
    client: redis_async.Redis,
    stream_name: str,
    shard_idx: int,
    entries: list[tuple[str, dict]],
    *,
    _sleep=asyncio.sleep,   # injectable for tests
) -> None:
    """
    Insert a batch into Postgres then XACK all IDs — both wrapped with
    retry + timeout + circuit breaker.

    DB resilience
    -------------
    executemany() is retried up to 3 times (delays 1s, 2s) before the
    db_breaker failure count increments. After 3 total failures the DB
    circuit opens and the batch is skipped — messages stay in the Redis
    PEL for re-delivery when Postgres recovers.

    Redis resilience
    ----------------
    xack is retried similarly. If XACK ultimately fails, messages are
    re-delivered (possible duplicate DB row). Adding UNIQUE (order_id)
    to the table prevents duplicate inserts.

    synchronous_commit=off
    ----------------------
    Removes the WAL fsync wait per commit (~1-5 ms). Up to ~200 ms of
    commits can be lost on a hard crash; Redis PEL covers that window.
    """
    records, msg_ids = [], []

    for msg_id, fields in entries:
        try:
            data = json.loads(fields["data"])
            records.append((
                data["order_id"],
                data["customer_id"],
                float(data["amount"]),
                json.dumps(data["items"]),
                msg_id,
                shard_idx,
            ))
            msg_ids.append(msg_id)
        except Exception as e:
            print(f"[shard-{shard_idx}] skipping malformed message {msg_id}: {e}")

    if not records:
        return

    t0 = time.perf_counter()

    # ── Postgres insert ───────────────────────────────────────────────────────
    try:
        async def _insert():
            async with db_pool.acquire() as conn:
                await conn.execute("SET synchronous_commit = off")
                await conn.executemany(
                    """INSERT INTO orders
                       (order_id, customer_id, amount, items, msg_id, shard)
                       VALUES ($1, $2, $3, $4::jsonb, $5, $6)""",
                    records,
                )

        await with_resilience(
            _insert,
            circuit_breaker=db_breaker,
            max_retries=3,
            delay=1.0,
            backoff=2.0,
            timeout=10.0,
            label=f"db-insert-shard-{shard_idx}",
            _sleep=_sleep,
        )
    except CircuitBreakerOpenError as e:
        BATCHES_FAILED.labels(shard=shard_idx, reason="circuit_open").inc()
        CB_STATE.labels(name="postgres").set(_CB_STATE_MAP.get(db_breaker.state, 0))
        log.warning("db circuit open shard=%d msgs=%d err=%s", shard_idx, len(records), e)
        return
    except Exception as e:
        BATCHES_FAILED.labels(shard=shard_idx, reason="retries_exhausted").inc()
        log.error("db insert failed shard=%d msgs=%d err=%s", shard_idx, len(records), e)
        return

    ROWS_INSERTED.labels(shard=shard_idx).inc(len(records))
    CB_STATE.labels(name="postgres").set(0)  # success → closed
    log.info("inserted shard=%d rows=%d", shard_idx, len(records))

    # ── Ack buffer ────────────────────────────────────────────────────────────
    # Accumulate order_ids and flush to Redis Hash when ACK_BATCH_SIZE is
    # reached or ACK_FLUSH_INTERVAL seconds have elapsed.  The hash lives on
    # the same Redis shard as the stream so no extra network hop is needed.
    _buf = _ack_buffers.setdefault(shard_idx, [])
    _buf.extend(r[0] for r in records)   # r[0] = order_id

    _now = time.monotonic()
    _elapsed = _now - _ack_last_flush.get(shard_idx, 0.0)
    if len(_buf) >= ACK_BATCH_SIZE or _elapsed >= ACK_FLUSH_INTERVAL:
        await _ack_flush_shard(shard_idx, client)

    # ── Redis XACK ────────────────────────────────────────────────────────────
    try:
        await with_resilience(
            lambda: client.xack(stream_name, GROUP_NAME, *msg_ids),
            circuit_breaker=redis_breakers[shard_idx],
            max_retries=3,
            delay=1.0,
            backoff=2.0,
            timeout=10.0,
            label=f"xack-shard-{shard_idx}",
            _sleep=_sleep,
        )
    except (CircuitBreakerOpenError, Exception) as e:
        XACK_FAILED.labels(shard=shard_idx).inc()
        log.error("xack failed shard=%d msgs=%d err=%s — may duplicate", shard_idx, len(msg_ids), e)

    BATCH_LATENCY.labels(shard=shard_idx).observe(time.perf_counter() - t0)


async def consume_shard(
    shard_idx: int,
    client: redis_async.Redis,
    db_pool: asyncpg.Pool,
) -> None:
    stream_name = f"{STREAM_BASE}:{shard_idx}"

    try:
        await client.xgroup_create(stream_name, GROUP_NAME, id="$", mkstream=True)
    except Exception:
        pass  # BUSYGROUP: group already exists

    print(f"[shard-{shard_idx}] consumer ready on {stream_name}")

    while True:
        try:
            messages = await client.xreadgroup(
                GROUP_NAME,
                CONSUMER_NAME,
                streams={stream_name: ">"},
                count=BATCH_SIZE,
                block=2000,
            )
            if messages:
                for _, entries in messages:
                    if entries:
                        await flush_batch(db_pool, client, stream_name, shard_idx, entries)
        except Exception as e:
            print(f"[shard-{shard_idx}] xreadgroup error: {e}")
            await asyncio.sleep(1)


async def main() -> None:
    global redis_breakers
    redis_breakers = [
        CircuitBreaker(f"redis-xack-shard-{i}", failure_threshold=3, recovery_timeout=30)
        for i in range(NUM_SHARDS)
    ]

    # Expose Prometheus metrics on a dedicated HTTP port (default 9090)
    start_http_server(METRICS_PORT)
    log.info("metrics server started port=%d", METRICS_PORT)

    db_pool = await asyncpg.create_pool(POSTGRES_DSN)
    await create_table(db_pool)

    clients = [redis_async.from_url(url.strip(), decode_responses=True) for url in REDIS_URLS]

    log.info("consumer started shards=%d batch_size=%d", NUM_SHARDS, BATCH_SIZE)

    await asyncio.gather(
        *(consume_shard(i, clients[i], db_pool) for i in range(NUM_SHARDS)),
        *(_ack_heartbeat(i, clients[i]) for i in range(NUM_SHARDS)),
    )


if __name__ == "__main__":
    asyncio.run(main())
