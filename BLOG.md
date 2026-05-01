# Building a High-Throughput Order Pipeline with Redis Streams, PostgreSQL, and Three Runtimes

*A five-part engineering series on designing and building a horizontally-sharded, fault-tolerant order processing system from first principles.*

---

## Part 1 — The Problem: 100,000 Orders Per Second Into a Database

### The Scenario

Imagine you are building the booking engine for a flash sale, a financial trading platform, or a high-frequency ticket marketplace. In these systems, the peak write rate is not steady — it is a vertical cliff. Ten thousand users click "Buy" in the same second. Each click is a transaction that must be acknowledged instantly and then durably stored.

The naive implementation — `POST /order → INSERT INTO orders` — works fine at a hundred requests per second. It starts breaking at ten thousand. At one hundred thousand it is simply impossible. The database becomes the wall.

### Why the Database Is Not the Bottleneck You Think It Is

PostgreSQL on modern hardware can comfortably handle 20,000–50,000 single-row inserts per second. That sounds fast enough, but consider what happens when that limit is reached:

- HTTP requests start queuing behind database connections.
- Connection pool exhaustion triggers 503 errors.
- Database load spikes, WAL flush times increase, the situation compounds.
- Even if you scale the database, the HTTP layer now waits for the DB on every request.

The problem is **coupling**: the HTTP write path and the database write path are synchronous. Every `POST /order` waits for Postgres before returning a response.

### The Solution: Decouple With a Message Queue

The standard fix is to decouple these two paths:

1. **Producer** — accepts HTTP requests, writes to a fast, in-memory queue, returns immediately. Latency: <1 ms.
2. **Consumer** — reads from the queue asynchronously, batches writes into the database. Throughput: limited only by DB write speed.

The HTTP response time is now the Redis write latency, not the PostgreSQL commit latency.

### Why Redis Streams and Not Kafka?

This is the question most engineers ask first. Kafka is the industry standard for this kind of problem. So why Redis Streams?

| Property | Redis Streams | Kafka | RabbitMQ |
|---|---|---|---|
| Write latency (p99) | **< 1 ms** | 5–20 ms | 2–10 ms |
| Ops complexity | **Near zero** | High (ZooKeeper / KRaft) | Medium |
| Consumer groups | Yes | Yes | Limited |
| At-least-once delivery | Yes (`XACK` + PEL) | Yes (offsets) | Yes |
| Message replay | Yes (up to `MAXLEN`) | Yes (unlimited retention) | No |
| Separate deployment | No (same Redis) | **Yes** (separate cluster) | Yes |

The key insight: **Kafka is designed for unlimited, long-term event retention with complex consumer topology**. For a booking pipeline where the consumer is fast and the goal is throughput (not multi-year replay), Kafka's operational overhead is not justified.

Redis Streams give you consumer groups, at-least-once delivery via the Pending Entries List (PEL), and message replay (up to your configured `MAXLEN`) — all at sub-millisecond write latency with zero additional infrastructure.

The PEL is the critical mechanism. When a consumer reads a message with `XREADGROUP`, that message enters the consumer's Pending Entries List. It stays there until the consumer calls `XACK`. If the consumer crashes, the message is re-delivered to the next available consumer. This is the "at-least-once" guarantee — data is never silently dropped.

### Why Not Just Use Redis Pub/Sub?

Pub/Sub in Redis is fire-and-forget. If no subscriber is connected when a message is published, the message disappears. There is no persistence, no replay, no consumer group, no delivery acknowledgement. A single Redis node failure loses everything in flight. Redis Streams solves all of these gaps — it is a persistent, ordered, consumer-group-aware log, not a broadcast channel.

### Why Horizontal Sharding?

A single Redis instance tops out at roughly 150,000–200,000 stream writes per second before CPU or network saturation. Our target is 100,000+ orders per second with headroom for spikes. With four Redis shards, the ceiling multiplies linearly:

```
4 shards × 150,000 ops/sec = 600,000 ops/sec ceiling
```

Each shard is completely independent. A failure on shard 2 does not affect shards 0, 1, or 3. Scaling is additive — add two more Redis instances, bump `NUM_SHARDS` to 6, throughput increases immediately with no code changes.

**The routing decision**: each order is routed to a shard by hashing its `order_id`. The hash must be deterministic across all processes and restarts. Python's built-in `hash()` is randomised on every interpreter start (controlled by `PYTHONHASHSEED`), which means different uvicorn worker processes would route the same `order_id` to different shards. We use `hashlib.sha1` instead — SHA-1 is defined by a specification and produces identical output across all processes, machines, and restarts.

```python
def get_shard(order_id: str) -> int:
    return int(hashlib.sha1(order_id.encode()).hexdigest(), 16) % NUM_SHARDS
```

This also preserves per-customer ordering: the same `order_id` always lands on the same shard, so a customer's orders are always processed in the order they were received.

---

## Part 2 — Infrastructure and the Python Foundation

### Assembling the Stack with Docker Compose

The first step was to define the infrastructure. We needed four Redis instances (one per shard), one PostgreSQL database, and two UI tools for observability during development: pgAdmin4 for Postgres and RedisInsight for Redis.

```yaml
# compose.yaml (abbreviated)
services:
  redis-0:
    image: redis:7-alpine
    ports: ["6379:6379"]
    command: redis-server --requirepass 123456

  redis-1:
    image: redis:7-alpine
    ports: ["6380:6379"]
    command: redis-server --requirepass 123456

  # redis-2, redis-3 follow the same pattern on ports 6381, 6382

  postgres-db:
    image: postgres:16-alpine
    environment:
      POSTGRES_DB: orders_db
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres123

  pgadmin:
    image: dpage/pgadmin4
    ports: ["8080:80"]

  redis-insight:
    image: redis/redisinsight:latest
    ports: ["5540:5540"]
```

A single `podman compose up -d` starts the entire stack. All application configuration is in `.env`, loaded by `python-dotenv` at runtime:

```
REDIS_URLS=redis://:123456@localhost:6379,redis://:123456@localhost:6380,...
POSTGRES_DSN=postgresql://postgres:postgres123@localhost:5432/orders_db
NUM_SHARDS=4
BATCH_SIZE=500
```

### The Python Producer

The producer is a FastAPI application. Its job is simple: receive an HTTP POST, route the order to the correct Redis shard, write it as a stream message, and return the message ID.

The earliest version of the producer had two bugs that would have caused silent data corruption at scale:

**Bug 1 — Hash instability**: using Python's `hash()` function for shard routing. Fixed by switching to `hashlib.sha1`. See Part 1 for the full explanation.

**Bug 2 — Deprecated startup hooks**: `@app.on_event("startup")` is deprecated since FastAPI 0.93. The correct pattern is the `lifespan` context manager, which uses a standard Python async generator to manage startup and shutdown:

```python
from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: create one Redis client per shard
    global clients, redis_breakers
    clients = [redis.from_url(url.strip(), decode_responses=True) for url in REDIS_URLS]
    redis_breakers = [
        CircuitBreaker(f"redis-shard-{i}", failure_threshold=3, recovery_timeout=30)
        for i in range(NUM_SHARDS)
    ]
    yield
    # Shutdown: close all connections cleanly
    for c in clients:
        await c.aclose()

app = FastAPI(lifespan=lifespan)
```

The `yield` is the boundary between startup and shutdown. Everything before `yield` runs when the application starts. Everything after runs when it shuts down.

### The Python Consumer

The consumer is a pure asyncio script. It uses `asyncio.gather` to run one coroutine per shard simultaneously on the same event loop:

```python
await asyncio.gather(*(consume_shard(i, clients[i], db_pool) for i in range(NUM_SHARDS)))
```

Each coroutine loops on `XREADGROUP`. When messages arrive, they are handed to `flush_batch`.

**The batching insight**: the first version of the consumer called one INSERT and one XACK per message. At 500 messages per second that is 500 DB round trips per second. We changed it to collect all messages from one `XREADGROUP` poll and flush them together:

```
Before (per-message):
  500 DB round trips  +  500 XACK calls  per batch
After  (batched):
  1 DB round trip     +  1 XACK call     per batch
```

`asyncpg.executemany()` sends all 500 rows in a single prepared-statement batch — one network round trip to Postgres instead of 500. The bulk XACK uses Redis's varargs syntax:

```python
await client.xack(stream_name, GROUP_NAME, *msg_ids)  # N IDs, 1 Redis command
```

**`synchronous_commit = off`**: normally PostgreSQL waits for WAL to be flushed to disk before acknowledging a commit. This fsync wait is 1–5 ms on a spinning disk, ~0.1 ms on NVMe. Setting `synchronous_commit = off` at session level tells Postgres to acknowledge before the fsync. Risk: up to ~200 ms of committed data lost on a hard crash. Mitigation: all unacknowledged messages remain in the Redis PEL and will be re-delivered on the next consumer start.

**At-least-once guarantee**: XACK is only called after the DB insert succeeds. On any failure, the function returns early without calling XACK. The messages stay in the PEL and are re-delivered.

```python
try:
    await with_resilience(_insert, circuit_breaker=db_breaker, ...)
except (CircuitBreakerOpenError, Exception):
    return          # ← XACK never called; messages stay in PEL

await client.xack(stream_name, GROUP_NAME, *msg_ids)  # only on success
```

### The Database Schema

```sql
CREATE TABLE IF NOT EXISTS orders (
    id          BIGSERIAL PRIMARY KEY,
    order_id    TEXT NOT NULL,
    customer_id TEXT NOT NULL,
    amount      NUMERIC(12, 2) NOT NULL,
    items       JSONB NOT NULL,        -- array stored as binary JSON
    msg_id      TEXT,                  -- Redis stream message ID for tracing
    shard       INT,                   -- which shard delivered this
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
```

`JSONB` stores the items array as binary JSON, supporting GIN-indexed containment queries (`items @> '["Laptop"]'`) without schema migrations as product catalogues evolve.

`BIGSERIAL` gives us a monotonically increasing surrogate key for pagination and cursor-based queries on the orders table.

### Testing the Python Layer

All tests run without any infrastructure — Redis and Postgres are mocked with `AsyncMock`:

```python
# AsyncMock makes every method awaitable — matches redis.asyncio.Redis behaviour
client = AsyncMock()
client.xadd.return_value = "1714000000000-0"

# ASGITransport sends HTTP requests directly into the FastAPI app
async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
    response = await ac.post("/orders", json=order)
```

The `asyncpg` pool pattern (async context manager with `pool.acquire() as conn`) required careful mocking:

```python
pool = MagicMock()
conn = AsyncMock()
acquire_ctx = AsyncMock()
acquire_ctx.__aenter__ = AsyncMock(return_value=conn)
acquire_ctx.__aexit__ = AsyncMock(return_value=False)
pool.acquire.return_value = acquire_ctx
```

`pytest-asyncio` with `asyncio_mode = auto` removes the need to mark every async test with `@pytest.mark.asyncio`. `autouse=True` fixtures ensure state (circuit breakers, ack buffers) is reset between every test.

---

## Part 3 — Spring Boot and Quarkus: The Same Principle, Three Runtimes

The Python implementation proved the architecture. The next challenge was implementing the same pipeline in Java — both to validate the design across runtimes and to demonstrate that the throughput gains were architectural, not language-specific.

### The Producer: Reactive vs Virtual Threads

**Spring Boot (WebFlux + Lettuce)** uses the reactive model. The Netty event loop handles all in-flight requests on a small thread pool. Reactive Lettuce multiplexes all in-flight `XADD` calls over a single TCP connection per shard using `CompletableFuture`/Reactor internally — no thread-per-request, no head-of-line blocking:

```java
@PostMapping
public Mono<Map<String, String>> createOrder(@RequestBody Order order) {
    int shardIdx = Math.abs(order.orderId().hashCode()) % numShards;
    return shards.get(shardIdx).reactive()
        .xadd(streamName, XAddArgs.Builder.maxlen(1_000_000).approximateTrimming(), fields)
        .map(msgId -> Map.of("status", "queued", "message_id", msgId, "shard", ...));
}
```

**Quarkus (RESTEasy + Virtual Threads)** takes a different approach. Java 21 virtual threads are cheap enough (200–500 bytes heap, no OS thread) that you can block them freely without exhausting a platform thread pool. `@RunOnVirtualThread` dispatches each HTTP request to a fresh virtual thread:

```java
@POST
@RunOnVirtualThread
public Response createOrder(Order order) throws Exception {
    int shardIdx = Math.abs(order.orderId().hashCode()) % numShards;
    // Blocking xadd on a virtual thread — safe because parking is cheap
    String msgId = redisConfig.get(shardIdx).sync().xadd(streamName, fields);
    return Response.ok(Map.of("status", "queued", "message_id", msgId)).build();
}
```

Both approaches achieve equivalent throughput. The reactive model has slightly less overhead at extreme concurrency; virtual threads have simpler code and better debuggability.

### The Consumer: Virtual Threads for Both

Both JVM consumers use the same pattern — one virtual thread per shard, running a blocking poll loop:

```java
// Spring Boot
Thread.ofVirtual().name("consumer-shard-" + shardIdx).start(() -> consumeShard(shardIdx));

// Quarkus (in @Observes StartupEvent)
Thread.ofVirtual().name("consumer-shard-" + shardIdx).start(() -> consumeShard(shardIdx));
```

The blocking `xreadgroup` with `block(2000)` parks the virtual thread for up to 2 seconds while waiting for messages — negligible cost compared to a platform thread.

**Spring Boot batch insert** uses `JdbcTemplate.batchUpdate`:
```java
jdbc.batchUpdate(
    "INSERT INTO orders (order_id, customer_id, amount, items, msg_id, shard) " +
    "VALUES (?, ?, ?, ?::jsonb, ?, ?)",
    batchArgs   // List<Object[]>, one per message
);
// One DB round trip for 500 rows
```

**Quarkus batch insert** uses raw JDBC with explicit transaction control — required because Quarkus injects `AgroalDataSource` (not `javax.sql.DataSource`) and its transaction integration works differently from Spring:

```java
conn.setAutoCommit(false);
PreparedStatement ps = conn.prepareStatement(SQL);
for (Order o : batch) {
    ps.setString(1, o.orderId);
    ps.addBatch();
}
ps.executeBatch();
conn.commit();
```

On any exception: `conn.rollback()` and return without XACK — the at-least-once guarantee holds.

### A Lettuce Gotcha That Cost Hours

The Lettuce 6.3.x API has a non-obvious structure: `StreamOffset` is a **nested static class** of `XReadArgs`, not a top-level class.

```java
// Correct
XReadArgs.StreamOffset.lastConsumed(streamName)
XReadArgs.StreamOffset.from(streamName, "$")

// Wrong — does not compile in 6.3.x
StreamOffset.lastConsumed(streamName)
```

Discovering this required reading the Lettuce source rather than the documentation, which had not caught up to the 6.x API reorganisation.

### Producer/Consumer Split Deployment

Both JVM services support a `MODE` parameter in `start.sh`:

```bash
./start.sh producer   # HTTP active, consumer threads disabled, port 8000
./start.sh consumer   # consumer threads active, HTTP returns 503, port 8002
./start.sh both       # both active (default, for development), port 8000
```

**Port assignment** (Spring Boot):
- Producer mode: `port 8000` (HTTP `/orders` and `/ack/*` endpoints)
- Consumer mode: `port 8002` (processes streams, writes ACKs to Redis)
- Both mode: `port 8000` (monolithic, development only)

Ports are configured via Spring profiles (`application-producer.yml`, `application-consumer.yml`, `application.yml`), allowing producer and consumer to run simultaneously without port conflicts when deployed separately.

This is implemented via conditional beans:

```java
// Spring Boot
@Component
@ConditionalOnProperty(name = "app.consumer.enabled", havingValue = "true", matchIfMissing = false)
public class ShardedConsumer implements ApplicationRunner { ... }
```

```java
// Quarkus
@ConfigProperty(name = "app.consumer.enabled", defaultValue = "true")
boolean consumerEnabled;

void onStart(@Observes StartupEvent ev) {
    if (!consumerEnabled) { return; }
    // start consumer threads
}
```

In production, producer pods and consumer pods scale independently. A spike in order volume scales producer pods; a growing Redis backlog scales consumer pods. Load tests can target the producer on one port and query ACKs on the consumer's port via the `ACK_URL` environment variable.

**Load testing split deployment**:
```bash
# Producer on 8000, consumer on 8002
PRODUCER_URL=http://localhost:8000 ACK_URL=http://localhost:8002 python load_test_producer.py
```

### Testing Java: Testcontainers

The Java integration tests spin up real Redis and PostgreSQL containers using Testcontainers, POST real orders, and wait for the consumer to process them before asserting on database rows.

The most subtle issue: **the consumer group race condition**. The consumer creates its group at offset `$` (latest) on startup. If a test message is published before the group exists, the message falls before `$` and is never delivered.

Fix: pre-create the consumer group at offset `0-0` before the application starts. In Spring Boot, this happens inside `@DynamicPropertySource` (which runs after containers start but before the Spring context boots). In Quarkus, inside `QuarkusTestResourceLifecycleManager.start()`.

```java
// Spring Boot — @DynamicPropertySource
redis.execInContainer("redis-cli", "-a", "123456",
    "XGROUP", "CREATE", "orders:stream:0", "order_processors", "0-0", "MKSTREAM");
```

Awaitility replaces `Thread.sleep` for async assertions:
```java
await().atMost(10, SECONDS).untilAsserted(() -> {
    Integer count = jdbc.queryForObject(
        "SELECT COUNT(*) FROM orders WHERE order_id = ?", Integer.class, orderId);
    assertThat(count).isEqualTo(1);
});
```

---

## Part 4 — Resilience: Timeout, Retry, and Circuit Breaker

### The Problem With "Always Available"

The initial design assumed Redis and Postgres were always reachable. In production, this assumption breaks constantly:

- Network partitions cause connections to hang indefinitely.
- Postgres restarts for maintenance or failover.
- Redis instances become temporarily overloaded.
- Cloud load balancers silently drop packets during rolling updates.

Without resilience, a single hung Redis connection blocks an asyncio event-loop task indefinitely. A Postgres restart triggers a cascade of 500 errors that could open circuit breakers in downstream systems watching your API.

### The Three Patterns

We implemented three layered patterns in `resilience.py`. They compose as follows:

```
call arrives
  ↓
[Circuit Breaker] — is the circuit OPEN?
  YES → raise CircuitBreakerOpenError immediately (fail fast, no attempt)
  NO  ↓
[Per-attempt loop]
  Attempt 1: asyncio.wait_for(coro_factory(), timeout=10s)
    success → circuit_breaker.on_success() → return result
    failure → sleep(1s) → attempt 2
  Attempt 2: asyncio.wait_for(coro_factory(), timeout=10s)
    success → circuit_breaker.on_success() → return result
    failure → sleep(2s) → attempt 3
  Attempt 3: asyncio.wait_for(coro_factory(), timeout=10s)
    success → circuit_breaker.on_success() → return result
    ALL retries exhausted → circuit_breaker.on_failure() → raise last exception
```

**Pattern 1 — Timeout**: each individual attempt is bounded by `asyncio.wait_for`. A hung Redis connection is cancelled after 10 seconds. Without this, one slow dependency holds an event-loop task indefinitely, eventually exhausting all available coroutines.

**Pattern 2 — Retry with exponential backoff**: transient failures (network blip, momentary overload) are retried up to 3 times total. Delays: 1 s → 2 s (backoff multiplier = 2). Crucially, only when **all** retries fail does the circuit breaker count it as one failure. Brief blips that recover within one retry window never open the circuit.

**Pattern 3 — Circuit Breaker**: tracks retry-exhaustion events. After 3 such events the circuit opens. All subsequent calls are rejected instantly with `CircuitBreakerOpenError` — fail fast rather than waiting for timeouts. After 30 seconds one probe request (HALF_OPEN state) is allowed. Success closes the circuit. Failure resets the 30-second window.

```
State       allow_request()  next transition
────────────────────────────────────────────────────────────
CLOSED      always yes       3 exhaustions → OPEN
OPEN        no (instant fail) 30 s elapsed → HALF_OPEN
HALF_OPEN   yes (one probe)  success → CLOSED / failure → OPEN
```

### The Circuit Breaker Implementation

```python
class CircuitBreaker:
    def __init__(self, name, failure_threshold=3, recovery_timeout=30.0, _clock=time.monotonic):
        self._state = "closed"
        self._failure_count = 0
        self._opened_at = None
        self._clock = _clock  # injectable for testing

    @property
    def state(self) -> str:
        if self._state == "open":
            elapsed = self._clock() - self._opened_at
            if elapsed >= self.recovery_timeout:
                self._state = "half_open"  # auto-transition
        return self._state

    def on_failure(self) -> None:
        self._failure_count += 1
        if self._state == "half_open" or self._failure_count >= self.failure_threshold:
            self._state = "open"
            self._opened_at = self._clock()

    def on_success(self) -> None:
        self._failure_count = 0
        self._opened_at = None
        self._state = "closed"
```

The `_clock` parameter is the key to testable design. In production it uses `time.monotonic`. In tests we inject a `lambda: tick` where `tick` is a float we control. This lets us test the HALF_OPEN transition without sleeping 30 seconds:

```python
tick = 0.0
cb = CircuitBreaker("t", recovery_timeout=30, _clock=lambda: tick)
cb.on_failure()
assert cb.state == "open"

tick = 31.0          # jump clock past recovery window
assert cb.state == "half_open"   # instant — no real sleep
```

Similarly, `_sleep=AsyncMock()` in `with_resilience()` eliminates the 1s and 2s waits during retry tests:

```python
sleep = AsyncMock()
await with_resilience(flaky, circuit_breaker=cb, max_retries=3, _sleep=sleep)
assert sleep.call_args_list == [call(1.0), call(2.0)]  # delays verified without waiting
```

### Separate Breakers Per Concern

We maintain independent circuit breakers for each external dependency:

- One breaker per Redis shard (producer: for XADD; consumer: for XACK)
- One breaker for PostgreSQL (consumer: for executemany)

This separation is critical. A Postgres outage must not open the Redis circuit — orders should still be accepted and queued. A Redis shard 2 outage must not affect shards 0, 1, and 3.

### What the Producer Signals to Callers

```
HTTP 200 → Order queued successfully in Redis
HTTP 502 → All 3 retries exhausted — Redis is intermittently failing
HTTP 503 → Circuit is OPEN — Redis shard is down, retry after 30 s
```

The `503` response includes the recovery time, letting clients implement smarter retry logic rather than hammering a broken endpoint.

### Resilience in Java (Spring Boot and Quarkus)

The JVM services use the same three-pattern approach, implemented using Java's native concurrency primitives. The consumer's virtual-thread blocking model means `Thread.sleep(retryDelayMs)` is the retry mechanism — a virtual thread parking for 1 second costs ~200 bytes of heap.

For the Spring Boot producer (WebFlux), timeouts use Reactor's `timeout()` operator; retries use `retry(maxAttempts)`. For the Quarkus producer (virtual threads), `java.util.concurrent.CompletableFuture.orTimeout()` bounds each attempt.

---

## Part 5 — The Ack Aggregator: Confirming End-to-End Delivery

### The Async Gap Problem

After the resilience layer was in place, a new question emerged during load testing: *how do we know all orders actually reached Postgres?*

`POST /orders` returns in under 1 millisecond. At that point the order is in Redis, queued for the consumer. The consumer may flush it to Postgres 100 milliseconds later, or 2 seconds later if the batch hasn't filled up, or longer if Postgres was briefly unreachable and the retry backoff is in effect.

Without some form of completion tracking:
- The load test cannot tell "sent 100,000 orders" from "sent 100,000 orders and they all reached the database."
- Monitoring systems cannot tell "consumer is keeping up" from "consumer is silently falling behind."
- Downstream systems have no way to confirm "all today's orders are ready to be processed."

### Design

We needed a mechanism that:
1. Has no impact on the fast path (POST /orders must stay <1 ms)
2. Requires no Postgres query per order (that would recreate the original bottleneck)
3. Is queryable by the producer, not only by the consumer
4. Scales with shard count

The solution: after each successful DB commit, the consumer writes the processed `order_id`s to a **Redis Hash** (`orders:acks:{shard}`) on the same Redis instance as the stream. The producer exposes `/ack/status` and `/ack/summary` endpoints that query these hashes.

```
Consumer (after DB commit):
  order_ids → in-memory buffer[shard]
       ↓ (when buffer ≥ ACK_BATCH_SIZE or ACK_FLUSH_INTERVAL elapses)
  HSET orders:acks:{shard} {order_id: timestamp, ...}
  EXPIRE orders:acks:{shard} 86400   ← 24h TTL, no manual cleanup needed

Producer (GET /ack/status?ids=ORD-1,ORD-2,...):
  group ids by get_shard(id)
  HMGET orders:acks:{shard} [ids_for_this_shard]  ← one Redis call per shard
  return {order_id: "acked" | "pending"}
```

### Why Redis Hash and Not a Separate Ack Stream?

We considered publishing acks to a separate Redis stream (`orders:ack:stream`) and having the producer consume it. This adds complexity: a new consumer group, cursor management, re-delivery handling.

A Redis Hash with `HSET`/`HMGET` is simpler:
- `HSET` with N field-value pairs: one Redis command, O(N) complexity
- `HMGET` for K fields: one Redis command, O(K) complexity
- 24h TTL on the hash: no maintenance job needed
- The hash lives on the **same Redis shard** as the stream: no extra network hop

### The Ack Buffer: Amortising Redis Writes

Writing one `HSET` per order would be 500 Redis round trips per second at our target load — identical to the per-message XACK problem we already solved. So we apply the same batch amortisation:

```python
ACK_BATCH_SIZE    = int(os.getenv("ACK_BATCH_SIZE", "500"))
ACK_FLUSH_INTERVAL = float(os.getenv("ACK_FLUSH_INTERVAL", "5.0"))

# In flush_batch, after successful DB insert:
_buf = _ack_buffers.setdefault(shard_idx, [])
_buf.extend(r[0] for r in records)   # r[0] = order_id

_now = time.monotonic()
_elapsed = _now - _ack_last_flush.get(shard_idx, 0.0)
if len(_buf) >= ACK_BATCH_SIZE or _elapsed >= ACK_FLUSH_INTERVAL:
    await client.hset(f"orders:acks:{shard_idx}", mapping={oid: str(time.time()) for oid in _buf})
    await client.expire(f"orders:acks:{shard_idx}", ACK_KEY_TTL)
    _buf.clear()
    _ack_last_flush[shard_idx] = _now
```

Two flush triggers:
- **Count**: flush when buffer reaches `ACK_BATCH_SIZE` (default 500). Under full load, this aligns with the DB batch size — every `flush_batch` call triggers an ack flush.
- **Time**: flush after `ACK_FLUSH_INTERVAL` seconds even if the buffer is small. Under low traffic (e.g., test sends, staging environments), acks would otherwise stall indefinitely.

Since asyncio is single-threaded, the module-level `_ack_buffers` dict is safe without locks. In Java (Spring Boot and Quarkus), each shard's virtual thread is the sole writer of `ackBuffers[shardIdx]` — no synchronisation needed for the same reason.

### The Producer Endpoints

**`GET /ack/status?ids=ORD-1,ORD-2,...`** — groups IDs by shard, issues one `HMGET` per shard, returns a flat map:

```python
@app.get("/ack/status")
async def ack_status(ids: str = Query(...)):
    order_ids = [i.strip() for i in ids.split(",") if i.strip()]
    shard_groups = {}
    for oid in order_ids:
        shard_groups.setdefault(get_shard(oid), []).append(oid)

    result = {}
    for shard_idx, oids in shard_groups.items():
        values = await clients[shard_idx].hmget(f"orders:acks:{shard_idx}", *oids)
        for oid, val in zip(oids, values):
            result[oid] = "acked" if val is not None else "pending"

    acked = sum(1 for v in result.values() if v == "acked")
    total = len(result)
    return {"total": total, "acked": acked, "pending": total - acked,
            "pct_complete": round(acked / total * 100, 2), "orders": result}
```

```json
{
  "total": 3, "acked": 2, "pending": 1, "pct_complete": 66.67,
  "orders": {"ORD-001": "acked", "ORD-002": "acked", "ORD-003": "pending"}
}
```

**`GET /ack/summary`** — uses `HLEN` (O(1) per shard) for a fast total count without listing IDs:

```bash
curl http://localhost:8000/ack/summary
# {"total_acked": 99843, "by_shard": [{"shard": 0, "acked": 24960}, ...]}
```

### Load Test Phase 2: Ack Polling

The load test now runs in two phases. Phase 1 sends all orders and collects the submitted `order_id`s. Phase 2 polls `/ack/status` in 1000-ID chunks until all orders confirm as acked:

```python
async def poll_ack_status(client, order_ids):
    deadline = time.monotonic() + ACK_POLL_TIMEOUT
    while time.monotonic() < deadline:
        acked = 0
        for chunk in chunks(order_ids, ACK_CHUNK_SIZE):
            r = await client.get("/ack/status", params={"ids": ",".join(chunk)})
            acked += r.json()["acked"]
        pct = acked / len(order_ids) * 100
        print(f"  {acked:,} / {len(order_ids):,}  ({pct:.2f}%)")
        if acked >= len(order_ids):
            print("  All orders acked!")
            return
        await asyncio.sleep(ACK_POLL_INTERVAL)
```

This transforms the load test from "sent N orders" to "sent N orders and confirmed all N reached Postgres."

### Testing the Ack Aggregator

Part 5 would be incomplete without tests. The ack aggregator has two surfaces to test: the consumer-side buffering and the producer-side query endpoints.

**Consumer-side tests** (`test_ack.py` — `TestAckBuffer`):

The most important invariant: *order_ids must never appear in the ack hash for orders that were not written to Postgres*. Tests verify:

```python
async def test_buffer_not_populated_when_db_fails(self, mock_db_pool, mock_redis, ...):
    """If executemany raises, buffer stays empty — no ack for unwritten orders."""
    pool, conn = mock_db_pool
    conn.executemany.side_effect = Exception("Postgres down")

    await flush_batch(pool, mock_redis, "orders:stream:0", 0, [("id-1", fields)])

    assert cons_module._ack_buffers.get(0, []) == []
    mock_redis.hset.assert_not_called()
```

Tests also cover the two flush triggers independently:

```python
async def test_hset_called_when_batch_size_reached(self, ...):
    """ACK_BATCH_SIZE=1 → flush on every successful batch."""
    with patch.object(cons_module, "ACK_BATCH_SIZE", 1), \
         patch.object(cons_module, "ACK_FLUSH_INTERVAL", 9999.0):
        await flush_batch(...)
    mock_redis.hset.assert_called_once()

async def test_hset_called_by_interval(self, ...):
    """Interval elapsed → flush even with small buffer."""
    cons_module._ack_last_flush[0] = 0.0   # pretend last flush was at boot
    with patch.object(cons_module, "ACK_BATCH_SIZE", 9999), \
         patch.object(cons_module, "ACK_FLUSH_INTERVAL", 0.0):
        await flush_batch(...)
    mock_redis.hset.assert_called_once()
```

**Producer-side tests** (`test_ack.py` — `TestAckStatusEndpoint`, `TestAckSummaryEndpoint`):

The key batching invariant — one `HMGET` per shard, not per order_id:

```python
async def test_ack_status_one_hmget_per_shard(self, ...):
    """With NUM_SHARDS=1, querying 3 ids issues exactly 1 HMGET."""
    with patch.object(prod_module, "NUM_SHARDS", 1):
        prod_module.clients = [mock_redis]
        mock_redis.hmget.return_value = ["ts", "ts", None]
        await ac.get("/ack/status", params={"ids": "ORD-1,ORD-2,ORD-3"})

    assert mock_redis.hmget.call_count == 1
```

The summary endpoint must use `HLEN` (O(1)), not `HGETALL` (O(N)):

```python
async def test_summary_calls_hlen_not_hgetall(self, ...):
    """Summary uses HLEN — fetching every field in a million-row hash would time out."""
    await ac.get("/ack/summary")
    mock_redis.hlen.assert_called()
    mock_redis.hgetall.assert_not_called()
```

Full suite: **76 tests, all passing**, no infrastructure required.

### Configuration Reference

| Parameter | Env / Property | Default | Effect |
|-----------|----------------|---------|--------|
| Ack batch size | `ACK_BATCH_SIZE` / `app.ack-batch-size` | 500 | Flush ack hash after N order_ids |
| Ack flush interval | `ACK_FLUSH_INTERVAL` / `app.ack-flush-interval-ms` | 5s | Flush after this duration (low-traffic safety net) |
| Ack key TTL | `ACK_KEY_TTL` | 86400s | Redis Hash expires after 24h — no manual cleanup |
| Max IDs per request | `ACK_MAX_IDS` | 10000 | Guards against unbounded HMGET |
| Poll timeout (load test) | `ACK_POLL_TIMEOUT` | 300s | How long to wait for 100% ack |
| Poll interval (load test) | `ACK_POLL_INTERVAL` | 3s | Seconds between poll rounds |
| Chunk size (load test) | `ACK_CHUNK_SIZE` | 1000 | Order IDs per `/ack/status` request |
| Producer URL | `PRODUCER_URL` (load test) | http://localhost:8000 | HTTP endpoint for `/orders` |
| ACK URL | `ACK_URL` (load test) | same as PRODUCER_URL | HTTP endpoint for `/ack/*` (query a different server if split deployment) |
| Netty HTTP line length | `server.netty.max-initial-line-length` (Spring Boot) | 65536 | Max bytes for HTTP request line. Increase if `/ack/status` hits HTTP 414 with large ID lists. |

**HTTP line length note**: when querying `/ack/status?ids=ORD-1,ORD-2,…` with many order IDs, the query string can exceed the HTTP server's line-length limit, returning HTTP 414 (URI Too Long). Spring Boot is configured to 64 KB by default, but this can be reduced by increasing `ACK_CHUNK_SIZE` in the load test (e.g., `ACK_CHUNK_SIZE=100` instead of the default 1000).

---

## Summary

What started as a solution to a throughput problem became a demonstration of how every layer of a production system — from queue choice to error handling to deployment topology — is a design decision with measurable consequences.

| Layer | Decision | Why |
|-------|----------|-----|
| Queue | Redis Streams | Sub-millisecond write, at-least-once via PEL, zero extra infra |
| Sharding | SHA-1 hash routing | Deterministic across all processes and restarts |
| Batching | 500 msgs per DB round trip | 50–200× insert throughput vs per-message inserts |
| Commit durability | `synchronous_commit=off` | Eliminates WAL fsync wait; PEL covers the risk window |
| Resilience | Timeout → Retry → Circuit Breaker | Transient blips don't open circuits; open circuits fail fast |
| Confirmation | Ack Aggregator (Redis Hash) | O(1) per-order lookup without Postgres query; auto-expiring |
| Testing | Injectable clock, sleep, mocked pools | Deterministic tests with zero infrastructure, run in <1s |

The architecture is live, tested across Python, Spring Boot, and Quarkus, with 76 automated tests verifying every invariant described in this series.
