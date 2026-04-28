# Skills Demonstrated — Redis Stream Sharding Pipeline

A reference index of every engineering skill exercised in this project, with concrete code examples and the reasoning behind each decision. Useful for code review, interview prep, or onboarding a new team member.

---

## Table of Contents

1. [Redis Streams](#1-redis-streams)
2. [Horizontal Sharding](#2-horizontal-sharding)
3. [Batch Processing & Throughput](#3-batch-processing--throughput)
4. [Resilience Patterns](#4-resilience-patterns)
5. [Async Python (asyncio)](#5-async-python-asyncio)
6. [FastAPI Patterns](#6-fastapi-patterns)
7. [Java — Virtual Threads](#7-java--virtual-threads)
8. [Java — Reactive (Spring WebFlux + Lettuce)](#8-java--reactive-spring-webflux--lettuce)
9. [Java — Quarkus CDI & Build-time Optimisation](#9-java--quarkus-cdi--build-time-optimisation)
10. [PostgreSQL — asyncpg & JDBC](#10-postgresql--asyncpg--jdbc)
11. [Testing — Python (pytest-asyncio)](#11-testing--python-pytest-asyncio)
12. [Testing — Java (Testcontainers)](#12-testing--java-testcontainers)
13. [Observability — Prometheus & Structured Logging](#13-observability--prometheus--structured-logging)
14. [Container & Infrastructure](#14-container--infrastructure)
15. [Producer / Consumer Deployment Split](#15-producer--consumer-deployment-split)

---

## 1. Redis Streams

**What**: Redis Streams (`XADD` / `XREADGROUP` / `XACK`) are an append-only log with consumer group semantics — the same conceptual model as Kafka, at sub-millisecond latency.

**Core commands used**:

| Command | Purpose |
|---------|---------|
| `XADD stream * field value …` | Append message, Redis auto-generates ID |
| `XGROUP CREATE stream group $ MKSTREAM` | Create consumer group; `$` = start from latest |
| `XREADGROUP group consumer STREAMS stream >` | Read new (undelivered) messages |
| `XACK stream group id1 id2 …` | Acknowledge processed messages, remove from PEL |
| `XPENDING stream group - + N` | Inspect unacknowledged messages |

**At-least-once delivery**: every message read via `XREADGROUP` enters the Pending Entries List (PEL). It stays there until `XACK` is called. If the consumer crashes before `XACK`, the message is re-delivered on reconnect — no data loss.

**XACK only after DB commit** (Python consumer, `consumer.py`):
```python
try:
    await with_resilience(_insert, ...)   # DB first
except Exception:
    return                                # XACK never called → stays in PEL

await client.xack(stream_name, GROUP_NAME, *msg_ids)  # only on success
```

**Bulk XACK** — one varargs call for the whole batch, not one call per message:
```python
# N=500 → 1 Redis round trip, not 500
await client.xack(stream_name, GROUP_NAME, *msg_ids)
```

**BUSYGROUP handling** — consumer group creation is idempotent:
```python
try:
    await client.xgroup_create(stream_name, GROUP_NAME, id="$", mkstream=True)
except Exception:
    pass  # BUSYGROUP: already exists — safe to ignore
```

---

## 2. Horizontal Sharding

**What**: One Redis instance tops out at ~150,000–200,000 stream writes/sec. Add N shards to multiply that ceiling linearly with zero coordination overhead between shards.

**Routing algorithm** — deterministic SHA-1 hash:
```python
import hashlib

def get_shard(order_id: str) -> int:
    return int(hashlib.sha1(order_id.encode()).hexdigest(), 16) % NUM_SHARDS
```

**Why SHA-1 and not Python's `hash()`?**

Python's built-in `hash()` is randomised on each interpreter start via `PYTHONHASHSEED`. With `uvicorn --workers 4` you get four separate processes with four different seeds — `hash("ORD-001")` returns a different value per worker, so the same order can land on a different shard depending on which worker handles the request. `hashlib.sha1` is deterministic across processes, machines, and restarts.

**Shard naming convention**:
```
orders:stream:0    port 6379
orders:stream:1    port 6380
orders:stream:2    port 6381
orders:stream:3    port 6382
```

**Per-order routing preserves ordering within a customer**: because `sha1(order_id)` is deterministic, the same customer's orders always land on the same shard, guaranteeing in-shard ordering.

**Independent failure domains**: a Redis instance crash on shard 2 only affects orders routing to shard 2. The other three shards continue uninterrupted. Per-shard circuit breakers enforce this isolation.

---

## 3. Batch Processing & Throughput

**What**: instead of one INSERT + one XACK per message, collect up to 500 messages, flush them in one DB round trip, and XACK all IDs in one Redis call.

```
Before (per-message):   500 DB round trips + 500 Redis XACK calls per batch
After  (batched):         1 DB round trip  +   1 Redis XACK call  per batch
```

### Python — asyncpg `executemany`

```python
await conn.execute("SET synchronous_commit = off")
await conn.executemany(
    "INSERT INTO orders (order_id, customer_id, amount, items, msg_id, shard) "
    "VALUES ($1, $2, $3, $4::jsonb, $5, $6)",
    records,   # list of tuples, one per message
)
```

### Spring Boot — `JdbcTemplate.batchUpdate`

```java
jdbcTemplate.batchUpdate(
    "INSERT INTO orders (order_id, customer_id, amount, items, msg_id, shard) "
    + "VALUES (?, ?, ?, ?::jsonb, ?, ?)",
    batchArgs   // List<Object[]>
);
```

### Quarkus — JDBC `addBatch` / `executeBatch`

```java
conn.setAutoCommit(false);
PreparedStatement ps = conn.prepareStatement(SQL);
for (Order o : orders) {
    ps.setString(1, o.orderId); // … set all params
    ps.addBatch();
}
ps.executeBatch();
conn.commit();
```

### `synchronous_commit = off`

Normally Postgres waits for the WAL to be flushed to disk before acknowledging a commit. `SET synchronous_commit = off` removes that wait. Trade-off:

- **Risk**: up to ~200 ms of committed data lost on a hard crash.
- **Mitigation**: Redis PEL retains all unacknowledged messages. On restart, the consumer replays them and reinserts. Adding `UNIQUE (order_id)` prevents duplicate rows.
- **Gain**: ~1–5 ms per commit eliminated → 10–50× throughput improvement.

---

## 4. Resilience Patterns

Implemented in `python-order-service/resilience.py`. Three patterns compose in sequence:

```
call → [circuit open?] → [attempt with timeout] → [retry on failure] → [open circuit after exhaustion]
```

### Timeout

Each individual attempt is bounded by `asyncio.wait_for`:

```python
result = await asyncio.wait_for(coro_factory(), timeout=10.0)
```

A stuck Redis/Postgres call is cancelled after 10 s. Without this, one slow dependency can block all event-loop tasks indefinitely.

### Retry with Exponential Backoff

```python
wait = 1.0  # initial delay
for attempt in range(1, max_retries + 1):
    try:
        result = await asyncio.wait_for(coro_factory(), timeout)
        circuit_breaker.on_success()
        return result
    except Exception:
        if attempt < max_retries:
            await _sleep(wait)
            wait *= 2.0   # backoff: 1s → 2s
```

Retries absorb transient blips. Only after **all** retries fail does the circuit breaker count it as one failure. This prevents the circuit from opening on brief network jitter.

### Circuit Breaker — Three States

```
CLOSED  ──(3 exhaustion events)──▶  OPEN  ──(30 s elapsed)──▶  HALF_OPEN
  ▲                                                                  │
  └─────────────────(probe success)────────────────────────────────┘
```

| State | `allow_request()` | Transition |
|-------|-------------------|-----------|
| CLOSED | always yes | 3 retry-exhaustion events → OPEN |
| OPEN | no — raises immediately | 30 s elapsed → HALF_OPEN |
| HALF_OPEN | yes (one probe) | success → CLOSED / failure → OPEN |

```python
@property
def state(self) -> str:
    if self._state == "open":
        elapsed = self._clock() - self._opened_at
        if elapsed >= self.recovery_timeout:
            self._state = "half_open"
    return self._state
```

**Injectable clock** (`_clock`) enables time-travel in tests without `sleep`:
```python
tick = 0.0
cb = CircuitBreaker("t", recovery_timeout=30, _clock=lambda: tick)
cb.on_failure()
tick = 31.0          # advance past recovery window
assert cb.state == "half_open"  # instant — no real sleep needed
```

**Injectable sleep** (`_sleep`) speeds up retry tests:
```python
await with_resilience(flaky, ..., _sleep=AsyncMock())  # waits 0 ms in tests
```

**Separate breakers per concern**: `db_breaker` and per-shard `redis_breakers` are independent. A Postgres outage does not affect Redis health tracking and vice versa.

---

## 5. Async Python (asyncio)

### asyncio.gather — Fan-out per shard

```python
await asyncio.gather(*(consume_shard(i, clients[i], db_pool) for i in range(NUM_SHARDS)))
```

Each shard runs as an independent coroutine on the same event loop. `asyncio.gather` starts all of them concurrently and waits for all to complete (or any to raise).

### asyncio.wait_for — Per-attempt deadline

```python
result = await asyncio.wait_for(coro_factory(), timeout=10.0)
```

Raises `asyncio.TimeoutError` if the coroutine does not complete within the deadline. The coroutine is cancelled automatically.

### asyncpg async context manager pattern

```python
async with db_pool.acquire() as conn:
    await conn.execute("SET synchronous_commit = off")
    await conn.executemany(SQL, records)
```

`pool.acquire()` checks out a connection and returns it to the pool when the `async with` block exits, even on exception.

### asyncio.Semaphore — concurrency cap in load tests

```python
sem = asyncio.Semaphore(CONCURRENCY)

async def send(order):
    async with sem:
        await client.post("/orders", json=order)
```

Without the semaphore, `asyncio.gather(*[send(o) for o in orders])` would open `TOTAL_ORDERS` connections simultaneously, exhausting file descriptors.

---

## 6. FastAPI Patterns

### Lifespan context manager (replaces deprecated `@app.on_event`)

```python
from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    # startup: runs before first request
    clients = [redis.from_url(url) for url in REDIS_URLS]
    yield
    # shutdown: runs after last request
    for c in clients:
        await c.aclose()

app = FastAPI(lifespan=lifespan)
```

`@app.on_event("startup")` / `@app.on_event("shutdown")` are deprecated since FastAPI 0.93.

### Pydantic model validation

```python
class Order(BaseModel):
    order_id: str
    customer_id: str
    amount: float
    items: list[str]
```

Pydantic validates the request body before the handler runs. Missing or wrong-type fields return `422 Unprocessable Entity` automatically — the handler never sees invalid data.

### JSONResponse for error codes

```python
except CircuitBreakerOpenError as e:
    return JSONResponse(status_code=503, content={
        "error": "service_unavailable",
        "detail": str(e),
    })
```

Returning `JSONResponse` with an explicit status code is correct for error paths in FastAPI. Raising `HTTPException` is equivalent but less readable for multi-step error handling.

### ASGI health and metrics endpoints

```python
@app.get("/health")
async def health():
    states = {cb.name: cb.state for cb in redis_breakers}
    healthy = all(s != "open" for s in states.values())
    return JSONResponse(status_code=200 if healthy else 503, content={...})

@app.get("/metrics")
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
```

`/health` is a Kubernetes readiness probe target. `/metrics` is a Prometheus scrape target. Both are standard endpoints any production service should expose.

---

## 7. Java — Virtual Threads

Java 21 virtual threads (Project Loom) allow millions of concurrent tasks with ~200 bytes of heap each, vs ~1 MB per platform thread. Key patterns in this project:

### Spawning one virtual thread per shard (Spring Boot)

```java
for (int i = 0; i < numShards; i++) {
    final int shard = i;
    Thread.ofVirtual().name("consumer-shard-" + shard).start(() -> consumeShard(shard));
}
```

### Quarkus `@RunOnVirtualThread`

```java
@POST
@RunOnVirtualThread
public Response createOrder(Order order) {
    // blocking Lettuce sync().xadd() runs on a virtual thread — safe to block
    String msgId = commands.xadd(streamKey, fields);
    return Response.ok(Map.of("status", "queued", "message_id", msgId)).build();
}
```

Blocking a virtual thread parks it cheaply (no OS thread blocked). This lets you write synchronous code while getting reactive-level concurrency.

### Why virtual threads beat platform threads for I/O-bound work

| | Platform thread | Virtual thread |
|---|---|---|
| Memory per thread | ~1 MB stack | ~200–500 bytes heap |
| OS scheduling | yes (expensive) | no (JVM-managed) |
| Max concurrent | ~10,000 | ~millions |
| Code style | blocking OK | blocking OK |

### JVM tuning (`start.sh`)

```bash
-XX:+UseZGC -XX:+ZGenerational    # sub-millisecond GC pauses
-XX:+UseNUMA                      # NUMA-aware memory allocation
-Xms${HEAP}m -Xmx${HEAP}m        # equal min/max: no heap resize pauses
-XX:+AlwaysPreTouch               # pre-fault heap pages at startup
```

---

## 8. Java — Reactive (Spring WebFlux + Lettuce)

### WebFlux — non-blocking HTTP

Spring WebFlux runs on Netty (not Tomcat). No thread-per-request — instead, a small pool of event-loop threads handles all in-flight requests via callbacks. This saturates the network before saturating the thread pool.

### Lettuce — reactive Redis client

```java
// Reactive Lettuce: one TCP connection multiplexes all in-flight XADD calls
commands.xadd(streamKey, XAddArgs.Builder.maxlen(1_000_000).approximateTrimming(), fields)
```

Lettuce uses `CompletableFuture` / Reactor `Mono` internally. Multiple concurrent `XADD` calls share one TCP connection via pipelining — no connection-per-request overhead.

### `XReadArgs.StreamOffset` (Lettuce 6.3.x API)

```java
// Correct — nested static class
XReadArgs.StreamOffset.lastConsumed(streamName)

// Wrong — does not exist as top-level class in 6.3.x
StreamOffset.lastConsumed(streamName)
```

### `@ConditionalOnProperty` — producer/consumer mode separation

```java
@Component
@ConditionalOnProperty(name = "app.consumer.enabled", havingValue = "true", matchIfMissing = false)
public class ShardedConsumer implements ApplicationRunner { … }
```

The consumer bean is only created when `app.consumer.enabled=true`. In producer-only mode, no consumer threads start.

---

## 9. Java — Quarkus CDI & Build-time Optimisation

### AgroalDataSource injection

```java
@Inject
AgroalDataSource dataSource;   // correct for Quarkus 3.x
```

Quarkus registers the datasource CDI bean under `io.agroal.api.AgroalDataSource`. Injecting `javax.sql.DataSource` is unreliable — Quarkus validates injection points at build time, not runtime.

### `@ApplicationScoped` + `@PostConstruct` for Redis clients

```java
@ApplicationScoped
public class RedisConfig {
    private final List<StatefulRedisConnection<String,String>> connections = new ArrayList<>();

    @PostConstruct
    void init() {
        for (String url : redisUrls) {
            connections.add(RedisClient.create(url).connect());
        }
    }
}
```

CDI `@PostConstruct` runs after injection is complete. `@ApplicationScoped` means one instance for the life of the app — Redis connections are shared across all requests.

### `@ConfigProperty` — runtime configuration

```java
@ConfigProperty(name = "app.num-shards", defaultValue = "4")
int numShards;

@ConfigProperty(name = "app.producer.enabled", defaultValue = "true")
boolean producerEnabled;
```

Quarkus resolves these from `application.properties`, environment variables, or `-D` flags at startup.

### `QuarkusTestResourceLifecycleManager` — Testcontainers integration

```java
public class RedisPostgresResource implements QuarkusTestResourceLifecycleManager {
    @Override
    public Map<String, String> start() {
        redis.start();
        postgres.start();
        // Pre-create consumer group so test messages land before app starts
        redis.execInContainer("redis-cli", "XGROUP", "CREATE", "orders:stream:0",
                              "order_processors", "0-0", "MKSTREAM");
        return Map.of(
            "redis.urls", redis.getRedisURI(),
            "quarkus.datasource.jdbc.url", postgres.getJdbcUrl()
        );
    }
}
```

`start()` runs before Quarkus boots, injecting real container URLs into the config. This is the Quarkus equivalent of Spring Boot's `@DynamicPropertySource`.

---

## 10. PostgreSQL — asyncpg & JDBC

### JSONB for the items array

```sql
CREATE TABLE orders (
    id          BIGSERIAL PRIMARY KEY,
    order_id    TEXT NOT NULL,
    customer_id TEXT NOT NULL,
    amount      NUMERIC(12, 2) NOT NULL,
    items       JSONB NOT NULL,           -- supports GIN indexing, JSON queries
    msg_id      TEXT,
    shard       INT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
```

`JSONB` stores the items list as binary JSON — efficient storage, supports GIN indexes for containment queries (`items @> '["Laptop"]'`), and avoids schema migrations when item structure changes.

### `synchronous_commit = off`

| Setting | WAL flush | Commit ACK | Risk |
|---------|-----------|------------|------|
| `on` (default) | before ACK | after fsync | none |
| `off` | after ACK | before fsync | last ~200 ms on hard crash |

Set at session level so only the consumer connection opts in:
```python
await conn.execute("SET synchronous_commit = off")
```

Or at pool level via `connection-init-sql` (HikariCP / Agroal):
```yaml
connection-init-sql: "SET synchronous_commit = off"
```

### `UNIQUE (order_id)` — idempotent inserts

If XACK fails after a successful DB write, the message is re-delivered and `flush_batch` runs again. Without a unique constraint the order row is duplicated. Adding `UNIQUE (order_id)` turns re-delivery into an `ON CONFLICT DO NOTHING` scenario:

```sql
INSERT INTO orders (order_id, …) VALUES (…)
ON CONFLICT (order_id) DO NOTHING
```

---

## 11. Testing — Python (pytest-asyncio)

### `pytest.ini` — async mode

```ini
[pytest]
asyncio_mode = auto
testpaths = tests
```

`asyncio_mode = auto` removes the need to mark every async test with `@pytest.mark.asyncio`.

### `AsyncMock` — awaitable method mocks

```python
client = AsyncMock()
client.xadd.return_value = "1714000000000-0"
# client.xadd(...) is now awaitable — no real Redis needed
```

`AsyncMock` makes every attribute and return value awaitable by default, matching how `redis.asyncio.Redis` behaves.

### `ASGITransport` — in-process HTTP testing

```python
from httpx import AsyncClient, ASGITransport

async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
    response = await ac.post("/orders", json=order)
```

No network socket opened. Requests go directly into FastAPI's ASGI handler — tests run at Python function call speed, no uvicorn process needed.

### `autouse` fixture — state reset between tests

```python
@pytest.fixture(autouse=True)
def init_circuit_breakers():
    import producer as prod_module
    import consumer as cons_module
    prod_module.redis_breakers = [CircuitBreaker(f"redis-shard-{i}") for i in range(4)]
    cons_module.db_breaker = CircuitBreaker("postgres")
    cons_module.redis_breakers = [CircuitBreaker(f"redis-xack-{i}") for i in range(4)]
    yield
    for b in prod_module.redis_breakers: b.reset()
    cons_module.db_breaker.reset()
    for b in cons_module.redis_breakers: b.reset()
```

`autouse=True` runs this fixture before every test without any explicit declaration. It prevents state bleed (an opened circuit in test N affecting test N+1) and ensures the module-level breaker lists are populated before any test accesses them by index.

### Injectable dependencies for deterministic tests

**Injectable clock** — test circuit breaker recovery without sleeping:
```python
tick = 0.0
cb = CircuitBreaker("t", recovery_timeout=30, _clock=lambda: tick)
cb.on_failure()
tick = 31.0              # time travel
assert cb.state == "half_open"
```

**Injectable sleep** — test retry backoff without waiting:
```python
sleep = AsyncMock()
await with_resilience(flaky, ..., _sleep=sleep)
assert sleep.call_args_list == [call(1.0), call(2.0)]
```

### `asyncpg` pool mock pattern

```python
pool = MagicMock()
conn = AsyncMock()
acquire_ctx = AsyncMock()
acquire_ctx.__aenter__ = AsyncMock(return_value=conn)
acquire_ctx.__aexit__ = AsyncMock(return_value=False)
pool.acquire.return_value = acquire_ctx
```

This replicates the `async with pool.acquire() as conn:` pattern so `flush_batch` can be tested without a real database.

### Test class organisation

```
TestFlushBatch         — processing kernel unit tests
TestCreateTable        — DDL idempotency
TestConsumeShard       — loop-level behaviour (BUSYGROUP, batch dispatch)
TestCircuitBreakerStates  — state machine transitions
TestCircuitBreakerRecovery — OPEN → HALF_OPEN → CLOSED via _clock
TestWithResilienceRetry    — backoff timing
TestWithResilienceTimeout  — timeout triggers retry
TestWithResilienceCircuit  — exhaustion opens circuit
TestProducerCircuitBreakerIntegration — HTTP 503 on open circuit
TestConsumerCircuitBreakerIntegration — DB open → no XACK
```

Industry convention: group related tests into classes, name classes after the unit under test, name test methods after the scenario (`test_<what>_<when>_<expected_outcome>`).

---

## 12. Testing — Java (Testcontainers)

### `@Testcontainers` + `@Container` (Spring Boot)

```java
@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class OrderServiceIntegrationTest {
    @Container
    static GenericContainer<?> redis = new GenericContainer<>("redis:7-alpine")
        .withExposedPorts(6379)
        .withCommand("redis-server", "--requirepass", "123456");

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        // Pre-create consumer group BEFORE Spring context starts
        redis.execInContainer("redis-cli", "-a", "123456", "XGROUP", "CREATE",
                              "orders:stream:0", "order_processors", "0-0", "MKSTREAM");
        r.add("spring.data.redis.host", redis::getHost);
        r.add("spring.datasource.url", postgres::getJdbcUrl);
    }
}
```

### Awaitility — async assertion without polling sleep

```java
await().atMost(10, SECONDS).untilAsserted(() -> {
    Integer count = jdbcTemplate.queryForObject(
        "SELECT COUNT(*) FROM orders WHERE order_id = ?", Integer.class, orderId);
    assertThat(count).isEqualTo(1);
});
```

`await().untilAsserted()` polls the lambda until it passes or times out. Never use `Thread.sleep(N)` in integration tests — it either slows the suite or flaps under load.

### Rootless Podman configuration

```properties
# src/test/resources/testcontainers.properties
docker.host=unix:///run/user/1000/podman/podman.sock
ryuk.disabled=true
```

Ryuk requires privileged socket access that rootless Podman does not grant. With `ryuk.disabled=true`, container lifecycle is managed by `@Container` / `QuarkusTestResourceLifecycleManager.stop()` callbacks instead.

### Consumer group pre-creation race condition

The consumer creates its group at offset `$` (latest) on startup. If a test message is published *before* the group exists, the message falls before `$` and is never delivered. Fix: pre-create the group at `0-0` in the test setup so all messages published during the test are visible:

```bash
XGROUP CREATE orders:stream:0 order_processors 0-0 MKSTREAM
```

---

## 13. Observability — Prometheus & Structured Logging

### Prometheus metrics (Python)

```python
from prometheus_client import Counter, Histogram, Gauge

ORDERS_PUBLISHED = Counter("orders_published_total", "…", ["shard"])
REQUEST_LATENCY  = Histogram("http_request_duration_seconds", "…", ["status_code"],
                             buckets=[.001, .005, .01, .025, .05, .1, .25, .5, 1.0])
CB_STATE         = Gauge("producer_circuit_breaker_state", "…", ["name"])
```

- **Counter** — monotonically increasing. Use for events: requests, errors, rows inserted.
- **Histogram** — tracks distribution. Use for latencies and sizes. `histogram_quantile(0.99, rate(...[1m]))` gives p99 in Grafana.
- **Gauge** — can go up or down. Use for state: queue depth, circuit breaker state, connection pool size.

Producer exposes `/metrics` via FastAPI. Consumer uses `start_http_server(9090)` (a separate thread, non-blocking relative to the asyncio event loop).

### Structured JSON logging

```python
class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        return json.dumps({
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        })
```

One JSON object per line. Log aggregators (Loki, Fluentd, CloudWatch) parse structured fields without regex, enabling queries like `{logger="consumer"} |= "circuit open"`.

### Health endpoint pattern

```python
@app.get("/health")
async def health():
    states = {cb.name: cb.state for cb in redis_breakers}
    healthy = all(s != "open" for s in states.values())
    return JSONResponse(
        status_code=200 if healthy else 503,
        content={"status": "ok" if healthy else "degraded", "circuit_breakers": states},
    )
```

Kubernetes liveness/readiness probes call `/health`. HTTP 503 → pod removed from load balancer rotation until circuit recovers.

### Key Grafana queries

```promql
# Orders per second
rate(orders_published_total[1m])

# Error rate
rate(orders_failed_total[1m])

# p99 latency
histogram_quantile(0.99, rate(http_request_duration_seconds_bucket[1m]))

# Consumer insert rate
rate(consumer_rows_inserted_total[1m])

# Circuit breaker state (0=ok, 1=probing, 2=open)
producer_circuit_breaker_state
consumer_circuit_breaker_state
```

---

## 14. Container & Infrastructure

### compose.yaml — multi-service stack

```yaml
services:
  redis-0:
    image: redis:7-alpine
    ports: ["6379:6379"]
    command: redis-server --requirepass 123456

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

Single `podman compose up -d` starts the entire development stack. `depends_on` and `healthcheck` ensure Redis and Postgres are ready before the app starts.

### Rootless Podman

Rootless containers run as the current user with no root privileges — better security posture than Docker daemon running as root. Key differences:

- Podman socket at `/run/user/<uid>/podman/podman.sock` (not `/var/run/docker.sock`)
- Enable with: `systemctl --user enable --now podman.socket`
- `TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE` or `~/.testcontainers.properties` for Java

### `.gitignore` — multi-project monorepo

```gitignore
# Java build output
**/target/
**/*.class
**/*.jar

# Python venv
bin/
lib/
lib64/
include/
pyvenv.cfg
__pycache__/
*.pyc

# IDE and secrets
.env
*.log
```

A single root `.gitignore` covers all three subprojects. `**` matches any depth.

---

## 15. Producer / Consumer Deployment Split

### Why split

In production, producers and consumers have different scaling characteristics:

- **Producers**: HTTP-bound — scale horizontally with request rate
- **Consumers**: CPU/DB-bound — scale with message backlog and DB write throughput

Running them in separate pods lets you scale each axis independently.

### `start.sh` MODE parameter

```bash
MODE=${1:-both}

case "$MODE" in
  producer) EXTRA_OPTS="-Dapp.consumer.enabled=false" ;;
  consumer) EXTRA_OPTS="-Dapp.producer.enabled=false" ;;
  both)     EXTRA_OPTS="" ;;
esac
```

| Mode | HTTP `/orders` | Consumer threads |
|------|---------------|-----------------|
| `both` | active | active |
| `producer` | active | disabled |
| `consumer` | returns 503 | active |

### Competing consumers — horizontal consumer scale

Multiple consumer instances can read from the same consumer group simultaneously. Redis distributes messages across all active consumers in the group — no messages are delivered to more than one consumer. Each instance must have a unique `CONSUMER_NAME`:

```bash
# Terminal 1
CONSUMER_NAME=worker-1 python consumer.py

# Terminal 2 — reads different messages from the same group
CONSUMER_NAME=worker-2 python consumer.py
```

Redis tracks each consumer's cursor independently via the PEL.

---

## Summary — Skills by Category

| Category | Skills |
|---|---|
| **Message Queue** | Redis Streams, consumer groups, PEL, at-least-once delivery, BUSYGROUP |
| **Sharding** | SHA-1 hash routing, shard independence, per-shard circuit breakers |
| **Performance** | Batch inserts, bulk XACK, `synchronous_commit=off`, connection pooling |
| **Resilience** | Circuit Breaker (CLOSED/OPEN/HALF_OPEN), exponential backoff retry, per-attempt timeout |
| **Async Python** | asyncio, asyncpg, redis-py async, `gather`, `wait_for`, `Semaphore` |
| **FastAPI** | Lifespan, Pydantic validation, JSONResponse, ASGI transport, health/metrics endpoints |
| **Java Concurrency** | Virtual threads (Java 21), WebFlux Netty, Lettuce reactive, `Thread.ofVirtual` |
| **Java Frameworks** | Spring Boot 3.3 (WebFlux, HikariCP, JdbcTemplate), Quarkus 3.10 (CDI, Agroal, RESTEasy) |
| **PostgreSQL** | asyncpg, JDBC batch, JSONB, `synchronous_commit=off`, idempotent DDL |
| **Testing (Python)** | pytest-asyncio, AsyncMock, ASGITransport, autouse fixtures, injectable clock/sleep |
| **Testing (Java)** | Testcontainers, Awaitility, `@DynamicPropertySource`, `QuarkusTestResourceLifecycleManager` |
| **Observability** | Prometheus (Counter/Histogram/Gauge), structured JSON logging, `/health`, `/metrics` |
| **Infrastructure** | Rootless Podman, compose.yaml, `.gitignore`, Testcontainers with Podman socket |
| **Deployment** | Producer/consumer split, competing consumers, JVM tuning (ZGC, NUMA, heap pre-touch) |
