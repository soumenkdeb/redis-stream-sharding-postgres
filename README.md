# Redis Stream Sharding — High-Throughput Order Processing

A horizontally-sharded order ingestion pipeline built on Redis Streams and PostgreSQL, implemented in three runtimes: **Python (FastAPI)**, **Spring Boot (WebFlux)**, and **Quarkus**. Designed to sustain hundreds of thousands of deal bookings per second with sub-millisecond producer latency.

> **Learning reference**: see [SKILLS.md](SKILLS.md) for a detailed breakdown of every engineering skill demonstrated in this project — Redis Streams, sharding, resilience patterns, async Python, virtual threads, Testcontainers, Prometheus observability, and more.

---

## Why This Architecture Is Fast for Booking Deals

In high-frequency trading, flash sales, ticket platforms, and financial deal booking, the bottleneck is almost never the compute — it is the serialisation of writes through a single queue or database. This pipeline eliminates that bottleneck at every layer.

### Sharded Redis Streams

A single Redis instance at typical cloud VM sizing can handle roughly 100,000–200,000 stream writes per second before CPU or network saturation. With 4 shards, that ceiling multiplies linearly. The producer routes each order to a shard deterministically by hashing the `order_id`, which means:

- No coordination overhead between shards — each shard is completely independent.
- Ordering is preserved per `order_id` (same customer's orders always land on the same shard).
- Horizontal scale is additive: 8 shards = 8× throughput ceiling.

### Why Redis Streams Over Kafka/RabbitMQ Here

| Property | Redis Streams | Kafka | RabbitMQ |
|---|---|---|---|
| Write latency (p99) | < 1 ms | 5–20 ms | 2–10 ms |
| Ops overhead | Near zero | High (ZK/KRaft) | Medium |
| Consumer groups | Yes | Yes | Limited |
| At-least-once delivery | Yes (`XACK`) | Yes | Yes |
| Replay | Yes (up to `MAXLEN`) | Yes (unlimited) | No |

Redis Streams give Kafka-level semantics at single-digit millisecond latency with zero cluster management overhead, making them ideal for latency-sensitive deal booking where speed of confirmation matters.

### Why PostgreSQL Over a Pure NoSQL Store

Deals require ACID guarantees. A deal booked twice, or a partial write that succeeds on Redis but not on the ledger, is a compliance and financial risk. PostgreSQL with JSONB (for the items array) gives:

- Row-level locking for idempotent inserts.
- Full SQL audit queries over the `orders` table.
- JSONB indexing for fast item-level analytics without schema migrations.

The async consumer pattern decouples the HTTP write path (Redis, sub-millisecond) from the durable write path (PostgreSQL, ~5 ms) so neither blocks the other.

---

## Architecture Overview

```
                          ┌─────────────────────────────────────────┐
                          │           Producer (HTTP API)            │
                          │  POST /orders  →  hash(order_id) % 4   │
                          │  GET  /ack/status?ids=…                 │
                          │  GET  /ack/summary                      │
                          └──────────┬──────┬──────┬──────┬─────────┘
                                     │      │      │      │
                                  shard:0 shard:1 shard:2 shard:3
                                     │      │      │      │
                          ┌──────────▼──────▼──────▼──────▼─────────┐
                          │           4 × Redis Stream               │
                          │   orders:stream:0  …  orders:stream:3   │
                          │   orders:acks:0    …  orders:acks:3     │
                          │   port 6379        …  port 6382         │
                          └──────────┬──────┬──────┬──────┬─────────┘
                                     │      │      │      │
                          ┌──────────▼──────▼──────▼──────▼─────────┐
                          │    Consumer (4 virtual threads)          │
                          │    XREADGROUP → INSERT → XACK            │
                          │    → buffer order_ids → HSET acks hash  │
                          └───────────────────┬─────────────────────┘
                                              │
                          ┌───────────────────▼─────────────────────┐
                          │          PostgreSQL  orders_db           │
                          │    orders table (BIGSERIAL, JSONB)       │
                          └─────────────────────────────────────────┘
```

---

## Projects

### 1. Python — `python-order-service/`

**Runtime**: Python 3.12+, FastAPI, asyncio, asyncpg, redis-py async

**Producer**: FastAPI async endpoint with `lifespan` context manager. One async Redis client per shard; shard routing uses `hashlib.sha1` for a stable hash that is consistent across all `uvicorn --workers N` processes (Python's built-in `hash()` is randomised per process via `PYTHONHASHSEED` — using it would route the same `order_id` to different shards in different workers).

**Consumer**: `asyncio.gather` launches one coroutine per shard. Each coroutine pulls up to 500 messages per `XREADGROUP` call, inserts them all via `asyncpg.executemany()` in one round trip (`SET synchronous_commit=off` removes the WAL fsync wait), then XACK's all IDs in a single varargs Redis command. XACK is only called after a successful DB write — on failure the batch is retried from the PEL. After each successful DB commit, `order_id`s are buffered and flushed to `orders:acks:{shard}` (Redis Hash, TTL 24 h) when `ACK_BATCH_SIZE` is reached or `ACK_FLUSH_INTERVAL` seconds elapse.

**Throughput**: ~30,000–80,000 orders/sec through the HTTP producer (uvicorn worker-bound). Direct Redis writes via `load_test_sharded.py` reach 300,000–600,000/sec, bypassing FastAPI entirely. The GIL limits true parallelism on CPU-bound work but the I/O-bound nature of this pipeline means asyncio largely sidesteps it.

**Tests**: 58 tests passing — `test_sharding.py` (hash stability, range, distribution), `test_producer.py` (HTTP contract, shard routing, input validation with mocked Redis), `test_consumer.py` (insert correctness, XACK ordering, error isolation, idempotent DDL), `test_resilience.py` (circuit breaker state machine, retry backoff, timeout, producer/consumer integration).

### 2. Spring Boot — `spring-boot-order-service/`

**Runtime**: Java 21, Spring Boot 3.3, WebFlux (Netty), Lettuce 6.3, HikariCP, JDBC

**Producer**: WebFlux `@RestController` on a Netty event loop. Reactive Lettuce multiplexes all in-flight `XADD` calls over a single TCP connection per shard — no thread-per-request, no head-of-line blocking.

**Consumer**: `ApplicationRunner` spawns one virtual thread per shard. Each thread pulls up to 500 messages per poll (`XReadArgs.Builder.count(500)`), inserts the full batch via `JdbcTemplate.batchUpdate()` in one DB round trip, then XACK's all IDs in one Lettuce varargs call. `synchronous_commit=off` is set via HikariCP `connection-init-sql`, removing the WAL fsync overhead per commit. After XACK, `order_id`s are buffered and flushed to `orders:acks:{shard}` via `HSET` when `app.ack-batch-size` (default 500) is reached or `app.ack-flush-interval-ms` (default 5 000 ms) elapses.

**Throughput**: ~150,000–400,000 orders/sec producer side under load. Consumer throughput is linear with shard count.

**Tests**: Verified passing — `ShardingStrategyTest` (unit), `OrderControllerTest` (`@WebFluxTest` + mocked Lettuce), `OrderServiceIntegrationTest` (`@SpringBootTest` + Testcontainers Redis + PostgreSQL, end-to-end including XACK verification).

### 3. Quarkus — `quarkus-order-service/`

**Runtime**: Java 21, Quarkus 3.10, RESTEasy Reactive (Vert.x), Lettuce 6.3, Agroal JDBC pool

**Producer**: `@RunOnVirtualThread` on the JAX-RS endpoint dispatches each request to a fresh virtual thread. Blocking Lettuce `sync().xadd()` runs on that virtual thread — simple, readable, and just as efficient as reactive code because virtual thread parking costs ~200 bytes of heap vs ~1 MB for a platform thread stack.

**Consumer**: `@Observes StartupEvent` spawns one virtual thread per shard. Pulls up to 500 messages per poll, batches them via JDBC `addBatch()`/`executeBatch()` inside an explicit transaction (`setAutoCommit(false)` + `commit()`), then XACK's all IDs in one varargs call. `synchronous_commit=off` is set via `quarkus.datasource.jdbc.new-connection-sql`. On DB failure the transaction is rolled back and XACK is skipped — messages stay in the PEL. Ack buffering follows the same pattern as Spring Boot: `HSET orders:acks:{shard}` flushed at `app.ack-batch-size` / `app.ack-flush-interval-ms`.

**Throughput**: Comparable to Spring Boot. Quarkus has a faster cold start (~0.4 s vs ~2 s for Spring Boot) which matters in auto-scaling environments where new pods spin up under surge load.

**Tests**: `ShardingStrategyTest` (unit), `OrderResourceTest` (`@QuarkusTest` + `@QuarkusTestResource` with Testcontainers Redis + PostgreSQL). DB assertions use `DriverManager` directly to avoid Quarkus CDI augmentation-time validation of injected datasources in test classes.

---

## Repository Layout

```
.
├── SKILLS.md                            # Engineering skills index with code examples
├── docs/
│   └── screenshots/
│       ├── pgadmin4.png                 # pgAdmin4 dashboard with live activity graphs
│       ├── redis-insights-stream-data.png      # RedisInsight stream entries view
│       └── redis-insights-consumer-groups.png  # RedisInsight consumer group pending counts
├── compose.yaml                         # All infrastructure (4 Redis, Postgres, pgAdmin, RedisInsight)
├── .env                                 # Runtime config (URLs, DSN)
├── python-order-service/
│   ├── producer.py                      # FastAPI producer + /ack/status + /ack/summary endpoints
│   ├── consumer.py                      # asyncio consumer (batch inserts, XACK, ack buffer → Redis)
│   ├── resilience.py                    # CircuitBreaker + with_resilience (timeout, retry, CB)
│   ├── load_test_sharded.py             # Direct Redis load test: bypasses producer, 1M orders
│   ├── load_test_producer.py            # HTTP load test: Phase 1 send + Phase 2 ack poll
│   ├── requirements.txt                 # Runtime dependencies
│   ├── requirements-dev.txt             # Test dependencies (pytest, pytest-asyncio, httpx)
│   ├── pytest.ini                       # asyncio_mode=auto, testpaths=tests
│   └── tests/
│       ├── conftest.py                  # Shared fixtures: mock Redis, mock asyncpg pool, CB init
│       ├── test_sharding.py             # Shard range, determinism, distribution (11 tests)
│       ├── test_producer.py             # HTTP contract + routing with mocked Redis (14 tests)
│       ├── test_consumer.py             # flush_batch, create_table, consume_shard (10 tests)
│       └── test_resilience.py           # CB state machine, retry backoff, timeout (29 tests)
├── spring-boot-order-service/
│   ├── pom.xml                          # Java 21, WebFlux, Lettuce, JDBC + test deps
│   ├── start.sh                         # Tuned JVM startup (ZGC, virtual thread pool)
│   └── src/
│       ├── main/java/com/example/orders/
│       │   ├── OrderServiceApplication.java
│       │   ├── config/RedisConfig.java  # One StatefulRedisConnection per shard
│       │   ├── model/Order.java
│       │   ├── producer/OrderController.java
│       │   ├── producer/AckController.java  # GET /ack/status, GET /ack/summary
│       │   └── consumer/ShardedConsumer.java
│       ├── main/resources/application.yml
│       └── test/
│           ├── java/com/example/orders/
│           │   ├── ShardingStrategyTest.java         # pure unit
│           │   ├── producer/OrderControllerTest.java  # @WebFluxTest + mocked Redis
│           │   └── OrderServiceIntegrationTest.java   # Testcontainers end-to-end
│           └── resources/testcontainers.properties    # Podman socket + ryuk config
└── quarkus-order-service/
    ├── pom.xml                          # Java 21, Quarkus 3.10, RESTEasy, Lettuce, Agroal + test deps
    ├── start.sh                         # Tuned JVM startup (ZGC, virtual thread pool)
    └── src/
        ├── main/java/com/example/orders/
        │   ├── config/RedisConfig.java  # @ApplicationScoped, @PostConstruct init
        │   ├── model/Order.java
        │   ├── producer/OrderResource.java
        │   ├── producer/AckResource.java    # GET /ack/status, GET /ack/summary
        │   └── consumer/ShardedConsumer.java
        ├── main/resources/application.properties
        └── test/
            ├── java/com/example/orders/
            │   ├── ShardingStrategyTest.java              # pure unit
            │   ├── infrastructure/RedisPostgresResource.java  # QuarkusTestResourceLifecycleManager
            │   └── producer/OrderResourceTest.java        # @QuarkusTest end-to-end
            └── resources/testcontainers.properties        # Podman socket + ryuk config
```

---

## Setup

### Prerequisites

- Docker **or** Podman with Compose support (see [Running on Podman](#running-on-podman))
- Java 21+ (`java -version`)
- Maven 3.9+ (`mvn -version`)
- Python 3.12+ with venv

### 1. Start Infrastructure

```bash
podman compose up -d
# or
docker compose up -d
```

This starts:
- `redis-0` → `localhost:6379`
- `redis-1` → `localhost:6380`
- `redis-2` → `localhost:6381`
- `redis-3` → `localhost:6382`
- `postgres-db` → `localhost:5432` (db: `orders_db`, user: `postgres`, password: `postgres123`)
- `pgadmin` → `http://localhost:8080` (email: `admin@local.dev`, password: `admin123`)
- `redis-insight` → `http://localhost:5540`

Wait for all services to be healthy:
```bash
podman compose ps
```

### 2. Python Producer & Consumer

```bash
cd python-order-service
python -m venv .
source bin/activate          # fish: source bin/activate.fish
pip install -r requirements.txt

# Terminal 1: producer (workers = CPU cores for maximum throughput)
uvicorn producer:app --host 0.0.0.0 --port 8000 --workers 4

# Terminal 2: consumer
python consumer.py

# Override shard count or consumer name at runtime
NUM_SHARDS=8 CONSUMER_NAME=worker-2 python consumer.py
```

### 3. Spring Boot

```bash
cd spring-boot-order-service
mvn package -DskipTests

# Both producer + consumer on port 8000 (default)
./start.sh

# Producer only on port 8000 — consumer threads disabled
./start.sh producer

# Consumer only on port 8002 — HTTP returns 503, consumer threads active
./start.sh consumer

# Override properties at runtime
./start.sh both -Dapp.num-shards=8 -Dapp.consumer-name=worker-1

# Ports
# - Producer (POST /orders, GET /ack/*): port 8000
# - Consumer (processes streams, writes ACKs): port 8002
# - When both: port 8000
```

### 4. Quarkus

```bash
cd quarkus-order-service
mvn package -DskipTests

# Both producer + consumer (default)
./start.sh

# Producer only — consumer threads disabled
./start.sh producer

# Consumer only — HTTP returns 503, consumer threads active
./start.sh consumer

# Override properties at runtime
./start.sh consumer -Dapp.num-shards=8 -Dapp.consumer-name=worker-2

# API available at http://localhost:8001
```

### 5. Send a Test Order

```bash
curl -s -X POST http://localhost:8000/orders \
  -H 'Content-Type: application/json' \
  -d '{"order_id":"ORD-001","customer_id":"CUST-42","amount":299.99,"items":["Laptop","Mouse"]}'
```

Expected response:
```json
{"status": "queued", "message_id": "1714123456789-0", "shard": "2"}
```

Check health, metrics, and ack status (Python producer):
```bash
curl http://localhost:8000/health    # circuit breaker states
curl http://localhost:8000/metrics   # Prometheus scrape
curl http://localhost:9090/metrics   # consumer metrics

# Check whether a specific order reached Postgres
curl "http://localhost:8000/ack/status?ids=ORD-001"
# {"total":1,"acked":1,"pending":0,"pct_complete":100.0,"orders":{"ORD-001":"acked"}}

# Bulk check (comma-separated, max 10 000 ids)
curl "http://localhost:8000/ack/status?ids=ORD-001,ORD-002,ORD-003"

# Total acked count per shard (no ID list needed)
curl http://localhost:8000/ack/summary
# {"total_acked":47823,"by_shard":[{"shard":0,"acked":11982},…]}
```

Spring Boot exposes the same endpoints on port 8000; Quarkus on port 8001:
```bash
curl "http://localhost:8001/ack/status?ids=ORD-001"
curl  http://localhost:8001/ack/summary
```

### 6. Run the Load Tests (Python)

Two load tests are available — choose based on what you want to benchmark:

**Direct Redis** — bypasses the producer HTTP layer entirely. Benchmarks raw Redis + network throughput. Producer does not need to be running.
```bash
cd python-order-service
python load_test_sharded.py
# 1,000,000 orders, 300 concurrent workers, direct XADD to Redis shards
```

**HTTP producer** — posts through the full stack (FastAPI → Pydantic → shard routing → Redis). Runs in two phases: **Phase 1** sends all orders and tracks submitted `order_id`s; **Phase 2** polls `GET /ack/status` until every order is confirmed persisted to Postgres or timeout elapses.
```bash
# Terminal 1: start the producer first
uvicorn producer:app --host 0.0.0.0 --port 8000 --workers 4

# Terminal 2: also start the consumer so orders get processed
python consumer.py

# Terminal 3: run the HTTP load test
python load_test_producer.py

# Override defaults
TOTAL_ORDERS=500000 CONCURRENCY=300 ACK_POLL_TIMEOUT=600 python load_test_producer.py
```

Phase 2 output example:
```
──────────────────────────────────────────────────
  Phase 2 — Ack polling (99 843 orders)
  Timeout: 300s  interval: 3s  chunk: 1000
──────────────────────────────────────────────────
       0 / 99 843  (  0.00%)
   24 500 / 99 843  ( 24.54%)  +24500 this round
   61 200 / 99 843  ( 61.30%)  +36700 this round
   99 843 / 99 843  (100.00%)  +38643 this round

  All 99 843 orders acked!
```

| Test | What it measures | Producer needed | Consumer needed |
|---|---|---|---|
| `load_test_sharded.py` | Redis write ceiling | No | No |
| `load_test_producer.py` | Full HTTP stack + end-to-end ack | **Yes** | **Yes** |

**Ack env vars** (load test):

| Var | Default | Meaning |
|-----|---------|---------|
| `PRODUCER_URL` | http://localhost:8000 | POST /orders endpoint (HTTP ingestion) |
| `ACK_URL` | (same as PRODUCER_URL) | GET /ack/* endpoints (status polling) — set separately if consumer on different port |
| `ACK_POLL_TIMEOUT` | 300 | Seconds to wait for 100% ack before giving up |
| `ACK_POLL_INTERVAL` | 3 | Seconds between poll rounds |
| `ACK_CHUNK_SIZE` | 1000 | `order_id`s per `/ack/status` request (reduce to 100–200 if hitting HTTP 414 URI Too Long) |

**Example** — separate producer and consumer servers:
```bash
# Producer on 8000, consumer on 8002 (split deployment)
PRODUCER_URL=http://localhost:8000 ACK_URL=http://localhost:8002 python load_test_producer.py

# Or reduce chunk size to avoid large query strings
ACK_CHUNK_SIZE=100 python load_test_producer.py
```

### 7. Run Tests

```bash
# Python — 58 unit tests, no infrastructure needed (all deps mocked)
cd python-order-service
pip install -r requirements-dev.txt
bin/python -m pytest tests/ -v

# Spring Boot — 16 tests: unit + @WebFluxTest + Testcontainers end-to-end
cd spring-boot-order-service
mvn test

# Quarkus — unit + Testcontainers end-to-end
cd quarkus-order-service
mvn test
```

**Python tests** run in ~0.3 s — all external calls (Redis, Postgres) are mocked with `AsyncMock`. No Docker or running services required.

**Java tests** use Testcontainers to spin up real Redis and PostgreSQL containers, pre-seed the consumer group, POST orders via HTTP, and await consumer processing with Awaitility before asserting on database rows and XACK state.

### 8. Verify Rows in pgAdmin

Connect pgAdmin to:
- **Host**: `postgres` (inside the Compose network) or `localhost` (from host)
- **Port**: `5432`
- **Database**: `orders_db`
- **Username**: `postgres` / **Password**: `postgres123`

Run in the Query Tool:
```sql
SELECT shard, COUNT(*) AS rows, AVG(amount)::NUMERIC(10,2) AS avg_amount
FROM orders
GROUP BY shard
ORDER BY shard;
```

The dashboard shows live insert activity — **Tuples in** spikes confirm the consumer is writing rows, and **Transactions per second** tracks commit rate in real time.

![pgAdmin4 dashboard showing live database activity](docs/screenshots/pgadmin4.png)

### 9. Monitor Redis Shards in RedisInsight

Open `http://localhost:5540` and add four databases using the **container hostnames** (not `127.0.0.1`):

| Name | Host | Port | Password |
|---|---|---|---|
| redis-0 | `redis-0` | `6379` | `123456` |
| redis-1 | `redis-1` | `6379` | `123456` |
| redis-2 | `redis-2` | `6379` | `123456` |
| redis-3 | `redis-3` | `6379` | `123456` |

The **Stream Data** tab shows every `order_created` entry with its JSON payload. With 1 million orders loaded, each shard holds ~250,000 entries:

![RedisInsight stream data view showing order_created entries on orders:stream:1](docs/screenshots/redis-insights-stream-data.png)

The **Consumer Groups** tab shows each consumer's name, how many messages are pending (unacknowledged), and idle time. A pending count of 0 means the consumer has processed and `XACK`'d everything it received:

![RedisInsight consumer groups view showing py-consumer-1 and quarkus-consumer-1 with pending counts](docs/screenshots/redis-insights-consumer-groups.png)

---

## Running Producer and Consumer Separately

Both JVM services support a `MODE` argument in `start.sh` to run as producer-only, consumer-only, or both. This enables a split-deployment model where you scale producers and consumers independently.

### Modes

| Mode | HTTP `/orders` | Consumer threads | Port | Use case |
|---|---|---|---|---|
| `both` (default) | active | active | 8000 | Development, single-node |
| `producer` | active | **disabled** | 8000 | Scale HTTP pods for ingestion |
| `consumer` | returns 503 | active | 8002 | Scale consumer pods for processing |

### Split Deployment Example

Start two terminals targeting the same Redis/PostgreSQL:

**Terminal 1 — producer node on port 8000** (handles HTTP traffic):
```bash
# Spring Boot
cd spring-boot-order-service && ./start.sh producer
# API at http://localhost:8000/orders and http://localhost:8000/ack/*

# Quarkus (port 8001)
cd quarkus-order-service && ./start.sh producer
```

**Terminal 2 — consumer node on port 8002** (processes from Redis streams):
```bash
# Spring Boot
cd spring-boot-order-service && ./start.sh consumer -Dapp.consumer-name=worker-1

# Quarkus
cd quarkus-order-service && ./start.sh consumer -Dapp.consumer-name=worker-1
```

Multiple consumer nodes can run simultaneously — each must have a unique `app.consumer-name` so Redis tracks their independent cursor positions within the consumer group. Messages are distributed across consumers by Redis (competing consumers model).

**Load testing against split deployment**:

When producer and consumer run on separate ports, point the load test to each:
```bash
# Producer on 8000, consumer on 8002
PRODUCER_URL=http://localhost:8000 ACK_URL=http://localhost:8002 python load_test_producer.py
```

### Overriding Any Property

All Spring Boot and Quarkus properties can be overridden as extra arguments:

```bash
# Spring Boot — override shard count and DB URL
./start.sh consumer \
  -Dapp.num-shards=8 \
  -Dspring.datasource.url=jdbc:postgresql://db-host:5432/orders_db \
  -Dspring.datasource.password=secret

# Quarkus
./start.sh producer \
  -Dapp.num-shards=8 \
  -Dquarkus.datasource.jdbc.url=jdbc:postgresql://db-host:5432/orders_db
```

---

## Running on Podman

Both the application infrastructure (`compose.yaml`) and the test suite (Testcontainers) work with rootless Podman. A few one-time steps are needed.

### Compose

```bash
# Install podman-compose if not present
pip install podman-compose
# or use the compose plugin
podman compose up -d
```

### Testcontainers with Podman

Testcontainers defaults to `/var/run/docker.sock`. Rootless Podman exposes its socket at `/run/user/<uid>/podman/podman.sock`. Two things are required:

**1. Enable the Podman socket service** (once, per login session or on boot):

```bash
systemctl --user enable --now podman.socket
```

**2. Configure Testcontainers** to use the Podman socket. Add the following to `~/.testcontainers.properties` (create if it does not exist):

```properties
docker.host=unix:///run/user/1000/podman/podman.sock
ryuk.disabled=true
```

Replace `1000` with your actual UID if different (`id -u`).

Both `testcontainers.properties` files in `src/test/resources/` already contain this configuration for UID 1000. The test code also sets `TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE` as a JVM system property at startup as a fallback for CI environments.

> **Why `ryuk.disabled=true`?** Testcontainers' Ryuk reaper container manages cleanup of test containers. It requires privileged socket access that rootless Podman does not grant. With Ryuk disabled, container lifecycle is managed by the test framework callbacks (`@Container` / `QuarkusTestResourceLifecycleManager.stop()`) instead.

---

## Key Implementation Notes

### Python Hash Stability

Python's built-in `hash()` is randomised on every interpreter start (`PYTHONHASHSEED`). With `uvicorn --workers 4`, each worker is a separate process with a different seed, so `hash("ORD-001")` returns a different value per worker — the same `order_id` can land on a different shard depending on which worker handles the request.

All three Python files (`producer.py`, `consumer.py`, `load_test_sharded.py`) use `hashlib.sha1` instead:

```python
import hashlib

def get_shard(order_id: str) -> int:
    return int(hashlib.sha1(order_id.encode()).hexdigest(), 16) % NUM_SHARDS
```

This produces the same shard assignment across all workers, restarts, and machines.

### FastAPI Lifespan

`@app.on_event("startup")` / `@app.on_event("shutdown")` are deprecated since FastAPI 0.93. The `lifespan` context manager is the current pattern — setup before `yield`, teardown after:

```python
from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    clients = [redis.from_url(url) for url in REDIS_URLS]  # startup
    yield
    for c in clients: await c.aclose()                      # shutdown

app = FastAPI(lifespan=lifespan)
```

### Batch Insert + `synchronous_commit=off`

All three consumers batch messages and flush them in one DB round trip per poll cycle. The flow per shard per loop iteration:

```
xreadgroup (count=500) → parse all → INSERT batch → XACK all IDs
       ↑ 1 Redis read        ↑ CPU only   ↑ 1 DB write  ↑ 1 Redis write
```

`synchronous_commit=off` is set at connection level so Postgres acknowledges the commit before flushing WAL to disk. This removes the fsync wait (1–5 ms on HDD, ~0.1 ms on NVMe) without affecting durability of already-committed data — only the last ~200 ms of commits can be lost on a hard crash, and Redis retains those messages in the Pending Entries List for re-delivery.

| Metric | Per-message (old) | Batched (new) |
|---|---|---|
| DB round trips per 500 msgs | 500 | 1 |
| Redis XACK calls per 500 msgs | 500 | 1 |
| WAL fsync calls per 500 msgs | 500 | 1 |
| Effective insert throughput | ~5–20k rows/sec | ~50–200k rows/sec |

### Lettuce API (6.3.x)

`StreamOffset` is a **nested static class** of `XReadArgs`, not a top-level class. Always reference it as `XReadArgs.StreamOffset`:

```java
// correct
XReadArgs.StreamOffset.from(streamName, "$")
XReadArgs.StreamOffset.lastConsumed(streamName)

// wrong — does not exist in 6.3.x
StreamOffset.from(...)
```

`StreamMessage.getId()` returns `String` directly — there is no `.getValue()` wrapper.

### Quarkus DataSource Injection

In Quarkus 3.x, inject `io.agroal.api.AgroalDataSource` rather than `javax.sql.DataSource`. Quarkus Arc registers the datasource CDI bean under the concrete `AgroalDataSource` type; resolving by the `javax.sql.DataSource` interface is unreliable during build-time augmentation.

```java
@Inject
AgroalDataSource dataSource;   // correct for Quarkus 3.x
```

Add `quarkus-agroal` as an explicit dependency (not just transitive) and set `quarkus.datasource.devservices.enabled=false` to prevent DevServices from interfering when an explicit JDBC URL is configured.

### Netty HTTP Line Length for Large Requests

When running `/ack/status` queries with many order IDs in the query string, the HTTP request line can exceed the default Netty limit of 4096 bytes, returning HTTP 414 (URI Too Long). Spring Boot applications use:

```yaml
server:
  netty:
    max-initial-line-length: 65536  # 64 KB — handles ~2500 order IDs per request
```

This is configured in `application.yml`, `application-producer.yml`, and `application-consumer.yml`. If load tests still hit 414, either:
1. Reduce `ACK_CHUNK_SIZE` in the load test (e.g., `ACK_CHUNK_SIZE=100`)
2. Increase `max-initial-line-length` further (e.g., to `131072`)

### Ack Aggregator — End-to-End Processing Confirmation

The pipeline is async by design: `POST /orders` returns in <1 ms after writing to Redis, long before the consumer inserts the row into Postgres. The **Ack Aggregator** bridges this gap — it lets callers (load tests, monitoring, downstream systems) know exactly when each order has been durably committed.

**How it works**:

```
Consumer DB commit succeeds
        │
        ▼
order_id added to per-shard in-memory buffer
        │
        ├── buffer size ≥ ACK_BATCH_SIZE (500)?   ─┐
        └── or seconds since last flush ≥ 5s?      ─┤
                                                     ▼
                                  HSET orders:acks:{shard} {order_id: timestamp}
                                  EXPIRE orders:acks:{shard} 86400   (24 h TTL)
```

**Producer reads acks** — `GET /ack/status` groups the requested `order_id`s by shard (same hash as `POST /orders`), then issues one `HMGET` per shard:

```bash
curl "http://localhost:8000/ack/status?ids=ORD-001,ORD-002,ORD-003"
```
```json
{
  "total": 3,
  "acked": 2,
  "pending": 1,
  "pct_complete": 66.67,
  "orders": {
    "ORD-001": "acked",
    "ORD-002": "acked",
    "ORD-003": "pending"
  }
}
```

**Summary without listing IDs** — `GET /ack/summary` calls `HLEN` on each shard's hash (O(1)):

```bash
curl http://localhost:8000/ack/summary
# {"total_acked": 99843, "by_shard": [{"shard": 0, "acked": 24960}, …]}
```

**Configuration** — same env vars / properties across all three runtimes:

| Parameter | Default | Meaning |
|-----------|---------|---------|
| `ACK_BATCH_SIZE` / `app.ack-batch-size` | 500 | Flush ack hash after N order_ids buffered |
| `ACK_FLUSH_INTERVAL` / `app.ack-flush-interval-ms` | 5 s | Or after this interval (prevents stale acks under low traffic) |
| `ACK_KEY_TTL` | 86 400 s | Redis Hash TTL — auto-cleaned after 24 h |

**Why Redis Hash and not a separate stream?**

`HSET` + `HMGET` give O(1) writes and O(k) reads for k requested IDs — no consumer group, no cursor, no re-delivery. The ack hash is stored on the **same Redis shard** as the order stream so no extra network hop is needed. The 24 h TTL prevents unbounded growth without any maintenance job.

**Why buffer on the consumer and not write one HSET per order?**

The same batch amortisation principle as `executemany` + bulk `XACK` — one `HSET` with 500 field-value pairs costs one Redis round trip instead of 500.

### Resilience Patterns (Python)

The Python producer and consumer wrap every external call in three layered safety nets defined in `resilience.py`.

```
Request → allow_request()? → asyncio.wait_for(coro, 10s) → on_success / on_failure
               ↑ OPEN: raise immediately (fail fast)
               ↑ HALF_OPEN: allow one probe
```

**Timeout** — each individual Redis or Postgres call is wrapped in `asyncio.wait_for(coro, timeout=10)`. A stuck connection is cancelled after 10 s rather than blocking the event loop indefinitely.

**Retry with exponential backoff** — after a timeout or error the call is retried up to 3 times total. The wait between attempts doubles: 1 s → 2 s. Only after all three attempts fail is the circuit breaker notified. This prevents the circuit from opening on brief transient blips that self-recover within one retry window.

**Circuit Breaker** — tracks retry-exhaustion events (not individual call failures). After 3 such events the circuit opens and all subsequent calls are rejected instantly with `CircuitBreakerOpenError`, avoiding a thundering-herd against a service that is clearly down. After 30 s one probe (HALF_OPEN) is allowed; a successful probe closes the circuit, a failed probe resets the 30 s window.

```
State         allow_request()   next transition
CLOSED        always yes        3 exhaustion events → OPEN
OPEN          no (fail fast)    30 s elapsed → HALF_OPEN
HALF_OPEN     yes (one probe)   success → CLOSED / failure → OPEN
```

**HTTP status codes** (producer):

| Status | Meaning |
|--------|---------|
| `200` | Order accepted, message written to Redis |
| `502` | Redis failed after all 3 retries |
| `503` | Circuit is open — Redis is unhealthy, retry after 30 s |

**Consumer behaviour on failure**:
- DB circuit open → batch skipped, messages stay in Redis PEL (re-delivered when Postgres recovers)
- All DB retries exhausted → same; no XACK, no data loss
- XACK fails after DB success → messages re-delivered; duplicate inserts are the only risk (mitigate with `UNIQUE (order_id)`)

**Separate circuit breakers**: the DB breaker and each shard's Redis breaker are independent. A Postgres outage does not affect Redis health tracking and vice versa.

**Separate circuit breakers per shard**: a failure on `redis-shard-2` opens only that shard's breaker; orders routing to shards 0, 1, and 3 continue unaffected.

### Consumer Group Race Condition in Tests

The consumer creates its group at offset `$` (latest) on startup. If a test message is published before the group is created, the message falls before `$` and is never delivered. Both integration test setups pre-create the group at offset `0-0` before the application starts:

- **Spring Boot**: done inside `@DynamicPropertySource` (runs after containers start, before Spring context).
- **Quarkus**: done inside `QuarkusTestResourceLifecycleManager.start()` (runs before Quarkus boots).

This guarantees that any message published during the test is always visible to the consumer group regardless of virtual-thread scheduling order.

---

## Further Optimisations

### Infrastructure

| Optimisation | Impact | How |
|---|---|---|
| Increase shard count to 8 or 16 | Linear throughput increase | Add `redis-4`–`redis-7` to `compose.yaml`, bump `NUM_SHARDS` |
| Redis 7 `LMPOP` / pipeline batching | Fewer round trips | Batch 100+ `XADD` in one pipeline call |
| Pin Redis to dedicated CPU cores | Eliminates noisy-neighbour jitter | `cpuset` in compose or `taskset` at runtime |
| Enable Redis AOF with `appendfsync no` | ~30% Redis write throughput gain | Trade durability guarantee for speed on non-critical replicas |
| Redis Cluster mode | Eliminates the app-side shard router | Use Lettuce `RedisClusterClient`; cluster handles slot routing automatically |
| Separate read/write PostgreSQL | Consumer writes to primary, analytics hit replica | `postgres.replica.url` in config, route `SELECT` there |
| PostgreSQL `COPY` bulk inserts | 10–20× insert throughput vs single-row `INSERT` | Accumulate a batch in the consumer then flush with `COPY orders FROM STDIN` |
| Partition `orders` table by `shard` | Query pruning, parallel vacuum | `PARTITION BY LIST (shard)`, one child table per shard |

### Java (Spring Boot & Quarkus)

| Optimisation | Impact | How |
|---|---|---|
| ~~Increase `xreadgroup COUNT` from 50 to 500~~ | ✅ Done — `app.batch-size=500` | Already applied in all three runtimes |
| ~~Batch JDBC inserts with `executeBatch`~~ | ✅ Done — 10–50× DB throughput | `executemany` (Python), `batchUpdate` (Spring Boot), `addBatch/executeBatch` (Quarkus) |
| ~~`synchronous_commit=off`~~ | ✅ Done — removes WAL fsync wait | Set via `connection-init-sql` / `new-connection-sql` / asyncpg session setting |
| ~~Shard-local bulk XACK~~ | ✅ Done — 1 Redis call per batch | `xack(stream, group, *ids)` varargs in all three runtimes |
| Tune HikariCP / Agroal pool size | Match pool to consumer concurrency | `maximum-pool-size = numShards * consumerThreadsPerShard` |
| GraalVM native image (Quarkus) | ~10× faster cold start, ~50% less RAM | `mvn package -Pnative`; eliminates JIT warmup cost |
| Spring AOT + CRaC checkpoint/restore | Near-instant restart for Spring Boot | `spring-boot:process-aot`, restore from CRaC checkpoint |
| Lettuce connection pooling | Reuse connections under burst load | `RedisClient` + `ConnectionPoolSupport.createGenericObjectPool` |
| Shard-local consumer batching | Amortise XACK cost | Collect msg IDs, call `XACK stream group id1 id2 … idN` once per batch |
| `-XX:+UseZGC -XX:+ZGenerational` | Sub-millisecond GC pauses under load | Already in `start.sh` — set `-Xms` = `-Xmx` to eliminate resize pauses |

### Python

| Optimisation | Impact | How |
|---|---|---|
| `uvicorn --workers N` where N = CPU cores | True parallelism around GIL | Already bypasses GIL via separate processes |
| `hiredis` parser | ~3× Redis response parse speed | Already in `requirements.txt`; auto-used by `redis-py` when installed |
| `asyncpg` `executemany` | Batched inserts | Accumulate rows, call `conn.executemany(sql, rows)` once per loop tick |
| Replace uvicorn with `granian` | ~20% HTTP throughput gain | `pip install granian && granian --interface asgi producer:app` |
| `PYTHONHASHSEED=0` | Stable sharding hash across restarts | Set in `.env`; Python's `hash()` is randomised by default |

### Observability

See the dedicated [Observability](#observability-1) section below for the full implementation guide.

---

## Performance Reference Numbers

These are indicative figures on a modern 8-core machine. Actual results depend on network, disk I/O, and JVM warmup state.

| Scenario | Approx. Orders/sec |
|---|---|
| Python HTTP producer, 4 uvicorn workers (`load_test_producer.py`) | 30,000 – 80,000 |
| Python direct Redis, no HTTP (`load_test_sharded.py`) | 300,000 – 600,000 |
| Spring Boot WebFlux producer (JIT warmed) | 150,000 – 400,000 |
| Quarkus producer (JIT warmed) | 140,000 – 380,000 |
| Quarkus native image producer | 200,000 – 450,000 |
| Consumer — single-row INSERT (old baseline) | ~5,000 – 20,000 rows/sec |
| Consumer — batch 500 rows + `synchronous_commit=off` | 50,000 – 200,000 rows/sec |

The effective system ceiling is whichever of these is smallest:
`min(producer throughput, Redis write throughput, consumer throughput, PG insert throughput)`

With 4 Redis shards and batched PostgreSQL inserts, the PostgreSQL `INSERT` rate (typically ~20,000–50,000 rows/sec single-threaded) is usually the first constraint to hit. The batched `COPY` or `executeBatch` optimisation listed above pushes that ceiling by an order of magnitude.

---

## Observability

### Python — Built-in Prometheus Metrics + Structured Logging

Both `producer.py` and `consumer.py` expose metrics out of the box using `prometheus_client` (already in `requirements.txt`).

#### Producer endpoints

| Endpoint | Description |
|---|---|
| `GET /health` | Returns `200 ok` when all circuit breakers are closed, `503 degraded` if any are open. Use as a Kubernetes readiness probe. |
| `GET /metrics` | Prometheus text format. Scrape with Prometheus or import into Grafana. |
| `GET /ack/status?ids=…` | Per-order acked/pending map. Routes each ID to its shard, issues one `HMGET` per shard. |
| `GET /ack/summary` | Total acked count per shard via `HLEN` — no ID list needed. |

```bash
# Check health
curl http://localhost:8000/health

# Scrape metrics
curl http://localhost:8000/metrics
```

#### Producer metrics

| Metric | Type | Labels | Description |
|---|---|---|---|
| `orders_published_total` | Counter | `shard` | Orders successfully written to Redis |
| `orders_failed_total` | Counter | `shard`, `reason` | Failed orders (`circuit_open` or `retries_exhausted`) |
| `http_request_duration_seconds` | Histogram | `status_code` | End-to-end POST /orders latency |
| `producer_circuit_breaker_state` | Gauge | `name` | CB state per shard: 0=closed, 1=half_open, 2=open |

#### Consumer metrics

The consumer exposes a Prometheus HTTP server on port `9090` (override with `METRICS_PORT` env var).

| Metric | Type | Labels | Description |
|---|---|---|---|
| `consumer_rows_inserted_total` | Counter | `shard` | Rows written to PostgreSQL |
| `consumer_batches_failed_total` | Counter | `shard`, `reason` | Batches that failed (`circuit_open` or `retries_exhausted`) |
| `consumer_xack_failed_total` | Counter | `shard` | XACK failures (messages may be re-delivered) |
| `consumer_acks_flushed_total` | Counter | `shard` | Order IDs written to ack hash after DB commit |
| `consumer_batch_duration_seconds` | Histogram | `shard` | INSERT + XACK time per batch |
| `consumer_circuit_breaker_state` | Gauge | `name` | CB state: 0=closed, 1=half_open, 2=open |

```bash
# Consumer metrics
curl http://localhost:9090/metrics
```

#### Structured JSON logging

All log output is in JSON — one object per line, suitable for ingestion by Loki, Fluentd, or AWS CloudWatch:

```json
{"ts": "2024-04-26T10:15:03", "level": "INFO", "logger": "producer", "msg": "Producer started — shards=4"}
{"ts": "2024-04-26T10:15:04", "level": "INFO", "logger": "consumer", "msg": "inserted shard=2 rows=500"}
{"ts": "2024-04-26T10:15:05", "level": "WARNING", "logger": "consumer", "msg": "db circuit open shard=0 msgs=500 err=..."}
```

Set `LOG_LEVEL=DEBUG` for per-order log lines (verbose; only useful during debugging).

### Consumer Lag Query

Consumer lag is the number of messages in a stream that have been delivered but not yet acknowledged. Query it directly from Redis:

```bash
# Pending count for a shard (messages delivered but not XACK'd)
redis-cli -p 6379 -a 123456 XPENDING orders:stream:0 order_processors - + 10

# Stream length vs pending — lag = XLEN - (XLEN - pending)
redis-cli -p 6379 -a 123456 XLEN orders:stream:0
```

In Grafana, alert when `XPENDING` for any shard exceeds a threshold (e.g. >50,000 means the consumer is falling behind the producer).

### Prometheus + Grafana Setup

Add a `prometheus` and `grafana` service to `compose.yaml` to get a full metrics pipeline:

```yaml
prometheus:
  image: prom/prometheus:v2.51.2
  ports: ["9091:9090"]
  volumes:
    - ./prometheus.yml:/etc/prometheus/prometheus.yml

grafana:
  image: grafana/grafana:10.4.2
  ports: ["3000:3000"]
  environment:
    GF_SECURITY_ADMIN_PASSWORD: admin
```

`prometheus.yml` scrape config:

```yaml
scrape_configs:
  - job_name: order_producer
    static_configs:
      - targets: ["host.docker.internal:8000"]   # /metrics on producer

  - job_name: order_consumer
    static_configs:
      - targets: ["host.docker.internal:9090"]   # prometheus_client HTTP server
```

Key Grafana panels to build:

| Panel | Query |
|---|---|
| Orders/sec (producer) | `rate(orders_published_total[1m])` |
| Error rate | `rate(orders_failed_total[1m])` |
| p99 latency | `histogram_quantile(0.99, rate(http_request_duration_seconds_bucket[1m]))` |
| Consumer insert rate | `rate(consumer_rows_inserted_total[1m])` |
| Ack flush rate | `rate(consumer_acks_flushed_total[1m])` |
| Circuit breaker state | `producer_circuit_breaker_state` / `consumer_circuit_breaker_state` |
| Batch insert p99 | `histogram_quantile(0.99, rate(consumer_batch_duration_seconds_bucket[1m]))` |

### Spring Boot — Micrometer

Add the Actuator + Prometheus dependency to `pom.xml`:

```xml
<dependency>
  <groupId>org.springframework.boot</groupId>
  <artifactId>spring-boot-starter-actuator</artifactId>
</dependency>
<dependency>
  <groupId>io.micrometer</groupId>
  <artifactId>micrometer-registry-prometheus</artifactId>
</dependency>
```

Expose the endpoints in `application.yml`:

```yaml
management:
  endpoints:
    web:
      exposure:
        include: health,metrics,prometheus
  endpoint:
    health:
      show-details: always
```

Access at:
- `GET /actuator/health` — liveness/readiness with datasource and Redis connection details
- `GET /actuator/prometheus` — Prometheus scrape endpoint with JVM, HikariCP, and custom metrics

Inject `MeterRegistry` to publish custom counters:

```java
@Autowired MeterRegistry registry;

registry.counter("orders.published", "shard", String.valueOf(shardIdx)).increment();
registry.timer("batch.insert.duration", "shard", String.valueOf(shardIdx))
        .record(duration, TimeUnit.MILLISECONDS);
```

### Quarkus — SmallRye Metrics

Add to `pom.xml`:

```xml
<dependency>
  <groupId>io.quarkus</groupId>
  <artifactId>quarkus-micrometer-registry-prometheus</artifactId>
</dependency>
```

Enable in `application.properties`:

```properties
quarkus.micrometer.export.prometheus.enabled=true
quarkus.micrometer.export.prometheus.path=/metrics
quarkus.smallrye-health.root-path=/health
```

Inject and use `MeterRegistry` identically to Spring Boot. Access metrics at `GET /metrics` and health at `GET /health`.
