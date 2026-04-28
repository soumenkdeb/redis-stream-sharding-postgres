import asyncio
import json
from dotenv import load_dotenv
import redis.asyncio as redis_async
import asyncpg
import os

load_dotenv()

REDIS_URLS = os.getenv("REDIS_URLS", "").split(",")
NUM_SHARDS = int(os.getenv("NUM_SHARDS", 4))
POSTGRES_DSN = os.getenv("POSTGRES_DSN")

STREAM_BASE = "orders:stream"
GROUP_NAME = "order_processors"
CONSUMER_NAME = "py-consumer-1"

async def create_table(db_pool):
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

async def process_message(db_pool, client, stream_name, shard_idx, msg_id, fields):
    data = json.loads(fields["data"])
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO orders (order_id, customer_id, amount, items, msg_id, shard)
            VALUES ($1, $2, $3, $4, $5, $6)
            """,
            data["order_id"],
            data["customer_id"],
            float(data["amount"]),
            json.dumps(data["items"]),
            msg_id,
            shard_idx,
        )
    await client.xack(stream_name, GROUP_NAME, msg_id)

async def main():
    global db_pool
    db_pool = await asyncpg.create_pool(POSTGRES_DSN)
    await create_table(db_pool)

    clients = [redis_async.from_url(url.strip(), decode_responses=True) for url in REDIS_URLS]

    # Create groups on all shards
    for i in range(NUM_SHARDS):
        try:
            await clients[i].xgroup_create(f"{STREAM_BASE}:{i}", GROUP_NAME, id="$", mkstream=True)
        except Exception:
            pass

    print(f"🎧 Sharded consumer started on {NUM_SHARDS} Redis instances")

    # Multiplexing: one task per shard
    async def consume_shard(shard_idx: int):
        client = clients[shard_idx]
        stream_name = f"{STREAM_BASE}:{shard_idx}"
        while True:
            try:
                messages = await client.xreadgroup(
                    GROUP_NAME, CONSUMER_NAME,
                    streams={stream_name: ">"},
                    count=50, block=2000
                )
                if messages:
                    for _, entries in messages:
                        for msg_id, fields in entries:
                            await process_message(db_pool, client, stream_name, shard_idx, msg_id, fields)
            except Exception as e:
                await asyncio.sleep(1)

    await asyncio.gather(*(consume_shard(i) for i in range(NUM_SHARDS)))

if __name__ == "__main__":
    asyncio.run(main())
