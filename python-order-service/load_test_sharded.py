import asyncio
import json
import random
import time
from dotenv import load_dotenv
import os
import redis.asyncio as redis_async

load_dotenv()

REDIS_URLS = os.getenv("REDIS_URLS", "").split(",")
NUM_SHARDS = int(os.getenv("NUM_SHARDS", 4))
TOTAL_ORDERS = 1_000_000
CONCURRENCY = 300

PRODUCTS = ["Laptop", "Smartphone", "Headphones", "Tablet", "Smartwatch", "Keyboard", "Mouse"]

def get_redis_index(order_id: str) -> int:
    return hash(order_id) % NUM_SHARDS

async def generate_order(order_num: int):
    items = random.sample(PRODUCTS, random.randint(1, 4))
    order_id = f"ORD-{int(time.time()*1000):x}-{order_num:06d}"
    return {
        "order_id": order_id,
        "customer_id": f"CUST-{random.randint(10000, 99999)}",
        "amount": round(random.uniform(99.99, 2999.99), 2),
        "items": items
    }

async def main():
    print(f"🚀 Sharded direct Redis load test → {TOTAL_ORDERS:,} orders")
    start_time = time.time()

    # Create one async client per shard (multiplexing)
    clients = [redis_async.from_url(url.strip(), decode_responses=True) for url in REDIS_URLS]

    success = 0

    async def worker(worker_id: int):
        nonlocal success
        local_success = 0
        for i in range(worker_id, TOTAL_ORDERS, CONCURRENCY):
            order = await generate_order(i)
            shard_idx = get_redis_index(order["order_id"])
            client = clients[shard_idx]

            message = {
                "type": "order_created",
                "data": json.dumps(order),
                "timestamp": str(time.time())
            }
            await client.xadd(f"orders:stream:{shard_idx}", message, maxlen=1_000_000, approximate=True)
            local_success += 1
        return local_success

    results = await asyncio.gather(*(worker(i) for i in range(CONCURRENCY)))
    success = sum(results)

    duration = time.time() - start_time
    print(f"\n✅ Finished! {success:,} orders in {duration:.2f}s → {success/duration:,.0f} orders/sec")

if __name__ == "__main__":
    asyncio.run(main())
