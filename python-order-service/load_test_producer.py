"""
HTTP load test — hits the FastAPI producer at POST /orders.

Unlike load_test_sharded.py (which writes directly to Redis and bypasses
the application entirely), this test exercises the full producer stack:
  network → FastAPI → Pydantic validation → shard routing → Redis XADD

Use this to benchmark the real producer throughput and find HTTP-layer
bottlenecks (connection limits, uvicorn worker count, serialisation cost).

Usage:
    # Make sure the producer is running first:
    uvicorn producer:app --host 0.0.0.0 --port 8000 --workers 4

    python load_test_producer.py

Environment overrides:
    PRODUCER_URL=http://localhost:8000   target host
    TOTAL_ORDERS=100000                  total orders to send
    CONCURRENCY=200                      simultaneous in-flight requests
"""

import asyncio
import random
import time
import os
from dotenv import load_dotenv
import httpx

load_dotenv()

PRODUCER_URL = os.getenv("PRODUCER_URL", "http://localhost:8000")
TOTAL_ORDERS = int(os.getenv("TOTAL_ORDERS", 100_000))
CONCURRENCY   = int(os.getenv("CONCURRENCY", 200))

PRODUCTS = ["Laptop", "Smartphone", "Headphones", "Tablet", "Smartwatch", "Keyboard", "Mouse"]


def make_order(order_num: int) -> dict:
    return {
        "order_id":   f"ORD-HTTP-{int(time.time()*1000):x}-{order_num:06d}",
        "customer_id": f"CUST-{random.randint(10000, 99999)}",
        "amount":      round(random.uniform(9.99, 2999.99), 2),
        "items":       random.sample(PRODUCTS, random.randint(1, 4)),
    }


async def run(client: httpx.AsyncClient, sem: asyncio.Semaphore,
              order_num: int, results: dict) -> None:
    order = make_order(order_num)
    async with sem:
        try:
            r = await client.post(f"{PRODUCER_URL}/orders", json=order, timeout=10.0)
            if r.status_code == 200:
                results["ok"] += 1
            else:
                results["err"] += 1
                results["errors"].append(f"order {order_num}: HTTP {r.status_code}")
        except Exception as e:
            results["err"] += 1
            results["errors"].append(f"order {order_num}: {e}")


async def main() -> None:
    print(f"HTTP load test → {PRODUCER_URL}/orders")
    print(f"  orders={TOTAL_ORDERS:,}  concurrency={CONCURRENCY}")
    print()

    results = {"ok": 0, "err": 0, "errors": []}

    # httpx.AsyncClient reuses connections across requests (HTTP keep-alive).
    # limits controls the connection pool — set max_connections >= CONCURRENCY
    # so we never block waiting for a free socket.
    limits = httpx.Limits(max_connections=CONCURRENCY + 50,
                          max_keepalive_connections=CONCURRENCY)

    sem = asyncio.Semaphore(CONCURRENCY)

    start = time.time()

    async with httpx.AsyncClient(limits=limits) as client:
        tasks = [
            asyncio.create_task(run(client, sem, i, results))
            for i in range(TOTAL_ORDERS)
        ]

        # Progress ticker every 5 seconds
        async def ticker():
            while True:
                await asyncio.sleep(5)
                done = results["ok"] + results["err"]
                elapsed = time.time() - start
                rps = done / elapsed if elapsed else 0
                print(f"  {done:>8,} / {TOTAL_ORDERS:,}  ({rps:,.0f} req/s)  "
                      f"errors={results['err']}")

        ticker_task = asyncio.create_task(ticker())
        await asyncio.gather(*tasks)
        ticker_task.cancel()

    duration = time.time() - start
    total    = results["ok"] + results["err"]
    rps      = total / duration

    print()
    print(f"{'─'*50}")
    print(f"  Done in {duration:.2f}s")
    print(f"  Sent    : {total:,}")
    print(f"  OK      : {results['ok']:,}")
    print(f"  Errors  : {results['err']:,}")
    print(f"  Rate    : {rps:,.0f} req/s  ({rps*60:,.0f} req/min)")
    print(f"{'─'*50}")

    if results["errors"]:
        print(f"\nFirst 10 errors:")
        for e in results["errors"][:10]:
            print(f"  {e}")


if __name__ == "__main__":
    asyncio.run(main())
