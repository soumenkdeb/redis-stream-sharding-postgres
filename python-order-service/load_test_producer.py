"""
HTTP load test — hits the FastAPI producer at POST /orders, then polls
GET /ack/status until every submitted order has been durably saved to Postgres.

Phase 1 — Send
  POST /orders for TOTAL_ORDERS orders at CONCURRENCY parallel requests.
  Tracks each submitted order_id so phase 2 can check completion.

Phase 2 — Ack poll
  Calls GET /ack/status in chunks of ACK_CHUNK_SIZE order_ids until
  100% are acked or ACK_POLL_TIMEOUT seconds elapses.

Usage:
    uvicorn producer:app --host 0.0.0.0 --port 8000 --workers 4
    python load_test_producer.py

Environment overrides:
    PRODUCER_URL=http://localhost:8000   target host
    TOTAL_ORDERS=100000                  total orders to send
    CONCURRENCY=200                      simultaneous in-flight requests
    ACK_POLL_TIMEOUT=300                 seconds to wait for 100% ack
    ACK_POLL_INTERVAL=3                  seconds between ack poll rounds
    ACK_CHUNK_SIZE=1000                  order_ids per /ack/status request
"""

import asyncio
import random
import time
import os
from dotenv import load_dotenv
import httpx

load_dotenv()

PRODUCER_URL     = os.getenv("PRODUCER_URL", "http://localhost:8000")
TOTAL_ORDERS     = int(os.getenv("TOTAL_ORDERS", 100_000))
CONCURRENCY      = int(os.getenv("CONCURRENCY", 200))
ACK_POLL_TIMEOUT = float(os.getenv("ACK_POLL_TIMEOUT", "300"))
ACK_POLL_INTERVAL = float(os.getenv("ACK_POLL_INTERVAL", "3"))
ACK_CHUNK_SIZE   = int(os.getenv("ACK_CHUNK_SIZE", "1000"))

PRODUCTS = ["Laptop", "Smartphone", "Headphones", "Tablet", "Smartwatch", "Keyboard", "Mouse"]


def make_order(order_num: int) -> dict:
    return {
        "order_id":    f"ORD-HTTP-{int(time.time()*1000):x}-{order_num:06d}",
        "customer_id": f"CUST-{random.randint(10000, 99999)}",
        "amount":      round(random.uniform(9.99, 2999.99), 2),
        "items":       random.sample(PRODUCTS, random.randint(1, 4)),
    }


async def send_order(
    client: httpx.AsyncClient,
    sem: asyncio.Semaphore,
    order: dict,
    results: dict,
    submitted: list,
) -> None:
    async with sem:
        try:
            r = await client.post(f"{PRODUCER_URL}/orders", json=order, timeout=10.0)
            if r.status_code == 200:
                results["ok"] += 1
                submitted.append(order["order_id"])
            else:
                results["err"] += 1
                results["errors"].append(f"{order['order_id']}: HTTP {r.status_code}")
        except Exception as e:
            results["err"] += 1
            results["errors"].append(f"{order['order_id']}: {e}")


async def poll_ack_status(client: httpx.AsyncClient, order_ids: list[str]) -> None:
    """
    Phase 2: poll GET /ack/status until every submitted order is acked.

    Sends order_ids in chunks to respect the ACK_MAX_IDS guard on the producer.
    Prints progress after each round and exits when pct_complete reaches 100%
    or ACK_POLL_TIMEOUT seconds elapse.
    """
    print(f"\n{'─'*50}")
    print(f"  Phase 2 — Ack polling ({len(order_ids):,} orders)")
    print(f"  Timeout: {ACK_POLL_TIMEOUT:.0f}s  interval: {ACK_POLL_INTERVAL:.0f}s  chunk: {ACK_CHUNK_SIZE}")
    print(f"{'─'*50}")

    deadline = time.monotonic() + ACK_POLL_TIMEOUT
    total = len(order_ids)
    last_acked = 0

    while time.monotonic() < deadline:
        acked = 0
        errors = 0

        for start in range(0, total, ACK_CHUNK_SIZE):
            chunk = order_ids[start : start + ACK_CHUNK_SIZE]
            ids_param = ",".join(chunk)
            try:
                r = await client.get(
                    f"{PRODUCER_URL}/ack/status",
                    params={"ids": ids_param},
                    timeout=15.0,
                )
                if r.status_code == 200:
                    data = r.json()
                    acked += data["acked"]
                else:
                    errors += 1
            except Exception as e:
                errors += 1
                print(f"  ack poll error: {e}")

        delta = acked - last_acked
        last_acked = acked
        pct = acked / total * 100 if total else 0.0
        rate_str = f"  +{delta:,} this round" if delta else ""
        print(f"  {acked:>8,} / {total:,}  ({pct:6.2f}%){rate_str}"
              + (f"  [poll errors={errors}]" if errors else ""))

        if acked >= total:
            print(f"\n  All {total:,} orders acked!")
            return

        await asyncio.sleep(ACK_POLL_INTERVAL)

    remaining = total - last_acked
    print(f"\n  Timed out — {last_acked:,}/{total:,} acked, {remaining:,} still pending")
    print(f"  (consumer may still be processing; check /ack/summary)")


async def main() -> None:
    print(f"HTTP load test → {PRODUCER_URL}/orders")
    print(f"  orders={TOTAL_ORDERS:,}  concurrency={CONCURRENCY}")
    print()

    results: dict = {"ok": 0, "err": 0, "errors": []}
    submitted: list[str] = []

    limits = httpx.Limits(
        max_connections=CONCURRENCY + 50,
        max_keepalive_connections=CONCURRENCY,
    )
    sem = asyncio.Semaphore(CONCURRENCY)
    orders = [make_order(i) for i in range(TOTAL_ORDERS)]

    start = time.time()

    async with httpx.AsyncClient(limits=limits) as client:

        # ── Phase 1: send ─────────────────────────────────────────────────────
        tasks = [
            asyncio.create_task(send_order(client, sem, o, results, submitted))
            for o in orders
        ]

        async def ticker():
            while True:
                await asyncio.sleep(5)
                done = results["ok"] + results["err"]
                elapsed = time.time() - start
                rps = done / elapsed if elapsed else 0
                print(f"  {done:>8,} / {TOTAL_ORDERS:,}  ({rps:,.0f} req/s)"
                      f"  errors={results['err']}")

        ticker_task = asyncio.create_task(ticker())
        await asyncio.gather(*tasks)
        ticker_task.cancel()

        duration = time.time() - start
        total_sent = results["ok"] + results["err"]
        rps = total_sent / duration

        print()
        print(f"{'─'*50}")
        print(f"  Phase 1 done in {duration:.2f}s")
        print(f"  Sent    : {total_sent:,}")
        print(f"  OK      : {results['ok']:,}")
        print(f"  Errors  : {results['err']:,}")
        print(f"  Rate    : {rps:,.0f} req/s  ({rps*60:,.0f} req/min)")
        print(f"{'─'*50}")

        if results["errors"]:
            print(f"\nFirst 10 send errors:")
            for e in results["errors"][:10]:
                print(f"  {e}")

        # ── Phase 2: ack poll ─────────────────────────────────────────────────
        if submitted:
            await poll_ack_status(client, submitted)
        else:
            print("\n  No orders submitted successfully — skipping ack poll")


if __name__ == "__main__":
    asyncio.run(main())
