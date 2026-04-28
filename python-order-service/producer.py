import asyncio
import json
from fastapi import FastAPI
from pydantic import BaseModel
import redis.asyncio as redis
from dotenv import load_dotenv
import os

load_dotenv()

REDIS_URL = os.getenv("REDIS_URL", "redis://:123456@localhost:6379")
STREAM_NAME = "orders:stream"

app = FastAPI(title="Order Producer")

redis_client = redis.from_url(REDIS_URL, decode_responses=True)

class Order(BaseModel):
    order_id: str
    customer_id: str
    amount: float
    items: list[str]

@app.post("/orders")
async def create_order(order: Order):
    message = {
        "type": "order_created",
        "data": order.model_dump_json(),
        "timestamp": str(asyncio.get_running_loop().time())
    }
    
    msg_id = await redis_client.xadd(
        STREAM_NAME,
        fields=message,
        maxlen=1000000,
        approximate=True
    )
    
    return {"status": "queued", "message_id": msg_id}

@app.on_event("startup")
async def startup():
    print("🚀 Producer started → pushing to Redis Stream")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
