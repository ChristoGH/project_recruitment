import os
import asyncio
import aio_pika
from functools import lru_cache

RABBIT_QUEUE = "recruitment_urls"

@lru_cache(maxsize=1)
async def get_channel() -> aio_pika.abc.AbstractChannel:
    """Return a singleton channel, creating the connection lazily."""
    conn = await aio_pika.connect_robust(
        host=os.getenv("RABBITMQ_HOST", "localhost"),
        port=int(os.getenv("RABBITMQ_PORT", 5672)),
        login=os.getenv("RABBITMQ_USER", "guest"),
        password=os.getenv("RABBITMQ_PASSWORD", "guest"),
        heartbeat=60,
    )
    ch = await conn.channel()
    await ch.declare_queue(RABBIT_QUEUE, durable=True)
    return ch 