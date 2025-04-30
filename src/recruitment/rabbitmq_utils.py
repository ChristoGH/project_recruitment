import os
import asyncio
import aio_pika
from functools import lru_cache
import logging
from typing import Optional

RABBIT_QUEUE = "recruitment_urls"

logger = logging.getLogger(__name__)

@lru_cache(maxsize=1)
async def get_rabbitmq_connection() -> aio_pika.Connection:
    """Return a singleton connection to RabbitMQ."""
    try:
        connection = await aio_pika.connect_robust(
            host=os.getenv("RABBITMQ_HOST", "localhost"),
            port=int(os.getenv("RABBITMQ_PORT", 5672)),
            login=os.getenv("RABBITMQ_USER", "guest"),
            password=os.getenv("RABBITMQ_PASSWORD", "guest"),
            heartbeat=60,
        )
        logger.info(f"Successfully connected to RabbitMQ at {os.getenv('RABBITMQ_HOST', 'localhost')}")
        return connection
    except Exception as e:
        logger.error(f"Failed to connect to RabbitMQ: {str(e)}")
        raise

async def get_channel() -> aio_pika.abc.AbstractChannel:
    """Get a channel from the RabbitMQ connection."""
    connection = await get_rabbitmq_connection()
    channel = await connection.channel()
    await channel.declare_queue(RABBIT_QUEUE, durable=True)
    return channel 