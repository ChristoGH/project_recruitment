import aio_pika
import os
import json
from functools import lru_cache
from typing import Optional, Tuple

# RabbitMQ configuration
RABBIT_QUEUE = os.getenv("RABBIT_QUEUE", "url_queue")
RABBIT_HOST = os.getenv("RABBIT_HOST", "rabbitmq")
RABBIT_PORT = int(os.getenv("RABBIT_PORT", "5672"))
RABBIT_USER = os.getenv("RABBIT_USER", "guest")
RABBIT_PASSWORD = os.getenv("RABBIT_PASSWORD", "guest")
RABBIT_VHOST = os.getenv("RABBIT_VHOST", "/")

QUEUE_ARGS = {
    "x-message-ttl": 86_400_000,  # 24 hours in milliseconds
    "x-max-length": 10_000  # Maximum number of messages in queue
}

# Cache for the connection
_connection = None

async def get_connection() -> aio_pika.RobustConnection:
    """Get a cached RabbitMQ connection."""
    global _connection
    if _connection is None or _connection.is_closed:
        _connection = await aio_pika.connect_robust(
            host=os.getenv("RABBIT_HOST", "rabbitmq"),
            port=int(os.getenv("RABBIT_PORT", "5672")),
            login=os.getenv("RABBIT_USER", "guest"),
            password=os.getenv("RABBIT_PASSWORD", "guest"),
            virtualhost=os.getenv("RABBIT_VHOST", "/")
        )
    return _connection

async def get_channel() -> aio_pika.RobustChannel:
    """Get a RabbitMQ channel."""
    connection = await get_connection()
    return await connection.channel()

async def ensure_queue_exists() -> None:
    """Ensure the RabbitMQ queue exists with proper configuration."""
    ch = await get_channel()
    await ch.declare_queue(
        RABBIT_QUEUE,
        durable=True,
        arguments=QUEUE_ARGS
    )

async def publish_json(payload: dict, queue: str = RABBIT_QUEUE) -> None:
    """Publish a JSON payload to the specified queue."""
    ch = await get_channel()
    await ch.default_exchange.publish(
        aio_pika.Message(
            body=json.dumps(payload).encode(),
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        ),
        routing_key=queue
    )

async def get_rabbitmq_connection():
    """Get a RabbitMQ connection."""
    try:
        # Create connection
        connection = await aio_pika.connect_robust(
            host=RABBIT_HOST,
            port=RABBIT_PORT,
            login=RABBIT_USER,
            password=RABBIT_PASSWORD,
            virtualhost=RABBIT_VHOST
        )
        return connection
    except Exception as e:
        raise Exception(f"Failed to connect to RabbitMQ: {str(e)}")

async def get_rabbitmq_channel():
    """Get a RabbitMQ channel."""
    try:
        connection = await get_rabbitmq_connection()
        channel = await connection.channel()
        return channel
    except Exception as e:
        raise Exception(f"Failed to get RabbitMQ channel: {str(e)}") 