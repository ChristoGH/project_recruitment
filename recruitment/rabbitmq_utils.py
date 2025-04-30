import aio_pika
import os
from typing import Optional

# RabbitMQ configuration
RABBIT_QUEUE = os.getenv("RABBIT_QUEUE", "url_queue")
RABBIT_HOST = os.getenv("RABBIT_HOST", "rabbitmq")
RABBIT_PORT = int(os.getenv("RABBIT_PORT", "5672"))
RABBIT_USER = os.getenv("RABBIT_USER", "guest")
RABBIT_PASSWORD = os.getenv("RABBIT_PASSWORD", "guest")
RABBIT_VHOST = os.getenv("RABBIT_VHOST", "/")

async def get_rabbitmq_connection():
    """Get a RabbitMQ connection and channel."""
    try:
        # Create connection
        connection = await aio_pika.connect_robust(
            host=RABBIT_HOST,
            port=RABBIT_PORT,
            login=RABBIT_USER,
            password=RABBIT_PASSWORD,
            virtualhost=RABBIT_VHOST
        )
        
        # Create channel
        channel = await connection.channel()
        
        # Declare queue
        await channel.declare_queue(
            RABBIT_QUEUE,
            durable=True,
            arguments={
                "x-message-ttl": 86400000,  # 24 hours in milliseconds
                "x-max-length": 10000  # Maximum number of messages in queue
            }
        )
        
        return channel
    except Exception as e:
        raise Exception(f"Failed to connect to RabbitMQ: {str(e)}")

async def ensure_queue_exists():
    """Ensure the RabbitMQ queue exists with proper configuration."""
    try:
        channel = await get_rabbitmq_connection()
        await channel.declare_queue(
            RABBIT_QUEUE,
            durable=True,
            arguments={
                "x-message-ttl": 86400000,  # 24 hours in milliseconds
                "x-max-length": 10000  # Maximum number of messages in queue
            }
        )
    except Exception as e:
        raise Exception(f"Failed to ensure queue exists: {str(e)}") 