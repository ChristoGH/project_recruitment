#!/usr/bin/env python3
"""
URL Processing Service

This FastAPI service consumes URLs from a RabbitMQ queue, processes them, and stores the results in a database.
"""

import os
import json
import logging
import asyncio
from datetime import datetime
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from contextlib import asynccontextmanager
import aio_pika
import sqlite3
from pathlib import Path

from ...logging_config import setup_logging
from ...utils.rabbitmq import get_rabbitmq_connection, RABBIT_QUEUE
from ...utils.web_crawler import crawl_website_sync_v2
from ...db.repository import RecruitmentDatabase

# Load environment variables
load_dotenv()

# Create module-specific logger
logger = setup_logging(__name__)

# Get the absolute path to the project root
PROJECT_ROOT = Path(__file__).parent.parent.parent
DB_PATH = os.getenv("RECRUITMENT_PATH", str(PROJECT_ROOT / "src" / "recruitment" / "db" / "recruitment.db"))

# Initialize database
db = RecruitmentDatabase(DB_PATH)
db.initialize()

def init_db():
    """Initialize the database with required tables."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS raw_content (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT NOT NULL,
            content TEXT NOT NULL,
            search_id TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            processed BOOLEAN DEFAULT 0
        )
    """)
    conn.commit()
    conn.close()

async def process_url(url: str, search_id: str):
    """Process a single URL and store its content."""
    result = await crawl_website_sync_v2(url)
    if result.success:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO raw_content (url, content, search_id, timestamp)
            VALUES (?, ?, ?, ?)
        """, (url, result.markdown, search_id, datetime.now().isoformat()))
        conn.commit()
        conn.close()
        logger.info(f"Processed URL: {url}")

async def consume_urls():
    """Consume URLs from RabbitMQ queue and process them."""
    connection = await get_rabbitmq_connection()
    channel = await connection.channel()
    queue = await channel.declare_queue(RABBIT_QUEUE, durable=True)
    
    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            async with message.process():
                data = json.loads(message.body.decode())
                await process_url(data["url"], data["search_id"])

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for FastAPI application."""
    init_db()
    
    connection = await get_rabbitmq_connection()
    channel = await connection.channel()
    await channel.declare_queue(RABBIT_QUEUE, durable=True)
    
    # Start consumer task
    consumer_task = asyncio.create_task(consume_urls())
    
    yield
    
    # Cleanup
    consumer_task.cancel()
    try:
        await consumer_task
    except asyncio.CancelledError:
        pass
    if not connection.is_closed:
        await connection.close()

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001) 