#!/usr/bin/env python3
"""
URL Processing Service API

This FastAPI service provides endpoints for URL processing and monitoring.
"""

import os
import json
import logging
import asyncio
import contextlib
from datetime import datetime
from typing import Dict, Any
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from contextlib import asynccontextmanager
import aio_pika
import sqlite3

from src.recruitment.utils.logging_config import setup_logging
from src.recruitment.utils.rabbitmq_utils import get_channel, RABBIT_QUEUE
from src.recruitment.services.processing.url_processing_service import URLProcessor

# Load environment variables
load_dotenv()

# Create module-specific logger
logger = setup_logging("url_processing_api")

# Database setup
DB_PATH = os.getenv("DB_PATH", "/app/databases/recruitment.db")
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

def init_db():
    """Initialize the database with required tables."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Create raw_content table if it doesn't exist
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

async def consume_urls(queue: aio_pika.abc.AbstractQueue):
    """Consume URLs from the given RabbitMQ queue and process them."""
    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            try:
                async with message.process():
                    data = json.loads(message.body.decode())
                    url = data["url"]
                    search_id = data["search_id"]

                    processor = URLProcessor()
                    result = await processor.process_url(url, search_id)

                    if result["status"] == "success":
                        logger.info(f"✔ processed %s (search_id=%s)", url, search_id)
                    else:
                        logger.error("✖ failed to process %s: %s", url, result["message"])

            except Exception as e:
                logger.exception("Error processing message: %s", e)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start the SQLite DB, attach to RabbitMQ, launch the consumer."""
    ch = await get_channel()                       # robust, cached

    # 📖  LOOK‑UP ONLY  – do **not** redeclare with new arguments
    queue = await ch.get_queue(RABBIT_QUEUE, passive=True)

    task = asyncio.create_task(consume_urls(queue))  # pass the queue
    logger.info("URL consumer started")
    try:
        yield
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        if not ch.is_closed:
            await ch.close()

app = FastAPI(
    title="URL Processing Service",
    description="Service for processing recruitment URLs and storing their content",
    version="1.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
async def health_check():
    """Health check endpoint for Docker healthcheck."""
    try:
        ch = await get_channel()
        if ch and not ch.is_closed:
            return {"status": "healthy", "rabbitmq": "connected"}
        return {"status": "unhealthy", "rabbitmq": "disconnected"}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}

@app.get("/stats")
async def get_stats():
    """Get processing statistics."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Get total URLs processed
        cursor.execute("SELECT COUNT(*) FROM raw_content")
        total_urls = cursor.fetchone()[0]
        
        # Get URLs by search ID
        cursor.execute("""
            SELECT search_id, COUNT(*) as count
            FROM raw_content
            GROUP BY search_id
        """)
        urls_by_search = dict(cursor.fetchall())
        
        conn.close()
        
        return {
            "total_urls": total_urls,
            "urls_by_search": urls_by_search
        }
    except Exception as e:
        logger.error(f"Error getting stats: {e}")
        raise HTTPException(status_code=500, detail=str(e)) 