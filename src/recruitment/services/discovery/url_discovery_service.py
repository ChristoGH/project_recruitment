#!/usr/bin/env python3
"""
URL Discovery Service

This FastAPI service searches for recruitment URLs and publishes them to a RabbitMQ queue.
It's based on the working recruitment_ad_search.py script.
"""

import os
import json
import logging
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Final
from urllib.parse import urlparse
from googlesearch import search
from pydantic import BaseModel
from fastapi import FastAPI, BackgroundTasks, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
import asyncio
from dotenv import load_dotenv
from contextlib import asynccontextmanager
import uuid
import aio_pika
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from concurrent.futures import ThreadPoolExecutor

from pathlib import Path

# =============================
# 0. Settings – one source only
# =============================
SEARCH_DAYS_BACK: Final[int] = int(os.getenv("SEARCH_DAYS_BACK", 7))
SEARCH_INTERVAL_S: Final[int] = int(os.getenv("SEARCH_INTERVAL_SECONDS", 1800))
BATCH_SIZE: Final[int] = int(os.getenv("GOOGLE_SEARCH_BATCH_SIZE", 50))
RABBITMQ_URL: Final[str] = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq/")
TLD: Final[str] = os.getenv("GOOGLE_TLD", "co.za")

# =============================
# 1. Thread pool for blocking I/O
# =============================
_executor = ThreadPoolExecutor(max_workers=10)

async def run_blocking(func, *args, **kw):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_executor, lambda: func(*args, **kw))

# =============================
# 2. Shared state with lock
# =============================
_search_lock = asyncio.Lock()
search_results = {}
def _now(): return datetime.utcnow().isoformat()

async def set_status(sid, **fields):
    async with _search_lock:
        search_results.setdefault(sid, {}).update(fields)

# =============================
# 3. FastAPI app and single RabbitMQ link
# =============================
logger = logging.getLogger(__name__)
app = FastAPI(
    title="URL Discovery Service",
    description="Service for discovering recruitment URLs and publishing them to a queue",
    version="1.0.0",
)
_connection = None
_channel = None

@app.on_event("startup")
async def connect_rabbit():
    global _connection, _channel
    _connection = await aio_pika.connect_robust(RABBITMQ_URL)
    _channel = await _connection.channel()
    await _channel.declare_queue("recruitment_urls", durable=True)
    logger.info("RabbitMQ connected")

@app.on_event("shutdown")
async def close_rabbit():
    await _connection.close()

# =============================
# 4. Search + publish in three steps
# =============================
async def gsearch_async(term, limit):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        _executor,
        lambda: list(search(term, tld=TLD, num=10, stop=limit, pause=2))
    )

async def publish_urls(sid, urls):
    msgs = [
        aio_pika.Message(
            body=json.dumps({"sid": sid, "url": u, "ts": _now()}).encode(),
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT
        ) for u in urls
    ]
    await _channel.default_exchange.publish_batch(msgs, routing_key="recruitment_urls")
    logger.info("Published %d urls (sid=%s)", len(msgs), sid)

def background_task_with_error_status(task_func):
    async def wrapper(sid, *args, **kwargs):
        try:
            await task_func(sid, *args, **kwargs)
        except Exception as exc:
            logger.exception("Task %s failed", sid)
            await set_status(sid, status="error", error=str(exc), finished=_now())
    return wrapper

@background_task_with_error_status
async def perform_search(sid, term):
    await set_status(sid, status="searching", started=_now())
    urls = await gsearch_async(term, BATCH_SIZE)
    await set_status(sid, status="publishing", url_count=len(urls))
    await publish_urls(sid, urls)
    await set_status(sid, status="done", finished=_now())

# =============================
# 5. API endpoints
# =============================
@app.post("/search")
async def start_search(config: dict, bg: BackgroundTasks):
    sid = config.get("id") or f"search-{_now()}"
    term = config["term"]
    bg.add_task(perform_search, sid, term)
    return {"id": sid, "status": "queued"}

@app.get("/search/{sid}")
async def check(sid: str):
    # Always return the latest status for the search
    return search_results.get(sid, {"status": "unknown"})

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for FastAPI application."""
    # Initialize RabbitMQ connection
    try:
        connection = await aio_pika.connect_robust(RABBITMQ_URL)
        channel = await connection.channel()
        await channel.declare_queue("recruitment_urls", durable=True)
        logger.info("RabbitMQ queue declared successfully")
    except Exception as e:
        logger.error(f"Failed to initialize RabbitMQ: {str(e)}")
        raise

    # Initialize scheduler
    scheduler = AsyncIOScheduler()
    scheduler.start()
    logger.info("Scheduler started")

    yield

    # Cleanup
    scheduler.shutdown()
    if not connection.is_closed:
        await connection.close()

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
        connection = await get_rabbitmq_connection()
        if connection and not connection.is_closed:
            return {"status": "healthy", "rabbitmq": "connected"}
        return {"status": "unhealthy", "rabbitmq": "disconnected"}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 