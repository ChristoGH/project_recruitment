#!/usr/bin/env python3
"""
URL Discovery Service

This FastAPI service searches for recruitment URLs and publishes them to a RabbitMQ queue.
It's based on the working recruitment_ad_search.py script.
"""

import os
import json
import logging
from datetime import datetime
from typing import Final
from googlesearch import search
from fastapi import FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
import asyncio
from contextlib import asynccontextmanager
import aio_pika
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4


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
search_results: dict[str, dict[str, str]] = {}


def _now():
    return datetime.utcnow().isoformat()


async def set_status(sid, **fields):
    async with _search_lock:
        search_results.setdefault(sid, {}).update(fields)


# =============================
# 3. FastAPI app and single RabbitMQ link
# =============================

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    channel = await connection.channel()
    await channel.declare_queue("recruitment_urls", durable=True)
    app.state.amqp_channel = channel

    scheduler = AsyncIOScheduler()
    scheduler.start()
    logger.info("Scheduler started")

    if os.getenv("ENABLE_SCHEDULED_SEARCH", "0") == "1":
        term = os.getenv("SCHEDULED_SEARCH_TERM", "recruitment jobs South Africa")
        interval = int(os.getenv("SCHEDULED_SEARCH_INTERVAL_SECONDS", "1800"))
        search_id = f"auto-{uuid4().hex[:8]}"
        scheduler.add_job(
            perform_search,
            trigger="interval",
            seconds=interval,
            args=[search_id, term],
            id="scheduled_search",
            replace_existing=True,
        )
        logger.info("Scheduled search every %s s ⇒ %s", interval, term)

    try:
        yield
    finally:
        scheduler.shutdown()
        await channel.close()
        await connection.close()


app = FastAPI(
    title="URL Discovery Service",
    description="Service for discovering recruitment URLs and publishing them to a queue",
    version="1.0.0",
    lifespan=lifespan,
)


# =============================
# 4. Search + publish in three steps
# =============================
async def gsearch_async(term, limit):
    loop = asyncio.get_running_loop()
    try:
        # First attempt: new approach with num_results
        return await loop.run_in_executor(
            _executor, lambda: list(search(term, num_results=limit))
        )
    except Exception as e:
        logger.warning(f"New search method failed: {e}, trying fallback...")
        # Fallback: original approach with tld, num, stop, pause
        return await loop.run_in_executor(
            _executor, lambda: list(search(term, tld=TLD, num=10, stop=limit, pause=2))
        )


async def publish_urls(sid, urls):
    channel = app.state.amqp_channel
    msgs = [
        aio_pika.Message(
            body=json.dumps({"sid": sid, "url": u, "ts": _now()}).encode(),
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        )
        for u in urls
    ]
    await channel.default_exchange.publish_batch(msgs, routing_key="recruitment_urls")
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
    logger.info("Search %s finished; %d URLs published", sid, len(urls))


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


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
async def health_check():
    channel = getattr(app.state, "amqp_channel", None)
    healthy = channel and not channel.is_closed
    return {"status": "healthy" if healthy else "unhealthy"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
