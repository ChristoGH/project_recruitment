#!/usr/bin/env python3
"""
URL Processing Service

This FastAPI service consumes URLs from a RabbitMQ queue and stores their content in the database.
"""

import os
import json
import asyncio
from datetime import datetime
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from contextlib import asynccontextmanager
import aio_pika
from pathlib import Path
from typing import Dict, Any, List
import psutil
from asyncio import Queue, Task
from concurrent.futures import ThreadPoolExecutor
import sqlite3

from ...logging_config import setup_logging, get_metrics_logger
from ...utils.rabbitmq import RABBIT_QUEUE
from ...utils.web_crawler_wrapper import crawl_website
from ...db.repository import (
    RecruitmentDatabase,
)

# Load environment variables
load_dotenv()

# Create module-specific logger and metrics logger
logger = setup_logging(__name__)
metrics_logger = get_metrics_logger(__name__)

# Service configuration
MAX_RETRIES = 3
RETRY_DELAY = 1  # seconds
MESSAGE_PROCESSING_TIMEOUT = 300  # 5 minutes
BATCH_SIZE = 10  # Number of URLs to process in parallel
MAX_WORKERS = 5  # Maximum number of concurrent workers

# Get database path from environment variable
project_root = Path(__file__).resolve().parent.parent.parent
DB_PATH = os.getenv(
    "RECRUITMENT_DB_PATH",
    str(project_root / "src" / "recruitment" / "db" / "recruitment.db"),
)

# Ensure database directory exists
try:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
except OSError as e:
    logger.warning(
        f"Could not create database directory: {e}. Using in-memory database."
    )
    DB_PATH = ":memory:"

logger.info(
    "Database configuration",
    extra={
        "db_path": DB_PATH,
        "db_dir_exists": os.path.exists(os.path.dirname(DB_PATH))
        if DB_PATH != ":memory:"
        else True,
        "db_file_exists": os.path.exists(DB_PATH) if DB_PATH != ":memory:" else True,
    },
)


class ProcessingError(Exception):
    """Base exception for processing errors."""

    pass


class CrawlerError(ProcessingError):
    """Exception raised when web crawler fails."""

    pass


class DatabaseError(ProcessingError):
    """Exception raised when database operations fail."""

    pass


class MessageProcessingError(ProcessingError):
    """Exception raised when message processing fails."""

    pass


class URLProcessor:
    """Handles URL processing with connection pooling and batch operations."""

    def __init__(self, db_path: str, max_workers: int = MAX_WORKERS):
        """Initialize the URL processor.

        Args:
            db_path: Path to the database file
            max_workers: Maximum number of concurrent workers
        """
        self.db = RecruitmentDatabase(db_path)  # Removed readonly=True
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.processing_queue = Queue()
        self.processing_tasks: List[Task] = []

    async def start(self):
        """Start the URL processor."""
        # Initialize database
        await self.db.ainit()

        # Start worker tasks
        for _ in range(MAX_WORKERS):
            task = asyncio.create_task(self._process_urls())
            self.processing_tasks.append(task)

        logger.info(
            "URL processor started",
            extra={"max_workers": MAX_WORKERS, "batch_size": BATCH_SIZE},
        )

    async def stop(self):
        """Stop the URL processor."""
        # Cancel all processing tasks
        for task in self.processing_tasks:
            task.cancel()

        # Wait for tasks to complete
        await asyncio.gather(*self.processing_tasks, return_exceptions=True)

        # Close database connection
        await self.db.close()

        logger.info("URL processor stopped")

    async def _process_urls(self):
        """Process URLs from the queue."""
        while True:
            try:
                # Get batch of URLs to process
                urls_to_process = []
                for _ in range(BATCH_SIZE):
                    try:
                        url_data = await asyncio.wait_for(
                            self.processing_queue.get(), timeout=1.0
                        )
                        urls_to_process.append(url_data)
                    except asyncio.TimeoutError:
                        break

                if not urls_to_process:
                    continue

                # Process URLs in parallel
                tasks = []
                for url_data in urls_to_process:
                    task = asyncio.create_task(self._process_single_url(url_data))
                    tasks.append(task)

                # Wait for all tasks to complete
                results = await asyncio.gather(*tasks, return_exceptions=True)

                # Handle results
                for url_data, result in zip(urls_to_process, results):
                    if isinstance(result, Exception):
                        logger.error(
                            "URL processing failed",
                            extra={"url": url_data["url"], "error": str(result)},
                        )
                    else:
                        logger.info(
                            "URL processing completed", extra={"url": url_data["url"]}
                        )

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error in URL processor", extra={"error": str(e)})
                await asyncio.sleep(RETRY_DELAY)

    async def _process_single_url(self, url_data: Dict[str, Any]) -> None:
        """Process a single URL.

        Args:
            url_data: Dictionary containing URL information
        """
        url = url_data["url"]
        search_id = url_data["search_id"]

        try:
            # Run the web crawler
            result = await crawl_website(url)
            if not result.success:
                raise CrawlerError(f"Web crawler failed: {result.error}")

            # First ensure URL exists and get its ID
            conn = await self.db._get_connection()
            try:
                async with conn.cursor() as cursor:
                    # Insert URL if not exists
                    await cursor.execute(
                        "INSERT OR IGNORE INTO urls (url, domain, source) VALUES (?, ?, ?)",
                        (url, url.split("/")[2], search_id),
                    )
                    await conn.commit()
                    logger.info("URL inserted/updated", extra={"url": url})

                    # Get the URL ID
                    await cursor.execute("SELECT id FROM urls WHERE url = ?", (url,))
                    row = await cursor.fetchone()
                    if not row:
                        raise sqlite3.Error(f"Failed to get URL ID for {url}")
                    url_id = row[0]
                    logger.info("Got URL ID", extra={"url": url, "url_id": url_id})

                    # Insert content with correct schema
                    try:
                        await cursor.execute(
                            "INSERT INTO raw_content (url_id, content) VALUES (?, ?)",
                            (url_id, result.markdown),
                        )
                        await conn.commit()
                        logger.info(
                            "Content stored",
                            extra={
                                "url": url,
                                "url_id": url_id,
                                "content_length": len(result.markdown),
                            },
                        )
                    except Exception as e:
                        logger.error(
                            "Failed to store content",
                            extra={
                                "url": url,
                                "url_id": url_id,
                                "error": str(e),
                                "error_type": type(e).__name__,
                            },
                        )
                        raise
            finally:
                await self.db._release_connection(conn)

            logger.info(
                "URL processed successfully",
                extra={
                    "url": url,
                    "search_id": search_id,
                    "content_length": len(result.markdown),
                },
            )

        except Exception as e:
            logger.error(
                "URL processing failed",
                extra={"url": url, "error": str(e), "error_type": type(e).__name__},
            )
            raise


async def process_message(
    message: aio_pika.IncomingMessage, processor: URLProcessor
) -> None:
    """Process a single message with timeout."""
    start_time = datetime.now()

    try:
        async with message.process():
            data = json.loads(message.body.decode())
            # Handle both 'sid' and 'search_id' fields
            search_id = data.get("search_id") or data.get("sid")
            if not search_id:
                raise KeyError("Neither 'search_id' nor 'sid' field found in message")

            url = data.get("url")
            if not url:
                raise KeyError("'url' field not found in message")

            logger.info(
                "Processing message",
                extra={"url": url, "search_id": search_id},
            )

            # Add to processing queue
            await processor.processing_queue.put({"url": url, "search_id": search_id})

            processing_time = (datetime.now() - start_time).total_seconds()
            logger.info(
                "Message queued for processing",
                extra={"url": url, "processing_time": processing_time},
            )

    except json.JSONDecodeError as e:
        logger.error("Failed to decode message", extra={"error": str(e)})
    except KeyError as e:
        logger.error("Missing required field", extra={"error": str(e)})
    except Exception as e:
        logger.error("Error processing message", extra={"error": str(e)})


async def consume_urls(processor: URLProcessor):
    """Consume URLs from RabbitMQ queue and store their content."""
    while True:
        try:
            # Get a new connection
            connection = await aio_pika.connect_robust(
                host=os.getenv("RABBITMQ_HOST", "rabbitmq"),
                port=int(os.getenv("RABBITMQ_PORT", 5672)),
                login=os.getenv("RABBITMQ_USER", "guest"),
                password=os.getenv("RABBITMQ_PASSWORD", "guest"),
                heartbeat=60,
            )

            async with connection:
                # Create a channel
                channel = await connection.channel()

                # Declare the queue
                queue = await channel.declare_queue(RABBIT_QUEUE, durable=True)

                logger.info(
                    "Starting URL consumer",
                    extra={
                        "queue": RABBIT_QUEUE,
                        "host": os.getenv("RABBITMQ_HOST", "rabbitmq"),
                    },
                )

                # Start consuming
                async with queue.iterator() as queue_iter:
                    async for message in queue_iter:
                        await process_message(message, processor)

                        # Log metrics periodically
                        metrics_logger.log_system_metrics()
                        metrics_logger.log_process_metrics()

        except aio_pika.exceptions.ConnectionClosed:
            logger.warning(
                "RabbitMQ connection closed",
                extra={"host": os.getenv("RABBITMQ_HOST", "rabbitmq")},
            )
            await asyncio.sleep(5)  # Wait before reconnecting
            continue

        except Exception as e:
            logger.error("Unexpected error in consumer", extra={"error": str(e)})
            await asyncio.sleep(5)  # Wait before retrying
            continue


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for FastAPI application."""
    logger.info("Starting service")

    # Initialize URL processor
    processor = URLProcessor(DB_PATH)
    await processor.start()
    app.state.processor = processor

    # Start consumer task
    consumer_task = asyncio.create_task(consume_urls(processor))
    app.state.consumer_task = consumer_task

    yield

    # Cleanup
    logger.info("Starting graceful shutdown")
    consumer_task.cancel()
    try:
        await consumer_task
    except asyncio.CancelledError:
        pass
    await processor.stop()
    logger.info("Service shutdown complete")


app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
async def health_check() -> Dict[str, Any]:
    """Health check endpoint."""
    try:
        # Check database connection
        db = RecruitmentDatabase(DB_PATH)
        if not await db.check_connection():
            raise HTTPException(status_code=503, detail="Database connection failed")

        # Get system metrics
        metrics = {
            "cpu_percent": psutil.cpu_percent(),
            "memory_percent": psutil.virtual_memory().percent,
            "disk_usage_percent": psutil.disk_usage("/").percent,
        }

        return {
            "status": "healthy",
            "database": "connected",
            "timestamp": datetime.now().isoformat(),
            "metrics": metrics,
        }
    except Exception as e:
        logger.error("Health check failed", extra={"error": str(e)})
        raise HTTPException(status_code=503, detail=str(e))


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8001)
