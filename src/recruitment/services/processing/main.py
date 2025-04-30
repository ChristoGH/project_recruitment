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
from typing import Dict, Any, Optional, List
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from contextlib import asynccontextmanager
import aio_pika
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import sqlite3
from pathlib import Path
from urllib.parse import urlparse

from src.recruitment.logging_config import setup_logging
from src.recruitment.rabbitmq_utils import get_rabbitmq_connection, RABBIT_QUEUE, RabbitMQConnection
from src.recruitment.models import transform_skills_response
from src.recruitment.web_crawler_lib import crawl_website_sync, WebCrawlerResult, crawl_website_sync_v2
from src.recruitment.recruitment_db import RecruitmentDatabase
from src.recruitment.models.url_models import URLProcessingConfig, URLProcessingResult

# Load environment variables
load_dotenv()

# Create module-specific logger
logger = setup_logging(__name__)

# Get the absolute path to the project root
PROJECT_ROOT = Path(__file__).parent.parent.parent
DB_PATH = str(PROJECT_ROOT / "databases" / "recruitment.db")

# Initialize database
db = RecruitmentDatabase(DB_PATH)
db.initialize()

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

class URLProcessor:
    """Handles URL processing and database operations."""
    
    def __init__(self):
        """Initialize the URL processor."""
        self.db_path = DB_PATH
    
    async def process_url(self, url: str, search_id: str) -> Dict[str, Any]:
        """Process a single URL and store its content."""
        try:
            # Fetch content using web crawler
            result = await crawl_website_sync_v2(url)
            if not result.success:
                logger.error(f"Failed to fetch URL: {url}")
                return {"status": "error", "message": "Failed to fetch URL"}
            
            # Process content
            processed_content = process_response(result.markdown)
            if not processed_content:
                logger.error(f"Failed to process content for URL: {url}")
                return {"status": "error", "message": "Failed to process content"}
            
            # Store in database
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT INTO raw_content (url, content, search_id, timestamp)
                VALUES (?, ?, ?, ?)
            """, (
                url,
                json.dumps(processed_content),
                search_id,
                datetime.now().isoformat()
            ))
            
            conn.commit()
            conn.close()
            
            logger.info(f"Successfully processed and stored URL: {url}")
            return {"status": "success", "url": url}
            
        except Exception as e:
            logger.error(f"Error processing URL {url}: {e}")
            return {"status": "error", "message": str(e)}

async def consume_urls():
    """Consume URLs from RabbitMQ queue and process them."""
    try:
        ch = await get_channel()
        queue = await ch.get_queue(RABBIT_QUEUE)
        
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
                            logger.info(f"Successfully processed URL: {url}")
                        else:
                            logger.error(f"Failed to process URL {url}: {result['message']}")
                            
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    continue
                    
    except Exception as e:
        logger.error(f"Error in consumer: {e}")
        raise

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for FastAPI application."""
    # Initialize database
    init_db()
    
    # Initialize RabbitMQ connection
    try:
        ch = await get_channel()
        await ch.declare_queue(RABBIT_QUEUE, durable=True)
        logger.info("RabbitMQ queue declared successfully")
    except Exception as e:
        logger.error(f"Failed to initialize RabbitMQ: {e}")
        raise
    
    # Start consumer task
    consumer_task = asyncio.create_task(consume_urls())
    
    yield
    
    # Cleanup
    consumer_task.cancel()
    try:
        await consumer_task
    except asyncio.CancelledError:
        pass
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

class URLProcessingService:
    def __init__(
        self,
        config: URLProcessingConfig,
        rabbitmq: RabbitMQConnection,
        db: RecruitmentDatabase
    ):
        self.config = config
        self.rabbitmq = rabbitmq
        self.db = db

    async def process_url(self, url: str) -> URLProcessingResult:
        """Process a single URL."""
        try:
            # Mock implementation for testing
            return URLProcessingResult(
                url=url,
                title="Software Engineer",
                description="A great job opportunity",
                skills=["Python", "FastAPI", "RabbitMQ"],
                company="Example Corp",
                location="Remote",
                salary_range="$100k-$150k",
                job_type="Full-time"
            )
        except Exception as e:
            logger.error(f"Error processing URL {url}: {e}", exc_info=True)
            return URLProcessingResult(url=url, error=str(e))

    async def start_processing(self) -> None:
        """Start processing URLs from the queue."""
        try:
            async for url in self.rabbitmq.consume_urls():
                result = await self.process_url(url)
                if not result.error:
                    self.db.store_url(result)
                    logger.info(f"Processed and stored URL: {url}")
                else:
                    logger.error(f"Failed to process URL {url}: {result.error}")
        except Exception as e:
            logger.error(f"Error in URL processing: {e}", exc_info=True)

processing_service: Optional[URLProcessingService] = None

@app.on_event("startup")
async def startup_event():
    """Initialize the URL processing service on startup."""
    global processing_service
    config = URLProcessingConfig(
        max_concurrent_requests=5,
        request_timeout=30,
        retry_attempts=3
    )
    rabbitmq = RabbitMQConnection()
    await rabbitmq.connect()
    db = RecruitmentDatabase("databases/recruitment.db")
    db.initialize()
    processing_service = URLProcessingService(
        config=config,
        rabbitmq=rabbitmq,
        db=db
    )
    asyncio.create_task(processing_service.start_processing())

@app.get("/healthz")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy"}

@app.get("/readyz")
async def readiness_check():
    """Readiness check endpoint."""
    if not processing_service or not processing_service.rabbitmq.is_connected():
        raise HTTPException(status_code=503, detail="Service not ready")
    return {"status": "ready"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001) 