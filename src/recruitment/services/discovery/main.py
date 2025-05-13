#!/usr/bin/env python3
"""
URL Discovery Service

This FastAPI service discovers recruitment URLs and publishes them to a RabbitMQ queue.
"""

import os
import json
import logging
import asyncio
from datetime import datetime
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from contextlib import asynccontextmanager
import aio_pika
from pathlib import Path
from typing import Optional, Dict, Any, List
import psutil
from asyncio import Queue, Task
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from googlesearch import search
from asyncio import to_thread, sleep

from ...logging_config import setup_logging, get_metrics_logger
from ...utils.rabbitmq import get_rabbitmq_connection, RABBIT_QUEUE
from ...db.repository import RecruitmentDatabase, DatabaseError, DatabaseLockError, DatabaseConnectionError

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
MAX_RESULTS = int(os.getenv("GOOGLE_SEARCH_BATCH_SIZE", 50))

# Get database path from environment variable
project_root = Path(__file__).resolve().parent.parent.parent
DB_PATH = os.getenv("RECRUITMENT_DB_PATH", str(project_root / "src" / "recruitment" / "db" / "recruitment.db"))

# Ensure database directory exists
try:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
except OSError as e:
    logger.warning(f"Could not create database directory: {e}. Using in-memory database.")
    DB_PATH = ":memory:"

logger.info("Database configuration", extra={
    "db_path": DB_PATH,
    "db_dir_exists": os.path.exists(os.path.dirname(DB_PATH)) if DB_PATH != ":memory:" else True,
    "db_file_exists": os.path.exists(DB_PATH) if DB_PATH != ":memory:" else True
})

class DiscoveryError(Exception):
    """Base exception for discovery errors."""
    pass

class SearchError(DiscoveryError):
    """Exception raised when search fails."""
    pass

class MessageProcessingError(DiscoveryError):
    """Exception raised when message processing fails."""
    pass

class URLDiscoverer:
    """Handles URL discovery with connection pooling and batch operations."""
    
    def __init__(self, db_path: str, max_workers: int = MAX_WORKERS):
        """Initialize the URL discoverer.
        
        Args:
            db_path: Path to the database file
            max_workers: Maximum number of concurrent workers
        """
        self.db = RecruitmentDatabase(db_path)
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.discovery_queue = Queue()
        self.discovery_tasks: List[Task] = []
        self.rabbitmq_connection = None
        self.rabbitmq_channel = None
    
    async def start(self):
        """Start the URL discoverer."""
        # Initialize database
        await self.db.ainit()
        
        # Initialize RabbitMQ connection
        self.rabbitmq_connection = await get_rabbitmq_connection()
        self.rabbitmq_channel = await self.rabbitmq_connection.channel()
        await self.rabbitmq_channel.declare_queue(RABBIT_QUEUE, durable=True)
        
        # Start worker tasks
        for _ in range(MAX_WORKERS):
            task = asyncio.create_task(self._discover_urls())
            self.discovery_tasks.append(task)
        
        logger.info("URL discoverer started", extra={
            "max_workers": MAX_WORKERS,
            "batch_size": BATCH_SIZE,
            "max_results": MAX_RESULTS
        })
    
    async def stop(self):
        """Stop the URL discoverer."""
        # Cancel all discovery tasks
        for task in self.discovery_tasks:
            task.cancel()
        
        # Wait for tasks to complete
        await asyncio.gather(*self.discovery_tasks, return_exceptions=True)
        
        # Close RabbitMQ connection
        if self.rabbitmq_channel:
            await self.rabbitmq_channel.close()
        if self.rabbitmq_connection:
            await self.rabbitmq_connection.close()
        
        # Close database connection
        await self.db.close()
        
        logger.info("URL discoverer stopped")
    
    async def _discover_urls(self):
        """Discover URLs from the queue."""
        while True:
            try:
                # Get batch of search queries to process
                queries_to_process = []
                for _ in range(BATCH_SIZE):
                    try:
                        query_data = await asyncio.wait_for(
                            self.discovery_queue.get(),
                            timeout=1.0
                        )
                        queries_to_process.append(query_data)
                    except asyncio.TimeoutError:
                        break
                
                if not queries_to_process:
                    continue
                
                # Process queries in parallel
                tasks = []
                for query_data in queries_to_process:
                    task = asyncio.create_task(
                        self._process_single_query(query_data)
                    )
                    tasks.append(task)
                
                # Wait for all tasks to complete
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Handle results
                for query_data, result in zip(queries_to_process, results):
                    if isinstance(result, Exception):
                        logger.error("Query processing failed", extra={
                            "query": query_data["query"],
                            "error": str(result)
                        })
                    else:
                        logger.info("Query processing completed", extra={
                            "query": query_data["query"],
                            "urls_found": len(result)
                        })
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error in URL discoverer", extra={
                    "error": str(e)
                })
                await asyncio.sleep(RETRY_DELAY)
    
    async def _process_single_query(self, query_data: Dict[str, Any]) -> List[str]:
        """Process a single search query.
        
        Args:
            query_data: Dictionary containing query information
            
        Returns:
            List[str]: List of discovered URLs
        """
        query = query_data["query"]
        search_id = query_data["search_id"]
        
        try:
            # Run the search
            results = await to_thread(
                partial(search, query, lang="en", num_results=MAX_RESULTS, start=0,
                        stop=MAX_RESULTS, pause=2)
            )
            
            # Store URLs in database
            urls = [(url, url.split('/')[2], search_id) for url in results]
            await self.db.batch_insert_urls(urls)
            
            # Publish URLs to RabbitMQ
            for url in results:
                message = aio_pika.Message(
                    body=json.dumps({
                        "url": url,
                        "search_id": search_id
                    }).encode(),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT
                )
                await self.rabbitmq_channel.default_exchange.publish(
                    message,
                    routing_key=RABBIT_QUEUE
                )
            
            logger.info("Query processed successfully", extra={
                "query": query,
                "search_id": search_id,
                "urls_found": len(results)
            })
            
            return results
            
        except Exception as e:
            logger.error("Query processing failed", extra={
                "query": query,
                "error": str(e)
            })
            raise

async def process_message(message: aio_pika.IncomingMessage, discoverer: URLDiscoverer) -> None:
    """Process a single message with timeout."""
    start_time = datetime.now()
    
    try:
        async with message.process():
            data = json.loads(message.body.decode())
            logger.info("Processing message", extra={
                "query": data['query'],
                "search_id": data['search_id']
            })
            
            # Add to discovery queue
            await discoverer.discovery_queue.put(data)
            
            processing_time = (datetime.now() - start_time).total_seconds()
            logger.info("Message queued for processing", extra={
                "query": data['query'],
                "processing_time": processing_time
            })
                
    except json.JSONDecodeError as e:
        logger.error("Failed to decode message", extra={
            "error": str(e)
        })
    except KeyError as e:
        logger.error("Missing required field", extra={
            "error": str(e)
        })
    except Exception as e:
        logger.error("Error processing message", extra={
            "error": str(e)
        })

async def discover_urls(discoverer: URLDiscoverer):
    """Discover URLs and publish them to RabbitMQ queue."""
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
                
                logger.info("Starting URL discoverer", extra={
                    "queue": RABBIT_QUEUE,
                    "host": os.getenv("RABBITMQ_HOST", "rabbitmq")
                })
                
                # Start consuming
                async with queue.iterator() as queue_iter:
                    async for message in queue_iter:
                        await process_message(message, discoverer)
                        
                        # Log metrics periodically
                        metrics_logger.log_system_metrics()
                        metrics_logger.log_process_metrics()
                                
        except aio_pika.exceptions.ConnectionClosed:
            logger.warning("RabbitMQ connection closed", extra={
                "host": os.getenv("RABBITMQ_HOST", "rabbitmq")
            })
            await asyncio.sleep(5)  # Wait before reconnecting
            continue
            
        except Exception as e:
            logger.error("Unexpected error in discoverer", extra={
                "error": str(e)
            })
            await asyncio.sleep(5)  # Wait before retrying
            continue

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for FastAPI application."""
    logger.info("Starting service")
    
    # Initialize URL discoverer
    discoverer = URLDiscoverer(DB_PATH)
    await discoverer.start()
    app.state.discoverer = discoverer
    
    # Start discoverer task
    discoverer_task = asyncio.create_task(discover_urls(discoverer))
    app.state.discoverer_task = discoverer_task
    
    yield
    
    # Cleanup
    logger.info("Starting graceful shutdown")
    discoverer_task.cancel()
    try:
        await discoverer_task
    except asyncio.CancelledError:
        pass
    await discoverer.stop()
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
            "disk_usage_percent": psutil.disk_usage('/').percent
        }
        
        return {
            "status": "healthy",
            "database": "connected",
            "timestamp": datetime.now().isoformat(),
            "metrics": metrics
        }
    except Exception as e:
        logger.error("Health check failed", extra={
            "error": str(e)
        })
        raise HTTPException(status_code=503, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 