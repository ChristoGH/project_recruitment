#!/usr/bin/env python3
"""
URL Discovery Service

This FastAPI service searches for recruitment URLs and publishes them to a RabbitMQ queue.
It's based on the working recruitment_ad_search.py script.
"""

import os
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional, Set, Annotated
from urllib.parse import urlparse
from googlesearch import search
from pydantic import BaseModel, Field, field_validator
from fastapi import FastAPI, BackgroundTasks, HTTPException, status, Depends
from fastapi.middleware.cors import CORSMiddleware
import asyncio
from dotenv import load_dotenv
from contextlib import asynccontextmanager
import aio_pika
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from functools import partial
from asyncio import to_thread, sleep
from dataclasses import dataclass
from ...logging_config import setup_logging
from ...utils.rabbitmq import RabbitMQConnection, get_rabbitmq_connection, RABBIT_QUEUE
from ...models.url_models import URLDiscoveryConfig
from ...db.repository import RecruitmentDatabase
from ...utils.url_loader import load_urls_from_directory
from ...config import settings
from aio_pika import Message, DeliveryMode
from aio_pika.exceptions import ChannelClosed
import random

async def perform_scheduled_search(channel: Optional[aio_pika.Channel] = None):
    """Perform scheduled search for recruitment URLs.
    
    Args:
        channel: Optional RabbitMQ channel to use. If not provided, a new one will be created.
    """
    try:
        # Create a default search config
        search_config = SearchConfig(
            id="scheduled_search",
            days_back=settings.search_days_back,
            batch_size=settings.google_search_batch_size,
            search_interval_seconds=settings.search_interval_seconds
        )
        
        # Perform the search
        searcher = RecruitmentAdSearch(search_config)
        urls = await searcher.get_recent_ads()
        
        # Publish URLs to queue
        if urls:
            try:
                # Use provided channel or get a new one
                channel_to_use = channel or await get_rabbitmq_channel()
                await publish_urls_to_queue(urls, search_config.id, channel_to_use)
            except Exception as e:
                logger.error("Failed to publish URLs to queue", extra={
                    "error": str(e),
                    "search_id": search_config.id,
                    "urls_count": len(urls)
                })
                # Don't re-raise - we want the scheduler to continue running
                return
            
        logger.info("Completed scheduled search", extra={
            "urls_found": len(urls),
            "search_id": search_config.id
        })
        
    except Exception as e:
        logger.error("Error in scheduled search", extra={
            "error": str(e),
            "search_id": "scheduled_search"
        })
        # Don't re-raise - we want the scheduler to continue running

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for FastAPI app."""
    # Initialize database
    app.state.db = await RecruitmentDatabase().ainit()
    
    # Initialize RabbitMQ connection
    app.state.rabbitmq_channel = await get_rabbitmq_connection()
    
    # Initialize scheduler
    app.state.scheduler = AsyncIOScheduler()
    app.state.scheduler.start()
    
    # Schedule initial search task with the channel
    app.state.scheduler.add_job(
        perform_scheduled_search,
        'interval',
        seconds=settings.search_interval_seconds,
        id='url_discovery_search',
        kwargs={"channel": app.state.rabbitmq_channel}
    )
    
    try:
        yield
    finally:
        # Cleanup in reverse order of initialization
        logger.info("Starting graceful shutdown...")
        
        # Shutdown scheduler first to stop any running jobs
        if hasattr(app.state, 'scheduler'):
            logger.info("Shutting down scheduler...")
            app.state.scheduler.shutdown(wait=True)
            logger.info("Scheduler shutdown complete")
        
        # Close RabbitMQ connection
        if hasattr(app.state, 'rabbitmq_channel'):
            logger.info("Closing RabbitMQ connection...")
            try:
                await app.state.rabbitmq_channel.close()
                logger.info("RabbitMQ connection closed")
            except Exception as e:
                logger.error(f"Error closing RabbitMQ connection: {e}")
        
        # Close database connection last
        if hasattr(app.state, 'db'):
            logger.info("Closing database connection...")
            try:
                await app.state.db.close()
                logger.info("Database connection closed")
            except Exception as e:
                logger.error(f"Error closing database connection: {e}")
        
        logger.info("Graceful shutdown complete")

# Initialize FastAPI app
app = FastAPI(
    title="URL Discovery Service",
    description="Service for discovering and publishing recruitment URLs",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

async def publish_urls_to_queue(urls: List[str], search_id: str, channel: aio_pika.Channel):
    """Publish URLs to RabbitMQ queue."""
    try:
        logger.info(f"Starting to publish {len(urls)} URLs to RabbitMQ queue")
        
        # Process URLs in batches
        for i in range(0, len(urls), settings.publish_batch_size):
            batch = urls[i:i + settings.publish_batch_size]
            messages = []
            
            # Prepare batch of messages
            for url in batch:
                message = aio_pika.Message(
                    body=json.dumps({
                        "url": url,
                        "search_id": search_id,
                        "timestamp": datetime.now(timezone.utc).isoformat()
                    }).encode(),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                )
                messages.append(message)
            
            # Publish batch
            for message in messages:
                await channel.default_exchange.publish(
                    message,
                    routing_key=RABBIT_QUEUE
                )
            
            logger.info(f"Published batch {i//settings.publish_batch_size + 1} of {(len(urls) + settings.publish_batch_size - 1)//settings.publish_batch_size}")
                
        logger.info(f"Successfully published all {len(urls)} URLs to RabbitMQ queue")
        
    except Exception as e:
        logger.error(f"Error publishing to RabbitMQ queue: {e}")
        await search_results.update_status(search_id, "error", str(e))

async def get_rabbitmq_channel() -> aio_pika.Channel:
    """Get or create a RabbitMQ channel with retry logic.
    
    Returns:
        aio_pika.Channel: A connected RabbitMQ channel
        
    Raises:
        HTTPException: If unable to establish connection after retries
    """
    max_retries = settings.retry_attempts
    retry_delay = settings.request_timeout  # Use configured timeout as base delay
    
    for attempt in range(max_retries):
        try:
            if not hasattr(app.state, 'rabbitmq_channel') or app.state.rabbitmq_channel.is_closed:
                if hasattr(app.state, 'rabbitmq_channel'):
                    logger.warning("RabbitMQ channel leaked; reopening", extra={
                        "attempt": attempt + 1,
                        "max_retries": max_retries
                    })
                    try:
                        await app.state.rabbitmq_channel.close()  # tidy leak
                    except Exception as e:
                        logger.warning(f"Error closing leaked channel: {e}")
                
                logger.info("Establishing new RabbitMQ connection", extra={
                    "attempt": attempt + 1,
                    "max_retries": max_retries
                })
                app.state.rabbitmq_channel = await get_rabbitmq_connection()
                
                # Declare queue once per channel lifecycle
                try:
                    await app.state.rabbitmq_channel.declare_queue(
                        RABBIT_QUEUE,
                        durable=True,
                        arguments={
                            'x-message-ttl': 86400000,  # 24 hours in milliseconds
                            'x-max-length': 100000,     # Maximum queue size
                            'x-overflow': 'reject-publish'  # Reject new messages when full
                        }
                    )
                    logger.info("Successfully declared RabbitMQ queue")
                except Exception as e:
                    logger.error(f"Error declaring queue: {e}")
                    raise
            
            return app.state.rabbitmq_channel
            
        except Exception as e:
            logger.error(f"RabbitMQ connection attempt {attempt + 1} failed", extra={
                "error": str(e),
                "attempt": attempt + 1,
                "max_retries": max_retries
            })
            
            if attempt == max_retries - 1:  # Last attempt
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail=f"Failed to establish RabbitMQ connection after {max_retries} attempts: {str(e)}"
                )
            
            # Exponential backoff with jitter
            delay = retry_delay * (2 ** attempt) + (random.random() * 0.1)
            logger.info(f"Retrying in {delay:.2f} seconds...")
            await asyncio.sleep(delay)
    
    # This should never be reached due to the raise in the loop
    raise HTTPException(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        detail="Failed to establish RabbitMQ connection"
    )

# Helper functions
async def get_db() -> RecruitmentDatabase:
    """Get database instance."""
    if not hasattr(app.state, 'db'):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database not initialized"
        )
    return app.state.db

async def get_rabbitmq() -> aio_pika.Channel:
    """Get RabbitMQ channel.
    
    Returns:
        aio_pika.Channel: A connected RabbitMQ channel
        
    Raises:
        HTTPException: If unable to establish connection
    """
    return await get_rabbitmq_channel()

async def get_scheduler() -> AsyncIOScheduler:
    """Get scheduler instance."""
    if not hasattr(app.state, 'scheduler'):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Scheduler not initialized"
        )
    return app.state.scheduler

# Type aliases for dependencies
DB = Annotated[RecruitmentDatabase, Depends(get_db)]
RabbitMQ = Annotated[aio_pika.Channel, Depends(get_rabbitmq)]
Scheduler = Annotated[AsyncIOScheduler, Depends(get_scheduler)]

# Load environment variables
load_dotenv()

# Configure JSON logging
class JSONFormatter(logging.Formatter):
    """JSON formatter for structured logging."""
    def format(self, record):
        log_record = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
            "service": "url_discovery"  # Add service context
        }
        if hasattr(record, "extra"):
            log_record.update(record.extra)
        return json.dumps(log_record)

# Create module-specific logger with JSON formatting
logger = setup_logging(__name__)
logger.propagate = False  # Prevent double emission if other libs attach handlers

# Replace the formatter on the existing handler instead of adding a new one
for h in logger.handlers:
    h.setFormatter(JSONFormatter())

# Ensure child loggers inherit JSON formatting
logging.root.handlers[:] = logger.handlers

# Pydantic models for API
class SearchConfig(BaseModel):
    id: str
    days_back: int = Field(default=settings.search_days_back)
    excluded_domains: List[str] = Field(default_factory=list)
    academic_suffixes: List[str] = Field(default_factory=list)
    recruitment_terms: List[str] = Field(
        default_factory=lambda: [
            "site:careers24.com job",
            "site:pnet.co.za job",
            "site:indeed.co.za job",
            "site:jobmail.co.za job",
            "site:careerjunction.co.za job",
            "site:jobvine.co.za job",
            "site:joburg.co.za job",
            "site:joburg.co.za vacancy",
            "site:joburg.co.za recruitment",
            "site:joburg.co.za hiring"
        ]
    )
    locations: List[str] = Field(default_factory=lambda: ["South Africa"])
    batch_size: int = Field(default=settings.google_search_batch_size)
    search_interval_seconds: int = Field(default=settings.search_interval_seconds)

    @field_validator(
        "recruitment_terms", "locations", "excluded_domains", "academic_suffixes",
        mode="before"
    )
    def split_env_strings(cls, v):
        if isinstance(v, str):
            return [x.strip() for x in v.split(",")]
        return v

class SearchResponse(BaseModel):
    search_id: str
    urls_found: int
    urls: List[str]
    timestamp: str

class SearchStatus(BaseModel):
    search_id: str
    status: str
    urls_found: int
    timestamp: str

@dataclass
class SearchResult:
    """Data class for storing search results."""
    search_id: str
    status: str
    urls_found: int
    urls: List[str]
    timestamp: str
    error: Optional[str] = None

class SearchResultsCache:
    """Cache for storing search results."""
    def __init__(self):
        self._results: Dict[str, SearchResult] = {}
        self._lock = asyncio.Lock()
    
    async def add(self, search_id: str, urls: Optional[List[str]] = None) -> None:
        """Add a new search result.
        
        Args:
            search_id: Unique identifier for the search
            urls: Optional list of URLs found in the search
        """
        async with self._lock:
            self._results[search_id] = SearchResult(
                search_id=search_id,
                status="pending",
                urls_found=len(urls) if urls else 0,
                urls=urls or [],
                timestamp=datetime.now(timezone.utc).isoformat()
            )
            logger.info("Added new search result", extra={
                "search_id": search_id,
                "urls_found": len(urls) if urls else 0
            })
    
    async def update_status(self, search_id: str, status: str, error: str = None) -> None:
        """Update the status of a search result."""
        async with self._lock:
            if search_id in self._results:
                self._results[search_id].status = status
                if error:
                    self._results[search_id].error = error
                logger.info("Updated search status", extra={
                    "search_id": search_id,
                    "status": status,
                    "error": error
                })
    
    async def update_urls(self, search_id: str, urls: List[str]) -> None:
        """Update the urls and urls_found for a search result."""
        async with self._lock:
            if search_id in self._results:
                self._results[search_id].urls = urls
                self._results[search_id].urls_found = len(urls)
    
    async def get(self, search_id: str) -> Optional[SearchResult]:
        """Get a search result by ID."""
        async with self._lock:
            return self._results.get(search_id)

# Initialize the cache
search_results = SearchResultsCache()

def is_valid_url(url: str) -> bool:
    """Validate URL format and structure.
    
    Args:
        url: The URL to validate
        
    Returns:
        bool: True if the URL is valid and safe
        
    Note:
        Performs checks in order of computational cost:
        1. Basic URL format (scheme, netloc)
        2. Domain validation
    """
    try:
        result = urlparse(url)
        # Check scheme first (cheapest check)
        if result.scheme not in {"http", "https"}:
            logger.debug("Invalid URL scheme", extra={"url": url, "scheme": result.scheme})
            return False
            
        # Check netloc (domain)
        if not result.netloc:
            logger.debug("Missing domain", extra={"url": url})
            return False
            
        return True
    except Exception as e:
        logger.error("URL validation error", extra={"url": url, "error": str(e)})
        return False

class RecruitmentAdSearch:
    """Encapsulates search logic for recruitment advertisements."""

    def __init__(self, search_config: SearchConfig) -> None:
        """Initialize the search handler.
        
        Args:
            search_config: Configuration for the search operation
        """
        self.search_name = search_config.id
        self.days_back = search_config.days_back
        self.excluded_domains: Set[str] = set(search_config.excluded_domains)
        self.academic_suffixes: Set[str] = set(search_config.academic_suffixes)
        self.recruitment_terms = search_config.recruitment_terms
        self.locations = search_config.locations
        self.batch_size = search_config.batch_size

    def is_valid_recruitment_site(self, url: str) -> bool:
        """Filter out non-relevant domains if necessary.
        
        Args:
            url: The URL to validate
            
        Returns:
            bool: True if the URL is valid for recruitment purposes
        """
        try:
            if not is_valid_url(url):
                return False

            parsed = urlparse(url)
            domain = parsed.netloc.lower()
            
            # Check excluded domains first (cheapest check)
            if any(excluded in domain for excluded in self.excluded_domains):
                logger.debug("Excluded domain", extra={"url": url, "domain": domain})
                return False

            # Check academic suffixes
            if any(domain.endswith(suffix) for suffix in self.academic_suffixes):
                logger.debug("Academic domain excluded", extra={"url": url, "domain": domain})
                return False

            return True

        except Exception as e:
            logger.error("Domain validation error", extra={"url": url, "error": str(e)})
            return False

    def read_urls_from_csv(self, max_results: int = None) -> List[str]:
        """Read URLs from CSV files in the data directory.
        
        Args:
            max_results: Maximum number of URLs to return
            
        Returns:
            List[str]: List of valid URLs found in CSV files. Returns empty list on error.
            
        Note:
            Returns an empty list on error to maintain type safety and allow graceful
            error handling. This is intentional to avoid propagating errors up the call stack.
        """
        try:
            return load_urls_from_directory(
                directory=settings.csv_dir,
                url_validator=self.is_valid_recruitment_site,
                max_results=max_results or settings.csv_read_batch_size,
                file_pattern="*.csv"
            )
        except Exception as e:
            logger.error(f"Error reading URLs from CSV: {e}")
            return []  # Return empty list on error

    async def fetch_ads_with_retry(self, query: str, max_results: int = None, max_retries: int = None) -> List[str]:
        """Fetch recruitment ads via live Google Search.
        
        Args:
            query: The search query to execute
            max_results: Maximum number of results to return
            max_retries: Number of retry attempts on failure
            
        Returns:
            List of valid URLs found in the search
        """
        articles: list[str] = []
        max_results = max_results or settings.google_search_batch_size
        max_retries = max_retries or settings.retry_attempts
        
        for attempt in range(max_retries):
            try:
                results = await to_thread(
                    partial(search, query, lang="en", num_results=max_results, start=0,
                            stop=max_results, pause=settings.request_timeout)
                )
                for url in results:
                    if self.is_valid_recruitment_site(url):
                        articles.append(url)
                        logger.info(f"Found valid URL: {url}")
                
                if len(articles) == 0:
                    logger.warning(f"No results found for query: {query}")
                else:
                    logger.info(f"Found {len(articles)} valid URLs")
                
                return articles[:max_results]
            except Exception as exc:
                logger.warning("Google search failed (%s/%s): %s", attempt+1,
                               max_retries, exc)
                await sleep(2 ** attempt)  # non-blocking exponential back-off
        return articles[:max_results]

    def get_date_range(self) -> tuple[str, str]:
        """Get the date range for the search.
        
        Returns:
            Tuple of (start_date, end_date) in YYYY-MM-DD format
        """
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=self.days_back)
        return (
            start_date.strftime("%Y-%m-%d"),
            end_date.strftime("%Y-%m-%d")
        )

    def construct_query(self, start_date: str, end_date: str) -> str:
        """Construct a targeted search query for recruitment adverts.
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            Formatted search query string
        """
        # Handle both site-specific and general search terms
        site_specific_terms = [term for term in self.recruitment_terms if "site:" in term]
        general_terms = [term for term in self.recruitment_terms if "site:" not in term]
        
        # Build the query parts
        site_specific_part = f"({' OR '.join(site_specific_terms)})" if site_specific_terms else ""
        general_part = f"({' OR '.join(general_terms)})" if general_terms else ""
        
        # Combine parts with OR if both exist
        recruitment_part = " OR ".join(filter(None, [site_specific_part, general_part]))
        
        # Add location filter
        location_part = " OR ".join(f'"{loc}"' for loc in self.locations)
        base_query = f"({recruitment_part}) AND ({location_part})"
        
        # Add date range
        final_query = f"{base_query} after:{start_date} before:{end_date}"
        logger.info(f"Constructed query: {final_query}")
        return final_query

    async def get_recent_ads(self) -> List[str]:
        """Retrieve recent recruitment advertisements based on days_back in the configuration.
        
        Returns:
            List of valid recruitment URLs found in the search
        """
        start_date, end_date = self.get_date_range()
        query = self.construct_query(start_date=start_date, end_date=end_date)
        logger.info("Searching for recruitment ads", extra={"query": query})
        
        ads = await self.fetch_ads_with_retry(query)
        
        logger.info("Retrieved recruitment ads", extra={
            "count": len(ads),
            "days_back": self.days_back
        })
        
        return ads[:self.batch_size]  # Limit to batch size

async def perform_search(
    search_config: SearchConfig,
    background_tasks: BackgroundTasks,
    rabbitmq: RabbitMQ
) -> SearchResponse:
    """Perform a search for recruitment URLs."""
    try:
        search_id = f"{search_config.id}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
        
        await search_results.add(search_id)
        
        searcher = RecruitmentAdSearch(search_config)
        urls = await searcher.get_recent_ads()
        
        await search_results.update_urls(search_id, urls)
        await search_results.update_status(search_id, "completed")
        
        # Create response first
        response = SearchResponse(
            search_id=search_id,
            urls_found=len(urls),
            urls=urls,
            timestamp=datetime.now(timezone.utc).isoformat()
        )
        
        # Then add the background task
        background_tasks.add_task(publish_urls_to_queue, urls, search_id, rabbitmq)
        
        return response
        
    except Exception as e:
        logger.error(f"Error creating search: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Route handlers
@app.post("/search", response_model=SearchResponse)
async def create_search(
    search_config: SearchConfig,
    background_tasks: BackgroundTasks,
    rabbitmq: RabbitMQ
) -> SearchResponse:
    """Create a new search for recruitment URLs."""
    return await perform_search(
        search_config=search_config,
        background_tasks=background_tasks,
        rabbitmq=rabbitmq
    )

@app.get("/search/{search_id}", response_model=SearchStatus)
async def get_search_status(search_id: str) -> SearchStatus:
    """Get the status of a search."""
    result = await search_results.get(search_id)
    if not result:
        raise HTTPException(status_code=404, detail=f"Search ID {search_id} not found")
    
    return SearchStatus(
        search_id=result.search_id,
        status=result.status,
        urls_found=result.urls_found,
        timestamp=result.timestamp
    )

@app.get("/search/{search_id}/urls", response_model=List[str])
async def get_search_urls(search_id: str) -> List[str]:
    """Get the URLs found by a search."""
    result = await search_results.get(search_id)
    if not result:
        raise HTTPException(status_code=404, detail=f"Search ID {search_id} not found")
    
    return result.urls

@app.get("/health")
async def health_check(
    rabbitmq: RabbitMQ,
    db: DB,
    scheduler: Scheduler
) -> Dict[str, Any]:
    """Health check endpoint for Docker healthcheck.
    
    Checks:
    - RabbitMQ connection and queue
    - Database connection
    - Scheduler status
    
    Returns:
        Dict[str, Any]: Health check response with the following structure:
            {
                "status": str,  # "healthy" or "unhealthy"
                "components": {  # Status of each component
                    "rabbitmq": str,    # "connected", "connection_closed", or "queue_not_found"
                    "database": str,    # "connected" or "error: <message>"
                    "scheduler": str    # "running" or "not_running"
                },
                "error": str,  # Optional error message if status is "unhealthy"
            }
    """
    health_status = {
        "status": "healthy",
        "components": {}
    }
    
    try:
        # Check RabbitMQ connection
        if rabbitmq.is_closed:
            health_status["status"] = "unhealthy"
            health_status["components"]["rabbitmq"] = "connection_closed"
            return health_status
        
        # Perform passive queue declaration to verify broker responsiveness
        try:
            await rabbitmq.declare_queue(RABBIT_QUEUE, passive=True)
            health_status["components"]["rabbitmq"] = "connected"
        except ChannelClosed:
            health_status["status"] = "unhealthy"
            health_status["components"]["rabbitmq"] = "queue_not_found"
            return health_status
            
        # Check database connection
        try:
            await db.ping()
            health_status["components"]["database"] = "connected"
        except Exception as e:
            health_status["status"] = "unhealthy"
            health_status["components"]["database"] = f"error: {str(e)}"
            return health_status
            
        # Check scheduler
        if not scheduler.running:
            health_status["status"] = "unhealthy"
            health_status["components"]["scheduler"] = "not_running"
            return health_status
        health_status["components"]["scheduler"] = "running"
            
        return health_status
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e),
            "components": health_status.get("components", {})
        }

class URLDiscoveryService:
    def __init__(self, config: URLDiscoveryConfig, rabbitmq: RabbitMQConnection):
        self.config = config
        self.rabbitmq = rabbitmq
        self.logger = setup_logging(__name__)

    async def discover_urls(self) -> List[str]:
        """Discover URLs based on configuration."""
        searcher = RecruitmentAdSearch(SearchConfig(
            id="discovery_service",
            days_back=7,
            batch_size=self.config.max_results,
            search_interval_seconds=self.config.interval_seconds
        ))
        return await searcher.get_recent_ads()

    async def publish_urls(self, urls: List[str]) -> None:
        """Publish URLs to RabbitMQ queue.
        
        Args:
            urls: List of URLs to publish
            
        Raises:
            Exception: If publishing fails
        """
        if not urls:
            self.logger.info("No URLs to publish")
            return
            
        try:
            channel = await get_rabbitmq_channel()
            await publish_urls_to_queue(urls, "discovery_service", channel)
            self.logger.info(f"Published {len(urls)} URLs to queue")
        except Exception as e:
            self.logger.error(f"Failed to publish URLs: {e}")
            raise

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 