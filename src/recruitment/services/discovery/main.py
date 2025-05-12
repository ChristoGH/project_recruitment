#!/usr/bin/env python3
"""
URL Discovery Service

This FastAPI service searches for recruitment URLs and publishes them to a RabbitMQ queue.
It's based on the working recruitment_ad_search.py script.
"""

import os
import json
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Set, Annotated
from urllib.parse import urlparse
from googlesearch import search
from pydantic import BaseModel, Field, validator
from pydantic_settings import BaseSettings
from fastapi import FastAPI, BackgroundTasks, HTTPException, status, Depends
from fastapi.middleware.cors import CORSMiddleware
import asyncio
from dotenv import load_dotenv
from contextlib import asynccontextmanager
import aio_pika
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from pathlib import Path
from functools import partial
from asyncio import to_thread, sleep
from dataclasses import dataclass
import logging.handlers
import json_logging

from ...logging_config import setup_logging
from ...utils.rabbitmq import RabbitMQConnection, get_rabbitmq_connection, RABBIT_QUEUE
from ...models.url_models import URLDiscoveryConfig, transform_skills_response
from ...utils.web_crawler import crawl_website_sync, WebCrawlerResult, crawl_website_sync_v2
from ...db.repository import RecruitmentDatabase
from ...utils.url_loader import load_urls_from_directory

# Load environment variables
load_dotenv()

# Configure JSON logging
class JSONFormatter(logging.Formatter):
    """JSON formatter for structured logging."""
    def format(self, record):
        log_record = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno
        }
        if hasattr(record, "extra"):
            log_record.update(record.extra)
        return json.dumps(log_record)

# Create module-specific logger with JSON formatting
logger = setup_logging(__name__)
handler = logging.StreamHandler()
handler.setFormatter(JSONFormatter())
logger.addHandler(handler)

# Get the absolute path to the project root
PROJECT_ROOT = Path(__file__).parent.parent.parent
DEFAULT_DB_PATH = str(PROJECT_ROOT / "databases" / "recruitment.db")

class Settings(BaseSettings):
    """Application settings."""
    # --- RabbitMQ ---
    rabbitmq_host: str = Field(default="localhost", env="RABBITMQ_HOST")
    rabbitmq_port: int = Field(default=5672, env="RABBITMQ_PORT")
    rabbitmq_user: str = Field(default="guest", env="RABBITMQ_USER")
    rabbitmq_password: str = Field(default="guest", env="RABBITMQ_PASSWORD")
    rabbitmq_vhost: str = Field(default="/", env="RABBITMQ_VHOST")
    
    # --- Database ---
    recruitment_db_url: str = Field(default=DEFAULT_DB_PATH, env="RECRUITMENT_DB_URL")
    
    # --- Search Configuration ---
    search_days_back: int = Field(default=7, env="SEARCH_DAYS_BACK")
    search_interval_seconds: int = Field(default=1800, env="SEARCH_INTERVAL_SECONDS")  # 30 minutes
    
    # --- Batch Sizes ---
    google_search_batch_size: int = Field(default=50, env="GOOGLE_SEARCH_BATCH_SIZE")
    publish_batch_size: int = Field(default=50, env="PUBLISH_BATCH_SIZE")
    csv_read_batch_size: int = Field(default=200, env="CSV_READ_BATCH_SIZE")

    class Config:
        env_file = ".env"
        case_sensitive = True

# Initialize settings
settings = Settings()

# Pydantic models for API
class SearchConfig(BaseModel):
    id: str
    days_back: int = Field(default=settings.search_days_back)
    excluded_domains: List[str] = Field(default=[])
    academic_suffixes: List[str] = Field(default=[])
    recruitment_terms: List[str] = Field(
        default=[
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
    locations: List[str] = Field(default=["South Africa"])
    batch_size: int = Field(default=settings.google_search_batch_size)
    search_interval_seconds: int = Field(default=settings.search_interval_seconds)

    @validator("recruitment_terms", "locations", "excluded_domains", "academic_suffixes")
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
    
    def add(self, search_id: str, urls: List[str] = None) -> None:
        """Add a new search result."""
        self._results[search_id] = SearchResult(
            search_id=search_id,
            status="pending",
            urls_found=len(urls) if urls else 0,
            urls=urls or [],
            timestamp=datetime.now().isoformat()
        )
        logger.info("Added new search result", extra={
            "search_id": search_id,
            "urls_found": len(urls) if urls else 0
        })
    
    def update_status(self, search_id: str, status: str, error: str = None) -> None:
        """Update the status of a search result."""
        if search_id in self._results:
            self._results[search_id].status = status
            if error:
                self._results[search_id].error = error
            logger.info("Updated search status", extra={
                "search_id": search_id,
                "status": status,
                "error": error
            })
    
    def get(self, search_id: str) -> Optional[SearchResult]:
        """Get a search result by ID."""
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
        2. Dangerous schemes
        3. Domain validation
    """
    try:
        result = urlparse(url)
        # Check scheme first (cheapest check)
        if result.scheme not in {"http", "https"}:
            logger.debug("Invalid URL scheme", extra={"url": url, "scheme": result.scheme})
            return False
            
        # Check for dangerous schemes
        if result.scheme in {"data", "javascript", "vbscript", "file"}:
            logger.debug("Dangerous URL scheme", extra={"url": url, "scheme": result.scheme})
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
            List of valid URLs found in CSV files
        """
        try:
            return load_urls_from_directory(
                directory="/app/csvs",
                url_validator=self.is_valid_recruitment_site,
                max_results=max_results or settings.csv_read_batch_size,
                file_pattern="*.csv"
            )
        except Exception as e:
            logger.error(f"Error reading URLs from CSV: {e}")
            return []

    async def fetch_ads_with_retry(self, query: str, max_results: int = None, max_retries: int = 3) -> List[str]:
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
        
        for attempt in range(max_retries):
            try:
                results = await to_thread(
                    partial(search, query, lang="en", num=10, start=0,
                            stop=max_results, pause=2)
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
        end_date = datetime.now()
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

async def get_rabbitmq_channel():
    """Get or create a RabbitMQ channel."""
    if not hasattr(app.state, 'rabbitmq') or app.state.rabbitmq.is_closed:
        connection = await get_rabbitmq_connection()
        app.state.rabbitmq = connection
    
    if not hasattr(app.state, 'rabbitmq_channel'):
        app.state.rabbitmq_channel = await app.state.rabbitmq.channel()
    
    return app.state.rabbitmq_channel

async def publish_urls_to_queue(urls: List[str], search_id: str, channel: aio_pika.Channel):
    """Publish URLs to RabbitMQ queue."""
    try:
        # Declare the queue to ensure it exists
        queue = await channel.declare_queue(
            RABBIT_QUEUE,
            durable=True
        )
        
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
                        "timestamp": datetime.now().isoformat()
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
        search_results.update_status(search_id, "error", str(e))

async def perform_search(
    search_config: SearchConfig,
    background_tasks: BackgroundTasks,
    db: DB,
    rabbitmq: RabbitMQ,
    scheduler: Scheduler
) -> SearchResponse:
    """Perform a search for recruitment URLs."""
    try:
        search_id = f"{search_config.id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        search_results.add(search_id)
        
        searcher = RecruitmentAdSearch(search_config)
        urls = await searcher.get_recent_ads()
        
        search_results.update_status(search_id, "completed")
        search_results.add(search_id, urls)
        
        # Create response first
        response = SearchResponse(
            search_id=search_id,
            urls_found=len(urls),
            urls=urls,
            timestamp=datetime.now().isoformat()
        )
        
        # Then add the background task
        background_tasks.add_task(publish_urls_to_queue, urls, search_id, rabbitmq)
        
        return response
        
    except Exception as e:
        logger.error(f"Error creating search: {e}")
        raise HTTPException(status_code=500, detail=str(e))

async def perform_scheduled_search(search_config: SearchConfig):
    """Perform scheduled search for recruitment URLs."""
    search_id = f"{search_config.id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    try:
        logger.info(f"Starting scheduled search with config: {search_config.id}")
        
        search_results.add(search_id)
        
        searcher = RecruitmentAdSearch(search_config)
        urls = await searcher.get_recent_ads()
        
        # Limit URLs to batch size
        urls = urls[:search_config.batch_size]
        
        search_results.update_status(search_id, "completed", None)
        search_results.add(search_id, urls)
        
        # Publish URLs to queue
        await publish_urls_to_queue(urls, search_id)
        
        logger.info(f"Completed scheduled search. Found {len(urls)} URLs.")
        
    except Exception as e:
        logger.error(f"Error in scheduled search: {e}")
        search_results.update_status(search_id, "error", str(e))

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for FastAPI application."""
    # Initialize database
    if not os.path.exists(settings.recruitment_db_url):
        raise RuntimeError(f"Database file not found at {settings.recruitment_db_url}. This is a required volume mount.")
    
    db = RecruitmentDatabase(settings.recruitment_db_url)
    db.initialize()
    app.state.db = db
    
    # Initialize RabbitMQ connection
    try:
        connection = await get_rabbitmq_connection(
            host=settings.rabbitmq_host,
            port=settings.rabbitmq_port,
            user=settings.rabbitmq_user,
            password=settings.rabbitmq_password,
            vhost=settings.rabbitmq_vhost
        )
        app.state.rabbitmq = connection
        channel = await connection.channel()
        app.state.rabbitmq_channel = channel
        await channel.declare_queue(RABBIT_QUEUE, durable=True)
        logger.info("RabbitMQ queue declared successfully")
    except Exception as e:
        logger.error(f"Failed to initialize RabbitMQ: {e}")
        raise

    # Initialize scheduler
    scheduler = AsyncIOScheduler()
    scheduler.start()
    app.state.scheduler = scheduler
    logger.info("Scheduler started")

    # Add initial search task
    search_config = SearchConfig(
        id="initial_search",
        days_back=settings.search_days_back,
        batch_size=settings.google_search_batch_size,
        search_interval_seconds=settings.search_interval_seconds
    )
    scheduler.add_job(
        perform_scheduled_search,
        'interval',
        seconds=settings.search_interval_seconds,
        args=[search_config],
        id="recruitment_search"
    )
    logger.info("Initial search task scheduled")

    yield

    # Cleanup
    scheduler.shutdown(wait=False)
    if hasattr(app.state, 'rabbitmq') and not app.state.rabbitmq.is_closed:
        await app.state.rabbitmq.close()

app = FastAPI(
    title="URL Discovery Service",
    description="Service for discovering recruitment URLs and publishing them to a queue",
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

# Dependencies
async def get_db() -> RecruitmentDatabase:
    """Get database instance."""
    if not hasattr(app.state, 'db'):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database not initialized"
        )
    return app.state.db

async def get_rabbitmq() -> aio_pika.Channel:
    """Get RabbitMQ channel."""
    if not hasattr(app.state, 'rabbitmq_channel'):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="RabbitMQ not initialized"
        )
    return app.state.rabbitmq_channel

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

@app.post("/search", response_model=SearchResponse)
async def create_search(
    search_config: SearchConfig,
    background_tasks: BackgroundTasks,
    db: DB,
    rabbitmq: RabbitMQ,
    scheduler: Scheduler
) -> SearchResponse:
    """Create a new search for recruitment URLs."""
    return await perform_search(
        search_config=search_config,
        background_tasks=background_tasks,
        db=db,
        rabbitmq=rabbitmq,
        scheduler=scheduler
    )

@app.get("/search/{search_id}", response_model=SearchStatus)
async def get_search_status(
    search_id: str,
    db: DB
) -> SearchStatus:
    """Get the status of a search."""
    result = search_results.get(search_id)
    if not result:
        raise HTTPException(status_code=404, detail=f"Search ID {search_id} not found")
    
    return SearchStatus(
        search_id=result.search_id,
        status=result.status,
        urls_found=result.urls_found,
        timestamp=result.timestamp
    )

@app.get("/search/{search_id}/urls", response_model=List[str])
async def get_search_urls(
    search_id: str,
    db: DB
) -> List[str]:
    """Get the URLs found by a search."""
    result = search_results.get(search_id)
    if not result:
        raise HTTPException(status_code=404, detail=f"Search ID {search_id} not found")
    
    return result.urls

@app.get("/health")
async def health_check(
    db: DB,
    rabbitmq: RabbitMQ
) -> Dict[str, Any]:
    """Health check endpoint for Docker healthcheck."""
    try:
        # Check database connection
        if not db.check_connection():
            return {"status": "unhealthy", "error": "Database connection failed"}
            
        # Check RabbitMQ connection
        if rabbitmq.is_closed:
            return {"status": "unhealthy", "error": "RabbitMQ connection closed"}
            
        return {"status": "healthy", "rabbitmq": "connected"}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}

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
        """Publish URLs to RabbitMQ queue."""
        for url in urls:
            await self.rabbitmq.publish_url(url)
        self.logger.info(f"Published {len(urls)} URLs to queue")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 