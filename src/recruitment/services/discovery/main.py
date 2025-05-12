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
import csv
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
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
from pathlib import Path

from ...logging_config import setup_logging
from ...utils.rabbitmq import RabbitMQConnection, get_rabbitmq_connection, RABBIT_QUEUE
from ...models.url_models import URLDiscoveryConfig, transform_skills_response
from ...utils.web_crawler import crawl_website_sync, WebCrawlerResult, crawl_website_sync_v2
from ...db.repository import RecruitmentDatabase

# Load environment variables
load_dotenv()

# Create module-specific logger
logger = setup_logging(__name__)

# Get the absolute path to the project root
PROJECT_ROOT = Path(__file__).parent.parent.parent
DB_PATH = os.getenv("RECRUITMENT_PATH", str(PROJECT_ROOT / "databases" / "recruitment.db"))

# Ensure database directory exists
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

# Initialize database
db = RecruitmentDatabase(DB_PATH)
db.initialize()

# Pydantic models for API
class SearchConfig(BaseModel):
    id: str
    days_back: int = 7
    excluded_domains: List[str] = []
    academic_suffixes: List[str] = []
    recruitment_terms: List[str] = [
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
    locations: List[str] = ["South Africa"]
    batch_size: int = 50
    search_interval_minutes: int = 30

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

# Global variables
search_results: Dict[str, Dict] = {}

def is_valid_url(url: str) -> bool:
    """Validate URL format and structure."""
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except Exception:
        return False

class RecruitmentAdSearch:
    """Encapsulates search logic for recruitment advertisements."""

    def __init__(self, search_config: SearchConfig) -> None:
        """Initialize the search handler."""
        self.search_name = search_config.id
        self.days_back = search_config.days_back
        self.excluded_domains = search_config.excluded_domains
        self.academic_suffixes = search_config.academic_suffixes
        self.recruitment_terms = search_config.recruitment_terms
        self.locations = search_config.locations

    def is_valid_recruitment_site(self, url: str) -> bool:
        """Filter out non-relevant domains if necessary."""
        try:
            if not is_valid_url(url):
                logger.debug(f"Invalid URL format: {url}")
                return False

            parsed = urlparse(url)
            domain = parsed.netloc.lower()
            
            logger.debug(f"Checking domain: {domain}")

            if any(excluded in domain for excluded in self.excluded_domains):
                logger.debug(f"Excluded domain: {url}")
                return False

            if any(domain.endswith(suffix) for suffix in self.academic_suffixes):
                logger.debug(f"Academic domain excluded: {url}")
                return False

            return True

        except Exception as e:
            logger.error(f"Error checking domain validity for {url}: {e}")
            return False

    def construct_query(self, start_date: str, end_date: str) -> str:
        """Construct a targeted search query for recruitment adverts."""
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

    def read_urls_from_csv(self, max_results: int = 200) -> List[str]:
        """Read URLs from CSV files in the data directory."""
        urls = []
        # Check the mounted CSV directory
        csv_dir = "/app/csvs"
        
        try:
            if not os.path.exists(csv_dir):
                logger.error(f"CSV directory does not exist: {csv_dir}")
                return []
                
            # Get all CSV files from the directory
            csv_files = [os.path.join(csv_dir, f) for f in os.listdir(csv_dir) if f.endswith('.csv')]
            logger.info(f"Found {len(csv_files)} CSV files in {csv_dir}: {csv_files}")
            
            if not csv_files:
                logger.error(f"No CSV files found in directory: {csv_dir}")
                return []
            
            for file_path in csv_files:
                logger.info(f"Reading URLs from {file_path}")
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        reader = csv.DictReader(f)
                        row_count = 0
                        valid_urls = 0
                        for row in reader:
                            row_count += 1
                            if 'URL' not in row:
                                logger.error(f"CSV file {file_path} does not contain 'URL' column. Available columns: {list(row.keys())}")
                                continue
                                
                            url = row['URL']
                            if self.is_valid_recruitment_site(url):
                                urls.append(url)
                                valid_urls += 1
                                if len(urls) >= max_results:
                                    logger.info(f"Reached max_results limit of {max_results}")
                                    return urls
                            
                        logger.info(f"Processed {row_count} rows from {file_path}, found {valid_urls} valid URLs")
                            
                except Exception as e:
                    logger.error(f"Error reading file {file_path}: {str(e)}")
                    continue
                            
            logger.info(f"Successfully read {len(urls)} valid URLs from all CSV files")
            return urls
            
        except Exception as e:
            logger.error(f"Error reading CSV files: {e}")
            return []

    def fetch_ads_with_retry(self, query: str, max_results: int = 5, max_retries: int = 3) -> List[str]:
        """Fetch recruitment ads via live Google Search."""
        articles: list[str] = []
        for attempt in range(max_retries):
            try:
                results = search(query, lang="en", num=10, start=0, stop=max_results, pause=2)
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
                logger.warning("Google search failed on attempt %s/%s: %s", attempt + 1, max_retries, exc)
                time.sleep(2 ** attempt)  # exponential back-off
        return articles[:max_results]

    def get_date_range(self) -> tuple[str, str]:
        """Get the date range for the search."""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=self.days_back)
        return (
            start_date.strftime("%Y-%m-%d"),
            end_date.strftime("%Y-%m-%d")
        )

    def get_recent_ads(self) -> List[str]:
        """Retrieve recent recruitment advertisements based on days_back in the configuration."""
        start_date, end_date = self.get_date_range()
        query = self.construct_query(start_date=start_date, end_date=end_date)
        logger.info(f"Searching for recruitment ads with query: {query}")
        
        ads = self.fetch_ads_with_retry(query)
        
        logger.info(f"Retrieved {len(ads)} recruitment ads in the past {self.days_back} day(s).")
        
        return ads

async def publish_urls_to_queue(urls: List[str], search_id: str):
    """Publish URLs to RabbitMQ queue."""
    try:
        # Create a new connection for this operation
        connection = await aio_pika.connect_robust(
            host=os.getenv("RABBITMQ_HOST", "localhost"),
            port=int(os.getenv("RABBITMQ_PORT", 5672)),
            login=os.getenv("RABBITMQ_USER", "guest"),
            password=os.getenv("RABBITMQ_PASSWORD", "guest"),
            heartbeat=60,
        )
        
        async with connection:
            channel = await connection.channel()
            
            # Declare the queue to ensure it exists
            queue = await channel.declare_queue(
                RABBIT_QUEUE,
                durable=True
            )
            
            logger.info(f"Starting to publish {len(urls)} URLs to RabbitMQ queue")
            
            for i, url in enumerate(urls, 1):
                message = aio_pika.Message(
                    body=json.dumps({
                        "url": url,
                        "search_id": search_id,
                        "timestamp": datetime.now().isoformat()
                    }).encode(),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                )
                
                await channel.default_exchange.publish(
                    message,
                    routing_key=RABBIT_QUEUE
                )
                
                if i % 10 == 0:  # Log progress every 10 URLs
                    logger.info(f"Published {i}/{len(urls)} URLs to queue")
                    
            logger.info(f"Successfully published all {len(urls)} URLs to RabbitMQ queue")
            
    except Exception as e:
        logger.error(f"Error publishing to RabbitMQ queue: {e}")
        # Don't raise HTTPException here as it's a background task
        search_results[search_id]["status"] = "error"
        search_results[search_id]["error"] = str(e)

async def perform_search(search_config: SearchConfig, background_tasks: BackgroundTasks) -> SearchResponse:
    """Perform a search for recruitment URLs."""
    try:
        search_id = f"{search_config.id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        search_results[search_id] = {
            "status": "pending",
            "urls_found": 0,
            "urls": [],
            "timestamp": datetime.now().isoformat()
        }
        
        searcher = RecruitmentAdSearch(search_config)
        urls = searcher.get_recent_ads()
        
        search_results[search_id]["urls"] = urls
        search_results[search_id]["urls_found"] = len(urls)
        
        # Create response first
        response = SearchResponse(
            search_id=search_id,
            urls_found=len(urls),
            urls=urls,
            timestamp=datetime.now().isoformat()
        )
        
        # Then add the background task
        background_tasks.add_task(publish_urls_to_queue, urls, search_id)
        
        return response
        
    except Exception as e:
        logger.error(f"Error creating search: {e}")
        raise HTTPException(status_code=500, detail=str(e))

async def perform_scheduled_search(search_config: SearchConfig):
    """Perform scheduled search for recruitment URLs."""
    try:
        logger.info(f"Starting scheduled search with config: {search_config.id}")
        search_id = f"{search_config.id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        search_results[search_id] = {
            "status": "pending",
            "urls_found": 0,
            "urls": [],
            "timestamp": datetime.now().isoformat()
        }
        
        searcher = RecruitmentAdSearch(search_config)
        urls = searcher.get_recent_ads()
        
        # Limit URLs to batch size
        urls = urls[:search_config.batch_size]
        
        search_results[search_id]["urls"] = urls
        search_results[search_id]["urls_found"] = len(urls)
        
        # Publish URLs to queue
        await publish_urls_to_queue(urls, search_id)
        
        logger.info(f"Completed scheduled search. Found {len(urls)} URLs.")
        
    except Exception as e:
        logger.error(f"Error in scheduled search: {e}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for FastAPI application."""
    # Initialize RabbitMQ connection
    try:
        connection = await get_rabbitmq_connection()
        channel = await connection.channel()
        await channel.declare_queue(RABBIT_QUEUE, durable=True)
        logger.info("RabbitMQ queue declared successfully")
    except Exception as e:
        logger.error(f"Failed to initialize RabbitMQ: {e}")
        raise

    # Initialize scheduler
    scheduler = AsyncIOScheduler()
    scheduler.start()
    logger.info("Scheduler started")

    # Add initial search task
    search_config = SearchConfig(
        id="initial_search",
        days_back=7,
        batch_size=50,
        search_interval_minutes=30
    )
    scheduler.add_job(
        perform_scheduled_search,
        'interval',
        minutes=search_config.search_interval_minutes,
        args=[search_config],
        id="recruitment_search"
    )
    logger.info("Initial search task scheduled")

    yield

    # Cleanup
    scheduler.shutdown()
    if not connection.is_closed:
        await connection.close()

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

@app.post("/search", response_model=SearchResponse)
async def create_search(search_config: SearchConfig, background_tasks: BackgroundTasks):
    """Create a new search for recruitment URLs."""
    return await perform_search(search_config=search_config, background_tasks=background_tasks)

@app.get("/search/{search_id}", response_model=SearchStatus)
async def get_search_status(search_id: str):
    """Get the status of a search."""
    if search_id not in search_results:
        raise HTTPException(status_code=404, detail=f"Search ID {search_id} not found")
    
    return SearchStatus(
        search_id=search_id,
        status=search_results[search_id]["status"],
        urls_found=search_results[search_id]["urls_found"],
        timestamp=search_results[search_id]["timestamp"]
    )

@app.get("/search/{search_id}/urls", response_model=List[str])
async def get_search_urls(search_id: str):
    """Get the URLs found by a search."""
    if search_id not in search_results:
        raise HTTPException(status_code=404, detail=f"Search ID {search_id} not found")
    
    return search_results[search_id]["urls"]

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
            search_interval_minutes=self.config.interval_minutes
        ))
        return searcher.get_recent_ads()

    async def publish_urls(self, urls: List[str]) -> None:
        """Publish URLs to RabbitMQ queue."""
        for url in urls:
            await self.rabbitmq.publish_url(url)
        self.logger.info(f"Published {len(urls)} URLs to queue")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 