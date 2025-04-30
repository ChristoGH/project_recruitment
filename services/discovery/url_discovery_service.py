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

from recruitment.logging_config import setup_logging
from recruitment.rabbitmq_utils import get_channel, RABBIT_QUEUE

# Load environment variables
load_dotenv()

# Create module-specific logger
logger = setup_logging("url_discovery_service")

# Pydantic models for API
class SearchConfig(BaseModel):
    id: str
    days_back: int = 7
    excluded_domains: List[str] = []
    academic_suffixes: List[str] = []
    recruitment_terms: List[str] = [
        '"recruitment advert"',
        '"job vacancy"',
        '"hiring now"',
        '"employment opportunity"',
        '"career opportunity"',
        '"job advertisement"',
        '"recruitment drive"'
    ]
    batch_size: int = 100
    search_interval_minutes: int = 60

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

    def is_valid_recruitment_site(self, url: str) -> bool:
        """Filter out non-relevant domains if necessary."""
        try:
            if not is_valid_url(url):
                logger.debug(f"Invalid URL format: {url}")
                return False

            parsed = urlparse(url)
            domain = parsed.netloc.lower()

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
        recruitment_part = f"({' OR '.join(self.recruitment_terms)})"
        base_query = f'{recruitment_part} AND "South Africa"'
        final_query = f"{base_query} after:{start_date} before:{end_date}"
        logger.info(f"Constructed query: {final_query}")
        return final_query

    def fetch_ads_with_retry(self, query: str, max_results: int = 200, max_retries: int = 3) -> List[str]:
        """Fetch articles with retry logic for transient failures."""
        articles = []
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                # Try with tld parameter first
                try:
                    results = search(query, tld="com", lang="en", num=10, start=0, stop=max_results, pause=2)
                except TypeError:
                    # Fall back to version without tld parameter
                    results = search(query, lang="en", num_results=100, sleep_interval=5)

                # Validate and filter results
                for url in results:
                    if self.is_valid_recruitment_site(url):
                        articles.append(url)
                        logger.info(f"Found valid URL: {url}")
                
                # Log results per search term
                for term in self.recruitment_terms:
                    term_results = [url for url in articles if term.lower() in url.lower()]
                    logger.info(f"Found {len(term_results)} results for term: {term}")
                
                if len(articles) == 0:
                    logger.warning(f"No results found for query: {query}")
                
                return articles
                
            except Exception as e:
                retry_count += 1
                logger.warning(f"Attempt {retry_count} failed: {str(e)}")
                if retry_count < max_retries:
                    time.sleep(2 ** retry_count)  # Exponential backoff
                else:
                    logger.error(f"Failed to fetch articles after {max_retries} attempts: {str(e)}")
                    return []

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
        ch = await get_channel()
        for url in urls:
            await ch.default_exchange.publish(
                aio_pika.Message(
                    body=json.dumps({
                        "url": url,
                        "search_id": search_id,
                        "timestamp": datetime.now().isoformat()
                    }).encode(),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                ),
                routing_key=RABBIT_QUEUE,
            )
        logger.info(f"Published {len(urls)} URLs to RabbitMQ queue")
    except Exception as e:
        logger.error(f"Error publishing to RabbitMQ queue: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to publish URLs: {str(e)}")

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
        
        background_tasks.add_task(publish_urls_to_queue, urls, search_id)
        
        return SearchResponse(
            search_id=search_id,
            urls_found=len(urls),
            urls=urls,
            timestamp=datetime.now().isoformat()
        )
        
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
        ch = await get_channel()
        await ch.declare_queue(RABBIT_QUEUE, durable=True)
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
        batch_size=100,
        search_interval_minutes=60
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
    if not ch.is_closed:
        await ch.close()

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
        ch = await get_channel()
        if ch and not ch.is_closed:
            return {"status": "healthy", "rabbitmq": "connected"}
        return {"status": "unhealthy", "rabbitmq": "disconnected"}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 