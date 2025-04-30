from fastapi import FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
import aio_pika
import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
import os
import logging
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import json
from fastapi import HTTPException

from recruitment.logging_config import setup_logging
from recruitment.rabbitmq_utils import get_rabbitmq_connection, RABBIT_QUEUE
from recruitment.recruitment_models import transform_skills_response
from recruitment.web_crawler_lib import crawl_website_sync, WebCrawlerResult
from recruitment.recruitment_db import init_db, save_raw_content

# Setup logging
logger = setup_logging("url_discovery")

app = FastAPI()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class SearchConfig(BaseModel):
    batch_size: int = 100
    search_interval_minutes: int = 60

async def get_recruitment_urls() -> List[str]:
    """
    Get a list of recruitment URLs from various sources.
    
    Returns:
        List[str]: List of recruitment URLs
    """
    urls = []
    search_terms = [
        "recruitment advert",
        "job vacancy",
        "hiring now",
        "employment opportunity",
        "career opportunity",
        "job advertisement",
        "recruitment drive"
    ]
    
    for term in search_terms:
        try:
            # Search for recruitment URLs
            response = requests.get(
                f"https://www.google.com/search?q={term}&num=100",
                headers={"User-Agent": "Mozilla/5.0"}
            )
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extract URLs from search results
            for link in soup.find_all('a'):
                href = link.get('href')
                if href and href.startswith('/url?q='):
                    url = href.split('&')[0][7:]
                    if url not in urls:
                        urls.append(url)
                        logger.info(f"Found valid URL: {url}")
            
            logger.info(f"Found {len(urls)} results for term: \"{term}\"")
        except Exception as e:
            logger.error(f"Error searching for term \"{term}\": {str(e)}")
    
    logger.info(f"Retrieved {len(urls)} recruitment ads in the past 7 day(s).")
    return urls

async def publish_urls_to_queue(urls: List[str], search_id: str):
    """Publish URLs to RabbitMQ queue."""
    try:
        ch = await get_rabbitmq_connection()
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

async def perform_search(background_tasks: BackgroundTasks):
    """Perform a search for recruitment URLs."""
    try:
        # Get RabbitMQ connection
        connection = await get_rabbitmq_connection()
        
        # Perform search and get URLs
        urls = await get_recruitment_urls()
        
        # Publish URLs to queue
        background_tasks.add_task(publish_urls_to_queue, urls, "search_id")
        
        return {"status": "success", "urls_found": len(urls)}
    except Exception as e:
        logger.error(f"Error performing search: {str(e)}")
        return {"status": "error", "message": str(e)}

async def perform_scheduled_search():
    """Perform a scheduled search for recruitment URLs."""
    try:
        # Get RabbitMQ connection
        connection = await get_rabbitmq_connection()
        
        # Perform search and get URLs
        urls = await get_recruitment_urls()
        
        # Publish URLs to queue
        await publish_urls_to_queue(urls, "scheduled_search")
        
        logger.info(f"Completed scheduled search. Found {len(urls)} URLs.")
    except Exception as e:
        logger.error(f"Error in scheduled search: {str(e)}")

@app.on_event("startup")
async def startup_event():
    """Initialize scheduler and start scheduled tasks."""
    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        perform_scheduled_search,
        IntervalTrigger(minutes=60),
        id="recruitment_search",
        replace_existing=True
    )
    scheduler.start()

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    try:
        connection = await get_rabbitmq_connection()
        await connection.close()
        return {"status": "healthy"}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}

@app.post("/search")
async def search(config: SearchConfig, background_tasks: BackgroundTasks):
    """Trigger a search for recruitment URLs."""
    return await perform_search(background_tasks)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 