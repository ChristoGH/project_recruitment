from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List
import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
import os
import logging
from datetime import datetime
import requests
from bs4 import BeautifulSoup
from uuid import uuid4
from contextlib import asynccontextmanager

from recruitment.logging_config import setup_logging
from recruitment.rabbitmq_utils import publish_json, RABBIT_QUEUE, ensure_queue_exists
from recruitment.recruitment_models import transform_skills_response
from recruitment.web_crawler_lib import crawl_website_sync, WebCrawlerResult
from recruitment.recruitment_db import init_db, save_raw_content

# Setup logging
logger = setup_logging("url_discovery")

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan handler for FastAPI application."""
    await ensure_queue_exists()
    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        perform_scheduled_search,
        IntervalTrigger(minutes=60),
        id="recruitment_search",
        replace_existing=True
    )
    scheduler.start()
    yield
    scheduler.shutdown()

app = FastAPI(lifespan=lifespan)

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
    """Get a list of recruitment URLs from various sources."""
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
            response = requests.get(
                f"https://www.google.com/search?q={term}&num=100",
                headers={"User-Agent": "Mozilla/5.0"}
            )
            soup = BeautifulSoup(response.text, 'html.parser')
            
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

async def perform_search(cfg: SearchConfig):
    """Perform a search for recruitment URLs and publish them to the queue."""
    try:
        urls = await get_recruitment_urls()
        search_id = uuid4().hex
        
        for url in urls:
            await publish_json({
                "url": url,
                "search_id": search_id,
                "timestamp": datetime.now().isoformat()
            })
        
        return {
            "status": "success",
            "urls_published": len(urls),
            "search_id": search_id
        }
    except Exception as e:
        logger.error(f"Error performing search: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

async def perform_scheduled_search():
    """Perform a scheduled search for recruitment URLs."""
    try:
        urls = await get_recruitment_urls()
        search_id = uuid4().hex
        
        for url in urls:
            await publish_json({
                "url": url,
                "search_id": search_id,
                "timestamp": datetime.now().isoformat()
            })
        
        logger.info(f"Completed scheduled search. Found {len(urls)} URLs.")
    except Exception as e:
        logger.error(f"Error in scheduled search: {str(e)}")

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    try:
        from recruitment.rabbitmq_utils import get_channel
        ch = await get_channel()
        return {"status": "up", "queue": ch.number}
    except Exception as e:
        return {"status": "down", "error": str(e)}

@app.post("/search")
async def search(config: SearchConfig):
    """Trigger a search for recruitment URLs."""
    return await perform_search(config)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 