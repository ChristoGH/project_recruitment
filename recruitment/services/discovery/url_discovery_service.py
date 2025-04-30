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
    """Configuration for URL search."""
    keywords: List[str] = [
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
    max_results: int = 100

async def get_recruitment_urls(keywords: List[str], locations: List[str]) -> List[str]:
    """Get a list of recruitment URLs from Google search."""
    urls = []
    
    for term in keywords:
        for location in locations:
            search_query = f"{term} {location}"
            try:
                logger.info(f"Starting search for query: {search_query}")
                response = requests.get(
                    f"https://www.google.com/search?q={search_query}&num=100",
                    headers={
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                        "Accept-Language": "en-US,en;q=0.5",
                        "Accept-Encoding": "gzip, deflate, br",
                        "Connection": "keep-alive",
                        "Upgrade-Insecure-Requests": "1",
                        "Cache-Control": "max-age=0"
                    }
                )
                
                logger.info(f"Google response status code: {response.status_code}")
                logger.info(f"Google response headers: {response.headers}")
                
                if response.status_code != 200:
                    logger.warning(f"Non-200 response from Google: {response.status_code}")
                    logger.warning(f"Response content: {response.text[:500]}")  # Log first 500 chars of response
                    continue
                    
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Check for CAPTCHA or blocking
                if "captcha" in response.text.lower() or "unusual traffic" in response.text.lower():
                    logger.error("Google is showing CAPTCHA or blocking the request")
                    logger.error(f"Response content: {response.text[:500]}")
                    continue
                
                found_urls = []
                
                # Log the HTML structure for debugging
                logger.debug(f"HTML structure: {soup.prettify()[:1000]}")
                
                # Try different selectors for Google search results
                for link in soup.find_all('a'):
                    href = link.get('href')
                    if href:
                        # Handle different URL formats
                        if href.startswith('/url?q='):
                            url = href.split('&')[0][7:]
                        elif href.startswith('http'):
                            url = href
                        else:
                            continue
                            
                        # Clean and validate URL
                        url = url.strip()
                        if not url.startswith(('http://', 'https://')):
                            continue
                            
                        if url not in urls:
                            urls.append(url)
                            found_urls.append(url)
                            logger.debug(f"Found valid URL: {url}")
                
                logger.info(f"Found {len(found_urls)} new URLs for term: \"{search_query}\"")
                
                # Add a longer delay between requests to avoid rate limiting
                await asyncio.sleep(5)
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Network error searching for term \"{search_query}\": {str(e)}")
            except Exception as e:
                logger.error(f"Unexpected error searching for term \"{search_query}\": {str(e)}")
    
    logger.info(f"Total unique URLs found: {len(urls)}")
    return urls

async def perform_search(cfg: SearchConfig):
    """Perform a search for recruitment URLs and publish them to the queue."""
    try:
        logger.info("Starting new search with config: %s", cfg)
        urls = await get_recruitment_urls(cfg.keywords, cfg.locations)
        search_id = uuid4().hex
        
        if not urls:
            logger.warning("No URLs found in search")
            return {
                "status": "no_results",
                "urls_published": 0,
                "search_id": search_id
            }
        
        logger.info(f"Publishing {len(urls)} URLs to queue")
        for url in urls:
            await publish_json({
                "url": url,
                "search_id": search_id,
                "timestamp": datetime.now().isoformat()
            })
        
        logger.info(f"Successfully published {len(urls)} URLs")
        return {
            "status": "success",
            "urls_published": len(urls),
            "search_id": search_id
        }
    except Exception as e:
        logger.error(f"Error performing search: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

async def perform_scheduled_search():
    """Perform a scheduled search for recruitment URLs."""
    try:
        logger.info("Starting scheduled search")
        cfg = SearchConfig()  # Use default configuration
        urls = await get_recruitment_urls(cfg.keywords, cfg.locations)
        search_id = uuid4().hex
        
        if not urls:
            logger.warning("No URLs found in scheduled search")
            return
            
        logger.info(f"Publishing {len(urls)} URLs from scheduled search")
        for url in urls:
            await publish_json({
                "url": url,
                "search_id": search_id,
                "timestamp": datetime.now().isoformat()
            })
        
        logger.info(f"Completed scheduled search. Found {len(urls)} URLs.")
    except Exception as e:
        logger.error(f"Error in scheduled search: {str(e)}", exc_info=True)

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