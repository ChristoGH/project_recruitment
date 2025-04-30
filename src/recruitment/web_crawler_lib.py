import aiohttp
import asyncio
import logging
from typing import Optional
from bs4 import BeautifulSoup
import requests
from dataclasses import dataclass
from datetime import datetime

logger = logging.getLogger(__name__)

@dataclass
class WebCrawlerResult:
    url: str
    content: Optional[str] = None
    error: Optional[str] = None
    timestamp: datetime = datetime.now()

async def crawl_website_sync(url: str) -> WebCrawlerResult:
    """
    Synchronously crawl a website and return its content.
    
    Args:
        url: The URL to crawl
        
    Returns:
        WebCrawlerResult: The result of the crawl
    """
    try:
        # Use requests for synchronous crawling
        response = requests.get(
            url,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            },
            timeout=30
        )
        response.raise_for_status()
        
        # Parse the content
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Remove script and style elements
        for script in soup(["script", "style"]):
            script.decompose()
            
        # Get text content
        text = soup.get_text()
        
        # Clean up the text
        lines = (line.strip() for line in text.splitlines())
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        text = ' '.join(chunk for chunk in chunks if chunk)
        
        logger.info(f"Successfully crawled URL: {url}")
        return WebCrawlerResult(url=url, content=text)
        
    except Exception as e:
        logger.error(f"Error crawling URL {url}: {str(e)}")
        return WebCrawlerResult(url=url, error=str(e))

async def crawl_website_sync_v2(url: str) -> WebCrawlerResult:
    """
    An alternative version of the synchronous crawler with different error handling.
    
    Args:
        url: The URL to crawl
        
    Returns:
        WebCrawlerResult: The result of the crawl
    """
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
                },
                timeout=30
            ) as response:
                response.raise_for_status()
                html = await response.text()
                
                # Parse the content
                soup = BeautifulSoup(html, 'html.parser')
                
                # Remove script and style elements
                for script in soup(["script", "style"]):
                    script.decompose()
                    
                # Get text content
                text = soup.get_text()
                
                # Clean up the text
                lines = (line.strip() for line in text.splitlines())
                chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
                text = ' '.join(chunk for chunk in chunks if chunk)
                
                logger.info(f"Successfully crawled URL: {url}")
                return WebCrawlerResult(url=url, content=text)
                
    except Exception as e:
        logger.error(f"Error crawling URL {url}: {str(e)}")
        return WebCrawlerResult(url=url, error=str(e)) 