"""Module for processing URLs from the RabbitMQ queue."""
import asyncio
from typing import List, Optional
from src.recruitment.db.repository import RecruitmentDatabase
from src.recruitment.models.url_models import URLProcessingResult
from src.recruitment.utils.web_crawler import crawl_website_sync_v2

async def process_urls_from_queue(
    urls: List[str],
    db: Optional[RecruitmentDatabase] = None
) -> List[URLProcessingResult]:
    """Process a list of URLs from the queue.
    
    Args:
        urls: List of URLs to process
        db: Optional RecruitmentDatabase instance
        
    Returns:
        List of processing results
    """
    if db is None:
        db = RecruitmentDatabase()
    
    results = []
    for url in urls:
        try:
            result = await crawl_website_sync_v2(url)
            if result.success:
                await db.save_processed_url(url, result.markdown, result.transformed)
                results.append(URLProcessingResult(url=url, success=True, content=result.markdown))
            else:
                results.append(URLProcessingResult(url=url, success=False, error=result.error))
        except Exception as e:
            # Log error and continue with next URL
            print(f"Error processing URL {url}: {str(e)}")
            results.append(URLProcessingResult(url=url, success=False, error=str(e)))
            continue
    
    return results 