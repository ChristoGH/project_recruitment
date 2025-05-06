"""Module for processing URLs from the RabbitMQ queue."""
import asyncio
from typing import List, Optional
from src.recruitment.db.repository import RecruitmentDatabase
from src.recruitment.models.url_models import URLProcessingResult
from src.recruitment.services.processing.main import URLProcessingService

async def process_urls_from_queue(
    urls: List[str],
    processing_service: Optional[URLProcessingService] = None,
    db: Optional[RecruitmentDatabase] = None
) -> List[URLProcessingResult]:
    """Process a list of URLs from the queue.
    
    Args:
        urls: List of URLs to process
        processing_service: Optional URLProcessingService instance
        db: Optional RecruitmentDatabase instance
        
    Returns:
        List of processing results
    """
    if processing_service is None:
        processing_service = URLProcessingService()
    
    if db is None:
        db = RecruitmentDatabase()
    
    results = []
    for url in urls:
        try:
            result = await processing_service.process_url(url)
            await db.save_result(result)
            results.append(result)
        except Exception as e:
            # Log error and continue with next URL
            print(f"Error processing URL {url}: {str(e)}")
            continue
    
    return results 