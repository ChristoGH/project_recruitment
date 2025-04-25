"""
Module for processing URLs.
"""
import logging
from typing import Dict, Any, Optional, List
from recruitment.models import URL
from recruitment.recruitment_db import RecruitmentDatabase
from recruitment.batch_processor import process_all_prompt_responses
from recruitment.logging_config import setup_logging

# Create module-specific logger
logger = setup_logging("url_processor")

class URLProcessor:
    """Class for processing URLs."""
    
    def __init__(self):
        """Initialize the URL processor."""
        self.db = RecruitmentDatabase()
    
    async def process_url(self, url: URL) -> Dict[str, Any]:
        """
        Process a URL without checking for existing records.
        
        Args:
            url: The URL to process
            
        Returns:
            Dict containing processing results and status
        """
        try:
            # Insert URL into database without checking for duplicates
            url_id = self.db.insert_url(url.url, url.domain, "direct")
            
            # Process the URL using batch processor
            results = process_all_prompt_responses(
                self.db,
                url_id,
                url.prompt_responses,
                use_transaction=True
            )
            
            # Update URL status based on results
            status = "completed" if results["success"] else "failed"
            self.db.update_url_processing_status(url_id, status, error_count=results["failed"])
            
            return {
                "url_id": url_id,
                "status": status,
                "processed": results["processed"],
                "failed": results["failed"],
                "errors": results["errors"]
            }
            
        except Exception as e:
            logger.error(f"Error processing URL {url.url}: {str(e)}")
            return {
                "url_id": None,
                "status": "failed",
                "processed": 0,
                "failed": 1,
                "errors": [str(e)]
            }

    def process_urls(self, urls: List[Dict[str, str]]) -> None:
        """Process a list of URLs and store them in the database."""
        for url_data in urls:
            try:
                url = url_data['url']
                domain = url_data.get('domain', '')
                source = url_data.get('source', '')
                
                # Insert URL and get its ID
                url_id = self.db.insert_url(url, domain, source)
                
                # Update processing status
                self.db.update_url_processing_status(
                    url_id=url_id,
                    status='pending',
                    error_message=None
                )
                
                logger.info(f"Successfully processed URL: {url}")
                
            except Exception as e:
                logger.error(f"Error processing URL {url_data.get('url', 'unknown')}: {str(e)}")
                continue 