import asyncio
import requests
from typing import List
from recruitment.url_discovery_service import RecruitmentAdSearch, SearchConfig

async def populate_queue_via_api(search_config: SearchConfig) -> None:
    """Populate queue using the FastAPI endpoint."""
    try:
        # Make POST request to the URL Discovery Service
        response = requests.post(
            "http://localhost:8000/search",
            json=search_config.model_dump()
        )
        response.raise_for_status()
        
        result = response.json()
        print(f"Search created with ID: {result['search_id']}")
        print(f"Found {result['urls_found']} URLs")
        print(f"URLs: {result['urls']}")
        
    except Exception as e:
        print(f"Error calling API: {e}")

async def populate_queue_direct(search_config: SearchConfig) -> None:
    """Populate queue using the RecruitmentAdSearch class directly."""
    try:
        # Create search instance
        searcher = RecruitmentAdSearch(search_config)
        
        # Get recent ads
        urls = searcher.get_recent_ads()
        
        print(f"Found {len(urls)} URLs")
        print(f"URLs: {urls}")
        
        # Note: To actually publish to the queue, you would need to:
        # 1. Set up RabbitMQ connection
        # 2. Call publish_urls_to_queue(urls, search_id)
        # This is handled automatically by the API endpoint
        
    except Exception as e:
        print(f"Error in direct search: {e}")

async def main():
    # Example search configuration
    search_config = SearchConfig(
        id="test_search",
        days_back=7,  # Search for ads from the last 7 days
        excluded_domains=["facebook.com", "twitter.com"],
        academic_suffixes=[".edu", ".ac.za"],
        recruitment_terms=[
            '"recruitment advert"',
            '"job vacancy"',
            '"hiring now"',
            '"employment opportunity"',
            '"career opportunity"',
            '"job advertisement"',
            '"recruitment drive"'
        ]
    )
    
    # Choose which method to use
    method = input("Choose method (1: API, 2: Direct): ")
    
    if method == "1":
        await populate_queue_via_api(search_config)
    elif method == "2":
        await populate_queue_direct(search_config)
    else:
        print("Invalid method selected")

if __name__ == "__main__":
    asyncio.run(main()) 