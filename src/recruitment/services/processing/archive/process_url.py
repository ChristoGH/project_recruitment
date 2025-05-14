import asyncio
import json
from typing import Dict, Any
from urllib.parse import urlparse
from libraries.web_crawler_lib import crawl_website
from src.recruitment.prompts import LIST_PROMPTS
from src.recruitment.recruitment_models import AdvertResponse
from src.recruitment.utils import get_model_for_prompt

async def verify_recruitment_url(url: str) -> Dict[str, Any]:
    """
    Verify if a URL is a recruitment page by:
    1. Crawling the website
    2. Using the recruitment prompt to analyze the content
    """
    # Step 1: Crawl the website
    print(f"Crawling website: {url}")
    crawler_result = await crawl_website(
        url=url,
        word_count_threshold=10,
        excluded_tags=['form', 'header'],
        exclude_external_links=True,
        process_iframes=True,
        remove_overlay_elements=True,
        use_cache=True,
        verbose=True
    )
    
    # Step 2: Use the recruitment prompt to analyze content
    print("Analyzing content for recruitment verification...")
    recruitment_prompt = LIST_PROMPTS["recruitment_prompt"]
    # Here you would typically call your LLM with the prompt and crawler_result.text
    # For this example, we'll simulate a response
    response = {
        "answer": "yes",
        "evidence": ["The page contains job requirements", "There is a salary range mentioned"]
    }
    
    # Validate the response using the AdvertResponse model
    try:
        model = get_model_for_prompt("recruitment_prompt")
        validated_response = model(**response)
        return {
            "is_recruitment": validated_response.answer == "yes",
            "evidence": validated_response.evidence,
            "crawled_text": crawler_result.text
        }
    except Exception as e:
        print(f"Error validating response: {e}")
        return {
            "is_recruitment": False,
            "evidence": None,
            "crawled_text": crawler_result.text
        }

async def main():
    # Example URL - replace with your URL
    url = "https://example.com/job-listing"
    
    result = await verify_recruitment_url(url)
    
    print("\nResults:")
    print(f"Is Recruitment Page: {result['is_recruitment']}")
    if result['evidence']:
        print("Evidence:")
        for evidence in result['evidence']:
            print(f"- {evidence}")
    
    # Save the crawled text for further analysis
    with open("crawled_text.txt", "w", encoding="utf-8") as f:
        f.write(result['crawled_text'])

if __name__ == "__main__":
    asyncio.run(main()) 