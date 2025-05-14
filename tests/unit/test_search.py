import asyncio
import requests
from bs4 import BeautifulSoup
import logging
from typing import List

# Setup basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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
                        "Cache-Control": "max-age=0",
                    },
                )

                if response.status_code != 200:
                    logger.warning(
                        f"Non-200 response from Google: {response.status_code}"
                    )
                    continue

                soup = BeautifulSoup(response.text, "html.parser")
                found_urls = []

                for link in soup.find_all("a"):
                    href = link.get("href")
                    if href and href.startswith("/url?q="):
                        url = href.split("&")[0][7:]
                        if url not in urls:
                            urls.append(url)
                            found_urls.append(url)
                            logger.debug(f"Found valid URL: {url}")

                logger.info(
                    f'Found {len(found_urls)} new URLs for term: "{search_query}"'
                )

                # Add a delay between requests to avoid rate limiting
                await asyncio.sleep(5)

            except Exception as e:
                logger.error(f'Error searching for term "{search_query}": {str(e)}')

    logger.info(f"Total unique URLs found: {len(urls)}")
    return urls


async def test_search():
    # Create search configuration
    search_config = {
        "keywords": [
            "recruitment advert",
            "job vacancy",
            "hiring now",
            "employment opportunity",
            "career opportunity",
            "job advertisement",
            "recruitment drive",
        ],
        "locations": ["South Africa"],
        "max_results": 10,
    }

    try:
        # Get URLs directly
        urls = await get_recruitment_urls(
            search_config["keywords"], search_config["locations"]
        )

        print(f"Found {len(urls)} URLs:")
        for url in urls:
            print(f"- {url}")

    except Exception as e:
        print("Error:", str(e))


if __name__ == "__main__":
    asyncio.run(test_search())
