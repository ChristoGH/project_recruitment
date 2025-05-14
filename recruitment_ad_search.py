#!/usr/bin/env python3
"""
Simple script to search for recruitment ads and save URLs to CSV.
Usage: python recruitment_ad_search.py --days_back 1
"""

import csv
import argparse
import logging
from datetime import datetime, timedelta
from googlesearch import search
import os

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def search_recruitment_ads(days_back: int) -> list[str]:
    """Search for recruitment ads in South Africa."""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_back)

    query = f'("recruitment advert" OR "job vacancy" OR "hiring now") AND "South Africa" after:{start_date.strftime("%Y-%m-%d")} before:{end_date.strftime("%Y-%m-%d")}'

    urls = []
    try:
        for url in search(query, num_results=100):
            urls.append(url)
            logger.info(f"Found: {url}")
    except Exception as e:
        logger.error(f"Search error: {e}")

    return urls


def save_to_csv(urls: list[str]) -> None:
    """Save URLs to a CSV file."""
    if not urls:
        logger.info("No URLs found")
        return

    # Create data directory if it doesn't exist
    data_dir = "data"
    os.makedirs(data_dir, exist_ok=True)

    # Use timestamp with seconds precision
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = os.path.join(data_dir, f"recruitment_ads_{timestamp}.csv")

    with open(filename, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["URL"])
        for url in urls:
            writer.writerow([url])
    logger.info(f"Saved {len(urls)} URLs to {filename}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days_back", type=int, default=1)
    args = parser.parse_args()

    urls = search_recruitment_ads(args.days_back)
    save_to_csv(urls)


if __name__ == "__main__":
    main()
