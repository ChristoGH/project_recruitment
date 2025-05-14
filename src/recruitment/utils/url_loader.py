"""Utility module for loading URLs from various sources."""

import csv
import json
import logging
from pathlib import Path
from typing import List, Optional, Callable

logger = logging.getLogger(__name__)


def load_urls_from_file(
    file_path: str,
    url_validator: Optional[Callable[[str], bool]] = None,
    max_results: Optional[int] = None,
) -> List[str]:
    """Load URLs from a file.

    Args:
        file_path: Path to the file containing URLs
        url_validator: Optional function to validate URLs
        max_results: Optional maximum number of URLs to return

    Returns:
        List of valid URLs found in the file

    Raises:
        FileNotFoundError: If the file does not exist
        ValueError: If the file format is not supported
    """
    file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    urls: List[str] = []

    try:
        if file_path.suffix.lower() == ".csv":
            with open(file_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                if "URL" not in reader.fieldnames:
                    raise ValueError(
                        f"CSV file {file_path} does not contain 'URL' column"
                    )

                for row in reader:
                    url = row["URL"]
                    if url_validator is None or url_validator(url):
                        urls.append(url)
                        if max_results and len(urls) >= max_results:
                            break

        elif file_path.suffix.lower() == ".json":
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict) and "url" in item:
                            url = item["url"]
                            if url_validator is None or url_validator(url):
                                urls.append(url)
                                if max_results and len(urls) >= max_results:
                                    break
                else:
                    raise ValueError(
                        f"JSON file {file_path} must contain a list of objects with 'url' field"
                    )

        else:
            # Try to read as plain text, one URL per line
            with open(file_path, "r", encoding="utf-8") as f:
                for line in f:
                    url = line.strip()
                    if url and (url_validator is None or url_validator(url)):
                        urls.append(url)
                        if max_results and len(urls) >= max_results:
                            break

        logger.info(f"Loaded {len(urls)} URLs from {file_path}")
        return urls

    except Exception as e:
        logger.error(f"Error loading URLs from {file_path}: {e}")
        raise


def load_urls_from_directory(
    directory: str,
    url_validator: Optional[Callable[[str], bool]] = None,
    max_results: Optional[int] = None,
    file_pattern: str = "*.{csv,json,txt}",
) -> List[str]:
    """Load URLs from all matching files in a directory.

    Args:
        directory: Path to the directory containing URL files
        url_validator: Optional function to validate URLs
        max_results: Optional maximum number of URLs to return
        file_pattern: Glob pattern for matching files

    Returns:
        List of valid URLs found in the directory

    Raises:
        FileNotFoundError: If the directory does not exist
    """
    directory = Path(directory)
    if not directory.exists():
        raise FileNotFoundError(f"Directory not found: {directory}")

    urls: List[str] = []

    try:
        for file_path in directory.glob(file_pattern):
            try:
                file_urls = load_urls_from_file(
                    str(file_path),
                    url_validator=url_validator,
                    max_results=max_results - len(urls) if max_results else None,
                )
                urls.extend(file_urls)

                if max_results and len(urls) >= max_results:
                    urls = urls[:max_results]
                    break

            except Exception as e:
                logger.error(f"Error processing {file_path}: {e}")
                continue

        logger.info(f"Loaded {len(urls)} URLs from {directory}")
        return urls

    except Exception as e:
        logger.error(f"Error loading URLs from directory {directory}: {e}")
        raise
