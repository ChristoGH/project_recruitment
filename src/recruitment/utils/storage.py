"""Storage library for managing recruitment data in SQLite database.

This module provides functionality for storing and retrieving recruitment-related data
from a SQLite database, including URLs, articles, and their metadata.
"""

import logging
import os
import sqlite3
from datetime import datetime
from typing import Any, Optional

# Configure logging
logger = logging.getLogger(__name__)


class Storage:
    """A class for managing storage operations in the recruitment database."""

    def __init__(self, db_path: str):
        """Initialize the storage with the database path.

        Args:
            db_path: Path to the SQLite database file.
        """
        self.db_path = db_path
        self._ensure_db_exists()

    def _ensure_db_exists(self) -> None:
        """Ensure the database file exists and create it if necessary."""
        if not os.path.exists(self.db_path):
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
            self._create_tables()

    def _create_tables(self) -> None:
        """Create the necessary tables in the database if they don't exist."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            # Create URLs table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS urls (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    url TEXT UNIQUE NOT NULL,
                    domain TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    processed BOOLEAN DEFAULT FALSE
                )
            """)

            # Create articles table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS articles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    url_id INTEGER NOT NULL,
                    title TEXT,
                    content TEXT,
                    published_date TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (url_id) REFERENCES urls(id)
                )
            """)

            conn.commit()

    def save_url(self, url: str, domain: str) -> bool:
        """Save a URL to the database if it doesn't already exist.

        Args:
            url: The URL to save.
            domain: The domain of the URL.

        Returns:
            bool: True if the URL was saved, False if it already existed.
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT OR IGNORE INTO urls (url, domain) VALUES (?, ?)",
                    (url, domain),
                )
                conn.commit()
                return cursor.rowcount > 0
        except sqlite3.Error as e:
            logger.error(f"Error saving URL to database: {e!s}")
            return False

    def get_unprocessed_urls(self, limit: int = 100) -> list[dict[str, Any]]:
        """Get a list of unprocessed URLs from the database.

        Args:
            limit: Maximum number of URLs to retrieve.

        Returns:
            List of dictionaries containing URL information.
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT * FROM urls WHERE processed = FALSE ORDER BY created_at LIMIT ?",
                    (limit,),
                )
                return [dict(row) for row in cursor.fetchall()]
        except sqlite3.Error as e:
            logger.error(f"Error retrieving unprocessed URLs: {e!s}")
            return []

    def mark_url_as_processed(self, url_id: int) -> bool:
        """Mark a URL as processed in the database.

        Args:
            url_id: The ID of the URL to mark as processed.

        Returns:
            bool: True if the URL was marked as processed, False otherwise.
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("UPDATE urls SET processed = TRUE WHERE id = ?", (url_id,))
                conn.commit()
                return cursor.rowcount > 0
        except sqlite3.Error as e:
            logger.error(f"Error marking URL as processed: {e!s}")
            return False

    def save_article(
        self,
        url_id: int,
        title: str,
        content: str,
        published_date: Optional[datetime] = None,
    ) -> bool:
        """Save an article to the database.

        Args:
            url_id: The ID of the URL the article is from.
            title: The title of the article.
            content: The content of the article.
            published_date: The date the article was published.

        Returns:
            bool: True if the article was saved successfully, False otherwise.
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    INSERT INTO articles (url_id, title, content, published_date)
                    VALUES (?, ?, ?, ?)
                    """,
                    (url_id, title, content, published_date),
                )
                conn.commit()
                return True
        except sqlite3.Error as e:
            logger.error(f"Error saving article to database: {e!s}")
            return False
