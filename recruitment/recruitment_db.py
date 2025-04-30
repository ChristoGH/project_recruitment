import aiosqlite
import os
from pathlib import Path
from typing import Optional
import json
from datetime import datetime

# Database configuration
DB_DIR = Path("/app/databases")
DB_FILE = DB_DIR / "recruitment.db"

async def init_db():
    """Initialize the database and create necessary tables."""
    try:
        # Create database directory if it doesn't exist
        DB_DIR.mkdir(exist_ok=True)
        
        # Connect to database
        async with aiosqlite.connect(DB_FILE) as db:
            # Create tables
            await db.execute("""
                CREATE TABLE IF NOT EXISTS raw_content (
                    url TEXT PRIMARY KEY,
                    content TEXT NOT NULL,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            await db.execute("""
                CREATE TABLE IF NOT EXISTS processed_content (
                    url TEXT PRIMARY KEY,
                    skills TEXT,
                    confidence REAL,
                    source TEXT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (url) REFERENCES raw_content(url)
                )
            """)
            
            await db.commit()
    except Exception as e:
        raise Exception(f"Failed to initialize database: {str(e)}")

async def save_raw_content(url: str, content: str) -> None:
    """
    Save raw content to the database.
    
    Args:
        url: URL of the content
        content: Raw content to save
    """
    try:
        async with aiosqlite.connect(DB_FILE) as db:
            await db.execute(
                "INSERT OR REPLACE INTO raw_content (url, content) VALUES (?, ?)",
                (url, content)
            )
            await db.commit()
    except Exception as e:
        raise Exception(f"Failed to save raw content: {str(e)}")

async def get_raw_content(url: str) -> Optional[str]:
    """
    Get raw content from the database.
    
    Args:
        url: URL to get content for
        
    Returns:
        Raw content if found, None otherwise
    """
    try:
        async with aiosqlite.connect(DB_FILE) as db:
            async with db.execute(
                "SELECT content FROM raw_content WHERE url = ?",
                (url,)
            ) as cursor:
                row = await cursor.fetchone()
                return row[0] if row else None
    except Exception as e:
        raise Exception(f"Failed to get raw content: {str(e)}") 