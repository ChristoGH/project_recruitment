#!/usr/bin/env python3
"""
URL Processing Service

This FastAPI service consumes URLs from a RabbitMQ queue and stores their content in the database.
"""

import os
import json
import logging
import asyncio
from datetime import datetime
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from contextlib import asynccontextmanager
import aio_pika
import sqlite3
from pathlib import Path

from ...logging_config import setup_logging
from ...utils.rabbitmq import get_rabbitmq_connection, RABBIT_QUEUE
from ...utils.web_crawler_wrapper import crawl_website

# Load environment variables
load_dotenv()

# Create module-specific logger
logger = setup_logging(__name__)

# Get the absolute path to the project root
PROJECT_ROOT = Path(__file__).parent.parent.parent
DB_PATH = os.getenv("RECRUITMENT_PATH", "/app/src/recruitment/db/recruitment.db")

# Ensure database directory exists
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
logger.info(f"Database path: {DB_PATH}")
logger.info(f"Database directory exists: {os.path.exists(os.path.dirname(DB_PATH))}")
logger.info(f"Database file exists: {os.path.exists(DB_PATH)}")

def init_db():
    """Initialize the database with required tables."""
    logger.info(f"Initializing database at: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Drop existing tables to start fresh
    cursor.execute("DROP TABLE IF EXISTS raw_content")
    cursor.execute("DROP TABLE IF EXISTS urls")
    
    # Create urls table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS urls (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT UNIQUE NOT NULL,
            source TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Create raw_content table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS raw_content (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url_id INTEGER NOT NULL,
            content TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (url_id) REFERENCES urls(id)
        )
    """)
    
    conn.commit()
    conn.close()
    logger.info("Database initialized with fresh schema")

def verify_db_state():
    """Verify the database state and log the results."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Check urls table
        cursor.execute("SELECT COUNT(*) FROM urls")
        url_count = cursor.fetchone()[0]
        logger.info(f"Number of URLs in database: {url_count}")
        
        # Check raw_content table
        cursor.execute("SELECT COUNT(*) FROM raw_content")
        content_count = cursor.fetchone()[0]
        logger.info(f"Number of content entries in database: {content_count}")
        
        # Check for any orphaned content
        cursor.execute("""
            SELECT COUNT(*) FROM raw_content rc
            LEFT JOIN urls u ON rc.url_id = u.id
            WHERE u.id IS NULL
        """)
        orphaned_count = cursor.fetchone()[0]
        if orphaned_count > 0:
            logger.warning(f"Found {orphaned_count} orphaned content entries")
        
        conn.close()
        logger.info("Database state verification completed")
    except Exception as e:
        logger.error(f"Error verifying database state: {str(e)}")

async def store_url_content(url: str, search_id: str):
    """Store URL and its content in the database."""
    try:
        logger.info(f"Starting to process URL: {url}")
        
        # Run the web crawler
        logger.info(f"Starting web crawl for URL: {url}")
        result = await crawl_website(url)
        logger.info(f"Web crawl completed for URL: {url}, success: {result.success}")
        
        # Store in database
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        try:
            # First, store the URL in the urls table
            logger.info(f"Inserting URL into urls table: {url}")
            cursor.execute("""
                INSERT OR IGNORE INTO urls (url, source, created_at)
                VALUES (?, ?, ?)
            """, (
                url,
                search_id,  # Using search_id as source
                datetime.now().isoformat()
            ))
            conn.commit()
            logger.info(f"URL inserted into urls table: {url}")
            
            # Get the url_id
            cursor.execute("SELECT id FROM urls WHERE url = ?", (url,))
            url_id = cursor.fetchone()
            if not url_id:
                raise Exception(f"Failed to get url_id for URL: {url}")
            url_id = url_id[0]
            logger.info(f"Retrieved url_id: {url_id} for URL: {url}")
            
            # Store the content
            logger.info(f"Inserting content into raw_content table for url_id: {url_id}")
            cursor.execute("""
                INSERT INTO raw_content (url_id, content, created_at)
                VALUES (?, ?, ?)
            """, (
                url_id,
                result.markdown if result.success else "Failed to crawl",
                datetime.now().isoformat()
            ))
            conn.commit()
            logger.info(f"Content stored successfully for url_id: {url_id}")
            
        except sqlite3.Error as db_error:
            logger.error(f"Database error while storing URL {url}: {str(db_error)}")
            raise
        finally:
            conn.close()
            logger.info(f"Database connection closed for URL: {url}")
        
        logger.info(f"Successfully completed processing for URL: {url}")
        
    except Exception as e:
        logger.error(f"Error storing URL {url}: {str(e)}")
        # Store error state
        try:
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()
            
            logger.info(f"Storing error state for URL: {url}")
            
            # First, store the URL in the urls table
            cursor.execute("""
                INSERT OR IGNORE INTO urls (url, source, created_at)
                VALUES (?, ?, ?)
            """, (
                url,
                search_id,  # Using search_id as source
                datetime.now().isoformat()
            ))
            conn.commit()
            
            # Get the url_id
            cursor.execute("SELECT id FROM urls WHERE url = ?", (url,))
            url_id = cursor.fetchone()
            if not url_id:
                raise Exception(f"Failed to get url_id for URL: {url}")
            url_id = url_id[0]
            
            # Store the error state
            cursor.execute("""
                INSERT INTO raw_content (url_id, content, created_at)
                VALUES (?, ?, ?)
            """, (
                url_id,
                f"Error: {str(e)}",
                datetime.now().isoformat()
            ))
            conn.commit()
            logger.info(f"Error state stored successfully for url_id: {url_id}")
            
        except Exception as db_error:
            logger.error(f"Failed to store error state for URL {url}: {str(db_error)}")
        finally:
            if 'conn' in locals():
                conn.close()
                logger.info(f"Database connection closed for error state of URL: {url}")

async def consume_urls():
    """Consume URLs from RabbitMQ queue and store their content."""
    while True:
        try:
            # Get a new connection
            connection = await aio_pika.connect_robust(
                host=os.getenv("RABBITMQ_HOST", "rabbitmq"),
                port=int(os.getenv("RABBITMQ_PORT", 5672)),
                login=os.getenv("RABBITMQ_USER", "guest"),
                password=os.getenv("RABBITMQ_PASSWORD", "guest"),
                heartbeat=60,
            )
            
            async with connection:
                # Create a channel
                channel = await connection.channel()
                
                # Declare the queue
                queue = await channel.declare_queue(RABBIT_QUEUE, durable=True)
                
                logger.info("Starting to consume URLs from queue")
                
                # Start consuming
                async with queue.iterator() as queue_iter:
                    async for message in queue_iter:
                        try:
                            async with message.process():
                                data = json.loads(message.body.decode())
                                logger.info(f"Received message for URL: {data['url']}")
                                await store_url_content(data["url"], data["search_id"])
                                logger.info(f"Successfully processed URL: {data['url']}")
                        except json.JSONDecodeError as e:
                            logger.error(f"Failed to decode message: {str(e)}")
                            continue
                        except KeyError as e:
                            logger.error(f"Missing required field in message: {str(e)}")
                            continue
                        except Exception as e:
                            logger.error(f"Error processing message: {str(e)}")
                            continue
                                
        except aio_pika.exceptions.ConnectionClosed:
            logger.warning("RabbitMQ connection closed, attempting to reconnect...")
            await asyncio.sleep(5)  # Wait before reconnecting
            continue
            
        except Exception as e:
            logger.error(f"Unexpected error in consumer: {str(e)}")
            await asyncio.sleep(5)  # Wait before retrying
            continue

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for FastAPI application."""
    init_db()
    verify_db_state()
    
    connection = await get_rabbitmq_connection()
    channel = await connection.channel()
    await channel.declare_queue(RABBIT_QUEUE, durable=True)
    
    # Start consumer task
    consumer_task = asyncio.create_task(consume_urls())
    logger.info("URL consumer started")
    
    yield
    
    # Cleanup
    consumer_task.cancel()
    try:
        await consumer_task
    except asyncio.CancelledError:
        pass
    if not connection.is_closed:
        await connection.close()
    logger.info("Service shutdown complete")

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001) 