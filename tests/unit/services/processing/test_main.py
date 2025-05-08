"""Test module for URL processing service."""
import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi import FastAPI
import sys
import os

# Add the parent directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))))

from src.recruitment.services.processing.main import app
from src.recruitment.models.url_models import URLProcessingResult
from src.recruitment.utils.web_crawler import WebCrawlerResult
from src.recruitment.db.repository import RecruitmentDatabase

@pytest.fixture
def mock_db_connection():
    """Create a mock database connection."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    return mock_conn

@pytest.fixture
def db():
    """Create a RecruitmentDatabase instance with a mocked connection."""
    with patch('sqlite3.connect') as mock_connect:
        mock_connect.return_value = MagicMock()
        db = RecruitmentDatabase()
        yield db

@pytest.fixture
def mock_rabbitmq():
    mock = AsyncMock()
    mock.consume_urls = AsyncMock(return_value=[
        "https://example.com/job1",
        "https://example.com/job2"
    ])
    return mock

@pytest.mark.asyncio
async def test_process_url():
    """Test processing a single URL."""
    with patch('src.recruitment.utils.web_crawler.crawl_website_sync_v2') as mock_crawl:
        mock_crawl.return_value = WebCrawlerResult(
            success=True,
            markdown="Test content",
            transformed={"title": "Test Job"}
        )
        
        with patch('src.recruitment.db.repository.RecruitmentDatabase.save_processed_url') as mock_save:
            mock_save.return_value = None
            
            from src.recruitment.services.processing.main import process_url
            await process_url("https://example.com/job1", "test_search")
            
            mock_crawl.assert_called_once_with("https://example.com/job1")
            mock_save.assert_called_once()

@pytest.mark.asyncio
async def test_consume_urls():
    """Test consuming URLs from the queue."""
    with patch('src.recruitment.utils.rabbitmq.get_rabbitmq_connection') as mock_conn:
        mock_channel = AsyncMock()
        mock_conn.return_value.channel.return_value = mock_channel
        
        mock_queue = AsyncMock()
        mock_channel.declare_queue.return_value = mock_queue
        
        mock_message = AsyncMock()
        mock_message.body.decode.return_value = '{"url": "https://example.com/job1", "search_id": "test_search"}'
        mock_queue.iterator.return_value.__aiter__.return_value = [mock_message]
        
        with patch('src.recruitment.services.processing.main.process_url') as mock_process:
            mock_process.return_value = None
            
            from src.recruitment.services.processing.main import consume_urls
            await consume_urls()
            
            mock_process.assert_called_once_with("https://example.com/job1", "test_search") 