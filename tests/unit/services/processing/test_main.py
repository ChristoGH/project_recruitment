"""Test module for URL processing service."""
import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi import FastAPI
import sys
import os

# Add the parent directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))))

from src.recruitment.services.processing.main import (
    URLProcessingService,
    URLProcessingConfig,
    URLProcessingResult
)
from src.recruitment.models.db_models import JobPosting
from src.recruitment.db.repository import DatabaseError, RecruitmentDatabase

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
def app():
    return FastAPI()

@pytest.fixture
def processing_config():
    return URLProcessingConfig(
        max_concurrent_requests=2,
        request_timeout=30,
        retry_attempts=3
    )

@pytest.fixture
def mock_rabbitmq():
    mock = AsyncMock()
    mock.consume_urls = AsyncMock(return_value=[
        "https://example.com/job1",
        "https://example.com/job2"
    ])
    return mock

@pytest.fixture
def mock_db():
    mock = AsyncMock()
    mock.save_result = AsyncMock()
    return mock

@pytest.fixture
def processing_service(processing_config, mock_rabbitmq, mock_db):
    service = URLProcessingService(
        config=processing_config,
        rabbitmq=mock_rabbitmq,
        db=mock_db
    )
    service._process_url = AsyncMock(return_value=URLProcessingResult(
        url="https://example.com/job1",
        success=True,
        job_posting=JobPosting(
            title="Software Engineer",
            company="Example Corp",
            location="Remote",
            description="Test job description"
        )
    ))
    return service

def test_processing_service_initialization(processing_service, processing_config):
    assert processing_service.config.max_concurrent_requests == processing_config.max_concurrent_requests
    assert processing_service.config.request_timeout == processing_config.request_timeout
    assert processing_service.config.retry_attempts == processing_config.retry_attempts

@pytest.mark.asyncio
async def test_process_url(processing_service):
    test_url = "https://example.com/job1"
    result = await processing_service.process_url(test_url)
    
    assert isinstance(result, URLProcessingResult)
    assert result.url == test_url
    assert result.success is True
    assert isinstance(result.job_posting, JobPosting)
    processing_service._process_url.assert_called_once_with(test_url)

@pytest.mark.asyncio
async def test_consume_urls(processing_service):
    await processing_service.consume_urls()
    
    processing_service.rabbitmq.consume_urls.assert_called_once()
    assert processing_service.db.save_result.call_count == 2 