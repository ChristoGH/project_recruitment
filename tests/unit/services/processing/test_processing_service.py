"""Test module for URL processing service."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from recruitment.services.processing.main import app, consume_urls


@pytest.fixture
def mock_rabbitmq():
    """Create a mock RabbitMQ connection."""
    mock_channel = AsyncMock()
    mock_queue = AsyncMock()
    mock_channel.declare_queue.return_value = mock_queue
    return mock_channel, mock_queue


@pytest.fixture
def mock_db():
    """Create a mock database."""
    mock = MagicMock()
    mock.insert_url.return_value = 1
    mock.update_url_processing_status = MagicMock()
    return mock


@pytest.mark.asyncio
async def test_consume_urls(mock_rabbitmq, mock_db):
    """Test consuming URLs from the queue."""
    mock_channel, mock_queue = mock_rabbitmq

    # Mock the message
    mock_message = AsyncMock()
    mock_message.body.decode.return_value = json.dumps(
        {
            "url": "https://example.com/job1",
            "search_id": "test_search",
            "term": "test term",
            "ts": "2024-01-01T00:00:00",
        }
    )
    mock_queue.iterator.return_value.__aiter__.return_value = [mock_message]

    # Mock the database
    with patch("recruitment.services.processing.main.RecruitmentDatabase") as mock_db_class:
        mock_db_class.return_value = mock_db

        # Mock the web crawler
        with patch("recruitment.services.processing.main.crawl_website_sync_v2") as mock_crawl:
            mock_crawl.return_value = MagicMock(
                success=True, markdown="Test content", transformed={"title": "Test Job"}
            )

            # Call the function
            await consume_urls()

            # Verify the results
            mock_db.insert_url.assert_called_once()
            mock_db.update_url_processing_status.assert_called_once_with(1, "completed")


@pytest.mark.asyncio
async def test_consume_urls_error(mock_rabbitmq, mock_db):
    """Test error handling when consuming URLs."""
    mock_channel, mock_queue = mock_rabbitmq

    # Mock the message
    mock_message = AsyncMock()
    mock_message.body.decode.return_value = json.dumps(
        {
            "url": "https://example.com/job1",
            "search_id": "test_search",
            "term": "test term",
            "ts": "2024-01-01T00:00:00",
        }
    )
    mock_queue.iterator.return_value.__aiter__.return_value = [mock_message]

    # Mock the database
    with patch("recruitment.services.processing.main.RecruitmentDatabase") as mock_db_class:
        mock_db_class.return_value = mock_db

        # Mock the web crawler to raise an error
        with patch("recruitment.services.processing.main.crawl_website_sync_v2") as mock_crawl:
            mock_crawl.side_effect = Exception("Test error")

            # Call the function
            await consume_urls()

            # Verify the results
            mock_db.insert_url.assert_called_once()
            mock_db.update_url_processing_status.assert_called_once_with(
                1, "error", error_message="Test error"
            )


@pytest.mark.asyncio
async def test_health_check():
    """Test the health check endpoint."""
    with patch("recruitment.services.processing.main.app") as mock_app:
        # Test healthy state
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_app.state.amqp_channel = mock_channel

        response = await app.get("/health")
        assert response["status"] == "healthy"

        # Test unhealthy state
        mock_channel.is_closed = True
        response = await app.get("/health")
        assert response["status"] == "unhealthy"
