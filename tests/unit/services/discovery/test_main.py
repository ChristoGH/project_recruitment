"""Test module for URL discovery service."""

import json
from unittest.mock import AsyncMock, Mock, patch

import pytest
from fastapi import FastAPI

from recruitment.services.discovery.url_discovery_service import (
    check,
    gsearch_async,
    health_check,
    perform_search,
    publish_urls,
    search_results,
    start_search,
)


@pytest.fixture
def app():
    return FastAPI()


@pytest.fixture
def search_config():
    return {"id": "test_search", "term": "recruitment jobs South Africa"}


@pytest.fixture
def mock_rabbitmq():
    mock_channel = AsyncMock()
    mock_channel.default_exchange.publish = AsyncMock()
    return mock_channel


@pytest.mark.asyncio
async def test_gsearch_async():
    """Test the Google search function."""
    test_urls = ["https://example1.com", "https://example2.com"]
    with patch("recruitment.services.discovery.url_discovery_service.search") as mock_search:
        mock_search.return_value = test_urls
        urls = await gsearch_async("test term", 10)
        assert urls == test_urls
        mock_search.assert_called_once()


@pytest.mark.asyncio
async def test_publish_urls(mock_rabbitmq):
    """Test publishing URLs to RabbitMQ."""
    urls = ["https://example1.com", "https://example2.com"]
    search_id = "test_search"
    term = "test term"

    with patch("recruitment.services.discovery.url_discovery_service.app") as mock_app:
        mock_app.state.amqp_channel = mock_rabbitmq
        await publish_urls(search_id, term, urls)

        assert mock_rabbitmq.default_exchange.publish.call_count == len(urls)
        for call in mock_rabbitmq.default_exchange.publish.call_args_list:
            args, kwargs = call
            message = json.loads(args[0].body.decode())
            assert message["search_id"] == search_id
            assert message["term"] == term
            assert message["url"] in urls
            assert "ts" in message


@pytest.mark.asyncio
async def test_perform_search(search_config, mock_rabbitmq):
    """Test the perform_search function."""
    search_id = search_config["id"]
    term = search_config["term"]
    test_urls = ["https://example1.com", "https://example2.com"]

    with (
        patch("recruitment.services.discovery.url_discovery_service.gsearch_async") as mock_search,
        patch("recruitment.services.discovery.url_discovery_service.publish_urls") as mock_publish,
        patch("recruitment.services.discovery.url_discovery_service.app") as mock_app,
    ):
        mock_search.return_value = test_urls
        mock_app.state.amqp_channel = mock_rabbitmq

        await perform_search(search_id, term)

        mock_search.assert_called_once_with(term, 5)  # BATCH_SIZE is 5
        mock_publish.assert_called_once_with(search_id, term, test_urls)
        assert search_results[search_id]["status"] == "done"
        assert search_results[search_id]["url_count"] == len(test_urls)


@pytest.mark.asyncio
async def test_start_search(search_config):
    """Test the start_search endpoint."""
    with patch(
        "recruitment.services.discovery.url_discovery_service.perform_search"
    ) as mock_search:
        response = await start_search(search_config, Mock())
        assert response["id"] == search_config["id"]
        assert response["status"] == "queued"
        mock_search.assert_called_once()


@pytest.mark.asyncio
async def test_check():
    """Test the check endpoint."""
    search_id = "test_search"
    search_results[search_id] = {
        "status": "done",
        "url_count": 2,
        "started": "2024-01-01T00:00:00",
        "finished": "2024-01-01T00:00:01",
    }

    response = await check(search_id)
    assert response["status"] == "done"
    assert response["url_count"] == 2


@pytest.mark.asyncio
async def test_health_check():
    """Test the health check endpoint."""
    with patch("recruitment.services.discovery.url_discovery_service.app") as mock_app:
        # Test healthy state
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_app.state.amqp_channel = mock_channel

        response = await health_check()
        assert response["status"] == "healthy"

        # Test unhealthy state
        mock_channel.is_closed = True
        response = await health_check()
        assert response["status"] == "unhealthy"
