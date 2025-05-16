from unittest.mock import AsyncMock

import pytest

from recruitment.models.url_models import URLDiscoveryConfig
from recruitment.services.discovery.main import URLDiscoveryService


@pytest.fixture
def discovery_config():
    return URLDiscoveryConfig(
        search_terms=["software engineer", "data scientist"],
        max_results=10,
        interval_minutes=60,
    )


@pytest.fixture
def mock_rabbitmq():
    mock = AsyncMock()
    mock.publish = AsyncMock()
    return mock


@pytest.fixture
def discovery_service(discovery_config, mock_rabbitmq):
    service = URLDiscoveryService(config=discovery_config, rabbitmq=mock_rabbitmq)
    # Mock the actual URL discovery method
    service._discover_urls = AsyncMock(
        return_value=["https://example.com/job1", "https://example.com/job2"]
    )
    return service


def test_discovery_service_initialization(discovery_service, discovery_config):
    assert discovery_service.config.search_terms == discovery_config.search_terms
    assert discovery_service.config.max_results == discovery_config.max_results
    assert discovery_service.config.interval_minutes == discovery_config.interval_minutes


@pytest.mark.asyncio
async def test_discover_urls(discovery_service):
    urls = await discovery_service.discover_urls()
    assert isinstance(urls, list)
    assert all(isinstance(url, str) for url in urls)
    assert len(urls) <= discovery_service.config.max_results
    # Verify the mock was called
    discovery_service._discover_urls.assert_called_once()


@pytest.mark.asyncio
async def test_publish_urls(discovery_service):
    test_urls = ["https://example.com/job1", "https://example.com/job2"]
    await discovery_service.publish_urls(test_urls)

    # Verify the mock was called correctly
    discovery_service.rabbitmq.publish.assert_called_once()
    # Verify the correct number of URLs were published
    assert len(discovery_service.rabbitmq.publish.call_args[0][0]) == len(test_urls)
