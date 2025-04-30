import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from src.recruitment.services.discovery.main import URLDiscoveryService
from src.recruitment.models.url_models import URLDiscoveryConfig

@pytest.fixture
def discovery_service():
    config = URLDiscoveryConfig(
        search_terms=["software engineer", "data scientist"],
        max_results=10,
        interval_minutes=60
    )
    return URLDiscoveryService(config)

def test_discovery_service_initialization(discovery_service):
    assert discovery_service.config.search_terms == ["software engineer", "data scientist"]
    assert discovery_service.config.max_results == 10
    assert discovery_service.config.interval_minutes == 60

@pytest.mark.asyncio
async def test_discover_urls(discovery_service):
    urls = await discovery_service.discover_urls()
    assert isinstance(urls, list)
    assert all(isinstance(url, str) for url in urls)
    assert len(urls) <= discovery_service.config.max_results 