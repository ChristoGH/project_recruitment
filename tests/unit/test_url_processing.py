import pytest
from unittest.mock import AsyncMock, MagicMock
from src.recruitment.services.processing.main import URLProcessingService
from src.recruitment.models.url_models import URLProcessingConfig, URLProcessingResult
from src.recruitment.models.db_models import JobPosting

@pytest.fixture
def processing_service():
    config = URLProcessingConfig(
        max_concurrent_requests=5,
        request_timeout=30,
        retry_attempts=3
    )
    return URLProcessingService(config)

def test_processing_service_initialization(processing_service):
    assert processing_service.config.max_concurrent_requests == 5
    assert processing_service.config.request_timeout == 30
    assert processing_service.config.retry_attempts == 3

@pytest.mark.asyncio
async def test_process_url(processing_service):
    test_url = "https://example.com/job"
    result = await processing_service.process_url(test_url)
    assert result is not None
    assert hasattr(result, "url")
    assert hasattr(result, "title")
    assert hasattr(result, "description")
    assert hasattr(result, "skills") 