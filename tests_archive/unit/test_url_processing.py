import pytest
from unittest.mock import AsyncMock
from recruitment.services.processing.main import URLProcessingService
from recruitment.models.url_models import URLProcessingConfig, URLProcessingResult
from recruitment.models.db_models import JobPosting


@pytest.fixture
def processing_config():
    return URLProcessingConfig(
        max_concurrent_requests=2, request_timeout=30, retry_attempts=3
    )


@pytest.fixture
def mock_rabbitmq():
    mock = AsyncMock()
    # Set up the mock to return a list of test URLs when consumed
    mock.consume_urls = AsyncMock(
        return_value=["https://example.com/job1", "https://example.com/job2"]
    )
    return mock


@pytest.fixture
def mock_db():
    mock = AsyncMock()
    mock.save_result = AsyncMock()
    return mock


@pytest.fixture
def processing_service(processing_config, mock_rabbitmq, mock_db):
    service = URLProcessingService(
        config=processing_config, rabbitmq=mock_rabbitmq, db=mock_db
    )
    # Mock the actual URL processing method
    service._process_url = AsyncMock(
        return_value=URLProcessingResult(
            url="https://example.com/job1",
            success=True,
            job_posting=JobPosting(
                title="Software Engineer",
                company="Example Corp",
                location="Remote",
                description="Test job description",
            ),
        )
    )
    return service


def test_processing_service_initialization(processing_service, processing_config):
    assert (
        processing_service.config.max_concurrent_requests
        == processing_config.max_concurrent_requests
    )
    assert (
        processing_service.config.request_timeout == processing_config.request_timeout
    )
    assert processing_service.config.retry_attempts == processing_config.retry_attempts


@pytest.mark.asyncio
async def test_process_url(processing_service):
    test_url = "https://example.com/job1"
    result = await processing_service.process_url(test_url)

    assert isinstance(result, URLProcessingResult)
    assert result.url == test_url
    assert result.success is True
    assert isinstance(result.job_posting, JobPosting)
    # Verify the mock was called
    processing_service._process_url.assert_called_once_with(test_url)


@pytest.mark.asyncio
async def test_consume_urls(processing_service):
    await processing_service.consume_urls()

    # Verify the mock was called
    processing_service.rabbitmq.consume_urls.assert_called_once()
    # Verify the correct number of results were saved
    assert processing_service.db.save_result.call_count == 2
