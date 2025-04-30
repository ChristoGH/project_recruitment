import pytest
import asyncio
from recruitment.services.discovery.main import URLDiscoveryService
from recruitment.services.processing.main import URLProcessingService
from recruitment.models.url_models import URLDiscoveryConfig, URLProcessingConfig
from recruitment.rabbitmq_utils import RabbitMQConnection

@pytest.fixture
async def rabbitmq_connection():
    connection = RabbitMQConnection()
    await connection.connect()
    yield connection
    await connection.close()

@pytest.fixture
def discovery_service():
    config = URLDiscoveryConfig(
        search_terms=["software engineer"],
        max_results=5,
        interval_minutes=60
    )
    return URLDiscoveryService(config)

@pytest.fixture
def processing_service():
    config = URLProcessingConfig(
        max_concurrent_requests=2,
        request_timeout=30,
        retry_attempts=3
    )
    return URLProcessingService(config)

@pytest.mark.asyncio
async def test_discovery_to_processing_pipeline(
    rabbitmq_connection,
    discovery_service,
    processing_service
):
    # Discover URLs
    urls = await discovery_service.discover_urls()
    assert len(urls) > 0

    # Publish URLs to queue
    await discovery_service.publish_urls(urls)

    # Process URLs from queue
    processed_count = 0
    async for url in rabbitmq_connection.consume_urls():
        result = await processing_service.process_url(url)
        assert result is not None
        processed_count += 1
        if processed_count >= len(urls):
            break

    assert processed_count == len(urls) 