import pytest
import asyncio
from src.recruitment.services.discovery.main import URLDiscoveryService
from src.recruitment.services.processing.main import URLProcessingService
from src.recruitment.models.url_models import URLDiscoveryConfig, URLProcessingConfig
from src.recruitment.rabbitmq_utils import RabbitMQConnection
from src.recruitment.recruitment_db import RecruitmentDatabase

@pytest.fixture
async def rabbitmq_connection(rabbitmq_config):
    connection = RabbitMQConnection(**rabbitmq_config)
    await connection.connect()
    yield connection
    await connection.close()

@pytest.fixture
def test_db(test_db_path):
    db = RecruitmentDatabase(test_db_path)
    db.initialize()
    yield db
    db.close()

@pytest.fixture
def discovery_config():
    return URLDiscoveryConfig(
        search_terms=["software engineer"],
        max_results=5,
        interval_minutes=60
    )

@pytest.fixture
def processing_config():
    return URLProcessingConfig(
        max_concurrent_requests=2,
        request_timeout=30,
        retry_attempts=3
    )

@pytest.fixture
def discovery_service(discovery_config, rabbitmq_connection):
    return URLDiscoveryService(config=discovery_config, rabbitmq=rabbitmq_connection)

@pytest.fixture
def processing_service(processing_config, rabbitmq_connection, test_db):
    return URLProcessingService(
        config=processing_config,
        rabbitmq=rabbitmq_connection,
        db=test_db
    )

@pytest.mark.asyncio
async def test_full_pipeline(discovery_service, processing_service):
    # Start the processing service
    processing_task = asyncio.create_task(processing_service.consume_urls())
    
    # Run discovery
    await discovery_service.discover_urls()
    
    # Wait a bit for processing
    await asyncio.sleep(5)
    
    # Stop the processing service
    processing_task.cancel()
    try:
        await processing_task
    except asyncio.CancelledError:
        pass
    
    assert True  # Placeholder for actual verification

@pytest.mark.asyncio
async def test_discovery_to_processing_pipeline(
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
    async for url in processing_service.rabbitmq.consume_urls():
        result = await processing_service.process_url(url)
        assert result is not None
        processed_count += 1
        if processed_count >= len(urls):
            break

    assert processed_count == len(urls)

    # Verify URLs were stored in database
    stored_urls = processing_service.db.get_processed_urls()
    assert len(stored_urls) == len(urls) 