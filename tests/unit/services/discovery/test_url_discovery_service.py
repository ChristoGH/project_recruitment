"""Unit tests for URL Discovery Service."""

import pytest
import aio_pika


@pytest.mark.asyncio
async def test_publish_method_exists():
    """Test that the exchange has the correct publish method."""
    # Create a mock channel with a mock exchange
    mock_exchange = aio_pika.Exchange(None, "test_exchange", "direct")

    # Verify the correct method exists
    assert hasattr(mock_exchange, "publish")
    assert not hasattr(mock_exchange, "publish_batch")
