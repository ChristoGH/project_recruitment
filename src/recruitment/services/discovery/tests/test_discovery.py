"""Tests for the URL Discovery Service."""

import pytest
from fastapi.testclient import TestClient

from recruitment.services.discovery.main import app


@pytest.fixture
def client():
    """Create a test client."""
    return TestClient(app)


def test_app_initialization():
    """Test that the app initializes correctly."""
    client = TestClient(app)
    assert client is not None


def test_health_check(client):
    """Test the health check endpoint."""
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert "rabbitmq" in data
