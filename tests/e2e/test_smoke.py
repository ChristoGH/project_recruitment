import pytest
import requests
import time
from typing import Generator

@pytest.fixture
def discovery_service_url() -> str:
    return "http://localhost:8000"

@pytest.fixture
def processing_service_url() -> str:
    return "http://localhost:8001"

def wait_for_service(url: str, timeout: int = 300) -> bool:
    """Wait for a service to become available."""
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            response = requests.get(f"{url}/healthz")
            if response.status_code == 200:
                return True
        except requests.exceptions.RequestException:
            pass
        time.sleep(5)
    return False

def test_discovery_service_health(discovery_service_url: str):
    """Test that the discovery service is healthy."""
    assert wait_for_service(discovery_service_url), "Discovery service did not become healthy"

def test_processing_service_health(processing_service_url: str):
    """Test that the processing service is healthy."""
    assert wait_for_service(processing_service_url), "Processing service did not become healthy"

def test_discovery_service_ready(discovery_service_url: str):
    """Test that the discovery service is ready."""
    response = requests.get(f"{discovery_service_url}/readyz")
    assert response.status_code == 200
    assert response.json()["status"] == "ready"

def test_processing_service_ready(processing_service_url: str):
    """Test that the processing service is ready."""
    response = requests.get(f"{processing_service_url}/readyz")
    assert response.status_code == 200
    assert response.json()["status"] == "ready" 