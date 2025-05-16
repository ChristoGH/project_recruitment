"""Test configuration and fixtures."""

import os

import pytest

# Test configuration
TEST_DB_PATH = "test_recruitment.db"
TEST_BACKUP_DIR = "test_backups"
TEST_MIGRATIONS_DIR = "src/recruitment/db/migrations"
TEST_LOG_DIR = "test_logs"


@pytest.fixture(scope="session")
def test_env():
    """Set up test environment variables."""
    os.environ["RECRUITMENT_DB_URL"] = TEST_DB_PATH
    os.environ["RABBITMQ_HOST"] = "localhost"
    os.environ["RABBITMQ_PORT"] = "5672"
    os.environ["RABBITMQ_USER"] = "guest"
    os.environ["RABBITMQ_PASSWORD"] = "guest"
    os.environ["LOG_DIR"] = TEST_LOG_DIR
    yield
    # Cleanup
    if "RECRUITMENT_DB_URL" in os.environ:
        del os.environ["RECRUITMENT_DB_URL"]
    if "LOG_DIR" in os.environ:
        del os.environ["LOG_DIR"]


@pytest.fixture(scope="function")
def test_db_path():
    """Provide test database path and clean up after tests."""
    yield TEST_DB_PATH
    # Cleanup
    if os.path.exists(TEST_DB_PATH):
        os.remove(TEST_DB_PATH)
    if os.path.exists(f"{TEST_DB_PATH}-journal"):
        os.remove(f"{TEST_DB_PATH}-journal")


@pytest.fixture(scope="function")
def test_backup_dir():
    """Create and clean up test backup directory."""
    os.makedirs(TEST_BACKUP_DIR, exist_ok=True)
    yield TEST_BACKUP_DIR
    # Cleanup
    for file in os.listdir(TEST_BACKUP_DIR):
        os.remove(os.path.join(TEST_BACKUP_DIR, file))
    os.rmdir(TEST_BACKUP_DIR)


@pytest.fixture(scope="function")
def test_migrations_dir():
    """Ensure test migrations directory exists."""
    os.makedirs(TEST_MIGRATIONS_DIR, exist_ok=True)
    return TEST_MIGRATIONS_DIR


@pytest.fixture(scope="function")
def test_log_dir():
    """Create and clean up test log directory."""
    os.makedirs(TEST_LOG_DIR, exist_ok=True)
    yield TEST_LOG_DIR
    # Cleanup
    for file in os.listdir(TEST_LOG_DIR):
        os.remove(os.path.join(TEST_LOG_DIR, file))
    os.rmdir(TEST_LOG_DIR)
