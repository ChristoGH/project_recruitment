"""Integration tests for database safety and migration integrity."""
import os
import pytest
import sqlite3
import shutil
from pathlib import Path
import asyncio
from datetime import datetime

from src.recruitment.db.repository import RecruitmentDatabase
from src.recruitment.services.discovery.main import URLDiscoverer
from src.recruitment.services.processing.main import URLProcessor

# Test configuration
TEST_DB_PATH = "test_recruitment.db"
BACKUP_DIR = "test_backups"
MIGRATIONS_DIR = "src/recruitment/db/migrations"

@pytest.fixture(scope="function")
def test_db():
    """Create a test database and clean it up after tests."""
    # Create test database
    db = RecruitmentDatabase(TEST_DB_PATH)
    yield db
    # Cleanup
    if os.path.exists(TEST_DB_PATH):
        os.remove(TEST_DB_PATH)
    if os.path.exists(f"{TEST_DB_PATH}-journal"):
        os.remove(f"{TEST_DB_PATH}-journal")

@pytest.fixture(scope="function")
def backup_dir():
    """Create and clean up backup directory."""
    os.makedirs(BACKUP_DIR, exist_ok=True)
    yield BACKUP_DIR
    shutil.rmtree(BACKUP_DIR)

@pytest.mark.asyncio
async def test_database_initialization(test_db):
    """Test that database initialization creates all required tables."""
    await test_db.init_db()
    
    # Verify schema_version table exists
    conn = sqlite3.connect(TEST_DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = {row[0] for row in cursor.fetchall()}
    
    required_tables = {
        'schema_version', 'urls', 'raw_content', 'jobs',
        'companies', 'locations', 'skills', 'job_skills',
        'qualifications', 'job_qualifications', 'attributes',
        'job_attributes', 'duties', 'job_duties', 'benefits',
        'job_benefits', 'agencies'
    }
    
    assert required_tables.issubset(tables)
    conn.close()

@pytest.mark.asyncio
async def test_migration_versioning(test_db):
    """Test that migrations are properly versioned and tracked."""
    await test_db.init_db()
    
    conn = sqlite3.connect(TEST_DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT version FROM schema_version ORDER BY version DESC LIMIT 1")
    version = cursor.fetchone()[0]
    
    assert version > 0
    conn.close()

@pytest.mark.asyncio
async def test_processing_service_readonly(test_db):
    """Test that processing service has read-only access to database."""
    await test_db.init_db()
    
    # Create a test URL
    conn = sqlite3.connect(TEST_DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO urls (url, domain, source) VALUES (?, ?, ?)",
        ("http://test.com", "test.com", "test")
    )
    conn.commit()
    conn.close()
    
    # Try to modify database through processing service
    processor = URLProcessor(TEST_DB_PATH)
    await processor.start()
    
    try:
        # Attempt to modify database (should fail)
        with pytest.raises(Exception):
            await processor.db.batch_insert_urls([("http://test2.com", "test2.com", "test")])
    finally:
        await processor.stop()

@pytest.mark.asyncio
async def test_discovery_service_migration(test_db):
    """Test that discovery service can apply migrations."""
    await test_db.init_db()
    
    discoverer = URLDiscoverer(TEST_DB_PATH)
    await discoverer.start()
    
    try:
        # Verify database is accessible
        assert await discoverer.db.check_connection()
        
        # Verify schema version is tracked
        conn = sqlite3.connect(TEST_DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT version FROM schema_version ORDER BY version DESC LIMIT 1")
        version = cursor.fetchone()[0]
        assert version > 0
        conn.close()
    finally:
        await discoverer.stop()

def test_database_backup(backup_dir, test_db):
    """Test database backup functionality."""
    # Create test data
    conn = sqlite3.connect(TEST_DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO urls (url, domain, source) VALUES (?, ?, ?)",
        ("http://test.com", "test.com", "test")
    )
    conn.commit()
    conn.close()
    
    # Create backup
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = os.path.join(backup_dir, f"recruitment.db.backup_{timestamp}")
    shutil.copy2(TEST_DB_PATH, backup_path)
    
    # Verify backup exists and contains data
    assert os.path.exists(backup_path)
    backup_conn = sqlite3.connect(backup_path)
    backup_cursor = backup_conn.cursor()
    backup_cursor.execute("SELECT url FROM urls")
    urls = backup_cursor.fetchall()
    assert len(urls) == 1
    assert urls[0][0] == "http://test.com"
    backup_conn.close()

@pytest.mark.asyncio
async def test_migration_idempotency(test_db):
    """Test that migrations can be run multiple times safely."""
    # Run initialization twice
    await test_db.init_db()
    await test_db.init_db()
    
    # Verify database is in a consistent state
    conn = sqlite3.connect(TEST_DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT version FROM schema_version ORDER BY version DESC LIMIT 1")
    version = cursor.fetchone()[0]
    assert version > 0
    conn.close()

def test_database_file_safety():
    """Test that database files are properly ignored by git."""
    # Check .gitignore
    with open(".gitignore", "r") as f:
        gitignore_content = f.read()
    
    assert "*.db" in gitignore_content
    assert "*.db-journal" in gitignore_content
    assert "src/recruitment/db/recruitment.db" in gitignore_content
    assert "databases/" in gitignore_content
    assert "data/" in gitignore_content

@pytest.mark.asyncio
async def test_service_dependencies(test_db):
    """Test that services start in the correct order."""
    discoverer = URLDiscoverer(TEST_DB_PATH)
    processor = URLProcessor(TEST_DB_PATH)
    
    # Start discovery service first
    await discoverer.start()
    assert await discoverer.db.check_connection()
    
    # Start processing service
    await processor.start()
    assert await processor.db.check_connection()
    
    # Cleanup
    await processor.stop()
    await discoverer.stop() 