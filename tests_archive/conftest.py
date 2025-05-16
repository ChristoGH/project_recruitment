import os
import sys
from pathlib import Path

import pytest

# Add the project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Set up test environment variables
os.environ["LOG_LEVEL"] = "DEBUG"
os.environ["LOG_DIR"] = str(project_root / "logs")
os.environ["DB_PATH"] = str(project_root / "databases" / "test.db")
os.environ["RABBITMQ_HOST"] = "localhost"
os.environ["RABBITMQ_PORT"] = "5672"
os.environ["RABBITMQ_USER"] = "guest"
os.environ["RABBITMQ_PASSWORD"] = "guest"


@pytest.fixture(scope="session")
def test_db_path():
    return os.environ["DB_PATH"]


@pytest.fixture(scope="session")
def rabbitmq_config():
    return {
        "host": os.environ["RABBITMQ_HOST"],
        "port": int(os.environ["RABBITMQ_PORT"]),
        "user": os.environ["RABBITMQ_USER"],
        "password": os.environ["RABBITMQ_PASSWORD"],
    }
