from pydantic_settings import BaseSettings
from pydantic import Field
from pathlib import Path

# Get the absolute path to the project root
PROJECT_ROOT = Path(__file__).parent.parent
DEFAULT_DB_PATH = str(PROJECT_ROOT / "databases" / "recruitment.db")

class Settings(BaseSettings):
    """
    Centralized runtime configuration for the recruitment service.
    All defaults are sensible for dev-mode; ops override via ENV.
    """
    # --- Database ---
    recruitment_db_url: str = Field(default=DEFAULT_DB_PATH, env="RECRUITMENT_DB_URL")
    
    # --- RabbitMQ ---
    rabbitmq_host: str = Field(default="localhost", env="RABBITMQ_HOST")
    rabbitmq_port: int = Field(default=5672, env="RABBITMQ_PORT")
    rabbitmq_user: str = Field(default="guest", env="RABBITMQ_USER")
    rabbitmq_password: str = Field(default="guest", env="RABBITMQ_PASSWORD")
    rabbitmq_vhost: str = Field(default="/", env="RABBITMQ_VHOST")
    
    # --- Search Configuration ---
    search_days_back: int = Field(default=7, env="SEARCH_DAYS_BACK")
    search_interval_seconds: int = Field(default=1800, env="SEARCH_INTERVAL_SECONDS")  # 30 minutes
    
    # --- Batch Sizes ---
    google_search_batch_size: int = Field(default=50, env="GOOGLE_SEARCH_BATCH_SIZE")
    publish_batch_size: int = Field(default=50, env="PUBLISH_BATCH_SIZE")
    max_concurrent_requests: int = Field(default=5, env="MAX_CONCURRENT_REQUESTS")
    request_timeout: int = Field(default=30, env="REQUEST_TIMEOUT")
    retry_attempts: int = Field(default=3, env="RETRY_ATTEMPTS")

    class Config:
        env_file = ".env"
        case_sensitive = True

# Create a singleton instance
settings = Settings() 