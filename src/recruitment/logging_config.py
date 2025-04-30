# logging_config.py
import logging
import logging.handlers
import os
from pathlib import Path
import json
from logging.handlers import RotatingFileHandler
from typing import Optional
import sys
import structlog

# Get the project root directory
project_root = Path(__file__).parent.parent.parent


class SensitiveDataFilter(logging.Filter):
    """Filter to remove sensitive data from log records."""

    def __init__(self, sensitive_patterns=None):
        super().__init__()
        self.sensitive_patterns = sensitive_patterns or ['password', 'token', 'api_key', 'secret']

    def filter(self, record):
        if isinstance(record.msg, str):
            for pattern in self.sensitive_patterns:
                # Simple pattern replacement, could be more sophisticated
                record.msg = record.msg.replace(pattern, '****REDACTED****')
        return True


def setup_logging(name: str, level: str = None) -> logging.Logger:
    """Set up logging configuration."""
    # Get log level from environment variable or use default
    log_level = level or os.getenv("LOG_LEVEL", "INFO")
    
    # Get log directory from environment variable or use default
    log_dir = os.getenv("LOG_DIR", str(project_root / "logs"))
    
    # Create logs directory if it doesn't exist
    os.makedirs(log_dir, exist_ok=True)
    
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    
    # Remove existing handlers
    logger.handlers = []
    
    # Create formatters
    file_formatter = structlog.stdlib.ProcessorFormatter(
        processor=structlog.processors.JSONRenderer(),
        foreign_pre_chain=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
        ]
    )
    
    console_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - [%(threadName)s] - %(message)s"
    )
    
    # Create file handler
    log_file = os.path.join(log_dir, f"{name}.log")
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=10485760,  # 10MB
        backupCount=5
    )
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)
    
    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    
    # Log debug message if debug logging is enabled
    if log_level.upper() == "DEBUG":
        logger.debug("Debug logging enabled")
    
    return logger


# Utility functions for logging structured data
def log_structured(logger, level, message, data=None, **kwargs):
    """Log a message with structured data."""
    if data is not None:
        if isinstance(data, (dict, list)):
            try:
                message = f"{message} {json.dumps(data, default=str)}"
            except (TypeError, ValueError):
                message = f"{message} {str(data)}"
        else:
            message = f"{message} {data}"

    # Add any additional kwargs to the message
    if kwargs:
        message = f"{message} {json.dumps(kwargs, default=str)}"

    if level == 'debug':
        logger.debug(message)
    elif level == 'info':
        logger.info(message)
    elif level == 'warning':
        logger.warning(message)
    elif level == 'error':
        logger.error(message)
    elif level == 'critical':
        logger.critical(message)


# Create a default logger for the application
app_logger = setup_logging("app")