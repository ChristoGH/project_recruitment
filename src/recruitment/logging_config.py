# logging_config.py
import logging
import logging.handlers
import os
from pathlib import Path
import json
from logging.handlers import RotatingFileHandler
from typing import Optional


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


def setup_logging(
    log_name: str,
    log_level: int = logging.INFO,
    log_dir: str = "/app/logs"
) -> logging.Logger:
    """
    Set up logging configuration for a module.
    
    Args:
        log_name: Name of the logger (usually __name__)
        log_level: Logging level (default: INFO)
        log_dir: Directory to store log files (default: /app/logs)
    
    Returns:
        logging.Logger: Configured logger instance
    """
    # Create logs directory if it doesn't exist
    os.makedirs(log_dir, exist_ok=True)
    
    # Create logger
    logger = logging.getLogger(log_name)
    logger.setLevel(log_level)
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - [%(threadName)s] - %(message)s'
    )
    
    # Create file handler
    log_file = os.path.join(log_dir, f"{log_name}.log")
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    logger.info(f"Logging configured - Level: {logging.getLevelName(log_level)}, Directory: {log_dir}")
    if log_level == logging.DEBUG:
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


# Create default application logger
app_logger = setup_logging("app")