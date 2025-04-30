import logging
import os
from pathlib import Path
from logging.handlers import RotatingFileHandler
from typing import Optional

def get_log_level() -> int:
    """Get log level from environment or default to INFO."""
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    return getattr(logging, level, logging.INFO)

def setup_logging(service_name: str, log_dir: Optional[Path] = None) -> logging.Logger:
    """
    Set up logging configuration for the service.
    
    Args:
        service_name: Name of the service (e.g., 'discovery' or 'processing')
        log_dir: Optional path to log directory. Defaults to /app/logs
    
    Returns:
        Configured logger instance
    """
    # Create logs directory if it doesn't exist
    if log_dir is None:
        log_dir = Path("/app/logs")
    log_dir.mkdir(exist_ok=True)
    
    # Configure logging
    logger = logging.getLogger(service_name)
    logger.setLevel(get_log_level())
    
    # Remove any existing handlers to avoid duplicate logging
    logger.handlers.clear()
    
    # Create formatter with structured format
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
    )
    
    try:
        # Create file handler with rotation
        log_file = log_dir / f"{service_name}.log"
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5,
            encoding='utf-8'
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        
        # Create console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        logger.info(f"Logging configured for service: {service_name}")
        return logger
    except Exception as e:
        # Fallback to basic console logging if file logging fails
        logging.basicConfig(level=get_log_level())
        logging.error(f"Failed to configure file logging: {e}")
        return logging.getLogger(service_name) 