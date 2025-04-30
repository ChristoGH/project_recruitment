import logging
import os
from pathlib import Path
from logging.handlers import RotatingFileHandler

def setup_logging(service_name: str) -> logging.Logger:
    """
    Set up logging configuration for the service.
    
    Args:
        service_name: Name of the service (e.g., 'discovery' or 'processing')
    
    Returns:
        Configured logger instance
    """
    # Create logs directory if it doesn't exist
    log_dir = Path("/app/logs")
    log_dir.mkdir(exist_ok=True)
    
    # Configure logging
    logger = logging.getLogger(service_name)
    logger.setLevel(logging.INFO)
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create file handler with rotation
    log_file = log_dir / f"{service_name}.log"
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
    
    logger.info(f"Logging configured for service: {service_name}")
    return logger 