"""
URL Processing Service

This service processes URLs from a RabbitMQ queue and stores the results in a database.
"""

from .main import app

__all__ = ['app'] 