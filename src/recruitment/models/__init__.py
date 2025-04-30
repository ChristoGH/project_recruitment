"""
Data models and schemas for the recruitment pipeline.
"""

from .url_models import (
    URLDiscoveryConfig,
    URLProcessingConfig,
    URLProcessingResult,
    transform_skills_response
)

__all__ = [
    "URLDiscoveryConfig",
    "URLProcessingConfig",
    "URLProcessingResult",
    "transform_skills_response"
] 