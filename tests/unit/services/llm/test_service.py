"""Test module for LLM service."""
import pytest
import asyncio
from unittest.mock import AsyncMock, patch
import sys
import os

# Add the parent directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))))

from src.recruitment.services.llm.llm_service import LLMService 