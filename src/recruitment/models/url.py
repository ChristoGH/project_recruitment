"""
URL model for recruitment processing.
"""
from typing import List, Dict, Optional
from pydantic import BaseModel

class URL(BaseModel):
    """Model representing a URL to be processed."""
    url: str
    domain: str
    prompt_responses: List[Dict[str, str]] = []
    source: str = "direct"
    status: Optional[str] = None
    error_count: int = 0
    error_message: Optional[str] = None 