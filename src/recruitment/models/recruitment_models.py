from typing import Dict, List, Optional
from pydantic import BaseModel

class SkillsResponse(BaseModel):
    """Model for skills response from the web crawler."""
    skills: List[str]
    confidence: float
    source: str

def transform_skills_response(response: Dict) -> SkillsResponse:
    """
    Transform the raw skills response into a structured format.
    
    Args:
        response: Raw response from the web crawler
        
    Returns:
        SkillsResponse: Structured skills response
    """
    try:
        # Extract skills from the response
        skills = response.get("skills", [])
        if isinstance(skills, str):
            skills = [skill.strip() for skill in skills.split(",")]
        
        # Create the response object
        return SkillsResponse(
            skills=skills,
            confidence=response.get("confidence", 0.0),
            source=response.get("source", "unknown")
        )
    except Exception as e:
        # Return empty response if transformation fails
        return SkillsResponse(
            skills=[],
            confidence=0.0,
            source="error"
        ) 