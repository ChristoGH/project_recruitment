"""Database models for the recruitment application."""

from dataclasses import dataclass
from typing import Optional


@dataclass
class JobPosting:
    """Model representing a job posting."""

    title: str
    company: str
    location: str
    description: str
    url: Optional[str] = None
    salary_range: Optional[str] = None
    job_type: Optional[str] = None
    experience_level: Optional[str] = None
    skills: Optional[list[str]] = None
    posted_date: Optional[str] = None
    application_deadline: Optional[str] = None
    contact_info: Optional[str] = None
    benefits: Optional[list[str]] = None
    remote_work: Optional[bool] = None
