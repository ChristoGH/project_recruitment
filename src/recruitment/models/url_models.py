# Enhanced models.py with Pydantic V2 validation

from typing import List, Optional, Literal, Dict, Any, Tuple, Union
from pydantic import BaseModel, Field, field_validator, EmailStr, model_validator, ConfigDict
import re
import logging
from datetime import datetime

from src.recruitment.logging_config import setup_logging

# Create module-specific logger
logger = setup_logging("recruitment_models")


class AdvertResponse(BaseModel):
    model_config = ConfigDict(extra='forbid')
    answer: Literal["yes", "no"]  # Restrict to only valid values
    evidence: Optional[List[str]] = None

    @model_validator(mode='after')
    def validate_evidence_provided(self):
        if self.answer == 'yes' and (not self.evidence or len(self.evidence) == 0):
            # Instead of raising an error, provide a default evidence message
            self.evidence = ["No specific evidence provided"]
        return self


class ConfirmResponse(BaseModel):
    model_config = ConfigDict(extra='forbid')
    answer: Literal["yes", "no"]
    evidence: Optional[List[str]] = None


class JobResponse(BaseModel):
    model_config = ConfigDict(extra='forbid')
    jobs: Optional[List[str]] = None

    @field_validator('jobs')
    @classmethod
    def validate_jobs(cls, v):
        if v:
            return [job.strip() for job in v if job.strip()]
        return v


class LocationResponse(BaseModel):
    model_config = ConfigDict(extra='forbid')
    country: Optional[str] = Field(None, min_length=2, max_length=100)
    province: Optional[str] = Field(None, max_length=100)
    city: Optional[str] = Field(None, max_length=100)
    street_address: Optional[str] = Field(None, max_length=500)


class ContactPersonResponse(BaseModel):
    model_config = ConfigDict(extra='forbid')
    contacts: Optional[List[str]] = None

    @field_validator('contacts')
    @classmethod
    def validate_contacts(cls, v):
        if v:
            for contact in v:
                if len(contact.strip().split()) < 2:
                    raise ValueError(f"Contact '{contact}' should include first and last name")
        return v


class SkillsResponse(BaseModel):
    model_config = ConfigDict(extra='forbid')
    skills: Optional[List[str]] = None

    @field_validator('skills')
    @classmethod
    def validate_skills(cls, v):
        if v:
            return [skill.strip() for skill in v if skill.strip()]
        return v


# Updated SkillExperience class with optional experience field
class SkillExperience(BaseModel):
    model_config = ConfigDict(extra='forbid')
    skill: str
    experience: Optional[str] = None

    @field_validator('skill')
    @classmethod
    def strip_skill_whitespace(cls, v):
        return v.strip() if v else v

    @field_validator('experience')
    @classmethod
    def validate_experience(cls, v):
        if v is None:
            return None
        return v.strip() if v else None


# Updated SkillExperienceResponse with flexible input handling
class SkillExperienceResponse(BaseModel):
    model_config = ConfigDict(extra='forbid')
    skills: Optional[List[Union[SkillExperience, Dict, List, Tuple, str]]] = None

    @model_validator(mode='after')
    def transform_skills(self):
        if not self.skills:
            return self

        transformed_skills = []

        for item in self.skills:
            # Already a SkillExperience object
            if isinstance(item, SkillExperience):
                transformed_skills.append(item)

            # List or tuple format: [skill, experience] or (skill, experience)
            elif isinstance(item, (list, tuple)):
                if len(item) >= 2:
                    skill, experience = item[0], item[1]
                else:
                    skill, experience = item[0], None

                transformed_skills.append(SkillExperience(
                    skill=skill,
                    experience=experience
                ))

            # String format (skill only)
            elif isinstance(item, str):
                transformed_skills.append(SkillExperience(
                    skill=item,
                    experience=None
                ))

            # Dictionary format
            elif isinstance(item, dict) and 'skill' in item:
                transformed_skills.append(SkillExperience(
                    skill=item['skill'],
                    experience=item.get('experience')
                ))

        self.skills = transformed_skills
        return self


class AttributesResponse(BaseModel):
    model_config = ConfigDict(extra='forbid')
    attributes: Optional[List[str]] = None

    @field_validator('attributes')
    @classmethod
    def validate_attributes(cls, v):
        if v:
            return [attr.strip() for attr in v if attr.strip()]
        return v


class AgencyResponse(BaseModel):
    model_config = ConfigDict(extra='forbid')
    agency: Optional[str] = Field(None, min_length=2, max_length=200)


class CompanyResponse(BaseModel):
    model_config = ConfigDict(extra='forbid')
    company: Optional[str] = Field(None, min_length=2, max_length=200)


class IndustryResponse(BaseModel):
    model_config = ConfigDict(extra='forbid')
    industry: Optional[str] = Field(None, min_length=2, max_length=200)

    @field_validator('industry')
    @classmethod
    def validate_industry(cls, v):
        if v is not None:
            v = v.strip()
            if len(v) < 2:
                return None
        return v


class BenefitsResponse(BaseModel):
    model_config = ConfigDict(extra='forbid')
    benefits: Optional[List[str]] = None

    @field_validator('benefits')
    @classmethod
    def validate_benefits(cls, v):
        if v:
            return [benefit.strip() for benefit in v if benefit.strip()]
        return v


class DutiesResponse(BaseModel):
    model_config = ConfigDict(extra='forbid')
    duties: Optional[List[str]] = None

    @field_validator('duties')
    @classmethod
    def validate_duties(cls, v):
        if v:
            return [duty.strip() for duty in v if duty.strip()]
        return v


class QualificationsResponse(BaseModel):
    model_config = ConfigDict(extra='forbid')
    qualifications: Optional[List[str]] = None

    @field_validator('qualifications')
    @classmethod
    def validate_qualifications(cls, v):
        if v:
            return [qual.strip() for qual in v if qual.strip()]
        return v


class LinkResponse(BaseModel):
    model_config = ConfigDict(extra='forbid')
    link: Optional[str] = None

    @field_validator('link')
    @classmethod
    def validate_link(cls, v):
        if v is not None:
            v = v.strip()
            if not v.startswith(('http://', 'https://')):
                v = 'https://' + v
        return v


class EmailResponse(BaseModel):
    model_config = ConfigDict(extra='forbid')
    email: Optional[EmailStr] = None  # Using EmailStr for email validation
    type: Optional[str] = "primary"  # Default to primary if not specified


class CompanyPhoneNumberResponse(BaseModel):
    model_config = ConfigDict(extra='forbid')
    number: Optional[str] = None

    @field_validator('number')
    @classmethod
    def validate_phone(cls, v):
        if v is not None:
            # Remove all non-digit characters
            v = re.sub(r'\D', '', v)
            if len(v) < 10:
                raise ValueError("Phone number must be at least 10 digits")
        return v


class JobAdvertResponse(BaseModel):
    model_config = ConfigDict(extra='forbid')
    description: Optional[str] = None
    salary: Optional[str] = None
    duration: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    posted_date: Optional[str] = None
    application_deadline: Optional[str] = None

    @field_validator('start_date', 'end_date', 'posted_date', 'application_deadline')
    @classmethod
    def validate_dates(cls, v):
        if v:
            try:
                # Check if date follows the required format
                datetime.strptime(v, '%Y-%m-%d')
            except ValueError:
                raise ValueError('Date must be in format YYYY-MM-DD')
        return v

    @model_validator(mode='after')
    def validate_date_ranges(self):
        if self.start_date and self.end_date:
            start = datetime.strptime(self.start_date, '%Y-%m-%d')
            end = datetime.strptime(self.end_date, '%Y-%m-%d')
            if end < start:
                raise ValueError('End date cannot be before start date')
        return self


# Helper function to transform skills responses


def transform_skills_response(response_data: Union[Dict[str, Any], BaseModel]) -> Dict[
    str, List[Tuple[str, Optional[str]]]]:
    """
    Transform skills response data into the expected tuple format.
    Handles various input formats and ensures "not_listed" values are properly converted to None.

    Args:
        response_data: The raw response data (dictionary or Pydantic model)

    Returns:
        Dictionary with skills transformed to (skill, experience) tuples
    """
    # Handle case when input is a Pydantic model
    if hasattr(response_data, 'model_dump'):
        data_dict = response_data.model_dump()
    else:
        data_dict = response_data

    if not data_dict or not isinstance(data_dict, dict) or 'skills' not in data_dict:
        logger.warning("No skills data found in response")
        return {'skills': []}

    skills_data = data_dict.get('skills', [])
    processed_skills = []

    # Log what we're working with
    logger.debug(f"Processing skills data: {skills_data}")

    if isinstance(skills_data, list):
        for item in skills_data:
            # Handle SkillExperience objects
            if hasattr(item, 'model_dump'):
                skill_dict = item.model_dump()
                skill = skill_dict.get('skill', '')
                experience = skill_dict.get('experience')

                # Convert "not_listed" to None
                if experience == "not_listed":
                    experience = None

                processed_skills.append((skill, experience))
                continue

            # Handle dictionary format
            if isinstance(item, dict) and 'skill' in item:
                skill = item['skill']
                experience = item.get('experience')

                # Convert "not_listed" to None
                if experience == "not_listed":
                    experience = None

                # Skip empty skills
                if not skill or not isinstance(skill, str) or not skill.strip():
                    continue

                processed_skills.append((
                    skill.strip(),
                    experience.strip() if experience and isinstance(experience,
                                                                    str) and experience != "not_listed" else None
                ))
                continue

            # Handle tuple format (from prompt response)
            if isinstance(item, tuple):
                if len(item) >= 2:
                    skill, experience = item[0], item[1]
                else:
                    skill, experience = item[0], None

                # Convert "not_listed" to None
                if experience == "not_listed":
                    experience = None

                # Skip empty skills
                if not skill or not isinstance(skill, str) or not skill.strip():
                    continue

                processed_skills.append((
                    skill.strip(),
                    experience.strip() if experience and isinstance(experience,
                                                                    str) and experience != "not_listed" else None
                ))
                continue

            # Handle list format (converted from tuple)
            if isinstance(item, list):
                if len(item) >= 2:
                    skill, experience = item[0], item[1]
                else:
                    skill, experience = item[0], None

                # Convert "not_listed" to None
                if experience == "not_listed":
                    experience = None

                # Skip empty skills
                if not skill or not isinstance(skill, str) or not skill.strip():
                    continue

                processed_skills.append((
                    skill.strip(),
                    experience.strip() if experience and isinstance(experience,
                                                                    str) and experience != "not_listed" else None
                ))
                continue

            # Handle string format (backward compatibility)
            if isinstance(item, str):
                if not item.strip():
                    continue

                processed_skills.append((item.strip(), None))
                continue

            # Special handling for string representation of tuples from LLM response
            if isinstance(item, str) and '(' in item and ')' in item:
                try:
                    # Try to parse string tuple format
                    tuple_str = item.strip()
                    if tuple_str.startswith('(') and tuple_str.endswith(')'):
                        tuple_str = tuple_str[1:-1]  # Remove outer parentheses
                        parts = tuple_str.split(',', 1)

                        if len(parts) == 2:
                            skill = parts[0].strip(' "\'')
                            experience = parts[1].strip(' "\'')

                            # Convert "not_listed" to None
                            if experience == "not_listed":
                                experience = None

                            processed_skills.append((skill, experience))
                        else:
                            skill = parts[0].strip(' "\'')
                            processed_skills.append((skill, None))
                except Exception as e:
                    logger.warning(f"Failed to parse tuple string: {item}, error: {e}")
                    continue

            # Fallback for any other type
            try:
                processed_skills.append((str(item).strip(), None))
            except Exception as e:
                logger.warning(f"Could not process skill item: {item}, error: {e}")
                pass

    # Log the processed skills
    logger.debug(f"Transformed skills: {processed_skills}")

    # Update the response data
    return {'skills': processed_skills}


# Modification to batch_processor.py
# Replace the process_skills function with this improved version:


class URLDiscoveryConfig(BaseModel):
    search_terms: List[str]
    max_results: int = Field(default=10, gt=0)
    interval_minutes: int = Field(default=60, gt=0)


class URLProcessingConfig(BaseModel):
    max_concurrent_requests: int = Field(default=5, gt=0)
    request_timeout: int = Field(default=30, gt=0)
    retry_attempts: int = Field(default=3, ge=0)


class URLProcessingResult(BaseModel):
    url: str
    title: Optional[str] = None
    description: Optional[str] = None
    skills: List[str] = Field(default_factory=list)
    company: Optional[str] = None
    location: Optional[str] = None
    salary_range: Optional[str] = None
    job_type: Optional[str] = None
    error: Optional[str] = None


class URL(BaseModel):
    """Model representing a URL to be processed."""
    url: str
    domain: str
    prompt_responses: List[Dict[str, str]] = []
    source: str = "direct"
    status: Optional[str] = None
    error_count: int = 0
    error_message: Optional[str] = None

