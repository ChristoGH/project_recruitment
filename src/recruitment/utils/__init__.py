# Add this to a utils.py file or in the main processing file

from typing import Dict, Type
from pydantic import BaseModel

# Import all the necessary model classes from url_models.py
from src.recruitment.models.url_models import (
    AdvertResponse,
    JobAdvertResponse,
    ConfirmResponse,
    JobResponse,
    LocationResponse,
    ContactPersonResponse,
    SkillsResponse,
    SkillExperienceResponse,
    AttributesResponse,
    AgencyResponse,
    CompanyResponse,
    IndustryResponse,
    BenefitsResponse,
    DutiesResponse,
    QualificationsResponse,
    LinkResponse,
    EmailResponse,
    CompanyPhoneNumberResponse,
)

# Map prompt keys to their corresponding model classes
PROMPT_MODEL_MAP: Dict[str, Type[BaseModel]] = {
    # Existing mappings
    "recruitment_prompt": AdvertResponse,
    "company_prompt": CompanyResponse,
    "agency_prompt": AgencyResponse,
    "job_prompt": JobResponse,
    "company_phone_number_prompt": CompanyPhoneNumberResponse,
    "email_prompt": EmailResponse,
    "link_prompt": LinkResponse,
    "benefits_prompt": BenefitsResponse,
    "skills_prompt": SkillExperienceResponse,
    "attributes_prompt": AttributesResponse,
    "location_prompt": LocationResponse,
    "jobadvert_prompt": JobAdvertResponse,
    "industry_prompt": IndustryResponse,

    # Add the missing mappings
    "duties_prompt": DutiesResponse,
    "qualifications_prompt": QualificationsResponse,
    "contacts_prompt": ContactPersonResponse,
}


def get_model_for_prompt(prompt_key: str) -> Type[BaseModel]:
    """
    Get the appropriate Pydantic model class for a specific prompt key.

    Args:
        prompt_key: The prompt key to look up

    Returns:
        The corresponding Pydantic model class

    Raises:
        KeyError: If no model is mapped for the given prompt key
    """
    if prompt_key not in PROMPT_MODEL_MAP:
        raise KeyError(f"No model class found for prompt key: {prompt_key}")

    return PROMPT_MODEL_MAP[prompt_key]