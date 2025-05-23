# batch_processor.py

import logging
from typing import Dict, Any, List, Optional, Tuple, Union
import json
from src.recruitment.recruitment_db import DatabaseError

from src.recruitment.logging_config import setup_logging

# Create module-specific logger
logger = setup_logging("batch_processor")


def process_all_prompt_responses(db, url_id: int, prompt_responses: Dict[str, Any],
                                 use_transaction: bool = True) -> Dict[str, Any]:
    """
    Process all prompt responses for a URL, optionally using a transaction.

    Args:
        db: RecruitmentDatabase instance
        url_id: The URL ID
        prompt_responses: Dictionary mapping prompt_type to response (model instance or dict)
        use_transaction: Whether to use a transaction (default: True)

    Returns:
        Dict containing status information about the processing
    """
    results = {
        "success": True,
        "processed": 0,
        "failed": 0,
        "errors": []
    }

    # Process with transaction if supported and requested
    if use_transaction and hasattr(db, "process_prompt_responses_in_transaction"):
        try:
            # Convert responses to dicts if they are model instances
            parsed_responses = {}
            for prompt_type, response in prompt_responses.items():
                try:
                    if hasattr(response, "model_dump"):
                        parsed_responses[prompt_type] = response.model_dump()
                    elif isinstance(response, dict):
                        parsed_responses[prompt_type] = response
                    elif isinstance(response, str):
                        parsed_responses[prompt_type] = json.loads(response)
                    else:
                        logger.warning(f"Skipping unknown response format for {prompt_type}")
                        results["failed"] += 1
                        results["errors"].append({
                            "prompt_type": prompt_type,
                            "error": "Unknown response format"
                        })
                        continue
                except Exception as e:
                    logger.error(f"Failed to parse response for {prompt_type}: {e}")
                    results["failed"] += 1
                    results["errors"].append({
                        "prompt_type": prompt_type,
                        "error": f"Parse error: {e}"
                    })

            if parsed_responses:
                # First process job_prompt to get all jobs
                job_response = parsed_responses.get("job_prompt", {})
                jobs = job_response.get("jobs", [])
                
                if not jobs:
                    logger.warning(f"No jobs found in response for URL ID {url_id}")
                    results["failed"] += 1
                    results["errors"].append({
                        "prompt_type": "job_prompt",
                        "error": "No jobs found"
                    })
                    return results
                
                # Process each job
                for job_title in jobs:
                    try:
                        # Insert job
                        job_id = db.insert_job(title=job_title, url_id=url_id)
                        
                        # Process job-specific prompts
                        for prompt_type in ["benefits_prompt", "skills_prompt", "duties_prompt", 
                                         "qualifications_prompt", "attributes_prompt"]:
                            if prompt_type in parsed_responses:
                                try:
                                    # Process the prompt for this specific job
                                    process_prompt_for_job(db, job_id, prompt_type, 
                                                         parsed_responses[prompt_type])
                                    results["processed"] += 1
                                except Exception as e:
                                    logger.error(f"Failed to process {prompt_type} for job '{job_title}': {e}")
                                    results["failed"] += 1
                                    results["errors"].append({
                                        "prompt_type": prompt_type,
                                        "job": job_title,
                                        "error": str(e)
                                    })
                    except Exception as e:
                        logger.error(f"Failed to process job '{job_title}': {e}")
                        results["failed"] += 1
                        results["errors"].append({
                            "prompt_type": "job_prompt",
                            "job": job_title,
                            "error": str(e)
                        })
                
                # Process non-job specific prompts
                for prompt_type in ["company_prompt", "agency_prompt", "location_prompt", 
                                  "jobadvert_prompt", "recruitment_prompt"]:
                    if prompt_type in parsed_responses:
                        try:
                            process_prompt(db, url_id, prompt_type, parsed_responses[prompt_type])
                            results["processed"] += 1
                        except Exception as e:
                            logger.error(f"Failed to process {prompt_type}: {e}")
                            results["failed"] += 1
                            results["errors"].append({
                                "prompt_type": prompt_type,
                                "error": str(e)
                            })

        except Exception as e:
            logger.error(f"Transaction processing failed: {e}", exc_info=True)
            results["success"] = False
            results["errors"].append({
                "prompt_type": "transaction",
                "error": str(e)
            })
    else:
        # Process individually using the PromptResponseProcessor
        processor = PromptResponseProcessor(db)
        return processor.process_all_responses(url_id, prompt_responses, use_transaction)

    return results


def extract_job_data_from_responses(prompt_responses: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract and combine job-related data from multiple prompt responses.
    Ensures skills without experience data are properly handled.

    Args:
        prompt_responses: Dictionary of prompt responses

    Returns:
        Combined job data dictionary
    """
    job_data = {}

    # Helper function to parse responses if they're strings
    def parse_if_string(resp):
        if isinstance(resp, str):
            try:
                return json.loads(resp)
            except json.JSONDecodeError:
                return {}
        elif hasattr(resp, "model_dump"):
            return resp.model_dump()
        return resp if isinstance(resp, dict) else {}

    # Extract job title
    if "job_prompt" in prompt_responses:
        job_response = parse_if_string(prompt_responses["job_prompt"])
        job_data["job_title"] = job_response.get("title")

    # Extract company name
    if "company_prompt" in prompt_responses:
        company_response = parse_if_string(prompt_responses["company_prompt"])
        job_data["company"] = company_response.get("company")

    # Extract location
    if "location_prompt" in prompt_responses:
        location_response = parse_if_string(prompt_responses["location_prompt"])
        job_data["location"] = {
            "country": location_response.get("country"),
            "province": location_response.get("province"),
            "city": location_response.get("city"),
            "street_address": location_response.get("street_address")
        }

    # Extract job advert details
    if "jobadvert_prompt" in prompt_responses:
        jobadvert_response = parse_if_string(prompt_responses["jobadvert_prompt"])
        for key in ["description", "salary", "duration", "start_date",
                    "end_date", "posted_date", "application_deadline"]:
            if key in jobadvert_response:
                job_data[key] = jobadvert_response[key]

    # Extract skills with experience (special case)
    if "skills_prompt" in prompt_responses:
        response = parse_if_string(prompt_responses["skills_prompt"])
        if "skills" in response:
            skills_data = response["skills"]
            # Handle the new format of skills with experience
            if skills_data and isinstance(skills_data, list):
                processed_skills = []
                for skill_item in skills_data:
                    # Handle tuple format
                    if isinstance(skill_item, tuple):
                        if len(skill_item) >= 2:
                            skill, experience = skill_item
                        else:
                            skill, experience = skill_item[0], None
                        processed_skills.append({
                            "skill": skill,
                            "experience": experience
                        })
                    # Handle list format (from converted tuples)
                    elif isinstance(skill_item, list):
                        if len(skill_item) >= 2:
                            skill, experience = skill_item[0], skill_item[1]
                        else:
                            skill, experience = skill_item[0], None
                        processed_skills.append({
                            "skill": skill,
                            "experience": experience
                        })
                    # Handle dictionary format (may come from Pydantic model)
                    elif isinstance(skill_item, dict) and "skill" in skill_item:
                        processed_skills.append({
                            "skill": skill_item["skill"],
                            "experience": skill_item.get("experience")
                        })
                    # Handle string format (for skills without experience)
                    elif isinstance(skill_item, str):
                        processed_skills.append({
                            "skill": skill_item,
                            "experience": None
                        })
                job_data["skills"] = processed_skills

    # Extract other list types (benefits, duties, qualifications)
    for data_type in ["benefits", "duties", "qualifications"]:
        prompt_key = f"{data_type}_prompt"
        if prompt_key in prompt_responses:
            response = parse_if_string(prompt_responses[prompt_key])
            if data_type in response and isinstance(response[data_type], list):
                job_data[data_type] = response[data_type]

    return job_data



# Individual processing functions for non-transaction mode

def process_recruitment(db, url_id: int, response: Any) -> None:
    """Process recruitment flag and evidence following new schema."""
    data = _ensure_dict(response)
    answer = data.get("answer")
    if answer == "yes":
        # Update recruitment flag
        db.update_field_by_id(url_id, "recruitment_flag", 1)
        
        # Process evidence
        evidence = data.get("evidence")
        if evidence and isinstance(evidence, list):
            for evidence_text in evidence:
                db.insert_recruitment_evidence(url_id, evidence_text)
    elif answer == "no":
        db.update_field_by_id(url_id, "recruitment_flag", 0)


def process_company(db, url_id: int, response: Any) -> Tuple[Optional[int], Optional[str]]:
    """
    Process company name and get company ID for linking to job adverts.
    
    Returns both company_id and company_name for later use in job linking.
    """
    data = _ensure_dict(response)
    company_name = data.get("company")
    if company_name:
        db.insert_company(url_id, company_name)
        # Get company_id for future reference
        company_id = db.get_company_id(url_id, company_name)
        return company_id, company_name
    return None, None


def process_agency(db, url_id: int, response: Any) -> None:
    """Process agency name and get recruiter ID for linking to job adverts."""
    data = _ensure_dict(response)
    agency = data.get("agency")
    if agency:
        db.insert_agency(url_id, agency)
        # For recruiter_id, we use agency record since this is the recruiter
        recruiter_id = None
        # Check if there's a get_recruiter_id method
        if hasattr(db, "get_recruiter_id"):
            recruiter_id = db.get_recruiter_id(url_id, agency)
        return recruiter_id
    return None


def process_job(db, url_id: int, response: Any, company_name: Optional[str] = None) -> None:
    """
    Process job title and link it to the correct company based on name.
    
    Args:
        db: Database instance
        url_id: URL ID
        response: Job response data
        company_name: Name of the company for this specific job (optional)
    """
    data = _ensure_dict(response)
    title = data.get("title")
    if title:
        # First insert the job to get its ID
        db.insert_job_advert(url_id, title)
        
        # Get the job_advert_id we just created
        job_advert_id = db.get_job_advert_id(url_id, title)
        
        if job_advert_id and company_name and hasattr(db, "link_job_advert_to_company"):
            # Directly link the job to the company by name
            db.link_job_advert_to_company(job_advert_id, company_name)
            logger.info(f"Linked job '{title}' directly with company '{company_name}'")
        else:
            # Fall back to the old method of linking by URL ID
            # Get company_id if available
            company_id = None
            if hasattr(db, "get_company_id_by_url"):
                company_id = db.get_company_id_by_url(url_id)
            
            # Get recruiter_id if available
            recruiter_id = None
            if hasattr(db, "get_recruiter_id_by_url"):
                recruiter_id = db.get_recruiter_id_by_url(url_id)
            
            # Update the job advert with company and recruiter IDs
            if job_advert_id and (company_id or recruiter_id):
                db.update_job_advert_relations(job_advert_id, company_id, recruiter_id)
                logger.info(f"Updated job '{title}' with company_id {company_id} using URL-based match")


def process_company_phone_number(db, url_id: int, response: Any) -> None:
    """Process phone number."""
    data = _ensure_dict(response)
    number = data.get("number")
    if number:
        db.insert_company_phone_number(url_id, number)


def process_email(db, url_id: int, response: Any) -> None:
    """Process email address."""
    data = _ensure_dict(response)
    email = data.get("email")
    if email:
        db.insert_email(url_id, email)


def process_link(db, url_id: int, response: Any) -> None:
    """Process contact URL link."""
    data = _ensure_dict(response)
    link = data.get("link")
    if link:
        db.insert_link(url_id, link)


def process_benefits(db, url_id: int, response: Any) -> None:
    """Process benefits list."""
    data = _ensure_dict(response)
    benefits = data.get("benefits")
    if benefits and isinstance(benefits, list):
        db.insert_benefits_list(url_id, benefits)


def process_duties(db, url_id: int, response: Any) -> None:
    """Process duties list."""
    data = _ensure_dict(response)
    duties = data.get("duties")
    if duties and isinstance(duties, list):
        db.insert_duties_list(url_id, duties)


def process_qualifications(db, url_id: int, response: Any) -> None:
    """Process qualifications list."""
    data = _ensure_dict(response)
    qualifications = data.get("qualifications")
    if qualifications and isinstance(qualifications, list):
        db.insert_qualifications_list(url_id, qualifications)


def direct_insert_skills(db, url_id: int, skills_data) -> None:
    """
    Directly insert skills data for a URL, bypassing the standard pipeline.
    Use this as a temporary fix or for backfilling missing data.

    Args:
        db: Database instance
        url_id: URL ID
        skills_data: List of tuples (skill, experience)
    """
    logger.info(f"Directly inserting {len(skills_data)} skills for URL ID {url_id}")

    success_count = 0
    for skill_item in skills_data:
        try:
            if isinstance(skill_item, tuple) and len(skill_item) >= 2:
                skill, experience = skill_item
                # Normalize "not_listed" to None
                if experience == "not_listed":
                    experience = None

                query = "INSERT OR IGNORE INTO skills (url_id, skill, experience) VALUES (?, ?, ?)"
                with db._execute_query(query, (url_id, skill, experience)) as cursor:
                    if cursor.rowcount > 0:
                        success_count += 1
                        logger.info(f"Inserted skill: {skill}, experience: {experience}")
            else:
                logger.warning(f"Skipping invalid skill format: {skill_item}")
        except Exception as e:
            logger.error(f"Error inserting skill {skill_item}: {e}")

    logger.info(f"Direct insertion complete: {success_count} skills inserted")


def process_skills(db, url_id: int, response: Any) -> None:
    """
    Process skills list with experience information.

    Args:
        db: Database instance
        url_id: URL ID
        response: Response object containing skills data
    """
    data = _ensure_dict(response)
    skills_data = data.get("skills")

    logger.info(f"Processing skills for URL ID {url_id}")
    logger.info(f"Raw skills data: {skills_data}")

    if not skills_data or not isinstance(skills_data, list):
        logger.warning(f"No valid skills data found for URL ID {url_id}")
        return

    # Normalize skills data to handle multiple formats
    normalized_skills = []

    for item in skills_data:
        try:
            # Handle SkillExperience objects (from Pydantic model)
            if hasattr(item, 'model_dump'):
                skill_dict = item.model_dump()
                skill = skill_dict.get('skill', '')
                experience = skill_dict.get('experience')
                normalized_skills.append((skill, experience))
                logger.debug(f"Normalized Pydantic object: ({skill}, {experience})")
                continue

            # Handle dictionary format with skill key
            if isinstance(item, dict) and "skill" in item:
                skill = item["skill"]
                experience = item.get("experience")
                normalized_skills.append((skill, experience))
                logger.debug(f"Normalized dict: ({skill}, {experience})")
                continue

            # Handle tuple format
            if isinstance(item, tuple):
                if len(item) >= 2:
                    skill, experience = item[0], item[1]
                else:
                    skill, experience = item[0], None
                normalized_skills.append((skill, experience))
                logger.debug(f"Normalized tuple: ({skill}, {experience})")
                continue

            # Handle list format (converted from tuple)
            if isinstance(item, list):
                if len(item) >= 2:
                    skill, experience = item[0], item[1]
                else:
                    skill, experience = item[0], None
                normalized_skills.append((skill, experience))
                logger.debug(f"Normalized list: ({skill}, {experience})")
                continue

            # Handle string format (for skills without experience data)
            if isinstance(item, str):
                normalized_skills.append((item, None))
                logger.debug(f"Normalized string: ({item}, None)")
                continue

            # Try to handle other potential formats
            logger.warning(f"Unrecognized skill format: {type(item).__name__} - {item}")
            normalized_skills.append((str(item), None))

        except Exception as e:
            logger.warning(f"Could not process skill item: {item}, error: {e}")

    logger.info(f"Processed {len(normalized_skills)} skills for URL ID {url_id}")
    logger.info(f"Normalized skills data: {normalized_skills}")

    # Send the normalized skills to the database
    if normalized_skills:
        try:
            db.insert_skills_list(url_id, normalized_skills)
            logger.info(f"Successfully sent {len(normalized_skills)} skills to insert_skills_list function")
        except Exception as e:
            logger.error(f"Error inserting skills for URL ID {url_id}: {e}")


def process_attributes(db, url_id: int, response: Any) -> None:
    """Process attributes list."""
    data = _ensure_dict(response)
    attributes = data.get("attributes")
    if attributes and isinstance(attributes, list):
        db.insert_attributes_list(url_id, attributes)


def process_contacts(db, url_id: int, response: Any) -> None:
    """Process contact persons list."""
    data = _ensure_dict(response)
    contacts = data.get("contacts")
    if contacts and isinstance(contacts, list):
        db.insert_contact_persons_list(url_id, contacts)


def process_location(db, url_id: int, response: Any) -> None:
    """Process location information."""
    data = _ensure_dict(response)
    country = data.get("country")
    province = data.get("province")
    city = data.get("city")
    street_address = data.get("street_address")

    if any([country, province, city, street_address]):
        db.insert_location(
            url_id=url_id,
            country=country,
            province=province,
            city=city,
            street_address=street_address
        )


def process_job_advert(db, url_id: int, response: Any) -> None:
    """Process job advertisement details."""
    data = _ensure_dict(response)
    
    # Get job_advert_id
    job_advert_id = None
    # Try to get the job_advert_id from the database
    query_job_advert = "SELECT id FROM job_adverts WHERE url_id = ? LIMIT 1"
    
    try:
        with db._execute_query(query_job_advert, (url_id,)) as cursor:
            row = cursor.fetchone()
            if row:
                job_advert_id = row[0]
                logger.info(f"Found job_advert_id {job_advert_id} for URL ID {url_id}")
            
            # If we found a job_advert_id, check if it needs company/recruiter linkage
            if job_advert_id:
                company_id = None
                recruiter_id = None
                
                # Try to get company_id if method exists
                if hasattr(db, "get_company_id_by_url"):
                    company_id = db.get_company_id_by_url(url_id)
                
                # Try to get recruiter_id if method exists
                if hasattr(db, "get_recruiter_id_by_url"):
                    recruiter_id = db.get_recruiter_id_by_url(url_id)
                
                # Update job advert relations if needed
                if (company_id or recruiter_id) and hasattr(db, "update_job_advert_relations"):
                    db.update_job_advert_relations(job_advert_id, company_id, recruiter_id)
    
    except Exception as e:
        logger.error(f"Error getting job_advert_id for URL ID {url_id}: {e}")
    
    # Continue with inserting job advert details
    db.insert_job_advert_details(
        url_id=url_id,
        description=data.get("description"),
        salary=data.get("salary"),
        duration=data.get("duration"),
        start_date=data.get("start_date"),
        end_date=data.get("end_date"),
        posted_date=data.get("posted_date"),
        application_deadline=data.get("application_deadline")
    )


def _ensure_dict(response: Any) -> Dict[str, Any]:
    """
    Ensure response is a dictionary.

    Args:
        response: Response object, string, or dictionary

    Returns:
        Dictionary containing response data
    """
    if hasattr(response, "model_dump"):
        # It's a Pydantic model
        return response.model_dump()
    elif isinstance(response, dict):
        # It's already a dict
        return response
    elif isinstance(response, str):
        try:
            # Try to parse as JSON
            return json.loads(response)
        except json.JSONDecodeError:
            # Return empty dict if parsing fails
            return {}
    else:
        # Unknown type, return empty dict
        return {}


def process_industry(db, url_id: int, response: Dict[str, Any]) -> None:
    """
    Process industry data from the industry prompt response.
    
    Args:
        db: Database connection
        url_id: The URL ID
        response: The industry prompt response
    """
    try:
        industry_name = response.get("industry")
        if industry_name:
            # Get the job ID for this URL
            job_id = db.get_job_id_by_url_id(url_id)
            if job_id:
                # Insert the industry and link it to the job
                industry_id = db.insert_industry(industry_name)
                db.link_job_industry(job_id, industry_id)
                logger.info(f"Processed industry '{industry_name}' for job {job_id}")
            else:
                logger.warning(f"No job found for URL ID {url_id}")
    except Exception as e:
        logger.error(f"Error processing industry: {e}")
        raise

def process_prompt_for_job(db, job_id: int, prompt_type: str, data: Dict[str, Any]) -> None:
    """Process a prompt response for a specific job."""
    if prompt_type == "benefits_prompt":
        benefits = data.get("benefits", [])
        for benefit in benefits:
            benefit_id = db.insert_benefit(benefit)
            db.link_job_benefit(job_id, benefit_id)
            
    elif prompt_type == "skills_prompt":
        skills = data.get("skills", [])
        for skill_data in skills:
            if isinstance(skill_data, (tuple, list)):
                skill, experience = skill_data[0], skill_data[1] if len(skill_data) > 1 else None
            else:
                skill, experience = skill_data.get("skill"), skill_data.get("experience")
            
            skill_id = db.insert_skill(skill)
            db.link_job_skill(job_id, skill_id, experience)
            
    elif prompt_type == "duties_prompt":
        duties = data.get("duties", [])
        for duty in duties:
            duty_id = db.insert_duty(duty)
            db.link_job_duty(job_id, duty_id)
            
    elif prompt_type == "qualifications_prompt":
        qualifications = data.get("qualifications", [])
        for qualification in qualifications:
            qual_id = db.insert_qualification(qualification)
            db.link_job_qualification(job_id, qual_id)
            
    elif prompt_type == "attributes_prompt":
        attributes = data.get("attributes", [])
        for attribute in attributes:
            attr_id = db.insert_attribute(attribute)
            db.link_job_attribute(job_id, attr_id)