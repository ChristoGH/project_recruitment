# Prompts that return lists
LIST_PROMPTS = {
    "job_prompt": (
        "Assistant, please extract all advertised job positions from this article if any. "
        "Each job should be descriptive, easily understood by and generally used in the industry. "
        "Return your answer in the following JSON format:\n"
        "{\n"
        '  "jobs": ["job1", "job2", "job3"] or null\n'
        "}"
    ),
    "benefits_prompt": (
        "Assistant, for the following job '{job_title}', please extract the benefits offered to successful candidates from this article, if any. "
        "Benefits are the perks, incentives, or rewards that the successful candidate will receive as part of their employment. "
        "Return your answer in the following JSON format:\n"
        "{\n"
        '  "benefits": ["benefit1", "benefit1", "benefit1"] or null\n'
        "}"
    ),
    "attributes_prompt": (
        "Assistant, for the following job '{job_title}', please extract the attributes required of candidates from this article, if any. "
        "Attributes are personal qualities, characteristics, or traits that are explicitly stated to be essential for the advertised job(s). "
        "Return your answer in the following JSON format:\n"
        "{\n"
        '  "attributes": ["attribute1", "attribute2", "attribute3"] or null\n'
        "}"
    ),
    "duties_prompt": (
        "Assistant, for the following job '{job_title}', please extract the duties and responsibilities that will be required of the candidate as stipulated by this advertised job, if any. "
        "Duties are the tasks and responsibilities that the succesful candidate will be expected to perform. "
        "Return your answer in the following JSON format:\n"
        "{\n"
        '  "duties": ["duty1", "duty2", "duty3"] or null\n'
        "}"
    ),
    "qualifications_prompt": (
        "Assistant, for the following job '{job_title}', please extract the qualifications needed of a successful candidate as stipulated by this advertised job, if any. "
        "Qualifications are strictly diplomas, degrees, certificates, or any other form of educational attainment. "
        "Return your answer in the following JSON format:\n"
        "{\n"
        '  "qualifications": ["qualification1", "qualification2", "qualification3"] or null\n'
        "}"
    ),
    "contacts_prompt": (
        "Assistant, for the following job '{job_title}', please extract the name of the contact person(s) for this advertised job from this article, if any. "
        "Return your answer in the following JSON format:\n"
        "{\n"
        '  "contacts": ["firstname1 secondname1", "firstname2 secondname2", "firstname2 secondname2"] or null\n'
        "}"
    ),
    "recruitment_prompt": (
        "Assistant, please indicate if it can be said with certainty that this is an actual recruitment advert, "
        "that is, a company seeking the services of an individual for remuneration on any form of contractual basis.  "
        "Return your answer in the following RAW JSON format with NO backticks OR code blocks:\n"
        "{\n"
        '  "answer": "yes" or "no",\n'
        '  "evidence": ["evidence1", "evidence2", "evidence3"] or null\n'
        "}"
    ),
}

# Prompts that return single values (non-lists)
NON_LIST_PROMPTS = {
    "company_prompt": (
        "Assistant, please extract the name of the company advertising this job from this article, if any. "
        "Return your answer in the following JSON format:\n"
        "{\n"
        '  "company": "company name" or null\n'
        "}"
    ),
    "agency_prompt": (
        "Assistant, please extract the name of the recruitment agency advertising this job from this article, if any. "
        "Return your answer in the following JSON format:\n"
        "{\n"
        '  "agency": "agency name" or null\n'
        "}"
    ),
    "industry_prompt": (
        "Assistant, please extract the industry or sector this advertised job belongs to from this text, if any. "
        "Return your answer in the following JSON format:\n"
        "{\n"
        '  "industry": "industry name" or null\n'
        "}"
    ),
    "job_prompt": (
        "Assistant, please extract the name of the advertised job(s) from this article if any. "
        "This name should be descriptive, easily understood by and generally used in the industry. "
        "Return your answer in the following JSON format:\n"
        "{\n"
        '  "title": "name" or null\n'
        "}"
    ),
    "company_phone_number_prompt": (
        "Assistant, please extract the contact phone number of the advertised job(s) from this article, if any. "
        "Return your answer in the following JSON format:\n"
        "{\n"
        '  "number": "number" or null\n'
        "}"
    ),
    "email_prompt": (
        "Assistant, please extract the contact email of the advertised job(s) from this article, if any. "
        "Return your answer in the following JSON format:\n"
        "{\n"
        '  "email": "email" or null\n'
        "}"
    ),
    "link_prompt": (
        "Assistant, please extract the contact url of the advertised job(s) from this article, if any. "
        "Return your answer in the following JSON format:\n"
        "{\n"
        '  "link": "url" or null\n'
        "}"
    ),
}

# Complex/nested structure prompts (these contain multiple fields but not arrays)
COMPLEX_PROMPTS = {
    "location_prompt": (
        "Assistant, please extract the location details: country, province, city and the street address of the hiring company for this advertised job(s) from this article, if any. "
        "Return your answer in the following JSON format:\n"
        "{\n"
        '  "country": "country" or null,\n'
        '  "province": "province" or null,\n'
        '  "city": "city" or null,\n'
        '  "street_address": "street_address" or null\n'
        "}"
    ),
    "jobadvert_prompt": (
        "Assistant, please extract the following details: a description of the job, the  in monetary value of salary or remuneration, the duration, the start date, the end date, the posted date of the advert, and the application deadline for the advertised job(s) from this article, if any. "
        "Return your answer in the following JSON format:\n"
        "{\n"
        '  "description": "description" or null,\n'
        '  "salary": "salary" or null,\n'
        '  "duration": "duration" or null,\n'
        '  "start_date": "YYYY-MM-DD" or null,\n'
        '  "end_date": "YYYY-MM-DD" or null,\n'
        '  "posted_date": "YYYY-MM-DD" or null,\n'
        '  "application_deadline": "YYYY-MM-DD" or null\n'
        "}"
    ),
    "skills_prompt": (
        "Assistant, please extract the skills with associated experience required of candidates"
        " of the advertised job(s) from this article, if any. "
        "Skills are job related abilities that are required to perform the advertised job(s) as explicitly stated in the advert. "
        " These are not 'soft skills' or personal strengths such as problem-solving abilities, qualities, traits  or characteristics. "
        " Experience is associated with a particular skill and is the number of years of experience required to perform the job. "
        " If experience is not explicitly stated, please leave it as 'not_listed' and nothing else. "
        "Return your answer strictly in the following JSON format:\n"
        "{\n"
        '  "skills": [("skill1", "experience1"), ("skill2", "experience2"), ("skill3", "not_listed")] or null\n'
        "}"
    ),
}