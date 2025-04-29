## URL Processing Dashboard

The URL Processing Dashboard is a Streamlit application that allows you to monitor and manually process URLs from the recruitment database. It provides a step-by-step interface for processing job postings and debugging any issues that arise.

### Features

- View all URLs from the database with their current processing status
- Select individual URLs for processing
- Step through each processing stage manually:
  1. Web Crawling
  2. Company Information Processing
  3. Job Details Processing
  4. Skills Processing
  5. Location Processing
  6. Benefits Processing
  7. Contacts Processing
  8. Job Advert Processing
  9. Industry Processing
- View detailed error messages and processing results
- Monitor the progress of each processing step
- Support for processing multiple jobs from a single URL

### Multiple Jobs Per URL Support

The system now supports processing multiple job listings from a single URL. This feature includes:

- **Job Extraction**: The system can identify and extract multiple job titles from a single URL
- **Job-Specific Processing**: Each job is processed independently with its own:
  - Benefits
  - Skills and experience requirements
  - Duties and responsibilities
  - Qualifications
  - Attributes
  - Contact information
- **Shared Information**: Common information across jobs is processed once:
  - Company details
  - Agency information
  - Location data
  - Job advert details
  - Industry classification

### Database Structure

The database has been updated to support multiple jobs per URL:

- Each job is stored as a separate record in the `jobs` table
- Job-specific information is linked through junction tables:
  - `job_skills`
  - `job_benefits`
  - `job_duties`
  - `job_qualifications`
  - `job_attributes`
  - `job_contacts`
- Common information is stored in separate tables and linked to the URL:
  - `companies`
  - `agencies`
  - `locations`
  - `job_adverts`
  - `industries`

### Installation

1. Install Streamlit if you haven't already:
```bash
pip install streamlit
```

### Usage

1. Start the Streamlit app:
```bash
streamlit run streamlit_app.py
```

2. The app will open in your default web browser. You can then:
   - Select a URL from the dropdown menu
   - Click the "Crawl Website" button to start the process
   - After crawling, click each subsequent button to process different aspects of the job posting
   - View the results and any errors that occur during processing
   - For URLs with multiple jobs, each job will be processed independently

### Troubleshooting

If you encounter any issues:
- Check that the database is properly initialized and accessible
- Verify that all required Python packages are installed
- Check the Streamlit console for detailed error messages
- Ensure the web crawler has proper access to the target URLs
- For multiple job processing issues, check the logs for job-specific errors

### Development Notes

Recent changes to support multiple jobs per URL:

1. **Models**:
   - Updated `JobResponse` model to handle multiple jobs
   - Added validation for job titles

2. **Prompts**:
   - Modified prompts to include job-specific context
   - Added job title prefix to relevant prompts

3. **Processing**:
   - Updated batch processor to handle multiple jobs
   - Added job-specific processing functions
   - Improved error handling and logging

4. **Database**:
   - Enhanced job insertion logic
   - Added job title retrieval functionality
   - Updated transaction handling for multiple jobs

## Known Issues and Solutions

### Uvicorn Configuration Issue
The URL Processing Service was experiencing startup issues due to an invalid uvicorn flag in the Dockerfile. The `--no-reload` flag was causing the service to fail to start properly. This has been fixed by removing the invalid flag from the Dockerfile.processing.

The correct uvicorn command in the Dockerfile should be:
```bash
CMD ["uvicorn", "url_processing_service:app", "--host", "0.0.0.0", "--port", "8001", "--log-level", "debug", "--no-access-log"]
```

### Running Services Locally
When running the services locally (outside of Docker), you need to set the PYTHONPATH to include the project root directory:
```bash
PYTHONPATH=$PYTHONPATH:. python3 recruitment/url_processing_service.py
```
