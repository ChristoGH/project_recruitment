# Recruitment URL Discovery and Processing Pipeline

A production-ready system for discovering and processing recruitment URLs. This system consists of two main services:

1. **URL Discovery Service**: Discovers recruitment URLs from various sources and publishes them to a message queue.
2. **URL Processing Service**: Consumes URLs from the queue, processes them, and stores the results in a database.

## Features

- Asynchronous URL discovery and processing
- Message queue-based communication (RabbitMQ)
- Structured logging
- Health checks and readiness probes
- Containerized deployment
- Comprehensive test suite
- CI/CD pipeline

## Project Structure

```text
project_recruitment/
│
├─ pyproject.toml          # single source‑of‑truth for build & deps (PEP 517)
├─ README.md
├─ .env.example            # show required env vars; real .env in Secrets
├─ docker-compose.yml
├─ docker/
│   ├─ discovery.Dockerfile
│   └─ processing.Dockerfile
│
├─ src/                    # importable code lives ONLY here
│   └─ recruitment/
│       ├─ __init__.py
│       ├─ logging_config.py
│       ├─ config.py       # Pydantic/BaseSettings → pulls from env
│       ├─ db/
│       │   ├─ __init__.py
│       │   ├─ models.py
│       │   ├─ migrations/         # (Alembic or SQL files)
│       │   └─ repository.py
│       ├─ services/
│       │   ├─ discovery/
│       │   │   ├─ __init__.py
│       │   │   └─ main.py
│       │   ├─ processing/
│       │   │   ├─ __init__.py
│       │   │   └─ main.py
│       │   └─ llm/
│       │       ├─ __init__.py
│       │       └─ llm_service.py
│       ├─ workers/                # background consumers, schedulers
│       │   ├─ __init__.py
│       │   └─ queue_processor.py
│       ├─ utils/
│       │   ├─ __init__.py
│       │   └─ rabbitmq.py
│       └─ prompts/                # prompt text or templates
│           └─ __init__.py
│
├─ tests/
│   ├─ unit/
│   │   ├─ services/
│   │   │   ├─ discovery/
│   │   │   │   └─ test_main.py
│   │   │   ├─ processing/
│   │   │   │   └─ test_main.py
│   │   │   └─ llm/
│   │   │       └─ test_service.py
│   │   └─ db/
│   │       └─ test_repository.py
│   ├─ integration/
│   │   └─ workers/
│   │       └─ test_queue_processor.py
│   └─ e2e/
│
├─ scripts/                # one‑off CLIs (populate_queue.py, etc.)
└─ .gitignore              # **logs/**  **databases/**  *.db  *.bak …
```

## Getting Started

### Prerequisites

- Python 3.12+
- Docker and Docker Compose
- RabbitMQ (managed via Docker)

### Local Development

1. Clone the repository:
   ```bash
   git clone https://github.com/your-org/project_recruitment.git
   cd project_recruitment
   ```

2. Create and activate a virtual environment:
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. Install dependencies:
   ```bash
   pip install -e ".[dev]"
   ```

4. Copy the example environment file:
   ```bash
   cp .env.example .env
   ```

5. Start the services:
   ```bash
   docker compose up -d
   ```

### Running Tests

```bash
# Run all tests
pytest

# Run unit tests only
pytest tests/unit

# Run integration tests
pytest tests/integration

# Run e2e tests
pytest tests/e2e

# Run specific test file
pytest tests/unit/services/discovery/test_main.py
```

## Deployment

The system is designed to be deployed using Docker Compose. The `docker-compose.yml` file defines three services:

1. `rabbitmq`: Message queue for communication between services
2. `url_discovery`: Service for discovering recruitment URLs
3. `url_processing`: Service for processing discovered URLs

To deploy:

```bash
docker compose up -d
```

## Monitoring

Both services expose health check endpoints:

- Discovery Service: `http://localhost:8000/healthz`
- Processing Service: `http://localhost:8001/healthz`

And readiness probes:

- Discovery Service: `http://localhost:8000/readyz`
- Processing Service: `http://localhost:8001/readyz`

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

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
