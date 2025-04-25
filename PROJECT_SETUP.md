# Recruitment Project Setup

This document outlines the project structure and setup instructions for the Recruitment project.

## Project Overview

The project consists of two main services:
1. URL Discovery Service (port 8000)
2. URL Processing Service (port 8001)

## Prerequisites

- Docker and Docker Compose installed
- OpenAI API key (set as environment variable OPENAI_API_KEY)

## Running the Project

1. Make sure you have set the OPENAI_API_KEY environment variable:
   ```bash
   export OPENAI_API_KEY=your_api_key_here
   ```

2. Start the services:
   ```bash
   docker-compose up --build
   ```

3. To stop the services:
   ```bash
   docker-compose down
   ```

## Services

### URL Discovery Service
- Port: 8000
- Health check endpoint: http://localhost:8000/health

### URL Processing Service
- Port: 8001
- Health check endpoint: http://localhost:8001/health

### RabbitMQ
- Management interface: http://localhost:15672
- Default credentials: guest/guest

## Project Structure

```
new_project/
├── databases/           # Database files
├── logs/               # Log files
├── libraries/          # Custom libraries
├── recruitment/        # Recruitment package
├── Dockerfile.discovery
├── Dockerfile.processing
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## Notes

- The services use RabbitMQ for message queuing
- Logs are stored in the `logs` directory
- Database files are stored in the `databases` directory

## Essential Files

The project requires the following files to run:

1. Docker Configuration:
   - `docker-compose.yml`
   - `Dockerfile.discovery`
   - `Dockerfile.processing`

2. Python Source Files:
   - `url_discovery_service.py`
   - `url_processing_service.py`
   - `logging_config.py`
   - `prompts.py`
   - `recruitment_models.py`
   - `utils.py`
   - `response_processor_functions.py`
   - `batch_processor.py`
   - `web_crawler_lib.py`

3. Dependencies:
   - `requirements.txt`

4. Custom Code:
   - `libraries/` directory
   - `recruitment/` package

5. Data Directories:
   - `databases/` for database files
   - `logs/` for log files

## Service Dependencies

- Both services depend on RabbitMQ for message queuing
- The URL Processing Service requires an OpenAI API key
- Both services use a shared database stored in the `databases` directory 