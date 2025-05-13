FROM python:3.11-slim

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy the entire project
COPY . .

# Install the package and its dependencies
RUN pip install --no-cache-dir .

# Set environment variables
ENV PYTHONPATH="/app/src:${PYTHONPATH}"
ENV PYTHONUNBUFFERED=1

# Create necessary directories and set permissions
RUN mkdir -p /app/logs /data /app/databases /app/src/recruitment/db && \
    chmod -R 777 /app/databases /app/src/recruitment/db

# Run the service
CMD ["uvicorn", "services.discovery.url_discovery_service:app", "--host", "0.0.0.0", "--port", "8000"] 