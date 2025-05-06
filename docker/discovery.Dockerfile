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
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Create necessary directories
RUN mkdir -p /app/logs /data /app/databases

# Run the service
CMD ["python", "-m", "recruitment.services.discovery.url_discovery_service"] 