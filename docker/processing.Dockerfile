FROM python:3.11-slim

WORKDIR /app

# Install system dependencies including Playwright requirements
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    curl \
    libglib2.0-0 \
    libnss3 \
    libnspr4 \
    libdbus-1-3 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libxkbcommon0 \
    libatspi2.0-0 \
    libxcomposite1 \
    libxdamage1 \
    libxext6 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libpango-1.0-0 \
    libcairo2 \
    libasound2 \
    && rm -rf /var/lib/apt/lists/*

# Copy the entire project
COPY . .

# Install the package and its dependencies
RUN pip install --no-cache-dir .

# Install Playwright browsers
RUN playwright install chromium
RUN playwright install-deps

# Set environment variables
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Create necessary directories and set permissions
RUN mkdir -p /app/logs /data /app/databases /app/src/recruitment/db && \
    chmod -R 777 /app/databases /app/src/recruitment/db

# Run the service
CMD ["uvicorn", "src.recruitment.services.processing.main:app", "--host", "0.0.0.0", "--port", "8001"] 