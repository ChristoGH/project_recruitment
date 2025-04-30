#!/bin/bash
set -e

# Wait for services to be healthy
echo "Waiting for services to be healthy..."
timeout=300
interval=10
elapsed=0

while [ $elapsed -lt $timeout ]; do
    if curl -s -f http://localhost:8000/healthz > /dev/null && \
       curl -s -f http://localhost:8001/healthz > /dev/null; then
        echo "All services are healthy!"
        break
    fi
    echo "Waiting for services to be healthy... ($elapsed/$timeout seconds)"
    sleep $interval
    elapsed=$((elapsed + interval))
done

if [ $elapsed -ge $timeout ]; then
    echo "Timeout waiting for services to be healthy"
    exit 1
fi

# Check if URLs are being discovered and processed
echo "Checking if URLs are being discovered and processed..."
sleep 60  # Wait for some URLs to be discovered and processed

# Check database for processed URLs
if [ -f "/app/databases/recruitment.db" ]; then
    count=$(sqlite3 /app/databases/recruitment.db "SELECT COUNT(*) FROM processed_urls;")
    if [ "$count" -gt 0 ]; then
        echo "Success! Found $count processed URLs in the database."
        exit 0
    else
        echo "Error: No processed URLs found in the database."
        exit 1
    fi
else
    echo "Error: Database file not found."
    exit 1
fi 