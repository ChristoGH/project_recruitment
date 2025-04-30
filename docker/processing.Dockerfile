FROM python:3.12-slim

# ----- system deps for headless browsing -----
RUN apt-get update && apt-get install -y chromium-driver fonts-liberation && rm -rf /var/lib/apt/lists/*

# ----- user -----
RUN useradd -m appuser
USER appuser
WORKDIR /app

# ----- install deps -----
COPY requirements.lock .
RUN pip install --no-cache-dir uv && \
    uv pip install --no-cache-dir -r requirements.lock

# ----- copy code -----
COPY --chown=appuser:appuser src/ /app/src
ENV PYTHONPATH=/app/src \
    PYTHONDONTWRITEBYTECODE=1

# ----- runtime -----
CMD ["python", "-m", "recruitment.services.processing"] 