# Use a lightweight Python base image
FROM python:3.11-slim

# Prevent Python from writing .pyc files and enable unbuffered logging
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV DBT_PROJECT_DIR=/app/dbt_mta_payroll

# Set the working directory inside the container
WORKDIR /app

# Install system dependencies (build-essential is useful for some data libraries)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy and install Python dependencies first to leverage Docker layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project structure
COPY scripts/ ./scripts/
COPY utils/ ./utils/
COPY tests/ ./tests/
COPY pyproject.toml .
COPY config.yaml .

# Copy dbt project and install dependencies
COPY dbt_mta_payroll/ ./dbt_mta_payroll/
RUN cd dbt_mta_payroll && dbt deps

# Default command (can be overridden by Makefile or docker run)
CMD ["python"]