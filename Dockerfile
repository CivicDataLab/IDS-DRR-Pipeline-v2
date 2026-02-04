FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    gdal-bin \
    libgdal-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY pyproject.toml .
COPY flood_risk_pipeline ./flood_risk_pipeline

# Install Python dependencies
RUN pip install --no-cache-dir -e .

# Copy configuration files
COPY workspace.yaml .
COPY dagster.yaml .

# Create directories for data
RUN mkdir -p /app/data /app/staging

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV DAGSTER_HOME=/app

EXPOSE 3000

CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000", "-w", "workspace.yaml"]
