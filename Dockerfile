# Bytewax Data Pipeline Dockerfile
# Multi-stage build for optimized production image

# ===== BUILD STAGE =====
FROM python:3.12-slim as builder

# Set build-time environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    librdkafka-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Create virtual environment
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Install Python dependencies
COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

# ===== PRODUCTION STAGE =====
FROM python:3.12-slim as production

# Set runtime environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    PATH="/opt/venv/bin:$PATH"

# Install runtime dependencies only
RUN apt-get update && apt-get install -y \
    librdkafka1 \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Copy virtual environment from builder stage
COPY --from=builder /opt/venv /opt/venv

# Create non-root user for OpenShift compatibility
# OpenShift runs containers with random UIDs in range 1000-2000000000
RUN groupadd -g 1001 bytewax && \
    useradd -u 1001 -g bytewax -m -s /bin/bash bytewax

# Create application directory
WORKDIR /app

# Copy application code
COPY --chown=1001:1001 pipeline/ ./pipeline/
COPY --chown=1001:1001 config/ ./config/
COPY --chown=1001:1001 utils/ ./utils/

# Create cache directory with proper permissions
RUN mkdir -p /app/cache && \
    chown -R 1001:1001 /app

# Switch to non-root user
USER 1001

# Expose metrics port
EXPOSE 8000

# Health check endpoint
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import requests; requests.get('http://localhost:8000/health')" || exit 1

# Set entrypoint for Bytewax pipeline
# This can be overridden for different utilities
ENTRYPOINT ["python", "-m", "bytewax.run"]
CMD ["pipeline.main:flow"]

# Metadata labels for OpenShift
LABEL name="bytewax-pipeline" \
      version="1.0.0" \
      description="Bytewax data processing pipeline for OpenShift" \
      maintainer="Claude AI Assistant" \
      io.k8s.description="Stream processing pipeline using Bytewax and Kafka" \
      io.k8s.display-name="Bytewax Data Pipeline" \
      io.openshift.tags="bytewax,kafka,streaming,data-processing" \
      io.openshift.expose-services="8000:http"