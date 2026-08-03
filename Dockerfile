# =============================================================================
# SentinelFlow - Production Dockerfile
# =============================================================================
# Multi-stage build for optimized production image

# -----------------------------------------------------------------------------
# Stage 1: Builder
# -----------------------------------------------------------------------------
FROM python:3.11-slim AS builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy project files needed for build
COPY pyproject.toml ./
COPY src/ ./src/

# Build wheel
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir build && \
    python -m build --wheel --outdir /app/wheels

# -----------------------------------------------------------------------------
# Stage 2: Production Runtime
# -----------------------------------------------------------------------------
FROM python:3.11-slim AS production

LABEL maintainer="Teknofest Team <team@sentinelflow.dev>"
LABEL description="SentinelFlow - Real-Time Financial Fraud Detection System"
LABEL version="2.1.0"

# Create non-root user
RUN groupadd --gid 1000 sentinelflow && \
    useradd --uid 1000 --gid sentinelflow --shell /bin/bash --create-home sentinelflow

WORKDIR /app

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy and install wheel
COPY --from=builder /app/wheels/*.whl /tmp/wheels/
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir /tmp/wheels/*.whl && \
    rm -rf /tmp/wheels

# Copy configuration and scripts
COPY config/ ./config/
COPY scripts/seed_admin.py ./scripts/seed_admin.py

# Create directories
RUN mkdir -p /app/logs /app/models /app/data && \
    chown -R sentinelflow:sentinelflow /app

# Switch to non-root user
USER sentinelflow

# Environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    APP_ENV=production

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD curl -f http://localhost:8000/api/v1/system/health || exit 1

# Expose port
EXPOSE 8000

# Default command (API server)
CMD ["uvicorn", "sentinelflow.api.app:app", "--host", "0.0.0.0", "--port", "8000"]
