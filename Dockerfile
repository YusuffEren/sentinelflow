# =============================================================================
# SentinelFlow - Production Dockerfile
# =============================================================================
# Multi-stage build for optimized production image

# -----------------------------------------------------------------------------
# Stage 1: Builder
# -----------------------------------------------------------------------------
FROM python:3.11-slim as builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY pyproject.toml ./
COPY src/ ./src/

RUN pip install --no-cache-dir --upgrade pip && \
    pip wheel --no-cache-dir --no-deps --wheel-dir /app/wheels -e .

# -----------------------------------------------------------------------------
# Stage 2: Production Runtime
# -----------------------------------------------------------------------------
FROM python:3.11-slim as production

LABEL maintainer="Teknofest Team <team@sentinelflow.dev>"
LABEL description="SentinelFlow - Real-Time Financial Fraud Detection System"
LABEL version="2.0.0"

# Create non-root user
RUN groupadd --gid 1000 sentinelflow && \
    useradd --uid 1000 --gid sentinelflow --shell /bin/bash --create-home sentinelflow

WORKDIR /app

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy wheels from builder
COPY --from=builder /app/wheels /wheels
COPY --from=builder /app/src ./src

# Install application
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir /wheels/* && \
    pip install --no-cache-dir -e .

# Copy configuration
COPY config/ ./config/

# Create directories
RUN mkdir -p /app/logs /app/models /app/data && \
    chown -R sentinelflow:sentinelflow /app

# Switch to non-root user
USER sentinelflow

# Environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app/src \
    APP_ENV=production

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Expose port
EXPOSE 8000

# Default command (API server)
CMD ["uvicorn", "sentinelflow.api.app:app", "--host", "0.0.0.0", "--port", "8000"]
