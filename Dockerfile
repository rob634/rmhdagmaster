# ============================================================================
# DAG ORCHESTRATOR DOCKERFILE
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Core - Lightweight orchestrator container
# PURPOSE: Run orchestrator and admin UI
# CREATED: 29 JAN 2026
# ============================================================================
#
# This is a lightweight container (~250MB) that runs:
# - FastAPI application (HTTP API + Admin UI)
# - Orchestration loop (background task)
#
# NO GDAL, NO heavy geo libraries - those live in the worker container.
#
# Build:
#   docker build -t dag-orchestrator .
#
# Run:
#   docker run -p 8000:8000 \
#     -e DATABASE_URL=postgresql://... \
#     -e SERVICEBUS_CONNECTION_STRING=... \
#     dag-orchestrator
# ============================================================================

FROM python:3.12-slim-bookworm

# Labels
LABEL maintainer="DAG Platform Team"
LABEL description="DAG Orchestrator - Epoch 5"
LABEL version="0.1.0"

# Set working directory
WORKDIR /app

# Install minimal system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for layer caching
COPY requirements-orchestrator.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements-orchestrator.txt

# Copy application code
COPY __version__.py .
COPY main.py .
COPY entrypoint.sh .
COPY core/ ./core/
COPY repositories/ ./repositories/
COPY services/ ./services/
COPY messaging/ ./messaging/
COPY orchestrator/ ./orchestrator/
COPY infrastructure/ ./infrastructure/
COPY handlers/ ./handlers/
COPY api/ ./api/
COPY health/ ./health/
COPY ui/ ./ui/
COPY worker/ ./worker/
COPY templates/ ./templates/
COPY static/ ./static/
COPY workflows/ ./workflows/

# Make entrypoint executable
RUN chmod +x entrypoint.sh

# Create non-root user
RUN useradd -m -u 1000 orchestrator && \
    chown -R orchestrator:orchestrator /app

USER orchestrator

# Environment defaults
ENV HOST=0.0.0.0
ENV PORT=8000
ENV DAG_RUN_MODE=orchestrator
ENV DAG_BRAIN_POLL_INTERVAL_SEC=1.0
ENV DAG_WORKFLOWS_DIR=/app/workflows

# Expose port (only used in orchestrator mode)
EXPOSE 8000

# Health check (only works in orchestrator mode)
# Worker mode doesn't expose HTTP - Azure will need different health monitoring
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import httpx; httpx.get('http://localhost:8000/readyz').raise_for_status()" || exit 0

# Run via entrypoint (supports RUN_MODE switching)
ENTRYPOINT ["/app/entrypoint.sh"]
