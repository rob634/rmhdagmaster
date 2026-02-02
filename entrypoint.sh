#!/bin/bash
# ============================================================================
# ENTRYPOINT SCRIPT - DAG ORCHESTRATOR / WORKER
# ============================================================================
# Supports multiple run modes via RUN_MODE environment variable:
#   - orchestrator (default): Run FastAPI + orchestration loop
#   - worker: Run Service Bus consumer
# ============================================================================

set -e

RUN_MODE=${RUN_MODE:-orchestrator}

echo "=============================================="
echo "DAG Platform Starting"
echo "RUN_MODE: $RUN_MODE"
echo "=============================================="

case "$RUN_MODE" in
    orchestrator)
        echo "Starting Orchestrator (FastAPI + Loop)..."
        exec uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000}
        ;;
    worker)
        echo "Starting Worker (Service Bus Consumer)..."
        exec python -m worker.main
        ;;
    *)
        echo "ERROR: Unknown RUN_MODE: $RUN_MODE"
        echo "Valid modes: orchestrator, worker"
        exit 1
        ;;
esac
