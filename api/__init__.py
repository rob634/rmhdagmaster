# ============================================================================
# API MODULE
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Core - FastAPI routes
# PURPOSE: HTTP API for job management
# CREATED: 29 JAN 2026
# ============================================================================
"""
API Module

FastAPI routes for the DAG orchestrator.
"""

from .routes import router
from .schemas import (
    JobCreate,
    JobResponse,
    NodeResponse,
    TaskResultCreate,
)

__all__ = [
    "router",
    "JobCreate",
    "JobResponse",
    "NodeResponse",
    "TaskResultCreate",
]
