# ============================================================================
# GATEWAY MODULE
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Gateway - Job submission interface for external apps
# PURPOSE: Provides endpoint and models for rmhgeogateway Function App
# CREATED: 04 FEB 2026
# ============================================================================
"""
Gateway Module

Contains the job submission endpoint and models for the rmhgeogateway Function App.
This module can be:
1. Imported directly if rmhgeogateway has access to this codebase
2. Copied to rmhgeogateway as a standalone module

The gateway receives job submission requests, validates them, creates a
JobQueueMessage, and publishes to the dag-jobs Service Bus queue.
"""

from gateway.models import JobSubmitRequest, JobSubmitResponse
from gateway.routes import router as gateway_router

__all__ = [
    "JobSubmitRequest",
    "JobSubmitResponse",
    "gateway_router",
]
