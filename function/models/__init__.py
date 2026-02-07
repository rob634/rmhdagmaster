# ============================================================================
# FUNCTION APP MODELS
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Gateway - Pydantic models for API
# PURPOSE: Request and response models for function app endpoints
# CREATED: 04 FEB 2026
# ============================================================================
"""
Function App Models

Pydantic V2 models for API requests and responses.
"""

from function.models.requests import (
    JobSubmitRequest,
    BatchSubmitRequest,
    JobQueryRequest,
    ProxyRequest,
)
from function.models.responses import (
    JobSubmitResponse,
    BatchSubmitResponse,
    JobStatusResponse,
    JobListResponse,
    NodeStatusResponse,
    EventListResponse,
    HealthResponse,
    ProxyResponse,
    ErrorResponse,
)

__all__ = [
    # Requests
    "JobSubmitRequest",
    "BatchSubmitRequest",
    "JobQueryRequest",
    "ProxyRequest",
    # Responses
    "JobSubmitResponse",
    "BatchSubmitResponse",
    "JobStatusResponse",
    "JobListResponse",
    "NodeStatusResponse",
    "EventListResponse",
    "HealthResponse",
    "ProxyResponse",
    "ErrorResponse",
]
