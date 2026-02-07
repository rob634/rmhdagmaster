# ============================================================================
# API REQUEST MODELS
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Gateway - Request schemas
# PURPOSE: Pydantic V2 models for incoming API requests
# CREATED: 04 FEB 2026
# ============================================================================
"""
API Request Models

Pydantic V2 models for incoming API requests.
All models use V2 patterns: ConfigDict, model_validate, model_dump.
"""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


class JobSubmitRequest(BaseModel):
    """Request to submit a new job for execution."""

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "workflow_id": "hello",
                "input_params": {"name": "World"},
                "correlation_id": "ext-req-12345",
            }
        }
    )

    workflow_id: str = Field(
        ...,
        max_length=64,
        description="ID of the workflow to execute",
    )
    input_params: Dict[str, Any] = Field(
        default_factory=dict,
        description="Parameters passed to the workflow",
    )
    correlation_id: Optional[str] = Field(
        default=None,
        max_length=64,
        description="External correlation ID for tracing across systems",
    )
    callback_url: Optional[str] = Field(
        default=None,
        max_length=512,
        description="URL to POST completion notification (webhook)",
    )
    priority: int = Field(
        default=0,
        ge=0,
        le=10,
        description="Priority (0=normal, 10=highest). Higher priority jobs may be processed first.",
    )
    idempotency_key: Optional[str] = Field(
        default=None,
        max_length=64,
        description="Unique key to prevent duplicate job creation",
    )


class BatchSubmitRequest(BaseModel):
    """Request to submit multiple jobs at once."""

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "jobs": [
                    {"workflow_id": "hello", "input_params": {"name": "Alice"}},
                    {"workflow_id": "hello", "input_params": {"name": "Bob"}},
                ]
            }
        }
    )

    jobs: List[JobSubmitRequest] = Field(
        ...,
        min_length=1,
        max_length=100,
        description="List of jobs to submit (max 100)",
    )


class JobQueryRequest(BaseModel):
    """Query parameters for job listing."""

    model_config = ConfigDict(extra="ignore")

    status: Optional[str] = Field(
        default=None,
        description="Filter by job status (pending, running, completed, failed)",
    )
    workflow_id: Optional[str] = Field(
        default=None,
        description="Filter by workflow ID",
    )
    limit: int = Field(
        default=50,
        ge=1,
        le=500,
        description="Maximum number of results to return",
    )
    offset: int = Field(
        default=0,
        ge=0,
        description="Pagination offset",
    )
    include_nodes: bool = Field(
        default=False,
        description="Include node states in response",
    )


class ProxyRequest(BaseModel):
    """Request to proxy to a Docker app."""

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "target": "orchestrator",
                "path": "/health",
                "method": "GET",
            }
        }
    )

    target: str = Field(
        ...,
        pattern=r"^(orchestrator|worker)$",
        description="Target service: 'orchestrator' or 'worker'",
    )
    path: str = Field(
        ...,
        description="Path to forward (e.g., /health, /api/v1/jobs)",
    )
    method: str = Field(
        default="GET",
        pattern=r"^(GET|POST|PUT|DELETE|PATCH)$",
        description="HTTP method",
    )
    body: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Request body for POST/PUT/PATCH",
    )
    headers: Optional[Dict[str, str]] = Field(
        default=None,
        description="Additional headers to forward",
    )


__all__ = [
    "JobSubmitRequest",
    "BatchSubmitRequest",
    "JobQueryRequest",
    "ProxyRequest",
]
