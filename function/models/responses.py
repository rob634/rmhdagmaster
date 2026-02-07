# ============================================================================
# API RESPONSE MODELS
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Gateway - Response schemas
# PURPOSE: Pydantic V2 models for API responses
# CREATED: 04 FEB 2026
# ============================================================================
"""
API Response Models

Pydantic V2 models for API responses.
All models use V2 patterns: ConfigDict, model_validate, model_dump.
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


class JobSubmitResponse(BaseModel):
    """Response after successful job submission."""

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "message_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
                "workflow_id": "hello",
                "submitted_at": "2026-02-04T10:30:00Z",
                "status": "queued",
                "correlation_id": "ext-req-12345",
            }
        }
    )

    message_id: str = Field(
        ...,
        description="Service Bus message ID for tracking",
    )
    workflow_id: str = Field(
        ...,
        description="Workflow that will be executed",
    )
    submitted_at: datetime = Field(
        ...,
        description="Timestamp when the submission was received",
    )
    status: str = Field(
        default="queued",
        description="Submission status ('queued' = waiting for orchestrator)",
    )
    correlation_id: Optional[str] = Field(
        default=None,
        description="Correlation ID if provided in request",
    )


class BatchSubmitResponse(BaseModel):
    """Response for batch job submission."""

    model_config = ConfigDict()

    submitted: int = Field(
        ...,
        description="Number of jobs successfully queued",
    )
    failed: int = Field(
        ...,
        description="Number of jobs that failed to queue",
    )
    message_ids: List[str] = Field(
        ...,
        description="Message IDs of submitted jobs (may be empty for batch)",
    )
    errors: List[str] = Field(
        default_factory=list,
        description="Error messages for failed jobs",
    )


class JobStatusResponse(BaseModel):
    """Job status response."""

    model_config = ConfigDict()

    job_id: str = Field(..., description="Unique job identifier")
    workflow_id: str = Field(..., description="Workflow being executed")
    status: str = Field(..., description="Job status")
    created_at: datetime = Field(..., description="When job was created")
    started_at: Optional[datetime] = Field(default=None, description="When processing started")
    completed_at: Optional[datetime] = Field(default=None, description="When job completed")
    error_message: Optional[str] = Field(default=None, description="Error if failed")
    owner_id: Optional[str] = Field(default=None, description="Orchestrator instance owning this job")
    correlation_id: Optional[str] = Field(default=None, description="External correlation ID")

    # Optional node states (when include_nodes=true)
    nodes: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Node states (only if requested)",
    )


class JobListResponse(BaseModel):
    """Response for job listing."""

    model_config = ConfigDict()

    jobs: List[JobStatusResponse] = Field(..., description="List of jobs")
    total: int = Field(..., description="Total count matching filters")
    limit: int = Field(..., description="Limit used in query")
    offset: int = Field(..., description="Offset used in query")


class NodeStatusResponse(BaseModel):
    """Node status response."""

    model_config = ConfigDict()

    node_id: str = Field(..., description="Unique node identifier")
    job_id: str = Field(..., description="Parent job ID")
    node_name: str = Field(..., description="Node name from workflow definition")
    status: str = Field(..., description="Node status")
    handler_name: str = Field(..., description="Handler that processes this node")
    created_at: datetime = Field(..., description="When node was created")
    started_at: Optional[datetime] = Field(default=None, description="When processing started")
    completed_at: Optional[datetime] = Field(default=None, description="When node completed")
    error_message: Optional[str] = Field(default=None, description="Error if failed")
    result_data: Optional[Dict[str, Any]] = Field(default=None, description="Result data if completed")


class EventListResponse(BaseModel):
    """Response for event listing."""

    model_config = ConfigDict()

    events: List[Dict[str, Any]] = Field(..., description="List of events")
    total: int = Field(..., description="Total count of events returned")


class HealthResponse(BaseModel):
    """Health check response."""

    model_config = ConfigDict()

    status: str = Field(default="healthy", description="Overall health status")
    service: str = Field(default="rmhdagmaster-gateway", description="Service name")
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Check timestamp",
    )
    version: str = Field(default="0.9.0", description="Service version")
    checks: Dict[str, bool] = Field(
        default_factory=dict,
        description="Individual health check results",
    )


class ProxyResponse(BaseModel):
    """Response from proxied request."""

    model_config = ConfigDict()

    success: bool = Field(..., description="Whether proxy request succeeded")
    target: str = Field(..., description="Target service")
    path: str = Field(..., description="Path that was requested")
    status_code: int = Field(..., description="HTTP status code from target")
    body: Optional[Any] = Field(default=None, description="Response body from target")
    error: Optional[str] = Field(default=None, description="Error message if failed")


class ErrorResponse(BaseModel):
    """Standard error response."""

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "error": "Job not found",
                "details": "No job with ID 'abc123' exists",
                "code": "NOT_FOUND",
            }
        }
    )

    error: str = Field(..., description="Error message")
    details: Optional[str] = Field(default=None, description="Additional details")
    code: Optional[str] = Field(default=None, description="Error code for programmatic handling")


__all__ = [
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
