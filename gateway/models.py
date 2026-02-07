# ============================================================================
# GATEWAY MODELS
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Gateway - Request/Response models for job submission
# PURPOSE: Pydantic models for the gateway API
# CREATED: 04 FEB 2026
# ============================================================================
"""
Gateway Models

Pydantic models for the job submission API.
These are separate from JobQueueMessage to allow the API contract
to evolve independently from the internal queue message format.
"""

from datetime import datetime
from typing import Any, Dict, Optional
from pydantic import BaseModel, Field


class JobSubmitRequest(BaseModel):
    """
    Request to submit a new job for execution.

    This is the external API contract. The gateway converts this
    to a JobQueueMessage for internal processing.
    """

    workflow_id: str = Field(
        ...,
        max_length=64,
        description="ID of the workflow to execute",
        examples=["raster_ingest", "vector_process"],
    )
    input_params: Dict[str, Any] = Field(
        default_factory=dict,
        description="Parameters passed to the workflow",
        examples=[{"container_name": "bronze", "blob_name": "data/file.tif"}],
    )
    correlation_id: Optional[str] = Field(
        default=None,
        max_length=64,
        description="External correlation ID for tracing across systems",
        examples=["ddh-req-12345"],
    )
    callback_url: Optional[str] = Field(
        default=None,
        max_length=512,
        description="URL to POST completion notification (webhook)",
        examples=["https://api.example.com/webhooks/job-complete"],
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
        description="Unique key to prevent duplicate job creation. "
                    "If a job with this key already exists, the existing job is returned.",
        examples=["import-abc123-2026-02-04"],
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "workflow_id": "raster_ingest",
                "input_params": {
                    "container_name": "bronze",
                    "blob_name": "data/satellite_image.tif",
                    "output_container": "silver",
                },
                "correlation_id": "ddh-req-12345",
                "priority": 0,
            }
        }
    }


class JobSubmitResponse(BaseModel):
    """
    Response after successful job submission.

    Note: The response returns message_id (Service Bus message ID), not job_id.
    The actual job_id is created by the orchestrator when it claims the message.
    Use correlation_id or idempotency_key for tracking if needed.
    """

    message_id: str = Field(
        ...,
        description="Service Bus message ID. Can be used to track the submission.",
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
        description="Submission status. 'queued' means the job is waiting for an orchestrator.",
    )
    correlation_id: Optional[str] = Field(
        default=None,
        description="Correlation ID if provided in request",
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "message_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
                "workflow_id": "raster_ingest",
                "submitted_at": "2026-02-04T10:30:00Z",
                "status": "queued",
                "correlation_id": "ddh-req-12345",
            }
        }
    }


class JobStatusResponse(BaseModel):
    """
    Response for job status queries.

    Used when querying job status by correlation_id or idempotency_key.
    """

    job_id: Optional[str] = Field(
        default=None,
        description="Job ID if job has been created by orchestrator",
    )
    workflow_id: str = Field(
        ...,
        description="Workflow being executed",
    )
    status: str = Field(
        ...,
        description="Job status: pending, running, completed, failed, cancelled",
    )
    created_at: Optional[datetime] = Field(
        default=None,
        description="When the job was created",
    )
    completed_at: Optional[datetime] = Field(
        default=None,
        description="When the job completed (if terminal)",
    )
    error_message: Optional[str] = Field(
        default=None,
        description="Error message if job failed",
    )
    result_data: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Job result data if completed",
    )


class HealthResponse(BaseModel):
    """Health check response."""

    status: str = Field(default="healthy")
    service: str = Field(default="rmhgeogateway")
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    version: str = Field(default="0.9.0")


# ============================================================================
# EXPORTS
# ============================================================================

__all__ = [
    "JobSubmitRequest",
    "JobSubmitResponse",
    "JobStatusResponse",
    "HealthResponse",
]
