# ============================================================================
# API SCHEMAS
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Core - Request/Response schemas
# PURPOSE: Pydantic models for API validation
# CREATED: 29 JAN 2026
# ============================================================================
"""
API Schemas

Request and response models for the API.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field

from core.contracts import JobStatus, NodeStatus, TaskStatus


# ============================================================================
# REQUEST SCHEMAS
# ============================================================================

class JobCreate(BaseModel):
    """Request to create a new job."""
    workflow_id: str = Field(..., max_length=64, description="Workflow to execute")
    input_params: Dict[str, Any] = Field(
        default_factory=dict,
        description="Input parameters for the workflow"
    )
    idempotency_key: Optional[str] = Field(
        None,
        max_length=128,
        description="Optional key for idempotent job creation"
    )
    correlation_id: Optional[str] = Field(
        None,
        max_length=64,
        description="External correlation ID for tracing"
    )

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "workflow_id": "raster_ingest",
                    "input_params": {
                        "blob_path": "uploads/image.tif",
                        "collection_id": "imagery"
                    }
                }
            ]
        }
    }


class TaskResultCreate(BaseModel):
    """Request to report task result (from worker callback)."""
    task_id: str = Field(..., max_length=128)
    job_id: str = Field(..., max_length=64)
    node_id: str = Field(..., max_length=64)
    status: TaskStatus
    output: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = Field(None, max_length=2000)
    worker_id: Optional[str] = Field(None, max_length=64)
    execution_duration_ms: Optional[int] = Field(None, ge=0)


# ============================================================================
# RESPONSE SCHEMAS
# ============================================================================

class NodeResponse(BaseModel):
    """Node state response."""
    node_id: str
    job_id: str
    status: NodeStatus
    task_id: Optional[str] = None
    output: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None
    retry_count: int = 0
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    model_config = {"from_attributes": True}


class JobResponse(BaseModel):
    """Job response."""
    job_id: str
    workflow_id: str
    status: JobStatus
    input_params: Dict[str, Any] = {}
    result_data: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    submitted_by: Optional[str] = None
    correlation_id: Optional[str] = None

    model_config = {"from_attributes": True}


class JobDetailResponse(BaseModel):
    """Detailed job response with nodes."""
    job: JobResponse
    nodes: List[NodeResponse]
    node_summary: Dict[str, int] = {}


class JobListResponse(BaseModel):
    """List of jobs response."""
    jobs: List[JobResponse]
    total: int


class WorkflowResponse(BaseModel):
    """Workflow definition response."""
    workflow_id: str
    name: str
    version: int
    description: Optional[str] = None
    is_active: bool = True


class WorkflowListResponse(BaseModel):
    """List of workflows response."""
    workflows: List[WorkflowResponse]


class HealthResponse(BaseModel):
    """
    Health check response.

    DEPRECATED: Health checks now use the health module.
    See /livez, /readyz, /health endpoints.
    This schema is kept for backwards compatibility.
    """
    status: str
    version: str
    orchestrator_running: bool
    database_connected: bool
    servicebus_connected: bool
    stats: Dict[str, Any] = {}


class ErrorResponse(BaseModel):
    """Error response."""
    error: str
    detail: Optional[str] = None
    job_id: Optional[str] = None
