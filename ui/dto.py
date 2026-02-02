# ============================================================================
# UI DATA TRANSFER OBJECTS
# ============================================================================
# EPOCH: 4/5 - DAG PORTABLE
# STATUS: Core - Stable DTOs for UI components
# PURPOSE: Define stable interfaces that work across Epoch 4 and DAG
# CREATED: 29 JAN 2026
# ============================================================================
"""
UI Data Transfer Objects.

These DTOs provide a stable interface for UI components. They are designed
to work with both Epoch 4 (current) and Epoch 5 (DAG orchestrator) data models.

Design Principles:
    1. DTOs use GENERIC field names that map to both epochs
    2. Optional fields allow gradual adoption of DAG features
    3. Status enums are UI-friendly (lowercase, no underscores in display)
    4. All timestamps are datetime objects (templates handle formatting)

Mapping Examples:
    Epoch 4                    DTO Field              DAG (Epoch 5)
    ─────────────────────────────────────────────────────────────────
    job.job_type        →     job.workflow_id    ←   job.workflow_id
    job.stage           →     job.current_step   ←   (derived from nodes)
    job.total_stages    →     job.total_steps    ←   len(workflow.nodes)
    task (in stage)     →     node               ←   node_state
    task.task_type      →     node.handler       ←   node.handler

Usage:
    from ui.dto import JobDTO, NodeDTO

    # Templates use DTO fields directly
    {{ job.workflow_id }}
    {{ job.current_step }} of {{ job.total_steps }}

    {% for node in nodes %}
        {{ node.node_id }}: {{ node.status }}
    {% endfor %}
"""

from datetime import datetime
from enum import Enum
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field, ConfigDict


# ============================================================================
# STATUS ENUMS (UI-friendly)
# ============================================================================

class JobStatusDTO(str, Enum):
    """
    UI-friendly job status.

    Maps from:
        Epoch 4: JobStatus (queued, processing, completed, failed, completed_with_errors)
        DAG: JobStatus (pending, running, completed, failed, cancelled)
    """
    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    COMPLETED_WITH_ERRORS = "completed_with_errors"

    @property
    def display(self) -> str:
        """Human-readable display name."""
        return self.value.replace("_", " ").title()

    @property
    def css_class(self) -> str:
        """CSS class for styling."""
        return {
            "pending": "warning",
            "queued": "info",
            "running": "primary",
            "completed": "success",
            "failed": "danger",
            "cancelled": "secondary",
            "completed_with_errors": "warning",
        }.get(self.value, "secondary")

    @property
    def is_terminal(self) -> bool:
        """Check if this is a terminal state."""
        return self in (
            JobStatusDTO.COMPLETED,
            JobStatusDTO.FAILED,
            JobStatusDTO.CANCELLED,
            JobStatusDTO.COMPLETED_WITH_ERRORS,
        )


class NodeStatusDTO(str, Enum):
    """
    UI-friendly node/step status.

    Maps from:
        Epoch 4: StageStatus or derived from tasks
        DAG: NodeStatus (pending, ready, dispatched, running, completed, failed, skipped)
    """
    PENDING = "pending"
    READY = "ready"
    DISPATCHED = "dispatched"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"

    @property
    def display(self) -> str:
        return self.value.title()

    @property
    def css_class(self) -> str:
        return {
            "pending": "secondary",
            "ready": "info",
            "dispatched": "info",
            "running": "primary",
            "completed": "success",
            "failed": "danger",
            "skipped": "secondary",
        }.get(self.value, "secondary")

    @property
    def is_terminal(self) -> bool:
        return self in (
            NodeStatusDTO.COMPLETED,
            NodeStatusDTO.FAILED,
            NodeStatusDTO.SKIPPED,
        )


class TaskStatusDTO(str, Enum):
    """
    UI-friendly task status.

    Maps from:
        Epoch 4: TaskStatus
        DAG: TaskStatus
    """
    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"
    CANCELLED = "cancelled"

    @property
    def display(self) -> str:
        return self.value.title()

    @property
    def css_class(self) -> str:
        return {
            "pending": "secondary",
            "queued": "info",
            "running": "primary",
            "completed": "success",
            "failed": "danger",
            "retrying": "warning",
            "cancelled": "secondary",
        }.get(self.value, "secondary")


class ApprovalStateDTO(str, Enum):
    """UI-friendly approval state."""
    PENDING_REVIEW = "pending_review"
    APPROVED = "approved"
    REJECTED = "rejected"

    @property
    def display(self) -> str:
        return self.value.replace("_", " ").title()

    @property
    def css_class(self) -> str:
        return {
            "pending_review": "warning",
            "approved": "success",
            "rejected": "danger",
        }.get(self.value, "secondary")


class ClearanceStateDTO(str, Enum):
    """UI-friendly clearance state."""
    UNCLEARED = "uncleared"
    OUO = "ouo"
    PUBLIC = "public"

    @property
    def display(self) -> str:
        return {
            "uncleared": "Uncleared",
            "ouo": "OUO (Internal)",
            "public": "Public",
        }.get(self.value, self.value.title())

    @property
    def css_class(self) -> str:
        return {
            "uncleared": "secondary",
            "ouo": "info",
            "public": "success",
        }.get(self.value, "secondary")


class ProcessingStatusDTO(str, Enum):
    """UI-friendly processing status (V0.8 / DAG)."""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"

    @property
    def display(self) -> str:
        return self.value.title()

    @property
    def css_class(self) -> str:
        return {
            "pending": "warning",
            "processing": "primary",
            "completed": "success",
            "failed": "danger",
        }.get(self.value, "secondary")


# ============================================================================
# DATA TRANSFER OBJECTS
# ============================================================================

class JobDTO(BaseModel):
    """
    UI representation of a job.

    This is the stable interface for job data in templates.
    Works with both Epoch 4 JobRecord and DAG Job models.

    Field Mappings:
        workflow_id: Epoch 4 job_type, DAG workflow_id
        current_step: Epoch 4 stage, DAG derived from node states
        total_steps: Epoch 4 total_stages, DAG len(workflow.nodes)
    """
    model_config = ConfigDict(use_enum_values=True)

    # Identity
    job_id: str
    workflow_id: str  # Generic name for job_type (E4) / workflow_id (DAG)

    # Status
    status: JobStatusDTO

    # Progress (works for both Epoch 4 stages and DAG nodes)
    current_step: int = 1
    total_steps: int = 1
    current_step_name: Optional[str] = None  # DAG: current node_id

    # DAG-specific progress (optional, ignored in Epoch 4)
    completed_nodes: Optional[int] = None
    failed_nodes: Optional[int] = None
    node_summary: Optional[Dict[str, Any]] = None

    # Results
    result_data: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None

    # Asset linkage (V0.8+)
    asset_id: Optional[str] = None

    # Timestamps
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    # Metadata
    parameters: Dict[str, Any] = Field(default_factory=dict)
    metadata: Dict[str, Any] = Field(default_factory=dict)

    @property
    def progress_percent(self) -> int:
        """Calculate progress percentage."""
        if self.total_steps == 0:
            return 0
        # For DAG, use completed_nodes if available
        if self.completed_nodes is not None and self.total_steps > 0:
            return int((self.completed_nodes / self.total_steps) * 100)
        # For Epoch 4, use current_step
        return int((self.current_step / self.total_steps) * 100)

    @property
    def is_terminal(self) -> bool:
        """Check if job is in terminal state."""
        return JobStatusDTO(self.status).is_terminal

    @property
    def duration_seconds(self) -> Optional[float]:
        """Calculate job duration."""
        if not self.created_at:
            return None
        end = self.completed_at or datetime.now()
        return (end - self.created_at).total_seconds()


class NodeDTO(BaseModel):
    """
    UI representation of a workflow step/node.

    Maps from:
        Epoch 4: Derived from tasks in a stage, or StageStatus
        DAG: NodeState

    In Epoch 4, "nodes" are stages. In DAG, they are actual DAG nodes.
    The UI treats them identically.
    """
    model_config = ConfigDict(use_enum_values=True)

    # Identity
    node_id: str  # Epoch 4: "stage_1", DAG: actual node_id
    job_id: str

    # Definition (from workflow)
    handler: Optional[str] = None  # Epoch 4: task_type, DAG: handler
    description: Optional[str] = None

    # Status
    status: NodeStatusDTO

    # Execution
    task_id: Optional[str] = None  # Link to dispatched task
    retry_count: int = 0
    max_retries: int = 3

    # Results
    output: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None

    # Timestamps
    created_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    # Fan-out tracking (DAG-specific)
    parent_node_id: Optional[str] = None
    fan_out_index: Optional[int] = None
    is_dynamic: bool = False

    @property
    def is_terminal(self) -> bool:
        return NodeStatusDTO(self.status).is_terminal

    @property
    def duration_seconds(self) -> Optional[float]:
        if not self.started_at:
            return None
        end = self.completed_at or datetime.now()
        return (end - self.started_at).total_seconds()


class TaskDTO(BaseModel):
    """
    UI representation of a task.

    A task is the unit of work dispatched to a worker queue.
    In Epoch 4, tasks are grouped by stage.
    In DAG, each node dispatches one task (except fan-out).
    """
    model_config = ConfigDict(use_enum_values=True)

    # Identity
    task_id: str
    job_id: str
    node_id: Optional[str] = None  # DAG: node that spawned this task

    # Definition
    task_type: str  # Handler name
    stage: int = 1
    task_index: int = 0

    # Status
    status: TaskStatusDTO
    retry_count: int = 0

    # Routing
    target_queue: Optional[str] = None
    executed_by_app: Optional[str] = None

    # Results
    result_data: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None

    # Checkpoint (for long-running Docker tasks)
    checkpoint_phase: Optional[int] = None
    checkpoint_data: Optional[Dict[str, Any]] = None

    # Timestamps
    created_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    last_pulse: Optional[datetime] = None

    # Parameters
    parameters: Dict[str, Any] = Field(default_factory=dict)

    @property
    def duration_seconds(self) -> Optional[float]:
        if not self.started_at:
            return None
        end = self.completed_at or datetime.now()
        return (end - self.started_at).total_seconds()


class AssetDTO(BaseModel):
    """
    UI representation of a GeospatialAsset (V0.8+).

    Links external identifiers to internal services with full state tracking.
    """
    model_config = ConfigDict(use_enum_values=True)

    # Identity
    asset_id: str
    dataset_id: str
    resource_id: str
    version_id: str

    # Type
    data_type: str  # "raster" or "vector"

    # Service outputs
    table_name: Optional[str] = None  # Vector: geo.{name}
    blob_path: Optional[str] = None   # Raster: silver-cogs/{path}
    stac_item_id: Optional[str] = None
    stac_collection_id: Optional[str] = None

    # Four state dimensions
    revision: int = 1
    approval_state: Optional[ApprovalStateDTO] = None
    clearance_state: Optional[ClearanceStateDTO] = None
    processing_status: Optional[ProcessingStatusDTO] = None

    # Job linkage
    current_job_id: Optional[str] = None
    job_count: int = 0
    workflow_id: Optional[str] = None

    # Approval details
    reviewer: Optional[str] = None
    reviewed_at: Optional[datetime] = None
    rejection_reason: Optional[str] = None

    # Processing details (DAG)
    processing_started_at: Optional[datetime] = None
    processing_completed_at: Optional[datetime] = None
    last_error: Optional[str] = None
    node_summary: Optional[Dict[str, Any]] = None

    # Soft delete
    deleted_at: Optional[datetime] = None
    deleted_by: Optional[str] = None

    # Timestamps
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    @property
    def is_deleted(self) -> bool:
        return self.deleted_at is not None

    @property
    def is_approved(self) -> bool:
        return self.approval_state == ApprovalStateDTO.APPROVED

    @property
    def is_public(self) -> bool:
        return self.clearance_state == ClearanceStateDTO.PUBLIC

    @property
    def is_processing(self) -> bool:
        return self.processing_status == ProcessingStatusDTO.PROCESSING


class JobEventDTO(BaseModel):
    """
    UI representation of a job event (audit trail).
    """
    model_config = ConfigDict(use_enum_values=True)

    # Identity
    event_id: Optional[int] = None
    job_id: str
    node_id: Optional[str] = None
    task_id: Optional[str] = None

    # Event details
    event_type: str
    event_status: str = "info"
    checkpoint_name: Optional[str] = None

    # Data
    event_data: Dict[str, Any] = Field(default_factory=dict)
    error_message: Optional[str] = None
    duration_ms: Optional[int] = None

    # Source
    source_app: Optional[str] = None

    # Timestamp
    created_at: Optional[datetime] = None

    @property
    def css_class(self) -> str:
        """CSS class based on event status."""
        return {
            "success": "success",
            "failure": "danger",
            "warning": "warning",
            "info": "info",
        }.get(self.event_status, "secondary")
