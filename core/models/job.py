# ============================================================================
# CLAUDE CONTEXT - JOB MODEL
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Core model - Job instance (workflow execution)
# PURPOSE: Track one execution of a workflow
# LAST_REVIEWED: 02 FEB 2026
# EXPORTS: Job
# DEPENDENCIES: pydantic
# ============================================================================
"""
Job Model

A Job represents one execution of a workflow (one "instance").

Key differences from Epoch 4:
- NO stages - progress tracked via NodeState
- NO "total_stages" - DAG structure determines completion
- NO stage_results - each node has its own output in NodeState

The orchestrator creates a Job when a workflow is submitted,
then creates NodeState records for each node in the workflow.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional, ClassVar
from pydantic import BaseModel, Field, computed_field

from core.contracts import JobData, JobStatus


class Job(JobData):
    """
    A job instance - one execution of a workflow.

    Maps to: app.dag_jobs table

    Lifecycle:
        1. Created with status=PENDING when workflow submitted
        2. Transitions to RUNNING when first node dispatched
        3. Transitions to COMPLETED when END node reached
        4. Transitions to FAILED if any node fails (and no retry/recovery)
    """

    # =========================================================================
    # SQL DDL METADATA (Used by PydanticToSQL generator)
    # =========================================================================
    __sql_table__: ClassVar[str] = "dag_jobs"
    __sql_schema__: ClassVar[str] = "dagapp"
    __sql_primary_key__: ClassVar[List[str]] = ["job_id"]
    __sql_foreign_keys__: ClassVar[Dict[str, str]] = {}
    __sql_indexes__: ClassVar[List[tuple]] = [
        ("idx_dag_jobs_status", ["status"]),
        ("idx_dag_jobs_workflow", ["workflow_id"]),
        ("idx_dag_jobs_created", ["created_at"]),
        # Multi-orchestrator indexes
        ("idx_dag_jobs_owner", ["owner_id"]),
        ("idx_dag_jobs_heartbeat", ["owner_heartbeat_at"]),
        # Asset tracing
        ("idx_dag_jobs_asset", ["asset_id"]),
    ]

    # Status
    status: JobStatus = Field(default=JobStatus.PENDING)

    # Input parameters (passed to workflow)
    input_params: Dict[str, Any] = Field(
        default_factory=dict,
        description="Input parameters for this job execution"
    )

    # Final results (populated on completion)
    result_data: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Final job output (aggregated from terminal nodes)"
    )
    error_message: Optional[str] = Field(
        default=None,
        max_length=2000,
        description="Error details if failed"
    )

    # Metadata
    metadata: Dict[str, Any] = Field(
        default_factory=dict,
        description="Additional job metadata (source, tags, etc.)"
    )

    # Workflow version pinning (captured at job creation, immutable)
    workflow_version: int = Field(
        ...,
        ge=1,
        description="Workflow version pinned at job creation time"
    )
    workflow_snapshot: Dict[str, Any] = Field(
        ...,
        description="Serialized WorkflowDefinition captured at job creation (immutable)"
    )

    # Timestamps
    created_at: datetime = Field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = Field(
        default=None,
        description="When first node was dispatched"
    )
    completed_at: Optional[datetime] = Field(
        default=None,
        description="When job reached terminal state"
    )
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    # Optimistic locking
    version: int = Field(
        default=1,
        ge=1,
        description="Version for optimistic locking - incremented on each update"
    )

    # Tracking (for debugging/correlation)
    submitted_by: Optional[str] = Field(
        default=None,
        max_length=64,
        description="User or system that submitted the job"
    )
    correlation_id: Optional[str] = Field(
        default=None,
        max_length=64,
        description="External correlation ID for tracing"
    )

    # Asset tracing (nullable - standalone jobs have no asset)
    asset_id: Optional[str] = Field(
        default=None,
        max_length=64,
        description="Geospatial asset ID this job processes (NULL for standalone jobs)"
    )

    # Multi-orchestrator ownership
    owner_id: Optional[str] = Field(
        default=None,
        max_length=64,
        description="Orchestrator instance ID that owns this job"
    )
    owner_heartbeat_at: Optional[datetime] = Field(
        default=None,
        description="Last heartbeat from owning orchestrator"
    )

    @computed_field
    @property
    def is_terminal(self) -> bool:
        """Check if job is in a terminal state."""
        return self.status.is_terminal()

    @computed_field
    @property
    def duration_seconds(self) -> Optional[float]:
        """Calculate job duration if started."""
        if not self.started_at:
            return None
        end_time = self.completed_at or datetime.utcnow()
        return (end_time - self.started_at).total_seconds()

    def get_pinned_workflow(self) -> "WorkflowDefinition":
        """Deserialize the pinned workflow snapshot."""
        from core.models.workflow import WorkflowDefinition
        return WorkflowDefinition.model_validate(self.workflow_snapshot)

    def can_transition_to(self, new_status: JobStatus) -> bool:
        """
        Validate if a status transition is allowed.

        Valid transitions:
            PENDING -> RUNNING, CANCELLED
            RUNNING -> COMPLETED, FAILED, CANCELLED
            COMPLETED, FAILED, CANCELLED -> (none, terminal)
        """
        if self.status == new_status:
            return True  # No-op is always allowed

        allowed = {
            JobStatus.PENDING: {JobStatus.RUNNING, JobStatus.CANCELLED},
            JobStatus.RUNNING: {JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED},
            JobStatus.COMPLETED: set(),
            JobStatus.FAILED: set(),
            JobStatus.CANCELLED: set(),
        }

        return new_status in allowed.get(self.status, set())

    def mark_started(self) -> None:
        """Mark job as started (first node dispatched)."""
        if self.status == JobStatus.PENDING:
            self.status = JobStatus.RUNNING
            self.started_at = datetime.utcnow()
            self.updated_at = datetime.utcnow()

    def mark_completed(self, result_data: Optional[Dict[str, Any]] = None) -> None:
        """Mark job as successfully completed."""
        if not self.can_transition_to(JobStatus.COMPLETED):
            raise ValueError(f"Cannot transition from {self.status} to COMPLETED")
        self.status = JobStatus.COMPLETED
        self.completed_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()
        if result_data:
            self.result_data = result_data

    def mark_failed(self, error_message: str) -> None:
        """Mark job as failed."""
        if not self.can_transition_to(JobStatus.FAILED):
            raise ValueError(f"Cannot transition from {self.status} to FAILED")
        self.status = JobStatus.FAILED
        self.error_message = error_message[:2000]  # Truncate if needed
        self.completed_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()

    def mark_cancelled(self) -> None:
        """Mark job as cancelled."""
        if not self.can_transition_to(JobStatus.CANCELLED):
            raise ValueError(f"Cannot transition from {self.status} to CANCELLED")
        self.status = JobStatus.CANCELLED
        self.completed_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()


# ============================================================================
# EXPORTS
# ============================================================================

__all__ = ["Job"]
