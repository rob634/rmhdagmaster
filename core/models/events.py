# ============================================================================
# CLAUDE CONTEXT - EVENT TRACKING MODEL
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Core model - Execution timeline events
# PURPOSE: Track execution milestones for debugging/auditing
# LAST_REVIEWED: 28 JAN 2026
# EXPORTS: JobEvent, EventType, EventStatus
# DEPENDENCIES: pydantic, enum
# ============================================================================
"""
Event Tracking Model

JobEvent records execution milestones for debugging and auditing.

Carried forward from Epoch 4 (was very useful).
Enables "last successful checkpoint" debugging for silent failures.
"""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, ClassVar
from pydantic import BaseModel, Field


class EventType(str, Enum):
    """Types of events that can occur during job execution."""

    # Job lifecycle
    JOB_CREATED = "job_created"
    JOB_STARTED = "job_started"
    JOB_COMPLETED = "job_completed"
    JOB_FAILED = "job_failed"
    JOB_CANCELLED = "job_cancelled"

    # Node lifecycle
    NODE_READY = "node_ready"
    NODE_DISPATCHED = "node_dispatched"
    NODE_STARTED = "node_started"
    NODE_COMPLETED = "node_completed"
    NODE_FAILED = "node_failed"
    NODE_SKIPPED = "node_skipped"
    NODE_RETRY = "node_retry"
    NODE_TIMEOUT = "node_timeout"

    # Task lifecycle (worker-side)
    TASK_RECEIVED = "task_received"
    TASK_COMPLETED = "task_completed"
    TASK_FAILED = "task_failed"

    # Orchestrator events
    ORCHESTRATOR_EVAL = "orchestrator_eval"      # DAG evaluation cycle
    ORCHESTRATOR_DISPATCH = "orchestrator_dispatch"

    # System events
    CHECKPOINT = "checkpoint"
    WARNING = "warning"
    ERROR = "error"


class EventStatus(str, Enum):
    """Status/severity of an event."""
    SUCCESS = "success"
    FAILURE = "failure"
    WARNING = "warning"
    INFO = "info"


class JobEvent(BaseModel):
    """
    A single event in the job execution timeline.

    Maps to: app.dag_job_events table

    Use cases:
    - Debug silent failures ("what was the last thing that worked?")
    - Audit trail for compliance
    - Performance analysis (where is time spent?)
    - Correlation with Application Insights
    """

    # =========================================================================
    # SQL DDL METADATA (Used by PydanticToSQL generator)
    # =========================================================================
    __sql_table__: ClassVar[str] = "dag_job_events"
    __sql_schema__: ClassVar[str] = "dagapp"
    __sql_primary_key__: ClassVar[List[str]] = ["event_id"]
    __sql_serial_columns__: ClassVar[List[str]] = ["event_id"]
    __sql_foreign_keys__: ClassVar[Dict[str, str]] = {
        "job_id": "dagapp.dag_jobs(job_id)"
    }
    __sql_indexes__: ClassVar[List[tuple]] = [
        ("idx_dag_events_job", ["job_id"]),
        ("idx_dag_events_job_created", ["job_id", "created_at"]),
        ("idx_dag_events_node", ["node_id"], "node_id IS NOT NULL"),
        ("idx_dag_events_task", ["task_id"], "task_id IS NOT NULL"),
        ("idx_dag_events_type", ["event_type"]),
        ("idx_dag_events_checkpoint", ["checkpoint_name"], "checkpoint_name IS NOT NULL"),
    ]

    # Identity
    event_id: Optional[int] = Field(
        default=None,
        description="Auto-increment primary key (SERIAL)"
    )
    job_id: str = Field(..., max_length=64)
    node_id: Optional[str] = Field(
        default=None,
        max_length=64,
        description="Node ID if node-specific event"
    )
    task_id: Optional[str] = Field(
        default=None,
        max_length=128,
        description="Task ID if task-specific event"
    )

    # Event details
    event_type: EventType
    event_status: EventStatus = Field(default=EventStatus.INFO)

    # Checkpoint for Application Insights correlation
    checkpoint_name: Optional[str] = Field(
        default=None,
        max_length=64,
        description="Named checkpoint for AI query correlation"
    )

    # Flexible data payload
    event_data: Dict[str, Any] = Field(
        default_factory=dict,
        description="Additional event-specific data (JSONB)"
    )

    # Error details
    error_message: Optional[str] = Field(
        default=None,
        max_length=2000
    )

    # Timing
    duration_ms: Optional[int] = Field(
        default=None,
        ge=0,
        description="Duration of operation if applicable"
    )
    created_at: datetime = Field(default_factory=datetime.utcnow)

    # Source tracking
    source_app: Optional[str] = Field(
        default=None,
        max_length=64,
        description="App that generated this event (orchestrator, worker, etc.)"
    )

    # ========================================================================
    # Factory Methods
    # ========================================================================

    @classmethod
    def job_event(
        cls,
        job_id: str,
        event_type: EventType,
        status: EventStatus = EventStatus.INFO,
        checkpoint_name: Optional[str] = None,
        event_data: Optional[Dict[str, Any]] = None,
        error_message: Optional[str] = None,
        source_app: Optional[str] = None,
    ) -> "JobEvent":
        """Create a job-level event."""
        return cls(
            job_id=job_id,
            event_type=event_type,
            event_status=status,
            checkpoint_name=checkpoint_name,
            event_data=event_data or {},
            error_message=error_message,
            source_app=source_app,
        )

    @classmethod
    def node_event(
        cls,
        job_id: str,
        node_id: str,
        event_type: EventType,
        status: EventStatus = EventStatus.INFO,
        task_id: Optional[str] = None,
        checkpoint_name: Optional[str] = None,
        event_data: Optional[Dict[str, Any]] = None,
        error_message: Optional[str] = None,
        duration_ms: Optional[int] = None,
        source_app: Optional[str] = None,
    ) -> "JobEvent":
        """Create a node-level event."""
        return cls(
            job_id=job_id,
            node_id=node_id,
            task_id=task_id,
            event_type=event_type,
            event_status=status,
            checkpoint_name=checkpoint_name,
            event_data=event_data or {},
            error_message=error_message,
            duration_ms=duration_ms,
            source_app=source_app,
        )

    @classmethod
    def task_event(
        cls,
        job_id: str,
        node_id: str,
        task_id: str,
        event_type: EventType,
        status: EventStatus = EventStatus.INFO,
        event_data: Optional[Dict[str, Any]] = None,
        error_message: Optional[str] = None,
        duration_ms: Optional[int] = None,
        source_app: Optional[str] = None,
    ) -> "JobEvent":
        """Create a task-level event."""
        return cls(
            job_id=job_id,
            node_id=node_id,
            task_id=task_id,
            event_type=event_type,
            event_status=status,
            event_data=event_data or {},
            error_message=error_message,
            duration_ms=duration_ms,
            source_app=source_app,
        )


# ============================================================================
# EXPORTS
# ============================================================================

__all__ = ["JobEvent", "EventType", "EventStatus"]
