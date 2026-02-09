# ============================================================================
# CLAUDE CONTEXT - NODE STATE MODEL
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Core model - Node runtime state
# PURPOSE: Track state of each node within a job execution
# LAST_REVIEWED: 02 FEB 2026
# EXPORTS: NodeState
# DEPENDENCIES: pydantic
# ============================================================================
"""
Node State Model

NodeState tracks the runtime state of a single node within a job.

Key concept:
- WorkflowDefinition.NodeDefinition = TEMPLATE (what to do)
- NodeState = INSTANCE (runtime state for one job)

Each Job creates N NodeState records (one per node in workflow).
The orchestrator updates these as nodes progress.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional, ClassVar
from pydantic import BaseModel, Field, computed_field

from core.contracts import NodeData, NodeStatus


class NodeState(NodeData):
    """
    Runtime state of a node within a job execution.

    Maps to: app.dag_node_states table
    Primary Key: (job_id, node_id)

    Lifecycle:
        1. Created with status=PENDING when job starts
        2. Transitions to READY when dependencies are met
        3. Transitions to DISPATCHED when sent to queue
        4. Transitions to RUNNING when worker picks it up
        5. Transitions to COMPLETED/FAILED when worker reports
        6. May transition to SKIPPED if conditional branch not taken
    """

    # =========================================================================
    # SQL DDL METADATA (Used by PydanticToSQL generator)
    # =========================================================================
    __sql_table__: ClassVar[str] = "dag_node_states"
    __sql_schema__: ClassVar[str] = "dagapp"
    __sql_primary_key__: ClassVar[List[str]] = ["job_id", "node_id"]  # Composite PK
    __sql_foreign_keys__: ClassVar[Dict[str, str]] = {
        "job_id": "dagapp.dag_jobs(job_id)"
    }
    __sql_indexes__: ClassVar[List[tuple]] = [
        ("idx_dag_node_status", ["status"]),
        ("idx_dag_node_job", ["job_id"]),
        ("idx_dag_node_task", ["task_id"], "task_id IS NOT NULL"),
        ("idx_dag_node_parent", ["parent_node_id"], "parent_node_id IS NOT NULL"),
    ]

    # Status
    status: NodeStatus = Field(default=NodeStatus.PENDING)

    # Task linkage (set when dispatched)
    task_id: Optional[str] = Field(
        default=None,
        max_length=128,
        description="Task ID of dispatched work"
    )

    # Output (set on completion)
    output: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Result data from handler execution"
    )
    error_message: Optional[str] = Field(
        default=None,
        max_length=2000,
        description="Error details if failed"
    )

    # Retry tracking
    retry_count: int = Field(default=0, ge=0)
    max_retries: int = Field(default=3, ge=0)

    # Timestamps
    created_at: datetime = Field(default_factory=datetime.utcnow)
    dispatched_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    # Optimistic locking
    version: int = Field(
        default=1,
        ge=1,
        description="Version for optimistic locking - incremented on each update"
    )

    # For fan-out: track parent node and index
    parent_node_id: Optional[str] = Field(
        default=None,
        max_length=64,
        description="For dynamic nodes, the fan-out node that created this"
    )
    fan_out_index: Optional[int] = Field(
        default=None,
        description="Index within fan-out (0, 1, 2, ...)"
    )
    input_params: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Resolved params stored at creation time (for dynamic node retries)"
    )

    # Checkpoint support (for resumable long-running tasks)
    checkpoint_phase: Optional[str] = Field(
        default=None,
        max_length=64,
        description="Current phase name for checkpoint resume"
    )
    checkpoint_data: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Checkpoint state data for resume"
    )
    checkpoint_saved_at: Optional[datetime] = Field(
        default=None,
        description="When checkpoint was last saved"
    )

    @computed_field
    @property
    def is_terminal(self) -> bool:
        """Check if node is in a terminal state."""
        return self.status.is_terminal()

    @computed_field
    @property
    def is_successful(self) -> bool:
        """Check if node completed successfully."""
        return self.status.is_successful()

    @computed_field
    @property
    def is_dynamic(self) -> bool:
        """Check if this is a dynamically created node (from fan-out)."""
        return self.parent_node_id is not None

    @computed_field
    @property
    def execution_duration_seconds(self) -> Optional[float]:
        """Calculate execution duration if started."""
        if not self.started_at:
            return None
        end_time = self.completed_at or datetime.utcnow()
        return (end_time - self.started_at).total_seconds()

    def can_transition_to(self, new_status: NodeStatus) -> bool:
        """
        Validate if a status transition is allowed.

        Valid transitions:
            PENDING -> READY, SKIPPED
            READY -> DISPATCHED, FAILED (template error), SKIPPED
            DISPATCHED -> RUNNING, FAILED (timeout)
            RUNNING -> COMPLETED, FAILED
            COMPLETED, FAILED, SKIPPED -> (none, terminal)

        Note: FAILED can retry back to READY if retry_count < max_retries
        """
        if self.status == new_status:
            return True

        allowed = {
            NodeStatus.PENDING: {NodeStatus.READY, NodeStatus.SKIPPED},
            NodeStatus.READY: {NodeStatus.DISPATCHED, NodeStatus.FAILED, NodeStatus.SKIPPED},
            NodeStatus.DISPATCHED: {NodeStatus.RUNNING, NodeStatus.FAILED},
            NodeStatus.RUNNING: {NodeStatus.COMPLETED, NodeStatus.FAILED},
            NodeStatus.COMPLETED: set(),
            NodeStatus.FAILED: {NodeStatus.READY} if self.retry_count < self.max_retries else set(),
            NodeStatus.SKIPPED: set(),
        }

        return new_status in allowed.get(self.status, set())

    def mark_ready(self) -> None:
        """Mark node as ready (dependencies met)."""
        if not self.can_transition_to(NodeStatus.READY):
            raise ValueError(f"Cannot transition from {self.status} to READY")
        self.status = NodeStatus.READY
        self.updated_at = datetime.utcnow()

    def mark_dispatched(self, task_id: str) -> None:
        """Mark node as dispatched (sent to queue)."""
        if not self.can_transition_to(NodeStatus.DISPATCHED):
            raise ValueError(f"Cannot transition from {self.status} to DISPATCHED")
        self.status = NodeStatus.DISPATCHED
        self.task_id = task_id
        self.dispatched_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()

    def mark_running(self) -> None:
        """Mark node as running (worker picked up task)."""
        if not self.can_transition_to(NodeStatus.RUNNING):
            raise ValueError(f"Cannot transition from {self.status} to RUNNING")
        self.status = NodeStatus.RUNNING
        self.started_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()

    def mark_completed(self, output: Optional[Dict[str, Any]] = None) -> None:
        """Mark node as completed successfully."""
        if not self.can_transition_to(NodeStatus.COMPLETED):
            raise ValueError(f"Cannot transition from {self.status} to COMPLETED")
        self.status = NodeStatus.COMPLETED
        self.output = output
        self.completed_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()

    def mark_failed(self, error_message: str) -> None:
        """Mark node as failed."""
        if not self.can_transition_to(NodeStatus.FAILED):
            raise ValueError(f"Cannot transition from {self.status} to FAILED")
        self.status = NodeStatus.FAILED
        self.error_message = error_message[:2000]
        self.completed_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()

    def mark_skipped(self) -> None:
        """Mark node as skipped (branch not taken)."""
        if not self.can_transition_to(NodeStatus.SKIPPED):
            raise ValueError(f"Cannot transition from {self.status} to SKIPPED")
        self.status = NodeStatus.SKIPPED
        self.completed_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()

    def prepare_retry(self) -> bool:
        """
        Prepare node for retry after failure.

        Returns True if retry is allowed, False if max retries exceeded.
        """
        if self.status != NodeStatus.FAILED:
            return False
        if self.retry_count >= self.max_retries:
            return False

        self.retry_count += 1
        self.status = NodeStatus.READY
        self.task_id = None
        self.dispatched_at = None
        self.started_at = None
        self.completed_at = None
        self.error_message = None
        self.updated_at = datetime.utcnow()
        return True


# ============================================================================
# EXPORTS
# ============================================================================

__all__ = ["NodeState"]
