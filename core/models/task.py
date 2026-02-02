# ============================================================================
# CLAUDE CONTEXT - TASK MODELS
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Core model - Task message and result
# PURPOSE: Define what gets sent to workers and what comes back
# LAST_REVIEWED: 28 JAN 2026
# EXPORTS: TaskMessage, TaskResult
# DEPENDENCIES: pydantic
# ============================================================================
"""
Task Models

Task = a unit of work dispatched to a worker.

Two models:
- TaskMessage: What the orchestrator sends to Service Bus
- TaskResult: What the worker writes back to the database

Key insight: Tasks are DUMB. They don't know about:
- The overall DAG structure
- What runs next
- Whether they're "last"

They just execute and report.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional, ClassVar
from pydantic import BaseModel, Field

from core.contracts import TaskData, TaskStatus


class TaskMessage(TaskData):
    """
    Message sent to Service Bus for worker execution.

    This is what goes ON the queue.
    Workers receive this, execute the handler, and report back.

    Compared to Epoch 4 TaskQueueMessage:
    - REMOVED: stage (no stages in DAG)
    - REMOVED: task_type (renamed to handler for clarity)
    - ADDED: node_id (which node in the DAG)
    - RENAMED: parameters -> params (shorter)
    """

    # Execution parameters
    params: Dict[str, Any] = Field(
        default_factory=dict,
        description="Parameters passed to handler (templates already resolved)"
    )

    # Timeout (worker should respect this)
    timeout_seconds: int = Field(
        default=3600,
        ge=1,
        le=86400,
        description="Max execution time before considered failed"
    )

    # Dispatch metadata
    dispatched_at: datetime = Field(default_factory=datetime.utcnow)
    retry_count: int = Field(default=0, ge=0, le=10)

    # Correlation (for tracing)
    correlation_id: Optional[str] = Field(
        default=None,
        max_length=64,
        description="External correlation ID"
    )

    def to_queue_message(self) -> Dict[str, Any]:
        """
        Serialize to JSON for Service Bus.

        Returns dict ready for json.dumps().
        """
        return {
            "task_id": self.task_id,
            "job_id": self.job_id,
            "node_id": self.node_id,
            "handler": self.handler,
            "params": self.params,
            "timeout_seconds": self.timeout_seconds,
            "dispatched_at": self.dispatched_at.isoformat(),
            "retry_count": self.retry_count,
            "correlation_id": self.correlation_id,
        }

    @classmethod
    def from_queue_message(cls, data: Dict[str, Any]) -> "TaskMessage":
        """
        Deserialize from Service Bus message.
        """
        # Handle datetime parsing
        if isinstance(data.get("dispatched_at"), str):
            data["dispatched_at"] = datetime.fromisoformat(data["dispatched_at"])
        return cls(**data)


class TaskResult(BaseModel):
    """
    Result reported by worker after task execution.

    Workers write this to app.dag_task_results table.
    Orchestrator polls this table for completions.

    This is SEPARATE from NodeState.output because:
    - Worker writes TaskResult immediately on completion
    - Orchestrator reads TaskResult, then updates NodeState
    - Decouples worker from orchestrator (worker doesn't need to know schema)
    """

    # =========================================================================
    # SQL DDL METADATA (Used by PydanticToSQL generator)
    # =========================================================================
    __sql_table__: ClassVar[str] = "dag_task_results"
    __sql_schema__: ClassVar[str] = "dagapp"
    __sql_primary_key__: ClassVar[List[str]] = ["task_id"]
    __sql_foreign_keys__: ClassVar[Dict[str, str]] = {}
    __sql_indexes__: ClassVar[List[tuple]] = [
        ("idx_dag_task_results_unprocessed", ["processed"], "processed = false"),
        ("idx_dag_task_results_job", ["job_id"]),
    ]

    # Identity
    task_id: str = Field(..., max_length=128)
    job_id: str = Field(..., max_length=64)
    node_id: str = Field(..., max_length=64)

    # Result
    status: TaskStatus = Field(...)
    output: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Handler result data"
    )
    error_message: Optional[str] = Field(
        default=None,
        max_length=2000,
        description="Error details if failed"
    )

    # Execution metadata
    worker_id: Optional[str] = Field(
        default=None,
        max_length=64,
        description="Identifier of worker that executed this"
    )
    execution_duration_ms: Optional[int] = Field(
        default=None,
        ge=0,
        description="How long execution took"
    )

    # Progress tracking (for long-running tasks)
    progress_current: Optional[int] = Field(
        default=None,
        ge=0,
        description="Current progress count (e.g., rows processed)"
    )
    progress_total: Optional[int] = Field(
        default=None,
        ge=0,
        description="Total items to process"
    )
    progress_percent: Optional[float] = Field(
        default=None,
        ge=0.0,
        le=100.0,
        description="Calculated progress percentage"
    )
    progress_message: Optional[str] = Field(
        default=None,
        max_length=256,
        description="Current progress status message"
    )

    # Timestamp
    reported_at: datetime = Field(default_factory=datetime.utcnow)

    # Processing flag (orchestrator sets this after reading)
    processed: bool = Field(
        default=False,
        description="Set to true after orchestrator processes this result"
    )

    @property
    def success(self) -> bool:
        """Check if task completed successfully."""
        return self.status == TaskStatus.COMPLETED

    @classmethod
    def success_result(
        cls,
        task_id: str,
        job_id: str,
        node_id: str,
        output: Dict[str, Any],
        worker_id: Optional[str] = None,
        duration_ms: Optional[int] = None,
    ) -> "TaskResult":
        """Factory for successful result."""
        return cls(
            task_id=task_id,
            job_id=job_id,
            node_id=node_id,
            status=TaskStatus.COMPLETED,
            output=output,
            worker_id=worker_id,
            execution_duration_ms=duration_ms,
        )

    @classmethod
    def failure_result(
        cls,
        task_id: str,
        job_id: str,
        node_id: str,
        error_message: str,
        worker_id: Optional[str] = None,
        duration_ms: Optional[int] = None,
    ) -> "TaskResult":
        """Factory for failed result."""
        return cls(
            task_id=task_id,
            job_id=job_id,
            node_id=node_id,
            status=TaskStatus.FAILED,
            error_message=error_message[:2000],
            worker_id=worker_id,
            execution_duration_ms=duration_ms,
        )


# ============================================================================
# EXPORTS
# ============================================================================

__all__ = ["TaskMessage", "TaskResult"]
