# ============================================================================
# WORKER CONTRACTS
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Core - Worker message contracts and schemas
# PURPOSE: Define task message format and detection logic
# CREATED: 31 JAN 2026
# ============================================================================
"""
Worker Contracts

Defines the message schemas for DAG task execution.

DAG Task Message Format:
{
    "task_id": "job-123_validate_0",
    "job_id": "job-123",
    "node_id": "validate",
    "handler": "raster_validate",
    "params": {
        "container_name": "bronze",
        "blob_name": "data/file.tif"
    },
    "timeout_seconds": 300,
    "retry_count": 0,
    "dispatched_at": "2026-01-31T12:00:00Z",
    "correlation_id": "req-456"
}

Legacy Task Message Format (Epoch 4):
{
    "job_id": "...",
    "task_type": "...",
    "stage": "...",
    "parameters": {...}
}

Detection:
- DAG tasks have "handler" field
- Legacy tasks have "task_type" field
"""

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field, field_validator

logger = logging.getLogger(__name__)


# ============================================================================
# MESSAGE TYPE DETECTION
# ============================================================================

class TaskMessageType(str, Enum):
    """Type of task message."""
    DAG = "dag"
    LEGACY = "legacy"
    UNKNOWN = "unknown"


def detect_message_type(message: Dict[str, Any]) -> TaskMessageType:
    """
    Detect whether a message is a DAG task or legacy task.

    DAG tasks have:
    - handler (required)
    - node_id (required)

    Legacy tasks have:
    - task_type (required)
    - stage (optional but common)

    Args:
        message: Message dict from Service Bus

    Returns:
        TaskMessageType enum
    """
    if "handler" in message and "node_id" in message:
        return TaskMessageType.DAG

    if "task_type" in message:
        return TaskMessageType.LEGACY

    return TaskMessageType.UNKNOWN


# ============================================================================
# DAG TASK MESSAGE
# ============================================================================

class DAGTaskMessage(BaseModel):
    """
    DAG task message received by workers.

    This is the contract between orchestrator and worker.
    """

    # Identity
    task_id: str = Field(..., max_length=128, description="Unique task identifier")
    job_id: str = Field(..., max_length=64, description="Parent job identifier")
    node_id: str = Field(..., max_length=64, description="Node in workflow DAG")

    # Execution
    handler: str = Field(..., max_length=64, description="Handler function name")
    params: Dict[str, Any] = Field(
        default_factory=dict,
        description="Parameters for handler execution"
    )

    # Timeouts and retries
    timeout_seconds: int = Field(
        default=3600,
        ge=1,
        le=86400,
        description="Maximum execution time"
    )
    retry_count: int = Field(
        default=0,
        ge=0,
        le=10,
        description="Current retry attempt (0 = first try)"
    )

    # Metadata
    dispatched_at: datetime = Field(
        default_factory=datetime.utcnow,
        description="When task was dispatched"
    )
    correlation_id: Optional[str] = Field(
        default=None,
        max_length=64,
        description="External correlation ID for tracing"
    )

    # Checkpoint resume
    checkpoint_phase: Optional[str] = Field(
        default=None,
        description="Resume from this checkpoint phase"
    )
    checkpoint_data: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Checkpoint data for resume"
    )

    @classmethod
    def from_json(cls, json_str: str) -> "DAGTaskMessage":
        """Parse from JSON string."""
        data = json.loads(json_str)
        return cls.from_dict(data)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DAGTaskMessage":
        """Parse from dict."""
        # Handle datetime parsing
        if isinstance(data.get("dispatched_at"), str):
            data["dispatched_at"] = datetime.fromisoformat(
                data["dispatched_at"].replace("Z", "+00:00")
            )
        return cls(**data)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dict for serialization."""
        return {
            "task_id": self.task_id,
            "job_id": self.job_id,
            "node_id": self.node_id,
            "handler": self.handler,
            "params": self.params,
            "timeout_seconds": self.timeout_seconds,
            "retry_count": self.retry_count,
            "dispatched_at": self.dispatched_at.isoformat(),
            "correlation_id": self.correlation_id,
            "checkpoint_phase": self.checkpoint_phase,
            "checkpoint_data": self.checkpoint_data,
        }

    def to_json(self) -> str:
        """Serialize to JSON string."""
        return json.dumps(self.to_dict(), default=str)


# ============================================================================
# TASK RESULT
# ============================================================================

class TaskResultStatus(str, Enum):
    """Status values for task results."""
    RECEIVED = "received"  # Worker received task
    RUNNING = "running"    # Task is executing
    COMPLETED = "completed"  # Task finished successfully
    FAILED = "failed"      # Task failed


class DAGTaskResult(BaseModel):
    """
    Result reported by worker after task execution.

    Written to dagapp.dag_task_results table.
    """

    # Identity
    task_id: str = Field(..., max_length=128)
    job_id: str = Field(..., max_length=64)
    node_id: str = Field(..., max_length=64)

    # Result
    status: TaskResultStatus = Field(...)
    output: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Handler output data"
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
        description="Worker that executed this task"
    )
    execution_duration_ms: Optional[int] = Field(
        default=None,
        ge=0,
        description="Execution time in milliseconds"
    )

    # Progress (for long-running tasks)
    progress_current: Optional[int] = Field(default=None, ge=0)
    progress_total: Optional[int] = Field(default=None, ge=0)
    progress_percent: Optional[float] = Field(default=None, ge=0, le=100)
    progress_message: Optional[str] = Field(default=None, max_length=256)

    # Checkpoint (for resumable tasks)
    checkpoint_phase: Optional[str] = Field(default=None, max_length=64)
    checkpoint_data: Optional[Dict[str, Any]] = Field(default=None)

    # Timestamp
    reported_at: datetime = Field(default_factory=datetime.utcnow)

    @classmethod
    def success(
        cls,
        task_id: str,
        job_id: str,
        node_id: str,
        output: Dict[str, Any],
        worker_id: Optional[str] = None,
        duration_ms: Optional[int] = None,
    ) -> "DAGTaskResult":
        """Create a success result."""
        return cls(
            task_id=task_id,
            job_id=job_id,
            node_id=node_id,
            status=TaskResultStatus.COMPLETED,
            output=output,
            worker_id=worker_id,
            execution_duration_ms=duration_ms,
        )

    @classmethod
    def failure(
        cls,
        task_id: str,
        job_id: str,
        node_id: str,
        error_message: str,
        worker_id: Optional[str] = None,
        duration_ms: Optional[int] = None,
    ) -> "DAGTaskResult":
        """Create a failure result."""
        return cls(
            task_id=task_id,
            job_id=job_id,
            node_id=node_id,
            status=TaskResultStatus.FAILED,
            error_message=error_message[:2000] if error_message else None,
            worker_id=worker_id,
            execution_duration_ms=duration_ms,
        )

    @classmethod
    def running(
        cls,
        task_id: str,
        job_id: str,
        node_id: str,
        worker_id: Optional[str] = None,
    ) -> "DAGTaskResult":
        """Create a running status result."""
        return cls(
            task_id=task_id,
            job_id=job_id,
            node_id=node_id,
            status=TaskResultStatus.RUNNING,
            worker_id=worker_id,
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dict for serialization."""
        result = {
            "task_id": self.task_id,
            "job_id": self.job_id,
            "node_id": self.node_id,
            "status": self.status.value,
            "output": self.output,
            "error_message": self.error_message,
            "worker_id": self.worker_id,
            "execution_duration_ms": self.execution_duration_ms,
            "reported_at": self.reported_at.isoformat(),
        }

        # Include progress if set
        if self.progress_current is not None:
            result["progress_current"] = self.progress_current
            result["progress_total"] = self.progress_total
            result["progress_percent"] = self.progress_percent
            result["progress_message"] = self.progress_message

        # Include checkpoint if set
        if self.checkpoint_phase is not None:
            result["checkpoint_phase"] = self.checkpoint_phase
            result["checkpoint_data"] = self.checkpoint_data

        return result

    def to_json(self) -> str:
        """Serialize to JSON string."""
        return json.dumps(self.to_dict(), default=str)


# ============================================================================
# WORKER CONFIGURATION
# ============================================================================

@dataclass
class WorkerConfig:
    """Configuration for a DAG worker."""

    # Identity
    worker_id: str
    worker_type: str = "docker"  # "docker" or "functionapp"

    # Queue (MUST be set explicitly - no defaults)
    queue_name: str = ""

    # Connection
    service_bus_connection: Optional[str] = None
    service_bus_namespace: Optional[str] = None
    use_managed_identity: bool = False

    # Database (for direct result reporting)
    database_url: Optional[str] = None

    # HTTP callback (alternative to direct DB)
    callback_url: Optional[str] = None

    # Execution
    max_concurrent_tasks: int = 1
    shutdown_timeout_seconds: int = 30

    # Handler loading
    handler_modules: List[str] = field(default_factory=lambda: ["handlers.examples"])

    @classmethod
    def from_env(cls) -> "WorkerConfig":
        """Create config from environment variables."""
        import os
        import socket

        return cls(
            worker_id=os.getenv("WORKER_ID", f"worker-{socket.gethostname()}"),
            worker_type=os.getenv("WORKER_TYPE", "docker"),
            queue_name=os.getenv("WORKER_QUEUE", ""),
            service_bus_connection=os.getenv("SERVICEBUS_CONNECTION_STRING"),
            service_bus_namespace=os.getenv("SERVICE_BUS_FQDN"),
            use_managed_identity=os.getenv("USE_MANAGED_IDENTITY", "").lower() == "true",
            database_url=os.getenv("DATABASE_URL"),
            callback_url=os.getenv("DAG_CALLBACK_URL"),
            max_concurrent_tasks=int(os.getenv("MAX_CONCURRENT_TASKS", "1")),
            shutdown_timeout_seconds=int(os.getenv("SHUTDOWN_TIMEOUT", "30")),
        )


# ============================================================================
# EXPORTS
# ============================================================================

__all__ = [
    "TaskMessageType",
    "detect_message_type",
    "DAGTaskMessage",
    "TaskResultStatus",
    "DAGTaskResult",
    "WorkerConfig",
]
