# ============================================================================
# CLAUDE CONTEXT - BASE CONTRACTS & ENUMS
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Foundation - Core enums and base contracts
# PURPOSE: Define status enums and base data contracts for DAG system
# LAST_REVIEWED: 28 JAN 2026
# EXPORTS: JobStatus, NodeStatus, TaskStatus, JobData, NodeData, TaskData
# DEPENDENCIES: enum, pydantic
# ============================================================================
"""
Base contracts for the DAG orchestration system.

These define the minimal identity fields that cross boundaries:
- SQL (PostgreSQL)
- Queue (Azure Service Bus)
- Python (Internal processing)

Boundary-specific models inherit from these contracts.
"""

from enum import Enum
from typing import Any, Dict, Optional
from pydantic import BaseModel, Field


# ============================================================================
# STATUS ENUMS
# ============================================================================

class JobStatus(str, Enum):
    """
    Job lifecycle states.

    State transitions:
        PENDING -> RUNNING -> COMPLETED
                          -> FAILED
                          -> CANCELLED
    """
    PENDING = "pending"          # Job created, not yet started
    RUNNING = "running"          # At least one node is executing
    COMPLETED = "completed"      # All nodes completed successfully
    FAILED = "failed"            # One or more nodes failed (unrecoverable)
    CANCELLED = "cancelled"      # Manually cancelled

    def is_terminal(self) -> bool:
        """Check if this is a terminal state (no further transitions)."""
        return self in (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED)


class NodeStatus(str, Enum):
    """
    Node lifecycle states within a job.

    State transitions:
        PENDING -> READY -> DISPATCHED -> RUNNING -> COMPLETED
                                                  -> FAILED
                -> SKIPPED (branch not taken)
    """
    PENDING = "pending"          # Waiting for dependencies
    READY = "ready"              # Dependencies met, awaiting dispatch
    DISPATCHED = "dispatched"    # Sent to queue, not yet picked up
    RUNNING = "running"          # Worker executing
    COMPLETED = "completed"      # Finished successfully
    FAILED = "failed"            # Finished with error
    SKIPPED = "skipped"          # Branch not taken (conditional routing)

    def is_terminal(self) -> bool:
        """Check if this is a terminal state."""
        return self in (NodeStatus.COMPLETED, NodeStatus.FAILED, NodeStatus.SKIPPED)

    def is_successful(self) -> bool:
        """Check if this represents successful completion."""
        return self in (NodeStatus.COMPLETED, NodeStatus.SKIPPED)


class TaskStatus(str, Enum):
    """
    Task execution states (worker-side).

    Simpler than NodeStatus - tasks just execute and report.
    """
    RECEIVED = "received"        # Worker received message
    RUNNING = "running"          # Handler executing
    COMPLETED = "completed"      # Success
    FAILED = "failed"            # Error


# ============================================================================
# BASE DATA CONTRACTS
# ============================================================================

class JobData(BaseModel):
    """
    Essential job identity - the minimum fields that define a job.

    All job-related models should include these fields.
    """
    job_id: str = Field(..., max_length=64, description="SHA256 hash, idempotent")
    workflow_id: str = Field(..., max_length=64, description="Reference to workflow definition")

    model_config = {"frozen": False}


class NodeData(BaseModel):
    """
    Essential node identity within a job.
    """
    job_id: str = Field(..., max_length=64)
    node_id: str = Field(..., max_length=64, description="Node name from workflow definition")

    model_config = {"frozen": False}


class TaskData(BaseModel):
    """
    Essential task identity - what gets dispatched to workers.
    """
    task_id: str = Field(..., max_length=128, description="Unique task identifier")
    job_id: str = Field(..., max_length=64)
    node_id: str = Field(..., max_length=64)
    handler: str = Field(..., max_length=64, description="Handler function to execute")

    model_config = {"frozen": False}
