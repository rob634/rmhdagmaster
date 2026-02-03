# ============================================================================
# CLAUDE CONTEXT - CHECKPOINT MODEL
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Core model - Task checkpoint for resumability
# PURPOSE: Track checkpoint state for long-running, resumable tasks
# CREATED: 02 FEB 2026
# EXPORTS: Checkpoint
# DEPENDENCIES: pydantic
# ============================================================================
"""
Checkpoint Model

Checkpoints enable long-running tasks to save progress and resume after
failure, timeout, or graceful shutdown.

Key concepts:
- Phases: Discrete steps in a handler (e.g., "download", "process", "upload")
- Progress: Fine-grained progress within a phase (e.g., "500 of 1000 rows")
- State data: Handler-specific data needed to resume (minimal JSONB)
- Artifacts: List of completed outputs (e.g., processed files)

A task can save multiple checkpoints during execution. On retry, the most
recent checkpoint is provided to the handler so it can resume from there.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional, ClassVar
from pydantic import BaseModel, Field
import uuid


class Checkpoint(BaseModel):
    """
    Checkpoint for resumable task execution.

    Maps to: dagapp.dag_checkpoints table

    Lifecycle:
        1. Handler starts, loads previous checkpoint (if any)
        2. Handler completes a phase, saves checkpoint
        3. If interrupted, checkpoint persists
        4. On retry, handler receives checkpoint and resumes

    Example:
        Phase 1: Download file (checkpoint: {"downloaded_bytes": 50000000})
        Phase 2: Process tiles (checkpoint: {"tiles_processed": [0,1,2,3,4]})
        Phase 3: Upload results (checkpoint: {"uploaded": ["tile_0.tif", ...]})
    """

    # =========================================================================
    # SQL DDL METADATA (Used by PydanticToSQL generator)
    # =========================================================================
    __sql_table__: ClassVar[str] = "dag_checkpoints"
    __sql_schema__: ClassVar[str] = "dagapp"
    __sql_primary_key__: ClassVar[List[str]] = ["checkpoint_id"]
    __sql_foreign_keys__: ClassVar[Dict[str, str]] = {
        "job_id": "dagapp.dag_jobs(job_id)",
    }
    __sql_indexes__: ClassVar[List[tuple]] = [
        ("idx_dag_checkpoints_job_node", ["job_id", "node_id"]),
        ("idx_dag_checkpoints_task", ["task_id"]),
        ("idx_dag_checkpoints_latest", ["job_id", "node_id", "created_at DESC"]),
    ]

    # =========================================================================
    # IDENTITY
    # =========================================================================
    checkpoint_id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        max_length=64,
        description="Unique checkpoint identifier"
    )
    job_id: str = Field(..., max_length=64, description="Parent job")
    node_id: str = Field(..., max_length=64, description="Node this checkpoint belongs to")
    task_id: str = Field(..., max_length=128, description="Task execution that created this")

    # =========================================================================
    # PHASE TRACKING
    # =========================================================================
    phase_name: str = Field(
        ...,
        max_length=64,
        description="Current phase name (e.g., 'download', 'process', 'upload')"
    )
    phase_index: int = Field(
        default=0,
        ge=0,
        description="Zero-based phase index (0, 1, 2, ...)"
    )
    total_phases: int = Field(
        default=1,
        ge=1,
        description="Total number of phases in this handler"
    )

    # =========================================================================
    # PROGRESS WITHIN PHASE
    # =========================================================================
    progress_current: int = Field(
        default=0,
        ge=0,
        description="Current progress count (e.g., rows processed, bytes downloaded)"
    )
    progress_total: Optional[int] = Field(
        default=None,
        ge=0,
        description="Total items to process (None if unknown)"
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
        description="Human-readable progress message"
    )

    # =========================================================================
    # STATE DATA (minimal JSONB for truly variable data)
    # =========================================================================
    state_data: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Handler-specific state data needed to resume (keep minimal)"
    )

    # =========================================================================
    # ARTIFACTS TRACKING
    # =========================================================================
    artifacts_completed: List[str] = Field(
        default_factory=list,
        description="List of completed artifact identifiers (e.g., file paths, blob names)"
    )
    artifacts_total: Optional[int] = Field(
        default=None,
        ge=0,
        description="Total artifacts expected (for progress tracking)"
    )

    # =========================================================================
    # RESOURCE METRICS
    # =========================================================================
    memory_usage_mb: Optional[int] = Field(
        default=None,
        ge=0,
        description="Memory usage at checkpoint time (MB)"
    )
    disk_usage_mb: Optional[int] = Field(
        default=None,
        ge=0,
        description="Disk usage at checkpoint time (MB)"
    )
    error_count: int = Field(
        default=0,
        ge=0,
        description="Number of recoverable errors encountered so far"
    )

    # =========================================================================
    # TIMESTAMPS
    # =========================================================================
    created_at: datetime = Field(
        default_factory=datetime.utcnow,
        description="When this checkpoint was created"
    )
    updated_at: datetime = Field(
        default_factory=datetime.utcnow,
        description="When this checkpoint was last updated"
    )

    # =========================================================================
    # STATUS FLAGS
    # =========================================================================
    is_final: bool = Field(
        default=False,
        description="True if this is the final checkpoint (task completed successfully)"
    )
    is_valid: bool = Field(
        default=True,
        description="False if checkpoint is known to be corrupted or invalid"
    )

    # =========================================================================
    # COMPUTED PROPERTIES
    # =========================================================================
    @property
    def is_complete(self) -> bool:
        """Check if all phases are complete."""
        return self.is_final or (
            self.phase_index >= self.total_phases - 1 and
            self.progress_total is not None and
            self.progress_current >= self.progress_total
        )

    @property
    def phase_progress_percent(self) -> Optional[float]:
        """Calculate progress percentage within current phase."""
        if self.progress_total is None or self.progress_total == 0:
            return None
        return min(100.0, (self.progress_current / self.progress_total) * 100)

    @property
    def overall_progress_percent(self) -> Optional[float]:
        """Calculate overall progress across all phases."""
        if self.total_phases == 0:
            return None

        # Base progress from completed phases
        completed_phases_pct = (self.phase_index / self.total_phases) * 100

        # Add progress within current phase
        phase_pct = self.phase_progress_percent or 0
        current_phase_contribution = (phase_pct / 100) * (100 / self.total_phases)

        return min(100.0, completed_phases_pct + current_phase_contribution)

    # =========================================================================
    # FACTORY METHODS
    # =========================================================================
    @classmethod
    def create(
        cls,
        job_id: str,
        node_id: str,
        task_id: str,
        phase_name: str,
        phase_index: int = 0,
        total_phases: int = 1,
    ) -> "Checkpoint":
        """
        Create a new checkpoint.

        Args:
            job_id: Parent job ID
            node_id: Node ID
            task_id: Task execution ID
            phase_name: Name of current phase
            phase_index: Zero-based phase index
            total_phases: Total number of phases

        Returns:
            New Checkpoint instance
        """
        return cls(
            job_id=job_id,
            node_id=node_id,
            task_id=task_id,
            phase_name=phase_name,
            phase_index=phase_index,
            total_phases=total_phases,
        )

    def advance_phase(
        self,
        new_phase_name: str,
        reset_progress: bool = True,
    ) -> "Checkpoint":
        """
        Create a new checkpoint for the next phase.

        Args:
            new_phase_name: Name of the next phase
            reset_progress: Whether to reset progress counters

        Returns:
            New Checkpoint instance for next phase
        """
        return Checkpoint(
            checkpoint_id=str(uuid.uuid4()),
            job_id=self.job_id,
            node_id=self.node_id,
            task_id=self.task_id,
            phase_name=new_phase_name,
            phase_index=self.phase_index + 1,
            total_phases=self.total_phases,
            progress_current=0 if reset_progress else self.progress_current,
            progress_total=None if reset_progress else self.progress_total,
            artifacts_completed=self.artifacts_completed.copy(),
            error_count=self.error_count,
        )

    def update_progress(
        self,
        current: int,
        total: Optional[int] = None,
        message: Optional[str] = None,
    ) -> None:
        """
        Update progress within current phase.

        Args:
            current: Current progress count
            total: Total items (optional, keeps existing if None)
            message: Progress message
        """
        self.progress_current = current
        if total is not None:
            self.progress_total = total
        if message is not None:
            self.progress_message = message
        if self.progress_total and self.progress_total > 0:
            self.progress_percent = (current / self.progress_total) * 100
        self.updated_at = datetime.utcnow()

    def add_artifact(self, artifact_id: str) -> None:
        """
        Record a completed artifact.

        Args:
            artifact_id: Identifier for the completed artifact
        """
        if artifact_id not in self.artifacts_completed:
            self.artifacts_completed.append(artifact_id)
        self.updated_at = datetime.utcnow()

    def mark_final(self) -> None:
        """Mark this checkpoint as the final one (task complete)."""
        self.is_final = True
        self.updated_at = datetime.utcnow()

    def mark_invalid(self, reason: Optional[str] = None) -> None:
        """Mark this checkpoint as invalid."""
        self.is_valid = False
        if reason:
            self.state_data = self.state_data or {}
            self.state_data["invalid_reason"] = reason
        self.updated_at = datetime.utcnow()


# ============================================================================
# EXPORTS
# ============================================================================

__all__ = ["Checkpoint"]
