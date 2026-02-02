# ============================================================================
# CHECKPOINT MANAGER
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Core - Checkpoint management for resumable tasks
# PURPOSE: Save and restore task state for long-running operations
# CREATED: 31 JAN 2026
# ============================================================================
"""
Checkpoint Manager

Provides phase-based checkpointing for long-running tasks.
Enables graceful shutdown and resume on restart.

Design:
- Phase-based: Tasks declare phases, checkpoint between them
- Artifact validation: Validate outputs before saving checkpoint
- SIGTERM handling: Graceful save on container termination
- Database persistence: Checkpoints stored in dag_node_states

Usage:
    async with CheckpointManager(context) as checkpoint:
        # Phase 1: Download
        if checkpoint.should_run("download"):
            result = await download_file()
            await checkpoint.save("download", {"file_path": result})

        # Phase 2: Process
        if checkpoint.should_run("process"):
            process_file(checkpoint.data.get("file_path"))
            await checkpoint.save("process", {"processed": True})

        # Phase 3: Upload
        if checkpoint.should_run("upload"):
            upload_result()
            await checkpoint.complete()
"""

import asyncio
import logging
import signal
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Set
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)


@dataclass
class Checkpoint:
    """
    Checkpoint state for a task.

    Represents the saved state at a particular phase.
    """
    phase: str
    data: Dict[str, Any]
    saved_at: datetime
    completed_phases: Set[str] = field(default_factory=set)
    artifacts: Dict[str, str] = field(default_factory=dict)  # artifact_name -> location

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dict for database storage."""
        return {
            "phase": self.phase,
            "data": self.data,
            "saved_at": self.saved_at.isoformat(),
            "completed_phases": list(self.completed_phases),
            "artifacts": self.artifacts,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Checkpoint":
        """Deserialize from database dict."""
        return cls(
            phase=data.get("phase", ""),
            data=data.get("data", {}),
            saved_at=datetime.fromisoformat(data["saved_at"]) if data.get("saved_at") else datetime.utcnow(),
            completed_phases=set(data.get("completed_phases", [])),
            artifacts=data.get("artifacts", {}),
        )


class CheckpointError(Exception):
    """Raised when checkpoint operation fails."""
    pass


class CheckpointManager:
    """
    Manager for task checkpoints.

    Handles saving, loading, and resuming checkpoints.
    Registers SIGTERM handler for graceful shutdown.
    """

    def __init__(
        self,
        task_id: str,
        job_id: str,
        node_id: str,
        save_callback: Optional[Callable[[Checkpoint], Any]] = None,
        initial_checkpoint: Optional[Checkpoint] = None,
    ):
        """
        Initialize checkpoint manager.

        Args:
            task_id: Task identifier
            job_id: Job identifier
            node_id: Node identifier
            save_callback: Async function to persist checkpoint
            initial_checkpoint: Checkpoint to resume from (if any)
        """
        self.task_id = task_id
        self.job_id = job_id
        self.node_id = node_id
        self._save_callback = save_callback

        # Current state
        self._current_checkpoint = initial_checkpoint
        self._completed_phases: Set[str] = set()
        self._pending_data: Dict[str, Any] = {}
        self._artifacts: Dict[str, str] = {}

        # If resuming, populate completed phases
        if initial_checkpoint:
            self._completed_phases = initial_checkpoint.completed_phases.copy()
            self._pending_data = initial_checkpoint.data.copy()
            self._artifacts = initial_checkpoint.artifacts.copy()
            logger.info(
                f"Resuming from checkpoint: phase={initial_checkpoint.phase}, "
                f"completed={self._completed_phases}"
            )

        # Shutdown handling
        self._shutdown_requested = False
        self._original_sigterm_handler = None

    async def __aenter__(self) -> "CheckpointManager":
        """Enter context, register signal handlers."""
        self._register_signal_handlers()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit context, restore signal handlers."""
        self._restore_signal_handlers()

        # If we're exiting due to shutdown, save checkpoint
        if self._shutdown_requested and self._pending_data:
            logger.info("Shutdown requested, saving final checkpoint")
            try:
                await self._persist_checkpoint()
            except Exception as e:
                logger.error(f"Failed to save shutdown checkpoint: {e}")

    def _register_signal_handlers(self) -> None:
        """Register SIGTERM handler for graceful shutdown."""
        try:
            self._original_sigterm_handler = signal.signal(
                signal.SIGTERM,
                self._sigterm_handler,
            )
            logger.debug("Registered SIGTERM handler for checkpoint")
        except (ValueError, OSError) as e:
            # Signal handlers can only be registered in main thread
            logger.debug(f"Could not register SIGTERM handler: {e}")

    def _restore_signal_handlers(self) -> None:
        """Restore original signal handlers."""
        try:
            if self._original_sigterm_handler is not None:
                signal.signal(signal.SIGTERM, self._original_sigterm_handler)
        except (ValueError, OSError):
            pass

    def _sigterm_handler(self, signum, frame) -> None:
        """Handle SIGTERM signal."""
        logger.info(f"Received SIGTERM, requesting graceful shutdown")
        self._shutdown_requested = True

    @property
    def shutdown_requested(self) -> bool:
        """Check if shutdown has been requested."""
        return self._shutdown_requested

    @property
    def data(self) -> Dict[str, Any]:
        """Get current checkpoint data."""
        return self._pending_data.copy()

    @property
    def current_phase(self) -> Optional[str]:
        """Get current phase name."""
        if self._current_checkpoint:
            return self._current_checkpoint.phase
        return None

    @property
    def completed_phases(self) -> Set[str]:
        """Get set of completed phase names."""
        return self._completed_phases.copy()

    def should_run(self, phase: str) -> bool:
        """
        Check if a phase should run.

        Returns True if:
        - Phase hasn't been completed yet
        - Shutdown hasn't been requested

        Args:
            phase: Phase name to check

        Returns:
            True if phase should execute
        """
        if self._shutdown_requested:
            logger.info(f"Skipping phase '{phase}' due to shutdown request")
            return False

        if phase in self._completed_phases:
            logger.debug(f"Skipping phase '{phase}' (already completed)")
            return False

        return True

    async def save(
        self,
        phase: str,
        data: Optional[Dict[str, Any]] = None,
        artifacts: Optional[Dict[str, str]] = None,
        validate_artifacts: bool = True,
    ) -> None:
        """
        Save a checkpoint after completing a phase.

        Args:
            phase: Phase name that was completed
            data: Data to merge into checkpoint state
            artifacts: New artifacts produced (name -> location)
            validate_artifacts: Whether to validate artifact existence

        Raises:
            CheckpointError: If validation fails
        """
        # Merge new data
        if data:
            self._pending_data.update(data)

        # Validate and add artifacts
        if artifacts:
            if validate_artifacts:
                await self._validate_artifacts(artifacts)
            self._artifacts.update(artifacts)

        # Mark phase complete
        self._completed_phases.add(phase)

        # Create checkpoint
        self._current_checkpoint = Checkpoint(
            phase=phase,
            data=self._pending_data.copy(),
            saved_at=datetime.utcnow(),
            completed_phases=self._completed_phases.copy(),
            artifacts=self._artifacts.copy(),
        )

        # Persist
        await self._persist_checkpoint()

        logger.info(f"Checkpoint saved: phase={phase}, phases_done={len(self._completed_phases)}")

    async def complete(self) -> None:
        """
        Mark task as fully complete.

        Clears checkpoint data since task finished successfully.
        """
        self._current_checkpoint = None
        self._pending_data = {}
        self._completed_phases = set()

        # Persist empty checkpoint (signals completion)
        if self._save_callback:
            await self._save_callback(None)

        logger.info("Task completed, checkpoint cleared")

    async def _persist_checkpoint(self) -> None:
        """Persist checkpoint via callback."""
        if self._save_callback and self._current_checkpoint:
            try:
                result = self._save_callback(self._current_checkpoint)
                if asyncio.iscoroutine(result):
                    await result
            except Exception as e:
                logger.error(f"Failed to persist checkpoint: {e}")
                raise CheckpointError(f"Failed to save checkpoint: {e}") from e

    async def _validate_artifacts(self, artifacts: Dict[str, str]) -> None:
        """
        Validate that artifacts exist.

        Args:
            artifacts: Map of artifact name -> location

        Raises:
            CheckpointError: If artifact doesn't exist
        """
        for name, location in artifacts.items():
            # For now, just log. Real validation would check blob storage.
            logger.debug(f"Validating artifact '{name}' at {location}")
            # TODO: Implement actual validation (check blob exists, etc.)


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

@asynccontextmanager
async def checkpoint_context(
    task_id: str,
    job_id: str,
    node_id: str,
    save_callback: Optional[Callable] = None,
    resume_data: Optional[Dict[str, Any]] = None,
):
    """
    Context manager for checkpoint-enabled task execution.

    Args:
        task_id: Task identifier
        job_id: Job identifier
        node_id: Node identifier
        save_callback: Function to persist checkpoints
        resume_data: Previous checkpoint data to resume from

    Yields:
        CheckpointManager instance
    """
    initial = Checkpoint.from_dict(resume_data) if resume_data else None

    manager = CheckpointManager(
        task_id=task_id,
        job_id=job_id,
        node_id=node_id,
        save_callback=save_callback,
        initial_checkpoint=initial,
    )

    async with manager:
        yield manager


# ============================================================================
# EXPORTS
# ============================================================================

__all__ = [
    "CheckpointManager",
    "Checkpoint",
    "CheckpointError",
    "checkpoint_context",
]
