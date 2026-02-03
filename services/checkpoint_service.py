# ============================================================================
# CHECKPOINT SERVICE
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Core - Checkpoint management
# PURPOSE: Save, load, and manage task checkpoints for resumability
# CREATED: 02 FEB 2026
# ============================================================================
"""
Checkpoint Service

Coordinates checkpoint operations for resumable task execution.

Key responsibilities:
- Save checkpoints reported by workers
- Load latest checkpoint for retry dispatch
- Clean up checkpoints on job completion
- Provide checkpoint data for TaskMessage creation
"""

import logging
from datetime import datetime
from typing import Dict, Any, Optional

from psycopg_pool import AsyncConnectionPool

from core.models import Checkpoint
from repositories import CheckpointRepository

logger = logging.getLogger(__name__)


class CheckpointService:
    """
    Service for managing task checkpoints.

    Used by:
    - Workers (via callback) to save progress
    - Orchestrator to load checkpoint for retry
    - JobService to clean up on completion
    """

    def __init__(self, pool: AsyncConnectionPool):
        """
        Initialize checkpoint service.

        Args:
            pool: Database connection pool
        """
        self.pool = pool
        self.checkpoint_repo = CheckpointRepository(pool)

    async def save_checkpoint(
        self,
        job_id: str,
        node_id: str,
        task_id: str,
        phase_name: str,
        phase_index: int = 0,
        total_phases: int = 1,
        progress_current: int = 0,
        progress_total: Optional[int] = None,
        progress_message: Optional[str] = None,
        state_data: Optional[Dict[str, Any]] = None,
        artifacts_completed: Optional[list] = None,
        memory_usage_mb: Optional[int] = None,
        disk_usage_mb: Optional[int] = None,
        error_count: int = 0,
        is_final: bool = False,
    ) -> Checkpoint:
        """
        Save a new checkpoint (worker reports progress).

        This creates a new checkpoint record. Multiple checkpoints can
        exist for a single node - the latest valid one is used on retry.

        Args:
            job_id: Parent job ID
            node_id: Node this checkpoint belongs to
            task_id: Task execution that created this
            phase_name: Current phase name (e.g., 'download', 'process')
            phase_index: Zero-based phase index
            total_phases: Total number of phases
            progress_current: Current progress count
            progress_total: Total items (optional)
            progress_message: Human-readable progress message
            state_data: Handler-specific state needed to resume
            artifacts_completed: List of completed artifact IDs
            memory_usage_mb: Memory usage at checkpoint time
            disk_usage_mb: Disk usage at checkpoint time
            error_count: Number of recoverable errors so far
            is_final: True if task completed successfully

        Returns:
            Created Checkpoint instance
        """
        checkpoint = Checkpoint(
            job_id=job_id,
            node_id=node_id,
            task_id=task_id,
            phase_name=phase_name,
            phase_index=phase_index,
            total_phases=total_phases,
            progress_current=progress_current,
            progress_total=progress_total,
            progress_message=progress_message,
            state_data=state_data,
            artifacts_completed=artifacts_completed or [],
            memory_usage_mb=memory_usage_mb,
            disk_usage_mb=disk_usage_mb,
            error_count=error_count,
            is_final=is_final,
        )

        # Calculate progress percent if we have total
        if progress_total and progress_total > 0:
            checkpoint.progress_percent = (progress_current / progress_total) * 100

        created = await self.checkpoint_repo.create(checkpoint)

        logger.info(
            f"Checkpoint saved: job={job_id} node={node_id} "
            f"phase={phase_name}[{phase_index}/{total_phases}] "
            f"progress={progress_current}/{progress_total or '?'}"
        )

        return created

    async def get_latest_checkpoint(
        self,
        job_id: str,
        node_id: str,
    ) -> Optional[Checkpoint]:
        """
        Get the most recent valid checkpoint for a node.

        Used when preparing a retry to load the last known state.

        Args:
            job_id: Job identifier
            node_id: Node identifier

        Returns:
            Most recent valid Checkpoint or None
        """
        checkpoint = await self.checkpoint_repo.get_latest_for_node(
            job_id=job_id,
            node_id=node_id,
            valid_only=True,
        )

        if checkpoint:
            logger.debug(
                f"Found checkpoint for retry: job={job_id} node={node_id} "
                f"phase={checkpoint.phase_name} progress={checkpoint.progress_current}"
            )

        return checkpoint

    async def get_checkpoint_data_for_retry(
        self,
        job_id: str,
        node_id: str,
    ) -> Optional[Dict[str, Any]]:
        """
        Get checkpoint data formatted for TaskMessage dispatch.

        Returns a dict suitable for TaskMessage.checkpoint_data field.
        Workers receive this to resume from where they left off.

        Args:
            job_id: Job identifier
            node_id: Node identifier

        Returns:
            Dict with checkpoint data or None if no checkpoint exists
        """
        checkpoint = await self.get_latest_checkpoint(job_id, node_id)

        if checkpoint is None:
            return None

        # Return structured checkpoint data for the worker
        return {
            "checkpoint_id": checkpoint.checkpoint_id,
            "phase_name": checkpoint.phase_name,
            "phase_index": checkpoint.phase_index,
            "total_phases": checkpoint.total_phases,
            "progress_current": checkpoint.progress_current,
            "progress_total": checkpoint.progress_total,
            "state_data": checkpoint.state_data,
            "artifacts_completed": checkpoint.artifacts_completed,
            "error_count": checkpoint.error_count,
        }

    async def mark_checkpoint_invalid(
        self,
        checkpoint_id: str,
        reason: str,
    ) -> bool:
        """
        Mark a checkpoint as invalid (e.g., data corruption detected).

        Args:
            checkpoint_id: Checkpoint to invalidate
            reason: Why it's being invalidated

        Returns:
            True if update succeeded
        """
        success = await self.checkpoint_repo.mark_invalid(checkpoint_id, reason)

        if success:
            logger.warning(f"Checkpoint {checkpoint_id} marked invalid: {reason}")

        return success

    async def cleanup_job_checkpoints(self, job_id: str) -> int:
        """
        Delete all checkpoints for a completed job.

        Called by JobService when a job completes or is cancelled.
        Checkpoints are no longer needed once the job is terminal.

        Args:
            job_id: Job identifier

        Returns:
            Number of checkpoints deleted
        """
        count = await self.checkpoint_repo.delete_for_job(job_id)

        if count > 0:
            logger.info(f"Cleaned up {count} checkpoints for job {job_id}")

        return count

    async def get_node_progress(
        self,
        job_id: str,
        node_id: str,
    ) -> Optional[Dict[str, Any]]:
        """
        Get current progress for a node from its latest checkpoint.

        Used by progress API endpoints.

        Args:
            job_id: Job identifier
            node_id: Node identifier

        Returns:
            Progress info dict or None
        """
        checkpoint = await self.get_latest_checkpoint(job_id, node_id)

        if checkpoint is None:
            return None

        return {
            "phase_name": checkpoint.phase_name,
            "phase_index": checkpoint.phase_index,
            "total_phases": checkpoint.total_phases,
            "phase_progress_percent": checkpoint.phase_progress_percent,
            "overall_progress_percent": checkpoint.overall_progress_percent,
            "progress_current": checkpoint.progress_current,
            "progress_total": checkpoint.progress_total,
            "progress_message": checkpoint.progress_message,
            "artifacts_completed": len(checkpoint.artifacts_completed),
            "artifacts_total": checkpoint.artifacts_total,
            "last_updated": checkpoint.updated_at.isoformat(),
        }

    async def list_checkpoints(
        self,
        job_id: str,
        node_id: str,
        limit: int = 10,
    ) -> list:
        """
        List checkpoint history for a node.

        Args:
            job_id: Job identifier
            node_id: Node identifier
            limit: Maximum results

        Returns:
            List of Checkpoint instances
        """
        return await self.checkpoint_repo.list_for_node(job_id, node_id, limit)
