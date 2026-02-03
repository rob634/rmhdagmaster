# ============================================================================
# CHECKPOINT REPOSITORY
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Core - Checkpoint CRUD operations
# PURPOSE: Database access for dag_checkpoints table
# CREATED: 02 FEB 2026
# ============================================================================
"""
Checkpoint Repository

CRUD operations for task checkpoints (resumable execution).
"""

import logging
from datetime import datetime
from typing import List, Optional, Dict, Any

from psycopg.rows import dict_row
from psycopg.types.json import Json
from psycopg_pool import AsyncConnectionPool

from core.models import Checkpoint
from .database import TABLE_CHECKPOINTS

logger = logging.getLogger(__name__)


class CheckpointRepository:
    """Repository for Checkpoint entities."""

    def __init__(self, pool: AsyncConnectionPool):
        self.pool = pool

    async def create(self, checkpoint: Checkpoint) -> Checkpoint:
        """
        Create a new checkpoint.

        Args:
            checkpoint: Checkpoint instance to persist

        Returns:
            Created checkpoint
        """
        async with self.pool.connection() as conn:
            await conn.execute(
                f"""
                INSERT INTO {TABLE_CHECKPOINTS} (
                    checkpoint_id, job_id, node_id, task_id,
                    phase_name, phase_index, total_phases,
                    progress_current, progress_total, progress_percent, progress_message,
                    state_data, artifacts_completed, artifacts_total,
                    memory_usage_mb, disk_usage_mb, error_count,
                    created_at, updated_at, is_final, is_valid
                ) VALUES (
                    %(checkpoint_id)s, %(job_id)s, %(node_id)s, %(task_id)s,
                    %(phase_name)s, %(phase_index)s, %(total_phases)s,
                    %(progress_current)s, %(progress_total)s, %(progress_percent)s, %(progress_message)s,
                    %(state_data)s, %(artifacts_completed)s, %(artifacts_total)s,
                    %(memory_usage_mb)s, %(disk_usage_mb)s, %(error_count)s,
                    %(created_at)s, %(updated_at)s, %(is_final)s, %(is_valid)s
                )
                """,
                {
                    "checkpoint_id": checkpoint.checkpoint_id,
                    "job_id": checkpoint.job_id,
                    "node_id": checkpoint.node_id,
                    "task_id": checkpoint.task_id,
                    "phase_name": checkpoint.phase_name,
                    "phase_index": checkpoint.phase_index,
                    "total_phases": checkpoint.total_phases,
                    "progress_current": checkpoint.progress_current,
                    "progress_total": checkpoint.progress_total,
                    "progress_percent": checkpoint.progress_percent,
                    "progress_message": checkpoint.progress_message,
                    "state_data": Json(checkpoint.state_data) if checkpoint.state_data else None,
                    "artifacts_completed": Json(checkpoint.artifacts_completed),
                    "artifacts_total": checkpoint.artifacts_total,
                    "memory_usage_mb": checkpoint.memory_usage_mb,
                    "disk_usage_mb": checkpoint.disk_usage_mb,
                    "error_count": checkpoint.error_count,
                    "created_at": checkpoint.created_at,
                    "updated_at": checkpoint.updated_at,
                    "is_final": checkpoint.is_final,
                    "is_valid": checkpoint.is_valid,
                },
            )
            logger.debug(
                f"Created checkpoint {checkpoint.checkpoint_id} for "
                f"job={checkpoint.job_id} node={checkpoint.node_id} "
                f"phase={checkpoint.phase_name}"
            )
            return checkpoint

    async def get(self, checkpoint_id: str) -> Optional[Checkpoint]:
        """
        Get a checkpoint by ID.

        Args:
            checkpoint_id: Checkpoint identifier

        Returns:
            Checkpoint instance or None if not found
        """
        async with self.pool.connection() as conn:
            conn.row_factory = dict_row
            result = await conn.execute(
                f"SELECT * FROM {TABLE_CHECKPOINTS} WHERE checkpoint_id = %s",
                (checkpoint_id,),
            )
            row = await result.fetchone()

            if row is None:
                return None

            return self._row_to_checkpoint(row)

    async def get_latest_for_node(
        self,
        job_id: str,
        node_id: str,
        valid_only: bool = True,
    ) -> Optional[Checkpoint]:
        """
        Get the most recent checkpoint for a job/node combination.

        Args:
            job_id: Job identifier
            node_id: Node identifier
            valid_only: Only return valid checkpoints (default True)

        Returns:
            Most recent Checkpoint or None
        """
        async with self.pool.connection() as conn:
            conn.row_factory = dict_row

            query = f"""
                SELECT * FROM {TABLE_CHECKPOINTS}
                WHERE job_id = %s AND node_id = %s
            """
            params = [job_id, node_id]

            if valid_only:
                query += " AND is_valid = true"

            query += " ORDER BY created_at DESC LIMIT 1"

            result = await conn.execute(query, tuple(params))
            row = await result.fetchone()

            if row is None:
                return None

            return self._row_to_checkpoint(row)

    async def get_latest_for_task(self, task_id: str) -> Optional[Checkpoint]:
        """
        Get the most recent checkpoint for a specific task execution.

        Args:
            task_id: Task identifier

        Returns:
            Most recent Checkpoint or None
        """
        async with self.pool.connection() as conn:
            conn.row_factory = dict_row
            result = await conn.execute(
                f"""
                SELECT * FROM {TABLE_CHECKPOINTS}
                WHERE task_id = %s AND is_valid = true
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (task_id,),
            )
            row = await result.fetchone()

            if row is None:
                return None

            return self._row_to_checkpoint(row)

    async def list_for_node(
        self,
        job_id: str,
        node_id: str,
        limit: int = 10,
    ) -> List[Checkpoint]:
        """
        List checkpoints for a job/node combination.

        Args:
            job_id: Job identifier
            node_id: Node identifier
            limit: Maximum results

        Returns:
            List of Checkpoint instances ordered by created_at DESC
        """
        async with self.pool.connection() as conn:
            conn.row_factory = dict_row
            result = await conn.execute(
                f"""
                SELECT * FROM {TABLE_CHECKPOINTS}
                WHERE job_id = %s AND node_id = %s
                ORDER BY created_at DESC
                LIMIT %s
                """,
                (job_id, node_id, limit),
            )
            rows = await result.fetchall()
            return [self._row_to_checkpoint(row) for row in rows]

    async def update(self, checkpoint: Checkpoint) -> bool:
        """
        Update an existing checkpoint.

        Args:
            checkpoint: Checkpoint instance with updated fields

        Returns:
            True if update succeeded
        """
        checkpoint.updated_at = datetime.utcnow()

        async with self.pool.connection() as conn:
            result = await conn.execute(
                f"""
                UPDATE {TABLE_CHECKPOINTS} SET
                    phase_name = %(phase_name)s,
                    phase_index = %(phase_index)s,
                    progress_current = %(progress_current)s,
                    progress_total = %(progress_total)s,
                    progress_percent = %(progress_percent)s,
                    progress_message = %(progress_message)s,
                    state_data = %(state_data)s,
                    artifacts_completed = %(artifacts_completed)s,
                    artifacts_total = %(artifacts_total)s,
                    memory_usage_mb = %(memory_usage_mb)s,
                    disk_usage_mb = %(disk_usage_mb)s,
                    error_count = %(error_count)s,
                    updated_at = %(updated_at)s,
                    is_final = %(is_final)s,
                    is_valid = %(is_valid)s
                WHERE checkpoint_id = %(checkpoint_id)s
                """,
                {
                    "checkpoint_id": checkpoint.checkpoint_id,
                    "phase_name": checkpoint.phase_name,
                    "phase_index": checkpoint.phase_index,
                    "progress_current": checkpoint.progress_current,
                    "progress_total": checkpoint.progress_total,
                    "progress_percent": checkpoint.progress_percent,
                    "progress_message": checkpoint.progress_message,
                    "state_data": Json(checkpoint.state_data) if checkpoint.state_data else None,
                    "artifacts_completed": Json(checkpoint.artifacts_completed),
                    "artifacts_total": checkpoint.artifacts_total,
                    "memory_usage_mb": checkpoint.memory_usage_mb,
                    "disk_usage_mb": checkpoint.disk_usage_mb,
                    "error_count": checkpoint.error_count,
                    "updated_at": checkpoint.updated_at,
                    "is_final": checkpoint.is_final,
                    "is_valid": checkpoint.is_valid,
                },
            )

            if result.rowcount == 0:
                logger.warning(f"No checkpoint found to update: {checkpoint.checkpoint_id}")
                return False

            logger.debug(
                f"Updated checkpoint {checkpoint.checkpoint_id} "
                f"phase={checkpoint.phase_name} progress={checkpoint.progress_current}"
            )
            return True

    async def mark_invalid(
        self,
        checkpoint_id: str,
        reason: Optional[str] = None,
    ) -> bool:
        """
        Mark a checkpoint as invalid.

        Args:
            checkpoint_id: Checkpoint identifier
            reason: Optional reason for invalidation

        Returns:
            True if update succeeded
        """
        async with self.pool.connection() as conn:
            if reason:
                result = await conn.execute(
                    f"""
                    UPDATE {TABLE_CHECKPOINTS}
                    SET is_valid = false,
                        state_data = COALESCE(state_data, '{{}}'::jsonb) ||
                                     %(invalid_reason)s::jsonb,
                        updated_at = %(updated_at)s
                    WHERE checkpoint_id = %(checkpoint_id)s
                    """,
                    {
                        "checkpoint_id": checkpoint_id,
                        "invalid_reason": Json({"invalid_reason": reason}),
                        "updated_at": datetime.utcnow(),
                    },
                )
            else:
                result = await conn.execute(
                    f"""
                    UPDATE {TABLE_CHECKPOINTS}
                    SET is_valid = false, updated_at = %s
                    WHERE checkpoint_id = %s
                    """,
                    (datetime.utcnow(), checkpoint_id),
                )

            return result.rowcount > 0

    async def delete_for_job(self, job_id: str) -> int:
        """
        Delete all checkpoints for a job (cleanup after completion).

        Args:
            job_id: Job identifier

        Returns:
            Number of checkpoints deleted
        """
        async with self.pool.connection() as conn:
            result = await conn.execute(
                f"DELETE FROM {TABLE_CHECKPOINTS} WHERE job_id = %s",
                (job_id,),
            )
            count = result.rowcount
            if count > 0:
                logger.info(f"Deleted {count} checkpoints for job {job_id}")
            return count

    async def count_for_job(self, job_id: str) -> int:
        """
        Count checkpoints for a job.

        Args:
            job_id: Job identifier

        Returns:
            Number of checkpoints
        """
        async with self.pool.connection() as conn:
            conn.row_factory = dict_row
            result = await conn.execute(
                f"SELECT COUNT(*) as count FROM {TABLE_CHECKPOINTS} WHERE job_id = %s",
                (job_id,),
            )
            row = await result.fetchone()
            return row["count"]

    def _row_to_checkpoint(self, row: Dict[str, Any]) -> Checkpoint:
        """Convert database row to Checkpoint model."""
        return Checkpoint(
            checkpoint_id=row["checkpoint_id"],
            job_id=row["job_id"],
            node_id=row["node_id"],
            task_id=row["task_id"],
            phase_name=row["phase_name"],
            phase_index=row["phase_index"],
            total_phases=row["total_phases"],
            progress_current=row["progress_current"],
            progress_total=row.get("progress_total"),
            progress_percent=row.get("progress_percent"),
            progress_message=row.get("progress_message"),
            state_data=row.get("state_data"),
            artifacts_completed=row.get("artifacts_completed") or [],
            artifacts_total=row.get("artifacts_total"),
            memory_usage_mb=row.get("memory_usage_mb"),
            disk_usage_mb=row.get("disk_usage_mb"),
            error_count=row.get("error_count", 0),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            is_final=row.get("is_final", False),
            is_valid=row.get("is_valid", True),
        )
