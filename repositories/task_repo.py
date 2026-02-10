# ============================================================================
# TASK RESULT REPOSITORY
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Core - TaskResult CRUD operations
# PURPOSE: Database access for dag_task_results table
# CREATED: 29 JAN 2026
# ============================================================================
"""
Task Result Repository

CRUD operations for task results reported by workers.
The orchestrator polls this table for completed tasks.
"""

import logging
from datetime import datetime
from typing import List, Optional, Dict, Any

from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Json
from psycopg_pool import AsyncConnectionPool

from core.models import TaskResult
from core.contracts import TaskStatus
from .database import TABLE_TASKS

logger = logging.getLogger(__name__)


class TaskResultRepository:
    """Repository for TaskResult entities."""

    def __init__(self, pool: AsyncConnectionPool):
        self.pool = pool

    async def create(self, result: TaskResult) -> TaskResult:
        """
        Create a new task result.

        Called by workers when task completes.

        Args:
            result: TaskResult instance

        Returns:
            Created task result
        """
        async with self.pool.connection() as conn:
            await conn.execute(
                sql.SQL("""
                INSERT INTO {} (
                    task_id, job_id, node_id, status, output, error_message,
                    worker_id, execution_duration_ms, reported_at, processed
                ) VALUES (
                    %(task_id)s, %(job_id)s, %(node_id)s, %(status)s,
                    %(output)s, %(error_message)s, %(worker_id)s,
                    %(execution_duration_ms)s, %(reported_at)s, false
                )
                ON CONFLICT (task_id) DO UPDATE SET
                    status = EXCLUDED.status,
                    output = EXCLUDED.output,
                    error_message = EXCLUDED.error_message,
                    worker_id = EXCLUDED.worker_id,
                    execution_duration_ms = EXCLUDED.execution_duration_ms,
                    reported_at = EXCLUDED.reported_at,
                    processed = false
                """).format(TABLE_TASKS),
                {
                    "task_id": result.task_id,
                    "job_id": result.job_id,
                    "node_id": result.node_id,
                    "status": result.status.value,
                    "output": Json(result.output) if result.output else None,
                    "error_message": result.error_message,
                    "worker_id": result.worker_id,
                    "execution_duration_ms": result.execution_duration_ms,
                    "reported_at": result.reported_at,
                },
            )
            logger.info(
                f"Task result created: {result.task_id} "
                f"status={result.status.value}"
            )
            return result

    async def get(self, task_id: str) -> Optional[TaskResult]:
        """
        Get a task result by ID.

        Args:
            task_id: Task identifier

        Returns:
            TaskResult or None
        """
        async with self.pool.connection() as conn:
            conn.row_factory = dict_row
            result = await conn.execute(
                sql.SQL("SELECT * FROM {} WHERE task_id = %s").format(TABLE_TASKS),
                (task_id,),
            )
            row = await result.fetchone()

            if row is None:
                return None

            return self._row_to_result(row)

    async def get_unprocessed(self, limit: int = 100) -> List[TaskResult]:
        """
        Get unprocessed task results.

        The orchestrator calls this to find completed tasks
        that need to be processed (update node state, advance job).

        Args:
            limit: Maximum results

        Returns:
            List of unprocessed TaskResult instances
        """
        async with self.pool.connection() as conn:
            conn.row_factory = dict_row
            result = await conn.execute(
                sql.SQL("""
                SELECT * FROM {}
                WHERE processed = false
                ORDER BY reported_at ASC
                LIMIT %s
                """).format(TABLE_TASKS),
                (limit,),
            )
            rows = await result.fetchall()
            return [self._row_to_result(row) for row in rows]

    async def get_unprocessed_for_job(self, job_id: str) -> List[TaskResult]:
        """
        Get unprocessed task results for a specific job.

        Args:
            job_id: Job identifier

        Returns:
            List of unprocessed TaskResult instances
        """
        async with self.pool.connection() as conn:
            conn.row_factory = dict_row
            result = await conn.execute(
                sql.SQL("""
                SELECT * FROM {}
                WHERE job_id = %s AND processed = false
                ORDER BY reported_at ASC
                """).format(TABLE_TASKS),
                (job_id,),
            )
            rows = await result.fetchall()
            return [self._row_to_result(row) for row in rows]

    async def mark_processed(self, task_id: str) -> bool:
        """
        Mark a task result as processed.

        Called by orchestrator after processing the result.

        Args:
            task_id: Task identifier

        Returns:
            True if update succeeded
        """
        async with self.pool.connection() as conn:
            result = await conn.execute(
                sql.SQL("""
                UPDATE {}
                SET processed = true
                WHERE task_id = %s AND processed = false
                """).format(TABLE_TASKS),
                (task_id,),
            )
            return result.rowcount > 0

    async def mark_many_processed(self, task_ids: List[str]) -> int:
        """
        Mark multiple task results as processed.

        Args:
            task_ids: List of task identifiers

        Returns:
            Number of tasks updated
        """
        if not task_ids:
            return 0

        async with self.pool.connection() as conn:
            result = await conn.execute(
                sql.SQL("""
                UPDATE {}
                SET processed = true
                WHERE task_id = ANY(%s) AND processed = false
                """).format(TABLE_TASKS),
                (task_ids,),
            )
            return result.rowcount

    async def get_for_job(self, job_id: str) -> List[TaskResult]:
        """
        Get all task results for a job.

        Args:
            job_id: Job identifier

        Returns:
            List of TaskResult instances
        """
        async with self.pool.connection() as conn:
            conn.row_factory = dict_row
            result = await conn.execute(
                sql.SQL("""
                SELECT * FROM {}
                WHERE job_id = %s
                ORDER BY reported_at ASC
                """).format(TABLE_TASKS),
                (job_id,),
            )
            rows = await result.fetchall()
            return [self._row_to_result(row) for row in rows]

    async def cleanup_old(self, days: int = 7) -> int:
        """
        Delete old processed task results.

        Args:
            days: Delete results older than this many days

        Returns:
            Number of deleted records
        """
        async with self.pool.connection() as conn:
            result = await conn.execute(
                sql.SQL("""
                DELETE FROM {}
                WHERE processed = true
                AND reported_at < NOW() - INTERVAL '%s days'
                """).format(TABLE_TASKS),
                (days,),
            )
            count = result.rowcount
            if count > 0:
                logger.info(f"Cleaned up {count} old task results")
            return count

    def _row_to_result(self, row: Dict[str, Any]) -> TaskResult:
        """Convert database row to TaskResult model."""
        return TaskResult(
            task_id=row["task_id"],
            job_id=row["job_id"],
            node_id=row["node_id"],
            status=TaskStatus(row["status"]),
            output=row.get("output"),
            error_message=row.get("error_message"),
            worker_id=row.get("worker_id"),
            execution_duration_ms=row.get("execution_duration_ms"),
            reported_at=row["reported_at"],
            processed=row.get("processed", False),
        )
