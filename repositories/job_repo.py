# ============================================================================
# JOB REPOSITORY
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Core - Job CRUD operations
# PURPOSE: Database access for dag_jobs table
# CREATED: 29 JAN 2026
# ============================================================================
"""
Job Repository

CRUD operations for DAG jobs.
"""

import logging
from datetime import datetime
from typing import List, Optional, Dict, Any

from psycopg import AsyncConnection
from psycopg.rows import dict_row
from psycopg.types.json import Json
from psycopg_pool import AsyncConnectionPool

from core.models import Job
from core.contracts import JobStatus
from .database import TABLE_JOBS

logger = logging.getLogger(__name__)


class JobRepository:
    """Repository for Job entities."""

    def __init__(self, pool: AsyncConnectionPool):
        self.pool = pool

    async def create(self, job: Job) -> Job:
        """
        Create a new job.

        Args:
            job: Job instance to persist

        Returns:
            Created job with any DB-generated fields
        """
        async with self.pool.connection() as conn:
            await conn.execute(
                f"""
                INSERT INTO {TABLE_JOBS} (
                    job_id, workflow_id, status, input_params, result_data,
                    error_message, created_at, started_at,
                    completed_at, submitted_by, correlation_id
                ) VALUES (
                    %(job_id)s, %(workflow_id)s, %(status)s, %(input_params)s,
                    %(result_data)s, %(error_message)s,
                    %(created_at)s, %(started_at)s, %(completed_at)s,
                    %(submitted_by)s, %(correlation_id)s
                )
                """,
                {
                    "job_id": job.job_id,
                    "workflow_id": job.workflow_id,
                    "status": job.status.value,
                    "input_params": Json(job.input_params),
                    "result_data": Json(job.result_data) if job.result_data else None,
                    "error_message": job.error_message,
                    "created_at": job.created_at,
                    "started_at": job.started_at,
                    "completed_at": job.completed_at,
                    "submitted_by": job.submitted_by,
                    "correlation_id": job.correlation_id,
                },
            )
            logger.info(f"Created job {job.job_id} for workflow {job.workflow_id}")
            return job

    async def get(self, job_id: str) -> Optional[Job]:
        """
        Get a job by ID.

        Args:
            job_id: Job identifier

        Returns:
            Job instance or None if not found
        """
        async with self.pool.connection() as conn:
            conn.row_factory = dict_row
            result = await conn.execute(
                f"SELECT * FROM {TABLE_JOBS} WHERE job_id = %s",
                (job_id,),
            )
            row = await result.fetchone()

            if row is None:
                return None

            return self._row_to_job(row)

    async def update(self, job: Job) -> Job:
        """
        Update an existing job.

        Args:
            job: Job instance with updated fields

        Returns:
            Updated job
        """
        job.updated_at = datetime.utcnow()

        async with self.pool.connection() as conn:
            await conn.execute(
                f"""
                UPDATE {TABLE_JOBS} SET
                    status = %(status)s,
                    result_data = %(result_data)s,
                    error_message = %(error_message)s,
                    metadata = %(metadata)s,
                    started_at = %(started_at)s,
                    completed_at = %(completed_at)s,
                    updated_at = %(updated_at)s
                WHERE job_id = %(job_id)s
                """,
                {
                    "job_id": job.job_id,
                    "status": job.status.value,
                    "result_data": job.result_data,
                    "error_message": job.error_message,
                    "metadata": job.metadata,
                    "started_at": job.started_at,
                    "completed_at": job.completed_at,
                    "updated_at": job.updated_at,
                },
            )
            logger.debug(f"Updated job {job.job_id} status={job.status.value}")
            return job

    async def update_status(
        self,
        job_id: str,
        status: JobStatus,
        error_message: Optional[str] = None,
        result_data: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """
        Update job status with optional error/result.

        Args:
            job_id: Job identifier
            status: New status
            error_message: Error details (for FAILED status)
            result_data: Result data (for COMPLETED status)

        Returns:
            True if update succeeded
        """
        now = datetime.utcnow()
        completed_at = now if status.is_terminal() else None
        started_at_clause = ", started_at = %s" if status == JobStatus.RUNNING else ""

        async with self.pool.connection() as conn:
            if status == JobStatus.RUNNING:
                result = await conn.execute(
                    f"""
                    UPDATE {TABLE_JOBS}
                    SET status = %s, updated_at = %s, started_at = %s
                    WHERE job_id = %s AND started_at IS NULL
                    """,
                    (status.value, now, now, job_id),
                )
            else:
                # Wrap result_data in Json() for psycopg3 serialization
                result_json = Json(result_data) if result_data else None
                result = await conn.execute(
                    f"""
                    UPDATE {TABLE_JOBS}
                    SET status = %s, updated_at = %s, completed_at = %s,
                        error_message = %s, result_data = %s
                    WHERE job_id = %s
                    """,
                    (status.value, now, completed_at, error_message, result_json, job_id),
                )

            return result.rowcount > 0

    async def list_by_status(
        self,
        status: JobStatus,
        limit: int = 100,
    ) -> List[Job]:
        """
        List jobs by status.

        Args:
            status: Status to filter by
            limit: Maximum results

        Returns:
            List of Job instances
        """
        async with self.pool.connection() as conn:
            conn.row_factory = dict_row
            result = await conn.execute(
                f"""
                SELECT * FROM {TABLE_JOBS}
                WHERE status = %s
                ORDER BY created_at DESC
                LIMIT %s
                """,
                (status.value, limit),
            )
            rows = await result.fetchall()
            return [self._row_to_job(row) for row in rows]

    async def list_active(self, limit: int = 100) -> List[Job]:
        """
        List active (non-terminal) jobs.

        Args:
            limit: Maximum results

        Returns:
            List of active Job instances
        """
        async with self.pool.connection() as conn:
            conn.row_factory = dict_row
            result = await conn.execute(
                f"""
                SELECT * FROM {TABLE_JOBS}
                WHERE status IN ('pending', 'running')
                ORDER BY created_at ASC
                LIMIT %s
                """,
                (limit,),
            )
            rows = await result.fetchall()
            return [self._row_to_job(row) for row in rows]

    async def exists(self, job_id: str) -> bool:
        """Check if a job exists."""
        async with self.pool.connection() as conn:
            conn.row_factory = dict_row
            result = await conn.execute(
                f"SELECT 1 as exists FROM {TABLE_JOBS} WHERE job_id = %s",
                (job_id,),
            )
            return await result.fetchone() is not None

    def _row_to_job(self, row: Dict[str, Any]) -> Job:
        """Convert database row to Job model."""
        # Handle updated_at - fallback to created_at if not present
        updated_at = row.get("updated_at") or row.get("created_at")

        return Job(
            job_id=row["job_id"],
            workflow_id=row["workflow_id"],
            status=JobStatus(row["status"]),
            input_params=row.get("input_params") or {},
            result_data=row.get("result_data"),
            error_message=row.get("error_message"),
            metadata=row.get("metadata") or {},
            created_at=row["created_at"],
            started_at=row.get("started_at"),
            completed_at=row.get("completed_at"),
            updated_at=updated_at,
            submitted_by=row.get("submitted_by"),
            correlation_id=row.get("correlation_id"),
        )
