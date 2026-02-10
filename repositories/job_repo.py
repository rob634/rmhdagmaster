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

from psycopg import AsyncConnection, sql
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
                sql.SQL("""
                INSERT INTO {} (
                    job_id, workflow_id, status, input_params, result_data,
                    error_message, created_at, started_at,
                    completed_at, submitted_by, correlation_id, version,
                    owner_id, owner_heartbeat_at
                ) VALUES (
                    %(job_id)s, %(workflow_id)s, %(status)s, %(input_params)s,
                    %(result_data)s, %(error_message)s,
                    %(created_at)s, %(started_at)s, %(completed_at)s,
                    %(submitted_by)s, %(correlation_id)s, %(version)s,
                    %(owner_id)s, %(owner_heartbeat_at)s
                )
                """).format(TABLE_JOBS),
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
                    "version": job.version,
                    "owner_id": job.owner_id,
                    "owner_heartbeat_at": job.owner_heartbeat_at,
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
                sql.SQL("SELECT * FROM {} WHERE job_id = %s").format(TABLE_JOBS),
                (job_id,),
            )
            row = await result.fetchone()

            if row is None:
                return None

            return self._row_to_job(row)

    async def update(self, job: Job) -> bool:
        """
        Update an existing job with optimistic locking.

        Uses version column for optimistic locking. If the version in the
        database doesn't match the expected version, the update fails and
        returns False (indicating a concurrent modification).

        Args:
            job: Job instance with updated fields

        Returns:
            True if update succeeded, False if version conflict
        """
        job.updated_at = datetime.utcnow()

        async with self.pool.connection() as conn:
            result = await conn.execute(
                sql.SQL("""
                UPDATE {} SET
                    status = %(status)s,
                    result_data = %(result_data)s,
                    error_message = %(error_message)s,
                    metadata = %(metadata)s,
                    started_at = %(started_at)s,
                    completed_at = %(completed_at)s,
                    updated_at = %(updated_at)s,
                    owner_id = %(owner_id)s,
                    owner_heartbeat_at = %(owner_heartbeat_at)s,
                    version = version + 1
                WHERE job_id = %(job_id)s
                  AND version = %(version)s
                """).format(TABLE_JOBS),
                {
                    "job_id": job.job_id,
                    "status": job.status.value,
                    "result_data": Json(job.result_data) if job.result_data else None,
                    "error_message": job.error_message,
                    "metadata": Json(job.metadata) if job.metadata else Json({}),
                    "started_at": job.started_at,
                    "completed_at": job.completed_at,
                    "updated_at": job.updated_at,
                    "owner_id": job.owner_id,
                    "owner_heartbeat_at": job.owner_heartbeat_at,
                    "version": job.version,
                },
            )

            if result.rowcount == 0:
                logger.warning(
                    f"Version conflict updating job {job.job_id} "
                    f"(expected version {job.version})"
                )
                return False

            # Update local version to match DB
            job.version += 1
            logger.debug(
                f"Updated job {job.job_id} status={job.status.value} "
                f"version={job.version}"
            )
            return True

    async def update_status(
        self,
        job_id: str,
        status: JobStatus,
        error_message: Optional[str] = None,
        result_data: Optional[Dict[str, Any]] = None,
        expected_version: Optional[int] = None,
    ) -> bool:
        """
        Update job status with optional error/result and optimistic locking.

        Args:
            job_id: Job identifier
            status: New status
            error_message: Error details (for FAILED status)
            result_data: Result data (for COMPLETED status)
            expected_version: If provided, uses optimistic locking. Update
                fails if DB version doesn't match.

        Returns:
            True if update succeeded, False if version conflict or no rows updated
        """
        now = datetime.utcnow()
        completed_at = now if status.is_terminal() else None

        async with self.pool.connection() as conn:
            if status == JobStatus.RUNNING:
                # For RUNNING transition, also check started_at IS NULL
                if expected_version is not None:
                    result = await conn.execute(
                        sql.SQL("""
                        UPDATE {}
                        SET status = %s, updated_at = %s, started_at = %s,
                            version = version + 1
                        WHERE job_id = %s AND started_at IS NULL
                          AND version = %s
                        """).format(TABLE_JOBS),
                        (status.value, now, now, job_id, expected_version),
                    )
                else:
                    result = await conn.execute(
                        sql.SQL("""
                        UPDATE {}
                        SET status = %s, updated_at = %s, started_at = %s,
                            version = version + 1
                        WHERE job_id = %s AND started_at IS NULL
                        """).format(TABLE_JOBS),
                        (status.value, now, now, job_id),
                    )
            else:
                # Wrap result_data in Json() for psycopg3 serialization
                result_json = Json(result_data) if result_data else None
                if expected_version is not None:
                    result = await conn.execute(
                        sql.SQL("""
                        UPDATE {}
                        SET status = %s, updated_at = %s, completed_at = %s,
                            error_message = %s, result_data = %s,
                            version = version + 1
                        WHERE job_id = %s AND version = %s
                        """).format(TABLE_JOBS),
                        (status.value, now, completed_at, error_message,
                         result_json, job_id, expected_version),
                    )
                else:
                    result = await conn.execute(
                        sql.SQL("""
                        UPDATE {}
                        SET status = %s, updated_at = %s, completed_at = %s,
                            error_message = %s, result_data = %s,
                            version = version + 1
                        WHERE job_id = %s
                        """).format(TABLE_JOBS),
                        (status.value, now, completed_at, error_message,
                         result_json, job_id),
                    )

            if result.rowcount == 0 and expected_version is not None:
                logger.warning(
                    f"Version conflict updating job {job_id} status to {status.value} "
                    f"(expected version {expected_version})"
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
                sql.SQL("""
                SELECT * FROM {}
                WHERE status = %s
                ORDER BY created_at DESC
                LIMIT %s
                """).format(TABLE_JOBS),
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
                sql.SQL("""
                SELECT * FROM {}
                WHERE status IN ('pending', 'running')
                ORDER BY created_at ASC
                LIMIT %s
                """).format(TABLE_JOBS),
                (limit,),
            )
            rows = await result.fetchall()
            return [self._row_to_job(row) for row in rows]

    async def exists(self, job_id: str) -> bool:
        """Check if a job exists."""
        async with self.pool.connection() as conn:
            conn.row_factory = dict_row
            result = await conn.execute(
                sql.SQL("SELECT 1 as exists FROM {} WHERE job_id = %s").format(TABLE_JOBS),
                (job_id,),
            )
            return await result.fetchone() is not None

    async def list_recent(self, limit: int = 100) -> List[Job]:
        """
        List most recent jobs regardless of status.

        Args:
            limit: Maximum results

        Returns:
            List of Job instances ordered by created_at DESC
        """
        async with self.pool.connection() as conn:
            conn.row_factory = dict_row
            result = await conn.execute(
                sql.SQL("""
                SELECT * FROM {}
                ORDER BY created_at DESC
                LIMIT %s
                """).format(TABLE_JOBS),
                (limit,),
            )
            rows = await result.fetchall()
            return [self._row_to_job(row) for row in rows]

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
            version=row.get("version", 1),
            # Multi-orchestrator ownership
            owner_id=row.get("owner_id"),
            owner_heartbeat_at=row.get("owner_heartbeat_at"),
        )

    # =========================================================================
    # MULTI-ORCHESTRATOR OWNERSHIP METHODS
    # =========================================================================

    async def create_with_owner(self, job: Job, owner_id: str) -> Job:
        """
        Create a new job with ownership claim.

        Used when orchestrator claims a job from the Service Bus queue.
        Sets owner_id and owner_heartbeat_at atomically on creation.

        Args:
            job: Job instance to persist
            owner_id: Orchestrator instance ID claiming ownership

        Returns:
            Created job with ownership fields set
        """
        # Set ownership fields
        job.owner_id = owner_id
        job.owner_heartbeat_at = datetime.utcnow()

        async with self.pool.connection() as conn:
            await conn.execute(
                sql.SQL("""
                INSERT INTO {} (
                    job_id, workflow_id, status, input_params, result_data,
                    error_message, created_at, started_at,
                    completed_at, submitted_by, correlation_id, version,
                    owner_id, owner_heartbeat_at
                ) VALUES (
                    %(job_id)s, %(workflow_id)s, %(status)s, %(input_params)s,
                    %(result_data)s, %(error_message)s,
                    %(created_at)s, %(started_at)s, %(completed_at)s,
                    %(submitted_by)s, %(correlation_id)s, %(version)s,
                    %(owner_id)s, %(owner_heartbeat_at)s
                )
                """).format(TABLE_JOBS),
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
                    "version": job.version,
                    "owner_id": job.owner_id,
                    "owner_heartbeat_at": job.owner_heartbeat_at,
                },
            )
            logger.info(
                f"Created job {job.job_id} with owner {owner_id[:8]}... "
                f"(workflow={job.workflow_id})"
            )
            return job

    async def list_active_for_owner(
        self,
        owner_id: str,
        limit: int = 100,
    ) -> List[Job]:
        """
        List active jobs owned by a specific orchestrator.

        This is the KEY query for multi-orchestrator: each orchestrator
        only processes jobs where owner_id matches its own ID.

        Args:
            owner_id: Orchestrator instance ID
            limit: Maximum results

        Returns:
            List of active jobs owned by this orchestrator
        """
        async with self.pool.connection() as conn:
            conn.row_factory = dict_row
            result = await conn.execute(
                sql.SQL("""
                SELECT * FROM {}
                WHERE owner_id = %s
                  AND status IN ('pending', 'running')
                ORDER BY created_at ASC
                LIMIT %s
                """).format(TABLE_JOBS),
                (owner_id, limit),
            )
            rows = await result.fetchall()
            return [self._row_to_job(row) for row in rows]

    async def update_heartbeat(self, owner_id: str) -> int:
        """
        Update heartbeat for all jobs owned by this orchestrator.

        Called periodically (every 30s) to keep ownership alive.
        If heartbeat stops, jobs become eligible for orphan reclaim.

        Args:
            owner_id: Orchestrator instance ID

        Returns:
            Number of jobs updated
        """
        async with self.pool.connection() as conn:
            result = await conn.execute(
                sql.SQL("""
                UPDATE {}
                SET owner_heartbeat_at = NOW()
                WHERE owner_id = %s
                  AND status IN ('pending', 'running')
                """).format(TABLE_JOBS),
                (owner_id,),
            )
            count = result.rowcount
            if count > 0:
                logger.debug(
                    f"Updated heartbeat for {count} jobs (owner={owner_id[:8]}...)"
                )
            return count

    async def reclaim_orphaned_jobs(
        self,
        new_owner_id: str,
        orphan_threshold_seconds: int = 120,
        limit: int = 10,
    ) -> List[str]:
        """
        Reclaim unowned or orphaned jobs.

        Picks up jobs that either:
        - Have no owner (created via direct API, not queue)
        - Have stale heartbeats (orphaned by crashed orchestrators)

        Uses atomic UPDATE with RETURNING to prevent race conditions.
        Only one orchestrator can win the reclaim for each job.
        Uses FOR UPDATE SKIP LOCKED to avoid contention.

        Args:
            new_owner_id: This orchestrator's ID (becomes new owner)
            orphan_threshold_seconds: How long since heartbeat before orphaned (default 120s)
            limit: Max jobs to reclaim per call (default 10)

        Returns:
            List of reclaimed job_ids
        """
        async with self.pool.connection() as conn:
            conn.row_factory = dict_row
            # Use CTE with FOR UPDATE SKIP LOCKED for safe concurrent reclaim
            result = await conn.execute(
                sql.SQL("""
                WITH orphans AS (
                    SELECT job_id FROM {}
                    WHERE status IN ('pending', 'running')
                      AND (
                          owner_id IS NULL
                          OR owner_heartbeat_at < NOW() - INTERVAL '%s seconds'
                      )
                    LIMIT %s
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE {}
                SET owner_id = %s,
                    owner_heartbeat_at = NOW()
                WHERE job_id IN (SELECT job_id FROM orphans)
                RETURNING job_id
                """).format(TABLE_JOBS, TABLE_JOBS),
                (orphan_threshold_seconds, limit, new_owner_id),
            )
            rows = await result.fetchall()
            job_ids = [row["job_id"] for row in rows]

            if job_ids:
                logger.info(
                    f"Reclaimed {len(job_ids)} orphaned jobs: "
                    f"{[jid[:8] + '...' for jid in job_ids]} "
                    f"(new_owner={new_owner_id[:8]}...)"
                )

            return job_ids

    async def get_by_idempotency_key(
        self,
        idempotency_key: str,
    ) -> Optional[Job]:
        """
        Get a job by its idempotency key.

        Used to prevent duplicate job creation when the same submission
        is received multiple times (e.g., Service Bus redelivery).

        Note: Requires idempotency_key to be stored in job metadata
        or as a dedicated column. Currently checks metadata.

        Args:
            idempotency_key: The idempotency key from the submission

        Returns:
            Existing Job with this idempotency key, or None
        """
        async with self.pool.connection() as conn:
            conn.row_factory = dict_row
            # Check metadata->idempotency_key
            result = await conn.execute(
                sql.SQL("""
                SELECT * FROM {}
                WHERE metadata->>'idempotency_key' = %s
                LIMIT 1
                """).format(TABLE_JOBS),
                (idempotency_key,),
            )
            row = await result.fetchone()

            if row is None:
                return None

            return self._row_to_job(row)

    async def release_ownership(self, job_id: str, owner_id: str) -> bool:
        """
        Release ownership of a job (on graceful shutdown or job completion).

        Only releases if the job is still owned by the specified owner_id.
        This prevents accidentally releasing a job that was reclaimed.

        Args:
            job_id: Job to release
            owner_id: Current owner (must match for release to succeed)

        Returns:
            True if ownership was released, False if job not owned by owner_id
        """
        async with self.pool.connection() as conn:
            result = await conn.execute(
                sql.SQL("""
                UPDATE {}
                SET owner_id = NULL,
                    owner_heartbeat_at = NULL
                WHERE job_id = %s
                  AND owner_id = %s
                """).format(TABLE_JOBS),
                (job_id, owner_id),
            )
            released = result.rowcount > 0
            if released:
                logger.debug(
                    f"Released ownership of job {job_id[:8]}... "
                    f"(owner={owner_id[:8]}...)"
                )
            return released

    async def count_active_by_owner(self) -> Dict[str, int]:
        """
        Count active jobs grouped by owner_id.

        Useful for monitoring job distribution across orchestrator instances.

        Returns:
            Dict mapping owner_id -> count of active jobs
        """
        async with self.pool.connection() as conn:
            conn.row_factory = dict_row
            result = await conn.execute(
                sql.SQL("""
                SELECT owner_id, COUNT(*) as count
                FROM {}
                WHERE status IN ('pending', 'running')
                  AND owner_id IS NOT NULL
                GROUP BY owner_id
                """).format(TABLE_JOBS),
            )
            rows = await result.fetchall()
            return {row["owner_id"]: row["count"] for row in rows}
