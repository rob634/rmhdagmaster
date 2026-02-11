# ============================================================================
# JOB QUERY REPOSITORY
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Gateway - Job status queries
# PURPOSE: Read-only queries for job status and listing
# CREATED: 04 FEB 2026
# ============================================================================
"""
Job Query Repository

Read-only queries for job status and listing.
All state changes go through the orchestrator.
"""

import logging
from typing import Any, Dict, List, Optional

from function.repositories.base import FunctionRepository

logger = logging.getLogger(__name__)


class JobQueryRepository(FunctionRepository):
    """
    Read-only repository for job queries.

    Provides:
    - Get job by ID
    - Get job by correlation_id or idempotency_key
    - List jobs with filters
    - Job statistics
    """

    TABLE = "dagapp.dag_jobs"

    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        """
        Get job by ID.

        Args:
            job_id: Unique job identifier

        Returns:
            Job dict or None if not found
        """
        query = f"""
            SELECT
                job_id, workflow_id, status,
                input_params, result_data, error_message,
                created_at, started_at, completed_at, updated_at,
                owner_id, owner_heartbeat_at,
                correlation_id, idempotency_key,
                version, workflow_version
            FROM {self.TABLE}
            WHERE job_id = %s
        """
        return self.execute_one(query, (job_id,))

    def get_job_by_correlation_id(self, correlation_id: str) -> Optional[Dict[str, Any]]:
        """
        Get most recent job by correlation ID.

        Args:
            correlation_id: External correlation ID

        Returns:
            Job dict or None if not found
        """
        query = f"""
            SELECT
                job_id, workflow_id, status,
                input_params, result_data, error_message,
                created_at, started_at, completed_at, updated_at,
                owner_id, correlation_id, idempotency_key,
                workflow_version
            FROM {self.TABLE}
            WHERE correlation_id = %s
            ORDER BY created_at DESC
            LIMIT 1
        """
        return self.execute_one(query, (correlation_id,))

    def get_job_by_idempotency_key(self, key: str) -> Optional[Dict[str, Any]]:
        """
        Get job by idempotency key.

        Args:
            key: Idempotency key

        Returns:
            Job dict or None if not found
        """
        query = f"""
            SELECT
                job_id, workflow_id, status,
                input_params, result_data, error_message,
                created_at, started_at, completed_at, updated_at,
                owner_id, correlation_id, idempotency_key
            FROM {self.TABLE}
            WHERE idempotency_key = %s
        """
        return self.execute_one(query, (key,))

    def list_jobs(
        self,
        status: Optional[str] = None,
        workflow_id: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """
        List jobs with optional filters.

        Args:
            status: Filter by job status
            workflow_id: Filter by workflow ID
            limit: Maximum results (default 50)
            offset: Pagination offset (default 0)

        Returns:
            List of job dicts
        """
        conditions = []
        params: List[Any] = []

        if status:
            conditions.append("status = %s")
            params.append(status)

        if workflow_id:
            conditions.append("workflow_id = %s")
            params.append(workflow_id)

        where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""

        query = f"""
            SELECT
                job_id, workflow_id, status,
                created_at, started_at, completed_at,
                error_message, owner_id, correlation_id
            FROM {self.TABLE}
            {where_clause}
            ORDER BY created_at DESC
            LIMIT %s OFFSET %s
        """
        params.extend([limit, offset])

        return self.execute_many(query, tuple(params))

    def count_jobs(
        self,
        status: Optional[str] = None,
        workflow_id: Optional[str] = None,
    ) -> int:
        """
        Count jobs with optional filters.

        Args:
            status: Filter by job status
            workflow_id: Filter by workflow ID

        Returns:
            Count of matching jobs
        """
        conditions = []
        params: List[Any] = []

        if status:
            conditions.append("status = %s")
            params.append(status)

        if workflow_id:
            conditions.append("workflow_id = %s")
            params.append(workflow_id)

        where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""

        query = f"""
            SELECT COUNT(*) as count
            FROM {self.TABLE}
            {where_clause}
        """

        return self.execute_count(query, tuple(params))

    def get_job_stats(self) -> Dict[str, int]:
        """
        Get job counts by status.

        Returns:
            Dict mapping status to count
        """
        query = f"""
            SELECT status, COUNT(*) as count
            FROM {self.TABLE}
            GROUP BY status
        """
        rows = self.execute_many(query)
        return {row["status"]: row["count"] for row in rows}

    def get_active_jobs(self, owner_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get jobs that are currently being processed.

        Args:
            owner_id: Optional filter by owner

        Returns:
            List of active job dicts
        """
        if owner_id:
            query = f"""
                SELECT
                    job_id, workflow_id, status,
                    created_at, started_at, owner_id, owner_heartbeat_at
                FROM {self.TABLE}
                WHERE status IN ('pending', 'running')
                  AND owner_id = %s
                ORDER BY created_at ASC
            """
            return self.execute_many(query, (owner_id,))
        else:
            query = f"""
                SELECT
                    job_id, workflow_id, status,
                    created_at, started_at, owner_id, owner_heartbeat_at
                FROM {self.TABLE}
                WHERE status IN ('pending', 'running')
                ORDER BY created_at ASC
            """
            return self.execute_many(query)

    def get_orphan_jobs(self, orphan_threshold_minutes: int = 2) -> List[Dict[str, Any]]:
        """
        Get jobs that may have been orphaned (owner heartbeat stale).

        Args:
            orphan_threshold_minutes: Minutes since last heartbeat

        Returns:
            List of potentially orphaned jobs
        """
        query = f"""
            SELECT
                job_id, workflow_id, status, owner_id,
                owner_heartbeat_at, created_at, started_at
            FROM {self.TABLE}
            WHERE status IN ('pending', 'running')
              AND owner_id IS NOT NULL
              AND owner_heartbeat_at < NOW() - INTERVAL '%s minutes'
            ORDER BY owner_heartbeat_at ASC
        """
        return self.execute_many(query, (orphan_threshold_minutes,))


__all__ = ["JobQueryRepository"]
