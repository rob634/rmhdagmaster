# ============================================================================
# EVENT QUERY REPOSITORY
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Gateway - Event queries
# PURPOSE: Read-only queries for job events (audit trail)
# CREATED: 04 FEB 2026
# ============================================================================
"""
Event Query Repository

Read-only queries for job events (audit trail).
"""

from typing import Any, Dict, List, Optional

from function.repositories.base import FunctionRepository


class EventQueryRepository(FunctionRepository):
    """
    Read-only repository for event queries.

    Provides:
    - Get events for a job
    - Get recent events across all jobs
    - Event statistics
    """

    TABLE = "dagapp.dag_job_events"

    def get_events_for_job(
        self,
        job_id: str,
        limit: int = 100,
        event_type: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get events for a specific job.

        Args:
            job_id: Job ID to get events for
            limit: Maximum events to return
            event_type: Optional filter by event type

        Returns:
            List of event dicts ordered by timestamp descending
        """
        if event_type:
            query = f"""
                SELECT
                    event_id, job_id, node_id,
                    event_type, event_status, error_message,
                    event_data, duration_ms, created_at
                FROM {self.TABLE}
                WHERE job_id = %s AND event_type = %s
                ORDER BY created_at DESC
                LIMIT %s
            """
            return self.execute_many(query, (job_id, event_type, limit))
        else:
            query = f"""
                SELECT
                    event_id, job_id, node_id,
                    event_type, event_status, error_message,
                    event_data, duration_ms, created_at
                FROM {self.TABLE}
                WHERE job_id = %s
                ORDER BY created_at DESC
                LIMIT %s
            """
            return self.execute_many(query, (job_id, limit))

    def get_recent_events(
        self,
        limit: int = 100,
        event_type: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get recent events across all jobs.

        Args:
            limit: Maximum events to return
            event_type: Optional filter by event type

        Returns:
            List of event dicts ordered by timestamp descending
        """
        if event_type:
            query = f"""
                SELECT
                    event_id, job_id, node_id,
                    event_type, event_status, error_message,
                    event_data, duration_ms, created_at
                FROM {self.TABLE}
                WHERE event_type = %s
                ORDER BY created_at DESC
                LIMIT %s
            """
            return self.execute_many(query, (event_type, limit))
        else:
            query = f"""
                SELECT
                    event_id, job_id, node_id,
                    event_type, event_status, error_message,
                    event_data, duration_ms, created_at
                FROM {self.TABLE}
                ORDER BY created_at DESC
                LIMIT %s
            """
            return self.execute_many(query, (limit,))

    def count_events_by_type(self, job_id: Optional[str] = None) -> Dict[str, int]:
        """
        Get event counts by type.

        Args:
            job_id: Optional filter by job

        Returns:
            Dict mapping event_type to count
        """
        if job_id:
            query = f"""
                SELECT event_type, COUNT(*) as count
                FROM {self.TABLE}
                WHERE job_id = %s
                GROUP BY event_type
            """
            rows = self.execute_many(query, (job_id,))
        else:
            query = f"""
                SELECT event_type, COUNT(*) as count
                FROM {self.TABLE}
                GROUP BY event_type
            """
            rows = self.execute_many(query)

        return {row["event_type"]: row["count"] for row in rows}

    def get_error_events(
        self,
        job_id: Optional[str] = None,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """
        Get error events.

        Args:
            job_id: Optional filter by job
            limit: Maximum events to return

        Returns:
            List of error event dicts
        """
        if job_id:
            query = f"""
                SELECT
                    event_id, job_id, node_id,
                    event_type, event_status, error_message,
                    event_data, duration_ms, created_at
                FROM {self.TABLE}
                WHERE job_id = %s AND event_status = 'error'
                ORDER BY created_at DESC
                LIMIT %s
            """
            return self.execute_many(query, (job_id, limit))
        else:
            query = f"""
                SELECT
                    event_id, job_id, node_id,
                    event_type, event_status, error_message,
                    event_data, duration_ms, created_at
                FROM {self.TABLE}
                WHERE event_status = 'error'
                ORDER BY created_at DESC
                LIMIT %s
            """
            return self.execute_many(query, (limit,))

    def get_events_timeline(
        self,
        job_id: str,
    ) -> List[Dict[str, Any]]:
        """
        Get events as a timeline for a job.

        Args:
            job_id: Job ID

        Returns:
            List of events ordered by timestamp ascending (chronological)
        """
        query = f"""
            SELECT
                event_id, job_id, node_id,
                event_type, event_status, error_message,
                event_data, duration_ms, created_at
            FROM {self.TABLE}
            WHERE job_id = %s
            ORDER BY created_at ASC
        """
        return self.execute_many(query, (job_id,))


__all__ = ["EventQueryRepository"]
