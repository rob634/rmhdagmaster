# ============================================================================
# NODE QUERY REPOSITORY
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Gateway - Node state queries
# PURPOSE: Read-only queries for node states
# CREATED: 04 FEB 2026
# ============================================================================
"""
Node Query Repository

Read-only queries for node states.
"""

from typing import Any, Dict, List, Optional

from function.repositories.base import FunctionRepository


class NodeQueryRepository(FunctionRepository):
    """
    Read-only repository for node state queries.

    Provides:
    - Get node by ID
    - Get all nodes for a job
    - Get nodes by status
    - Node statistics
    """

    TABLE = "dagapp.dag_node_states"

    def get_node(self, job_id: str, node_id: str) -> Optional[Dict[str, Any]]:
        """
        Get node by composite PK (job_id, node_id).

        Args:
            job_id: Parent job ID
            node_id: Node identifier within the job

        Returns:
            Node dict or None if not found
        """
        query = f"""
            SELECT
                node_id, job_id, status, task_id,
                input_params, output,
                error_message, retry_count, max_retries,
                created_at, dispatched_at, started_at, completed_at, updated_at,
                version
            FROM {self.TABLE}
            WHERE job_id = %s AND node_id = %s
        """
        return self.execute_one(query, (job_id, node_id))

    def get_nodes_for_job(self, job_id: str) -> List[Dict[str, Any]]:
        """
        Get all nodes for a job.

        Args:
            job_id: Parent job ID

        Returns:
            List of node dicts ordered by creation time
        """
        query = f"""
            SELECT
                node_id, job_id, status, task_id,
                input_params, output,
                error_message, retry_count,
                created_at, dispatched_at, started_at, completed_at
            FROM {self.TABLE}
            WHERE job_id = %s
            ORDER BY created_at ASC
        """
        return self.execute_many(query, (job_id,))

    def get_nodes_by_status(self, job_id: str, status: str) -> List[Dict[str, Any]]:
        """
        Get nodes with specific status for a job.

        Args:
            job_id: Parent job ID
            status: Node status to filter by

        Returns:
            List of matching node dicts
        """
        query = f"""
            SELECT
                node_id, job_id, status, task_id,
                error_message, retry_count,
                created_at, dispatched_at, started_at, completed_at
            FROM {self.TABLE}
            WHERE job_id = %s AND status = %s
            ORDER BY created_at ASC
        """
        return self.execute_many(query, (job_id, status))

    def count_nodes_by_status(self, job_id: str) -> Dict[str, int]:
        """
        Get node counts by status for a job.

        Args:
            job_id: Parent job ID

        Returns:
            Dict mapping status to count
        """
        query = f"""
            SELECT status, COUNT(*) as count
            FROM {self.TABLE}
            WHERE job_id = %s
            GROUP BY status
        """
        rows = self.execute_many(query, (job_id,))
        return {row["status"]: row["count"] for row in rows}

    def get_running_nodes(self, job_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get currently running nodes.

        Args:
            job_id: Optional filter by job

        Returns:
            List of running node dicts
        """
        if job_id:
            query = f"""
                SELECT
                    node_id, job_id, status, task_id,
                    started_at
                FROM {self.TABLE}
                WHERE job_id = %s AND status = 'running'
                ORDER BY started_at ASC
            """
            return self.execute_many(query, (job_id,))
        else:
            query = f"""
                SELECT
                    node_id, job_id, status, task_id,
                    started_at
                FROM {self.TABLE}
                WHERE status = 'running'
                ORDER BY started_at ASC
            """
            return self.execute_many(query)

    def get_failed_nodes(self, job_id: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Get failed nodes.

        Args:
            job_id: Optional filter by job
            limit: Maximum results

        Returns:
            List of failed node dicts
        """
        if job_id:
            query = f"""
                SELECT
                    node_id, job_id, status, task_id,
                    error_message, retry_count,
                    started_at, completed_at
                FROM {self.TABLE}
                WHERE job_id = %s AND status = 'failed'
                ORDER BY completed_at DESC
                LIMIT %s
            """
            return self.execute_many(query, (job_id, limit))
        else:
            query = f"""
                SELECT
                    node_id, job_id, status, task_id,
                    error_message, retry_count,
                    started_at, completed_at
                FROM {self.TABLE}
                WHERE status = 'failed'
                ORDER BY completed_at DESC
                LIMIT %s
            """
            return self.execute_many(query, (limit,))


__all__ = ["NodeQueryRepository"]
