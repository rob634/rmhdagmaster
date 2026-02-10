# ============================================================================
# NODE REPOSITORY
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Core - NodeState CRUD operations
# PURPOSE: Database access for dag_node_states table
# CREATED: 29 JAN 2026
# ============================================================================
"""
Node Repository

CRUD operations for DAG node states.
"""

import logging
from datetime import datetime
from typing import List, Optional, Dict, Any

from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Json
from psycopg_pool import AsyncConnectionPool

from core.models import NodeState
from core.contracts import NodeStatus
from .database import TABLE_NODES

logger = logging.getLogger(__name__)


class NodeRepository:
    """Repository for NodeState entities."""

    def __init__(self, pool: AsyncConnectionPool):
        self.pool = pool

    async def create(self, node: NodeState) -> NodeState:
        """
        Create a new node state.

        Args:
            node: NodeState instance to persist

        Returns:
            Created node state
        """
        async with self.pool.connection() as conn:
            await conn.execute(
                sql.SQL("""
                INSERT INTO {} (
                    job_id, node_id, status, task_id, output, error_message,
                    retry_count, max_retries, created_at, dispatched_at,
                    started_at, completed_at, updated_at,
                    parent_node_id, fan_out_index, input_params
                ) VALUES (
                    %(job_id)s, %(node_id)s, %(status)s, %(task_id)s,
                    %(output)s, %(error_message)s, %(retry_count)s, %(max_retries)s,
                    %(created_at)s, %(dispatched_at)s, %(started_at)s,
                    %(completed_at)s, %(updated_at)s,
                    %(parent_node_id)s, %(fan_out_index)s, %(input_params)s
                )
                """).format(TABLE_NODES),
                {
                    "job_id": node.job_id,
                    "node_id": node.node_id,
                    "status": node.status.value,
                    "task_id": node.task_id,
                    "output": Json(node.output) if node.output else None,
                    "error_message": node.error_message,
                    "retry_count": node.retry_count,
                    "max_retries": node.max_retries,
                    "created_at": node.created_at,
                    "dispatched_at": node.dispatched_at,
                    "started_at": node.started_at,
                    "completed_at": node.completed_at,
                    "updated_at": node.updated_at,
                    "parent_node_id": node.parent_node_id,
                    "fan_out_index": node.fan_out_index,
                    "input_params": Json(node.input_params) if node.input_params else None,
                },
            )
            logger.debug(f"Created node {node.node_id} for job {node.job_id}")
            return node

    async def create_many(self, nodes: List[NodeState]) -> List[NodeState]:
        """
        Create multiple node states in a single transaction.

        Args:
            nodes: List of NodeState instances

        Returns:
            List of created nodes
        """
        if not nodes:
            return []

        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                for node in nodes:
                    await cur.execute(
                        sql.SQL("""
                        INSERT INTO {} (
                            job_id, node_id, status, task_id, output, error_message,
                            retry_count, max_retries, created_at, dispatched_at,
                            started_at, completed_at, updated_at,
                            parent_node_id, fan_out_index, input_params
                        ) VALUES (
                            %(job_id)s, %(node_id)s, %(status)s, %(task_id)s,
                            %(output)s, %(error_message)s, %(retry_count)s, %(max_retries)s,
                            %(created_at)s, %(dispatched_at)s, %(started_at)s,
                            %(completed_at)s, %(updated_at)s,
                            %(parent_node_id)s, %(fan_out_index)s, %(input_params)s
                        )
                        """).format(TABLE_NODES),
                        {
                            "job_id": node.job_id,
                            "node_id": node.node_id,
                            "status": node.status.value,
                            "task_id": node.task_id,
                            "output": Json(node.output) if node.output else None,
                            "error_message": node.error_message,
                            "retry_count": node.retry_count,
                            "max_retries": node.max_retries,
                            "created_at": node.created_at,
                            "dispatched_at": node.dispatched_at,
                            "started_at": node.started_at,
                            "completed_at": node.completed_at,
                            "updated_at": node.updated_at,
                            "parent_node_id": node.parent_node_id,
                            "fan_out_index": node.fan_out_index,
                            "input_params": Json(node.input_params) if node.input_params else None,
                        },
                    )
            logger.info(f"Created {len(nodes)} nodes for job {nodes[0].job_id}")
            return nodes

    async def get(self, job_id: str, node_id: str) -> Optional[NodeState]:
        """
        Get a node state by job_id and node_id.

        Args:
            job_id: Job identifier
            node_id: Node identifier

        Returns:
            NodeState instance or None
        """
        async with self.pool.connection() as conn:
            conn.row_factory = dict_row
            result = await conn.execute(
                sql.SQL("SELECT * FROM {} WHERE job_id = %s AND node_id = %s").format(TABLE_NODES),
                (job_id, node_id),
            )
            row = await result.fetchone()

            if row is None:
                return None

            return self._row_to_node(row)

    async def get_all_for_job(self, job_id: str) -> List[NodeState]:
        """
        Get all node states for a job.

        Args:
            job_id: Job identifier

        Returns:
            List of NodeState instances
        """
        async with self.pool.connection() as conn:
            conn.row_factory = dict_row
            result = await conn.execute(
                sql.SQL("SELECT * FROM {} WHERE job_id = %s ORDER BY created_at").format(TABLE_NODES),
                (job_id,),
            )
            rows = await result.fetchall()
            return [self._row_to_node(row) for row in rows]

    async def update(self, node: NodeState) -> bool:
        """
        Update a node state with optimistic locking.

        Uses version column for optimistic locking. If the version in the
        database doesn't match the expected version, the update fails and
        returns False (indicating a concurrent modification).

        Args:
            node: NodeState with updated fields

        Returns:
            True if update succeeded, False if version conflict
        """
        async with self.pool.connection() as conn:
            result = await conn.execute(
                sql.SQL("""
                UPDATE {} SET
                    status = %(status)s,
                    task_id = %(task_id)s,
                    output = %(output)s,
                    error_message = %(error_message)s,
                    retry_count = %(retry_count)s,
                    started_at = %(started_at)s,
                    completed_at = %(completed_at)s,
                    version = version + 1
                WHERE job_id = %(job_id)s
                  AND node_id = %(node_id)s
                  AND version = %(version)s
                """).format(TABLE_NODES),
                {
                    "job_id": node.job_id,
                    "node_id": node.node_id,
                    "status": node.status.value,
                    "task_id": node.task_id,
                    "output": Json(node.output) if node.output else None,
                    "error_message": node.error_message,
                    "retry_count": node.retry_count,
                    "started_at": node.started_at,
                    "completed_at": node.completed_at,
                    "version": node.version,
                },
            )

            if result.rowcount == 0:
                logger.warning(
                    f"Version conflict updating node {node.node_id} "
                    f"(expected version {node.version})"
                )
                return False

            # Update local version to match DB
            node.version += 1
            logger.debug(
                f"Updated node {node.node_id} status={node.status.value} "
                f"version={node.version}"
            )
            return True

    async def get_ready_nodes(self, job_id: str) -> List[NodeState]:
        """
        Get nodes that are ready for dispatch.

        Args:
            job_id: Job identifier

        Returns:
            List of READY nodes
        """
        async with self.pool.connection() as conn:
            conn.row_factory = dict_row
            result = await conn.execute(
                sql.SQL("""
                SELECT * FROM {}
                WHERE job_id = %s AND status = 'ready'
                ORDER BY created_at
                """).format(TABLE_NODES),
                (job_id,),
            )
            rows = await result.fetchall()
            return [self._row_to_node(row) for row in rows]

    async def get_pending_nodes(self, job_id: str) -> List[NodeState]:
        """
        Get nodes that are pending (waiting for dependencies).

        Args:
            job_id: Job identifier

        Returns:
            List of PENDING nodes
        """
        async with self.pool.connection() as conn:
            conn.row_factory = dict_row
            result = await conn.execute(
                sql.SQL("""
                SELECT * FROM {}
                WHERE job_id = %s AND status = 'pending'
                ORDER BY created_at
                """).format(TABLE_NODES),
                (job_id,),
            )
            rows = await result.fetchall()
            return [self._row_to_node(row) for row in rows]

    async def count_by_status(self, job_id: str) -> Dict[str, int]:
        """
        Count nodes by status for a job.

        Args:
            job_id: Job identifier

        Returns:
            Dict mapping status to count
        """
        async with self.pool.connection() as conn:
            conn.row_factory = dict_row
            result = await conn.execute(
                sql.SQL("""
                SELECT status, COUNT(*) as count
                FROM {}
                WHERE job_id = %s
                GROUP BY status
                """).format(TABLE_NODES),
                (job_id,),
            )
            rows = await result.fetchall()
            return {row["status"]: row["count"] for row in rows}

    async def all_nodes_terminal(self, job_id: str) -> bool:
        """
        Check if all nodes for a job are in terminal state.

        Args:
            job_id: Job identifier

        Returns:
            True if all nodes are completed, failed, or skipped
        """
        async with self.pool.connection() as conn:
            conn.row_factory = dict_row
            result = await conn.execute(
                sql.SQL("""
                SELECT COUNT(*) as count FROM {}
                WHERE job_id = %s
                AND status NOT IN ('completed', 'failed', 'skipped')
                """).format(TABLE_NODES),
                (job_id,),
            )
            row = await result.fetchone()
            return row["count"] == 0

    async def any_node_failed(self, job_id: str) -> bool:
        """
        Check if any node has failed for a job.

        Args:
            job_id: Job identifier

        Returns:
            True if any node is in FAILED status
        """
        async with self.pool.connection() as conn:
            conn.row_factory = dict_row
            result = await conn.execute(
                sql.SQL("""
                SELECT 1 as failed FROM {}
                WHERE job_id = %s AND status = 'failed'
                LIMIT 1
                """).format(TABLE_NODES),
                (job_id,),
            )
            return await result.fetchone() is not None

    async def mark_dispatched(
        self,
        job_id: str,
        node_id: str,
        task_id: str,
    ) -> bool:
        """
        Mark a node as dispatched with task ID.

        Args:
            job_id: Job identifier
            node_id: Node identifier
            task_id: Assigned task ID

        Returns:
            True if update succeeded
        """
        now = datetime.utcnow()
        async with self.pool.connection() as conn:
            result = await conn.execute(
                sql.SQL("""
                UPDATE {}
                SET status = 'dispatched', task_id = %s,
                    dispatched_at = %s, updated_at = %s
                WHERE job_id = %s AND node_id = %s AND status = 'ready'
                """).format(TABLE_NODES),
                (task_id, now, now, job_id, node_id),
            )
            return result.rowcount > 0

    async def list_by_status(
        self,
        status: Optional[NodeStatus] = None,
        limit: int = 100,
    ) -> List[NodeState]:
        """
        List nodes across all jobs, optionally filtered by status.

        Args:
            status: Optional status filter
            limit: Maximum number of nodes to return

        Returns:
            List of NodeState instances, ordered by most recent first
        """
        async with self.pool.connection() as conn:
            conn.row_factory = dict_row
            if status:
                result = await conn.execute(
                    sql.SQL("""
                    SELECT * FROM {}
                    WHERE status = %s
                    ORDER BY updated_at DESC
                    LIMIT %s
                    """).format(TABLE_NODES),
                    (status.value, limit),
                )
            else:
                result = await conn.execute(
                    sql.SQL("""
                    SELECT * FROM {}
                    ORDER BY updated_at DESC
                    LIMIT %s
                    """).format(TABLE_NODES),
                    (limit,),
                )
            rows = await result.fetchall()
            return [self._row_to_node(row) for row in rows]

    async def list_active(self, limit: int = 100) -> List[NodeState]:
        """
        List active (non-terminal) nodes across all jobs.

        Args:
            limit: Maximum number of nodes to return

        Returns:
            List of active NodeState instances
        """
        async with self.pool.connection() as conn:
            conn.row_factory = dict_row
            result = await conn.execute(
                sql.SQL("""
                SELECT * FROM {}
                WHERE status NOT IN ('completed', 'failed', 'skipped')
                ORDER BY updated_at DESC
                LIMIT %s
                """).format(TABLE_NODES),
                (limit,),
            )
            rows = await result.fetchall()
            return [self._row_to_node(row) for row in rows]

    async def get_status_counts(self) -> Dict[str, int]:
        """
        Get counts of nodes by status across all jobs.

        Returns:
            Dict mapping status to count
        """
        async with self.pool.connection() as conn:
            conn.row_factory = dict_row
            result = await conn.execute(
                sql.SQL("""
                SELECT status, COUNT(*) as count
                FROM {}
                GROUP BY status
                """).format(TABLE_NODES),
            )
            rows = await result.fetchall()
            return {row["status"]: row["count"] for row in rows}

    async def get_children_by_parent(
        self,
        job_id: str,
        parent_node_id: str,
    ) -> List[NodeState]:
        """
        Get all dynamic child nodes created by a fan-out node.

        Args:
            job_id: Job identifier
            parent_node_id: The fan-out node that created these children

        Returns:
            List of child NodeState instances ordered by fan_out_index
        """
        async with self.pool.connection() as conn:
            conn.row_factory = dict_row
            result = await conn.execute(
                sql.SQL("""
                SELECT * FROM {}
                WHERE job_id = %s AND parent_node_id = %s
                ORDER BY fan_out_index
                """).format(TABLE_NODES),
                (job_id, parent_node_id),
            )
            rows = await result.fetchall()
            return [self._row_to_node(row) for row in rows]

    async def all_children_terminal(
        self,
        job_id: str,
        parent_node_id: str,
    ) -> bool:
        """
        Check if all dynamic children of a fan-out node are terminal.

        Args:
            job_id: Job identifier
            parent_node_id: The fan-out parent node

        Returns:
            True if all children are completed/failed/skipped
        """
        async with self.pool.connection() as conn:
            conn.row_factory = dict_row
            result = await conn.execute(
                sql.SQL("""
                SELECT COUNT(*) as count FROM {}
                WHERE job_id = %s AND parent_node_id = %s
                AND status NOT IN ('completed', 'failed', 'skipped')
                """).format(TABLE_NODES),
                (job_id, parent_node_id),
            )
            row = await result.fetchone()
            return row["count"] == 0

    def _row_to_node(self, row: Dict[str, Any]) -> NodeState:
        """Convert database row to NodeState model."""
        return NodeState(
            job_id=row["job_id"],
            node_id=row["node_id"],
            status=NodeStatus(row["status"]),
            task_id=row.get("task_id"),
            output=row.get("output"),
            error_message=row.get("error_message"),
            retry_count=row.get("retry_count", 0),
            max_retries=row.get("max_retries", 3),
            created_at=row["created_at"],
            dispatched_at=row.get("dispatched_at"),
            started_at=row.get("started_at"),
            completed_at=row.get("completed_at"),
            updated_at=row["updated_at"],
            parent_node_id=row.get("parent_node_id"),
            fan_out_index=row.get("fan_out_index"),
            input_params=row.get("input_params"),
            version=row.get("version", 1),
        )
