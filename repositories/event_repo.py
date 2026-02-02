# ============================================================================
# EVENT REPOSITORY
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Core - JobEvent CRUD operations
# PURPOSE: Database access for dag_job_events table
# CREATED: 02 FEB 2026
# ============================================================================
"""
Event Repository

CRUD operations for job execution events.
Events provide an audit trail and debugging timeline.
"""

import logging
from typing import List, Optional

from psycopg.rows import dict_row
from psycopg.types.json import Json
from psycopg_pool import AsyncConnectionPool

from core.models import JobEvent
from core.models.events import EventType, EventStatus
from .database import TABLE_EVENTS

logger = logging.getLogger(__name__)


class EventRepository:
    """Repository for JobEvent entities."""

    def __init__(self, pool: AsyncConnectionPool):
        self.pool = pool

    async def create(self, event: JobEvent) -> JobEvent:
        """
        Create a new event.

        Args:
            event: JobEvent instance to persist

        Returns:
            Created event with event_id populated
        """
        async with self.pool.connection() as conn:
            conn.row_factory = dict_row
            result = await conn.execute(
                f"""
                INSERT INTO {TABLE_EVENTS} (
                    job_id, node_id, task_id, event_type, event_status,
                    checkpoint_name, event_data, error_message, duration_ms,
                    created_at, source_app
                ) VALUES (
                    %(job_id)s, %(node_id)s, %(task_id)s, %(event_type)s,
                    %(event_status)s, %(checkpoint_name)s, %(event_data)s,
                    %(error_message)s, %(duration_ms)s, %(created_at)s,
                    %(source_app)s
                )
                RETURNING event_id
                """,
                {
                    "job_id": event.job_id,
                    "node_id": event.node_id,
                    "task_id": event.task_id,
                    "event_type": event.event_type.value,
                    "event_status": event.event_status.value,
                    "checkpoint_name": event.checkpoint_name,
                    "event_data": Json(event.event_data) if event.event_data else None,
                    "error_message": event.error_message,
                    "duration_ms": event.duration_ms,
                    "created_at": event.created_at,
                    "source_app": event.source_app,
                },
            )
            row = await result.fetchone()
            event.event_id = row["event_id"]
            return event

    async def get_for_job(
        self,
        job_id: str,
        limit: int = 100,
        event_types: Optional[List[EventType]] = None,
    ) -> List[JobEvent]:
        """
        Get all events for a job.

        Args:
            job_id: Job identifier
            limit: Maximum number of events to return
            event_types: Optional filter by event types

        Returns:
            List of JobEvent instances, newest first
        """
        async with self.pool.connection() as conn:
            conn.row_factory = dict_row

            if event_types:
                type_values = [t.value for t in event_types]
                result = await conn.execute(
                    f"""
                    SELECT * FROM {TABLE_EVENTS}
                    WHERE job_id = %s AND event_type = ANY(%s)
                    ORDER BY created_at DESC
                    LIMIT %s
                    """,
                    (job_id, type_values, limit),
                )
            else:
                result = await conn.execute(
                    f"""
                    SELECT * FROM {TABLE_EVENTS}
                    WHERE job_id = %s
                    ORDER BY created_at DESC
                    LIMIT %s
                    """,
                    (job_id, limit),
                )

            rows = await result.fetchall()
            return [self._row_to_event(row) for row in rows]

    async def get_for_node(
        self,
        job_id: str,
        node_id: str,
        limit: int = 50,
    ) -> List[JobEvent]:
        """
        Get events for a specific node.

        Args:
            job_id: Job identifier
            node_id: Node identifier
            limit: Maximum number of events

        Returns:
            List of JobEvent instances for the node
        """
        async with self.pool.connection() as conn:
            conn.row_factory = dict_row
            result = await conn.execute(
                f"""
                SELECT * FROM {TABLE_EVENTS}
                WHERE job_id = %s AND node_id = %s
                ORDER BY created_at DESC
                LIMIT %s
                """,
                (job_id, node_id, limit),
            )
            rows = await result.fetchall()
            return [self._row_to_event(row) for row in rows]

    async def get_timeline(
        self,
        job_id: str,
        limit: int = 100,
    ) -> List[JobEvent]:
        """
        Get chronological timeline for a job.

        Args:
            job_id: Job identifier
            limit: Maximum events

        Returns:
            List of JobEvent instances in chronological order (oldest first)
        """
        async with self.pool.connection() as conn:
            conn.row_factory = dict_row
            result = await conn.execute(
                f"""
                SELECT * FROM {TABLE_EVENTS}
                WHERE job_id = %s
                ORDER BY created_at ASC
                LIMIT %s
                """,
                (job_id, limit),
            )
            rows = await result.fetchall()
            return [self._row_to_event(row) for row in rows]

    def _row_to_event(self, row: dict) -> JobEvent:
        """Convert database row to JobEvent model."""
        return JobEvent(
            event_id=row["event_id"],
            job_id=row["job_id"],
            node_id=row.get("node_id"),
            task_id=row.get("task_id"),
            event_type=EventType(row["event_type"]),
            event_status=EventStatus(row["event_status"]),
            checkpoint_name=row.get("checkpoint_name"),
            event_data=row.get("event_data") or {},
            error_message=row.get("error_message"),
            duration_ms=row.get("duration_ms"),
            created_at=row["created_at"],
            source_app=row.get("source_app"),
        )
