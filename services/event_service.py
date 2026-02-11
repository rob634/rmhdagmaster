# ============================================================================
# EVENT SERVICE
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Core - Event emission and retrieval
# PURPOSE: Emit lifecycle events for debugging and auditing
# CREATED: 02 FEB 2026
# ============================================================================
"""
Event Service

Provides methods to emit events at key lifecycle points.
Events are fire-and-forget - failures are logged but don't propagate.

This enables:
- Audit trail for compliance
- Debugging silent failures ("what was the last checkpoint?")
- Performance analysis
- Application Insights correlation
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from psycopg_pool import AsyncConnectionPool

from core.models import JobEvent, Job, NodeState
from core.models.events import EventType, EventStatus
from repositories import EventRepository

logger = logging.getLogger(__name__)

# Source identifier for events from orchestrator
SOURCE_ORCHESTRATOR = "orchestrator"
SOURCE_WORKER = "worker"


class EventService:
    """Service for emitting and retrieving job events."""

    def __init__(self, pool: AsyncConnectionPool):
        """
        Initialize event service.

        Args:
            pool: Database connection pool
        """
        self.pool = pool
        self._repo = EventRepository(pool)

    # =========================================================================
    # CORE EMIT METHOD
    # =========================================================================

    async def emit(
        self,
        event_type: EventType,
        job_id: str,
        node_id: Optional[str] = None,
        task_id: Optional[str] = None,
        status: EventStatus = EventStatus.INFO,
        data: Optional[Dict[str, Any]] = None,
        error_message: Optional[str] = None,
        duration_ms: Optional[int] = None,
        checkpoint_name: Optional[str] = None,
        source_app: str = SOURCE_ORCHESTRATOR,
    ) -> Optional[JobEvent]:
        """
        Emit an event. Fire-and-forget - logs errors but doesn't raise.

        Args:
            event_type: Type of event
            job_id: Job identifier
            node_id: Optional node identifier
            task_id: Optional task identifier
            status: Event status/severity
            data: Additional event data
            error_message: Error details if applicable
            duration_ms: Duration of operation if applicable
            checkpoint_name: Named checkpoint for correlation
            source_app: Source application identifier

        Returns:
            Created JobEvent or None if emission failed
        """
        try:
            event = JobEvent(
                job_id=job_id,
                node_id=node_id,
                task_id=task_id,
                event_type=event_type,
                event_status=status,
                event_data=data or {},
                error_message=error_message,
                duration_ms=duration_ms,
                checkpoint_name=checkpoint_name,
                source_app=source_app,
            )

            created = await self._repo.create(event)

            logger.debug(
                f"Event emitted: {event_type.value} for job={job_id}"
                + (f", node={node_id}" if node_id else "")
            )

            return created

        except Exception as e:
            # Fire-and-forget - log but don't raise
            logger.warning(
                f"Failed to emit event {event_type.value} for job {job_id}: {e}"
            )
            return None

    # =========================================================================
    # JOB LIFECYCLE EVENTS
    # =========================================================================

    async def emit_job_created(
        self,
        job: Job,
        workflow_id: str,
    ) -> None:
        """Emit JOB_CREATED event."""
        await self.emit(
            event_type=EventType.JOB_CREATED,
            job_id=job.job_id,
            status=EventStatus.SUCCESS,
            data={
                "workflow_id": workflow_id,
                "workflow_version": job.workflow_version,
                "submitted_by": job.submitted_by,
                "correlation_id": job.correlation_id,
            },
            checkpoint_name="job_created",
        )

    async def emit_job_started(
        self,
        job_id: str,
        first_node_id: Optional[str] = None,
    ) -> None:
        """Emit JOB_STARTED event."""
        await self.emit(
            event_type=EventType.JOB_STARTED,
            job_id=job_id,
            status=EventStatus.SUCCESS,
            data={"first_node": first_node_id} if first_node_id else {},
            checkpoint_name="job_started",
        )

    async def emit_job_completed(
        self,
        job_id: str,
        duration_seconds: Optional[float] = None,
        node_count: Optional[int] = None,
    ) -> None:
        """Emit JOB_COMPLETED event."""
        await self.emit(
            event_type=EventType.JOB_COMPLETED,
            job_id=job_id,
            status=EventStatus.SUCCESS,
            data={
                "duration_seconds": duration_seconds,
                "node_count": node_count,
            },
            duration_ms=int(duration_seconds * 1000) if duration_seconds else None,
            checkpoint_name="job_completed",
        )

    async def emit_job_failed(
        self,
        job_id: str,
        error_message: str,
        failed_nodes: Optional[List[str]] = None,
    ) -> None:
        """Emit JOB_FAILED event."""
        await self.emit(
            event_type=EventType.JOB_FAILED,
            job_id=job_id,
            status=EventStatus.FAILURE,
            data={"failed_nodes": failed_nodes or []},
            error_message=error_message,
            checkpoint_name="job_failed",
        )

    async def emit_job_cancelled(
        self,
        job_id: str,
        cancelled_by: Optional[str] = None,
    ) -> None:
        """Emit JOB_CANCELLED event."""
        await self.emit(
            event_type=EventType.JOB_CANCELLED,
            job_id=job_id,
            status=EventStatus.WARNING,
            data={"cancelled_by": cancelled_by} if cancelled_by else {},
            checkpoint_name="job_cancelled",
        )

    # =========================================================================
    # NODE LIFECYCLE EVENTS
    # =========================================================================

    async def emit_node_ready(
        self,
        node: NodeState,
        dependencies_met: Optional[List[str]] = None,
    ) -> None:
        """Emit NODE_READY event."""
        await self.emit(
            event_type=EventType.NODE_READY,
            job_id=node.job_id,
            node_id=node.node_id,
            status=EventStatus.INFO,
            data={"dependencies_met": dependencies_met or []},
        )

    async def emit_node_dispatched(
        self,
        node: NodeState,
        task_id: str,
        queue: str,
        handler: Optional[str] = None,
    ) -> None:
        """Emit NODE_DISPATCHED event."""
        await self.emit(
            event_type=EventType.NODE_DISPATCHED,
            job_id=node.job_id,
            node_id=node.node_id,
            task_id=task_id,
            status=EventStatus.SUCCESS,
            data={
                "queue": queue,
                "handler": handler,
            },
            checkpoint_name=f"node_dispatched_{node.node_id}",
        )

    async def emit_node_started(
        self,
        job_id: str,
        node_id: str,
        task_id: str,
        worker_id: Optional[str] = None,
    ) -> None:
        """Emit NODE_STARTED event (when worker begins execution)."""
        await self.emit(
            event_type=EventType.NODE_STARTED,
            job_id=job_id,
            node_id=node_id,
            task_id=task_id,
            status=EventStatus.INFO,
            data={"worker_id": worker_id} if worker_id else {},
            source_app=SOURCE_WORKER,
        )

    async def emit_node_completed(
        self,
        node: NodeState,
        duration_ms: Optional[int] = None,
        output_keys: Optional[List[str]] = None,
    ) -> None:
        """Emit NODE_COMPLETED event."""
        await self.emit(
            event_type=EventType.NODE_COMPLETED,
            job_id=node.job_id,
            node_id=node.node_id,
            task_id=node.task_id,
            status=EventStatus.SUCCESS,
            data={"output_keys": output_keys or []},
            duration_ms=duration_ms,
            checkpoint_name=f"node_completed_{node.node_id}",
        )

    async def emit_node_failed(
        self,
        node: NodeState,
        error_message: str,
        will_retry: bool = False,
    ) -> None:
        """Emit NODE_FAILED event."""
        await self.emit(
            event_type=EventType.NODE_FAILED,
            job_id=node.job_id,
            node_id=node.node_id,
            task_id=node.task_id,
            status=EventStatus.FAILURE,
            data={
                "will_retry": will_retry,
                "retry_count": node.retry_count,
                "max_retries": node.max_retries,
            },
            error_message=error_message,
            checkpoint_name=f"node_failed_{node.node_id}",
        )

    async def emit_node_retry(
        self,
        node: NodeState,
    ) -> None:
        """Emit NODE_RETRY event."""
        await self.emit(
            event_type=EventType.NODE_RETRY,
            job_id=node.job_id,
            node_id=node.node_id,
            task_id=node.task_id,
            status=EventStatus.WARNING,
            data={
                "retry_count": node.retry_count,
                "max_retries": node.max_retries,
            },
        )

    async def emit_node_skipped(
        self,
        node: NodeState,
        reason: str = "conditional_branch",
    ) -> None:
        """Emit NODE_SKIPPED event."""
        await self.emit(
            event_type=EventType.NODE_SKIPPED,
            job_id=node.job_id,
            node_id=node.node_id,
            status=EventStatus.INFO,
            data={"reason": reason},
        )

    # =========================================================================
    # RETRIEVAL METHODS
    # =========================================================================

    async def get_job_timeline(
        self,
        job_id: str,
        limit: int = 100,
    ) -> List[JobEvent]:
        """
        Get chronological event timeline for a job.

        Args:
            job_id: Job identifier
            limit: Maximum events

        Returns:
            Events in chronological order (oldest first)
        """
        return await self._repo.get_timeline(job_id, limit)

    async def get_job_events(
        self,
        job_id: str,
        limit: int = 100,
        event_types: Optional[List[EventType]] = None,
    ) -> List[JobEvent]:
        """
        Get events for a job.

        Args:
            job_id: Job identifier
            limit: Maximum events
            event_types: Optional filter

        Returns:
            Events in reverse chronological order (newest first)
        """
        return await self._repo.get_for_job(job_id, limit, event_types)

    async def get_node_events(
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
            limit: Maximum events

        Returns:
            Events for the node
        """
        return await self._repo.get_for_node(job_id, node_id, limit)
