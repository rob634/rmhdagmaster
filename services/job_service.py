# ============================================================================
# JOB SERVICE
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Core - Job lifecycle management
# PURPOSE: Create jobs, track status, manage completion
# CREATED: 29 JAN 2026
# ============================================================================
"""
Job Service

Manages job lifecycle:
- Create job from workflow submission
- Instantiate node states from workflow definition
- Track job completion
- Handle job failure
"""

import hashlib
import logging
from datetime import datetime
from typing import Dict, Any, Optional, List

from psycopg_pool import AsyncConnectionPool

from core.models import Job, NodeState, WorkflowDefinition
from core.contracts import JobStatus, NodeStatus, NodeData
from repositories import JobRepository, NodeRepository
from .workflow_service import WorkflowService

logger = logging.getLogger(__name__)


class JobService:
    """Service for job lifecycle management."""

    def __init__(
        self,
        pool: AsyncConnectionPool,
        workflow_service: WorkflowService,
        event_service: "EventService" = None,
    ):
        """
        Initialize job service.

        Args:
            pool: Database connection pool
            workflow_service: Workflow definition service
            event_service: Optional event service for timeline logging
        """
        self.pool = pool
        self.workflow_service = workflow_service
        self.job_repo = JobRepository(pool)
        self.node_repo = NodeRepository(pool)
        self._event_service = event_service

    async def create_job(
        self,
        workflow_id: str,
        input_params: Dict[str, Any],
        submitted_by: Optional[str] = None,
        correlation_id: Optional[str] = None,
        idempotency_key: Optional[str] = None,
    ) -> Job:
        """
        Create a new job from a workflow.

        Args:
            workflow_id: Workflow to execute
            input_params: Input parameters for the workflow
            submitted_by: User or system submitting the job
            correlation_id: External correlation ID
            idempotency_key: Optional key for idempotent job creation

        Returns:
            Created Job instance

        Raises:
            KeyError if workflow not found
        """
        # Load workflow definition
        workflow = self.workflow_service.get_or_raise(workflow_id)

        # Generate job ID (idempotent based on key or hash of params)
        job_id = self._generate_job_id(
            workflow_id=workflow_id,
            input_params=input_params,
            idempotency_key=idempotency_key,
        )

        # Check for existing job with same ID
        existing = await self.job_repo.get(job_id)
        if existing:
            logger.info(f"Returning existing job {job_id}")
            return existing

        # Create job with pinned workflow version + snapshot
        job = Job(
            job_id=job_id,
            workflow_id=workflow_id,
            workflow_version=workflow.version,
            workflow_snapshot=workflow.model_dump(mode="json"),
            status=JobStatus.PENDING,
            input_params=input_params,
            submitted_by=submitted_by,
            correlation_id=correlation_id,
        )

        await self.job_repo.create(job)

        # Create node states from workflow
        nodes = self._create_node_states(job_id, workflow, input_params)
        await self.node_repo.create_many(nodes)

        logger.info(
            f"Created job {job_id} with {len(nodes)} nodes "
            f"for workflow {workflow_id}"
        )

        # Emit JOB_CREATED event
        if self._event_service:
            await self._event_service.emit_job_created(job, workflow_id)

        return job

    async def get_job(self, job_id: str) -> Optional[Job]:
        """Get a job by ID."""
        return await self.job_repo.get(job_id)

    async def get_job_with_nodes(self, job_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a job with all its node states.

        Returns:
            Dict with 'job' and 'nodes' keys, or None if not found
        """
        job = await self.job_repo.get(job_id)
        if job is None:
            return None

        nodes = await self.node_repo.get_all_for_job(job_id)

        return {
            "job": job,
            "nodes": nodes,
        }

    async def list_active_jobs(self, limit: int = 100) -> List[Job]:
        """List jobs that are not in terminal state."""
        return await self.job_repo.list_active(limit)

    async def mark_job_started(self, job_id: str) -> bool:
        """
        Mark a job as started (first node dispatched).

        Args:
            job_id: Job identifier

        Returns:
            True if update succeeded
        """
        return await self.job_repo.update_status(job_id, JobStatus.RUNNING)

    async def check_job_completion(self, job_id: str) -> Optional[JobStatus]:
        """
        Check if a job should transition to terminal state.

        Called after processing task results.

        Args:
            job_id: Job identifier

        Returns:
            New status if job should transition, None otherwise
        """
        # Check if all nodes are terminal
        all_terminal = await self.node_repo.all_nodes_terminal(job_id)
        if not all_terminal:
            return None

        # Check if any failed
        any_failed = await self.node_repo.any_node_failed(job_id)
        if any_failed:
            return JobStatus.FAILED

        return JobStatus.COMPLETED

    async def complete_job(
        self,
        job_id: str,
        result_data: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """
        Mark a job as completed.

        Args:
            job_id: Job identifier
            result_data: Optional result data

        Returns:
            True if update succeeded
        """
        success = await self.job_repo.update_status(
            job_id=job_id,
            status=JobStatus.COMPLETED,
            result_data=result_data,
        )
        if success:
            logger.info(f"Job {job_id} completed successfully")

            # Emit JOB_COMPLETED event
            if self._event_service:
                # Get job for duration calculation
                job = await self.job_repo.get(job_id)
                duration = job.duration_seconds if job else None
                node_count = len(result_data) if result_data else None
                await self._event_service.emit_job_completed(
                    job_id, duration, node_count
                )

        return success

    async def fail_job(self, job_id: str, error_message: str) -> bool:
        """
        Mark a job as failed.

        Args:
            job_id: Job identifier
            error_message: Error details

        Returns:
            True if update succeeded
        """
        success = await self.job_repo.update_status(
            job_id=job_id,
            status=JobStatus.FAILED,
            error_message=error_message,
        )
        if success:
            logger.error(f"Job {job_id} failed: {error_message}")

            # Emit JOB_FAILED event
            if self._event_service:
                await self._event_service.emit_job_failed(job_id, error_message)

        return success

    async def cancel_job(self, job_id: str) -> bool:
        """
        Cancel a job.

        Args:
            job_id: Job identifier

        Returns:
            True if cancellation succeeded
        """
        job = await self.job_repo.get(job_id)
        if job is None:
            return False

        if job.status.is_terminal():
            logger.warning(f"Cannot cancel terminal job {job_id}")
            return False

        success = await self.job_repo.update_status(
            job_id=job_id,
            status=JobStatus.CANCELLED,
        )
        if success:
            logger.info(f"Job {job_id} cancelled")

            # Emit JOB_CANCELLED event
            if self._event_service:
                await self._event_service.emit_job_cancelled(job_id)

        return success

    def _generate_job_id(
        self,
        workflow_id: str,
        input_params: Dict[str, Any],
        idempotency_key: Optional[str] = None,
    ) -> str:
        """
        Generate a job ID.

        If idempotency_key is provided, uses that.
        Otherwise generates SHA256 hash of workflow + params + timestamp.
        """
        if idempotency_key:
            # Hash the idempotency key for consistent format
            return hashlib.sha256(
                idempotency_key.encode()
            ).hexdigest()[:32]

        # Generate unique ID with timestamp
        timestamp = datetime.utcnow().isoformat()
        content = f"{workflow_id}:{timestamp}:{str(input_params)}"
        return hashlib.sha256(content.encode()).hexdigest()[:32]

    def _create_node_states(
        self,
        job_id: str,
        workflow: WorkflowDefinition,
        input_params: Dict[str, Any],
    ) -> List[NodeState]:
        """
        Create node states from workflow definition.

        START node is created as READY (no dependencies).
        All other nodes are created as PENDING.
        """
        nodes = []

        for node_id, node_def in workflow.nodes.items():
            # START nodes are immediately READY
            # All others wait for dependencies
            if node_def.type.value == "start":
                status = NodeStatus.READY
            else:
                status = NodeStatus.PENDING

            # Get retry config from node definition
            max_retries = 3
            if node_def.retry:
                max_retries = node_def.retry.max_attempts

            node = NodeState(
                job_id=job_id,
                node_id=node_id,
                status=status,
                max_retries=max_retries,
            )
            nodes.append(node)

        return nodes
