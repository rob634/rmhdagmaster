# ============================================================================
# ORCHESTRATION LOOP
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Core - Main orchestration loop
# PURPOSE: Drive DAG execution: dispatch, collect, advance
# CREATED: 29 JAN 2026
# ============================================================================
"""
Orchestration Loop

The main loop that drives DAG execution:
1. Find active jobs
2. Check for ready nodes
3. Dispatch tasks
4. Process results
5. Advance job state
6. Repeat

Runs as a background task in the FastAPI application.
"""

import asyncio
import logging
from datetime import datetime
from typing import Optional, Dict, Any

from psycopg_pool import AsyncConnectionPool

from core.models import Job, NodeState, TaskResult, WorkflowDefinition
from core.contracts import JobStatus, NodeStatus
from services import JobService, NodeService, WorkflowService, EventService
from messaging import TaskPublisher, get_publisher
from infrastructure.locking import LockService

logger = logging.getLogger(__name__)


class Orchestrator:
    """
    Main orchestration loop.

    Coordinates job execution by:
    - Polling for work
    - Dispatching tasks
    - Processing results
    - Managing state transitions
    """

    def __init__(
        self,
        pool: AsyncConnectionPool,
        workflow_service: WorkflowService,
        poll_interval: float = 1.0,
        event_service: "EventService" = None,
    ):
        """
        Initialize orchestrator.

        Args:
            pool: Database connection pool
            workflow_service: Workflow definition service
            poll_interval: Seconds between polling cycles
            event_service: Optional event service for timeline logging
        """
        self.pool = pool
        self.workflow_service = workflow_service
        self.poll_interval = poll_interval
        self._event_service = event_service

        self.job_service = JobService(pool, workflow_service, event_service)
        self.node_service = NodeService(pool, workflow_service, event_service)

        # Locking service for concurrency control
        self.lock_service = LockService(pool)
        self._has_orchestrator_lock = False

        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._publisher: Optional[TaskPublisher] = None

        # Metrics
        self._cycles = 0
        self._tasks_dispatched = 0
        self._results_processed = 0
        self._errors = 0
        self._started_at: Optional[datetime] = None
        self._last_cycle_at: Optional[datetime] = None
        self._active_jobs_count = 0

    async def start(self) -> None:
        """
        Start the orchestration loop.

        Acquires the orchestrator lock first to ensure only one
        orchestrator instance runs at a time. Raises RuntimeError
        if another orchestrator holds the lock.
        """
        if self._running:
            logger.warning("Orchestrator already running")
            return

        # Acquire orchestrator lock first (Layer 1: single orchestrator)
        self._has_orchestrator_lock = await self.lock_service.try_acquire_orchestrator_lock()
        if not self._has_orchestrator_lock:
            raise RuntimeError(
                "Cannot start orchestrator: another instance is already running. "
                "Check for duplicate deployments or stale processes."
            )

        self._running = True
        self._started_at = datetime.utcnow()
        self._publisher = await get_publisher()
        self._task = asyncio.create_task(self._run_loop())
        logger.info("Orchestrator started with exclusive lock")

    async def stop(self) -> None:
        """
        Stop the orchestration loop.

        Releases the orchestrator lock on shutdown. The lock also
        auto-releases if the connection closes unexpectedly.
        """
        self._running = False

        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

        # Release orchestrator lock (Layer 1)
        if self._has_orchestrator_lock:
            await self.lock_service.release_orchestrator_lock()
            self._has_orchestrator_lock = False

        logger.info(
            f"Orchestrator stopped, lock released. "
            f"Cycles: {self._cycles}, "
            f"Tasks dispatched: {self._tasks_dispatched}, "
            f"Results processed: {self._results_processed}"
        )

    async def _run_loop(self) -> None:
        """Main orchestration loop."""
        logger.info("Orchestration loop starting")

        while self._running:
            try:
                await self._cycle()
                self._cycles += 1
                self._last_cycle_at = datetime.utcnow()
            except Exception as e:
                self._errors += 1
                logger.exception(f"Error in orchestration cycle: {e}")

            await asyncio.sleep(self.poll_interval)

    async def _cycle(self) -> None:
        """
        Single orchestration cycle.

        Steps:
        1. Process pending results
        2. Find active jobs
        3. For each job: check dependencies, dispatch ready nodes
        4. Check for job completion
        """
        # Step 1: Process any pending task results
        results_count = await self._process_results()

        # Step 2: Get active jobs
        active_jobs = await self.job_service.list_active_jobs()
        self._active_jobs_count = len(active_jobs)

        if not active_jobs:
            return

        # Step 3: Process each active job
        for job in active_jobs:
            await self._process_job(job)

    async def _process_results(self) -> int:
        """
        Process unprocessed task results.

        Returns:
            Number of results processed
        """
        results = await self.node_service.get_unprocessed_results(limit=50)

        if not results:
            return 0

        count = 0
        for result in results:
            try:
                # Get workflow for this job
                job = await self.job_service.get_job(result.job_id)
                if job is None:
                    logger.warning(f"Job not found for result: {result.job_id}")
                    continue

                workflow = self.workflow_service.get(job.workflow_id)
                if workflow is None:
                    logger.warning(f"Workflow not found: {job.workflow_id}")
                    continue

                # Process the result
                await self.node_service.process_task_result(result, workflow)
                count += 1
                self._results_processed += 1

            except Exception as e:
                logger.exception(f"Error processing result {result.task_id}: {e}")

        if count > 0:
            logger.debug(f"Processed {count} task results")

        return count

    async def _process_job(self, job: Job) -> None:
        """
        Process a single active job with job-level locking.

        Uses Layer 2 job lock to ensure only one process handles
        this job at a time. Non-blocking: if job is locked, skip it.

        Args:
            job: Job to process
        """
        # Acquire job-specific lock (Layer 2: per-job coordination)
        async with self.lock_service.job_lock(job.job_id, blocking=False) as acquired:
            if not acquired:
                # Another process is handling this job, skip
                logger.debug(f"Skipping job {job.job_id} - locked by another process")
                return

            await self._process_job_internal(job)

    async def _process_job_internal(self, job: Job) -> None:
        """
        Internal job processing (assumes job lock is held).

        Args:
            job: Job to process
        """
        workflow = self.workflow_service.get(job.workflow_id)
        if workflow is None:
            logger.error(f"Workflow not found for job {job.job_id}: {job.workflow_id}")
            await self.job_service.fail_job(
                job.job_id,
                f"Workflow not found: {job.workflow_id}",
            )
            return

        # Check dependencies and mark nodes ready
        await self.node_service.check_and_ready_nodes(job.job_id, workflow)

        # Get ready nodes
        ready_nodes = await self.node_service.get_ready_nodes(job.job_id)

        # Dispatch ready nodes
        for node in ready_nodes:
            await self._dispatch_node(job, node, workflow)

        # Check for job completion
        # Note: _check_job_completion() queries the database fresh via
        # job_service.check_job_completion(), so it always sees current state
        # regardless of any dispatches that happened above.
        await self._check_job_completion(job)

    async def _dispatch_node(
        self,
        job: Job,
        node: NodeState,
        workflow: WorkflowDefinition,
    ) -> bool:
        """
        Dispatch a node for execution.

        Args:
            job: Parent job
            node: Node to dispatch
            workflow: Workflow definition

        Returns:
            True if dispatch succeeded
        """
        node_def = workflow.get_node(node.node_id)

        # Skip non-task nodes (START, END, etc.)
        if node_def.type.value in ("start", "end"):
            # Auto-complete START and END nodes by transitioning through the proper state machine
            # READY -> DISPATCHED -> RUNNING -> COMPLETED
            node.mark_dispatched(f"auto-{node.node_id}")  # Synthetic task_id
            node.mark_running()
            node.mark_completed({})
            from repositories import NodeRepository
            node_repo = NodeRepository(self.pool)
            success = await node_repo.update(node)
            if success:
                logger.debug(f"Auto-completed {node_def.type.value} node: {node.node_id}")
            else:
                # Version conflict on auto-complete is unexpected but not fatal
                logger.warning(
                    f"Version conflict auto-completing {node_def.type.value} node "
                    f"{node.node_id}, will retry"
                )
            return success

        # Create task message
        task_message = self._publisher.create_task_message(
            node=node,
            workflow=workflow,
            job_params=job.input_params,
        )

        # Mark node as dispatched
        success = await self.node_service.mark_dispatched(
            job.job_id,
            node.node_id,
            task_message.task_id,
        )

        if not success:
            logger.warning(f"Failed to mark node {node.node_id} as dispatched")
            return False

        # Dispatch to queue
        dispatch_success = await self._publisher.dispatch_task(task_message)

        if dispatch_success:
            self._tasks_dispatched += 1

            # Emit NODE_DISPATCHED event
            if self._event_service:
                await self._event_service.emit_node_dispatched(
                    node,
                    task_message.task_id,
                    queue=node_def.queue or "functionapp-tasks",
                    handler=node_def.handler,
                )

            # Mark job as started if this is the first dispatch
            if job.status == JobStatus.PENDING:
                await self.job_service.mark_job_started(job.job_id)

                # Emit JOB_STARTED event
                if self._event_service:
                    await self._event_service.emit_job_started(job.job_id, node.node_id)

        return dispatch_success

    async def _check_job_completion(self, job: Job) -> None:
        """
        Check if a job should complete.

        Args:
            job: Job to check
        """
        new_status = await self.job_service.check_job_completion(job.job_id)

        if new_status is None:
            return

        if new_status == JobStatus.COMPLETED:
            # Aggregate results from terminal nodes
            result_data = await self._aggregate_results(job.job_id)
            await self.job_service.complete_job(job.job_id, result_data)

        elif new_status == JobStatus.FAILED:
            # Get error from failed node
            from repositories import NodeRepository
            node_repo = NodeRepository(self.pool)
            nodes = await node_repo.get_all_for_job(job.job_id)
            error = "Unknown error"
            for node in nodes:
                if node.status == NodeStatus.FAILED and node.error_message:
                    error = node.error_message
                    break
            await self.job_service.fail_job(job.job_id, error)

    async def _aggregate_results(self, job_id: str) -> Dict[str, Any]:
        """
        Aggregate results from completed nodes.

        Args:
            job_id: Job identifier

        Returns:
            Aggregated result data
        """
        from repositories import NodeRepository
        node_repo = NodeRepository(self.pool)

        nodes = await node_repo.get_all_for_job(job_id)

        results = {}
        for node in nodes:
            if node.status == NodeStatus.COMPLETED and node.output:
                results[node.node_id] = node.output

        return results

    @property
    def is_running(self) -> bool:
        """Check if orchestrator is running."""
        return self._running

    @property
    def stats(self) -> Dict[str, Any]:
        """Get orchestrator statistics."""
        # Calculate uptime
        uptime_seconds = None
        if self._started_at:
            uptime_seconds = (datetime.utcnow() - self._started_at).total_seconds()

        return {
            "running": self._running,
            "has_orchestrator_lock": self._has_orchestrator_lock,
            "started_at": self._started_at.isoformat() if self._started_at else None,
            "uptime_seconds": uptime_seconds,
            "poll_interval": self.poll_interval,
            "cycles": self._cycles,
            "last_cycle_at": self._last_cycle_at.isoformat() if self._last_cycle_at else None,
            "active_jobs": self._active_jobs_count,
            "tasks_dispatched": self._tasks_dispatched,
            "results_processed": self._results_processed,
            "errors": self._errors,
        }


# ============================================================================
# SINGLE CYCLE EXECUTION (for testing/debugging)
# ============================================================================

async def run_single_cycle(
    pool: AsyncConnectionPool,
    workflow_service: WorkflowService,
) -> Dict[str, Any]:
    """
    Run a single orchestration cycle.

    Useful for testing or manual intervention.

    Args:
        pool: Database connection pool
        workflow_service: Workflow service

    Returns:
        Stats from the cycle
    """
    orchestrator = Orchestrator(pool, workflow_service)
    orchestrator._publisher = await get_publisher()

    await orchestrator._cycle()

    return orchestrator.stats
