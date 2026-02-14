# ============================================================================
# ORCHESTRATION LOOP - MULTI-INSTANCE
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Core - Multi-instance orchestration loop
# PURPOSE: Drive DAG execution with competing consumers pattern
# CREATED: 29 JAN 2026
# UPDATED: 04 FEB 2026 - Multi-orchestrator redesign
# ============================================================================
"""
Orchestration Loop - Multi-Instance Design

Key changes from single-orchestrator:
1. NO lease acquisition - multiple instances can run simultaneously
2. Consumes jobs from Service Bus queue (competing consumers)
3. Only processes jobs where owner_id = self.owner_id
4. Background heartbeat keeps ownership alive
5. Background orphan scan reclaims abandoned jobs

The main loop drives DAG execution:
1. Consume job submissions from dag-jobs queue
2. Find active jobs owned by this instance
3. Check for ready nodes
4. Dispatch tasks
5. Process results
6. Advance job state
7. Repeat

Runs as background tasks in the FastAPI application.
"""

import asyncio
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

from psycopg_pool import AsyncConnectionPool

from core.models import Job, JobQueueMessage, NodeState, TaskResult, WorkflowDefinition, NodeType
from core.models.task import TaskMessage
from core.contracts import JobStatus, NodeStatus
from services import JobService, NodeService, WorkflowService, EventService, CheckpointService
from messaging import TaskPublisher, get_publisher
from orchestrator.engine.templates import TemplateResolutionError, TemplateContext, get_resolver
from orchestrator.engine.evaluator import FanOutHandler
from repositories import JobRepository, NodeRepository
from repositories.job_queue_repo import JobQueueRepository, create_job_queue_repository

logger = logging.getLogger(__name__)


class Orchestrator:
    """
    Multi-instance orchestration loop.

    Each instance:
    - Has a unique owner_id (UUID)
    - Consumes job submissions from the dag-jobs queue
    - Only processes jobs it owns (owner_id match)
    - Sends heartbeats to keep ownership alive
    - Can reclaim orphaned jobs from crashed instances

    Multiple orchestrator instances can run simultaneously,
    each processing different jobs with no coordination needed.
    """

    # Configuration (can be overridden via environment)
    HEARTBEAT_INTERVAL_SEC = int(os.environ.get("HEARTBEAT_INTERVAL_SEC", "30"))
    ORPHAN_SCAN_INTERVAL_SEC = int(os.environ.get("ORPHAN_SCAN_INTERVAL_SEC", "60"))
    ORPHAN_THRESHOLD_SEC = int(os.environ.get("ORPHAN_THRESHOLD_SEC", "120"))
    DEFAULT_TASK_TIMEOUT_SEC = int(os.environ.get("DEFAULT_TASK_TIMEOUT_SEC", "3600"))

    def __init__(
        self,
        pool: AsyncConnectionPool,
        workflow_service: WorkflowService,
        poll_interval: float = 1.0,
        event_service: Optional[EventService] = None,
        asset_service: Optional["GeospatialAssetService"] = None,
    ):
        """
        Initialize orchestrator.

        Args:
            pool: Database connection pool
            workflow_service: Workflow definition service
            poll_interval: Seconds between main loop cycles
            event_service: Optional event service for timeline logging
            asset_service: Optional asset service for processing callbacks
        """
        self.pool = pool
        self.workflow_service = workflow_service
        self.poll_interval = poll_interval
        self._event_service = event_service
        self.asset_service = asset_service

        # Unique ID for this orchestrator instance
        self._owner_id = str(uuid.uuid4())

        # Services
        self.job_service = JobService(pool, workflow_service, event_service)
        self.node_service = NodeService(pool, workflow_service, event_service)
        self.checkpoint_service = CheckpointService(pool)

        # Repositories
        self._job_repo = JobRepository(pool)
        self._job_queue_repo: Optional[JobQueueRepository] = None

        # State
        self._running = False
        self._stop_event = asyncio.Event()

        # Brain guard — session-level advisory lock ensures single leader
        self._is_leader = False
        self._leader_conn = None  # Dedicated connection holding the advisory lock
        self._standby_task: Optional[asyncio.Task] = None
        # Fixed lock ID for brain guard (hash of "rmhdag:brain:leader")
        self._BRAIN_LOCK_ID = 5528965954220049258  # Pre-computed stable hash

        # Background tasks
        self._main_loop_task: Optional[asyncio.Task] = None
        self._queue_consumer_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._orphan_scan_task: Optional[asyncio.Task] = None

        # Publisher
        self._publisher: Optional[TaskPublisher] = None

        # Metrics
        self._started_at: Optional[datetime] = None
        self._cycles = 0
        self._jobs_claimed = 0
        self._jobs_reclaimed = 0
        self._tasks_dispatched = 0
        self._results_processed = 0
        self._errors = 0
        self._last_cycle_at: Optional[datetime] = None
        self._last_heartbeat_at: Optional[datetime] = None
        self._last_orphan_scan_at: Optional[datetime] = None
        self._active_jobs_count = 0

    @property
    def owner_id(self) -> str:
        """This orchestrator's unique ID."""
        return self._owner_id

    # Brain guard retry interval (how often standby tries to become leader)
    STANDBY_RETRY_SEC = int(os.environ.get("BRAIN_STANDBY_RETRY_SEC", "10"))

    async def start(self) -> None:
        """
        Start the orchestrator with brain guard.

        Uses a PostgreSQL session-level advisory lock to ensure only one
        instance runs the orchestration loop at a time. If the lock is
        already held, this instance enters standby mode and retries
        periodically. When the leader dies, its session ends, the lock
        is released, and a standby instance promotes itself.

        All instances serve HTTP regardless of leader status.
        """
        if self._running:
            logger.warning("Orchestrator already running")
            return

        self._running = True
        self._started_at = datetime.now(timezone.utc)
        self._stop_event.clear()

        # Try to become leader
        acquired = await self._try_acquire_brain_lock()

        if acquired:
            await self._start_as_leader()
        else:
            # Start standby loop — retries lock acquisition periodically
            logger.info(
                f"Orchestrator entering standby mode "
                f"(owner_id={self._owner_id[:8]}..., "
                f"retry_interval={self.STANDBY_RETRY_SEC}s)"
            )
            self._standby_task = asyncio.create_task(
                self._standby_loop(),
                name=f"orchestrator-standby-{self._owner_id[:8]}"
            )

    async def _try_acquire_brain_lock(self) -> bool:
        """
        Try to acquire the brain guard advisory lock.

        Uses a session-level advisory lock (pg_try_advisory_lock) on a
        dedicated connection. The lock lives as long as the connection
        is open. If the process dies, PostgreSQL closes the connection
        and releases the lock automatically.

        Returns:
            True if this instance is now the leader.
        """
        try:
            # Get a dedicated connection (NOT from the pool — we hold it open)
            from repositories.database import get_connection_string
            from psycopg import AsyncConnection
            from psycopg.rows import dict_row

            conn = await AsyncConnection.connect(
                get_connection_string(),
                autocommit=True,
                row_factory=dict_row,
            )

            result = await conn.execute(
                "SELECT pg_try_advisory_lock(%s) AS acquired",
                (self._BRAIN_LOCK_ID,),
            )
            row = await result.fetchone()
            acquired = row["acquired"] if row else False

            if acquired:
                # Keep the connection open — it holds the lock
                self._leader_conn = conn
                self._is_leader = True
                logger.info(
                    f"Brain guard: acquired leader lock "
                    f"(owner_id={self._owner_id[:8]}...)"
                )
                return True
            else:
                # We didn't get the lock — close this connection
                await conn.close()
                return False

        except Exception as e:
            logger.error(f"Brain guard: failed to acquire lock: {e}")
            return False

    async def _start_as_leader(self) -> None:
        """Start all orchestrator background tasks (leader mode)."""
        # Initialize publisher
        self._publisher = await get_publisher()

        # Initialize job queue consumer
        self._job_queue_repo = create_job_queue_repository()
        await self._job_queue_repo.connect()

        # Start background tasks
        self._main_loop_task = asyncio.create_task(
            self._main_loop(),
            name=f"orchestrator-main-{self._owner_id[:8]}"
        )
        self._queue_consumer_task = asyncio.create_task(
            self._queue_consumer_loop(),
            name=f"orchestrator-queue-{self._owner_id[:8]}"
        )
        self._heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(),
            name=f"orchestrator-heartbeat-{self._owner_id[:8]}"
        )
        self._orphan_scan_task = asyncio.create_task(
            self._orphan_scan_loop(),
            name=f"orchestrator-orphan-{self._owner_id[:8]}"
        )

        logger.info(
            f"Orchestrator started as LEADER (owner_id={self._owner_id[:8]}..., "
            f"poll={self.poll_interval}s, heartbeat={self.HEARTBEAT_INTERVAL_SEC}s, "
            f"orphan_threshold={self.ORPHAN_THRESHOLD_SEC}s)"
        )

    async def _standby_loop(self) -> None:
        """
        Standby loop: periodically retry brain lock acquisition.

        When the leader dies, its PostgreSQL session closes and the
        advisory lock is released. This loop detects that and promotes
        this instance to leader.
        """
        while self._running and not self._stop_event.is_set():
            try:
                await asyncio.sleep(self.STANDBY_RETRY_SEC)

                if self._stop_event.is_set():
                    break

                acquired = await self._try_acquire_brain_lock()
                if acquired:
                    logger.info(
                        f"Brain guard: standby promoted to LEADER "
                        f"(owner_id={self._owner_id[:8]}...)"
                    )
                    await self._start_as_leader()
                    return  # Exit standby loop — now running as leader
                else:
                    logger.debug(
                        f"Brain guard: lock still held by another instance, "
                        f"retrying in {self.STANDBY_RETRY_SEC}s"
                    )

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Brain guard standby error: {e}")
                await asyncio.sleep(self.STANDBY_RETRY_SEC)

    async def _verify_brain_lock(self) -> bool:
        """
        Verify the brain guard lock connection is still alive.

        Sends a lightweight query on the dedicated lock connection.
        If the connection is dead, returns False.
        """
        if self._leader_conn is None:
            return False

        try:
            result = await self._leader_conn.execute("SELECT 1 AS alive")
            row = await result.fetchone()
            return row is not None
        except Exception as e:
            logger.warning(f"Brain guard: lock connection check failed: {e}")
            return False

    async def _demote_to_standby(self) -> None:
        """
        Demote this instance from leader to standby.

        Cancels all leader background tasks, cleans up the dead lock
        connection, and starts the standby retry loop.
        """
        self._is_leader = False

        # Cancel leader tasks
        leader_tasks = [
            self._main_loop_task,
            self._queue_consumer_task,
            self._heartbeat_task,
            self._orphan_scan_task,
        ]
        for task in leader_tasks:
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        self._main_loop_task = None
        self._queue_consumer_task = None
        self._heartbeat_task = None
        self._orphan_scan_task = None

        # Close dead lock connection
        if self._leader_conn:
            try:
                await self._leader_conn.close()
            except Exception:
                pass
            self._leader_conn = None

        # Close job queue
        if self._job_queue_repo:
            try:
                await self._job_queue_repo.close()
            except Exception:
                pass

        logger.info(
            f"Brain guard: demoted to standby, will retry lock in "
            f"{self.STANDBY_RETRY_SEC}s (owner_id={self._owner_id[:8]}...)"
        )

        # Start standby loop to try re-acquiring leadership
        self._standby_task = asyncio.create_task(
            self._standby_loop(),
            name=f"orchestrator-standby-{self._owner_id[:8]}"
        )

    async def stop(self) -> None:
        """
        Stop the orchestrator gracefully.

        Signals all background tasks to stop and waits for completion.
        Releases the brain guard advisory lock so a standby can promote.
        Does NOT release job ownership - jobs remain owned and will be
        reclaimed by other instances after heartbeat expires.
        """
        logger.info(f"Stopping orchestrator (owner_id={self._owner_id[:8]}...)")

        self._running = False
        self._stop_event.set()

        # Cancel all background tasks (leader + standby)
        tasks = [
            self._main_loop_task,
            self._queue_consumer_task,
            self._heartbeat_task,
            self._orphan_scan_task,
            self._standby_task,
        ]

        for task in tasks:
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        # Close job queue connection
        if self._job_queue_repo:
            await self._job_queue_repo.close()

        # Release brain guard lock by closing the dedicated connection
        was_leader = self._is_leader
        if self._leader_conn:
            try:
                await self._leader_conn.close()
                logger.info("Brain guard: released leader lock")
            except Exception as e:
                logger.warning(f"Brain guard: error releasing lock: {e}")
            finally:
                self._leader_conn = None
                self._is_leader = False

        logger.info(
            f"Orchestrator stopped (owner_id={self._owner_id[:8]}..., "
            f"was_leader={was_leader}, "
            f"cycles={self._cycles}, jobs_claimed={self._jobs_claimed}, "
            f"jobs_reclaimed={self._jobs_reclaimed}, "
            f"tasks_dispatched={self._tasks_dispatched})"
        )

    # =========================================================================
    # QUEUE CONSUMER LOOP - Claims jobs from dag-jobs queue
    # =========================================================================

    async def _queue_consumer_loop(self) -> None:
        """
        Consume job submissions from the dag-jobs Service Bus queue.

        This is the entry point for new jobs. When a message is received:
        1. Validate workflow exists
        2. Create Job in database with owner_id = self
        3. Complete the message (remove from queue)

        Uses competing consumers pattern - first orchestrator to receive wins.
        """
        logger.info(f"Starting queue consumer loop (owner={self._owner_id[:8]}...)")

        try:
            await self._job_queue_repo.consume_loop(
                handler=self._handle_job_submission,
                stop_event=self._stop_event,
            )
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.exception(f"Queue consumer loop error: {e}")

        logger.info(f"Queue consumer loop stopped (owner={self._owner_id[:8]}...)")

    async def _handle_job_submission(
        self,
        message: JobQueueMessage,
        raw_message,
    ) -> bool:
        """
        Handle a job submission message from the queue.

        Args:
            message: Parsed JobQueueMessage
            raw_message: Raw Service Bus message (for dead-letter)

        Returns:
            True to complete message (success or permanent failure)
            False to abandon message (retry)
        """
        logger.info(
            f"Received job submission: workflow={message.workflow_id}, "
            f"correlation={message.correlation_id}, owner={self._owner_id[:8]}..."
        )

        # Validate workflow exists
        workflow = self.workflow_service.get(message.workflow_id)
        if workflow is None:
            logger.error(f"Workflow not found: {message.workflow_id}")
            await self._job_queue_repo.dead_letter(
                raw_message,
                reason="WorkflowNotFound",
                description=f"Workflow '{message.workflow_id}' does not exist",
            )
            return True  # Don't retry - permanent failure

        # Check idempotency key for duplicates
        if message.idempotency_key:
            existing = await self._job_repo.get_by_idempotency_key(
                message.idempotency_key
            )
            if existing:
                logger.info(
                    f"Duplicate submission (idempotency_key={message.idempotency_key}), "
                    f"existing job={existing.job_id}"
                )
                return True  # Already processed - complete without creating

        # Create job with ownership
        try:
            job = await self.job_service.create_job(
                workflow_id=message.workflow_id,
                input_params=message.input_params,
                submitted_by=message.submitted_by,
                correlation_id=message.correlation_id,
                asset_id=message.asset_id,
                idempotency_key=message.idempotency_key,
            )

            # Set ownership
            job.owner_id = self._owner_id
            job.owner_heartbeat_at = datetime.utcnow()
            await self._job_repo.update(job)

            self._jobs_claimed += 1
            logger.info(
                f"Claimed job {job.job_id} (workflow={message.workflow_id}, "
                f"owner={self._owner_id[:8]}...)"
            )
            return True  # Success

        except Exception as e:
            logger.exception(f"Failed to create job: {e}")
            return False  # Abandon for retry

    # =========================================================================
    # MAIN LOOP - Processes owned jobs
    # =========================================================================

    # How often to verify the brain lock is still held (every N cycles)
    BRAIN_LOCK_CHECK_INTERVAL = int(os.environ.get("BRAIN_LOCK_CHECK_CYCLES", "30"))

    async def _main_loop(self) -> None:
        """
        Main orchestration loop.

        Only processes jobs where owner_id = self.owner_id.
        Periodically verifies the brain guard lock is still held.
        """
        logger.info(f"Starting main loop (owner={self._owner_id[:8]}...)")

        while self._running and not self._stop_event.is_set():
            try:
                await self._cycle()
                self._cycles += 1
                self._last_cycle_at = datetime.utcnow()

                # Periodic brain lock health check
                if self._cycles % self.BRAIN_LOCK_CHECK_INTERVAL == 0:
                    if not await self._verify_brain_lock():
                        logger.error(
                            "Brain guard: lost leader lock — demoting to standby "
                            f"(owner_id={self._owner_id[:8]}...)"
                        )
                        await self._demote_to_standby()
                        return  # Exit main loop

            except asyncio.CancelledError:
                break
            except Exception as e:
                self._errors += 1
                logger.exception(f"Error in orchestration cycle: {e}")

            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=self.poll_interval,
                )
                break  # Stop event was set
            except asyncio.TimeoutError:
                pass  # Continue loop

        logger.info(f"Main loop stopped (owner={self._owner_id[:8]}...)")

    async def _cycle(self) -> None:
        """
        Single orchestration cycle.

        Steps:
        1. Process pending results
        2. Find active jobs OWNED BY THIS INSTANCE
        3. For each job: check dependencies, dispatch ready nodes
        4. Check for job completion
        """
        # Step 1: Process any pending task results
        await self._process_results()

        # Step 2: Get active jobs owned by THIS orchestrator only
        active_jobs = await self._job_repo.list_active_for_owner(
            self._owner_id,
            limit=100,
        )
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

                # Only process results for jobs we own
                if job.owner_id != self._owner_id:
                    continue

                workflow = self._get_workflow_for_job(job)

                # Process the result
                await self.node_service.process_task_result(result, workflow)
                count += 1
                self._results_processed += 1

            except Exception as e:
                logger.exception(f"Error processing result {result.task_id}: {e}")

        if count > 0:
            logger.debug(f"Processed {count} task results")

        return count

    def _get_workflow_for_job(self, job: Job) -> WorkflowDefinition:
        """Get the pinned workflow definition for a job."""
        return job.get_pinned_workflow()

    async def _process_job(self, job: Job) -> None:
        """
        Process a single active job.

        No job-level lock needed - we own the job via owner_id.

        Args:
            job: Job to process
        """
        workflow = self._get_workflow_for_job(job)

        # Check for stuck nodes (timeout detection)
        await self._check_stuck_nodes(job, workflow)

        # Check dependencies and mark nodes ready
        await self.node_service.check_and_ready_nodes(job.job_id, workflow)

        # Get ready nodes
        ready_nodes = await self.node_service.get_ready_nodes(job.job_id)

        # Dispatch ready nodes
        for node in ready_nodes:
            await self._dispatch_node(job, node, workflow)

        # Check for job completion
        await self._check_job_completion(job)

    # =========================================================================
    # HEARTBEAT LOOP - Keeps ownership alive
    # =========================================================================

    async def _heartbeat_loop(self) -> None:
        """
        Background loop that updates heartbeat for owned jobs.

        If this stops (crash), jobs become orphaned after ORPHAN_THRESHOLD_SEC.
        Other orchestrator instances can then reclaim them.
        """
        logger.info(
            f"Starting heartbeat loop (owner={self._owner_id[:8]}..., "
            f"interval={self.HEARTBEAT_INTERVAL_SEC}s)"
        )

        while not self._stop_event.is_set():
            try:
                count = await self._job_repo.update_heartbeat(self._owner_id)
                self._last_heartbeat_at = datetime.utcnow()

                if count > 0:
                    logger.debug(f"Heartbeat sent for {count} jobs")

            except Exception as e:
                logger.error(f"Heartbeat error: {e}")

            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=self.HEARTBEAT_INTERVAL_SEC,
                )
                break
            except asyncio.TimeoutError:
                pass

        logger.info(f"Heartbeat loop stopped (owner={self._owner_id[:8]}...)")

    # =========================================================================
    # ORPHAN SCAN LOOP - Reclaims abandoned jobs
    # =========================================================================

    async def _orphan_scan_loop(self) -> None:
        """
        Background loop that scans for and reclaims orphaned jobs.

        Orphaned jobs have owner_heartbeat_at older than ORPHAN_THRESHOLD_SEC.
        This handles crashed orchestrator recovery.
        """
        logger.info(
            f"Starting orphan scan loop (owner={self._owner_id[:8]}..., "
            f"interval={self.ORPHAN_SCAN_INTERVAL_SEC}s, "
            f"threshold={self.ORPHAN_THRESHOLD_SEC}s)"
        )

        while not self._stop_event.is_set():
            try:
                reclaimed = await self._job_repo.reclaim_orphaned_jobs(
                    new_owner_id=self._owner_id,
                    orphan_threshold_seconds=self.ORPHAN_THRESHOLD_SEC,
                    limit=10,
                )

                self._last_orphan_scan_at = datetime.utcnow()

                if reclaimed:
                    self._jobs_reclaimed += len(reclaimed)
                    logger.info(f"Reclaimed {len(reclaimed)} orphaned jobs")

            except Exception as e:
                logger.error(f"Orphan scan error: {e}")

            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=self.ORPHAN_SCAN_INTERVAL_SEC,
                )
                break
            except asyncio.TimeoutError:
                pass

        logger.info(f"Orphan scan loop stopped (owner={self._owner_id[:8]}...)")

    # =========================================================================
    # NODE DISPATCH (unchanged from single-orchestrator)
    # =========================================================================

    async def _dispatch_node(
        self,
        job: Job,
        node: NodeState,
        workflow: WorkflowDefinition,
    ) -> bool:
        """
        Dispatch a node for execution.

        Handles different node types:
        - Dynamic (fan-out child): Dispatch using stored input_params (for retries)
        - START/END: Auto-complete immediately
        - CONDITIONAL: Evaluate and route immediately (no worker dispatch)
        - FAN_OUT: Expand into N dynamic children, dispatch all
        - FAN_IN: Aggregate results from fan-out children, auto-complete
        - TASK: Dispatch to worker queue

        Args:
            job: Parent job
            node: Node to dispatch
            workflow: Workflow definition

        Returns:
            True if dispatch succeeded
        """
        # Dynamic child nodes (created by fan-out) are not in the workflow
        if node.is_dynamic:
            return await self._dispatch_dynamic_child_node(job, node, workflow)

        node_def = workflow.get_node(node.node_id)

        # Handle START and END nodes - auto-complete immediately
        if node_def.type.value in ("start", "end"):
            return await self._auto_complete_node(node, node_def.type.value)

        # Handle CONDITIONAL nodes - evaluate and route immediately
        if node_def.type.value == "conditional":
            return await self._handle_conditional_node(job, node, workflow)

        # Handle FAN_OUT nodes - expand and dispatch children
        if node_def.type == NodeType.FAN_OUT:
            return await self._handle_fan_out_node(job, node, workflow)

        # Handle FAN_IN nodes - aggregate and auto-complete
        if node_def.type == NodeType.FAN_IN:
            return await self._handle_fan_in_node(job, node, workflow)

        # Handle TASK nodes - dispatch to worker queue
        return await self._dispatch_task_node(job, node, workflow, node_def)

    async def _auto_complete_node(self, node: NodeState, node_type: str) -> bool:
        """
        Auto-complete START and END nodes.

        These nodes don't dispatch to workers - they complete immediately.
        """
        # READY -> DISPATCHED -> RUNNING -> COMPLETED
        node.mark_dispatched(f"auto-{node.node_id}")  # Synthetic task_id
        node.mark_running()
        node.mark_completed({})
        node_repo = NodeRepository(self.pool)
        success = await node_repo.update(node)
        if success:
            logger.debug(f"Auto-completed {node_type} node: {node.node_id}")
        else:
            logger.warning(
                f"Version conflict auto-completing {node_type} node "
                f"{node.node_id}, will retry"
            )
        return success

    async def _handle_conditional_node(
        self,
        job: Job,
        node: NodeState,
        workflow: WorkflowDefinition,
    ) -> bool:
        """
        Handle a conditional node - evaluate and route immediately.

        Conditional nodes don't dispatch to workers. They evaluate their
        condition based on upstream output and route to the appropriate branch.

        Args:
            job: Parent job
            node: The conditional node
            workflow: Workflow definition

        Returns:
            True if evaluation succeeded
        """
        # Get outputs from completed nodes for template resolution
        node_repo = NodeRepository(self.pool)
        all_nodes = await node_repo.get_all_for_job(job.job_id)

        node_outputs = {
            n.node_id: n.output or {}
            for n in all_nodes
            if n.status == NodeStatus.COMPLETED and n.output
        }

        # Evaluate the conditional and determine routing
        taken_branch, skipped_branches = await self.node_service.evaluate_and_route_conditional(
            job_id=job.job_id,
            conditional_node_id=node.node_id,
            workflow=workflow,
            job_params=job.input_params,
            node_outputs=node_outputs,
        )

        if taken_branch is None:
            # No branch matched - this is an error
            logger.error(
                f"Conditional node {node.node_id} failed to match any branch"
            )
            node.mark_failed("No conditional branch matched")
            await node_repo.update(node)
            return False

        # Mark the conditional node as complete
        # Store which branch was taken in the output for debugging
        node.mark_dispatched(f"conditional-{node.node_id}")
        node.mark_running()
        node.mark_completed({
            "taken_branch": taken_branch,
            "skipped_branches": skipped_branches,
        })
        success = await node_repo.update(node)

        if success:
            logger.info(
                f"Conditional node {node.node_id} routed to {taken_branch}, "
                f"skipping {skipped_branches}"
            )

            # Skip the untaken branches and their exclusive descendants
            if skipped_branches:
                await self.node_service.skip_untaken_branches(
                    job.job_id, skipped_branches, workflow
                )

            # Emit event
            if self._event_service:
                await self._event_service.emit_node_completed(
                    node,
                    duration_ms=0,
                    output_keys=["taken_branch", "skipped_branches"],
                )
        else:
            logger.warning(
                f"Version conflict completing conditional node {node.node_id}"
            )

        return success

    async def _handle_fan_out_node(
        self,
        job: Job,
        node: NodeState,
        workflow: WorkflowDefinition,
    ) -> bool:
        """
        Handle a fan-out node — expand into N dynamic child nodes and dispatch all.

        Fan-out nodes don't dispatch to workers. They:
        1. Resolve the source array from template expression
        2. Expand into N dynamic child NodeState records
        3. Create TaskMessages for each child
        4. Batch dispatch all to Service Bus
        5. Auto-complete the fan-out node itself
        """
        node_def = workflow.get_node(node.node_id)
        node_repo = NodeRepository(self.pool)

        # Gather context for template resolution
        all_nodes = await node_repo.get_all_for_job(job.job_id)
        node_outputs = {
            n.node_id: n.output or {}
            for n in all_nodes
            if n.status == NodeStatus.COMPLETED and n.output
        }

        # Expand fan-out using FanOutHandler
        fan_out_handler = FanOutHandler()
        context = {
            "inputs": job.input_params,
            "nodes": {
                nid: {"output": out}
                for nid, out in node_outputs.items()
            },
        }

        try:
            expansions = fan_out_handler.expand(node_def, context)
        except Exception as e:
            logger.error(f"Fan-out expansion failed for node {node.node_id}: {e}")
            node.mark_failed(f"Fan-out expansion error: {e}")
            await node_repo.update(node)
            return False

        if not expansions:
            logger.error(f"Fan-out node {node.node_id} produced 0 expansions")
            node.mark_failed("Fan-out source resolved to empty array")
            await node_repo.update(node)
            return False

        # Resolve remaining templates in expanded params (e.g., {{ inputs.x }})
        resolver = get_resolver()
        template_context = TemplateContext.from_job(job.input_params, node_outputs)

        # Get retry config from fan-out node
        max_retries = 3
        if node_def.retry:
            max_retries = node_def.retry.max_attempts

        # Create dynamic child NodeState records and TaskMessages
        child_nodes = []
        task_messages = []

        for exp in expansions:
            child_node_id = f"{node.node_id}__{exp['index']}"

            # Resolve remaining template expressions
            try:
                resolved_params = resolver.resolve(exp["params"], template_context)
            except TemplateResolutionError as e:
                logger.error(
                    f"Template resolution failed for fan-out child {child_node_id}: {e}"
                )
                resolved_params = exp["params"]

            task_id = f"{job.job_id}_{child_node_id}_0"

            child_node = NodeState(
                job_id=job.job_id,
                node_id=child_node_id,
                status=NodeStatus.DISPATCHED,
                task_id=task_id,
                parent_node_id=node.node_id,
                fan_out_index=exp["index"],
                input_params=resolved_params,
                max_retries=max_retries,
                dispatched_at=datetime.utcnow(),
            )
            child_nodes.append(child_node)

            task_msg = TaskMessage(
                task_id=task_id,
                job_id=job.job_id,
                node_id=child_node_id,
                handler=exp["handler"],
                params=resolved_params,
                timeout_seconds=exp.get("timeout_seconds", 3600),
                retry_count=0,
                correlation_id=getattr(job, "correlation_id", None),
            )
            task_messages.append(task_msg)

        # Persist child nodes
        await node_repo.create_many(child_nodes)

        # Batch dispatch all task messages
        dispatch_count = await self._publisher.dispatch_tasks(task_messages)
        self._tasks_dispatched += dispatch_count

        if dispatch_count != len(task_messages):
            logger.warning(
                f"Fan-out dispatch: {dispatch_count}/{len(task_messages)} succeeded "
                f"for node {node.node_id}"
            )

        # Mark fan-out node as COMPLETED
        child_node_ids = [cn.node_id for cn in child_nodes]
        node.mark_dispatched(f"fan-out-{node.node_id}")
        node.mark_running()
        node.mark_completed({
            "fan_out_count": len(child_nodes),
            "child_node_ids": child_node_ids,
        })
        success = await node_repo.update(node)

        if success:
            logger.info(
                f"Fan-out node {node.node_id} expanded to {len(child_nodes)} children, "
                f"{dispatch_count} dispatched"
            )

            if job.status == JobStatus.PENDING:
                await self.job_service.mark_job_started(job.job_id)
                if self._event_service:
                    await self._event_service.emit_job_started(job.job_id, node.node_id)

            if self._event_service:
                await self._event_service.emit_node_completed(
                    node, duration_ms=0,
                    output_keys=["fan_out_count", "child_node_ids"],
                )

        return success

    async def _handle_fan_in_node(
        self,
        job: Job,
        node: NodeState,
        workflow: WorkflowDefinition,
    ) -> bool:
        """
        Handle a fan-in node — aggregate results from fan-out children.

        By the time this is called, all children are guaranteed terminal
        (because check_and_ready_nodes blocks FAN_IN until children finish).
        """
        node_def = workflow.get_node(node.node_id)
        node_repo = NodeRepository(self.pool)

        # Find the upstream FAN_OUT node (the one whose 'next' points here)
        fan_out_node_id = None
        for other_id, other_def in workflow.nodes.items():
            if other_def.next and other_def.type == NodeType.FAN_OUT:
                next_list = [other_def.next] if isinstance(other_def.next, str) else other_def.next
                if node.node_id in next_list:
                    fan_out_node_id = other_id
                    break

        if fan_out_node_id is None:
            logger.error(f"Fan-in node {node.node_id} has no upstream FAN_OUT node")
            node.mark_failed("No upstream FAN_OUT node found")
            await node_repo.update(node)
            return False

        # Get all children of the fan-out node
        children = await node_repo.get_children_by_parent(job.job_id, fan_out_node_id)

        # Check for failures
        failed_children = [c for c in children if c.status == NodeStatus.FAILED]
        if failed_children:
            error = (
                f"Fan-out children failed: "
                f"{[c.node_id for c in failed_children[:5]]}"
            )
            node.mark_dispatched(f"fan-in-{node.node_id}")
            node.mark_running()
            node.mark_failed(error)
            await node_repo.update(node)
            return False

        # Aggregate results
        aggregation_mode = node_def.aggregation or "collect"
        aggregated = self._aggregate_fan_in_results(children, aggregation_mode)

        # Auto-complete the fan-in node
        node.mark_dispatched(f"fan-in-{node.node_id}")
        node.mark_running()
        node.mark_completed(aggregated)
        success = await node_repo.update(node)

        if success:
            logger.info(
                f"Fan-in node {node.node_id} aggregated {len(children)} results "
                f"(mode={aggregation_mode})"
            )
            if self._event_service:
                await self._event_service.emit_node_completed(
                    node, duration_ms=0,
                    output_keys=list(aggregated.keys()) if aggregated else [],
                )

        return success

    def _aggregate_fan_in_results(
        self,
        children: List[NodeState],
        mode: str,
    ) -> Dict[str, Any]:
        """Aggregate child node outputs according to the specified mode."""
        outputs = [c.output or {} for c in children if c.status == NodeStatus.COMPLETED]

        if mode == "collect":
            return {"results": outputs, "count": len(outputs)}
        elif mode == "concat":
            concatenated = []
            for out in outputs:
                for value in out.values():
                    if isinstance(value, list):
                        concatenated.extend(value)
                    else:
                        concatenated.append(value)
            return {"results": concatenated, "count": len(concatenated)}
        elif mode == "sum":
            total = 0
            for out in outputs:
                for value in out.values():
                    if isinstance(value, (int, float)):
                        total += value
            return {"total": total, "count": len(outputs)}
        elif mode == "first":
            return {"result": outputs[0] if outputs else {}, "count": len(outputs)}
        elif mode == "last":
            return {"result": outputs[-1] if outputs else {}, "count": len(outputs)}
        else:
            return {"results": outputs, "count": len(outputs)}

    async def _dispatch_dynamic_child_node(
        self,
        job: Job,
        node: NodeState,
        workflow: WorkflowDefinition,
    ) -> bool:
        """
        Dispatch a dynamic child node (created by fan-out) on retry.

        Dynamic nodes are not in the workflow definition. Their handler,
        queue, and params come from the parent fan-out node's task definition
        and the node's stored input_params.
        """
        node_repo = NodeRepository(self.pool)

        # Get parent fan-out node's definition
        parent_def = workflow.get_node(node.parent_node_id)
        if parent_def is None or parent_def.task is None:
            logger.error(
                f"Cannot dispatch dynamic node {node.node_id}: "
                f"parent {node.parent_node_id} not found or has no task definition"
            )
            node.mark_failed("Parent fan-out node definition not found")
            await node_repo.update(node)
            return False

        task_def = parent_def.task
        params = node.input_params or {}

        # Load checkpoint data for resumable retries
        checkpoint_data = None
        if node.retry_count > 0:
            checkpoint_data = await self.checkpoint_service.get_checkpoint_data_for_retry(
                job_id=job.job_id,
                node_id=node.node_id,
            )

        task_id = f"{job.job_id}_{node.node_id}_{node.retry_count}"

        task_msg = TaskMessage(
            task_id=task_id,
            job_id=job.job_id,
            node_id=node.node_id,
            handler=task_def.handler,
            params=params,
            timeout_seconds=task_def.timeout_seconds,
            retry_count=node.retry_count,
            checkpoint_data=checkpoint_data,
            correlation_id=getattr(job, "correlation_id", None),
        )

        # Mark node as dispatched
        success = await self.node_service.mark_dispatched(
            job.job_id, node.node_id, task_id,
        )
        if not success:
            logger.warning(f"Failed to mark dynamic node {node.node_id} as dispatched")
            return False

        # Dispatch to queue
        dispatch_success = await self._publisher.dispatch_task(task_msg)

        if dispatch_success:
            self._tasks_dispatched += 1
            if self._event_service:
                await self._event_service.emit_node_dispatched(
                    node, task_id,
                    queue=task_def.queue,
                    handler=task_def.handler,
                )

        return dispatch_success

    async def _dispatch_task_node(
        self,
        job: Job,
        node: NodeState,
        workflow: WorkflowDefinition,
        node_def,
    ) -> bool:
        """
        Dispatch a task node to a worker queue.

        Args:
            job: Parent job
            node: The task node
            workflow: Workflow definition
            node_def: Node definition

        Returns:
            True if dispatch succeeded
        """
        # Load checkpoint data for retries (resumable tasks)
        checkpoint_data = None
        if node.retry_count > 0:
            checkpoint_data = await self.checkpoint_service.get_checkpoint_data_for_retry(
                job_id=job.job_id,
                node_id=node.node_id,
            )
            if checkpoint_data:
                logger.info(
                    f"Loaded checkpoint for retry: job={job.job_id} node={node.node_id} "
                    f"phase={checkpoint_data.get('phase_name')}"
                )

        # Gather completed node outputs for template resolution
        # e.g. {{ nodes.validate.output.crs }} needs validate's output
        node_repo = NodeRepository(self.pool)
        all_nodes = await node_repo.get_all_for_job(job.job_id)
        node_outputs = {
            n.node_id: n.output or {}
            for n in all_nodes
            if n.status == NodeStatus.COMPLETED and n.output
        }

        # Create task message with template resolution
        try:
            task_message = self._publisher.create_task_message(
                node=node,
                workflow=workflow,
                job_params=job.input_params,
                checkpoint_data=checkpoint_data,
                node_outputs=node_outputs,
            )
        except TemplateResolutionError as e:
            logger.error(
                f"Template resolution failed for node {node.node_id} "
                f"in job {job.job_id}: {e}"
            )
            node.mark_failed(f"Template resolution error: {e}")
            await node_repo.update(node)
            return False

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
                    queue=node_def.queue or "unknown",
                    handler=node_def.handler,
                )

            # Mark job as started if this is the first dispatch
            if job.status == JobStatus.PENDING:
                await self.job_service.mark_job_started(job.job_id)

                # Emit JOB_STARTED event
                if self._event_service:
                    await self._event_service.emit_job_started(job.job_id, node.node_id)

        return dispatch_success

    async def _check_stuck_nodes(
        self,
        job: Job,
        workflow: WorkflowDefinition,
    ) -> int:
        """
        Check for nodes stuck in DISPATCHED/RUNNING past their timeout.

        Marks timed-out nodes as FAILED, then auto-retries if eligible.
        Runs every cycle per-job so timeout detection is within 1-2 seconds
        of the actual timeout threshold.

        Args:
            job: Job to check
            workflow: Workflow definition for timeout config

        Returns:
            Number of nodes timed out
        """
        node_repo = NodeRepository(self.pool)
        all_nodes = await node_repo.get_all_for_job(job.job_id)
        now = datetime.now(timezone.utc)
        timed_out = 0

        for node in all_nodes:
            if node.status not in (NodeStatus.DISPATCHED, NodeStatus.RUNNING):
                continue

            # Determine timeout from workflow definition
            timeout_sec = self.DEFAULT_TASK_TIMEOUT_SEC
            node_def = workflow.nodes.get(node.node_id)
            if node_def and node_def.timeout_seconds:
                timeout_sec = node_def.timeout_seconds
            elif node.is_dynamic and node.parent_node_id:
                # Dynamic children use parent fan-out's task timeout
                parent_def = workflow.nodes.get(node.parent_node_id)
                if parent_def and parent_def.task:
                    timeout_sec = parent_def.task.timeout_seconds

            # Check age against timeout
            check_time = node.started_at or node.dispatched_at
            if not check_time:
                continue

            elapsed = (now - check_time).total_seconds()
            if elapsed <= timeout_sec:
                continue

            # Node has timed out
            error_msg = f"Task timed out after {timeout_sec}s (elapsed: {elapsed:.0f}s)"
            node.mark_failed(error_msg)
            logger.warning(
                f"Timeout: node {node.node_id} in job {job.job_id} "
                f"({node.status.value} for {elapsed:.0f}s, limit {timeout_sec}s)"
            )

            # Emit NODE_FAILED event for timeout
            can_retry = node.retry_count < node.max_retries
            if self._event_service:
                await self._event_service.emit_node_failed(
                    node, error_msg, can_retry
                )

            # Auto-retry if eligible (same logic as process_task_result)
            if can_retry and node.prepare_retry():
                logger.info(
                    f"Node {node.node_id} prepared for retry after timeout "
                    f"(attempt {node.retry_count}/{node.max_retries})"
                )
                if self._event_service:
                    await self._event_service.emit_node_retry(node)

            # Persist the state change (FAILED or READY after retry)
            success = await node_repo.update(node)
            if success:
                timed_out += 1

        if timed_out > 0:
            logger.info(f"Timed out {timed_out} stuck nodes in job {job.job_id}")

        return timed_out

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

            # Update linked asset version (no-op for standalone jobs)
            if self.asset_service:
                try:
                    await self.asset_service.on_processing_completed(
                        job.job_id, result_data
                    )
                except Exception as e:
                    logger.error(
                        f"Asset callback failed for completed job {job.job_id}: {e}"
                    )

            # Release ownership (job is done)
            await self._job_repo.release_ownership(job.job_id, self._owner_id)

        elif new_status == JobStatus.FAILED:
            # Get error from failed node
            node_repo = NodeRepository(self.pool)
            nodes = await node_repo.get_all_for_job(job.job_id)
            error = "Unknown error"
            for node in nodes:
                if node.status == NodeStatus.FAILED and node.error_message:
                    error = node.error_message
                    break
            await self.job_service.fail_job(job.job_id, error)

            # Update linked asset version (no-op for standalone jobs)
            if self.asset_service:
                try:
                    await self.asset_service.on_processing_failed(
                        job.job_id, error
                    )
                except Exception as e:
                    logger.error(
                        f"Asset callback failed for failed job {job.job_id}: {e}"
                    )

            # Release ownership (job is done)
            await self._job_repo.release_ownership(job.job_id, self._owner_id)

    async def _aggregate_results(self, job_id: str) -> Dict[str, Any]:
        """
        Aggregate results from completed nodes.

        Args:
            job_id: Job identifier

        Returns:
            Aggregated result data
        """
        node_repo = NodeRepository(self.pool)
        nodes = await node_repo.get_all_for_job(job_id)

        results = {}
        for node in nodes:
            if node.status == NodeStatus.COMPLETED and node.output:
                results[node.node_id] = node.output

        # Add provenance metadata
        job = await self._job_repo.get(job_id)
        if job:
            results["_provenance"] = {
                "workflow_id": job.workflow_id,
                "workflow_version": job.workflow_version,
                "job_id": job_id,
                "completed_at": datetime.now(timezone.utc).isoformat(),
            }

        return results

    # =========================================================================
    # STATS AND PROPERTIES
    # =========================================================================

    @property
    def is_running(self) -> bool:
        """Check if orchestrator is running."""
        return self._running

    @property
    def is_leader(self) -> bool:
        """Check if this instance is the active brain (holds the leader lock)."""
        return self._is_leader

    @property
    def stats(self) -> Dict[str, Any]:
        """Get orchestrator statistics."""
        uptime_seconds = None
        if self._started_at:
            uptime_seconds = (datetime.now(timezone.utc) - self._started_at).total_seconds()

        return {
            "running": self._running,
            "is_leader": self._is_leader,
            "role": "leader" if self._is_leader else "standby",
            "owner_id": self._owner_id,
            "started_at": self._started_at.isoformat() if self._started_at else None,
            "uptime_seconds": uptime_seconds,
            "poll_interval": self.poll_interval,
            "heartbeat_interval": self.HEARTBEAT_INTERVAL_SEC,
            "orphan_threshold": self.ORPHAN_THRESHOLD_SEC,
            "standby_retry_interval": self.STANDBY_RETRY_SEC,
            "cycles": self._cycles,
            "last_cycle_at": self._last_cycle_at.isoformat() if self._last_cycle_at else None,
            "active_jobs": self._active_jobs_count,
            "jobs_claimed": self._jobs_claimed,
            "jobs_reclaimed": self._jobs_reclaimed,
            "tasks_dispatched": self._tasks_dispatched,
            "results_processed": self._results_processed,
            "errors": self._errors,
            "last_heartbeat_at": self._last_heartbeat_at.isoformat() if self._last_heartbeat_at else None,
            "last_orphan_scan_at": self._last_orphan_scan_at.isoformat() if self._last_orphan_scan_at else None,
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
    Note: In multi-orchestrator mode, this only processes jobs
    owned by the temporary orchestrator instance.

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
