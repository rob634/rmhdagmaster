# ============================================================================
# WORKER EXECUTOR
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Core - Handler execution engine
# PURPOSE: Execute handlers with timeout, checkpoint, and progress support
# CREATED: 31 JAN 2026
# ============================================================================
"""
Worker Executor

Executes handlers from the registry with:
- Timeout enforcement
- Checkpoint save/resume
- Progress tracking
- Error capture

The executor is the core of the worker - it takes a DAGTaskMessage
and produces a DAGTaskResult.
"""

import asyncio
import logging
import time
import traceback
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Dict, Optional

from handlers.registry import (
    get_handler_or_raise,
    execute_handler,
    HandlerContext,
    HandlerResult,
    HandlerNotFoundError,
)
from worker.contracts import (
    DAGTaskMessage,
    DAGTaskResult,
    TaskResultStatus,
)
from worker.checkpoint import CheckpointManager, Checkpoint
from worker.progress import ProgressTracker, ProgressReport

logger = logging.getLogger(__name__)


# ============================================================================
# EXECUTION CONTEXT
# ============================================================================

@dataclass
class ExecutionContext:
    """Context for a single task execution."""
    task: DAGTaskMessage
    worker_id: str
    start_time: float
    checkpoint_manager: Optional[CheckpointManager] = None
    progress_tracker: Optional[ProgressTracker] = None

    @property
    def elapsed_ms(self) -> int:
        """Elapsed time in milliseconds."""
        return int((time.time() - self.start_time) * 1000)


# ============================================================================
# EXECUTOR
# ============================================================================

class TaskExecutor:
    """
    Executes DAG tasks.

    Takes a DAGTaskMessage, runs the handler, returns DAGTaskResult.
    """

    def __init__(
        self,
        worker_id: str,
        checkpoint_callback: Optional[Callable] = None,
        progress_callback: Optional[Callable] = None,
    ):
        """
        Initialize executor.

        Args:
            worker_id: Identifier for this worker
            checkpoint_callback: Function to save checkpoints
            progress_callback: Function to report progress
        """
        self.worker_id = worker_id
        self._checkpoint_callback = checkpoint_callback
        self._progress_callback = progress_callback

    async def execute(self, task: DAGTaskMessage) -> DAGTaskResult:
        """
        Execute a task.

        Args:
            task: Task message to execute

        Returns:
            DAGTaskResult with success or failure
        """
        start_time = time.time()
        logger.info(
            f"Executing task {task.task_id}: "
            f"handler={task.handler}, node={task.node_id}"
        )

        try:
            # Create execution context
            ctx = ExecutionContext(
                task=task,
                worker_id=self.worker_id,
                start_time=start_time,
            )

            # Set up checkpoint manager if resuming
            initial_checkpoint = None
            if task.checkpoint_phase and task.checkpoint_data:
                initial_checkpoint = Checkpoint(
                    phase=task.checkpoint_phase,
                    data=task.checkpoint_data,
                    saved_at=datetime.utcnow(),
                )

            ctx.checkpoint_manager = CheckpointManager(
                task_id=task.task_id,
                job_id=task.job_id,
                node_id=task.node_id,
                save_callback=self._checkpoint_callback,
                initial_checkpoint=initial_checkpoint,
            )

            # Set up progress tracker
            ctx.progress_tracker = ProgressTracker(
                task_id=task.task_id,
                job_id=task.job_id,
                node_id=task.node_id,
                report_callback=self._progress_callback,
            )

            # Execute with timeout
            result = await self._execute_with_timeout(ctx)

            return result

        except asyncio.TimeoutError:
            duration_ms = int((time.time() - start_time) * 1000)
            logger.error(
                f"Task {task.task_id} timed out after {task.timeout_seconds}s"
            )
            return DAGTaskResult.failure(
                task_id=task.task_id,
                job_id=task.job_id,
                node_id=task.node_id,
                error_message=f"Task timed out after {task.timeout_seconds} seconds",
                worker_id=self.worker_id,
                duration_ms=duration_ms,
            )

        except HandlerNotFoundError as e:
            duration_ms = int((time.time() - start_time) * 1000)
            logger.error(f"Handler not found: {e.handler_name}")
            return DAGTaskResult.failure(
                task_id=task.task_id,
                job_id=task.job_id,
                node_id=task.node_id,
                error_message=f"Handler not found: {e.handler_name}",
                worker_id=self.worker_id,
                duration_ms=duration_ms,
            )

        except Exception as e:
            duration_ms = int((time.time() - start_time) * 1000)
            error_msg = f"{type(e).__name__}: {str(e)}"
            logger.exception(f"Task {task.task_id} failed with exception")
            return DAGTaskResult.failure(
                task_id=task.task_id,
                job_id=task.job_id,
                node_id=task.node_id,
                error_message=error_msg[:2000],
                worker_id=self.worker_id,
                duration_ms=duration_ms,
            )

    async def _execute_with_timeout(
        self,
        ctx: ExecutionContext,
    ) -> DAGTaskResult:
        """Execute task with timeout enforcement."""
        task = ctx.task

        # Create handler context
        handler_ctx = HandlerContext(
            task_id=task.task_id,
            job_id=task.job_id,
            node_id=task.node_id,
            handler=task.handler,
            params=task.params,
            timeout_seconds=task.timeout_seconds,
            retry_count=task.retry_count,
            correlation_id=task.correlation_id,
            checkpoint_phase=task.checkpoint_phase,
            checkpoint_data=task.checkpoint_data,
            worker_id=self.worker_id,
            progress_callback=self._make_progress_callback(ctx),
        )

        # Execute handler with timeout
        async with ctx.checkpoint_manager:
            try:
                result = await asyncio.wait_for(
                    execute_handler(task.handler, handler_ctx),
                    timeout=task.timeout_seconds,
                )
            except asyncio.TimeoutError:
                raise  # Re-raise to be handled by outer try

        # Convert HandlerResult to DAGTaskResult
        return self._convert_result(ctx, result)

    def _make_progress_callback(
        self,
        ctx: ExecutionContext,
    ) -> Callable[[int, int, str], None]:
        """Create progress callback for handler."""
        async def callback(current: int, total: int, message: str) -> None:
            if ctx.progress_tracker:
                await ctx.progress_tracker.update(
                    current=current,
                    message=message,
                )
                ctx.progress_tracker.total = total

        # Return sync wrapper that schedules async call
        def sync_callback(current: int, total: int, message: str) -> None:
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    asyncio.create_task(callback(current, total, message))
                else:
                    loop.run_until_complete(callback(current, total, message))
            except Exception as e:
                logger.debug(f"Progress callback failed: {e}")

        return sync_callback

    def _convert_result(
        self,
        ctx: ExecutionContext,
        result: HandlerResult,
    ) -> DAGTaskResult:
        """Convert HandlerResult to DAGTaskResult."""
        task = ctx.task
        duration_ms = ctx.elapsed_ms

        if result.success:
            dag_result = DAGTaskResult.success(
                task_id=task.task_id,
                job_id=task.job_id,
                node_id=task.node_id,
                output=result.output,
                worker_id=self.worker_id,
                duration_ms=duration_ms,
            )
        else:
            dag_result = DAGTaskResult.failure(
                task_id=task.task_id,
                job_id=task.job_id,
                node_id=task.node_id,
                error_message=result.error_message or "Handler returned failure",
                worker_id=self.worker_id,
                duration_ms=duration_ms,
            )

        # Include checkpoint if present
        if result.checkpoint_phase:
            dag_result.checkpoint_phase = result.checkpoint_phase
            dag_result.checkpoint_data = result.checkpoint_data

        # Include final progress
        if ctx.progress_tracker:
            dag_result.progress_current = ctx.progress_tracker.current
            dag_result.progress_total = ctx.progress_tracker.total
            dag_result.progress_percent = ctx.progress_tracker.percent

        logger.info(
            f"Task {task.task_id} completed: "
            f"success={result.success}, duration={duration_ms}ms"
        )

        return dag_result


# ============================================================================
# BATCH EXECUTOR
# ============================================================================

class BatchExecutor:
    """
    Executes multiple tasks concurrently.

    Uses a semaphore to limit concurrency.
    """

    def __init__(
        self,
        worker_id: str,
        max_concurrent: int = 1,
        checkpoint_callback: Optional[Callable] = None,
        progress_callback: Optional[Callable] = None,
    ):
        """
        Initialize batch executor.

        Args:
            worker_id: Worker identifier
            max_concurrent: Maximum concurrent tasks
            checkpoint_callback: Checkpoint save function
            progress_callback: Progress report function
        """
        self.worker_id = worker_id
        self.max_concurrent = max_concurrent
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._executor = TaskExecutor(
            worker_id=worker_id,
            checkpoint_callback=checkpoint_callback,
            progress_callback=progress_callback,
        )
        self._active_tasks: Dict[str, asyncio.Task] = {}

    async def execute(self, task: DAGTaskMessage) -> DAGTaskResult:
        """
        Execute a task (with concurrency limiting).

        Args:
            task: Task to execute

        Returns:
            DAGTaskResult
        """
        async with self._semaphore:
            return await self._executor.execute(task)

    async def execute_many(
        self,
        tasks: list[DAGTaskMessage],
    ) -> list[DAGTaskResult]:
        """
        Execute multiple tasks concurrently.

        Args:
            tasks: List of tasks to execute

        Returns:
            List of results (same order as input)
        """
        async def run_one(task: DAGTaskMessage) -> DAGTaskResult:
            async with self._semaphore:
                return await self._executor.execute(task)

        # Run all tasks concurrently
        results = await asyncio.gather(
            *[run_one(task) for task in tasks],
            return_exceptions=False,
        )

        return results

    @property
    def active_count(self) -> int:
        """Number of currently executing tasks."""
        return self.max_concurrent - self._semaphore._value


# ============================================================================
# EXPORTS
# ============================================================================

__all__ = [
    "ExecutionContext",
    "TaskExecutor",
    "BatchExecutor",
]
