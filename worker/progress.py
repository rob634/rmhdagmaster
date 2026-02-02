# ============================================================================
# PROGRESS TRACKING
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Core - Progress tracking for long-running tasks
# PURPOSE: Track and report progress during task execution
# CREATED: 31 JAN 2026
# ============================================================================
"""
Progress Tracking

Enables workers to report progress during long-running tasks.
Progress is stored in dag_task_results and can be queried via API.

Design:
- Throttled: Don't spam the database with updates
- Batched: Buffer updates, flush periodically
- Callback-based: Works with HTTP callbacks or direct DB writes

Usage:
    tracker = ProgressTracker(
        task_id=task_id,
        total=len(items),
        report_callback=db_update_fn,
    )

    for i, item in enumerate(items):
        process(item)
        await tracker.update(current=i+1, message=f"Processing {item}")

    await tracker.complete()
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, Optional, Union, Awaitable

logger = logging.getLogger(__name__)


@dataclass
class ProgressReport:
    """
    Progress report data.

    Represents a single progress update.
    """
    task_id: str
    job_id: str
    node_id: str
    current: int
    total: int
    percent: float
    message: str = ""
    timestamp: datetime = field(default_factory=datetime.utcnow)

    # ETA calculation
    elapsed_seconds: Optional[float] = None
    eta_seconds: Optional[float] = None
    items_per_second: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dict."""
        return {
            "task_id": self.task_id,
            "job_id": self.job_id,
            "node_id": self.node_id,
            "current": self.current,
            "total": self.total,
            "percent": self.percent,
            "message": self.message,
            "timestamp": self.timestamp.isoformat(),
            "elapsed_seconds": self.elapsed_seconds,
            "eta_seconds": self.eta_seconds,
            "items_per_second": self.items_per_second,
        }


# Callback type
ProgressCallback = Callable[[ProgressReport], Union[None, Awaitable[None]]]


class ProgressTracker:
    """
    Tracker for task progress.

    Handles throttling, batching, and reporting.
    """

    def __init__(
        self,
        task_id: str,
        job_id: str,
        node_id: str,
        total: int = 100,
        report_callback: Optional[ProgressCallback] = None,
        min_report_interval: float = 1.0,  # seconds
        min_percent_change: float = 1.0,  # percent
    ):
        """
        Initialize progress tracker.

        Args:
            task_id: Task identifier
            job_id: Job identifier
            node_id: Node identifier
            total: Total items to process
            report_callback: Async function to report progress
            min_report_interval: Minimum seconds between reports
            min_percent_change: Minimum percent change to trigger report
        """
        self.task_id = task_id
        self.job_id = job_id
        self.node_id = node_id
        self.total = max(1, total)  # Avoid division by zero
        self._callback = report_callback
        self._min_interval = min_report_interval
        self._min_percent_change = min_percent_change

        # State
        self._current = 0
        self._last_reported_percent = 0.0
        self._last_report_time = 0.0
        self._start_time = time.time()
        self._message = ""

    @property
    def current(self) -> int:
        """Current progress count."""
        return self._current

    @property
    def percent(self) -> float:
        """Current progress percentage."""
        return min(100.0, (self._current / self.total) * 100)

    @property
    def elapsed_seconds(self) -> float:
        """Elapsed time in seconds."""
        return time.time() - self._start_time

    @property
    def eta_seconds(self) -> Optional[float]:
        """Estimated time remaining in seconds."""
        if self._current == 0:
            return None
        rate = self._current / self.elapsed_seconds
        if rate == 0:
            return None
        remaining = self.total - self._current
        return remaining / rate

    @property
    def items_per_second(self) -> float:
        """Processing rate."""
        elapsed = self.elapsed_seconds
        if elapsed == 0:
            return 0.0
        return self._current / elapsed

    async def update(
        self,
        current: Optional[int] = None,
        increment: int = 0,
        message: str = "",
        force_report: bool = False,
    ) -> bool:
        """
        Update progress.

        Args:
            current: Set current to this value
            increment: Increment current by this amount
            message: Progress message
            force_report: Report even if throttle not met

        Returns:
            True if progress was reported
        """
        # Update current
        if current is not None:
            self._current = min(current, self.total)
        else:
            self._current = min(self._current + increment, self.total)

        if message:
            self._message = message

        # Check if we should report
        if not force_report and not self._should_report():
            return False

        await self._report()
        return True

    async def complete(self, message: str = "Completed") -> None:
        """
        Mark progress as complete.

        Forces a final report at 100%.
        """
        self._current = self.total
        self._message = message
        await self._report()

    def _should_report(self) -> bool:
        """Check if we should report based on throttling."""
        now = time.time()

        # Check time interval
        if now - self._last_report_time < self._min_interval:
            return False

        # Check percent change
        current_percent = self.percent
        if abs(current_percent - self._last_reported_percent) < self._min_percent_change:
            return False

        return True

    async def _report(self) -> None:
        """Send progress report via callback."""
        self._last_report_time = time.time()
        self._last_reported_percent = self.percent

        report = ProgressReport(
            task_id=self.task_id,
            job_id=self.job_id,
            node_id=self.node_id,
            current=self._current,
            total=self.total,
            percent=self.percent,
            message=self._message,
            elapsed_seconds=self.elapsed_seconds,
            eta_seconds=self.eta_seconds,
            items_per_second=self.items_per_second,
        )

        logger.debug(
            f"Progress: {report.percent:.1f}% ({report.current}/{report.total}) "
            f"- {report.message}"
        )

        if self._callback:
            try:
                result = self._callback(report)
                if asyncio.iscoroutine(result):
                    await result
            except Exception as e:
                logger.warning(f"Failed to report progress: {e}")


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

async def report_progress(
    task_id: str,
    job_id: str,
    node_id: str,
    current: int,
    total: int,
    message: str = "",
    callback: Optional[ProgressCallback] = None,
) -> ProgressReport:
    """
    Create and optionally send a progress report.

    Args:
        task_id: Task identifier
        job_id: Job identifier
        node_id: Node identifier
        current: Current progress
        total: Total items
        message: Progress message
        callback: Optional callback to send report

    Returns:
        ProgressReport instance
    """
    percent = min(100.0, (current / max(1, total)) * 100)

    report = ProgressReport(
        task_id=task_id,
        job_id=job_id,
        node_id=node_id,
        current=current,
        total=total,
        percent=percent,
        message=message,
    )

    if callback:
        result = callback(report)
        if asyncio.iscoroutine(result):
            await result

    return report


# ============================================================================
# JOB-LEVEL AGGREGATION
# ============================================================================

@dataclass
class JobProgress:
    """
    Aggregated progress for a job.

    Combines progress from all nodes.
    """
    job_id: str
    total_nodes: int
    completed_nodes: int
    in_progress_nodes: int
    pending_nodes: int
    failed_nodes: int

    # Weighted progress (based on node progress)
    overall_percent: float

    # Node-level details
    node_progress: Dict[str, ProgressReport] = field(default_factory=dict)

    @property
    def is_complete(self) -> bool:
        """Check if job is complete."""
        return self.completed_nodes == self.total_nodes

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dict."""
        return {
            "job_id": self.job_id,
            "total_nodes": self.total_nodes,
            "completed_nodes": self.completed_nodes,
            "in_progress_nodes": self.in_progress_nodes,
            "pending_nodes": self.pending_nodes,
            "failed_nodes": self.failed_nodes,
            "overall_percent": self.overall_percent,
            "is_complete": self.is_complete,
            "node_progress": {
                node_id: report.to_dict()
                for node_id, report in self.node_progress.items()
            },
        }


def aggregate_job_progress(
    job_id: str,
    node_statuses: Dict[str, str],
    node_progress: Dict[str, ProgressReport],
) -> JobProgress:
    """
    Aggregate progress from nodes into job-level progress.

    Args:
        job_id: Job identifier
        node_statuses: Map of node_id -> status string
        node_progress: Map of node_id -> ProgressReport

    Returns:
        JobProgress instance
    """
    total = len(node_statuses)
    completed = sum(1 for s in node_statuses.values() if s in ("completed", "skipped"))
    failed = sum(1 for s in node_statuses.values() if s == "failed")
    in_progress = sum(1 for s in node_statuses.values() if s in ("running", "dispatched"))
    pending = sum(1 for s in node_statuses.values() if s in ("pending", "ready"))

    # Calculate weighted progress
    # Completed nodes count as 100%, in-progress use their actual progress
    total_weight = 0.0
    for node_id, status in node_statuses.items():
        if status in ("completed", "skipped"):
            total_weight += 100.0
        elif status == "failed":
            total_weight += 0.0  # Failed nodes don't contribute
        elif node_id in node_progress:
            total_weight += node_progress[node_id].percent
        # Pending nodes contribute 0

    overall_percent = total_weight / max(1, total)

    return JobProgress(
        job_id=job_id,
        total_nodes=total,
        completed_nodes=completed,
        in_progress_nodes=in_progress,
        pending_nodes=pending,
        failed_nodes=failed,
        overall_percent=overall_percent,
        node_progress=node_progress,
    )


# ============================================================================
# EXPORTS
# ============================================================================

__all__ = [
    "ProgressTracker",
    "ProgressReport",
    "ProgressCallback",
    "JobProgress",
    "report_progress",
    "aggregate_job_progress",
]
