# ============================================================================
# RESULT REPORTER
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Core - Task result reporting
# PURPOSE: Report task results to orchestrator (DB or HTTP)
# CREATED: 31 JAN 2026
# ============================================================================
"""
Result Reporter

Reports task results back to the orchestrator.

Supports two modes:
1. Direct DB: Write directly to dagapp.dag_task_results
2. HTTP Callback: POST to orchestrator API endpoint

The HTTP mode is preferred for:
- Workers without direct DB access
- Function App workers (serverless)
- Reduced coupling
"""

import asyncio
import json
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, Optional

import aiohttp

from worker.contracts import DAGTaskResult, TaskResultStatus

logger = logging.getLogger(__name__)


# ============================================================================
# ABSTRACT REPORTER
# ============================================================================

class ResultReporter(ABC):
    """Abstract base for result reporters."""

    @abstractmethod
    async def report(self, result: DAGTaskResult) -> bool:
        """
        Report a task result.

        Args:
            result: Task result to report

        Returns:
            True if report succeeded
        """
        pass

    @abstractmethod
    async def report_progress(
        self,
        task_id: str,
        job_id: str,
        node_id: str,
        current: int,
        total: int,
        message: str = "",
    ) -> bool:
        """
        Report task progress.

        Args:
            task_id: Task identifier
            job_id: Job identifier
            node_id: Node identifier
            current: Current progress
            total: Total items
            message: Progress message

        Returns:
            True if report succeeded
        """
        pass

    @abstractmethod
    async def report_checkpoint(
        self,
        task_id: str,
        job_id: str,
        node_id: str,
        phase_name: str,
        phase_index: int,
        total_phases: int,
        progress_current: int,
        progress_total: Optional[int],
        progress_message: Optional[str],
        state_data: Optional[Dict[str, Any]],
        artifacts_completed: Optional[list],
        is_final: bool = False,
    ) -> Optional[str]:
        """
        Report a checkpoint during task execution.

        Args:
            task_id: Task identifier
            job_id: Job identifier
            node_id: Node identifier
            phase_name: Current phase name
            phase_index: Zero-based phase index
            total_phases: Total number of phases
            progress_current: Current progress count
            progress_total: Total items (optional)
            progress_message: Progress message
            state_data: Handler-specific state for resume
            artifacts_completed: List of completed artifact IDs
            is_final: True if task completed successfully

        Returns:
            Checkpoint ID if successful, None otherwise
        """
        pass

    async def close(self) -> None:
        """Clean up resources."""
        pass


# ============================================================================
# DATABASE REPORTER
# ============================================================================

class DatabaseReporter(ResultReporter):
    """
    Reports results directly to PostgreSQL.

    Uses psycopg3 for async database access.
    """

    def __init__(self, connection_string: str):
        """
        Initialize database reporter.

        Args:
            connection_string: PostgreSQL connection string
        """
        self._connection_string = connection_string
        self._pool = None

    async def _get_pool(self):
        """Get or create connection pool."""
        if self._pool is None:
            import psycopg_pool
            self._pool = psycopg_pool.AsyncConnectionPool(
                self._connection_string,
                min_size=1,
                max_size=5,
            )
            await self._pool.open()
        return self._pool

    async def report(self, result: DAGTaskResult) -> bool:
        """Report result to database."""
        try:
            pool = await self._get_pool()

            async with pool.connection() as conn:
                async with conn.cursor() as cur:
                    # Upsert result (idempotent)
                    await cur.execute(
                        """
                        INSERT INTO dagapp.dag_task_results (
                            task_id, job_id, node_id, status, output,
                            error_message, worker_id, execution_duration_ms,
                            progress_current, progress_total, progress_percent,
                            progress_message, checkpoint_phase, checkpoint_data,
                            reported_at, processed
                        ) VALUES (
                            %s, %s, %s, %s, %s,
                            %s, %s, %s,
                            %s, %s, %s,
                            %s, %s, %s,
                            %s, false
                        )
                        ON CONFLICT (task_id) DO UPDATE SET
                            status = EXCLUDED.status,
                            output = EXCLUDED.output,
                            error_message = EXCLUDED.error_message,
                            execution_duration_ms = EXCLUDED.execution_duration_ms,
                            progress_current = EXCLUDED.progress_current,
                            progress_total = EXCLUDED.progress_total,
                            progress_percent = EXCLUDED.progress_percent,
                            progress_message = EXCLUDED.progress_message,
                            checkpoint_phase = EXCLUDED.checkpoint_phase,
                            checkpoint_data = EXCLUDED.checkpoint_data,
                            reported_at = EXCLUDED.reported_at
                        """,
                        (
                            result.task_id,
                            result.job_id,
                            result.node_id,
                            result.status.value,
                            json.dumps(result.output) if result.output else None,
                            result.error_message,
                            result.worker_id,
                            result.execution_duration_ms,
                            result.progress_current,
                            result.progress_total,
                            result.progress_percent,
                            result.progress_message,
                            result.checkpoint_phase,
                            json.dumps(result.checkpoint_data) if result.checkpoint_data else None,
                            result.reported_at,
                        )
                    )

            logger.debug(f"Reported result for task {result.task_id} to database")
            return True

        except Exception as e:
            logger.exception(f"Failed to report result to database: {e}")
            return False

    async def report_progress(
        self,
        task_id: str,
        job_id: str,
        node_id: str,
        current: int,
        total: int,
        message: str = "",
    ) -> bool:
        """Report progress to database."""
        try:
            pool = await self._get_pool()
            percent = (current / max(1, total)) * 100

            async with pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        INSERT INTO dagapp.dag_task_results (
                            task_id, job_id, node_id, status,
                            progress_current, progress_total, progress_percent,
                            progress_message, reported_at, processed
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, false)
                        ON CONFLICT (task_id) DO UPDATE SET
                            progress_current = EXCLUDED.progress_current,
                            progress_total = EXCLUDED.progress_total,
                            progress_percent = EXCLUDED.progress_percent,
                            progress_message = EXCLUDED.progress_message,
                            reported_at = EXCLUDED.reported_at
                        """,
                        (
                            task_id,
                            job_id,
                            node_id,
                            TaskResultStatus.RUNNING.value,
                            current,
                            total,
                            percent,
                            message,
                            datetime.utcnow(),
                        )
                    )

            return True

        except Exception as e:
            logger.warning(f"Failed to report progress: {e}")
            return False

    async def report_checkpoint(
        self,
        task_id: str,
        job_id: str,
        node_id: str,
        phase_name: str,
        phase_index: int,
        total_phases: int,
        progress_current: int,
        progress_total: Optional[int],
        progress_message: Optional[str],
        state_data: Optional[Dict[str, Any]],
        artifacts_completed: Optional[list],
        is_final: bool = False,
    ) -> Optional[str]:
        """Report checkpoint to database."""
        import uuid

        try:
            pool = await self._get_pool()
            checkpoint_id = str(uuid.uuid4())
            progress_percent = None
            if progress_total and progress_total > 0:
                progress_percent = (progress_current / progress_total) * 100

            async with pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        INSERT INTO dagapp.dag_checkpoints (
                            checkpoint_id, job_id, node_id, task_id,
                            phase_name, phase_index, total_phases,
                            progress_current, progress_total, progress_percent,
                            progress_message, state_data, artifacts_completed,
                            is_final, is_valid, created_at, updated_at
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s,
                            %s, true, %s, %s
                        )
                        """,
                        (
                            checkpoint_id,
                            job_id,
                            node_id,
                            task_id,
                            phase_name,
                            phase_index,
                            total_phases,
                            progress_current,
                            progress_total,
                            progress_percent,
                            progress_message,
                            json.dumps(state_data) if state_data else None,
                            json.dumps(artifacts_completed) if artifacts_completed else '[]',
                            is_final,
                            datetime.utcnow(),
                            datetime.utcnow(),
                        )
                    )

            logger.debug(f"Saved checkpoint {checkpoint_id} for task {task_id}")
            return checkpoint_id

        except Exception as e:
            logger.warning(f"Failed to save checkpoint: {e}")
            return None

    async def close(self) -> None:
        """Close connection pool."""
        if self._pool:
            await self._pool.close()
            self._pool = None


# ============================================================================
# HTTP REPORTER
# ============================================================================

class HTTPReporter(ResultReporter):
    """
    Reports results via HTTP callback to orchestrator.

    POST /api/v1/callbacks/task-result
    """

    def __init__(
        self,
        callback_url: str,
        timeout_seconds: float = 30.0,
        max_retries: int = 3,
    ):
        """
        Initialize HTTP reporter.

        Args:
            callback_url: Base URL for callback endpoint
            timeout_seconds: Request timeout
            max_retries: Maximum retry attempts
        """
        self._callback_url = callback_url.rstrip("/")
        self._timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        self._max_retries = max_retries
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create HTTP session."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=self._timeout)
        return self._session

    async def report(self, result: DAGTaskResult) -> bool:
        """Report result via HTTP POST."""
        url = f"{self._callback_url}/api/v1/callbacks/task-result"
        payload = result.to_dict()

        for attempt in range(self._max_retries):
            try:
                session = await self._get_session()

                async with session.post(
                    url,
                    json=payload,
                    headers={"Content-Type": "application/json"},
                ) as response:
                    if response.status in (200, 201, 202):
                        logger.debug(
                            f"Reported result for task {result.task_id} "
                            f"via HTTP (status={response.status})"
                        )
                        return True

                    body = await response.text()
                    logger.warning(
                        f"HTTP callback failed: status={response.status}, "
                        f"body={body[:500]}"
                    )

            except asyncio.TimeoutError:
                logger.warning(
                    f"HTTP callback timeout (attempt {attempt + 1}/{self._max_retries})"
                )

            except aiohttp.ClientError as e:
                logger.warning(
                    f"HTTP callback error: {e} "
                    f"(attempt {attempt + 1}/{self._max_retries})"
                )

            # Exponential backoff
            if attempt < self._max_retries - 1:
                await asyncio.sleep(2 ** attempt)

        logger.error(f"Failed to report result after {self._max_retries} attempts")
        return False

    async def report_progress(
        self,
        task_id: str,
        job_id: str,
        node_id: str,
        current: int,
        total: int,
        message: str = "",
    ) -> bool:
        """Report progress via HTTP POST."""
        url = f"{self._callback_url}/api/v1/callbacks/task-progress"
        percent = (current / max(1, total)) * 100

        payload = {
            "task_id": task_id,
            "job_id": job_id,
            "node_id": node_id,
            "progress_current": current,
            "progress_total": total,
            "progress_percent": percent,
            "progress_message": message,
        }

        try:
            session = await self._get_session()

            async with session.post(
                url,
                json=payload,
                headers={"Content-Type": "application/json"},
            ) as response:
                return response.status in (200, 201, 202)

        except Exception as e:
            logger.debug(f"Progress report failed: {e}")
            return False

    async def report_checkpoint(
        self,
        task_id: str,
        job_id: str,
        node_id: str,
        phase_name: str,
        phase_index: int,
        total_phases: int,
        progress_current: int,
        progress_total: Optional[int],
        progress_message: Optional[str],
        state_data: Optional[Dict[str, Any]],
        artifacts_completed: Optional[list],
        is_final: bool = False,
    ) -> Optional[str]:
        """Report checkpoint via HTTP POST."""
        url = f"{self._callback_url}/api/v1/callbacks/checkpoint"

        payload = {
            "task_id": task_id,
            "job_id": job_id,
            "node_id": node_id,
            "phase_name": phase_name,
            "phase_index": phase_index,
            "total_phases": total_phases,
            "progress_current": progress_current,
            "progress_total": progress_total,
            "progress_message": progress_message,
            "state_data": state_data,
            "artifacts_completed": artifacts_completed or [],
            "is_final": is_final,
        }

        try:
            session = await self._get_session()

            async with session.post(
                url,
                json=payload,
                headers={"Content-Type": "application/json"},
            ) as response:
                if response.status in (200, 201, 202):
                    data = await response.json()
                    checkpoint_id = data.get("checkpoint_id")
                    logger.debug(f"Saved checkpoint {checkpoint_id} for task {task_id}")
                    return checkpoint_id

                body = await response.text()
                logger.warning(f"Checkpoint save failed: status={response.status}, body={body[:500]}")
                return None

        except Exception as e:
            logger.warning(f"Checkpoint save failed: {e}")
            return None

    async def close(self) -> None:
        """Close HTTP session."""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None


# ============================================================================
# FACTORY
# ============================================================================

def create_reporter(
    database_url: Optional[str] = None,
    callback_url: Optional[str] = None,
) -> ResultReporter:
    """
    Create appropriate reporter based on configuration.

    Prefers HTTP callback if both are provided.

    Args:
        database_url: PostgreSQL connection string
        callback_url: HTTP callback URL

    Returns:
        ResultReporter instance

    Raises:
        ValueError if neither option is provided
    """
    if callback_url:
        logger.info(f"Using HTTP reporter: {callback_url}")
        return HTTPReporter(callback_url)

    if database_url:
        logger.info("Using database reporter")
        return DatabaseReporter(database_url)

    raise ValueError(
        "Either database_url or callback_url must be provided"
    )


# ============================================================================
# EXPORTS
# ============================================================================

__all__ = [
    "ResultReporter",
    "DatabaseReporter",
    "HTTPReporter",
    "create_reporter",
]
