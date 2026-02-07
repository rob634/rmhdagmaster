# ============================================================================
# JOB QUEUE REPOSITORY
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Core - Service Bus consumer for job submissions
# PURPOSE: Repository for consuming job messages from dag-jobs queue
# CREATED: 03 FEB 2026
# PATTERNS: Wraps ServiceBusConsumer with JobQueueMessage type binding
# ============================================================================
"""
Job Queue Repository

Repository for consuming job submission messages from the dag-jobs Service Bus queue.
Wraps ServiceBusConsumer with JobQueueMessage-specific logic.

This is the entry point for new jobs in the multi-orchestrator pattern:
    1. Gateway publishes JobQueueMessage to dag-jobs queue
    2. Orchestrator instances consume messages (competing consumers)
    3. First orchestrator to receive claims the job
    4. JobQueueRepository handles message parsing and disposition

Design:
    - Thin wrapper around ServiceBusConsumer
    - Type-bound to JobQueueMessage
    - Environment-based queue name configuration
    - All Service Bus I/O isolated here (repository pattern)
"""

import asyncio
import logging
import os
from typing import Awaitable, Callable, Optional, Tuple

from azure.servicebus import ServiceBusReceivedMessage

from core.models import JobQueueMessage
from infrastructure.service_bus import ServiceBusConfig, ServiceBusConsumer

logger = logging.getLogger(__name__)


class JobQueueRepository:
    """
    Repository for the dag-jobs Service Bus queue.

    Consumes job submission messages using competing consumers pattern.
    Each orchestrator instance has its own consumer - first to receive wins.

    Usage:
        repo = JobQueueRepository()
        await repo.connect()

        # Single message receive
        result = await repo.receive_one()
        if result:
            message, raw = result
            # Process message...
            await repo.complete(raw)

        # Continuous loop
        await repo.consume_loop(handler, stop_event)
    """

    def __init__(self, config: Optional[ServiceBusConfig] = None):
        """
        Initialize job queue repository.

        Args:
            config: Service Bus config. If None, loads from environment.
        """
        self._config = config or ServiceBusConfig.from_env()
        self._queue_name = os.environ.get("JOB_QUEUE_NAME")
        if not self._queue_name:
            raise ValueError("JOB_QUEUE_NAME environment variable is required")
        self._consumer: Optional[ServiceBusConsumer] = None

        logger.debug(f"JobQueueRepository initialized for queue: {self._queue_name}")

    @property
    def queue_name(self) -> str:
        """Get the queue name."""
        return self._queue_name

    async def connect(self) -> None:
        """Establish connection to Service Bus."""
        if self._consumer is not None:
            return

        self._consumer = ServiceBusConsumer(
            queue_name=self._queue_name,
            config=self._config,
        )
        await self._consumer.connect()
        logger.info(f"JobQueueRepository connected to queue: {self._queue_name}")

    async def close(self) -> None:
        """Close Service Bus connection."""
        if self._consumer:
            await self._consumer.close()
            self._consumer = None
        logger.info("JobQueueRepository connection closed")

    async def receive_one(
        self,
        max_wait_time: float = 5.0,
    ) -> Optional[Tuple[JobQueueMessage, ServiceBusReceivedMessage]]:
        """
        Receive a single job submission message.

        Non-blocking after max_wait_time - returns None if no message available.

        Args:
            max_wait_time: Maximum seconds to wait for a message

        Returns:
            Tuple of (JobQueueMessage, raw ServiceBusReceivedMessage) or None.
            The raw message is needed for complete/abandon/dead_letter operations.
        """
        if self._consumer is None:
            await self.connect()

        return await self._consumer.receive_one(
            max_wait_time=max_wait_time,
            message_type=JobQueueMessage,
        )

    async def complete(self, raw_message: ServiceBusReceivedMessage) -> None:
        """
        Complete message after successful job creation.

        Call this AFTER the job has been successfully created in the database.
        This removes the message from the queue permanently.

        Args:
            raw_message: The raw Service Bus message from receive_one()
        """
        await self._consumer.complete(raw_message)

    async def abandon(self, raw_message: ServiceBusReceivedMessage) -> None:
        """
        Abandon message for retry.

        Call this if job creation fails with a transient error.
        The message returns to the queue and will be redelivered.

        Args:
            raw_message: The raw Service Bus message from receive_one()
        """
        await self._consumer.abandon(raw_message)

    async def dead_letter(
        self,
        raw_message: ServiceBusReceivedMessage,
        reason: str,
        description: str,
    ) -> None:
        """
        Dead-letter a message (permanent failure).

        Call this for messages that cannot be processed:
        - Workflow does not exist
        - Invalid message format
        - Business rule violations

        Args:
            raw_message: The raw Service Bus message from receive_one()
            reason: Short reason code (e.g., "WorkflowNotFound")
            description: Detailed error description
        """
        await self._consumer.dead_letter(raw_message, reason, description)

    async def consume_loop(
        self,
        handler: Callable[[JobQueueMessage, ServiceBusReceivedMessage], Awaitable[bool]],
        stop_event: asyncio.Event,
    ) -> None:
        """
        Continuous consumption loop.

        Runs until stop_event is set, calling handler for each message received.

        Args:
            handler: Async function(message: JobQueueMessage, raw: ServiceBusReceivedMessage) -> bool.
                     Return True to complete the message (success).
                     Return False to abandon the message (retry).
                     Exceptions are caught and result in abandon.
            stop_event: asyncio.Event to signal loop termination.

        Example:
            async def handle_job(msg: JobQueueMessage, raw) -> bool:
                try:
                    await create_job_from_message(msg)
                    return True  # Complete
                except TransientError:
                    return False  # Abandon for retry
                except PermanentError:
                    await repo.dead_letter(raw, "PermanentError", str(e))
                    return True  # Don't retry

            await repo.consume_loop(handle_job, stop_event)
        """
        if self._consumer is None:
            await self.connect()

        logger.info(f"Starting job queue consume loop on {self._queue_name}")

        await self._consumer.consume_loop(
            handler=handler,
            stop_event=stop_event,
            message_type=JobQueueMessage,
        )

        logger.info(f"Job queue consume loop stopped on {self._queue_name}")


# ============================================================================
# FACTORY FUNCTION
# ============================================================================

def create_job_queue_repository(
    config: Optional[ServiceBusConfig] = None,
) -> JobQueueRepository:
    """
    Create a JobQueueRepository instance.

    Args:
        config: Optional Service Bus config. If None, loads from environment.

    Returns:
        Configured JobQueueRepository
    """
    return JobQueueRepository(config)


# ============================================================================
# EXPORTS
# ============================================================================

__all__ = [
    "JobQueueRepository",
    "create_job_queue_repository",
]
