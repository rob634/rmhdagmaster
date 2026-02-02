# ============================================================================
# SERVICE BUS CONSUMER
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Core - Service Bus message consumer
# PURPOSE: Receive and process DAG tasks from Service Bus queues
# CREATED: 31 JAN 2026
# ============================================================================
"""
Service Bus Consumer

Listens to Azure Service Bus queues for DAG task messages.
Dispatches tasks to executor and reports results.

Features:
- Async message processing
- Concurrent task execution (configurable)
- Graceful shutdown
- Message settlement (complete/abandon)
- Dual-mode support (DAG and legacy)
"""

import asyncio
import json
import logging
import signal
from datetime import datetime
from typing import Any, Dict, Optional, Callable

from azure.servicebus.aio import ServiceBusClient, ServiceBusReceiver
from azure.servicebus import ServiceBusMessage, ServiceBusReceivedMessage
from azure.identity.aio import DefaultAzureCredential

from worker.contracts import (
    DAGTaskMessage,
    DAGTaskResult,
    WorkerConfig,
    TaskMessageType,
    detect_message_type,
)
from worker.executor import TaskExecutor, BatchExecutor
from worker.reporter import ResultReporter, create_reporter

logger = logging.getLogger(__name__)


# ============================================================================
# CONSUMER
# ============================================================================

class ServiceBusConsumer:
    """
    Consumes messages from Azure Service Bus.

    Receives DAG task messages, executes them, and reports results.
    """

    def __init__(
        self,
        config: WorkerConfig,
        executor: Optional[TaskExecutor] = None,
        reporter: Optional[ResultReporter] = None,
    ):
        """
        Initialize consumer.

        Args:
            config: Worker configuration
            executor: Task executor (created if not provided)
            reporter: Result reporter (created if not provided)
        """
        self.config = config
        self._client: Optional[ServiceBusClient] = None
        self._receiver: Optional[ServiceBusReceiver] = None
        self._credential: Optional[DefaultAzureCredential] = None

        # Executor
        self._executor = executor or TaskExecutor(
            worker_id=config.worker_id,
        )

        # Reporter
        self._reporter = reporter

        # State
        self._running = False
        self._shutdown_event = asyncio.Event()
        self._active_tasks: Dict[str, asyncio.Task] = {}

        # Stats
        self._messages_received = 0
        self._tasks_completed = 0
        self._tasks_failed = 0

    async def start(self) -> None:
        """Start the consumer."""
        if self._running:
            logger.warning("Consumer already running")
            return

        logger.info(
            f"Starting consumer: worker_id={self.config.worker_id}, "
            f"queue={self.config.queue_name}"
        )

        # Create reporter if not provided
        if self._reporter is None:
            self._reporter = create_reporter(
                database_url=self.config.database_url,
                callback_url=self.config.callback_url,
            )

        # Create Service Bus client
        await self._connect()

        self._running = True
        self._shutdown_event.clear()

        logger.info("Consumer started")

    async def stop(self) -> None:
        """Stop the consumer gracefully."""
        if not self._running:
            return

        logger.info("Stopping consumer...")
        self._running = False
        self._shutdown_event.set()

        # Wait for active tasks
        if self._active_tasks:
            logger.info(f"Waiting for {len(self._active_tasks)} active tasks...")
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self._active_tasks.values(), return_exceptions=True),
                    timeout=self.config.shutdown_timeout_seconds,
                )
            except asyncio.TimeoutError:
                logger.warning("Shutdown timeout - cancelling remaining tasks")
                for task in self._active_tasks.values():
                    task.cancel()

        # Close connections
        await self._disconnect()

        # Close reporter
        if self._reporter:
            await self._reporter.close()

        logger.info(
            f"Consumer stopped. Stats: received={self._messages_received}, "
            f"completed={self._tasks_completed}, failed={self._tasks_failed}"
        )

    async def run(self) -> None:
        """
        Run the consumer loop.

        Receives messages until shutdown is requested.
        """
        await self.start()

        try:
            while self._running:
                try:
                    await self._receive_batch()
                except Exception as e:
                    logger.exception(f"Error in receive loop: {e}")
                    await asyncio.sleep(1)  # Brief pause before retry

        finally:
            await self.stop()

    async def _connect(self) -> None:
        """Connect to Service Bus."""
        if self.config.use_managed_identity:
            self._credential = DefaultAzureCredential()
            self._client = ServiceBusClient(
                fully_qualified_namespace=self.config.service_bus_namespace,
                credential=self._credential,
            )
        else:
            self._client = ServiceBusClient.from_connection_string(
                self.config.service_bus_connection,
            )

        self._receiver = self._client.get_queue_receiver(
            queue_name=self.config.queue_name,
            max_wait_time=5,  # seconds to wait for messages
        )

        logger.info(f"Connected to queue: {self.config.queue_name}")

    async def _disconnect(self) -> None:
        """Disconnect from Service Bus."""
        if self._receiver:
            await self._receiver.close()
            self._receiver = None

        if self._client:
            await self._client.close()
            self._client = None

        if self._credential:
            await self._credential.close()
            self._credential = None

    async def _receive_batch(self) -> None:
        """Receive and process a batch of messages."""
        if not self._receiver:
            return

        # Calculate how many messages we can receive
        available_slots = max(
            1,
            self.config.max_concurrent_tasks - len(self._active_tasks)
        )

        # Receive messages
        messages = await self._receiver.receive_messages(
            max_message_count=available_slots,
            max_wait_time=5,
        )

        for message in messages:
            self._messages_received += 1

            # Process message asynchronously
            task = asyncio.create_task(
                self._process_message(message)
            )
            task_id = str(id(message))
            self._active_tasks[task_id] = task

            # Clean up when done
            task.add_done_callback(
                lambda t, tid=task_id: self._active_tasks.pop(tid, None)
            )

    async def _process_message(
        self,
        message: ServiceBusReceivedMessage,
    ) -> None:
        """
        Process a single message.

        Args:
            message: Service Bus message
        """
        try:
            # Parse message body
            body = str(message)
            data = json.loads(body)

            # Detect message type
            msg_type = detect_message_type(data)

            if msg_type == TaskMessageType.DAG:
                await self._process_dag_task(message, data)

            elif msg_type == TaskMessageType.LEGACY:
                logger.info("Received legacy task - forwarding to legacy handler")
                # For now, just complete the message
                # In production, this would call the legacy task processor
                await self._receiver.complete_message(message)

            else:
                logger.warning(f"Unknown message type: {data}")
                await self._receiver.dead_letter_message(
                    message,
                    reason="unknown_message_type",
                    error_description="Could not determine message type",
                )

        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in message: {e}")
            await self._receiver.dead_letter_message(
                message,
                reason="invalid_json",
                error_description=str(e),
            )

        except Exception as e:
            logger.exception(f"Error processing message: {e}")
            await self._receiver.abandon_message(message)

    async def _process_dag_task(
        self,
        message: ServiceBusReceivedMessage,
        data: Dict[str, Any],
    ) -> None:
        """
        Process a DAG task message.

        Args:
            message: Original Service Bus message
            data: Parsed message data
        """
        # Parse task message
        task = DAGTaskMessage.from_dict(data)
        logger.info(f"Processing DAG task: {task.task_id}")

        try:
            # Report that we're running
            await self._reporter.report(
                DAGTaskResult.running(
                    task_id=task.task_id,
                    job_id=task.job_id,
                    node_id=task.node_id,
                    worker_id=self.config.worker_id,
                )
            )

            # Execute task
            result = await self._executor.execute(task)

            # Report result
            success = await self._reporter.report(result)

            if not success:
                logger.warning(f"Failed to report result for {task.task_id}")

            # Complete or abandon message based on result
            if result.status.value == "completed":
                await self._receiver.complete_message(message)
                self._tasks_completed += 1
            else:
                # Failed tasks are still completed from queue perspective
                # (retry logic is handled by orchestrator)
                await self._receiver.complete_message(message)
                self._tasks_failed += 1

            logger.info(
                f"Task {task.task_id} processed: status={result.status.value}"
            )

        except Exception as e:
            logger.exception(f"Error executing task {task.task_id}: {e}")

            # Report failure
            await self._reporter.report(
                DAGTaskResult.failure(
                    task_id=task.task_id,
                    job_id=task.job_id,
                    node_id=task.node_id,
                    error_message=str(e),
                    worker_id=self.config.worker_id,
                )
            )

            # Abandon message for retry
            await self._receiver.abandon_message(message)
            self._tasks_failed += 1


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

async def run_consumer(config: Optional[WorkerConfig] = None) -> None:
    """
    Run a consumer with the given configuration.

    Args:
        config: Worker configuration (uses env vars if not provided)
    """
    config = config or WorkerConfig.from_env()

    consumer = ServiceBusConsumer(config)

    # Handle shutdown signals
    loop = asyncio.get_event_loop()

    def shutdown_handler():
        logger.info("Shutdown signal received")
        asyncio.create_task(consumer.stop())

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, shutdown_handler)
        except NotImplementedError:
            # Windows doesn't support add_signal_handler
            pass

    await consumer.run()


# ============================================================================
# EXPORTS
# ============================================================================

__all__ = [
    "ServiceBusConsumer",
    "run_consumer",
]
