# ============================================================================
# TASK PUBLISHER
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Core - Service Bus task dispatch
# PURPOSE: Send task messages to worker queues
# CREATED: 29 JAN 2026
# ============================================================================
"""
Task Publisher

Sends task messages to Azure Service Bus for worker execution.
"""

import json
import logging
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta

from azure.servicebus.aio import ServiceBusClient, ServiceBusSender
from azure.servicebus import ServiceBusMessage

from core.models import TaskMessage, NodeState, WorkflowDefinition
from .config import MessagingConfig

logger = logging.getLogger(__name__)

# Global publisher instance
_publisher: Optional["TaskPublisher"] = None


class TaskPublisher:
    """Publisher for dispatching tasks to Service Bus."""

    def __init__(self, config: MessagingConfig):
        """
        Initialize task publisher.

        Args:
            config: Messaging configuration
        """
        self.config = config
        self._client: Optional[ServiceBusClient] = None
        self._sender: Optional[ServiceBusSender] = None

    async def connect(self) -> None:
        """Establish connection to Service Bus."""
        if self._client is not None:
            return

        if self.config.use_managed_identity:
            # Use managed identity authentication
            from azure.identity.aio import ManagedIdentityCredential

            if self.config.managed_identity_client_id:
                credential = ManagedIdentityCredential(
                    client_id=self.config.managed_identity_client_id
                )
            else:
                credential = ManagedIdentityCredential()

            self._client = ServiceBusClient(
                fully_qualified_namespace=self.config.fully_qualified_namespace,
                credential=credential,
            )
            logger.info(
                f"Connecting to Service Bus via managed identity: "
                f"{self.config.fully_qualified_namespace}"
            )
        else:
            # Use connection string authentication
            self._client = ServiceBusClient.from_connection_string(
                self.config.connection_string
            )
            logger.info("Connecting to Service Bus via connection string")

        self._sender = self._client.get_queue_sender(
            queue_name=self.config.worker_queue
        )
        logger.info(f"Connected to Service Bus queue: {self.config.worker_queue}")

    async def close(self) -> None:
        """Close connection to Service Bus."""
        if self._sender:
            await self._sender.close()
            self._sender = None

        if self._client:
            await self._client.close()
            self._client = None

        logger.info("Service Bus connection closed")

    async def dispatch_task(self, task: TaskMessage) -> bool:
        """
        Dispatch a single task to the worker queue.

        Args:
            task: TaskMessage to dispatch

        Returns:
            True if dispatch succeeded
        """
        if self._sender is None:
            await self.connect()

        try:
            message = ServiceBusMessage(
                body=json.dumps(task.to_queue_message()),
                message_id=task.task_id,
                correlation_id=task.correlation_id,
                subject=task.handler,
                application_properties={
                    "job_id": task.job_id,
                    "node_id": task.node_id,
                    "handler": task.handler,
                },
            )

            # Set TTL based on timeout
            message.time_to_live = timedelta(seconds=task.timeout_seconds * 2)

            await self._sender.send_messages(message)

            logger.info(
                f"Dispatched task {task.task_id} "
                f"handler={task.handler} job={task.job_id}"
            )
            return True

        except Exception as e:
            logger.error(f"Failed to dispatch task {task.task_id}: {e}")
            return False

    async def dispatch_tasks(self, tasks: List[TaskMessage]) -> int:
        """
        Dispatch multiple tasks in a batch.

        Args:
            tasks: List of TaskMessage instances

        Returns:
            Number of tasks successfully dispatched
        """
        if not tasks:
            return 0

        if self._sender is None:
            await self.connect()

        success_count = 0

        try:
            messages = []
            for task in tasks:
                message = ServiceBusMessage(
                    body=json.dumps(task.to_queue_message()),
                    message_id=task.task_id,
                    correlation_id=task.correlation_id,
                    subject=task.handler,
                    application_properties={
                        "job_id": task.job_id,
                        "node_id": task.node_id,
                        "handler": task.handler,
                    },
                )
                message.time_to_live = timedelta(seconds=task.timeout_seconds * 2)
                messages.append(message)

            await self._sender.send_messages(messages)
            success_count = len(tasks)

            logger.info(f"Dispatched batch of {success_count} tasks")

        except Exception as e:
            logger.error(f"Failed to dispatch task batch: {e}")
            # Fall back to individual dispatch
            for task in tasks:
                if await self.dispatch_task(task):
                    success_count += 1

        return success_count

    def create_task_message(
        self,
        node: NodeState,
        workflow: WorkflowDefinition,
        job_params: dict,
        checkpoint_data: Optional[Dict[str, Any]] = None,
        node_outputs: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> TaskMessage:
        """
        Create a TaskMessage from a node state.

        Args:
            node: NodeState to dispatch
            workflow: Workflow definition for handler info
            job_params: Job input parameters
            checkpoint_data: Optional checkpoint data for resumable tasks (on retry)
            node_outputs: Optional map of node_id -> output dict from completed nodes

        Returns:
            TaskMessage ready for dispatch

        Raises:
            TemplateResolutionError: If template expressions cannot be resolved
        """
        node_def = workflow.get_node(node.node_id)

        # Generate task ID
        task_id = f"{node.job_id}_{node.node_id}_{node.retry_count}"

        # Resolve parameters using Jinja2 template engine
        # Supports: {{ inputs.x }}, {{ nodes.prev.output.y }}, {{ env.VAR }}
        # Deferred import to avoid circular: messaging -> orchestrator -> messaging
        from orchestrator.engine.templates import resolve_params

        params = resolve_params(
            params=node_def.params,
            job_params=job_params,
            node_outputs=node_outputs,
        )

        return TaskMessage(
            task_id=task_id,
            job_id=node.job_id,
            node_id=node.node_id,
            handler=node_def.handler,
            params=params,
            timeout_seconds=node_def.timeout_seconds,
            retry_count=node.retry_count,
            checkpoint_data=checkpoint_data,
        )

    async def __aenter__(self) -> "TaskPublisher":
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()


async def get_publisher() -> TaskPublisher:
    """
    Get the global TaskPublisher instance.

    Returns:
        TaskPublisher instance (connected)
    """
    global _publisher

    if _publisher is None:
        config = MessagingConfig.from_env()
        _publisher = TaskPublisher(config)
        await _publisher.connect()

    return _publisher


async def close_publisher() -> None:
    """Close the global TaskPublisher instance."""
    global _publisher

    if _publisher is not None:
        await _publisher.close()
        _publisher = None
