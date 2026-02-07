# ============================================================================
# SERVICE BUS INFRASTRUCTURE
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Infrastructure - Azure Service Bus messaging
# PURPOSE: Repository pattern for Service Bus queue operations
# CREATED: 03 FEB 2026
# PATTERNS: Extracted from rmhgeoapi/infrastructure/service_bus.py
# ============================================================================
"""
Service Bus Infrastructure

Repository pattern implementation for Azure Service Bus operations.
Provides both publisher (send) and consumer (receive) capabilities.

Key Design Decisions (from rmhgeoapi):
    - Singleton pattern for credential reuse
    - Sender caching with AMQP connection warmup
    - Dual auth: connection string OR managed identity
    - Error categorization: permanent vs transient
    - Pydantic model serialization

Usage:
    # Publisher (send messages)
    publisher = ServiceBusPublisher.instance()
    publisher.send_message("queue-name", my_pydantic_model)

    # Consumer (receive messages)
    consumer = ServiceBusConsumer("queue-name")
    async for message in consumer.receive_loop():
        process(message)
"""

import asyncio
import json
import logging
import os
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional, TypeVar

from azure.identity import DefaultAzureCredential
from azure.identity.aio import DefaultAzureCredential as AsyncDefaultAzureCredential
from azure.servicebus import ServiceBusClient, ServiceBusMessage, ServiceBusSender
from azure.servicebus.aio import ServiceBusClient as AsyncServiceBusClient
from azure.servicebus.aio import ServiceBusReceiver as AsyncServiceBusReceiver
from azure.servicebus.exceptions import (
    MessageAlreadySettled,
    MessageLockLostError,
    MessageSizeExceededError,
    MessagingEntityNotFoundError,
    OperationTimeoutError,
    ServiceBusAuthenticationError,
    ServiceBusAuthorizationError,
    ServiceBusConnectionError,
    ServiceBusCommunicationError,
    ServiceBusError,
    ServiceBusQuotaExceededError,
    ServiceBusServerBusyError,
)
from pydantic import BaseModel

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)


# ============================================================================
# CONFIGURATION
# ============================================================================

@dataclass
class ServiceBusConfig:
    """Service Bus configuration from environment."""

    fully_qualified_namespace: str = ""
    connection_string: Optional[str] = None
    managed_identity_client_id: Optional[str] = None
    max_batch_size: int = 100
    retry_count: int = 3
    retry_delay_seconds: float = 1.0

    @classmethod
    def from_env(cls) -> "ServiceBusConfig":
        """Load configuration from environment variables."""
        return cls(
            fully_qualified_namespace=os.environ.get(
                "SERVICE_BUS_NAMESPACE",
                os.environ.get("SERVICE_BUS_FQDN", "")
            ),
            connection_string=os.environ.get("SERVICE_BUS_CONNECTION_STRING"),
            managed_identity_client_id=os.environ.get("MANAGED_IDENTITY_CLIENT_ID"),
            max_batch_size=int(os.environ.get("SERVICE_BUS_MAX_BATCH_SIZE", "100")),
            retry_count=int(os.environ.get("SERVICE_BUS_RETRY_COUNT", "3")),
            retry_delay_seconds=float(os.environ.get("SERVICE_BUS_RETRY_DELAY", "1.0")),
        )

    @property
    def use_connection_string(self) -> bool:
        """Check if connection string auth should be used."""
        return bool(self.connection_string)


# ============================================================================
# BATCH RESULT
# ============================================================================

@dataclass
class BatchResult:
    """Result of a batch send operation."""
    success: bool
    messages_sent: int
    batch_count: int
    elapsed_ms: float
    errors: List[str] = field(default_factory=list)


# ============================================================================
# SERVICE BUS PUBLISHER (Singleton)
# ============================================================================

class ServiceBusPublisher:
    """
    Service Bus message publisher with sender caching.

    Thread-safe singleton that manages Service Bus connections and provides
    high-performance message sending with automatic retry.

    Key Features:
        - Singleton pattern for credential reuse
        - Sender caching with AMQP connection warmup
        - Health checks before send operations
        - Automatic retry with exponential backoff
        - Batch sending support
    """

    _instance: Optional["ServiceBusPublisher"] = None
    _lock = threading.Lock()

    def __new__(cls):
        """Thread-safe singleton creation."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        """Initialize Service Bus publisher."""
        if hasattr(self, "_initialized"):
            return

        logger.info("Initializing ServiceBusPublisher")

        self.config = ServiceBusConfig.from_env()
        self._client: Optional[ServiceBusClient] = None
        self._senders: Dict[str, ServiceBusSender] = {}
        self._credential = None

        self._connect()
        self._initialized = True

        logger.info(
            f"ServiceBusPublisher initialized "
            f"(namespace={self.config.fully_qualified_namespace or 'connection_string'}, "
            f"batch_size={self.config.max_batch_size})"
        )

    def _connect(self) -> None:
        """Establish connection to Service Bus."""
        if self.config.use_connection_string:
            logger.info("Using connection string authentication")
            self._client = ServiceBusClient.from_connection_string(
                self.config.connection_string,
                retry_total=5,
                retry_backoff_factor=0.5,
                retry_backoff_max=60,
                retry_mode="exponential",
            )
        else:
            if not self.config.fully_qualified_namespace:
                raise ValueError(
                    "SERVICE_BUS_NAMESPACE environment variable not set. "
                    "Required for managed identity authentication."
                )

            logger.info(f"Using managed identity for namespace: {self.config.fully_qualified_namespace}")
            self._credential = DefaultAzureCredential()
            self._client = ServiceBusClient(
                fully_qualified_namespace=self.config.fully_qualified_namespace,
                credential=self._credential,
                retry_total=5,
                retry_backoff_factor=0.5,
                retry_backoff_max=60,
                retry_mode="exponential",
            )

    @classmethod
    def instance(cls) -> "ServiceBusPublisher":
        """Get singleton instance."""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def _get_sender(self, queue_name: str) -> ServiceBusSender:
        """
        Get or create a message sender with connection warmup.

        CRITICAL: The Azure Service Bus SDK creates senders with lazy AMQP
        connections. If we send a message immediately after creating a sender,
        the message can be lost during AMQP link establishment.

        Solution: Explicitly open the sender connection before caching it.
        """
        if queue_name in self._senders:
            sender = self._senders[queue_name]
            # Check if sender is still healthy
            if self._check_sender_health(sender):
                return sender
            else:
                # Unhealthy - recreate
                logger.warning(f"Sender unhealthy for {queue_name}, recreating")
                try:
                    sender.close()
                except Exception:
                    pass
                del self._senders[queue_name]

        # Create new sender with warmup
        logger.debug(f"Creating new sender for queue: {queue_name}")
        sender = self._client.get_queue_sender(queue_name)

        # CRITICAL: Warm up AMQP connection before caching
        try:
            sender._open()
            logger.debug(f"Sender AMQP link established for queue: {queue_name}")
        except Exception as e:
            logger.error(f"Sender warmup failed for {queue_name}: {e}")
            try:
                sender.close()
            except Exception:
                pass
            raise RuntimeError(f"Sender warmup failed: {e}")

        self._senders[queue_name] = sender
        return sender

    def _check_sender_health(self, sender: ServiceBusSender) -> bool:
        """Check if sender is healthy (has active AMQP link)."""
        try:
            running = getattr(sender, "_running", False)
            handler = getattr(sender, "_handler", None)
            shutdown = getattr(sender, "_shutdown", None)

            if shutdown and shutdown.is_set():
                return False

            return running and handler is not None
        except Exception:
            return False

    def send_message(
        self,
        queue_name: str,
        message: BaseModel,
        ttl_hours: int = 24,
    ) -> str:
        """
        Send a single message to Service Bus.

        Args:
            queue_name: Target queue name
            message: Pydantic model to send
            ttl_hours: Message time-to-live in hours

        Returns:
            Message ID

        Raises:
            RuntimeError: If send fails after retries
        """
        sender = self._get_sender(queue_name)

        # Serialize message
        message_json = message.model_dump_json()

        # Create Service Bus message
        sb_message = ServiceBusMessage(
            body=message_json,
            content_type="application/json",
            time_to_live=timedelta(hours=ttl_hours),
        )

        # Add application properties for tracing
        sb_message.application_properties = {}
        for attr in ("task_id", "job_id", "node_id", "message_id"):
            if hasattr(message, attr):
                sb_message.application_properties[attr] = getattr(message, attr)

        # Send with retry
        for attempt in range(self.config.retry_count):
            try:
                sender.send_messages(sb_message)
                message_id = sb_message.message_id or f"sb_{datetime.now(timezone.utc).timestamp()}"

                logger.info(
                    f"Message sent to {queue_name}: {message_id}",
                    extra={
                        "queue": queue_name,
                        "message_id": message_id,
                        "message_type": type(message).__name__,
                    }
                )
                return message_id

            except (ServiceBusAuthenticationError, ServiceBusAuthorizationError) as e:
                # Permanent: Auth failures won't resolve with retry
                logger.error(f"Auth failed for {queue_name}: {e}")
                raise RuntimeError(f"Service Bus auth failed: {e}")

            except MessageSizeExceededError as e:
                # Permanent: Message too large (256KB limit)
                logger.error(f"Message too large for {queue_name}: {e}")
                raise RuntimeError(f"Message exceeds 256KB limit: {e}")

            except MessagingEntityNotFoundError as e:
                # Permanent: Queue doesn't exist
                logger.error(f"Queue '{queue_name}' not found: {e}")
                raise RuntimeError(f"Queue '{queue_name}' does not exist: {e}")

            except ServiceBusQuotaExceededError as e:
                # Permanent: Quota exceeded
                logger.error(f"Service Bus quota exceeded: {e}")
                raise RuntimeError(f"Service Bus quota exceeded: {e}")

            except (OperationTimeoutError, ServiceBusServerBusyError,
                    ServiceBusConnectionError, ServiceBusCommunicationError) as e:
                # Transient: Retry with backoff
                logger.warning(
                    f"Transient error on attempt {attempt + 1}/{self.config.retry_count}: "
                    f"{type(e).__name__}"
                )
                if attempt == self.config.retry_count - 1:
                    raise RuntimeError(f"Failed to send after {self.config.retry_count} attempts: {e}")
                time.sleep(self.config.retry_delay_seconds * (2 ** attempt))

            except ServiceBusError as e:
                # Other Service Bus errors - retry
                logger.warning(f"ServiceBusError on attempt {attempt + 1}: {type(e).__name__}")
                if attempt == self.config.retry_count - 1:
                    raise RuntimeError(f"Failed to send to {queue_name}: {e}")
                time.sleep(self.config.retry_delay_seconds * (2 ** attempt))

    def batch_send_messages(
        self,
        queue_name: str,
        messages: List[BaseModel],
        batch_size: Optional[int] = None,
    ) -> BatchResult:
        """
        Send messages in batches for high performance.

        Args:
            queue_name: Target queue name
            messages: List of Pydantic models to send
            batch_size: Override default batch size (max 100)

        Returns:
            BatchResult with statistics
        """
        start_time = time.time()
        batch_size = min(batch_size or self.config.max_batch_size, 100)

        logger.info(f"Batch sending {len(messages)} messages to {queue_name}")

        sender = self._get_sender(queue_name)
        messages_sent = 0
        batch_count = 0
        errors = []

        for i in range(0, len(messages), batch_size):
            batch = messages[i:i + batch_size]
            batch_count += 1

            sb_messages = []
            for msg in batch:
                sb_message = ServiceBusMessage(
                    body=msg.model_dump_json(),
                    content_type="application/json",
                )
                if hasattr(msg, "task_id"):
                    sb_message.application_properties = {"task_id": msg.task_id}
                sb_messages.append(sb_message)

            for attempt in range(self.config.retry_count):
                try:
                    sender.send_messages(sb_messages)
                    messages_sent += len(sb_messages)
                    break
                except (OperationTimeoutError, ServiceBusServerBusyError) as e:
                    if attempt == self.config.retry_count - 1:
                        errors.append(f"Batch {batch_count} failed: {type(e).__name__}")
                    else:
                        time.sleep(self.config.retry_delay_seconds * (2 ** attempt))
                except Exception as e:
                    errors.append(f"Batch {batch_count}: {e}")
                    break

        elapsed_ms = (time.time() - start_time) * 1000

        result = BatchResult(
            success=len(errors) == 0,
            messages_sent=messages_sent,
            batch_count=batch_count,
            elapsed_ms=elapsed_ms,
            errors=errors,
        )

        logger.info(
            f"Batch send complete: {messages_sent}/{len(messages)} messages "
            f"in {batch_count} batches, {elapsed_ms:.2f}ms"
        )

        return result

    def close(self) -> None:
        """Close all connections."""
        for queue_name, sender in self._senders.items():
            try:
                sender.close()
            except Exception as e:
                logger.warning(f"Error closing sender for {queue_name}: {e}")
        self._senders.clear()

        if self._client:
            try:
                self._client.close()
            except Exception as e:
                logger.warning(f"Error closing client: {e}")


# ============================================================================
# SERVICE BUS CONSUMER (Async)
# ============================================================================

class ServiceBusConsumer:
    """
    Async Service Bus message consumer.

    Provides async iteration over messages from a Service Bus queue
    with automatic message completion/abandonment.

    Usage:
        consumer = ServiceBusConsumer("my-queue")
        await consumer.connect()

        message = await consumer.receive_one()
        if message:
            parsed, raw = message
            try:
                await process(parsed)
                await consumer.complete(raw)
            except Exception:
                await consumer.abandon(raw)
    """

    def __init__(
        self,
        queue_name: str,
        config: Optional[ServiceBusConfig] = None,
    ):
        """
        Initialize consumer for a specific queue.

        Args:
            queue_name: Queue to consume from
            config: Optional configuration (defaults to from_env())
        """
        self.queue_name = queue_name
        self.config = config or ServiceBusConfig.from_env()

        self._client: Optional[AsyncServiceBusClient] = None
        self._receiver: Optional[AsyncServiceBusReceiver] = None
        self._credential = None

    async def connect(self) -> None:
        """Establish async connection to Service Bus."""
        if self._client is not None:
            return

        if self.config.use_connection_string:
            logger.info(f"Consumer connecting to {self.queue_name} (connection string)")
            self._client = AsyncServiceBusClient.from_connection_string(
                self.config.connection_string,
                retry_total=5,
                retry_backoff_factor=0.5,
                retry_backoff_max=60,
                retry_mode="exponential",
            )
        else:
            logger.info(
                f"Consumer connecting to {self.queue_name} "
                f"(namespace={self.config.fully_qualified_namespace})"
            )
            self._credential = AsyncDefaultAzureCredential()
            self._client = AsyncServiceBusClient(
                fully_qualified_namespace=self.config.fully_qualified_namespace,
                credential=self._credential,
                retry_total=5,
                retry_backoff_factor=0.5,
                retry_backoff_max=60,
                retry_mode="exponential",
            )

        self._receiver = self._client.get_queue_receiver(
            queue_name=self.queue_name,
            max_wait_time=5,
        )

        logger.info(f"Consumer connected to queue: {self.queue_name}")

    async def close(self) -> None:
        """Close connection."""
        if self._receiver:
            await self._receiver.close()
            self._receiver = None

        if self._client:
            await self._client.close()
            self._client = None

        logger.info(f"Consumer closed for queue: {self.queue_name}")

    async def receive_one(
        self,
        max_wait_time: float = 5.0,
        message_type: Optional[type] = None,
    ) -> Optional[tuple]:
        """
        Receive a single message (non-blocking after timeout).

        Args:
            max_wait_time: Max seconds to wait for a message
            message_type: Optional Pydantic model type to parse into

        Returns:
            Tuple of (parsed_message, raw_message) or None if no message.
            If message_type is provided, parsed_message is that type.
            Otherwise, parsed_message is a dict.
        """
        if self._receiver is None:
            await self.connect()

        messages = await self._receiver.receive_messages(
            max_message_count=1,
            max_wait_time=max_wait_time,
        )

        if not messages:
            return None

        raw_msg = messages[0]

        try:
            body = str(raw_msg)

            if message_type is not None:
                parsed = message_type.model_validate_json(body)
            else:
                parsed = json.loads(body)

            return (parsed, raw_msg)

        except Exception as e:
            logger.error(f"Failed to parse message: {e}")
            # Dead-letter malformed messages
            await self._receiver.dead_letter_message(
                raw_msg,
                reason="ParseError",
                error_description=str(e),
            )
            return None

    async def complete(self, raw_message) -> None:
        """Complete (acknowledge) a message after successful processing."""
        await self._receiver.complete_message(raw_message)
        logger.debug(f"Completed message: {raw_message.message_id}")

    async def abandon(self, raw_message) -> None:
        """Abandon a message (return to queue for retry)."""
        await self._receiver.abandon_message(raw_message)
        logger.debug(f"Abandoned message: {raw_message.message_id}")

    async def dead_letter(
        self,
        raw_message,
        reason: str,
        description: str,
    ) -> None:
        """Dead-letter a message (permanent failure)."""
        await self._receiver.dead_letter_message(
            raw_message,
            reason=reason,
            error_description=description,
        )
        logger.warning(f"Dead-lettered message {raw_message.message_id}: {reason}")

    async def consume_loop(
        self,
        handler: Callable,
        stop_event: asyncio.Event,
        message_type: Optional[type] = None,
    ) -> None:
        """
        Continuous consumption loop.

        Args:
            handler: Async function called for each message.
                     Signature: handler(parsed, raw) -> bool
                     Returns True to complete, False to abandon.
            stop_event: Event to signal loop termination.
            message_type: Optional Pydantic model type to parse into.
        """
        logger.info(f"Starting consume loop for queue: {self.queue_name}")

        await self.connect()

        while not stop_event.is_set():
            try:
                result = await self.receive_one(
                    max_wait_time=5.0,
                    message_type=message_type,
                )

                if result is None:
                    continue

                parsed, raw = result

                try:
                    success = await handler(parsed, raw)
                    if success:
                        await self.complete(raw)
                    else:
                        await self.abandon(raw)
                except Exception as e:
                    logger.exception(f"Handler error for message {raw.message_id}: {e}")
                    await self.abandon(raw)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception(f"Error in consume loop: {e}")
                await asyncio.sleep(1)

        logger.info(f"Consume loop stopped for queue: {self.queue_name}")


# ============================================================================
# FACTORY FUNCTIONS
# ============================================================================

def get_publisher() -> ServiceBusPublisher:
    """Get the singleton ServiceBusPublisher instance."""
    return ServiceBusPublisher.instance()


def create_consumer(queue_name: str) -> ServiceBusConsumer:
    """Create a new ServiceBusConsumer for a queue."""
    return ServiceBusConsumer(queue_name)


# ============================================================================
# EXPORTS
# ============================================================================

__all__ = [
    "ServiceBusConfig",
    "ServiceBusPublisher",
    "ServiceBusConsumer",
    "BatchResult",
    "get_publisher",
    "create_consumer",
]
