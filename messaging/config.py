# ============================================================================
# MESSAGING CONFIGURATION
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Core - Service Bus configuration
# PURPOSE: Centralize messaging configuration
# CREATED: 29 JAN 2026
# ============================================================================
"""
Messaging Configuration

Configuration for Azure Service Bus connections and queues.
Supports both connection string and managed identity authentication.
"""

import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class MessagingConfig:
    """
    Configuration for Azure Service Bus messaging.

    Loaded from environment variables.
    Supports both connection string and managed identity authentication.
    """
    # Connection - either connection_string OR fully_qualified_namespace
    connection_string: Optional[str] = None
    fully_qualified_namespace: Optional[str] = None

    # Managed identity settings
    use_managed_identity: bool = False
    managed_identity_client_id: Optional[str] = None

    # Queue names (MUST be set explicitly - no defaults)
    worker_queue: str = ""
    result_queue: str = ""

    # Callback configuration (alternative to result queue)
    callback_base_url: Optional[str] = None

    # Timeouts
    send_timeout_seconds: int = 30
    receive_timeout_seconds: int = 60

    # Retry configuration
    max_retries: int = 3

    @classmethod
    def from_env(cls) -> "MessagingConfig":
        """
        Load configuration from environment variables.

        For connection string auth:
            DAG_SERVICEBUS_CONNECTION_STRING: Service Bus connection string

        For managed identity auth:
            USE_MANAGED_IDENTITY: Set to "true" to use managed identity
            DAG_SERVICEBUS_FQDN: Fully qualified namespace (e.g., rmhazure.servicebus.windows.net)
            AZURE_CLIENT_ID: Optional client ID for user-assigned managed identity

        Common:
            DAG_WORKER_QUEUE: Worker task queue (REQUIRED)
            DAG_RESULT_QUEUE: Result queue (REQUIRED)
            DAG_CALLBACK_URL: Base URL for HTTP callbacks (optional)
        """
        worker_queue = os.environ.get("DAG_WORKER_QUEUE")
        if not worker_queue:
            raise ValueError(
                "DAG_WORKER_QUEUE environment variable is required"
            )

        result_queue = os.environ.get("DAG_RESULT_QUEUE", "")

        use_mi = os.environ.get("USE_MANAGED_IDENTITY", "").lower() == "true"

        if use_mi:
            fqdn = os.environ.get("DAG_SERVICEBUS_FQDN")
            if not fqdn:
                raise ValueError(
                    "DAG_SERVICEBUS_FQDN required when USE_MANAGED_IDENTITY=true"
                )
            return cls(
                use_managed_identity=True,
                fully_qualified_namespace=fqdn,
                managed_identity_client_id=os.environ.get("AZURE_CLIENT_ID"),
                worker_queue=worker_queue,
                result_queue=result_queue,
                callback_base_url=os.environ.get("DAG_CALLBACK_URL"),
            )
        else:
            connection_string = os.environ.get("DAG_SERVICEBUS_CONNECTION_STRING")
            if not connection_string:
                raise ValueError(
                    "DAG_SERVICEBUS_CONNECTION_STRING required (or set USE_MANAGED_IDENTITY=true)"
                )
            return cls(
                connection_string=connection_string,
                worker_queue=worker_queue,
                result_queue=result_queue,
                callback_base_url=os.environ.get("DAG_CALLBACK_URL"),
            )

    @property
    def use_callbacks(self) -> bool:
        """Whether to use HTTP callbacks instead of result queue."""
        return self.callback_base_url is not None
