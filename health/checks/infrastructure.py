# ============================================================================
# INFRASTRUCTURE HEALTH CHECKS
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Infrastructure - Storage and messaging checks
# PURPOSE: Azure Blob Storage and Service Bus connectivity
# CREATED: 31 JAN 2026
# ============================================================================
"""
Infrastructure Health Checks

Azure infrastructure connectivity checks (priority 20):
- BlobStorageCheck: Azure Blob Storage access
- ServiceBusCheck: Azure Service Bus connectivity
"""

import os
import logging

from health.core import (
    HealthCheckPlugin,
    HealthCheckResult,
    HealthCheckCategory,
)
from health.registry import register_check

logger = logging.getLogger(__name__)


@register_check(category="infrastructure")
class BlobStorageCheck(HealthCheckPlugin):
    """
    Azure Blob Storage health check.

    Tests connectivity to configured storage accounts.
    Checks Bronze zone by default (most commonly used).
    """

    name = "blob_storage"
    timeout_seconds = 10.0

    async def check(self) -> HealthCheckResult:
        # Check if storage is configured
        storage_account = os.environ.get("DAG_STORAGE_BRONZE_ACCOUNT")

        if not storage_account:
            return HealthCheckResult.degraded(
                message="No storage account configured",
                hint="Set DAG_STORAGE_BRONZE_ACCOUNT environment variable",
            )

        try:
            from infrastructure.storage import BlobRepository

            # Get repository (this tests credential acquisition)
            repo = BlobRepository.for_zone("bronze")

            # Try to list containers (data plane operation)
            blob_service = repo._get_blob_service()
            # Just iterate once to verify connectivity
            for container in blob_service.list_containers():
                break  # Only need to verify we can list

            return HealthCheckResult.healthy(
                message="Blob storage connected",
                storage_account=storage_account,
            )

        except ImportError:
            return HealthCheckResult.degraded(
                message="azure-storage-blob not installed",
            )
        except Exception as e:
            return HealthCheckResult.unhealthy(
                message=f"Blob storage connection failed: {str(e)}",
                storage_account=storage_account,
            )


@register_check(category="infrastructure")
class ServiceBusCheck(HealthCheckPlugin):
    """
    Azure Service Bus health check.

    Tests connectivity to Service Bus namespace.
    """

    name = "service_bus"
    timeout_seconds = 10.0

    async def check(self) -> HealthCheckResult:
        # Check configuration
        connection_string = os.environ.get("DAG_SERVICEBUS_CONNECTION_STRING")
        fqdn = os.environ.get("DAG_SERVICEBUS_FQDN")

        if not connection_string and not fqdn:
            return HealthCheckResult.degraded(
                message="Service Bus not configured",
                hint="Set DAG_SERVICEBUS_FQDN or DAG_SERVICEBUS_CONNECTION_STRING",
            )

        namespace = fqdn

        try:
            from messaging import get_publisher

            # Get publisher (tests connection)
            publisher = await get_publisher()

            if publisher._client is None:
                return HealthCheckResult.unhealthy(
                    message="Service Bus client not initialized",
                )

            # We don't want to actually send a message, just verify connection
            # The publisher existing and having a client is sufficient

            return HealthCheckResult.healthy(
                message="Service Bus connected",
                namespace=namespace or "(from connection string)",
            )

        except ImportError:
            return HealthCheckResult.degraded(
                message="azure-servicebus not installed",
            )
        except Exception as e:
            return HealthCheckResult.unhealthy(
                message=f"Service Bus connection failed: {str(e)}",
            )


# ============================================================================
# EXPORTS
# ============================================================================

__all__ = [
    "BlobStorageCheck",
    "ServiceBusCheck",
]
