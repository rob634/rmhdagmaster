# ============================================================================
# WORKER HEALTH CHECK
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Infrastructure - DAG Worker connectivity check
# PURPOSE: Verify DAG worker app is healthy and consuming from correct queue
# CREATED: 02 FEB 2026
# ============================================================================
"""
Worker Health Check

Checks the health of the DAG worker application:
- HTTP connectivity to worker health endpoint
- Version match verification
- Queue configuration verification
- Consumer status (is it actively consuming?)
"""

import os
import logging
from typing import Optional

import httpx

from health.core import (
    HealthCheckPlugin,
    HealthCheckResult,
    HealthCheckCategory,
)
from health.registry import register_check
from __version__ import __version__

logger = logging.getLogger(__name__)

# Worker URL from environment
WORKER_HEALTH_URL = os.environ.get(
    "WORKER_HEALTH_URL",
    "https://rmhdagworker-fedshwfme6drd6gq.eastus-01.azurewebsites.net/health"
)


@register_check(category="external")
class WorkerHealthCheck(HealthCheckPlugin):
    """
    DAG Worker health check.

    Verifies the worker app is:
    1. Reachable via HTTP
    2. Running the correct version
    3. Consuming from the expected queue
    4. Actively processing messages
    """

    name = "worker"
    category = HealthCheckCategory.EXTERNAL
    timeout_seconds = 15.0
    required_for_ready = False  # Orchestrator can start without worker

    async def check(self) -> HealthCheckResult:
        worker_url = os.environ.get("WORKER_HEALTH_URL", WORKER_HEALTH_URL)
        expected_queue = os.environ.get("EXPECTED_WORKER_QUEUE", "dag-worker-tasks")

        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(worker_url)

                if response.status_code != 200:
                    return HealthCheckResult.unhealthy(
                        message=f"Worker returned status {response.status_code}",
                        url=worker_url,
                        status_code=response.status_code,
                    )

                data = response.json()

                # Check worker status
                worker_status = data.get("status", "unknown")
                if worker_status != "healthy":
                    return HealthCheckResult.unhealthy(
                        message=f"Worker reports unhealthy: {data.get('worker_status', 'unknown')}",
                        url=worker_url,
                        worker_response=data,
                    )

                # Check version match
                worker_version = data.get("version", "unknown")
                version_match = worker_version == __version__

                # Check queue configuration
                worker_queue = data.get("queue", "unknown")
                queue_match = worker_queue == expected_queue

                # Build result details
                details = {
                    "url": worker_url,
                    "worker_version": worker_version,
                    "orchestrator_version": __version__,
                    "version_match": version_match,
                    "worker_queue": worker_queue,
                    "expected_queue": expected_queue,
                    "queue_match": queue_match,
                    "worker_type": data.get("worker_type", "unknown"),
                    "consuming": data.get("consuming", False),
                }

                # Determine overall status
                if not queue_match:
                    return HealthCheckResult.unhealthy(
                        message=f"Worker queue mismatch: {worker_queue} vs expected {expected_queue}",
                        **details,
                    )

                if not version_match:
                    return HealthCheckResult.degraded(
                        message=f"Version mismatch: worker={worker_version}, orchestrator={__version__}",
                        **details,
                    )

                if not data.get("consuming", False):
                    return HealthCheckResult.degraded(
                        message="Worker not actively consuming messages",
                        **details,
                    )

                return HealthCheckResult.healthy(
                    message="Worker healthy and consuming",
                    **details,
                )

        except httpx.TimeoutException:
            return HealthCheckResult.unhealthy(
                message=f"Worker health check timed out",
                url=worker_url,
            )

        except httpx.ConnectError as e:
            return HealthCheckResult.unhealthy(
                message=f"Cannot connect to worker: {str(e)}",
                url=worker_url,
            )

        except Exception as e:
            logger.exception(f"Worker health check failed: {e}")
            return HealthCheckResult.unhealthy(
                message=f"Worker health check error: {str(e)}",
                url=worker_url,
                exception_type=type(e).__name__,
            )


# ============================================================================
# EXPORTS
# ============================================================================

__all__ = [
    "WorkerHealthCheck",
]
