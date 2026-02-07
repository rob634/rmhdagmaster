# ============================================================================
# STARTUP HEALTH CHECKS
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Infrastructure - Startup health checks
# PURPOSE: Basic process and configuration checks
# CREATED: 31 JAN 2026
# ============================================================================
"""
Startup Health Checks

Basic checks that run first (priority 10):
- ProcessCheck: Always healthy if process is running
- ConfigCheck: Required environment variables present
"""

import os
import logging
from typing import List

from health.core import (
    HealthCheckPlugin,
    HealthCheckResult,
    HealthCheckCategory,
)
from health.registry import register_check

logger = logging.getLogger(__name__)


@register_check(category="startup")
class ProcessCheck(HealthCheckPlugin):
    """
    Basic process health check.

    Always returns healthy if the check runs (proves process is alive).
    """

    name = "process"
    timeout_seconds = 1.0

    async def check(self) -> HealthCheckResult:
        import sys
        import platform

        return HealthCheckResult.healthy(
            message="Process running",
            python_version=sys.version,
            platform=platform.platform(),
            pid=os.getpid(),
        )


@register_check(category="startup")
class ConfigCheck(HealthCheckPlugin):
    """
    Configuration health check.

    Verifies required environment variables are present.
    Does NOT check if values are valid (that's for other checks).
    """

    name = "config"
    timeout_seconds = 1.0

    # Required for any mode
    REQUIRED_VARS = [
        "RUN_MODE",
    ]

    # Required for orchestrator mode
    ORCHESTRATOR_VARS = [
        "POSTGRES_HOST",
        "POSTGRES_DB",
        "JOB_QUEUE_NAME",
        "DAG_WORKER_QUEUE",
    ]

    # Required for worker mode
    WORKER_VARS = [
        "WORKER_TYPE",
        "WORKER_QUEUE",
    ]

    async def check(self) -> HealthCheckResult:
        missing: List[str] = []
        present: List[str] = []

        # Check required vars
        for var in self.REQUIRED_VARS:
            if os.environ.get(var):
                present.append(var)
            else:
                missing.append(var)

        # Check mode-specific vars
        run_mode = os.environ.get("RUN_MODE", "orchestrator")

        if run_mode == "orchestrator":
            for var in self.ORCHESTRATOR_VARS:
                if os.environ.get(var):
                    present.append(var)
                else:
                    missing.append(var)
        elif run_mode == "worker":
            for var in self.WORKER_VARS:
                if os.environ.get(var):
                    present.append(var)
                else:
                    missing.append(var)

        if missing:
            return HealthCheckResult.unhealthy(
                message=f"Missing required config: {', '.join(missing)}",
                missing=missing,
                present=present,
                run_mode=run_mode,
            )

        return HealthCheckResult.healthy(
            message="All required config present",
            present=present,
            run_mode=run_mode,
        )


# ============================================================================
# EXPORTS
# ============================================================================

__all__ = [
    "ProcessCheck",
    "ConfigCheck",
]
