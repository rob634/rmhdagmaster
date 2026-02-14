# ============================================================================
# APPLICATION HEALTH CHECKS
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Infrastructure - Application state checks
# PURPOSE: Orchestrator, workflow, and handler availability
# CREATED: 31 JAN 2026
# ============================================================================
"""
Application Health Checks

Application-level checks (priority 40):
- OrchestratorCheck: Orchestrator loop running (orchestrator mode only)
- WorkflowsCheck: Workflow definitions loaded
- HandlersCheck: Handler registry populated
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


# Global reference to orchestrator (set by main app)
_orchestrator = None


def set_orchestrator(orchestrator):
    """Set orchestrator reference for health checks."""
    global _orchestrator
    _orchestrator = orchestrator


@register_check(category="application")
class OrchestratorCheck(HealthCheckPlugin):
    """
    Orchestrator health check.

    Verifies the orchestrator loop is running (orchestrator mode only).
    In worker mode, this check is skipped.
    """

    name = "orchestrator"
    timeout_seconds = 2.0
    required_for_ready = True

    async def check(self) -> HealthCheckResult:
        run_mode = os.environ.get("RUN_MODE", "orchestrator")

        if run_mode != "orchestrator":
            return HealthCheckResult.healthy(
                message="Not in orchestrator mode (skipped)",
                run_mode=run_mode,
            )

        if _orchestrator is None:
            return HealthCheckResult.unhealthy(
                message="Orchestrator not initialized",
                hint="Orchestrator reference not set",
            )

        if not _orchestrator.is_running:
            return HealthCheckResult.unhealthy(
                message="Orchestrator loop not running",
            )

        # Get stats
        stats = getattr(_orchestrator, "stats", {})
        is_leader = stats.get("is_leader", False)
        role = stats.get("role", "unknown")

        details = {
            "role": role,
            "owner_id": stats.get("owner_id"),
            "uptime_seconds": stats.get("uptime_seconds"),
            "cycles": stats.get("cycles", 0),
            "active_jobs": stats.get("active_jobs", 0),
            "tasks_dispatched": stats.get("tasks_dispatched", 0),
            "last_cycle_at": stats.get("last_cycle_at"),
        }

        if is_leader:
            return HealthCheckResult.healthy(
                message=f"Orchestrator running (leader, {stats.get('cycles', 0)} cycles)",
                **details,
            )
        else:
            return HealthCheckResult.degraded(
                message="Orchestrator running in standby mode (not leader)",
                **details,
            )


@register_check(category="application")
class WorkflowsCheck(HealthCheckPlugin):
    """
    Workflow definitions health check.

    Verifies workflow definitions are loaded.
    """

    name = "workflows"
    timeout_seconds = 2.0
    required_for_ready = True

    async def check(self) -> HealthCheckResult:
        try:
            from services.workflow_service import WorkflowService

            service = WorkflowService()
            workflows = service.list_all()

            if not workflows:
                return HealthCheckResult.degraded(
                    message="No workflows loaded",
                    hint="Check workflows/ directory for YAML definitions",
                )

            # Get active workflows
            active = [w for w in workflows if w.is_active]

            return HealthCheckResult.healthy(
                message=f"{len(workflows)} workflows loaded",
                total=len(workflows),
                active=len(active),
                workflow_ids=[w.workflow_id for w in workflows],
            )

        except ImportError:
            return HealthCheckResult.degraded(
                message="Workflow service not available",
            )
        except Exception as e:
            return HealthCheckResult.unhealthy(
                message=f"Workflow check failed: {str(e)}",
            )


@register_check(category="application")
class HandlersCheck(HealthCheckPlugin):
    """
    Handler registry health check.

    Verifies handlers are registered and available.
    """

    name = "handlers"
    timeout_seconds = 2.0
    required_for_ready = True

    async def check(self) -> HealthCheckResult:
        try:
            from handlers import list_handlers

            handlers = list_handlers()

            if not handlers:
                return HealthCheckResult.degraded(
                    message="No handlers registered",
                    hint="Import handler modules to register handlers",
                )

            # Group by queue (handlers is a list of dicts)
            by_queue = {}
            handler_names = []
            for info in handlers:
                name = info.get("name", "unknown")
                queue = info.get("queue", "default")
                handler_names.append(name)
                if queue not in by_queue:
                    by_queue[queue] = []
                by_queue[queue].append(name)

            return HealthCheckResult.healthy(
                message=f"{len(handlers)} handlers registered",
                total=len(handlers),
                handlers=handler_names,
                by_queue=by_queue,
            )

        except ImportError:
            return HealthCheckResult.degraded(
                message="Handler registry not available",
            )
        except Exception as e:
            return HealthCheckResult.unhealthy(
                message=f"Handler check failed: {str(e)}",
            )


# ============================================================================
# EXPORTS
# ============================================================================

__all__ = [
    "OrchestratorCheck",
    "WorkflowsCheck",
    "HandlersCheck",
    "set_orchestrator",
]
