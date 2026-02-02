# ============================================================================
# HEALTH CHECK MODULE
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Infrastructure - Health check plugin system
# PURPOSE: Kubernetes probes and comprehensive health monitoring
# CREATED: 31 JAN 2026
# ============================================================================
"""
Health Check Module

Plugin-based health check system for DAG orchestrator:
- /livez: Process alive (instant, for Kubernetes liveness probe)
- /readyz: Ready to accept work (startup checks passed)
- /health: Comprehensive status (all plugins, 30-60 second target)

Architecture:
- HealthCheckPlugin: Base class for health checks
- HealthCheckRegistry: Plugin discovery and registration
- HealthCheckExecutor: Parallel execution with timeouts
- Three endpoint tiers: livez (instant), readyz (fast), health (thorough)

Usage:
    from health import health_router, register_check

    # Register custom check
    @register_check(category="infrastructure", priority=50)
    class MyCheck(HealthCheckPlugin):
        async def check(self) -> HealthCheckResult:
            return HealthCheckResult(status="healthy")

    # Mount router
    app.include_router(health_router)
"""

from health.core import (
    HealthStatus,
    HealthCheckResult,
    HealthCheckPlugin,
    HealthCheckCategory,
)
from health.registry import (
    HealthCheckRegistry,
    register_check,
    get_registry,
)
from health.executor import HealthCheckExecutor
from health.router import health_router

__all__ = [
    # Core types
    "HealthStatus",
    "HealthCheckResult",
    "HealthCheckPlugin",
    "HealthCheckCategory",
    # Registry
    "HealthCheckRegistry",
    "register_check",
    "get_registry",
    # Executor
    "HealthCheckExecutor",
    # Router
    "health_router",
]
