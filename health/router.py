# ============================================================================
# HEALTH CHECK ROUTER
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Infrastructure - FastAPI health check endpoints
# PURPOSE: Kubernetes probes and health monitoring endpoints
# CREATED: 31 JAN 2026
# ============================================================================
"""
Health Check Router

FastAPI router providing three health check tiers:

Endpoints:
    GET /livez   - Liveness probe (is the process alive?)
                   Returns 200 if process is running, 503 if not.
                   Kubernetes uses this to restart dead containers.

    GET /readyz  - Readiness probe (can we accept work?)
                   Returns 200 if all required checks pass.
                   Kubernetes uses this to route traffic.

    GET /health  - Full health status (comprehensive check)
                   Returns detailed status of all registered checks.
                   Target execution time: 30-60 seconds max.

    GET /health/{check_name} - Single check status

Response Codes:
    200 - Healthy
    206 - Degraded (partial content)
    503 - Unhealthy (service unavailable)
"""

import logging
from typing import Optional

from fastapi import APIRouter, Response
from fastapi.responses import JSONResponse

from health.core import HealthStatus, HealthCheckResult
from health.registry import get_registry
from health.executor import HealthCheckExecutor
from __version__ import __version__, BUILD_DATE

logger = logging.getLogger(__name__)

health_router = APIRouter(tags=["Health"])


def _status_to_http_code(status: HealthStatus) -> int:
    """Map health status to HTTP status code."""
    return {
        HealthStatus.HEALTHY: 200,
        HealthStatus.DEGRADED: 206,  # Partial Content
        HealthStatus.UNHEALTHY: 503,  # Service Unavailable
    }[status]


# ============================================================================
# LIVENESS PROBE
# ============================================================================

@health_router.get("/livez")
async def liveness_probe():
    """
    Kubernetes liveness probe.

    Returns 200 if the process is alive. This is an instant check
    with no external dependencies. If this fails, Kubernetes will
    restart the container.

    No external checks - just confirms the process is responsive.
    """
    return {"status": "alive", "version": __version__, "build_date": BUILD_DATE}


# ============================================================================
# READINESS PROBE
# ============================================================================

@health_router.get("/readyz")
async def readiness_probe():
    """
    Kubernetes readiness probe.

    Returns 200 if the service is ready to accept work.
    Checks only the required components (those marked required_for_ready=True).

    Typical checks:
    - Database connection
    - Service Bus connection
    - Required configuration present

    If this fails, Kubernetes removes the pod from the service load balancer.
    """
    registry = get_registry()

    # Fast path: if no checks registered, assume ready
    if len(registry) == 0:
        return {"status": "ready", "message": "No checks registered"}

    executor = HealthCheckExecutor(
        registry=registry,
        overall_timeout=10.0,  # Fast timeout for readiness
    )

    result = await executor.execute_required()

    if result.status == HealthStatus.UNHEALTHY:
        return JSONResponse(
            status_code=503,
            content={
                "status": "not_ready",
                "checks": {
                    name: check.to_dict()
                    for name, check in result.checks.items()
                    if check.status == HealthStatus.UNHEALTHY
                },
                "total_duration_ms": round(result.total_duration_ms, 2),
            },
        )

    return {
        "status": "ready",
        "checks_passed": len(result.checks),
        "total_duration_ms": round(result.total_duration_ms, 2),
    }


# ============================================================================
# FULL HEALTH CHECK
# ============================================================================

@health_router.get("/health")
async def full_health_check():
    """
    Comprehensive health check.

    Runs all registered health checks and returns detailed status.
    Target execution time: 30-60 seconds max.

    This endpoint is for:
    - Monitoring dashboards
    - Pre-deployment validation
    - Debugging connectivity issues

    Returns:
        200: All checks healthy
        206: Some checks degraded (warnings)
        503: Critical checks failing
    """
    registry = get_registry()

    if len(registry) == 0:
        return {
            "status": "healthy",
            "message": "No checks registered",
            "checks": {},
        }

    executor = HealthCheckExecutor(
        registry=registry,
        overall_timeout=60.0,  # Allow full check suite
    )

    result = await executor.execute_all()
    http_code = _status_to_http_code(result.status)

    response_body = result.to_dict()
    response_body["version"] = __version__
    response_body["build_date"] = BUILD_DATE

    # Add summary by category
    summary = {}
    for name, check_result in result.checks.items():
        check = registry.get(name)
        if check:
            category = check.category.value
            if category not in summary:
                summary[category] = {"healthy": 0, "degraded": 0, "unhealthy": 0}
            summary[category][check_result.status.value] += 1

    response_body["summary"] = summary

    return JSONResponse(status_code=http_code, content=response_body)


# ============================================================================
# SINGLE CHECK
# ============================================================================

@health_router.get("/health/{check_name}")
async def single_health_check(check_name: str):
    """
    Run a single health check by name.

    Useful for debugging specific components.
    """
    executor = HealthCheckExecutor()
    result = await executor.execute_single(check_name)

    if result is None:
        return JSONResponse(
            status_code=404,
            content={"error": f"Health check not found: {check_name}"},
        )

    http_code = _status_to_http_code(result.status)
    return JSONResponse(status_code=http_code, content=result.to_dict())


# ============================================================================
# EXPORTS
# ============================================================================

__all__ = [
    "health_router",
]
