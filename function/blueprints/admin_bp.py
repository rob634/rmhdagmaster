# ============================================================================
# ADMIN BLUEPRINT
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Gateway - Administrative endpoints
# PURPOSE: Administrative and maintenance endpoints
# CREATED: 04 FEB 2026
# ============================================================================
"""
Admin Blueprint

Administrative and maintenance endpoints:
- GET /api/admin/health - Comprehensive health check
- GET /api/admin/config - Show configuration (non-sensitive)
- GET /api/admin/active-jobs - List active jobs
- GET /api/admin/recent-events - Recent events across all jobs
- GET /api/admin/orphans - Jobs with stale heartbeats
"""

import json
import logging
from datetime import datetime, timezone

import azure.functions as func

from function.config import get_config
from function.models.responses import ErrorResponse
from function.repositories.job_query_repo import JobQueryRepository
from function.repositories.node_query_repo import NodeQueryRepository
from function.repositories.event_query_repo import EventQueryRepository

logger = logging.getLogger(__name__)
admin_bp = func.Blueprint()


def _json_response(data: dict, status_code: int = 200) -> func.HttpResponse:
    """Create JSON HTTP response."""
    return func.HttpResponse(
        json.dumps(data, default=str),
        status_code=status_code,
        headers={"Content-Type": "application/json"},
    )


@admin_bp.route(route="admin/health", methods=["GET"])
def admin_health(req: func.HttpRequest) -> func.HttpResponse:
    """
    Comprehensive health check.

    GET /api/admin/health

    Checks database connectivity and Service Bus configuration.
    """
    config = get_config()
    checks = {}

    # Database check
    try:
        job_repo = JobQueryRepository()
        job_repo.execute_scalar("SELECT 1")
        checks["database"] = {"status": "healthy"}
    except Exception as e:
        checks["database"] = {"status": "unhealthy", "error": str(e)}

    # Service Bus check (config only - connectivity check too slow)
    checks["service_bus"] = {
        "status": "configured" if config.has_service_bus_config else "not_configured"
    }

    # Docker app URLs check
    checks["orchestrator_url"] = {
        "configured": config.orchestrator_url != "http://localhost:8000",
        "url": config.orchestrator_url,
    }
    checks["worker_url"] = {
        "configured": config.worker_url != "http://localhost:8001",
        "url": config.worker_url,
    }

    all_healthy = (
        checks["database"].get("status") == "healthy"
        and checks["service_bus"].get("status") == "configured"
    )

    return _json_response({
        "status": "healthy" if all_healthy else "degraded",
        "checks": checks,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "version": config.version,
        "service": config.service_name,
    })


@admin_bp.route(route="admin/config", methods=["GET"])
def admin_config(req: func.HttpRequest) -> func.HttpResponse:
    """
    Show current configuration (non-sensitive values only).

    GET /api/admin/config
    """
    config = get_config()

    # Only expose non-sensitive configuration
    safe_config = {
        "job_queue_name": config.job_queue_name,
        "service_bus_namespace": config.service_bus_namespace or "(connection string)",
        "database_host": config.postgres_host,
        "database_name": config.postgres_db,
        "db_schema": config.db_schema,
        "orchestrator_url": config.orchestrator_url,
        "worker_url": config.worker_url,
        "version": config.version,
        "service_name": config.service_name,
    }

    return _json_response({"config": safe_config})


@admin_bp.route(route="admin/active-jobs", methods=["GET"])
def admin_active_jobs(req: func.HttpRequest) -> func.HttpResponse:
    """
    List currently active jobs (pending/running).

    GET /api/admin/active-jobs
    Query params: owner_id (optional filter)
    """
    owner_id = req.params.get("owner_id")

    try:
        job_repo = JobQueryRepository()
        active = job_repo.get_active_jobs(owner_id=owner_id)

        return _json_response({
            "active_jobs": active,
            "count": len(active),
            "filter": {"owner_id": owner_id} if owner_id else None,
        })

    except Exception as e:
        logger.exception(f"Error fetching active jobs: {e}")
        return _json_response(
            ErrorResponse(error="Database error", details=str(e)).model_dump(),
            status_code=500,
        )


@admin_bp.route(route="admin/recent-events", methods=["GET"])
def admin_recent_events(req: func.HttpRequest) -> func.HttpResponse:
    """
    List recent events across all jobs.

    GET /api/admin/recent-events
    Query params: limit (default 100), event_type (optional filter)
    """
    limit = int(req.params.get("limit", "100"))
    event_type = req.params.get("event_type")

    # Validate limit
    limit = min(max(limit, 1), 500)

    try:
        event_repo = EventQueryRepository()
        events = event_repo.get_recent_events(limit=limit, event_type=event_type)

        return _json_response({
            "events": events,
            "count": len(events),
            "filter": {"event_type": event_type} if event_type else None,
        })

    except Exception as e:
        logger.exception(f"Error fetching events: {e}")
        return _json_response(
            ErrorResponse(error="Database error", details=str(e)).model_dump(),
            status_code=500,
        )


@admin_bp.route(route="admin/orphans", methods=["GET"])
def admin_orphan_jobs(req: func.HttpRequest) -> func.HttpResponse:
    """
    List jobs with stale heartbeats (potential orphans).

    GET /api/admin/orphans
    Query params: threshold_minutes (default 2)

    Orphan jobs have an owner but the owner's heartbeat is stale,
    indicating the orchestrator instance may have crashed.
    """
    threshold = int(req.params.get("threshold_minutes", "2"))

    # Validate threshold
    threshold = max(threshold, 1)

    try:
        job_repo = JobQueryRepository()
        orphans = job_repo.get_orphan_jobs(orphan_threshold_minutes=threshold)

        return _json_response({
            "orphan_jobs": orphans,
            "count": len(orphans),
            "threshold_minutes": threshold,
        })

    except Exception as e:
        logger.exception(f"Error fetching orphan jobs: {e}")
        return _json_response(
            ErrorResponse(error="Database error", details=str(e)).model_dump(),
            status_code=500,
        )


@admin_bp.route(route="admin/running-nodes", methods=["GET"])
def admin_running_nodes(req: func.HttpRequest) -> func.HttpResponse:
    """
    List currently running nodes.

    GET /api/admin/running-nodes
    Query params: job_id (optional filter)
    """
    job_id = req.params.get("job_id")

    try:
        node_repo = NodeQueryRepository()
        running = node_repo.get_running_nodes(job_id=job_id)

        return _json_response({
            "running_nodes": running,
            "count": len(running),
            "filter": {"job_id": job_id} if job_id else None,
        })

    except Exception as e:
        logger.exception(f"Error fetching running nodes: {e}")
        return _json_response(
            ErrorResponse(error="Database error", details=str(e)).model_dump(),
            status_code=500,
        )


@admin_bp.route(route="admin/failed-nodes", methods=["GET"])
def admin_failed_nodes(req: func.HttpRequest) -> func.HttpResponse:
    """
    List recently failed nodes.

    GET /api/admin/failed-nodes
    Query params: job_id (optional), limit (default 50)
    """
    job_id = req.params.get("job_id")
    limit = int(req.params.get("limit", "50"))

    # Validate limit
    limit = min(max(limit, 1), 200)

    try:
        node_repo = NodeQueryRepository()
        failed = node_repo.get_failed_nodes(job_id=job_id, limit=limit)

        return _json_response({
            "failed_nodes": failed,
            "count": len(failed),
            "filter": {"job_id": job_id} if job_id else None,
        })

    except Exception as e:
        logger.exception(f"Error fetching failed nodes: {e}")
        return _json_response(
            ErrorResponse(error="Database error", details=str(e)).model_dump(),
            status_code=500,
        )


__all__ = ["admin_bp"]
