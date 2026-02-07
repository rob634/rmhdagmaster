# ============================================================================
# STATUS BLUEPRINT
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Gateway - Status query endpoints
# PURPOSE: HTTP endpoints for querying job and node status
# CREATED: 04 FEB 2026
# ============================================================================
"""
Status Blueprint

Status query endpoints (read-only):
- GET /api/status/job/{job_id} - Get job by ID
- GET /api/status/jobs - List jobs with filters
- GET /api/status/job/{job_id}/nodes - Get nodes for job
- GET /api/status/job/{job_id}/events - Get events for job
- GET /api/status/stats - Job statistics
- GET /api/status/lookup - Find by correlation/idempotency key
"""

import json
import logging

import azure.functions as func

from function.models.responses import (
    JobStatusResponse,
    JobListResponse,
    EventListResponse,
    ErrorResponse,
)
from function.repositories.job_query_repo import JobQueryRepository
from function.repositories.node_query_repo import NodeQueryRepository
from function.repositories.event_query_repo import EventQueryRepository

logger = logging.getLogger(__name__)
status_bp = func.Blueprint()


def _json_response(data: dict, status_code: int = 200) -> func.HttpResponse:
    """Create JSON HTTP response."""
    return func.HttpResponse(
        json.dumps(data, default=str),
        status_code=status_code,
        headers={"Content-Type": "application/json"},
    )


@status_bp.route(route="status/job/{job_id}", methods=["GET"])
def status_job_by_id(req: func.HttpRequest) -> func.HttpResponse:
    """
    Get job status by ID.

    GET /api/status/job/{job_id}
    Query params: include_nodes=true/false
    """
    job_id = req.route_params.get("job_id")
    include_nodes = req.params.get("include_nodes", "false").lower() == "true"

    try:
        job_repo = JobQueryRepository()
        job = job_repo.get_job(job_id)

        if not job:
            return _json_response(
                ErrorResponse(error="Job not found", code="NOT_FOUND").model_dump(),
                status_code=404,
            )

        # Optionally include nodes
        nodes = None
        if include_nodes:
            node_repo = NodeQueryRepository()
            nodes = node_repo.get_nodes_for_job(job_id)

        response = JobStatusResponse(
            job_id=job["job_id"],
            workflow_id=job["workflow_id"],
            status=job["status"],
            created_at=job["created_at"],
            started_at=job.get("started_at"),
            completed_at=job.get("completed_at"),
            error_message=job.get("error_message"),
            owner_id=job.get("owner_id"),
            correlation_id=job.get("correlation_id"),
            nodes=nodes,
        )

        return _json_response(response.model_dump())

    except Exception as e:
        logger.exception(f"Error fetching job {job_id}: {e}")
        return _json_response(
            ErrorResponse(error="Database error", details=str(e)).model_dump(),
            status_code=500,
        )


@status_bp.route(route="status/jobs", methods=["GET"])
def status_jobs_list(req: func.HttpRequest) -> func.HttpResponse:
    """
    List jobs with filtering.

    GET /api/status/jobs
    Query params: status, workflow_id, limit, offset
    """
    status = req.params.get("status")
    workflow_id = req.params.get("workflow_id")
    limit = int(req.params.get("limit", "50"))
    offset = int(req.params.get("offset", "0"))

    # Validate limits
    limit = min(max(limit, 1), 500)
    offset = max(offset, 0)

    try:
        job_repo = JobQueryRepository()

        jobs = job_repo.list_jobs(
            status=status,
            workflow_id=workflow_id,
            limit=limit,
            offset=offset,
        )

        total = job_repo.count_jobs(status=status, workflow_id=workflow_id)

        response = JobListResponse(
            jobs=[
                JobStatusResponse(
                    job_id=j["job_id"],
                    workflow_id=j["workflow_id"],
                    status=j["status"],
                    created_at=j["created_at"],
                    started_at=j.get("started_at"),
                    completed_at=j.get("completed_at"),
                    error_message=j.get("error_message"),
                    owner_id=j.get("owner_id"),
                    correlation_id=j.get("correlation_id"),
                )
                for j in jobs
            ],
            total=total,
            limit=limit,
            offset=offset,
        )

        return _json_response(response.model_dump())

    except Exception as e:
        logger.exception(f"Error listing jobs: {e}")
        return _json_response(
            ErrorResponse(error="Database error", details=str(e)).model_dump(),
            status_code=500,
        )


@status_bp.route(route="status/job/{job_id}/nodes", methods=["GET"])
def status_job_nodes(req: func.HttpRequest) -> func.HttpResponse:
    """
    Get nodes for a job.

    GET /api/status/job/{job_id}/nodes
    Query params: status (optional filter)
    """
    job_id = req.route_params.get("job_id")
    status_filter = req.params.get("status")

    try:
        node_repo = NodeQueryRepository()

        if status_filter:
            nodes = node_repo.get_nodes_by_status(job_id, status_filter)
        else:
            nodes = node_repo.get_nodes_for_job(job_id)

        return _json_response({
            "job_id": job_id,
            "nodes": nodes,
            "count": len(nodes),
        })

    except Exception as e:
        logger.exception(f"Error fetching nodes for job {job_id}: {e}")
        return _json_response(
            ErrorResponse(error="Database error", details=str(e)).model_dump(),
            status_code=500,
        )


@status_bp.route(route="status/job/{job_id}/events", methods=["GET"])
def status_job_events(req: func.HttpRequest) -> func.HttpResponse:
    """
    Get events for a job.

    GET /api/status/job/{job_id}/events
    Query params: limit (default 100), event_type (optional filter)
    """
    job_id = req.route_params.get("job_id")
    limit = int(req.params.get("limit", "100"))
    event_type = req.params.get("event_type")

    # Validate limit
    limit = min(max(limit, 1), 1000)

    try:
        event_repo = EventQueryRepository()
        events = event_repo.get_events_for_job(
            job_id=job_id,
            limit=limit,
            event_type=event_type,
        )

        return _json_response(EventListResponse(
            events=events,
            total=len(events),
        ).model_dump())

    except Exception as e:
        logger.exception(f"Error fetching events for job {job_id}: {e}")
        return _json_response(
            ErrorResponse(error="Database error", details=str(e)).model_dump(),
            status_code=500,
        )


@status_bp.route(route="status/stats", methods=["GET"])
def status_stats(req: func.HttpRequest) -> func.HttpResponse:
    """
    Get overall job statistics.

    GET /api/status/stats
    """
    try:
        job_repo = JobQueryRepository()
        stats = job_repo.get_job_stats()

        return _json_response({
            "job_counts_by_status": stats,
            "total_jobs": sum(stats.values()),
        })

    except Exception as e:
        logger.exception(f"Error fetching stats: {e}")
        return _json_response(
            ErrorResponse(error="Database error", details=str(e)).model_dump(),
            status_code=500,
        )


@status_bp.route(route="status/lookup", methods=["GET"])
def status_lookup(req: func.HttpRequest) -> func.HttpResponse:
    """
    Lookup job by correlation_id or idempotency_key.

    GET /api/status/lookup
    Query params: correlation_id OR idempotency_key (one required)
    """
    correlation_id = req.params.get("correlation_id")
    idempotency_key = req.params.get("idempotency_key")

    if not correlation_id and not idempotency_key:
        return _json_response(
            ErrorResponse(
                error="Missing parameter",
                details="Provide correlation_id or idempotency_key",
            ).model_dump(),
            status_code=400,
        )

    try:
        job_repo = JobQueryRepository()

        if correlation_id:
            job = job_repo.get_job_by_correlation_id(correlation_id)
        else:
            job = job_repo.get_job_by_idempotency_key(idempotency_key)

        if not job:
            return _json_response(
                ErrorResponse(error="Job not found", code="NOT_FOUND").model_dump(),
                status_code=404,
            )

        return _json_response(job)

    except Exception as e:
        logger.exception(f"Error in lookup: {e}")
        return _json_response(
            ErrorResponse(error="Database error", details=str(e)).model_dump(),
            status_code=500,
        )


__all__ = ["status_bp"]
