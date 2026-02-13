# ============================================================================
# CLAUDE CONTEXT - PLATFORM BLUEPRINT
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Gateway - B2B platform integration endpoints (ACL proxy)
# PURPOSE: HTTP endpoints for B2B asset submission and status polling
# LAST_REVIEWED: 12 FEB 2026
# ============================================================================
"""
Platform Blueprint

B2B-facing endpoints for partner platform integration:
- POST /api/platform/submit          - Proxy to orchestrator domain API
- GET  /api/platform/status/{req_id} - Poll job status by request_id (read-only)

The gateway is a read-only ACL proxy. Submit requests are forwarded
to the orchestrator's domain HTTP API. The gateway never writes to
the database or publishes to Service Bus.
"""

import json
import logging

import azure.functions as func

from function.repositories.job_query_repo import JobQueryRepository

logger = logging.getLogger(__name__)
platform_bp = func.Blueprint()


# ============================================================================
# HELPERS
# ============================================================================

def _json_response(data: dict, status_code: int = 200) -> func.HttpResponse:
    """Create JSON HTTP response."""
    return func.HttpResponse(
        json.dumps(data, default=str),
        status_code=status_code,
        headers={"Content-Type": "application/json"},
    )


def _error(error: str, details: str = None, status_code: int = 400) -> func.HttpResponse:
    """Create error response."""
    from function.models.responses import ErrorResponse
    return _json_response(
        ErrorResponse(error=error, details=details).model_dump(),
        status_code=status_code,
    )


# ============================================================================
# SUBMIT (proxy stub — awaiting orchestrator domain endpoint)
# ============================================================================

@platform_bp.route(route="platform/submit", methods=["POST"])
def platform_submit(req: func.HttpRequest) -> func.HttpResponse:
    """
    Submit a geospatial asset for processing.

    POST /api/platform/submit

    STUB: Returns 501 until orchestrator domain endpoint is built (Pass 2).
    Will proxy to: POST {ORCHESTRATOR_BASE_URL}/api/v1/domain/submit
    """
    return _error(
        "Not implemented",
        "Submit endpoint is being migrated to orchestrator domain API. "
        "See TODO.md Phase 4B-REBUILD, step R2b.",
        501,
    )


# ============================================================================
# STATUS POLLING (read-only — stays in gateway)
# ============================================================================

@platform_bp.route(route="platform/status/{request_id}", methods=["GET"])
def platform_status(req: func.HttpRequest) -> func.HttpResponse:
    """
    Poll job status by B2B request_id.

    GET /api/platform/status/{request_id}

    B2B clients use this to check processing status.
    Looks up the most recent job where correlation_id = request_id.
    """
    request_id = req.route_params.get("request_id")

    try:
        repo = JobQueryRepository()
        job = repo.get_job_by_correlation_id(request_id)

        if job is None:
            return _error(
                "Request not found",
                f"No job found for request_id '{request_id}'",
                404,
            )

        return _json_response(job)

    except Exception as e:
        logger.exception(f"Error polling status: {e}")
        return _error("Database error", str(e), 500)


__all__ = ["platform_bp"]
