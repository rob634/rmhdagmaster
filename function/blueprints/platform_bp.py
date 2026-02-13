# ============================================================================
# CLAUDE CONTEXT - PLATFORM BLUEPRINT
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Gateway - B2B platform integration endpoints (proxy + read)
# PURPOSE: HTTP endpoints for B2B asset submission (proxy) and status polling (read)
# LAST_REVIEWED: 12 FEB 2026
# ============================================================================
"""
Platform Blueprint

B2B-facing endpoints for partner platform integration:
- POST /api/platform/submit          - Proxy to orchestrator domain API
- GET  /api/platform/status/{req_id} - Poll job status by request_id (read-only)

Submit requests are validated, then forwarded to the orchestrator's
domain HTTP API. Status polling reads directly from the gateway's
database connection.
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
# SUBMIT (proxy to orchestrator domain API)
# ============================================================================

@platform_bp.route(route="platform/submit", methods=["POST"])
def platform_submit(req: func.HttpRequest) -> func.HttpResponse:
    """
    Submit a geospatial asset for processing.

    POST /api/platform/submit

    Validates the B2B request, then proxies to the orchestrator's domain
    API: POST {ORCHESTRATOR_BASE_URL}/api/v1/domain/submit.
    The orchestrator computes asset_id, creates version, creates DAG job.
    """
    from function.models.requests import AssetSubmitRequest
    from function.services.orchestrator_client import OrchestratorClient

    # Validate request body
    try:
        body = req.get_json()
        request = AssetSubmitRequest.model_validate(body)
    except ValueError:
        return _error("Invalid JSON", "Request body must be valid JSON", 400)
    except Exception as e:
        return _error("Validation error", str(e), 422)

    # Build orchestrator payload
    payload = {
        "platform_id": request.platform_id,
        "platform_refs": request.platform_refs,
        "data_type": request.data_type,
        "version_label": request.version_label,
        "workflow_id": request.workflow_id,
        "input_params": request.input_params,
        "submitted_by": request.submitted_by,
        "correlation_id": request.request_id,
    }

    # Proxy to orchestrator
    client = OrchestratorClient()
    status_code, resp_body = client.submit(payload)

    # Return B2B-friendly response
    if status_code == 202:
        return _json_response({
            "asset_id": resp_body.get("asset_id"),
            "version_ordinal": resp_body.get("version_ordinal"),
            "request_id": request.request_id,
            "job_id": resp_body.get("job_id"),
            "status": "accepted",
        }, 202)

    # Relay orchestrator error codes directly
    return _json_response(resp_body, status_code)


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
