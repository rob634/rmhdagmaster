# ============================================================================
# CLAUDE CONTEXT - PLATFORM BLUEPRINT
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Gateway - B2B platform integration endpoints
# PURPOSE: HTTP endpoints for B2B asset submission and status polling
# LAST_REVIEWED: 11 FEB 2026
# ============================================================================
"""
Platform Blueprint

B2B-facing endpoints for partner platform integration:
- POST /api/platform/submit          - Submit asset for processing
- GET  /api/platform/status/{req_id} - Poll job status by request_id

Submit flow:
1. Validate platform (DB read)
2. Compute deterministic asset_id
3. Get-or-create asset (DB write)
4. Check version ordering constraint (DB read)
5. Create version (DB write)
6. Publish DAG job to Service Bus with:
   - correlation_id = request_id (B2B polling)
   - asset_id = computed asset_id (job→asset tracing)
7. Update version with job reference (DB write)
8. Return response
"""

import hashlib
import json
import logging
import os
from datetime import datetime, timezone

import azure.functions as func

from function.models.requests import AssetSubmitRequest
from function.models.responses import (
    AssetSubmitResponse,
    ErrorResponse,
)
from function.repositories.asset_query_repo import AssetQueryRepository
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
    return _json_response(
        ErrorResponse(error=error, details=details).model_dump(),
        status_code=status_code,
    )


def _compute_asset_id(platform_id: str, platform_refs: dict) -> str:
    """Deterministic asset_id = SHA256(platform_id + canonical_json(refs))[:32]."""
    canonical = json.dumps(platform_refs, sort_keys=True, separators=(",", ":"))
    composite = f"{platform_id}|{canonical}"
    return hashlib.sha256(composite.encode()).hexdigest()[:32]


# ============================================================================
# SUBMIT
# ============================================================================

@platform_bp.route(route="platform/submit", methods=["POST"])
def platform_submit(req: func.HttpRequest) -> func.HttpResponse:
    """
    Submit a geospatial asset for processing.

    POST /api/platform/submit
    Body: AssetSubmitRequest JSON
    Returns: AssetSubmitResponse (202 Accepted)

    B2B endpoint. Sets:
    - correlation_id = request_id (for B2B status polling)
    - asset_id = computed deterministic ID (for job→asset tracing)
    """
    # Parse request
    try:
        body = req.get_json()
        request = AssetSubmitRequest.model_validate(body)
    except Exception as e:
        return _error("Invalid request", str(e))

    repo = AssetQueryRepository()

    # 1. Validate platform
    platform = repo.get_platform(request.platform_id)
    if platform is None:
        return _error("Platform not found", f"Platform '{request.platform_id}' does not exist", 404)

    if not platform.get("is_active", True):
        return _error("Platform inactive", f"Platform '{request.platform_id}' is inactive")

    # 2. Validate required refs
    required_refs = platform.get("required_refs") or []
    missing = [ref for ref in required_refs if ref not in request.platform_refs]
    if missing:
        return _error("Missing required refs", f"Missing: {', '.join(missing)}")

    # 3. Compute deterministic asset_id
    asset_id = _compute_asset_id(request.platform_id, request.platform_refs)

    # 4. Get-or-create asset
    try:
        asset_row, created = repo.get_or_create_asset(
            asset_id=asset_id,
            platform_id=request.platform_id,
            platform_refs=request.platform_refs,
            data_type=request.data_type,
        )
    except Exception as e:
        logger.exception(f"Failed to create asset: {e}")
        return _error("Database error", str(e), 500)

    # 5. Check version ordering
    if repo.has_unresolved_versions(asset_id):
        return _error(
            "Unresolved versions exist",
            "Approve or reject pending versions before submitting a new one",
            409,
        )

    # 6. Create version
    next_ordinal = repo.get_next_ordinal(asset_id)
    try:
        version_row = repo.create_version(
            asset_id=asset_id,
            version_ordinal=next_ordinal,
            version_label=request.version_label,
        )
    except Exception as e:
        logger.exception(f"Failed to create version: {e}")
        return _error("Database error", str(e), 500)

    # 7. Queue DAG job to Service Bus
    queue_name = os.environ.get("JOB_QUEUE_NAME")
    if not queue_name:
        return _error("Server misconfigured", "JOB_QUEUE_NAME not set", 503)

    try:
        from core.models import JobQueueMessage
        from infrastructure.service_bus import ServiceBusPublisher

        queue_message = JobQueueMessage(
            workflow_id=request.workflow_id,
            input_params=request.input_params,
            correlation_id=request.request_id,
            asset_id=asset_id,
            callback_url=request.callback_url,
            submitted_by=request.submitted_by or "rmhdagmaster-gateway",
            submitted_at=datetime.now(timezone.utc),
        )

        publisher = ServiceBusPublisher.instance()
        message_id = publisher.send_message(
            queue_name=queue_name,
            message=queue_message,
            ttl_hours=24,
        )
    except Exception as e:
        logger.exception(f"Failed to queue DAG job: {e}")
        return _error("Failed to queue job", str(e), 503)

    # 8. Update version with processing state
    repo.update_version_processing(
        asset_id=asset_id,
        version_ordinal=next_ordinal,
        processing_state="processing",
        current_job_id=message_id,
        processing_started_at=datetime.now(timezone.utc),
    )

    response = AssetSubmitResponse(
        asset_id=asset_id,
        version_ordinal=next_ordinal,
        request_id=request.request_id,
        message_id=message_id,
        workflow_id=request.workflow_id,
        status="queued",
        created=created,
    )

    logger.info(
        f"Platform submit: asset={asset_id} v{next_ordinal} "
        f"workflow={request.workflow_id} message_id={message_id} "
        f"correlation_id={request.request_id}"
    )

    return _json_response(response.model_dump(), status_code=202)


# ============================================================================
# STATUS POLLING
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
