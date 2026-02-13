# ============================================================================
# CLAUDE CONTEXT - ASSET BLUEPRINT
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Gateway - Asset query endpoints (read-only) + mutation proxies
# PURPOSE: HTTP endpoints for asset queries (read) and lifecycle mutations (proxy)
# LAST_REVIEWED: 12 FEB 2026
# ============================================================================
"""
Asset Blueprint

Read endpoints served directly from gateway DB connection.
Mutation endpoints proxy to the orchestrator's domain API.
NOTE: Submit lives in platform_bp.py (B2B-facing).

Read endpoints (gateway-local):
- GET    /api/assets/{asset_id}                           - Get asset details
- GET    /api/assets                                      - List assets
- GET    /api/assets/{asset_id}/versions                  - List versions
- GET    /api/assets/{asset_id}/versions/{ordinal}        - Get specific version
- GET    /api/assets/{asset_id}/versions/latest           - Get latest approved
- GET    /api/assets/platforms                             - List platforms

Mutation endpoints (proxy to orchestrator):
- POST   /api/assets/{asset_id}/versions/{ordinal}/approve - Approve version
- POST   /api/assets/{asset_id}/versions/{ordinal}/reject  - Reject version
- POST   /api/assets/{asset_id}/clearance/cleared         - Mark cleared
- POST   /api/assets/{asset_id}/clearance/public          - Mark public
- DELETE /api/assets/{asset_id}                            - Soft delete
"""

import json
import logging

import azure.functions as func

from function.models.responses import (
    AssetResponse,
    AssetListResponse,
    VersionResponse,
    VersionListResponse,
    ErrorResponse,
)
from function.repositories.asset_query_repo import AssetQueryRepository

logger = logging.getLogger(__name__)
asset_bp = func.Blueprint()


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


def _proxy_response(status_code: int, body: dict) -> func.HttpResponse:
    """Relay an orchestrator response back to the caller."""
    return _json_response(body, status_code)


def _asset_to_response(row: dict) -> dict:
    """Convert DB row to AssetResponse dict."""
    return AssetResponse(
        asset_id=row["asset_id"],
        platform_id=row["platform_id"],
        platform_refs=row.get("platform_refs") or {},
        data_type=row["data_type"],
        clearance_state=row["clearance_state"],
        is_deleted=row.get("deleted_at") is not None,
        created_at=row["created_at"],
        updated_at=row.get("updated_at") or row["created_at"],
        version=row.get("version", 1),
    ).model_dump()


def _version_to_response(row: dict) -> dict:
    """Convert DB row to VersionResponse dict."""
    return VersionResponse(
        asset_id=row["asset_id"],
        version_ordinal=row["version_ordinal"],
        version_label=row.get("version_label"),
        approval_state=row["approval_state"],
        processing_state=row["processing_state"],
        current_job_id=row.get("current_job_id"),
        revision=row.get("revision", 0),
        blob_path=row.get("blob_path"),
        table_name=row.get("table_name"),
        stac_item_id=row.get("stac_item_id"),
        stac_collection_id=row.get("stac_collection_id"),
        created_at=row["created_at"],
        updated_at=row.get("updated_at") or row["created_at"],
        version=row.get("version", 1),
    ).model_dump()


# ============================================================================
# ASSET QUERIES (read-only)
# ============================================================================

@asset_bp.route(route="assets/{asset_id}", methods=["GET"])
def asset_get(req: func.HttpRequest) -> func.HttpResponse:
    """
    Get asset details.

    GET /api/assets/{asset_id}
    """
    asset_id = req.route_params.get("asset_id")

    try:
        repo = AssetQueryRepository()
        row = repo.get_asset(asset_id)
        if row is None:
            return _error("Asset not found", f"No asset with ID '{asset_id}'", 404)

        return _json_response(_asset_to_response(row))

    except Exception as e:
        logger.exception(f"Error fetching asset: {e}")
        return _error("Database error", str(e), 500)


@asset_bp.route(route="assets", methods=["GET"])
def asset_list(req: func.HttpRequest) -> func.HttpResponse:
    """
    List assets with optional filters.

    GET /api/assets
    Query params: platform_id, include_deleted, limit, offset
    """
    platform_id = req.params.get("platform_id")
    include_deleted = req.params.get("include_deleted", "false").lower() == "true"
    limit = min(int(req.params.get("limit", "50")), 500)
    offset = int(req.params.get("offset", "0"))

    try:
        repo = AssetQueryRepository()
        rows = repo.list_assets(
            platform_id=platform_id,
            include_deleted=include_deleted,
            limit=limit,
            offset=offset,
        )
        total = repo.count_assets(
            platform_id=platform_id,
            include_deleted=include_deleted,
        )

        response = AssetListResponse(
            assets=[AssetResponse(**_asset_to_response(r)) for r in rows],
            total=total,
            limit=limit,
            offset=offset,
        )
        return _json_response(response.model_dump())

    except Exception as e:
        logger.exception(f"Error listing assets: {e}")
        return _error("Database error", str(e), 500)


# ============================================================================
# VERSION QUERIES (read-only)
# ============================================================================

@asset_bp.route(route="assets/{asset_id}/versions", methods=["GET"])
def version_list(req: func.HttpRequest) -> func.HttpResponse:
    """
    List all versions for an asset.

    GET /api/assets/{asset_id}/versions
    """
    asset_id = req.route_params.get("asset_id")

    try:
        repo = AssetQueryRepository()
        rows = repo.list_versions(asset_id)

        response = VersionListResponse(
            versions=[VersionResponse(**_version_to_response(r)) for r in rows],
            asset_id=asset_id,
        )
        return _json_response(response.model_dump())

    except Exception as e:
        logger.exception(f"Error listing versions: {e}")
        return _error("Database error", str(e), 500)


@asset_bp.route(route="assets/{asset_id}/versions/latest", methods=["GET"])
def version_latest(req: func.HttpRequest) -> func.HttpResponse:
    """
    Get the latest approved version.

    GET /api/assets/{asset_id}/versions/latest
    """
    asset_id = req.route_params.get("asset_id")

    try:
        repo = AssetQueryRepository()
        row = repo.get_latest_approved_version(asset_id)
        if row is None:
            return _error("No approved version", f"No approved version for asset '{asset_id}'", 404)

        return _json_response(_version_to_response(row))

    except Exception as e:
        logger.exception(f"Error fetching latest version: {e}")
        return _error("Database error", str(e), 500)


@asset_bp.route(route="assets/{asset_id}/versions/{ordinal}", methods=["GET"])
def version_get(req: func.HttpRequest) -> func.HttpResponse:
    """
    Get a specific version.

    GET /api/assets/{asset_id}/versions/{ordinal}
    """
    asset_id = req.route_params.get("asset_id")
    try:
        ordinal = int(req.route_params.get("ordinal"))
    except (TypeError, ValueError):
        return _error("Invalid ordinal", "version_ordinal must be an integer")

    try:
        repo = AssetQueryRepository()
        row = repo.get_version(asset_id, ordinal)
        if row is None:
            return _error("Version not found", f"Version {ordinal} not found for asset '{asset_id}'", 404)

        return _json_response(_version_to_response(row))

    except Exception as e:
        logger.exception(f"Error fetching version: {e}")
        return _error("Database error", str(e), 500)


# ============================================================================
# APPROVAL (proxy to orchestrator domain API)
# ============================================================================

@asset_bp.route(route="assets/{asset_id}/versions/{ordinal}/approve", methods=["POST"])
def version_approve(req: func.HttpRequest) -> func.HttpResponse:
    """
    Approve an asset version.

    POST /api/assets/{asset_id}/versions/{ordinal}/approve
    Proxies to: POST {ORCHESTRATOR_BASE_URL}/api/v1/domain/assets/{id}/versions/{ord}/approve
    """
    from function.models.requests import ApprovalRequest
    from function.services.orchestrator_client import OrchestratorClient

    asset_id = req.route_params.get("asset_id")
    ordinal = int(req.route_params.get("ordinal"))

    try:
        body = req.get_json()
        request = ApprovalRequest.model_validate(body)
    except Exception as e:
        return _error("Validation error", str(e), 422)

    client = OrchestratorClient()
    status_code, resp_body = client.approve(
        asset_id, ordinal, {"reviewer": request.reviewer}
    )
    return _proxy_response(status_code, resp_body)


@asset_bp.route(route="assets/{asset_id}/versions/{ordinal}/reject", methods=["POST"])
def version_reject(req: func.HttpRequest) -> func.HttpResponse:
    """
    Reject an asset version.

    POST /api/assets/{asset_id}/versions/{ordinal}/reject
    Proxies to: POST {ORCHESTRATOR_BASE_URL}/api/v1/domain/assets/{id}/versions/{ord}/reject
    """
    from function.models.requests import ApprovalRequest
    from function.services.orchestrator_client import OrchestratorClient

    asset_id = req.route_params.get("asset_id")
    ordinal = int(req.route_params.get("ordinal"))

    try:
        body = req.get_json()
        request = ApprovalRequest.model_validate(body)
    except Exception as e:
        return _error("Validation error", str(e), 422)

    client = OrchestratorClient()
    status_code, resp_body = client.reject(
        asset_id, ordinal, {"reviewer": request.reviewer, "reason": request.reason}
    )
    return _proxy_response(status_code, resp_body)


# ============================================================================
# CLEARANCE (proxy to orchestrator domain API)
# ============================================================================

@asset_bp.route(route="assets/{asset_id}/clearance/cleared", methods=["POST"])
def asset_mark_cleared(req: func.HttpRequest) -> func.HttpResponse:
    """
    Mark asset clearance: UNCLEARED -> OUO.

    POST /api/assets/{asset_id}/clearance/cleared
    Proxies to: POST {ORCHESTRATOR_BASE_URL}/api/v1/domain/assets/{id}/clearance/cleared
    """
    from function.models.requests import ClearanceRequest
    from function.services.orchestrator_client import OrchestratorClient

    asset_id = req.route_params.get("asset_id")

    try:
        body = req.get_json()
        request = ClearanceRequest.model_validate(body)
    except Exception as e:
        return _error("Validation error", str(e), 422)

    client = OrchestratorClient()
    status_code, resp_body = client.mark_cleared(asset_id, {"actor": request.actor})
    return _proxy_response(status_code, resp_body)


@asset_bp.route(route="assets/{asset_id}/clearance/public", methods=["POST"])
def asset_mark_public(req: func.HttpRequest) -> func.HttpResponse:
    """
    Mark asset clearance: OUO -> PUBLIC.

    POST /api/assets/{asset_id}/clearance/public
    Proxies to: POST {ORCHESTRATOR_BASE_URL}/api/v1/domain/assets/{id}/clearance/public
    """
    from function.models.requests import ClearanceRequest
    from function.services.orchestrator_client import OrchestratorClient

    asset_id = req.route_params.get("asset_id")

    try:
        body = req.get_json()
        request = ClearanceRequest.model_validate(body)
    except Exception as e:
        return _error("Validation error", str(e), 422)

    client = OrchestratorClient()
    status_code, resp_body = client.mark_public(asset_id, {"actor": request.actor})
    return _proxy_response(status_code, resp_body)


# ============================================================================
# SOFT DELETE (proxy to orchestrator domain API)
# ============================================================================

@asset_bp.route(route="assets/{asset_id}", methods=["DELETE"])
def asset_delete(req: func.HttpRequest) -> func.HttpResponse:
    """
    Soft-delete an asset.

    DELETE /api/assets/{asset_id}?actor=...
    Proxies to: DELETE {ORCHESTRATOR_BASE_URL}/api/v1/domain/assets/{id}?actor=...
    """
    from function.services.orchestrator_client import OrchestratorClient

    asset_id = req.route_params.get("asset_id")
    actor = req.params.get("actor")

    if not actor:
        return _error("Missing parameter", "'actor' query parameter is required", 400)

    client = OrchestratorClient()
    status_code, resp_body = client.delete_asset(asset_id, actor)
    return _proxy_response(status_code, resp_body)


# ============================================================================
# PLATFORMS (read-only)
# ============================================================================

@asset_bp.route(route="assets/platforms", methods=["GET"])
def platform_list(req: func.HttpRequest) -> func.HttpResponse:
    """
    List active platforms.

    GET /api/assets/platforms
    """
    try:
        repo = AssetQueryRepository()
        platforms = repo.list_active_platforms()
        return _json_response({"platforms": platforms, "count": len(platforms)})

    except Exception as e:
        logger.exception(f"Error listing platforms: {e}")
        return _error("Database error", str(e), 500)


__all__ = ["asset_bp"]
