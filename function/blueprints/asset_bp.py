# ============================================================================
# CLAUDE CONTEXT - ASSET BLUEPRINT
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Gateway - Asset query endpoints (read-only) + mutation proxies
# PURPOSE: HTTP endpoints for geospatial asset queries and lifecycle admin
# LAST_REVIEWED: 12 FEB 2026
# ============================================================================
"""
Asset Blueprint

Read-only query endpoints served directly from gateway DB connection.
Mutation endpoints (approve, reject, clearance, delete) are proxy stubs
awaiting orchestrator domain endpoints (Pass 2/3).
NOTE: Submit lives in platform_bp.py (B2B-facing).

Read endpoints (gateway-local):
- GET    /api/assets/{asset_id}                           - Get asset details
- GET    /api/assets                                      - List assets
- GET    /api/assets/{asset_id}/versions                  - List versions
- GET    /api/assets/{asset_id}/versions/{ordinal}        - Get specific version
- GET    /api/assets/{asset_id}/versions/latest           - Get latest approved
- GET    /api/assets/platforms                             - List platforms

Mutation stubs (will proxy to orchestrator):
- POST   /api/assets/{asset_id}/versions/{ordinal}/approve - 501 stub
- POST   /api/assets/{asset_id}/versions/{ordinal}/reject  - 501 stub
- POST   /api/assets/{asset_id}/clearance/cleared         - 501 stub
- POST   /api/assets/{asset_id}/clearance/public          - 501 stub
- DELETE /api/assets/{asset_id}                            - 501 stub
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


def _not_implemented(action: str, step: str) -> func.HttpResponse:
    """Return 501 for mutation endpoints awaiting orchestrator domain API."""
    return _error(
        "Not implemented",
        f"{action} is being migrated to orchestrator domain API. "
        f"See TODO.md Phase 4B-REBUILD, step {step}.",
        501,
    )


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
# APPROVAL (proxy stubs — awaiting orchestrator domain endpoints)
# ============================================================================

@asset_bp.route(route="assets/{asset_id}/versions/{ordinal}/approve", methods=["POST"])
def version_approve(req: func.HttpRequest) -> func.HttpResponse:
    """
    Approve an asset version.

    POST /api/assets/{asset_id}/versions/{ordinal}/approve

    STUB: Returns 501 until orchestrator domain endpoint is built.
    Will proxy to: POST {ORCHESTRATOR_BASE_URL}/api/v1/domain/assets/{id}/versions/{ord}/approve
    """
    return _not_implemented("Version approval", "R2c")


@asset_bp.route(route="assets/{asset_id}/versions/{ordinal}/reject", methods=["POST"])
def version_reject(req: func.HttpRequest) -> func.HttpResponse:
    """
    Reject an asset version.

    POST /api/assets/{asset_id}/versions/{ordinal}/reject

    STUB: Returns 501 until orchestrator domain endpoint is built.
    Will proxy to: POST {ORCHESTRATOR_BASE_URL}/api/v1/domain/assets/{id}/versions/{ord}/reject
    """
    return _not_implemented("Version rejection", "R2d")


# ============================================================================
# CLEARANCE (proxy stubs — awaiting orchestrator domain endpoints)
# ============================================================================

@asset_bp.route(route="assets/{asset_id}/clearance/cleared", methods=["POST"])
def asset_mark_cleared(req: func.HttpRequest) -> func.HttpResponse:
    """
    Mark asset clearance: UNCLEARED -> OUO.

    POST /api/assets/{asset_id}/clearance/cleared

    STUB: Returns 501 until orchestrator domain endpoint is built.
    Will proxy to: POST {ORCHESTRATOR_BASE_URL}/api/v1/domain/assets/{id}/clearance/cleared
    """
    return _not_implemented("Clearance update", "R2e")


@asset_bp.route(route="assets/{asset_id}/clearance/public", methods=["POST"])
def asset_mark_public(req: func.HttpRequest) -> func.HttpResponse:
    """
    Mark asset clearance: OUO -> PUBLIC.

    POST /api/assets/{asset_id}/clearance/public

    STUB: Returns 501 until orchestrator domain endpoint is built.
    Will proxy to: POST {ORCHESTRATOR_BASE_URL}/api/v1/domain/assets/{id}/clearance/public
    """
    return _not_implemented("Clearance update", "R2f")


# ============================================================================
# SOFT DELETE (proxy stub — awaiting orchestrator domain endpoint)
# ============================================================================

@asset_bp.route(route="assets/{asset_id}", methods=["DELETE"])
def asset_delete(req: func.HttpRequest) -> func.HttpResponse:
    """
    Soft-delete an asset.

    DELETE /api/assets/{asset_id}

    STUB: Returns 501 until orchestrator domain endpoint is built.
    Will proxy to: DELETE {ORCHESTRATOR_BASE_URL}/api/v1/domain/assets/{id}
    """
    return _not_implemented("Asset deletion", "R2g")


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
