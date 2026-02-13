# ============================================================================
# CLAUDE CONTEXT - ASSET BLUEPRINT
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Gateway - Business domain query/admin endpoints
# PURPOSE: HTTP endpoints for geospatial asset queries and lifecycle admin
# LAST_REVIEWED: 11 FEB 2026
# ============================================================================
"""
Asset Blueprint

Query and admin endpoints for geospatial asset lifecycle.
NOTE: Submit lives in platform_bp.py (B2B-facing).

Endpoints:
- GET    /api/assets/{asset_id}                           - Get asset details
- GET    /api/assets                                      - List assets
- GET    /api/assets/{asset_id}/versions                  - List versions
- GET    /api/assets/{asset_id}/versions/{ordinal}        - Get specific version
- GET    /api/assets/{asset_id}/versions/latest           - Get latest approved
- POST   /api/assets/{asset_id}/versions/{ordinal}/approve - Approve version
- POST   /api/assets/{asset_id}/versions/{ordinal}/reject  - Reject version
- POST   /api/assets/{asset_id}/clearance/cleared         - Mark cleared
- POST   /api/assets/{asset_id}/clearance/public          - Mark public
- DELETE /api/assets/{asset_id}                            - Soft delete
- GET    /api/assets/platforms                             - List platforms
"""

import json
import logging
from datetime import datetime, timezone

import azure.functions as func

from function.models.requests import ApprovalRequest, ClearanceRequest
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
# ASSET QUERIES
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
# VERSION QUERIES
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
# APPROVAL
# ============================================================================

@asset_bp.route(route="assets/{asset_id}/versions/{ordinal}/approve", methods=["POST"])
def version_approve(req: func.HttpRequest) -> func.HttpResponse:
    """
    Approve an asset version.

    POST /api/assets/{asset_id}/versions/{ordinal}/approve
    Body: {"reviewer": "analyst@gov"}
    """
    asset_id = req.route_params.get("asset_id")
    try:
        ordinal = int(req.route_params.get("ordinal"))
    except (TypeError, ValueError):
        return _error("Invalid ordinal", "version_ordinal must be an integer")

    try:
        body = req.get_json()
        request = ApprovalRequest.model_validate(body)
    except Exception as e:
        return _error("Invalid request", str(e))

    try:
        repo = AssetQueryRepository()

        # Get current version
        row = repo.get_version(asset_id, ordinal)
        if row is None:
            return _error("Version not found", f"Version {ordinal} not found for asset '{asset_id}'", 404)

        if row["approval_state"] != "pending_review":
            return _error(
                "Invalid state transition",
                f"Cannot approve from state '{row['approval_state']}' (must be 'pending_review')",
                409,
            )

        updated = repo.update_version_approval(
            asset_id=asset_id,
            version_ordinal=ordinal,
            approval_state="approved",
            reviewer=request.reviewer,
            expected_version=row["version"],
        )
        if not updated:
            return _error("Version conflict", "Record was modified by another request", 409)

        # Fetch updated version
        updated_row = repo.get_version(asset_id, ordinal)
        logger.info(f"Approved asset {asset_id} v{ordinal} by {request.reviewer}")
        return _json_response(_version_to_response(updated_row))

    except Exception as e:
        logger.exception(f"Error approving version: {e}")
        return _error("Database error", str(e), 500)


@asset_bp.route(route="assets/{asset_id}/versions/{ordinal}/reject", methods=["POST"])
def version_reject(req: func.HttpRequest) -> func.HttpResponse:
    """
    Reject an asset version.

    POST /api/assets/{asset_id}/versions/{ordinal}/reject
    Body: {"reviewer": "analyst@gov", "reason": "Bad CRS"}
    """
    asset_id = req.route_params.get("asset_id")
    try:
        ordinal = int(req.route_params.get("ordinal"))
    except (TypeError, ValueError):
        return _error("Invalid ordinal", "version_ordinal must be an integer")

    try:
        body = req.get_json()
        request = ApprovalRequest.model_validate(body)
    except Exception as e:
        return _error("Invalid request", str(e))

    if not request.reason:
        return _error("Reason required", "Rejection reason is required")

    try:
        repo = AssetQueryRepository()

        row = repo.get_version(asset_id, ordinal)
        if row is None:
            return _error("Version not found", f"Version {ordinal} not found for asset '{asset_id}'", 404)

        if row["approval_state"] != "pending_review":
            return _error(
                "Invalid state transition",
                f"Cannot reject from state '{row['approval_state']}' (must be 'pending_review')",
                409,
            )

        updated = repo.update_version_approval(
            asset_id=asset_id,
            version_ordinal=ordinal,
            approval_state="rejected",
            reviewer=request.reviewer,
            rejection_reason=request.reason,
            expected_version=row["version"],
        )
        if not updated:
            return _error("Version conflict", "Record was modified by another request", 409)

        updated_row = repo.get_version(asset_id, ordinal)
        logger.info(f"Rejected asset {asset_id} v{ordinal} by {request.reviewer}: {request.reason}")
        return _json_response(_version_to_response(updated_row))

    except Exception as e:
        logger.exception(f"Error rejecting version: {e}")
        return _error("Database error", str(e), 500)


# ============================================================================
# CLEARANCE
# ============================================================================

@asset_bp.route(route="assets/{asset_id}/clearance/cleared", methods=["POST"])
def asset_mark_cleared(req: func.HttpRequest) -> func.HttpResponse:
    """
    Mark asset clearance: UNCLEARED → OUO.

    POST /api/assets/{asset_id}/clearance/cleared
    Body: {"actor": "reviewer@gov"}
    """
    asset_id = req.route_params.get("asset_id")

    try:
        body = req.get_json()
        request = ClearanceRequest.model_validate(body)
    except Exception as e:
        return _error("Invalid request", str(e))

    try:
        repo = AssetQueryRepository()

        row = repo.get_asset(asset_id)
        if row is None:
            return _error("Asset not found", f"No asset with ID '{asset_id}'", 404)

        if row["clearance_state"] != "uncleared":
            return _error(
                "Invalid state transition",
                f"Cannot clear from state '{row['clearance_state']}' (must be 'uncleared')",
                409,
            )

        now = datetime.now(timezone.utc)
        updated = repo.update_asset_clearance(
            asset_id=asset_id,
            clearance_state="ouo",
            actor=request.actor,
            expected_version=row["version"],
            cleared_at=now,
        )
        if not updated:
            return _error("Version conflict", "Record was modified by another request", 409)

        updated_row = repo.get_asset(asset_id)
        logger.info(f"Asset {asset_id} cleared by {request.actor}")
        return _json_response(_asset_to_response(updated_row))

    except Exception as e:
        logger.exception(f"Error clearing asset: {e}")
        return _error("Database error", str(e), 500)


@asset_bp.route(route="assets/{asset_id}/clearance/public", methods=["POST"])
def asset_mark_public(req: func.HttpRequest) -> func.HttpResponse:
    """
    Mark asset clearance: OUO → PUBLIC.

    POST /api/assets/{asset_id}/clearance/public
    Body: {"actor": "reviewer@gov"}
    """
    asset_id = req.route_params.get("asset_id")

    try:
        body = req.get_json()
        request = ClearanceRequest.model_validate(body)
    except Exception as e:
        return _error("Invalid request", str(e))

    try:
        repo = AssetQueryRepository()

        row = repo.get_asset(asset_id)
        if row is None:
            return _error("Asset not found", f"No asset with ID '{asset_id}'", 404)

        if row["clearance_state"] != "ouo":
            return _error(
                "Invalid state transition",
                f"Cannot make public from state '{row['clearance_state']}' (must be 'ouo')",
                409,
            )

        now = datetime.now(timezone.utc)
        updated = repo.update_asset_clearance(
            asset_id=asset_id,
            clearance_state="public",
            actor=request.actor,
            expected_version=row["version"],
            made_public_at=now,
        )
        if not updated:
            return _error("Version conflict", "Record was modified by another request", 409)

        updated_row = repo.get_asset(asset_id)
        logger.info(f"Asset {asset_id} made public by {request.actor}")
        return _json_response(_asset_to_response(updated_row))

    except Exception as e:
        logger.exception(f"Error making asset public: {e}")
        return _error("Database error", str(e), 500)


# ============================================================================
# SOFT DELETE
# ============================================================================

@asset_bp.route(route="assets/{asset_id}", methods=["DELETE"])
def asset_delete(req: func.HttpRequest) -> func.HttpResponse:
    """
    Soft-delete an asset.

    DELETE /api/assets/{asset_id}
    Query params: actor (required)
    """
    asset_id = req.route_params.get("asset_id")
    actor = req.params.get("actor")

    if not actor:
        return _error("Actor required", "Query parameter 'actor' is required")

    try:
        repo = AssetQueryRepository()

        row = repo.get_asset(asset_id)
        if row is None:
            return _error("Asset not found", f"No asset with ID '{asset_id}'", 404)

        if row.get("deleted_at") is not None:
            return _error("Already deleted", f"Asset '{asset_id}' is already deleted", 409)

        deleted = repo.soft_delete_asset(
            asset_id=asset_id,
            actor=actor,
            expected_version=row["version"],
        )
        if not deleted:
            return _error("Version conflict", "Record was modified by another request", 409)

        logger.info(f"Asset {asset_id} soft-deleted by {actor}")
        return _json_response({"deleted": True, "asset_id": asset_id})

    except Exception as e:
        logger.exception(f"Error deleting asset: {e}")
        return _error("Database error", str(e), 500)


# ============================================================================
# PLATFORMS (read-only convenience)
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
