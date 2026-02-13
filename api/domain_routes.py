# ============================================================================
# CLAUDE CONTEXT - DOMAIN ROUTES
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Core - Orchestrator domain authority HTTP endpoints
# PURPOSE: HTTP API for business domain mutations (asset lifecycle)
# CREATED: 12 FEB 2026
# ============================================================================
"""
Domain Routes

Orchestrator-side HTTP endpoints for business domain mutations.
The gateway proxies all mutation requests here. These endpoints
call GeospatialAssetService for atomic domain operations.

Endpoints:
- POST   /api/v1/domain/submit                                    - Submit asset
- POST   /api/v1/domain/assets/{asset_id}/versions/{ordinal}/approve - Approve
- POST   /api/v1/domain/assets/{asset_id}/versions/{ordinal}/reject  - Reject
- POST   /api/v1/domain/assets/{asset_id}/clearance/cleared        - Mark cleared
- POST   /api/v1/domain/assets/{asset_id}/clearance/public         - Mark public
- DELETE /api/v1/domain/assets/{asset_id}                          - Soft delete
"""

import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/domain", tags=["domain"])


# ============================================================================
# DEPENDENCY INJECTION
# ============================================================================

_asset_service = None


def set_domain_services(asset_service):
    """Called by main.py at startup to inject the asset service."""
    global _asset_service
    _asset_service = asset_service


def _get_asset_service():
    """Get the asset service, raising 503 if not initialized."""
    if _asset_service is None:
        raise HTTPException(503, "Asset service not initialized")
    return _asset_service


# ============================================================================
# REQUEST / RESPONSE MODELS
# ============================================================================

class DomainSubmitRequest(BaseModel):
    """Request body for asset submission (from gateway proxy)."""
    platform_id: str
    platform_refs: Dict[str, Any]
    data_type: str = "raster"
    version_label: Optional[str] = None
    workflow_id: str
    input_params: Dict[str, Any] = Field(default_factory=dict)
    submitted_by: Optional[str] = None
    correlation_id: Optional[str] = None


class DomainSubmitResponse(BaseModel):
    """Response after successful asset submission."""
    asset_id: str
    version_ordinal: int
    job_id: str
    correlation_id: Optional[str] = None
    status: str = "accepted"


class ApprovalRequest(BaseModel):
    """Request body for version approval/rejection."""
    reviewer: str
    reason: Optional[str] = None


class ClearanceRequest(BaseModel):
    """Request body for clearance state change."""
    actor: str


# ============================================================================
# SUBMIT
# ============================================================================

@router.post("/submit", response_model=DomainSubmitResponse, status_code=202)
async def domain_submit(request: DomainSubmitRequest):
    """
    Submit a geospatial asset for processing.

    Atomic operation: validate platform → get-or-create asset →
    create version → create DAG job → link version to job.

    Called by gateway proxy (POST /api/platform/submit).
    """
    svc = _get_asset_service()

    try:
        asset, version, job = await svc.submit_asset(
            platform_id=request.platform_id,
            platform_refs=request.platform_refs,
            data_type=request.data_type,
            version_label=request.version_label,
            workflow_id=request.workflow_id,
            input_params=request.input_params,
            submitted_by=request.submitted_by,
            correlation_id=request.correlation_id,
        )
    except ValueError as e:
        raise HTTPException(400, str(e))
    except KeyError as e:
        raise HTTPException(404, str(e))

    return DomainSubmitResponse(
        asset_id=asset.asset_id,
        version_ordinal=version.version_ordinal,
        job_id=job.job_id,
        correlation_id=request.correlation_id,
    )


# ============================================================================
# APPROVAL
# ============================================================================

@router.post("/assets/{asset_id}/versions/{ordinal}/approve")
async def domain_approve(asset_id: str, ordinal: int, request: ApprovalRequest):
    """
    Approve an asset version: PENDING_REVIEW → APPROVED.

    Called by gateway proxy (POST /api/assets/{id}/versions/{ord}/approve).
    """
    svc = _get_asset_service()

    try:
        version = await svc.approve_version(asset_id, ordinal, request.reviewer)
    except ValueError as e:
        error_msg = str(e)
        if "not found" in error_msg:
            raise HTTPException(404, error_msg)
        if "conflict" in error_msg.lower():
            raise HTTPException(409, error_msg)
        raise HTTPException(400, error_msg)

    return {
        "asset_id": version.asset_id,
        "version_ordinal": version.version_ordinal,
        "approval_state": version.approval_state.value,
        "reviewer": version.reviewer,
    }


@router.post("/assets/{asset_id}/versions/{ordinal}/reject")
async def domain_reject(asset_id: str, ordinal: int, request: ApprovalRequest):
    """
    Reject an asset version: PENDING_REVIEW → REJECTED.

    Called by gateway proxy (POST /api/assets/{id}/versions/{ord}/reject).
    """
    svc = _get_asset_service()

    if not request.reason:
        raise HTTPException(400, "Rejection reason is required")

    try:
        version = await svc.reject_version(
            asset_id, ordinal, request.reviewer, request.reason
        )
    except ValueError as e:
        error_msg = str(e)
        if "not found" in error_msg:
            raise HTTPException(404, error_msg)
        if "conflict" in error_msg.lower():
            raise HTTPException(409, error_msg)
        raise HTTPException(400, error_msg)

    return {
        "asset_id": version.asset_id,
        "version_ordinal": version.version_ordinal,
        "approval_state": version.approval_state.value,
        "reviewer": version.reviewer,
        "rejection_reason": version.rejection_reason,
    }


# ============================================================================
# CLEARANCE
# ============================================================================

@router.post("/assets/{asset_id}/clearance/cleared")
async def domain_mark_cleared(asset_id: str, request: ClearanceRequest):
    """
    Mark asset clearance: UNCLEARED → OUO.

    Called by gateway proxy (POST /api/assets/{id}/clearance/cleared).
    """
    svc = _get_asset_service()

    try:
        asset = await svc.mark_cleared(asset_id, request.actor)
    except ValueError as e:
        error_msg = str(e)
        if "not found" in error_msg:
            raise HTTPException(404, error_msg)
        if "conflict" in error_msg.lower():
            raise HTTPException(409, error_msg)
        raise HTTPException(400, error_msg)

    return {
        "asset_id": asset.asset_id,
        "clearance_state": asset.clearance_state.value,
    }


@router.post("/assets/{asset_id}/clearance/public")
async def domain_mark_public(asset_id: str, request: ClearanceRequest):
    """
    Mark asset clearance: OUO → PUBLIC.

    Called by gateway proxy (POST /api/assets/{id}/clearance/public).
    """
    svc = _get_asset_service()

    try:
        asset = await svc.mark_public(asset_id, request.actor)
    except ValueError as e:
        error_msg = str(e)
        if "not found" in error_msg:
            raise HTTPException(404, error_msg)
        if "conflict" in error_msg.lower():
            raise HTTPException(409, error_msg)
        raise HTTPException(400, error_msg)

    return {
        "asset_id": asset.asset_id,
        "clearance_state": asset.clearance_state.value,
    }


# ============================================================================
# SOFT DELETE
# ============================================================================

@router.delete("/assets/{asset_id}")
async def domain_delete(asset_id: str, actor: str):
    """
    Soft-delete an asset.

    Called by gateway proxy (DELETE /api/assets/{id}?actor=...).
    """
    svc = _get_asset_service()

    try:
        result = await svc.soft_delete_asset(asset_id, actor)
    except ValueError as e:
        error_msg = str(e)
        if "not found" in error_msg:
            raise HTTPException(404, error_msg)
        raise HTTPException(400, error_msg)

    return {"deleted": result, "asset_id": asset_id}
