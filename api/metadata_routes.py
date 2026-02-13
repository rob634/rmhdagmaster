# ============================================================================
# CLAUDE CONTEXT - METADATA ROUTES
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Core - Orchestrator domain authority HTTP endpoints
# PURPOSE: HTTP API for layer metadata CRUD (gateway proxies mutations here)
# CREATED: 13 FEB 2026
# ============================================================================
"""
Metadata Routes

Orchestrator-side HTTP endpoints for layer metadata CRUD.
The gateway proxies all mutation requests here. Read endpoints
are also available here (gateway serves reads locally too).

Endpoints:
- POST   /api/v1/domain/metadata/              - Create metadata
- GET    /api/v1/domain/metadata/              - List metadata
- GET    /api/v1/domain/metadata/{asset_id}    - Get metadata
- PUT    /api/v1/domain/metadata/{asset_id}    - Update metadata
- DELETE /api/v1/domain/metadata/{asset_id}    - Delete metadata
"""

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/domain/metadata", tags=["metadata"])


# ============================================================================
# DEPENDENCY INJECTION
# ============================================================================

_metadata_service = None


def set_metadata_services(metadata_service):
    """Called by main.py at startup to inject the metadata service."""
    global _metadata_service
    _metadata_service = metadata_service


def _get_metadata_service():
    """Get the metadata service, raising 503 if not initialized."""
    if _metadata_service is None:
        raise HTTPException(503, "Metadata service not initialized")
    return _metadata_service


# ============================================================================
# REQUEST / RESPONSE MODELS
# ============================================================================

class MetadataCreateRequest(BaseModel):
    """Request body for creating layer metadata."""
    asset_id: str
    data_type: str
    display_name: str
    description: Optional[str] = None
    attribution: Optional[str] = None
    keywords: List[str] = Field(default_factory=list)
    style: Dict[str, Any] = Field(default_factory=dict)
    legend: List[Dict[str, Any]] = Field(default_factory=list)
    field_metadata: Dict[str, Dict[str, Any]] = Field(default_factory=dict)
    min_zoom: Optional[int] = None
    max_zoom: Optional[int] = None
    default_visible: bool = True
    sort_order: int = 0
    thumbnail_url: Optional[str] = None


class MetadataUpdateRequest(BaseModel):
    """Request body for updating layer metadata. version required for optimistic locking."""
    version: int
    display_name: Optional[str] = None
    description: Optional[str] = None
    attribution: Optional[str] = None
    keywords: Optional[List[str]] = None
    style: Optional[Dict[str, Any]] = None
    legend: Optional[List[Dict[str, Any]]] = None
    field_metadata: Optional[Dict[str, Dict[str, Any]]] = None
    min_zoom: Optional[int] = None
    max_zoom: Optional[int] = None
    default_visible: Optional[bool] = None
    sort_order: Optional[int] = None
    thumbnail_url: Optional[str] = None


# ============================================================================
# CREATE
# ============================================================================

@router.post("/", status_code=201)
async def create_metadata(request: MetadataCreateRequest):
    """
    Create layer metadata for an asset.

    Called by gateway proxy (POST /api/admin/layer-metadata).
    """
    svc = _get_metadata_service()

    try:
        metadata = await svc.create_metadata(
            asset_id=request.asset_id,
            data_type=request.data_type,
            display_name=request.display_name,
            description=request.description,
            attribution=request.attribution,
            keywords=request.keywords,
            style=request.style,
            legend=request.legend,
            field_metadata=request.field_metadata,
            min_zoom=request.min_zoom,
            max_zoom=request.max_zoom,
            default_visible=request.default_visible,
            sort_order=request.sort_order,
            thumbnail_url=request.thumbnail_url,
        )
    except ValueError as e:
        error_msg = str(e)
        if "not found" in error_msg:
            raise HTTPException(404, error_msg)
        raise HTTPException(400, error_msg)

    return metadata.model_dump(mode="json")


# ============================================================================
# READ
# ============================================================================

@router.get("/")
async def list_metadata(data_type: Optional[str] = None, limit: int = 100):
    """List layer metadata entries with optional data_type filter."""
    svc = _get_metadata_service()

    entries = await svc.list_metadata(data_type=data_type, limit=limit)
    return [m.model_dump(mode="json") for m in entries]


@router.get("/{asset_id}")
async def get_metadata(asset_id: str):
    """Get layer metadata for a specific asset."""
    svc = _get_metadata_service()

    metadata = await svc.get_metadata(asset_id)
    if metadata is None:
        raise HTTPException(404, f"Metadata not found for asset {asset_id}")

    return metadata.model_dump(mode="json")


# ============================================================================
# UPDATE
# ============================================================================

@router.put("/{asset_id}")
async def update_metadata(asset_id: str, request: MetadataUpdateRequest):
    """
    Update layer metadata with optimistic locking.

    Called by gateway proxy (PUT /api/admin/layer-metadata/{asset_id}).
    """
    svc = _get_metadata_service()

    # Collect non-None updates (exclude version which is used for locking)
    updates = {
        k: v for k, v in request.model_dump(exclude={"version"}).items()
        if v is not None
    }

    try:
        metadata = await svc.update_metadata(
            asset_id=asset_id,
            expected_version=request.version,
            **updates,
        )
    except ValueError as e:
        error_msg = str(e)
        if "not found" in error_msg:
            raise HTTPException(404, error_msg)
        if "conflict" in error_msg.lower():
            raise HTTPException(409, error_msg)
        raise HTTPException(400, error_msg)

    return metadata.model_dump(mode="json")


# ============================================================================
# DELETE
# ============================================================================

@router.delete("/{asset_id}")
async def delete_metadata(asset_id: str):
    """
    Delete layer metadata (hard delete).

    Called by gateway proxy (DELETE /api/admin/layer-metadata/{asset_id}).
    """
    svc = _get_metadata_service()

    try:
        await svc.delete_metadata(asset_id)
    except ValueError as e:
        error_msg = str(e)
        if "not found" in error_msg:
            raise HTTPException(404, error_msg)
        raise HTTPException(400, error_msg)

    return {"deleted": True, "asset_id": asset_id}
