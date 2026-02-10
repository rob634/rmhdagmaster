# ============================================================================
# CLAUDE CONTEXT - GEOSPATIAL ASSET MODEL
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Domain model - Aggregate root for geospatial data
# PURPOSE: Track geospatial assets across B2B platforms with clearance state
# LAST_REVIEWED: 09 FEB 2026
# ============================================================================
"""
GeospatialAsset Model

Aggregate root for geospatial data assets. Identity is deterministic:
    asset_id = SHA256(platform_id + sorted(platform_refs))[:32]

Version is NOT part of asset_id — one asset manages multiple AssetVersions.
Clearance (uncleared/ouo/public) is asset-level: all versions share one clearance.

Lifecycle:
    1. B2B app submits data → GeospatialAsset created (or found via asset_id)
    2. ETL job processes data → AssetVersion tracks progress
    3. Reviewer approves → version becomes serveable
    4. Clearance set to public → ADF export job triggered
"""

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, ClassVar, Dict, List, Optional

from pydantic import BaseModel, Field, computed_field

from core.contracts import ClearanceState


class GeospatialAsset(BaseModel):
    """
    Aggregate root for geospatial data assets.

    Identity: asset_id = SHA256(platform_id + sorted(platform_refs))[:32]
    Clearance: asset-level (all versions share one clearance)
    Soft delete: deleted_at/deleted_by (reversible)

    Maps to: dagapp.geospatial_assets
    """

    # SQL DDL METADATA
    __sql_table__: ClassVar[str] = "geospatial_assets"
    __sql_schema__: ClassVar[str] = "dagapp"
    __sql_primary_key__: ClassVar[List[str]] = ["asset_id"]
    __sql_foreign_keys__: ClassVar[Dict[str, str]] = {
        "platform_id": "dagapp.platforms(platform_id)",
    }
    __sql_indexes__: ClassVar[List] = [
        ("idx_geospatial_assets_platform", ["platform_id"]),
        ("idx_geospatial_assets_clearance", ["clearance_state"]),
        ("idx_geospatial_assets_data_type", ["data_type"]),
        ("idx_geospatial_assets_created", ["created_at"]),
        ("idx_geospatial_assets_not_deleted", ["asset_id"], "deleted_at IS NULL"),
        {
            "name": "idx_geospatial_assets_platform_refs",
            "columns": ["platform_refs"],
            "type": "gin",
        },
    ]

    # Identity
    asset_id: str = Field(
        ..., max_length=32,
        description="SHA256(platform_id + sorted(platform_refs))[:32]",
    )
    platform_id: str = Field(
        ..., max_length=50,
        description="FK to platforms table",
    )
    platform_refs: Dict[str, Any] = Field(
        default_factory=dict,
        description="B2B identity references (JSONB). Does NOT include version.",
    )
    data_type: str = Field(
        ..., max_length=20,
        description="'vector' or 'raster'",
    )

    # Clearance — asset-level, all versions share
    clearance_state: ClearanceState = Field(
        default=ClearanceState.UNCLEARED,
        description="Security classification: uncleared → ouo → public",
    )
    cleared_at: Optional[datetime] = Field(
        default=None, description="When clearance first changed from UNCLEARED",
    )
    cleared_by: Optional[str] = Field(
        default=None, max_length=64, description="Who set clearance",
    )
    made_public_at: Optional[datetime] = Field(
        default=None, description="When changed to PUBLIC",
    )
    made_public_by: Optional[str] = Field(
        default=None, max_length=64, description="Who made it public",
    )

    # Soft delete
    deleted_at: Optional[datetime] = Field(default=None)
    deleted_by: Optional[str] = Field(default=None, max_length=64)

    # Timestamps
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    # Optimistic locking
    version: int = Field(default=1, ge=1, description="Row version for optimistic locking")

    model_config = {"frozen": False}

    # ----------------------------------------------------------------
    # Computed fields
    # ----------------------------------------------------------------

    @computed_field
    @property
    def is_deleted(self) -> bool:
        """True if asset has been soft-deleted."""
        return self.deleted_at is not None

    @computed_field
    @property
    def is_public(self) -> bool:
        """True if asset has public clearance."""
        return self.clearance_state == ClearanceState.PUBLIC

    # ----------------------------------------------------------------
    # Static helpers
    # ----------------------------------------------------------------

    @staticmethod
    def compute_asset_id(platform_id: str, platform_refs: Dict[str, Any]) -> str:
        """
        Deterministic asset_id from platform identity.

        asset_id = SHA256(platform_id + canonical_json(platform_refs))[:32]

        Version is NOT included — multiple versions share one asset_id.
        Key order does not matter (sorted).
        """
        canonical = json.dumps(platform_refs, sort_keys=True, separators=(",", ":"))
        composite = f"{platform_id}|{canonical}"
        return hashlib.sha256(composite.encode()).hexdigest()[:32]

    # ----------------------------------------------------------------
    # State transitions
    # ----------------------------------------------------------------

    def mark_cleared(self, cleared_by: str) -> None:
        """Transition clearance: UNCLEARED → OUO."""
        if self.clearance_state != ClearanceState.UNCLEARED:
            raise ValueError(
                f"Cannot clear asset from state '{self.clearance_state.value}' "
                f"(must be '{ClearanceState.UNCLEARED.value}')"
            )
        self.clearance_state = ClearanceState.OUO
        self.cleared_at = datetime.now(timezone.utc)
        self.cleared_by = cleared_by
        self.updated_at = datetime.now(timezone.utc)

    def mark_public(self, made_public_by: str) -> None:
        """Transition clearance: OUO → PUBLIC. Triggers ADF export job."""
        if self.clearance_state != ClearanceState.OUO:
            raise ValueError(
                f"Cannot make public from state '{self.clearance_state.value}' "
                f"(must be '{ClearanceState.OUO.value}')"
            )
        self.clearance_state = ClearanceState.PUBLIC
        self.made_public_at = datetime.now(timezone.utc)
        self.made_public_by = made_public_by
        self.updated_at = datetime.now(timezone.utc)

    def soft_delete(self, deleted_by: str) -> None:
        """Mark asset as soft-deleted."""
        if self.deleted_at is not None:
            raise ValueError("Asset is already deleted")
        self.deleted_at = datetime.now(timezone.utc)
        self.deleted_by = deleted_by
        self.updated_at = datetime.now(timezone.utc)


__all__ = ["GeospatialAsset"]
