# ============================================================================
# CLAUDE CONTEXT - LAYER CATALOG MODEL
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Domain model - B2C projection target for layer metadata
# PURPOSE: Layer catalog entry keyed by table_name for B2C consumption
# LAST_REVIEWED: 13 FEB 2026
# ============================================================================
"""
LayerCatalog Model

Projection target for B2C consumption. Keyed by table_name (what B2C apps
know), not asset_id (dagapp internal). Written during metadata publish
(Phase 4E). Read by TiPG middleware for enriched collection responses.

Single-DB (today): lives in dagapp schema.
Three-DB (future): projected to geo schema in B2C databases.
"""

from datetime import datetime, timezone
from typing import Any, ClassVar, Dict, List, Optional

from pydantic import BaseModel, Field


class LayerCatalog(BaseModel):
    """
    Layer catalog entry for B2C consumption.
    Projected from dagapp.layer_metadata during publish.
    Keyed by table_name — B2C apps know tables, not dagapp internals.
    Maps to: dagapp.layer_catalog
    """

    # SQL DDL METADATA
    __sql_table__: ClassVar[str] = "layer_catalog"
    __sql_schema__: ClassVar[str] = "dagapp"
    __sql_primary_key__: ClassVar[List[str]] = ["table_name"]
    __sql_indexes__: ClassVar[List] = [
        ("idx_layer_catalog_data_type", ["data_type"]),
        ("idx_layer_catalog_source_asset", ["source_asset_id"], "source_asset_id IS NOT NULL"),
        ("idx_layer_catalog_sort", ["sort_order"]),
    ]

    # Identity — PK is table_name, NOT asset_id
    table_name: str = Field(..., max_length=128, description="PostGIS table name or STAC collection ID")
    data_type: str = Field(..., max_length=20, description="'vector' or 'raster'")

    # Display
    display_name: str = Field(..., max_length=256)
    description: Optional[str] = Field(default=None)
    attribution: Optional[str] = Field(default=None, max_length=500)
    keywords: List[str] = Field(default_factory=list)

    # Style / Legend / Field Metadata — same schema as LayerMetadata
    style: Dict[str, Any] = Field(default_factory=dict)
    legend: List[Dict[str, Any]] = Field(default_factory=list)
    field_metadata: Dict[str, Dict[str, Any]] = Field(default_factory=dict)

    # Visibility
    min_zoom: Optional[int] = Field(default=None, ge=0, le=24)
    max_zoom: Optional[int] = Field(default=None, ge=0, le=24)
    default_visible: bool = Field(default=True)
    sort_order: int = Field(default=0)

    # Thumbnail
    thumbnail_url: Optional[str] = Field(default=None, max_length=500)

    # Traceability (NOT an FK — may be different databases)
    source_asset_id: Optional[str] = Field(default=None, max_length=32)

    # Lifecycle
    published_at: Optional[datetime] = Field(default=None)
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    model_config = {"frozen": False}


__all__ = ["LayerCatalog"]
