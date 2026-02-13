# ============================================================================
# CLAUDE CONTEXT - LAYER METADATA MODEL
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Domain model - Presentation metadata for geospatial layers
# PURPOSE: Source of truth for styles, legends, colormaps, field descriptions
# LAST_REVIEWED: 13 FEB 2026
# ============================================================================
"""
LayerMetadata Model

Per-asset presentation metadata (styles, legends, colormaps, field descriptions).
Decoupled from ETL processing — admins curate metadata independently.
Projected to B2C schemas (STAC + layer_catalog) in Phase 4E.

Lifecycle:
    1. Asset created via submit → no metadata yet
    2. Admin creates metadata (style, legend, field_metadata)
    3. Admin publishes → projected to STAC + layer_catalog (Phase 4E)
    4. B2C apps consume projected metadata
"""

from datetime import datetime, timezone
from typing import Any, ClassVar, Dict, List, Optional

from pydantic import BaseModel, Field, computed_field, field_validator


class LayerMetadata(BaseModel):
    """
    Presentation metadata for a geospatial layer.
    Source of truth in dagapp. Projected to B2C schemas in Phase 4E.
    Maps to: dagapp.layer_metadata
    """

    # SQL DDL METADATA
    __sql_table__: ClassVar[str] = "layer_metadata"
    __sql_schema__: ClassVar[str] = "dagapp"
    __sql_primary_key__: ClassVar[List[str]] = ["asset_id"]
    __sql_foreign_keys__: ClassVar[Dict[str, str]] = {
        "asset_id": "dagapp.geospatial_assets(asset_id)",
    }
    __sql_indexes__: ClassVar[List] = [
        ("idx_layer_metadata_data_type", ["data_type"]),
        ("idx_layer_metadata_sort", ["sort_order"]),
        ("idx_layer_metadata_published", ["published_at"], "published_at IS NOT NULL"),
    ]

    # Identity
    asset_id: str = Field(..., max_length=32, description="FK to geospatial_assets")
    data_type: str = Field(..., max_length=20, description="'vector' or 'raster'")

    # Display
    display_name: str = Field(..., max_length=256, description="Human-readable layer name")
    description: Optional[str] = Field(default=None, description="Markdown-capable description")
    attribution: Optional[str] = Field(default=None, max_length=500, description="Data source credit")
    keywords: List[str] = Field(default_factory=list, description="Searchable tags")

    # Style / Symbology (JSONB — shape varies by data_type)
    style: Dict[str, Any] = Field(default_factory=dict, description="Rendering parameters")
    #   Raster: {"colormap": "viridis", "rescale": [0, 255], "bidx": [1]}
    #   Vector: {"fill-color": "#3388ff", "fill-opacity": 0.5,
    #            "stroke-color": "#000", "stroke-width": 1}

    # Legend (JSONB — ordered list)
    legend: List[Dict[str, Any]] = Field(default_factory=list, description="Ordered legend entries")
    #   [{"label": "High Risk", "color": "#FF0000", "min": 80, "max": 100}]

    # Field Metadata — vectors only (JSONB — keyed by column name)
    field_metadata: Dict[str, Dict[str, Any]] = Field(
        default_factory=dict,
        description="Per-column metadata keyed by column name",
    )
    #   {"population": {"display_name": "Population", "unit": "people", "format": "integer"}}

    # Visibility
    min_zoom: Optional[int] = Field(default=None, ge=0, le=24)
    max_zoom: Optional[int] = Field(default=None, ge=0, le=24)
    default_visible: bool = Field(default=True)
    sort_order: int = Field(default=0, description="Layer ordering in UI")

    # Thumbnail
    thumbnail_url: Optional[str] = Field(default=None, max_length=500)

    # Lifecycle
    published_at: Optional[datetime] = Field(default=None, description="Last projected to B2C")

    # Timestamps
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    # Optimistic locking
    version: int = Field(default=1, ge=1)

    model_config = {"frozen": False}

    # ----------------------------------------------------------------
    # Validators
    # ----------------------------------------------------------------

    @field_validator("data_type")
    @classmethod
    def validate_data_type(cls, v: str) -> str:
        if v not in ("vector", "raster"):
            raise ValueError(f"data_type must be 'vector' or 'raster', got '{v}'")
        return v

    # ----------------------------------------------------------------
    # Computed fields
    # ----------------------------------------------------------------

    @computed_field
    @property
    def is_published(self) -> bool:
        """True if metadata has been projected to B2C."""
        return self.published_at is not None

    @computed_field
    @property
    def has_style(self) -> bool:
        """True if style parameters are defined."""
        return len(self.style) > 0

    @computed_field
    @property
    def has_legend(self) -> bool:
        """True if legend entries are defined."""
        return len(self.legend) > 0


__all__ = ["LayerMetadata"]
