# ============================================================================
# CLAUDE CONTEXT - PLATFORM MODEL
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Domain model - B2B platform registry
# PURPOSE: Track registered B2B applications and their identity ref schemas
# LAST_REVIEWED: 09 FEB 2026
# ============================================================================
"""
Platform Model

B2B application registry. Each platform defines which identity refs it
requires when submitting geospatial data. The Platform table validates
incoming submissions and governs asset_id generation.

Example:
    DDH platform requires ["dataset_id", "resource_id"].
    When DDH submits, platform_refs must contain both keys.
    asset_id = SHA256("ddh" + sorted(platform_refs))[:32]
"""

from datetime import datetime, timezone
from typing import Any, ClassVar, Dict, List, Optional

from pydantic import BaseModel, Field


class Platform(BaseModel):
    """
    B2B platform registry entry.

    Defines which identity reference fields a B2B app must provide
    when submitting geospatial data.

    Maps to: dagapp.platforms
    """

    # SQL DDL METADATA
    __sql_table__: ClassVar[str] = "platforms"
    __sql_schema__: ClassVar[str] = "dagapp"
    __sql_primary_key__: ClassVar[List[str]] = ["platform_id"]
    __sql_foreign_keys__: ClassVar[Dict[str, str]] = {}
    __sql_indexes__: ClassVar[List] = [
        ("idx_platforms_active", ["is_active"], "is_active = true"),
    ]

    # Identity
    platform_id: str = Field(
        ..., max_length=50,
        description="Unique platform identifier (lowercase, underscores)",
    )
    display_name: str = Field(
        ..., max_length=100,
        description="Human-readable platform name",
    )
    description: Optional[str] = Field(
        default=None, max_length=500,
        description="Platform description",
    )

    # Ref schema — defines what identity refs this platform requires
    required_refs: List[str] = Field(
        default_factory=list,
        description="Required identity reference fields for asset_id generation",
    )
    optional_refs: List[str] = Field(
        default_factory=list,
        description="Optional reference fields (not used in asset_id)",
    )

    # Status
    is_active: bool = Field(default=True, description="Whether platform can submit new data")

    # Timestamps
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    model_config = {"frozen": False}

    # ----------------------------------------------------------------
    # Validation
    # ----------------------------------------------------------------

    def validate_refs(self, platform_refs: Dict[str, Any]) -> List[str]:
        """
        Validate that all required_refs are present in platform_refs.

        Returns:
            List of missing ref names (empty if all present).
        """
        return [ref for ref in self.required_refs if ref not in platform_refs]


__all__ = ["Platform"]
