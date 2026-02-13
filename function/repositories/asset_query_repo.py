# ============================================================================
# CLAUDE CONTEXT - ASSET QUERY REPOSITORY (GATEWAY)
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Gateway - Read-only queries for assets, versions, platforms
# PURPOSE: Database access for asset query API endpoints
# LAST_REVIEWED: 12 FEB 2026
# ============================================================================
"""
Asset Query Repository (Gateway)

Read-only repository for geospatial asset queries.
The gateway NEVER writes to the database — all mutations go through
the orchestrator's domain HTTP API.

Uses dict_row (from base class).
"""

import logging
from typing import Any, Dict, List, Optional

from function.repositories.base import FunctionRepository

logger = logging.getLogger(__name__)


class AssetQueryRepository(FunctionRepository):
    """Read-only repository for asset-related queries."""

    TABLE_PLATFORMS = "dagapp.platforms"
    TABLE_ASSETS = "dagapp.geospatial_assets"
    TABLE_VERSIONS = "dagapp.asset_versions"

    # ================================================================
    # PLATFORMS
    # ================================================================

    def get_platform(self, platform_id: str) -> Optional[Dict[str, Any]]:
        """Get a platform by ID."""
        return self.execute_one(
            f"SELECT * FROM {self.TABLE_PLATFORMS} WHERE platform_id = %s",
            (platform_id,),
        )

    def list_active_platforms(self) -> List[Dict[str, Any]]:
        """List all active platforms."""
        return self.execute_many(
            f"SELECT * FROM {self.TABLE_PLATFORMS} "
            f"WHERE is_active = true ORDER BY display_name",
        )

    # ================================================================
    # ASSETS
    # ================================================================

    def get_asset(self, asset_id: str) -> Optional[Dict[str, Any]]:
        """Get an asset by ID."""
        return self.execute_one(
            f"SELECT * FROM {self.TABLE_ASSETS} WHERE asset_id = %s",
            (asset_id,),
        )

    def list_assets(
        self,
        platform_id: Optional[str] = None,
        include_deleted: bool = False,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """List assets with optional filters."""
        conditions = []
        params: List[Any] = []

        if platform_id:
            conditions.append("platform_id = %s")
            params.append(platform_id)

        if not include_deleted:
            conditions.append("deleted_at IS NULL")

        where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""

        query = f"""
            SELECT * FROM {self.TABLE_ASSETS}
            {where_clause}
            ORDER BY created_at DESC
            LIMIT %s OFFSET %s
        """
        params.extend([limit, offset])
        return self.execute_many(query, tuple(params))

    def count_assets(
        self,
        platform_id: Optional[str] = None,
        include_deleted: bool = False,
    ) -> int:
        """Count assets with optional filters."""
        conditions = []
        params: List[Any] = []

        if platform_id:
            conditions.append("platform_id = %s")
            params.append(platform_id)

        if not include_deleted:
            conditions.append("deleted_at IS NULL")

        where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""

        return self.execute_count(
            f"SELECT COUNT(*) AS count FROM {self.TABLE_ASSETS} {where_clause}",
            tuple(params),
        )

    # ================================================================
    # VERSIONS
    # ================================================================

    def get_version(
        self, asset_id: str, version_ordinal: int
    ) -> Optional[Dict[str, Any]]:
        """Get a specific version by composite PK."""
        return self.execute_one(
            f"SELECT * FROM {self.TABLE_VERSIONS} "
            f"WHERE asset_id = %s AND version_ordinal = %s",
            (asset_id, version_ordinal),
        )

    def list_versions(
        self, asset_id: str, limit: int = 50
    ) -> List[Dict[str, Any]]:
        """List all versions for an asset, newest first."""
        return self.execute_many(
            f"SELECT * FROM {self.TABLE_VERSIONS} "
            f"WHERE asset_id = %s ORDER BY version_ordinal DESC LIMIT %s",
            (asset_id, limit),
        )

    def get_latest_approved_version(
        self, asset_id: str
    ) -> Optional[Dict[str, Any]]:
        """Get the highest-ordinal approved version."""
        return self.execute_one(
            f"SELECT * FROM {self.TABLE_VERSIONS} "
            f"WHERE asset_id = %s AND approval_state = 'approved' "
            f"ORDER BY version_ordinal DESC LIMIT 1",
            (asset_id,),
        )

    def has_unresolved_versions(self, asset_id: str) -> bool:
        """Check if any version is still pending_review."""
        count = self.execute_count(
            f"SELECT COUNT(*) AS count FROM {self.TABLE_VERSIONS} "
            f"WHERE asset_id = %s AND approval_state = 'pending_review'",
            (asset_id,),
        )
        return count > 0

    def get_next_ordinal(self, asset_id: str) -> int:
        """Get the next version_ordinal for an asset."""
        result = self.execute_scalar(
            f"SELECT COALESCE(MAX(version_ordinal), -1) + 1 "
            f"FROM {self.TABLE_VERSIONS} WHERE asset_id = %s",
            (asset_id,),
        )
        return int(result) if result is not None else 0


__all__ = ["AssetQueryRepository"]
