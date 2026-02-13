# ============================================================================
# CLAUDE CONTEXT - ASSET QUERY REPOSITORY (GATEWAY)
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Gateway - Sync CRUD for assets, versions, platforms
# PURPOSE: Database access for asset business API endpoints
# LAST_REVIEWED: 11 FEB 2026
# ============================================================================
"""
Asset Query Repository (Gateway)

Sync repository for geospatial asset operations.
Extends FunctionRepository with read + write methods.

Uses dict_row (from base class) and psycopg.types.json.Json for JSONB.
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from psycopg.types.json import Json

from function.repositories.base import FunctionRepository

logger = logging.getLogger(__name__)


class AssetQueryRepository(FunctionRepository):
    """Sync repository for asset-related queries and writes."""

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

    def get_or_create_asset(
        self,
        asset_id: str,
        platform_id: str,
        platform_refs: Dict[str, Any],
        data_type: str,
    ) -> tuple:
        """
        Get existing asset or create new one (idempotent by asset_id).

        Returns:
            (asset_row, created: bool)
        """
        now = datetime.now(timezone.utc)

        # Try insert, skip on conflict
        row = self.execute_write_returning(
            f"""
            INSERT INTO {self.TABLE_ASSETS} (
                asset_id, platform_id, platform_refs, data_type,
                clearance_state, created_at, updated_at, version
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (asset_id) DO NOTHING
            RETURNING *
            """,
            (asset_id, platform_id, Json(platform_refs), data_type,
             "uncleared", now, now, 1),
        )

        if row is not None:
            return row, True

        # Already exists — fetch it
        existing = self.execute_one(
            f"SELECT * FROM {self.TABLE_ASSETS} WHERE asset_id = %s",
            (asset_id,),
        )
        return existing, False

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

    def update_asset_clearance(
        self,
        asset_id: str,
        clearance_state: str,
        actor: str,
        expected_version: int,
        cleared_at: Optional[datetime] = None,
        made_public_at: Optional[datetime] = None,
    ) -> bool:
        """Update asset clearance with optimistic locking. Returns True if updated."""
        now = datetime.now(timezone.utc)
        rowcount = self.execute_write(
            f"""
            UPDATE {self.TABLE_ASSETS} SET
                clearance_state = %s,
                cleared_at = COALESCE(%s, cleared_at),
                cleared_by = CASE WHEN %s IS NOT NULL THEN %s ELSE cleared_by END,
                made_public_at = COALESCE(%s, made_public_at),
                made_public_by = CASE WHEN %s IS NOT NULL THEN %s ELSE made_public_by END,
                updated_at = %s,
                version = version + 1
            WHERE asset_id = %s AND version = %s
            """,
            (clearance_state,
             cleared_at, cleared_at, actor,
             made_public_at, made_public_at, actor,
             now, asset_id, expected_version),
        )
        return rowcount > 0

    def soft_delete_asset(
        self,
        asset_id: str,
        actor: str,
        expected_version: int,
    ) -> bool:
        """Soft-delete an asset. Returns True if updated."""
        now = datetime.now(timezone.utc)
        rowcount = self.execute_write(
            f"""
            UPDATE {self.TABLE_ASSETS} SET
                deleted_at = %s, deleted_by = %s,
                updated_at = %s, version = version + 1
            WHERE asset_id = %s AND version = %s AND deleted_at IS NULL
            """,
            (now, actor, now, asset_id, expected_version),
        )
        return rowcount > 0

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

    def create_version(
        self,
        asset_id: str,
        version_ordinal: int,
        version_label: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Create a new asset version (PENDING_REVIEW, QUEUED)."""
        now = datetime.now(timezone.utc)
        row = self.execute_write_returning(
            f"""
            INSERT INTO {self.TABLE_VERSIONS} (
                asset_id, version_ordinal, version_label,
                approval_state, processing_state,
                revision, created_at, updated_at, version
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING *
            """,
            (asset_id, version_ordinal, version_label,
             "pending_review", "queued",
             0, now, now, 1),
        )
        return row

    def update_version_processing(
        self,
        asset_id: str,
        version_ordinal: int,
        processing_state: str,
        current_job_id: Optional[str] = None,
        processing_started_at: Optional[datetime] = None,
    ) -> bool:
        """Update version processing state. Returns True if updated."""
        now = datetime.now(timezone.utc)
        rowcount = self.execute_write(
            f"""
            UPDATE {self.TABLE_VERSIONS} SET
                processing_state = %s,
                current_job_id = COALESCE(%s, current_job_id),
                processing_started_at = COALESCE(%s, processing_started_at),
                updated_at = %s,
                version = version + 1
            WHERE asset_id = %s AND version_ordinal = %s
            """,
            (processing_state, current_job_id, processing_started_at,
             now, asset_id, version_ordinal),
        )
        return rowcount > 0

    def update_version_approval(
        self,
        asset_id: str,
        version_ordinal: int,
        approval_state: str,
        reviewer: str,
        rejection_reason: Optional[str] = None,
        expected_version: int = 1,
    ) -> bool:
        """Update version approval with optimistic locking. Returns True if updated."""
        now = datetime.now(timezone.utc)
        rowcount = self.execute_write(
            f"""
            UPDATE {self.TABLE_VERSIONS} SET
                approval_state = %s,
                reviewer = %s,
                reviewed_at = %s,
                rejection_reason = %s,
                updated_at = %s,
                version = version + 1
            WHERE asset_id = %s AND version_ordinal = %s AND version = %s
            """,
            (approval_state, reviewer, now, rejection_reason,
             now, asset_id, version_ordinal, expected_version),
        )
        return rowcount > 0


__all__ = ["AssetQueryRepository"]
