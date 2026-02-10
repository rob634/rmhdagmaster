# ============================================================================
# GEOSPATIAL ASSET REPOSITORY
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Domain - GeospatialAsset CRUD operations
# PURPOSE: Database access for geospatial_assets table
# CREATED: 09 FEB 2026
# ============================================================================
"""
GeospatialAsset Repository

CRUD operations for the GeospatialAsset aggregate root.
All SQL uses psycopg sql.SQL composition for injection safety.
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Json
from psycopg_pool import AsyncConnectionPool

from core.models.geospatial_asset import GeospatialAsset
from core.contracts import ClearanceState
from .database import TABLE_GEOSPATIAL_ASSETS

logger = logging.getLogger(__name__)


class AssetRepository:
    """Repository for GeospatialAsset entities."""

    def __init__(self, pool: AsyncConnectionPool):
        self.pool = pool

    async def create(self, asset: GeospatialAsset) -> GeospatialAsset:
        """Create a new geospatial asset."""
        async with self.pool.connection() as conn:
            await conn.execute(
                sql.SQL("""
                    INSERT INTO {} (
                        asset_id, platform_id, platform_refs, data_type,
                        clearance_state, cleared_at, cleared_by,
                        made_public_at, made_public_by,
                        deleted_at, deleted_by,
                        created_at, updated_at, version
                    ) VALUES (
                        %(asset_id)s, %(platform_id)s, %(platform_refs)s, %(data_type)s,
                        %(clearance_state)s, %(cleared_at)s, %(cleared_by)s,
                        %(made_public_at)s, %(made_public_by)s,
                        %(deleted_at)s, %(deleted_by)s,
                        %(created_at)s, %(updated_at)s, %(version)s
                    )
                """).format(TABLE_GEOSPATIAL_ASSETS),
                {
                    "asset_id": asset.asset_id,
                    "platform_id": asset.platform_id,
                    "platform_refs": Json(asset.platform_refs),
                    "data_type": asset.data_type,
                    "clearance_state": asset.clearance_state.value,
                    "cleared_at": asset.cleared_at,
                    "cleared_by": asset.cleared_by,
                    "made_public_at": asset.made_public_at,
                    "made_public_by": asset.made_public_by,
                    "deleted_at": asset.deleted_at,
                    "deleted_by": asset.deleted_by,
                    "created_at": asset.created_at,
                    "updated_at": asset.updated_at,
                    "version": asset.version,
                },
            )
            logger.info(f"Created asset {asset.asset_id} (platform={asset.platform_id})")
            return asset

    async def get(self, asset_id: str) -> Optional[GeospatialAsset]:
        """Get an asset by ID."""
        async with self.pool.connection() as conn:
            conn.row_factory = dict_row
            result = await conn.execute(
                sql.SQL("SELECT * FROM {} WHERE asset_id = %s").format(
                    TABLE_GEOSPATIAL_ASSETS
                ),
                (asset_id,),
            )
            row = await result.fetchone()
            return self._row_to_model(row) if row else None

    async def get_or_create(
        self, asset: GeospatialAsset
    ) -> Tuple[GeospatialAsset, bool]:
        """
        Get existing asset or create new one (idempotent by asset_id).

        Returns:
            Tuple of (asset, created). created=True if new, False if existing.
        """
        async with self.pool.connection() as conn:
            conn.row_factory = dict_row

            # Try insert, skip on conflict
            result = await conn.execute(
                sql.SQL("""
                    INSERT INTO {} (
                        asset_id, platform_id, platform_refs, data_type,
                        clearance_state, created_at, updated_at, version
                    ) VALUES (
                        %(asset_id)s, %(platform_id)s, %(platform_refs)s, %(data_type)s,
                        %(clearance_state)s, %(created_at)s, %(updated_at)s, %(version)s
                    )
                    ON CONFLICT (asset_id) DO NOTHING
                    RETURNING *
                """).format(TABLE_GEOSPATIAL_ASSETS),
                {
                    "asset_id": asset.asset_id,
                    "platform_id": asset.platform_id,
                    "platform_refs": Json(asset.platform_refs),
                    "data_type": asset.data_type,
                    "clearance_state": asset.clearance_state.value,
                    "created_at": asset.created_at,
                    "updated_at": asset.updated_at,
                    "version": asset.version,
                },
            )
            row = await result.fetchone()

            if row is not None:
                # Newly created
                return self._row_to_model(row), True

            # Already exists — fetch it
            result = await conn.execute(
                sql.SQL("SELECT * FROM {} WHERE asset_id = %s").format(
                    TABLE_GEOSPATIAL_ASSETS
                ),
                (asset.asset_id,),
            )
            row = await result.fetchone()
            return self._row_to_model(row), False

    async def update(self, asset: GeospatialAsset) -> bool:
        """
        Update an asset with optimistic locking.

        Returns:
            True if update succeeded, False if version conflict.
        """
        asset.updated_at = datetime.now(timezone.utc)

        async with self.pool.connection() as conn:
            result = await conn.execute(
                sql.SQL("""
                    UPDATE {} SET
                        clearance_state = %(clearance_state)s,
                        cleared_at = %(cleared_at)s,
                        cleared_by = %(cleared_by)s,
                        made_public_at = %(made_public_at)s,
                        made_public_by = %(made_public_by)s,
                        deleted_at = %(deleted_at)s,
                        deleted_by = %(deleted_by)s,
                        updated_at = %(updated_at)s,
                        version = version + 1
                    WHERE asset_id = %(asset_id)s
                      AND version = %(version)s
                """).format(TABLE_GEOSPATIAL_ASSETS),
                {
                    "asset_id": asset.asset_id,
                    "clearance_state": asset.clearance_state.value,
                    "cleared_at": asset.cleared_at,
                    "cleared_by": asset.cleared_by,
                    "made_public_at": asset.made_public_at,
                    "made_public_by": asset.made_public_by,
                    "deleted_at": asset.deleted_at,
                    "deleted_by": asset.deleted_by,
                    "updated_at": asset.updated_at,
                    "version": asset.version,
                },
            )

            if result.rowcount == 0:
                logger.warning(
                    f"Version conflict updating asset {asset.asset_id} "
                    f"(expected version {asset.version})"
                )
                return False

            asset.version += 1
            return True

    async def list_by_platform(
        self,
        platform_id: str,
        include_deleted: bool = False,
        limit: int = 100,
    ) -> List[GeospatialAsset]:
        """List assets for a platform, optionally excluding deleted."""
        async with self.pool.connection() as conn:
            conn.row_factory = dict_row

            if include_deleted:
                query = sql.SQL(
                    "SELECT * FROM {} WHERE platform_id = %s "
                    "ORDER BY created_at DESC LIMIT %s"
                ).format(TABLE_GEOSPATIAL_ASSETS)
            else:
                query = sql.SQL(
                    "SELECT * FROM {} WHERE platform_id = %s AND deleted_at IS NULL "
                    "ORDER BY created_at DESC LIMIT %s"
                ).format(TABLE_GEOSPATIAL_ASSETS)

            result = await conn.execute(query, (platform_id, limit))
            rows = await result.fetchall()
            return [self._row_to_model(row) for row in rows]

    async def soft_delete(
        self, asset_id: str, deleted_by: str, expected_version: int
    ) -> bool:
        """
        Soft-delete an asset with optimistic locking.

        Returns:
            True if deleted, False if version conflict or already deleted.
        """
        now = datetime.now(timezone.utc)
        async with self.pool.connection() as conn:
            result = await conn.execute(
                sql.SQL("""
                    UPDATE {} SET
                        deleted_at = %s,
                        deleted_by = %s,
                        updated_at = %s,
                        version = version + 1
                    WHERE asset_id = %s
                      AND version = %s
                      AND deleted_at IS NULL
                """).format(TABLE_GEOSPATIAL_ASSETS),
                (now, deleted_by, now, asset_id, expected_version),
            )
            return result.rowcount > 0

    def _row_to_model(self, row: Dict[str, Any]) -> GeospatialAsset:
        """Convert a database row to a GeospatialAsset instance."""
        return GeospatialAsset(
            asset_id=row["asset_id"],
            platform_id=row["platform_id"],
            platform_refs=row.get("platform_refs") or {},
            data_type=row["data_type"],
            clearance_state=ClearanceState(row["clearance_state"]),
            cleared_at=row.get("cleared_at"),
            cleared_by=row.get("cleared_by"),
            made_public_at=row.get("made_public_at"),
            made_public_by=row.get("made_public_by"),
            deleted_at=row.get("deleted_at"),
            deleted_by=row.get("deleted_by"),
            created_at=row["created_at"],
            updated_at=row.get("updated_at") or row["created_at"],
            version=row.get("version", 1),
        )
