# ============================================================================
# LAYER METADATA REPOSITORY
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Domain - LayerMetadata CRUD operations
# PURPOSE: Database access for layer_metadata table
# CREATED: 13 FEB 2026
# ============================================================================
"""
LayerMetadata Repository

CRUD operations for the LayerMetadata entity (presentation metadata).
Hard delete (metadata is re-creatable, not lifecycle-managed).
Optimistic locking via version column.
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Json
from psycopg_pool import AsyncConnectionPool

from core.models.layer_metadata import LayerMetadata
from .database import TABLE_LAYER_METADATA

logger = logging.getLogger(__name__)


class LayerMetadataRepository:
    """Repository for LayerMetadata entities."""

    def __init__(self, pool: AsyncConnectionPool):
        self.pool = pool

    async def create(self, metadata: LayerMetadata) -> LayerMetadata:
        """Create a new layer metadata entry."""
        async with self.pool.connection() as conn:
            await conn.execute(
                sql.SQL("""
                    INSERT INTO {} (
                        asset_id, data_type, display_name, description,
                        attribution, keywords, style, legend, field_metadata,
                        min_zoom, max_zoom, default_visible, sort_order,
                        thumbnail_url, published_at,
                        created_at, updated_at, version
                    ) VALUES (
                        %(asset_id)s, %(data_type)s, %(display_name)s, %(description)s,
                        %(attribution)s, %(keywords)s, %(style)s, %(legend)s, %(field_metadata)s,
                        %(min_zoom)s, %(max_zoom)s, %(default_visible)s, %(sort_order)s,
                        %(thumbnail_url)s, %(published_at)s,
                        %(created_at)s, %(updated_at)s, %(version)s
                    )
                """).format(TABLE_LAYER_METADATA),
                {
                    "asset_id": metadata.asset_id,
                    "data_type": metadata.data_type,
                    "display_name": metadata.display_name,
                    "description": metadata.description,
                    "attribution": metadata.attribution,
                    "keywords": Json(metadata.keywords),
                    "style": Json(metadata.style),
                    "legend": Json(metadata.legend),
                    "field_metadata": Json(metadata.field_metadata),
                    "min_zoom": metadata.min_zoom,
                    "max_zoom": metadata.max_zoom,
                    "default_visible": metadata.default_visible,
                    "sort_order": metadata.sort_order,
                    "thumbnail_url": metadata.thumbnail_url,
                    "published_at": metadata.published_at,
                    "created_at": metadata.created_at,
                    "updated_at": metadata.updated_at,
                    "version": metadata.version,
                },
            )
            logger.info(f"Created layer metadata for asset {metadata.asset_id}")
            return metadata

    async def get(self, asset_id: str) -> Optional[LayerMetadata]:
        """Get layer metadata by asset_id."""
        async with self.pool.connection() as conn:
            conn.row_factory = dict_row
            result = await conn.execute(
                sql.SQL("SELECT * FROM {} WHERE asset_id = %s").format(
                    TABLE_LAYER_METADATA
                ),
                (asset_id,),
            )
            row = await result.fetchone()
            return self._row_to_model(row) if row else None

    async def update(self, metadata: LayerMetadata) -> bool:
        """
        Update layer metadata with optimistic locking.

        Returns:
            True if update succeeded, False if version conflict.
        """
        metadata.updated_at = datetime.now(timezone.utc)

        async with self.pool.connection() as conn:
            result = await conn.execute(
                sql.SQL("""
                    UPDATE {} SET
                        data_type = %(data_type)s,
                        display_name = %(display_name)s,
                        description = %(description)s,
                        attribution = %(attribution)s,
                        keywords = %(keywords)s,
                        style = %(style)s,
                        legend = %(legend)s,
                        field_metadata = %(field_metadata)s,
                        min_zoom = %(min_zoom)s,
                        max_zoom = %(max_zoom)s,
                        default_visible = %(default_visible)s,
                        sort_order = %(sort_order)s,
                        thumbnail_url = %(thumbnail_url)s,
                        published_at = %(published_at)s,
                        updated_at = %(updated_at)s,
                        version = version + 1
                    WHERE asset_id = %(asset_id)s
                      AND version = %(version)s
                """).format(TABLE_LAYER_METADATA),
                {
                    "asset_id": metadata.asset_id,
                    "data_type": metadata.data_type,
                    "display_name": metadata.display_name,
                    "description": metadata.description,
                    "attribution": metadata.attribution,
                    "keywords": Json(metadata.keywords),
                    "style": Json(metadata.style),
                    "legend": Json(metadata.legend),
                    "field_metadata": Json(metadata.field_metadata),
                    "min_zoom": metadata.min_zoom,
                    "max_zoom": metadata.max_zoom,
                    "default_visible": metadata.default_visible,
                    "sort_order": metadata.sort_order,
                    "thumbnail_url": metadata.thumbnail_url,
                    "published_at": metadata.published_at,
                    "updated_at": metadata.updated_at,
                    "version": metadata.version,
                },
            )

            if result.rowcount == 0:
                logger.warning(
                    f"Version conflict updating layer metadata for asset {metadata.asset_id} "
                    f"(expected version {metadata.version})"
                )
                return False

            metadata.version += 1
            return True

    async def delete(self, asset_id: str) -> bool:
        """
        Hard delete layer metadata. Metadata is re-creatable.

        Returns:
            True if deleted, False if not found.
        """
        async with self.pool.connection() as conn:
            result = await conn.execute(
                sql.SQL("DELETE FROM {} WHERE asset_id = %s").format(
                    TABLE_LAYER_METADATA
                ),
                (asset_id,),
            )
            if result.rowcount > 0:
                logger.info(f"Deleted layer metadata for asset {asset_id}")
            return result.rowcount > 0

    async def list_all(self, limit: int = 100) -> List[LayerMetadata]:
        """List all layer metadata entries ordered by sort_order, display_name."""
        async with self.pool.connection() as conn:
            conn.row_factory = dict_row
            result = await conn.execute(
                sql.SQL(
                    "SELECT * FROM {} ORDER BY sort_order, display_name LIMIT %s"
                ).format(TABLE_LAYER_METADATA),
                (limit,),
            )
            rows = await result.fetchall()
            return [self._row_to_model(row) for row in rows]

    async def list_by_data_type(
        self, data_type: str, limit: int = 100
    ) -> List[LayerMetadata]:
        """List layer metadata entries filtered by data_type."""
        async with self.pool.connection() as conn:
            conn.row_factory = dict_row
            result = await conn.execute(
                sql.SQL(
                    "SELECT * FROM {} WHERE data_type = %s "
                    "ORDER BY sort_order, display_name LIMIT %s"
                ).format(TABLE_LAYER_METADATA),
                (data_type, limit),
            )
            rows = await result.fetchall()
            return [self._row_to_model(row) for row in rows]

    def _row_to_model(self, row: Dict[str, Any]) -> LayerMetadata:
        """Convert a database row to a LayerMetadata instance."""
        return LayerMetadata(
            asset_id=row["asset_id"],
            data_type=row["data_type"],
            display_name=row["display_name"],
            description=row.get("description"),
            attribution=row.get("attribution"),
            keywords=row.get("keywords") or [],
            style=row.get("style") or {},
            legend=row.get("legend") or [],
            field_metadata=row.get("field_metadata") or {},
            min_zoom=row.get("min_zoom"),
            max_zoom=row.get("max_zoom"),
            default_visible=row.get("default_visible", True),
            sort_order=row.get("sort_order", 0),
            thumbnail_url=row.get("thumbnail_url"),
            published_at=row.get("published_at"),
            created_at=row["created_at"],
            updated_at=row.get("updated_at") or row["created_at"],
            version=row.get("version", 1),
        )
