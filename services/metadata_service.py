# ============================================================================
# CLAUDE CONTEXT - METADATA SERVICE
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Domain service - Business rules for layer metadata
# PURPOSE: Coordinate metadata CRUD with asset validation
# LAST_REVIEWED: 13 FEB 2026
# ============================================================================
"""
MetadataService

Business logic for layer metadata CRUD. Validates that assets exist
before creating metadata, enforces zoom constraints, and manages
optimistic locking.

Pattern: Constructor injection of AsyncConnectionPool, repos instantiated
in __init__, async methods, optional EventService for observability.
"""

from typing import Any, Dict, List, Optional

from psycopg_pool import AsyncConnectionPool

from core.logging import get_logger
from core.models.layer_metadata import LayerMetadata
from repositories import AssetRepository
from repositories.metadata_repo import LayerMetadataRepository

logger = get_logger(__name__)


class MetadataService:
    """Business rules for layer metadata lifecycle."""

    def __init__(
        self,
        pool: AsyncConnectionPool,
        event_service: "EventService" = None,
    ):
        self.pool = pool
        self.metadata_repo = LayerMetadataRepository(pool)
        self.asset_repo = AssetRepository(pool)
        self._event_service = event_service

    async def create_metadata(
        self,
        asset_id: str,
        data_type: str,
        display_name: str,
        **kwargs: Any,
    ) -> LayerMetadata:
        """
        Create layer metadata for an asset.

        Validates:
        - Asset exists
        - min_zoom <= max_zoom if both provided

        Raises:
            ValueError: Asset not found, invalid data_type, or zoom constraint violated.
        """
        # Validate asset exists
        asset = await self.asset_repo.get(asset_id)
        if asset is None:
            raise ValueError(f"Asset {asset_id} not found")

        # Validate zoom constraints
        min_zoom = kwargs.get("min_zoom")
        max_zoom = kwargs.get("max_zoom")
        if min_zoom is not None and max_zoom is not None and min_zoom > max_zoom:
            raise ValueError(
                f"min_zoom ({min_zoom}) cannot be greater than max_zoom ({max_zoom})"
            )

        metadata = LayerMetadata(
            asset_id=asset_id,
            data_type=data_type,
            display_name=display_name,
            **kwargs,
        )

        await self.metadata_repo.create(metadata)
        logger.info(f"Created metadata for asset {asset_id}: {display_name}")
        return metadata

    async def get_metadata(self, asset_id: str) -> Optional[LayerMetadata]:
        """Get layer metadata by asset_id."""
        return await self.metadata_repo.get(asset_id)

    async def update_metadata(
        self,
        asset_id: str,
        expected_version: int,
        **updates: Any,
    ) -> LayerMetadata:
        """
        Update layer metadata with merge semantics and optimistic locking.

        Only supplied fields are updated. The `version` field is required
        for optimistic locking (409 on conflict).

        Raises:
            ValueError: Metadata not found, version conflict, or zoom constraint violated.
        """
        metadata = await self.metadata_repo.get(asset_id)
        if metadata is None:
            raise ValueError(f"Metadata not found for asset {asset_id}")

        if metadata.version != expected_version:
            raise ValueError(
                f"Version conflict for asset {asset_id}: "
                f"expected {expected_version}, actual {metadata.version}"
            )

        # Merge updates into existing metadata
        for field, value in updates.items():
            if hasattr(metadata, field) and field not in ("asset_id", "version", "created_at"):
                setattr(metadata, field, value)

        # Validate zoom constraints after merge
        if (
            metadata.min_zoom is not None
            and metadata.max_zoom is not None
            and metadata.min_zoom > metadata.max_zoom
        ):
            raise ValueError(
                f"min_zoom ({metadata.min_zoom}) cannot be greater than max_zoom ({metadata.max_zoom})"
            )

        updated = await self.metadata_repo.update(metadata)
        if not updated:
            raise ValueError(f"Version conflict updating metadata for asset {asset_id}")

        logger.info(f"Updated metadata for asset {asset_id}")
        return metadata

    async def delete_metadata(self, asset_id: str) -> bool:
        """
        Delete layer metadata (hard delete).

        Raises:
            ValueError: Metadata not found.
        """
        deleted = await self.metadata_repo.delete(asset_id)
        if not deleted:
            raise ValueError(f"Metadata not found for asset {asset_id}")

        logger.info(f"Deleted metadata for asset {asset_id}")
        return True

    async def list_metadata(
        self,
        data_type: Optional[str] = None,
        limit: int = 100,
    ) -> List[LayerMetadata]:
        """List layer metadata with optional data_type filter."""
        if data_type:
            return await self.metadata_repo.list_by_data_type(data_type, limit)
        return await self.metadata_repo.list_all(limit)
