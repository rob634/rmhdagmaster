# ============================================================================
# PGSTAC REPOSITORY
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Infrastructure - pgSTAC database operations
# PURPOSE: STAC collection and item CRUD operations
# CREATED: 31 JAN 2026
# ============================================================================
"""
pgSTAC Repository

Provides CRUD operations for STAC collections and items:
- insert_collection: Upsert STAC collection
- insert_item: Upsert STAC item
- insert_items_bulk: Bulk insert items
- collection_exists: Check collection existence
- delete_item: Remove item (for job resubmission)

All operations use pgSTAC's upsert functions for idempotency.

Adapted from rmhgeoapi/infrastructure/pgstac_repository.py for DAG orchestrator.
"""

import json
import logging
from typing import Any, Dict, List, Optional

from infrastructure.postgresql import PostgreSQLRepository

logger = logging.getLogger(__name__)


class PgStacRepository:
    """
    Repository for pgSTAC database operations.

    Uses pgSTAC's built-in functions for upsert semantics:
    - pgstac.upsert_collection()
    - pgstac.upsert_item()

    All operations are idempotent - safe for job resubmission.

    Usage:
        repo = PgStacRepository()

        # Insert collection
        repo.insert_collection({
            "type": "Collection",
            "id": "my-collection",
            ...
        })

        # Insert item
        repo.insert_item(item_dict, "my-collection")
    """

    PGSTAC_SCHEMA = "pgstac"

    def __init__(self, connection_string: Optional[str] = None):
        """Initialize pgSTAC repository."""
        self._db = PostgreSQLRepository(
            connection_string=connection_string,
            schema_name=self.PGSTAC_SCHEMA,
        )

    # ========================================================================
    # COLLECTION OPERATIONS
    # ========================================================================

    def collection_exists(self, collection_id: str) -> bool:
        """
        Check if a collection exists in pgSTAC.

        CRITICAL: Collections must exist before inserting items.
        pgSTAC creates partitions per collection.

        Args:
            collection_id: STAC collection ID

        Returns:
            True if collection exists
        """
        try:
            result = self._db.fetch_one(
                f"SELECT 1 FROM {self.PGSTAC_SCHEMA}.collections WHERE id = %s",
                (collection_id,)
            )
            return result is not None
        except Exception as e:
            logger.error(f"Error checking collection existence: {e}")
            return False

    def insert_collection(self, collection: Dict[str, Any]) -> str:
        """
        Insert or update a STAC collection.

        Uses pgstac.upsert_collection() for idempotent operation.

        Args:
            collection: STAC Collection dict with required fields:
                - type: "Collection"
                - id: Collection identifier
                - stac_version: STAC version
                - description: Collection description
                - extent: Spatial and temporal extent
                - links: Collection links

        Returns:
            Collection ID
        """
        collection_id = collection.get("id")
        if not collection_id:
            raise ValueError("Collection must have 'id' field")

        logger.info(f"Upserting collection: {collection_id}")

        try:
            collection_json = json.dumps(collection)

            with self._db.get_connection() as conn:
                with conn.cursor() as cur:
                    # Set search path for pgSTAC functions
                    cur.execute(f"SET search_path TO {self.PGSTAC_SCHEMA}, public")

                    # Use pgSTAC's upsert function
                    cur.execute(
                        f"SELECT {self.PGSTAC_SCHEMA}.upsert_collection(%s::jsonb)",
                        (collection_json,)
                    )

                conn.commit()

            logger.info(f"Collection upserted successfully: {collection_id}")
            return collection_id

        except Exception as e:
            logger.error(f"Failed to upsert collection {collection_id}: {e}")
            raise

    def get_collection(self, collection_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a collection by ID.

        Args:
            collection_id: STAC collection ID

        Returns:
            Collection dict or None if not found
        """
        try:
            result = self._db.fetch_one(
                f"SELECT content FROM {self.PGSTAC_SCHEMA}.collections WHERE id = %s",
                (collection_id,)
            )
            if result:
                return result["content"]
            return None
        except Exception as e:
            logger.error(f"Error getting collection {collection_id}: {e}")
            return None

    def update_collection_metadata(
        self,
        collection_id: str,
        metadata: Dict[str, Any]
    ) -> bool:
        """
        Update collection metadata (merge into existing).

        Useful for storing search_id and other runtime metadata.

        Args:
            collection_id: STAC collection ID
            metadata: Metadata to merge into collection

        Returns:
            True if updated successfully
        """
        try:
            metadata_json = json.dumps(metadata)

            with self._db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        UPDATE {self.PGSTAC_SCHEMA}.collections
                        SET content = content || %s::jsonb
                        WHERE id = %s
                        """,
                        (metadata_json, collection_id)
                    )
                    updated = cur.rowcount > 0
                conn.commit()

            if updated:
                logger.debug(f"Updated collection metadata: {collection_id}")
            return updated

        except Exception as e:
            logger.error(f"Failed to update collection metadata: {e}")
            return False

    def list_collections(
        self,
        limit: int = 100,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """
        List all collections with pagination.

        Args:
            limit: Maximum collections to return
            offset: Offset for pagination

        Returns:
            List of collection dicts
        """
        try:
            results = self._db.fetch_all(
                f"""
                SELECT id, content
                FROM {self.PGSTAC_SCHEMA}.collections
                ORDER BY id
                LIMIT %s OFFSET %s
                """,
                (limit, offset)
            )
            return [r["content"] for r in results]
        except Exception as e:
            logger.error(f"Error listing collections: {e}")
            return []

    # ========================================================================
    # ITEM OPERATIONS
    # ========================================================================

    def insert_item(
        self,
        item: Dict[str, Any],
        collection_id: str
    ) -> str:
        """
        Insert or update a STAC item.

        Uses pgstac.upsert_item() for idempotent operation.

        IMPORTANT: Collection must exist before inserting items.

        Args:
            item: STAC Item dict with required fields:
                - type: "Feature"
                - stac_version: STAC version
                - id: Item identifier
                - geometry: GeoJSON geometry
                - bbox: Bounding box
                - properties: Item properties (must include datetime)
                - assets: Item assets
                - links: Item links
            collection_id: Collection to insert item into

        Returns:
            Item ID
        """
        item_id = item.get("id")
        if not item_id:
            raise ValueError("Item must have 'id' field")

        # Ensure collection field is set
        item["collection"] = collection_id

        # Validate collection exists (critical for partitioning)
        if not self.collection_exists(collection_id):
            raise ValueError(
                f"Collection '{collection_id}' does not exist. "
                "Collections must be created before inserting items."
            )

        logger.debug(f"Upserting item: {item_id} in collection {collection_id}")

        try:
            item_json = json.dumps(item)

            with self._db.get_connection() as conn:
                with conn.cursor() as cur:
                    # Set search path for pgSTAC functions
                    cur.execute(f"SET search_path TO {self.PGSTAC_SCHEMA}, public")

                    # Use pgSTAC's upsert function
                    cur.execute(
                        f"SELECT {self.PGSTAC_SCHEMA}.upsert_item(%s::jsonb)",
                        (item_json,)
                    )

                conn.commit()

            logger.debug(f"Item upserted successfully: {item_id}")
            return item_id

        except Exception as e:
            logger.error(f"Failed to upsert item {item_id}: {e}")
            raise

    def insert_items_bulk(
        self,
        items: List[Dict[str, Any]],
        collection_id: str
    ) -> int:
        """
        Bulk insert multiple STAC items.

        More efficient than individual inserts for large batches.

        Args:
            items: List of STAC Item dicts
            collection_id: Collection to insert items into

        Returns:
            Number of items inserted
        """
        if not items:
            return 0

        # Validate collection exists
        if not self.collection_exists(collection_id):
            raise ValueError(
                f"Collection '{collection_id}' does not exist. "
                "Collections must be created before inserting items."
            )

        logger.info(f"Bulk inserting {len(items)} items into {collection_id}")

        inserted = 0
        try:
            with self._db.get_connection() as conn:
                with conn.cursor() as cur:
                    # Set search path for pgSTAC functions
                    cur.execute(f"SET search_path TO {self.PGSTAC_SCHEMA}, public")

                    for item in items:
                        item["collection"] = collection_id
                        item_json = json.dumps(item)

                        cur.execute(
                            f"SELECT {self.PGSTAC_SCHEMA}.upsert_item(%s::jsonb)",
                            (item_json,)
                        )
                        inserted += 1

                conn.commit()

            logger.info(f"Bulk insert complete: {inserted} items")
            return inserted

        except Exception as e:
            logger.error(f"Bulk insert failed after {inserted} items: {e}")
            raise

    def get_item(
        self,
        item_id: str,
        collection_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get an item by ID and collection.

        Args:
            item_id: STAC item ID
            collection_id: Collection the item belongs to

        Returns:
            Item dict or None if not found
        """
        try:
            result = self._db.fetch_one(
                f"""
                SELECT content
                FROM {self.PGSTAC_SCHEMA}.items
                WHERE id = %s AND collection = %s
                """,
                (item_id, collection_id)
            )
            if result:
                return result["content"]
            return None
        except Exception as e:
            logger.error(f"Error getting item {item_id}: {e}")
            return None

    def delete_item(self, collection_id: str, item_id: str) -> bool:
        """
        Delete a STAC item.

        Used for job resubmission cleanup.

        Args:
            collection_id: Collection the item belongs to
            item_id: Item ID to delete

        Returns:
            True if item was deleted
        """
        try:
            with self._db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        DELETE FROM {self.PGSTAC_SCHEMA}.items
                        WHERE id = %s AND collection = %s
                        """,
                        (item_id, collection_id)
                    )
                    deleted = cur.rowcount > 0
                conn.commit()

            if deleted:
                logger.info(f"Deleted item: {item_id} from {collection_id}")
            return deleted

        except Exception as e:
            logger.error(f"Failed to delete item {item_id}: {e}")
            return False

    def get_collection_item_ids(self, collection_id: str) -> List[str]:
        """
        Get all item IDs in a collection.

        Args:
            collection_id: STAC collection ID

        Returns:
            List of item IDs
        """
        try:
            results = self._db.fetch_all(
                f"""
                SELECT id FROM {self.PGSTAC_SCHEMA}.items
                WHERE collection = %s
                ORDER BY id
                """,
                (collection_id,)
            )
            return [r["id"] for r in results]
        except Exception as e:
            logger.error(f"Error getting item IDs for {collection_id}: {e}")
            return []

    # ========================================================================
    # PLATFORM CATALOG OPERATIONS (B2B)
    # ========================================================================

    def search_by_platform_ids(
        self,
        dataset_id: str,
        resource_id: str,
        version_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Search for STAC item by DDH platform identifiers.

        Used for artifact lookup in platform jobs.

        Args:
            dataset_id: DDH dataset ID
            resource_id: DDH resource ID
            version_id: DDH version ID

        Returns:
            Item dict or None if not found
        """
        try:
            # Build JSONB containment query
            properties_filter = {
                "ddh:dataset_id": dataset_id,
                "ddh:resource_id": resource_id,
                "ddh:version_id": version_id,
            }
            filter_json = json.dumps(properties_filter)

            result = self._db.fetch_one(
                f"""
                SELECT id, collection, content
                FROM {self.PGSTAC_SCHEMA}.items
                WHERE content->'properties' @> %s::jsonb
                LIMIT 1
                """,
                (filter_json,)
            )

            if result:
                item = result["content"]
                item["id"] = result["id"]
                item["collection"] = result["collection"]
                return item

            return None

        except Exception as e:
            logger.error(f"Error searching by platform IDs: {e}")
            return None

    # ========================================================================
    # HEALTH CHECK
    # ========================================================================

    def is_available(self) -> bool:
        """
        Check if pgSTAC is available and functioning.

        Returns:
            True if pgSTAC is responding
        """
        try:
            result = self._db.fetch_one(
                f"SELECT count(*) as count FROM {self.PGSTAC_SCHEMA}.collections"
            )
            return result is not None
        except Exception as e:
            logger.warning(f"pgSTAC availability check failed: {e}")
            return False


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

_default_repo: Optional[PgStacRepository] = None


def get_pgstac_repository() -> PgStacRepository:
    """Get shared pgSTAC repository instance."""
    global _default_repo
    if _default_repo is None:
        _default_repo = PgStacRepository()
    return _default_repo


# ============================================================================
# EXPORTS
# ============================================================================

__all__ = [
    "PgStacRepository",
    "get_pgstac_repository",
]
