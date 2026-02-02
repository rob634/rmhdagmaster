# ============================================================================
# PGSTAC SEARCH REGISTRATION
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Infrastructure - pgSTAC search registration for TiTiler
# PURPOSE: Register searches and generate viewer URLs
# CREATED: 31 JAN 2026
# ============================================================================
"""
pgSTAC Search Registration

Registers searches in pgSTAC for TiTiler mosaic visualization:
- register_search: Register arbitrary search query
- register_collection_search: Register all items in a collection
- get_search_urls: Generate TiTiler viewer/tilejson/tiles URLs

Bypasses TiTiler API for direct database registration (faster, no auth issues).

Adapted from rmhgeoapi/services/pgstac_search_registration.py for DAG orchestrator.
"""

import os
import json
import hashlib
import logging
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone

from infrastructure.postgresql import PostgreSQLRepository

logger = logging.getLogger(__name__)


class PgStacSearchRegistration:
    """
    Service for registering pgSTAC searches.

    Creates search entries directly in pgstac.searches table,
    bypassing TiTiler's /searches/register endpoint.

    The search hash is computed using SHA256 of canonical JSON,
    matching TiTiler's algorithm for compatibility.

    Usage:
        registrar = PgStacSearchRegistration()

        # Register search for a collection
        search_id = registrar.register_collection_search(
            collection_id="cogs",
            bbox=[71.6, 40.9, 71.7, 41.0]
        )

        # Get viewer URLs
        urls = registrar.get_search_urls(search_id)
        print(urls["viewer"])  # TiTiler viewer URL
    """

    PGSTAC_SCHEMA = "pgstac"

    def __init__(self, connection_string: Optional[str] = None):
        """Initialize search registration service."""
        self._db = PostgreSQLRepository(
            connection_string=connection_string,
            schema_name=self.PGSTAC_SCHEMA,
        )

    def register_search(
        self,
        collections: List[str],
        metadata: Optional[Dict[str, Any]] = None,
        bbox: Optional[List[float]] = None,
        datetime_str: Optional[str] = None,
        filter_cql: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Register a pgSTAC search in the database.

        Mimics TiTiler's /searches/register endpoint behavior
        but bypasses the API for direct database access.

        Args:
            collections: List of collection IDs to search
            metadata: Optional metadata (name, bounds, etc.)
            bbox: Optional bounding box filter [minx, miny, maxx, maxy]
            datetime_str: Optional datetime filter
            filter_cql: Optional CQL2 filter

        Returns:
            Search ID (64-character SHA256 hash)
        """
        # Build search query (matching TiTiler's format)
        search_query = {"collections": collections}

        if bbox:
            search_query["bbox"] = bbox
        if datetime_str:
            search_query["datetime"] = datetime_str
        if filter_cql:
            search_query["filter"] = filter_cql

        # Compute hash using canonical JSON (matching TiTiler's algorithm)
        canonical_json = json.dumps(search_query, sort_keys=True, separators=(",", ":"))
        search_hash = hashlib.sha256(canonical_json.encode()).hexdigest()

        logger.debug(f"Registering search with hash: {search_hash[:16]}...")

        # Prepare metadata
        if metadata is None:
            metadata = {}
        if "name" not in metadata:
            metadata["name"] = f"Search: {', '.join(collections)}"
        if bbox and "bounds" not in metadata:
            metadata["bounds"] = bbox

        try:
            with self._db.get_connection() as conn:
                with conn.cursor() as cur:
                    # CRITICAL: Set search_path for GENERATED column to work
                    cur.execute(f"SET search_path TO {self.PGSTAC_SCHEMA}, public")

                    # Check if search already exists (by hash)
                    cur.execute(
                        f"""
                        SELECT hash FROM {self.PGSTAC_SCHEMA}.searches
                        WHERE hash = %s
                        """,
                        (search_hash,)
                    )
                    existing = cur.fetchone()

                    if existing:
                        # Update existing search (increment usecount)
                        cur.execute(
                            f"""
                            UPDATE {self.PGSTAC_SCHEMA}.searches
                            SET usecount = usecount + 1,
                                lastused = %s,
                                metadata = %s::jsonb
                            WHERE hash = %s
                            """,
                            (
                                datetime.now(timezone.utc),
                                json.dumps(metadata),
                                search_hash,
                            )
                        )
                        logger.debug(f"Updated existing search: {search_hash[:16]}...")
                    else:
                        # Insert new search
                        # NOTE: hash column is GENERATED, but we use our computed value
                        # to match TiTiler's algorithm
                        cur.execute(
                            f"""
                            INSERT INTO {self.PGSTAC_SCHEMA}.searches
                            (search, metadata, usecount, lastused)
                            VALUES (%s::jsonb, %s::jsonb, 1, %s)
                            """,
                            (
                                json.dumps(search_query),
                                json.dumps(metadata),
                                datetime.now(timezone.utc),
                            )
                        )
                        logger.info(f"Created new search: {search_hash[:16]}...")

                conn.commit()

            return search_hash

        except Exception as e:
            logger.error(f"Failed to register search: {e}")
            raise

    def register_collection_search(
        self,
        collection_id: str,
        metadata: Optional[Dict[str, Any]] = None,
        bbox: Optional[List[float]] = None,
    ) -> str:
        """
        Register a search for all items in a collection.

        This is the most common use case - create a mosaic of all
        items in a collection for TiTiler visualization.

        Args:
            collection_id: STAC collection ID
            metadata: Optional metadata (auto-generates name if not provided)
            bbox: Optional bbox for TiTiler auto-zoom (stored in metadata)

        Returns:
            Search ID (64-character SHA256 hash)
        """
        if metadata is None:
            metadata = {}

        # Set default name if not provided
        if "name" not in metadata:
            metadata["name"] = f"{collection_id} mosaic"

        # Store bbox in metadata for TiTiler's map.html auto-zoom
        if bbox and "bounds" not in metadata:
            metadata["bounds"] = bbox

        return self.register_search(
            collections=[collection_id],
            metadata=metadata,
            bbox=None,  # Don't filter by bbox, just store in metadata
        )

    def get_search_urls(
        self,
        search_id: str,
        assets: Optional[List[str]] = None,
        band_indexes: Optional[List[int]] = None,
    ) -> Dict[str, str]:
        """
        Generate TiTiler viewer URLs for a registered search.

        Args:
            search_id: Search hash (64-character hex string)
            assets: Asset keys to use (default: ["data"])
            band_indexes: Band indexes for multi-band imagery

        Returns:
            Dict with viewer, tilejson, and tiles URLs
        """
        titiler_base = os.environ.get(
            "TITILER_URL",
            "https://titiler.example.com"
        ).rstrip("/")

        # Build query parameters
        params = []

        # Assets parameter
        if assets:
            for asset in assets:
                params.append(f"assets={asset}")
        else:
            params.append("assets=data")

        # Band indexes (required for multi-band imagery)
        # BUG-011 FIX: TiTiler assumes 3-band RGB by default
        if band_indexes:
            for idx in band_indexes:
                params.append(f"bidx={idx}")

        params_str = "&".join(params)

        # Build URLs
        base_search_url = f"{titiler_base}/searches/{search_id}"

        urls = {
            "viewer": f"{base_search_url}/viewer?{params_str}",
            "tilejson": f"{base_search_url}/tilejson.json?{params_str}",
            "tiles": f"{base_search_url}/tiles/WebMercatorQuad/{{z}}/{{x}}/{{y}}?{params_str}",
            "search_id": search_id,
        }

        return urls

    def get_search(self, search_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a search by its hash.

        Args:
            search_id: Search hash

        Returns:
            Search dict or None if not found
        """
        try:
            result = self._db.fetch_one(
                f"""
                SELECT hash, search, metadata, usecount, lastused
                FROM {self.PGSTAC_SCHEMA}.searches
                WHERE hash = %s
                """,
                (search_id,)
            )
            if result:
                return {
                    "hash": result["hash"],
                    "search": result["search"],
                    "metadata": result["metadata"],
                    "usecount": result["usecount"],
                    "lastused": result["lastused"],
                }
            return None
        except Exception as e:
            logger.error(f"Error getting search {search_id}: {e}")
            return None

    def delete_search(self, search_id: str) -> bool:
        """
        Delete a search by its hash.

        Args:
            search_id: Search hash

        Returns:
            True if search was deleted
        """
        try:
            with self._db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        DELETE FROM {self.PGSTAC_SCHEMA}.searches
                        WHERE hash = %s
                        """,
                        (search_id,)
                    )
                    deleted = cur.rowcount > 0
                conn.commit()

            if deleted:
                logger.info(f"Deleted search: {search_id[:16]}...")
            return deleted

        except Exception as e:
            logger.error(f"Failed to delete search {search_id}: {e}")
            return False


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

_default_registrar: Optional[PgStacSearchRegistration] = None


def get_search_registrar() -> PgStacSearchRegistration:
    """Get shared search registration instance."""
    global _default_registrar
    if _default_registrar is None:
        _default_registrar = PgStacSearchRegistration()
    return _default_registrar


# ============================================================================
# EXPORTS
# ============================================================================

__all__ = [
    "PgStacSearchRegistration",
    "get_search_registrar",
]
