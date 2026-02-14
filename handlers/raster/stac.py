# ============================================================================
# STAC REGISTRATION HANDLERS
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Handler - Node F
# PURPOSE: Register COGs in STAC catalog (collection + items)
# CREATED: 31 JAN 2026
# ============================================================================
"""
STAC Registration Handlers (Node F)

Modular STAC registration:
- stac_ensure_collection: Create collection if doesn't exist (idempotent)
- stac_register_item: Add single item to existing collection
- stac_register_items: Add multiple items to existing collection (tiled)

This modular design allows swapping "ensure_collection" for
"use_existing_collection" when adding to an existing collection.

Source to copy from rmhgeoapi:
- /services/stac_catalog.py: extract_stac_metadata(), insert_stac_item()
- /services/stac_collection.py: create_stac_collection()
- /infrastructure/pgstac.py: PgStacRepository
"""

import os
import logging
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone

from handlers.registry import register_handler, HandlerContext, HandlerResult
from infrastructure.pgstac import PgStacRepository
from infrastructure.pgstac_search import PgStacSearchRegistration
from infrastructure.storage import BlobRepository

logger = logging.getLogger(__name__)


# =============================================================================
# ENSURE COLLECTION (idempotent create)
# =============================================================================

@register_handler("raster.stac_ensure_collection", queue="container-tasks", timeout_seconds=300)
async def stac_ensure_collection(ctx: HandlerContext) -> HandlerResult:
    """
    Ensure STAC collection exists, creating if necessary.

    This is idempotent - safe to call multiple times. Will update
    collection metadata if it already exists.

    SWAPPABLE: Replace with a no-op node when adding to existing collection.

    Params:
        collection_id: STAC collection ID
        title: Optional collection title
        bounds: Collection spatial extent [minx, miny, maxx, maxy]
        grid_dimensions: Optional {rows, cols} for tiled collections
        is_tiled: Whether this is a tiled collection
        dataset_id: Optional DDH dataset ID
        access_level: Optional DDH access level

    Output:
        collection_id: STAC collection ID
        created: True if newly created, False if already existed
        updated: True if metadata was updated
        collection_url: URL to collection in pgSTAC
    """
    collection_id = ctx.params["collection_id"]
    title = ctx.params.get("title")
    bounds = ctx.params.get("bounds", [-180, -90, 180, 90])
    grid_dimensions = ctx.params.get("grid_dimensions")
    is_tiled = ctx.params.get("is_tiled", False)
    dataset_id = ctx.params.get("dataset_id")
    access_level = ctx.params.get("access_level")

    logger.info(f"Ensuring STAC collection exists: {collection_id}")

    # =========================================================================
    # Build STAC Collection
    # =========================================================================
    description = f"Tiled COG collection ({grid_dimensions['rows']}x{grid_dimensions['cols']})" \
        if is_tiled and grid_dimensions else "Cloud-Optimized GeoTIFF collection"

    collection = {
        "type": "Collection",
        "stac_version": "1.0.0",
        "stac_extensions": [],
        "id": collection_id,
        "title": title or collection_id,
        "description": description,
        "license": "proprietary",
        "extent": {
            "spatial": {
                "bbox": [bounds],
            },
            "temporal": {
                "interval": [[datetime.now(timezone.utc).isoformat(), None]],
            },
        },
        "links": [],
    }

    # Add platform metadata
    if dataset_id or access_level:
        collection["properties"] = {}
        if dataset_id:
            collection["properties"]["ddh:dataset_id"] = dataset_id
        if access_level:
            collection["properties"]["ddh:access_level"] = access_level

    # Add tiling info
    if is_tiled and grid_dimensions:
        collection["summaries"] = {
            "tile:grid_rows": grid_dimensions["rows"],
            "tile:grid_cols": grid_dimensions["cols"],
        }

    # =========================================================================
    # Upsert into pgSTAC
    # =========================================================================
    repo = PgStacRepository()

    # Check if collection already exists
    existing = repo.collection_exists(collection_id)

    # Upsert collection (idempotent)
    repo.insert_collection(collection)

    created = not existing
    updated = existing

    pgstac_base = os.environ.get("DAG_PGSTAC_URL", "https://pgstac.example.com")
    collection_url = f"{pgstac_base}/collections/{collection_id}"

    logger.info(f"[Node F.1] Collection {'created' if created else 'updated'}: {collection_id}")

    return HandlerResult.success_result({
        "collection_id": collection_id,
        "created": created,
        "updated": updated,
        "collection_url": collection_url,
    })


# =============================================================================
# REGISTER SINGLE ITEM
# =============================================================================

@register_handler("raster.stac_register_item", queue="container-tasks", timeout_seconds=300)
async def stac_register_item(ctx: HandlerContext) -> HandlerResult:
    """
    Register single COG as STAC item in existing collection.

    Params:
        cog_blob: COG blob path in silver-cogs
        cog_container: COG container name
        validation: Validation output with bounds, CRS, type
        collection_id: STAC collection ID (must exist)
        item_id: Optional custom item ID (auto-generated if not provided)
        title: Optional title for metadata
        tags: Optional tags for metadata
        dataset_id: Optional DDH dataset ID
        resource_id: Optional DDH resource ID
        version_id: Optional DDH version ID
        access_level: Optional DDH access level

    Output:
        item_id: STAC item ID
        collection_id: STAC collection ID
        pgstac_id: UUID in pgSTAC database
        inserted: True if successfully inserted
        viewer_url: TiTiler viewer URL
        tilejson_url: TileJSON endpoint
        tiles_url: XYZ tiles endpoint template
        stac_urls: {collection_url, item_url}
    """
    cog_blob = ctx.params["cog_blob"]
    cog_container = ctx.params["cog_container"]
    validation = ctx.params["validation"]
    collection_id = ctx.params["collection_id"]
    item_id = ctx.params.get("item_id")
    title = ctx.params.get("title")
    tags = ctx.params.get("tags", [])

    # Platform passthrough
    dataset_id = ctx.params.get("dataset_id")
    resource_id = ctx.params.get("resource_id")
    version_id = ctx.params.get("version_id")
    access_level = ctx.params.get("access_level")

    logger.info(f"Registering STAC item for: {cog_blob}")

    # Generate item ID if not provided
    if not item_id:
        item_id = _generate_item_id(cog_blob)

    # Extract validation data
    bounds = validation["bounds"]
    source_crs = validation["source_crs"]
    raster_type = validation["raster_type"]

    # Build STAC Item
    stac_item = _build_stac_item(
        item_id=item_id,
        collection_id=collection_id,
        cog_blob=cog_blob,
        cog_container=cog_container,
        bounds=bounds,
        source_crs=source_crs,
        raster_type=raster_type,
        validation=validation,
        title=title,
        tags=tags,
        dataset_id=dataset_id,
        resource_id=resource_id,
        version_id=version_id,
        access_level=access_level,
    )

    # =========================================================================
    # Insert into pgSTAC
    # =========================================================================
    repo = PgStacRepository()
    repo.insert_item(stac_item, collection_id)

    inserted = True

    # Register search for TiTiler visualization
    search_registrar = PgStacSearchRegistration()
    search_id = search_registrar.register_collection_search(
        collection_id=collection_id,
        bbox=bounds,
    )

    # Generate viewer URLs
    urls = _generate_viewer_urls(cog_blob, cog_container, collection_id, item_id)

    # Add search-based URLs
    search_urls = search_registrar.get_search_urls(search_id)
    urls["mosaic_viewer"] = search_urls["viewer"]
    urls["mosaic_tilejson"] = search_urls["tilejson"]
    urls["search_id"] = search_id

    logger.info(f"[Node F.2] STAC item registered: {item_id}")

    return HandlerResult.success_result({
        "item_id": item_id,
        "collection_id": collection_id,
        "inserted": inserted,
        "viewer_url": urls["viewer_url"],
        "tilejson_url": urls["tilejson_url"],
        "tiles_url": urls["tiles_url"],
        "mosaic_viewer": urls.get("mosaic_viewer"),
        "search_id": urls.get("search_id"),
        "stac_urls": urls["stac_urls"],
        "stac_item": stac_item,
    })


# =============================================================================
# REGISTER MULTIPLE ITEMS (for tiled output)
# =============================================================================

@register_handler("raster.stac_register_items", queue="container-tasks", timeout_seconds=1800)
async def stac_register_items(ctx: HandlerContext) -> HandlerResult:
    """
    Register multiple tile COGs as STAC items in existing collection.

    Params:
        tile_cogs: List of tile COG outputs [{tile_id, cog_blob, geo_bounds}, ...]
        cog_container: COG container name
        validation: Validation output
        collection_id: STAC collection ID (must exist)
        tags: Optional tags
        dataset_id: Optional DDH dataset ID
        resource_id: Optional DDH resource ID
        version_id: Optional DDH version ID
        access_level: Optional DDH access level

    Output:
        collection_id: STAC collection ID
        items_created: Number of items created
        items_failed: Number of items that failed
        item_ids: List of created item IDs
        pgstac_search_id: pgSTAC search ID for mosaic access
    """
    tile_cogs = ctx.params["tile_cogs"]
    cog_container = ctx.params["cog_container"]
    validation = ctx.params["validation"]
    collection_id = ctx.params["collection_id"]
    tags = ctx.params.get("tags", [])

    # Platform passthrough
    dataset_id = ctx.params.get("dataset_id")
    resource_id = ctx.params.get("resource_id")
    version_id = ctx.params.get("version_id")
    access_level = ctx.params.get("access_level")

    logger.info(f"Registering {len(tile_cogs)} STAC items in collection {collection_id}")

    # =========================================================================
    # Build STAC items for each tile
    # =========================================================================
    items_created = 0
    items_failed = 0
    item_ids = []
    stac_items = []

    for tile in tile_cogs:
        try:
            tile_id = tile["tile_id"]
            cog_blob = tile["cog_blob"]
            geo_bounds = tile["geo_bounds"]

            item = _build_tile_stac_item(
                tile_id=tile_id,
                collection_id=collection_id,
                cog_blob=cog_blob,
                cog_container=cog_container,
                geo_bounds=geo_bounds,
                row=tile["row"],
                col=tile["col"],
                tags=tags,
                dataset_id=dataset_id,
                resource_id=resource_id,
                version_id=version_id,
                access_level=access_level,
            )

            stac_items.append(item)
            item_ids.append(tile_id)
            items_created += 1

        except Exception as e:
            logger.error(f"Failed to create item for {tile.get('tile_id', 'unknown')}: {e}")
            items_failed += 1

    # =========================================================================
    # Bulk insert into pgSTAC
    # =========================================================================
    repo = PgStacRepository()
    repo.insert_items_bulk(stac_items, collection_id)

    # Register search for TiTiler mosaic access
    search_registrar = PgStacSearchRegistration()

    # Calculate collection bounds for search metadata
    all_bounds = [tile["geo_bounds"] for tile in tile_cogs]
    collection_bounds = [
        min(b[0] for b in all_bounds),
        min(b[1] for b in all_bounds),
        max(b[2] for b in all_bounds),
        max(b[3] for b in all_bounds),
    ]

    pgstac_search_id = search_registrar.register_collection_search(
        collection_id=collection_id,
        bbox=collection_bounds,
    )

    logger.info(
        f"[Node F.2] STAC items registered: {items_created} created, {items_failed} failed"
    )

    # Get mosaic viewer URLs
    search_urls = search_registrar.get_search_urls(pgstac_search_id)

    return HandlerResult.success_result({
        "collection_id": collection_id,
        "items_created": items_created,
        "items_failed": items_failed,
        "total_tiles": len(tile_cogs),
        "item_ids": item_ids,
        "pgstac_search_id": pgstac_search_id,
        "mosaic_viewer": search_urls["viewer"],
        "mosaic_tilejson": search_urls["tilejson"],
    })


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def _generate_item_id(cog_blob: str) -> str:
    """Generate STAC item ID from COG blob path."""
    basename = os.path.basename(cog_blob)
    name, _ = os.path.splitext(basename)
    return name


def _extract_epsg(crs_string: str) -> Optional[int]:
    """Extract EPSG code from CRS string."""
    if not crs_string:
        return None
    if crs_string.upper().startswith("EPSG:"):
        try:
            return int(crs_string.split(":")[1])
        except (IndexError, ValueError):
            pass
    return None


def _bounds_to_geometry(bounds: List[float]) -> Dict[str, Any]:
    """Convert bounds [minx, miny, maxx, maxy] to GeoJSON Polygon."""
    minx, miny, maxx, maxy = bounds
    return {
        "type": "Polygon",
        "coordinates": [[
            [minx, miny],
            [maxx, miny],
            [maxx, maxy],
            [minx, maxy],
            [minx, miny],
        ]],
    }


def _build_stac_item(
    item_id: str,
    collection_id: str,
    cog_blob: str,
    cog_container: str,
    bounds: List[float],
    source_crs: str,
    raster_type: Dict[str, Any],
    validation: Dict[str, Any],
    title: Optional[str] = None,
    tags: Optional[List[str]] = None,
    dataset_id: Optional[str] = None,
    resource_id: Optional[str] = None,
    version_id: Optional[str] = None,
    access_level: Optional[str] = None,
) -> Dict[str, Any]:
    """Build a STAC item for a single COG."""
    silver_account = BlobRepository.for_zone("silver").account_name
    cog_url = f"https://{silver_account}.blob.core.windows.net/{cog_container}/{cog_blob}"

    item = {
        "type": "Feature",
        "stac_version": "1.0.0",
        "stac_extensions": [
            "https://stac-extensions.github.io/raster/v1.1.0/schema.json",
        ],
        "id": item_id,
        "collection": collection_id,
        "geometry": _bounds_to_geometry(bounds),
        "bbox": bounds,
        "properties": {
            "datetime": datetime.now(timezone.utc).isoformat(),
            "title": title or item_id,
            "tags": tags or [],
            "proj:epsg": _extract_epsg(source_crs),
            "raster:type": raster_type.get("type", "unknown"),
            "raster:bands": validation.get("band_count"),
            "raster:data_type": validation.get("dtype"),
        },
        "assets": {
            "data": {
                "href": cog_url,
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "title": "Cloud-Optimized GeoTIFF",
                "roles": ["data"],
            },
        },
        "links": [],
    }

    # Add platform metadata
    if dataset_id:
        item["properties"]["ddh:dataset_id"] = dataset_id
    if resource_id:
        item["properties"]["ddh:resource_id"] = resource_id
    if version_id:
        item["properties"]["ddh:version_id"] = version_id
    if access_level:
        item["properties"]["ddh:access_level"] = access_level

    return item


def _build_tile_stac_item(
    tile_id: str,
    collection_id: str,
    cog_blob: str,
    cog_container: str,
    geo_bounds: List[float],
    row: int,
    col: int,
    tags: Optional[List[str]] = None,
    dataset_id: Optional[str] = None,
    resource_id: Optional[str] = None,
    version_id: Optional[str] = None,
    access_level: Optional[str] = None,
) -> Dict[str, Any]:
    """Build a STAC item for a tile COG."""
    silver_account = BlobRepository.for_zone("silver").account_name
    cog_url = f"https://{silver_account}.blob.core.windows.net/{cog_container}/{cog_blob}"

    item = {
        "type": "Feature",
        "stac_version": "1.0.0",
        "id": tile_id,
        "collection": collection_id,
        "geometry": _bounds_to_geometry(geo_bounds),
        "bbox": geo_bounds,
        "properties": {
            "datetime": datetime.now(timezone.utc).isoformat(),
            "tile:row": row,
            "tile:col": col,
            "tags": tags or [],
        },
        "assets": {
            "data": {
                "href": cog_url,
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "roles": ["data"],
            },
        },
        "links": [],
    }

    # Add platform metadata
    if dataset_id:
        item["properties"]["ddh:dataset_id"] = dataset_id
    if resource_id:
        item["properties"]["ddh:resource_id"] = resource_id
    if version_id:
        item["properties"]["ddh:version_id"] = version_id
    if access_level:
        item["properties"]["ddh:access_level"] = access_level

    return item


def _generate_viewer_urls(
    cog_blob: str,
    cog_container: str,
    collection_id: str,
    item_id: str,
) -> Dict[str, str]:
    """Generate TiTiler and pgSTAC viewer URLs."""
    import urllib.parse

    silver_account = BlobRepository.for_zone("silver").account_name
    titiler_base = os.environ.get("DAG_TITILER_URL", "https://titiler.example.com")
    pgstac_base = os.environ.get("DAG_PGSTAC_URL", "https://pgstac.example.com")

    cog_url = f"https://{silver_account}.blob.core.windows.net/{cog_container}/{cog_blob}"
    encoded_cog_url = urllib.parse.quote(cog_url, safe="")

    return {
        "viewer_url": f"{titiler_base}/cog/viewer?url={encoded_cog_url}",
        "tilejson_url": f"{titiler_base}/cog/tilejson.json?url={encoded_cog_url}",
        "tiles_url": f"{titiler_base}/cog/tiles/WebMercatorQuad/{{z}}/{{x}}/{{y}}?url={encoded_cog_url}",
        "stac_urls": {
            "collection_url": f"{pgstac_base}/collections/{collection_id}",
            "item_url": f"{pgstac_base}/collections/{collection_id}/items/{item_id}",
        },
    }
