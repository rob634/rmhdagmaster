# ============================================================================
# RASTER HANDLERS
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Handler Module - Raster processing handlers
# PURPOSE: DAG-native handlers for raster workflow
# CREATED: 31 JAN 2026
# ============================================================================
"""
Raster Processing Handlers

Handlers for the raster_process workflow:
- blob_to_mount: Copy blob to mounted Azure Files storage
- validate: Validate raster with type detection and compression profile
- create_cog: Reproject and create Cloud-Optimized GeoTIFF
- generate_tiling_scheme: Calculate tile grid for large outputs
- process_tile_batch: Process tiles with checkpointing
- stac_register_item: Register single COG as STAC item
- stac_register_collection: Register tiled COGs as STAC collection
"""

from handlers.raster.blob_copy import blob_to_mount
from handlers.raster.validate import validate
from handlers.raster.cog_create import create_cog
from handlers.raster.tiling import generate_tiling_scheme, process_tile_batch
from handlers.raster.stac import (
    stac_ensure_collection,
    stac_register_item,
    stac_register_items,
)

__all__ = [
    "blob_to_mount",
    "validate",
    "create_cog",
    "generate_tiling_scheme",
    "process_tile_batch",
    "stac_ensure_collection",
    "stac_register_item",
    "stac_register_items",
]
