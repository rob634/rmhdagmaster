# ============================================================================
# TILING HANDLERS
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Handler - Tiling nodes
# PURPOSE: Generate tiling scheme and process tiles with checkpointing
# CREATED: 31 JAN 2026
# ============================================================================
"""
Tiling Handlers

Handlers for the tiled output path:
- generate_tiling_scheme: Calculate tile grid for large outputs
- process_tile_batch: Process all tiles with per-tile checkpointing

Source to copy from rmhgeoapi:
- /services/tiling_scheme.py: generate_tiling_scheme()
- /services/tiling_extraction.py: extract_tile()
- /services/raster_cog.py: create_cog() (reused per tile)

Architecture note:
This is Option B - internal loop with checkpointing, NOT fan-out.
The handler processes tiles sequentially with checkpoint after each,
allowing resume from last completed tile if interrupted.
"""

import os
import logging
from typing import Any, Dict, List
from datetime import datetime

from handlers.registry import register_handler, HandlerContext, HandlerResult

logger = logging.getLogger(__name__)


@register_handler("raster.generate_tiling_scheme", queue="container-tasks", timeout_seconds=600)
async def generate_tiling_scheme(ctx: HandlerContext) -> HandlerResult:
    """
    Generate tiling scheme for large raster output.

    Params:
        local_path: Path to source raster on mounted storage
        validation: Validation output with bounds, CRS, etc.
        tile_size: Tile size in pixels (auto-calculated if None)
        tile_overlap: Overlap in pixels for seamless mosaics

    Output:
        tiles: List of tile specs [{row, col, bounds, pixel_bounds}, ...]
        tile_count: Total number of tiles
        grid_dimensions: {rows, cols}
        tile_size: Actual tile size used
        tile_overlap: Overlap used
    """
    local_path = ctx.params["local_path"]
    validation = ctx.params["validation"]
    tile_size = ctx.params.get("tile_size")
    tile_overlap = ctx.params.get("tile_overlap", 512)

    logger.info(f"Generating tiling scheme for: {local_path}")

    # Extract validation data
    shape = validation["shape"]  # [height, width]
    bounds = validation["bounds"]  # [minx, miny, maxx, maxy]

    height, width = shape

    # Auto-calculate tile size if not specified
    # Target: tiles around 256MB compressed
    if tile_size is None:
        # Rough heuristic: aim for ~4000x4000 pixel tiles
        tile_size = 4096

    # Calculate grid dimensions
    cols = (width + tile_size - tile_overlap - 1) // (tile_size - tile_overlap)
    rows = (height + tile_size - tile_overlap - 1) // (tile_size - tile_overlap)

    # Ensure at least 1x1 grid
    cols = max(1, cols)
    rows = max(1, rows)

    # Calculate geographic bounds per pixel
    minx, miny, maxx, maxy = bounds
    pixel_width = (maxx - minx) / width
    pixel_height = (maxy - miny) / height

    # Generate tile specs
    tiles = []
    for row in range(rows):
        for col in range(cols):
            # Pixel bounds (with overlap)
            px_min_x = col * (tile_size - tile_overlap)
            px_min_y = row * (tile_size - tile_overlap)
            px_max_x = min(px_min_x + tile_size, width)
            px_max_y = min(px_min_y + tile_size, height)

            # Geographic bounds
            geo_min_x = minx + px_min_x * pixel_width
            geo_min_y = miny + px_min_y * pixel_height
            geo_max_x = minx + px_max_x * pixel_width
            geo_max_y = miny + px_max_y * pixel_height

            tiles.append({
                "row": row,
                "col": col,
                "tile_id": f"tile_r{row}_c{col}",
                "pixel_bounds": [px_min_x, px_min_y, px_max_x, px_max_y],
                "geo_bounds": [geo_min_x, geo_min_y, geo_max_x, geo_max_y],
            })

    logger.info(f"Tiling scheme: {rows}x{cols} = {len(tiles)} tiles")

    return HandlerResult.success_result({
        "tiles": tiles,
        "tile_count": len(tiles),
        "grid_dimensions": {"rows": rows, "cols": cols},
        "tile_size": tile_size,
        "tile_overlap": tile_overlap,
        "source_shape": shape,
        "source_bounds": bounds,
    })


@register_handler("raster.process_tile_batch", queue="container-tasks", timeout_seconds=86400)
async def process_tile_batch(ctx: HandlerContext) -> HandlerResult:
    """
    Process all tiles with per-tile checkpointing.

    This is Option B architecture - sequential processing with checkpoints.
    NOT fan-out to Service Bus.

    Params:
        local_path: Path to source raster on mounted storage
        validation: Validation output
        tiling_scheme: Output from generate_tiling_scheme
        target_crs: Target CRS for reprojection
        output_tier: COG tier
        output_folder: Optional subfolder in output container

    Output:
        tile_cogs: List of output COG paths [{tile_id, cog_blob, size_mb}, ...]
        cog_container: Output container name
        tiles_completed: Count of completed tiles
        total_size_mb: Total output size
        processing_seconds: Total time taken
    """
    local_path = ctx.params["local_path"]
    validation = ctx.params["validation"]
    tiling_scheme = ctx.params["tiling_scheme"]
    target_crs = ctx.params.get("target_crs", "EPSG:4326")
    output_tier = ctx.params.get("output_tier", "analysis")
    output_folder = ctx.params.get("output_folder", "")

    tiles = tiling_scheme["tiles"]
    tile_count = tiling_scheme["tile_count"]

    logger.info(f"Processing {tile_count} tiles from: {local_path}")
    start_time = datetime.utcnow()

    # =========================================================================
    # Checkpoint: Resume from last completed tile
    # =========================================================================
    completed_tiles = []
    start_index = 0

    if ctx.checkpoint:
        checkpoint_data = ctx.checkpoint.data or {}
        completed_tiles = checkpoint_data.get("completed_tiles", [])
        start_index = checkpoint_data.get("next_index", 0)
        logger.info(f"Resuming from tile {start_index}/{tile_count}")

    # =========================================================================
    # Process tiles sequentially with checkpoints
    # =========================================================================
    mount_base = os.environ.get("DAG_WORKER_ETL_MOUNT_PATH", "/mnt/etl")
    cog_container = "silver-cogs"
    tile_cogs = list(completed_tiles)  # Copy already completed

    for i, tile in enumerate(tiles[start_index:], start=start_index):
        tile_id = tile["tile_id"]
        logger.info(f"Processing tile {i + 1}/{tile_count}: {tile_id}")

        # Report progress
        if ctx.progress:
            ctx.progress.update(
                current=i + 1,
                total=tile_count,
                message=f"Processing {tile_id}",
            )

        # ---------------------------------------------------------------------
        # Extract tile from source
        # ---------------------------------------------------------------------
        # TODO: Use GDAL windowed read
        # from rasterio import open as rio_open
        # from rasterio.windows import Window
        #
        # px_bounds = tile["pixel_bounds"]
        # window = Window(
        #     col_off=px_bounds[0],
        #     row_off=px_bounds[1],
        #     width=px_bounds[2] - px_bounds[0],
        #     height=px_bounds[3] - px_bounds[1],
        # )
        #
        # with rio_open(local_path) as src:
        #     tile_data = src.read(window=window)
        #     tile_transform = src.window_transform(window)
        #     tile_crs = src.crs
        #
        # # Write tile to temp file
        # tile_local_path = os.path.join(mount_base, "tiles", f"{tile_id}.tif")
        # os.makedirs(os.path.dirname(tile_local_path), exist_ok=True)
        # _write_tile(tile_local_path, tile_data, tile_transform, tile_crs)

        tile_local_path = os.path.join(mount_base, "tiles", f"{tile_id}.tif")

        # ---------------------------------------------------------------------
        # Create COG for tile
        # ---------------------------------------------------------------------
        # TODO: Reuse cog_translate logic
        # cog_translate(tile_local_path, cog_local_path, ...)

        cog_filename = f"{tile_id}_cog.tif"
        cog_blob = os.path.join(output_folder, cog_filename) if output_folder else cog_filename
        cog_local_path = os.path.join(mount_base, "output", cog_filename)

        # Placeholder size
        size_mb = 50.0  # Placeholder

        # ---------------------------------------------------------------------
        # Upload tile COG to blob storage
        # ---------------------------------------------------------------------
        # TODO: Upload to blob
        # await repo.upload_file(cog_local_path, cog_blob, cog_container)

        tile_cogs.append({
            "tile_id": tile_id,
            "cog_blob": cog_blob,
            "cog_local_path": cog_local_path,
            "size_mb": size_mb,
            "row": tile["row"],
            "col": tile["col"],
            "geo_bounds": tile["geo_bounds"],
        })

        # ---------------------------------------------------------------------
        # Save checkpoint after each tile
        # ---------------------------------------------------------------------
        if ctx.checkpoint:
            await ctx.checkpoint.save(
                phase=f"tile_{i + 1}",
                data={
                    "completed_tiles": tile_cogs,
                    "next_index": i + 1,
                },
            )

        # Clean up temp tile file
        if os.path.exists(tile_local_path):
            os.remove(tile_local_path)

    # =========================================================================
    # Final summary
    # =========================================================================
    end_time = datetime.utcnow()
    processing_seconds = (end_time - start_time).total_seconds()
    total_size_mb = sum(t["size_mb"] for t in tile_cogs)

    logger.info(
        f"Tile processing complete: {len(tile_cogs)} tiles, "
        f"{total_size_mb:.1f} MB total, {processing_seconds:.1f}s"
    )

    return HandlerResult.success_result({
        "tile_cogs": tile_cogs,
        "cog_container": cog_container,
        "tiles_completed": len(tile_cogs),
        "total_size_mb": total_size_mb,
        "processing_seconds": processing_seconds,
        "grid_dimensions": tiling_scheme["grid_dimensions"],
    })
