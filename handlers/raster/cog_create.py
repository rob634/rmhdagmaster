# ============================================================================
# COG CREATION HANDLER
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Handler - Node D+E
# PURPOSE: Reproject and create Cloud-Optimized GeoTIFF (combined)
# CREATED: 31 JAN 2026
# ============================================================================
"""
COG Creation Handler (Node D+E)

Creates Cloud-Optimized GeoTIFF with optional reprojection.
Uses rio-cogeo cog_translate() which does both operations in one pass.

Source to copy from rmhgeoapi:
- /services/raster_cog.py: create_cog() - MAIN LOGIC
- /services/raster_cog.py: _get_cog_profile()
- /services/blob_operations.py: stream_mount_to_blob()

Key features:
1. Single-pass reprojection + COG creation
2. Type-specific compression (from validation output)
3. Automatic overview generation
4. Upload to silver-cogs container
5. File checksum for deduplication

DOES NOT INCLUDE (removed from Epoch 4):
- Pulse thread for timeout prevention (no timeout in Docker)
- Memory tracking (not needed with mounted storage)
- In-memory mode (always use disk)
"""

import os
import hashlib
import logging
from pathlib import Path
from typing import Any, Dict
from datetime import datetime

from handlers.registry import register_handler, HandlerContext, HandlerResult
from infrastructure.storage import BlobRepository

logger = logging.getLogger(__name__)


@register_handler("raster.create_cog", queue="container-tasks", timeout_seconds=7200)
async def create_cog(ctx: HandlerContext) -> HandlerResult:
    """
    Create Cloud-Optimized GeoTIFF from source raster.

    Params:
        local_path: Path to source raster on mounted storage
        validation: Validation output with type, CRS, compression profile
        target_crs: Target CRS for reprojection (default: EPSG:4326)
        output_tier: COG tier (visualization, analysis, archive)
        output_folder: Optional subfolder in output container

    Output:
        cog_blob: Output blob path in silver-cogs
        cog_container: Output container name
        size_mb: Output file size in MB
        file_checksum: SHA-256 hash (multihash format)
        compression: Compression used
        overview_levels: Number of overview levels
        processing_seconds: Time taken
    """
    local_path = ctx.params["local_path"]
    validation = ctx.params["validation"]
    target_crs = ctx.params.get("target_crs", "EPSG:4326")
    output_tier = ctx.params.get("output_tier", "analysis")
    output_folder = ctx.params.get("output_folder", "")

    logger.info(f"Creating COG from: {local_path}")
    start_time = datetime.utcnow()

    # Extract validation data
    source_crs = validation["source_crs"]
    compression_profile = validation["compression_profile"]
    raster_type = validation["raster_type"]["type"]

    # Determine output path
    mount_base = os.environ.get("ETL_MOUNT_PATH", "/mnt/etl")
    source_basename = os.path.basename(local_path)
    cog_filename = _generate_cog_filename(source_basename)
    cog_local_path = os.path.join(mount_base, "output", cog_filename)

    os.makedirs(os.path.dirname(cog_local_path), exist_ok=True)

    # =========================================================================
    # COG Creation with cog_translate
    # =========================================================================
    # TODO: Import and use rio-cogeo
    # from rio_cogeo.cogeo import cog_translate
    # from rio_cogeo.profiles import cog_profiles
    #
    # Logic from rmhgeoapi/services/raster_cog.py:
    #
    # # Get profile from compression type
    # profile = _get_cog_profile(compression_profile)
    #
    # # Build config
    # config = {
    #     "GDAL_TIFF_INTERNAL_MASK": True,
    #     "GDAL_TIFF_OVR_BLOCKSIZE": 512,
    # }
    #
    # # Determine if reprojection needed
    # needs_reproject = source_crs != target_crs
    #
    # if needs_reproject:
    #     # cog_translate with dst_crs does both in one pass
    #     cog_translate(
    #         local_path,
    #         cog_local_path,
    #         profile,
    #         dst_crs=target_crs,
    #         resampling=compression_profile["reproject_resampling"],
    #         overview_resampling=compression_profile["overview_resampling"],
    #         config=config,
    #         overview_level=8,
    #         use_cog_driver=True,
    #     )
    # else:
    #     # No reprojection needed
    #     cog_translate(
    #         local_path,
    #         cog_local_path,
    #         profile,
    #         overview_resampling=compression_profile["overview_resampling"],
    #         config=config,
    #         overview_level=8,
    #         use_cog_driver=True,
    #     )

    logger.info(f"[Node D+E] COG created at: {cog_local_path}")

    # =========================================================================
    # Verify COG was created
    # =========================================================================
    cog_path_obj = Path(cog_local_path)
    if not cog_path_obj.exists():
        error_msg = f"COG file not created at expected path: {cog_local_path}"
        logger.error(f"[Node D+E] {error_msg}")
        return HandlerResult.failure_result(error_msg)

    size_bytes = cog_path_obj.stat().st_size
    size_mb = size_bytes / (1024 * 1024)

    # =========================================================================
    # Calculate checksum (Multihash format: 1220 prefix for SHA-256)
    # =========================================================================
    logger.info(f"[Node D+E] Calculating checksum...")
    sha256_hash = hashlib.sha256()
    with open(cog_local_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256_hash.update(chunk)
    file_checksum = f"1220{sha256_hash.hexdigest()}"

    # =========================================================================
    # Upload to blob storage (silver zone)
    # =========================================================================
    cog_container = os.environ.get("SILVER_COGS_CONTAINER", "silver-cogs")
    cog_blob = os.path.join(output_folder, cog_filename) if output_folder else cog_filename

    logger.info(f"[Node D+E] Uploading COG to {cog_container}/{cog_blob}")

    repo = BlobRepository.for_zone("silver")
    upload_result = repo.stream_mount_to_blob(
        container=cog_container,
        blob_path=cog_blob,
        mount_path=cog_local_path,
        content_type="image/tiff",
        metadata={
            "raster_type": raster_type,
            "source_crs": source_crs,
            "target_crs": target_crs,
            "compression": compression_profile["compression"],
            "checksum": file_checksum,
        },
        overwrite_existing=True,
    )

    if not upload_result.get("success"):
        error_msg = upload_result.get("error", "Unknown error during COG upload")
        logger.error(f"[Node D+E] COG upload failed: {error_msg}")
        return HandlerResult.failure_result(error_msg)

    # Calculate timing
    end_time = datetime.utcnow()
    processing_seconds = (end_time - start_time).total_seconds()

    logger.info(
        f"[Node D+E] COG complete: {cog_blob} ({size_mb:.1f} MB) "
        f"in {processing_seconds:.1f}s"
    )

    return HandlerResult.success_result({
        "cog_blob": cog_blob,
        "cog_container": cog_container,
        "cog_local_path": cog_local_path,
        "size_bytes": size_bytes,
        "size_mb": round(size_mb, 2),
        "file_checksum": file_checksum,
        "compression": compression_profile["compression"],
        "overview_levels": 8,
        "source_crs": source_crs,
        "target_crs": target_crs,
        "raster_type": raster_type,
        "processing_seconds": round(processing_seconds, 2),
        "upload_throughput_mbps": upload_result.get("throughput_mbps", 0),
    })


def _generate_cog_filename(source_basename: str) -> str:
    """Generate COG filename from source filename."""
    name, ext = os.path.splitext(source_basename)
    return f"{name}_cog.tif"


def _get_cog_profile(compression_profile: Dict[str, Any]) -> str:
    """
    Get rio-cogeo profile name from compression settings.

    Copy from: rmhgeoapi/services/raster_cog.py:_get_cog_profile()
    """
    compression = compression_profile.get("compression", "deflate")

    profile_map = {
        "jpeg": "jpeg",
        "webp": "webp",
        "deflate": "deflate",
        "lzw": "lzw",
        "zstd": "zstd",
        "lerc": "lerc",
        "lerc_deflate": "lerc_deflate",
        "lerc_zstd": "lerc_zstd",
    }

    return profile_map.get(compression, "deflate")
