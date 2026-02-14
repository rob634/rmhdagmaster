# ============================================================================
# BLOB COPY HANDLER
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Handler - Node A
# PURPOSE: Copy blob from Azure Blob Storage to mounted Azure Files
# CREATED: 31 JAN 2026
# ============================================================================
"""
Blob Copy Handler (Node A)

Copies source raster from blob storage to mounted file storage for processing.
Uses streaming download to avoid memory spikes with large files.

This is the first node in the raster workflow - it stages data on the
mounted filesystem where GDAL can process it with disk-based I/O.
"""

import os
import logging
from pathlib import Path
from typing import Any, Dict

from handlers.registry import register_handler, HandlerContext, HandlerResult
from infrastructure.storage import BlobRepository

logger = logging.getLogger(__name__)


@register_handler("raster.blob_to_mount", queue="container-tasks", timeout_seconds=1800)
async def blob_to_mount(ctx: HandlerContext) -> HandlerResult:
    """
    Copy blob from Azure Blob Storage to mounted Azure Files.

    Streams the blob directly to disk without loading into memory.
    Supports files of any size (tested with 25GB+ files).

    Params:
        blob_name: Source blob path (e.g., "uploads/raster.tif")
        container_name: Source container (e.g., "bronze-rasters")

    Output:
        local_path: Path on mounted storage (e.g., "/mnt/etl/raster.tif")
        size_bytes: File size after copy
        size_mb: File size in MB
        duration_seconds: Time taken for transfer
        throughput_mbps: Transfer speed in MB/s
        blob_name: Original blob name (passthrough)
        container_name: Original container (passthrough)
    """
    blob_name = ctx.params["blob_name"]
    container_name = ctx.params["container_name"]

    logger.info(f"[Node A] Starting blob copy: {container_name}/{blob_name}")

    # Determine mount path
    mount_base = os.environ.get("DAG_WORKER_ETL_MOUNT_PATH", "/mnt/etl")

    # Preserve directory structure from blob path
    local_path = os.path.join(mount_base, "input", blob_name)

    # Source data lives in bronze zone
    repo = BlobRepository.for_zone("bronze")

    # Check if blob exists first
    if not repo.blob_exists(container_name, blob_name):
        error_msg = f"Blob not found: {container_name}/{blob_name}"
        logger.error(f"[Node A] {error_msg}")
        return HandlerResult.failure_result(error_msg)

    # Get blob properties for logging
    props = repo.get_blob_properties(container_name, blob_name)
    if props.get("exists"):
        logger.info(f"[Node A] Blob size: {props.get('size_mb', 0):.2f} MB")

    # Stream blob to mount
    result = repo.stream_blob_to_mount(
        container=container_name,
        blob_path=blob_name,
        mount_path=local_path,
        chunk_size_mb=32,
        overwrite_existing=True,
    )

    if not result.get("success"):
        error_msg = result.get("error", "Unknown error during blob transfer")
        logger.error(f"[Node A] Blob copy failed: {error_msg}")
        return HandlerResult.failure_result(error_msg)

    # Verify file was written
    local_path_obj = Path(local_path)
    if not local_path_obj.exists():
        error_msg = f"File not created at expected path: {local_path}"
        logger.error(f"[Node A] {error_msg}")
        return HandlerResult.failure_result(error_msg)

    size_bytes = local_path_obj.stat().st_size
    size_mb = size_bytes / (1024 * 1024)

    logger.info(
        f"[Node A] Blob copy complete: {local_path} "
        f"({size_mb:.2f} MB in {result.get('duration_seconds', 0):.1f}s)"
    )

    return HandlerResult.success_result({
        "local_path": local_path,
        "size_bytes": size_bytes,
        "size_mb": round(size_mb, 2),
        "duration_seconds": result.get("duration_seconds", 0),
        "throughput_mbps": result.get("throughput_mbps", 0),
        "blob_name": blob_name,
        "container_name": container_name,
        "source_uri": result.get("source_uri"),
    })


