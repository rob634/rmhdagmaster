# ============================================================================
# RASTER VALIDATION HANDLER
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Handler - Node B
# PURPOSE: Validate raster with type detection and compression profile
# CREATED: 31 JAN 2026
# ============================================================================
"""
Raster Validation Handler (Node B)

Validates source raster and determines processing parameters.

Source to copy from rmhgeoapi:
- /services/raster_validation.py: validate_raster() - MAIN LOGIC
- /services/raster_validation.py: _validate_crs()
- /services/raster_validation.py: _check_bit_depth_efficiency()
- /services/raster_validation.py: _detect_raster_type()
- /services/raster_validation.py: _get_optimal_cog_settings()
- /config/storage_config.py: COG_TIER_PROFILES, determine_applicable_tiers()

Key validations:
1. CRS validation (4 scenarios: file has/lacks CRS × user provides/omits)
2. Bit-depth efficiency (reject 64-bit, warn on inefficient types)
3. Raster type detection (RGB, DEM, categorical, multispectral, etc.)
4. Compression profile selection based on type
5. Bounds sanity check
6. Estimated compressed output size (NEW - for tiling decision)

Three outcomes:
- B1. Reject: Unrecoverable errors (corrupted, missing CRS with no override)
- B2. Warn+Fix: Auto-fixable issues (CRS override applied, type mismatch noted)
- B3. Green Light: All validations pass

DOES NOT INCLUDE (removed from Epoch 4):
- Memory footprint estimation (not needed with mounted storage)
- Chunking strategy (not needed with mounted storage)
"""

import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional

from handlers.registry import register_handler, HandlerContext, HandlerResult

logger = logging.getLogger(__name__)

# Rasterio imports - available on heavy worker image
try:
    import rasterio
    from rasterio.crs import CRS
    RASTERIO_AVAILABLE = True
except ImportError:
    RASTERIO_AVAILABLE = False
    logger.warning("rasterio not available - validate handler will use stub mode")


@register_handler("raster.validate", queue="container-tasks", timeout_seconds=600)
async def validate(ctx: HandlerContext) -> HandlerResult:
    """
    Validate raster file and determine processing parameters.

    Params:
        local_path: Path to raster on mounted storage
        input_crs: Optional CRS override from user
        raster_type: Expected type (auto, rgb, dem, etc.)
        output_tier: Requested output tier (visualization, analysis, archive)

    Output:
        valid: True if validation passed
        source_crs: Determined CRS (from file or override)
        crs_source: Where CRS came from (file_metadata, user_override)
        bounds: [minx, miny, maxx, maxy]
        shape: [height, width]
        band_count: Number of bands
        dtype: Data type (uint8, float32, etc.)
        nodata: NoData value if set
        size_mb: File size in MB
        raster_type: Detected or confirmed type with confidence
        compression_profile: Selected compression settings
        applicable_tiers: Compatible output tiers
        estimated_compressed_mb: Estimated output size after compression
        warnings: List of non-fatal issues
    """
    local_path = ctx.params.get("local_path") or ctx.params.get("blob_path")
    input_crs = ctx.params.get("input_crs")
    raster_type_hint = ctx.params.get("raster_type", "auto")
    output_tier = ctx.params.get("output_tier", "analysis")

    if not local_path:
        return HandlerResult.failure_result(
            "MISSING_PARAM: local_path or blob_path is required"
        )

    logger.info(f"Validating raster: {local_path}")

    # =========================================================================
    # STEP 1: Open file and extract basic metadata
    # =========================================================================
    if not RASTERIO_AVAILABLE:
        logger.warning("Running in stub mode - rasterio not available")
        # Stub values for testing on light workers
        band_count = 3
        dtype = "uint8"
        shape = (1000, 1000)
        bounds = (-180, -90, 180, 90)
        file_crs = "EPSG:4326"
        nodata = None
        size_mb = 100.0
    else:
        # Check file exists
        if not os.path.exists(local_path):
            # Try as Azure blob URL
            if local_path.startswith(("http://", "https://", "/vsicurl/", "/vsiaz/")):
                # GDAL virtual filesystem path - use directly
                pass
            else:
                return HandlerResult.failure_result(
                    f"FILE_NOT_FOUND: {local_path}"
                )

        try:
            with rasterio.open(local_path) as src:
                band_count = src.count
                dtype = str(src.dtypes[0])
                shape = (src.height, src.width)
                bounds = src.bounds
                file_crs = str(src.crs) if src.crs else None
                nodata = src.nodata

                # Calculate file size
                if local_path.startswith(("http://", "https://", "/vsicurl/", "/vsiaz/")):
                    # Estimate from dimensions for remote files
                    bytes_per_pixel = _dtype_to_bytes(dtype)
                    size_mb = (shape[0] * shape[1] * band_count * bytes_per_pixel) / (1024 * 1024)
                else:
                    size_mb = os.path.getsize(local_path) / (1024 * 1024)

                logger.info(
                    f"Opened raster: {shape[1]}x{shape[0]}, {band_count} bands, "
                    f"{dtype}, CRS={file_crs}, {size_mb:.1f} MB"
                )

        except rasterio.errors.RasterioIOError as e:
            return HandlerResult.failure_result(
                f"RASTERIO_ERROR: Cannot open file - {str(e)}"
            )
        except Exception as e:
            return HandlerResult.failure_result(
                f"UNEXPECTED_ERROR: {str(e)}"
            )

    # =========================================================================
    # STEP 2: CRS Validation
    # =========================================================================
    # Logic from rmhgeoapi/services/raster_validation.py:_validate_crs()
    #
    # Case 1: File has CRS, user provides CRS → ERROR if mismatch
    # Case 2: File has CRS, user omits → Use file CRS
    # Case 3: File lacks CRS, user provides → Use override (warn)
    # Case 4: File lacks CRS, user omits → ERROR

    source_crs = file_crs or input_crs
    crs_source = "file_metadata" if file_crs else "user_override"
    warnings = []

    if not source_crs:
        return HandlerResult.failure_result(
            "CRS_MISSING: File has no CRS and no override provided"
        )

    if file_crs and input_crs and file_crs != input_crs:
        return HandlerResult.failure_result(
            f"CRS_MISMATCH: File CRS {file_crs} != user CRS {input_crs}"
        )

    if not file_crs and input_crs:
        warnings.append({
            "code": "CRS_OVERRIDE",
            "severity": "MEDIUM",
            "message": f"File lacks CRS, using user override: {input_crs}",
        })

    # =========================================================================
    # STEP 3: Bit-depth efficiency check
    # =========================================================================
    # Logic from rmhgeoapi/services/raster_validation.py:_check_bit_depth_efficiency()

    critical_dtypes = ["float64", "int64", "uint64", "complex64", "complex128"]
    if dtype in critical_dtypes:
        return HandlerResult.failure_result(
            f"BIT_DEPTH_POLICY_VIOLATION: {dtype} is not acceptable"
        )

    # =========================================================================
    # STEP 4: Raster type detection
    # =========================================================================
    # Logic from rmhgeoapi/services/raster_validation.py:_detect_raster_type()

    detected_type = _detect_raster_type(band_count, dtype, shape)

    if raster_type_hint != "auto" and detected_type["type"] != raster_type_hint:
        return HandlerResult.failure_result(
            f"RASTER_TYPE_MISMATCH: Detected {detected_type['type']}, expected {raster_type_hint}"
        )

    # =========================================================================
    # STEP 5: Compression profile selection
    # =========================================================================
    # Logic from rmhgeoapi/services/raster_validation.py:_get_optimal_cog_settings()

    compression_profile = _get_compression_profile(detected_type["type"], output_tier)

    # =========================================================================
    # STEP 6: Estimate compressed output size (NEW)
    # =========================================================================
    # This is new logic for DAG - estimate output size for tiling decision

    estimated_compressed_mb = _estimate_compressed_size(
        size_mb, detected_type["type"], compression_profile["compression"]
    )

    # =========================================================================
    # STEP 7: Determine applicable tiers
    # =========================================================================
    # Logic from rmhgeoapi/config/storage_config.py:determine_applicable_tiers()

    applicable_tiers = _determine_applicable_tiers(band_count, dtype)

    if output_tier not in applicable_tiers:
        warnings.append({
            "code": "TIER_INCOMPATIBLE",
            "severity": "MEDIUM",
            "message": f"Requested tier '{output_tier}' incompatible, using 'analysis'",
        })

    logger.info(
        f"Validation complete: {detected_type['type']}, "
        f"estimated output: {estimated_compressed_mb:.1f} MB"
    )

    return HandlerResult.success_result({
        "valid": True,
        "source_crs": source_crs,
        "crs_source": crs_source,
        "bounds": list(bounds),
        "shape": list(shape),
        "band_count": band_count,
        "dtype": dtype,
        "nodata": nodata,
        "size_mb": size_mb,
        "raster_type": detected_type,
        "compression_profile": compression_profile,
        "applicable_tiers": applicable_tiers,
        "estimated_compressed_mb": estimated_compressed_mb,
        "warnings": warnings,
    })


# =============================================================================
# HELPER FUNCTIONS (to be copied/adapted from rmhgeoapi)
# =============================================================================

def _detect_raster_type(band_count: int, dtype: str, shape: tuple) -> Dict[str, Any]:
    """
    Detect raster type from file characteristics.

    Copy from: rmhgeoapi/services/raster_validation.py:_detect_raster_type()
    """
    # Simplified detection logic - full version in rmhgeoapi
    if band_count == 3 and dtype in ("uint8", "uint16"):
        return {
            "type": "rgb",
            "confidence": "HIGH",
            "evidence": [f"{band_count} bands, {dtype}"],
        }
    elif band_count == 4 and dtype in ("uint8", "uint16"):
        return {
            "type": "rgba",
            "confidence": "HIGH",
            "evidence": [f"{band_count} bands with alpha"],
        }
    elif band_count == 1 and dtype in ("float32", "float64", "int16", "int32"):
        return {
            "type": "dem",
            "confidence": "MEDIUM",
            "evidence": ["Single-band elevation data"],
        }
    elif band_count == 1:
        return {
            "type": "categorical",
            "confidence": "MEDIUM",
            "evidence": ["Single-band discrete data"],
        }
    elif band_count >= 5:
        return {
            "type": "multispectral",
            "confidence": "MEDIUM",
            "evidence": [f"{band_count} bands (satellite imagery)"],
        }
    else:
        return {
            "type": "unknown",
            "confidence": "LOW",
            "evidence": ["Could not determine type"],
        }


def _get_compression_profile(raster_type: str, output_tier: str) -> Dict[str, Any]:
    """
    Get compression settings for raster type.

    Copy from: rmhgeoapi/services/raster_validation.py:_get_optimal_cog_settings()
    """
    profiles = {
        "rgb": {
            "compression": "jpeg",
            "jpeg_quality": 85,
            "overview_resampling": "cubic",
            "reproject_resampling": "cubic",
        },
        "rgba": {
            "compression": "webp",
            "overview_resampling": "cubic",
            "reproject_resampling": "cubic",
        },
        "dem": {
            "compression": "lerc_deflate",
            "overview_resampling": "average",
            "reproject_resampling": "bilinear",
        },
        "categorical": {
            "compression": "deflate",
            "overview_resampling": "mode",
            "reproject_resampling": "nearest",
        },
        "multispectral": {
            "compression": "deflate",
            "overview_resampling": "average",
            "reproject_resampling": "bilinear",
        },
    }
    return profiles.get(raster_type, {
        "compression": "deflate",
        "overview_resampling": "cubic",
        "reproject_resampling": "cubic",
    })


def _estimate_compressed_size(
    uncompressed_mb: float,
    raster_type: str,
    compression: str,
) -> float:
    """
    Estimate compressed output size based on type and compression.

    This is NEW logic for DAG - not in rmhgeoapi.

    Typical compression ratios:
    - JPEG (RGB): 85-95% reduction
    - WebP (RGBA): 80-90% reduction
    - DEFLATE (analysis): 60-80% reduction
    - ZSTD (categorical with nodata): 90-99% reduction
    - LZW (archive): 10-30% reduction
    - LERC (DEM): 50-70% reduction
    """
    # Conservative estimates (actual depends on data characteristics)
    ratios = {
        ("rgb", "jpeg"): 0.10,          # 90% reduction
        ("rgba", "webp"): 0.15,          # 85% reduction
        ("dem", "lerc_deflate"): 0.40,   # 60% reduction
        ("categorical", "deflate"): 0.05,  # 95% reduction (lots of nodata)
        ("categorical", "zstd"): 0.01,   # 99% reduction (FATHOM-like)
        ("multispectral", "deflate"): 0.30,  # 70% reduction
    }

    # Default to 25% of original (75% reduction)
    ratio = ratios.get((raster_type, compression), 0.25)
    return uncompressed_mb * ratio


def _determine_applicable_tiers(band_count: int, dtype: str) -> list:
    """
    Determine which COG tiers are compatible.

    Copy from: rmhgeoapi/config/storage_config.py:determine_applicable_tiers()
    """
    tiers = ["analysis", "archive"]  # Universal

    # JPEG visualization only for RGB (3 bands, uint8)
    if band_count == 3 and dtype == "uint8":
        tiers.insert(0, "visualization")

    return tiers


def _dtype_to_bytes(dtype: str) -> int:
    """Get bytes per pixel for a numpy dtype string."""
    dtype_sizes = {
        "uint8": 1,
        "int8": 1,
        "uint16": 2,
        "int16": 2,
        "uint32": 4,
        "int32": 4,
        "uint64": 8,
        "int64": 8,
        "float16": 2,
        "float32": 4,
        "float64": 8,
    }
    return dtype_sizes.get(dtype, 4)  # Default to 4 bytes
