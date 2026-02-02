# ============================================================================
# CONFIGURATION DEFAULTS
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Core - Default configuration values
# PURPOSE: Centralized defaults for task routing, processing, timeouts
# CREATED: 31 JAN 2026
# ============================================================================
"""
Configuration Defaults

Provides sensible defaults for various processing operations.
These can be overridden via environment variables or workflow parameters.

Design:
- Immutable dataclasses for defaults
- Environment variable overrides
- Type-safe access
"""

import os
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from enum import Enum


class QueueType(str, Enum):
    """Queue types for task routing."""
    FUNCTION_APP = "functionapp-tasks"
    CONTAINER_LIGHT = "container-tasks"
    CONTAINER_HEAVY = "container-tasks-heavy"


@dataclass(frozen=True)
class TaskRoutingDefaults:
    """
    Defaults for task routing decisions.

    Controls when tasks go to Function App vs Container workers.
    """
    # Size thresholds (bytes)
    fa_max_size_bytes: int = 100 * 1024 * 1024  # 100 MB - FA limit
    light_max_size_bytes: int = 1024 * 1024 * 1024  # 1 GB - light container
    heavy_max_size_bytes: int = 50 * 1024 * 1024 * 1024  # 50 GB - heavy container

    # Default queues by handler type
    default_queue: str = QueueType.FUNCTION_APP.value
    raster_queue: str = QueueType.CONTAINER_LIGHT.value
    vector_queue: str = QueueType.FUNCTION_APP.value
    heavy_queue: str = QueueType.CONTAINER_HEAVY.value

    # Memory thresholds (MB) for routing
    fa_max_memory_mb: int = 1536
    light_max_memory_mb: int = 8192
    heavy_max_memory_mb: int = 32768

    def get_queue_for_size(self, size_bytes: int) -> str:
        """Determine queue based on file size."""
        if size_bytes <= self.fa_max_size_bytes:
            return QueueType.FUNCTION_APP.value
        elif size_bytes <= self.light_max_size_bytes:
            return QueueType.CONTAINER_LIGHT.value
        else:
            return QueueType.CONTAINER_HEAVY.value

    @classmethod
    def from_env(cls) -> "TaskRoutingDefaults":
        """Create from environment variables."""
        return cls(
            fa_max_size_bytes=int(os.getenv("FA_MAX_SIZE_BYTES", 100 * 1024 * 1024)),
            light_max_size_bytes=int(os.getenv("LIGHT_MAX_SIZE_BYTES", 1024 * 1024 * 1024)),
            heavy_max_size_bytes=int(os.getenv("HEAVY_MAX_SIZE_BYTES", 50 * 1024 * 1024 * 1024)),
        )


@dataclass(frozen=True)
class RasterDefaults:
    """
    Defaults for raster processing.

    Controls COG creation, tiling, compression.
    """
    # COG settings
    cog_blocksize: int = 512
    cog_overview_resampling: str = "average"
    cog_compression: str = "deflate"
    cog_compression_level: int = 6
    cog_predictor: int = 2  # Horizontal differencing

    # Tiling thresholds
    tile_size_threshold_bytes: int = 500 * 1024 * 1024  # 500 MB
    tile_dimension_threshold: int = 10000  # pixels
    max_tile_dimension: int = 4096

    # Supported formats
    supported_formats: tuple = ("GTiff", "COG", "JP2OpenJPEG", "PNG", "JPEG")

    # Default CRS
    default_target_crs: str = "EPSG:4326"
    web_mercator_crs: str = "EPSG:3857"

    @classmethod
    def from_env(cls) -> "RasterDefaults":
        """Create from environment variables."""
        return cls(
            cog_blocksize=int(os.getenv("COG_BLOCKSIZE", 512)),
            cog_compression=os.getenv("COG_COMPRESSION", "deflate"),
            cog_compression_level=int(os.getenv("COG_COMPRESSION_LEVEL", 6)),
        )


@dataclass(frozen=True)
class VectorDefaults:
    """
    Defaults for vector processing.

    Controls chunking, SRID handling, uploads.
    """
    # Chunking
    chunk_size: int = 10000  # rows per chunk
    max_chunk_size: int = 50000
    min_chunk_size: int = 1000

    # SRID
    default_srid: int = 4326
    target_srid: int = 4326

    # Upload settings
    batch_size: int = 1000  # rows per INSERT batch
    use_copy: bool = True  # Use COPY instead of INSERT

    # Validation
    validate_geometry: bool = True
    fix_invalid_geometry: bool = True

    # Supported formats
    supported_formats: tuple = (
        "GeoJSON",
        "GPKG",
        "Shapefile",
        "GeoPackage",
        "FlatGeobuf",
        "Parquet",
    )

    @classmethod
    def from_env(cls) -> "VectorDefaults":
        """Create from environment variables."""
        return cls(
            chunk_size=int(os.getenv("VECTOR_CHUNK_SIZE", 10000)),
            batch_size=int(os.getenv("VECTOR_BATCH_SIZE", 1000)),
            default_srid=int(os.getenv("VECTOR_DEFAULT_SRID", 4326)),
        )


@dataclass(frozen=True)
class STACDefaults:
    """
    Defaults for STAC metadata.

    Controls STAC item/collection creation.
    """
    # STAC version
    stac_version: str = "1.0.0"
    stac_extensions: tuple = (
        "https://stac-extensions.github.io/projection/v1.1.0/schema.json",
        "https://stac-extensions.github.io/raster/v1.1.0/schema.json",
    )

    # Default collection
    default_collection_id: str = "rmhgeo-data"

    # Asset roles
    data_role: str = "data"
    overview_role: str = "overview"
    thumbnail_role: str = "thumbnail"
    metadata_role: str = "metadata"

    # Media types
    geotiff_media_type: str = "image/tiff; application=geotiff; profile=cloud-optimized"
    geojson_media_type: str = "application/geo+json"
    geopackage_media_type: str = "application/geopackage+sqlite3"

    # Catalog base URL
    catalog_base_url: str = ""

    @classmethod
    def from_env(cls) -> "STACDefaults":
        """Create from environment variables."""
        return cls(
            stac_version=os.getenv("STAC_VERSION", "1.0.0"),
            default_collection_id=os.getenv("STAC_DEFAULT_COLLECTION", "rmhgeo-data"),
            catalog_base_url=os.getenv("STAC_CATALOG_URL", ""),
        )


@dataclass(frozen=True)
class TimeoutDefaults:
    """
    Defaults for task timeouts.

    Per-handler-type timeout settings.
    """
    # Default timeout (seconds)
    default_timeout: int = 3600  # 1 hour

    # Handler-specific timeouts
    handler_timeouts: Dict[str, int] = field(default_factory=lambda: {
        # Light operations
        "raster_validate": 300,  # 5 min
        "vector_validate": 300,
        "extract_stac_metadata": 300,

        # Medium operations
        "create_cog": 1800,  # 30 min
        "vector_prepare": 1800,
        "vector_upload_chunk": 900,  # 15 min

        # Heavy operations
        "raster_process_complete": 7200,  # 2 hours
        "vector_docker_complete": 7200,
        "fathom_band_stack": 3600,
        "fathom_spatial_merge": 7200,

        # Very heavy
        "h3_raster_zonal": 14400,  # 4 hours
    })

    # Timeout multiplier for retries
    retry_timeout_multiplier: float = 1.5

    def get_timeout(self, handler: str, retry_count: int = 0) -> int:
        """Get timeout for a handler, with retry multiplier."""
        base_timeout = self.handler_timeouts.get(handler, self.default_timeout)
        if retry_count > 0:
            return int(base_timeout * (self.retry_timeout_multiplier ** retry_count))
        return base_timeout

    @classmethod
    def from_env(cls) -> "TimeoutDefaults":
        """Create from environment variables."""
        return cls(
            default_timeout=int(os.getenv("DEFAULT_TIMEOUT_SECONDS", 3600)),
        )


# ============================================================================
# GLOBAL DEFAULTS INSTANCE
# ============================================================================

@dataclass
class Defaults:
    """Container for all default configurations."""
    routing: TaskRoutingDefaults = field(default_factory=TaskRoutingDefaults)
    raster: RasterDefaults = field(default_factory=RasterDefaults)
    vector: VectorDefaults = field(default_factory=VectorDefaults)
    stac: STACDefaults = field(default_factory=STACDefaults)
    timeouts: TimeoutDefaults = field(default_factory=TimeoutDefaults)

    @classmethod
    def from_env(cls) -> "Defaults":
        """Create all defaults from environment variables."""
        return cls(
            routing=TaskRoutingDefaults.from_env(),
            raster=RasterDefaults.from_env(),
            vector=VectorDefaults.from_env(),
            stac=STACDefaults.from_env(),
            timeouts=TimeoutDefaults.from_env(),
        )


_defaults: Optional[Defaults] = None


def get_defaults() -> Defaults:
    """Get global defaults instance."""
    global _defaults
    if _defaults is None:
        _defaults = Defaults.from_env()
    return _defaults


def reset_defaults() -> None:
    """Reset defaults (for testing)."""
    global _defaults
    _defaults = None


# ============================================================================
# EXPORTS
# ============================================================================

__all__ = [
    "QueueType",
    "TaskRoutingDefaults",
    "RasterDefaults",
    "VectorDefaults",
    "STACDefaults",
    "TimeoutDefaults",
    "Defaults",
    "get_defaults",
    "reset_defaults",
]
