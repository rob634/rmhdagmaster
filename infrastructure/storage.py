# ============================================================================
# BLOB STORAGE INFRASTRUCTURE
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Infrastructure - Azure Blob Storage operations
# PURPOSE: Streaming blob operations for DAG workers
# CREATED: 31 JAN 2026
# ============================================================================
"""
Blob Storage Infrastructure

Provides BlobRepository for Azure Blob Storage operations:
- stream_blob_to_mount: Download blob to mounted filesystem (no memory spike)
- stream_mount_to_blob: Upload file from mount to blob storage
- get_blob_sas_url: Generate SAS URLs for external access
- blob_exists: Check if blob exists
- delete_blob: Delete a blob

Uses DefaultAzureCredential for authentication (works with Managed Identity).
Implements connection pooling with thread-safe container client caching.

Adapted from rmhgeoapi/infrastructure/blob.py for DAG orchestrator.
"""

import os
import time
import logging
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)


# ============================================================================
# BLOB REPOSITORY
# ============================================================================

class BlobRepository:
    """
    Azure Blob Storage repository with streaming operations.

    Multi-instance singleton pattern: one instance per storage account.
    Uses connection pooling with thread-safe container client caching.

    Usage:
        # Get repository for a storage account
        repo = BlobRepository(account_name="rmhazuregeo")

        # Or use zone-based factory
        repo = BlobRepository.for_zone("bronze")

        # Stream blob to mounted filesystem
        result = repo.stream_blob_to_mount(
            container="bronze-rasters",
            blob_path="uploads/file.tif",
            mount_path="/mnt/etl/file.tif"
        )
    """

    # Multi-instance singleton storage
    _instances: Dict[str, "BlobRepository"] = {}
    _instances_lock = threading.Lock()

    def __new__(cls, account_name: Optional[str] = None, **kwargs):
        """Multi-instance singleton: one instance per storage account."""
        if not account_name:
            raise ValueError(
                "BlobRepository requires an explicit account_name. "
                "Use BlobRepository.for_zone('bronze') or BlobRepository.for_zone('silver') instead."
            )

        with cls._instances_lock:
            if account_name not in cls._instances:
                instance = super().__new__(cls)
                instance._initialized = False
                cls._instances[account_name] = instance
            return cls._instances[account_name]

    def __init__(self, account_name: Optional[str] = None):
        """Initialize blob repository for storage account."""
        if getattr(self, "_initialized", False):
            return

        self.account_name = account_name

        # Container client cache with thread-safe access
        self._container_clients: Dict[str, Any] = {}
        self._container_clients_lock = threading.Lock()

        # Lazy initialization of Azure clients
        self._blob_service = None
        self._credential = None

        self._initialized = True
        logger.info(f"BlobRepository initialized for account: {self.account_name}")

    # ========================================================================
    # AZURE CLIENT INITIALIZATION
    # ========================================================================

    def _get_credential(self):
        """Get Azure credential (lazy initialization)."""
        if self._credential is None:
            try:
                # Check for User-Assigned Managed Identity
                client_id = os.environ.get("AZURE_CLIENT_ID") or os.environ.get(
                    "DAG_DB_IDENTITY_CLIENT_ID"
                )

                if client_id:
                    from azure.identity import ManagedIdentityCredential
                    self._credential = ManagedIdentityCredential(client_id=client_id)
                    logger.debug(f"ManagedIdentityCredential initialized with client_id")
                else:
                    from azure.identity import DefaultAzureCredential
                    self._credential = DefaultAzureCredential()
                    logger.debug("DefaultAzureCredential initialized")
            except ImportError:
                raise ImportError(
                    "azure-identity package required. "
                    "Install with: pip install azure-identity"
                )
        return self._credential

    def _get_blob_service(self):
        """Get BlobServiceClient (lazy initialization)."""
        if self._blob_service is None:
            try:
                from azure.storage.blob import BlobServiceClient
                account_url = f"https://{self.account_name}.blob.core.windows.net"
                self._blob_service = BlobServiceClient(
                    account_url=account_url,
                    credential=self._get_credential(),
                )
                logger.debug(f"BlobServiceClient initialized for {account_url}")
            except ImportError:
                raise ImportError(
                    "azure-storage-blob package required. "
                    "Install with: pip install azure-storage-blob"
                )
        return self._blob_service

    def _get_container_client(self, container: str, validate: bool = False):
        """
        Get or create cached container client.

        Thread-safe with double-checked locking pattern.
        """
        # Fast path: check without lock
        if container in self._container_clients:
            return self._container_clients[container]

        # Slow path: acquire lock for creation
        with self._container_clients_lock:
            # Double-check after lock
            if container in self._container_clients:
                return self._container_clients[container]

            # Create new container client
            container_client = self._get_blob_service().get_container_client(container)

            # Validate if requested
            if validate:
                try:
                    container_client.get_container_properties()
                except Exception as e:
                    raise ValueError(
                        f"Container '{container}' does not exist in "
                        f"storage account '{self.account_name}': {e}"
                    )

            self._container_clients[container] = container_client
            logger.debug(f"Created container client for: {container}")
            return container_client

    # ========================================================================
    # ZONE-BASED FACTORY
    # ========================================================================

    @classmethod
    def for_zone(cls, zone: str) -> "BlobRepository":
        """
        Get BlobRepository instance for a storage zone.

        Args:
            zone: Storage zone name (bronze, silver, silverext, gold)

        Returns:
            BlobRepository configured for that zone's storage account
        """
        zone_env_map = {
            "bronze": "DAG_STORAGE_BRONZE_ACCOUNT",
            "silver": "DAG_STORAGE_SILVER_ACCOUNT",
            "silverext": "DAG_STORAGE_SILVEREXT_ACCOUNT",
            "gold": "DAG_STORAGE_GOLD_ACCOUNT",
        }

        env_var = zone_env_map.get(zone.lower())
        if not env_var:
            raise ValueError(f"Unknown zone: {zone}. Valid: {list(zone_env_map.keys())}")

        account_name = os.environ.get(env_var)
        if not account_name:
            raise ValueError(
                f"Storage zone '{zone}' not configured. "
                f"Set environment variable {env_var} to the storage account name."
            )

        return cls(account_name=account_name)

    # ========================================================================
    # STREAMING OPERATIONS
    # ========================================================================

    def stream_blob_to_mount(
        self,
        container: str,
        blob_path: str,
        mount_path: str,
        chunk_size_mb: int = 32,
        overwrite_existing: bool = True,
    ) -> Dict[str, Any]:
        """
        Stream blob directly to mounted filesystem without loading into memory.

        This is the primary method for getting data from blob storage onto
        the mounted filesystem so GDAL can process it with disk-based I/O.

        Args:
            container: Source container name
            blob_path: Source blob path within container
            mount_path: Destination path on mounted filesystem (must be absolute)
            chunk_size_mb: Download chunk size in MB (default 32)
            overwrite_existing: Overwrite if file exists (default True)

        Returns:
            Dict with operation results:
            - success: bool
            - bytes_transferred: int
            - duration_seconds: float
            - throughput_mbps: float
            - source_uri: str
            - destination_uri: str
            - error: str (if failed)
        """
        mount_path_obj = Path(mount_path)
        source_uri = f"blob://{container}/{blob_path}"
        dest_uri = f"file://{mount_path}"

        # Validate mount_path
        if not mount_path_obj.is_absolute():
            raise ValueError(f"mount_path must be absolute, got: {mount_path}")

        # Create parent directories
        mount_path_obj.parent.mkdir(parents=True, exist_ok=True)

        if mount_path_obj.exists() and not overwrite_existing:
            raise FileExistsError(f"File already exists: {mount_path}")

        logger.info(f"STREAM_BLOB_TO_MOUNT starting")
        logger.info(f"   Source: {source_uri}")
        logger.info(f"   Destination: {dest_uri}")
        logger.info(f"   Chunk size: {chunk_size_mb}MB")

        start_time = time.time()
        bytes_transferred = 0
        chunks_transferred = 0

        try:
            container_client = self._get_container_client(container)
            blob_client = container_client.get_blob_client(blob_path)

            # Get blob properties for size
            props = blob_client.get_blob_properties()
            blob_size = props.size
            blob_size_mb = blob_size / (1024 * 1024)
            logger.info(f"   Blob size: {blob_size_mb:.2f}MB")

            # Stream download to file
            download_stream = blob_client.download_blob()

            with open(mount_path, "wb") as f:
                for chunk in download_stream.chunks():
                    f.write(chunk)
                    chunk_len = len(chunk)
                    bytes_transferred += chunk_len
                    chunks_transferred += 1

                    # Log progress every ~100MB
                    progress_interval = max(1, int(100 / chunk_size_mb))
                    if chunks_transferred % progress_interval == 0:
                        pct = (bytes_transferred / blob_size * 100) if blob_size > 0 else 0
                        logger.info(
                            f"   Progress: {bytes_transferred / (1024*1024):.1f}MB / "
                            f"{blob_size_mb:.1f}MB ({pct:.0f}%)"
                        )

            duration = time.time() - start_time
            throughput = (bytes_transferred / (1024 * 1024)) / duration if duration > 0 else 0

            logger.info(f"STREAM_BLOB_TO_MOUNT complete")
            logger.info(f"   Transferred: {bytes_transferred / (1024*1024):.2f}MB in {duration:.1f}s")
            logger.info(f"   Throughput: {throughput:.1f}MB/s")

            return {
                "success": True,
                "operation": "blob_to_mount",
                "bytes_transferred": bytes_transferred,
                "duration_seconds": round(duration, 2),
                "throughput_mbps": round(throughput, 2),
                "chunks_transferred": chunks_transferred,
                "chunk_size_mb": chunk_size_mb,
                "source_uri": source_uri,
                "destination_uri": dest_uri,
                "local_path": mount_path,
            }

        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"STREAM_BLOB_TO_MOUNT failed: {e}")

            # Clean up partial file
            if mount_path_obj.exists():
                try:
                    mount_path_obj.unlink()
                    logger.info(f"   Cleaned up partial file: {mount_path}")
                except Exception as cleanup_error:
                    logger.warning(f"   Failed to clean up partial file: {cleanup_error}")

            return {
                "success": False,
                "operation": "blob_to_mount",
                "bytes_transferred": bytes_transferred,
                "duration_seconds": round(duration, 2),
                "throughput_mbps": 0,
                "chunks_transferred": chunks_transferred,
                "chunk_size_mb": chunk_size_mb,
                "source_uri": source_uri,
                "destination_uri": dest_uri,
                "error": str(e),
            }

    def stream_mount_to_blob(
        self,
        container: str,
        blob_path: str,
        mount_path: str,
        content_type: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
        overwrite_existing: bool = True,
    ) -> Dict[str, Any]:
        """
        Stream file from mounted filesystem to blob without loading into memory.

        This is the primary method for uploading processed data from the
        mounted filesystem back to blob storage.

        Args:
            container: Destination container name
            blob_path: Destination blob path within container
            mount_path: Source path on mounted filesystem
            content_type: Optional content type (auto-detected if not provided)
            metadata: Optional blob metadata
            overwrite_existing: Overwrite if blob exists (default True)

        Returns:
            Dict with operation results
        """
        mount_path_obj = Path(mount_path)
        source_uri = f"file://{mount_path}"
        dest_uri = f"blob://{container}/{blob_path}"

        # Validate source file
        if not mount_path_obj.exists():
            raise FileNotFoundError(f"Source file does not exist: {mount_path}")
        if mount_path_obj.is_dir():
            raise ValueError(f"mount_path must be a file, not directory: {mount_path}")

        # Auto-detect content type
        if content_type is None:
            content_type = self._detect_content_type(mount_path)

        file_size = mount_path_obj.stat().st_size
        file_size_mb = file_size / (1024 * 1024)

        logger.info(f"STREAM_MOUNT_TO_BLOB starting")
        logger.info(f"   Source: {source_uri}")
        logger.info(f"   Destination: {dest_uri}")
        logger.info(f"   File size: {file_size_mb:.2f}MB")
        logger.info(f"   Content-Type: {content_type}")

        start_time = time.time()

        try:
            from azure.storage.blob import ContentSettings

            container_client = self._get_container_client(container)
            blob_client = container_client.get_blob_client(blob_path)

            content_settings = ContentSettings(content_type=content_type)

            # Stream upload from file
            with open(mount_path, "rb") as f:
                result = blob_client.upload_blob(
                    f,
                    overwrite=overwrite_existing,
                    content_settings=content_settings,
                    metadata=metadata,
                    max_concurrency=4,
                    length=file_size,
                )

            duration = time.time() - start_time
            throughput = (file_size / (1024 * 1024)) / duration if duration > 0 else 0

            logger.info(f"STREAM_MOUNT_TO_BLOB complete")
            logger.info(f"   Transferred: {file_size_mb:.2f}MB in {duration:.1f}s")
            logger.info(f"   Throughput: {throughput:.1f}MB/s")

            return {
                "success": True,
                "operation": "mount_to_blob",
                "bytes_transferred": file_size,
                "duration_seconds": round(duration, 2),
                "throughput_mbps": round(throughput, 2),
                "source_uri": source_uri,
                "destination_uri": dest_uri,
                "blob_path": blob_path,
                "container": container,
                "etag": result.get("etag"),
                "content_type": content_type,
            }

        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"STREAM_MOUNT_TO_BLOB failed: {e}")

            return {
                "success": False,
                "operation": "mount_to_blob",
                "bytes_transferred": 0,
                "duration_seconds": round(duration, 2),
                "throughput_mbps": 0,
                "source_uri": source_uri,
                "destination_uri": dest_uri,
                "error": str(e),
            }

    # ========================================================================
    # SAS URL GENERATION
    # ========================================================================

    def get_blob_sas_url(
        self,
        container: str,
        blob_path: str,
        hours: int = 1,
        write: bool = False,
    ) -> str:
        """
        Generate blob URL with user delegation SAS token.

        Uses managed identity to generate SAS tokens without account keys.

        Args:
            container: Container name
            blob_path: Blob path
            hours: Token validity in hours (default 1)
            write: Include write permission (default False)

        Returns:
            Full blob URL with SAS token
        """
        try:
            from azure.storage.blob import BlobSasPermissions, generate_blob_sas

            blob_client = self._get_container_client(container).get_blob_client(blob_path)

            # Calculate times
            start_time = datetime.now(timezone.utc)
            expiry_time = start_time + timedelta(hours=hours)

            # Get user delegation key
            user_delegation_key = self._get_blob_service().get_user_delegation_key(
                key_start_time=start_time,
                key_expiry_time=expiry_time,
            )

            # Set permissions
            if write:
                permissions = BlobSasPermissions(read=True, write=True, create=True)
            else:
                permissions = BlobSasPermissions(read=True)

            # Generate SAS token
            sas_token = generate_blob_sas(
                account_name=self.account_name,
                container_name=container,
                blob_name=blob_path,
                user_delegation_key=user_delegation_key,
                permission=permissions,
                expiry=expiry_time,
                start=start_time,
            )

            return f"{blob_client.url}?{sas_token}"

        except Exception as e:
            logger.error(f"Failed to generate SAS URL: {e}")
            raise

    # ========================================================================
    # UTILITY METHODS
    # ========================================================================

    def blob_exists(self, container: str, blob_path: str) -> bool:
        """Check if a blob exists."""
        try:
            blob_client = self._get_container_client(container).get_blob_client(blob_path)
            blob_client.get_blob_properties()
            return True
        except Exception:
            return False

    def get_blob_properties(self, container: str, blob_path: str) -> Dict[str, Any]:
        """Get blob properties (size, content type, etc.)."""
        try:
            blob_client = self._get_container_client(container).get_blob_client(blob_path)
            props = blob_client.get_blob_properties()
            return {
                "exists": True,
                "size": props.size,
                "size_mb": props.size / (1024 * 1024),
                "content_type": props.content_settings.content_type,
                "last_modified": props.last_modified.isoformat() if props.last_modified else None,
                "etag": props.etag,
                "metadata": dict(props.metadata) if props.metadata else {},
            }
        except Exception as e:
            return {"exists": False, "error": str(e)}

    def delete_blob(self, container: str, blob_path: str) -> bool:
        """Delete a blob. Returns True if deleted, False if not found."""
        try:
            blob_client = self._get_container_client(container).get_blob_client(blob_path)
            blob_client.delete_blob()
            logger.info(f"Deleted blob: {container}/{blob_path}")
            return True
        except Exception as e:
            logger.warning(f"Failed to delete blob {container}/{blob_path}: {e}")
            return False

    def list_blobs(
        self,
        container: str,
        prefix: Optional[str] = None,
        limit: int = 1000,
    ) -> List[Dict[str, Any]]:
        """List blobs in container with optional prefix filter."""
        try:
            container_client = self._get_container_client(container)
            blobs = []

            for blob in container_client.list_blobs(name_starts_with=prefix):
                blobs.append({
                    "name": blob.name,
                    "size": blob.size,
                    "size_mb": blob.size / (1024 * 1024),
                    "last_modified": blob.last_modified.isoformat() if blob.last_modified else None,
                    "content_type": blob.content_settings.content_type if blob.content_settings else None,
                })
                if len(blobs) >= limit:
                    break

            return blobs

        except Exception as e:
            logger.error(f"Failed to list blobs: {e}")
            return []

    def _detect_content_type(self, path: str) -> str:
        """Auto-detect content type from file extension."""
        ext = Path(path).suffix.lower()
        content_types = {
            ".tif": "image/tiff",
            ".tiff": "image/tiff",
            ".png": "image/png",
            ".jpg": "image/jpeg",
            ".jpeg": "image/jpeg",
            ".json": "application/json",
            ".geojson": "application/geo+json",
            ".parquet": "application/vnd.apache.parquet",
            ".gpkg": "application/geopackage+sqlite3",
            ".shp": "application/x-shapefile",
        }
        return content_types.get(ext, "application/octet-stream")


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

def get_blob_repository(zone: str) -> BlobRepository:
    """
    Get BlobRepository instance for a storage zone.

    Args:
        zone: Zone name (bronze, silver, silverext, gold) — required

    Returns:
        BlobRepository instance
    """
    return BlobRepository.for_zone(zone)


# ============================================================================
# EXPORTS
# ============================================================================

__all__ = [
    "BlobRepository",
    "get_blob_repository",
]
