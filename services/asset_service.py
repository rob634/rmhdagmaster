# ============================================================================
# CLAUDE CONTEXT - GEOSPATIAL ASSET SERVICE
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Domain service - Business rules for geospatial assets
# PURPOSE: Coordinate asset submission, approval, clearance, and processing
# LAST_REVIEWED: 11 FEB 2026
# ============================================================================
"""
GeospatialAssetService

Coordination layer between the domain model (Platform, GeospatialAsset,
AssetVersion) and the DAG execution engine (JobService).

Encodes all business rules for:
- Asset submission (validate platform, compute identity, create version)
- Processing callbacks (DAG job → domain state)
- Approval workflow (pending_review → approved | rejected)
- Clearance workflow (uncleared → ouo → public)
- Reprocessing (same version, new ETL run)
- Version resolution (latest = highest approved ordinal)

Pattern: Constructor injection of AsyncConnectionPool, repos instantiated
in __init__, async methods, optional EventService for observability.
"""

from typing import Any, Dict, List, Optional, Tuple

from psycopg_pool import AsyncConnectionPool

from core.logging import get_logger
from core.models.geospatial_asset import GeospatialAsset
from core.models.asset_version import AssetVersion
from core.contracts import ApprovalState, ProcessingStatus
from repositories import PlatformRepository, AssetRepository, AssetVersionRepository
from services.preflight import get_preflight_validator

logger = get_logger(__name__)


class GeospatialAssetService:
    """Business rules for geospatial asset lifecycle."""

    def __init__(
        self,
        pool: AsyncConnectionPool,
        job_service: "JobService",
        event_service: "EventService" = None,
        blob_repo=None,
    ):
        self.pool = pool
        self.platform_repo = PlatformRepository(pool)
        self.asset_repo = AssetRepository(pool)
        self.version_repo = AssetVersionRepository(pool)
        self.job_service = job_service
        self._event_service = event_service
        self._blob_repo = blob_repo

    # ================================================================
    # SUBMIT
    # ================================================================

    async def submit_asset(
        self,
        platform_id: str,
        platform_refs: Dict[str, Any],
        data_type: str,
        version_label: Optional[str],
        workflow_id: str,
        input_params: Dict[str, Any],
        submitted_by: Optional[str] = None,
        correlation_id: Optional[str] = None,
    ) -> Tuple[GeospatialAsset, AssetVersion, Any]:
        """
        Main entry point: validate, create-or-find asset, create version, start DAG job.

        Returns:
            (asset, version, job)

        Raises:
            ValueError: Platform not found, inactive, missing refs, or unresolved version.
            KeyError: Workflow not found (from JobService).
        """
        # 0. Pre-flight validation (cheap checks before job creation)
        validator = get_preflight_validator(
            workflow_id, self.pool, blob_repo=self._blob_repo,
        )
        if validator is not None:
            preflight = await validator.validate(
                input_params, correlation_id=correlation_id,
            )
            if not preflight.valid:
                raise ValueError(
                    f"Pre-flight validation failed: "
                    f"{'; '.join(preflight.errors)}"
                )

        # 1. Validate platform
        platform = await self.platform_repo.get(platform_id)
        if platform is None:
            raise ValueError(f"Platform '{platform_id}' not found")
        if not platform.is_active:
            raise ValueError(f"Platform '{platform_id}' is inactive")

        # 2. Validate required refs
        missing = platform.validate_refs(platform_refs)
        if missing:
            raise ValueError(
                f"Missing required platform refs: {', '.join(missing)}"
            )

        # 3. Compute deterministic asset_id
        asset_id = GeospatialAsset.compute_asset_id(platform_id, platform_refs)

        # 4. Get-or-create asset
        asset_shell = GeospatialAsset(
            asset_id=asset_id,
            platform_id=platform_id,
            platform_refs=platform_refs,
            data_type=data_type,
        )
        asset, created = await self.asset_repo.get_or_create(asset_shell)

        if created:
            logger.info(f"Created new asset {asset_id} for platform {platform_id}")
        else:
            logger.info(f"Found existing asset {asset_id}")

        # 5. Check version ordering constraint
        has_unresolved = await self.version_repo.has_unresolved_versions(asset_id)
        if has_unresolved:
            raise ValueError(
                f"Asset {asset_id} has unresolved versions — "
                f"approve or reject pending versions before submitting a new one"
            )

        # 6. Get next ordinal
        next_ordinal = await self.version_repo.get_next_ordinal(asset_id)

        # 7. Create version
        version = AssetVersion(
            asset_id=asset_id,
            version_ordinal=next_ordinal,
            version_label=version_label,
            approval_state=ApprovalState.PENDING_REVIEW,
            processing_state=ProcessingStatus.QUEUED,
        )
        await self.version_repo.create(version)

        # 8. Create DAG job (with asset_id + correlation_id for tracing)
        job = await self.job_service.create_job(
            workflow_id=workflow_id,
            input_params=input_params,
            submitted_by=submitted_by,
            correlation_id=correlation_id,
            asset_id=asset_id,
        )

        # 9. Link version to job and start processing
        version.mark_processing_started(job.job_id)
        await self.version_repo.update(version)

        logger.info(
            f"Submitted asset {asset_id} v{next_ordinal} → job {job.job_id}"
        )

        return asset, version, job

    # ================================================================
    # PROCESSING CALLBACKS
    # ================================================================

    async def on_processing_completed(
        self,
        job_id: str,
        artifacts: Dict[str, Any],
    ) -> Optional[AssetVersion]:
        """
        Called when a DAG job completes. Updates the linked asset version.

        Returns None if no version is tied to this job (standalone job).
        """
        version = await self.version_repo.get_by_job_id(job_id)
        if version is None:
            return None

        # Write artifact fields
        if "blob_path" in artifacts:
            version.blob_path = artifacts["blob_path"]
        if "table_name" in artifacts:
            version.table_name = artifacts["table_name"]
        if "stac_item_id" in artifacts:
            version.stac_item_id = artifacts["stac_item_id"]
        if "stac_collection_id" in artifacts:
            version.stac_collection_id = artifacts["stac_collection_id"]

        version.mark_processing_completed()
        await self.version_repo.update(version)

        logger.info(
            f"Processing completed for asset {version.asset_id} "
            f"v{version.version_ordinal} (job {job_id})"
        )
        return version

    async def on_processing_failed(
        self,
        job_id: str,
        error_message: str,
    ) -> Optional[AssetVersion]:
        """
        Called when a DAG job fails. Updates the linked asset version.

        Returns None if no version is tied to this job (standalone job).
        """
        version = await self.version_repo.get_by_job_id(job_id)
        if version is None:
            return None

        version.mark_processing_failed(error_message)
        await self.version_repo.update(version)

        logger.info(
            f"Processing failed for asset {version.asset_id} "
            f"v{version.version_ordinal} (job {job_id}): {error_message}"
        )
        return version

    # ================================================================
    # APPROVAL
    # ================================================================

    async def approve_version(
        self,
        asset_id: str,
        version_ordinal: int,
        reviewer: str,
    ) -> AssetVersion:
        """
        Approve a version: PENDING_REVIEW → APPROVED.

        Raises:
            ValueError: Version not found or invalid state transition.
        """
        version = await self.version_repo.get(asset_id, version_ordinal)
        if version is None:
            raise ValueError(
                f"Version {version_ordinal} not found for asset {asset_id}"
            )

        version.mark_approved(reviewer)
        updated = await self.version_repo.update(version)
        if not updated:
            raise ValueError(
                f"Version conflict updating asset {asset_id} v{version_ordinal}"
            )

        logger.info(
            f"Approved asset {asset_id} v{version_ordinal} by {reviewer}"
        )
        return version

    async def reject_version(
        self,
        asset_id: str,
        version_ordinal: int,
        reviewer: str,
        reason: str,
    ) -> AssetVersion:
        """
        Reject a version: PENDING_REVIEW → REJECTED.

        Raises:
            ValueError: Version not found or invalid state transition.
        """
        version = await self.version_repo.get(asset_id, version_ordinal)
        if version is None:
            raise ValueError(
                f"Version {version_ordinal} not found for asset {asset_id}"
            )

        version.mark_rejected(reviewer, reason)
        updated = await self.version_repo.update(version)
        if not updated:
            raise ValueError(
                f"Version conflict updating asset {asset_id} v{version_ordinal}"
            )

        logger.info(
            f"Rejected asset {asset_id} v{version_ordinal} by {reviewer}: {reason}"
        )
        return version

    # ================================================================
    # CLEARANCE
    # ================================================================

    async def mark_cleared(
        self,
        asset_id: str,
        actor: str,
    ) -> GeospatialAsset:
        """
        Transition clearance: UNCLEARED → OUO.

        Raises:
            ValueError: Asset not found or invalid state transition.
        """
        asset = await self.asset_repo.get(asset_id)
        if asset is None:
            raise ValueError(f"Asset {asset_id} not found")

        asset.mark_cleared(actor)
        updated = await self.asset_repo.update(asset)
        if not updated:
            raise ValueError(f"Version conflict updating asset {asset_id}")

        logger.info(f"Asset {asset_id} cleared by {actor}")
        return asset

    async def mark_public(
        self,
        asset_id: str,
        actor: str,
    ) -> GeospatialAsset:
        """
        Transition clearance: OUO → PUBLIC.

        Raises:
            ValueError: Asset not found or invalid state transition.
        """
        asset = await self.asset_repo.get(asset_id)
        if asset is None:
            raise ValueError(f"Asset {asset_id} not found")

        asset.mark_public(actor)
        updated = await self.asset_repo.update(asset)
        if not updated:
            raise ValueError(f"Version conflict updating asset {asset_id}")

        logger.info(f"Asset {asset_id} made public by {actor}")
        return asset

    # ================================================================
    # REPROCESSING
    # ================================================================

    async def reprocess_version(
        self,
        asset_id: str,
        version_ordinal: int,
        workflow_id: str,
        input_params: Dict[str, Any],
        submitted_by: Optional[str] = None,
    ) -> Tuple[AssetVersion, Any]:
        """
        Reprocess an existing version (increment revision, new DAG job).

        Raises:
            ValueError: Version not found or processing not terminal.
        """
        version = await self.version_repo.get(asset_id, version_ordinal)
        if version is None:
            raise ValueError(
                f"Version {version_ordinal} not found for asset {asset_id}"
            )

        if not version.processing_state.is_terminal():
            raise ValueError(
                f"Cannot reprocess version in state '{version.processing_state.value}' "
                f"(must be completed or failed)"
            )

        # Create new DAG job (link back to asset for tracing)
        job = await self.job_service.create_job(
            workflow_id=workflow_id,
            input_params=input_params,
            submitted_by=submitted_by,
            asset_id=asset_id,
        )

        # Increment revision, reset processing state
        version.start_reprocessing(job.job_id)
        await self.version_repo.update(version)

        logger.info(
            f"Reprocessing asset {asset_id} v{version_ordinal} "
            f"revision {version.revision} → job {job.job_id}"
        )
        return version, job

    # ================================================================
    # QUERIES
    # ================================================================

    async def get_asset(self, asset_id: str) -> Optional[GeospatialAsset]:
        """Get an asset by ID."""
        return await self.asset_repo.get(asset_id)

    async def get_version(
        self, asset_id: str, version_ordinal: int
    ) -> Optional[AssetVersion]:
        """Get a specific version."""
        return await self.version_repo.get(asset_id, version_ordinal)

    async def get_latest_version(
        self, asset_id: str
    ) -> Optional[AssetVersion]:
        """Get the highest approved version (version=latest resolution)."""
        return await self.version_repo.get_latest_approved(asset_id)

    async def list_assets(
        self,
        platform_id: str,
        include_deleted: bool = False,
    ) -> List[GeospatialAsset]:
        """List assets for a platform."""
        return await self.asset_repo.list_by_platform(
            platform_id, include_deleted=include_deleted
        )

    async def list_versions(self, asset_id: str) -> List[AssetVersion]:
        """List all versions for an asset."""
        return await self.version_repo.list_for_asset(asset_id)

    async def soft_delete_asset(
        self, asset_id: str, actor: str
    ) -> bool:
        """
        Soft-delete an asset.

        Raises:
            ValueError: Asset not found.
        """
        asset = await self.asset_repo.get(asset_id)
        if asset is None:
            raise ValueError(f"Asset {asset_id} not found")

        return await self.asset_repo.soft_delete(asset_id, actor, asset.version)
