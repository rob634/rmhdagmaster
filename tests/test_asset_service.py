# ============================================================================
# GEOSPATIAL ASSET SERVICE TESTS
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Tests - Phase 4A business rule tests
# PURPOSE: Verify GeospatialAssetService with mocked repositories
# CREATED: 11 FEB 2026
# ============================================================================
"""
GeospatialAssetService Tests

Unit tests with mocked repos. Tests business logic in isolation —
no database, no Service Bus, no I/O.

Run with:
    pytest tests/test_asset_service.py -v
"""

import asyncio
import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from core.models.geospatial_asset import GeospatialAsset
from core.models.asset_version import AssetVersion
from core.models.platform import Platform
from core.contracts import ApprovalState, ClearanceState, ProcessingStatus

from services.asset_service import GeospatialAssetService


# ============================================================================
# HELPERS
# ============================================================================

def _make_platform(
    platform_id="ddh",
    required_refs=None,
    is_active=True,
):
    """Create a test Platform."""
    return Platform(
        platform_id=platform_id,
        display_name="DDH",
        required_refs=required_refs or ["dataset_id", "resource_id"],
        is_active=is_active,
    )


def _make_asset(
    asset_id="abc123",
    platform_id="ddh",
    data_type="raster",
    clearance_state=ClearanceState.UNCLEARED,
    version=1,
):
    """Create a test GeospatialAsset."""
    return GeospatialAsset(
        asset_id=asset_id,
        platform_id=platform_id,
        platform_refs={"dataset_id": "d1", "resource_id": "r1"},
        data_type=data_type,
        clearance_state=clearance_state,
        version=version,
    )


def _make_version(
    asset_id="abc123",
    version_ordinal=0,
    approval_state=ApprovalState.PENDING_REVIEW,
    processing_state=ProcessingStatus.QUEUED,
    current_job_id=None,
    version=1,
):
    """Create a test AssetVersion."""
    return AssetVersion(
        asset_id=asset_id,
        version_ordinal=version_ordinal,
        approval_state=approval_state,
        processing_state=processing_state,
        current_job_id=current_job_id,
        version=version,
    )


def _make_job(job_id="job-001"):
    """Create a mock Job with a job_id attribute."""
    job = MagicMock()
    job.job_id = job_id
    return job


def _build_service():
    """Build a GeospatialAssetService with all repos mocked."""
    pool = MagicMock()
    job_service = AsyncMock()
    svc = GeospatialAssetService(pool, job_service)

    # Replace repos with async mocks
    svc.platform_repo = AsyncMock()
    svc.asset_repo = AsyncMock()
    svc.version_repo = AsyncMock()

    return svc


# ============================================================================
# SUBMIT ASSET
# ============================================================================

class TestSubmitAsset:
    """Tests for submit_asset — the main entry point."""

    def test_happy_path_new_asset(self):
        """New platform + new asset + first version → returns (asset, version, job)."""
        svc = _build_service()
        platform = _make_platform()
        asset = _make_asset()
        job = _make_job()

        svc.platform_repo.get = AsyncMock(return_value=platform)
        svc.asset_repo.get_or_create = AsyncMock(return_value=(asset, True))
        svc.version_repo.has_unresolved_versions = AsyncMock(return_value=False)
        svc.version_repo.get_next_ordinal = AsyncMock(return_value=0)
        svc.version_repo.create = AsyncMock(side_effect=lambda v: v)
        svc.version_repo.update = AsyncMock(return_value=True)
        svc.job_service.create_job = AsyncMock(return_value=job)

        result_asset, result_version, result_job = asyncio.run(
            svc.submit_asset(
                platform_id="ddh",
                platform_refs={"dataset_id": "d1", "resource_id": "r1"},
                data_type="raster",
                version_label="V1",
                workflow_id="raster_ingest",
                input_params={"blob_path": "bronze/test.tif"},
            )
        )

        assert result_asset.asset_id == asset.asset_id
        assert result_version.version_ordinal == 0
        assert result_version.version_label == "V1"
        assert result_job.job_id == "job-001"
        svc.job_service.create_job.assert_awaited_once()
        svc.version_repo.create.assert_awaited_once()
        # Version should be linked to job and processing started
        svc.version_repo.update.assert_awaited_once()

    def test_submit_passes_asset_id_and_correlation_id(self):
        """create_job receives asset_id and correlation_id for tracing."""
        svc = _build_service()
        platform = _make_platform()
        asset = _make_asset(asset_id="computed-hash-abc")
        job = _make_job()

        svc.platform_repo.get = AsyncMock(return_value=platform)
        svc.asset_repo.get_or_create = AsyncMock(return_value=(asset, True))
        svc.version_repo.has_unresolved_versions = AsyncMock(return_value=False)
        svc.version_repo.get_next_ordinal = AsyncMock(return_value=0)
        svc.version_repo.create = AsyncMock(side_effect=lambda v: v)
        svc.version_repo.update = AsyncMock(return_value=True)
        svc.job_service.create_job = AsyncMock(return_value=job)

        asyncio.run(
            svc.submit_asset(
                platform_id="ddh",
                platform_refs={"dataset_id": "d1", "resource_id": "r1"},
                data_type="raster",
                version_label="V1",
                workflow_id="raster_ingest",
                input_params={"blob_path": "bronze/test.tif"},
                correlation_id="req-12345",
            )
        )

        # Verify asset_id and correlation_id passed to create_job
        call_kwargs = svc.job_service.create_job.call_args.kwargs
        assert call_kwargs["correlation_id"] == "req-12345"
        # asset_id is computed from platform_id + refs, not "computed-hash-abc"
        assert call_kwargs["asset_id"] is not None
        assert len(call_kwargs["asset_id"]) == 32  # SHA256[:32]

    def test_idempotent_existing_asset(self):
        """Same platform_refs → same asset_id, new version ordinal."""
        svc = _build_service()
        platform = _make_platform()
        existing_asset = _make_asset()
        job = _make_job()

        svc.platform_repo.get = AsyncMock(return_value=platform)
        svc.asset_repo.get_or_create = AsyncMock(return_value=(existing_asset, False))
        svc.version_repo.has_unresolved_versions = AsyncMock(return_value=False)
        svc.version_repo.get_next_ordinal = AsyncMock(return_value=1)
        svc.version_repo.create = AsyncMock(side_effect=lambda v: v)
        svc.version_repo.update = AsyncMock(return_value=True)
        svc.job_service.create_job = AsyncMock(return_value=job)

        _, version, _ = asyncio.run(
            svc.submit_asset(
                platform_id="ddh",
                platform_refs={"dataset_id": "d1", "resource_id": "r1"},
                data_type="raster",
                version_label="V2",
                workflow_id="raster_ingest",
                input_params={},
            )
        )

        assert version.version_ordinal == 1

    def test_platform_not_found(self):
        """Platform doesn't exist → ValueError."""
        svc = _build_service()
        svc.platform_repo.get = AsyncMock(return_value=None)

        with pytest.raises(ValueError, match="not found"):
            asyncio.run(
                svc.submit_asset(
                    platform_id="nonexistent",
                    platform_refs={},
                    data_type="raster",
                    version_label=None,
                    workflow_id="w",
                    input_params={},
                )
            )

    def test_platform_inactive(self):
        """Inactive platform → ValueError."""
        svc = _build_service()
        platform = _make_platform(is_active=False)
        svc.platform_repo.get = AsyncMock(return_value=platform)

        with pytest.raises(ValueError, match="inactive"):
            asyncio.run(
                svc.submit_asset(
                    platform_id="ddh",
                    platform_refs={"dataset_id": "d1", "resource_id": "r1"},
                    data_type="raster",
                    version_label=None,
                    workflow_id="w",
                    input_params={},
                )
            )

    def test_missing_required_refs(self):
        """Missing platform refs → ValueError with missing ref names."""
        svc = _build_service()
        platform = _make_platform(required_refs=["dataset_id", "resource_id"])
        svc.platform_repo.get = AsyncMock(return_value=platform)

        with pytest.raises(ValueError, match="resource_id"):
            asyncio.run(
                svc.submit_asset(
                    platform_id="ddh",
                    platform_refs={"dataset_id": "d1"},  # missing resource_id
                    data_type="raster",
                    version_label=None,
                    workflow_id="w",
                    input_params={},
                )
            )

    def test_unresolved_version_blocks_submit(self):
        """Unresolved version exists → ValueError (version ordering)."""
        svc = _build_service()
        platform = _make_platform()
        asset = _make_asset()

        svc.platform_repo.get = AsyncMock(return_value=platform)
        svc.asset_repo.get_or_create = AsyncMock(return_value=(asset, False))
        svc.version_repo.has_unresolved_versions = AsyncMock(return_value=True)

        with pytest.raises(ValueError, match="unresolved"):
            asyncio.run(
                svc.submit_asset(
                    platform_id="ddh",
                    platform_refs={"dataset_id": "d1", "resource_id": "r1"},
                    data_type="raster",
                    version_label=None,
                    workflow_id="w",
                    input_params={},
                )
            )


# ============================================================================
# PROCESSING CALLBACKS
# ============================================================================

class TestProcessingCallbacks:
    """Tests for on_processing_completed / on_processing_failed."""

    def test_on_processing_completed_writes_artifacts(self):
        """Completed callback writes artifact fields and transitions state."""
        svc = _build_service()
        version = _make_version(
            processing_state=ProcessingStatus.PROCESSING,
            current_job_id="job-001",
        )
        svc.version_repo.get_by_job_id = AsyncMock(return_value=version)
        svc.version_repo.update = AsyncMock(return_value=True)

        result = asyncio.run(
            svc.on_processing_completed(
                job_id="job-001",
                artifacts={
                    "blob_path": "silver/output.tif",
                    "stac_collection_id": "flood_risk",
                },
            )
        )

        assert result is not None
        assert result.blob_path == "silver/output.tif"
        assert result.stac_collection_id == "flood_risk"
        assert result.processing_state == ProcessingStatus.COMPLETED

    def test_on_processing_failed_writes_error(self):
        """Failed callback writes error and transitions state."""
        svc = _build_service()
        version = _make_version(
            processing_state=ProcessingStatus.PROCESSING,
            current_job_id="job-001",
        )
        svc.version_repo.get_by_job_id = AsyncMock(return_value=version)
        svc.version_repo.update = AsyncMock(return_value=True)

        result = asyncio.run(
            svc.on_processing_failed(
                job_id="job-001",
                error_message="GDAL open failed",
            )
        )

        assert result is not None
        assert result.processing_state == ProcessingStatus.FAILED
        assert result.last_error == "GDAL open failed"

    def test_on_processing_completed_unknown_job(self):
        """Unknown job_id → returns None (no-op)."""
        svc = _build_service()
        svc.version_repo.get_by_job_id = AsyncMock(return_value=None)

        result = asyncio.run(
            svc.on_processing_completed(job_id="unknown", artifacts={})
        )

        assert result is None

    def test_on_processing_failed_unknown_job(self):
        """Unknown job_id → returns None (no-op)."""
        svc = _build_service()
        svc.version_repo.get_by_job_id = AsyncMock(return_value=None)

        result = asyncio.run(
            svc.on_processing_failed(job_id="unknown", error_message="err")
        )

        assert result is None


# ============================================================================
# APPROVAL
# ============================================================================

class TestApproval:
    """Tests for approve_version / reject_version."""

    def test_approve_version(self):
        """Approve transitions PENDING_REVIEW → APPROVED."""
        svc = _build_service()
        version = _make_version(approval_state=ApprovalState.PENDING_REVIEW)
        svc.version_repo.get = AsyncMock(return_value=version)
        svc.version_repo.update = AsyncMock(return_value=True)

        result = asyncio.run(
            svc.approve_version("abc123", 0, reviewer="analyst@gov")
        )

        assert result.approval_state == ApprovalState.APPROVED
        assert result.reviewer == "analyst@gov"

    def test_reject_version_with_reason(self):
        """Reject transitions PENDING_REVIEW → REJECTED with reason."""
        svc = _build_service()
        version = _make_version(approval_state=ApprovalState.PENDING_REVIEW)
        svc.version_repo.get = AsyncMock(return_value=version)
        svc.version_repo.update = AsyncMock(return_value=True)

        result = asyncio.run(
            svc.reject_version("abc123", 0, "analyst@gov", "Bad CRS")
        )

        assert result.approval_state == ApprovalState.REJECTED
        assert result.rejection_reason == "Bad CRS"

    def test_approve_already_rejected(self):
        """Approving a rejected version → ValueError (from model)."""
        svc = _build_service()
        version = _make_version(approval_state=ApprovalState.REJECTED)
        svc.version_repo.get = AsyncMock(return_value=version)

        with pytest.raises(ValueError, match="Cannot approve"):
            asyncio.run(
                svc.approve_version("abc123", 0, "analyst@gov")
            )

    def test_approve_version_not_found(self):
        """Version not found → ValueError."""
        svc = _build_service()
        svc.version_repo.get = AsyncMock(return_value=None)

        with pytest.raises(ValueError, match="not found"):
            asyncio.run(
                svc.approve_version("abc123", 99, "analyst@gov")
            )


# ============================================================================
# CLEARANCE
# ============================================================================

class TestClearance:
    """Tests for mark_cleared / mark_public."""

    def test_mark_cleared(self):
        """Transitions UNCLEARED → OUO."""
        svc = _build_service()
        asset = _make_asset(clearance_state=ClearanceState.UNCLEARED)
        svc.asset_repo.get = AsyncMock(return_value=asset)
        svc.asset_repo.update = AsyncMock(return_value=True)

        result = asyncio.run(svc.mark_cleared("abc123", "reviewer@gov"))

        assert result.clearance_state == ClearanceState.OUO

    def test_mark_public(self):
        """Transitions OUO → PUBLIC."""
        svc = _build_service()
        asset = _make_asset(clearance_state=ClearanceState.OUO)
        svc.asset_repo.get = AsyncMock(return_value=asset)
        svc.asset_repo.update = AsyncMock(return_value=True)

        result = asyncio.run(svc.mark_public("abc123", "reviewer@gov"))

        assert result.clearance_state == ClearanceState.PUBLIC

    def test_mark_public_from_uncleared(self):
        """Cannot jump UNCLEARED → PUBLIC (from model)."""
        svc = _build_service()
        asset = _make_asset(clearance_state=ClearanceState.UNCLEARED)
        svc.asset_repo.get = AsyncMock(return_value=asset)

        with pytest.raises(ValueError, match="Cannot make public"):
            asyncio.run(svc.mark_public("abc123", "reviewer@gov"))

    def test_clearance_asset_not_found(self):
        """Asset not found → ValueError."""
        svc = _build_service()
        svc.asset_repo.get = AsyncMock(return_value=None)

        with pytest.raises(ValueError, match="not found"):
            asyncio.run(svc.mark_cleared("nonexistent", "reviewer@gov"))


# ============================================================================
# REPROCESSING
# ============================================================================

class TestReprocessing:
    """Tests for reprocess_version."""

    def test_reprocess_increments_revision(self):
        """Reprocess from COMPLETED creates new job and increments revision."""
        svc = _build_service()
        version = _make_version(
            processing_state=ProcessingStatus.COMPLETED,
            current_job_id="old-job",
        )
        new_job = _make_job("new-job")
        svc.version_repo.get = AsyncMock(return_value=version)
        svc.version_repo.update = AsyncMock(return_value=True)
        svc.job_service.create_job = AsyncMock(return_value=new_job)

        result_version, result_job = asyncio.run(
            svc.reprocess_version(
                "abc123", 0, "raster_ingest", {"blob_path": "new.tif"}
            )
        )

        assert result_version.revision == 1
        assert result_version.current_job_id == "new-job"
        assert result_version.processing_state == ProcessingStatus.PROCESSING
        assert result_job.job_id == "new-job"

    def test_reprocess_from_queued_fails(self):
        """Cannot reprocess from non-terminal state."""
        svc = _build_service()
        version = _make_version(processing_state=ProcessingStatus.QUEUED)
        svc.version_repo.get = AsyncMock(return_value=version)

        with pytest.raises(ValueError, match="Cannot reprocess"):
            asyncio.run(
                svc.reprocess_version("abc123", 0, "w", {})
            )

    def test_reprocess_from_failed(self):
        """Reprocess from FAILED is allowed."""
        svc = _build_service()
        version = _make_version(processing_state=ProcessingStatus.FAILED)
        new_job = _make_job("retry-job")
        svc.version_repo.get = AsyncMock(return_value=version)
        svc.version_repo.update = AsyncMock(return_value=True)
        svc.job_service.create_job = AsyncMock(return_value=new_job)

        result_version, _ = asyncio.run(
            svc.reprocess_version("abc123", 0, "w", {})
        )

        assert result_version.revision == 1
        assert result_version.processing_state == ProcessingStatus.PROCESSING

    def test_reprocess_passes_asset_id(self):
        """Reprocess passes asset_id to create_job for tracing."""
        svc = _build_service()
        version = _make_version(
            asset_id="my-asset-id",
            processing_state=ProcessingStatus.COMPLETED,
        )
        new_job = _make_job("reprocess-job")
        svc.version_repo.get = AsyncMock(return_value=version)
        svc.version_repo.update = AsyncMock(return_value=True)
        svc.job_service.create_job = AsyncMock(return_value=new_job)

        asyncio.run(
            svc.reprocess_version("my-asset-id", 0, "w", {})
        )

        call_kwargs = svc.job_service.create_job.call_args.kwargs
        assert call_kwargs["asset_id"] == "my-asset-id"


# ============================================================================
# QUERIES
# ============================================================================

class TestQueries:
    """Tests for query methods."""

    def test_get_latest_version(self):
        """Returns highest approved ordinal."""
        svc = _build_service()
        approved = _make_version(
            version_ordinal=2,
            approval_state=ApprovalState.APPROVED,
        )
        svc.version_repo.get_latest_approved = AsyncMock(return_value=approved)

        result = asyncio.run(svc.get_latest_version("abc123"))

        assert result.version_ordinal == 2
        assert result.approval_state == ApprovalState.APPROVED

    def test_soft_delete_asset(self):
        """Soft delete marks asset deleted."""
        svc = _build_service()
        asset = _make_asset(version=3)
        svc.asset_repo.get = AsyncMock(return_value=asset)
        svc.asset_repo.soft_delete = AsyncMock(return_value=True)

        result = asyncio.run(svc.soft_delete_asset("abc123", "admin"))

        assert result is True
        svc.asset_repo.soft_delete.assert_awaited_once_with("abc123", "admin", 3)

    def test_soft_delete_not_found(self):
        """Asset not found → ValueError."""
        svc = _build_service()
        svc.asset_repo.get = AsyncMock(return_value=None)

        with pytest.raises(ValueError, match="not found"):
            asyncio.run(svc.soft_delete_asset("nonexistent", "admin"))


# ============================================================================
# B2B LIFECYCLE (end-to-end service-level chain)
# ============================================================================

class TestB2BLifecycle:
    """
    Tests the full B2B lifecycle chain at the service level:
    submit (with correlation_id) → processing callback → approve.

    Verifies that identity threading (correlation_id, asset_id) and
    state transitions work correctly across the complete flow.
    """

    def test_submit_complete_approve_lifecycle(self):
        """Full chain: submit → processing complete → approve version."""
        svc = _build_service()
        platform = _make_platform()
        job = _make_job("job-lifecycle")

        svc.platform_repo.get = AsyncMock(return_value=platform)
        svc.asset_repo.get_or_create = AsyncMock(
            side_effect=lambda shell: (shell, True)
        )
        svc.version_repo.has_unresolved_versions = AsyncMock(return_value=False)
        svc.version_repo.get_next_ordinal = AsyncMock(return_value=0)
        svc.version_repo.create = AsyncMock(side_effect=lambda v: v)
        svc.version_repo.update = AsyncMock(return_value=True)
        svc.job_service.create_job = AsyncMock(return_value=job)

        # Step 1: Submit
        asset, version, returned_job = asyncio.run(
            svc.submit_asset(
                platform_id="ddh",
                platform_refs={"dataset_id": "d1", "resource_id": "r1"},
                data_type="raster",
                version_label="V1",
                workflow_id="raster_ingest",
                input_params={"blob_path": "bronze/test.tif"},
                correlation_id="req-B2B-001",
            )
        )

        # Verify identity threading
        call_kwargs = svc.job_service.create_job.call_args.kwargs
        assert call_kwargs["correlation_id"] == "req-B2B-001"
        assert call_kwargs["asset_id"] == asset.asset_id
        assert len(call_kwargs["asset_id"]) == 32
        assert version.processing_state == ProcessingStatus.PROCESSING
        assert version.current_job_id == "job-lifecycle"

        # Step 2: Processing completed callback
        svc.version_repo.get_by_job_id = AsyncMock(return_value=version)
        completed = asyncio.run(
            svc.on_processing_completed(
                job_id="job-lifecycle",
                artifacts={"blob_path": "silver/output.tif"},
            )
        )

        assert completed.processing_state == ProcessingStatus.COMPLETED
        assert completed.blob_path == "silver/output.tif"

        # Step 3: Approve version
        svc.version_repo.get = AsyncMock(return_value=version)
        approved = asyncio.run(
            svc.approve_version(asset.asset_id, 0, "analyst@gov")
        )

        assert approved.approval_state == ApprovalState.APPROVED
        assert approved.reviewer == "analyst@gov"

    def test_submit_fail_reprocess_lifecycle(self):
        """Full chain: submit → processing fail → reprocess → complete."""
        svc = _build_service()
        platform = _make_platform()
        job1 = _make_job("job-fail")
        job2 = _make_job("job-retry")

        svc.platform_repo.get = AsyncMock(return_value=platform)
        svc.asset_repo.get_or_create = AsyncMock(
            side_effect=lambda shell: (shell, True)
        )
        svc.version_repo.has_unresolved_versions = AsyncMock(return_value=False)
        svc.version_repo.get_next_ordinal = AsyncMock(return_value=0)
        svc.version_repo.create = AsyncMock(side_effect=lambda v: v)
        svc.version_repo.update = AsyncMock(return_value=True)
        svc.job_service.create_job = AsyncMock(return_value=job1)

        # Step 1: Submit
        asset, version, _ = asyncio.run(
            svc.submit_asset(
                platform_id="ddh",
                platform_refs={"dataset_id": "d1", "resource_id": "r1"},
                data_type="raster",
                version_label="V1",
                workflow_id="raster_ingest",
                input_params={"blob_path": "bronze/test.tif"},
                correlation_id="req-B2B-002",
            )
        )

        # Step 2: Processing failed callback
        svc.version_repo.get_by_job_id = AsyncMock(return_value=version)
        failed = asyncio.run(
            svc.on_processing_failed(
                job_id="job-fail",
                error_message="GDAL: unsupported format",
            )
        )

        assert failed.processing_state == ProcessingStatus.FAILED
        assert failed.last_error == "GDAL: unsupported format"

        # Step 3: Reprocess
        svc.version_repo.get = AsyncMock(return_value=version)
        svc.job_service.create_job = AsyncMock(return_value=job2)

        reprocessed, retry_job = asyncio.run(
            svc.reprocess_version(
                asset.asset_id, 0, "raster_ingest",
                {"blob_path": "bronze/test_fixed.tif"},
            )
        )

        assert reprocessed.revision == 1
        assert reprocessed.current_job_id == "job-retry"
        assert reprocessed.processing_state == ProcessingStatus.PROCESSING
        assert retry_job.job_id == "job-retry"

        # Step 4: Second attempt completes
        svc.version_repo.get_by_job_id = AsyncMock(return_value=reprocessed)
        completed = asyncio.run(
            svc.on_processing_completed(
                job_id="job-retry",
                artifacts={"blob_path": "silver/output_v2.tif"},
            )
        )

        assert completed.processing_state == ProcessingStatus.COMPLETED
        assert completed.blob_path == "silver/output_v2.tif"
        assert completed.revision == 1
