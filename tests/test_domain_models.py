# ============================================================================
# DOMAIN MODEL TESTS
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Tests - Business domain model unit tests
# PURPOSE: Verify enums, models, state transitions, and schema generation
# CREATED: 09 FEB 2026
# ============================================================================
"""
Domain Model Tests

Unit tests for the business domain layer:
- Enums: ApprovalState, ClearanceState, ProcessingStatus
- Models: Platform, GeospatialAsset, AssetVersion
- State transitions and computed fields
- Schema generator registration

Run with:
    pytest tests/test_domain_models.py -v
"""

import pytest
from datetime import datetime, timezone

from core.contracts import ApprovalState, ClearanceState, ProcessingStatus
from core.models.platform import Platform
from core.models.geospatial_asset import GeospatialAsset
from core.models.asset_version import AssetVersion


# ============================================================================
# ENUM TESTS
# ============================================================================


class TestApprovalState:
    def test_values(self):
        assert ApprovalState.PENDING_REVIEW.value == "pending_review"
        assert ApprovalState.APPROVED.value == "approved"
        assert ApprovalState.REJECTED.value == "rejected"

    def test_is_terminal(self):
        assert not ApprovalState.PENDING_REVIEW.is_terminal()
        assert ApprovalState.APPROVED.is_terminal()
        assert ApprovalState.REJECTED.is_terminal()

    def test_value_serialization(self):
        """Enum .value is used for DB serialization, not str()."""
        assert ApprovalState.APPROVED.value == "approved"
        assert ApprovalState.PENDING_REVIEW.value == "pending_review"
        assert ApprovalState.REJECTED.value == "rejected"


class TestClearanceState:
    def test_values(self):
        assert ClearanceState.UNCLEARED.value == "uncleared"
        assert ClearanceState.OUO.value == "ouo"
        assert ClearanceState.PUBLIC.value == "public"

    def test_value_serialization(self):
        assert ClearanceState.PUBLIC.value == "public"


class TestProcessingStatus:
    def test_values(self):
        assert ProcessingStatus.QUEUED.value == "queued"
        assert ProcessingStatus.PROCESSING.value == "processing"
        assert ProcessingStatus.COMPLETED.value == "completed"
        assert ProcessingStatus.FAILED.value == "failed"

    def test_is_terminal(self):
        assert not ProcessingStatus.QUEUED.is_terminal()
        assert not ProcessingStatus.PROCESSING.is_terminal()
        assert ProcessingStatus.COMPLETED.is_terminal()
        assert ProcessingStatus.FAILED.is_terminal()

    def test_value_serialization(self):
        assert ProcessingStatus.QUEUED.value == "queued"


# ============================================================================
# PLATFORM TESTS
# ============================================================================


class TestPlatform:
    def test_creation_with_defaults(self):
        p = Platform(
            platform_id="ddh",
            display_name="Data Distribution Hub",
        )
        assert p.platform_id == "ddh"
        assert p.display_name == "Data Distribution Hub"
        assert p.is_active is True
        assert p.required_refs == []
        assert p.optional_refs == []
        assert p.description is None

    def test_validate_refs_passes(self):
        p = Platform(
            platform_id="ddh",
            display_name="DDH",
            required_refs=["dataset_id", "resource_id"],
        )
        missing = p.validate_refs({"dataset_id": "birds", "resource_id": "viewer"})
        assert missing == []

    def test_validate_refs_missing(self):
        p = Platform(
            platform_id="ddh",
            display_name="DDH",
            required_refs=["dataset_id", "resource_id"],
        )
        missing = p.validate_refs({"dataset_id": "birds"})
        assert missing == ["resource_id"]


# ============================================================================
# GEOSPATIAL ASSET TESTS
# ============================================================================


class TestGeospatialAsset:
    def _make_asset(self, **overrides):
        defaults = {
            "asset_id": "abc123def456abc123def456abc123de",
            "platform_id": "ddh",
            "platform_refs": {"dataset_id": "birds2023", "resource_id": "viewer"},
            "data_type": "raster",
        }
        defaults.update(overrides)
        return GeospatialAsset(**defaults)

    def test_creation_with_defaults(self):
        asset = self._make_asset()
        assert asset.clearance_state == ClearanceState.UNCLEARED
        assert asset.version == 1
        assert asset.is_deleted is False
        assert asset.is_public is False
        assert asset.deleted_at is None

    def test_compute_asset_id_deterministic(self):
        refs = {"dataset_id": "birds2023", "resource_id": "viewer"}
        id1 = GeospatialAsset.compute_asset_id("ddh", refs)
        id2 = GeospatialAsset.compute_asset_id("ddh", refs)
        assert id1 == id2
        assert len(id1) == 32

    def test_compute_asset_id_different_inputs(self):
        id1 = GeospatialAsset.compute_asset_id("ddh", {"dataset_id": "birds"})
        id2 = GeospatialAsset.compute_asset_id("ddh", {"dataset_id": "fish"})
        assert id1 != id2

    def test_compute_asset_id_order_independent(self):
        """Key order should not affect hash (sorted internally)."""
        id1 = GeospatialAsset.compute_asset_id(
            "ddh", {"dataset_id": "birds", "resource_id": "viewer"}
        )
        id2 = GeospatialAsset.compute_asset_id(
            "ddh", {"resource_id": "viewer", "dataset_id": "birds"}
        )
        assert id1 == id2

    def test_compute_asset_id_platform_matters(self):
        """Different platform_id should produce different hash."""
        refs = {"dataset_id": "birds"}
        id1 = GeospatialAsset.compute_asset_id("ddh", refs)
        id2 = GeospatialAsset.compute_asset_id("other_app", refs)
        assert id1 != id2

    def test_mark_cleared(self):
        asset = self._make_asset()
        asset.mark_cleared("admin@example.com")
        assert asset.clearance_state == ClearanceState.OUO
        assert asset.cleared_by == "admin@example.com"
        assert asset.cleared_at is not None

    def test_mark_public(self):
        asset = self._make_asset(clearance_state=ClearanceState.OUO)
        asset.mark_public("admin@example.com")
        assert asset.clearance_state == ClearanceState.PUBLIC
        assert asset.is_public is True
        assert asset.made_public_by == "admin@example.com"

    def test_mark_public_from_uncleared_fails(self):
        asset = self._make_asset()
        with pytest.raises(ValueError, match="Cannot make public"):
            asset.mark_public("admin@example.com")

    def test_mark_cleared_from_ouo_fails(self):
        asset = self._make_asset(clearance_state=ClearanceState.OUO)
        with pytest.raises(ValueError, match="Cannot clear"):
            asset.mark_cleared("admin@example.com")

    def test_soft_delete(self):
        asset = self._make_asset()
        asset.soft_delete("admin@example.com")
        assert asset.is_deleted is True
        assert asset.deleted_by == "admin@example.com"
        assert asset.deleted_at is not None

    def test_soft_delete_already_deleted_fails(self):
        asset = self._make_asset(deleted_at=datetime.now(timezone.utc))
        with pytest.raises(ValueError, match="already deleted"):
            asset.soft_delete("admin@example.com")


# ============================================================================
# ASSET VERSION TESTS
# ============================================================================


class TestAssetVersion:
    def _make_version(self, **overrides):
        defaults = {
            "asset_id": "abc123def456abc123def456abc123de",
            "version_ordinal": 0,
            "version_label": "V1",
        }
        defaults.update(overrides)
        return AssetVersion(**defaults)

    def test_creation_with_defaults(self):
        v = self._make_version()
        assert v.approval_state == ApprovalState.PENDING_REVIEW
        assert v.processing_state == ProcessingStatus.QUEUED
        assert v.revision == 0
        assert v.version == 1
        assert v.is_approved is False
        assert v.is_processing_terminal is False
        assert v.has_artifacts is False

    def test_mark_approved(self):
        v = self._make_version()
        v.mark_approved("reviewer@example.com")
        assert v.approval_state == ApprovalState.APPROVED
        assert v.is_approved is True
        assert v.reviewer == "reviewer@example.com"
        assert v.reviewed_at is not None

    def test_mark_rejected(self):
        v = self._make_version()
        v.mark_rejected("reviewer@example.com", "Data quality issues")
        assert v.approval_state == ApprovalState.REJECTED
        assert v.rejection_reason == "Data quality issues"
        assert v.reviewer == "reviewer@example.com"

    def test_mark_approved_from_rejected_fails(self):
        v = self._make_version(approval_state=ApprovalState.REJECTED)
        with pytest.raises(ValueError, match="Cannot approve"):
            v.mark_approved("reviewer@example.com")

    def test_mark_rejected_from_approved_fails(self):
        v = self._make_version(approval_state=ApprovalState.APPROVED)
        with pytest.raises(ValueError, match="Cannot reject"):
            v.mark_rejected("reviewer@example.com", "reason")

    def test_mark_processing_started(self):
        v = self._make_version()
        v.mark_processing_started("job-123")
        assert v.processing_state == ProcessingStatus.PROCESSING
        assert v.current_job_id == "job-123"
        assert v.processing_started_at is not None

    def test_mark_processing_completed(self):
        v = self._make_version(processing_state=ProcessingStatus.PROCESSING)
        v.mark_processing_completed()
        assert v.processing_state == ProcessingStatus.COMPLETED
        assert v.is_processing_terminal is True
        assert v.processing_completed_at is not None

    def test_mark_processing_failed(self):
        v = self._make_version(processing_state=ProcessingStatus.PROCESSING)
        v.mark_processing_failed("Out of memory")
        assert v.processing_state == ProcessingStatus.FAILED
        assert v.last_error == "Out of memory"

    def test_mark_processing_failed_truncates_error(self):
        v = self._make_version(processing_state=ProcessingStatus.PROCESSING)
        long_error = "x" * 3000
        v.mark_processing_failed(long_error)
        assert len(v.last_error) == 2000

    def test_start_reprocessing(self):
        v = self._make_version(
            processing_state=ProcessingStatus.COMPLETED,
            revision=0,
            current_job_id="old-job",
        )
        v.start_reprocessing("new-job-456")
        assert v.revision == 1
        assert v.processing_state == ProcessingStatus.PROCESSING
        assert v.current_job_id == "new-job-456"
        assert v.processing_completed_at is None
        assert v.last_error is None

    def test_processing_started_from_failed(self):
        """Can restart processing from FAILED state."""
        v = self._make_version(processing_state=ProcessingStatus.FAILED)
        v.mark_processing_started("retry-job")
        assert v.processing_state == ProcessingStatus.PROCESSING

    def test_processing_started_from_completed_fails(self):
        """Cannot start processing from COMPLETED state (use start_reprocessing)."""
        v = self._make_version(processing_state=ProcessingStatus.COMPLETED)
        with pytest.raises(ValueError, match="Cannot start processing"):
            v.mark_processing_started("job-123")

    def test_has_artifacts_with_blob(self):
        v = self._make_version(blob_path="silver/cog/test.tif")
        assert v.has_artifacts is True

    def test_has_artifacts_with_table(self):
        v = self._make_version(table_name="silver.my_table")
        assert v.has_artifacts is True


# ============================================================================
# SCHEMA GENERATION SMOKE TEST
# ============================================================================


class TestSchemaGeneration:
    def test_generate_all_includes_domain_tables(self):
        """Verify the schema generator produces DDL for new domain tables."""
        from core.schema.sql_generator import PydanticToSQL

        generator = PydanticToSQL(schema_name="dagapp")
        statements = generator.generate_all()

        # Convert all statements to strings for searching
        ddl_text = " ".join(str(s) for s in statements)

        assert "platforms" in ddl_text, "DDL should include platforms table"
        assert "geospatial_assets" in ddl_text, "DDL should include geospatial_assets table"
        assert "asset_versions" in ddl_text, "DDL should include asset_versions table"
        assert "approval_state" in ddl_text, "DDL should include approval_state enum"
        assert "clearance_state" in ddl_text, "DDL should include clearance_state enum"
        assert "processing_status" in ddl_text, "DDL should include processing_status enum"
