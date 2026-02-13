# ============================================================================
# PRE-FLIGHT VALIDATION TESTS
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Tests - Pre-flight validator
# PURPOSE: Verify pre-flight validation with mocked storage + repos
# CREATED: 13 FEB 2026
# ============================================================================
"""
Pre-flight Validation Tests

Unit tests with mocked BlobRepository and JobRepository.
Tests universal checks (required params, blob exists, idempotency)
and the factory function.

Run with:
    pytest tests/test_preflight.py -v
"""

import asyncio
import pytest
from unittest.mock import MagicMock, AsyncMock, patch

from core.contracts import JobStatus
from services.preflight import (
    PreflightResult,
    PreflightValidator,
    RasterPreflightValidator,
    get_preflight_validator,
)


# ============================================================================
# HELPERS
# ============================================================================

def _make_validator(blob_exists=True, blob_error=None):
    """
    Create a RasterPreflightValidator with mocked BlobRepository.

    Args:
        blob_exists: What blob_repo.blob_exists() returns.
        blob_error: If set, blob_repo.blob_exists() raises this exception.
    """
    pool = MagicMock()
    blob_repo = MagicMock()

    if blob_error:
        blob_repo.blob_exists.side_effect = blob_error
    else:
        blob_repo.blob_exists.return_value = blob_exists

    return RasterPreflightValidator(pool=pool, blob_repo=blob_repo)


def _make_job_mock(job_id="job-001", status=JobStatus.PENDING, is_terminal=False):
    """Create a mock Job for idempotency checks."""
    job = MagicMock()
    job.job_id = job_id
    job.status = status
    job.is_terminal = is_terminal
    return job


def _valid_raster_params(**overrides):
    """Build valid raster input_params dict."""
    params = {
        "blob_name": "uploads/test.tif",
        "container_name": "rmhazuregeobronze",
        "collection_id": "my-collection",
    }
    params.update(overrides)
    return params


# ============================================================================
# PREFLIGHT RESULT
# ============================================================================

class TestPreflightResult:
    """Tests for the PreflightResult value object."""

    def test_valid_result(self):
        result = PreflightResult(valid=True)
        assert result.valid is True
        assert result.errors == []
        assert result.warnings == []

    def test_invalid_result_with_errors(self):
        result = PreflightResult(
            valid=False,
            errors=["Missing param: x", "Blob not found"],
        )
        assert result.valid is False
        assert len(result.errors) == 2

    def test_valid_with_warnings(self):
        result = PreflightResult(
            valid=True,
            warnings=["Unusual file extension"],
        )
        assert result.valid is True
        assert len(result.warnings) == 1


# ============================================================================
# REQUIRED PARAMS
# ============================================================================

class TestRequiredParams:
    """Tests for _check_required_params."""

    def test_all_params_present(self):
        validator = _make_validator()
        result = asyncio.run(
            validator.validate(_valid_raster_params())
        )
        assert result.valid is True
        assert result.errors == []

    def test_missing_blob_name(self):
        validator = _make_validator()
        params = _valid_raster_params()
        del params["blob_name"]

        result = asyncio.run(validator.validate(params))
        assert result.valid is False
        assert any("blob_name" in e for e in result.errors)

    def test_missing_container_name(self):
        validator = _make_validator()
        params = _valid_raster_params()
        del params["container_name"]

        result = asyncio.run(validator.validate(params))
        assert result.valid is False
        assert any("container_name" in e for e in result.errors)

    def test_missing_collection_id(self):
        validator = _make_validator()
        params = _valid_raster_params()
        del params["collection_id"]

        result = asyncio.run(validator.validate(params))
        assert result.valid is False
        assert any("collection_id" in e for e in result.errors)

    def test_empty_string_treated_as_missing(self):
        validator = _make_validator()
        params = _valid_raster_params(blob_name="   ")

        result = asyncio.run(validator.validate(params))
        assert result.valid is False
        assert any("blob_name" in e for e in result.errors)

    def test_multiple_missing_params_all_reported(self):
        """All errors collected, not just the first."""
        validator = _make_validator()
        result = asyncio.run(validator.validate({}))
        assert result.valid is False
        assert len(result.errors) == 3
        param_names = " ".join(result.errors)
        assert "blob_name" in param_names
        assert "container_name" in param_names
        assert "collection_id" in param_names


# ============================================================================
# BLOB EXISTS
# ============================================================================

class TestBlobExists:
    """Tests for _check_blob_exists."""

    def test_blob_found(self):
        validator = _make_validator(blob_exists=True)
        result = asyncio.run(
            validator.validate(_valid_raster_params())
        )
        assert result.valid is True
        validator._blob_repo.blob_exists.assert_called_once_with(
            "rmhazuregeobronze", "uploads/test.tif"
        )

    def test_blob_not_found(self):
        validator = _make_validator(blob_exists=False)
        result = asyncio.run(
            validator.validate(_valid_raster_params())
        )
        assert result.valid is False
        assert any("Blob not found" in e for e in result.errors)
        assert "rmhazuregeobronze/uploads/test.tif" in result.errors[0]

    def test_storage_error(self):
        validator = _make_validator(
            blob_error=ConnectionError("Storage unavailable")
        )
        result = asyncio.run(
            validator.validate(_valid_raster_params())
        )
        assert result.valid is False
        assert any("Storage error" in e for e in result.errors)

    def test_blob_check_skipped_when_params_missing(self):
        """Blob check doesn't run if blob_name or container_name is missing."""
        validator = _make_validator(blob_exists=False)
        # Missing blob_name — blob check should be skipped (only required param error)
        result = asyncio.run(
            validator.validate({"container_name": "x", "collection_id": "y"})
        )
        validator._blob_repo.blob_exists.assert_not_called()
        # Should only have the required param error, not a blob error
        assert any("blob_name" in e for e in result.errors)
        assert not any("Blob not found" in e for e in result.errors)


# ============================================================================
# IDEMPOTENCY
# ============================================================================

class TestIdempotency:
    """Tests for _check_idempotency."""

    @patch('repositories.JobRepository')
    def test_no_existing_job(self, MockJobRepo):
        """No prior job with this correlation_id → valid."""
        mock_repo = AsyncMock()
        mock_repo.get_by_correlation_id.return_value = None
        MockJobRepo.return_value = mock_repo

        validator = _make_validator()
        result = asyncio.run(
            validator.validate(
                _valid_raster_params(),
                correlation_id="req-001",
            )
        )
        assert result.valid is True
        mock_repo.get_by_correlation_id.assert_called_once_with("req-001")

    @patch('repositories.JobRepository')
    def test_existing_active_job_rejected(self, MockJobRepo):
        """Active job with same correlation_id → duplicate error."""
        existing_job = _make_job_mock(
            job_id="job-existing",
            status=JobStatus.PENDING,
            is_terminal=False,
        )
        mock_repo = AsyncMock()
        mock_repo.get_by_correlation_id.return_value = existing_job
        MockJobRepo.return_value = mock_repo

        validator = _make_validator()
        result = asyncio.run(
            validator.validate(
                _valid_raster_params(),
                correlation_id="req-001",
            )
        )
        assert result.valid is False
        assert any("Duplicate submission" in e for e in result.errors)
        assert any("job-existing" in e for e in result.errors)

    @patch('repositories.JobRepository')
    def test_completed_job_allows_resubmit(self, MockJobRepo):
        """Terminal job with same correlation_id → allowed (resubmission)."""
        completed_job = _make_job_mock(
            job_id="job-old",
            status=JobStatus.COMPLETED,
            is_terminal=True,
        )
        mock_repo = AsyncMock()
        mock_repo.get_by_correlation_id.return_value = completed_job
        MockJobRepo.return_value = mock_repo

        validator = _make_validator()
        result = asyncio.run(
            validator.validate(
                _valid_raster_params(),
                correlation_id="req-001",
            )
        )
        assert result.valid is True

    @patch('repositories.JobRepository')
    def test_failed_job_allows_resubmit(self, MockJobRepo):
        """Failed job with same correlation_id → allowed."""
        failed_job = _make_job_mock(
            job_id="job-failed",
            status=JobStatus.FAILED,
            is_terminal=True,
        )
        mock_repo = AsyncMock()
        mock_repo.get_by_correlation_id.return_value = failed_job
        MockJobRepo.return_value = mock_repo

        validator = _make_validator()
        result = asyncio.run(
            validator.validate(
                _valid_raster_params(),
                correlation_id="req-001",
            )
        )
        assert result.valid is True

    def test_no_correlation_id_skips_check(self):
        """No correlation_id → idempotency check skipped entirely."""
        validator = _make_validator()
        result = asyncio.run(
            validator.validate(_valid_raster_params())
        )
        # No correlation_id passed, so no idempotency check
        assert result.valid is True


# ============================================================================
# ERROR COLLECTION
# ============================================================================

class TestErrorCollection:
    """Tests that multiple errors are collected, not fail-fast."""

    @patch('repositories.JobRepository')
    def test_multiple_failures_all_reported(self, MockJobRepo):
        """Missing param + blob not found + duplicate → 3 errors."""
        # Set up: blob doesn't exist
        validator = _make_validator(blob_exists=False)

        # Set up: active duplicate job
        existing_job = _make_job_mock(is_terminal=False)
        mock_repo = AsyncMock()
        mock_repo.get_by_correlation_id.return_value = existing_job
        MockJobRepo.return_value = mock_repo

        # Missing collection_id + blob not found + duplicate
        params = {
            "blob_name": "uploads/test.tif",
            "container_name": "rmhazuregeobronze",
            # collection_id missing
        }

        result = asyncio.run(
            validator.validate(params, correlation_id="req-dup")
        )
        assert result.valid is False
        assert len(result.errors) == 3
        error_text = " ".join(result.errors)
        assert "collection_id" in error_text
        assert "Blob not found" in error_text
        assert "Duplicate submission" in error_text


# ============================================================================
# FACTORY
# ============================================================================

class TestGetPreflightValidator:
    """Tests for the get_preflight_validator factory function."""

    def test_raster_ingest_returns_raster_validator(self):
        pool = MagicMock()
        validator = get_preflight_validator("raster_ingest", pool)
        assert isinstance(validator, RasterPreflightValidator)

    def test_raster_process_returns_raster_validator(self):
        pool = MagicMock()
        validator = get_preflight_validator("raster_process", pool)
        assert isinstance(validator, RasterPreflightValidator)

    def test_echo_test_returns_none(self):
        """Unregistered workflow → no validator (standalone workflows skip preflight)."""
        pool = MagicMock()
        validator = get_preflight_validator("echo_test", pool)
        assert validator is None

    def test_unknown_workflow_returns_none(self):
        pool = MagicMock()
        validator = get_preflight_validator("custom_pipeline", pool)
        assert validator is None

    def test_blob_repo_passthrough(self):
        """Custom blob_repo is passed to the validator."""
        pool = MagicMock()
        custom_repo = MagicMock()
        validator = get_preflight_validator("raster_ingest", pool, blob_repo=custom_repo)
        assert validator._blob_repo is custom_repo


# ============================================================================
# INTEGRATION WITH ASSET SERVICE
# ============================================================================

class TestAssetServicePreflight:
    """Tests that preflight is wired into submit_asset correctly."""

    def test_submit_fails_on_missing_blob(self):
        """submit_asset raises ValueError when blob doesn't exist."""
        from services.asset_service import GeospatialAssetService

        pool = MagicMock()
        job_service = AsyncMock()
        blob_repo = MagicMock()
        blob_repo.blob_exists.return_value = False

        svc = GeospatialAssetService(pool, job_service, blob_repo=blob_repo)
        # Mock out repos so platform validation doesn't fail first
        svc.platform_repo = AsyncMock()
        svc.asset_repo = AsyncMock()
        svc.version_repo = AsyncMock()

        with pytest.raises(ValueError, match="Pre-flight validation failed"):
            asyncio.run(
                svc.submit_asset(
                    platform_id="ddh",
                    platform_refs={"dataset_id": "d1", "resource_id": "r1"},
                    data_type="raster",
                    version_label="V1",
                    workflow_id="raster_ingest",
                    input_params={
                        "blob_name": "uploads/missing.tif",
                        "container_name": "rmhazuregeobronze",
                        "collection_id": "test-coll",
                    },
                )
            )

        # Platform validation should NOT have been called — pre-flight rejects first
        svc.platform_repo.get.assert_not_called()

    def test_submit_passes_preflight_then_continues(self):
        """submit_asset proceeds to platform validation when preflight passes."""
        from services.asset_service import GeospatialAssetService
        from core.models.platform import Platform

        pool = MagicMock()
        job_service = AsyncMock()
        blob_repo = MagicMock()
        blob_repo.blob_exists.return_value = True

        svc = GeospatialAssetService(pool, job_service, blob_repo=blob_repo)
        svc.platform_repo = AsyncMock()
        svc.platform_repo.get.return_value = None  # Will fail at platform check

        svc.asset_repo = AsyncMock()
        svc.version_repo = AsyncMock()

        with pytest.raises(ValueError, match="Platform.*not found"):
            asyncio.run(
                svc.submit_asset(
                    platform_id="ddh",
                    platform_refs={"dataset_id": "d1", "resource_id": "r1"},
                    data_type="raster",
                    version_label="V1",
                    workflow_id="raster_ingest",
                    input_params={
                        "blob_name": "uploads/good.tif",
                        "container_name": "rmhazuregeobronze",
                        "collection_id": "test-coll",
                    },
                )
            )

        # Pre-flight passed, so platform validation was reached
        svc.platform_repo.get.assert_called_once_with("ddh")

    def test_non_raster_workflow_skips_preflight(self):
        """Non-raster workflows skip pre-flight entirely."""
        from services.asset_service import GeospatialAssetService

        pool = MagicMock()
        job_service = AsyncMock()
        # No blob_repo — would fail if preflight tried to check blob
        svc = GeospatialAssetService(pool, job_service)
        svc.platform_repo = AsyncMock()
        svc.platform_repo.get.return_value = None

        svc.asset_repo = AsyncMock()
        svc.version_repo = AsyncMock()

        # echo_test doesn't match raster_ prefix → no preflight
        with pytest.raises(ValueError, match="Platform.*not found"):
            asyncio.run(
                svc.submit_asset(
                    platform_id="ddh",
                    platform_refs={},
                    data_type="raster",
                    version_label="V1",
                    workflow_id="echo_test",
                    input_params={},
                )
            )

        # Platform check was reached (preflight was skipped)
        svc.platform_repo.get.assert_called_once()
