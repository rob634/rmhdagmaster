# ============================================================================
# DOMAIN ROUTES + CALLBACK WIRING TESTS
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Tests - Phase 4B-REBUILD domain endpoint and callback tests
# PURPOSE: Verify domain_routes.py endpoints and orchestrator callback wiring
# CREATED: 12 FEB 2026
# ============================================================================
"""
Domain Routes + Callback Wiring Tests

Tests the orchestrator domain HTTP endpoints (api/domain_routes.py) and
the processing callback wiring in orchestrator/loop.py.

Uses FastAPI TestClient for endpoint tests, asyncio.run + mocks for
callback tests.

Run with:
    pytest tests/test_domain_routes.py -v
"""

import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from fastapi.testclient import TestClient
from fastapi import FastAPI

from core.models.geospatial_asset import GeospatialAsset
from core.models.asset_version import AssetVersion
from core.contracts import ApprovalState, ClearanceState, ProcessingStatus

from api.domain_routes import router, set_domain_services


# ============================================================================
# FIXTURES
# ============================================================================

def _make_test_app(asset_service_mock):
    """Create a test FastAPI app with domain routes and mocked service."""
    app = FastAPI()
    app.include_router(router, prefix="/api/v1")
    set_domain_services(asset_service=asset_service_mock)
    return app


def _make_asset(asset_id="abc123", clearance_state=ClearanceState.UNCLEARED):
    """Create a test GeospatialAsset."""
    return GeospatialAsset(
        asset_id=asset_id,
        platform_id="ddh",
        platform_refs={"dataset_id": "d1", "resource_id": "r1"},
        data_type="raster",
        clearance_state=clearance_state,
    )


def _make_version(
    asset_id="abc123",
    version_ordinal=0,
    approval_state=ApprovalState.PENDING_REVIEW,
    processing_state=ProcessingStatus.QUEUED,
    current_job_id=None,
    reviewer=None,
    rejection_reason=None,
):
    """Create a test AssetVersion."""
    return AssetVersion(
        asset_id=asset_id,
        version_ordinal=version_ordinal,
        approval_state=approval_state,
        processing_state=processing_state,
        current_job_id=current_job_id,
        reviewer=reviewer,
        rejection_reason=rejection_reason,
    )


def _make_job(job_id="job-001"):
    """Create a mock Job."""
    job = MagicMock()
    job.job_id = job_id
    return job


# ============================================================================
# SUBMIT
# ============================================================================

class TestDomainSubmit:
    """Tests for POST /api/v1/domain/submit."""

    def test_submit_happy_path(self):
        svc = AsyncMock()
        asset = _make_asset()
        version = _make_version()
        job = _make_job()
        svc.submit_asset = AsyncMock(return_value=(asset, version, job))

        app = _make_test_app(svc)
        client = TestClient(app)

        resp = client.post("/api/v1/domain/submit", json={
            "platform_id": "ddh",
            "platform_refs": {"dataset_id": "d1", "resource_id": "r1"},
            "data_type": "raster",
            "workflow_id": "raster_ingest",
            "input_params": {"blob_path": "test.tif"},
            "correlation_id": "req-123",
        })

        assert resp.status_code == 202
        data = resp.json()
        assert data["asset_id"] == "abc123"
        assert data["version_ordinal"] == 0
        assert data["job_id"] == "job-001"
        assert data["correlation_id"] == "req-123"
        assert data["status"] == "accepted"

    def test_submit_platform_not_found(self):
        svc = AsyncMock()
        svc.submit_asset = AsyncMock(side_effect=ValueError("Platform 'bad' not found"))

        app = _make_test_app(svc)
        client = TestClient(app)

        resp = client.post("/api/v1/domain/submit", json={
            "platform_id": "bad",
            "platform_refs": {},
            "workflow_id": "w",
        })

        assert resp.status_code == 400
        assert "not found" in resp.json()["detail"]

    def test_submit_workflow_not_found(self):
        svc = AsyncMock()
        svc.submit_asset = AsyncMock(side_effect=KeyError("Workflow 'bad' not found"))

        app = _make_test_app(svc)
        client = TestClient(app)

        resp = client.post("/api/v1/domain/submit", json={
            "platform_id": "ddh",
            "platform_refs": {"dataset_id": "d1", "resource_id": "r1"},
            "workflow_id": "bad",
        })

        assert resp.status_code == 404

    def test_submit_missing_required_field(self):
        svc = AsyncMock()
        app = _make_test_app(svc)
        client = TestClient(app)

        resp = client.post("/api/v1/domain/submit", json={
            "platform_id": "ddh",
            # missing workflow_id
        })

        assert resp.status_code == 422  # Pydantic validation


# ============================================================================
# APPROVAL
# ============================================================================

class TestDomainApproval:
    """Tests for approve/reject endpoints."""

    def test_approve_happy_path(self):
        svc = AsyncMock()
        version = _make_version(approval_state=ApprovalState.APPROVED, reviewer="analyst@gov")
        svc.approve_version = AsyncMock(return_value=version)

        app = _make_test_app(svc)
        client = TestClient(app)

        resp = client.post(
            "/api/v1/domain/assets/abc123/versions/0/approve",
            json={"reviewer": "analyst@gov"},
        )

        assert resp.status_code == 200
        data = resp.json()
        assert data["approval_state"] == "approved"
        assert data["reviewer"] == "analyst@gov"

    def test_approve_not_found(self):
        svc = AsyncMock()
        svc.approve_version = AsyncMock(
            side_effect=ValueError("Version 99 not found for asset abc123")
        )

        app = _make_test_app(svc)
        client = TestClient(app)

        resp = client.post(
            "/api/v1/domain/assets/abc123/versions/99/approve",
            json={"reviewer": "analyst@gov"},
        )

        assert resp.status_code == 404

    def test_reject_happy_path(self):
        svc = AsyncMock()
        version = _make_version(
            approval_state=ApprovalState.REJECTED,
            reviewer="analyst@gov",
            rejection_reason="Bad CRS",
        )
        svc.reject_version = AsyncMock(return_value=version)

        app = _make_test_app(svc)
        client = TestClient(app)

        resp = client.post(
            "/api/v1/domain/assets/abc123/versions/0/reject",
            json={"reviewer": "analyst@gov", "reason": "Bad CRS"},
        )

        assert resp.status_code == 200
        data = resp.json()
        assert data["approval_state"] == "rejected"
        assert data["rejection_reason"] == "Bad CRS"

    def test_reject_requires_reason(self):
        svc = AsyncMock()
        app = _make_test_app(svc)
        client = TestClient(app)

        resp = client.post(
            "/api/v1/domain/assets/abc123/versions/0/reject",
            json={"reviewer": "analyst@gov"},  # no reason
        )

        assert resp.status_code == 400
        assert "reason" in resp.json()["detail"].lower()


# ============================================================================
# CLEARANCE
# ============================================================================

class TestDomainClearance:
    """Tests for clearance endpoints."""

    def test_mark_cleared(self):
        svc = AsyncMock()
        asset = _make_asset(clearance_state=ClearanceState.OUO)
        svc.mark_cleared = AsyncMock(return_value=asset)

        app = _make_test_app(svc)
        client = TestClient(app)

        resp = client.post(
            "/api/v1/domain/assets/abc123/clearance/cleared",
            json={"actor": "reviewer@gov"},
        )

        assert resp.status_code == 200
        assert resp.json()["clearance_state"] == "ouo"

    def test_mark_public(self):
        svc = AsyncMock()
        asset = _make_asset(clearance_state=ClearanceState.PUBLIC)
        svc.mark_public = AsyncMock(return_value=asset)

        app = _make_test_app(svc)
        client = TestClient(app)

        resp = client.post(
            "/api/v1/domain/assets/abc123/clearance/public",
            json={"actor": "reviewer@gov"},
        )

        assert resp.status_code == 200
        assert resp.json()["clearance_state"] == "public"

    def test_clearance_invalid_transition(self):
        svc = AsyncMock()
        svc.mark_public = AsyncMock(
            side_effect=ValueError("Cannot make public from 'uncleared'")
        )

        app = _make_test_app(svc)
        client = TestClient(app)

        resp = client.post(
            "/api/v1/domain/assets/abc123/clearance/public",
            json={"actor": "reviewer@gov"},
        )

        assert resp.status_code == 400


# ============================================================================
# DELETE
# ============================================================================

class TestDomainDelete:
    """Tests for DELETE endpoint."""

    def test_soft_delete(self):
        svc = AsyncMock()
        svc.soft_delete_asset = AsyncMock(return_value=True)

        app = _make_test_app(svc)
        client = TestClient(app)

        resp = client.delete("/api/v1/domain/assets/abc123?actor=admin")

        assert resp.status_code == 200
        assert resp.json()["deleted"] is True

    def test_delete_not_found(self):
        svc = AsyncMock()
        svc.soft_delete_asset = AsyncMock(
            side_effect=ValueError("Asset xyz not found")
        )

        app = _make_test_app(svc)
        client = TestClient(app)

        resp = client.delete("/api/v1/domain/assets/xyz?actor=admin")

        assert resp.status_code == 404


# ============================================================================
# CALLBACK WIRING (orchestrator loop)
# ============================================================================

class TestCallbackWiring:
    """Tests that orchestrator calls asset_service callbacks on job completion."""

    def _make_orchestrator(self, asset_service=None):
        """Create a minimal Orchestrator with mocked dependencies."""
        from orchestrator.loop import Orchestrator

        pool = MagicMock()
        workflow_service = MagicMock()
        orch = Orchestrator(
            pool=pool,
            workflow_service=workflow_service,
            poll_interval=1.0,
            event_service=None,
            asset_service=asset_service,
        )
        # Mock the services the loop uses
        orch.job_service = AsyncMock()
        orch._job_repo = AsyncMock()
        return orch

    def test_completed_job_calls_on_processing_completed(self):
        """When a job completes, asset_service.on_processing_completed is called."""
        from core.contracts import JobStatus

        asset_svc = AsyncMock()
        asset_svc.on_processing_completed = AsyncMock(return_value=None)
        orch = self._make_orchestrator(asset_service=asset_svc)

        # Mock job completion check
        orch.job_service.check_job_completion = AsyncMock(return_value=JobStatus.COMPLETED)
        orch.job_service.complete_job = AsyncMock()
        orch._job_repo.release_ownership = AsyncMock()

        # Mock _aggregate_results
        orch._aggregate_results = AsyncMock(return_value={"output": "data"})

        job = MagicMock()
        job.job_id = "job-completed"

        asyncio.run(orch._check_job_completion(job))

        asset_svc.on_processing_completed.assert_awaited_once_with(
            "job-completed", {"output": "data"}
        )

    def test_failed_job_calls_on_processing_failed(self):
        """When a job fails, asset_service.on_processing_failed is called."""
        asset_svc = AsyncMock()
        asset_svc.on_processing_failed = AsyncMock(return_value=None)
        orch = self._make_orchestrator(asset_service=asset_svc)

        from core.contracts import JobStatus, NodeStatus
        orch.job_service.check_job_completion = AsyncMock(return_value=JobStatus.FAILED)
        orch.job_service.fail_job = AsyncMock()
        orch._job_repo.release_ownership = AsyncMock()

        # Mock node repo to return a failed node
        failed_node = MagicMock()
        failed_node.status = NodeStatus.FAILED
        failed_node.error_message = "GDAL error"
        mock_node_repo = AsyncMock()
        mock_node_repo.get_all_for_job = AsyncMock(return_value=[failed_node])

        with patch("orchestrator.loop.NodeRepository", return_value=mock_node_repo):
            job = MagicMock()
            job.job_id = "job-failed"
            asyncio.run(orch._check_job_completion(job))

        asset_svc.on_processing_failed.assert_awaited_once_with(
            "job-failed", "GDAL error"
        )

    def test_standalone_job_no_asset_service(self):
        """Without asset_service, callbacks are skipped (no error)."""
        orch = self._make_orchestrator(asset_service=None)

        from core.contracts import JobStatus
        orch.job_service.check_job_completion = AsyncMock(return_value=JobStatus.COMPLETED)
        orch.job_service.complete_job = AsyncMock()
        orch._job_repo.release_ownership = AsyncMock()
        orch._aggregate_results = AsyncMock(return_value={})

        job = MagicMock()
        job.job_id = "standalone-job"

        # Should not raise
        asyncio.run(orch._check_job_completion(job))

    def test_callback_error_does_not_block_job_completion(self):
        """If asset callback throws, the job still completes normally."""
        asset_svc = AsyncMock()
        asset_svc.on_processing_completed = AsyncMock(
            side_effect=Exception("DB connection lost")
        )
        orch = self._make_orchestrator(asset_service=asset_svc)

        from core.contracts import JobStatus
        orch.job_service.check_job_completion = AsyncMock(return_value=JobStatus.COMPLETED)
        orch.job_service.complete_job = AsyncMock()
        orch._job_repo.release_ownership = AsyncMock()
        orch._aggregate_results = AsyncMock(return_value={})

        job = MagicMock()
        job.job_id = "job-with-bad-callback"

        # Should not raise — callback error is logged but swallowed
        asyncio.run(orch._check_job_completion(job))

        # Job ownership should still be released
        orch._job_repo.release_ownership.assert_awaited_once()
