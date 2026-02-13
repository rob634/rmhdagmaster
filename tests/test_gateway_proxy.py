# ============================================================================
# GATEWAY PROXY TESTS
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Tests - Phase 4B Pass 3 gateway proxy tests
# PURPOSE: Verify OrchestratorClient and gateway proxy endpoints
# CREATED: 12 FEB 2026
# ============================================================================
"""
Gateway Proxy Tests

Tests the OrchestratorClient HTTP client and the gateway blueprint
proxy endpoints (platform_bp submit, asset_bp mutations).

Uses unittest.mock to patch httpx calls — no real HTTP traffic.

Run with:
    pytest tests/test_gateway_proxy.py -v
"""

import json
import pytest
from unittest.mock import patch, MagicMock

from function.services.orchestrator_client import OrchestratorClient


# ============================================================================
# ORCHESTRATOR CLIENT TESTS
# ============================================================================

class TestOrchestratorClient:
    """Tests for OrchestratorClient HTTP methods."""

    def _mock_response(self, status_code=200, json_data=None):
        """Create a mock httpx.Response."""
        resp = MagicMock()
        resp.status_code = status_code
        resp.json.return_value = json_data or {}
        resp.text = json.dumps(json_data or {})
        return resp

    @patch("function.services.orchestrator_client.httpx.Client")
    def test_submit_happy_path(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.request.return_value = self._mock_response(
            202, {"asset_id": "abc", "version_ordinal": 0, "job_id": "j1", "status": "accepted"}
        )
        mock_client_cls.return_value = mock_client

        client = OrchestratorClient(base_url="http://orch:8000")
        status, body = client.submit({"platform_id": "ddh", "workflow_id": "w"})

        assert status == 202
        assert body["asset_id"] == "abc"
        assert body["job_id"] == "j1"

        mock_client.request.assert_called_once()
        call_args = mock_client.request.call_args
        assert call_args[0][0] == "POST"
        assert "/api/v1/domain/submit" in call_args[0][1]

    @patch("function.services.orchestrator_client.httpx.Client")
    def test_approve_relays_200(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.request.return_value = self._mock_response(
            200, {"approval_state": "approved", "reviewer": "bob"}
        )
        mock_client_cls.return_value = mock_client

        client = OrchestratorClient(base_url="http://orch:8000")
        status, body = client.approve("abc", 0, {"reviewer": "bob"})

        assert status == 200
        assert body["approval_state"] == "approved"

    @patch("function.services.orchestrator_client.httpx.Client")
    def test_reject_relays_200(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.request.return_value = self._mock_response(
            200, {"approval_state": "rejected", "rejection_reason": "bad"}
        )
        mock_client_cls.return_value = mock_client

        client = OrchestratorClient(base_url="http://orch:8000")
        status, body = client.reject("abc", 0, {"reviewer": "bob", "reason": "bad"})

        assert status == 200
        assert body["approval_state"] == "rejected"

    @patch("function.services.orchestrator_client.httpx.Client")
    def test_error_400_relayed(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.request.return_value = self._mock_response(
            400, {"detail": "Platform 'bad' not found"}
        )
        mock_client_cls.return_value = mock_client

        client = OrchestratorClient(base_url="http://orch:8000")
        status, body = client.submit({"platform_id": "bad"})

        assert status == 400
        assert "not found" in body["detail"]

    @patch("function.services.orchestrator_client.httpx.Client")
    def test_error_404_relayed(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.request.return_value = self._mock_response(
            404, {"detail": "Version 99 not found"}
        )
        mock_client_cls.return_value = mock_client

        client = OrchestratorClient(base_url="http://orch:8000")
        status, body = client.approve("abc", 99, {"reviewer": "bob"})

        assert status == 404

    @patch("function.services.orchestrator_client.httpx.Client")
    def test_error_409_relayed(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.request.return_value = self._mock_response(
            409, {"detail": "Conflict: already approved"}
        )
        mock_client_cls.return_value = mock_client

        client = OrchestratorClient(base_url="http://orch:8000")
        status, body = client.approve("abc", 0, {"reviewer": "bob"})

        assert status == 409

    @patch("function.services.orchestrator_client.httpx.Client")
    def test_orchestrator_500_becomes_502(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.request.return_value = self._mock_response(
            500, {"detail": "Internal Server Error"}
        )
        mock_client_cls.return_value = mock_client

        client = OrchestratorClient(base_url="http://orch:8000")
        status, body = client.submit({"platform_id": "ddh"})

        assert status == 502
        assert "Orchestrator error" in body["error"]

    @patch("function.services.orchestrator_client.httpx.Client")
    def test_connection_error_returns_502(self, mock_client_cls):
        import httpx

        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.request.side_effect = httpx.ConnectError("Connection refused")
        mock_client_cls.return_value = mock_client

        client = OrchestratorClient(base_url="http://orch:8000")
        status, body = client.submit({"platform_id": "ddh"})

        assert status == 502
        assert "unreachable" in body["error"].lower()

    @patch("function.services.orchestrator_client.httpx.Client")
    def test_timeout_returns_504(self, mock_client_cls):
        import httpx

        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.request.side_effect = httpx.ReadTimeout("Read timed out")
        mock_client_cls.return_value = mock_client

        client = OrchestratorClient(base_url="http://orch:8000")
        status, body = client.submit({"platform_id": "ddh"})

        assert status == 504
        assert "timeout" in body["error"].lower()

    @patch("function.services.orchestrator_client.httpx.Client")
    def test_mark_cleared(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.request.return_value = self._mock_response(
            200, {"asset_id": "abc", "clearance_state": "ouo"}
        )
        mock_client_cls.return_value = mock_client

        client = OrchestratorClient(base_url="http://orch:8000")
        status, body = client.mark_cleared("abc", {"actor": "admin"})

        assert status == 200
        assert body["clearance_state"] == "ouo"

    @patch("function.services.orchestrator_client.httpx.Client")
    def test_mark_public(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.request.return_value = self._mock_response(
            200, {"asset_id": "abc", "clearance_state": "public"}
        )
        mock_client_cls.return_value = mock_client

        client = OrchestratorClient(base_url="http://orch:8000")
        status, body = client.mark_public("abc", {"actor": "admin"})

        assert status == 200
        assert body["clearance_state"] == "public"

    @patch("function.services.orchestrator_client.httpx.Client")
    def test_delete_asset(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.request.return_value = self._mock_response(
            200, {"deleted": True, "asset_id": "abc"}
        )
        mock_client_cls.return_value = mock_client

        client = OrchestratorClient(base_url="http://orch:8000")
        status, body = client.delete_asset("abc", "admin")

        assert status == 200
        assert body["deleted"] is True

        # Verify actor passed as query param
        call_args = mock_client.request.call_args
        assert call_args[1]["params"] == {"actor": "admin"}

    @patch("function.services.orchestrator_client.httpx.Client")
    def test_delete_not_found_relayed(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.request.return_value = self._mock_response(
            404, {"detail": "Asset xyz not found"}
        )
        mock_client_cls.return_value = mock_client

        client = OrchestratorClient(base_url="http://orch:8000")
        status, body = client.delete_asset("xyz", "admin")

        assert status == 404
