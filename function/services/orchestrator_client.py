# ============================================================================
# ORCHESTRATOR HTTP CLIENT
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Gateway - Sync HTTP client for orchestrator domain API
# PURPOSE: Forward mutation requests from gateway to orchestrator
# CREATED: 12 FEB 2026
# ============================================================================
"""
Orchestrator HTTP Client

Sync httpx client for proxying gateway mutation requests to the
orchestrator's domain API (POST /api/v1/domain/*).

Azure Functions are synchronous, so this uses httpx sync client.
All methods return (status_code, response_dict) tuples — the caller
decides how to format the HTTP response.
"""

import logging
from typing import Any, Dict, Optional, Tuple

import httpx

from function.config import get_config

logger = logging.getLogger(__name__)

# Timeout: 30s connect, 60s read (orchestrator may create job + publish)
DEFAULT_TIMEOUT = httpx.Timeout(connect=30.0, read=60.0, write=30.0, pool=30.0)


class OrchestratorClient:
    """Sync HTTP client for orchestrator domain API."""

    def __init__(self, base_url: Optional[str] = None, timeout: Optional[httpx.Timeout] = None):
        self._base_url = (base_url or get_config().orchestrator_url).rstrip("/")
        self._timeout = timeout or DEFAULT_TIMEOUT

    def _request(
        self,
        method: str,
        path: str,
        json_body: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, str]] = None,
    ) -> Tuple[int, Dict[str, Any]]:
        """
        Make a request to the orchestrator domain API.

        Returns (status_code, response_body_dict).
        On connection failure, returns (502, error_dict).
        """
        url = f"{self._base_url}{path}"

        try:
            with httpx.Client(timeout=self._timeout) as client:
                resp = client.request(method, url, json=json_body, params=params)

            # Parse JSON response
            try:
                body = resp.json()
            except Exception:
                body = {"detail": resp.text}

            # Map orchestrator 5xx to gateway 502
            if resp.status_code >= 500:
                logger.error(f"Orchestrator error {resp.status_code}: {path} -> {body}")
                return 502, {"error": "Orchestrator error", "detail": body.get("detail", str(body))}

            return resp.status_code, body

        except httpx.ConnectError as e:
            logger.error(f"Cannot reach orchestrator at {url}: {e}")
            return 502, {"error": "Orchestrator unreachable", "detail": str(e)}
        except httpx.TimeoutException as e:
            logger.error(f"Orchestrator timeout: {url}: {e}")
            return 504, {"error": "Orchestrator timeout", "detail": str(e)}
        except Exception as e:
            logger.exception(f"Unexpected error proxying to orchestrator: {e}")
            return 502, {"error": "Proxy error", "detail": str(e)}

    # ------------------------------------------------------------------
    # SUBMIT
    # ------------------------------------------------------------------

    def submit(self, body: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        """
        Proxy asset submission to orchestrator.

        POST /api/v1/domain/submit
        """
        return self._request("POST", "/api/v1/domain/submit", json_body=body)

    # ------------------------------------------------------------------
    # APPROVAL
    # ------------------------------------------------------------------

    def approve(
        self, asset_id: str, ordinal: int, body: Dict[str, Any]
    ) -> Tuple[int, Dict[str, Any]]:
        """POST /api/v1/domain/assets/{asset_id}/versions/{ordinal}/approve"""
        return self._request(
            "POST",
            f"/api/v1/domain/assets/{asset_id}/versions/{ordinal}/approve",
            json_body=body,
        )

    def reject(
        self, asset_id: str, ordinal: int, body: Dict[str, Any]
    ) -> Tuple[int, Dict[str, Any]]:
        """POST /api/v1/domain/assets/{asset_id}/versions/{ordinal}/reject"""
        return self._request(
            "POST",
            f"/api/v1/domain/assets/{asset_id}/versions/{ordinal}/reject",
            json_body=body,
        )

    # ------------------------------------------------------------------
    # CLEARANCE
    # ------------------------------------------------------------------

    def mark_cleared(
        self, asset_id: str, body: Dict[str, Any]
    ) -> Tuple[int, Dict[str, Any]]:
        """POST /api/v1/domain/assets/{asset_id}/clearance/cleared"""
        return self._request(
            "POST",
            f"/api/v1/domain/assets/{asset_id}/clearance/cleared",
            json_body=body,
        )

    def mark_public(
        self, asset_id: str, body: Dict[str, Any]
    ) -> Tuple[int, Dict[str, Any]]:
        """POST /api/v1/domain/assets/{asset_id}/clearance/public"""
        return self._request(
            "POST",
            f"/api/v1/domain/assets/{asset_id}/clearance/public",
            json_body=body,
        )

    # ------------------------------------------------------------------
    # DELETE
    # ------------------------------------------------------------------

    def delete_asset(
        self, asset_id: str, actor: str
    ) -> Tuple[int, Dict[str, Any]]:
        """DELETE /api/v1/domain/assets/{asset_id}?actor=..."""
        return self._request(
            "DELETE",
            f"/api/v1/domain/assets/{asset_id}",
            params={"actor": actor},
        )

    # ------------------------------------------------------------------
    # LAYER METADATA
    # ------------------------------------------------------------------

    def create_metadata(
        self, body: Dict[str, Any]
    ) -> Tuple[int, Dict[str, Any]]:
        """POST /api/v1/domain/metadata/"""
        return self._request("POST", "/api/v1/domain/metadata/", json_body=body)

    def update_metadata(
        self, asset_id: str, body: Dict[str, Any]
    ) -> Tuple[int, Dict[str, Any]]:
        """PUT /api/v1/domain/metadata/{asset_id}"""
        return self._request(
            "PUT",
            f"/api/v1/domain/metadata/{asset_id}",
            json_body=body,
        )

    def delete_metadata(
        self, asset_id: str
    ) -> Tuple[int, Dict[str, Any]]:
        """DELETE /api/v1/domain/metadata/{asset_id}"""
        return self._request(
            "DELETE",
            f"/api/v1/domain/metadata/{asset_id}",
        )
