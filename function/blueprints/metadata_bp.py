# ============================================================================
# CLAUDE CONTEXT - METADATA BLUEPRINT
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Gateway - Layer metadata read endpoints + mutation proxies
# PURPOSE: HTTP endpoints for layer metadata queries (read) and CRUD (proxy)
# CREATED: 13 FEB 2026
# ============================================================================
"""
Metadata Blueprint

Read endpoints served directly from gateway DB connection.
Mutation endpoints proxy to the orchestrator's domain API.

Read endpoints (gateway-local):
- GET    /api/admin/layer-metadata                - List metadata
- GET    /api/admin/layer-metadata/{asset_id}     - Get single

Mutation endpoints (proxy to orchestrator):
- POST   /api/admin/layer-metadata                - Create
- PUT    /api/admin/layer-metadata/{asset_id}     - Update (version required)
- DELETE /api/admin/layer-metadata/{asset_id}     - Delete
"""

import json
import logging

import azure.functions as func

from function.repositories.metadata_query_repo import MetadataQueryRepository

logger = logging.getLogger(__name__)
metadata_bp = func.Blueprint()


# ============================================================================
# HELPERS
# ============================================================================

def _json_response(data, status_code: int = 200) -> func.HttpResponse:
    """Create JSON HTTP response."""
    return func.HttpResponse(
        json.dumps(data, default=str),
        status_code=status_code,
        headers={"Content-Type": "application/json"},
    )


def _error(error: str, details: str = None, status_code: int = 400) -> func.HttpResponse:
    """Create error response."""
    body = {"error": error}
    if details:
        body["details"] = details
    return _json_response(body, status_code)


def _proxy_response(status_code: int, body: dict) -> func.HttpResponse:
    """Relay an orchestrator response back to the caller."""
    return _json_response(body, status_code)


# ============================================================================
# READ — gateway-local (sync, direct DB)
# ============================================================================

@metadata_bp.route(route="admin/layer-metadata", methods=["GET"])
def metadata_list(req: func.HttpRequest) -> func.HttpResponse:
    """
    List layer metadata entries.

    GET /api/admin/layer-metadata?data_type=raster&limit=100&offset=0
    """
    data_type = req.params.get("data_type")
    limit = int(req.params.get("limit", "100"))
    offset = int(req.params.get("offset", "0"))

    try:
        repo = MetadataQueryRepository()
        rows = repo.list_metadata(data_type=data_type, limit=limit, offset=offset)
        total = repo.count_metadata(data_type=data_type)

        return _json_response({
            "metadata": rows,
            "total": total,
            "limit": limit,
            "offset": offset,
        })

    except Exception as e:
        logger.exception(f"Error listing metadata: {e}")
        return _error("Database error", str(e), 500)


@metadata_bp.route(route="admin/layer-metadata/{asset_id}", methods=["GET"])
def metadata_get(req: func.HttpRequest) -> func.HttpResponse:
    """
    Get layer metadata for a specific asset.

    GET /api/admin/layer-metadata/{asset_id}
    """
    asset_id = req.route_params.get("asset_id")

    try:
        repo = MetadataQueryRepository()
        row = repo.get_metadata(asset_id)
        if row is None:
            return _error("Not found", f"Metadata not found for asset '{asset_id}'", 404)

        return _json_response(row)

    except Exception as e:
        logger.exception(f"Error fetching metadata: {e}")
        return _error("Database error", str(e), 500)


# ============================================================================
# CREATE — proxy to orchestrator
# ============================================================================

@metadata_bp.route(route="admin/layer-metadata", methods=["POST"])
def metadata_create(req: func.HttpRequest) -> func.HttpResponse:
    """
    Create layer metadata.

    POST /api/admin/layer-metadata
    Proxies to: POST {ORCHESTRATOR_BASE_URL}/api/v1/domain/metadata/
    """
    from function.services.orchestrator_client import OrchestratorClient

    try:
        body = req.get_json()
    except Exception as e:
        return _error("Invalid JSON", str(e), 400)

    # Validate required fields
    for field in ("asset_id", "data_type", "display_name"):
        if field not in body:
            return _error("Validation error", f"Missing required field: {field}", 422)

    client = OrchestratorClient()
    status_code, resp_body = client.create_metadata(body)
    return _proxy_response(status_code, resp_body)


# ============================================================================
# UPDATE — proxy to orchestrator
# ============================================================================

@metadata_bp.route(route="admin/layer-metadata/{asset_id}", methods=["PUT"])
def metadata_update(req: func.HttpRequest) -> func.HttpResponse:
    """
    Update layer metadata.

    PUT /api/admin/layer-metadata/{asset_id}
    Proxies to: PUT {ORCHESTRATOR_BASE_URL}/api/v1/domain/metadata/{asset_id}
    """
    from function.services.orchestrator_client import OrchestratorClient

    asset_id = req.route_params.get("asset_id")

    try:
        body = req.get_json()
    except Exception as e:
        return _error("Invalid JSON", str(e), 400)

    if "version" not in body:
        return _error("Validation error", "Missing required field: version (for optimistic locking)", 422)

    client = OrchestratorClient()
    status_code, resp_body = client.update_metadata(asset_id, body)
    return _proxy_response(status_code, resp_body)


# ============================================================================
# DELETE — proxy to orchestrator
# ============================================================================

@metadata_bp.route(route="admin/layer-metadata/{asset_id}", methods=["DELETE"])
def metadata_delete(req: func.HttpRequest) -> func.HttpResponse:
    """
    Delete layer metadata.

    DELETE /api/admin/layer-metadata/{asset_id}
    Proxies to: DELETE {ORCHESTRATOR_BASE_URL}/api/v1/domain/metadata/{asset_id}
    """
    from function.services.orchestrator_client import OrchestratorClient

    asset_id = req.route_params.get("asset_id")

    client = OrchestratorClient()
    status_code, resp_body = client.delete_metadata(asset_id)
    return _proxy_response(status_code, resp_body)
