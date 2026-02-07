# ============================================================================
# PROXY BLUEPRINT
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Gateway - HTTP proxy endpoints
# PURPOSE: Forward HTTP requests to Docker apps (orchestrator, worker)
# CREATED: 04 FEB 2026
# ============================================================================
"""
Proxy Blueprint

HTTP proxy endpoints for forwarding requests to Docker apps:
- * /api/proxy/{target}/{path} - Proxy any request to orchestrator/worker
- GET /api/proxy/health - Health check all Docker apps
"""

import json
import logging
from typing import Optional

import azure.functions as func
import httpx

from function.config import get_config
from function.models.responses import ProxyResponse, ErrorResponse

logger = logging.getLogger(__name__)
proxy_bp = func.Blueprint()


def _get_target_url(target: str) -> Optional[str]:
    """Get base URL for target service."""
    config = get_config()
    if target == "orchestrator":
        return config.orchestrator_url
    elif target == "worker":
        return config.worker_url
    return None


def _json_response(data: dict, status_code: int = 200) -> func.HttpResponse:
    """Create JSON HTTP response."""
    return func.HttpResponse(
        json.dumps(data, default=str),
        status_code=status_code,
        headers={"Content-Type": "application/json"},
    )


@proxy_bp.route(route="proxy/{target}/{*path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
def proxy_request(req: func.HttpRequest) -> func.HttpResponse:
    """
    Proxy request to Docker app.

    ANY /api/proxy/{target}/{path}

    Target must be 'orchestrator' or 'worker'.
    Path is forwarded as-is to the target.

    Examples:
      GET /api/proxy/orchestrator/health
      GET /api/proxy/orchestrator/api/v1/jobs
      POST /api/proxy/worker/api/v1/task
    """
    target = req.route_params.get("target")
    path = req.route_params.get("path", "")

    # Validate target
    base_url = _get_target_url(target)
    if not base_url:
        return _json_response(
            ErrorResponse(
                error="Invalid target",
                details=f"Target must be 'orchestrator' or 'worker', got '{target}'",
            ).model_dump(),
            status_code=400,
        )

    # Build full URL
    full_url = f"{base_url}/{path}"
    if req.params:
        query_string = "&".join(f"{k}={v}" for k, v in req.params.items())
        full_url = f"{full_url}?{query_string}"

    logger.info(f"Proxying {req.method} to {target}: /{path}")

    try:
        # Forward request with timeout
        with httpx.Client(timeout=30.0) as client:
            # Prepare headers (forward relevant ones)
            headers = {}
            forward_headers = [
                "Content-Type",
                "Authorization",
                "X-Correlation-ID",
                "X-Request-ID",
            ]
            for key in forward_headers:
                if value := req.headers.get(key):
                    headers[key] = value

            # Make request
            response = client.request(
                method=req.method,
                url=full_url,
                headers=headers,
                content=req.get_body() if req.method in ["POST", "PUT", "PATCH"] else None,
            )

            logger.info(f"Proxy response: {target}/{path} -> {response.status_code}")

            # Return proxied response with original content type
            return func.HttpResponse(
                response.text,
                status_code=response.status_code,
                headers={
                    "Content-Type": response.headers.get("Content-Type", "application/json"),
                    "X-Proxy-Target": target,
                },
            )

    except httpx.ConnectError as e:
        logger.error(f"Failed to connect to {target}: {e}")
        return _json_response(
            ProxyResponse(
                success=False,
                target=target,
                path=path,
                status_code=503,
                error=f"Cannot connect to {target}: connection refused",
            ).model_dump(),
            status_code=503,
        )
    except httpx.TimeoutException as e:
        logger.error(f"Timeout connecting to {target}: {e}")
        return _json_response(
            ProxyResponse(
                success=False,
                target=target,
                path=path,
                status_code=504,
                error=f"Timeout connecting to {target}",
            ).model_dump(),
            status_code=504,
        )
    except Exception as e:
        logger.exception(f"Proxy error: {e}")
        return _json_response(
            ErrorResponse(error="Proxy error", details=str(e)).model_dump(),
            status_code=500,
        )


@proxy_bp.route(route="proxy/health", methods=["GET"])
def proxy_health_all(req: func.HttpRequest) -> func.HttpResponse:
    """
    Check health of all Docker apps.

    GET /api/proxy/health

    Returns health status for orchestrator and worker.
    """
    results = {}
    config = get_config()

    for target in ["orchestrator", "worker"]:
        base_url = _get_target_url(target)
        if not base_url or base_url == "http://localhost:8000" and target == "worker":
            # Check if URL is actually configured vs default
            configured = (
                target == "orchestrator" and config.orchestrator_url != "http://localhost:8000"
            ) or (
                target == "worker" and config.worker_url != "http://localhost:8001"
            )
            if not configured:
                results[target] = {
                    "status": "not_configured",
                    "url": base_url,
                }
                continue

        try:
            with httpx.Client(timeout=5.0) as client:
                # Try common health endpoints
                for health_path in ["/health", "/healthz", "/readyz"]:
                    try:
                        response = client.get(f"{base_url}{health_path}")
                        if response.status_code == 200:
                            results[target] = {
                                "status": "healthy",
                                "status_code": response.status_code,
                                "url": base_url,
                                "endpoint": health_path,
                            }
                            break
                    except Exception:
                        continue
                else:
                    # No health endpoint responded
                    results[target] = {
                        "status": "unhealthy",
                        "url": base_url,
                        "error": "No health endpoint responded",
                    }

        except httpx.ConnectError:
            results[target] = {
                "status": "unreachable",
                "url": base_url,
            }
        except httpx.TimeoutException:
            results[target] = {
                "status": "timeout",
                "url": base_url,
            }
        except Exception as e:
            results[target] = {
                "status": "error",
                "error": str(e),
                "url": base_url,
            }

    all_healthy = all(
        r.get("status") in ("healthy", "not_configured")
        for r in results.values()
    )

    return _json_response({
        "targets": results,
        "all_healthy": all_healthy,
    })


__all__ = ["proxy_bp"]
