# ============================================================================
# RMHDAGMASTER - Azure Function App (Gateway)
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Gateway - Job submission and status queries
# PURPOSE: API Gateway for DAG orchestration platform
# CREATED: 04 FEB 2026
# ============================================================================
"""
RMH DAG Gateway Function App

Azure Functions V2 entry point providing:
- Job submission to dag-jobs Service Bus queue
- Status queries (jobs, nodes, events)
- Health check proxying to Docker apps
- Administrative endpoints

Deployment:
- Azure Function App (Consumption or Premium plan)
- Uses .funcignore to exclude heavy modules (~50MB vs ~250MB)

Endpoints:
- /api/livez - Liveness probe (always available)
- /api/readyz - Readiness probe (checks startup validation)
- /api/gateway/* - Job submission
- /api/status/* - Status queries
- /api/proxy/* - Forward to Docker apps
- /api/admin/* - Administrative endpoints
"""

import azure.functions as func
import json
import logging

# ============================================================================
# CREATE APP FIRST (before any imports that might fail)
# ============================================================================

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

logger = logging.getLogger(__name__)
logger.info("=" * 60)
logger.info("RMHDAGMASTER Gateway Function App Starting")
logger.info("=" * 60)

# ============================================================================
# EARLY PROBES (Before validation - always available)
# ============================================================================
# These endpoints must be available even if startup validation fails.
# They allow health checks to determine if the function app is running.


@app.route(route="livez", methods=["GET"])
def liveness_probe(req: func.HttpRequest) -> func.HttpResponse:
    """
    Liveness probe - always returns 200 if function is running.

    GET /api/livez

    Used by Azure to determine if the function app process is alive.
    Always responds regardless of startup validation state.
    """
    return func.HttpResponse(
        json.dumps({"alive": True, "service": "rmhdagmaster-gateway"}),
        status_code=200,
        headers={"Content-Type": "application/json"},
    )


@app.route(route="readyz", methods=["GET"])
def readiness_probe(req: func.HttpRequest) -> func.HttpResponse:
    """
    Readiness probe - returns 200 if startup validation passed.

    GET /api/readyz

    Used by Azure to determine if the function app is ready to receive traffic.
    Returns 503 if startup validation failed.
    """
    from function.startup import STARTUP_STATE

    if STARTUP_STATE.all_passed:
        return func.HttpResponse(
            json.dumps({"ready": True, "service": "rmhdagmaster-gateway"}),
            status_code=200,
            headers={"Content-Type": "application/json"},
        )

    return func.HttpResponse(
        json.dumps({
            "ready": False,
            "service": "rmhdagmaster-gateway",
            "failed_checks": STARTUP_STATE.failed_check_names(),
            "details": STARTUP_STATE.to_dict(),
        }),
        status_code=503,
        headers={"Content-Type": "application/json"},
    )


# ============================================================================
# STARTUP VALIDATION
# ============================================================================
# Validate environment and dependencies before registering blueprints.
# If validation fails, only /livez and /readyz are available.

logger.info("Running startup validation...")

from function.startup import validate_startup, STARTUP_STATE

_startup_result = validate_startup()

if not STARTUP_STATE.all_passed:
    logger.error("=" * 60)
    logger.error("STARTUP VALIDATION FAILED")
    logger.error("=" * 60)
    logger.error("Only /api/livez and /api/readyz endpoints available")
    for check in STARTUP_STATE.failed_checks():
        logger.error(f"  FAILED: {check.name} - {check.error_message}")
    logger.error("=" * 60)
else:
    logger.info("Startup validation PASSED")


# ============================================================================
# BLUEPRINT REGISTRATION (Conditional on startup success)
# ============================================================================
# Blueprints are only registered if startup validation passes.
# This prevents errors from propagating to all endpoints.

if STARTUP_STATE.all_passed:
    logger.info("Registering blueprints...")

    # Gateway blueprint (job submission)
    from function.blueprints.gateway_bp import gateway_bp
    app.register_functions(gateway_bp)
    logger.info("  Registered: gateway_bp (job submission)")

    # Status blueprint (status queries)
    from function.blueprints.status_bp import status_bp
    app.register_functions(status_bp)
    logger.info("  Registered: status_bp (status queries)")

    # Admin blueprint (administrative endpoints)
    from function.blueprints.admin_bp import admin_bp
    app.register_functions(admin_bp)
    logger.info("  Registered: admin_bp (admin endpoints)")

    # Proxy blueprint (forward to Docker apps)
    from function.blueprints.proxy_bp import proxy_bp
    app.register_functions(proxy_bp)
    logger.info("  Registered: proxy_bp (HTTP proxy)")

    logger.info("=" * 60)
    logger.info("RMHDAGMASTER Gateway Function App Ready")
    logger.info("=" * 60)
else:
    logger.warning("=" * 60)
    logger.warning("SKIPPING blueprint registration - startup validation failed")
    logger.warning("=" * 60)


# ============================================================================
# EXPORTS
# ============================================================================

__all__ = ["app"]
