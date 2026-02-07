# ============================================================================
# FUNCTION APP BLUEPRINTS
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Gateway - HTTP endpoint blueprints
# PURPOSE: Azure Functions V2 blueprints for HTTP routes
# CREATED: 04 FEB 2026
# ============================================================================
"""
Function App Blueprints

Azure Functions V2 blueprints organizing HTTP endpoints.
Each blueprint is conditionally registered based on startup validation.
"""

from function.blueprints.gateway_bp import gateway_bp
from function.blueprints.status_bp import status_bp
from function.blueprints.admin_bp import admin_bp
from function.blueprints.proxy_bp import proxy_bp

__all__ = [
    "gateway_bp",
    "status_bp",
    "admin_bp",
    "proxy_bp",
]
