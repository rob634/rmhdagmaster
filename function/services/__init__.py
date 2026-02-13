# ============================================================================
# FUNCTION APP SERVICES
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Gateway - Service layer for orchestrator proxying
# PURPOSE: HTTP client for forwarding mutations to orchestrator domain API
# CREATED: 12 FEB 2026
# ============================================================================

from function.services.orchestrator_client import OrchestratorClient

__all__ = ["OrchestratorClient"]
