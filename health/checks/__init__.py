# ============================================================================
# HEALTH CHECK PLUGINS
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Infrastructure - Health check implementations
# PURPOSE: Specific health checks for DAG orchestrator components
# CREATED: 31 JAN 2026
# ============================================================================
"""
Health Check Plugins

Concrete implementations of health checks for the DAG orchestrator:

Startup Checks (priority 10):
- process: Basic process health (always healthy if running)
- config: Required environment variables present

Infrastructure Checks (priority 20):
- blob_storage: Azure Blob Storage connectivity
- service_bus: Azure Service Bus connectivity

Database Checks (priority 30):
- postgres: PostgreSQL connection
- pgstac: pgSTAC schema available
- dagapp: DAG app schema available

Application Checks (priority 40):
- orchestrator: Orchestrator loop running
- workflows: Workflow definitions loaded
- handlers: Handler registry populated

External Checks (priority 50):
- worker: DAG worker app health and queue status

Import this module to register all checks:
    import health.checks
"""

# Import all check modules to trigger registration
from health.checks.startup import ProcessCheck, ConfigCheck
from health.checks.infrastructure import BlobStorageCheck, ServiceBusCheck
from health.checks.database import PostgresCheck, PgStacCheck, DagAppCheck
from health.checks.application import OrchestratorCheck, WorkflowsCheck, HandlersCheck
from health.checks.worker import WorkerHealthCheck

__all__ = [
    # Startup
    "ProcessCheck",
    "ConfigCheck",
    # Infrastructure
    "BlobStorageCheck",
    "ServiceBusCheck",
    # Database
    "PostgresCheck",
    "PgStacCheck",
    "DagAppCheck",
    # Application
    "OrchestratorCheck",
    "WorkflowsCheck",
    "HandlersCheck",
    # External
    "WorkerHealthCheck",
]
