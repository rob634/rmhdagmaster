# ============================================================================
# VERSION - DAG ORCHESTRATOR
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# ============================================================================
"""
Version information for DAG Orchestrator.

This is the single source of truth for the application version.
Updated manually for each release.
"""
# Version format: major.minor.patch.build
# Criteria for 0.2 - complete echo test works
__version__ = "0.2.2.1"
__version_info__ = tuple(int(x) for x in __version__.split("."))

# Build metadata
BUILD_DATE = "2026-02-02"

# Deployment info
ORCHESTRATOR_IMAGE = f"rmhazureacr.azurecr.io/rmhdagmaster:v{__version__}"
WORKER_IMAGE = f"rmhazureacr.azurecr.io/rmhdagworker:v{__version__}"
EPOCH = 5
CODENAME = "DAG Orchestrator"
