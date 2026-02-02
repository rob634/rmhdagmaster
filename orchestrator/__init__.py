# ============================================================================
# ORCHESTRATOR MODULE
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Core - Main orchestration loop
# PURPOSE: Coordinate job execution across nodes
# CREATED: 29 JAN 2026
# ============================================================================
"""
Orchestrator Module

The orchestration loop that drives DAG execution.

Usage:
    from orchestrator import Orchestrator

    orchestrator = Orchestrator(pool, workflow_service)
    await orchestrator.run()  # Starts the loop
"""

from .loop import Orchestrator

__all__ = ["Orchestrator"]
