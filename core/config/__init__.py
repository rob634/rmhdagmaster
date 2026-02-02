# ============================================================================
# CONFIGURATION MODULE
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Core - Configuration and defaults
# PURPOSE: Centralized configuration management
# CREATED: 31 JAN 2026
# ============================================================================
"""
Configuration Module

Provides centralized configuration and defaults for the DAG orchestrator.
"""

from core.config.defaults import (
    TaskRoutingDefaults,
    RasterDefaults,
    VectorDefaults,
    STACDefaults,
    TimeoutDefaults,
    get_defaults,
)

__all__ = [
    "TaskRoutingDefaults",
    "RasterDefaults",
    "VectorDefaults",
    "STACDefaults",
    "TimeoutDefaults",
    "get_defaults",
]
