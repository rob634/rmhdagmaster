# ============================================================================
# AUTHENTICATION MODULE
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# PURPOSE: Azure authentication for PostgreSQL and Service Bus
# CREATED: 29 JAN 2026
# ============================================================================
"""
Authentication module for DAG Orchestrator.

Provides Azure Managed Identity authentication for:
- PostgreSQL (dagapp schema)
- Service Bus (task dispatch)

Usage:
    from infrastructure.auth import get_postgres_connection_string

    conn_str = get_postgres_connection_string()
"""

from infrastructure.auth.postgres_auth import (
    get_postgres_connection_string,
    get_postgres_token,
    get_postgres_token_status,
)

__all__ = [
    'get_postgres_connection_string',
    'get_postgres_token',
    'get_postgres_token_status',
]
