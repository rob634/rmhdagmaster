# ============================================================================
# CLAUDE CONTEXT - SCHEMA MODULE
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Core - Schema generation from Pydantic models
# PURPOSE: Generate PostgreSQL DDL from Pydantic models (single source of truth)
# LAST_REVIEWED: 28 JAN 2026
# ============================================================================

from core.schema.ddl_utils import (
    IndexBuilder,
    TriggerBuilder,
    CommentBuilder,
    SchemaUtils,
    TYPE_MAP,
    get_postgres_type,
)
from core.schema.sql_generator import PydanticToSQL

__all__ = [
    # Generator
    "PydanticToSQL",
    # Utilities
    "IndexBuilder",
    "TriggerBuilder",
    "CommentBuilder",
    "SchemaUtils",
    "TYPE_MAP",
    "get_postgres_type",
]
