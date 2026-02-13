# ============================================================================
# BASE REPOSITORY FOR FUNCTION APP
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Gateway - Database access base class
# PURPOSE: Read-only PostgreSQL repository for gateway queries
# CREATED: 04 FEB 2026
# ============================================================================
"""
Base Repository for Function App

Read-only PostgreSQL repository for gateway queries.
Uses psycopg3 with dict_row factory (NEVER tuple indexing).

The gateway NEVER writes to the database. All mutations go through
the orchestrator's domain HTTP API. This class intentionally has
NO execute_write or execute_write_returning methods.

Design Principles:
- Read-only queries ONLY (all state changes go through orchestrator)
- dict_row factory ALWAYS (never tuple indexing)
- Connection per query (function app pattern)
- Managed identity support (future)
"""

import logging
from typing import Any, Dict, List, Optional

import psycopg
from psycopg.rows import dict_row

from function.config import get_config

logger = logging.getLogger(__name__)


class FunctionRepository:
    """
    Base repository for function app database access.

    READ-ONLY. The gateway never writes to the database.
    All mutations go through the orchestrator's domain HTTP API.

    Pattern:
    - Connection per query (no connection pooling in functions)
    - dict_row factory always (access columns by name, never index)
    - Simple error handling (let exceptions propagate)
    """

    def __init__(self, schema: str = "dagapp"):
        """
        Initialize repository.

        Args:
            schema: Database schema name (default: dagapp)
        """
        self.schema = schema
        self._config = get_config()
        self._conn_string = self._config.get_connection_string()

    def _get_connection(self) -> psycopg.Connection:
        """
        Get a database connection with dict_row factory.

        Returns:
            psycopg.Connection configured with dict_row
        """
        conn = psycopg.connect(self._conn_string)
        conn.row_factory = dict_row
        return conn

    def execute_scalar(self, query: str, params: tuple = ()) -> Any:
        """
        Execute query and return single scalar value.

        Args:
            query: SQL query string
            params: Query parameters tuple

        Returns:
            Single value from first column of first row, or None
        """
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                row = cur.fetchone()
                if row:
                    # dict_row returns dict, get first value
                    return list(row.values())[0]
                return None

    def execute_one(self, query: str, params: tuple = ()) -> Optional[Dict[str, Any]]:
        """
        Execute query and return single row as dict.

        Args:
            query: SQL query string
            params: Query parameters tuple

        Returns:
            Single row as dict, or None if no rows
        """
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                return cur.fetchone()

    def execute_many(self, query: str, params: tuple = ()) -> List[Dict[str, Any]]:
        """
        Execute query and return all rows as list of dicts.

        Args:
            query: SQL query string
            params: Query parameters tuple

        Returns:
            List of rows as dicts
        """
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                return cur.fetchall()

    def execute_count(self, query: str, params: tuple = ()) -> int:
        """
        Execute COUNT query and return integer.

        Args:
            query: SQL query (should return count in first column)
            params: Query parameters tuple

        Returns:
            Integer count
        """
        result = self.execute_scalar(query, params)
        return int(result) if result else 0


__all__ = ["FunctionRepository"]
