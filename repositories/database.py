# ============================================================================
# DATABASE CONNECTION POOL
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Core - Async PostgreSQL connection management
# PURPOSE: Provide connection pooling for psycopg3 async
# CREATED: 29 JAN 2026
# UPDATED: 29 JAN 2026 - Added managed identity support
# ============================================================================
"""
Database Connection Pool

Manages async PostgreSQL connections using psycopg3 and psycopg_pool.
Singleton pattern ensures one pool per application.

Supports two authentication methods:
1. Managed Identity (Azure) - USE_MANAGED_IDENTITY=true
2. Password auth (local dev) - DATABASE_URL or POSTGRES_* vars

Usage:
    from repositories.database import get_pool

    pool = await get_pool()
    async with pool.connection() as conn:
        result = await conn.execute("SELECT 1")
"""

import os
import logging
from typing import Optional
from contextlib import asynccontextmanager

from psycopg_pool import AsyncConnectionPool
from psycopg import AsyncConnection

logger = logging.getLogger(__name__)

# Global pool instance
_pool: Optional[AsyncConnectionPool] = None


def get_connection_string() -> str:
    """
    Get database connection string from environment.

    Priority:
    1. Managed Identity (if USE_MANAGED_IDENTITY=true)
    2. DATABASE_URL environment variable
    3. Individual POSTGRES_* components

    Returns:
        PostgreSQL connection string
    """
    # Check if managed identity is enabled
    use_mi = os.environ.get("USE_MANAGED_IDENTITY", "false").lower() == "true"

    if use_mi:
        try:
            from infrastructure.auth import get_postgres_connection_string
            logger.info("Using Managed Identity for PostgreSQL authentication")
            return get_postgres_connection_string()
        except ImportError:
            logger.warning("Auth module not available, falling back to password auth")
        except Exception as e:
            logger.error(f"Managed identity auth failed: {e}, falling back to password auth")

    # Check for DATABASE_URL
    if url := os.environ.get("DATABASE_URL"):
        return url

    # Build from individual components
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = os.environ.get("POSTGRES_PORT", "5432")
    name = os.environ.get("POSTGRES_DB", "postgres")
    user = os.environ.get("POSTGRES_USER", "postgres")
    password = os.environ.get("POSTGRES_PASSWORD", "")
    sslmode = os.environ.get("POSTGRES_SSLMODE", "require")

    return f"postgresql://{user}:{password}@{host}:{port}/{name}?sslmode={sslmode}"


async def init_pool(
    min_size: int = 2,
    max_size: int = 10,
    connection_string: Optional[str] = None,
) -> AsyncConnectionPool:
    """
    Initialize the global connection pool.

    Args:
        min_size: Minimum connections to maintain
        max_size: Maximum connections allowed
        connection_string: Override connection string (defaults to env)

    Returns:
        AsyncConnectionPool instance
    """
    global _pool

    if _pool is not None:
        logger.warning("Pool already initialized, returning existing pool")
        return _pool

    conninfo = connection_string or get_connection_string()

    # Mask password in logs
    if "@" in conninfo:
        # URL format
        safe_conninfo = conninfo.split("@")[-1]
    elif "password=" in conninfo:
        # Key-value format
        safe_conninfo = conninfo.split("password=")[0] + "password=*** " + conninfo.split("sslmode=")[-1] if "sslmode=" in conninfo else conninfo.split("password=")[0] + "password=***"
    else:
        safe_conninfo = conninfo

    logger.info(f"Initializing connection pool: {safe_conninfo}")

    _pool = AsyncConnectionPool(
        conninfo=conninfo,
        min_size=min_size,
        max_size=max_size,
        open=False,  # We'll open it explicitly
    )

    await _pool.open()
    logger.info(f"Connection pool opened (min={min_size}, max={max_size})")

    return _pool


async def get_pool() -> AsyncConnectionPool:
    """
    Get the global connection pool, initializing if needed.

    Returns:
        AsyncConnectionPool instance
    """
    global _pool

    if _pool is None:
        await init_pool()

    return _pool


async def close_pool() -> None:
    """Close the global connection pool."""
    global _pool

    if _pool is not None:
        await _pool.close()
        _pool = None
        logger.info("Connection pool closed")


@asynccontextmanager
async def get_connection():
    """
    Get a connection from the pool.

    Usage:
        async with get_connection() as conn:
            await conn.execute(...)
    """
    pool = await get_pool()
    async with pool.connection() as conn:
        yield conn


class DatabasePool:
    """
    Context manager for pool lifecycle.

    Usage:
        async with DatabasePool() as pool:
            async with pool.connection() as conn:
                ...
    """

    def __init__(
        self,
        min_size: int = 2,
        max_size: int = 10,
        connection_string: Optional[str] = None,
    ):
        self.min_size = min_size
        self.max_size = max_size
        self.connection_string = connection_string
        self._pool: Optional[AsyncConnectionPool] = None

    async def __aenter__(self) -> AsyncConnectionPool:
        self._pool = await init_pool(
            min_size=self.min_size,
            max_size=self.max_size,
            connection_string=self.connection_string,
        )
        return self._pool

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await close_pool()


# ============================================================================
# SCHEMA CONSTANTS
# ============================================================================

SCHEMA = "dagapp"

# Table identifiers — use with psycopg sql.SQL().format() for injection-safe queries
from psycopg import sql as psycopg_sql

TABLE_JOBS = psycopg_sql.Identifier(SCHEMA, "dag_jobs")
TABLE_NODES = psycopg_sql.Identifier(SCHEMA, "dag_node_states")
TABLE_TASKS = psycopg_sql.Identifier(SCHEMA, "dag_task_results")
TABLE_EVENTS = psycopg_sql.Identifier(SCHEMA, "dag_job_events")
TABLE_WORKFLOWS = psycopg_sql.Identifier(SCHEMA, "dag_workflows")
TABLE_CHECKPOINTS = psycopg_sql.Identifier(SCHEMA, "dag_checkpoints")

# Domain model tables
TABLE_PLATFORMS = psycopg_sql.Identifier(SCHEMA, "platforms")
TABLE_GEOSPATIAL_ASSETS = psycopg_sql.Identifier(SCHEMA, "geospatial_assets")
TABLE_ASSET_VERSIONS = psycopg_sql.Identifier(SCHEMA, "asset_versions")
