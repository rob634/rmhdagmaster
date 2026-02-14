# ============================================================================
# DATABASE HEALTH CHECKS
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Infrastructure - PostgreSQL and schema checks
# PURPOSE: Database connectivity and schema availability
# CREATED: 31 JAN 2026
# ============================================================================
"""
Database Health Checks

PostgreSQL connectivity and schema checks (priority 30):
- PostgresCheck: Basic PostgreSQL connectivity
- PgStacCheck: pgSTAC schema and functions available
- DagAppCheck: DAG app schema (dagapp) available
"""

import os
import logging

from health.core import (
    HealthCheckPlugin,
    HealthCheckResult,
    HealthCheckCategory,
)
from health.registry import register_check

logger = logging.getLogger(__name__)


@register_check(category="database")
class PostgresCheck(HealthCheckPlugin):
    """
    PostgreSQL connectivity health check.

    Tests basic database connection.
    """

    name = "postgres"
    timeout_seconds = 5.0

    async def check(self) -> HealthCheckResult:
        host = os.environ.get("POSTGRES_HOST")
        database = os.environ.get("POSTGRES_DB")

        if not host or not database:
            return HealthCheckResult.unhealthy(
                message="PostgreSQL not configured",
                hint="Set POSTGRES_HOST and POSTGRES_DB",
            )

        try:
            from infrastructure.postgresql import PostgreSQLRepository

            repo = PostgreSQLRepository()

            # Test connection with simple query
            result = repo.fetch_one("SELECT 1 as health_check")

            if result and result.get("health_check") == 1:
                return HealthCheckResult.healthy(
                    message="PostgreSQL connected",
                    host=host,
                    database=database,
                )
            else:
                return HealthCheckResult.unhealthy(
                    message="PostgreSQL query returned unexpected result",
                )

        except ImportError:
            return HealthCheckResult.unhealthy(
                message="psycopg not installed",
            )
        except Exception as e:
            return HealthCheckResult.unhealthy(
                message=f"PostgreSQL connection failed: {str(e)}",
                host=host,
                database=database,
            )


@register_check(category="database")
class PgStacCheck(HealthCheckPlugin):
    """
    pgSTAC schema health check.

    Verifies pgSTAC schema exists and key tables/functions available.
    """

    name = "pgstac"
    timeout_seconds = 5.0

    async def check(self) -> HealthCheckResult:
        try:
            from infrastructure.pgstac import PgStacRepository

            repo = PgStacRepository()

            # Check schema availability
            if repo.is_available():
                # Get collection count as additional verification
                collections = repo.list_collections(limit=1)

                return HealthCheckResult.healthy(
                    message="pgSTAC schema available",
                    has_collections=len(collections) > 0,
                )
            else:
                return HealthCheckResult.unhealthy(
                    message="pgSTAC schema not responding",
                )

        except ImportError:
            return HealthCheckResult.unhealthy(
                message="pgstac repository not available",
            )
        except Exception as e:
            # pgSTAC not being available is degraded, not unhealthy
            # (orchestrator can still run for non-STAC workflows)
            return HealthCheckResult.degraded(
                message=f"pgSTAC check failed: {str(e)}",
            )


@register_check(category="database")
class DagAppCheck(HealthCheckPlugin):
    """
    DAG App schema health check.

    Verifies dagapp schema exists with required tables:
    - dag_jobs
    - dag_node_states
    - dag_task_results
    """

    name = "dagapp"
    timeout_seconds = 5.0

    REQUIRED_TABLES = [
        "dag_jobs",
        "dag_node_states",
        "dag_task_results",
    ]

    async def check(self) -> HealthCheckResult:
        try:
            from infrastructure.postgresql import PostgreSQLRepository

            repo = PostgreSQLRepository(schema_name="dagapp")

            # Check each required table exists
            missing_tables = []
            existing_tables = []

            for table in self.REQUIRED_TABLES:
                result = repo.fetch_one(
                    """
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_schema = 'dagapp'
                        AND table_name = %s
                    ) as exists
                    """,
                    (table,)
                )

                if result and result.get("exists"):
                    existing_tables.append(table)
                else:
                    missing_tables.append(table)

            if missing_tables:
                return HealthCheckResult.unhealthy(
                    message=f"Missing tables: {', '.join(missing_tables)}",
                    missing_tables=missing_tables,
                    existing_tables=existing_tables,
                    hint="Run database initialization to create schema",
                )

            return HealthCheckResult.healthy(
                message="DAG app schema available",
                tables=existing_tables,
            )

        except Exception as e:
            return HealthCheckResult.unhealthy(
                message=f"DAG app schema check failed: {str(e)}",
            )


@register_check(category="database")
class ConnectionPoolCheck(HealthCheckPlugin):
    """
    Connection pool health check.

    Exposes psycopg_pool stats for observability:
    - pool_size: Current number of connections in the pool
    - pool_available: Connections idle and ready to use
    - pool_min/pool_max: Configured bounds
    - requests_waiting: Requests currently blocked waiting for a connection
    - requests_num: Total requests served (lifetime)
    - requests_errors: Requests that failed to get a connection (lifetime)
    - requests_queued: Requests that had to wait (lifetime)
    - connections_num: Total connections opened (lifetime)
    - connections_lost: Connections lost unexpectedly (lifetime)
    - usage_ms: Total connection usage time in ms (lifetime)
    - requests_wait_ms: Total time spent waiting for connections in ms (lifetime)

    Status logic:
    - UNHEALTHY: Pool not initialized
    - DEGRADED: requests_waiting > 0 or pool_available == 0
    - HEALTHY: Connections available, nothing waiting
    """

    name = "connection_pool"
    timeout_seconds = 2.0
    required_for_ready = True

    async def check(self) -> HealthCheckResult:
        try:
            from repositories.database import _pool

            if _pool is None:
                return HealthCheckResult.unhealthy(
                    message="Connection pool not initialized",
                )

            stats = _pool.get_stats()

            # Key operational metrics
            pool_size = stats.get("pool_size", 0)
            pool_available = stats.get("pool_available", 0)
            pool_min = stats.get("pool_min", 0)
            pool_max = stats.get("pool_max", 0)
            requests_waiting = stats.get("requests_waiting", 0)

            # Build details dict with all stats
            details = {
                "pool_size": pool_size,
                "pool_available": pool_available,
                "pool_min": pool_min,
                "pool_max": pool_max,
                "requests_waiting": requests_waiting,
                "requests_num": stats.get("requests_num", 0),
                "requests_queued": stats.get("requests_queued", 0),
                "requests_errors": stats.get("requests_errors", 0),
                "requests_wait_ms": stats.get("requests_wait_ms", 0),
                "connections_num": stats.get("connections_num", 0),
                "connections_lost": stats.get("connections_lost", 0),
                "connections_errors": stats.get("connections_errors", 0),
                "connections_ms": stats.get("connections_ms", 0),
                "returns_bad": stats.get("returns_bad", 0),
                "usage_ms": stats.get("usage_ms", 0),
            }

            if requests_waiting > 0:
                return HealthCheckResult.degraded(
                    message=f"Pool saturated: {requests_waiting} requests waiting "
                            f"({pool_available}/{pool_size} available)",
                    **details,
                )

            if pool_available == 0 and pool_size >= pool_max:
                return HealthCheckResult.degraded(
                    message=f"Pool fully utilized: 0/{pool_size} available "
                            f"(max {pool_max})",
                    **details,
                )

            return HealthCheckResult.healthy(
                message=f"Pool OK: {pool_available}/{pool_size} available "
                        f"(max {pool_max})",
                **details,
            )

        except Exception as e:
            return HealthCheckResult.from_exception(e)


# ============================================================================
# EXPORTS
# ============================================================================

__all__ = [
    "PostgresCheck",
    "PgStacCheck",
    "DagAppCheck",
    "ConnectionPoolCheck",
]
