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


# ============================================================================
# EXPORTS
# ============================================================================

__all__ = [
    "PostgresCheck",
    "PgStacCheck",
    "DagAppCheck",
]
