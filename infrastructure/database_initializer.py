# ============================================================================
# DATABASE INITIALIZER - INFRASTRUCTURE AS CODE
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Infrastructure - Database initialization orchestrator
# PURPOSE: Bootstrap dagapp schema from Pydantic models
# CREATED: 29 JAN 2026
# ============================================================================
"""
DatabaseInitializer - Infrastructure as Code for DAG Orchestrator.

Provides a standardized workflow for initializing the dagapp schema:
1. Schema creation (dagapp)
2. Enum type creation (job_status, node_status, task_status)
3. Table creation from Pydantic models
4. Index creation
5. Trigger creation (updated_at)

All operations are idempotent (safe to run multiple times).
Uses IF NOT EXISTS and CASCADE patterns.

Usage:
    from infrastructure import DatabaseInitializer

    # Initialize with connection string from environment
    initializer = DatabaseInitializer()
    result = initializer.initialize_all()

    # Dry run (show SQL without executing)
    result = initializer.initialize_all(dry_run=True)

    # Verify installation
    status = initializer.verify_installation()

Environment Variables:
    DATABASE_URL: PostgreSQL connection string
    POSTGRES_HOST, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD: Individual parts
"""

import os
import logging
import traceback
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field
from datetime import datetime, timezone

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

logger = logging.getLogger(__name__)


# ============================================================================
# DATA CLASSES
# ============================================================================

@dataclass
class StepResult:
    """Result of a single initialization step."""
    name: str
    status: str  # 'success', 'failed', 'skipped'
    message: str = ""
    error: Optional[str] = None
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class InitializationResult:
    """Complete result of database initialization."""
    database_host: str
    database_name: str
    timestamp: str
    success: bool
    steps: List[StepResult] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "database_host": self.database_host,
            "database_name": self.database_name,
            "timestamp": self.timestamp,
            "success": self.success,
            "steps": [
                {
                    "name": s.name,
                    "status": s.status,
                    "message": s.message,
                    "error": s.error,
                    "details": s.details
                }
                for s in self.steps
            ],
            "errors": self.errors,
            "warnings": self.warnings,
            "summary": {
                "total_steps": len(self.steps),
                "successful": len([s for s in self.steps if s.status == "success"]),
                "failed": len([s for s in self.steps if s.status == "failed"]),
                "skipped": len([s for s in self.steps if s.status == "skipped"])
            }
        }


# ============================================================================
# DATABASE INITIALIZER
# ============================================================================

class DatabaseInitializer:
    """
    Database initialization orchestrator for DAG Orchestrator.

    Provides Infrastructure as Code workflow for dagapp schema setup:
    1. Schema creation
    2. Enum type creation
    3. Table creation from Pydantic models
    4. Index creation
    5. Trigger creation

    All operations are idempotent (safe to run multiple times).
    """

    SCHEMA_NAME = "dagapp"

    def __init__(
        self,
        connection_string: Optional[str] = None,
        host: Optional[str] = None,
        database: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        port: int = 5432,
        sslmode: str = "require",
    ):
        """
        Initialize the database initializer.

        Args:
            connection_string: Full PostgreSQL connection string (overrides other params)
            host: Database host
            database: Database name
            user: Database user
            password: Database password
            port: Database port (default 5432)
            sslmode: SSL mode (default 'require' for Azure)

        If no parameters provided, reads from environment:
            DATABASE_URL or POSTGRES_* variables
        """
        self.connection_string = connection_string or self._build_connection_string(
            host, database, user, password, port, sslmode
        )

        # Parse host/database for logging
        self.host = host or os.environ.get("POSTGRES_HOST", "localhost")
        self.database = database or os.environ.get("POSTGRES_DB", "postgres")

        logger.info(f"DatabaseInitializer created for {self.host}/{self.database}")

    def _build_connection_string(
        self,
        host: Optional[str],
        database: Optional[str],
        user: Optional[str],
        password: Optional[str],
        port: int,
        sslmode: str,
    ) -> str:
        """Build connection string from parameters or environment."""
        # Check for DATABASE_URL first
        database_url = os.environ.get("DATABASE_URL")
        if database_url:
            return database_url

        # Build from individual parameters or environment
        h = host or os.environ.get("POSTGRES_HOST", "localhost")
        d = database or os.environ.get("POSTGRES_DB", "postgres")
        u = user or os.environ.get("POSTGRES_USER", "postgres")
        p = password or os.environ.get("POSTGRES_PASSWORD", "")
        pt = port or int(os.environ.get("POSTGRES_PORT", "5432"))
        ssl = sslmode or os.environ.get("POSTGRES_SSLMODE", "require")

        return f"postgresql://{u}:{p}@{h}:{pt}/{d}?sslmode={ssl}"

    def _get_connection(self):
        """Get a database connection."""
        return psycopg.connect(self.connection_string, row_factory=dict_row)

    # ========================================================================
    # MAIN ORCHESTRATION
    # ========================================================================

    def initialize_all(self, dry_run: bool = False) -> InitializationResult:
        """
        Initialize database with dagapp schema.

        Args:
            dry_run: If True, log SQL but don't execute

        Returns:
            InitializationResult with detailed step results
        """
        result = InitializationResult(
            database_host=self.host,
            database_name=self.database,
            timestamp=datetime.now(timezone.utc).isoformat(),
            success=False
        )

        logger.info("=" * 70)
        logger.info("DAG ORCHESTRATOR - DATABASE INITIALIZATION")
        logger.info(f"   Target: {self.host}/{self.database}")
        logger.info(f"   Schema: {self.SCHEMA_NAME}")
        logger.info(f"   Mode: {'DRY RUN' if dry_run else 'EXECUTE'}")
        logger.info("=" * 70)

        try:
            # Step 1: Test connection
            step_result = self._test_connection()
            result.steps.append(step_result)
            if step_result.status == "failed":
                result.errors.append(f"Connection failed: {step_result.error}")
                return result

            # Step 2: Create schema and deploy DDL
            step_result = self._deploy_schema(dry_run=dry_run)
            result.steps.append(step_result)
            if step_result.status == "failed":
                result.errors.append(f"Schema deployment failed: {step_result.error}")

            # Step 3: Verify installation
            if not dry_run:
                step_result = self._verify_tables()
                result.steps.append(step_result)
                if step_result.status == "failed":
                    result.warnings.append(f"Verification issue: {step_result.error}")

            # Determine overall success
            critical_failures = [
                s for s in result.steps
                if s.status == "failed" and s.name != "verify_tables"
            ]
            result.success = len(critical_failures) == 0

        except Exception as e:
            logger.error(f"Initialization failed: {e}")
            logger.error(traceback.format_exc())
            result.errors.append(str(e))
            result.success = False

        # Log summary
        summary = result.to_dict()["summary"]
        logger.info("=" * 70)
        logger.info(f"INITIALIZATION {'COMPLETE' if result.success else 'FAILED'}")
        logger.info(f"   Steps: {summary['successful']} succeeded, {summary['failed']} failed")
        if result.errors:
            logger.warning(f"   Errors: {result.errors}")
        logger.info("=" * 70)

        return result

    # ========================================================================
    # INDIVIDUAL STEPS
    # ========================================================================

    def _test_connection(self) -> StepResult:
        """Test database connection."""
        step = StepResult(name="test_connection", status="pending")

        logger.info("Step: Testing database connection...")

        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT version() as version, current_database() as db")
                    row = cur.fetchone()

                    step.status = "success"
                    step.message = f"Connected to {row['db']}"
                    step.details = {
                        "version": row["version"][:50] + "...",
                        "database": row["db"],
                    }

        except Exception as e:
            step.status = "failed"
            step.error = str(e)
            step.message = f"Connection failed: {e}"
            logger.error(f"Connection test failed: {e}")

        logger.info(f"   Result: {step.status} - {step.message}")
        return step

    def _deploy_schema(self, dry_run: bool = False) -> StepResult:
        """Deploy dagapp schema using PydanticToSQL generator."""
        step = StepResult(name="deploy_schema", status="pending")

        logger.info(f"Step: Deploying {self.SCHEMA_NAME} schema...")

        try:
            from core.schema.sql_generator import PydanticToSQL

            generator = PydanticToSQL(schema_name=self.SCHEMA_NAME)
            statements = generator.generate_all()

            logger.info(f"   Generated {len(statements)} DDL statements")

            if dry_run:
                # Log statements without executing
                for i, stmt in enumerate(statements, 1):
                    # Try to get string representation
                    try:
                        stmt_str = stmt.as_string(None)[:80]
                    except Exception:
                        stmt_str = str(stmt)[:80]
                    logger.info(f"   [{i}] {stmt_str}...")

                step.status = "success"
                step.message = f"[DRY RUN] Would execute {len(statements)} statements"
                step.details = {"statements_count": len(statements)}
                return step

            # Execute statements
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    executed = 0
                    skipped = 0

                    for stmt in statements:
                        try:
                            cur.execute(stmt)
                            executed += 1
                        except Exception as e:
                            # Log but continue (IF NOT EXISTS should handle most)
                            logger.debug(f"Statement skipped: {e}")
                            skipped += 1

                conn.commit()

                step.status = "success"
                step.message = f"Deployed {executed} statements ({skipped} skipped)"
                step.details = {
                    "statements_executed": executed,
                    "statements_skipped": skipped,
                    "schema": self.SCHEMA_NAME,
                }

        except Exception as e:
            step.status = "failed"
            step.error = str(e)
            step.message = f"Schema deployment failed: {e}"
            logger.error(f"Schema deployment failed: {e}")
            logger.error(traceback.format_exc())

        logger.info(f"   Result: {step.status} - {step.message}")
        return step

    def _verify_tables(self) -> StepResult:
        """Verify expected tables exist."""
        step = StepResult(name="verify_tables", status="pending")

        logger.info("Step: Verifying tables...")

        expected_tables = ["dag_jobs", "dag_node_states", "dag_task_results", "dag_job_events"]

        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT table_name
                        FROM information_schema.tables
                        WHERE table_schema = %s
                        ORDER BY table_name
                    """, [self.SCHEMA_NAME])

                    existing = [row["table_name"] for row in cur.fetchall()]

                    missing = [t for t in expected_tables if t not in existing]
                    extra = [t for t in existing if t not in expected_tables]

                    if missing:
                        step.status = "failed"
                        step.error = f"Missing tables: {missing}"
                        step.message = f"Verification failed: {len(missing)} tables missing"
                    else:
                        step.status = "success"
                        step.message = f"All {len(expected_tables)} expected tables exist"

                    step.details = {
                        "expected": expected_tables,
                        "existing": existing,
                        "missing": missing,
                        "extra": extra,
                    }

        except Exception as e:
            step.status = "failed"
            step.error = str(e)
            step.message = f"Verification failed: {e}"

        logger.info(f"   Result: {step.status} - {step.message}")
        if step.details.get("existing"):
            for table in step.details["existing"]:
                logger.info(f"      - {self.SCHEMA_NAME}.{table}")

        return step

    # ========================================================================
    # CONVENIENCE METHODS
    # ========================================================================

    def verify_installation(self) -> Dict[str, Any]:
        """
        Quick verification of database installation state.

        Returns:
            Dict with schema existence and table info
        """
        result = {
            "database": f"{self.host}/{self.database}",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "schema": self.SCHEMA_NAME,
            "tables": {},
        }

        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    # Check schema exists
                    cur.execute("""
                        SELECT EXISTS(
                            SELECT 1 FROM pg_namespace WHERE nspname = %s
                        ) as exists
                    """, [self.SCHEMA_NAME])
                    result["schema_exists"] = cur.fetchone()["exists"]

                    if result["schema_exists"]:
                        # Get table info
                        cur.execute("""
                            SELECT
                                t.table_name,
                                (SELECT COUNT(*) FROM information_schema.columns c
                                 WHERE c.table_schema = t.table_schema
                                 AND c.table_name = t.table_name) as column_count
                            FROM information_schema.tables t
                            WHERE t.table_schema = %s
                            ORDER BY t.table_name
                        """, [self.SCHEMA_NAME])

                        for row in cur.fetchall():
                            result["tables"][row["table_name"]] = {
                                "columns": row["column_count"]
                            }

                        # Get enum types
                        cur.execute("""
                            SELECT typname
                            FROM pg_type t
                            JOIN pg_namespace n ON t.typnamespace = n.oid
                            WHERE n.nspname = %s
                            AND t.typtype = 'e'
                            ORDER BY typname
                        """, [self.SCHEMA_NAME])
                        result["enum_types"] = [row["typname"] for row in cur.fetchall()]

        except Exception as e:
            result["error"] = str(e)

        return result

    def get_table_counts(self) -> Dict[str, int]:
        """Get row counts for all tables."""
        counts = {}

        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    for table in ["dag_jobs", "dag_node_states", "dag_task_results", "dag_job_events"]:
                        try:
                            cur.execute(
                                sql.SQL("SELECT COUNT(*) as count FROM {}.{}").format(
                                    sql.Identifier(self.SCHEMA_NAME),
                                    sql.Identifier(table)
                                )
                            )
                            counts[table] = cur.fetchone()["count"]
                        except Exception:
                            counts[table] = -1  # Table doesn't exist

        except Exception as e:
            logger.warning(f"Failed to get table counts: {e}")

        return counts


# ============================================================================
# CONVENIENCE FUNCTION
# ============================================================================

def initialize_database(
    connection_string: Optional[str] = None,
    dry_run: bool = False,
) -> InitializationResult:
    """
    Initialize database with dagapp schema.

    Convenience function for deployment scripts.

    Args:
        connection_string: PostgreSQL connection string (optional, reads from env)
        dry_run: If True, log SQL but don't execute

    Returns:
        InitializationResult with detailed results
    """
    initializer = DatabaseInitializer(connection_string=connection_string)
    return initializer.initialize_all(dry_run=dry_run)


# ============================================================================
# EXPORTS
# ============================================================================

__all__ = [
    'DatabaseInitializer',
    'InitializationResult',
    'StepResult',
    'initialize_database',
]
