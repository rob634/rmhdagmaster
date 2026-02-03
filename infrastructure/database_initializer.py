# ============================================================================
# DATABASE INITIALIZER - INFRASTRUCTURE AS CODE
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Infrastructure - Database initialization orchestrator
# PURPOSE: Bootstrap dagapp schema from Pydantic models using repository pattern
# CREATED: 29 JAN 2026
# UPDATED: 02 FEB 2026 - Refactored to use repository pattern (rmhgeoapi style)
# ============================================================================
"""
DatabaseInitializer - Infrastructure as Code for DAG Orchestrator.

Provides a standardized workflow for initializing the dagapp schema:
1. Schema creation (dagapp)
2. Enum type creation (job_status, node_status, task_status, event_type, event_status)
3. Table creation from Pydantic models (Job, NodeState, TaskResult, JobEvent)
4. Index creation
5. Trigger creation (updated_at)

All operations use the repository pattern for database access:
- Sync operations use PostgreSQLRepository
- Async operations use AsyncPostgreSQLRepository

Pydantic models are the SINGLE SOURCE OF TRUTH for schema.
DDL is generated via PydanticToSQL.generate_all().

Usage:
    # Sync (for scripts/CLI)
    from infrastructure import DatabaseInitializer

    initializer = DatabaseInitializer()
    result = initializer.initialize_all()

    # Async (for API endpoints)
    initializer = DatabaseInitializer()
    result = await initializer.initialize_all_async()

    # Dry run (show SQL without executing)
    result = initializer.initialize_all(dry_run=True)

    # Verify installation
    status = initializer.verify_installation()
"""

import logging
import traceback
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field
from datetime import datetime, timezone

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
    Uses repository pattern for database access.
    """

    SCHEMA_NAME = "dagapp"
    EXPECTED_TABLES = ["dag_jobs", "dag_node_states", "dag_task_results", "dag_job_events", "dag_checkpoints"]

    def __init__(self):
        """
        Initialize the database initializer.

        Uses environment variables for database configuration:
        - POSTGRES_HOST, POSTGRES_DB for connection info
        - USE_MANAGED_IDENTITY for Azure AD auth
        """
        import os

        self.host = os.environ.get("POSTGRES_HOST", "localhost")
        self.database = os.environ.get("POSTGRES_DB", "postgres")

        # Repositories are created lazily
        self._sync_repo = None
        self._async_repo = None

        logger.info(f"DatabaseInitializer created for {self.host}/{self.database}")

    @property
    def sync_repo(self):
        """Get sync PostgreSQL repository (lazy initialization)."""
        if self._sync_repo is None:
            from infrastructure.postgresql import PostgreSQLRepository
            self._sync_repo = PostgreSQLRepository(schema_name=self.SCHEMA_NAME)
        return self._sync_repo

    @property
    def async_repo(self):
        """Get async PostgreSQL repository (lazy initialization)."""
        if self._async_repo is None:
            from infrastructure.postgresql import AsyncPostgreSQLRepository
            self._async_repo = AsyncPostgreSQLRepository(schema_name=self.SCHEMA_NAME)
        return self._async_repo

    # ========================================================================
    # DDL GENERATION (Pydantic is the source of truth)
    # ========================================================================

    def _generate_ddl_statements(self) -> List:
        """
        Generate DDL statements from Pydantic models.

        Returns:
            List of sql.Composed DDL statements
        """
        from core.schema.sql_generator import PydanticToSQL

        generator = PydanticToSQL(schema_name=self.SCHEMA_NAME)
        return generator.generate_all()

    # ========================================================================
    # SYNC OPERATIONS (for scripts/CLI)
    # ========================================================================

    def initialize_all(self, dry_run: bool = False) -> InitializationResult:
        """
        Initialize database with dagapp schema (synchronous).

        Uses PostgreSQLRepository for database operations.

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

            # Step 2: Deploy schema
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

    def _test_connection(self) -> StepResult:
        """Test database connection using repository."""
        step = StepResult(name="test_connection", status="pending")

        logger.info("Step: Testing database connection...")

        try:
            result = self.sync_repo.fetch_one(
                "SELECT version() as version, current_database() as db"
            )

            step.status = "success"
            step.message = f"Connected to {result['db']}"
            step.details = {
                "version": result["version"][:50] + "...",
                "database": result["db"],
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
            statements = self._generate_ddl_statements()
            logger.info(f"   Generated {len(statements)} DDL statements from Pydantic models")

            if dry_run:
                # Log statements without executing
                for i, stmt in enumerate(statements[:10], 1):
                    try:
                        stmt_str = str(stmt)[:80]
                    except Exception:
                        stmt_str = "<complex statement>"
                    logger.info(f"   [{i}] {stmt_str}...")

                if len(statements) > 10:
                    logger.info(f"   ... and {len(statements) - 10} more statements")

                step.status = "success"
                step.message = f"[DRY RUN] Would execute {len(statements)} statements"
                step.details = {"statements_count": len(statements)}
                return step

            # Execute via repository
            exec_result = self.sync_repo.execute_ddl_statements(statements)

            step.status = "success" if exec_result["success"] else "failed"
            step.message = f"Deployed {exec_result['executed']} statements ({exec_result['skipped']} skipped)"
            step.details = {
                "statements_executed": exec_result["executed"],
                "statements_skipped": exec_result["skipped"],
                "schema": self.SCHEMA_NAME,
                "errors": exec_result["errors"] if exec_result["errors"] else None,
            }

            if exec_result["errors"]:
                step.error = "; ".join(exec_result["errors"][:3])

        except Exception as e:
            step.status = "failed"
            step.error = str(e)
            step.message = f"Schema deployment failed: {e}"
            logger.error(f"Schema deployment failed: {e}")
            logger.error(traceback.format_exc())

        logger.info(f"   Result: {step.status} - {step.message}")
        return step

    def _verify_tables(self) -> StepResult:
        """Verify expected tables exist using repository."""
        step = StepResult(name="verify_tables", status="pending")

        logger.info("Step: Verifying tables...")

        try:
            existing = self.sync_repo.get_tables_in_schema(self.SCHEMA_NAME)

            missing = [t for t in self.EXPECTED_TABLES if t not in existing]
            extra = [t for t in existing if t not in self.EXPECTED_TABLES]

            if missing:
                step.status = "failed"
                step.error = f"Missing tables: {missing}"
                step.message = f"Verification failed: {len(missing)} tables missing"
            else:
                step.status = "success"
                step.message = f"All {len(self.EXPECTED_TABLES)} expected tables exist"

            step.details = {
                "expected": self.EXPECTED_TABLES,
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
    # ASYNC OPERATIONS (for API endpoints)
    # ========================================================================

    async def initialize_all_async(self, dry_run: bool = False) -> InitializationResult:
        """
        Initialize database with dagapp schema (asynchronous).

        Uses AsyncPostgreSQLRepository for database operations.
        Suitable for FastAPI endpoint handlers.

        Args:
            dry_run: If True, return DDL preview without executing

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
        logger.info("DAG ORCHESTRATOR - DATABASE INITIALIZATION (ASYNC)")
        logger.info(f"   Target: {self.host}/{self.database}")
        logger.info(f"   Schema: {self.SCHEMA_NAME}")
        logger.info(f"   Mode: {'DRY RUN' if dry_run else 'EXECUTE'}")
        logger.info("=" * 70)

        try:
            # Step 1: Test connection
            step_result = await self._test_connection_async()
            result.steps.append(step_result)
            if step_result.status == "failed":
                result.errors.append(f"Connection failed: {step_result.error}")
                return result

            # Step 2: Deploy schema
            step_result = await self._deploy_schema_async(dry_run=dry_run)
            result.steps.append(step_result)
            if step_result.status == "failed":
                result.errors.append(f"Schema deployment failed: {step_result.error}")

            # Step 3: Verify installation
            if not dry_run:
                step_result = await self._verify_tables_async()
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
        logger.info("=" * 70)

        return result

    async def _test_connection_async(self) -> StepResult:
        """Test database connection using async repository."""
        step = StepResult(name="test_connection", status="pending")

        logger.info("Step: Testing database connection (async)...")

        try:
            async with self.async_repo.get_connection() as conn:
                result = await conn.execute(
                    "SELECT version() as version, current_database() as db"
                )
                row = await result.fetchone()

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

    async def _deploy_schema_async(self, dry_run: bool = False) -> StepResult:
        """Deploy dagapp schema using async repository."""
        step = StepResult(name="deploy_schema", status="pending")

        logger.info(f"Step: Deploying {self.SCHEMA_NAME} schema (async)...")

        try:
            statements = self._generate_ddl_statements()
            logger.info(f"   Generated {len(statements)} DDL statements from Pydantic models")

            if dry_run:
                step.status = "success"
                step.message = f"[DRY RUN] Would execute {len(statements)} statements"
                step.details = {"statements_count": len(statements)}
                return step

            # Execute via async repository
            exec_result = await self.async_repo.execute_ddl_statements(statements)

            step.status = "success" if exec_result["success"] else "failed"
            step.message = f"Deployed {exec_result['executed']} statements ({exec_result['skipped']} skipped)"
            step.details = {
                "statements_executed": exec_result["executed"],
                "statements_skipped": exec_result["skipped"],
                "schema": self.SCHEMA_NAME,
                "errors": exec_result["errors"] if exec_result["errors"] else None,
            }

            if exec_result["errors"]:
                step.error = "; ".join(exec_result["errors"][:3])

        except Exception as e:
            step.status = "failed"
            step.error = str(e)
            step.message = f"Schema deployment failed: {e}"
            logger.error(f"Schema deployment failed: {e}")
            logger.error(traceback.format_exc())

        logger.info(f"   Result: {step.status} - {step.message}")
        return step

    async def _verify_tables_async(self) -> StepResult:
        """Verify expected tables exist using async repository."""
        step = StepResult(name="verify_tables", status="pending")

        logger.info("Step: Verifying tables (async)...")

        try:
            existing = await self.async_repo.get_tables_in_schema(self.SCHEMA_NAME)

            missing = [t for t in self.EXPECTED_TABLES if t not in existing]
            extra = [t for t in existing if t not in self.EXPECTED_TABLES]

            if missing:
                step.status = "failed"
                step.error = f"Missing tables: {missing}"
                step.message = f"Verification failed: {len(missing)} tables missing"
            else:
                step.status = "success"
                step.message = f"All {len(self.EXPECTED_TABLES)} expected tables exist"

            step.details = {
                "expected": self.EXPECTED_TABLES,
                "existing": existing,
                "missing": missing,
                "extra": extra,
            }

        except Exception as e:
            step.status = "failed"
            step.error = str(e)
            step.message = f"Verification failed: {e}"

        logger.info(f"   Result: {step.status} - {step.message}")
        return step

    # ========================================================================
    # CONVENIENCE METHODS
    # ========================================================================

    def verify_installation(self) -> Dict[str, Any]:
        """
        Quick verification of database installation state (sync).

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
            result["schema_exists"] = self.sync_repo.check_schema_exists(self.SCHEMA_NAME)

            if result["schema_exists"]:
                tables = self.sync_repo.get_tables_in_schema(self.SCHEMA_NAME)
                for table in tables:
                    result["tables"][table] = {"exists": True}

        except Exception as e:
            result["error"] = str(e)

        return result

    async def verify_installation_async(self) -> Dict[str, Any]:
        """
        Quick verification of database installation state (async).

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
            result["schema_exists"] = await self.async_repo.check_schema_exists(self.SCHEMA_NAME)

            if result["schema_exists"]:
                result["tables"] = await self.async_repo.get_table_info(self.SCHEMA_NAME)
                result["enum_types"] = await self.async_repo.get_enum_types(self.SCHEMA_NAME)

        except Exception as e:
            result["error"] = str(e)

        return result

    def get_table_counts(self) -> Dict[str, int]:
        """Get row counts for all tables (sync)."""
        counts = {}

        try:
            for table in self.EXPECTED_TABLES:
                if self.sync_repo.check_table_exists(self.SCHEMA_NAME, table):
                    counts[table] = self.sync_repo.get_table_row_count(self.SCHEMA_NAME, table)
                else:
                    counts[table] = -1

        except Exception as e:
            logger.warning(f"Failed to get table counts: {e}")

        return counts

    async def get_table_counts_async(self) -> Dict[str, int]:
        """Get row counts for all tables (async)."""
        counts = {}

        try:
            for table in self.EXPECTED_TABLES:
                counts[table] = await self.async_repo.get_table_row_count(
                    self.SCHEMA_NAME, table
                )

        except Exception as e:
            logger.warning(f"Failed to get table counts: {e}")

        return counts


# ============================================================================
# CONVENIENCE FUNCTION
# ============================================================================

def initialize_database(
    dry_run: bool = False,
) -> InitializationResult:
    """
    Initialize database with dagapp schema.

    Convenience function for deployment scripts.

    Args:
        dry_run: If True, log SQL but don't execute

    Returns:
        InitializationResult with detailed results
    """
    initializer = DatabaseInitializer()
    return initializer.initialize_all(dry_run=dry_run)


async def initialize_database_async(
    dry_run: bool = False,
) -> InitializationResult:
    """
    Initialize database with dagapp schema (async).

    Convenience function for async contexts.

    Args:
        dry_run: If True, log SQL but don't execute

    Returns:
        InitializationResult with detailed results
    """
    initializer = DatabaseInitializer()
    return await initializer.initialize_all_async(dry_run=dry_run)


# ============================================================================
# EXPORTS
# ============================================================================

__all__ = [
    'DatabaseInitializer',
    'InitializationResult',
    'StepResult',
    'initialize_database',
    'initialize_database_async',
]
