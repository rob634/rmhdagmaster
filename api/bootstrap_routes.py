# ============================================================================
# BOOTSTRAP API ROUTES
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Infrastructure - Database bootstrap endpoints
# PURPOSE: HTTP endpoints for schema deployment and verification
# CREATED: 29 JAN 2026
# UPDATED: 02 FEB 2026 - Refactored to use DatabaseInitializer (repository pattern)
# ============================================================================
"""
Bootstrap API Routes

Provides HTTP endpoints for database schema management:
- GET /api/v1/bootstrap/status - Check schema status
- POST /api/v1/bootstrap/deploy - Deploy schema (requires confirmation)
- GET /api/v1/bootstrap/tables - List tables and row counts
- GET /api/v1/bootstrap/ddl - Preview DDL statements

All operations delegate to DatabaseInitializer which uses:
- AsyncPostgreSQLRepository for database access
- PydanticToSQL for DDL generation (Pydantic models as single source of truth)

These endpoints are intended for operational use, not regular API consumers.
Consider restricting access in production.
"""

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/bootstrap", tags=["Bootstrap"])


@router.get("/status")
async def get_bootstrap_status():
    """
    Check database schema status.

    Uses DatabaseInitializer.verify_installation_async() which
    delegates to AsyncPostgreSQLRepository for database access.

    Returns:
        Schema existence, tables, and enum types
    """
    try:
        from infrastructure import DatabaseInitializer

        initializer = DatabaseInitializer()
        status = await initializer.verify_installation_async()

        return JSONResponse(content={
            "status": "ok" if status.get("schema_exists") else "not_initialized",
            **status
        })

    except Exception as e:
        logger.error(f"Bootstrap status check failed: {e}")
        raise HTTPException(500, f"Status check failed: {e}")


@router.post("/deploy")
async def deploy_schema(
    confirm: str = Query(None, description="Must be 'yes' to execute"),
    dry_run: bool = Query(False, description="Preview SQL without executing"),
):
    """
    Deploy dagapp schema to database.

    Uses DatabaseInitializer.initialize_all_async() which:
    - Generates DDL from Pydantic models via PydanticToSQL
    - Executes via AsyncPostgreSQLRepository

    Operations are idempotent (safe to run multiple times).

    Args:
        confirm: Must be 'yes' to actually execute (safety check)
        dry_run: If True, preview SQL statements without executing

    Returns:
        Deployment result with step details
    """
    if not dry_run and confirm != "yes":
        return JSONResponse(
            status_code=400,
            content={
                "error": "Confirmation required",
                "message": "Add ?confirm=yes to execute, or ?dry_run=true to preview",
                "example": "POST /api/v1/bootstrap/deploy?confirm=yes"
            }
        )

    try:
        from infrastructure import DatabaseInitializer

        initializer = DatabaseInitializer()
        result = await initializer.initialize_all_async(dry_run=dry_run)

        return JSONResponse(
            status_code=200 if result.success else 500,
            content=result.to_dict()
        )

    except Exception as e:
        logger.error(f"Schema deployment failed: {e}")
        raise HTTPException(500, f"Deployment failed: {e}")


@router.get("/tables")
async def get_table_counts():
    """
    Get row counts for all dagapp tables.

    Uses DatabaseInitializer.get_table_counts_async() which
    delegates to AsyncPostgreSQLRepository.

    Useful for monitoring and debugging.
    """
    try:
        from infrastructure import DatabaseInitializer

        initializer = DatabaseInitializer()
        counts = await initializer.get_table_counts_async()

        return JSONResponse(content={
            "schema": "dagapp",
            "tables": counts,
            "total_rows": sum(c for c in counts.values() if c >= 0),
        })

    except Exception as e:
        logger.error(f"Failed to get table counts: {e}")
        raise HTTPException(500, f"Failed to get table counts: {e}")


@router.get("/ddl")
async def get_ddl_preview():
    """
    Preview DDL statements that would be executed.

    Returns the SQL statements generated from Pydantic models
    via PydanticToSQL.generate_all().

    Pydantic models are the single source of truth for schema.
    """
    try:
        from core.schema.sql_generator import PydanticToSQL

        generator = PydanticToSQL(schema_name="dagapp")
        statements = generator.generate_all()

        # Convert to string representations
        ddl_strings = []
        for stmt in statements:
            try:
                ddl_strings.append(stmt.as_string(None))
            except Exception:
                ddl_strings.append(str(stmt))

        return JSONResponse(content={
            "schema": "dagapp",
            "statement_count": len(statements),
            "source": "PydanticToSQL.generate_all() - Pydantic models are the source of truth",
            "statements": ddl_strings,
        })

    except Exception as e:
        logger.error(f"Failed to generate DDL preview: {e}")
        raise HTTPException(500, f"Failed to generate DDL: {e}")


@router.post("/migrate")
async def run_migrations(
    confirm: str = Query(None, description="Must be 'yes' to execute"),
):
    """
    Add missing columns to existing tables.

    This endpoint checks for columns that exist in Pydantic models but are
    missing from database tables, and adds them with appropriate defaults.

    IMPORTANT: This does not drop columns - only adds missing ones.

    For full schema deployment (including new tables), use POST /deploy.
    """
    if confirm != "yes":
        return JSONResponse(
            status_code=400,
            content={
                "error": "Confirmation required",
                "message": "Add ?confirm=yes to execute migrations",
                "example": "POST /api/v1/bootstrap/migrate?confirm=yes"
            }
        )

    try:
        from infrastructure.postgresql import AsyncPostgreSQLRepository

        repo = AsyncPostgreSQLRepository(schema_name="dagapp")

        migrations_applied = []
        migrations_failed = []

        async with repo.get_connection() as conn:
            # Define missing column migrations
            # These are columns that may have been added to models after initial deployment
            migrations = [
                # dag_task_results.processed
                {
                    "table": "dag_task_results",
                    "column": "processed",
                    "sql": """
                        ALTER TABLE dagapp.dag_task_results
                        ADD COLUMN IF NOT EXISTS processed BOOLEAN DEFAULT false NOT NULL
                    """,
                },
                # dag_jobs.updated_at
                {
                    "table": "dag_jobs",
                    "column": "updated_at",
                    "sql": """
                        ALTER TABLE dagapp.dag_jobs
                        ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW() NOT NULL
                    """,
                },
                # dag_jobs.metadata
                {
                    "table": "dag_jobs",
                    "column": "metadata",
                    "sql": """
                        ALTER TABLE dagapp.dag_jobs
                        ADD COLUMN IF NOT EXISTS metadata JSONB DEFAULT '{}'
                    """,
                },
                # dag_node_states.updated_at
                {
                    "table": "dag_node_states",
                    "column": "updated_at",
                    "sql": """
                        ALTER TABLE dagapp.dag_node_states
                        ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW() NOT NULL
                    """,
                },
                # dag_node_states.dispatched_at
                {
                    "table": "dag_node_states",
                    "column": "dispatched_at",
                    "sql": """
                        ALTER TABLE dagapp.dag_node_states
                        ADD COLUMN IF NOT EXISTS dispatched_at TIMESTAMPTZ
                    """,
                },
                # dag_node_states.max_retries
                {
                    "table": "dag_node_states",
                    "column": "max_retries",
                    "sql": """
                        ALTER TABLE dagapp.dag_node_states
                        ADD COLUMN IF NOT EXISTS max_retries INTEGER DEFAULT 3
                    """,
                },
                # dag_node_states.parent_node_id
                {
                    "table": "dag_node_states",
                    "column": "parent_node_id",
                    "sql": """
                        ALTER TABLE dagapp.dag_node_states
                        ADD COLUMN IF NOT EXISTS parent_node_id VARCHAR(64)
                    """,
                },
                # dag_node_states.fan_out_index
                {
                    "table": "dag_node_states",
                    "column": "fan_out_index",
                    "sql": """
                        ALTER TABLE dagapp.dag_node_states
                        ADD COLUMN IF NOT EXISTS fan_out_index INTEGER
                    """,
                },
                # Create index for unprocessed task results
                {
                    "table": "dag_task_results",
                    "column": "idx_unprocessed",
                    "sql": """
                        CREATE INDEX IF NOT EXISTS idx_dag_task_results_unprocessed
                        ON dagapp.dag_task_results (processed, reported_at)
                        WHERE processed = false
                    """,
                },
            ]

            for migration in migrations:
                try:
                    await conn.execute(migration["sql"])
                    migrations_applied.append({
                        "table": migration["table"],
                        "column": migration["column"],
                        "status": "applied"
                    })
                    logger.info(f"Migration applied: {migration['table']}.{migration['column']}")
                except Exception as e:
                    migrations_failed.append({
                        "table": migration["table"],
                        "column": migration["column"],
                        "error": str(e)
                    })
                    logger.error(f"Migration failed: {migration['table']}.{migration['column']}: {e}")

        return JSONResponse(content={
            "status": "completed",
            "applied": len(migrations_applied),
            "failed": len(migrations_failed),
            "migrations_applied": migrations_applied,
            "migrations_failed": migrations_failed,
        })

    except Exception as e:
        logger.error(f"Migration failed: {e}")
        raise HTTPException(500, f"Migration failed: {e}")
