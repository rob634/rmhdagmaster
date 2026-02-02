# ============================================================================
# BOOTSTRAP API ROUTES
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Infrastructure - Database bootstrap endpoints
# PURPOSE: HTTP endpoints for schema deployment and verification
# CREATED: 29 JAN 2026
# ============================================================================
"""
Bootstrap API Routes

Provides HTTP endpoints for database schema management:
- GET /api/v1/bootstrap/status - Check schema status
- POST /api/v1/bootstrap/deploy - Deploy schema (requires confirmation)
- GET /api/v1/bootstrap/tables - List tables and row counts

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

    Returns schema existence, tables, and enum types.
    Uses async pool with Azure AD authentication.
    """
    try:
        from datetime import datetime, timezone
        from repositories.database import get_pool
        from psycopg.rows import dict_row

        pool = await get_pool()
        result = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "schema": "dagapp",
            "tables": {},
        }

        async with pool.connection() as conn:
            conn.row_factory = dict_row

            # Check schema exists
            row = await conn.execute("""
                SELECT EXISTS(
                    SELECT 1 FROM pg_namespace WHERE nspname = 'dagapp'
                ) as exists
            """)
            schema_row = await row.fetchone()
            result["schema_exists"] = schema_row["exists"]

            if result["schema_exists"]:
                # Get table info
                rows = await conn.execute("""
                    SELECT
                        t.table_name,
                        (SELECT COUNT(*) FROM information_schema.columns c
                         WHERE c.table_schema = t.table_schema
                         AND c.table_name = t.table_name) as column_count
                    FROM information_schema.tables t
                    WHERE t.table_schema = 'dagapp'
                    ORDER BY t.table_name
                """)
                for row in await rows.fetchall():
                    result["tables"][row["table_name"]] = {
                        "columns": row["column_count"]
                    }

                # Get enum types
                rows = await conn.execute("""
                    SELECT typname
                    FROM pg_type t
                    JOIN pg_namespace n ON t.typnamespace = n.oid
                    WHERE n.nspname = 'dagapp'
                    AND t.typtype = 'e'
                    ORDER BY typname
                """)
                result["enum_types"] = [row["typname"] for row in await rows.fetchall()]

        return JSONResponse(content={
            "status": "ok" if result.get("schema_exists") else "not_initialized",
            **result
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
    Deploy dagapp schema to database using PydanticToSQL generator.

    Generates DDL from Pydantic models (single source of truth) and executes
    via the async connection pool with proper Azure AD authentication.

    Operations are idempotent (safe to run multiple times).

    Args:
        confirm: Must be 'yes' to actually execute (safety check)
        dry_run: If True, preview SQL statements without executing

    Returns:
        Deployment result with executed statements
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
        from core.schema.sql_generator import PydanticToSQL
        from repositories.database import get_pool

        generator = PydanticToSQL(schema_name="dagapp")
        statements = generator.generate_all()

        logger.info(f"Generated {len(statements)} DDL statements from Pydantic models")

        if dry_run:
            # Return preview of statements
            ddl_preview = []
            for stmt in statements:
                try:
                    ddl_preview.append(stmt.as_string(None))
                except Exception:
                    ddl_preview.append(str(stmt))

            return JSONResponse(content={
                "status": "dry_run",
                "message": f"Would execute {len(statements)} DDL statements",
                "statement_count": len(statements),
                "statements": ddl_preview,
            })

        # Execute via async pool
        pool = await get_pool()
        executed = 0
        skipped = 0
        errors = []

        async with pool.connection() as conn:
            for stmt in statements:
                try:
                    await conn.execute(stmt)
                    executed += 1
                except Exception as e:
                    # Log but continue - IF NOT EXISTS handles most cases
                    error_msg = str(e)
                    if "already exists" in error_msg.lower():
                        skipped += 1
                    else:
                        errors.append(error_msg)
                        logger.warning(f"DDL statement skipped: {e}")

        success = len(errors) == 0
        return JSONResponse(
            status_code=200 if success else 207,  # 207 = partial success
            content={
                "status": "success" if success else "partial",
                "message": f"Executed {executed} statements, {skipped} skipped",
                "executed": executed,
                "skipped": skipped,
                "errors": errors if errors else None,
            }
        )

    except Exception as e:
        logger.error(f"Schema deployment failed: {e}")
        raise HTTPException(500, f"Deployment failed: {e}")


@router.get("/tables")
async def get_table_counts():
    """
    Get row counts for all dagapp tables.

    Uses async pool with Azure AD authentication.
    Useful for monitoring and debugging.
    """
    try:
        from repositories.database import get_pool
        from psycopg.rows import dict_row
        from psycopg import sql

        pool = await get_pool()
        counts = {}
        tables = ["dag_jobs", "dag_node_states", "dag_task_results", "dag_job_events"]

        async with pool.connection() as conn:
            conn.row_factory = dict_row
            for table in tables:
                try:
                    result = await conn.execute(
                        sql.SQL("SELECT COUNT(*) as count FROM dagapp.{}").format(
                            sql.Identifier(table)
                        )
                    )
                    row = await result.fetchone()
                    counts[table] = row["count"]
                except Exception:
                    counts[table] = -1  # Table doesn't exist

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

    Returns the SQL statements generated from Pydantic models.
    Useful for review before deployment.
    """
    try:
        from core.schema.sql_generator import PydanticToSQL

        generator = PydanticToSQL(schema_name="dagapp")
        statements = generator.generate_all()

        # Convert to string representations
        ddl_strings = []
        for stmt in statements:
            try:
                # Try to get string representation
                ddl_strings.append(stmt.as_string(None))
            except Exception:
                ddl_strings.append(str(stmt))

        return JSONResponse(content={
            "schema": "dagapp",
            "statement_count": len(statements),
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
        from repositories.database import get_pool

        pool = await get_pool()

        migrations_applied = []
        migrations_failed = []

        async with pool.connection() as conn:
            # Define missing column migrations
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
