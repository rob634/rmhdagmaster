# ============================================================================
# BOOTSTRAP API ROUTES
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Infrastructure - Database bootstrap endpoints
# PURPOSE: HTTP endpoints for schema deployment and verification
# CREATED: 29 JAN 2026
# UPDATED: 02 FEB 2026 - Added rebuild endpoint, made deploy safe (non-destructive enums)
# ============================================================================
"""
Bootstrap API Routes

Provides HTTP endpoints for database schema management.

ENDPOINT SUMMARY:
-----------------
| Endpoint              | Behavior                    | Destructive? |
|-----------------------|-----------------------------|--------------|
| GET  /status          | Check schema status         | No           |
| GET  /tables          | List tables and row counts  | No           |
| GET  /ddl             | Preview DDL statements      | No           |
| POST /deploy          | Create schema (idempotent)  | No (safe)    |
| POST /migrate         | Add missing columns         | No (safe)    |
| POST /rebuild         | DROP CASCADE + recreate     | YES! ⚠️      |

SAFETY MODEL:
-------------
- deploy: Uses CREATE IF NOT EXISTS for tables and enums. Safe to run repeatedly.
- migrate: Uses ADD COLUMN IF NOT EXISTS. Only adds, never drops.
- rebuild: Requires ALLOW_DESTRUCTIVE_BOOTSTRAP=true AND ?confirm=DESTROY.
           DELETES ALL DATA. Development only. Remove before UAT/Prod.

All operations use:
- AsyncPostgreSQLRepository for database access (Azure AD auth)
- PydanticToSQL for DDL generation (Pydantic models are single source of truth)

These endpoints are intended for operational use, not regular API consumers.
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
                # dag_node_states.input_params (for dynamic node retries)
                {
                    "table": "dag_node_states",
                    "column": "input_params",
                    "sql": """
                        ALTER TABLE dagapp.dag_node_states
                        ADD COLUMN IF NOT EXISTS input_params JSONB
                    """,
                },
                # dag_node_states.checkpoint_phase
                {
                    "table": "dag_node_states",
                    "column": "checkpoint_phase",
                    "sql": """
                        ALTER TABLE dagapp.dag_node_states
                        ADD COLUMN IF NOT EXISTS checkpoint_phase VARCHAR(64)
                    """,
                },
                # dag_node_states.checkpoint_data
                {
                    "table": "dag_node_states",
                    "column": "checkpoint_data",
                    "sql": """
                        ALTER TABLE dagapp.dag_node_states
                        ADD COLUMN IF NOT EXISTS checkpoint_data JSONB
                    """,
                },
                # dag_node_states.checkpoint_saved_at
                {
                    "table": "dag_node_states",
                    "column": "checkpoint_saved_at",
                    "sql": """
                        ALTER TABLE dagapp.dag_node_states
                        ADD COLUMN IF NOT EXISTS checkpoint_saved_at TIMESTAMPTZ
                    """,
                },
                # Create index for parent_node_id (fan-out children)
                {
                    "table": "dag_node_states",
                    "column": "idx_parent",
                    "sql": """
                        CREATE INDEX IF NOT EXISTS idx_dag_node_parent
                        ON dagapp.dag_node_states (parent_node_id)
                        WHERE parent_node_id IS NOT NULL
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
                # dag_node_states.version (optimistic locking)
                {
                    "table": "dag_node_states",
                    "column": "version",
                    "sql": """
                        ALTER TABLE dagapp.dag_node_states
                        ADD COLUMN IF NOT EXISTS version INTEGER DEFAULT 1 NOT NULL
                    """,
                },
                # dag_jobs.version (optimistic locking)
                {
                    "table": "dag_jobs",
                    "column": "version",
                    "sql": """
                        ALTER TABLE dagapp.dag_jobs
                        ADD COLUMN IF NOT EXISTS version INTEGER DEFAULT 1 NOT NULL
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


@router.post("/rebuild")
async def rebuild_schema(
    confirm: str = Query(None, description="Must be 'DESTROY' to execute"),
):
    """
    DESTRUCTIVE: Drop and recreate the entire dagapp schema.

    ⚠️  WARNING: This will DELETE ALL DATA in the dagapp schema!

    This endpoint is for DEVELOPMENT ONLY. It:
    1. Drops the entire dagapp schema (CASCADE)
    2. Recreates all tables, enums, indexes from Pydantic models

    Safety requirements:
    - Environment variable ALLOW_DESTRUCTIVE_BOOTSTRAP must be 'true'
    - Query parameter confirm must be 'DESTROY'

    For production deployments, use:
    - POST /deploy for initial schema creation (idempotent, safe)
    - POST /migrate for adding missing columns (non-destructive)

    This endpoint should be REMOVED before UAT/Production deployment.
    """
    import os

    # Safety check 1: Environment variable
    if os.environ.get("ALLOW_DESTRUCTIVE_BOOTSTRAP", "").lower() != "true":
        return JSONResponse(
            status_code=403,
            content={
                "error": "Destructive bootstrap disabled",
                "message": "Set ALLOW_DESTRUCTIVE_BOOTSTRAP=true to enable",
                "hint": "This endpoint is for development only"
            }
        )

    # Safety check 2: Confirmation parameter
    if confirm != "DESTROY":
        return JSONResponse(
            status_code=400,
            content={
                "error": "Confirmation required",
                "message": "Add ?confirm=DESTROY to execute (this will DELETE ALL DATA)",
                "example": "POST /api/v1/bootstrap/rebuild?confirm=DESTROY",
                "warning": "This will drop the entire dagapp schema and all data!"
            }
        )

    try:
        from infrastructure.postgresql import AsyncPostgreSQLRepository
        from core.schema.sql_generator import PydanticToSQL

        repo = AsyncPostgreSQLRepository(schema_name="dagapp")

        # Use destructive mode for full rebuild
        generator = PydanticToSQL(schema_name="dagapp", destructive=True)

        results = {
            "drop_schema": None,
            "create_schema": None,
            "statements_executed": 0,
            "errors": []
        }

        async with repo.get_connection() as conn:
            # Step 1: Drop schema CASCADE
            try:
                drop_stmt = generator.generate_drop_schema()
                await conn.execute(drop_stmt.as_string(None))
                results["drop_schema"] = "success"
                logger.warning("REBUILD: Dropped dagapp schema (CASCADE)")
            except Exception as e:
                results["drop_schema"] = f"failed: {e}"
                results["errors"].append(f"Drop schema failed: {e}")
                logger.error(f"REBUILD: Drop schema failed: {e}")

            # Step 2: Create schema and all objects
            try:
                statements = generator.generate_all()
                executed = 0
                for stmt in statements:
                    try:
                        await conn.execute(stmt.as_string(None))
                        executed += 1
                    except Exception as e:
                        results["errors"].append(f"Statement failed: {e}")
                        logger.error(f"REBUILD: Statement failed: {e}")

                results["create_schema"] = "success"
                results["statements_executed"] = executed
                logger.info(f"REBUILD: Executed {executed} DDL statements")
            except Exception as e:
                results["create_schema"] = f"failed: {e}"
                results["errors"].append(f"Create schema failed: {e}")
                logger.error(f"REBUILD: Create schema failed: {e}")

        success = results["drop_schema"] == "success" and results["create_schema"] == "success"

        return JSONResponse(
            status_code=200 if success else 500,
            content={
                "status": "rebuilt" if success else "failed",
                "warning": "ALL DATA WAS DELETED" if success else "Rebuild may be incomplete",
                **results
            }
        )

    except Exception as e:
        logger.error(f"REBUILD: Unexpected error: {e}")
        raise HTTPException(500, f"Rebuild failed: {e}")


# ============================================================================
# PLATFORM SEED
# ============================================================================

# Default platform registrations.
# These are the B2B platforms the system knows about.
# Platforms are reference data — they define what identity fields
# each B2B application must provide when submitting assets.
DEFAULT_PLATFORMS = [
    {
        "platform_id": "ddh",
        "display_name": "Data Distribution Hub",
        "description": "DDH geospatial data platform",
        "required_refs": ["dataset_id", "resource_id"],
        "optional_refs": [],
    },
    {
        "platform_id": "arcgis",
        "display_name": "ArcGIS Online",
        "description": "Esri ArcGIS Online hosted layers",
        "required_refs": ["item_id", "layer_index"],
        "optional_refs": [],
    },
    {
        "platform_id": "geonode",
        "display_name": "GeoNode",
        "description": "GeoNode open-source SDI",
        "required_refs": ["uuid"],
        "optional_refs": [],
    },
]


@router.post("/seed-platforms")
async def seed_platforms(
    confirm: str = Query(None, description="Must be 'yes' to execute"),
):
    """
    Seed the platforms table with default B2B platform registrations.

    Idempotent: uses INSERT ON CONFLICT DO NOTHING. Safe to run repeatedly.

    Platforms define what identity fields each B2B application must provide
    when submitting assets. Without platform records, asset submission fails.

    Default platforms: ddh, arcgis, geonode.
    """
    if confirm != "yes":
        return JSONResponse(
            status_code=400,
            content={
                "error": "Confirmation required",
                "message": "Add ?confirm=yes to execute",
                "example": "POST /api/v1/bootstrap/seed-platforms?confirm=yes",
                "platforms": [p["platform_id"] for p in DEFAULT_PLATFORMS],
            }
        )

    try:
        from infrastructure.postgresql import AsyncPostgreSQLRepository
        from psycopg.types.json import Json

        repo = AsyncPostgreSQLRepository(schema_name="dagapp")

        seeded = []
        skipped = []
        errors = []

        async with repo.get_connection() as conn:
            for p in DEFAULT_PLATFORMS:
                try:
                    result = await conn.execute(
                        """
                        INSERT INTO dagapp.platforms (
                            platform_id, display_name, description,
                            required_refs, optional_refs, is_active,
                            created_at, updated_at
                        ) VALUES (
                            %(platform_id)s, %(display_name)s, %(description)s,
                            %(required_refs)s, %(optional_refs)s, true,
                            NOW(), NOW()
                        )
                        ON CONFLICT (platform_id) DO NOTHING
                        """,
                        {
                            "platform_id": p["platform_id"],
                            "display_name": p["display_name"],
                            "description": p["description"],
                            "required_refs": Json(p["required_refs"]),
                            "optional_refs": Json(p["optional_refs"]),
                        },
                    )
                    if result.rowcount > 0:
                        seeded.append(p["platform_id"])
                        logger.info(f"Seeded platform: {p['platform_id']}")
                    else:
                        skipped.append(p["platform_id"])
                except Exception as e:
                    errors.append({"platform_id": p["platform_id"], "error": str(e)})
                    logger.error(f"Failed to seed platform {p['platform_id']}: {e}")

        return JSONResponse(content={
            "status": "completed",
            "seeded": seeded,
            "skipped_existing": skipped,
            "errors": errors,
        })

    except Exception as e:
        logger.error(f"Platform seeding failed: {e}")
        raise HTTPException(500, f"Seeding failed: {e}")
