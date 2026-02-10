# ============================================================================
# PLATFORM REPOSITORY
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Domain - Platform CRUD operations
# PURPOSE: Database access for platforms table
# CREATED: 09 FEB 2026
# ============================================================================
"""
Platform Repository

CRUD operations for the B2B platform registry.
All SQL uses psycopg sql.SQL composition for injection safety.
"""

import logging
from typing import Any, Dict, List, Optional

from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Json
from psycopg_pool import AsyncConnectionPool

from core.models.platform import Platform
from .database import TABLE_PLATFORMS

logger = logging.getLogger(__name__)


class PlatformRepository:
    """Repository for Platform entities."""

    def __init__(self, pool: AsyncConnectionPool):
        self.pool = pool

    async def create(self, platform: Platform) -> Platform:
        """Create a new platform registration."""
        async with self.pool.connection() as conn:
            await conn.execute(
                sql.SQL("""
                    INSERT INTO {} (
                        platform_id, display_name, description,
                        required_refs, optional_refs, is_active,
                        created_at, updated_at
                    ) VALUES (
                        %(platform_id)s, %(display_name)s, %(description)s,
                        %(required_refs)s, %(optional_refs)s, %(is_active)s,
                        %(created_at)s, %(updated_at)s
                    )
                """).format(TABLE_PLATFORMS),
                {
                    "platform_id": platform.platform_id,
                    "display_name": platform.display_name,
                    "description": platform.description,
                    "required_refs": Json(platform.required_refs),
                    "optional_refs": Json(platform.optional_refs),
                    "is_active": platform.is_active,
                    "created_at": platform.created_at,
                    "updated_at": platform.updated_at,
                },
            )
            logger.info(f"Created platform '{platform.platform_id}'")
            return platform

    async def get(self, platform_id: str) -> Optional[Platform]:
        """Get a platform by ID."""
        async with self.pool.connection() as conn:
            conn.row_factory = dict_row
            result = await conn.execute(
                sql.SQL("SELECT * FROM {} WHERE platform_id = %s").format(TABLE_PLATFORMS),
                (platform_id,),
            )
            row = await result.fetchone()
            return self._row_to_model(row) if row else None

    async def list_active(self) -> List[Platform]:
        """List all active platforms."""
        async with self.pool.connection() as conn:
            conn.row_factory = dict_row
            result = await conn.execute(
                sql.SQL(
                    "SELECT * FROM {} WHERE is_active = true ORDER BY display_name"
                ).format(TABLE_PLATFORMS),
            )
            rows = await result.fetchall()
            return [self._row_to_model(row) for row in rows]

    def _row_to_model(self, row: Dict[str, Any]) -> Platform:
        """Convert a database row to a Platform instance."""
        return Platform(
            platform_id=row["platform_id"],
            display_name=row["display_name"],
            description=row.get("description"),
            required_refs=row.get("required_refs") or [],
            optional_refs=row.get("optional_refs") or [],
            is_active=row.get("is_active", True),
            created_at=row["created_at"],
            updated_at=row.get("updated_at") or row["created_at"],
        )
