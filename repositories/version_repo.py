# ============================================================================
# ASSET VERSION REPOSITORY
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Domain - AssetVersion CRUD operations
# PURPOSE: Database access for asset_versions table
# CREATED: 09 FEB 2026
# ============================================================================
"""
AssetVersion Repository

CRUD operations for asset versions.
All SQL uses psycopg sql.SQL composition for injection safety.
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Json
from psycopg_pool import AsyncConnectionPool

from core.models.asset_version import AssetVersion
from core.contracts import ApprovalState, ProcessingStatus
from .database import TABLE_ASSET_VERSIONS

logger = logging.getLogger(__name__)


class AssetVersionRepository:
    """Repository for AssetVersion entities."""

    def __init__(self, pool: AsyncConnectionPool):
        self.pool = pool

    async def create(self, version: AssetVersion) -> AssetVersion:
        """Create a new asset version."""
        async with self.pool.connection() as conn:
            await conn.execute(
                sql.SQL("""
                    INSERT INTO {} (
                        asset_id, version_ordinal, version_label,
                        approval_state, reviewer, reviewed_at, rejection_reason,
                        processing_state, processing_started_at, processing_completed_at,
                        last_error, current_job_id,
                        revision, content_hash,
                        blob_path, table_name, stac_item_id, stac_collection_id,
                        external_blob_path, adf_run_id,
                        created_at, updated_at, version
                    ) VALUES (
                        %(asset_id)s, %(version_ordinal)s, %(version_label)s,
                        %(approval_state)s, %(reviewer)s, %(reviewed_at)s, %(rejection_reason)s,
                        %(processing_state)s, %(processing_started_at)s, %(processing_completed_at)s,
                        %(last_error)s, %(current_job_id)s,
                        %(revision)s, %(content_hash)s,
                        %(blob_path)s, %(table_name)s, %(stac_item_id)s, %(stac_collection_id)s,
                        %(external_blob_path)s, %(adf_run_id)s,
                        %(created_at)s, %(updated_at)s, %(version)s
                    )
                """).format(TABLE_ASSET_VERSIONS),
                {
                    "asset_id": version.asset_id,
                    "version_ordinal": version.version_ordinal,
                    "version_label": version.version_label,
                    "approval_state": version.approval_state.value,
                    "reviewer": version.reviewer,
                    "reviewed_at": version.reviewed_at,
                    "rejection_reason": version.rejection_reason,
                    "processing_state": version.processing_state.value,
                    "processing_started_at": version.processing_started_at,
                    "processing_completed_at": version.processing_completed_at,
                    "last_error": version.last_error,
                    "current_job_id": version.current_job_id,
                    "revision": version.revision,
                    "content_hash": version.content_hash,
                    "blob_path": version.blob_path,
                    "table_name": version.table_name,
                    "stac_item_id": version.stac_item_id,
                    "stac_collection_id": version.stac_collection_id,
                    "external_blob_path": version.external_blob_path,
                    "adf_run_id": version.adf_run_id,
                    "created_at": version.created_at,
                    "updated_at": version.updated_at,
                    "version": version.version,
                },
            )
            logger.info(
                f"Created version {version.version_ordinal} for asset {version.asset_id}"
            )
            return version

    async def get(
        self, asset_id: str, version_ordinal: int
    ) -> Optional[AssetVersion]:
        """Get a specific version by composite PK."""
        async with self.pool.connection() as conn:
            conn.row_factory = dict_row
            result = await conn.execute(
                sql.SQL(
                    "SELECT * FROM {} WHERE asset_id = %s AND version_ordinal = %s"
                ).format(TABLE_ASSET_VERSIONS),
                (asset_id, version_ordinal),
            )
            row = await result.fetchone()
            return self._row_to_model(row) if row else None

    async def update(self, version: AssetVersion) -> bool:
        """
        Update an asset version with optimistic locking.

        Returns:
            True if update succeeded, False if version conflict.
        """
        version.updated_at = datetime.now(timezone.utc)

        async with self.pool.connection() as conn:
            result = await conn.execute(
                sql.SQL("""
                    UPDATE {} SET
                        approval_state = %(approval_state)s,
                        reviewer = %(reviewer)s,
                        reviewed_at = %(reviewed_at)s,
                        rejection_reason = %(rejection_reason)s,
                        processing_state = %(processing_state)s,
                        processing_started_at = %(processing_started_at)s,
                        processing_completed_at = %(processing_completed_at)s,
                        last_error = %(last_error)s,
                        current_job_id = %(current_job_id)s,
                        revision = %(revision)s,
                        content_hash = %(content_hash)s,
                        blob_path = %(blob_path)s,
                        table_name = %(table_name)s,
                        stac_item_id = %(stac_item_id)s,
                        stac_collection_id = %(stac_collection_id)s,
                        external_blob_path = %(external_blob_path)s,
                        adf_run_id = %(adf_run_id)s,
                        updated_at = %(updated_at)s,
                        version = version + 1
                    WHERE asset_id = %(asset_id)s
                      AND version_ordinal = %(version_ordinal)s
                      AND version = %(version)s
                """).format(TABLE_ASSET_VERSIONS),
                {
                    "asset_id": version.asset_id,
                    "version_ordinal": version.version_ordinal,
                    "approval_state": version.approval_state.value,
                    "reviewer": version.reviewer,
                    "reviewed_at": version.reviewed_at,
                    "rejection_reason": version.rejection_reason,
                    "processing_state": version.processing_state.value,
                    "processing_started_at": version.processing_started_at,
                    "processing_completed_at": version.processing_completed_at,
                    "last_error": version.last_error,
                    "current_job_id": version.current_job_id,
                    "revision": version.revision,
                    "content_hash": version.content_hash,
                    "blob_path": version.blob_path,
                    "table_name": version.table_name,
                    "stac_item_id": version.stac_item_id,
                    "stac_collection_id": version.stac_collection_id,
                    "external_blob_path": version.external_blob_path,
                    "adf_run_id": version.adf_run_id,
                    "updated_at": version.updated_at,
                    "version": version.version,
                },
            )

            if result.rowcount == 0:
                logger.warning(
                    f"Version conflict updating asset_version "
                    f"{version.asset_id}:{version.version_ordinal} "
                    f"(expected version {version.version})"
                )
                return False

            version.version += 1
            return True

    async def get_by_job_id(self, job_id: str) -> Optional[AssetVersion]:
        """Get the asset version associated with a DAG job.

        Used by processing callbacks to find which version a completed
        job belongs to.
        """
        async with self.pool.connection() as conn:
            conn.row_factory = dict_row
            result = await conn.execute(
                sql.SQL(
                    "SELECT * FROM {} WHERE current_job_id = %s LIMIT 1"
                ).format(TABLE_ASSET_VERSIONS),
                (job_id,),
            )
            row = await result.fetchone()
            return self._row_to_model(row) if row else None

    async def list_for_asset(
        self, asset_id: str, limit: int = 50
    ) -> List[AssetVersion]:
        """List all versions for an asset, newest first."""
        async with self.pool.connection() as conn:
            conn.row_factory = dict_row
            result = await conn.execute(
                sql.SQL(
                    "SELECT * FROM {} WHERE asset_id = %s "
                    "ORDER BY version_ordinal DESC LIMIT %s"
                ).format(TABLE_ASSET_VERSIONS),
                (asset_id, limit),
            )
            rows = await result.fetchall()
            return [self._row_to_model(row) for row in rows]

    async def get_latest_approved(self, asset_id: str) -> Optional[AssetVersion]:
        """Get the highest-ordinal approved version (version=latest resolution)."""
        async with self.pool.connection() as conn:
            conn.row_factory = dict_row
            result = await conn.execute(
                sql.SQL(
                    "SELECT * FROM {} "
                    "WHERE asset_id = %s AND approval_state = %s "
                    "ORDER BY version_ordinal DESC LIMIT 1"
                ).format(TABLE_ASSET_VERSIONS),
                (asset_id, ApprovalState.APPROVED.value),
            )
            row = await result.fetchone()
            return self._row_to_model(row) if row else None

    async def get_next_ordinal(self, asset_id: str) -> int:
        """Get the next version_ordinal for an asset (0 if first version)."""
        async with self.pool.connection() as conn:
            conn.row_factory = dict_row
            result = await conn.execute(
                sql.SQL(
                    "SELECT COALESCE(MAX(version_ordinal), -1) + 1 AS next_ordinal "
                    "FROM {} WHERE asset_id = %s"
                ).format(TABLE_ASSET_VERSIONS),
                (asset_id,),
            )
            row = await result.fetchone()
            return row["next_ordinal"]

    async def has_unresolved_versions(self, asset_id: str) -> bool:
        """
        Check if any version is still pending_review.

        Used to enforce version ordering constraint:
        must resolve version N before submitting N+1.
        """
        async with self.pool.connection() as conn:
            conn.row_factory = dict_row
            result = await conn.execute(
                sql.SQL(
                    "SELECT COUNT(*) AS count FROM {} "
                    "WHERE asset_id = %s AND approval_state = %s"
                ).format(TABLE_ASSET_VERSIONS),
                (asset_id, ApprovalState.PENDING_REVIEW.value),
            )
            row = await result.fetchone()
            return row["count"] > 0

    def _row_to_model(self, row: Dict[str, Any]) -> AssetVersion:
        """Convert a database row to an AssetVersion instance."""
        return AssetVersion(
            asset_id=row["asset_id"],
            version_ordinal=row["version_ordinal"],
            version_label=row.get("version_label"),
            approval_state=ApprovalState(row["approval_state"]),
            reviewer=row.get("reviewer"),
            reviewed_at=row.get("reviewed_at"),
            rejection_reason=row.get("rejection_reason"),
            processing_state=ProcessingStatus(row["processing_state"]),
            processing_started_at=row.get("processing_started_at"),
            processing_completed_at=row.get("processing_completed_at"),
            last_error=row.get("last_error"),
            current_job_id=row.get("current_job_id"),
            revision=row.get("revision", 0),
            content_hash=row.get("content_hash"),
            blob_path=row.get("blob_path"),
            table_name=row.get("table_name"),
            stac_item_id=row.get("stac_item_id"),
            stac_collection_id=row.get("stac_collection_id"),
            external_blob_path=row.get("external_blob_path"),
            adf_run_id=row.get("adf_run_id"),
            created_at=row["created_at"],
            updated_at=row.get("updated_at") or row["created_at"],
            version=row.get("version", 1),
        )
