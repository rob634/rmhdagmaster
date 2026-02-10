# ============================================================================
# CLAUDE CONTEXT - ASSET VERSION MODEL
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Domain model - Per-version state and artifacts
# PURPOSE: Track approval, processing, revision, and service artifacts per version
# LAST_REVIEWED: 09 FEB 2026
# ============================================================================
"""
AssetVersion Model

One version of a GeospatialAsset. Each version tracks its own:
- Approval state (pending_review → approved | rejected)
- Processing state (queued → processing → completed | failed)
- Revision counter (reprocessing increments this)
- Service artifacts (blob_path, table_name, stac IDs, external artifacts)

Composite PK: (asset_id, version_ordinal)

version_ordinal is auto-incremented per asset (0, 1, 2, ...).
version_label is an opaque B2B string ("V1", "2025-Q1", "Version Purple").

Version ordering constraint:
    Must resolve (approve or delete) version N before submitting N+1.
"""

from datetime import datetime, timezone
from typing import Any, ClassVar, Dict, List, Optional

from pydantic import BaseModel, Field, computed_field

from core.contracts import ApprovalState, ProcessingStatus


class AssetVersion(BaseModel):
    """
    One semantic version of a geospatial asset.

    Composite PK: (asset_id, version_ordinal)
    FK: asset_id → geospatial_assets(asset_id) CASCADE

    Maps to: dagapp.asset_versions
    """

    # SQL DDL METADATA
    __sql_table__: ClassVar[str] = "asset_versions"
    __sql_schema__: ClassVar[str] = "dagapp"
    __sql_primary_key__: ClassVar[List[str]] = ["asset_id", "version_ordinal"]
    __sql_foreign_keys__: ClassVar[Dict[str, str]] = {
        "asset_id": "dagapp.geospatial_assets(asset_id)",
        # NOTE: current_job_id intentionally NOT a DB FK.
        # sql_generator hardcodes ON DELETE CASCADE — deleting a job
        # must not cascade-delete the asset version.
    }
    __sql_indexes__: ClassVar[List] = [
        ("idx_asset_versions_asset", ["asset_id"]),
        ("idx_asset_versions_approval", ["approval_state"]),
        ("idx_asset_versions_processing", ["processing_state"]),
        ("idx_asset_versions_job", ["current_job_id"], "current_job_id IS NOT NULL"),
        {
            "name": "idx_asset_versions_latest",
            "columns": ["asset_id", "version_ordinal"],
            "descending": True,
        },
    ]

    # ----------------------------------------------------------------
    # Identity
    # ----------------------------------------------------------------

    asset_id: str = Field(
        ..., max_length=32,
        description="FK to geospatial_assets",
    )
    version_ordinal: int = Field(
        ..., ge=0,
        description="Auto-incremented version number (0, 1, 2, ...)",
    )
    version_label: Optional[str] = Field(
        default=None, max_length=100,
        description="Opaque B2B version name ('V1', '2025-Q1', 'Version Purple')",
    )

    # ----------------------------------------------------------------
    # Approval
    # ----------------------------------------------------------------

    approval_state: ApprovalState = Field(
        default=ApprovalState.PENDING_REVIEW,
        description="Per-version approval: pending_review → approved | rejected",
    )
    reviewer: Optional[str] = Field(default=None, max_length=64)
    reviewed_at: Optional[datetime] = Field(default=None)
    rejection_reason: Optional[str] = Field(default=None, max_length=500)

    # ----------------------------------------------------------------
    # Processing
    # ----------------------------------------------------------------

    processing_state: ProcessingStatus = Field(
        default=ProcessingStatus.QUEUED,
        description="ETL job lifecycle: queued → processing → completed | failed",
    )
    processing_started_at: Optional[datetime] = Field(default=None)
    processing_completed_at: Optional[datetime] = Field(default=None)
    last_error: Optional[str] = Field(default=None, max_length=2000)
    current_job_id: Optional[str] = Field(
        default=None, max_length=64,
        description="FK to dag_jobs (application-enforced, not DB FK)",
    )

    # ----------------------------------------------------------------
    # Revision (reprocessing counter)
    # ----------------------------------------------------------------

    revision: int = Field(
        default=0, ge=0,
        description="Increments on reprocessing (same version, new ETL run)",
    )
    content_hash: Optional[str] = Field(
        default=None, max_length=64,
        description="SHA256 of source file for change detection",
    )

    # ----------------------------------------------------------------
    # Service artifacts (Option A: flat fields)
    # ----------------------------------------------------------------

    # Internal artifacts — always produced by ETL
    blob_path: Optional[str] = Field(
        default=None, max_length=500,
        description="Silver storage COG path",
    )
    table_name: Optional[str] = Field(
        default=None, max_length=128,
        description="PostGIS table name",
    )
    stac_item_id: Optional[str] = Field(
        default=None, max_length=128,
        description="STAC catalog item ID",
    )
    stac_collection_id: Optional[str] = Field(
        default=None, max_length=128,
        description="STAC catalog collection ID",
    )

    # External artifacts — only produced if clearance = public (ADF export)
    external_blob_path: Optional[str] = Field(
        default=None, max_length=500,
        description="External-facing blob path (null if OUO)",
    )
    adf_run_id: Optional[str] = Field(
        default=None, max_length=128,
        description="ADF pipeline run ID (null if OUO)",
    )

    # ----------------------------------------------------------------
    # Timestamps & locking
    # ----------------------------------------------------------------

    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    version: int = Field(default=1, ge=1, description="Row version for optimistic locking")

    model_config = {"frozen": False}

    # ----------------------------------------------------------------
    # Computed fields
    # ----------------------------------------------------------------

    @computed_field
    @property
    def is_approved(self) -> bool:
        """True if this version has been approved."""
        return self.approval_state == ApprovalState.APPROVED

    @computed_field
    @property
    def is_processing_terminal(self) -> bool:
        """True if processing is completed or failed."""
        return self.processing_state.is_terminal()

    @computed_field
    @property
    def has_artifacts(self) -> bool:
        """True if ETL has produced at least one artifact."""
        return self.blob_path is not None or self.table_name is not None

    # ----------------------------------------------------------------
    # Approval transitions
    # ----------------------------------------------------------------

    def mark_approved(self, reviewer: str) -> None:
        """Transition: PENDING_REVIEW → APPROVED."""
        if self.approval_state != ApprovalState.PENDING_REVIEW:
            raise ValueError(
                f"Cannot approve from state '{self.approval_state.value}' "
                f"(must be '{ApprovalState.PENDING_REVIEW.value}')"
            )
        self.approval_state = ApprovalState.APPROVED
        self.reviewer = reviewer
        self.reviewed_at = datetime.now(timezone.utc)
        self.updated_at = datetime.now(timezone.utc)

    def mark_rejected(self, reviewer: str, reason: str) -> None:
        """Transition: PENDING_REVIEW → REJECTED."""
        if self.approval_state != ApprovalState.PENDING_REVIEW:
            raise ValueError(
                f"Cannot reject from state '{self.approval_state.value}' "
                f"(must be '{ApprovalState.PENDING_REVIEW.value}')"
            )
        self.approval_state = ApprovalState.REJECTED
        self.reviewer = reviewer
        self.reviewed_at = datetime.now(timezone.utc)
        self.rejection_reason = reason[:500]
        self.updated_at = datetime.now(timezone.utc)

    # ----------------------------------------------------------------
    # Processing transitions
    # ----------------------------------------------------------------

    def mark_processing_started(self, job_id: str) -> None:
        """Transition: QUEUED | FAILED → PROCESSING."""
        if self.processing_state not in (ProcessingStatus.QUEUED, ProcessingStatus.FAILED):
            raise ValueError(
                f"Cannot start processing from state '{self.processing_state.value}' "
                f"(must be 'queued' or 'failed')"
            )
        self.processing_state = ProcessingStatus.PROCESSING
        self.processing_started_at = datetime.now(timezone.utc)
        self.current_job_id = job_id
        self.updated_at = datetime.now(timezone.utc)

    def mark_processing_completed(self) -> None:
        """Transition: PROCESSING → COMPLETED."""
        if self.processing_state != ProcessingStatus.PROCESSING:
            raise ValueError(
                f"Cannot complete from state '{self.processing_state.value}' "
                f"(must be '{ProcessingStatus.PROCESSING.value}')"
            )
        self.processing_state = ProcessingStatus.COMPLETED
        self.processing_completed_at = datetime.now(timezone.utc)
        self.updated_at = datetime.now(timezone.utc)

    def mark_processing_failed(self, error: str) -> None:
        """Transition: PROCESSING → FAILED."""
        if self.processing_state != ProcessingStatus.PROCESSING:
            raise ValueError(
                f"Cannot fail from state '{self.processing_state.value}' "
                f"(must be '{ProcessingStatus.PROCESSING.value}')"
            )
        self.processing_state = ProcessingStatus.FAILED
        self.last_error = error[:2000]
        self.updated_at = datetime.now(timezone.utc)

    # ----------------------------------------------------------------
    # Reprocessing
    # ----------------------------------------------------------------

    def start_reprocessing(self, job_id: str) -> None:
        """Increment revision and restart processing (same version, new ETL run)."""
        self.revision += 1
        self.processing_state = ProcessingStatus.PROCESSING
        self.processing_started_at = datetime.now(timezone.utc)
        self.processing_completed_at = None
        self.last_error = None
        self.current_job_id = job_id
        self.updated_at = datetime.now(timezone.utc)


__all__ = ["AssetVersion"]
