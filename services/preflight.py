# ============================================================================
# CLAUDE CONTEXT - PRE-FLIGHT VALIDATION
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Service - Submission pre-flight validation
# PURPOSE: Validate submissions before job creation (cheap checks, no GDAL)
# CREATED: 13 FEB 2026
# ============================================================================
"""
Pre-flight Validation

Two-stage validation architecture:
  Stage 1 (this module): Synchronous, at submit time, no file loading.
    Catches bad params, missing blobs, duplicate submissions BEFORE
    burning a job, queue message, and worker pickup cycle.

  Stage 2 (raster.validate handler): Async, on worker, file loaded with GDAL.
    CRS validation, type detection, compression profile selection.

Design:
  - PreflightValidator ABC with universal checks (blob exists, required params,
    idempotency) enforced via ClassVar declarations.
  - Subclasses override _validate_workflow_specific() for type-specific checks.
  - Shallow hierarchy: 1 level deep, 2-3 concrete leaves.
  - PreflightResult collects ALL errors (not fail-fast on first).
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import ClassVar, Dict, Any, List, Optional

from psycopg_pool import AsyncConnectionPool

from core.logging import get_logger

logger = get_logger(__name__)


# ============================================================================
# RESULT
# ============================================================================

@dataclass
class PreflightResult:
    """
    Result of pre-flight validation.

    Collects all errors — reports every problem at once rather than
    failing on the first and making the caller fix them one at a time.
    """
    valid: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


# ============================================================================
# BASE VALIDATOR
# ============================================================================

class PreflightValidator(ABC):
    """
    Base pre-flight validator with universal checks.

    Runs at submit time BEFORE job creation. All checks are cheap
    (parameter validation, DB reads, blob HEAD requests).

    Subclasses set ClassVars and override _validate_workflow_specific().
    """

    # Subclass declares which input_params keys are required
    REQUIRED_PARAMS: ClassVar[List[str]] = []

    def __init__(
        self,
        pool: AsyncConnectionPool,
        blob_repo=None,
    ):
        self.pool = pool
        self._blob_repo = blob_repo

    @property
    def blob_repo(self):
        """Lazy-init BlobRepository (avoids import at module level on gateway)."""
        if self._blob_repo is None:
            from infrastructure.storage import BlobRepository
            self._blob_repo = BlobRepository()
        return self._blob_repo

    async def validate(
        self,
        input_params: Dict[str, Any],
        correlation_id: Optional[str] = None,
    ) -> PreflightResult:
        """
        Run all pre-flight checks.

        Args:
            input_params: The workflow input parameters from the submission.
            correlation_id: Optional B2B request_id for idempotency check.

        Returns:
            PreflightResult with collected errors and warnings.
        """
        errors: List[str] = []
        warnings: List[str] = []

        # 1. Required params
        errors.extend(self._check_required_params(input_params))

        # 2. Blob existence (only if the relevant params are present)
        blob_name = input_params.get("blob_name")
        container_name = input_params.get("container_name")
        if blob_name and container_name:
            errors.extend(self._check_blob_exists(container_name, blob_name))

        # 3. Idempotency (only if correlation_id provided)
        if correlation_id:
            idemp_errors = await self._check_idempotency(correlation_id)
            errors.extend(idemp_errors)

        # 4. Workflow-specific checks
        specific = self._validate_workflow_specific(input_params)
        errors.extend(specific.errors)
        warnings.extend(specific.warnings)

        return PreflightResult(
            valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
        )

    # ================================================================
    # UNIVERSAL CHECKS
    # ================================================================

    def _check_required_params(
        self, input_params: Dict[str, Any]
    ) -> List[str]:
        """Check that all REQUIRED_PARAMS are present and non-empty."""
        errors = []
        for param in self.REQUIRED_PARAMS:
            value = input_params.get(param)
            if value is None or (isinstance(value, str) and not value.strip()):
                errors.append(f"Missing required parameter: {param}")
        return errors

    def _check_blob_exists(
        self, container_name: str, blob_name: str
    ) -> List[str]:
        """
        Check that the source blob exists in storage.

        Uses BlobRepository.blob_exists() which is a single HEAD request.
        Implicitly validates the container — if the container doesn't exist,
        the blob also doesn't exist.
        """
        try:
            exists = self.blob_repo.blob_exists(container_name, blob_name)
            if not exists:
                return [f"Blob not found: {container_name}/{blob_name}"]
            return []
        except Exception as e:
            return [
                f"Storage error checking blob "
                f"{container_name}/{blob_name}: {e}"
            ]

    async def _check_idempotency(
        self, correlation_id: str
    ) -> List[str]:
        """
        Check if a non-terminal job already exists for this correlation_id.

        Allows resubmission if all previous jobs with this correlation_id
        are terminal (completed/failed/cancelled). Only rejects if there's
        an active (non-terminal) job — prevents duplicate processing.
        """
        from repositories import JobRepository

        job_repo = JobRepository(self.pool)
        existing = await job_repo.get_by_correlation_id(correlation_id)

        if existing is not None and not existing.is_terminal:
            return [
                f"Duplicate submission: job {existing.job_id} already "
                f"exists for correlation_id '{correlation_id}' "
                f"(status: {existing.status.value})"
            ]
        return []

    # ================================================================
    # WORKFLOW-SPECIFIC (subclass override)
    # ================================================================

    @abstractmethod
    def _validate_workflow_specific(
        self, input_params: Dict[str, Any]
    ) -> PreflightResult:
        """
        Override in subclasses for workflow-specific validation.

        Returns PreflightResult with any additional errors/warnings.
        Extension checks, enum validation, CRS format checks, etc.
        """
        ...


# ============================================================================
# RASTER VALIDATOR
# ============================================================================

class RasterPreflightValidator(PreflightValidator):
    """
    Pre-flight validation for raster ingest workflows.

    Currently only enforces universal checks (required params, blob exists,
    idempotency). Raster-specific checks (file extension, raster_type enum,
    output_tier enum, target_crs format) will be added later.
    """

    REQUIRED_PARAMS: ClassVar[List[str]] = [
        "blob_name",
        "container_name",
        "collection_id",
    ]

    def _validate_workflow_specific(
        self, input_params: Dict[str, Any]
    ) -> PreflightResult:
        """Raster-specific checks — placeholder for future extension."""
        return PreflightResult(valid=True)


# ============================================================================
# FACTORY
# ============================================================================

# Registry of workflow_id prefixes → validator classes
_VALIDATOR_REGISTRY: Dict[str, type] = {
    "raster_": RasterPreflightValidator,
}


def get_preflight_validator(
    workflow_id: str,
    pool: AsyncConnectionPool,
    blob_repo=None,
) -> Optional[PreflightValidator]:
    """
    Get the appropriate pre-flight validator for a workflow.

    Matches workflow_id against registered prefixes. Returns None if
    no validator is registered (standalone/test workflows skip pre-flight).

    Args:
        workflow_id: The workflow being submitted (e.g., "raster_ingest")
        pool: AsyncConnectionPool for DB checks
        blob_repo: Optional BlobRepository override (for testing)

    Returns:
        PreflightValidator instance, or None if no validator registered.
    """
    for prefix, validator_cls in _VALIDATOR_REGISTRY.items():
        if workflow_id.startswith(prefix):
            return validator_cls(pool=pool, blob_repo=blob_repo)
    return None


# ============================================================================
# EXPORTS
# ============================================================================

__all__ = [
    "PreflightResult",
    "PreflightValidator",
    "RasterPreflightValidator",
    "get_preflight_validator",
]
