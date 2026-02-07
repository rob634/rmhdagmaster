# ============================================================================
# BASE REPOSITORY - ERROR HANDLING AND VALIDATION PATTERNS
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Infrastructure - Base repository patterns
# PURPOSE: Common validation, error handling, and logging for all repositories
# CREATED: 03 FEB 2026
# PATTERNS: Extracted from rmhgeoapi/infrastructure/base.py
# ============================================================================
"""
Base Repository Patterns

Abstract base class that provides common infrastructure for all repositories:
- Consistent error handling with context managers
- Status transition validation
- Standardized logging
- Contract enforcement patterns

This is the foundation layer - storage-specific repositories (PostgreSQL,
Service Bus, Blob) extend this with their connection management.
"""

import logging
from abc import ABC
from contextlib import contextmanager
from typing import Any, Dict, Optional, Union

from core.contracts import JobStatus, NodeStatus

logger = logging.getLogger(__name__)


class RepositoryError(Exception):
    """Base exception for repository operations."""

    def __init__(self, message: str, operation: str = None, entity_id: str = None):
        self.operation = operation
        self.entity_id = entity_id
        super().__init__(message)


class ValidationError(RepositoryError):
    """Raised when validation fails (status transition, schema, etc)."""

    def __init__(self, message: str, field: str = None, value: Any = None):
        self.field = field
        self.value = value
        super().__init__(message)


class BaseRepository(ABC):
    """
    Abstract base repository with common patterns.

    Provides:
    - Error context manager for consistent error handling
    - Status transition validation
    - Standardized logging

    Subclasses implement storage-specific operations.
    """

    def __init__(self):
        """Initialize base repository."""
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.debug(f"{self.__class__.__name__} initialized")

    @contextmanager
    def _error_context(self, operation: str, entity_id: Optional[str] = None):
        """
        Context manager for consistent error handling.

        Wraps repository operations with standardized logging and
        exception handling. All exceptions are logged with context
        before being re-raised.

        Args:
            operation: Human-readable description of the operation
            entity_id: Optional entity ID for context

        Yields:
            None - the actual operation happens in the with block

        Example:
            with self._error_context("job creation", job_id):
                await self._execute_insert(job)
        """
        try:
            yield
        except ValidationError:
            # Already has context, just re-raise
            raise
        except Exception as e:
            error_msg = f"{operation} failed"
            if entity_id:
                error_msg += f" for {entity_id}"
            error_msg += f": {e}"
            self.logger.error(error_msg)
            raise RepositoryError(error_msg, operation=operation, entity_id=entity_id) from e

    def _validate_status_transition(
        self,
        current_status: Union[JobStatus, NodeStatus],
        new_status: Union[JobStatus, NodeStatus],
        allowed_transitions: Dict[Any, set],
    ) -> None:
        """
        Validate status transition against allowed transitions map.

        Args:
            current_status: Current status enum value
            new_status: Proposed new status
            allowed_transitions: Dict mapping status -> set of allowed next statuses

        Raises:
            ValidationError: If transition is not allowed

        Example:
            ALLOWED = {
                JobStatus.PENDING: {JobStatus.RUNNING, JobStatus.CANCELLED},
                JobStatus.RUNNING: {JobStatus.COMPLETED, JobStatus.FAILED},
            }
            self._validate_status_transition(job.status, new_status, ALLOWED)
        """
        if current_status == new_status:
            return  # No-op is always allowed

        allowed = allowed_transitions.get(current_status, set())
        if new_status not in allowed:
            raise ValidationError(
                f"Invalid status transition: {current_status.value} -> {new_status.value}. "
                f"Allowed from {current_status.value}: {[s.value for s in allowed]}",
                field="status",
                value=f"{current_status.value} -> {new_status.value}",
            )

    def _log_operation(
        self,
        success: bool,
        operation: str,
        entity_id: str,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Log operation result with consistent formatting.

        Args:
            success: True if operation succeeded
            operation: Description of the operation
            entity_id: Entity identifier (truncated for readability)
            details: Optional additional context

        Format:
            Success: "operation: entity_id | details"
            Failure: "operation failed: entity_id | details"
        """
        # Truncate long IDs for readability
        short_id = entity_id[:16] + "..." if len(entity_id) > 16 else entity_id

        if success:
            msg = f"{operation}: {short_id}"
        else:
            msg = f"{operation} failed: {short_id}"

        if details:
            msg += f" | {details}"

        if success:
            self.logger.info(msg)
        else:
            self.logger.warning(msg)


class AsyncBaseRepository(BaseRepository):
    """
    Async version of BaseRepository.

    Same patterns as BaseRepository but for async context managers.
    """

    @contextmanager
    def _error_context(self, operation: str, entity_id: Optional[str] = None):
        """Sync error context - same as base class."""
        try:
            yield
        except ValidationError:
            raise
        except Exception as e:
            error_msg = f"{operation} failed"
            if entity_id:
                error_msg += f" for {entity_id}"
            error_msg += f": {e}"
            self.logger.error(error_msg)
            raise RepositoryError(error_msg, operation=operation, entity_id=entity_id) from e


# ============================================================================
# STATUS TRANSITION RULES
# ============================================================================

# Job status transitions
JOB_STATUS_TRANSITIONS = {
    JobStatus.PENDING: {JobStatus.RUNNING, JobStatus.CANCELLED},
    JobStatus.RUNNING: {JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED},
    JobStatus.COMPLETED: set(),  # Terminal
    JobStatus.FAILED: set(),     # Terminal
    JobStatus.CANCELLED: set(),  # Terminal
}

# Node status transitions
NODE_STATUS_TRANSITIONS = {
    NodeStatus.PENDING: {NodeStatus.READY, NodeStatus.SKIPPED},
    NodeStatus.READY: {NodeStatus.DISPATCHED, NodeStatus.SKIPPED},
    NodeStatus.DISPATCHED: {NodeStatus.RUNNING, NodeStatus.FAILED},
    NodeStatus.RUNNING: {NodeStatus.COMPLETED, NodeStatus.FAILED},
    NodeStatus.COMPLETED: set(),  # Terminal
    NodeStatus.FAILED: {NodeStatus.READY},  # Can retry -> READY
    NodeStatus.SKIPPED: set(),    # Terminal
}


# ============================================================================
# EXPORTS
# ============================================================================

__all__ = [
    "BaseRepository",
    "AsyncBaseRepository",
    "RepositoryError",
    "ValidationError",
    "JOB_STATUS_TRANSITIONS",
    "NODE_STATUS_TRANSITIONS",
]
