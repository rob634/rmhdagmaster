# ============================================================================
# HANDLER REGISTRY
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Core - Handler registration and lookup
# PURPOSE: Register and discover task handlers by name
# CREATED: 31 JAN 2026
# ============================================================================
"""
Handler Registry

Central registry for task handlers. Workers use this to look up
the handler function to execute for a given task.

Design:
- Handlers are registered at import time via decorator
- Registry is a simple dict (handler_name -> handler_func)
- Fail-fast on duplicate registration
- Supports both sync and async handlers
"""

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Union, Awaitable

logger = logging.getLogger(__name__)


# ============================================================================
# HANDLER TYPES
# ============================================================================

@dataclass
class HandlerContext:
    """
    Context passed to handler functions.

    Contains all information needed to execute the task.
    """
    task_id: str
    job_id: str
    node_id: str
    handler: str
    params: Dict[str, Any]
    timeout_seconds: int
    retry_count: int
    correlation_id: Optional[str] = None

    # Checkpoint resume data (if resuming from checkpoint)
    checkpoint_phase: Optional[str] = None
    checkpoint_data: Optional[Dict[str, Any]] = None

    # Worker info
    worker_id: Optional[str] = None

    # Progress callback (optional)
    progress_callback: Optional[Callable[[int, int, str], None]] = None

    def report_progress(
        self,
        current: int,
        total: int,
        message: str = "",
    ) -> None:
        """Report progress if callback is available."""
        if self.progress_callback:
            self.progress_callback(current, total, message)


@dataclass
class HandlerResult:
    """
    Result returned by handler functions.

    Handlers should return this to indicate success/failure.
    """
    success: bool = True
    output: Dict[str, Any] = field(default_factory=dict)
    error_message: Optional[str] = None

    # Checkpoint data (for resumable handlers)
    checkpoint_phase: Optional[str] = None
    checkpoint_data: Optional[Dict[str, Any]] = None

    # Orchestration instructions (for advanced use)
    # e.g., dynamic node creation, fan-out
    orchestration_instructions: Optional[Dict[str, Any]] = None

    @classmethod
    def success_result(
        cls,
        output: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> "HandlerResult":
        """Create a success result."""
        return cls(success=True, output=output or {}, **kwargs)

    @classmethod
    def failure_result(
        cls,
        error_message: str,
        output: Optional[Dict[str, Any]] = None,
    ) -> "HandlerResult":
        """Create a failure result."""
        return cls(success=False, error_message=error_message, output=output or {})

    @classmethod
    def checkpoint_result(
        cls,
        phase: str,
        data: Dict[str, Any],
        output: Optional[Dict[str, Any]] = None,
    ) -> "HandlerResult":
        """Create a checkpoint result (partial progress)."""
        return cls(
            success=True,
            output=output or {},
            checkpoint_phase=phase,
            checkpoint_data=data,
        )


# Handler function type
HandlerFunc = Callable[[HandlerContext], Union[HandlerResult, Awaitable[HandlerResult]]]


# ============================================================================
# EXCEPTIONS
# ============================================================================

class HandlerError(Exception):
    """Base exception for handler errors."""
    pass


class HandlerNotFoundError(HandlerError):
    """Raised when a handler is not found in the registry."""
    def __init__(self, handler_name: str):
        self.handler_name = handler_name
        super().__init__(f"Handler not found: {handler_name}")


class DuplicateHandlerError(HandlerError):
    """Raised when a handler name is already registered."""
    def __init__(self, handler_name: str):
        self.handler_name = handler_name
        super().__init__(f"Handler already registered: {handler_name}")


# ============================================================================
# REGISTRY
# ============================================================================

# Global registry
_handlers: Dict[str, HandlerFunc] = {}
_handler_metadata: Dict[str, Dict[str, Any]] = {}


def register_handler(
    name: str,
    *,
    description: str = "",
    queue: str = "",
    timeout_seconds: int = 3600,
    max_retries: int = 3,
    tags: Optional[List[str]] = None,
) -> Callable[[HandlerFunc], HandlerFunc]:
    """
    Decorator to register a handler function.

    Args:
        name: Handler name (must be unique)
        description: Human-readable description
        queue: Default queue for this handler
        timeout_seconds: Default timeout
        max_retries: Default max retries
        tags: Optional tags for categorization

    Returns:
        Decorator function

    Example:
        @register_handler("validate_raster", queue="container-tasks")
        async def validate_raster(ctx: HandlerContext) -> HandlerResult:
            # implementation
            return HandlerResult.success_result({"crs": "EPSG:4326"})
    """
    def decorator(func: HandlerFunc) -> HandlerFunc:
        if name in _handlers:
            raise DuplicateHandlerError(name)

        _handlers[name] = func
        _handler_metadata[name] = {
            "name": name,
            "description": description,
            "queue": queue,
            "timeout_seconds": timeout_seconds,
            "max_retries": max_retries,
            "tags": tags or [],
            "function": func.__name__,
            "module": func.__module__,
            "is_async": asyncio.iscoroutinefunction(func),
            "registered_at": datetime.utcnow().isoformat(),
        }

        logger.debug(f"Registered handler: {name} ({func.__module__}.{func.__name__})")
        return func

    return decorator


def get_handler(name: str) -> Optional[HandlerFunc]:
    """
    Get a handler by name.

    Args:
        name: Handler name

    Returns:
        Handler function or None if not found
    """
    return _handlers.get(name)


def get_handler_or_raise(name: str) -> HandlerFunc:
    """
    Get a handler by name, raising if not found.

    Args:
        name: Handler name

    Returns:
        Handler function

    Raises:
        HandlerNotFoundError if handler not found
    """
    handler = _handlers.get(name)
    if handler is None:
        raise HandlerNotFoundError(name)
    return handler


def list_handlers() -> List[Dict[str, Any]]:
    """
    List all registered handlers with metadata.

    Returns:
        List of handler metadata dicts
    """
    return list(_handler_metadata.values())


def get_handler_metadata(name: str) -> Optional[Dict[str, Any]]:
    """
    Get metadata for a specific handler.

    Args:
        name: Handler name

    Returns:
        Metadata dict or None if not found
    """
    return _handler_metadata.get(name)


def clear_handlers() -> None:
    """
    Clear all registered handlers.

    Primarily for testing.
    """
    _handlers.clear()
    _handler_metadata.clear()
    logger.debug("Cleared all handlers")


def validate_handlers(workflow_handlers: List[str]) -> List[str]:
    """
    Validate that all handlers in a workflow are registered.

    Args:
        workflow_handlers: List of handler names from workflow

    Returns:
        List of missing handler names (empty if all valid)
    """
    missing = []
    for handler_name in workflow_handlers:
        if handler_name not in _handlers:
            missing.append(handler_name)
    return missing


# ============================================================================
# ASYNC HANDLER EXECUTION
# ============================================================================

async def execute_handler(
    name: str,
    context: HandlerContext,
) -> HandlerResult:
    """
    Execute a handler by name.

    Handles both sync and async handlers.

    Args:
        name: Handler name
        context: Execution context

    Returns:
        HandlerResult

    Raises:
        HandlerNotFoundError if handler not found
    """
    handler = get_handler_or_raise(name)

    try:
        if asyncio.iscoroutinefunction(handler):
            result = await handler(context)
        else:
            # Run sync handler in thread pool
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(None, handler, context)

        return result

    except Exception as e:
        logger.exception(f"Handler {name} failed: {e}")
        return HandlerResult.failure_result(str(e))


# ============================================================================
# EXPORTS
# ============================================================================

__all__ = [
    "register_handler",
    "get_handler",
    "get_handler_or_raise",
    "list_handlers",
    "get_handler_metadata",
    "clear_handlers",
    "validate_handlers",
    "execute_handler",
    "HandlerFunc",
    "HandlerContext",
    "HandlerResult",
    "HandlerError",
    "HandlerNotFoundError",
    "DuplicateHandlerError",
]
