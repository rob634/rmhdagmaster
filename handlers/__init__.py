# ============================================================================
# HANDLER REGISTRY
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Core - Handler registration and lookup
# PURPOSE: Register and discover task handlers
# CREATED: 31 JAN 2026
# ============================================================================
"""
Handler Registry

Provides a decorator-based registration system for task handlers.

Usage:
    from handlers import register_handler, get_handler

    @register_handler("my_handler")
    async def my_handler(context: HandlerContext) -> HandlerResult:
        # Do work
        return HandlerResult(output={"key": "value"})

    # Later, to execute:
    handler = get_handler("my_handler")
    result = await handler(context)
"""

from handlers.registry import (
    register_handler,
    get_handler,
    get_handler_or_raise,
    list_handlers,
    clear_handlers,
    validate_handlers,
    HandlerFunc,
    HandlerContext,
    HandlerResult,
    HandlerError,
    HandlerNotFoundError,
    DuplicateHandlerError,
)

# Import handler modules to trigger registration
# Each module uses @register_handler decorator which adds handlers to registry
import handlers.raster  # noqa: F401 - import for side effects
import handlers.examples  # noqa: F401 - import for side effects (echo, hello_world, etc.)

__all__ = [
    "register_handler",
    "get_handler",
    "get_handler_or_raise",
    "list_handlers",
    "clear_handlers",
    "validate_handlers",
    "HandlerFunc",
    "HandlerContext",
    "HandlerResult",
    "HandlerError",
    "HandlerNotFoundError",
    "DuplicateHandlerError",
]
