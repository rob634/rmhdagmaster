# ============================================================================
# EXAMPLE HANDLERS
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Examples - Sample handler implementations
# PURPOSE: Demonstrate handler registration and implementation patterns
# CREATED: 31 JAN 2026
# ============================================================================
"""
Example Handlers

Sample implementations showing how to create handlers.
These can be used for testing and as templates for real handlers.
"""

import asyncio
import logging
from typing import Any, Dict

from handlers.registry import (
    register_handler,
    HandlerContext,
    HandlerResult,
)

logger = logging.getLogger(__name__)


# ============================================================================
# BASIC HANDLERS
# ============================================================================

@register_handler(
    "echo",
    description="Echoes input parameters back as output",
    queue="functionapp-tasks",
    timeout_seconds=60,
)
async def echo_handler(ctx: HandlerContext) -> HandlerResult:
    """
    Simple echo handler for testing.

    Returns the input params as output.
    """
    logger.info(f"Echo handler called with params: {ctx.params}")

    return HandlerResult.success_result(
        output={
            "echoed_params": ctx.params,
            "handler": ctx.handler,
            "task_id": ctx.task_id,
        }
    )


@register_handler(
    "hello_world",
    description="Returns a hello world message",
    queue="functionapp-tasks",
    timeout_seconds=30,
)
async def hello_world_handler(ctx: HandlerContext) -> HandlerResult:
    """
    Hello world handler for testing.

    Returns a greeting message.
    """
    name = ctx.params.get("name", "World")
    message = f"Hello, {name}!"

    return HandlerResult.success_result(
        output={"message": message}
    )


@register_handler(
    "sleep",
    description="Sleeps for specified duration (for testing)",
    queue="functionapp-tasks",
    timeout_seconds=300,
)
async def sleep_handler(ctx: HandlerContext) -> HandlerResult:
    """
    Sleep handler for testing delays and timeouts.

    Params:
        duration_seconds: How long to sleep (default 1)
    """
    duration = ctx.params.get("duration_seconds", 1)
    logger.info(f"Sleeping for {duration} seconds")

    await asyncio.sleep(duration)

    return HandlerResult.success_result(
        output={"slept_for": duration}
    )


@register_handler(
    "fail",
    description="Always fails (for testing error handling)",
    queue="functionapp-tasks",
    timeout_seconds=30,
)
async def fail_handler(ctx: HandlerContext) -> HandlerResult:
    """
    Handler that always fails.

    Used for testing error handling and retry logic.
    """
    error_message = ctx.params.get("error_message", "Intentional failure for testing")
    return HandlerResult.failure_result(error_message)


# ============================================================================
# CHECKPOINT EXAMPLE
# ============================================================================

@register_handler(
    "multi_phase",
    description="Multi-phase handler demonstrating checkpoints",
    queue="container-tasks",
    timeout_seconds=600,
)
async def multi_phase_handler(ctx: HandlerContext) -> HandlerResult:
    """
    Multi-phase handler demonstrating checkpoint usage.

    Simulates a long-running task with multiple phases.
    """
    # Check if resuming from checkpoint
    start_phase = 1
    if ctx.checkpoint_phase:
        logger.info(f"Resuming from checkpoint: {ctx.checkpoint_phase}")
        start_phase = int(ctx.checkpoint_phase.split("_")[1]) + 1

    results = ctx.checkpoint_data.get("results", []) if ctx.checkpoint_data else []

    # Simulate multiple phases
    total_phases = ctx.params.get("phases", 3)

    for phase in range(start_phase, total_phases + 1):
        logger.info(f"Executing phase {phase}/{total_phases}")

        # Report progress
        if ctx.progress_callback:
            ctx.report_progress(phase, total_phases, f"Phase {phase}")

        # Simulate work
        await asyncio.sleep(0.5)

        # Store result
        results.append(f"phase_{phase}_complete")

        # If not last phase, return checkpoint
        if phase < total_phases:
            return HandlerResult.checkpoint_result(
                phase=f"phase_{phase}",
                data={"results": results},
                output={"current_phase": phase},
            )

    # All phases complete
    return HandlerResult.success_result(
        output={
            "phases_completed": total_phases,
            "results": results,
        }
    )


# ============================================================================
# CONDITIONAL EXAMPLE
# ============================================================================

@register_handler(
    "size_check",
    description="Checks input size and returns routing info",
    queue="functionapp-tasks",
    timeout_seconds=60,
)
async def size_check_handler(ctx: HandlerContext) -> HandlerResult:
    """
    Handler that checks size and returns routing info.

    Used for conditional routing based on size.
    """
    size_bytes = ctx.params.get("size_bytes", 0)

    # Determine route based on size
    if size_bytes < 100 * 1024 * 1024:  # < 100MB
        route = "light"
        queue = "functionapp-tasks"
    elif size_bytes < 1024 * 1024 * 1024:  # < 1GB
        route = "medium"
        queue = "container-tasks"
    else:
        route = "heavy"
        queue = "container-tasks-heavy"

    return HandlerResult.success_result(
        output={
            "size_bytes": size_bytes,
            "route": route,
            "recommended_queue": queue,
        }
    )


# ============================================================================
# FAN-OUT EXAMPLE
# ============================================================================

@register_handler(
    "chunk_processor",
    description="Processes a single chunk (used with fan-out)",
    queue="functionapp-tasks",
    timeout_seconds=300,
)
async def chunk_processor_handler(ctx: HandlerContext) -> HandlerResult:
    """
    Processes a single chunk.

    Designed to be called from a fan-out node.
    """
    chunk_id = ctx.params.get("chunk_id", "unknown")
    chunk_data = ctx.params.get("chunk_data", {})

    logger.info(f"Processing chunk {chunk_id}")

    # Simulate processing
    await asyncio.sleep(0.1)

    return HandlerResult.success_result(
        output={
            "chunk_id": chunk_id,
            "processed": True,
            "item_count": len(chunk_data) if isinstance(chunk_data, (list, dict)) else 1,
        }
    )


# ============================================================================
# EXPORTS
# ============================================================================

__all__ = [
    "echo_handler",
    "hello_world_handler",
    "sleep_handler",
    "fail_handler",
    "multi_phase_handler",
    "size_check_handler",
    "chunk_processor_handler",
]
