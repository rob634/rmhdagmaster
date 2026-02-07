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
import random
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
    description="Echoes input parameters back as output (supports checkpointing)",
    queue="functionapp-tasks",
    timeout_seconds=60,
)
async def echo_handler(ctx: HandlerContext) -> HandlerResult:
    """
    Simple echo handler for testing.

    Returns the input params as output.

    Supports checkpointing:
    - If params.steps > 1, simulates multi-step execution with checkpoints
    - On retry, resumes from last checkpoint
    """
    logger.info(f"Echo handler called with params: {ctx.params}")

    # Check if multi-step mode is requested (cast to int as YAML templates return strings)
    total_steps = int(ctx.params.get("steps", 1))

    if total_steps <= 1:
        # Simple mode: just echo params
        return HandlerResult.success_result(
            output={
                "echoed_params": ctx.params,
                "handler": ctx.handler,
                "task_id": ctx.task_id,
            }
        )

    # Multi-step mode with checkpointing
    start_step = 1
    completed_steps = []

    # Resume from checkpoint if available
    if ctx.checkpoint_data:
        start_step = ctx.checkpoint_data.get("next_step", 1)
        completed_steps = ctx.checkpoint_data.get("completed_steps", [])
        logger.info(f"Resuming from checkpoint: step {start_step}, completed={completed_steps}")

    # Process steps
    for step in range(start_step, total_steps + 1):
        logger.info(f"Processing step {step}/{total_steps}")

        # Report progress
        ctx.report_progress(step, total_steps, f"Step {step} of {total_steps}")

        # Simulate work
        step_delay = float(ctx.params.get("step_delay", 0.1))
        await asyncio.sleep(step_delay)

        # Record completion
        completed_steps.append(f"step_{step}")

        # Return checkpoint after each step except the last
        if step < total_steps:
            return HandlerResult.checkpoint_result(
                phase=f"step_{step}",
                data={
                    "next_step": step + 1,
                    "completed_steps": completed_steps,
                },
                output={
                    "current_step": step,
                    "total_steps": total_steps,
                    "status": "in_progress",
                },
            )

    # All steps complete
    return HandlerResult.success_result(
        output={
            "echoed_params": ctx.params,
            "handler": ctx.handler,
            "task_id": ctx.task_id,
            "steps_completed": total_steps,
            "completed_steps": completed_steps,
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


@register_handler(
    "flaky_echo",
    description="Echo handler with configurable failure rate (for testing retries)",
    timeout_seconds=60,
)
async def flaky_echo_handler(ctx: HandlerContext) -> HandlerResult:
    """
    Echo handler that fails randomly at a configurable rate.

    Used for testing retry logic. With failure_rate=0.2 and max_retries=5,
    probability of all 6 attempts failing = 0.2^6 = 0.000064 (near zero).

    Params:
        failure_rate: Probability of failure per attempt (0.0-1.0, default 0.2)
    """
    failure_rate = float(ctx.params.get("failure_rate", 0.2))

    if random.random() < failure_rate:
        logger.warning(
            f"Flaky echo: random failure (rate={failure_rate}, "
            f"attempt={ctx.retry_count})"
        )
        return HandlerResult.failure_result(
            f"Random failure (rate={failure_rate}, attempt={ctx.retry_count})"
        )

    logger.info(
        f"Flaky echo: success (rate={failure_rate}, attempt={ctx.retry_count})"
    )
    return HandlerResult.success_result(
        output={
            "echoed_params": ctx.params,
            "retry_count": ctx.retry_count,
            "task_id": ctx.task_id,
            "handler": ctx.handler,
        }
    )


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
    "flaky_echo_handler",
    "multi_phase_handler",
    "size_check_handler",
    "chunk_processor_handler",
]
