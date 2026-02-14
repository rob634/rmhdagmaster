# ============================================================================
# API ROUTES
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Core - FastAPI route definitions
# PURPOSE: HTTP endpoints for job management
# CREATED: 29 JAN 2026
# ============================================================================
"""
API Routes

FastAPI routes for the DAG orchestrator.
"""

import logging
from typing import Optional
from datetime import datetime

from fastapi import APIRouter, HTTPException, Depends, Query
from fastapi.responses import JSONResponse

from core.models import TaskResult
from core.contracts import JobStatus, TaskStatus
from .schemas import (
    JobCreate,
    JobResponse,
    JobDetailResponse,
    JobListResponse,
    NodeResponse,
    TaskResultCreate,
    CheckpointCreate,
    WorkflowResponse,
    WorkflowListResponse,
    ErrorResponse,
)

logger = logging.getLogger(__name__)

router = APIRouter()


# ============================================================================
# DEPENDENCY INJECTION
# ============================================================================
# These will be set by the main app at startup

_job_service = None
_node_service = None
_workflow_service = None
_orchestrator = None
_pool = None
_event_service = None
_checkpoint_service = None


def set_services(job_service, node_service, workflow_service, orchestrator, pool, event_service=None, checkpoint_service=None):
    """Set service instances for dependency injection."""
    global _job_service, _node_service, _workflow_service, _orchestrator, _pool, _event_service, _checkpoint_service
    _job_service = job_service
    _node_service = node_service
    _workflow_service = workflow_service
    _orchestrator = orchestrator
    _pool = pool
    _event_service = event_service
    _checkpoint_service = checkpoint_service


def get_event_service():
    if _event_service is None:
        raise HTTPException(500, "Event service not initialized")
    return _event_service


def get_job_service():
    if _job_service is None:
        raise HTTPException(500, "Services not initialized")
    return _job_service


def get_node_service():
    if _node_service is None:
        raise HTTPException(500, "Services not initialized")
    return _node_service


def get_workflow_service():
    if _workflow_service is None:
        raise HTTPException(500, "Services not initialized")
    return _workflow_service


# ============================================================================
# HEALTH
# ============================================================================
# NOTE: Health checks are now handled by the health module.
# See: /livez, /readyz, /health (root level, not under /api/v1)
# The health module provides plugin-based checks with parallel execution.
# ============================================================================


# ============================================================================
# ORCHESTRATOR STATUS
# ============================================================================

@router.get("/orchestrator/status", tags=["Orchestrator"])
async def get_orchestrator_status():
    """
    Get orchestrator status and statistics.

    Returns metrics about the orchestration loop including:
    - Running state
    - Uptime
    - Cycle count
    - Tasks dispatched/processed
    - Active jobs
    - Error count
    """
    if _orchestrator is None:
        raise HTTPException(500, "Orchestrator not initialized")

    stats = _orchestrator.stats

    return {
        "status": "running" if stats["running"] else "stopped",
        "started_at": stats["started_at"],
        "uptime_seconds": stats["uptime_seconds"],
        "poll_interval_seconds": stats["poll_interval"],
        "metrics": {
            "cycles": stats["cycles"],
            "last_cycle_at": stats["last_cycle_at"],
            "active_jobs": stats["active_jobs"],
            "tasks_dispatched": stats["tasks_dispatched"],
            "results_processed": stats["results_processed"],
            "errors": stats["errors"],
        },
    }


# ============================================================================
# WORKFLOWS
# ============================================================================

@router.get("/workflows", response_model=WorkflowListResponse, tags=["Workflows"])
async def list_workflows():
    """
    List available workflow definitions.
    """
    service = get_workflow_service()
    workflows = service.list_all()

    return WorkflowListResponse(
        workflows=[
            WorkflowResponse(
                workflow_id=w.workflow_id,
                name=w.name,
                version=w.version,
                description=w.description,
                is_active=w.is_active,
            )
            for w in workflows
        ]
    )


@router.get("/workflows/{workflow_id}", response_model=WorkflowResponse, tags=["Workflows"])
async def get_workflow(workflow_id: str):
    """
    Get a workflow definition.
    """
    service = get_workflow_service()
    workflow = service.get(workflow_id)

    if workflow is None:
        raise HTTPException(404, f"Workflow not found: {workflow_id}")

    return WorkflowResponse(
        workflow_id=workflow.workflow_id,
        name=workflow.name,
        version=workflow.version,
        description=workflow.description,
        is_active=workflow.is_active,
    )


# ============================================================================
# JOBS
# ============================================================================

@router.post(
    "/jobs",
    response_model=JobResponse,
    status_code=201,
    tags=["Jobs"],
    responses={
        201: {"description": "Job created"},
        400: {"model": ErrorResponse, "description": "Invalid request"},
        404: {"model": ErrorResponse, "description": "Workflow not found"},
    },
)
async def create_job(request: JobCreate):
    """
    Create a new job.

    Submits a workflow for execution. Returns immediately with job ID.
    Poll GET /jobs/{job_id} to monitor progress.
    """
    service = get_job_service()

    try:
        job = await service.create_job(
            workflow_id=request.workflow_id,
            input_params=request.input_params,
            correlation_id=request.correlation_id,
            idempotency_key=request.idempotency_key,
        )

        logger.info(f"Created job {job.job_id} for workflow {request.workflow_id}")

        return JobResponse(
            job_id=job.job_id,
            workflow_id=job.workflow_id,
            status=job.status,
            input_params=job.input_params,
            result_data=job.result_data,
            error_message=job.error_message,
            created_at=job.created_at,
            started_at=job.started_at,
            completed_at=job.completed_at,
            submitted_by=job.submitted_by,
            correlation_id=job.correlation_id,
            owner_id=job.owner_id,
            owner_heartbeat_at=job.owner_heartbeat_at,
        )

    except KeyError as e:
        raise HTTPException(404, f"Workflow not found: {request.workflow_id}")
    except Exception as e:
        logger.exception(f"Error creating job: {e}")
        raise HTTPException(500, str(e))


@router.get("/jobs", response_model=JobListResponse, tags=["Jobs"])
async def list_jobs(
    status: Optional[JobStatus] = Query(None, description="Filter by status"),
    limit: int = Query(100, ge=1, le=1000),
):
    """
    List jobs.

    Optionally filter by status.
    """
    service = get_job_service()

    if status:
        from repositories import JobRepository
        repo = JobRepository(_pool)
        jobs = await repo.list_by_status(status, limit)
    else:
        jobs = await service.list_active_jobs(limit)

    return JobListResponse(
        jobs=[
            JobResponse(
                job_id=j.job_id,
                workflow_id=j.workflow_id,
                status=j.status,
                input_params=j.input_params,
                result_data=j.result_data,
                error_message=j.error_message,
                created_at=j.created_at,
                started_at=j.started_at,
                completed_at=j.completed_at,
                submitted_by=j.submitted_by,
                correlation_id=j.correlation_id,
                owner_id=j.owner_id,
                owner_heartbeat_at=j.owner_heartbeat_at,
            )
            for j in jobs
        ],
        total=len(jobs),
    )


@router.get(
    "/jobs/{job_id}",
    response_model=JobDetailResponse,
    tags=["Jobs"],
    responses={404: {"model": ErrorResponse}},
)
async def get_job(job_id: str):
    """
    Get job details with node states.
    """
    service = get_job_service()
    result = await service.get_job_with_nodes(job_id)

    if result is None:
        raise HTTPException(404, f"Job not found: {job_id}")

    job = result["job"]
    nodes = result["nodes"]

    # Calculate node summary
    node_summary = {}
    for node in nodes:
        status = node.status.value
        node_summary[status] = node_summary.get(status, 0) + 1

    return JobDetailResponse(
        job=JobResponse(
            job_id=job.job_id,
            workflow_id=job.workflow_id,
            status=job.status,
            input_params=job.input_params,
            result_data=job.result_data,
            error_message=job.error_message,
            created_at=job.created_at,
            started_at=job.started_at,
            completed_at=job.completed_at,
            submitted_by=job.submitted_by,
            correlation_id=job.correlation_id,
            owner_id=job.owner_id,
            owner_heartbeat_at=job.owner_heartbeat_at,
        ),
        nodes=[
            NodeResponse(
                node_id=n.node_id,
                job_id=n.job_id,
                status=n.status,
                task_id=n.task_id,
                output=n.output,
                error_message=n.error_message,
                retry_count=n.retry_count,
                created_at=n.created_at,
                started_at=n.started_at,
                completed_at=n.completed_at,
            )
            for n in nodes
        ],
        node_summary=node_summary,
    )


@router.post(
    "/jobs/{job_id}/cancel",
    response_model=JobResponse,
    tags=["Jobs"],
    responses={404: {"model": ErrorResponse}},
)
async def cancel_job(job_id: str):
    """
    Cancel a running job.
    """
    service = get_job_service()

    success = await service.cancel_job(job_id)
    if not success:
        job = await service.get_job(job_id)
        if job is None:
            raise HTTPException(404, f"Job not found: {job_id}")
        raise HTTPException(400, f"Cannot cancel job in status: {job.status.value}")

    job = await service.get_job(job_id)
    return JobResponse(
        job_id=job.job_id,
        workflow_id=job.workflow_id,
        status=job.status,
        input_params=job.input_params,
        result_data=job.result_data,
        error_message=job.error_message,
        created_at=job.created_at,
        started_at=job.started_at,
        completed_at=job.completed_at,
        submitted_by=job.submitted_by,
        correlation_id=job.correlation_id,
        owner_id=job.owner_id,
        owner_heartbeat_at=job.owner_heartbeat_at,
    )


# ============================================================================
# TIMELINE / EVENTS
# ============================================================================

@router.get(
    "/jobs/{job_id}/timeline",
    tags=["Jobs"],
    responses={404: {"model": ErrorResponse}},
)
async def get_job_timeline(
    job_id: str,
    limit: int = Query(100, ge=1, le=1000, description="Maximum events to return"),
):
    """
    Get chronological event timeline for a job.

    Returns events in chronological order (oldest first), useful for
    debugging and understanding job execution flow.
    """
    service = get_job_service()

    # Check job exists
    job = await service.get_job(job_id)
    if job is None:
        raise HTTPException(404, f"Job not found: {job_id}")

    # Get timeline
    event_service = get_event_service()
    events = await event_service.get_job_timeline(job_id, limit)

    return {
        "job_id": job_id,
        "event_count": len(events),
        "events": [
            {
                "event_id": e.event_id,
                "event_type": e.event_type.value,
                "event_status": e.event_status.value,
                "node_id": e.node_id,
                "task_id": e.task_id,
                "checkpoint_name": e.checkpoint_name,
                "event_data": e.event_data,
                "error_message": e.error_message,
                "duration_ms": e.duration_ms,
                "created_at": e.created_at.isoformat() if e.created_at else None,
                "source_app": e.source_app,
            }
            for e in events
        ],
    }


@router.get(
    "/jobs/{job_id}/nodes/{node_id}/events",
    tags=["Jobs"],
    responses={404: {"model": ErrorResponse}},
)
async def get_node_events(
    job_id: str,
    node_id: str,
    limit: int = Query(50, ge=1, le=500),
):
    """
    Get events for a specific node.

    Useful for debugging a single node's execution.
    """
    service = get_job_service()

    # Check job exists
    job = await service.get_job(job_id)
    if job is None:
        raise HTTPException(404, f"Job not found: {job_id}")

    # Get node events
    event_service = get_event_service()
    events = await event_service.get_node_events(job_id, node_id, limit)

    return {
        "job_id": job_id,
        "node_id": node_id,
        "event_count": len(events),
        "events": [
            {
                "event_id": e.event_id,
                "event_type": e.event_type.value,
                "event_status": e.event_status.value,
                "task_id": e.task_id,
                "event_data": e.event_data,
                "error_message": e.error_message,
                "duration_ms": e.duration_ms,
                "created_at": e.created_at.isoformat() if e.created_at else None,
            }
            for e in events
        ],
    }


# ============================================================================
# TASK CALLBACKS
# ============================================================================

@router.post(
    "/callbacks/task-result",
    status_code=202,
    tags=["Callbacks"],
    responses={202: {"description": "Result accepted"}},
)
async def receive_task_result(request: TaskResultCreate):
    """
    Receive task result from worker.

    Workers call this endpoint when a task completes.
    """
    # Create TaskResult and persist
    from repositories import TaskResultRepository

    result = TaskResult(
        task_id=request.task_id,
        job_id=request.job_id,
        node_id=request.node_id,
        status=request.status,
        output=request.output,
        error_message=request.error_message,
        worker_id=request.worker_id,
        execution_duration_ms=request.execution_duration_ms,
        reported_at=datetime.utcnow(),
    )

    repo = TaskResultRepository(_pool)
    await repo.create(result)

    logger.info(
        f"Received task result: {request.task_id} "
        f"status={request.status.value}"
    )

    return {"status": "accepted", "task_id": request.task_id}


@router.post(
    "/callbacks/task-progress",
    status_code=202,
    tags=["Callbacks"],
    responses={202: {"description": "Progress accepted"}},
)
async def receive_task_progress(request: dict):
    """
    Receive task progress update from worker.

    Workers call this endpoint to report progress during execution.
    """
    from repositories import TaskResultRepository
    from core.contracts import TaskStatus

    # Update or insert progress
    result = TaskResult(
        task_id=request["task_id"],
        job_id=request["job_id"],
        node_id=request["node_id"],
        status=TaskStatus.RUNNING,
        progress_current=request.get("progress_current"),
        progress_total=request.get("progress_total"),
        progress_percent=request.get("progress_percent"),
        progress_message=request.get("progress_message"),
        reported_at=datetime.utcnow(),
    )

    repo = TaskResultRepository(_pool)
    await repo.create(result)

    logger.debug(
        f"Received progress: {request['task_id']} "
        f"{request.get('progress_percent', 0):.1f}%"
    )

    return {"status": "accepted", "task_id": request["task_id"]}


@router.post(
    "/callbacks/checkpoint",
    status_code=202,
    tags=["Callbacks"],
    responses={202: {"description": "Checkpoint accepted"}},
)
async def receive_checkpoint(request: CheckpointCreate):
    """
    Receive checkpoint from worker.

    Workers call this endpoint to save progress checkpoints during
    long-running task execution. Checkpoints enable resumption on retry.
    """
    from services import CheckpointService

    # Use injected service if available, otherwise create one
    if _checkpoint_service:
        service = _checkpoint_service
    else:
        service = CheckpointService(_pool)

    checkpoint = await service.save_checkpoint(
        job_id=request.job_id,
        node_id=request.node_id,
        task_id=request.task_id,
        phase_name=request.phase_name,
        phase_index=request.phase_index,
        total_phases=request.total_phases,
        progress_current=request.progress_current,
        progress_total=request.progress_total,
        progress_message=request.progress_message,
        state_data=request.state_data,
        artifacts_completed=request.artifacts_completed,
        memory_usage_mb=request.memory_usage_mb,
        disk_usage_mb=request.disk_usage_mb,
        error_count=request.error_count,
        is_final=request.is_final,
    )

    logger.info(
        f"Received checkpoint: {request.task_id} "
        f"phase={request.phase_name}[{request.phase_index}] "
        f"progress={request.progress_current}/{request.progress_total or '?'}"
    )

    return {
        "status": "accepted",
        "checkpoint_id": checkpoint.checkpoint_id,
        "task_id": request.task_id,
    }


# ============================================================================
# HANDLER TESTING (Development)
# ============================================================================

@router.post(
    "/test/handler/{handler_name}",
    tags=["Testing"],
    responses={
        200: {"description": "Handler executed successfully"},
        400: {"model": ErrorResponse, "description": "Handler failed"},
        404: {"model": ErrorResponse, "description": "Handler not found"},
        403: {"model": ErrorResponse, "description": "Testing disabled"},
    },
)
async def test_handler(handler_name: str, request: dict):
    """
    Test a handler directly without running a full workflow.

    Development/debugging endpoint - invoke a handler with params
    and see the result immediately.

    Enable with: DAG_BRAIN_ENABLE_HANDLER_TESTING=true

    Example:
        POST /api/v1/test/handler/raster.validate
        {
            "params": {
                "local_path": "/mnt/data/test.tif"
            }
        }
    """
    import os
    from handlers import get_handler, HandlerContext, HandlerNotFoundError

    # Check if testing is enabled
    if os.environ.get("DAG_BRAIN_ENABLE_HANDLER_TESTING", "").lower() != "true":
        raise HTTPException(
            403,
            "Handler testing disabled. Set DAG_BRAIN_ENABLE_HANDLER_TESTING=true to enable."
        )

    # Get handler
    try:
        handler = get_handler(handler_name)
    except HandlerNotFoundError:
        raise HTTPException(404, f"Handler not found: {handler_name}")

    # Build context
    import uuid
    test_id = str(uuid.uuid4())[:8]

    context = HandlerContext(
        task_id=f"test-{test_id}",
        job_id=f"test-job-{test_id}",
        node_id=f"test-node-{test_id}",
        handler=handler_name,
        params=request.get("params", {}),
        timeout_seconds=request.get("timeout_seconds", 300),
        retry_count=0,
        correlation_id=f"test-{test_id}",
    )

    # Execute handler
    try:
        import time
        start = time.monotonic()

        result = await handler(context)

        duration_ms = (time.monotonic() - start) * 1000

        return {
            "success": result.success,
            "handler": handler_name,
            "output": result.output,
            "error_message": result.error_message,
            "duration_ms": round(duration_ms, 2),
            "test_id": test_id,
        }

    except Exception as e:
        logger.exception(f"Handler test failed: {handler_name}")
        raise HTTPException(400, f"Handler failed: {str(e)}")


@router.get("/test/handlers", tags=["Testing"])
async def list_testable_handlers():
    """
    List all handlers available for testing.
    """
    import os
    from handlers import list_handlers

    testing_enabled = os.environ.get("DAG_BRAIN_ENABLE_HANDLER_TESTING", "").lower() == "true"

    handlers = list_handlers()

    return {
        "testing_enabled": testing_enabled,
        "handlers": [
            {
                "name": h.get("name"),
                "queue": h.get("queue"),
                "timeout_seconds": h.get("timeout_seconds"),
            }
            for h in handlers
        ],
        "hint": "POST /api/v1/test/handler/{name} with {\"params\": {...}}" if testing_enabled else "Set DAG_BRAIN_ENABLE_HANDLER_TESTING=true to enable",
    }
