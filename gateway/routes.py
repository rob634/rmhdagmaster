# ============================================================================
# GATEWAY ROUTES
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Gateway - FastAPI routes for job submission
# PURPOSE: HTTP endpoints for rmhgeogateway Function App
# CREATED: 04 FEB 2026
# ============================================================================
"""
Gateway Routes

FastAPI router for the job submission gateway.
Provides endpoints for:
- POST /submit - Submit a new job
- GET /health - Health check

The gateway publishes JobQueueMessage to the dag-jobs Service Bus queue.
Orchestrator instances consume messages and create jobs.
"""

import logging
import os
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException, status

from core.models import JobQueueMessage
from infrastructure.service_bus import ServiceBusConfig, ServiceBusPublisher
from gateway.models import (
    JobSubmitRequest,
    JobSubmitResponse,
    HealthResponse,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/gateway", tags=["Gateway"])

# Publisher singleton (lazy initialization)
_publisher: Optional[ServiceBusPublisher] = None


def get_publisher() -> ServiceBusPublisher:
    """
    Get or create the Service Bus publisher singleton.

    Uses lazy initialization to avoid startup failures if Service Bus
    is not configured (e.g., in local development without queue).
    """
    global _publisher
    if _publisher is None:
        try:
            _publisher = ServiceBusPublisher.instance()
            logger.info("ServiceBusPublisher initialized for gateway")
        except Exception as e:
            logger.error(f"Failed to initialize ServiceBusPublisher: {e}")
            raise
    return _publisher


@router.post(
    "/submit",
    response_model=JobSubmitResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Submit a job for execution",
    description="""
    Submit a new job for asynchronous execution.

    The job is queued for processing by an orchestrator instance.
    Returns immediately with a message_id for tracking.

    **Note**: The actual job_id is created by the orchestrator when it
    claims the message from the queue. Use correlation_id or idempotency_key
    for tracking if you need to query job status later.
    """,
)
async def submit_job(request: JobSubmitRequest) -> JobSubmitResponse:
    """
    Submit a job for execution.

    Flow:
    1. Validate request
    2. Create JobQueueMessage
    3. Publish to dag-jobs Service Bus queue
    4. Return message_id for tracking

    The orchestrator will:
    1. Consume the message
    2. Validate workflow exists
    3. Create Job with owner_id
    4. Process the job
    """
    submitted_at = datetime.utcnow()

    # Create internal queue message
    queue_message = JobQueueMessage(
        workflow_id=request.workflow_id,
        input_params=request.input_params,
        correlation_id=request.correlation_id,
        callback_url=request.callback_url,
        priority=request.priority,
        idempotency_key=request.idempotency_key,
        submitted_by="rmhgeogateway",
        submitted_at=submitted_at,
    )

    # Get queue name from environment
    queue_name = os.environ.get("JOB_QUEUE_NAME")
    if not queue_name:
        raise HTTPException(status_code=503, detail="JOB_QUEUE_NAME not configured")

    # Publish to Service Bus
    try:
        publisher = get_publisher()
        message_id = publisher.send_message(
            queue_name=queue_name,
            message=queue_message,
            ttl_hours=24,
        )

        logger.info(
            f"Job submitted: workflow={request.workflow_id}, "
            f"message_id={message_id}, correlation_id={request.correlation_id}"
        )

        return JobSubmitResponse(
            message_id=message_id,
            workflow_id=request.workflow_id,
            submitted_at=submitted_at,
            status="queued",
            correlation_id=request.correlation_id,
        )

    except RuntimeError as e:
        # Service Bus errors (queue not found, auth failed, etc.)
        logger.error(f"Failed to submit job: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Failed to queue job: {str(e)}",
        )
    except Exception as e:
        logger.exception(f"Unexpected error submitting job: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal error submitting job",
        )


@router.get(
    "/health",
    response_model=HealthResponse,
    summary="Health check",
    description="Check if the gateway is healthy and can accept requests.",
)
async def health_check() -> HealthResponse:
    """
    Health check endpoint.

    Returns basic health information. Does not check Service Bus connectivity
    (that would add latency to health checks used by load balancers).
    """
    return HealthResponse(
        status="healthy",
        service="rmhgeogateway",
        timestamp=datetime.utcnow(),
        version="0.9.0",
    )


# ============================================================================
# OPTIONAL: Batch submission endpoint
# ============================================================================

from typing import List
from pydantic import BaseModel, Field


class BatchSubmitRequest(BaseModel):
    """Request to submit multiple jobs at once."""

    jobs: List[JobSubmitRequest] = Field(
        ...,
        min_length=1,
        max_length=100,
        description="List of jobs to submit (max 100)",
    )


class BatchSubmitResponse(BaseModel):
    """Response for batch job submission."""

    submitted: int = Field(..., description="Number of jobs successfully queued")
    failed: int = Field(..., description="Number of jobs that failed to queue")
    message_ids: List[str] = Field(..., description="Message IDs of submitted jobs")
    errors: List[str] = Field(default_factory=list, description="Error messages for failed jobs")


@router.post(
    "/submit/batch",
    response_model=BatchSubmitResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Submit multiple jobs",
    description="Submit multiple jobs in a single request (max 100).",
)
async def submit_jobs_batch(request: BatchSubmitRequest) -> BatchSubmitResponse:
    """
    Submit multiple jobs in a batch.

    More efficient than multiple single submissions for bulk operations.
    Uses batch send to Service Bus for better throughput.
    """
    submitted_at = datetime.utcnow()
    queue_name = os.environ.get("JOB_QUEUE_NAME")
    if not queue_name:
        raise HTTPException(status_code=503, detail="JOB_QUEUE_NAME not configured")

    # Create queue messages
    queue_messages = []
    for job_request in request.jobs:
        queue_messages.append(
            JobQueueMessage(
                workflow_id=job_request.workflow_id,
                input_params=job_request.input_params,
                correlation_id=job_request.correlation_id,
                callback_url=job_request.callback_url,
                priority=job_request.priority,
                idempotency_key=job_request.idempotency_key,
                submitted_by="rmhgeogateway",
                submitted_at=submitted_at,
            )
        )

    # Batch send
    try:
        publisher = get_publisher()
        result = publisher.batch_send_messages(
            queue_name=queue_name,
            messages=queue_messages,
        )

        logger.info(
            f"Batch submitted: {result.messages_sent}/{len(queue_messages)} jobs, "
            f"elapsed={result.elapsed_ms:.2f}ms"
        )

        return BatchSubmitResponse(
            submitted=result.messages_sent,
            failed=len(queue_messages) - result.messages_sent,
            message_ids=[],  # Batch send doesn't return individual IDs
            errors=result.errors,
        )

    except Exception as e:
        logger.exception(f"Batch submission failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Batch submission failed: {str(e)}",
        )


# ============================================================================
# EXPORTS
# ============================================================================

__all__ = ["router"]
