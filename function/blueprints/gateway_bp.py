# ============================================================================
# GATEWAY BLUEPRINT
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Gateway - Job submission endpoints
# PURPOSE: HTTP endpoints for submitting jobs to the queue
# CREATED: 04 FEB 2026
# ============================================================================
"""
Gateway Blueprint

Job submission endpoints:
- POST /api/gateway/submit - Submit single job
- POST /api/gateway/submit/batch - Submit multiple jobs
- GET /api/gateway/health - Gateway health check
"""

import json
import logging
import os
from datetime import datetime, timezone

import azure.functions as func

from function.models.requests import JobSubmitRequest, BatchSubmitRequest
from function.models.responses import (
    JobSubmitResponse,
    BatchSubmitResponse,
    HealthResponse,
    ErrorResponse,
)

logger = logging.getLogger(__name__)
gateway_bp = func.Blueprint()


def _json_response(data: dict, status_code: int = 200) -> func.HttpResponse:
    """Create JSON HTTP response."""
    return func.HttpResponse(
        json.dumps(data, default=str),
        status_code=status_code,
        headers={"Content-Type": "application/json"},
    )


@gateway_bp.route(route="gateway/submit", methods=["POST"])
def gateway_submit(req: func.HttpRequest) -> func.HttpResponse:
    """
    Submit a job for execution.

    POST /api/gateway/submit
    Body: JobSubmitRequest JSON
    Returns: JobSubmitResponse (202 Accepted)

    The job is published to the dag-jobs Service Bus queue.
    An orchestrator instance will consume and process it.
    """
    # Parse request
    try:
        body = req.get_json()
        request = JobSubmitRequest.model_validate(body)
    except Exception as e:
        logger.warning(f"Invalid request: {e}")
        return _json_response(
            ErrorResponse(error="Invalid request", details=str(e)).model_dump(),
            status_code=400,
        )

    submitted_at = datetime.now(timezone.utc)
    queue_name = os.environ.get("JOB_QUEUE_NAME", "dag-jobs")

    # Import here to avoid startup dependency issues
    from core.models import JobQueueMessage
    from infrastructure.service_bus import ServiceBusPublisher

    # Create queue message
    queue_message = JobQueueMessage(
        workflow_id=request.workflow_id,
        input_params=request.input_params,
        correlation_id=request.correlation_id,
        callback_url=request.callback_url,
        priority=request.priority,
        idempotency_key=request.idempotency_key,
        submitted_by="rmhdagmaster-gateway",
        submitted_at=submitted_at,
    )

    try:
        publisher = ServiceBusPublisher.instance()
        message_id = publisher.send_message(
            queue_name=queue_name,
            message=queue_message,
            ttl_hours=24,
        )

        response = JobSubmitResponse(
            message_id=message_id,
            workflow_id=request.workflow_id,
            submitted_at=submitted_at,
            status="queued",
            correlation_id=request.correlation_id,
        )

        logger.info(
            f"Job submitted: workflow={request.workflow_id}, "
            f"message_id={message_id}, correlation_id={request.correlation_id}"
        )

        return _json_response(response.model_dump(), status_code=202)

    except RuntimeError as e:
        logger.error(f"Failed to submit job: {e}")
        return _json_response(
            ErrorResponse(error="Failed to queue job", details=str(e)).model_dump(),
            status_code=503,
        )
    except Exception as e:
        logger.exception(f"Unexpected error submitting job: {e}")
        return _json_response(
            ErrorResponse(error="Internal error", details=str(e)).model_dump(),
            status_code=500,
        )


@gateway_bp.route(route="gateway/submit/batch", methods=["POST"])
def gateway_submit_batch(req: func.HttpRequest) -> func.HttpResponse:
    """
    Submit multiple jobs in a batch.

    POST /api/gateway/submit/batch
    Body: BatchSubmitRequest JSON
    Returns: BatchSubmitResponse (202 Accepted)
    """
    # Parse request
    try:
        body = req.get_json()
        request = BatchSubmitRequest.model_validate(body)
    except Exception as e:
        logger.warning(f"Invalid batch request: {e}")
        return _json_response(
            ErrorResponse(error="Invalid request", details=str(e)).model_dump(),
            status_code=400,
        )

    submitted_at = datetime.now(timezone.utc)
    queue_name = os.environ.get("JOB_QUEUE_NAME", "dag-jobs")

    # Import here to avoid startup dependency issues
    from core.models import JobQueueMessage
    from infrastructure.service_bus import ServiceBusPublisher

    # Create queue messages
    queue_messages = [
        JobQueueMessage(
            workflow_id=job.workflow_id,
            input_params=job.input_params,
            correlation_id=job.correlation_id,
            callback_url=job.callback_url,
            priority=job.priority,
            idempotency_key=job.idempotency_key,
            submitted_by="rmhdagmaster-gateway",
            submitted_at=submitted_at,
        )
        for job in request.jobs
    ]

    try:
        publisher = ServiceBusPublisher.instance()
        result = publisher.batch_send_messages(
            queue_name=queue_name,
            messages=queue_messages,
        )

        response = BatchSubmitResponse(
            submitted=result.messages_sent,
            failed=len(queue_messages) - result.messages_sent,
            message_ids=[],  # Batch send doesn't return individual IDs
            errors=result.errors,
        )

        logger.info(f"Batch submitted: {result.messages_sent}/{len(queue_messages)} jobs")

        return _json_response(response.model_dump(), status_code=202)

    except Exception as e:
        logger.exception(f"Batch submission failed: {e}")
        return _json_response(
            ErrorResponse(error="Batch submission failed", details=str(e)).model_dump(),
            status_code=503,
        )


@gateway_bp.route(route="gateway/health", methods=["GET"])
def gateway_health(req: func.HttpRequest) -> func.HttpResponse:
    """
    Gateway health check.

    GET /api/gateway/health
    Returns: HealthResponse
    """
    from function.config import get_config

    config = get_config()

    response = HealthResponse(
        status="healthy",
        service=config.service_name,
        timestamp=datetime.now(timezone.utc),
        version=config.version,
        checks={
            "service_bus_configured": config.has_service_bus_config,
            "database_configured": config.has_database_config,
        },
    )

    return _json_response(response.model_dump())


__all__ = ["gateway_bp"]
