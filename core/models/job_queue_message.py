# ============================================================================
# CLAUDE CONTEXT - JOB QUEUE MESSAGE MODEL
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Core model - Service Bus message for job submissions
# PURPOSE: Pydantic model for dag-jobs queue messages
# CREATED: 03 FEB 2026
# EXPORTS: JobQueueMessage
# DEPENDENCIES: pydantic
# ============================================================================
"""
Job Queue Message Model

Pydantic model for messages on the dag-jobs Service Bus queue.
This is the "work order" submitted by the Gateway and consumed by orchestrators.

Key Design:
- NOT the Job model - this is the submission request
- Gateway creates this message
- Orchestrator consumes and creates Job with ownership
- Explicit serialization via model_dump_json() / model_validate_json()
"""

from datetime import datetime
from typing import Any, Dict, Optional
from pydantic import BaseModel, Field
import uuid


class JobQueueMessage(BaseModel):
    """
    Message format for the dag-jobs Service Bus queue.

    Lifecycle:
        1. Gateway receives submission request
        2. Gateway creates JobQueueMessage with workflow_id + params
        3. Gateway publishes to dag-jobs queue
        4. Orchestrator consumes message (competing consumers)
        5. Orchestrator creates Job with owner_id = self
        6. Orchestrator completes message (removes from queue)

    This decouples job submission from job creation, enabling:
        - Multiple orchestrator instances
        - At-least-once delivery guarantees
        - Idempotency via idempotency_key
    """

    # Required fields
    workflow_id: str = Field(
        ...,
        max_length=64,
        description="ID of workflow to execute"
    )
    input_params: Dict[str, Any] = Field(
        default_factory=dict,
        description="Parameters passed to workflow execution"
    )

    # Optional tracking
    correlation_id: Optional[str] = Field(
        default=None,
        max_length=64,
        description="External correlation ID for tracing across systems"
    )
    submitted_by: Optional[str] = Field(
        default=None,
        max_length=64,
        description="User or system that submitted the job"
    )
    callback_url: Optional[str] = Field(
        default=None,
        max_length=512,
        description="URL to POST completion notification"
    )

    # Idempotency
    idempotency_key: Optional[str] = Field(
        default=None,
        max_length=64,
        description="Unique key to prevent duplicate job creation"
    )

    # Metadata
    priority: int = Field(
        default=0,
        ge=0,
        le=10,
        description="Priority (0=normal, higher=more urgent)"
    )
    submitted_at: datetime = Field(
        default_factory=datetime.utcnow,
        description="When the submission was received by gateway"
    )

    # Message ID (for Service Bus deduplication)
    message_id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        description="Unique message ID for Service Bus"
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "workflow_id": "raster_ingest",
                "input_params": {
                    "container_name": "bronze",
                    "blob_name": "data/file.tif"
                },
                "correlation_id": "ddh-req-12345",
                "submitted_by": "platform-gateway",
                "priority": 0
            }
        }
    }

    # =========================================================================
    # SERIALIZATION (Explicit methods per design principles)
    # =========================================================================

    def to_service_bus_body(self) -> str:
        """
        Serialize to JSON string for Service Bus message body.

        Use this when publishing to Service Bus queue.
        """
        return self.model_dump_json()

    @classmethod
    def from_service_bus_body(cls, body: str) -> "JobQueueMessage":
        """
        Deserialize from Service Bus message body.

        Use this when consuming from Service Bus queue.

        Args:
            body: JSON string from message body

        Returns:
            JobQueueMessage instance

        Raises:
            ValidationError: If JSON is invalid or missing required fields
        """
        return cls.model_validate_json(body)


# ============================================================================
# EXPORTS
# ============================================================================

__all__ = ["JobQueueMessage"]
