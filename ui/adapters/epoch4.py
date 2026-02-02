# ============================================================================
# EPOCH 4 ADAPTERS
# ============================================================================
# EPOCH: 4 - CURRENT
# STATUS: Core - Convert Epoch 4 models to UI DTOs
# PURPOSE: Map JobRecord, TaskRecord, GeospatialAsset to stable DTOs
# CREATED: 29 JAN 2026
# ============================================================================
"""
Epoch 4 Adapters.

Convert Epoch 4 data models to stable UI DTOs. These adapters handle
the mapping between Epoch 4's job_type/stage model and the generic
workflow_id/node model used by the UI.

Key Mappings:
    JobRecord.job_type     → JobDTO.workflow_id
    JobRecord.stage        → JobDTO.current_step
    JobRecord.total_stages → JobDTO.total_steps
    TaskRecord.task_type   → TaskDTO.task_type, NodeDTO.handler
    Stage (concept)        → NodeDTO

Usage:
    from ui.adapters.epoch4 import job_to_dto, task_to_dto

    job_dto = job_to_dto(job_record)
    task_dto = task_to_dto(task_record)
"""

from typing import List, Optional, Any, Dict
from datetime import datetime

from ui.dto import (
    JobDTO,
    TaskDTO,
    NodeDTO,
    AssetDTO,
    JobEventDTO,
    JobStatusDTO,
    TaskStatusDTO,
    NodeStatusDTO,
    ApprovalStateDTO,
    ClearanceStateDTO,
    ProcessingStatusDTO,
)


# ============================================================================
# STATUS MAPPING
# ============================================================================

def map_job_status(status: Any) -> JobStatusDTO:
    """
    Map Epoch 4 JobStatus to JobStatusDTO.

    Epoch 4 values: queued, processing, completed, failed, completed_with_errors
    """
    if status is None:
        return JobStatusDTO.PENDING

    # Handle enum or string
    value = status.value if hasattr(status, 'value') else str(status).lower()

    mapping = {
        "queued": JobStatusDTO.QUEUED,
        "processing": JobStatusDTO.RUNNING,
        "completed": JobStatusDTO.COMPLETED,
        "failed": JobStatusDTO.FAILED,
        "completed_with_errors": JobStatusDTO.COMPLETED_WITH_ERRORS,
        "cancelled": JobStatusDTO.CANCELLED,
    }
    return mapping.get(value, JobStatusDTO.PENDING)


def map_task_status(status: Any) -> TaskStatusDTO:
    """
    Map Epoch 4 TaskStatus to TaskStatusDTO.

    Epoch 4 values: pending, queued, processing, completed, failed, retrying, pending_retry, cancelled
    """
    if status is None:
        return TaskStatusDTO.PENDING

    value = status.value if hasattr(status, 'value') else str(status).lower()

    mapping = {
        "pending": TaskStatusDTO.PENDING,
        "queued": TaskStatusDTO.QUEUED,
        "processing": TaskStatusDTO.RUNNING,
        "completed": TaskStatusDTO.COMPLETED,
        "failed": TaskStatusDTO.FAILED,
        "retrying": TaskStatusDTO.RETRYING,
        "pending_retry": TaskStatusDTO.RETRYING,
        "cancelled": TaskStatusDTO.CANCELLED,
    }
    return mapping.get(value, TaskStatusDTO.PENDING)


def map_stage_status(tasks: List[Any]) -> NodeStatusDTO:
    """
    Derive stage status from tasks in the stage.

    Logic:
    - No tasks: PENDING
    - Any task running: RUNNING
    - All tasks completed: COMPLETED
    - Any task failed (and none running): FAILED
    - Otherwise: PENDING
    """
    if not tasks:
        return NodeStatusDTO.PENDING

    statuses = [map_task_status(t.status) for t in tasks]

    if TaskStatusDTO.RUNNING in statuses:
        return NodeStatusDTO.RUNNING
    if all(s == TaskStatusDTO.COMPLETED for s in statuses):
        return NodeStatusDTO.COMPLETED
    if TaskStatusDTO.FAILED in statuses:
        return NodeStatusDTO.FAILED
    if TaskStatusDTO.QUEUED in statuses:
        return NodeStatusDTO.DISPATCHED

    return NodeStatusDTO.PENDING


def map_approval_state(state: Any) -> Optional[ApprovalStateDTO]:
    """Map approval state enum to DTO."""
    if state is None:
        return None
    value = state.value if hasattr(state, 'value') else str(state).lower()
    mapping = {
        "pending_review": ApprovalStateDTO.PENDING_REVIEW,
        "approved": ApprovalStateDTO.APPROVED,
        "rejected": ApprovalStateDTO.REJECTED,
    }
    return mapping.get(value)


def map_clearance_state(state: Any) -> Optional[ClearanceStateDTO]:
    """Map clearance state enum to DTO."""
    if state is None:
        return None
    value = state.value if hasattr(state, 'value') else str(state).lower()
    mapping = {
        "uncleared": ClearanceStateDTO.UNCLEARED,
        "ouo": ClearanceStateDTO.OUO,
        "public": ClearanceStateDTO.PUBLIC,
    }
    return mapping.get(value)


def map_processing_status(status: Any) -> Optional[ProcessingStatusDTO]:
    """Map processing status enum to DTO."""
    if status is None:
        return None
    value = status.value if hasattr(status, 'value') else str(status).lower()
    mapping = {
        "pending": ProcessingStatusDTO.PENDING,
        "processing": ProcessingStatusDTO.PROCESSING,
        "completed": ProcessingStatusDTO.COMPLETED,
        "failed": ProcessingStatusDTO.FAILED,
    }
    return mapping.get(value)


# ============================================================================
# MODEL ADAPTERS
# ============================================================================

def job_to_dto(job: Any) -> JobDTO:
    """
    Convert Epoch 4 JobRecord to JobDTO.

    Args:
        job: JobRecord instance

    Returns:
        JobDTO with mapped fields
    """
    return JobDTO(
        # Identity
        job_id=job.job_id,
        workflow_id=job.job_type,  # E4: job_type → workflow_id

        # Status
        status=map_job_status(job.status),

        # Progress (E4: stage-based)
        current_step=job.stage,
        total_steps=job.total_stages,
        current_step_name=f"Stage {job.stage}",

        # Results
        result_data=job.result_data,
        error_message=job.error_details,

        # Timestamps
        created_at=job.created_at,
        updated_at=job.updated_at,
        completed_at=_get_completed_at(job),

        # Metadata
        parameters=job.parameters if hasattr(job, 'parameters') else {},
        metadata=job.metadata if hasattr(job, 'metadata') else {},
    )


def _get_completed_at(job: Any) -> Optional[datetime]:
    """Extract completed_at from job if available."""
    if hasattr(job, 'completed_at'):
        return job.completed_at
    # Check if terminal status and use updated_at
    status = map_job_status(job.status)
    if status.is_terminal:
        return job.updated_at
    return None


def task_to_dto(task: Any) -> TaskDTO:
    """
    Convert Epoch 4 TaskRecord to TaskDTO.

    Args:
        task: TaskRecord instance

    Returns:
        TaskDTO with mapped fields
    """
    return TaskDTO(
        # Identity
        task_id=task.task_id,
        job_id=task.parent_job_id,
        node_id=f"stage_{task.stage}",  # Synthetic node_id for E4

        # Definition
        task_type=task.task_type,
        stage=task.stage,
        task_index=task.task_index,

        # Status
        status=map_task_status(task.status),
        retry_count=task.retry_count if hasattr(task, 'retry_count') else 0,

        # Routing
        target_queue=getattr(task, 'target_queue', None),
        executed_by_app=getattr(task, 'executed_by_app', None),

        # Results
        result_data=task.result_data,
        error_message=task.error_details,

        # Checkpoint
        checkpoint_phase=getattr(task, 'checkpoint_phase', None),
        checkpoint_data=getattr(task, 'checkpoint_data', None),

        # Timestamps
        created_at=task.created_at,
        started_at=getattr(task, 'execution_started_at', None),
        completed_at=_get_task_completed_at(task),
        last_pulse=getattr(task, 'last_pulse', None),

        # Parameters
        parameters=task.parameters if hasattr(task, 'parameters') else {},
    )


def _get_task_completed_at(task: Any) -> Optional[datetime]:
    """Extract completed_at from task if available."""
    if hasattr(task, 'completed_at') and task.completed_at:
        return task.completed_at
    status = map_task_status(task.status)
    if status in (TaskStatusDTO.COMPLETED, TaskStatusDTO.FAILED):
        return task.updated_at
    return None


def stage_to_node_dto(
    job_id: str,
    stage: int,
    tasks: Optional[List[Any]] = None,
    handler: Optional[str] = None,
) -> NodeDTO:
    """
    Convert Epoch 4 stage concept to NodeDTO.

    In Epoch 4, stages are implicit (numbered 1, 2, 3...).
    This creates a NodeDTO representation for UI consistency.

    Args:
        job_id: Parent job ID
        stage: Stage number
        tasks: Optional list of tasks in this stage
        handler: Optional handler name (derived from first task if not provided)

    Returns:
        NodeDTO representing the stage
    """
    tasks = tasks or []

    # Derive handler from first task if not provided
    if not handler and tasks:
        handler = tasks[0].task_type if hasattr(tasks[0], 'task_type') else None

    # Calculate status from tasks
    status = map_stage_status(tasks)

    # Find timestamps from tasks
    started_at = None
    completed_at = None
    if tasks:
        starts = [t.execution_started_at for t in tasks
                  if hasattr(t, 'execution_started_at') and t.execution_started_at]
        if starts:
            started_at = min(starts)

        if status in (NodeStatusDTO.COMPLETED, NodeStatusDTO.FAILED):
            ends = [t.updated_at for t in tasks if t.updated_at]
            if ends:
                completed_at = max(ends)

    # Get error from failed task
    error_message = None
    for task in tasks:
        if map_task_status(task.status) == TaskStatusDTO.FAILED:
            error_message = task.error_details
            break

    return NodeDTO(
        node_id=f"stage_{stage}",
        job_id=job_id,
        handler=handler,
        description=f"Stage {stage}",
        status=status,
        task_id=tasks[0].task_id if tasks else None,
        retry_count=max((t.retry_count for t in tasks), default=0) if tasks else 0,
        output=_aggregate_task_outputs(tasks),
        error_message=error_message,
        started_at=started_at,
        completed_at=completed_at,
        is_dynamic=False,
    )


def _aggregate_task_outputs(tasks: List[Any]) -> Optional[Dict[str, Any]]:
    """Aggregate outputs from all tasks in a stage."""
    if not tasks:
        return None

    outputs = {}
    for task in tasks:
        if task.result_data:
            outputs[f"task_{task.task_index}"] = task.result_data

    return outputs if outputs else None


def asset_to_dto(asset: Any) -> AssetDTO:
    """
    Convert GeospatialAsset to AssetDTO.

    Works with the V0.8 GeospatialAsset model.

    Args:
        asset: GeospatialAsset instance

    Returns:
        AssetDTO with mapped fields
    """
    return AssetDTO(
        # Identity
        asset_id=asset.asset_id,
        dataset_id=asset.dataset_id,
        resource_id=asset.resource_id,
        version_id=asset.version_id,

        # Type
        data_type=asset.data_type,

        # Service outputs
        table_name=getattr(asset, 'table_name', None),
        blob_path=getattr(asset, 'blob_path', None),
        stac_item_id=getattr(asset, 'stac_item_id', None),
        stac_collection_id=getattr(asset, 'stac_collection_id', None),

        # State dimensions
        revision=getattr(asset, 'revision', 1),
        approval_state=map_approval_state(getattr(asset, 'approval_state', None)),
        clearance_state=map_clearance_state(getattr(asset, 'clearance_state', None)),
        processing_status=map_processing_status(getattr(asset, 'processing_status', None)),

        # Job linkage
        current_job_id=getattr(asset, 'current_job_id', None),
        job_count=getattr(asset, 'job_count', 0),
        workflow_id=getattr(asset, 'workflow_id', None),

        # Approval details
        reviewer=getattr(asset, 'reviewer', None),
        reviewed_at=getattr(asset, 'reviewed_at', None),
        rejection_reason=getattr(asset, 'rejection_reason', None),

        # Processing details
        processing_started_at=getattr(asset, 'processing_started_at', None),
        processing_completed_at=getattr(asset, 'processing_completed_at', None),
        last_error=getattr(asset, 'last_error', None),
        node_summary=getattr(asset, 'node_summary', None),

        # Soft delete
        deleted_at=getattr(asset, 'deleted_at', None),
        deleted_by=getattr(asset, 'deleted_by', None),

        # Timestamps
        created_at=getattr(asset, 'created_at', None),
        updated_at=getattr(asset, 'updated_at', None),
    )


def job_event_to_dto(event: Any) -> JobEventDTO:
    """
    Convert JobEvent to JobEventDTO.

    Args:
        event: JobEvent instance

    Returns:
        JobEventDTO with mapped fields
    """
    return JobEventDTO(
        event_id=getattr(event, 'event_id', None),
        job_id=event.job_id,
        node_id=getattr(event, 'node_id', None),
        task_id=getattr(event, 'task_id', None),
        event_type=event.event_type.value if hasattr(event.event_type, 'value') else str(event.event_type),
        event_status=event.event_status.value if hasattr(event.event_status, 'value') else str(event.event_status),
        checkpoint_name=getattr(event, 'checkpoint_name', None),
        event_data=getattr(event, 'event_data', {}),
        error_message=getattr(event, 'error_message', None),
        duration_ms=getattr(event, 'duration_ms', None),
        source_app=getattr(event, 'source_app', None),
        created_at=getattr(event, 'created_at', None),
    )


def jobs_to_dto(jobs: List[Any]) -> List[JobDTO]:
    """Convert a list of JobRecords to JobDTOs."""
    return [job_to_dto(job) for job in jobs]


def tasks_to_dto(tasks: List[Any]) -> List[TaskDTO]:
    """Convert a list of TaskRecords to TaskDTOs."""
    return [task_to_dto(task) for task in tasks]
