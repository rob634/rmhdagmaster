# ============================================================================
# UI ADAPTERS
# ============================================================================
# EPOCH: 4/5 - DAG PORTABLE
# STATUS: Core - Model to DTO conversion
# PURPOSE: Convert epoch-specific models to stable UI DTOs
# CREATED: 29 JAN 2026
# ============================================================================
"""
UI Adapters.

Adapters convert epoch-specific data models to stable UI DTOs.
This abstraction allows the same UI templates to work with both
Epoch 4 and DAG (Epoch 5) data models.

Current Mode Detection:
    The adapters auto-detect which mode is active based on available
    imports. In Epoch 4, only epoch4 adapters are used. When DAG models
    become available, the DAG adapters will be used.

Usage:
    from ui.adapters import job_to_dto, task_to_dto, asset_to_dto

    # These work with both Epoch 4 and DAG models
    job_dto = job_to_dto(job_record)
    task_dto = task_to_dto(task_record)
    asset_dto = asset_to_dto(asset)
"""

from typing import List, Any

# Import epoch4 adapters (always available)
from .epoch4 import (
    job_to_dto as _epoch4_job_to_dto,
    task_to_dto as _epoch4_task_to_dto,
    asset_to_dto as _epoch4_asset_to_dto,
    jobs_to_dto as _epoch4_jobs_to_dto,
    tasks_to_dto as _epoch4_tasks_to_dto,
    stage_to_node_dto as _epoch4_stage_to_node_dto,
    job_event_to_dto as _epoch4_job_event_to_dto,
)

# Try to import DAG adapters (available when DAG models exist)
try:
    from .dag import (
        job_to_dto as _dag_job_to_dto,
        node_to_dto as _dag_node_to_dto,
        task_to_dto as _dag_task_to_dto,
        asset_to_dto as _dag_asset_to_dto,
    )
    DAG_AVAILABLE = True
except ImportError:
    DAG_AVAILABLE = False


def job_to_dto(job: Any) -> "JobDTO":
    """
    Convert a job model to JobDTO.

    Automatically detects whether the input is an Epoch 4 JobRecord
    or a DAG Job model and uses the appropriate adapter.

    Args:
        job: JobRecord (Epoch 4) or Job (DAG)

    Returns:
        JobDTO: Stable UI representation
    """
    # Check if it's a DAG Job (has workflow_id attribute directly)
    if DAG_AVAILABLE and hasattr(job, 'workflow_id') and not hasattr(job, 'job_type'):
        return _dag_job_to_dto(job)
    # Default to Epoch 4 adapter
    return _epoch4_job_to_dto(job)


def task_to_dto(task: Any) -> "TaskDTO":
    """
    Convert a task model to TaskDTO.

    Args:
        task: TaskRecord (Epoch 4) or TaskResult (DAG)

    Returns:
        TaskDTO: Stable UI representation
    """
    # Check if it's a DAG TaskResult (has node_id)
    if DAG_AVAILABLE and hasattr(task, 'node_id') and not hasattr(task, 'task_type'):
        return _dag_task_to_dto(task)
    return _epoch4_task_to_dto(task)


def asset_to_dto(asset: Any) -> "AssetDTO":
    """
    Convert an asset model to AssetDTO.

    Works with GeospatialAsset from both Epoch 4 and DAG.
    The model is the same, so we use the Epoch 4 adapter.

    Args:
        asset: GeospatialAsset

    Returns:
        AssetDTO: Stable UI representation
    """
    return _epoch4_asset_to_dto(asset)


def jobs_to_dto(jobs: List[Any]) -> List["JobDTO"]:
    """Convert a list of job models to JobDTOs."""
    return [job_to_dto(job) for job in jobs]


def tasks_to_dto(tasks: List[Any]) -> List["TaskDTO"]:
    """Convert a list of task models to TaskDTOs."""
    return [task_to_dto(task) for task in tasks]


def stage_to_node_dto(job_id: str, stage: int, tasks: List[Any] = None) -> "NodeDTO":
    """
    Convert Epoch 4 stage concept to NodeDTO.

    In Epoch 4, a "node" is a stage with associated tasks.
    This adapter creates a NodeDTO from stage information.

    Args:
        job_id: Parent job ID
        stage: Stage number (1, 2, 3...)
        tasks: Optional list of tasks in this stage

    Returns:
        NodeDTO: Node representation of the stage
    """
    return _epoch4_stage_to_node_dto(job_id, stage, tasks)


def job_event_to_dto(event: Any) -> "JobEventDTO":
    """Convert a job event to JobEventDTO."""
    return _epoch4_job_event_to_dto(event)


__all__ = [
    "job_to_dto",
    "task_to_dto",
    "asset_to_dto",
    "jobs_to_dto",
    "tasks_to_dto",
    "stage_to_node_dto",
    "job_event_to_dto",
    "DAG_AVAILABLE",
]
