# ============================================================================
# CLAUDE CONTEXT - MODELS MODULE
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Model exports
# PURPOSE: Central export point for all Pydantic models
# LAST_REVIEWED: 28 JAN 2026
# ============================================================================
"""
Models Module - Central Export Point

All Pydantic models for the DAG orchestration system.
Models define SQL metadata via __sql_* ClassVar attributes for DDL generation.

Single Source of Truth Pattern:
    - Pydantic models define structure
    - PydanticToSQL reads __sql_* metadata
    - PostgreSQL schema generated from models
"""

from core.models.workflow import WorkflowDefinition, NodeDefinition, NodeType
from core.models.job import Job
from core.models.node import NodeState
from core.models.task import TaskMessage, TaskResult
from core.models.events import JobEvent, EventType, EventStatus

__all__ = [
    # Workflow
    "WorkflowDefinition",
    "NodeDefinition",
    "NodeType",
    # Job
    "Job",
    # Node
    "NodeState",
    # Task
    "TaskMessage",
    "TaskResult",
    # Events
    "JobEvent",
    "EventType",
    "EventStatus",
]
