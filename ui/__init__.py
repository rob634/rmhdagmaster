# ============================================================================
# UI ABSTRACTION LAYER
# ============================================================================
# EPOCH: 4/5 - DAG PORTABLE
# STATUS: Core - UI abstraction for portability between Epoch 4 and DAG
# PURPOSE: Provide stable DTOs, adapters, and configuration for UI components
# CREATED: 29 JAN 2026
# ============================================================================
"""
UI Abstraction Layer.

This module provides a stable interface between the UI (Jinja2 templates)
and the underlying data models. It enables the same UI components to work
with both Epoch 4 (current) and Epoch 5 (DAG orchestrator).

Architecture:
    DTOs (dto.py)
        Stable data transfer objects that don't change between epochs.
        UI components consume these, not raw database models.

    Adapters (adapters/)
        Convert epoch-specific models to DTOs.
        - epoch4.py: JobRecord -> JobDTO, TaskRecord -> TaskDTO, etc.
        - dag.py: DagJob -> JobDTO, NodeState -> NodeDTO, etc. (future)

    Configuration
        - navigation.py: Nav items with conditional visibility
        - terminology.py: UI term mappings (Stage vs Node)
        - features.py: Feature flags for gradual rollout

Usage:
    from ui import JobDTO, job_to_dto, get_nav_items, get_terms, is_enabled

    # In route handler
    job_record = await job_repo.get(job_id)
    job_dto = job_to_dto(job_record)  # Adapter converts to DTO

    return render_template(
        request,
        "jobs/detail.html",
        job=job_dto,
        nav_items=get_nav_items(),
        terms=get_terms(),
    )

See ui/README.md for detailed documentation.
"""

# DTOs - Stable data transfer objects
from .dto import (
    JobStatusDTO,
    NodeStatusDTO,
    TaskStatusDTO,
    ApprovalStateDTO,
    ClearanceStateDTO,
    ProcessingStatusDTO,
    JobDTO,
    NodeDTO,
    TaskDTO,
    AssetDTO,
    JobEventDTO,
)

# Adapters - Convert models to DTOs
from .adapters import (
    job_to_dto,
    task_to_dto,
    asset_to_dto,
    jobs_to_dto,
    tasks_to_dto,
)

# Navigation
from .navigation import (
    NavItem,
    get_nav_items,
    get_nav_items_for_mode,
)

# Terminology
from .terminology import (
    get_terms,
    Terms,
)

# Feature flags
from .features import (
    is_enabled,
    get_enabled_features,
    FEATURES,
)

__all__ = [
    # DTOs
    "JobStatusDTO",
    "NodeStatusDTO",
    "TaskStatusDTO",
    "ApprovalStateDTO",
    "ClearanceStateDTO",
    "ProcessingStatusDTO",
    "JobDTO",
    "NodeDTO",
    "TaskDTO",
    "AssetDTO",
    "JobEventDTO",
    # Adapters
    "job_to_dto",
    "task_to_dto",
    "asset_to_dto",
    "jobs_to_dto",
    "tasks_to_dto",
    # Navigation
    "NavItem",
    "get_nav_items",
    "get_nav_items_for_mode",
    # Terminology
    "get_terms",
    "Terms",
    # Features
    "is_enabled",
    "get_enabled_features",
    "FEATURES",
]
