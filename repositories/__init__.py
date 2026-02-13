# ============================================================================
# REPOSITORIES MODULE
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Core - Database access layer
# PURPOSE: CRUD operations for DAG entities
# CREATED: 29 JAN 2026
# ============================================================================
"""
Repositories Module

Provides database access for DAG orchestration entities.
Uses psycopg3 async with connection pooling.

Usage:
    from repositories import JobRepository, NodeRepository, TaskResultRepository

    async with get_pool() as pool:
        job_repo = JobRepository(pool)
        job = await job_repo.get(job_id)
"""

from .database import get_pool, DatabasePool
from .job_repo import JobRepository
from .job_queue_repo import JobQueueRepository, create_job_queue_repository
from .node_repo import NodeRepository
from .task_repo import TaskResultRepository
from .event_repo import EventRepository
from .checkpoint_repo import CheckpointRepository
from .platform_repo import PlatformRepository
from .asset_repo import AssetRepository
from .version_repo import AssetVersionRepository
from .metadata_repo import LayerMetadataRepository

__all__ = [
    "get_pool",
    "DatabasePool",
    "JobRepository",
    "JobQueueRepository",
    "create_job_queue_repository",
    "NodeRepository",
    "TaskResultRepository",
    "EventRepository",
    "CheckpointRepository",
    "PlatformRepository",
    "AssetRepository",
    "AssetVersionRepository",
    "LayerMetadataRepository",
]
