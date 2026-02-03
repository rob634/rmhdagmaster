# ============================================================================
# INFRASTRUCTURE MODULE
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Infrastructure - Database and storage operations
# PURPOSE: Database schema deployment and Azure storage operations
# CREATED: 29 JAN 2026
# ============================================================================
"""
Infrastructure module for DAG Orchestrator.

Provides:
- DatabaseInitializer: Bootstrap database schema from Pydantic models
- BlobRepository: Azure Blob Storage operations with streaming
- initialize_database: Convenience function for deployment

Usage:
    from infrastructure import DatabaseInitializer, BlobRepository

    # Initialize database schema
    initializer = DatabaseInitializer()
    result = initializer.initialize_all()

    # Use blob storage
    repo = BlobRepository.for_zone("bronze")
    result = repo.stream_blob_to_mount(
        container="bronze-rasters",
        blob_path="uploads/file.tif",
        mount_path="/mnt/etl/file.tif"
    )
"""

from infrastructure.database_initializer import (
    DatabaseInitializer,
    InitializationResult,
    StepResult,
    initialize_database,
)
from infrastructure.storage import (
    BlobRepository,
    get_blob_repository,
)
from infrastructure.postgresql import (
    PostgreSQLRepository,
    get_postgres_repository,
)
from infrastructure.pgstac import (
    PgStacRepository,
    get_pgstac_repository,
)
from infrastructure.pgstac_search import (
    PgStacSearchRegistration,
    get_search_registrar,
)
from infrastructure.locking import (
    LockService,
    LockNotAcquired,
)

__all__ = [
    # Database Initialization
    'DatabaseInitializer',
    'InitializationResult',
    'StepResult',
    'initialize_database',
    # Blob Storage
    'BlobRepository',
    'get_blob_repository',
    # PostgreSQL
    'PostgreSQLRepository',
    'get_postgres_repository',
    # pgSTAC
    'PgStacRepository',
    'get_pgstac_repository',
    'PgStacSearchRegistration',
    'get_search_registrar',
    # Locking
    'LockService',
    'LockNotAcquired',
]
