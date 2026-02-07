# Multi-Orchestrator Implementation Plan

**Created**: 03 FEB 2026
**Updated**: 03 FEB 2026
**Status**: INFRASTRUCTURE COMPLETE - Ready for Phase 1
**Objective**: Replace single-leader lease model with competing consumers architecture

---

## Design Principles

1. **Pydantic models are the schema** - All data structures defined as Pydantic models with explicit serialization/deserialization
2. **Repository pattern for all I/O** - Database, Service Bus, Blob Storage accessed only through repository classes
3. **Service layer for business logic** - Orchestration logic lives in services, not repositories
4. **Explicit over implicit** - No magic; all behavior declared in code

---

## Core Infrastructure (IMPLEMENTED)

The following infrastructure components have been extracted from `rmhgeoapi` and are ready to use:

### Base Repository (`infrastructure/base_repository.py`)

**Status**: ✅ COMPLETE

Provides common patterns for all repositories:

```python
from infrastructure.base_repository import (
    BaseRepository,
    AsyncBaseRepository,
    RepositoryError,
    ValidationError,
    JOB_STATUS_TRANSITIONS,
    NODE_STATUS_TRANSITIONS,
)

class MyRepository(AsyncBaseRepository):
    """Repository with built-in error handling and validation."""

    async def some_operation(self, entity_id: str):
        # Automatic error wrapping with context
        with self._error_context("some operation", entity_id):
            # ... database operations ...
            pass

    def update_status(self, current: JobStatus, new: JobStatus):
        # Built-in status transition validation
        self._validate_status_transition(
            current, new, JOB_STATUS_TRANSITIONS
        )
```

**Features**:
- `_error_context()` - Consistent error handling with logging
- `_validate_status_transition()` - State machine validation against allowed transitions
- `_log_operation()` - Standardized operation logging
- `RepositoryError`, `ValidationError` - Typed exceptions with context
- `JOB_STATUS_TRANSITIONS` - Allowed job status transitions
- `NODE_STATUS_TRANSITIONS` - Allowed node status transitions

### Service Bus Infrastructure (`infrastructure/service_bus.py`)

**Status**: ✅ COMPLETE

Provides Service Bus publisher and consumer with production patterns:

```python
from infrastructure.service_bus import (
    ServiceBusConfig,
    ServiceBusPublisher,
    ServiceBusConsumer,
    BatchResult,
)

# Configuration from environment
config = ServiceBusConfig.from_env()

# Publisher (singleton, thread-safe)
publisher = ServiceBusPublisher.get_instance(config)
message_id = publisher.send_message("queue-name", pydantic_model)

# Consumer (async)
consumer = ServiceBusConsumer(config, "queue-name")
await consumer.connect()

# Single message receive
result = await consumer.receive_one(max_wait_time=5.0, message_type=MyModel)
if result:
    parsed_message, raw_message = result
    # Process...
    await consumer.complete(raw_message)

# Continuous consumption loop
async def handler(message: MyModel, raw) -> bool:
    # Process message...
    return True  # Complete (or False to abandon)

await consumer.consume_loop(handler, stop_event)
```

**Publisher Features**:
- Thread-safe singleton pattern
- Sender caching with AMQP connection warmup
- Automatic Pydantic serialization (`model_dump_json()`)
- Retry logic with error categorization (permanent vs transient)
- Batch send support with `BatchResult` tracking
- TTL configuration per message

**Consumer Features**:
- Async context manager pattern
- `receive_one()` for single message receive
- `consume_loop()` for continuous processing
- Automatic Pydantic deserialization (`model_validate_json()`)
- Dead-letter for malformed messages
- Complete/abandon/dead-letter message disposition

**Environment Variables**:
```bash
SERVICE_BUS_NAMESPACE=rmhazure.servicebus.windows.net
SERVICE_BUS_CONNECTION_STRING=  # Optional, for local dev
MANAGED_IDENTITY_CLIENT_ID=     # Optional, for managed identity
```

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        MULTI-ORCHESTRATOR ARCHITECTURE                       │
└─────────────────────────────────────────────────────────────────────────────┘

   External Request
         │
         ▼
┌─────────────────────────────┐
│   Function App (Gateway)    │  ◄── Validates, generates job_id, enqueues
│   /api/platform/submit      │
└──────────────┬──────────────┘
               │
               ▼
┌─────────────────────────────┐
│   dag-jobs Queue            │  ◄── Service Bus queue
│   (orchestrator-jobs)       │
└──────────────┬──────────────┘
               │
     ┌─────────┼─────────┐
     ▼         ▼         ▼
┌─────────┐ ┌─────────┐ ┌─────────┐
│ Orch 1  │ │ Orch 2  │ │ Orch N  │  ◄── Competing consumers
│         │ │         │ │         │      Each owns claimed jobs
└────┬────┘ └────┬────┘ └────┬────┘
     │           │           │
     └───────────┼───────────┘
                 ▼
┌─────────────────────────────┐
│   PostgreSQL (dagapp)       │  ◄── dag_jobs with owner_id
│   - dag_jobs                │
│   - dag_node_states         │
│   - dag_task_results        │
└─────────────────────────────┘
```

---

## Component Design

### 1. Job Model Extensions (Pydantic IaC Pattern)

**File**: `core/models/job.py`

The Pydantic model IS the schema. Add fields and update `__sql_indexes__`:

```python
class Job(JobData):
    """Extended with ownership fields for multi-orchestrator support."""

    # =========================================================================
    # SQL DDL METADATA (Used by PydanticToSQL generator)
    # =========================================================================
    __sql_table__: ClassVar[str] = "dag_jobs"
    __sql_schema__: ClassVar[str] = "dagapp"
    __sql_primary_key__: ClassVar[List[str]] = ["job_id"]
    __sql_foreign_keys__: ClassVar[Dict[str, str]] = {}
    __sql_indexes__: ClassVar[List[tuple]] = [
        ("idx_dag_jobs_status", ["status"]),
        ("idx_dag_jobs_workflow", ["workflow_id"]),
        ("idx_dag_jobs_created", ["created_at"]),
        # NEW: Multi-orchestrator indexes
        ("idx_dag_jobs_owner", ["owner_id"]),           # Filter by owner
        ("idx_dag_jobs_heartbeat", ["owner_heartbeat_at"]),  # Orphan scan
    ]

    # ... existing fields ...

    # NEW: Ownership (multi-orchestrator)
    owner_id: Optional[str] = Field(
        default=None,
        max_length=64,
        description="Orchestrator instance ID that owns this job"
    )
    owner_heartbeat_at: Optional[datetime] = Field(
        default=None,
        description="Last heartbeat from owning orchestrator"
    )
```

**Schema Deployment** (via Bootstrap API - no raw SQL):

```bash
# Development: Preview DDL changes
curl -X GET "https://host/api/v1/bootstrap/ddl"

# Development: Apply new columns (preserves data)
curl -X POST "https://host/api/v1/bootstrap/migrate?confirm=yes"

# Production: Export DDL for DBA review
curl -X GET "https://host/api/v1/bootstrap/ddl" > migrations/v0.9.0_multi_orch.sql
```

The `PydanticToSQL` generator reads the model's `__sql_*__` metadata and generates:
- `ADD COLUMN IF NOT EXISTS owner_id VARCHAR(64)`
- `ADD COLUMN IF NOT EXISTS owner_heartbeat_at TIMESTAMPTZ`
- `CREATE INDEX IF NOT EXISTS idx_dag_jobs_owner ...`
- `CREATE INDEX IF NOT EXISTS idx_dag_jobs_heartbeat ...`

---

### 2. Job Queue Message Model

**File**: `core/models/job_queue_message.py` (NEW)

```python
"""
Job Queue Message

Pydantic model for messages on the dag-jobs Service Bus queue.
This is the "work order" submitted by the Gateway and consumed by orchestrators.
"""

from datetime import datetime
from typing import Any, Dict, Optional
from pydantic import BaseModel, Field
import uuid


class JobQueueMessage(BaseModel):
    """
    Message format for the dag-jobs queue.

    The Gateway creates this message; orchestrators consume it.
    This is NOT the Job model - it's the submission request.
    """

    # Required fields
    workflow_id: str = Field(..., max_length=64)
    input_params: Dict[str, Any] = Field(default_factory=dict)

    # Optional tracking
    correlation_id: Optional[str] = Field(
        default=None,
        max_length=64,
        description="External correlation ID for tracing"
    )
    submitted_by: Optional[str] = Field(
        default=None,
        max_length=64,
        description="User or system that submitted"
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
    submitted_at: datetime = Field(default_factory=datetime.utcnow)

    # Message ID (for Service Bus deduplication)
    message_id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        description="Unique message ID"
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
                "submitted_by": "ddh-gateway",
                "priority": 0
            }
        }
    }

    def to_service_bus_body(self) -> str:
        """Serialize to JSON string for Service Bus message body."""
        return self.model_dump_json()

    @classmethod
    def from_service_bus_body(cls, body: str) -> "JobQueueMessage":
        """Deserialize from Service Bus message body."""
        return cls.model_validate_json(body)
```

---

### 3. Job Queue Repository (Consumer)

**File**: `repositories/job_queue_repo.py` (NEW)

**Uses**: `infrastructure/service_bus.py` (ServiceBusConsumer)

```python
"""
Job Queue Repository

Repository for consuming job submission messages from Service Bus.
Wraps ServiceBusConsumer with JobQueueMessage-specific logic.
"""

import asyncio
import logging
import os
from typing import Optional, Callable, Awaitable

from azure.servicebus import ServiceBusReceivedMessage

from core.models import JobQueueMessage
from infrastructure.service_bus import ServiceBusConfig, ServiceBusConsumer

logger = logging.getLogger(__name__)


class JobQueueRepository:
    """
    Repository for the dag-jobs Service Bus queue.

    Thin wrapper around ServiceBusConsumer that adds:
    - JobQueueMessage type binding
    - Workflow validation before completion
    - Environment-based queue name configuration

    Uses competing consumers pattern - first orchestrator to receive wins.
    """

    def __init__(self, config: Optional[ServiceBusConfig] = None):
        """
        Initialize job queue repository.

        Args:
            config: Service Bus config. If None, loads from environment.
        """
        self._config = config or ServiceBusConfig.from_env()
        self._queue_name = os.environ.get("JOB_QUEUE_NAME", "dag-jobs")
        self._consumer: Optional[ServiceBusConsumer] = None

    async def connect(self) -> None:
        """Establish connection to Service Bus."""
        if self._consumer is not None:
            return

        self._consumer = ServiceBusConsumer(self._config, self._queue_name)
        await self._consumer.connect()
        logger.info(f"JobQueueRepository connected to queue: {self._queue_name}")

    async def close(self) -> None:
        """Close Service Bus connection."""
        if self._consumer:
            await self._consumer.close()
            self._consumer = None
        logger.info("JobQueueRepository connection closed")

    async def receive_one(
        self,
        max_wait_time: float = 5.0,
    ) -> Optional[tuple[JobQueueMessage, ServiceBusReceivedMessage]]:
        """
        Receive a single job message.

        Returns:
            Tuple of (parsed JobQueueMessage, raw message) or None
        """
        if self._consumer is None:
            await self.connect()

        return await self._consumer.receive_one(
            max_wait_time=max_wait_time,
            message_type=JobQueueMessage,
        )

    async def complete(self, raw_message: ServiceBusReceivedMessage) -> None:
        """Complete message after successful job creation."""
        await self._consumer.complete(raw_message)

    async def abandon(self, raw_message: ServiceBusReceivedMessage) -> None:
        """Abandon message for retry."""
        await self._consumer.abandon(raw_message)

    async def dead_letter(
        self,
        raw_message: ServiceBusReceivedMessage,
        reason: str,
        description: str,
    ) -> None:
        """Dead-letter a message (permanent failure)."""
        await self._consumer.dead_letter(raw_message, reason, description)

    async def consume_loop(
        self,
        handler: Callable[[JobQueueMessage, ServiceBusReceivedMessage], Awaitable[bool]],
        stop_event: asyncio.Event,
    ) -> None:
        """
        Continuous consumption loop.

        Args:
            handler: Async function(message, raw) -> bool. Return True to complete.
            stop_event: Event to signal loop termination.
        """
        if self._consumer is None:
            await self.connect()

        await self._consumer.consume_loop(
            handler=handler,
            stop_event=stop_event,
            message_type=JobQueueMessage,
        )


# ============================================================================
# Factory function
# ============================================================================

def create_job_queue_repository() -> JobQueueRepository:
    """Create JobQueueRepository from environment configuration."""
    return JobQueueRepository()
```

---

### 4. Job Repository Extensions

**File**: `repositories/job_repo.py` (EXTEND)

```python
# Add these methods to existing JobRepository class

async def create_with_owner(
    self,
    job: Job,
    owner_id: str,
) -> Job:
    """
    Create job with ownership claim.

    Used when orchestrator claims a job from the queue.
    Sets owner_id and owner_heartbeat_at atomically on creation.
    """
    job.owner_id = owner_id
    job.owner_heartbeat_at = datetime.utcnow()

    async with self.pool.connection() as conn:
        await conn.execute(
            f"""
            INSERT INTO {TABLE_JOBS} (
                job_id, workflow_id, status, input_params,
                owner_id, owner_heartbeat_at,
                created_at, submitted_by, correlation_id, version
            ) VALUES (
                %(job_id)s, %(workflow_id)s, %(status)s, %(input_params)s,
                %(owner_id)s, %(owner_heartbeat_at)s,
                %(created_at)s, %(submitted_by)s, %(correlation_id)s, %(version)s
            )
            """,
            {
                "job_id": job.job_id,
                "workflow_id": job.workflow_id,
                "status": job.status.value,
                "input_params": Json(job.input_params),
                "owner_id": job.owner_id,
                "owner_heartbeat_at": job.owner_heartbeat_at,
                "created_at": job.created_at,
                "submitted_by": job.submitted_by,
                "correlation_id": job.correlation_id,
                "version": job.version,
            },
        )
        logger.info(
            f"Created job {job.job_id} with owner {owner_id[:8]}..."
        )
        return job

async def list_active_for_owner(
    self,
    owner_id: str,
    limit: int = 100,
) -> List[Job]:
    """
    List active jobs owned by a specific orchestrator.

    This is the KEY query for multi-orchestrator: each orchestrator
    only processes jobs where owner_id matches its own ID.
    """
    async with self.pool.connection() as conn:
        conn.row_factory = dict_row
        result = await conn.execute(
            f"""
            SELECT * FROM {TABLE_JOBS}
            WHERE owner_id = %s
              AND status IN ('pending', 'running')
            ORDER BY created_at ASC
            LIMIT %s
            """,
            (owner_id, limit),
        )
        rows = await result.fetchall()
        return [self._row_to_job(row) for row in rows]

async def update_heartbeat(
    self,
    owner_id: str,
) -> int:
    """
    Update heartbeat for all jobs owned by this orchestrator.

    Called periodically (every 30s) to keep ownership alive.

    Returns:
        Number of jobs updated
    """
    async with self.pool.connection() as conn:
        result = await conn.execute(
            f"""
            UPDATE {TABLE_JOBS}
            SET owner_heartbeat_at = NOW()
            WHERE owner_id = %s
              AND status IN ('pending', 'running')
            """,
            (owner_id,),
        )
        count = result.rowcount
        if count > 0:
            logger.debug(f"Updated heartbeat for {count} jobs (owner={owner_id[:8]}...)")
        return count

async def reclaim_orphaned_jobs(
    self,
    new_owner_id: str,
    orphan_threshold_seconds: int = 120,
    limit: int = 10,
) -> List[str]:
    """
    Reclaim jobs with stale heartbeats (orphaned by crashed orchestrators).

    Uses atomic UPDATE with RETURNING to prevent race conditions.
    Only one orchestrator can win the reclaim for each job.

    Args:
        new_owner_id: This orchestrator's ID
        orphan_threshold_seconds: How long before job is considered orphaned
        limit: Max jobs to reclaim per call

    Returns:
        List of reclaimed job_ids
    """
    async with self.pool.connection() as conn:
        conn.row_factory = dict_row
        result = await conn.execute(
            f"""
            UPDATE {TABLE_JOBS}
            SET owner_id = %s,
                owner_heartbeat_at = NOW()
            WHERE job_id IN (
                SELECT job_id FROM {TABLE_JOBS}
                WHERE status IN ('pending', 'running')
                  AND owner_heartbeat_at < NOW() - INTERVAL '%s seconds'
                LIMIT %s
                FOR UPDATE SKIP LOCKED
            )
            RETURNING job_id
            """,
            (new_owner_id, orphan_threshold_seconds, limit),
        )
        rows = await result.fetchall()
        job_ids = [row["job_id"] for row in rows]

        if job_ids:
            logger.info(
                f"Reclaimed {len(job_ids)} orphaned jobs: "
                f"{[jid[:8] for jid in job_ids]} (new_owner={new_owner_id[:8]}...)"
            )

        return job_ids
```

---

### 5. Orchestrator Loop Redesign

**File**: `orchestrator/loop.py` (MODIFY)

```python
"""
Orchestrator Loop - Multi-Instance Design

Key changes from single-orchestrator:
1. NO lease acquisition - multiple instances can run
2. Consumes jobs from Service Bus queue (competing consumers)
3. Only processes jobs where owner_id = self.owner_id
4. Background heartbeat keeps ownership alive
5. Background orphan scan reclaims abandoned jobs
"""

import asyncio
import logging
import uuid
from datetime import datetime
from typing import Optional, Dict, Any

from psycopg_pool import AsyncConnectionPool

from core.models import Job, NodeState, WorkflowDefinition, JobQueueMessage
from core.contracts import JobStatus, NodeStatus
from services import JobService, NodeService, WorkflowService, EventService
from messaging import TaskPublisher, get_publisher
from repositories import JobRepository, JobQueueRepository, create_job_queue_repository

logger = logging.getLogger(__name__)


class Orchestrator:
    """
    Multi-instance orchestrator.

    Each instance:
    - Has a unique owner_id
    - Consumes job submissions from the dag-jobs queue
    - Only processes jobs it owns
    - Sends heartbeats to keep ownership
    - Can reclaim orphaned jobs from crashed instances
    """

    # Configuration
    POLL_INTERVAL_SEC = 1.0         # Main loop cycle interval
    HEARTBEAT_INTERVAL_SEC = 30     # How often to update heartbeat
    ORPHAN_SCAN_INTERVAL_SEC = 60   # How often to scan for orphans
    ORPHAN_THRESHOLD_SEC = 120      # How old before job is orphaned

    def __init__(
        self,
        pool: AsyncConnectionPool,
        workflow_service: WorkflowService,
        poll_interval: float = 1.0,
        event_service: Optional[EventService] = None,
    ):
        self.pool = pool
        self.workflow_service = workflow_service
        self.poll_interval = poll_interval
        self._event_service = event_service

        # Unique ID for this orchestrator instance
        self._owner_id = str(uuid.uuid4())

        # Services
        self.job_service = JobService(pool, workflow_service, event_service)
        self.node_service = NodeService(pool, workflow_service, event_service)

        # Repositories
        self._job_repo = JobRepository(pool)
        self._job_queue_repo: Optional[JobQueueRepository] = None

        # State
        self._running = False
        self._stop_event = asyncio.Event()

        # Background tasks
        self._main_loop_task: Optional[asyncio.Task] = None
        self._queue_consumer_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._orphan_scan_task: Optional[asyncio.Task] = None

        # Publisher
        self._publisher: Optional[TaskPublisher] = None

        # Metrics
        self._started_at: Optional[datetime] = None
        self._cycles = 0
        self._jobs_claimed = 0
        self._jobs_reclaimed = 0
        self._tasks_dispatched = 0
        self._results_processed = 0
        self._errors = 0
        self._last_heartbeat_at: Optional[datetime] = None
        self._last_orphan_scan_at: Optional[datetime] = None

    @property
    def owner_id(self) -> str:
        """This orchestrator's unique ID."""
        return self._owner_id

    async def start(self) -> None:
        """
        Start the orchestrator.

        No lease acquisition - multiple instances can start.
        """
        if self._running:
            logger.warning("Orchestrator already running")
            return

        self._running = True
        self._started_at = datetime.utcnow()
        self._stop_event.clear()

        # Initialize publisher
        self._publisher = await get_publisher()

        # Initialize job queue consumer
        self._job_queue_repo = create_job_queue_repository()
        await self._job_queue_repo.connect()

        # Start background tasks
        self._main_loop_task = asyncio.create_task(self._main_loop())
        self._queue_consumer_task = asyncio.create_task(self._queue_consumer_loop())
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        self._orphan_scan_task = asyncio.create_task(self._orphan_scan_loop())

        logger.info(
            f"Orchestrator started (owner_id={self._owner_id[:8]}..., "
            f"poll={self.poll_interval}s, heartbeat={self.HEARTBEAT_INTERVAL_SEC}s)"
        )

    async def stop(self) -> None:
        """Stop the orchestrator gracefully."""
        self._running = False
        self._stop_event.set()

        # Cancel background tasks
        for task in [
            self._main_loop_task,
            self._queue_consumer_task,
            self._heartbeat_task,
            self._orphan_scan_task,
        ]:
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        # Close job queue
        if self._job_queue_repo:
            await self._job_queue_repo.close()

        logger.info(
            f"Orchestrator stopped (owner_id={self._owner_id[:8]}..., "
            f"cycles={self._cycles}, jobs_claimed={self._jobs_claimed}, "
            f"jobs_reclaimed={self._jobs_reclaimed})"
        )

    # =========================================================================
    # QUEUE CONSUMER - Claims jobs from dag-jobs queue
    # =========================================================================

    async def _queue_consumer_loop(self) -> None:
        """
        Consume job submissions from the dag-jobs queue.

        This is the entry point for new jobs. When a message is received:
        1. Validate workflow exists
        2. Create Job in database with owner_id = self
        3. Complete the message (remove from queue)
        """
        logger.info("Starting queue consumer loop")

        await self._job_queue_repo.consume_loop(
            handler=self._handle_job_submission,
            stop_event=self._stop_event,
        )

    async def _handle_job_submission(
        self,
        message: JobQueueMessage,
        raw_message,
    ) -> bool:
        """
        Handle a job submission message.

        Returns True to complete (success), False to abandon (retry).
        """
        logger.info(
            f"Received job submission: workflow={message.workflow_id}, "
            f"correlation={message.correlation_id}"
        )

        # Validate workflow exists
        workflow = self.workflow_service.get(message.workflow_id)
        if workflow is None:
            logger.error(f"Workflow not found: {message.workflow_id}")
            await self._job_queue_repo.dead_letter(
                raw_message,
                reason="WorkflowNotFound",
                description=f"Workflow '{message.workflow_id}' does not exist",
            )
            return True  # Don't retry - permanent failure

        # Check idempotency key
        if message.idempotency_key:
            existing = await self._job_repo.get_by_idempotency_key(
                message.idempotency_key
            )
            if existing:
                logger.info(
                    f"Duplicate submission (idempotency_key={message.idempotency_key}), "
                    f"returning existing job {existing.job_id}"
                )
                return True  # Already processed

        # Create job with ownership
        try:
            job = await self.job_service.create_job_with_owner(
                workflow_id=message.workflow_id,
                input_params=message.input_params,
                owner_id=self._owner_id,
                correlation_id=message.correlation_id,
                submitted_by=message.submitted_by,
                idempotency_key=message.idempotency_key,
            )

            self._jobs_claimed += 1
            logger.info(
                f"Claimed job {job.job_id} (workflow={message.workflow_id}, "
                f"owner={self._owner_id[:8]}...)"
            )
            return True

        except Exception as e:
            logger.exception(f"Failed to create job: {e}")
            return False  # Abandon for retry

    # =========================================================================
    # MAIN LOOP - Processes owned jobs
    # =========================================================================

    async def _main_loop(self) -> None:
        """
        Main orchestration loop.

        Only processes jobs where owner_id = self.owner_id.
        """
        logger.info("Starting main orchestration loop")

        while self._running:
            try:
                await self._cycle()
                self._cycles += 1
            except Exception as e:
                self._errors += 1
                logger.exception(f"Error in orchestration cycle: {e}")

            await asyncio.sleep(self.poll_interval)

    async def _cycle(self) -> None:
        """
        Single orchestration cycle.

        Key difference: list_active_for_owner() only returns MY jobs.
        """
        # Process results first
        await self._process_results()

        # Get MY active jobs only
        active_jobs = await self._job_repo.list_active_for_owner(
            self._owner_id,
            limit=100,
        )

        if not active_jobs:
            return

        # Process each job
        for job in active_jobs:
            await self._process_job(job)

    async def _process_job(self, job: Job) -> None:
        """Process a single job (unchanged from current implementation)."""
        # ... existing _process_job logic ...
        pass

    async def _process_results(self) -> int:
        """Process task results (unchanged from current implementation)."""
        # ... existing _process_results logic ...
        pass

    # =========================================================================
    # HEARTBEAT - Keeps ownership alive
    # =========================================================================

    async def _heartbeat_loop(self) -> None:
        """
        Background loop that updates heartbeat for owned jobs.

        If this stops (crash), jobs become orphaned after ORPHAN_THRESHOLD_SEC.
        """
        logger.info(f"Starting heartbeat loop (interval={self.HEARTBEAT_INTERVAL_SEC}s)")

        while not self._stop_event.is_set():
            try:
                count = await self._job_repo.update_heartbeat(self._owner_id)
                self._last_heartbeat_at = datetime.utcnow()

                if count > 0:
                    logger.debug(f"Heartbeat sent for {count} jobs")

            except Exception as e:
                logger.error(f"Heartbeat error: {e}")

            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=self.HEARTBEAT_INTERVAL_SEC,
                )
            except asyncio.TimeoutError:
                pass

    # =========================================================================
    # ORPHAN SCAN - Reclaims abandoned jobs
    # =========================================================================

    async def _orphan_scan_loop(self) -> None:
        """
        Background loop that scans for and reclaims orphaned jobs.

        Orphaned jobs have owner_heartbeat_at older than ORPHAN_THRESHOLD_SEC.
        This handles crashed orchestrator recovery.
        """
        logger.info(
            f"Starting orphan scan loop "
            f"(interval={self.ORPHAN_SCAN_INTERVAL_SEC}s, "
            f"threshold={self.ORPHAN_THRESHOLD_SEC}s)"
        )

        while not self._stop_event.is_set():
            try:
                reclaimed = await self._job_repo.reclaim_orphaned_jobs(
                    new_owner_id=self._owner_id,
                    orphan_threshold_seconds=self.ORPHAN_THRESHOLD_SEC,
                    limit=10,
                )

                self._last_orphan_scan_at = datetime.utcnow()

                if reclaimed:
                    self._jobs_reclaimed += len(reclaimed)
                    logger.info(f"Reclaimed {len(reclaimed)} orphaned jobs")

            except Exception as e:
                logger.error(f"Orphan scan error: {e}")

            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=self.ORPHAN_SCAN_INTERVAL_SEC,
                )
            except asyncio.TimeoutError:
                pass

    # =========================================================================
    # STATS
    # =========================================================================

    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def stats(self) -> Dict[str, Any]:
        uptime = None
        if self._started_at:
            uptime = (datetime.utcnow() - self._started_at).total_seconds()

        return {
            "running": self._running,
            "owner_id": self._owner_id,
            "started_at": self._started_at.isoformat() if self._started_at else None,
            "uptime_seconds": uptime,
            "poll_interval": self.poll_interval,
            "cycles": self._cycles,
            "jobs_claimed": self._jobs_claimed,
            "jobs_reclaimed": self._jobs_reclaimed,
            "tasks_dispatched": self._tasks_dispatched,
            "results_processed": self._results_processed,
            "errors": self._errors,
            "last_heartbeat_at": self._last_heartbeat_at.isoformat() if self._last_heartbeat_at else None,
            "last_orphan_scan_at": self._last_orphan_scan_at.isoformat() if self._last_orphan_scan_at else None,
        }
```

---

### 6. Gateway Endpoint (Platform Function App)

**File**: External - `rmhazuregeoapi/api/platform_routes.py`

**Uses**: `infrastructure/service_bus.py` (ServiceBusPublisher)

```python
"""
Platform Gateway - Job Submission Endpoint

This endpoint is the entry point for all job submissions.
It validates, generates identifiers, and enqueues to dag-jobs queue.
Uses ServiceBusPublisher from infrastructure layer.
"""

import os
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import Any, Dict, Optional
from datetime import datetime
import uuid

from infrastructure.service_bus import ServiceBusConfig, ServiceBusPublisher
from core.models import JobQueueMessage

router = APIRouter(prefix="/api/platform", tags=["Platform"])

# Publisher singleton - thread-safe
_publisher: Optional[ServiceBusPublisher] = None


def get_publisher() -> ServiceBusPublisher:
    """Get or create the Service Bus publisher singleton."""
    global _publisher
    if _publisher is None:
        config = ServiceBusConfig.from_env()
        _publisher = ServiceBusPublisher.get_instance(config)
    return _publisher


class JobSubmitRequest(BaseModel):
    """Request to submit a new job."""
    workflow_id: str = Field(..., max_length=64)
    input_params: Dict[str, Any] = Field(default_factory=dict)
    correlation_id: Optional[str] = Field(default=None, max_length=64)
    callback_url: Optional[str] = Field(default=None, max_length=512)
    priority: int = Field(default=0, ge=0, le=10)
    idempotency_key: Optional[str] = Field(default=None, max_length=64)


class JobSubmitResponse(BaseModel):
    """Response after job submission."""
    message_id: str
    workflow_id: str
    submitted_at: datetime
    status: str = "queued"


@router.post("/submit", response_model=JobSubmitResponse)
async def submit_job(request: JobSubmitRequest):
    """
    Submit a job for execution.

    This endpoint:
    1. Validates the request
    2. Creates JobQueueMessage with Pydantic
    3. Sends to dag-jobs queue via ServiceBusPublisher
    4. Returns immediately (async processing)

    The job will be picked up by an orchestrator instance.
    """
    submitted_at = datetime.utcnow()

    # Create queue message (Pydantic model)
    message = JobQueueMessage(
        workflow_id=request.workflow_id,
        input_params=request.input_params,
        correlation_id=request.correlation_id,
        callback_url=request.callback_url,
        priority=request.priority,
        submitted_by="platform-gateway",
        submitted_at=submitted_at,
        idempotency_key=request.idempotency_key,
    )

    # Send to queue using publisher singleton
    queue_name = os.environ.get("JOB_QUEUE_NAME", "dag-jobs")
    try:
        publisher = get_publisher()
        message_id = publisher.send_message(
            queue_name=queue_name,
            message=message,
            ttl_hours=24,
        )
    except Exception as e:
        raise HTTPException(500, f"Failed to enqueue job: {e}")

    return JobSubmitResponse(
        message_id=message_id,
        workflow_id=request.workflow_id,
        submitted_at=submitted_at,
    )
```

**Note**: The Gateway returns `message_id` (Service Bus message ID), not `job_id`. The actual `job_id` is created by the orchestrator when it claims the job from the queue. Clients can use `correlation_id` or `idempotency_key` for tracking.

---

## Implementation Phases

### Phase 0: Core Infrastructure (COMPLETE ✅)

| Step | Task | File | Status |
|------|------|------|--------|
| 0.1 | Extract BaseRepository patterns | `infrastructure/base_repository.py` | ✅ |
| 0.2 | Extract ServiceBusPublisher | `infrastructure/service_bus.py` | ✅ |
| 0.3 | Extract ServiceBusConsumer | `infrastructure/service_bus.py` | ✅ |
| 0.4 | Add ServiceBusConfig environment loader | `infrastructure/service_bus.py` | ✅ |
| 0.5 | Define status transition maps | `infrastructure/base_repository.py` | ✅ |

**Dependencies provided for subsequent phases**:
- `ServiceBusConsumer` → Used by `JobQueueRepository` (Phase 2)
- `ServiceBusPublisher` → Used by Gateway (Phase 6)
- `BaseRepository` → Pattern for all repository classes

### Phase 1: Data Model & Schema (M1, M2)

**Pattern**: Pydantic model IS the schema (IaC)

| Step | Task | File |
|------|------|------|
| 1.1 | Add `owner_id` field to Job model | `core/models/job.py` |
| 1.2 | Add `owner_heartbeat_at` field to Job model | `core/models/job.py` |
| 1.3 | Add indexes to `__sql_indexes__` ClassVar | `core/models/job.py` |
| 1.4 | Preview DDL: `GET /api/v1/bootstrap/ddl` | Bootstrap API |
| 1.5 | Apply schema: `POST /api/v1/bootstrap/migrate` | Bootstrap API |
| 1.6 | Update `_row_to_job()` to handle new columns | `repositories/job_repo.py` |

### Phase 2: Job Queue Infrastructure (M3, M4)

**Note**: `JobQueueRepository` now wraps `ServiceBusConsumer` from Phase 0, reducing implementation complexity significantly.

| Step | Task | File |
|------|------|------|
| 2.1 | Create `JobQueueMessage` Pydantic model | `core/models/job_queue_message.py` |
| 2.2 | Create `JobQueueRepository` (wraps `ServiceBusConsumer`) | `repositories/job_queue_repo.py` |
| 2.3 | Create `dag-jobs` Service Bus queue | Azure Portal / CLI |
| 2.4 | Add `JOB_QUEUE_NAME` to environment config | `messaging/config.py` |

### Phase 3: Repository Extensions (M5)

| Step | Task | File |
|------|------|------|
| 3.1 | Add `create_with_owner()` method | `repositories/job_repo.py` |
| 3.2 | Add `list_active_for_owner()` method | `repositories/job_repo.py` |
| 3.3 | Add `update_heartbeat()` method | `repositories/job_repo.py` |
| 3.4 | Add `reclaim_orphaned_jobs()` method | `repositories/job_repo.py` |
| 3.5 | Add `get_by_idempotency_key()` method | `repositories/job_repo.py` |

### Phase 4: Orchestrator Redesign (M6, M7, M8)

| Step | Task | File |
|------|------|------|
| 4.1 | Add `owner_id` property to Orchestrator | `orchestrator/loop.py` |
| 4.2 | Add `_queue_consumer_loop()` | `orchestrator/loop.py` |
| 4.3 | Add `_heartbeat_loop()` | `orchestrator/loop.py` |
| 4.4 | Add `_orphan_scan_loop()` | `orchestrator/loop.py` |
| 4.5 | Modify `start()` - remove lease, start loops | `orchestrator/loop.py` |
| 4.6 | Modify `_cycle()` - use owner-filtered query | `orchestrator/loop.py` |
| 4.7 | Update stats property | `orchestrator/loop.py` |

### Phase 5: Remove Legacy (M8 continued)

| Step | Task | File |
|------|------|------|
| 5.1 | Remove lease acquisition from `start()` | `orchestrator/loop.py` |
| 5.2 | Remove `LockService` dependency | `orchestrator/loop.py` |
| 5.3 | Remove `dag_orchestrator_lease` table | Bootstrap API |
| 5.4 | Delete or deprecate `infrastructure/locking.py` Layer 1 | `infrastructure/locking.py` |

### Phase 6: Gateway Integration (M9)

**Note**: Gateway uses `ServiceBusPublisher` directly from Phase 0 - no additional repository needed.

| Step | Task | File |
|------|------|------|
| 6.1 | ~~Create `JobQueuePublisher` repository~~ | N/A - Use `ServiceBusPublisher` directly |
| 6.2 | Create `/api/platform/submit` endpoint | External: Platform FA |
| 6.3 | Update Platform FA to use new endpoint | External: Platform FA |
| 6.4 | Deprecate direct `/api/v1/jobs` POST | `api/routes.py` |

---

## Configuration

### Environment Variables

```bash
# Multi-orchestrator settings
HEARTBEAT_INTERVAL_SEC=30      # How often to update heartbeat
ORPHAN_THRESHOLD_SEC=120       # How long before job is orphaned
ORPHAN_SCAN_INTERVAL_SEC=60    # How often to scan for orphans

# Job queue
JOB_QUEUE_NAME=dag-jobs        # Service Bus queue name
```

### Service Bus Queue Configuration

```bash
# Create queue via Azure CLI
az servicebus queue create \
  --resource-group rmhazure_rg \
  --namespace-name rmhazure \
  --name dag-jobs \
  --max-size 1024 \
  --default-message-time-to-live P1D \
  --lock-duration PT5M \
  --max-delivery-count 10
```

---

## Testing Strategy

### Unit Tests

| Test | Description |
|------|-------------|
| `test_job_queue_message_serialization` | Round-trip JSON serialization |
| `test_create_with_owner` | Job creation with ownership |
| `test_list_active_for_owner` | Owner filtering works |
| `test_heartbeat_update` | Heartbeat updates correct jobs |
| `test_orphan_reclaim` | Stale jobs get reclaimed |
| `test_orphan_race_condition` | Only one orchestrator wins reclaim |

### Integration Tests

| Test | Description |
|------|-------------|
| `test_queue_consumer_claims_job` | Message → Job with owner_id |
| `test_heartbeat_keeps_ownership` | Jobs not orphaned while heartbeat active |
| `test_crash_recovery` | Simulated crash → orphan reclaim |
| `test_multiple_orchestrators` | Two instances process different jobs |

### Load Tests

| Test | Description |
|------|-------------|
| `test_100_concurrent_jobs` | Queue → 100 jobs → completion |
| `test_orchestrator_failover` | Kill one instance, verify jobs continue |

---

## Rollback Plan

If issues arise:

1. **Revert to single-orchestrator**: Re-enable lease check in `start()`
2. **Keep direct job creation**: `/api/v1/jobs` continues to work
3. **Schema is additive**: `owner_id` columns can be ignored

The old direct-creation path via `/api/v1/jobs` can remain as fallback.

---

## Success Criteria

- [ ] Multiple orchestrator instances can run simultaneously
- [ ] Each job is processed by exactly one orchestrator
- [ ] Crashed orchestrator's jobs are recovered within 2 minutes
- [ ] No job duplication or loss
- [ ] Health check shows all instances healthy
- [ ] Metrics show even job distribution
