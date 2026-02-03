# ADVANCED.md - Orchestrator State Management & Implementation Plan

**Last Updated**: 02 FEB 2026
**Epoch**: 5 - DAG Orchestration
**Status**: Architecture Analysis & Roadmap

---

## Table of Contents

1. [Current State Architecture](#1-current-state-architecture)
2. [Python Objects & SQL Entities](#2-python-objects--sql-entities)
3. [State Machine Definitions](#3-state-machine-definitions)
4. [State Flow Through the System](#4-state-flow-through-the-system)
5. [Implementation Gaps](#5-implementation-gaps)
6. [Orchestrator Wishlist](#6-orchestrator-wishlist)
7. [Implementation Plan: P0 Critical](#7-implementation-plan-p0-critical)
8. [Implementation Plan: P1 Important](#8-implementation-plan-p1-important)
9. [Implementation Plan: P2 Advanced Features](#9-implementation-plan-p2-advanced-features)
10. [Implementation Plan: P3 Observability](#10-implementation-plan-p3-observability)
11. [Implementation Priority Summary](#11-implementation-priority-summary)
12. [Concurrency Control & Locking Implementation](#12-concurrency-control--locking-implementation)

---

## 1. Current State Architecture

### 1.1 The Core State Model

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         ORCHESTRATOR STATE                              │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│   JOB (dag_jobs)                                                       │
│   ├── Identity: job_id, workflow_id                                    │
│   ├── Status: PENDING → RUNNING → COMPLETED|FAILED|CANCELLED           │
│   ├── Timing: created_at, started_at, completed_at                     │
│   ├── Data: input_params (JSONB), result_data (JSONB)                  │
│   └── Metadata: correlation_id, submitted_by                           │
│                                                                         │
│   NODE STATE (dag_node_states) - one per workflow node per job         │
│   ├── Identity: (job_id, node_id) composite key                        │
│   ├── Status: PENDING → READY → DISPATCHED → RUNNING → COMPLETED       │
│   ├── Task Link: task_id (set on dispatch)                             │
│   ├── Timing: dispatched_at, started_at, completed_at                  │
│   ├── Output: output (JSONB), error_message                            │
│   └── Retry: retry_count, max_retries                                  │
│                                                                         │
│   TASK RESULT (dag_task_results) - worker reports here                 │
│   ├── Identity: task_id                                                │
│   ├── Status: RUNNING → COMPLETED|FAILED                               │
│   ├── Result: output (JSONB), error_message                            │
│   ├── Timing: reported_at, execution_duration_ms                       │
│   └── Flag: processed (boolean) - orchestrator marks after reading     │
│                                                                         │
│   JOB EVENT (dag_job_events) - audit trail                             │
│   ├── Identity: event_id (serial), job_id, node_id, task_id            │
│   ├── Type: 15 event types (JOB_CREATED, NODE_DISPATCHED, etc.)        │
│   └── Data: event_data (JSONB), duration_ms, checkpoint_name           │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 1.2 Design Principles

1. **Database as Single Source of Truth**: All durable state lives in PostgreSQL. The orchestrator is stateless and can restart without losing work.

2. **Polling-Based Coordination**: No event bus or pub/sub—just polling. The orchestrator polls `dag_task_results` for work, processes results, and updates `dag_node_states`.

3. **Idempotency via Conditionals**: Updates use `WHERE status = 'ready'` to prevent double-dispatch. The `processed` flag prevents double-processing of results.

4. **dict_row Pattern**: All queries use psycopg3's `dict_row` factory. Column access is always by name, never by index.

5. **Composite Keys**: NodeState uses `(job_id, node_id)` composite primary key, correctly modeling "many nodes per job."

---

## 2. Python Objects & SQL Entities

### 2.1 Entity Mapping

| Python Model | SQL Table | Primary Key | Key Files |
|--------------|-----------|-------------|-----------|
| `Job` | `dagapp.dag_jobs` | `job_id` | `core/models/job.py`, `repositories/job_repo.py` |
| `NodeState` | `dagapp.dag_node_states` | `(job_id, node_id)` | `core/models/node.py`, `repositories/node_repo.py` |
| `TaskResult` | `dagapp.dag_task_results` | `task_id` | `core/models/task.py`, `repositories/task_repo.py` |
| `JobEvent` | `dagapp.dag_job_events` | `event_id` (serial) | `core/models/events.py` |
| `TaskMessage` | *(Service Bus JSON)* | — | `core/models/task.py` |

### 2.2 Job Model (`core/models/job.py`)

```python
class Job(BaseModel):
    # Identity
    job_id: str                    # UUID hex string
    workflow_id: str               # References workflow definition

    # Status
    status: JobStatus              # PENDING, RUNNING, COMPLETED, FAILED, CANCELLED

    # Data
    input_params: Dict[str, Any]   # Workflow inputs (JSONB)
    result_data: Optional[Dict]    # Aggregated results (JSONB)
    error_message: Optional[str]   # Max 2000 chars

    # Timing
    created_at: datetime
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    updated_at: datetime

    # Metadata
    submitted_by: Optional[str]
    correlation_id: Optional[str]
    metadata: Dict[str, Any]       # Flexible tags, source info

    # Computed
    @property
    def is_terminal(self) -> bool
    @property
    def duration_seconds(self) -> Optional[float]

    # State machine
    def can_transition_to(self, new_status: JobStatus) -> bool
    def mark_started(self) -> None
    def mark_completed(self, result_data: Dict) -> None
    def mark_failed(self, error_message: str) -> None
    def mark_cancelled(self) -> None
```

### 2.3 NodeState Model (`core/models/node.py`)

```python
class NodeState(BaseModel):
    # Identity (composite key)
    job_id: str
    node_id: str

    # Status
    status: NodeStatus             # PENDING, READY, DISPATCHED, RUNNING,
                                   # COMPLETED, FAILED, SKIPPED

    # Task linkage
    task_id: Optional[str]         # Set when dispatched

    # Execution
    output: Optional[Dict]         # Handler result (JSONB)
    error_message: Optional[str]

    # Retry
    retry_count: int = 0
    max_retries: int = 3

    # Timing
    created_at: datetime
    dispatched_at: Optional[datetime]
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    updated_at: datetime

    # Dynamic nodes (fan-out)
    parent_node_id: Optional[str]
    fan_out_index: Optional[int]

    # Checkpoint (resumable tasks)
    checkpoint_phase: Optional[str]
    checkpoint_data: Optional[Dict]
    checkpoint_saved_at: Optional[datetime]

    # Computed
    @property
    def is_terminal(self) -> bool
    @property
    def is_successful(self) -> bool
    @property
    def is_dynamic(self) -> bool

    # State machine
    def can_transition_to(self, new_status: NodeStatus) -> bool
    def mark_ready(self) -> None
    def mark_dispatched(self, task_id: str) -> None
    def mark_running(self) -> None
    def mark_completed(self, output: Dict) -> None
    def mark_failed(self, error_message: str) -> None
    def mark_skipped(self) -> None
    def prepare_retry(self) -> None
```

### 2.4 TaskResult Model (`core/models/task.py`)

```python
class TaskResult(BaseModel):
    # Identity
    task_id: str
    job_id: str
    node_id: str

    # Status
    status: TaskStatus             # RECEIVED, RUNNING, COMPLETED, FAILED

    # Result
    output: Optional[Dict]
    error_message: Optional[str]

    # Execution metadata
    worker_id: Optional[str]
    execution_duration_ms: Optional[int]

    # Progress (long-running tasks)
    progress_current: Optional[int]
    progress_total: Optional[int]
    progress_percent: Optional[float]
    progress_message: Optional[str]

    # Timing
    reported_at: datetime

    # Processing flag
    processed: bool = False        # Orchestrator sets True after reading
```

### 2.5 JobEvent Model (`core/models/events.py`)

```python
class JobEvent(BaseModel):
    # Identity
    event_id: Optional[int]        # Auto-increment
    job_id: str
    node_id: Optional[str]
    task_id: Optional[str]

    # Event details
    event_type: EventType          # 15 types (see below)
    event_status: EventStatus      # SUCCESS, FAILURE, WARNING, INFO
    checkpoint_name: Optional[str] # For Application Insights correlation

    # Data
    event_data: Dict[str, Any]     # Flexible JSONB
    error_message: Optional[str]
    duration_ms: Optional[int]

    # Timing
    created_at: datetime

    # Source
    source_app: str                # "orchestrator", "worker", etc.
```

**Event Types** (`core/contracts.py`):
```python
class EventType(str, Enum):
    # Job lifecycle
    JOB_CREATED = "job_created"
    JOB_STARTED = "job_started"
    JOB_COMPLETED = "job_completed"
    JOB_FAILED = "job_failed"
    JOB_CANCELLED = "job_cancelled"

    # Node lifecycle
    NODE_READY = "node_ready"
    NODE_DISPATCHED = "node_dispatched"
    NODE_RUNNING = "node_running"
    NODE_COMPLETED = "node_completed"
    NODE_FAILED = "node_failed"
    NODE_SKIPPED = "node_skipped"
    NODE_RETRYING = "node_retrying"

    # Orchestrator
    ORCHESTRATOR_CYCLE = "orchestrator_cycle"
    ORCHESTRATOR_ERROR = "orchestrator_error"
    ORCHESTRATOR_STALL = "orchestrator_stall"
```

---

## 3. State Machine Definitions

### 3.1 Job State Machine

```
                    ┌─────────────┐
                    │   PENDING   │
                    └──────┬──────┘
                           │ first node dispatched
                           ▼
                    ┌─────────────┐
         ┌──────────│   RUNNING   │──────────┐
         │          └──────┬──────┘          │
         │                 │                 │
         │ manual cancel   │                 │ manual cancel
         │                 │                 │
         ▼                 │                 ▼
┌─────────────┐           │          ┌─────────────┐
│  CANCELLED  │           │          │  CANCELLED  │
└─────────────┘           │          └─────────────┘
                          │
           ┌──────────────┴──────────────┐
           │                             │
           │ all nodes terminal,         │ any node failed
           │ none failed                 │
           ▼                             ▼
    ┌─────────────┐               ┌─────────────┐
    │  COMPLETED  │               │   FAILED    │
    └─────────────┘               └─────────────┘
```

**Valid Transitions**:
| From | To | Trigger |
|------|-----|---------|
| PENDING | RUNNING | First node dispatched |
| PENDING | CANCELLED | Manual cancellation |
| RUNNING | COMPLETED | All nodes terminal, none failed |
| RUNNING | FAILED | Any node failed |
| RUNNING | CANCELLED | Manual cancellation |

### 3.2 Node State Machine

```
┌─────────────┐
│   PENDING   │
└──────┬──────┘
       │ dependencies met
       ▼
┌─────────────┐      conditional
│    READY    │─────────────────────┐
└──────┬──────┘                     │
       │ dispatched to queue        │
       ▼                            ▼
┌─────────────┐               ┌─────────────┐
│ DISPATCHED  │               │   SKIPPED   │
└──────┬──────┘               └─────────────┘
       │ worker picked up           (terminal)
       ▼
┌─────────────┐
│   RUNNING   │
└──────┬──────┘
       │
       ├─────────────────────┐
       │ success             │ error
       ▼                     ▼
┌─────────────┐       ┌─────────────┐
│  COMPLETED  │       │   FAILED    │
└─────────────┘       └──────┬──────┘
    (terminal)               │
                             │ retry_count < max_retries
                             ▼
                      ┌─────────────┐
                      │    READY    │ (retry)
                      └─────────────┘
```

**Valid Transitions**:
| From | To | Trigger |
|------|-----|---------|
| PENDING | READY | Dependencies satisfied |
| PENDING | SKIPPED | Conditional branch not taken |
| READY | DISPATCHED | Task sent to queue |
| READY | SKIPPED | Conditional evaluation |
| DISPATCHED | RUNNING | Worker reports RUNNING |
| DISPATCHED | FAILED | Dispatch timeout |
| RUNNING | COMPLETED | Handler success |
| RUNNING | FAILED | Handler error |
| FAILED | READY | Retry (if retries remain) |

---

## 4. State Flow Through the System

### 4.1 Main Orchestration Loop

**File**: `orchestrator/loop.py`

```
┌─────────────────────────────────────────────────────────────────┐
│                    ORCHESTRATOR MAIN LOOP                       │
│                  (repeats every poll_interval)                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Step 1: PROCESS RESULTS                                        │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ SELECT * FROM dag_task_results WHERE processed = false   │   │
│  │                                                          │   │
│  │ For each result:                                         │   │
│  │   ├─ Load Job from database                              │   │
│  │   ├─ Load WorkflowDefinition                             │   │
│  │   ├─ Call node_service.process_task_result()             │   │
│  │   │   ├─ Update NodeState based on result.status         │   │
│  │   │   └─ Persist NodeState                               │   │
│  │   └─ Mark result as processed = true                     │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  Step 2: GET ACTIVE JOBS                                        │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ SELECT * FROM dag_jobs                                   │   │
│  │ WHERE status IN ('pending', 'running')                   │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  Step 3: PROCESS EACH JOB                                       │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ For each active job:                                     │   │
│  │   ├─ Load workflow definition                            │   │
│  │   ├─ Check dependencies → mark PENDING nodes as READY    │   │
│  │   ├─ Get all READY nodes                                 │   │
│  │   ├─ For each READY node:                                │   │
│  │   │   ├─ Create TaskMessage with resolved params         │   │
│  │   │   ├─ Mark node as DISPATCHED                         │   │
│  │   │   ├─ Send to Service Bus queue                       │   │
│  │   │   └─ If first dispatch, mark Job as RUNNING          │   │
│  │   └─ Check if job is complete                            │   │
│  │       ├─ All nodes terminal + none failed → COMPLETED    │   │
│  │       └─ Any node failed → FAILED                        │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 4.2 Dependency Resolution

**File**: `orchestrator/engine/evaluator.py`

```python
def find_ready_nodes(workflow, node_states) -> List[str]:
    """
    For each PENDING node, check if dependencies are satisfied.

    Dependencies come from:
    1. Explicit depends_on.all_of (AND semantics)
    2. Explicit depends_on.any_of (OR semantics)
    3. Implicit "previous" link in linear workflows

    A node is ready when:
    - All all_of dependencies are COMPLETED or SKIPPED
    - At least one any_of dependency is COMPLETED or SKIPPED
    """
```

### 4.3 Template Resolution

**File**: `orchestrator/engine/templates.py`

```python
def resolve_params(params, job_params, node_outputs) -> Dict:
    """
    Resolve {{ }} expressions in node parameters.

    Supported patterns:
    - {{ inputs.param_name }}          # From job input_params
    - {{ nodes.node_id.output.field }} # From completed node output
    - {{ nodes.node_id.status }}       # Node status
    - {{ env.VAR_NAME }}               # Environment variable
    """
```

### 4.4 Worker Result Flow

```
┌──────────────────┐     ┌───────────────────┐     ┌──────────────────┐
│      WORKER      │     │   dag_task_results │     │   ORCHESTRATOR   │
├──────────────────┤     ├───────────────────┤     ├──────────────────┤
│                  │     │                   │     │                  │
│  Execute handler │     │                   │     │                  │
│        │         │     │                   │     │                  │
│        ▼         │     │                   │     │                  │
│  Report RUNNING  │────▶│ INSERT (processed │     │                  │
│                  │     │  = false)         │     │                  │
│        │         │     │                   │     │                  │
│        ▼         │     │                   │     │                  │
│  Execute...      │     │                   │     │  Poll loop       │
│        │         │     │                   │     │     │            │
│        ▼         │     │                   │     │     ▼            │
│  Report COMPLETED│────▶│ INSERT (processed │◀────│  SELECT WHERE   │
│  or FAILED       │     │  = false)         │     │  processed=false │
│                  │     │                   │     │     │            │
│                  │     │                   │     │     ▼            │
│                  │     │                   │     │  Update NodeState│
│                  │     │                   │     │     │            │
│                  │     │ UPDATE processed  │◀────│  Mark processed │
│                  │     │  = true           │     │                  │
└──────────────────┘     └───────────────────┘     └──────────────────┘
```

---

## 5. Implementation Gaps

### 5.1 Status Overview

| Feature | Models Ready | DB Schema | Implemented | Priority |
|---------|-------------|-----------|-------------|----------|
| Basic job/node tracking | ✅ | ✅ | ✅ | — |
| Task dispatch/result | ✅ | ✅ | ✅ | — |
| **Event timeline logging** | ✅ | ✅ | ❌ | P0 |
| **Retry logic** | ✅ | ✅ | ❌ | P0 |
| **Orchestrator stats endpoint** | Partial | — | ❌ | P0 |
| **Stale state bug** | — | — | ❌ (bug) | P1 |
| **Workflow version pinning** | Partial | ❌ | ❌ | P1 |
| **Transaction wrapping** | — | — | ❌ | P1 |
| Conditional routing | ✅ | ✅ | ❌ | P2 |
| Fan-out/fan-in | ✅ | ✅ | ❌ | P2 |
| Checkpointing | ✅ | ✅ | ❌ | P2 |
| Progress API | ✅ | ✅ | ❌ | P3 |
| Timeline API | ✅ | ✅ | ❌ | P3 |
| Metrics dashboard | — | — | ❌ | P3 |

### 5.2 Gap Details

#### P0.1: Event Timeline Logging (NOT IMPLEMENTED)

**Problem**: The `JobEvent` model and `dag_job_events` table exist, but no code writes events.

**Impact**:
- Cannot audit execution timeline
- Cannot debug silent failures
- No visibility into orchestrator decisions

**Missing writes at**:
- `JobService.create_job()` → JOB_CREATED
- `JobService.mark_job_started()` → JOB_STARTED
- `NodeService.check_and_ready_nodes()` → NODE_READY
- `NodeService.mark_dispatched()` → NODE_DISPATCHED
- `Orchestrator._dispatch_node()` → NODE_DISPATCHED
- `NodeService.process_task_result()` → NODE_RUNNING/COMPLETED/FAILED

#### P0.2: Retry Logic (NOT INTEGRATED)

**Problem**: Fields exist (`retry_count`, `max_retries`, `prepare_retry()`), but never called.

**Impact**: Failed nodes stay failed; no automatic retry.

**Missing**:
```python
# After marking node FAILED:
if node.retry_count < node.max_retries:
    node.prepare_retry()  # Increments retry_count, sets status to READY
    await node_repo.update(node)
    # Node will be re-dispatched on next cycle
```

#### P0.3: Orchestrator Stats Endpoint (PARTIAL)

**Problem**: `Orchestrator.stats` property exists but no HTTP endpoint exposes it.

**Impact**: Cannot monitor orchestrator health from outside.

**Missing**: `GET /api/v1/orchestrator/status`

#### P1.1: Stale State Bug

**Problem**: The END node completes before task nodes because the evaluator uses stale `node_states` from the start of the cycle.

**Root cause** (`loop.py`):
```python
# At start of _process_job():
node_states = await node_repo.get_all_for_job(job_id)  # Snapshot

# Later, after dispatching nodes:
# node_states is stale - doesn't reflect dispatches

# _check_job_completion() uses stale node_states
# Sees START=COMPLETED, echo_handler=PENDING, END=PENDING
# Since START is completed and END has no depends_on checking task nodes,
# END gets marked READY and completes
```

**Fix**: Re-fetch node states before completion check, or track dispatched nodes in memory.

#### P1.2: Workflow Version Pinning (NOT TRACKED)

**Problem**: Job stores `workflow_id` but not the version. If workflow changes, running jobs use new definition.

**Impact**: In-flight jobs could break if workflow is updated.

**Missing**:
- Add `workflow_version` column to `dag_jobs`
- Store version at job creation
- Load correct version when processing

#### P1.3: Transaction Wrapping (MISSING)

**Problem**: Multi-step updates done in separate queries.

**Example** (`loop.py`):
```python
# Step 1: Mark node DISPATCHED
await node_repo.mark_dispatched(job_id, node_id, task_id)

# Step 2: Mark job RUNNING (if first dispatch)
await job_repo.update_status(job_id, JobStatus.RUNNING)

# If crash between steps: node dispatched but job still PENDING
```

**Fix**: Wrap in transaction.

---

## 6. Orchestrator Wishlist

*"What would I want if I were an Orchestrator?"*

### 6.1 Self-Awareness State

```python
class OrchestratorState:
    # Identity
    instance_id: str               # Who am I? (for multi-instance future)
    started_at: datetime           # When did I wake up?
    last_heartbeat: datetime       # Am I still alive?

    # Performance
    cycles_completed: int          # Total poll cycles
    cycle_duration_avg_ms: float   # How fast am I?

    # Throughput
    jobs_processed_total: int
    nodes_dispatched_total: int
    results_processed_total: int

    # Current workload
    active_jobs: int               # How busy am I?
    pending_results: int           # Backlog to process

    # Errors
    last_error: Optional[str]
    error_count: int
```

### 6.2 Activity Tracking

```python
class OrchestratorActivity:
    # Current cycle
    current_cycle_id: str
    cycle_started_at: datetime
    phase: str  # "polling_results", "evaluating_jobs", "dispatching", "idle"

    # Current focus
    processing_job_id: Optional[str]
    processing_node_id: Optional[str]

    # Queue depths
    service_bus_queue_depth: int   # Messages waiting
    db_unprocessed_results: int    # Results to handle
```

### 6.3 Historical Awareness

```python
class OrchestratorMemory:
    # Recent outcomes (sliding window)
    recent_completions: List[JobSummary]  # Last 100
    recent_failures: List[JobSummary]     # Last 100

    # Failure patterns
    failing_handlers: Dict[str, int]      # Handler → failure count
    failing_queues: Dict[str, int]        # Queue → failure count

    # Workflow stats
    workflow_avg_duration: Dict[str, float]   # workflow_id → avg seconds
    workflow_success_rate: Dict[str, float]   # workflow_id → % success
```

### 6.4 Event Emissions

Every significant action should emit a `JobEvent`:

```python
# Job lifecycle
JOB_CREATED      # with input_params snapshot
JOB_STARTED      # first node dispatched
JOB_COMPLETED    # with result summary
JOB_FAILED       # with error details

# Node lifecycle
NODE_READY       # dependencies met
NODE_DISPATCHED  # sent to queue (with queue name, task_id)
NODE_RUNNING     # worker picked up
NODE_COMPLETED   # with output summary
NODE_FAILED      # with error + retry decision
NODE_RETRYING    # retry attempt starting
NODE_SKIPPED     # conditional branch not taken

# Orchestrator operations
ORCHESTRATOR_CYCLE_START
ORCHESTRATOR_CYCLE_END    # with stats
ORCHESTRATOR_STALL        # no progress detected
ORCHESTRATOR_BACKPRESSURE # queue full, slowing down
```

### 6.5 Desired APIs

```
GET /api/v1/orchestrator/status
  → is_running, uptime, current_phase, active_jobs, pending_results

GET /api/v1/orchestrator/stats
  → cycles, throughput, avg_latency, error_rate

GET /api/v1/orchestrator/activity
  → current cycle, what job/node being processed

GET /api/v1/jobs/{job_id}/timeline
  → all events for this job in chronological order

GET /api/v1/jobs/{job_id}/graph
  → node states with visual representation

GET /api/v1/metrics/workflows
  → per-workflow success rate, avg duration, common failures
```

### 6.6 Self-Healing Behaviors

```python
# Stall detection
if no_progress_for(minutes=5):
    log_warning("Potential stall", job_id=job_id)
    emit_event(ORCHESTRATOR_STALL, job_id=job_id)

# Zombie job cleanup
if job.status == RUNNING and job.started_at < hours_ago(24):
    mark_as_failed("Timeout: no progress for 24 hours")

# Stuck dispatch detection
if node.status == DISPATCHED and node.dispatched_at < minutes_ago(30):
    log_warning("Node stuck in DISPATCHED", node_id=node_id)
    # Consider: re-dispatch or fail?

# Backpressure
if queue_depth > threshold:
    slow_down_dispatch_rate()
    emit_event(ORCHESTRATOR_BACKPRESSURE)
```

---

## 7. Implementation Plan: P0 Critical

### P0.1: Event Timeline Logging

**Goal**: Write `JobEvent` records at all lifecycle points for audit trail.

#### Step 1: Create Event Repository

**File**: `repositories/event_repo.py`

```python
class EventRepository:
    async def create(self, event: JobEvent) -> JobEvent:
        """Insert event and return with event_id."""

    async def get_for_job(self, job_id: str, limit: int = 100) -> List[JobEvent]:
        """Get all events for a job, newest first."""

    async def get_for_node(self, job_id: str, node_id: str) -> List[JobEvent]:
        """Get events for a specific node."""
```

#### Step 2: Create Event Service

**File**: `services/event_service.py`

```python
class EventService:
    def __init__(self, pool: AsyncConnectionPool):
        self._repo = EventRepository(pool)

    async def emit(
        self,
        event_type: EventType,
        job_id: str,
        node_id: Optional[str] = None,
        task_id: Optional[str] = None,
        status: EventStatus = EventStatus.SUCCESS,
        data: Optional[Dict] = None,
        error_message: Optional[str] = None,
        duration_ms: Optional[int] = None,
    ) -> None:
        """Emit an event. Fire-and-forget (logs errors, doesn't raise)."""

    async def emit_job_created(self, job: Job) -> None:
    async def emit_job_started(self, job: Job) -> None:
    async def emit_job_completed(self, job: Job) -> None:
    async def emit_job_failed(self, job: Job) -> None:
    async def emit_node_ready(self, node: NodeState) -> None:
    async def emit_node_dispatched(self, node: NodeState, queue: str) -> None:
    async def emit_node_completed(self, node: NodeState) -> None:
    async def emit_node_failed(self, node: NodeState) -> None:
```

#### Step 3: Inject Event Service

**Files to modify**:
- `main.py`: Create EventService in lifespan, pass to services
- `services/job_service.py`: Add event emissions
- `services/node_service.py`: Add event emissions
- `orchestrator/loop.py`: Add event emissions

#### Step 4: Add Events in Lifecycle Points

| Location | Event Type | Data |
|----------|------------|------|
| `JobService.create_job()` | JOB_CREATED | `{workflow_id, input_params}` |
| `Orchestrator._dispatch_node()` (first) | JOB_STARTED | `{first_node: node_id}` |
| `Orchestrator._check_job_completion()` | JOB_COMPLETED | `{duration_seconds, node_count}` |
| `Orchestrator._check_job_completion()` | JOB_FAILED | `{failed_nodes: [...]}` |
| `NodeService.check_and_ready_nodes()` | NODE_READY | `{dependencies_met: [...]}` |
| `Orchestrator._dispatch_node()` | NODE_DISPATCHED | `{queue, task_id}` |
| `NodeService.process_task_result()` | NODE_RUNNING | `{worker_id}` |
| `NodeService.process_task_result()` | NODE_COMPLETED | `{output_summary, duration_ms}` |
| `NodeService.process_task_result()` | NODE_FAILED | `{error_message, will_retry}` |

#### Step 5: Add Timeline API Endpoint

**File**: `api/routes.py`

```python
@router.get("/jobs/{job_id}/timeline")
async def get_job_timeline(job_id: str) -> List[EventResponse]:
    """Get all events for a job in chronological order."""
```

#### Estimated Changes

| File | Changes |
|------|---------|
| `repositories/event_repo.py` | New file (~100 lines) |
| `services/event_service.py` | New file (~150 lines) |
| `main.py` | Add EventService init (~10 lines) |
| `services/job_service.py` | Add 3 emit calls (~15 lines) |
| `services/node_service.py` | Add 4 emit calls (~20 lines) |
| `orchestrator/loop.py` | Add 4 emit calls (~20 lines) |
| `api/routes.py` | Add 1 endpoint (~20 lines) |
| `api/schemas.py` | Add EventResponse (~15 lines) |

---

### P0.2: Retry Logic

**Goal**: Automatically retry failed nodes up to `max_retries` times.

#### Step 1: Implement Retry in process_task_result

**File**: `services/node_service.py`

```python
async def process_task_result(self, result: TaskResult, workflow) -> bool:
    node = await self._node_repo.get(result.job_id, result.node_id)

    if result.status == TaskStatus.COMPLETED:
        node.mark_completed(result.output)

    elif result.status == TaskStatus.FAILED:
        node.mark_failed(result.error_message)

        # NEW: Check for retry
        if node.retry_count < node.max_retries:
            node.prepare_retry()  # Sets status=READY, increments retry_count
            # Emit NODE_RETRYING event
            await self._event_service.emit_node_retrying(node)
        else:
            # Emit NODE_FAILED (final) event
            await self._event_service.emit_node_failed(node, final=True)

    await self._node_repo.update(node)
    return True
```

#### Step 2: Ensure prepare_retry() Works

**File**: `core/models/node.py` (already exists, verify):

```python
def prepare_retry(self) -> None:
    """Reset node for retry attempt."""
    if not self.can_retry:
        raise ValueError(f"Cannot retry: {self.retry_count}/{self.max_retries}")

    self.status = NodeStatus.READY
    self.retry_count += 1
    self.task_id = None
    self.dispatched_at = None
    self.started_at = None
    self.error_message = None
    self.updated_at = datetime.utcnow()
```

#### Step 3: Configure max_retries from Workflow

**File**: `orchestrator/loop.py`

When creating NodeState for a job, read `max_retries` from workflow node definition:

```python
# In _create_node_states():
for node_def in workflow.nodes.values():
    node = NodeState(
        job_id=job_id,
        node_id=node_def.id,
        max_retries=node_def.retries if hasattr(node_def, 'retries') else 3,
        # ...
    )
```

#### Estimated Changes

| File | Changes |
|------|---------|
| `services/node_service.py` | Add retry logic (~20 lines) |
| `core/models/node.py` | Verify prepare_retry() exists |
| `orchestrator/loop.py` | Read max_retries from workflow (~5 lines) |

---

### P0.3: Orchestrator Stats Endpoint

**Goal**: Expose orchestrator health and metrics via HTTP.

#### Step 1: Enhance Orchestrator.stats Property

**File**: `orchestrator/loop.py`

```python
@property
def stats(self) -> Dict[str, Any]:
    return {
        "is_running": self._running,
        "started_at": self._started_at.isoformat() if self._started_at else None,
        "uptime_seconds": (datetime.utcnow() - self._started_at).total_seconds()
                          if self._started_at else 0,
        "cycles_completed": self._cycles,
        "last_cycle_at": self._last_cycle_at.isoformat() if self._last_cycle_at else None,
        "tasks_dispatched": self._tasks_dispatched,
        "results_processed": self._results_processed,
        "errors": self._error_count,
        "last_error": self._last_error,
    }
```

#### Step 2: Add HTTP Endpoint

**File**: `api/routes.py`

```python
@router.get("/orchestrator/status", tags=["Orchestrator"])
async def get_orchestrator_status():
    """Get orchestrator health and metrics."""
    if _orchestrator is None:
        raise HTTPException(503, "Orchestrator not initialized")

    return {
        "status": "running" if _orchestrator.is_running else "stopped",
        **_orchestrator.stats,
    }
```

#### Step 3: Track Additional Metrics

**File**: `orchestrator/loop.py`

Add tracking in `__init__`:
```python
self._started_at: Optional[datetime] = None
self._last_cycle_at: Optional[datetime] = None
self._error_count: int = 0
self._last_error: Optional[str] = None
```

Update in `start()`:
```python
self._started_at = datetime.utcnow()
```

Update in `_run_loop()`:
```python
self._last_cycle_at = datetime.utcnow()
```

Catch errors:
```python
except Exception as e:
    self._error_count += 1
    self._last_error = str(e)[:500]
    logger.exception(...)
```

#### Estimated Changes

| File | Changes |
|------|---------|
| `orchestrator/loop.py` | Add tracking fields + enhanced stats (~30 lines) |
| `api/routes.py` | Add endpoint (~15 lines) |

---

## 8. Implementation Plan: P1 Important

### P1.1: Fix Stale State Bug

**Goal**: Prevent END node from completing before task nodes.

#### Problem Analysis

In `orchestrator/loop.py`, the `_process_job()` method:

1. Fetches all node_states at the start
2. Dispatches READY nodes (which updates DB but not in-memory list)
3. Calls `_check_job_completion()` with stale node_states
4. END node appears PENDING (dependencies met from stale view)
5. END gets marked READY and completes prematurely

#### Solution: Re-fetch Before Completion Check

**File**: `orchestrator/loop.py`

```python
async def _process_job(self, job: Job) -> None:
    workflow = self._workflow_service.get(job.workflow_id)

    # Step 1: Ready pending nodes
    await self._node_service.check_and_ready_nodes(job.job_id, workflow)

    # Step 2: Dispatch ready nodes
    node_states = await self._node_repo.get_all_for_job(job.job_id)
    ready_nodes = [n for n in node_states if n.status == NodeStatus.READY]

    for node in ready_nodes:
        await self._dispatch_node(job, node, workflow)

    # Step 3: RE-FETCH before completion check (FIX)
    node_states = await self._node_repo.get_all_for_job(job.job_id)

    # Step 4: Check completion with fresh state
    await self._check_job_completion(job, node_states, workflow)
```

#### Alternative: Track Dispatched In-Memory

```python
async def _process_job(self, job: Job) -> None:
    # ...
    dispatched_node_ids = set()

    for node in ready_nodes:
        if await self._dispatch_node(job, node, workflow):
            dispatched_node_ids.add(node.node_id)

    # Update in-memory state
    for node in node_states:
        if node.node_id in dispatched_node_ids:
            node.status = NodeStatus.DISPATCHED

    await self._check_job_completion(job, node_states, workflow)
```

#### Estimated Changes

| File | Changes |
|------|---------|
| `orchestrator/loop.py` | Re-fetch before completion check (~5 lines) |

---

### P1.2: Workflow Version Pinning

**Goal**: Store workflow version with job; load correct version when processing.

#### Step 1: Add Column to dag_jobs

**Migration SQL**:
```sql
ALTER TABLE dagapp.dag_jobs
ADD COLUMN workflow_version INTEGER DEFAULT 1;
```

#### Step 2: Update Job Model

**File**: `core/models/job.py`

```python
class Job(BaseModel):
    # ...existing...
    workflow_version: int = 1  # NEW
```

#### Step 3: Store Version on Job Creation

**File**: `services/job_service.py`

```python
async def create_job(self, workflow_id: str, input_params: Dict, ...) -> Job:
    workflow = self._workflow_service.get(workflow_id)

    job = Job(
        job_id=uuid.uuid4().hex,
        workflow_id=workflow_id,
        workflow_version=workflow.version,  # NEW
        # ...
    )
```

#### Step 4: Load Correct Version When Processing

**File**: `orchestrator/loop.py`

```python
async def _process_job(self, job: Job) -> None:
    # Load workflow at specific version
    workflow = self._workflow_service.get(
        job.workflow_id,
        version=job.workflow_version
    )
```

**File**: `services/workflow_service.py`

```python
def get(self, workflow_id: str, version: Optional[int] = None) -> WorkflowDefinition:
    """Get workflow, optionally at specific version."""
    # For now, just return current (version support is future work)
    # This at least captures the version at creation time
```

#### Estimated Changes

| File | Changes |
|------|---------|
| Schema migration | Add column (~3 lines SQL) |
| `core/models/job.py` | Add field (~2 lines) |
| `repositories/job_repo.py` | Include in queries (~5 lines) |
| `services/job_service.py` | Store version on create (~3 lines) |
| `orchestrator/loop.py` | Pass version when loading (~2 lines) |

---

### P1.3: Transaction Wrapping

**Goal**: Wrap multi-step updates in transactions for atomicity.

#### Problem Areas

1. **Node dispatch + Job status update**:
   ```python
   await node_repo.mark_dispatched(...)  # Step 1
   await job_repo.update_status(...)     # Step 2 (crash here = inconsistent)
   ```

2. **Result processing + Node update + Mark processed**:
   ```python
   await node_repo.update(node)          # Step 1
   await result_repo.mark_processed(...) # Step 2 (crash here = double-process)
   ```

#### Solution: Use psycopg3 Transactions

**File**: `orchestrator/loop.py`

```python
async def _dispatch_node(self, job: Job, node: NodeState, workflow) -> bool:
    async with self._pool.connection() as conn:
        async with conn.transaction():
            # Both operations in same transaction
            await self._node_repo.mark_dispatched(
                node.job_id, node.node_id, task_id,
                conn=conn  # Pass connection
            )

            if job.status == JobStatus.PENDING:
                await self._job_repo.update_status(
                    job.job_id, JobStatus.RUNNING,
                    conn=conn  # Same connection
                )

    # Send to queue AFTER transaction commits
    await self._publisher.publish(message, queue_name)
```

#### Update Repositories to Accept Connection

**File**: `repositories/node_repo.py`

```python
async def mark_dispatched(
    self,
    job_id: str,
    node_id: str,
    task_id: str,
    conn: Optional[AsyncConnection] = None,  # NEW
) -> bool:
    async def _execute(c: AsyncConnection):
        # ... existing logic using c instead of self._pool

    if conn:
        return await _execute(conn)
    else:
        async with self._pool.connection() as c:
            return await _execute(c)
```

#### Estimated Changes

| File | Changes |
|------|---------|
| `repositories/job_repo.py` | Add optional conn param (~20 lines) |
| `repositories/node_repo.py` | Add optional conn param (~20 lines) |
| `repositories/task_repo.py` | Add optional conn param (~20 lines) |
| `orchestrator/loop.py` | Wrap in transactions (~30 lines) |
| `services/node_service.py` | Wrap result processing (~15 lines) |

---

## 9. Implementation Plan: P2 Advanced Features

### P2.1: Conditional Routing

**Goal**: Evaluate conditions after node completion; skip branches not taken.

#### Current State

The `NodeDefinition` model already supports conditional nodes:

```python
class NodeDefinition(BaseModel):
    type: NodeType  # Includes CONDITIONAL
    condition: Optional[str]  # e.g., "{{ nodes.validate.output.valid }}"
    on_true: Optional[str]   # Next node if condition is true
    on_false: Optional[str]  # Next node if condition is false
```

The evaluator has stub code but conditional logic isn't wired up.

#### Step 1: Implement Condition Evaluation

**File**: `orchestrator/engine/evaluator.py`

```python
def evaluate_condition(
    self,
    condition: str,
    job_params: Dict[str, Any],
    node_outputs: Dict[str, Dict],
) -> bool:
    """
    Evaluate a {{ }} condition expression.

    Supported patterns:
    - {{ nodes.node_id.output.field }} - Access node output
    - {{ nodes.node_id.status }} - Check node status
    - {{ inputs.param_name }} - Access job input
    - Comparison operators: ==, !=, >, <, >=, <=
    - Boolean: true, false

    Examples:
    - "{{ nodes.validate.output.valid }}" → bool
    - "{{ nodes.validate.output.count }} > 0" → comparison
    - "{{ inputs.skip_validation }}" → direct bool input
    """
    # Resolve template expressions
    resolved = self._resolve_template(condition, job_params, node_outputs)

    # Handle comparison expressions
    if any(op in resolved for op in [' == ', ' != ', ' > ', ' < ', ' >= ', ' <= ']):
        return self._evaluate_comparison(resolved)

    # Handle direct boolean
    if resolved.lower() in ('true', '1', 'yes'):
        return True
    if resolved.lower() in ('false', '0', 'no', 'none', ''):
        return False

    # Truthy evaluation
    return bool(resolved)


def _evaluate_comparison(self, expr: str) -> bool:
    """Safely evaluate comparison expressions."""
    import operator

    ops = {
        '==': operator.eq,
        '!=': operator.ne,
        '>': operator.gt,
        '<': operator.lt,
        '>=': operator.ge,
        '<=': operator.le,
    }

    for op_str, op_func in ops.items():
        if f' {op_str} ' in expr:
            left, right = expr.split(f' {op_str} ', 1)
            left = self._coerce_value(left.strip())
            right = self._coerce_value(right.strip())
            return op_func(left, right)

    return bool(expr)


def _coerce_value(self, value: str) -> Any:
    """Coerce string to appropriate type for comparison."""
    # Try int
    try:
        return int(value)
    except ValueError:
        pass

    # Try float
    try:
        return float(value)
    except ValueError:
        pass

    # Boolean strings
    if value.lower() in ('true', 'false'):
        return value.lower() == 'true'

    # Strip quotes from strings
    if (value.startswith('"') and value.endswith('"')) or \
       (value.startswith("'") and value.endswith("'")):
        return value[1:-1]

    return value
```

#### Step 2: Wire Up Conditional Routing After Node Completion

**File**: `orchestrator/loop.py`

When a node completes, if downstream nodes are conditional, evaluate them:

```python
async def _process_completed_node(
    self,
    job: Job,
    completed_node: NodeState,
    workflow: WorkflowDefinition,
    node_states: Dict[str, NodeState],
) -> None:
    """Process routing after a node completes."""
    node_def = workflow.nodes[completed_node.node_id]

    # Handle conditional routing from this node
    if node_def.type == NodeType.CONDITIONAL:
        await self._evaluate_conditional_node(
            job, completed_node, node_def, workflow, node_states
        )

    # Check if any downstream nodes are conditional and need evaluation
    for downstream_id in self._get_downstream_nodes(node_def):
        downstream_def = workflow.nodes.get(downstream_id)
        if downstream_def and downstream_def.type == NodeType.CONDITIONAL:
            downstream_state = node_states.get(downstream_id)
            if downstream_state and downstream_state.status == NodeStatus.PENDING:
                await self._evaluate_conditional_node(
                    job, downstream_state, downstream_def, workflow, node_states
                )


async def _evaluate_conditional_node(
    self,
    job: Job,
    node_state: NodeState,
    node_def: NodeDefinition,
    workflow: WorkflowDefinition,
    node_states: Dict[str, NodeState],
) -> None:
    """Evaluate a conditional node and route accordingly."""
    # Build node outputs map
    node_outputs = {
        n.node_id: n.output or {}
        for n in node_states.values()
        if n.status == NodeStatus.COMPLETED
    }

    # Evaluate condition
    result = self._evaluator.evaluate_condition(
        node_def.condition,
        job.input_params,
        node_outputs,
    )

    # Determine which branch to take
    taken_branch = node_def.on_true if result else node_def.on_false
    skipped_branch = node_def.on_false if result else node_def.on_true

    # Mark the conditional node itself as completed (it's just routing)
    node_state.mark_completed({"condition_result": result, "taken": taken_branch})
    await self._node_repo.update(node_state)

    # Skip the not-taken branch and all its descendants
    if skipped_branch:
        await self._skip_branch(job.job_id, skipped_branch, workflow, node_states)

    # Emit event
    await self._event_service.emit(
        EventType.NODE_COMPLETED,
        job_id=job.job_id,
        node_id=node_state.node_id,
        data={"condition": node_def.condition, "result": result, "taken": taken_branch},
    )


async def _skip_branch(
    self,
    job_id: str,
    start_node_id: str,
    workflow: WorkflowDefinition,
    node_states: Dict[str, NodeState],
) -> None:
    """Recursively skip a branch of the DAG."""
    to_skip = [start_node_id]
    skipped = set()

    while to_skip:
        node_id = to_skip.pop(0)
        if node_id in skipped:
            continue

        node_state = node_states.get(node_id)
        if node_state and node_state.status == NodeStatus.PENDING:
            node_state.mark_skipped()
            await self._node_repo.update(node_state)
            skipped.add(node_id)

            # Emit event
            await self._event_service.emit(
                EventType.NODE_SKIPPED,
                job_id=job_id,
                node_id=node_id,
                data={"reason": "conditional_branch_not_taken"},
            )

            # Add downstream nodes
            node_def = workflow.nodes.get(node_id)
            if node_def:
                to_skip.extend(self._get_downstream_nodes(node_def))
```

#### Step 3: Update find_ready_nodes for Conditional Awareness

**File**: `orchestrator/engine/evaluator.py`

```python
def find_ready_nodes(
    self,
    workflow: WorkflowDefinition,
    node_states: Dict[str, NodeState],
) -> List[str]:
    """Find nodes ready for dispatch, respecting conditionals."""
    ready = []

    for node_id, node_def in workflow.nodes.items():
        state = node_states.get(node_id)
        if not state or state.status != NodeStatus.PENDING:
            continue

        # Skip conditional nodes - they're handled separately
        if node_def.type == NodeType.CONDITIONAL:
            continue

        # Check dependencies
        if self._dependencies_met(node_def, node_states):
            ready.append(node_id)

    return ready
```

#### Estimated Changes

| File | Changes |
|------|---------|
| `orchestrator/engine/evaluator.py` | Add evaluate_condition(), _evaluate_comparison() (~80 lines) |
| `orchestrator/loop.py` | Add _process_completed_node(), _evaluate_conditional_node(), _skip_branch() (~100 lines) |
| `services/node_service.py` | Call _process_completed_node() after result processing (~10 lines) |

---

### P2.2: Fan-Out/Fan-In

**Goal**: Dynamically create parallel tasks from arrays; aggregate results.

#### Workflow YAML Syntax

```yaml
nodes:
  split_files:
    type: fan_out
    source: "{{ inputs.file_list }}"  # Array to expand
    child_node: process_file          # Template for each item
    max_parallel: 10                   # Limit concurrency

  process_file:
    type: task
    handler: process_single_file
    params:
      file_path: "{{ fan_out.item }}"       # Current array item
      index: "{{ fan_out.index }}"           # 0-based index
      total: "{{ fan_out.total }}"           # Array length

  aggregate:
    type: fan_in
    source_node: process_file
    aggregation: collect              # collect | sum | first | last
    depends_on:
      all_of: [split_files]           # Wait for fan-out to complete
```

#### Step 1: Extend NodeState for Dynamic Nodes

**File**: `core/models/node.py` (fields already exist, verify usage)

```python
class NodeState(BaseModel):
    # ... existing fields ...

    # Dynamic node tracking (for fan-out)
    parent_node_id: Optional[str] = None    # ID of the fan-out node
    fan_out_index: Optional[int] = None     # Index in the array (0-based)
    fan_out_item_key: Optional[str] = None  # Key for looking up the item

    @property
    def is_dynamic(self) -> bool:
        """True if this node was dynamically created by fan-out."""
        return self.parent_node_id is not None
```

#### Step 2: Implement Fan-Out Expansion

**File**: `orchestrator/engine/fan_out.py` (new file)

```python
from typing import List, Dict, Any, Optional
from core.models import NodeState, NodeStatus
from core.models.workflow import NodeDefinition


class FanOutExpander:
    """Handles dynamic node creation for fan-out nodes."""

    def expand_fan_out(
        self,
        fan_out_node: NodeDefinition,
        source_array: List[Any],
        job_id: str,
        child_node_def: NodeDefinition,
        max_parallel: Optional[int] = None,
    ) -> List[NodeState]:
        """
        Create NodeState instances for each item in the source array.

        Returns list of new NodeState objects (not yet persisted).
        """
        dynamic_nodes = []

        for index, item in enumerate(source_array):
            # Generate unique node_id for dynamic node
            dynamic_node_id = f"{child_node_def.id}__fan_{index}"

            node_state = NodeState(
                job_id=job_id,
                node_id=dynamic_node_id,
                status=NodeStatus.PENDING,
                parent_node_id=fan_out_node.id,
                fan_out_index=index,
                max_retries=child_node_def.retries or 3,
            )

            dynamic_nodes.append(node_state)

        return dynamic_nodes

    def get_fan_out_context(
        self,
        node_state: NodeState,
        source_array: List[Any],
    ) -> Dict[str, Any]:
        """
        Build the fan_out context for template resolution.

        Available in templates as:
        - {{ fan_out.item }} - current array item
        - {{ fan_out.index }} - 0-based index
        - {{ fan_out.total }} - total array length
        """
        if node_state.fan_out_index is None:
            return {}

        return {
            "item": source_array[node_state.fan_out_index],
            "index": node_state.fan_out_index,
            "total": len(source_array),
        }
```

#### Step 3: Implement Fan-In Aggregation

**File**: `orchestrator/engine/fan_in.py` (new file)

```python
from typing import List, Dict, Any, Optional
from core.models import NodeState, NodeStatus


class FanInAggregator:
    """Handles result aggregation for fan-in nodes."""

    def aggregate_results(
        self,
        dynamic_nodes: List[NodeState],
        aggregation_type: str = "collect",
    ) -> Dict[str, Any]:
        """
        Aggregate results from all dynamic nodes.

        Aggregation types:
        - collect: List of all outputs
        - sum: Sum numeric outputs
        - first: First successful output
        - last: Last successful output
        - merge: Merge all dicts into one
        """
        completed = [n for n in dynamic_nodes if n.status == NodeStatus.COMPLETED]
        outputs = [n.output for n in completed if n.output]

        if aggregation_type == "collect":
            return {"results": outputs, "count": len(outputs)}

        elif aggregation_type == "sum":
            total = sum(
                o.get("value", 0) for o in outputs
                if isinstance(o.get("value"), (int, float))
            )
            return {"sum": total, "count": len(outputs)}

        elif aggregation_type == "first":
            return outputs[0] if outputs else {}

        elif aggregation_type == "last":
            return outputs[-1] if outputs else {}

        elif aggregation_type == "merge":
            merged = {}
            for o in outputs:
                if isinstance(o, dict):
                    merged.update(o)
            return merged

        else:
            return {"results": outputs}

    def check_fan_in_ready(
        self,
        fan_out_node_id: str,
        all_node_states: Dict[str, NodeState],
    ) -> bool:
        """
        Check if all dynamic nodes from a fan-out are complete.
        """
        dynamic_nodes = [
            n for n in all_node_states.values()
            if n.parent_node_id == fan_out_node_id
        ]

        if not dynamic_nodes:
            return False

        return all(n.is_terminal for n in dynamic_nodes)
```

#### Step 4: Wire Up in Orchestrator Loop

**File**: `orchestrator/loop.py`

```python
from orchestrator.engine.fan_out import FanOutExpander
from orchestrator.engine.fan_in import FanInAggregator


async def _process_fan_out_node(
    self,
    job: Job,
    fan_out_node: NodeState,
    fan_out_def: NodeDefinition,
    workflow: WorkflowDefinition,
) -> None:
    """Expand a fan-out node into dynamic child nodes."""
    # Resolve the source array
    source_expr = fan_out_def.source
    node_outputs = await self._get_node_outputs(job.job_id)
    source_array = self._template_resolver.resolve(
        source_expr, job.input_params, node_outputs
    )

    if not isinstance(source_array, list):
        raise ValueError(f"Fan-out source must be array, got {type(source_array)}")

    # Get child node definition
    child_def = workflow.nodes[fan_out_def.child_node]

    # Create dynamic nodes
    expander = FanOutExpander()
    dynamic_nodes = expander.expand_fan_out(
        fan_out_def, source_array, job.job_id, child_def,
        max_parallel=fan_out_def.max_parallel,
    )

    # Persist dynamic nodes
    for node in dynamic_nodes:
        await self._node_repo.create(node)

    # Mark fan-out node as completed
    fan_out_node.mark_completed({
        "dynamic_nodes": [n.node_id for n in dynamic_nodes],
        "total": len(dynamic_nodes),
    })
    await self._node_repo.update(fan_out_node)

    # Emit event
    await self._event_service.emit(
        EventType.NODE_COMPLETED,
        job_id=job.job_id,
        node_id=fan_out_node.node_id,
        data={"type": "fan_out", "children_created": len(dynamic_nodes)},
    )


async def _process_fan_in_node(
    self,
    job: Job,
    fan_in_node: NodeState,
    fan_in_def: NodeDefinition,
    node_states: Dict[str, NodeState],
) -> None:
    """Aggregate results from dynamic nodes."""
    aggregator = FanInAggregator()

    # Check if ready
    if not aggregator.check_fan_in_ready(fan_in_def.source_node, node_states):
        return  # Not ready yet

    # Get dynamic nodes
    dynamic_nodes = [
        n for n in node_states.values()
        if n.parent_node_id == fan_in_def.source_node
    ]

    # Aggregate
    result = aggregator.aggregate_results(
        dynamic_nodes,
        aggregation_type=fan_in_def.aggregation or "collect",
    )

    # Mark fan-in as completed
    fan_in_node.mark_completed(result)
    await self._node_repo.update(fan_in_node)

    # Emit event
    await self._event_service.emit(
        EventType.NODE_COMPLETED,
        job_id=job.job_id,
        node_id=fan_in_node.node_id,
        data={"type": "fan_in", "aggregated_count": len(dynamic_nodes)},
    )
```

#### Step 5: Update Template Resolution for Fan-Out Context

**File**: `orchestrator/engine/templates.py`

```python
def resolve_params(
    self,
    params: Dict[str, Any],
    job_params: Dict[str, Any],
    node_outputs: Dict[str, Dict],
    fan_out_context: Optional[Dict[str, Any]] = None,  # NEW
) -> Dict[str, Any]:
    """Resolve {{ }} expressions in params."""
    context = {
        "inputs": job_params,
        "nodes": node_outputs,
        "env": os.environ,
    }

    if fan_out_context:
        context["fan_out"] = fan_out_context

    return self._resolve_dict(params, context)
```

#### Estimated Changes

| File | Changes |
|------|---------|
| `orchestrator/engine/fan_out.py` | New file (~80 lines) |
| `orchestrator/engine/fan_in.py` | New file (~70 lines) |
| `orchestrator/loop.py` | Add fan-out/fan-in processing (~100 lines) |
| `orchestrator/engine/templates.py` | Add fan_out context support (~15 lines) |
| `core/models/workflow.py` | Add source, child_node, aggregation fields (~10 lines) |
| `repositories/node_repo.py` | Add create() method for dynamic nodes (~20 lines) |

---

### P2.3: Checkpointing

**Goal**: Save progress during long-running tasks; resume from checkpoint on retry.

#### Current State

`NodeState` already has checkpoint fields:
```python
checkpoint_phase: Optional[str]       # e.g., "downloading", "processing", "uploading"
checkpoint_data: Optional[Dict]       # Arbitrary state data
checkpoint_saved_at: Optional[datetime]
```

#### Step 1: Add Checkpoint Reporting to TaskResult

**File**: `core/models/task.py`

```python
class TaskResult(BaseModel):
    # ... existing fields ...

    # Checkpoint (for resumable tasks)
    checkpoint_phase: Optional[str] = None
    checkpoint_data: Optional[Dict[str, Any]] = None
```

#### Step 2: Worker Checkpoint Reporting

**File**: `worker/reporter.py`

```python
async def report_checkpoint(
    self,
    task_id: str,
    job_id: str,
    node_id: str,
    phase: str,
    data: Dict[str, Any],
) -> None:
    """
    Report a checkpoint during task execution.

    Checkpoints are saved to the database and can be used
    to resume tasks that fail mid-execution.
    """
    result = TaskResult(
        task_id=task_id,
        job_id=job_id,
        node_id=node_id,
        status=TaskStatus.RUNNING,  # Still running
        checkpoint_phase=phase,
        checkpoint_data=data,
        reported_at=datetime.utcnow(),
    )

    await self._send_result(result)
```

#### Step 3: Handler Checkpoint Interface

**File**: `handlers/base.py`

```python
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, AsyncGenerator


class CheckpointableHandler(ABC):
    """Base class for handlers that support checkpointing."""

    @abstractmethod
    async def execute(
        self,
        params: Dict[str, Any],
        checkpoint: Optional[Dict[str, Any]] = None,
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Execute the handler, yielding checkpoints and final result.

        If checkpoint is provided, resume from that point.

        Yields:
        - {"type": "checkpoint", "phase": "...", "data": {...}}
        - {"type": "progress", "current": N, "total": M, "message": "..."}
        - {"type": "result", "output": {...}}  # Final yield
        """
        pass


# Example implementation
class RasterProcessingHandler(CheckpointableHandler):
    async def execute(
        self,
        params: Dict[str, Any],
        checkpoint: Optional[Dict[str, Any]] = None,
    ) -> AsyncGenerator[Dict[str, Any], None]:
        file_path = params["file_path"]

        # Determine starting phase
        start_phase = checkpoint.get("phase") if checkpoint else "download"

        if start_phase == "download":
            # Download file
            local_path = await self._download(file_path)
            yield {
                "type": "checkpoint",
                "phase": "download",
                "data": {"local_path": local_path},
            }
            start_phase = "process"
        else:
            local_path = checkpoint["data"]["local_path"]

        if start_phase == "process":
            # Process file
            result_path = await self._process(local_path)
            yield {
                "type": "checkpoint",
                "phase": "process",
                "data": {"local_path": local_path, "result_path": result_path},
            }
            start_phase = "upload"
        else:
            result_path = checkpoint["data"]["result_path"]

        if start_phase == "upload":
            # Upload result
            url = await self._upload(result_path)
            yield {
                "type": "result",
                "output": {"url": url},
            }
```

#### Step 4: Orchestrator Checkpoint Processing

**File**: `services/node_service.py`

```python
async def process_task_result(self, result: TaskResult, workflow) -> bool:
    node = await self._node_repo.get(result.job_id, result.node_id)

    # Handle checkpoint updates (task still running)
    if result.status == TaskStatus.RUNNING and result.checkpoint_phase:
        node.checkpoint_phase = result.checkpoint_phase
        node.checkpoint_data = result.checkpoint_data
        node.checkpoint_saved_at = datetime.utcnow()
        await self._node_repo.update(node)

        # Emit checkpoint event
        await self._event_service.emit(
            EventType.NODE_CHECKPOINT,
            job_id=result.job_id,
            node_id=result.node_id,
            data={"phase": result.checkpoint_phase},
        )
        return True

    # ... rest of existing logic ...
```

#### Step 5: Resume from Checkpoint on Retry

**File**: `orchestrator/loop.py`

```python
async def _dispatch_node(
    self,
    job: Job,
    node: NodeState,
    workflow: WorkflowDefinition,
) -> bool:
    node_def = workflow.nodes[node.node_id]

    # Build task message
    params = self._resolve_params(node_def.params, job, workflow)

    # Include checkpoint for retry (if available)
    checkpoint = None
    if node.retry_count > 0 and node.checkpoint_data:
        checkpoint = {
            "phase": node.checkpoint_phase,
            "data": node.checkpoint_data,
        }

    message = TaskMessage(
        task_id=uuid.uuid4().hex,
        job_id=job.job_id,
        node_id=node.node_id,
        handler=node_def.handler,
        params=params,
        checkpoint=checkpoint,  # NEW
        retry_count=node.retry_count,
    )

    # ... dispatch logic ...
```

#### Step 6: Worker Checkpoint Handling

**File**: `worker/executor.py`

```python
async def execute_task(self, message: TaskMessage) -> TaskResult:
    handler = self._registry.get(message.handler)

    # Check if handler supports checkpointing
    if isinstance(handler, CheckpointableHandler):
        return await self._execute_checkpointable(handler, message)
    else:
        return await self._execute_simple(handler, message)


async def _execute_checkpointable(
    self,
    handler: CheckpointableHandler,
    message: TaskMessage,
) -> TaskResult:
    """Execute a checkpointable handler, reporting progress."""
    checkpoint = message.checkpoint

    async for event in handler.execute(message.params, checkpoint):
        if event["type"] == "checkpoint":
            await self._reporter.report_checkpoint(
                task_id=message.task_id,
                job_id=message.job_id,
                node_id=message.node_id,
                phase=event["phase"],
                data=event["data"],
            )

        elif event["type"] == "progress":
            await self._reporter.report_progress(
                task_id=message.task_id,
                current=event["current"],
                total=event["total"],
                message=event.get("message"),
            )

        elif event["type"] == "result":
            return TaskResult(
                task_id=message.task_id,
                job_id=message.job_id,
                node_id=message.node_id,
                status=TaskStatus.COMPLETED,
                output=event["output"],
            )

    raise RuntimeError("Handler did not yield a result")
```

#### Estimated Changes

| File | Changes |
|------|---------|
| `core/models/task.py` | Add checkpoint fields to TaskMessage (~5 lines) |
| `handlers/base.py` | New CheckpointableHandler base class (~50 lines) |
| `worker/executor.py` | Add checkpointable execution (~60 lines) |
| `worker/reporter.py` | Add report_checkpoint() (~25 lines) |
| `services/node_service.py` | Handle checkpoint in process_task_result (~20 lines) |
| `orchestrator/loop.py` | Include checkpoint in TaskMessage (~15 lines) |
| `core/contracts.py` | Add NODE_CHECKPOINT event type (~2 lines) |

---

## 10. Implementation Plan: P3 Observability

### P3.1: Progress API

**Goal**: Expose real-time progress for running jobs.

#### Step 1: Create Progress Aggregation Service

**File**: `services/progress_service.py`

```python
from typing import Dict, List, Any, Optional
from core.models import Job, NodeState, NodeStatus


class ProgressService:
    """Aggregates progress information for jobs."""

    def __init__(self, node_repo, task_repo):
        self._node_repo = node_repo
        self._task_repo = task_repo

    async def get_job_progress(self, job_id: str) -> Dict[str, Any]:
        """
        Get aggregated progress for a job.

        Returns:
        {
            "job_id": "...",
            "overall_percent": 45.5,
            "nodes": {
                "total": 5,
                "completed": 2,
                "running": 1,
                "pending": 2,
                "failed": 0,
            },
            "current_tasks": [
                {
                    "node_id": "process_raster",
                    "task_id": "...",
                    "progress_percent": 67.0,
                    "progress_message": "Processing tile 134/200",
                }
            ],
            "estimated_completion": "2026-02-02T15:30:00Z",  # Optional
        }
        """
        node_states = await self._node_repo.get_all_for_job(job_id)

        # Count by status
        status_counts = {status: 0 for status in NodeStatus}
        for node in node_states:
            status_counts[node.status] += 1

        # Calculate overall progress
        total = len(node_states)
        completed = status_counts[NodeStatus.COMPLETED] + status_counts[NodeStatus.SKIPPED]
        overall_percent = (completed / total * 100) if total > 0 else 0

        # Get current task progress
        running_nodes = [n for n in node_states if n.status == NodeStatus.RUNNING]
        current_tasks = []

        for node in running_nodes:
            if node.task_id:
                task_result = await self._task_repo.get_latest_for_task(node.task_id)
                if task_result:
                    current_tasks.append({
                        "node_id": node.node_id,
                        "task_id": node.task_id,
                        "progress_percent": task_result.progress_percent,
                        "progress_current": task_result.progress_current,
                        "progress_total": task_result.progress_total,
                        "progress_message": task_result.progress_message,
                    })

        return {
            "job_id": job_id,
            "overall_percent": round(overall_percent, 1),
            "nodes": {
                "total": total,
                "completed": status_counts[NodeStatus.COMPLETED],
                "running": status_counts[NodeStatus.RUNNING],
                "pending": status_counts[NodeStatus.PENDING] + status_counts[NodeStatus.READY],
                "failed": status_counts[NodeStatus.FAILED],
                "skipped": status_counts[NodeStatus.SKIPPED],
            },
            "current_tasks": current_tasks,
        }

    async def get_node_progress(self, job_id: str, node_id: str) -> Dict[str, Any]:
        """Get detailed progress for a specific node."""
        node = await self._node_repo.get(job_id, node_id)
        if not node:
            return None

        result = {
            "job_id": job_id,
            "node_id": node_id,
            "status": node.status.value,
            "retry_count": node.retry_count,
            "max_retries": node.max_retries,
        }

        if node.task_id:
            task_result = await self._task_repo.get_latest_for_task(node.task_id)
            if task_result:
                result["task"] = {
                    "task_id": node.task_id,
                    "progress_percent": task_result.progress_percent,
                    "progress_current": task_result.progress_current,
                    "progress_total": task_result.progress_total,
                    "progress_message": task_result.progress_message,
                }

        if node.checkpoint_phase:
            result["checkpoint"] = {
                "phase": node.checkpoint_phase,
                "saved_at": node.checkpoint_saved_at.isoformat() if node.checkpoint_saved_at else None,
            }

        return result
```

#### Step 2: Add Progress API Endpoints

**File**: `api/routes.py`

```python
@router.get("/jobs/{job_id}/progress", tags=["Jobs"])
async def get_job_progress(
    job_id: str,
    progress_service: ProgressService = Depends(get_progress_service),
) -> Dict[str, Any]:
    """
    Get real-time progress for a job.

    Returns overall completion percentage, node status counts,
    and detailed progress for currently running tasks.
    """
    job = await job_service.get(job_id)
    if not job:
        raise HTTPException(404, f"Job not found: {job_id}")

    progress = await progress_service.get_job_progress(job_id)
    progress["job_status"] = job.status.value
    progress["workflow_id"] = job.workflow_id

    return progress


@router.get("/jobs/{job_id}/nodes/{node_id}/progress", tags=["Jobs"])
async def get_node_progress(
    job_id: str,
    node_id: str,
    progress_service: ProgressService = Depends(get_progress_service),
) -> Dict[str, Any]:
    """Get detailed progress for a specific node."""
    progress = await progress_service.get_node_progress(job_id, node_id)
    if not progress:
        raise HTTPException(404, f"Node not found: {job_id}/{node_id}")

    return progress
```

#### Step 3: Add Task Repository Method

**File**: `repositories/task_repo.py`

```python
async def get_latest_for_task(self, task_id: str) -> Optional[TaskResult]:
    """Get the most recent result/progress for a task."""
    async with self._pool.connection() as conn:
        conn.row_factory = dict_row
        result = await conn.execute(
            """
            SELECT * FROM dagapp.dag_task_results
            WHERE task_id = %s
            ORDER BY reported_at DESC
            LIMIT 1
            """,
            (task_id,),
        )
        row = await result.fetchone()
        return TaskResult(**row) if row else None
```

#### Estimated Changes

| File | Changes |
|------|---------|
| `services/progress_service.py` | New file (~100 lines) |
| `api/routes.py` | Add 2 endpoints (~40 lines) |
| `repositories/task_repo.py` | Add get_latest_for_task() (~15 lines) |
| `main.py` | Initialize ProgressService (~5 lines) |

---

### P3.2: Timeline API

**Goal**: Expose event history for debugging and audit.

**Prerequisite**: P0.1 (Event Timeline Logging) must be implemented first.

#### Step 1: Create Timeline Service

**File**: `services/timeline_service.py`

```python
from typing import List, Dict, Any, Optional
from datetime import datetime
from core.models import JobEvent


class TimelineService:
    """Provides formatted timeline views of job execution."""

    def __init__(self, event_repo):
        self._event_repo = event_repo

    async def get_job_timeline(
        self,
        job_id: str,
        limit: int = 100,
        event_types: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get chronological event timeline for a job.

        Returns events formatted for display:
        [
            {
                "timestamp": "2026-02-02T14:30:15.123Z",
                "event_type": "node_dispatched",
                "node_id": "process_raster",
                "task_id": "abc123",
                "status": "success",
                "message": "Dispatched to queue: container-tasks",
                "duration_ms": null,
                "data": {...}
            },
            ...
        ]
        """
        events = await self._event_repo.get_for_job(job_id, limit=limit)

        if event_types:
            events = [e for e in events if e.event_type.value in event_types]

        return [self._format_event(e) for e in events]

    async def get_node_timeline(
        self,
        job_id: str,
        node_id: str,
    ) -> List[Dict[str, Any]]:
        """Get timeline for a specific node."""
        events = await self._event_repo.get_for_node(job_id, node_id)
        return [self._format_event(e) for e in events]

    def _format_event(self, event: JobEvent) -> Dict[str, Any]:
        """Format event for API response."""
        return {
            "event_id": event.event_id,
            "timestamp": event.created_at.isoformat(),
            "event_type": event.event_type.value,
            "node_id": event.node_id,
            "task_id": event.task_id,
            "status": event.event_status.value,
            "message": self._get_event_message(event),
            "duration_ms": event.duration_ms,
            "data": event.event_data,
        }

    def _get_event_message(self, event: JobEvent) -> str:
        """Generate human-readable message for event."""
        messages = {
            "job_created": f"Job created with workflow: {event.event_data.get('workflow_id')}",
            "job_started": "Job execution started",
            "job_completed": f"Job completed in {event.duration_ms}ms" if event.duration_ms else "Job completed",
            "job_failed": f"Job failed: {event.error_message or 'Unknown error'}",
            "node_ready": f"Node ready (dependencies met)",
            "node_dispatched": f"Dispatched to queue: {event.event_data.get('queue')}",
            "node_running": f"Picked up by worker: {event.event_data.get('worker_id', 'unknown')}",
            "node_completed": f"Completed in {event.duration_ms}ms" if event.duration_ms else "Completed",
            "node_failed": f"Failed: {event.error_message or 'Unknown error'}",
            "node_retrying": f"Retrying (attempt {event.event_data.get('retry_count', '?')})",
            "node_skipped": f"Skipped: {event.event_data.get('reason', 'conditional')}",
        }
        return messages.get(event.event_type.value, event.event_type.value)

    async def get_timeline_summary(self, job_id: str) -> Dict[str, Any]:
        """
        Get summary statistics from timeline.

        Returns:
        {
            "total_events": 42,
            "first_event": "2026-02-02T14:30:00Z",
            "last_event": "2026-02-02T14:35:00Z",
            "duration_seconds": 300,
            "event_counts": {"node_completed": 5, "node_failed": 1, ...},
            "nodes_with_retries": ["validate_input"],
        }
        """
        events = await self._event_repo.get_for_job(job_id, limit=1000)

        if not events:
            return {"total_events": 0}

        # Count by type
        event_counts = {}
        for e in events:
            event_counts[e.event_type.value] = event_counts.get(e.event_type.value, 0) + 1

        # Find retried nodes
        retried_nodes = set()
        for e in events:
            if e.event_type.value == "node_retrying" and e.node_id:
                retried_nodes.add(e.node_id)

        return {
            "total_events": len(events),
            "first_event": events[-1].created_at.isoformat(),  # Oldest (list is newest first)
            "last_event": events[0].created_at.isoformat(),    # Newest
            "duration_seconds": (events[0].created_at - events[-1].created_at).total_seconds(),
            "event_counts": event_counts,
            "nodes_with_retries": list(retried_nodes),
        }
```

#### Step 2: Add Timeline API Endpoints

**File**: `api/routes.py`

```python
@router.get("/jobs/{job_id}/timeline", tags=["Jobs"])
async def get_job_timeline(
    job_id: str,
    limit: int = Query(100, ge=1, le=1000),
    event_types: Optional[str] = Query(None, description="Comma-separated event types"),
    timeline_service: TimelineService = Depends(get_timeline_service),
) -> Dict[str, Any]:
    """
    Get event timeline for a job.

    Returns chronological list of all events that occurred during job execution.
    Useful for debugging and audit trails.
    """
    types_filter = event_types.split(",") if event_types else None

    timeline = await timeline_service.get_job_timeline(
        job_id, limit=limit, event_types=types_filter
    )
    summary = await timeline_service.get_timeline_summary(job_id)

    return {
        "job_id": job_id,
        "summary": summary,
        "events": timeline,
    }


@router.get("/jobs/{job_id}/nodes/{node_id}/timeline", tags=["Jobs"])
async def get_node_timeline(
    job_id: str,
    node_id: str,
    timeline_service: TimelineService = Depends(get_timeline_service),
) -> Dict[str, Any]:
    """Get event timeline for a specific node."""
    events = await timeline_service.get_node_timeline(job_id, node_id)

    return {
        "job_id": job_id,
        "node_id": node_id,
        "events": events,
    }
```

#### Estimated Changes

| File | Changes |
|------|---------|
| `services/timeline_service.py` | New file (~120 lines) |
| `api/routes.py` | Add 2 endpoints (~40 lines) |
| `main.py` | Initialize TimelineService (~5 lines) |

---

### P3.3: Metrics Dashboard

**Goal**: Aggregate statistics for monitoring and optimization.

#### Step 1: Create Metrics Service

**File**: `services/metrics_service.py`

```python
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from core.contracts import JobStatus, NodeStatus


class MetricsService:
    """Aggregates metrics for workflows and orchestrator performance."""

    def __init__(self, pool):
        self._pool = pool

    async def get_workflow_metrics(
        self,
        workflow_id: Optional[str] = None,
        days: int = 7,
    ) -> Dict[str, Any]:
        """
        Get per-workflow statistics.

        Returns:
        {
            "period": {"start": "...", "end": "...", "days": 7},
            "workflows": [
                {
                    "workflow_id": "raster_processing",
                    "total_jobs": 150,
                    "completed": 140,
                    "failed": 8,
                    "cancelled": 2,
                    "success_rate": 93.3,
                    "avg_duration_seconds": 125.5,
                    "p50_duration_seconds": 100.0,
                    "p95_duration_seconds": 250.0,
                    "common_failures": [
                        {"node_id": "validate", "count": 5, "error": "Invalid format"},
                    ],
                },
                ...
            ],
        }
        """
        since = datetime.utcnow() - timedelta(days=days)

        async with self._pool.connection() as conn:
            conn.row_factory = dict_row

            # Get workflow stats
            query = """
                SELECT
                    workflow_id,
                    COUNT(*) as total_jobs,
                    COUNT(*) FILTER (WHERE status = 'completed') as completed,
                    COUNT(*) FILTER (WHERE status = 'failed') as failed,
                    COUNT(*) FILTER (WHERE status = 'cancelled') as cancelled,
                    AVG(EXTRACT(EPOCH FROM (completed_at - started_at)))
                        FILTER (WHERE completed_at IS NOT NULL) as avg_duration,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (
                        ORDER BY EXTRACT(EPOCH FROM (completed_at - started_at))
                    ) FILTER (WHERE completed_at IS NOT NULL) as p50_duration,
                    PERCENTILE_CONT(0.95) WITHIN GROUP (
                        ORDER BY EXTRACT(EPOCH FROM (completed_at - started_at))
                    ) FILTER (WHERE completed_at IS NOT NULL) as p95_duration
                FROM dagapp.dag_jobs
                WHERE created_at >= %s
            """
            params = [since]

            if workflow_id:
                query += " AND workflow_id = %s"
                params.append(workflow_id)

            query += " GROUP BY workflow_id ORDER BY total_jobs DESC"

            result = await conn.execute(query, params)
            rows = await result.fetchall()

        workflows = []
        for row in rows:
            total = row["total_jobs"]
            completed = row["completed"]
            success_rate = (completed / total * 100) if total > 0 else 0

            workflows.append({
                "workflow_id": row["workflow_id"],
                "total_jobs": total,
                "completed": completed,
                "failed": row["failed"],
                "cancelled": row["cancelled"],
                "success_rate": round(success_rate, 1),
                "avg_duration_seconds": round(row["avg_duration"], 1) if row["avg_duration"] else None,
                "p50_duration_seconds": round(row["p50_duration"], 1) if row["p50_duration"] else None,
                "p95_duration_seconds": round(row["p95_duration"], 1) if row["p95_duration"] else None,
            })

        return {
            "period": {
                "start": since.isoformat(),
                "end": datetime.utcnow().isoformat(),
                "days": days,
            },
            "workflows": workflows,
        }

    async def get_node_metrics(
        self,
        workflow_id: str,
        days: int = 7,
    ) -> Dict[str, Any]:
        """
        Get per-node statistics for a workflow.

        Returns node-level success rates, durations, and failure patterns.
        """
        since = datetime.utcnow() - timedelta(days=days)

        async with self._pool.connection() as conn:
            conn.row_factory = dict_row

            result = await conn.execute(
                """
                SELECT
                    n.node_id,
                    COUNT(*) as total_executions,
                    COUNT(*) FILTER (WHERE n.status = 'completed') as completed,
                    COUNT(*) FILTER (WHERE n.status = 'failed') as failed,
                    AVG(EXTRACT(EPOCH FROM (n.completed_at - n.started_at)))
                        FILTER (WHERE n.completed_at IS NOT NULL) as avg_duration,
                    SUM(n.retry_count) as total_retries
                FROM dagapp.dag_node_states n
                JOIN dagapp.dag_jobs j ON j.job_id = n.job_id
                WHERE j.workflow_id = %s AND j.created_at >= %s
                GROUP BY n.node_id
                ORDER BY node_id
                """,
                (workflow_id, since),
            )
            rows = await result.fetchall()

        nodes = []
        for row in rows:
            total = row["total_executions"]
            success_rate = (row["completed"] / total * 100) if total > 0 else 0

            nodes.append({
                "node_id": row["node_id"],
                "total_executions": total,
                "completed": row["completed"],
                "failed": row["failed"],
                "success_rate": round(success_rate, 1),
                "avg_duration_seconds": round(row["avg_duration"], 1) if row["avg_duration"] else None,
                "total_retries": row["total_retries"] or 0,
            })

        return {
            "workflow_id": workflow_id,
            "period_days": days,
            "nodes": nodes,
        }

    async def get_failure_analysis(
        self,
        workflow_id: Optional[str] = None,
        days: int = 7,
        limit: int = 20,
    ) -> Dict[str, Any]:
        """
        Analyze failure patterns.

        Returns common error messages grouped by node.
        """
        since = datetime.utcnow() - timedelta(days=days)

        async with self._pool.connection() as conn:
            conn.row_factory = dict_row

            query = """
                SELECT
                    j.workflow_id,
                    n.node_id,
                    n.error_message,
                    COUNT(*) as occurrences
                FROM dagapp.dag_node_states n
                JOIN dagapp.dag_jobs j ON j.job_id = n.job_id
                WHERE n.status = 'failed'
                  AND j.created_at >= %s
                  AND n.error_message IS NOT NULL
            """
            params = [since]

            if workflow_id:
                query += " AND j.workflow_id = %s"
                params.append(workflow_id)

            query += """
                GROUP BY j.workflow_id, n.node_id, n.error_message
                ORDER BY occurrences DESC
                LIMIT %s
            """
            params.append(limit)

            result = await conn.execute(query, params)
            rows = await result.fetchall()

        failures = []
        for row in rows:
            # Truncate long error messages
            error = row["error_message"]
            if len(error) > 200:
                error = error[:197] + "..."

            failures.append({
                "workflow_id": row["workflow_id"],
                "node_id": row["node_id"],
                "error_message": error,
                "occurrences": row["occurrences"],
            })

        return {
            "period_days": days,
            "total_failures": sum(f["occurrences"] for f in failures),
            "unique_errors": len(failures),
            "failures": failures,
        }

    async def get_throughput_metrics(
        self,
        hours: int = 24,
        bucket_minutes: int = 60,
    ) -> Dict[str, Any]:
        """
        Get time-series throughput data.

        Returns jobs completed per time bucket.
        """
        since = datetime.utcnow() - timedelta(hours=hours)

        async with self._pool.connection() as conn:
            conn.row_factory = dict_row

            result = await conn.execute(
                """
                SELECT
                    date_trunc('hour', completed_at) as bucket,
                    COUNT(*) as jobs_completed,
                    AVG(EXTRACT(EPOCH FROM (completed_at - started_at))) as avg_duration
                FROM dagapp.dag_jobs
                WHERE completed_at >= %s AND status = 'completed'
                GROUP BY bucket
                ORDER BY bucket
                """,
                (since,),
            )
            rows = await result.fetchall()

        buckets = []
        for row in rows:
            buckets.append({
                "timestamp": row["bucket"].isoformat(),
                "jobs_completed": row["jobs_completed"],
                "avg_duration_seconds": round(row["avg_duration"], 1) if row["avg_duration"] else None,
            })

        return {
            "period_hours": hours,
            "bucket_minutes": bucket_minutes,
            "total_completed": sum(b["jobs_completed"] for b in buckets),
            "buckets": buckets,
        }
```

#### Step 2: Add Metrics API Endpoints

**File**: `api/routes.py`

```python
@router.get("/metrics/workflows", tags=["Metrics"])
async def get_workflow_metrics(
    workflow_id: Optional[str] = None,
    days: int = Query(7, ge=1, le=90),
    metrics_service: MetricsService = Depends(get_metrics_service),
) -> Dict[str, Any]:
    """
    Get per-workflow statistics.

    Returns success rates, durations, and job counts for each workflow.
    """
    return await metrics_service.get_workflow_metrics(workflow_id, days)


@router.get("/metrics/workflows/{workflow_id}/nodes", tags=["Metrics"])
async def get_node_metrics(
    workflow_id: str,
    days: int = Query(7, ge=1, le=90),
    metrics_service: MetricsService = Depends(get_metrics_service),
) -> Dict[str, Any]:
    """Get per-node statistics for a workflow."""
    return await metrics_service.get_node_metrics(workflow_id, days)


@router.get("/metrics/failures", tags=["Metrics"])
async def get_failure_analysis(
    workflow_id: Optional[str] = None,
    days: int = Query(7, ge=1, le=90),
    limit: int = Query(20, ge=1, le=100),
    metrics_service: MetricsService = Depends(get_metrics_service),
) -> Dict[str, Any]:
    """Analyze failure patterns across workflows."""
    return await metrics_service.get_failure_analysis(workflow_id, days, limit)


@router.get("/metrics/throughput", tags=["Metrics"])
async def get_throughput_metrics(
    hours: int = Query(24, ge=1, le=168),
    metrics_service: MetricsService = Depends(get_metrics_service),
) -> Dict[str, Any]:
    """Get time-series throughput data."""
    return await metrics_service.get_throughput_metrics(hours)
```

#### Estimated Changes

| File | Changes |
|------|---------|
| `services/metrics_service.py` | New file (~250 lines) |
| `api/routes.py` | Add 4 endpoints (~60 lines) |
| `main.py` | Initialize MetricsService (~5 lines) |

---

### P3.4: Distributed Tracing (OpenTelemetry)

**Goal**: Correlate traces across orchestrator, workers, and external services.

#### Overview

OpenTelemetry integration enables:
- Trace propagation from job creation through completion
- Correlation of orchestrator decisions with worker execution
- Integration with Azure Application Insights
- Performance profiling and bottleneck identification

#### Step 1: Add OpenTelemetry Dependencies

**File**: `requirements-orchestrator.txt`

```
opentelemetry-api>=1.20.0
opentelemetry-sdk>=1.20.0
opentelemetry-exporter-otlp>=1.20.0
opentelemetry-instrumentation-fastapi>=0.41b0
opentelemetry-instrumentation-psycopg>=0.41b0
azure-monitor-opentelemetry-exporter>=1.0.0b15
```

#### Step 2: Configure Tracing

**File**: `infrastructure/tracing.py`

```python
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from azure.monitor.opentelemetry.exporter import AzureMonitorTraceExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.psycopg import PsycopgInstrumentor
import os


def configure_tracing(service_name: str = "rmhdagmaster") -> trace.Tracer:
    """
    Configure OpenTelemetry tracing.

    Exports to:
    - Azure Application Insights (if APPLICATIONINSIGHTS_CONNECTION_STRING set)
    - OTLP endpoint (if OTEL_EXPORTER_OTLP_ENDPOINT set)
    """
    provider = TracerProvider()

    # Azure Application Insights exporter
    app_insights_conn = os.environ.get("APPLICATIONINSIGHTS_CONNECTION_STRING")
    if app_insights_conn:
        azure_exporter = AzureMonitorTraceExporter(
            connection_string=app_insights_conn
        )
        provider.add_span_processor(BatchSpanProcessor(azure_exporter))

    # OTLP exporter (for local dev with Jaeger/Zipkin)
    otlp_endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT")
    if otlp_endpoint:
        otlp_exporter = OTLPSpanExporter(endpoint=otlp_endpoint)
        provider.add_span_processor(BatchSpanProcessor(otlp_exporter))

    trace.set_tracer_provider(provider)

    return trace.get_tracer(service_name)


def instrument_app(app):
    """Instrument FastAPI app with OpenTelemetry."""
    FastAPIInstrumentor.instrument_app(app)
    PsycopgInstrumentor().instrument()
```

#### Step 3: Add Tracing to Orchestrator Loop

**File**: `orchestrator/loop.py`

```python
from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode


class Orchestrator:
    def __init__(self, ...):
        # ... existing ...
        self._tracer = trace.get_tracer("orchestrator")

    async def _process_job(self, job: Job) -> None:
        with self._tracer.start_as_current_span(
            "process_job",
            attributes={
                "job.id": job.job_id,
                "job.workflow_id": job.workflow_id,
                "job.status": job.status.value,
            },
        ) as span:
            try:
                # ... existing processing logic ...
                span.set_status(Status(StatusCode.OK))
            except Exception as e:
                span.set_status(Status(StatusCode.ERROR, str(e)))
                span.record_exception(e)
                raise

    async def _dispatch_node(self, job: Job, node: NodeState, ...) -> bool:
        with self._tracer.start_as_current_span(
            "dispatch_node",
            attributes={
                "job.id": job.job_id,
                "node.id": node.node_id,
                "node.handler": workflow.nodes[node.node_id].handler,
            },
        ) as span:
            # Create trace context for worker
            context = trace.get_current_span().get_span_context()
            trace_parent = f"00-{context.trace_id:032x}-{context.span_id:016x}-01"

            # Include trace context in task message
            message = TaskMessage(
                # ... existing fields ...
                trace_parent=trace_parent,  # NEW: propagate trace
            )

            # ... dispatch logic ...
```

#### Step 4: Worker Trace Continuation

**File**: `worker/executor.py`

```python
from opentelemetry import trace
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator


class TaskExecutor:
    def __init__(self, ...):
        self._tracer = trace.get_tracer("worker")
        self._propagator = TraceContextTextMapPropagator()

    async def execute_task(self, message: TaskMessage) -> TaskResult:
        # Extract trace context from message
        carrier = {"traceparent": message.trace_parent} if message.trace_parent else {}
        context = self._propagator.extract(carrier)

        with self._tracer.start_as_current_span(
            f"execute_{message.handler}",
            context=context,
            attributes={
                "task.id": message.task_id,
                "job.id": message.job_id,
                "node.id": message.node_id,
                "handler": message.handler,
            },
        ) as span:
            try:
                result = await self._execute(message)
                span.set_status(Status(StatusCode.OK))
                return result
            except Exception as e:
                span.set_status(Status(StatusCode.ERROR, str(e)))
                span.record_exception(e)
                raise
```

#### Estimated Changes

| File | Changes |
|------|---------|
| `requirements-orchestrator.txt` | Add OpenTelemetry deps (~6 lines) |
| `requirements-worker.txt` | Add OpenTelemetry deps (~6 lines) |
| `infrastructure/tracing.py` | New file (~60 lines) |
| `main.py` | Configure tracing, instrument app (~10 lines) |
| `worker/main.py` | Configure tracing (~10 lines) |
| `orchestrator/loop.py` | Add tracing spans (~40 lines) |
| `worker/executor.py` | Add trace continuation (~30 lines) |
| `core/models/task.py` | Add trace_parent field (~2 lines) |

---

## 11. Implementation Priority Summary

### Priority Matrix

| Priority | Feature | Effort | Impact | Dependencies |
|----------|---------|--------|--------|--------------|
| **P0.1** | Event Timeline Logging | Medium | High | None |
| **P0.2** | Retry Logic | Small | High | None |
| **P0.3** | Orchestrator Stats Endpoint | Small | Medium | None |
| **P1.1** | Fix Stale State Bug | Small | High | None |
| **P1.2** | Workflow Version Pinning | Small | Medium | None |
| **P1.3** | Transaction Wrapping | Medium | Medium | None |
| **P2.1** | Conditional Routing | Medium | Medium | None |
| **P2.2** | Fan-Out/Fan-In | Large | High | None |
| **P2.3** | Checkpointing | Medium | Medium | None |
| **P3.1** | Progress API | Small | Medium | None |
| **P3.2** | Timeline API | Small | Medium | P0.1 |
| **P3.3** | Metrics Dashboard | Large | High | P0.1 |
| **P3.4** | Distributed Tracing | Medium | Medium | None |

### Recommended Implementation Order

**Phase A: Foundation (P0 + P1.1)**
1. P0.1 Event Timeline Logging - enables debugging
2. P0.2 Retry Logic - production resilience
3. P1.1 Fix Stale State Bug - correctness
4. P0.3 Orchestrator Stats - monitoring

**Phase B: Stability (P1.2-P1.3)**
5. P1.2 Workflow Version Pinning - safe deployments
6. P1.3 Transaction Wrapping - data integrity

**Phase C: Observability (P3.1-P3.3)**
7. P3.1 Progress API - user visibility
8. P3.2 Timeline API - debugging UI
9. P3.3 Metrics Dashboard - operations

**Phase D: Advanced Features (P2)**
10. P2.1 Conditional Routing - workflow power
11. P2.3 Checkpointing - long-running tasks
12. P2.2 Fan-Out/Fan-In - parallel processing

**Phase E: Enterprise (P3.4)**
13. P3.4 Distributed Tracing - cross-service correlation

---

## 12. Concurrency Control & Locking Implementation

**Added**: 02 FEB 2026
**Priority**: P1.5 (Foundation - recommended before advanced features)

### 12.1 Problem Statement

Even with a single orchestrator, race conditions can occur:

1. **Accidental multi-instance**: Developer runs orchestrator locally while Azure instance runs
2. **Container restart overlap**: Brief window where old and new container both run
3. **Future scaling**: Horizontal scaling requires proper locking from the start
4. **Read-modify-write**: `check_and_ready_nodes()` has a read-modify-write pattern

### 12.2 Locking Strategy

Four layers of defense, using PostgreSQL advisory locks:

```
Layer 1: ORCHESTRATOR LOCK (Session-level)
├── Only one orchestrator process runs at a time
├── Acquired at startup, held for process lifetime
├── Auto-releases on crash/disconnect
└── Implementation: pg_try_advisory_lock(hash)

Layer 2: JOB LOCK (Transaction-level)
├── One process handles a job at a time
├── Non-blocking: skip job if locked
├── Enables future horizontal scaling
└── Implementation: pg_try_advisory_xact_lock(hash)

Layer 3: OPTIMISTIC LOCKING (Version column)
├── Detect concurrent modifications
├── Retry on conflict, don't crash
├── Fine-grained: per-row protection
└── Implementation: WHERE version = expected

Layer 4: IDEMPOTENT TRANSITIONS (Already have)
├── Conditional WHERE clauses
├── WHERE status = 'ready' for dispatch
├── WHERE processed = false for results
└── Implementation: Already exists
```

### 12.3 PostgreSQL Advisory Locks

Advisory locks are application-level locks in PostgreSQL:

```sql
-- Session-level (held until released or disconnect)
SELECT pg_try_advisory_lock(12345);      -- Returns true if acquired
SELECT pg_advisory_unlock(12345);         -- Release

-- Transaction-level (auto-release at commit/rollback)
SELECT pg_try_advisory_xact_lock(12345);  -- Returns true if acquired
-- No unlock needed - releases at transaction end
```

**Key properties**:
- Fast (in-memory, no disk I/O)
- Auto-release on disconnect (crash-safe)
- Supports non-blocking try_lock
- 64-bit key space

### 12.4 Implementation: LockService

**File**: `infrastructure/locking.py`

```python
"""
Distributed Locking Service

Uses PostgreSQL advisory locks for coordination.
Zero external dependencies - just Postgres.
"""

import hashlib
import logging
from contextlib import asynccontextmanager
from typing import Optional

from psycopg_pool import AsyncConnectionPool

logger = logging.getLogger(__name__)


class LockService:
    """PostgreSQL-based distributed locking."""

    ORCHESTRATOR_LOCK = "rmhdag:orchestrator"
    JOB_LOCK_PREFIX = "rmhdag:job:"

    def __init__(self, pool: AsyncConnectionPool):
        self.pool = pool
        self._orchestrator_conn = None

    @staticmethod
    def _hash_to_lock_id(key: str) -> int:
        """Convert string key to int64 for PostgreSQL advisory lock."""
        h = hashlib.sha256(key.encode()).digest()[:8]
        return int.from_bytes(h, byteorder='big', signed=True)

    # ═══════════════════════════════════════════════════════════════
    # LAYER 1: ORCHESTRATOR LOCK
    # ═══════════════════════════════════════════════════════════════

    async def try_acquire_orchestrator_lock(self) -> bool:
        """
        Try to acquire the global orchestrator lock.

        Only one orchestrator should run at a time. This lock is held
        for the lifetime of the orchestrator process via a dedicated connection.

        Returns:
            True if lock acquired, False if another orchestrator holds it
        """
        lock_id = self._hash_to_lock_id(self.ORCHESTRATOR_LOCK)

        # Get a dedicated connection to hold the session-level lock
        self._orchestrator_conn = await self.pool.getconn()

        try:
            result = await self._orchestrator_conn.execute(
                "SELECT pg_try_advisory_lock(%s) as acquired",
                (lock_id,),
            )
            row = await result.fetchone()
            acquired = row[0] if row else False

            if acquired:
                logger.info("Acquired orchestrator lock")
            else:
                logger.warning(
                    "Failed to acquire orchestrator lock - another instance running?"
                )
                await self.pool.putconn(self._orchestrator_conn)
                self._orchestrator_conn = None

            return acquired

        except Exception as e:
            logger.error(f"Error acquiring orchestrator lock: {e}")
            if self._orchestrator_conn:
                await self.pool.putconn(self._orchestrator_conn)
                self._orchestrator_conn = None
            return False

    async def release_orchestrator_lock(self) -> None:
        """Release the orchestrator lock."""
        if self._orchestrator_conn:
            lock_id = self._hash_to_lock_id(self.ORCHESTRATOR_LOCK)
            try:
                await self._orchestrator_conn.execute(
                    "SELECT pg_advisory_unlock(%s)",
                    (lock_id,),
                )
                logger.info("Released orchestrator lock")
            finally:
                await self.pool.putconn(self._orchestrator_conn)
                self._orchestrator_conn = None

    # ═══════════════════════════════════════════════════════════════
    # LAYER 2: JOB LOCK
    # ═══════════════════════════════════════════════════════════════

    @asynccontextmanager
    async def job_lock(self, job_id: str, blocking: bool = False):
        """
        Context manager for job-level locking.

        Args:
            job_id: Job to lock
            blocking: If True, wait for lock. If False, skip if unavailable.

        Yields:
            True if lock acquired, False otherwise

        Usage:
            async with lock_service.job_lock(job_id) as acquired:
                if acquired:
                    await process_job(job)
        """
        lock_id = self._hash_to_lock_id(f"{self.JOB_LOCK_PREFIX}{job_id}")

        async with self.pool.connection() as conn:
            if blocking:
                # Blocking: wait for lock
                await conn.execute(
                    "SELECT pg_advisory_xact_lock(%s)", (lock_id,)
                )
                acquired = True
            else:
                # Non-blocking: return immediately
                result = await conn.execute(
                    "SELECT pg_try_advisory_xact_lock(%s) as acquired",
                    (lock_id,),
                )
                row = await result.fetchone()
                acquired = row[0] if row else False

            if acquired:
                logger.debug(f"Acquired lock for job {job_id}")
            else:
                logger.debug(f"Job {job_id} locked by another process")

            try:
                yield acquired
            finally:
                # Transaction-level locks auto-release
                if acquired:
                    logger.debug(f"Released lock for job {job_id}")


class LockNotAcquired(Exception):
    """Raised when a required lock cannot be acquired."""
    pass
```

### 12.5 Implementation: Version Column (Layer 3)

**Model update** (`core/models/node.py`):

```python
class NodeState(NodeData):
    """Runtime state of a node within a job."""

    # ... existing fields ...

    # Optimistic locking
    version: int = Field(default=1, description="Version for optimistic locking")
```

**Repository update** (`repositories/node_repo.py`):

```python
async def update(self, node: NodeState) -> bool:
    """
    Update a node state with optimistic locking.

    Returns:
        True if update succeeded, False if version conflict
    """
    async with self.pool.connection() as conn:
        result = await conn.execute(
            f"""
            UPDATE {TABLE_NODES} SET
                status = %(status)s,
                task_id = %(task_id)s,
                output = %(output)s,
                error_message = %(error_message)s,
                retry_count = %(retry_count)s,
                started_at = %(started_at)s,
                completed_at = %(completed_at)s,
                version = version + 1
            WHERE job_id = %(job_id)s
              AND node_id = %(node_id)s
              AND version = %(version)s
            """,
            {
                "job_id": node.job_id,
                "node_id": node.node_id,
                "status": node.status.value,
                "task_id": node.task_id,
                "output": Json(node.output) if node.output else None,
                "error_message": node.error_message,
                "retry_count": node.retry_count,
                "started_at": node.started_at,
                "completed_at": node.completed_at,
                "version": node.version,
            },
        )

        if result.rowcount == 0:
            logger.warning(
                f"Version conflict updating node {node.node_id} "
                f"(expected version {node.version})"
            )
            return False

        node.version += 1
        return True
```

**Migration** (`api/bootstrap_routes.py`):

```python
{
    "table": "dag_node_states",
    "column": "version",
    "sql": """
        ALTER TABLE dagapp.dag_node_states
        ADD COLUMN IF NOT EXISTS version INTEGER DEFAULT 1 NOT NULL
    """,
},
{
    "table": "dag_jobs",
    "column": "version",
    "sql": """
        ALTER TABLE dagapp.dag_jobs
        ADD COLUMN IF NOT EXISTS version INTEGER DEFAULT 1 NOT NULL
    """,
},
```

### 12.6 Implementation: Orchestrator Integration

**Update Orchestrator.start()** (`orchestrator/loop.py`):

```python
async def start(self) -> None:
    """Start the orchestration loop."""
    if self._running:
        logger.warning("Orchestrator already running")
        return

    # Acquire orchestrator lock first
    self._has_orchestrator_lock = await self.lock_service.try_acquire_orchestrator_lock()
    if not self._has_orchestrator_lock:
        raise RuntimeError(
            "Cannot start: another orchestrator instance is running. "
            "Check for duplicate deployments."
        )

    self._running = True
    self._started_at = datetime.utcnow()
    self._publisher = await get_publisher()
    self._task = asyncio.create_task(self._run_loop())
    logger.info("Orchestrator started with exclusive lock")
```

**Update Orchestrator.stop()** (`orchestrator/loop.py`):

```python
async def stop(self) -> None:
    """Stop the orchestration loop."""
    self._running = False

    if self._task:
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        self._task = None

    # Release orchestrator lock
    if self._has_orchestrator_lock:
        await self.lock_service.release_orchestrator_lock()
        self._has_orchestrator_lock = False

    logger.info("Orchestrator stopped, lock released")
```

**Update _process_job()** (`orchestrator/loop.py`):

```python
async def _process_job(self, job: Job) -> None:
    """Process a single active job with job-level locking."""
    async with self.lock_service.job_lock(job.job_id, blocking=False) as acquired:
        if not acquired:
            logger.debug(f"Skipping job {job.job_id} - locked by another process")
            return

        await self._process_job_internal(job)
```

### 12.7 Service Layer: Handling Version Conflicts

**Update NodeService.check_and_ready_nodes()** (`services/node_service.py`):

```python
async def check_and_ready_nodes(
    self,
    job_id: str,
    workflow: WorkflowDefinition,
) -> List[NodeState]:
    """
    Check pending nodes and mark them READY if dependencies are met.

    Uses optimistic locking - logs and skips on version conflict.
    """
    all_nodes = await self.node_repo.get_all_for_job(job_id)
    completed_nodes: Set[str] = {
        n.node_id for n in all_nodes
        if n.status in (NodeStatus.COMPLETED, NodeStatus.SKIPPED)
    }

    newly_ready: List[NodeState] = []
    pending_nodes = await self.node_repo.get_pending_nodes(job_id)

    for node in pending_nodes:
        node_def = workflow.nodes.get(node.node_id)
        if node_def is None:
            continue

        if self._dependencies_met(node.node_id, node_def, completed_nodes, workflow):
            node.mark_ready()

            # Optimistic update with version check
            success = await self.node_repo.update(node)
            if success:
                newly_ready.append(node)
                logger.debug(f"Node {node.node_id} is now READY")

                if self._event_service:
                    await self._event_service.emit_node_ready(
                        node, list(completed_nodes)
                    )
            else:
                # Version conflict - another process modified this node
                # This is fine - will catch it next cycle
                logger.debug(
                    f"Version conflict marking {node.node_id} ready, will retry"
                )

    return newly_ready
```

### 12.8 Why Each Lock Lives Where It Does

| Lock Type | Layer | Location | Rationale |
|-----------|-------|----------|-----------|
| Orchestrator lock | Infrastructure | `LockService` | Global singleton, infrastructure concern. Not business logic. |
| Job lock | Infrastructure | `LockService` | Coordination mechanism, called from orchestrator. |
| Version check | Repository | `NodeRepository` | Data integrity at storage boundary. Prevents bad writes regardless of caller. |
| Conflict handling | Service | `NodeService` | Business logic decides: retry vs skip vs fail. Has context to make decision. |

**The principle**: Each layer has a single responsibility:
- **Infrastructure**: "How do we coordinate?"
- **Repository**: "How do we ensure writes are valid?"
- **Service**: "What do we do when something goes wrong?"

### 12.9 Testing Concurrency

```python
# tests/test_locking.py

import pytest
import asyncio
from infrastructure.locking import LockService


@pytest.mark.asyncio
async def test_orchestrator_lock_exclusive(pool):
    """Only one orchestrator can acquire the lock."""
    lock1 = LockService(pool)
    lock2 = LockService(pool)

    # First acquires
    assert await lock1.try_acquire_orchestrator_lock() is True

    # Second fails
    assert await lock2.try_acquire_orchestrator_lock() is False

    # First releases, second can now acquire
    await lock1.release_orchestrator_lock()
    assert await lock2.try_acquire_orchestrator_lock() is True


@pytest.mark.asyncio
async def test_job_lock_non_blocking(pool):
    """Job locks skip when already held."""
    lock_service = LockService(pool)
    job_id = "test-job-123"

    results = []

    async def worker(name: str):
        async with lock_service.job_lock(job_id, blocking=False) as acquired:
            results.append((name, acquired))
            if acquired:
                await asyncio.sleep(0.1)  # Hold lock briefly

    # Run two workers concurrently
    await asyncio.gather(worker("A"), worker("B"))

    # One should acquire, one should skip
    acquired_count = sum(1 for _, acquired in results if acquired)
    assert acquired_count == 1


@pytest.mark.asyncio
async def test_version_conflict_detection(pool, node_repo):
    """Version conflicts are detected and reported."""
    node = NodeState(job_id="job1", node_id="node1", status=NodeStatus.PENDING)
    await node_repo.create(node)

    # Load two copies
    node_a = await node_repo.get("job1", "node1")
    node_b = await node_repo.get("job1", "node1")

    # Both have version 1
    assert node_a.version == 1
    assert node_b.version == 1

    # A updates successfully
    node_a.mark_ready()
    assert await node_repo.update(node_a) is True
    assert node_a.version == 2

    # B's update fails (version mismatch)
    node_b.mark_ready()
    assert await node_repo.update(node_b) is False
```

### 12.10 Future: Horizontal Scaling

With this locking foundation, horizontal scaling becomes straightforward:

```python
# Future: Multiple orchestrators with job partitioning

async def _process_jobs_partitioned(self):
    """Process jobs assigned to this orchestrator instance."""

    # Each orchestrator takes jobs where hash(job_id) % N == instance_id
    my_jobs = await self.job_service.list_active_jobs_for_partition(
        partition_id=self.instance_id,
        total_partitions=self.total_instances,
    )

    for job in my_jobs:
        # Job lock still needed for edge cases (rebalancing, failover)
        async with self.lock_service.job_lock(job.job_id) as acquired:
            if acquired:
                await self._process_job_internal(job)
```

The current locking implementation enables this future without changes.

---

## Appendix: Key File Locations

### Existing Files

| Component | File | Key Lines |
|-----------|------|-----------|
| Main loop | `orchestrator/loop.py` | 109-143 (loop), 184-211 (process_job) |
| Dependency resolution | `orchestrator/engine/evaluator.py` | 502-607 |
| Template resolution | `orchestrator/engine/templates.py` | 55-116 |
| Job model | `core/models/job.py` | 32-184 |
| Node model | `core/models/node.py` | 31-251 |
| Task result model | `core/models/task.py` | 103-235 |
| Event model | `core/models/events.py` | 69-234 |
| Job repository | `repositories/job_repo.py` | All |
| Node repository | `repositories/node_repo.py` | All |
| Task repository | `repositories/task_repo.py` | All |
| Job service | `services/job_service.py` | All |
| Node service | `services/node_service.py` | 130-168 (process_result) |
| API routes | `api/routes.py` | 328-403 (callbacks) |

### New Files (To Be Created)

| Priority | Component | File |
|----------|-----------|------|
| P0.1 | Event Repository | `repositories/event_repo.py` |
| P0.1 | Event Service | `services/event_service.py` |
| **P1.5** | **Lock Service** | **`infrastructure/locking.py`** |
| P2.2 | Fan-Out Logic | `orchestrator/engine/fan_out.py` |
| P2.2 | Fan-In Logic | `orchestrator/engine/fan_in.py` |
| P2.3 | Checkpointable Handler | `handlers/base.py` |
| P3.1 | Progress Service | `services/progress_service.py` |
| P3.2 | Timeline Service | `services/timeline_service.py` |
| P3.3 | Metrics Service | `services/metrics_service.py` |
| P3.4 | Tracing Config | `infrastructure/tracing.py` |

---

## Appendix: Estimated Total Effort

| Priority | Items | New Lines | Modified Lines |
|----------|-------|-----------|----------------|
| P0 | Event Logging, Retry, Stats | ~350 | ~65 |
| P1 | Stale Bug, Version Pin, Transactions | ~0 | ~105 |
| P2 | Conditional, Fan-Out/In, Checkpoint | ~440 | ~80 |
| P3 | Progress, Timeline, Metrics, Tracing | ~590 | ~80 |
| **Total** | | **~1,380** | **~330** |

---

*Document generated: 02 FEB 2026*
*Last updated: 02 FEB 2026 - Added P2 & P3 implementation plans*
