# DAG Orchestrator Architecture

**Last Updated**: 02 FEB 2026
**Version**: 0.2.0.0

---

## Overview

The DAG Orchestrator (`rmhdagmaster`) is the workflow execution engine for the geospatial data platform. It coordinates multi-step processing pipelines and integrates with external B2B applications.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           PLATFORM ARCHITECTURE                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   B2B APPS (External)              PLATFORM (Internal)                      │
│   ───────────────────              ──────────────────                       │
│                                                                             │
│   ┌─────────────┐                  ┌─────────────────┐                      │
│   │     DDH     │  ──request───►   │  rmhdagmaster   │                      │
│   │  (dataset/  │                  │  (orchestrator) │                      │
│   │  resource/  │  ◄──callback──   │                 │                      │
│   │  version)   │                  └────────┬────────┘                      │
│   └─────────────┘                           │                               │
│                                             │ dispatches                    │
│   ┌─────────────┐                           ▼                               │
│   │   ArcGIS    │                  ┌─────────────────┐                      │
│   │  (item_id/  │  ──request───►   │   rmhdagworker  │                      │
│   │  layer_idx) │                  │    (workers)    │                      │
│   └─────────────┘                  └────────┬────────┘                      │
│                                             │                               │
│   ┌─────────────┐                           │ produces                      │
│   │  Future B2B │                           ▼                               │
│   │  (???)      │                  ┌─────────────────┐                      │
│   └─────────────┘                  │ GeospatialAsset │                      │
│                                    │   (rmhgeoapi)   │                      │
│                                    └─────────────────┘                      │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 1. Critical Design Principles

### 1.1 B2B ID Decoupling

**External B2B identifier schemes are NEVER adopted as internal identity.**

The platform abstracts away whatever combination of unique identifiers a B2B application uses. We do not map to, inherit, or depend on external ID structures.

```
B2B Request                              Internal Platform
───────────────────────────────────      ─────────────────────────────────

DDH sends:                               We generate:
  dataset_id:  "0038272"        ─┐
  resource_id: "shapefile"      ─┼──►    asset_id = SHA256(ddh|{...})[:32]
  version_id:  "2025-01"        ─┘                   ▲
                                                     │
                                         This is OUR identity.
                                         DDH's scheme is stored as metadata
                                         in platform_refs (JSONB), but never
                                         used as a primary key.
```

| Scenario | Without Abstraction | With Our Architecture |
|----------|--------------------|-----------------------|
| DDH changes ID format | Breaking change | No impact |
| Add new B2B platform | Schema migration | Just new platform_id |
| Query across platforms | Platform-specific logic | Unified by asset_id |
| DDH uses compound keys | Complex joins | Single deterministic hash |

### 1.2 Semantic Versioning vs Revision Tracking

| Concept | Owner | Purpose | Scope |
|---------|-------|---------|-------|
| **Semantic Version** | B2B App | Content lineage | Their domain |
| **Revision** | Our Platform | Operational rollback | Our domain |

**Semantic versioning is explicitly the domain of the B2B application.** When DDH says "World Bank Boundaries v5", that's their semantic version indicating content changes. We **store** this in `platform_refs.version_id` for traceability, but we do NOT manage, increment, validate, or make decisions based on it.

Our `revision` field tracks **operational state** for the same B2B semantic version:

```
DDH version_id: "2025-01" (their v5)
│
├── revision 1: Initial processing
├── revision 2: Reprocessed after worker bug fix
├── revision 3: Reprocessed with updated COG parameters
└── revision 4: Current (after rollback from revision 3)
```

**Invariant**: For any GeospatialAsset, `platform_refs` and `asset_id` stay CONSTANT across revisions. Only revision number increments.

---

## 2. Data Flow Architecture

### 2.1 Complete Job Lifecycle

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              DATA FLOW                                       │
└─────────────────────────────────────────────────────────────────────────────┘

    B2B Partner                    External User
         │                              │
         ▼                              ▼
   ┌───────────┐                 ┌───────────┐
   │  DDH Hub  │                 │  Portal   │
   └─────┬─────┘                 └─────┬─────┘
         │                              │
         └──────────────┬───────────────┘
                        ▼
         ┌──────────────────────────────┐
         │     Platform Function App    │
         │     (rmhazuregeoapi)         │
         │                              │
         │  /platform/submit_dag        │
         │  /platform/dag_status/{id}   │
         └──────────────┬───────────────┘
                        │
                        ▼
         ┌──────────────────────────────┐
         │     dag-jobs Queue           │
         │     (Service Bus)            │
         └──────────────┬───────────────┘
                        │
                        ▼
┌───────────────────────────────────────────────────────────────────┐
│                         ORCHESTRATOR                               │
│                                                                    │
│  ┌─────────────┐    ┌──────────────┐    ┌─────────────────────┐  │
│  │ Job Creator │───▶│ DAG Evaluator│───▶│ Task Dispatcher     │  │
│  │             │    │              │    │                     │  │
│  │ Reads queue │    │ Find ready   │    │ Send to worker      │  │
│  │ Creates job │    │ nodes        │    │ queues              │  │
│  │ In DB       │    │              │    │                     │  │
│  └─────────────┘    └──────────────┘    └──────────┬──────────┘  │
│                                                     │             │
│                          ┌──────────────────────────┘             │
│                          │                                        │
│                          ▼                                        │
│         ┌────────────────────────────────────────┐               │
│         │            Service Bus Queues          │               │
│         ├────────────────────┬───────────────────┤               │
│         │ functionapp-tasks  │ container-tasks   │               │
│         │ (light work)       │ (heavy work)      │               │
│         └─────────┬──────────┴─────────┬─────────┘               │
│                   │                    │                          │
└───────────────────│────────────────────│──────────────────────────┘
                    │                    │
                    ▼                    ▼
           ┌────────────────┐   ┌────────────────┐
           │ dag-worker-    │   │ dag-worker-    │
           │ light          │   │ heavy          │
           │                │   │                │
           │ Execute task   │   │ Execute task   │
           │ Report result  │   │ Report result  │
           └───────┬────────┘   └───────┬────────┘
                   │                    │
                   └────────┬───────────┘
                            │
                            ▼
         ┌──────────────────────────────────────────┐
         │              PostgreSQL                  │
         │              dagapp schema               │
         │                                          │
         │  dag_task_results ◄── Workers write here │
         │  dag_node_states  ◄── Orchestrator reads │
         │  dag_jobs         ◄── Job state          │
         │  dag_job_events   ◄── Audit trail        │
         └──────────────────────────────────────────┘
```

### 2.2 Orchestrator Loop

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         ORCHESTRATOR LOOP                                    │
│                         (runs every 5 seconds)                               │
└─────────────────────────────────────────────────────────────────────────────┘

 START
   │
   ▼
┌──────────────────────┐
│ 1. Poll dag-jobs     │
│    queue for new     │
│    job requests      │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│ 2. Create Job record │
│    in dag_jobs       │
│    Create NodeState  │
│    for each node     │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│ 3. Query for jobs    │
│    with pending work │
│    (PENDING/RUNNING) │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐      ┌──────────────────────────────────┐
│ 4. For each job:     │      │  DAG Evaluator Logic:            │
│    Run DAG Evaluator │ ───▶ │  - Check node dependencies       │
│                      │      │  - All deps COMPLETED? → READY   │
│                      │      │  - Evaluate conditionals         │
│                      │      │  - Expand fan-outs               │
└──────────┬───────────┘      └──────────────────────────────────┘
           │
           ▼
┌──────────────────────┐
│ 5. Dispatch READY    │
│    nodes to queues   │
│    Mark DISPATCHED   │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│ 6. Poll task_results │
│    for completed     │
│    tasks             │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│ 7. Update NodeState  │
│    from results      │
│    COMPLETED/FAILED  │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│ 8. Check for job     │
│    completion        │
│    (END node done?)  │
└──────────┬───────────┘
           │
           ▼
   SLEEP(5 seconds)
   │
   └─────▶ START
```

---

## 3. Queue Architecture

### 3.1 Service Bus Queues

```
┌─────────────────────────┐
│  dag-jobs               │
│  (job submission)       │
│                         │
│  Producer: Platform FA  │
│  Consumer: Orchestrator │
│                         │
│  Message:               │
│  {                      │
│    "workflow_id": "...",│
│    "inputs": {...},     │
│    "priority": 1,       │
│    "callback_url": ".." │
│  }                      │
└─────────────────────────┘

┌─────────────────────────┐      ┌─────────────────────────────┐
│  functionapp-tasks      │      │  container-tasks            │
│  (light work)           │      │  (heavy work)               │
│                         │      │                             │
│  Producer: Orchestrator │      │  Producer: Orchestrator     │
│  Consumer: Light Worker │      │  Consumer: Heavy Worker     │
│                         │      │                             │
│  Typical work:          │      │  Typical work:              │
│  - Validation           │      │  - COG translation          │
│  - STAC registration    │      │  - Large raster processing  │
│  - Small transforms     │      │  - Vector chunking          │
│  - API calls            │      │  - Memory-intensive ops     │
└─────────────────────────┘      └─────────────────────────────┘
```

### 3.2 Task Message Format

```json
{
  "task_id": "task-abc123",
  "job_id": "job-xyz789",
  "node_id": "validate",
  "handler": "raster_validate",
  "params": {
    "container_name": "bronze",
    "blob_name": "data/file.tif"
  },
  "timeout_seconds": 300,
  "retry_count": 0
}
```

### 3.3 Queue Routing Logic

Routing is declared in workflow YAML:

```yaml
nodes:
  validate:
    handler: raster_validate
    queue: functionapp-tasks    # Quick check
    next: process

  process:
    handler: raster_cog_docker
    queue: container-tasks      # Heavy lifting
    next: END
```

---

## 4. State Machines

### 4.1 Job State Machine

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

| From | To | Trigger |
|------|-----|---------|
| PENDING | RUNNING | First node dispatched |
| PENDING | CANCELLED | Manual cancellation |
| RUNNING | COMPLETED | All nodes terminal, none failed |
| RUNNING | FAILED | Any node failed (after max retries) |
| RUNNING | CANCELLED | Manual cancellation |

### 4.2 Node State Machine

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

## 5. Database Schema

### 4.1 dagapp Schema

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        dagapp SCHEMA                                         │
└─────────────────────────────────────────────────────────────────────────────┘

                    ┌──────────────────────────────────┐
                    │          dag_jobs                │
                    │          (job instances)         │
                    ├──────────────────────────────────┤
                    │ job_id (PK)         VARCHAR(64)  │
                    │ workflow_id         VARCHAR(64)  │
                    │ status              ENUM         │
                    │ input_params        JSONB        │
                    │ created_at          TIMESTAMP    │
                    │ updated_at          TIMESTAMP    │
                    │ completed_at        TIMESTAMP    │
                    └───────────────┬──────────────────┘
                                    │
                                    │ 1:N
                                    │
               ┌────────────────────┼────────────────────┐
               │                    │                    │
               ▼                    ▼                    ▼
┌──────────────────────┐ ┌──────────────────────┐ ┌──────────────────────┐
│   dag_node_states    │ │   dag_task_results   │ │   dag_job_events     │
│   (node runtime)     │ │   (worker reports)   │ │   (audit trail)      │
├──────────────────────┤ ├──────────────────────┤ ├──────────────────────┤
│ job_id (PK,FK)       │ │ task_id (PK)         │ │ event_id (PK,SERIAL) │
│ node_id (PK)         │ │ job_id (FK)          │ │ job_id (FK)          │
│ status               │ │ node_id              │ │ node_id              │
│ task_id              │ │ status               │ │ event_type           │
│ output (JSONB)       │ │ output (JSONB)       │ │ event_status         │
│ error_message        │ │ error_message        │ │ event_data (JSONB)   │
│ retry_count          │ │ worker_id            │ │ checkpoint_name      │
│ timestamps...        │ │ reported_at          │ │ created_at           │
└──────────────────────┘ └──────────────────────┘ └──────────────────────┘
```

### 4.2 Schema Ownership

```
┌──────────────────────────────────────────────────────────────────────┐
│                         Azure PostgreSQL                             │
│                         rmhpostgres.postgres.database.azure.com      │
└──────────────────────────────────────────────────────────────────────┘
                                      │
         ┌────────────────────────────┼────────────────────────────┐
         │                            │                            │
         ▼                            ▼                            ▼
   ┌───────────────┐          ┌───────────────┐          ┌───────────────┐
   │  dagapp       │          │  app          │          │  geo          │
   │  (NEW)        │          │  (Epoch 4)    │          │  (Replicated) │
   │               │          │               │          │               │
   │ Owned by:     │          │ Owned by:     │          │ Owned by:     │
   │ rmhdagmaster  │          │ rmhgeoapi     │          │ Service Layer │
   └───────────────┘          └───────────────┘          └───────────────┘

Note: During migration, both schemas coexist.
```

---

## 6. GeospatialAsset Integration

### 5.1 Request vs Job Separation

**Requests and Jobs are separate entities.**

| Concept | Request (Work Order) | Job (Execution Record) |
|---------|---------------------|------------------------|
| Purpose | "Please process this" | "Here's what I did" |
| Timing | Acknowledged immediately | Created when work starts |
| Identity | `request_id` (B2B link) | `job_id` (internal tracking) |
| Lifecycle | Can exist without job | Always tied to a request |

### 5.2 Ownership Model (Mechanic Shop Analogy)

```
Client brings BMW,                       POST /api/platform/submit
places work order                        with dataset_id, resource_id
    │                                        │
    ▼                                        ▼
Front desk creates                       Platform creates
master record for vehicle                GeospatialAsset (if new)
(VIN = asset_id)                         (asset_id generated)
    │                                        │
    ▼                                        ▼
Mechanic does the work                   Orchestrator creates Job,
(oil change, brakes, etc.)               dispatches to Workers
    │                                        │
    ▼                                        ▼
Mechanic marks job done,                 Orchestrator updates asset:
updates the master record                processing_status = completed
```

**Key Insight**: The mechanic (Orchestrator) updates the master record directly when work is done. The manager (Platform) doesn't need to "check and update" - the record is already current.

### 5.3 Field Ownership

| Concern | Platform | Orchestrator |
|---------|----------|--------------|
| **Entity Creation** | Creates asset | Never creates |
| **Entity Deletion** | Soft-deletes | Never deletes |
| **Approval State** | Updates | Never touches |
| **Processing Status** | Never touches | Updates |
| **Content Hash** | Never touches | Updates |
| **Job Linkage** | Never touches | Updates current_job_id |

**The Boundary**: Platform manages **business state**. Orchestrator manages **execution state**.

---

## 7. Workflow Definition

### 6.1 YAML Structure

```yaml
workflow_id: raster_processing
name: Raster Processing Pipeline
version: 1

inputs:
  container_name:
    type: string
    required: true
  blob_name:
    type: string
    required: true
  target_crs:
    type: string
    default: "EPSG:3857"

nodes:
  START:
    type: start
    next: validate

  validate:
    type: task
    handler: raster_validate
    queue: functionapp-tasks
    params:
      container_name: "{{ inputs.container_name }}"
      blob_name: "{{ inputs.blob_name }}"
    next: route_by_size

  route_by_size:
    type: conditional
    condition_field: "{{ nodes.validate.output.file_size_mb }}"
    branches:
      - condition: "< 100"
        next: process_fa
      - default: true
        next: process_docker

  process_docker:
    type: task
    handler: raster_cog_docker
    queue: container-tasks
    timeout_seconds: 7200
    params:
      source_url: "{{ nodes.validate.output.blob_url }}"
    next: END

  END:
    type: end
```

### 6.2 Node Types

| Type | Purpose |
|------|---------|
| `start` | Entry point. Exactly one per workflow. |
| `end` | Exit point. At least one per workflow. |
| `task` | Executes a handler function. |
| `conditional` | Routes based on a value. |
| `fan_out` | Creates N parallel tasks from array. |
| `fan_in` | Waits for multiple upstream nodes. |

---

## 8. Handler Library

### 7.1 Handler Interface

```python
@dataclass
class HandlerContext:
    """Context passed to every handler."""
    task_id: str
    job_id: str
    node_id: str
    params: Dict[str, Any]
    logger: logging.Logger
    pool: Optional[AsyncConnectionPool] = None
    blob_repository: Optional[BlobRepository] = None

@dataclass
class HandlerResult:
    """Result returned by every handler."""
    success: bool
    output: Dict[str, Any] = field(default_factory=dict)
    error_message: Optional[str] = None
    metrics: Dict[str, Any] = field(default_factory=dict)

# Every handler follows this signature:
async def handler_name(ctx: HandlerContext) -> HandlerResult:
    pass
```

### 7.2 Handler Registration

```python
@register_handler("raster.validate", queue="container-tasks", timeout_seconds=600)
async def validate(ctx: HandlerContext) -> HandlerResult:
    blob_path = ctx.params.get("blob_path")
    # ... validation logic ...
    return HandlerResult(
        success=True,
        output={"crs": "EPSG:4326", "bounds": [...], "size_mb": 150}
    )
```

### 7.3 Registered Handlers (Current)

**Raster Handlers** (`handlers/raster/`):
- `raster.validate` - Validate file, get CRS/bounds/size metadata
- `raster.create_cog` - Create Cloud-Optimized GeoTIFF
- `raster.blob_to_mount` - Copy blob to local mount
- `raster.generate_tiling_scheme` - Generate tile grid
- `raster.process_tile_batch` - Process batch of tiles
- `raster.stac_ensure_collection` - Create/verify STAC collection
- `raster.stac_register_item` - Register single STAC item
- `raster.stac_register_items` - Batch register STAC items

**Test Handlers** (`handlers/examples.py`):
- `echo`, `hello_world`, `sleep`, `fail`, `multi_phase`, `size_check`, `chunk_processor`

---

## 9. Repository Architecture

### 8.1 Repository Hierarchy

```
AsyncBaseRepository (ABC)
     │
     │   Defines interface:
     │   - create(entity) → entity
     │   - get_by_id(id) → Optional[entity]
     │   - update(entity) → entity
     │   - list(**filters) → List[entity]
     │
     ▼
AsyncPostgreSQLRepository
     │
     │   Provides:
     │   - Connection pool management
     │   - Auto Json() wrapping for dicts
     │   - dict_row factory enforcement
     │   - Named parameter building
     │
     ├─── JobRepository
     ├─── NodeStateRepository
     ├─── TaskResultRepository
     └─── JobEventRepository
```

### 8.2 Data Integrity Flow

```
External Input (JSON)
     │
     ▼
Pydantic Model (validates)
     │
     ▼
Service Layer (business logic)
     │
     ▼
Repository (DB operations)
     │
     ▼
Database (JSONB stored)
```

Every piece of data entering Python (from API, queue, or database) MUST go through a Pydantic model for validation.

---

## 10. Checkpoint Management

### 9.1 Why Checkpoints?

Long-running Docker tasks (30+ minutes) need resumability:
- Container may be killed (spot instance, scaling, deployment)
- SIGTERM gives ~30 seconds to save state
- Resume from last checkpoint instead of restart

### 9.2 Checkpoint Flow

```
Task Start
    │
    ▼
┌─────────────────────┐
│ Load checkpoint     │
│ (if exists)         │
└──────────┬──────────┘
           │
    ┌──────┴──────┐
    │             │
    ▼             ▼
No checkpoint   Has checkpoint
    │             │
    ▼             ▼
Start Phase 1   Skip to saved phase
    │             │
    └──────┬──────┘
           │
           ▼
Execute phase → Validate artifact → Save checkpoint → Next phase?
```

### 9.3 Graceful Shutdown

```python
class TaskExecutor:
    def __init__(self):
        self.shutdown_requested = False
        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _handle_sigterm(self, signum, frame):
        self.shutdown_requested = True
        # Worker has ~30 seconds to save checkpoint
```

---

## 11. Error Handling

### 10.1 Failure Detection

**Epoch 4**: Task starts → ... silence ... → 24 hours later: "where is it?"

**Epoch 5**: Task starts → Orchestrator polls every 5 seconds → Timeout detected!

```python
for node in nodes_with_status(DISPATCHED):
    if now() - node.dispatched_at > node.timeout:
        mark_failed(node, "Timeout")
        if node.retry_count < node.max_retries:
            schedule_retry(node)
        else:
            fail_job(node.job_id)
```

### 10.2 Retry Policy

```yaml
nodes:
  process:
    type: task
    handler: raster_cog_docker
    retry:
      max_attempts: 3
      backoff: exponential
      initial_delay_seconds: 5
      max_delay_seconds: 300
```

---

## 12. Platform Integration

### 11.1 Anti-Corruption Layer

The Platform Function App (rmhazuregeoapi) serves as an Anti-Corruption Layer between external B2B systems and the internal DAG orchestrator.

```
External Systems                    Internal Systems
(DDH, Portal, etc.)                 (DAG Orchestrator)

      │   Different protocols,             │   Clean internal
      │   auth methods, formats            │   domain model
      │                                    │
      ▼                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      Platform Function App                          │
│                      (Anti-Corruption Layer)                        │
│                                                                     │
│   1. Receive DDH/Portal request                                     │
│   2. Validate & authenticate                                        │
│   3. Transform to internal format                                   │
│   4. Send to dag-jobs queue                                         │
│   5. Return job_id to caller                                        │
└─────────────────────────────────────────────────────────────────────┘
```

### 11.2 Callback Architecture

```
Job completes
     │
     ▼
┌────────────────────────┐
│ Check callback_url     │
│ in job metadata        │
└───────────┬────────────┘
            │
     ┌──────┴──────┐
     │             │
     ▼             ▼
 Has callback   No callback
     │             │
     ▼             │
POST to callback   │
{job_id, status,   │
 result_url}       │
     │             │
     └──────┬──────┘
            │
         Done
```

---

## Key Invariants

1. **Never use B2B identifiers as primary keys**
2. **Revision ≠ Semantic Version** (we own revision, they own version)
3. **platform_refs is immutable** for a given asset_id
4. **DAG model is independent** of B2B data models
5. **All data through Pydantic** before touching the database
6. **Repository pattern** for all database access

---

## References

- `CLAUDE.md` - Design principles and code patterns
- `docs/DEPLOYMENT.md` - Azure resources, build commands, environment variables
- `TODO.md` - Implementation plan and status
