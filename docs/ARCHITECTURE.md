# rmhdagmaster Architecture

**Last Updated**: 06 FEB 2026
**Version**: 0.3.0.0

> For implementation details, task checklists, and code specs, see `IMPLEMENTATION.md`.

---

## 1. System Overview & Design Principles

### 1.1 What rmhdagmaster Is

The DAG Orchestrator (`rmhdagmaster`) is the workflow execution engine for the geospatial data platform. It coordinates multi-step processing pipelines (raster translation, vector tiling, STAC registration) and integrates with external B2B applications (DDH, ArcGIS, GeoNode).

### 1.2 Why It Exists

**Epoch 5 represents a fundamental architectural shift**: separating orchestration from execution.

| Epoch | Core Architecture | Insight |
|-------|-------------------|---------|
| 1-3 | Previous iterations | - |
| 4 | Function App + Docker workers with distributed orchestration | "Build a proper geospatial platform on Azure" |
| **5** | Dedicated orchestrator + dumb workers | "Orchestration is its own concern -- separate it from execution" |

The previous "last task turns out the lights" pattern embedded orchestration logic inside workers. Every worker ran a `CoreMachine`, every worker made coordination decisions. This was fundamentally fragile -- distributed consensus without a consensus protocol, papered over with janitor services, heartbeats, and atomic SQL. Epoch 5 extracts all coordination into a single orchestrator process.

```
Epoch 4: Workers ARE the orchestration (distributed state, defensive coding)
         Every worker has CoreMachine, every worker makes decisions

Epoch 5: Workers EXECUTE, Orchestrator ORCHESTRATES (separation of concerns)
         Workers are dumb. One orchestrator makes all decisions.
```

**Resource context**: The platform runs on Azure App Service Environment (ASE) with virtually unlimited resources. Scaling to zero is not a priority. Always-on services are acceptable.

### 1.3 Three Deployment Targets

One codebase builds three deployment targets:

| Target | Base Image | Size | Purpose |
|--------|-----------|------|---------|
| **Function App** | Azure Functions runtime | ~50MB | Gateway: B2B job submission, status queries, HTTP proxy |
| **DAG Orchestrator** | python:3.12-slim | ~250MB | Coordination: main loop, heartbeat, orphan recovery |
| **DAG Worker** | osgeo/gdal:ubuntu-full | ~2-3GB | Execution: GDAL processing, heavy ETL |

```
                           PLATFORM ARCHITECTURE

   B2B APPS (External)              PLATFORM (Internal)
   ───────────────────              ──────────────────

   ┌─────────────┐                  ┌───────────────────────────────────┐
   │     DDH     │  ──request───>   │        orchestrator-jobs          │
   │  (dataset/  │                  │        (Service Bus Queue)        │
   │  resource/  │                  └─────────────┬─────────────────────┘
   │  version)   │                                │
   └─────────────┘                                │ competing consumers
                                   ┌──────────────┼──────────────┐
   ┌─────────────┐                 v              v              v
   │   ArcGIS    │  ──request───>  ┌──────┐   ┌──────┐   ┌──────┐
   │  (item_id/  │                 │Orch 1│   │Orch 2│   │Orch N│
   │  layer_idx) │                 │      │   │      │   │      │
   └─────────────┘                 └──┬───┘   └──┬───┘   └──┬───┘
                                      │          │          │
   ┌─────────────┐                    │    dispatches       │
   │  Future B2B │                    v          v          v
   │  (???)      │                 ┌─────────────────────────────┐
   └─────────────┘                 │       rmhdagworker          │
                                   │       (workers N+)          │
                                   └─────────────┬───────────────┘
                                                 │
                     <────callback────           │ produces
                                                 v
                                   ┌─────────────────────────────┐
                                   │      GeospatialAsset        │
                                   │        (rmhgeoapi)          │
                                   └─────────────────────────────┘
```

### 1.4 Core Design Principles

1. **Orchestrator purity** -- The orchestrator coordinates, never executes business logic.
2. **Worker idempotency** -- Any task can be retried safely; workers handle duplicate delivery.
3. **State lives in Postgres** -- Workers are stateless; all durable state is in the database.
4. **Repository pattern** -- All database access goes through repository classes. No raw `psycopg.connect` outside the infrastructure layer.
5. **Pydantic is the schema** -- Models are the single source of truth for data structures. All serialization crosses boundaries via `.model_dump()` / `.model_validate()`.
6. **Explicit over implicit** -- Dependencies, queues, and handlers are declared in YAML. No magic.
7. **Fail fast, recover gracefully** -- Errors surface immediately; system resumes from last known state.
8. **Observable by default** -- Every state transition logs a JobEvent.
9. **Defensive concurrency** -- PostgreSQL advisory locks prevent multi-instance conflicts. Optimistic locking (version columns) detects concurrent modifications.

---

## 2. Multi-Orchestrator Architecture

### 2.1 Design Philosophy

Azure App Service Environment (ASE) runs multiple container instances for high availability. Rather than fighting this with single-leader election, we embrace **competing consumers** where each orchestrator owns and processes its claimed jobs independently.

**Key Insight**: Jobs are independent workflow executions with no cross-job dependencies. This means orchestrators don't need to coordinate with each other -- they only need to claim exclusive ownership of jobs.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    MULTI-ORCHESTRATOR JOB DISTRIBUTION                       │
└─────────────────────────────────────────────────────────────────────────────┘

   Platform Function App              orchestrator-jobs Queue
   (B2B Gateway/ACL)                  (Service Bus)
         │                                   │
         │  POST /platform/submit            │
         v                                   │
   ┌───────────┐                             │
   │  Validate │                             │
   │  & Enqueue├────────────────────────────>│
   └───────────┘                             │
                                             │
              ┌──────────────────────────────┼──────────────────────────────┐
              │                              │                              │
              │         COMPETING CONSUMERS (Azure ASE Instances)           │
              │                              │                              │
              │    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐   │
              │    │Orchestrator │    │Orchestrator │    │Orchestrator │   │
              │    │  Instance 1 │    │  Instance 2 │    │  Instance N │   │
              │    │             │    │             │    │             │   │
              │    │ owner_id:   │    │ owner_id:   │    │ owner_id:   │   │
              │    │ "orch-a1b2" │    │ "orch-c3d4" │    │ "orch-e5f6" │   │
              │    └──────┬──────┘    └──────┬──────┘    └──────┬──────┘   │
              │           │                  │                  │          │
              │           │  Each claims &   │                  │          │
              │           │  owns jobs       │                  │          │
              │           v                  v                  v          │
              │    ┌─────────────────────────────────────────────────┐     │
              │    │                  PostgreSQL                     │     │
              │    │                                                 │     │
              │    │  dag_jobs WHERE owner_id = 'orch-a1b2'          │     │
              │    │  dag_jobs WHERE owner_id = 'orch-c3d4'          │     │
              │    │  dag_jobs WHERE owner_id = 'orch-e5f6'          │     │
              │    │                                                 │     │
              │    │  (Each orchestrator only queries its own jobs)  │     │
              │    └─────────────────────────────────────────────────┘     │
              │                                                            │
              └────────────────────────────────────────────────────────────┘
```

### 2.2 Job Ownership Model

Each orchestrator has a unique `owner_id` (UUID generated at startup). When an orchestrator claims a job from the queue, it writes its `owner_id` to the job record.

**Ownership Invariants**:
1. A job can only have one owner at a time
2. An orchestrator only polls/updates jobs where `owner_id = self.owner_id`
3. Ownership is claimed atomically via database transaction
4. Heartbeat maintains ownership; stale ownership can be reclaimed

### 2.3 Job Claiming Flow

```
                    orchestrator-jobs Queue
                           │
                           │ Message: {workflow_id, inputs, callback_url}
                           │
                           v
┌─────────────────────────────────────────────────────────────────────────────┐
│                        ORCHESTRATOR (any instance)                           │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  1. Receive message from queue                                              │
│                                                                             │
│  2. BEGIN TRANSACTION                                                       │
│     INSERT INTO dag_jobs (                                                  │
│       job_id, workflow_id, status, input_params,                            │
│       owner_id, owner_heartbeat_at                    <── CLAIM OWNERSHIP   │
│     ) VALUES (                                                              │
│       gen_uuid(), msg.workflow_id, 'PENDING', msg.inputs,                   │
│       self.owner_id, NOW()                                                  │
│     )                                                                       │
│     COMMIT                                                                  │
│                                                                             │
│  3. Complete message (remove from queue)                                    │
│                                                                             │
│  4. This orchestrator now owns the job exclusively                          │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 2.4 Heartbeat & Orphan Recovery

Orchestrators periodically update `owner_heartbeat_at` for their jobs. If an orchestrator crashes, its jobs become "orphaned" and can be reclaimed.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           HEARTBEAT MECHANISM                                │
└─────────────────────────────────────────────────────────────────────────────┘

Every 30 seconds, each orchestrator:

  UPDATE dag_jobs
  SET owner_heartbeat_at = NOW()
  WHERE owner_id = %(my_owner_id)s
    AND status IN ('PENDING', 'RUNNING')

┌─────────────────────────────────────────────────────────────────────────────┐
│                           ORPHAN RECOVERY                                    │
└─────────────────────────────────────────────────────────────────────────────┘

Every 60 seconds, any orchestrator can reclaim stale jobs:

  UPDATE dag_jobs
  SET owner_id = %(my_owner_id)s,
      owner_heartbeat_at = NOW()
  WHERE status IN ('PENDING', 'RUNNING')
    AND owner_heartbeat_at < NOW() - INTERVAL '2 minutes'   <── STALE
  RETURNING job_id

  (Atomic: only one orchestrator wins the UPDATE)
```

**Timing Configuration**:

| Parameter | Value | Purpose |
|-----------|-------|---------|
| Heartbeat interval | 30 seconds | Keep ownership alive |
| Orphan threshold | 2 minutes | Time before job considered abandoned |
| Orphan scan interval | 60 seconds | How often to check for orphans |

### 2.5 Benefits

| Concern | How It's Addressed |
|---------|-------------------|
| **Horizontal scaling** | Add more orchestrator instances; work distributes automatically |
| **Crash recovery** | Orphan reclaim picks up abandoned jobs |
| **No coordination** | Orchestrators are independent; no leader election needed |
| **No conflicts** | Each orchestrator only touches its own jobs |
| **Azure ASE friendly** | Multiple instances are an asset, not a problem |

---

## 3. Data Flow & Job Lifecycle

### 3.1 Complete Job Lifecycle

```
    B2B Partner                    External User
         │                              │
         v                              v
   ┌───────────┐                 ┌───────────┐
   │  DDH Hub  │                 │  Portal   │
   └─────┬─────┘                 └─────┬─────┘
         │                              │
         └──────────────┬───────────────┘
                        v
         ┌──────────────────────────────┐
         │     Platform Function App    │
         │     (rmhazuregeoapi)         │
         │                              │
         │  /platform/submit            │
         │  /platform/status/{id}       │
         └──────────────┬───────────────┘
                        │
                        v
         ┌──────────────────────────────┐
         │     dag-jobs Queue           │
         │     (Service Bus)            │
         └──────────────┬───────────────┘
                        │
                        v
┌───────────────────────────────────────────────────────────────────┐
│                         ORCHESTRATOR                               │
│                                                                    │
│  ┌─────────────┐    ┌──────────────┐    ┌─────────────────────┐  │
│  │ Job Creator │───>│ DAG Evaluator│───>│ Task Dispatcher     │  │
│  │             │    │              │    │                     │  │
│  │ Reads queue │    │ Find ready   │    │ Send to worker      │  │
│  │ Creates job │    │ nodes        │    │ queues              │  │
│  │ In DB       │    │              │    │                     │  │
│  └─────────────┘    └──────────────┘    └──────────┬──────────┘  │
│                                                     │             │
│                          ┌──────────────────────────┘             │
│                          │                                        │
│                          v                                        │
│         ┌────────────────────────────────────────┐               │
│         │            Service Bus Queues          │               │
│         ├────────────────────┬───────────────────┤               │
│         │ functionapp-tasks  │ container-tasks   │               │
│         │ (light work)       │ (heavy work)      │               │
│         └─────────┬──────────┴─────────┬─────────┘               │
│                   │                    │                          │
└───────────────────│────────────────────│──────────────────────────┘
                    │                    │
                    v                    v
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
                            v
         ┌──────────────────────────────────────────┐
         │              PostgreSQL                  │
         │              dagapp schema               │
         │                                          │
         │  dag_task_results <── Workers write here │
         │  dag_node_states  <── Orchestrator reads │
         │  dag_jobs         <── Job state          │
         │  dag_job_events   <── Audit trail        │
         └──────────────────────────────────────────┘
```

### 3.2 Request vs Job Separation

Requests and Jobs are separate entities. B2B applications interact only with requests; internal systems work with jobs.

| Concept | Request (Work Order) | Job (Execution Record) |
|---------|---------------------|------------------------|
| Purpose | "Please process this" | "Here's what I did" |
| Timing | Acknowledged immediately | Created when work starts |
| Identity | `request_id` (B2B link) | `job_id` (internal tracking) |
| Lifecycle | Can exist without job | Always tied to a request |
| Visibility | B2B sees this | B2B never sees this |

**Flow**:
1. B2B submits job request with their identity + parameters
2. Gateway generates `request_id` (UUID), returns immediately
3. Gateway queues job with `request_id` as internal `correlation_id`
4. Orchestrator claims job, creates internal `job_id`
5. B2B polls by `request_id` -- Gateway translates to internal lookup
6. B2B receives status/result -- never sees `job_id`

---

## 4. Orchestrator Loop

### 4.1 Loop Overview

The orchestrator runs a continuous loop (default: every 1 second) that processes results, evaluates dependencies, dispatches ready nodes, and checks for job completion.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         ORCHESTRATOR LOOP                                    │
│                         (runs every ~1-5 seconds)                           │
│                         (each instance has unique owner_id)                 │
└─────────────────────────────────────────────────────────────────────────────┘

 START
   │
   v
┌──────────────────────┐
│ 1. Poll orchestrator-│    <── Competing consumers pattern
│    jobs queue for    │        First orchestrator to receive
│    new job requests  │        claims ownership
└──────────┬───────────┘
           │
           v
┌──────────────────────┐
│ 2. Create Job record │    <── INSERT with owner_id = self.owner_id
│    with OWNERSHIP    │        This orchestrator now owns the job
│    Create NodeState  │
│    for each node     │
└──────────┬───────────┘
           │
           v
┌──────────────────────┐
│ 3. Query for MY jobs │    <── WHERE owner_id = self.owner_id
│    with pending work │        AND status IN ('PENDING', 'RUNNING')
│    (PENDING/RUNNING) │        (Only process own jobs!)
└──────────┬───────────┘
           │
           v
┌──────────────────────┐      ┌──────────────────────────────────┐
│ 4. For each job:     │      │  DAG Evaluator Logic:            │
│    Run DAG Evaluator │ ───> │  - Check node dependencies       │
│                      │      │  - All deps COMPLETED? -> READY  │
│                      │      │  - Evaluate conditionals         │
│                      │      │  - Expand fan-outs               │
└──────────┬───────────┘      └──────────────────────────────────┘
           │
           v
┌──────────────────────┐
│ 5. Dispatch READY    │
│    nodes to queues   │
│    Mark DISPATCHED   │
└──────────┬───────────┘
           │
           v
┌──────────────────────┐
│ 6. Poll task_results │    <── WHERE job_id IN (my owned jobs)
│    for completed     │
│    tasks             │
└──────────┬───────────┘
           │
           v
┌──────────────────────┐
│ 7. Update NodeState  │
│    from results      │
│    COMPLETED/FAILED  │
└──────────┬───────────┘
           │
           v
┌──────────────────────┐
│ 8. Check for job     │
│    completion        │
│    (END node done?)  │
└──────────┬───────────┘
           │
           v
┌──────────────────────┐
│ 9. Update heartbeat  │    <── UPDATE owner_heartbeat_at = NOW()
│    for my jobs       │        (Every 30 seconds)
└──────────┬───────────┘
           │
           v
┌──────────────────────┐
│ 10. Reclaim orphaned │    <── Claim jobs with stale heartbeat
│     jobs (optional)  │        (Every 60 seconds)
└──────────┬───────────┘
           │
           v
   SLEEP(interval)
   │
   └──────> START
```

### 4.2 Ownership Queries

```sql
-- Get my active jobs (step 3)
SELECT * FROM dagapp.dag_jobs
WHERE owner_id = %(my_owner_id)s
  AND status IN ('PENDING', 'RUNNING')

-- Update my heartbeat (step 9)
UPDATE dagapp.dag_jobs
SET owner_heartbeat_at = NOW()
WHERE owner_id = %(my_owner_id)s
  AND status IN ('PENDING', 'RUNNING')

-- Reclaim orphaned jobs (step 10)
UPDATE dagapp.dag_jobs
SET owner_id = %(my_owner_id)s,
    owner_heartbeat_at = NOW()
WHERE status IN ('PENDING', 'RUNNING')
  AND owner_heartbeat_at < NOW() - INTERVAL '2 minutes'
RETURNING job_id
```

### 4.3 Detailed Execution Trace: Echo Test Workflow

This trace walks through a complete job lifecycle using the `echo_test` workflow:

```yaml
workflow_id: echo_test
nodes:
  start:
    type: start
    next: [echo_handler]
  echo_handler:
    type: task
    handler: echo
    next: [end]
  end:
    type: end
```

**Phase 1 -- Job Creation**

`JobService.create_job()` performs the following in a single transaction:

| Action | State Change | Table |
|--------|--------------|-------|
| Generate job_id | - | - |
| Create Job model | `status: PENDING` | - |
| Insert job | - | `dag_jobs` |
| Create NodeState for each node | `status: PENDING` | `dag_node_states` |
| Mark START node ready | `PENDING -> READY` | `dag_node_states` |
| Emit JOB_CREATED event | - | `dag_job_events` |

Database state after Phase 1:
```
dag_jobs:        job_id: abc123, status: PENDING
dag_node_states: (abc123, start): READY
                 (abc123, echo_handler): PENDING
                 (abc123, end): PENDING
```

**Phase 2 -- First Orchestration Cycle**

The orchestrator finds job `abc123` and checks dependencies. The `start` node is already READY, so it auto-completes (START and END nodes never dispatch to workers):

```
(abc123, start): READY -> DISPATCHED -> RUNNING -> COMPLETED
```

The `echo_handler` node depends on `start`. Since `start` is not yet COMPLETED at the time of this cycle's dependency check, `echo_handler` stays PENDING.

**Phase 3 -- Second Orchestration Cycle**

On the next cycle, `start` is now COMPLETED. Dependency check promotes `echo_handler`:

| Node | Dependencies | All Complete? | Action |
|------|--------------|---------------|--------|
| echo_handler | [start] | Yes | PENDING -> READY |
| end | [start, echo_handler] | No | Stay PENDING |

The orchestrator dispatches `echo_handler` to Service Bus. The job transitions from PENDING to RUNNING on first dispatch.

Message sent to Service Bus:
```json
{
  "task_id": "abc123_echo_handler_0",
  "job_id": "abc123",
  "node_id": "echo_handler",
  "handler": "echo",
  "params": {"message": "hello"},
  "queue": "functionapp-tasks",
  "timeout_seconds": 300
}
```

**Phase 4 -- Worker Execution**

A worker pulls the message, executes the `echo` handler, and reports the result:

```json
{
  "task_id": "abc123_echo_handler_0",
  "status": "completed",
  "output": {"echoed_params": {"message": "hello"}},
  "worker_id": "worker-xyz"
}
```

The result is inserted into `dag_task_results` with `processed = false`.

**Phase 5 -- Result Processing Cycle**

The orchestrator queries `dag_task_results WHERE processed = false`, finds the result, and:

1. Updates `echo_handler` node: DISPATCHED -> COMPLETED
2. Marks the result as `processed = true`
3. Re-checks dependencies: `end` node depends on `[start, echo_handler]` -- both now COMPLETED
4. Promotes `end`: PENDING -> READY
5. Auto-completes `end` (END node)
6. Checks job completion: END node is COMPLETED -- job is done
7. Aggregates results and marks job COMPLETED

**Final state:**
```
dag_jobs:        abc123: COMPLETED
dag_node_states: (abc123, start): COMPLETED
                 (abc123, echo_handler): COMPLETED
                 (abc123, end): COMPLETED
dag_job_events:  job_created -> node_ready -> node_dispatched ->
                 job_started -> node_completed -> job_completed
```

### 4.4 Component Responsibilities

| Component | Responsibilities |
|-----------|------------------|
| **API Routes** | Accept job submissions, return results |
| **JobService** | Create jobs, track status, aggregate results |
| **NodeService** | Check dependencies, mark ready, process results |
| **Orchestrator** | Main loop: poll, dispatch, complete |
| **TaskPublisher** | Create messages, send to Service Bus |
| **Worker** | Execute handlers, report results |
| **EventService** | Log all state transitions to timeline |
| **Repositories** | Database CRUD operations |

### 4.5 Key Design Decisions

1. **Fresh reads** -- The orchestrator always re-fetches node states from the database before checking job completion. This prevents stale state bugs.
2. **Processed flag** -- Task results have a `processed` flag to ensure each result is handled exactly once.
3. **Auto-complete START/END** -- These nodes do not dispatch to workers; they complete immediately in the orchestrator.
4. **Event timeline** -- Every state transition emits an event, enabling full audit trail and debugging.
5. **Idempotent operations** -- All state transitions are idempotent (safe to retry).

---

## 5. Queue Architecture

### 5.1 Service Bus Queues

```
                              JOB SUBMISSION LAYER
┌─────────────────────────────────────────────────────────────────────────────┐
│  orchestrator-jobs                                                          │
│  (job distribution to orchestrators)                                        │
│                                                                             │
│  Producer: Platform Function App (B2B Gateway)                              │
│  Consumer: Orchestrator instances (competing consumers)                     │
│                                                                             │
│  Message:                        │  Behavior:                               │
│  {                               │  - Multiple orchestrators compete        │
│    "workflow_id": "...",         │  - First to receive claims ownership     │
│    "inputs": {...},              │  - Message completed after DB insert     │
│    "priority": 1,                │  - No duplicate processing               │
│    "callback_url": ".."          │                                          │
│  }                               │                                          │
└─────────────────────────────────────────────────────────────────────────────┘

                              TASK EXECUTION LAYER
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

### 5.2 Queue Hierarchy

| Layer | Queue | Producers | Consumers | Pattern |
|-------|-------|-----------|-----------|---------|
| Job Submission | `orchestrator-jobs` | Platform FA | Orchestrators | Competing consumers |
| Task Execution | `functionapp-tasks` | Orchestrators | Light workers | Load balanced |
| Task Execution | `container-tasks` | Orchestrators | Heavy workers | Load balanced |

### 5.3 Task Message Format

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

### 5.4 Queue Routing

Routing is declared in workflow YAML -- each node specifies its target queue:

```yaml
nodes:
  validate:
    handler: raster_validate
    queue: functionapp-tasks    # Quick check -> light worker
    next: process

  process:
    handler: raster_cog_docker
    queue: container-tasks      # Heavy lifting -> GDAL worker
    next: END
```

---

## 6. State Machines

### 6.1 Job State Machine

```
                    ┌─────────────┐
                    │   PENDING   │
                    └──────┬──────┘
                           │ first node dispatched
                           v
                    ┌─────────────┐
         ┌──────────│   RUNNING   │──────────┐
         │          └──────┬──────┘          │
         │                 │                 │
         │ manual cancel   │                 │ manual cancel
         v                 │                 v
┌─────────────┐           │          ┌─────────────┐
│  CANCELLED  │           │          │  CANCELLED  │
└─────────────┘           │          └─────────────┘
                          │
           ┌──────────────┴──────────────┐
           │                             │
           │ all nodes terminal,         │ any node failed
           │ none failed                 │
           v                             v
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

### 6.2 Node State Machine

```
┌─────────────┐
│   PENDING   │
└──────┬──────┘
       │ dependencies met
       v
┌─────────────┐      conditional
│    READY    │─────────────────────┐
└──────┬──────┘                     │
       │ dispatched to queue        │
       v                            v
┌─────────────┐               ┌─────────────┐
│ DISPATCHED  │               │   SKIPPED   │
└──────┬──────┘               └─────────────┘
       │ worker picked up           (terminal)
       v
┌─────────────┐
│   RUNNING   │
└──────┬──────┘
       │
       ├─────────────────────┐
       │ success             │ error
       v                     v
┌─────────────┐       ┌─────────────┐
│  COMPLETED  │       │   FAILED    │
└─────────────┘       └──────┬──────┘
    (terminal)               │
                             │ retry_count < max_retries
                             v
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

## 7. Database Schema

### 7.1 dagapp Schema

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
                    │ owner_id            VARCHAR(64)  │ <── Orchestrator ownership
                    │ owner_heartbeat_at  TIMESTAMPTZ  │ <── Heartbeat timestamp
                    │ created_at          TIMESTAMP    │
                    │ updated_at          TIMESTAMP    │
                    │ completed_at        TIMESTAMP    │
                    └───────────────┬──────────────────┘
                                    │
                                    │ 1:N
                                    │
               ┌────────────────────┼────────────────────┐
               │                    │                    │
               v                    v                    v
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

### 7.2 Schema Ownership

```
┌──────────────────────────────────────────────────────────────────────┐
│                         Azure PostgreSQL                             │
│                         rmhpostgres.postgres.database.azure.com      │
└──────────────────────────────────────────────────────────────────────┘
                                      │
         ┌────────────────────────────┼────────────────────────────┐
         │                            │                            │
         v                            v                            v
   ┌───────────────┐          ┌───────────────┐          ┌───────────────┐
   │  dagapp       │          │  app          │          │  geo          │
   │  (NEW)        │          │  (Epoch 4)    │          │  (Replicated) │
   │               │          │               │          │               │
   │ Owned by:     │          │ Owned by:     │          │ Owned by:     │
   │ rmhdagmaster  │          │ rmhgeoapi     │          │ Service Layer │
   └───────────────┘          └───────────────┘          └───────────────┘

Note: During migration, both schemas coexist.
```

### 7.3 Schema Management

Pydantic models are the single source of truth for database schema. Models define SQL metadata via `ClassVar` attributes, and the `PydanticToSQL` generator produces DDL.

```
1. Update Pydantic model     ->  core/models/*.py
2. Regenerate DDL            ->  PydanticToSQL.execute(conn)
3. Postgres is now aligned   ->  Schema matches Python
```

The `/api/v1/bootstrap/*` endpoints manage schema lifecycle:

| Endpoint | Behavior | Destructive? |
|----------|----------|--------------|
| `GET /status` | Check schema status | No |
| `GET /tables` | List tables and row counts | No |
| `GET /ddl` | Preview DDL statements | No |
| `POST /deploy` | CREATE IF NOT EXISTS (idempotent) | No |
| `POST /migrate` | ADD COLUMN IF NOT EXISTS | No |
| `POST /rebuild` | DROP CASCADE + recreate | **YES** (dev only) |

---

## 8. Workflow Definition & Patterns

### 8.1 YAML Structure

Workflows are defined in YAML and declare nodes, dependencies, handlers, and routing:

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

### 8.2 Node Types

| Type | Purpose | Dispatched to Worker? |
|------|---------|----------------------|
| `start` | Entry point. Exactly one per workflow. | No (auto-completed) |
| `end` | Exit point. At least one per workflow. | No (auto-completed) |
| `task` | Executes a handler function via a worker. | Yes |
| `conditional` | Routes based on a runtime value. | No (evaluated in orchestrator) |
| `fan_out` | Expands a source array into N dynamic child nodes. | No (expansion + dispatch in orchestrator) |
| `fan_in` | Aggregates results from all fan-out children. | No (aggregation in orchestrator, auto-completed) |

**Note**: `fan_out_target` (previously planned) was replaced by dynamic child nodes. Children are created at runtime as `NodeState` records with `parent_node_id` linking them to the fan-out node. They are not defined in the workflow YAML.

### 8.3 Raster Workflow Nodes (Real-World Example)

The primary use case is raster processing with these steps:

| Step | Description | Outcomes |
|------|-------------|----------|
| **A. Copy to Mount** | Copy blob data from Azure Blob Storage to mounted Azure File Storage | Prerequisite for most workflows |
| **B. Validate Raster** | Check CRS, bit depth, COG status, band count, projection | Reject (corrupted), Warn+Fix (auto-fixable), or Green Light |
| **C. Tiling Decision** | If input exceeds size threshold, generate tiling scheme | Determines parallel vs direct processing path |
| **D. Reproject** | Reproject to EPSG:4326 (default) or specified CRS | May combine with COG translation |
| **E. Cloud Optimize** | Create Cloud-Optimized GeoTIFF | May combine with reprojection in single pass |
| **F. STAC Registration** | Register metadata in STAC catalog | Tile collection pattern vs single-item pattern |

If tiling is required (step C), steps D and E are parallelized across tiles, and step F uses a tile collection STAC pattern.

### 8.4 Conditional Routing

A workflow can branch based on runtime data. When a branch is not taken, nodes on that branch are marked `SKIPPED`. The `SKIPPED` status counts as "successful" for dependency resolution.

```yaml
nodes:
  validate:
    handler: raster.validate
    next: [route_by_size]

  route_by_size:
    type: conditional
    condition: "{{ nodes.validate.output.file_size_mb }} > 100"
    on_true: process_heavy      # Large file -> container worker
    on_false: process_light     # Small file -> function app

  process_heavy:
    handler: raster.process
    queue: container-tasks
    next: [merge]

  process_light:
    handler: raster.process
    queue: functionapp-tasks
    next: [merge]

  merge:
    depends_on:
      any_of: [process_heavy, process_light]  # Whichever ran
    handler: finalize
```

Execution flow:
```
                    validate
                        │
                        v
                  route_by_size (evaluates condition)
                   /         \
          (true) /           \ (false)
               v               v
        process_heavy    process_light
               │               │
               │    SKIPPED    │
               v       v       v
                    merge
```

For `any_of` dependencies, at least one dependency must be COMPLETED (not just SKIPPED).

### 8.5 Fan-Out (Dynamic Parallelism)

**Status**: DONE (06 FEB 2026)

Fan-out creates a variable number of parallel task instances at runtime. The number of items is not known at workflow definition time. The `FAN_OUT` node is NOT dispatched to a worker -- the orchestrator handles expansion and dispatch internally.

#### YAML Syntax (Actual)

```yaml
nodes:
  prepare:
    type: task
    handler: echo
    params:
      item_list: "{{ inputs.item_list }}"
    next: split

  split:
    type: fan_out
    source: "{{ nodes.prepare.output.echoed_params.item_list }}"
    task:
      handler: echo
      queue: dag-worker-tasks
      params:
        item_value: "{{ item }}"
        item_index: "{{ index }}"
      timeout_seconds: 60
    next: aggregate

  aggregate:
    type: fan_in
    aggregation: collect
    next: end
```

#### Key Fields

- **`source`**: Jinja2 expression resolving to an array (from `inputs` or upstream `nodes.*.output.*`)
- **`task`**: `FanOutTaskDef` -- defines handler, queue, params, and timeout for each child
- **`task.params`**: Supports `{{ item }}` (current array element), `{{ index }}` (0-based position), and `{{ item.key }}` (dict item access)

#### How It Works

1. Orchestrator resolves `source` expression via Jinja2 to get the array
2. `FanOutHandler.expand()` creates N expansion dicts with `{{ item }}`/`{{ index }}` resolved
3. Remaining templates (`{{ inputs.x }}`) resolved via second-pass Jinja2
4. N `NodeState` records created in DISPATCHED state (parent_node_id, fan_out_index, input_params set)
5. N `TaskMessage` objects created and batch-dispatched to Service Bus
6. FAN_OUT node auto-completes with `{fan_out_count, child_node_ids}` output

```
        split (fan_out)
              │
              │ source = ["alpha", "bravo", "charlie"]
              v
    ┌─────────┼─────────┐
    v         v         v
 split__0  split__1  split__2
  (idx=0)   (idx=1)   (idx=2)
    │         │         │
    └─────────┼─────────┘
              v
       aggregate (fan_in)
```

Dynamic child nodes are named `{parent_node_id}__{index}` (e.g., `split__0`). They exist only in the database as `NodeState` records -- not in the workflow YAML.

**Important**: The `source` expression must not use dict method names as keys. See the Jinja2 Key Name Gotcha in CLAUDE.md.

### 8.6 Fan-In (Aggregation)

**Status**: DONE (06 FEB 2026)

Fan-in is the complement to fan-out. A `FAN_IN` node waits for ALL children of the upstream `FAN_OUT` node before auto-completing with aggregated results. It is NOT dispatched to a worker.

#### How It Works

1. `check_and_ready_nodes()` excludes FAN_OUT from `completed_nodes` until all children are terminal
2. This naturally blocks FAN_IN (which depends on FAN_OUT via `next` pointer) until children finish
3. When FAN_IN is finally dispatched, the orchestrator finds the upstream FAN_OUT, queries its children
4. If any child FAILED, FAN_IN fails
5. Otherwise, outputs are aggregated per the `aggregation` mode

#### Aggregation Modes

| Mode | Output | Description |
|------|--------|-------------|
| `collect` (default) | `{"results": [{...}, {...}], "count": N}` | List of all child outputs |
| `concat` | `{"results": [flattened...], "count": N}` | Flatten list values from all outputs |
| `sum` | `{"total": N, "count": N}` | Sum all numeric values |
| `first` | `{"result": {...}, "count": N}` | First child's output |
| `last` | `{"result": {...}, "count": N}` | Last child's output |

### 8.7 Complete Example: Tiled Raster Processing

This workflow combines conditional routing, fan-out, and fan-in:

```yaml
workflow_id: raster_tiled_processing
name: "Tiled Raster Processing with Size-Based Routing"
version: "1.0"

nodes:
  start:
    type: start
    next: [validate]

  validate:
    handler: raster.validate
    next: [route_by_size]

  route_by_size:
    type: conditional
    condition: "{{ nodes.validate.output.file_size_mb }} > 500"
    on_true: generate_tiles    # Large -> tile and parallelize
    on_false: process_direct   # Small -> process directly

  # Branch A: Direct processing for small files
  process_direct:
    handler: raster.create_cog
    queue: container-tasks
    next: [finalize]

  # Branch B: Tiled processing for large files
  generate_tiles:
    type: task
    handler: raster.generate_tiling_scheme
    params:
      tile_size_mb: 50
    next: process_tiles

  process_tiles:
    type: fan_out
    source: "{{ nodes.generate_tiles.output.tile_list }}"
    task:
      handler: raster.process_tile
      queue: container-tasks
      params:
        tile: "{{ item }}"
        tile_index: "{{ index }}"
      timeout_seconds: 3600
    next: merge_tiles

  merge_tiles:
    type: fan_in
    aggregation: collect
    next: finalize

  # Both branches converge
  finalize:
    depends_on:
      any_of: [process_direct, merge_tiles]
    handler: raster.stac_register
    next: [end]

  end:
    type: end
```

Execution flow for a large file (750 MB):
```
start
  │
validate ──────────────────────────────────────┐
  │                                            │
route_by_size                                  │
  │ (file_size_mb=750 > 500 -> true)           │
  v                                            │
generate_tiles                                 │ SKIPPED
  │ output: {tiles: [{...}, {...}, ...]}       │
  │                                            v
  ├──────┬──────┬──────┐              process_direct
  v      v      v      v                       │
tile_0 tile_1 tile_2 tile_3                    │
  │      │      │      │                       │
  └──────┴──────┴──────┘                       │
           │                                   │
     merge_tiles <─────────────────────────────┘
           │
      finalize
           │
          end
```

---

## 9. Handler Interface & Registry

### 9.1 Handler Interface

Every handler follows a uniform contract:

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

### 9.2 Handler Registration

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

### 9.3 Registered Handlers

**Raster Handlers** (`handlers/raster/`):

| Handler | Purpose |
|---------|---------|
| `raster.validate` | Validate file, get CRS/bounds/size metadata |
| `raster.create_cog` | Create Cloud-Optimized GeoTIFF |
| `raster.blob_to_mount` | Copy blob to local mount |
| `raster.generate_tiling_scheme` | Generate tile grid for large rasters |
| `raster.process_tile_batch` | Process batch of tiles |
| `raster.stac_ensure_collection` | Create/verify STAC collection |
| `raster.stac_register_item` | Register single STAC item |
| `raster.stac_register_items` | Batch register STAC items |

**Test Handlers** (`handlers/examples.py`):

| Handler | Purpose |
|---------|---------|
| `echo` | Return input params as output |
| `hello_world` | Minimal handler for testing |
| `sleep` | Configurable delay for testing |
| `fail` | Always fails (for error flow testing) |
| `multi_phase` | Multi-step handler with checkpoints |
| `size_check` | File size evaluation |
| `chunk_processor` | Batch processing simulation |

---

## 10. Repository Architecture

### 10.1 Repository Hierarchy

```
AsyncBaseRepository (ABC)
     │
     │   Defines interface:
     │   - create(entity) -> entity
     │   - get_by_id(id) -> Optional[entity]
     │   - update(entity) -> entity
     │   - list(**filters) -> List[entity]
     │
     v
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

### 10.2 Data Integrity Flow

```
External Input (JSON)
     │
     v
Pydantic Model (validates)
     │
     v
Service Layer (business logic)
     │
     v
Repository (DB operations)
     │
     v
Database (JSONB stored)
```

Every piece of data entering Python (from API, queue, or database) MUST go through a Pydantic model for validation. All database queries use `dict_row` factory and access results by column name, never tuple indexing.

### 10.3 Critical Patterns

**Named parameters with auto-Json wrapping**:
```python
params = self._build_named_params({
    'job_id': job_id,
    'status': status.value,
    'result_data': result_data,  # Auto-wrapped with Json()
})
await conn.execute("""
    UPDATE dagapp.dag_jobs
    SET status = %(status)s, result_data = %(result_data)s
    WHERE job_id = %(job_id)s
""", params)
```

---

## 11. Checkpoint Management

### 11.1 Why Checkpoints

Long-running Docker tasks (30+ minutes) need resumability:
- Container may be killed (spot instance, scaling, deployment)
- SIGTERM gives ~30 seconds to save state
- Resume from last checkpoint instead of restart from scratch

### 11.2 Checkpoint Flow

```
Task Start
    │
    v
┌─────────────────────┐
│ Load checkpoint     │
│ (if exists)         │
└──────────┬──────────┘
           │
    ┌──────┴──────┐
    │             │
    v             v
No checkpoint   Has checkpoint
    │             │
    v             v
Start Phase 1   Skip to saved phase
    │             │
    └──────┬──────┘
           │
           v
Execute phase -> Validate artifact -> Save checkpoint -> Next phase?
```

### 11.3 Graceful Shutdown

```python
class TaskExecutor:
    def __init__(self):
        self.shutdown_requested = False
        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _handle_sigterm(self, signum, frame):
        self.shutdown_requested = True
        # Worker has ~30 seconds to save checkpoint
```

When `shutdown_requested` becomes True, the current phase completes (or saves intermediate state), writes a checkpoint to the database, and exits. The next execution of the same task resumes from that checkpoint.

---

## 12. Error Handling & Retry

### 12.1 Failure Detection

The orchestrator's polling loop provides active failure detection, replacing the passive "wait and hope" approach:

**Epoch 4**: Task starts -> ... silence ... -> 24 hours later: "where is it?"

**Epoch 5**: Task starts -> Orchestrator polls every 5 seconds -> Timeout detected!

```python
for node in nodes_with_status(DISPATCHED):
    if now() - node.dispatched_at > node.timeout:
        mark_failed(node, "Timeout")
        if node.retry_count < node.max_retries:
            schedule_retry(node)
        else:
            fail_job(node.job_id)
```

### 12.2 Retry Policy

Retry behavior is declared per-node in workflow YAML:

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

When a node fails and retries remain, the node transitions from FAILED back to READY and is re-dispatched. The `retry_count` field on the node state tracks the current attempt.

### 12.3 Failure Modes Addressed

| Failure Mode | Detection | Recovery |
|--------------|-----------|----------|
| Worker crash mid-task | Dispatch timeout | Retry with new worker |
| Handler returns error | Task result with `success: false` | Retry or fail job |
| Queue message loss | Dispatch timeout (no result arrives) | Retry dispatch |
| Orchestrator crash | Heartbeat goes stale | Orphan recovery by another instance |
| Database connection failure | Exception in loop iteration | Loop continues on next cycle |

---

## 13. Platform Integration & B2B

### 13.1 Anti-Corruption Layer

The Platform Function App serves as an Anti-Corruption Layer (ACL) between external B2B systems and the internal DAG orchestrator. External systems use different protocols, auth methods, and data formats. The ACL normalizes these into a clean internal domain model.

```
External Systems                    Internal Systems
(DDH, Portal, etc.)                 (DAG Orchestrator)

      │   Different protocols,             │   Clean internal
      │   auth methods, formats            │   domain model
      │                                    │
      v                                    v
┌─────────────────────────────────────────────────────────────────────┐
│                      Platform Function App                          │
│                      (Anti-Corruption Layer)                        │
│                                                                     │
│   1. Receive DDH/Portal request                                     │
│   2. Validate & authenticate                                        │
│   3. Transform to internal format                                   │
│   4. Send to orchestrator-jobs queue                                │
│   5. Return request_id to caller (job_id assigned later)            │
└─────────────────────────────────────────────────────────────────────┘
```

### 13.2 B2B ID Decoupling

**External B2B identifier schemes are NEVER adopted as internal identity.**

The platform abstracts away whatever combination of unique identifiers a B2B application uses. We do not map to, inherit, or depend on external ID structures.

```
B2B Request                              Internal Platform
───────────────────────────────────      ─────────────────────────────────

DDH sends:                               We generate:
  dataset_id:  "0038272"        ─┐
  resource_id: "shapefile"      ─┼──>    asset_id = SHA256(ddh|{...})[:32]
  version_id:  "2025-01"        ─┘
                                         This is OUR identity.
                                         DDH's scheme is stored as metadata
                                         in platform_refs (JSONB), but never
                                         used as a primary key.
```

| Platform | Their Identifiers |
|----------|-------------------|
| DDH | dataset_id + resource_id + version_id |
| ArcGIS | item_id + layer_index |
| GeoNode | uuid |
| CKAN | package_id + resource_id |
| Custom | ??? |

The solution: a deterministic hash abstraction.

```python
def generate_asset_id(platform_id: str, platform_refs: dict) -> str:
    composite = f"{platform_id}|{json.dumps(platform_refs, sort_keys=True)}"
    return hashlib.sha256(composite.encode()).hexdigest()[:32]
```

| Scenario | Without Abstraction | With Our Architecture |
|----------|--------------------|-----------------------|
| DDH changes ID format | Breaking change | No impact |
| Add new B2B platform | Schema migration | Just new platform_id |
| Query across platforms | Platform-specific logic | Unified by asset_id |
| DDH uses compound keys | Complex joins | Single deterministic hash |

New B2B platforms are added without code changes -- just a new row in the platform registry.

### 13.3 Semantic Versioning vs Revision Tracking

These are two distinct concepts with different owners:

| Concept | Owner | Purpose | Scope |
|---------|-------|---------|-------|
| **Semantic Version** | B2B App | Content lineage | Their domain |
| **Revision** | Our Platform | Operational rollback | Our domain |

**Semantic versioning is explicitly the domain of the B2B application.** When DDH says "World Bank Boundaries v5", that is their semantic version indicating content changes. We store this in `platform_refs.version_id` for traceability but do NOT manage, increment, validate, or make decisions based on it.

Our `revision` field tracks operational state for the same B2B semantic version:

```
DDH version_id: "2025-01" (their v5)
│
├── revision 1: Initial processing
├── revision 2: Reprocessed after worker bug fix
├── revision 3: Reprocessed with updated COG parameters
└── revision 4: Current (after rollback from revision 3)
```

**Invariant**: For any GeospatialAsset, `platform_refs` and `asset_id` stay CONSTANT across revisions. Only revision number increments.

### 13.4 GeospatialAsset Integration

The orchestrator interacts with `GeospatialAsset` records (from `rmhgeoapi`) following strict field ownership:

| Concern | Platform (rmhgeoapi) | Orchestrator (rmhdagmaster) |
|---------|----------------------|----------------------------|
| Entity Creation | Creates asset | Never creates |
| Entity Deletion | Soft-deletes | Never deletes |
| Approval State | Updates | Never touches |
| Processing Status | Never touches | Updates |
| Content Hash | Never touches | Updates |
| Job Linkage | Never touches | Updates current_job_id |

**The boundary**: Platform manages **business state**. Orchestrator manages **execution state**.

### 13.5 Callback Architecture

When a job completes, the orchestrator can notify the original caller:

```
Job completes
     │
     v
┌────────────────────────┐
│ Check callback_url     │
│ in job metadata        │
└───────────┬────────────┘
            │
     ┌──────┴──────┐
     │             │
     v             v
 Has callback   No callback
     │             │
     v             │
POST to callback   │
{job_id, status,   │
 result_url}       │
     │             │
     └──────┬──────┘
            │
         Done
```

### 13.6 STAC Traceability

STAC items store B2B identifiers in namespaced properties for traceability:

```json
{
  "id": "a7f3b2c1...",
  "properties": {
    "platform:dataset_id": "0038272",
    "platform:resource_id": "shapefile_v5",
    "platform:version_id": "2025-01",
    "platform:request_id": "req_abc123"
  }
}
```

This enables B2B apps to trace back from STAC catalog to their original request without coupling to their ID scheme.

---

## 14. Function App Gateway

### 14.1 Role and Purpose

The Function App is a lightweight Azure Functions deployment (~50MB) serving as the B2B Gateway and ACL. It handles job submission, status queries, and HTTP proxying to the Docker-based orchestrator and workers.

**Critical principle**: B2B applications know NOTHING about internal processes. They submit requests and poll for status using `request_id` -- they never see `job_id`, node states, or orchestrator internals.

### 14.2 Endpoint Summary

**B2B Platform Endpoints** (production):

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/platform/submit` | POST | Submit job request, returns `request_id` |
| `/api/platform/status/{request_id}` | GET | Poll job status by request_id |

**Operational Endpoints** (development):

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/gateway/submit/batch` | POST | Batch job submission |
| `/api/gateway/health` | GET | Gateway health check |
| `/api/status/job/{job_id}` | GET | Get job by internal ID |
| `/api/status/jobs` | GET | List jobs with filters |
| `/api/status/job/{job_id}/nodes` | GET | Get node states for a job |
| `/api/status/job/{job_id}/events` | GET | Get event timeline for a job |
| `/api/status/stats` | GET | Job statistics |
| `/api/status/lookup` | GET | Lookup by various criteria |
| `/api/proxy/{target}/{path}` | ANY | Forward to orchestrator/worker |
| `/api/proxy/health` | GET | Aggregated health of all targets |
| `/api/admin/health` | GET | Health check with DB connectivity |
| `/api/admin/config` | GET | Current configuration |
| `/api/admin/active-jobs` | GET | List active jobs |
| `/api/admin/recent-events` | GET | Recent event timeline |
| `/api/admin/workflows` | GET | Available workflows |

**Infrastructure Probes**:

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/livez` | GET | Liveness probe (always 200) |
| `/api/readyz` | GET | Readiness probe (checks startup) |

### 14.3 Blueprint Architecture

Endpoints are organized into blueprints:

| Blueprint | File | Endpoints |
|-----------|------|-----------|
| Gateway | `function/blueprints/gateway_bp.py` | `/platform/*`, `/gateway/*` |
| Status | `function/blueprints/status_bp.py` | `/status/*` |
| Admin | `function/blueprints/admin_bp.py` | `/admin/*` |
| Proxy | `function/blueprints/proxy_bp.py` | `/proxy/*` |

The `/api/ops/*` endpoints are for development debugging only and will be removed before UAT.

---

## 15. Deployment & Infrastructure

### 15.1 Azure Resources

| Resource | Name | Purpose |
|----------|------|---------|
| **ACR** | `rmhazureacr.azurecr.io` | Container registry |
| **Orchestrator Image** | `rmhdagmaster:v0.2.x` | Lightweight orchestrator |
| **Worker Image** | `rmhdagworker:v0.2.x` | Heavy GDAL worker |
| **Web App** | `rmhdagmaster` | Orchestrator + API + UI |
| **Web App** | `rmhdagworker` | Heavy worker (GDAL tasks) |
| **PostgreSQL** | `rmhpostgres.postgres.database.azure.com` | Shared database |
| **Service Bus** | `rmhazure.servicebus.windows.net` | Task queues |
| **Managed Identity** | `rmhpgflexadmin` | Authentication (UMI) |
| **Storage (Bronze)** | `rmhazuregeo` | Raw uploads |
| **Storage (Silver)** | `rmhstorage123` | Processed data |

### 15.2 Two-Image Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          SHARED CODEBASE                                     │
├─────────────────────────────────────────────────────────────────────────────┤
│   core/           # Pydantic models, contracts, logging                      │
│   repositories/   # Database access (psycopg3, dict_row)                     │
│   services/       # Business logic                                           │
│   handlers/       # Task execution logic                                     │
│   worker/         # Service Bus consumer                                     │
│   orchestrator/   # DAG evaluation, dispatch                                 │
│   messaging/      # Service Bus publisher                                    │
└─────────────────────────────────────────────────────────────────────────────┘
                              │
              ┌───────────────┴───────────────┐
              v                               v
┌─────────────────────────────┐   ┌─────────────────────────────┐
│  Dockerfile                 │   │  Dockerfile.worker          │
│  (Orchestrator - Light)     │   │  (Worker - Heavy GDAL)      │
├─────────────────────────────┤   ├─────────────────────────────┤
│  Base: python:3.12-slim     │   │  Base: osgeo/gdal:full      │
│  Size: ~250MB               │   │  Size: ~2-3GB               │
│  No GDAL                    │   │  GDAL + rasterio            │
│  FastAPI + Loop             │   │  Consumer + Health          │
└─────────────────────────────┘   └─────────────────────────────┘
              │                               │
              v                               v
       rmhdagmaster:latest            rmhdagworker:latest
```

| Concern | Orchestrator | Worker |
|---------|--------------|--------|
| **Purpose** | Coordinate jobs, serve API/UI | Execute geo tasks |
| **Base Image** | `python:3.12-slim` | `osgeo/gdal:ubuntu-full` |
| **Size** | ~250MB | ~2-3GB |
| **GDAL/Rasterio** | No | Yes |
| **HTTP Server** | FastAPI (full) | aiohttp (health only) |
| **Scaling** | 1+ instances (competing consumers) | Auto-scale 0-N |

### 15.3 Build & Deploy

```bash
# Build orchestrator (lightweight ~250MB)
az acr build --registry rmhazureacr \
  --image rmhdagmaster:v0.2.0.0 \
  -f Dockerfile .

# Build worker (heavy ~2-3GB with GDAL)
az acr build --registry rmhazureacr \
  --image rmhdagworker:v0.2.0.0 \
  -f Dockerfile.worker .

# Deploy to Web Apps
az webapp config container set --name rmhdagmaster --resource-group rmhazure_rg \
  --container-image-name rmhazureacr.azurecr.io/rmhdagmaster:v0.2.0.0

az webapp config container set --name rmhdagworker --resource-group rmhazure_rg \
  --container-image-name rmhazureacr.azurecr.io/rmhdagworker:v0.2.0.0

# Restart to pick up new images
az webapp restart --name rmhdagmaster --resource-group rmhazure_rg
az webapp restart --name rmhdagworker --resource-group rmhazure_rg
```

### 15.4 Environment Variables

**Orchestrator (rmhdagmaster)**:

| Variable | Value | Purpose |
|----------|-------|---------|
| `RUN_MODE` | `orchestrator` | Deployment mode |
| `LOG_LEVEL` | `INFO` | Logging level |
| `LOG_FORMAT` | `json` | JSON logging for production |
| `USE_MANAGED_IDENTITY` | `true` | Entra ID authentication |
| `DB_ADMIN_MANAGED_IDENTITY_CLIENT_ID` | `a533cb80-...` | UMI client ID |
| `POSTGRES_HOST` | `rmhpostgres.postgres.database.azure.com` | Database host |
| `POSTGRES_DB` | `geopgflex` | Database name |
| `SERVICE_BUS_FQDN` | `rmhazure.servicebus.windows.net` | Service Bus namespace |
| `ORCHESTRATOR_POLL_INTERVAL` | `5` | Loop interval (seconds) |

**Worker (rmhdagworker)**:

| Variable | Value | Purpose |
|----------|-------|---------|
| `RUN_MODE` | `worker` | Deployment mode |
| `WORKER_TYPE` | `docker` | Worker type |
| `WORKER_QUEUE` | `container-tasks` | Queue to consume from |
| `DAG_CALLBACK_URL` | `https://rmhdagmaster-.../api/v1/callbacks/task-result` | Result reporting URL |

### 15.5 Connection Details

**Database**:
```
Host:     rmhpostgres.postgres.database.azure.com
Database: geopgflex
Schema:   dagapp
Auth:     Managed Identity (Entra ID)
SSL:      Required
```

**Service Bus**:
```
Namespace:  rmhazure.servicebus.windows.net
Auth:       Managed Identity
Queues:
  - dag-jobs           (job submission)
  - functionapp-tasks  (light work)
  - container-tasks    (heavy work - GDAL)
```

**Container Registry**:
```
Registry:   rmhazureacr.azurecr.io
Images:
  - rmhdagmaster:v0.2.x  (orchestrator)
  - rmhdagworker:v0.2.x  (worker)
```

### 15.6 RBAC Requirements

| Identity | Resource | Role |
|----------|----------|------|
| `rmhpgflexadmin` | Service Bus `rmhazure` | Azure Service Bus Data Sender |
| `rmhpgflexadmin` | Storage `rmhazuregeo` | Storage Blob Data Contributor |
| `rmhpgflexadmin` | Storage `rmhstorage123` | Storage Blob Data Contributor |
| `rmhpgflexadmin` | PostgreSQL `rmhpostgres` | Entra ID principal with dagapp schema privileges |

### 15.7 Deployment Topology

```
                              GitHub Push
                                   │
                                   v
                    ┌──────────────────────────────────────┐
                    │  ACR Build (Two Images)              │
                    │  az acr build -f Dockerfile          │  -> rmhdagmaster:latest
                    │  az acr build -f Dockerfile.worker   │  -> rmhdagworker:latest
                    └──────────────────────────────────────┘
                                   │
                    ┌──────────────┴──────────────┐
                    │    Azure Container Registry │
                    │    rmhazureacr.azurecr.io   │
                    │    ├── rmhdagmaster:v0.2.x  │
                    │    └── rmhdagworker:v0.2.x  │
                    └──────────────┬──────────────┘
                                   │
                    ┌──────────────┴──────────────┐
                    v                             v
     ┌──────────────────────────┐   ┌──────────────────────────┐
     │  rmhdagmaster            │   │  rmhdagworker            │
     │  (Azure Web App)         │   │  (Azure Web App)         │
     ├──────────────────────────┤   ├──────────────────────────┤
     │  RUN_MODE=orchestrator   │   │  RUN_MODE=worker         │
     │  Always-on (1+ instance) │   │  WORKER_TYPE=docker      │
     │                          │   │  WORKER_QUEUE=           │
     │  - Owns job state        │   │    container-tasks       │
     │  - Evaluates DAG         │   │                          │
     │  - Dispatches to workers │   │  - Listens to queue      │
     │  - FastAPI endpoints     │   │  - Executes handlers     │
     │                          │   │  - Reports results to DB │
     └──────────────────────────┘   └──────────────────────────┘
```

### 15.8 Migration Strategy

```
Phase 1: Parallel (Current)
─────────────────────────────────────
  - Both Epoch 4 and Epoch 5 systems operational
  - New jobs can go to either system
  - Feature flag controls routing
  - app schema + dagapp schema both active

Phase 2: DAG Primary
──────────────────────────────
  - DAG handles majority of jobs
  - Legacy only for edge cases
  - Monitor for issues

Phase 3: Legacy Deprecated
────────────────────────────────────
  - All traffic to DAG
  - Legacy read-only
  - Plan cleanup timeline

Phase 4: Legacy Removed
──────────────────────────────────────
  - Remove legacy code
  - Archive app schema
  - Single system
```

---

## Key Invariants

1. **Never use B2B identifiers as primary keys** -- External IDs are metadata in `platform_refs` JSONB.
2. **Revision is not Semantic Version** -- We own revision (operational), they own version (content).
3. **platform_refs is immutable** for a given asset_id.
4. **DAG model is independent** of B2B data models.
5. **All data through Pydantic** before touching the database.
6. **Repository pattern** for all database access -- no raw SQL outside repositories.
7. **dict_row factory** for all database queries -- no tuple indexing.
8. **Owner-scoped queries** -- Each orchestrator only touches its own jobs.
9. **Heartbeat maintains ownership** -- Stale heartbeats trigger orphan recovery.
10. **Every state transition is logged** -- JobEvents provide full audit trail.

---

## References

| Document | Purpose |
|----------|---------|
| `CLAUDE.md` | Project constitution, design principles, coding standards |
| `docs/IMPLEMENTATION.md` | Detailed implementation specs, task checklists, code patterns |
| `docs/TODO.md` | Progress tracking and next priorities |
