# rmhdagmaster Architecture

**Last Updated**: 12 FEB 2026
**Version**: 0.15.0

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
| **Function App** | Azure Functions runtime | ~50MB | Gateway: ACL proxy, read-only queries, forwards mutations to orchestrator |
| **DAG Orchestrator** | python:3.12-slim | ~250MB | Domain authority + DAG execution: business services, HTTP API, main loop, heartbeat, orphan recovery |
| **DAG Worker** | osgeo/gdal:ubuntu-full | ~2-3GB | Execution: GDAL processing, heavy ETL |

```
                           PLATFORM ARCHITECTURE

   B2B APPS (External)     GATEWAY (ACL Proxy)        PLATFORM (Internal)
   ───────────────────      ────────────────           ──────────────────

   ┌─────────────┐         ┌──────────────┐
   │     DDH     │──req──> │  Function App│──HTTP──> ┌──────┐ ┌──────┐ ┌──────┐
   │  (dataset/  │         │  (read-only  │          │Orch 1│ │Orch 2│ │Orch N│
   │  resource/  │         │   ACL proxy) │          │      │ │      │ │      │
   │  version)   │         └──────────────┘          └──┬───┘ └──┬───┘ └──┬───┘
   └─────────────┘                                      │        │        │
                                                        │  dispatches     │
   ┌─────────────┐                                      v        v        v
   │   ArcGIS    │──req──> (same gateway)            ┌─────────────────────────┐
   │  (item_id/  │                                   │      rmhdagworker       │
   │  layer_idx) │                                   │      (workers N+)       │
   └─────────────┘                                   └──────────┬──────────────┘
                                                                │
   ┌─────────────┐                                    callback  │ produces
   │  Future B2B │──req──> (same gateway)                       v
   │  (???)      │                                   ┌─────────────────────────┐
   └─────────────┘                                   │    GeospatialAsset      │
                                                     │    (PostgreSQL)         │
                                                     └─────────────────────────┘
```

### 1.4 Core Design Principles

1. **Orchestrator authority** -- The orchestrator owns all domain mutations and DAG execution. Workers execute tasks. The Gateway is a read-only ACL proxy -- it never writes to the database.
2. **Worker idempotency** -- Any task can be retried safely; workers handle duplicate delivery.
3. **State lives in Postgres** -- Workers are stateless; all durable state is in the database.
4. **Repository pattern** -- All database access goes through repository classes. No raw `psycopg.connect` outside the infrastructure layer. Orchestrator repositories have full CRUD authority. Gateway repositories are read-only (query repos only).
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
   (ACL Proxy, read-only)             (Service Bus)
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
2. Gateway generates `request_id` (UUID), forwards request to orchestrator HTTP API
3. Orchestrator creates asset, version, and DAG job atomically; stores `request_id` as `correlation_id`
4. Gateway returns `request_id` to B2B caller
5. B2B polls by `request_id` -- Gateway forwards to orchestrator status endpoint, filters response
6. B2B receives status/result -- never sees `job_id`, node states, or orchestrator internals

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
       │                            v
       ├───────────────┐      ┌─────────────┐
       │               │      │   SKIPPED   │
       v               │      └─────────────┘
┌─────────────┐        │           (terminal)
│ DISPATCHED  │        │
└──────┬──────┘        │ pre-dispatch error
       │ worker        │ (template failure,
       │ picked up     │  misconfiguration)
       v               v
┌─────────────┐  ┌─────────────┐
│   RUNNING   │  │   FAILED    │ (from READY)
└──────┬──────┘  └─────────────┘
       │
       ├─────────────────────┐
       │ success             │ error / timeout
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
| READY | FAILED | Pre-dispatch error (template failure, misconfiguration) |
| READY | SKIPPED | Conditional evaluation |
| DISPATCHED | RUNNING | Worker reports RUNNING |
| DISPATCHED | FAILED | Dispatch timeout |
| RUNNING | COMPLETED | Handler success |
| RUNNING | FAILED | Handler error or task timeout |
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
| `flaky_echo` | Configurable failure rate (~20% default) for retry testing |
| `multi_phase` | Multi-step handler with checkpoints |
| `size_check` | File size evaluation |
| `chunk_processor` | Batch processing simulation |

### 9.4 Blob SAS Delegation Pattern

**Status**: DONE (09 FEB 2026) — Verified E2E with 11.3 GB WorldView-2 multispectral image.

Handlers that need blob access use ephemeral user-delegation SAS tokens via managed identity. No account keys, no persistent tokens.

**How it works**:

```
Workflow YAML:                    Handler receives:
  blob_path: "bronze/data.tif"   →  _resolve_blob_path("bronze/data.tif", storage_account)
                                         │
                                         │ splits on first "/"
                                         v
                                  container="bronze", blob="data.tif"
                                         │
                                         │ BlobRepository.get_blob_sas_url()
                                         v
                                  https://rmhazuregeo.blob.core.windows.net/bronze/data.tif?sv=...&sig=...
                                         │
                                         │ 1-hour ephemeral SAS token
                                         v
                                  rasterio.open(sas_url)  # GDAL reads via HTTP range requests
```

**Supported input formats** (in `_resolve_blob_path()`):

| Format | Example | Action |
|--------|---------|--------|
| `container/blob_path` | `rmhazuregeobronze/2022dec21wv2.tif` | Generate SAS URL via managed identity |
| `https://...` | Full blob URL | Pass through unchanged |
| `/vsicurl/...` | GDAL virtual path | Pass through unchanged |
| `/local/path` | Absolute local path | Pass through unchanged |

**Key design decisions**:
- SAS tokens are generated per-request, valid for 1 hour
- Worker managed identity requires `Storage Blob Data Reader` + `Storage Blob Delegator` roles
- GDAL/rasterio reads metadata via HTTP range requests — no need to download full file (11.3 GB image metadata read in <1 second)
- SAS URLs are never logged (security) — only `container/blob_path` is logged before resolution

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

**Status**: DONE (09 FEB 2026) — Timeout detection, retry wiring, and flaky handler verified E2E in Azure.

### 12.1 Failure Detection

The orchestrator's polling loop provides active failure detection, replacing the passive "wait and hope" approach:

**Epoch 4**: Task starts -> ... silence ... -> 24 hours later: "where is it?"

**Epoch 5**: Task starts -> Orchestrator polls every 5 seconds -> Timeout detected!

### 12.2 Timeout Detection (`_check_stuck_nodes`)

Integrated into `_process_job()` — runs on every orchestration cycle for each active job. No separate background task needed.

```
For each active job:
  1. Load all node states
  2. Find DISPATCHED or RUNNING nodes
  3. For each, determine timeout:
     - Static task node: use node_def.timeout_seconds
     - Dynamic child node: use parent fan_out task.timeout_seconds
     - Fallback: DEFAULT_TASK_TIMEOUT_SEC (3600s)
  4. Compare (now - started_at or dispatched_at) against timeout
  5. If exceeded: mark_failed() → prepare_retry() if retries remain
  6. Emit NODE_FAILED event with timeout details
```

**Important**: Database timestamps are timezone-aware (PostgreSQL `TIMESTAMPTZ`). The timeout check uses `datetime.now(timezone.utc)` — never `datetime.utcnow()` — to avoid `TypeError` on offset-naive vs offset-aware datetime comparison.

### 12.3 Retry Policy

Retry behavior is declared per-node in workflow YAML:

```yaml
nodes:
  process:
    type: task
    handler: raster_cog_docker
    retry:
      max_attempts: 5        # 5 retries allowed (6 total attempts)
      backoff: fixed
      initial_delay_seconds: 1
    timeout_seconds: 30
    next: end
```

**Wiring**: `_create_node_states()` in `services/job_service.py` reads `node_def.retry.max_attempts` and sets `NodeState.max_retries` at job creation time. Default is 3 retries if no retry policy specified.

When a node fails and retries remain:
1. `process_task_result()` or `_check_stuck_nodes()` calls `mark_failed()`
2. Immediately calls `prepare_retry()` — increments `retry_count`, resets to READY
3. Next orchestration cycle dispatches the retried node
4. Task ID includes retry count: `{job_id}_{node_id}_{retry_count}`

### 12.4 Pre-Dispatch Failure

Nodes can fail before reaching a worker (READY → FAILED):
- Jinja2 template resolution error (e.g., missing required param with `StrictUndefined`)
- Invalid handler configuration
- Queue routing failure

These failures still trigger the retry mechanism. Optional template params must use `| default('', true)` to avoid `UndefinedError`.

### 12.5 Failure Modes Addressed

| Failure Mode | Detection | Recovery |
|--------------|-----------|----------|
| Worker crash mid-task | Dispatch timeout | Retry with new worker |
| Handler returns error | Task result with `success: false` | Retry or fail job |
| Queue message loss | Dispatch timeout (no result arrives) | Retry dispatch |
| Template resolution error | Exception during dispatch | READY → FAILED, retry |
| Task exceeds timeout | `_check_stuck_nodes()` per-cycle scan | Mark failed, retry |
| Orchestrator crash | Heartbeat goes stale | Orphan recovery by another instance |
| Database connection failure | Exception in loop iteration | Loop continues on next cycle |

### 12.6 E2E Retry Verification

Verified in Azure (09 FEB 2026) using `flaky_echo` handler with 50% failure rate and 5 max retries:
- `retry_test` workflow: flaky node fails → auto-retries → succeeds within retry budget
- `retry_fan_out_test` workflow: fan-out children with 30% failure rate all eventually succeed
- Timeout detection: nodes stuck past `timeout_seconds` are failed and retried automatically
- 93 unit tests across 4 test files cover retry, timeout, and fan-out retry scenarios

---

## 13. Platform Integration & B2B

### 13.1 Anti-Corruption Layer

The Platform Function App serves as a read-only Anti-Corruption Layer (ACL) proxy between external B2B systems and the internal DAG orchestrator. External systems use different protocols, auth methods, and data formats. The ACL normalizes these into a clean internal domain model and forwards mutations to the orchestrator's HTTP API. The gateway never writes to the database.

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
                                ─┘
                                         NOTE: version_id is NOT part of asset_id.
                                         Semantic versions are children of the asset,
                                         not separate assets.
```

| Platform | Their Identifiers | In asset_id hash | In AssetVersion |
|----------|-------------------|-------------------|-----------------|
| DDH | dataset_id + resource_id + version_id | dataset_id + resource_id | version_id |
| ArcGIS | item_id + layer_index + version | item_id + layer_index | version |
| GeoNode | uuid + version | uuid | version |
| Custom | ??? | identity fields | version field |

The solution: a deterministic hash abstraction that **excludes version**.

```python
def generate_asset_id(platform_id: str, platform_refs: dict) -> str:
    """
    Generate asset_id from platform identity fields (NOT version).
    Version is tracked via AssetVersion, not asset identity.
    """
    composite = f"{platform_id}|{json.dumps(platform_refs, sort_keys=True)}"
    return hashlib.sha256(composite.encode()).hexdigest()[:32]

# Example: version_id is NOT in platform_refs for asset_id generation
asset_id = generate_asset_id("ddh", {
    "dataset_id": "0038272",
    "resource_id": "shapefile"
})
# Same asset_id for v1, v2, v3... of this dataset+resource
```

| Scenario | Without Abstraction | With Our Architecture |
|----------|--------------------|-----------------------|
| DDH changes ID format | Breaking change | No impact |
| Add new B2B platform | Schema migration | Just new platform_id |
| Query across platforms | Platform-specific logic | Unified by asset_id |
| DDH uses compound keys | Complex joins | Single deterministic hash |
| New semantic version | New entity | New AssetVersion under same asset |

New B2B platforms are added without code changes -- just a new row in the platform registry.

### 13.3 Business Domain Model

**Status**: DESIGN COMPLETE (09 FEB 2026) -- not yet implemented.

The business domain sits above the execution domain (Job → Node → Task → Result). It answers: "what are we processing, who asked for it, is it approved, and where is it served?"

#### 13.3.1 Entity Overview

```
GeospatialAsset (Aggregate Root)
│
├── asset_id              hash(platform_id + dataset + resource)  ← NO version
├── platform_id           "ddh"
├── platform_refs         {"dataset_id": "birds2023", "resource_id": "viewer"}
│
├── clearance_state       uncleared | ouo | public          ← ASSET-LEVEL (all versions)
├── clearance audit       cleared_at, cleared_by, made_public_at, made_public_by
│
├── latest_approved_version_ordinal    → highest approved ordinal
│
├── soft delete           deleted_at, deleted_by
├── timestamps            created_at, updated_at
│
└──◄ AssetVersion (1:N, ordered by version_ordinal)
      │
      ├── asset_id + version_ordinal     composite PK
      ├── version_label                  B2B's name: "V1", "2025-Q1", "Version Purple"
      ├── version_ordinal                our ordering: 0, 1, 2, 3... (auto-incremented)
      │
      ├── approval_state                 pending_review | approved | rejected  ← VERSION-LEVEL
      ├── reviewer, reviewed_at, rejection_reason
      │
      ├── processing_state               queued | processing | completed | failed
      ├── current_job_id                 → dagapp.dag_jobs
      ├── content_hash                   SHA256 of source file
      │
      ├── revision                       1, 2, 3... (reprocessing counter)
      │
      ├── service outputs                table_name, blob_path, stac_item_id, stac_collection_id
      └── timestamps                     created_at, updated_at

Platform (Registry)
│
├── platform_id           "ddh", "arcgis", "geonode"
├── display_name          "Data Distribution Hub"
├── required_refs         ["dataset_id", "resource_id"]
├── optional_refs         []
└── is_active             true
```

#### 13.3.2 State Ownership

States are split between asset and version:

| State | Lives On | Rationale |
|-------|----------|-----------|
| **Clearance** (uncleared/ouo/public) | **GeospatialAsset** | Security classification applies to the dataset as a whole -- all versions share the same access level |
| **Approval** (pending_review/approved/rejected) | **AssetVersion** | Each version is independently reviewed -- v2 doesn't invalidate v1's approval |
| **Processing** (queued/processing/completed/failed) | **AssetVersion** | Each version has its own ETL pipeline |
| **Revision** (1, 2, 3...) | **AssetVersion** | Reprocessing is per-version |

Clearance gates all versions: if `clearance=ouo`, no version's service URLs are exposed externally, regardless of individual approval states.

#### 13.3.3 Semantic Version vs Revision

Two distinct concepts, two different owners:

| Concept | Owner | What It Means | Scope |
|---------|-------|---------------|-------|
| **Semantic Version** | B2B App | "Here's new data" -- content update | AssetVersion (new child record) |
| **Revision** | Our Platform | "We reprocessed the same data" -- operational retry | Revision counter within AssetVersion |

```
GeospatialAsset "abc123" (DDH birds2023/viewer)
│
├── AssetVersion ordinal=0, label="V1" (approved, revision 3)
│     ├── revision 1: Initial processing
│     ├── revision 2: Reprocessed after worker bug fix
│     └── revision 3: Current (updated COG parameters)
│
└── AssetVersion ordinal=1, label="V2" (pending_review, revision 1)
      └── revision 1: Initial processing
```

**Version labels are opaque strings.** We never parse, validate, or make decisions based on them. "V1", "2025-Q1", "Version Purple" -- all valid. The `version_ordinal` (auto-incremented integer) is what drives ordering, `version=latest` resolution, and submission enforcement.

#### 13.3.4 Version Ordering Constraint

**Versions must be approved (or deleted) in order.** B2B app teams have curation responsibility.

Valid states:

```
v0: approved   v1: approved   v2: pending     ← normal progression
v0: approved   v1: pending                    ← v1 under review
v0: pending                                   ← first submission
```

Invalid -- **rejected at `platform/submit`**:

```
v0: pending    v1: submitted                  ← REJECTED: resolve v0 first
v0: rejected   v1: submitted                  ← REJECTED: approve or delete v0
v0: approved   v1: rejected   v2: submitted   ← REJECTED: resolve v1
```

Enforcement at `platform/submit`: if the asset has any version in `pending_review` or `rejected` state, the submission is bounced. "Version N is unresolved. Approve, delete, or resubmit before adding a new version."

If a version is rejected and deleted, the next submission gets the next ordinal. The ordinal sequence may have gaps -- that's fine. `version=latest` resolves to the highest ordinal with `approval_state = approved`, not the highest ordinal overall.

#### 13.3.5 Service URL Routing

Service URLs route through the asset, versioned by ordinal:

```
/tiles/{asset_id}?version=latest   → highest approved ordinal → blob_path/table_name
/tiles/{asset_id}?version=0        → ordinal 0 → blob_path/table_name (if approved)
/tiles/{asset_id}?version=1        → ordinal 1 → blob_path/table_name (if approved)
```

URL exposure is configurable per-asset:

| Mode | Behavior |
|------|----------|
| **latest-only** (default) | Only `version=latest` is exposed. Historical versions exist in the database but have no live service URLs. |
| **all-versions** | `version=latest` plus `version=N` for each approved version. Used when B2B apps need access to historical data. |

`version=latest` updates automatically when a new version is approved. Existing versioned URLs are unaffected -- approving v2 does not change what `version=0` serves.

#### 13.3.6 Lifecycle Workflow

```
┌────────────────────────────────────────────────────────────────────────────┐
│                              ASSET LIFECYCLE                                │
├────────────────────────────────────────────────────────────────────────────┤
│                                                                            │
│  POST /platform/submit (new dataset+resource)                              │
│       │                                                                    │
│       ▼                                                                    │
│  ┌──────────────────────────────────┐                                      │
│  │ GeospatialAsset CREATED          │                                      │
│  │ clearance: uncleared             │                                      │
│  │                                  │                                      │
│  │ AssetVersion ordinal=0           │                                      │
│  │   label: "V1" (from B2B)        │                                      │
│  │   approval: pending_review       │                                      │
│  │   processing: queued             │                                      │
│  └──────────────────┬───────────────┘                                      │
│                     │                                                      │
│              DAG job runs ETL                                               │
│                     │                                                      │
│                     ▼                                                      │
│  ┌──────────────────────────────────┐                                      │
│  │ AssetVersion ordinal=0           │                                      │
│  │   processing: completed          │                                      │
│  │   approval: pending_review       │  ◄── preview URLs + approval UI      │
│  └──────────────────┬───────────────┘                                      │
│                     │                                                      │
│        ┌────────────┼────────────┐                                         │
│        ▼            ▼            ▼                                         │
│   POST /approve  POST /reject  POST /unpublish                             │
│   (clearance     (reason)      (soft delete)                               │
│    required)                                                               │
│        │            │                                                      │
│        ▼            ▼                                                      │
│   ┌──────────┐ ┌──────────┐                                               │
│   │ approved │ │ rejected │                                               │
│   │          │ │          │                                               │
│   │ version= │ │ must     │                                               │
│   │ latest   │ │ resolve  │                                               │
│   │ → v0     │ │ before   │                                               │
│   └──────────┘ │ next     │                                               │
│                │ submit   │                                               │
│                └──────────┘                                               │
│                                                                            │
│  POST /platform/submit (new version, same dataset+resource)                │
│       │  ← BLOCKED if any version is pending/rejected                      │
│       ▼                                                                    │
│  ┌──────────────────────────────────┐                                      │
│  │ AssetVersion ordinal=1 ADDED     │                                      │
│  │   label: "V2" (from B2B)        │                                      │
│  │   approval: pending_review       │                                      │
│  │   processing: queued             │                                      │
│  │                                  │                                      │
│  │ v0 still approved + serving      │  ◄── version=latest still → v0      │
│  └──────────────────────────────────┘                                      │
│                                                                            │
│  After v1 approved:                                                        │
│    version=latest → v1 (updated automatically)                             │
│    version=0 → v0 (unchanged, if all-versions mode)                        │
│                                                                            │
└────────────────────────────────────────────────────────────────────────────┘
```

### 13.4 Platform Registry

B2B platforms are registered entities with declared identifier requirements:

```python
class Platform(BaseModel):
    platform_id: str          # "ddh", "arcgis", "geonode"
    display_name: str         # "Data Distribution Hub"
    required_refs: List[str]  # ["dataset_id", "resource_id"]
    optional_refs: List[str]  # []
    is_active: bool           # true
```

On submission, `platform_refs` is validated against the platform's `required_refs`. The `platform_refs` JSONB column has a GIN index for efficient containment queries:

```sql
-- All assets for a DDH dataset (any resource)
SELECT * FROM dagapp.geospatial_assets
WHERE platform_id = 'ddh'
  AND platform_refs @> '{"dataset_id": "IDN_lulc"}'
  AND deleted_at IS NULL;
```

### 13.5 Field Ownership

The orchestrator is the sole write authority for all domain entities. The gateway is a read-only ACL proxy.

| Concern | Orchestrator (domain authority) | Gateway |
|---------|-------------------------------|---------|
| Asset creation | Creates GeospatialAsset via `asset_service` | Never writes |
| Asset deletion | Soft-deletes via `asset_service` | Never writes |
| Version creation | Creates AssetVersion on submit | Never writes |
| Approval state | Updates on approve/reject (via HTTP API) | Proxies request |
| Clearance state | Updates on clearance change (via HTTP API) | Proxies request |
| Processing state | Updates (queued → processing → completed/failed) | Never writes |
| Content hash | Updates on job completion | Never writes |
| Job linkage | Sets current_job_id atomically with job creation | Never writes |
| Service outputs | Updates table_name, blob_path, stac IDs on completion | Never writes |

**The boundary**: The orchestrator owns **all mutations** (both business state and execution state). The gateway **reads and proxies** -- it can query any entity but never writes directly to the database. This provides a security boundary: a compromised gateway cannot corrupt domain state.

### 13.6 Service Artifacts & Multi-Workflow Lifecycle

#### 13.6.1 The Problem

A single AssetVersion may go through **multiple DAG workflows at different lifecycle stages**. The initial ETL workflow runs on submission. But approval with `clearance=public` triggers a second workflow (ADF export) that produces additional artifacts. The entity model must accommodate this.

#### 13.6.2 Workflow Stages Per AssetVersion

```
AssetVersion ordinal=0, label="V1"
│
├── Stage 1: ETL Workflow (triggered by platform/submit)
│     │
│     ├── DAG Job: "raster_ingest"
│     └── Produces: blob_path, table_name, stac_item_id
│           These exist immediately after processing completes.
│           Internal preview URLs work. TiTiler/TiPG can serve internally.
│           Approval UI shows preview.
│
├── Stage 2: Approval (gateway, no DAG job needed)
│     │
│     ├── POST /platform/approve (clearance_level=ouo)
│     │     └── Done. Internal URLs ARE the production URLs.
│     │         No additional workflow needed.
│     │
│     └── POST /platform/approve (clearance_level=public)
│           └── Gateway queues Stage 3...
│
└── Stage 3: ADF Export Workflow (triggered by public clearance)
      │
      ├── DAG Job: "adf_export"
      │     input_params: { asset_id, version_ordinal, blob_path }
      └── Produces: external zone artifacts, adf_run_id
            External-facing URLs now exist.
```

Key insight: **approval does not create artifacts -- it gates access to them.** The internal artifacts exist from the moment ETL completes. Approval flips a state that the routing layer checks. Only public clearance triggers additional work (ADF export), and that's modeled as "orchestrator queues another DAG job when the approval + clearance transition occurs."

For OUO: submit → ETL → approve → done (two steps, one DAG job).
For Public: submit → ETL → approve → ADF export → done (three steps, two DAG jobs).

#### 13.6.3 Where Service Artifacts Live

The ETL workflow produces service outputs (blob_path, table_name, stac_item_id). The ADF workflow produces external zone outputs. Two options for modeling this:

**Option A: Flat fields on AssetVersion (simpler)**

```
AssetVersion
  │
  │  ── ETL outputs (written by ETL DAG job) ──
  ├── blob_path              "bronze/processed/birds.tif"
  ├── table_name             "geo.birds_2023"
  ├── stac_item_id           "birds-2023-v1"
  ├── stac_collection_id     "birds"
  │
  │  ── ADF outputs (written by ADF DAG job, NULL until public) ──
  ├── external_blob_path     NULL → "external/birds.tif"
  ├── adf_run_id             NULL → "run-xyz"
  │
  └── current_job_id         most recent active DAG job
```

Pros:
- Simple schema, no joins for service URL resolution
- `SELECT blob_path FROM asset_versions WHERE asset_id = X AND version_ordinal = Y`
- Works if artifact topology is always the same (one internal set, optionally one external set)

Cons:
- Adding new output types means schema migration (new columns)
- Doesn't scale if a version can have multiple export targets

**Option B: Child entity for service artifacts (more flexible)**

```
AssetVersion
  └──◄ ServiceArtifact (1:N)
        ├── artifact_id       UUID
        ├── artifact_type     "internal" | "external"
        ├── blob_path         where the data lives
        ├── table_name        PostGIS table (if vector)
        ├── stac_item_id      STAC entry
        ├── stac_collection_id
        ├── created_by_job    which DAG job produced this
        ├── zone              "internal" | "external"
        └── active            bool
```

Pros:
- Handles arbitrary output topology (multiple export targets, multiple formats)
- Each artifact traces back to its producing DAG job
- New artifact types are rows, not schema migrations

Cons:
- Adds a join to every service URL resolution
- More complex queries for the common case

**Current recommendation**: Start with Option A. The internal/external split is predictable -- every asset has at most one internal set and optionally one external set. If the topology grows more complex later (multiple export targets, multiple formats per version), introduce the child entity then. Premature abstraction costs more than a future migration.

#### 13.6.4 Orchestrator Domain Authority Pattern

The orchestrator owns all mutations. The gateway proxies B2B requests and reads query results.

```
Stage 1 (Submit):
  Gateway:       Generates request_id, forwards to orchestrator HTTP API
  Orchestrator:  Creates GeospatialAsset + AssetVersion + DAG job atomically
                 Runs ETL → writes blob_path, table_name, stac IDs
                 Calls asset_service.on_processing_completed() → sets processing_state=completed

Stage 2 (Approve):
  Gateway:       Proxies approve/reject request to orchestrator HTTP API
  Orchestrator:  Sets approval_state=approved via asset_service
                 If clearance=public: queues DAG job "adf_export"

Stage 3 (ADF Export, if public):
  Orchestrator:  Runs ADF workflow → writes external_blob_path, adf_run_id
                 External URLs now resolvable
```

The gateway never writes to the database -- it proxies all mutations to the orchestrator's HTTP API and filters responses for B2B consumption. The orchestrator handles both business decisions (approve, clearance) and execution (ETL, ADF export) through its service layer, keeping domain logic in one place.

All jobs trace back to the same AssetVersion via `asset_id + version_ordinal`. The `current_job_id` on AssetVersion is set atomically when the orchestrator creates the job, eliminating the broken linkage that occurred when the gateway wrote a Service Bus message_id instead of the real job_id.

### 13.7 Callback Architecture

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

### 13.8 STAC Traceability

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

The Function App is a lightweight Azure Functions deployment (~50MB) serving as a **read-only ACL proxy**. It forwards B2B mutation requests to the orchestrator's HTTP API, serves read-only queries from its own DB connection, and filters responses to hide internal details.

**Critical principles**:
- B2B applications know NOTHING about internal processes. They submit requests and poll for status using `request_id` -- they never see `job_id`, node states, or orchestrator internals.
- The Gateway **never writes to the database**. All mutations (submit, approve, reject, clearance) are forwarded to the orchestrator via HTTP proxy. Read-only queries (list assets, get status) use the gateway's own DB connection for performance.
- A compromised gateway cannot corrupt domain state -- it can only proxy requests to the orchestrator, which enforces its own validation.

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

| Blueprint | File | Endpoints | DB Access |
|-----------|------|-----------|-----------|
| Platform | `function/blueprints/platform_bp.py` | `/platform/*` (B2B submit + poll) | Proxy to orchestrator (mutations), read-only (status) |
| Asset | `function/blueprints/asset_bp.py` | `/assets/*` (queries + admin actions) | Read-only queries; mutations proxy to orchestrator |
| Gateway | `function/blueprints/gateway_bp.py` | `/gateway/*` (direct job submission) | Proxy to orchestrator |
| Status | `function/blueprints/status_bp.py` | `/status/*` | Read-only |
| Admin | `function/blueprints/admin_bp.py` | `/admin/*` | Read-only |
| Proxy | `function/blueprints/proxy_bp.py` | `/proxy/*` | HTTP forwarding |

All mutation endpoints (submit, approve, reject, clearance, delete) forward to the orchestrator via HTTP proxy. Read-only endpoints (list assets, get status, get versions) query the gateway's own DB connection for performance.

---

## 15. Execution Models

The orchestrator supports two execution models. Every operation in the system falls into one of these categories.

### 15.1 DAG Workflows

Multi-step, asynchronous, fault-tolerant execution via the orchestration loop.

```
Gateway ──> Orchestrator HTTP API ──> asset_service.submit() ──> DAG Job
                                          │
                               Orchestrator Loop ──> Service Bus ──> Worker ──> Callback
                                          │
                                          │ (retry, timeout, fan-out, event logging)
                                          │
                                      PostgreSQL (job/node/asset state)
```

**Lifecycle**: B2B submit → Gateway proxy → Orchestrator creates asset + version + job → queue → claim → create nodes → evaluate DAG → dispatch → execute → callback → evaluate → complete → `asset_service.on_processing_completed()`.

**Properties**:
- Async: caller submits and polls for result
- Multi-step: arbitrary DAG of dependent nodes
- Fault-tolerant: automatic retry, timeout detection, crash recovery
- Observable: per-node event trail, timeline API, job status
- Resource-flexible: nodes route to worker queues by handler type (light → `functionapp-tasks`, heavy → `container-tasks`)

### 15.2 Direct Commands

Synchronous, single-operation execution on the orchestrator's HTTP API.

```
Gateway ──> proxy_bp ──> Orchestrator /api/v1/commands/{name} ──> response
                                  │
                                  │ (single event logged)
                                  │
                              PostgreSQL (direct read/write)
```

**Lifecycle**: Request → proxy → execute → respond.

**Properties**:
- Synchronous: caller gets immediate response
- Single logical operation: may touch multiple tables/schemas, but is one conceptual action
- No automatic retry: caller retries on failure
- Idempotent: safe to retry (same as DAG handlers)
- Observable: single `JobEvent` audit record per execution (not per-step)
- Lightweight: no Service Bus, no worker dispatch, no node state machine

### 15.3 Selection Criteria

Use this table to decide which execution model fits an operation:

| Criterion | DAG Workflow | Direct Command |
|-----------|:---:|:---:|
| **Duration** | > 30 seconds | < 10 seconds |
| **Compute** | Heavy (GDAL, rasterio, network I/O) | Lightweight DB reads/writes |
| **Steps** | Multi-step with dependencies | Single logical operation |
| **Failure granularity** | Per-step retry needed | All-or-nothing acceptable |
| **Caller feedback** | Fire-and-forget + poll | Immediate response expected |
| **Audit trail** | Per-node event timeline | Single audit event sufficient |
| **Execution host** | Worker (needs GDAL, large memory) | Orchestrator (has connection pool, pgstac access) |
| **Examples** | Raster ingest, vector ETL, ADF export | Metadata publish, bulk status update, schema migration |

**Rule of thumb**: If it needs the worker image (GDAL/rasterio), it's a DAG workflow. If it's DB writes the admin expects an immediate answer for, it's a direct command.

### 15.4 Architecture Constraints

Both models share these guarantees:

1. **Idempotent** — Safe to call twice with the same inputs.
2. **Event-logged** — Every execution produces at least one audit record via `EventService`.
3. **Pydantic-validated** — All inputs and outputs cross boundaries via Pydantic models.
4. **Repository pattern** — All database access goes through repository classes.

Direct commands have additional constraints:

5. **No Service Bus** — Commands never publish to or consume from queues.
6. **No worker dispatch** — Commands execute entirely within the orchestrator process.
7. **Caller-managed retry** — The orchestrator does not retry failed commands automatically. The caller (gateway or admin) decides whether to retry.
8. **Timeout is HTTP** — Standard HTTP request timeout applies (not the orchestrator's node timeout detection).

### 15.5 Orchestrator API Namespace

The orchestrator's FastAPI router uses namespacing to distinguish concerns:

| Prefix | Purpose | Execution Model |
|--------|---------|-----------------|
| `/api/v1/jobs/*` | Job CRUD, status, submission | DAG (async) |
| `/api/v1/workflows/*` | Workflow definitions | Read-only |
| `/api/v1/callbacks/*` | Worker result callbacks | DAG (internal) |
| `/api/v1/commands/*` | Direct command execution | Command (sync) |
| `/api/v1/bootstrap/*` | Schema management | Command (sync) |

The `/api/v1/commands/` namespace is the entry point for all direct command operations. Each command is a POST endpoint that:
1. Validates input via Pydantic schema
2. Executes the operation (DB writes, pgstac updates, etc.)
3. Logs a single audit event
4. Returns the result synchronously

### 15.6 Gateway vs Orchestrator Responsibility Split

The orchestrator is the **sole write authority** for all domain entities. The gateway is a **read-only ACL proxy**.

- **Orchestrator owns all mutations.** Business domain writes (submit, approve, reject, clearance), execution state (processing, job linkage, service outputs), and operational commands (metadata projection, schema migrations) all run on the orchestrator. Business logic lives in the service layer (`asset_service`, `job_service`) which the orchestrator's HTTP API and DAG loop both use.
- **Gateway reads and proxies.** B2B mutation requests are forwarded to the orchestrator via HTTP. Read-only queries (list assets, get versions, poll status) use the gateway's own DB connection for performance. The gateway never writes to the database.

```
                    ┌──────────────────────────────────────────────────┐
                    │              Function App Gateway                 │
                    │              (Azure Functions — read-only proxy)  │
                    └─────────┬───────────┬───────────┬────────────────┘
                              │           │           │
           B2B Submit +       │  Read-only│           │  Direct
           Mutations          │  Queries  │           │  Commands
           (proxy)            │  (local)  │           │  (proxy)
                              │           │           │
                              v           v           v
                    ┌───────────┐ ┌────────────┐ ┌──────────────────┐
                    │platform_bp│ │ asset_bp   │ │  proxy_bp         │
                    │           │ │ status_bp  │ │  → orchestrator   │
                    │ HTTP fwd  │ │ admin_bp   │ │  /api/v1/commands/ │
                    │ to orch   │ │            │ │                    │
                    │ /api/v1/* │ │ Read-only  │ │  Sync HTTP         │
                    │ endpoints │ │ DB queries │ │  forwarding        │
                    └───────────┘ └────────────┘ └──────────────────┘
                              │                         │
                              └───────────┬─────────────┘
                                          v
                    ┌──────────────────────────────────────────────────┐
                    │              DAG Orchestrator                     │
                    │              (Domain Authority + DAG Execution)   │
                    ├──────────────────────────────────────────────────┤
                    │  HTTP API:                                        │
                    │    /api/v1/assets/*    (business domain CRUD)     │
                    │    /api/v1/submit      (asset submission)         │
                    │    /api/v1/status/*    (detailed status)          │
                    │    /api/v1/commands/*  (direct commands)          │
                    │    /api/v1/callbacks/* (worker results)           │
                    │                                                   │
                    │  DAG Loop:                                        │
                    │    Queue consumer → job creation → node dispatch  │
                    │    Result processing → job completion callbacks   │
                    │                                                   │
                    │  Service Layer:                                   │
                    │    asset_service  (business domain logic)         │
                    │    job_service    (execution lifecycle)           │
                    │    event_service  (audit trail)                   │
                    └──────────────────────────────────────────────────┘
```

**Security boundary**: A compromised gateway cannot corrupt domain state. It can only forward requests to the orchestrator's HTTP API, which enforces its own validation, state machine checks, and optimistic locking. The gateway has no DB write credentials.

### 15.7 Future Command Candidates

Operations that fit the direct command model (not yet implemented):

| Command | What It Does | Why Not DAG |
|---------|-------------|-------------|
| `metadata.publish` | Project layer_metadata → STAC + layer_catalog | 3 SQL writes, < 2s, admin wants immediate feedback |
| `metadata.unpublish` | Remove projections | 2 SQL deletes, < 1s |
| `metadata.publish_external` | Project to external B2C database | Same as publish but different connection |
| `asset.reindex` | Rebuild STAC items from existing data | Read + write, no GDAL needed |
| `schema.migrate` | Add columns to existing tables | Already exists as bootstrap endpoint |
| `cache.invalidate` | Clear workflow cache across instances | In-memory operation |

---

## 16. Deployment & Infrastructure

### 16.1 Azure Resources

| Resource | Name | Purpose |
|----------|------|---------|
| **ACR** | `rmhazureacr.azurecr.io` | Container registry |
| **Orchestrator Image** | `rmhdagmaster:v0.12.x` | Lightweight orchestrator |
| **Worker Image** | `rmhdagworker:v0.12.x` | Heavy GDAL worker |
| **Web App** | `rmhdagmaster` | Orchestrator + API + UI |
| **Web App** | `rmhdagworker` | Heavy worker (GDAL tasks) |
| **PostgreSQL** | `rmhpostgres.postgres.database.azure.com` | Shared database |
| **Service Bus** | `rmhazure.servicebus.windows.net` | Task queues |
| **Managed Identity** | `rmhpgflexadmin` | Authentication (UMI) |
| **Storage (Bronze)** | `rmhazuregeo` | Raw uploads |
| **Storage (Silver)** | `rmhstorage123` | Processed data |

### 16.2 Two-Image Architecture

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

### 16.3 Build & Deploy

```bash
# Build orchestrator (lightweight ~250MB)
# IMPORTANT: --platform linux/amd64 is required — without it, entrypoint.sh
# gets "exec format error" on App Service
az acr build --registry rmhazureacr \
  --platform linux/amd64 \
  --image rmhdagmaster:v0.12.0 \
  -f Dockerfile .

# Build worker (heavy ~2-3GB with GDAL)
az acr build --registry rmhazureacr \
  --platform linux/amd64 \
  --image rmhdagworker:v0.12.0 \
  -f Dockerfile.worker .

# Deploy to Web Apps (must set container image explicitly — restart alone
# doesn't pull new images if the tag is unchanged in config)
az webapp config container set --name rmhdagmaster --resource-group rmhazure_rg \
  --container-image-name rmhazureacr.azurecr.io/rmhdagmaster:v0.12.0

az webapp config container set --name rmhdagworker --resource-group rmhazure_rg \
  --container-image-name rmhazureacr.azurecr.io/rmhdagworker:v0.12.0

# Restart to pick up new images
az webapp restart --name rmhdagmaster --resource-group rmhazure_rg
az webapp restart --name rmhdagworker --resource-group rmhazure_rg
```

A convenience deploy script at `scripts/deploy.sh` handles ACR build, container config update, restart, and health check in one command.

### 16.4 Environment Variables

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
| `AZURE_STORAGE_ACCOUNT_NAME` | `rmhazuregeo` | Storage account for blob SAS delegation |

### 16.5 Connection Details

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
  - rmhdagmaster:v0.12.x  (orchestrator)
  - rmhdagworker:v0.12.x  (worker)
```

### 16.6 RBAC Requirements

**Orchestrator Identity (`rmhpgflexadmin` UMI)**:

| Identity | Resource | Role |
|----------|----------|------|
| `rmhpgflexadmin` | Service Bus `rmhazure` | Azure Service Bus Data Sender |
| `rmhpgflexadmin` | Storage `rmhazuregeo` | Storage Blob Data Contributor |
| `rmhpgflexadmin` | Storage `rmhstorage123` | Storage Blob Data Contributor |
| `rmhpgflexadmin` | PostgreSQL `rmhpostgres` | Entra ID principal with dagapp schema privileges |

**Worker Identity (system-assigned managed identity)**:

| Identity | Resource | Role | Purpose |
|----------|----------|------|---------|
| Worker MI | Storage `rmhazuregeo` | Storage Blob Data Reader | Read blob content via SAS delegation |
| Worker MI | Storage `rmhazuregeo` | Storage Blob Delegator | Generate user delegation SAS tokens |

**Azure RBAC Note**: Management plane roles (Contributor, Owner) do NOT include data plane access (Storage Blob Data Reader, Storage Blob Delegator). These must be explicitly assigned. See [Azure data plane vs management plane](https://learn.microsoft.com/en-us/azure/role-based-access-control/role-definitions#data-actions).

### 16.7 Deployment Topology

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
                    │    ├── rmhdagmaster:v0.12.x  │
                    │    └── rmhdagworker:v0.12.x  │
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

### 16.8 Migration Strategy

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
2. **asset_id excludes version** -- `hash(platform_id + identity refs)`. Semantic versions are children of the asset.
3. **Revision is not Semantic Version** -- We own revision (operational reprocessing), B2B owns version (content update).
4. **Version labels are opaque** -- We never parse or validate them. The auto-incremented `version_ordinal` drives all ordering.
5. **Versions must be resolved in order** -- Cannot submit version N+1 while version N is pending or rejected.
6. **Clearance is asset-level** -- All versions share one clearance state. Approval is per-version.
7. **platform_refs is immutable** for a given asset_id.
8. **DAG model is independent** of B2B data models.
9. **All data through Pydantic** before touching the database.
10. **Repository pattern** for all database access -- no raw SQL outside repositories.
11. **dict_row factory** for all database queries -- no tuple indexing.
12. **Owner-scoped queries** -- Each orchestrator only touches its own jobs.
13. **Heartbeat maintains ownership** -- Stale heartbeats trigger orphan recovery.
14. **Every state transition is logged** -- JobEvents provide full audit trail.
15. **Two execution models** -- DAG workflows for multi-step async processing, direct commands for synchronous lightweight operations. See Section 15.

---

## References

| Document | Purpose |
|----------|---------|
| `CLAUDE.md` | Project constitution, design principles, coding standards |
| `docs/IMPLEMENTATION.md` | Detailed implementation specs, task checklists, code patterns |
| `docs/TODO.md` | Progress tracking, phase ordering, next priorities |
| `docs/PHASE1_ASSET_SERVICE.md` | GeospatialAssetService implementation plan (Tier 4A) |
| `docs/LAYER_METADATA_IMPL.md` | Layer metadata implementation plan (Tier 4D-4F) |
| `docs/HEXAGONS.md` | H3 hexagonal aggregation pipeline — raster zonal stats, vector aggregation, cell connectivity graph (future, after core DAG) |
