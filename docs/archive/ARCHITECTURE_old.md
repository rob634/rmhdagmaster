# DAG Orchestrator Architecture

**Last Updated**: 03 FEB 2026
**Version**: 0.3.0.0

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
│   ┌─────────────┐                  ┌───────────────────────────────────┐    │
│   │     DDH     │  ──request───►   │        orchestrator-jobs          │    │
│   │  (dataset/  │                  │        (Service Bus Queue)        │    │
│   │  resource/  │                  └─────────────┬─────────────────────┘    │
│   │  version)   │                                │                          │
│   └─────────────┘                                │ competing consumers      │
│                                   ┌──────────────┼──────────────┐           │
│   ┌─────────────┐                 ▼              ▼              ▼           │
│   │   ArcGIS    │  ──request───►  ┌──────┐   ┌──────┐   ┌──────┐           │
│   │  (item_id/  │                 │Orch 1│   │Orch 2│   │Orch N│           │
│   │  layer_idx) │                 │      │   │      │   │      │           │
│   └─────────────┘                 └──┬───┘   └──┬───┘   └──┬───┘           │
│                                      │          │          │                │
│   ┌─────────────┐                    │    dispatches       │                │
│   │  Future B2B │                    ▼          ▼          ▼                │
│   │  (???)      │                 ┌─────────────────────────────┐           │
│   └─────────────┘                 │       rmhdagworker          │           │
│                                   │       (workers N+)          │           │
│                                   └─────────────┬───────────────┘           │
│                                                 │                           │
│                     ◄────callback────           │ produces                  │
│                                                 ▼                           │
│                                   ┌─────────────────────────────┐           │
│                                   │      GeospatialAsset        │           │
│                                   │        (rmhgeoapi)          │           │
│                                   └─────────────────────────────┘           │
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

## 2. Multi-Orchestrator Architecture

### 2.1 Design Philosophy

Azure App Service Environment (ASE) runs multiple container instances for high availability. Rather than fighting this with single-leader election, we embrace **competing consumers** where each orchestrator owns and processes its claimed jobs independently.

**Key Insight**: Jobs are independent workflow executions with no cross-job dependencies. This means orchestrators don't need to coordinate with each other—they only need to claim exclusive ownership of jobs.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    MULTI-ORCHESTRATOR JOB DISTRIBUTION                       │
└─────────────────────────────────────────────────────────────────────────────┘

   Platform Function App              orchestrator-jobs Queue
   (B2B Gateway/ACL)                  (Service Bus)
         │                                   │
         │  POST /platform/submit            │
         ▼                                   │
   ┌───────────┐                             │
   │  Validate │                             │
   │  & Enqueue├────────────────────────────►│
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
              │           ▼                  ▼                  ▼          │
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
                           ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        ORCHESTRATOR (any instance)                           │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  1. Receive message from queue                                              │
│                                                                             │
│  2. BEGIN TRANSACTION                                                       │
│     INSERT INTO dag_jobs (                                                  │
│       job_id, workflow_id, status, input_params,                            │
│       owner_id, owner_heartbeat_at                    ◄── CLAIM OWNERSHIP   │
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
    AND owner_heartbeat_at < NOW() - INTERVAL '2 minutes'   ◄── STALE
  RETURNING job_id

  (Atomic: only one orchestrator wins the UPDATE)
```

**Timing Configuration**:
| Parameter | Value | Purpose |
|-----------|-------|---------|
| Heartbeat interval | 30 seconds | Keep ownership alive |
| Orphan threshold | 2 minutes | Time before job considered abandoned |
| Orphan scan interval | 60 seconds | How often to check for orphans |

### 2.5 Benefits of This Architecture

| Concern | How It's Addressed |
|---------|-------------------|
| **Horizontal scaling** | Add more orchestrator instances; work distributes automatically |
| **Crash recovery** | Orphan reclaim picks up abandoned jobs |
| **No coordination** | Orchestrators are independent; no leader election needed |
| **No conflicts** | Each orchestrator only touches its own jobs |
| **Azure ASE friendly** | Multiple instances are an asset, not a problem |

---

## 3. Data Flow Architecture

### 3.1 Complete Job Lifecycle

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

### 3.2 Orchestrator Loop

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         ORCHESTRATOR LOOP                                    │
│                         (runs every 5 seconds)                               │
│                         (each instance has unique owner_id)                  │
└─────────────────────────────────────────────────────────────────────────────┘

 START
   │
   ▼
┌──────────────────────┐
│ 1. Poll orchestrator-│    ◄── Competing consumers pattern
│    jobs queue for    │        First orchestrator to receive
│    new job requests  │        claims ownership
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│ 2. Create Job record │    ◄── INSERT with owner_id = self.owner_id
│    with OWNERSHIP    │        This orchestrator now owns the job
│    Create NodeState  │
│    for each node     │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│ 3. Query for MY jobs │    ◄── WHERE owner_id = self.owner_id
│    with pending work │        AND status IN ('PENDING', 'RUNNING')
│    (PENDING/RUNNING) │        (Only process own jobs!)
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
│ 6. Poll task_results │    ◄── WHERE job_id IN (my owned jobs)
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
┌──────────────────────┐
│ 9. Update heartbeat  │    ◄── UPDATE owner_heartbeat_at = NOW()
│    for my jobs       │        (Every 30 seconds)
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│ 10. Reclaim orphaned │    ◄── Claim jobs with stale heartbeat
│     jobs (optional)  │        (Every 60 seconds)
└──────────┬───────────┘
           │
           ▼
   SLEEP(5 seconds)
   │
   └─────▶ START
```

**Ownership Queries**:
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

---

## 4. Queue Architecture

### 4.1 Service Bus Queues

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

**Queue Hierarchy**:
| Layer | Queue | Producers | Consumers | Pattern |
|-------|-------|-----------|-----------|---------|
| Job Submission | `orchestrator-jobs` | Platform FA | Orchestrators | Competing consumers |
| Task Execution | `functionapp-tasks` | Orchestrators | Light workers | Load balanced |
| Task Execution | `container-tasks` | Orchestrators | Heavy workers | Load balanced |

### 4.2 Task Message Format

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

### 4.3 Queue Routing Logic

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

## 5. State Machines

### 5.1 Job State Machine

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

### 5.2 Node State Machine

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

## 6. Database Schema

### 6.1 dagapp Schema

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
                    │ owner_id            VARCHAR(64)  │ ◄── Orchestrator ownership
                    │ owner_heartbeat_at  TIMESTAMPTZ  │ ◄── Heartbeat timestamp
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

### 6.2 Schema Ownership

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

## 7. GeospatialAsset Integration

### 7.1 Request vs Job Separation

**Requests and Jobs are separate entities.**

| Concept | Request (Work Order) | Job (Execution Record) |
|---------|---------------------|------------------------|
| Purpose | "Please process this" | "Here's what I did" |
| Timing | Acknowledged immediately | Created when work starts |
| Identity | `request_id` (B2B link) | `job_id` (internal tracking) |
| Lifecycle | Can exist without job | Always tied to a request |

### 7.2 Ownership Model (Mechanic Shop Analogy)

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

### 7.3 Field Ownership

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

## 8. Workflow Definition

### 8.1 YAML Structure

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

| Type | Purpose |
|------|---------|
| `start` | Entry point. Exactly one per workflow. |
| `end` | Exit point. At least one per workflow. |
| `task` | Executes a handler function. |
| `conditional` | Routes based on a value. |
| `fan_out` | Creates N parallel tasks from array. |
| `fan_in` | Waits for multiple upstream nodes. |

---

## 9. Handler Library

### 9.1 Handler Interface

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

### 9.3 Registered Handlers (Current)

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

## 10. Repository Architecture

### 10.1 Repository Hierarchy

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

### 10.2 Data Integrity Flow

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

## 11. Checkpoint Management

### 11.1 Why Checkpoints?

Long-running Docker tasks (30+ minutes) need resumability:
- Container may be killed (spot instance, scaling, deployment)
- SIGTERM gives ~30 seconds to save state
- Resume from last checkpoint instead of restart

### 11.2 Checkpoint Flow

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

---

## 12. Error Handling

### 12.1 Failure Detection

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

### 12.2 Retry Policy

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

## 13. Platform Integration

### 13.1 Anti-Corruption Layer

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
│   4. Send to orchestrator-jobs queue                                │
│   5. Return request_id to caller (job_id assigned later)            │
└─────────────────────────────────────────────────────────────────────┘
```

### 13.2 Callback Architecture

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
