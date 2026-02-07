# LOOP.md - Orchestrator Execution Trace

**Last Updated**: 02 FEB 2026

This document traces a complete execution of the DAG orchestrator loop, detailing each step, state changes, and which component is responsible.

---

## Overview

The orchestrator runs a continuous loop (default: every 1 second) that:
1. Processes task results from workers
2. Finds active jobs
3. Checks dependencies and marks nodes ready
4. Dispatches ready nodes to workers
5. Checks for job completion

```
┌─────────────────────────────────────────────────────────────────┐
│                     ORCHESTRATOR LOOP                            │
│                                                                  │
│   ┌──────────────┐     ┌──────────────┐     ┌──────────────┐   │
│   │   Process    │────▶│    Find      │────▶│   Process    │   │
│   │   Results    │     │ Active Jobs  │     │   Each Job   │   │
│   └──────────────┘     └──────────────┘     └──────────────┘   │
│          ▲                                          │           │
│          │                                          ▼           │
│          │         ┌──────────────────────────────────┐        │
│          │         │  For each job:                    │        │
│          │         │  1. Check dependencies            │        │
│          │         │  2. Mark nodes ready              │        │
│          │         │  3. Dispatch ready nodes          │        │
│          │         │  4. Check job completion          │        │
│          │         └──────────────────────────────────┘        │
│          │                                          │           │
│          └──────────────── sleep(1s) ◀──────────────┘          │
└─────────────────────────────────────────────────────────────────┘
```

---

## Example: Echo Test Workflow

We'll trace execution of the `echo_test` workflow:

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

---

## Phase 1: Job Creation

### Step 1.1: API Request Received
**Actor**: User via HTTP
**Endpoint**: `POST /api/v1/jobs`

```json
{
  "workflow_id": "echo_test",
  "input_data": {"message": "hello"}
}
```

### Step 1.2: JobService.create_job()
**Actor**: `JobService`
**File**: `services/job_service.py`

| Action | State Change | Table |
|--------|--------------|-------|
| Generate job_id | - | - |
| Create Job model | `status: PENDING` | - |
| Insert job | - | `dag_jobs` |
| Create NodeState for each node | `status: PENDING` | `dag_node_states` |
| Mark START node ready | `status: PENDING → READY` | `dag_node_states` |
| Emit JOB_CREATED event | - | `dag_job_events` |

**Database State After:**
```
dag_jobs:
  job_id: abc123, status: PENDING, workflow_id: echo_test

dag_node_states:
  (abc123, start):        status: READY
  (abc123, echo_handler): status: PENDING
  (abc123, end):          status: PENDING

dag_job_events:
  event_type: job_created, job_id: abc123
```

---

## Phase 2: First Orchestration Cycle

### Step 2.1: Process Results
**Actor**: `Orchestrator._process_results()`
**File**: `orchestrator/loop.py:156`

No results to process (job just created).

### Step 2.2: Find Active Jobs
**Actor**: `JobService.list_active_jobs()`
**File**: `services/job_service.py:148`

Finds job `abc123` with status `PENDING` or `RUNNING`.

### Step 2.3: Process Job - Check Dependencies
**Actor**: `NodeService.check_and_ready_nodes()`
**File**: `services/node_service.py:76`

For each node in PENDING status:
- Get node's dependencies from workflow definition
- Check if all dependencies are COMPLETED or SKIPPED
- If yes, mark node as READY

| Node | Dependencies | All Complete? | Action |
|------|--------------|---------------|--------|
| start | (none) | N/A | Already READY |
| echo_handler | [start] | No (start=READY) | Stay PENDING |
| end | [start, echo_handler] | No | Stay PENDING |

### Step 2.4: Get Ready Nodes
**Actor**: `NodeService.get_ready_nodes()`
**File**: `services/node_service.py:69`

Returns: `[start]`

### Step 2.5: Dispatch START Node
**Actor**: `Orchestrator._dispatch_node()`
**File**: `orchestrator/loop.py:227`

START nodes are auto-completed (no actual task dispatch):

| Action | State Change | Who |
|--------|--------------|-----|
| Detect node type = "start" | - | Orchestrator |
| mark_dispatched() | `status: READY → DISPATCHED` | NodeState model |
| mark_running() | `status: DISPATCHED → RUNNING` | NodeState model |
| mark_completed({}) | `status: RUNNING → COMPLETED` | NodeState model |
| Update database | - | NodeRepository |

**Database State After:**
```
dag_node_states:
  (abc123, start):        status: COMPLETED ✓
  (abc123, echo_handler): status: PENDING
  (abc123, end):          status: PENDING
```

### Step 2.6: Check Job Completion
**Actor**: `JobService.check_job_completion()`
**File**: `services/job_service.py:161`

- Fetches fresh node states from database
- Checks if END node is COMPLETED → No
- Checks if any node is FAILED → No
- Returns `None` (job continues)

---

## Phase 3: Second Orchestration Cycle

### Step 3.1: Process Results
No results yet.

### Step 3.2: Find Active Jobs
Returns job `abc123`.

### Step 3.3: Check Dependencies Again
**Actor**: `NodeService.check_and_ready_nodes()`

| Node | Dependencies | All Complete? | Action |
|------|--------------|---------------|--------|
| echo_handler | [start] | Yes (start=COMPLETED) | **PENDING → READY** |
| end | [start, echo_handler] | No (echo_handler=READY) | Stay PENDING |

**Emits**: `NODE_READY` event for `echo_handler`

### Step 3.4: Get Ready Nodes
Returns: `[echo_handler]`

### Step 3.5: Dispatch echo_handler
**Actor**: `Orchestrator._dispatch_node()`

| Step | Action | Actor |
|------|--------|-------|
| 1 | Create TaskMessage | `TaskPublisher.create_task_message()` |
| 2 | Mark node dispatched | `NodeService.mark_dispatched()` |
| 3 | Send to Service Bus | `TaskPublisher.dispatch_task()` |
| 4 | Emit NODE_DISPATCHED event | `EventService` |
| 5 | Mark job started (first dispatch) | `JobService.mark_job_started()` |
| 6 | Emit JOB_STARTED event | `EventService` |

**State Changes:**
```
dag_jobs:
  job_id: abc123, status: PENDING → RUNNING, started_at: NOW()

dag_node_states:
  (abc123, echo_handler): status: READY → DISPATCHED,
                          task_id: abc123_echo_handler_0,
                          dispatched_at: NOW()

dag_job_events:
  + event_type: node_ready, node_id: echo_handler
  + event_type: node_dispatched, node_id: echo_handler
  + event_type: job_started, job_id: abc123
```

**Message sent to Service Bus:**
```json
{
  "task_id": "abc123_echo_handler_0",
  "job_id": "abc123",
  "node_id": "echo_handler",
  "handler": "echo",
  "params": {"message": "hello"},
  "queue": "functionapp-tasks",
  "timeout_seconds": 300,
  "retry_count": 0
}
```

---

## Phase 4: Worker Execution

### Step 4.1: Worker Receives Task
**Actor**: Worker (separate process)
**File**: `worker/main.py`

Worker pulls message from Service Bus queue.

### Step 4.2: Handler Executes
**Actor**: `echo` handler
**File**: `handlers/testing.py`

```python
async def echo(context: HandlerContext) -> HandlerResult:
    return HandlerResult(
        success=True,
        output={"echoed_params": context.params}
    )
```

### Step 4.3: Worker Reports Result
**Actor**: Worker
**Endpoint**: `POST /api/v1/callbacks/task-result`

```json
{
  "task_id": "abc123_echo_handler_0",
  "job_id": "abc123",
  "node_id": "echo_handler",
  "status": "completed",
  "output": {"echoed_params": {"message": "hello"}},
  "worker_id": "worker-xyz",
  "execution_duration_ms": 5
}
```

**Database Insert:**
```
dag_task_results:
  task_id: abc123_echo_handler_0
  status: completed
  output: {...}
  processed: false  ← Important: not yet processed by orchestrator
```

**Event Emitted:**
```
dag_job_events:
  + event_type: node_started, node_id: echo_handler (from worker)
```

---

## Phase 5: Result Processing Cycle

### Step 5.1: Process Results
**Actor**: `Orchestrator._process_results()`
**File**: `orchestrator/loop.py:156`

Queries: `SELECT * FROM dag_task_results WHERE processed = false`

Returns result for `abc123_echo_handler_0`.

### Step 5.2: Process Task Result
**Actor**: `NodeService.process_task_result()`
**File**: `services/node_service.py:148`

| Step | Action | State Change |
|------|--------|--------------|
| 1 | Find node state | - |
| 2 | Verify node is DISPATCHED or RUNNING | - |
| 3 | Update node with result | `status: DISPATCHED → COMPLETED`, `output: {...}` |
| 4 | Mark result as processed | `dag_task_results.processed: true` |
| 5 | Emit NODE_COMPLETED event | - |

**Database State:**
```
dag_node_states:
  (abc123, echo_handler): status: COMPLETED,
                          output: {"echoed_params": {"message": "hello"}},
                          completed_at: NOW()

dag_task_results:
  task_id: abc123_echo_handler_0, processed: true

dag_job_events:
  + event_type: node_completed, node_id: echo_handler
```

### Step 5.3: Check Dependencies (END node)
**Actor**: `NodeService.check_and_ready_nodes()`

| Node | Dependencies | All Complete? | Action |
|------|--------------|---------------|--------|
| end | [start, echo_handler] | Yes! | **PENDING → READY** |

**Emits**: `NODE_READY` event for `end`

### Step 5.4: Dispatch END Node
**Actor**: `Orchestrator._dispatch_node()`

END nodes are auto-completed (like START):

| Action | State Change |
|--------|--------------|
| mark_dispatched("auto-end") | `READY → DISPATCHED` |
| mark_running() | `DISPATCHED → RUNNING` |
| mark_completed({}) | `RUNNING → COMPLETED` |

### Step 5.5: Check Job Completion
**Actor**: `JobService.check_job_completion()`

- Fetches fresh node states
- Checks: Is END node COMPLETED? **Yes!**
- Returns `JobStatus.COMPLETED`

### Step 5.6: Complete Job
**Actor**: `JobService.complete_job()`
**File**: `services/job_service.py:186`

| Step | Action |
|------|--------|
| 1 | Aggregate results from all nodes |
| 2 | Update job status to COMPLETED |
| 3 | Set completed_at timestamp |
| 4 | Emit JOB_COMPLETED event |

**Final Database State:**
```
dag_jobs:
  job_id: abc123
  status: COMPLETED
  started_at: 2026-02-02T22:30:53
  completed_at: 2026-02-02T22:30:55
  result_data: {
    "echo_handler": {"echoed_params": {"message": "hello"}}
  }

dag_node_states:
  (abc123, start):        status: COMPLETED
  (abc123, echo_handler): status: COMPLETED
  (abc123, end):          status: COMPLETED

dag_job_events: (chronological)
  1. job_created     - job_id: abc123
  2. node_ready      - node_id: echo_handler
  3. node_dispatched - node_id: echo_handler
  4. job_started     - job_id: abc123
  5. node_started    - node_id: echo_handler (from worker)
  6. node_completed  - node_id: echo_handler
  7. node_ready      - node_id: end
  8. job_completed   - job_id: abc123
```

---

## State Machine Reference

### Job Status Transitions

```
           create_job()
                │
                ▼
            PENDING
                │
                │ first node dispatched
                ▼
            RUNNING
                │
        ┌───────┴───────┐
        │               │
        ▼               ▼
   COMPLETED        FAILED
        │               │
        └───────┬───────┘
                │
                ▼
           (terminal)
```

### Node Status Transitions

```
           create_nodes()
                │
                ▼
            PENDING
                │
                │ dependencies met
                ▼
             READY
                │
                │ dispatched to queue
                ▼
           DISPATCHED
                │
                │ worker picks up
                ▼
            RUNNING
                │
        ┌───────┼───────┐
        │       │       │
        ▼       ▼       ▼
   COMPLETED  FAILED  SKIPPED
```

---

## Component Responsibility Summary

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

---

## Key Design Decisions

1. **Fresh Reads**: The orchestrator always re-fetches node states from the database before checking job completion. This prevents stale state bugs.

2. **Processed Flag**: Task results have a `processed` flag to ensure each result is handled exactly once.

3. **Auto-complete START/END**: These nodes don't dispatch to workers; they complete immediately in the orchestrator.

4. **Event Timeline**: Every state transition emits an event, enabling full audit trail.

5. **Idempotent Operations**: All state transitions are idempotent (safe to retry).
