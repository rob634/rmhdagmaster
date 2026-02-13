# rmhdagmaster Implementation Guide

**Last Updated**: 12 FEB 2026

> This document contains detailed implementation specs, code examples, and task checklists.
> For system architecture and design, see `ARCHITECTURE.md`.
> For project conventions and quick reference, see `../CLAUDE.md`.
> For progress tracking, see `TODO.md`.

---

## Table of Contents

1. [Function App Gateway](#1-function-app-gateway)
2. [Stability (Priority 1)](#2-stability-priority-1)
3. [Parallel Patterns](#3-parallel-patterns)
4. [Observability](#4-observability)
5. [Core Engine (Remaining Phase 1 Work)](#5-core-engine-remaining-phase-1-work)
6. [Worker Integration (Phase 2)](#6-worker-integration-phase-2)
7. [Real Workflows (Phase 3)](#7-real-workflows-phase-3)
8. [API Integration (Phase 4)](#8-api-integration-phase-4)
9. [Migration & Cutover (Phase 5)](#9-migration--cutover-phase-5)
10. [UI Implementation](#10-ui-implementation)
11. [H3 Network Synthesis](#11-h3-network-synthesis)
12. [Concurrency & Locking Reference](#12-concurrency--locking-reference)
13. [Multi-Orchestrator Implementation Reference](#13-multi-orchestrator-implementation-reference)
- [Appendix A: Completed Work Log](#appendix-a-completed-work-log)
- [Appendix B: Handler Library](#appendix-b-handler-library)
- [Appendix C: Key File Locations](#appendix-c-key-file-locations)

---

## 1. Function App Gateway (ACL Proxy)

**Status**: Phase 0.9.5 DONE (04 FEB 2026) — **Architectural revision 12 FEB 2026**: Gateway is now a read-only ACL proxy. All mutations forward to orchestrator HTTP API. See ARCHITECTURE.md Section 15.6.

The Azure Function App serves as a **read-only ACL proxy** for the DAG orchestration platform. It forwards B2B mutation requests to the orchestrator's HTTP API and serves read-only queries from its own DB connection. It is a lightweight deployment (~50MB) separate from the Docker orchestrator and worker images. **The gateway never writes to the database.**

### 1.1 Design Philosophy: Request/Job Separation

B2B applications know NOTHING about internal processes.

```
B2B App submits:                    Gateway returns:
  - workflow_id                       - request_id  (Gateway generates)
  - input_params                      - status: "accepted"
  - submitted_by (their identity)     - submitted_at

B2B App polls:                      Gateway returns:
  GET /platform/status/{request_id}   - status: pending/running/completed
                                      - result (if completed)
                                      - error (if failed)

B2B NEVER sees: job_id, node states, orchestrator details, correlation_id
```

**Flow**:
1. B2B submits job request with their identity + parameters
2. Gateway generates `request_id` (UUID)
3. Gateway forwards request to orchestrator HTTP API (`POST /api/v1/submit`) with `request_id`
4. Orchestrator creates asset + version + DAG job atomically, stores `request_id` as `correlation_id`
5. Gateway returns `request_id` to B2B caller
6. B2B polls by `request_id` — Gateway forwards to orchestrator, filters response to B2B-safe fields
7. B2B receives status/result — never sees `job_id`, node states, or orchestrator internals

### 1.2 File Structure

```
rmhdagmaster/
├── function_app.py              # Azure Functions V2 entry point
├── host.json                    # Function app configuration
├── requirements-function.txt    # Minimal function dependencies
├── .funcignore                  # Exclude heavy modules
│
├── function/                    # Function app specific code
│   ├── __init__.py
│   ├── config.py               # Function app configuration
│   ├── startup.py              # Startup validation
│   │
│   ├── blueprints/             # HTTP endpoint blueprints (read-only + proxy)
│   │   ├── __init__.py
│   │   ├── platform_bp.py      # B2B submit (proxy) + status polling (read)
│   │   ├── asset_bp.py         # Asset queries (read) + mutations (proxy)
│   │   ├── gateway_bp.py       # Direct job submission (proxy)
│   │   ├── status_bp.py        # Status query endpoints (read-only)
│   │   ├── admin_bp.py         # Admin/maintenance endpoints (read-only)
│   │   └── proxy_bp.py         # HTTP proxy to Docker apps
│   │
│   ├── models/                 # Function-specific Pydantic models
│   │   ├── __init__.py
│   │   ├── requests.py         # API request models
│   │   ├── responses.py        # API response models
│   │   └── health.py           # Health check models
│   │
│   └── repositories/           # Read-only database access (NO write methods)
│       ├── __init__.py
│       ├── base.py             # PostgreSQL base repository (read-only)
│       ├── job_query_repo.py   # Job queries (read-only)
│       ├── node_query_repo.py  # Node queries (read-only)
│       ├── event_query_repo.py # Event queries (read-only)
│       └── asset_query_repo.py # Asset/version queries (read-only)
```

### 1.3 Entry Point

**File**: `function_app.py`

The entry point creates the Function App, runs startup validation, then conditionally registers blueprints.

```python
import azure.functions as func

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# Early probes (always available)
@app.route(route="livez", methods=["GET"])
def liveness_probe(req: func.HttpRequest) -> func.HttpResponse:
    return func.HttpResponse('{"alive": true}', status_code=200,
                             headers={"Content-Type": "application/json"})

@app.route(route="readyz", methods=["GET"])
def readiness_probe(req: func.HttpRequest) -> func.HttpResponse:
    from function.startup import STARTUP_STATE
    if STARTUP_STATE.all_passed:
        return func.HttpResponse('{"ready": true}', status_code=200,
                                 headers={"Content-Type": "application/json"})
    # ... return 503 with failed check details ...

# Startup validation
from function.startup import validate_startup, STARTUP_STATE
_startup_result = validate_startup()

# Conditional blueprint registration
if STARTUP_STATE.all_passed:
    from function.blueprints.gateway_bp import gateway_bp
    app.register_functions(gateway_bp)
    from function.blueprints.status_bp import status_bp
    app.register_functions(status_bp)
    from function.blueprints.admin_bp import admin_bp
    app.register_functions(admin_bp)
    from function.blueprints.proxy_bp import proxy_bp
    app.register_functions(proxy_bp)
```

### 1.4 Startup Validation

**File**: `function/startup.py`

Validates environment variables, database connectivity, and Service Bus configuration before registering blueprints. Uses a `StartupState` dataclass with three checks:

| Check | What It Validates |
|-------|-------------------|
| `env_vars` | DATABASE_URL or POSTGIS_HOST+DATABASE; SERVICE_BUS_NAMESPACE or CONNECTION_STRING |
| `database` | Can execute `SELECT 1` against PostgreSQL |
| `service_bus` | Service Bus config is present (not connectivity - too slow) |

### 1.5 Models

#### Request Models (`function/models/requests.py`)

```python
class PlatformSubmitRequest(BaseModel):
    """B2B apps provide ONLY their identity and job parameters."""
    workflow_id: str = Field(..., max_length=64)
    input_params: Dict[str, Any] = Field(default_factory=dict)
    submitted_by: str = Field(..., max_length=64)
    callback_url: Optional[str] = Field(default=None, max_length=512)
    priority: int = Field(default=0, ge=0, le=10)
    idempotency_key: Optional[str] = Field(default=None, max_length=128)

class BatchSubmitRequest(BaseModel):
    jobs: List[JobSubmitRequest] = Field(..., min_length=1, max_length=100)

class JobQueryRequest(BaseModel):
    status: Optional[str] = None
    workflow_id: Optional[str] = None
    limit: int = Field(default=50, ge=1, le=500)
    offset: int = Field(default=0, ge=0)
    include_nodes: bool = False
```

#### Response Models (`function/models/responses.py`)

```python
class PlatformSubmitResponse(BaseModel):
    """The request_id is the ONLY identifier B2B apps need."""
    request_id: str
    workflow_id: str
    submitted_at: datetime
    status: str = "accepted"

class PlatformStatusResponse(BaseModel):
    """B2B-friendly status. Internal details are hidden."""
    request_id: str
    workflow_id: str
    status: str  # pending, running, completed, failed
    submitted_at: datetime
    completed_at: Optional[datetime] = None
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    progress: Optional[Dict[str, Any]] = None

class InternalJobStatusResponse(BaseModel):
    """Admin/debugging only - NOT exposed through /platform/* endpoints."""
    job_id: str
    workflow_id: str
    status: str
    correlation_id: Optional[str] = None
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    owner_id: Optional[str] = None
    nodes: Optional[List[Dict[str, Any]]] = None
```

### 1.6 Endpoint Summary (22 Endpoints)

| Group | Route | Method | Purpose |
|-------|-------|--------|---------|
| **Infrastructure** | `/livez` | GET | Liveness probe (always 200) |
| | `/readyz` | GET | Readiness probe (checks startup) |
| **Platform** | `/platform/submit` | POST | Submit job (returns request_id) |
| | `/platform/status/{request_id}` | GET | B2B status polling |
| **Gateway** | `/gateway/submit/batch` | POST | Batch job submission |
| | `/gateway/health` | GET | Gateway health check |
| **Status** | `/status/job/{job_id}` | GET | Internal job status |
| | `/status/jobs` | GET | List jobs with filters |
| | `/status/job/{job_id}/nodes` | GET | Nodes for a job |
| | `/status/job/{job_id}/events` | GET | Events for a job |
| | `/status/stats` | GET | Job counts by status |
| | `/status/lookup` | GET | Lookup by correlation_id or idempotency_key |
| **Admin** | `/admin/health` | GET | Comprehensive health check |
| | `/admin/config` | GET | Non-sensitive configuration |
| | `/admin/active-jobs` | GET | Currently active jobs |
| **Proxy** | `/proxy/{target}/{path}` | * | Forward to orchestrator/worker |
| | `/proxy/health` | GET | Health of all Docker apps |

### 1.7 Repository Pattern (Read-Only — NO Write Methods)

**File**: `function/repositories/base.py`

The Function App uses synchronous psycopg3 with `dict_row` factory. All repos extend `FunctionRepository`. **Gateway repositories must NEVER have write methods** — all mutations are forwarded to the orchestrator's HTTP API. This is a security boundary: even if the gateway is compromised, it cannot corrupt domain state.

```python
class FunctionRepository:
    def __init__(self, schema: str = "dagapp"):
        self.schema = schema
        self._conn_string = self._build_connection_string()

    def _get_connection(self) -> psycopg.Connection:
        conn = psycopg.connect(self._conn_string)
        conn.row_factory = dict_row
        return conn

    def execute_scalar(self, query, params=()) -> Any: ...
    def execute_one(self, query, params=()) -> Optional[Dict]: ...
    def execute_many(self, query, params=()) -> List[Dict]: ...
    def execute_count(self, query, params=()) -> int: ...
```

Domain repositories:
- `JobQueryRepository` - `get_job()`, `list_jobs()`, `count_jobs()`, `get_job_stats()`, `get_active_jobs()`, `get_job_by_correlation_id()`, `get_job_by_idempotency_key()`
- `NodeQueryRepository` - `get_node()`, `get_nodes_for_job()`, `get_nodes_by_status()`, `count_nodes_by_status()`
- `EventQueryRepository` - `get_events_for_job()`, `get_recent_events()`, `count_events_by_type()`

### 1.8 Platform Blueprint (B2B ACL Proxy)

**File**: `function/blueprints/platform_bp.py`

The platform submit endpoint (**proxy — no DB writes**):
1. Validates request via Pydantic
2. Generates `request_id` (UUID)
3. Forwards full request to orchestrator HTTP API (`POST /api/v1/submit`) with `request_id`
4. Orchestrator creates asset + version + job atomically, returns full details
5. Gateway filters response, returns 202 Accepted with `request_id` only

The platform status endpoint (**read-only**):
1. Forwards to orchestrator status endpoint, OR queries gateway's read-only DB by `correlation_id`
2. Maps internal status to B2B-friendly status
3. Returns only B2B-safe fields (no job_id, nodes, orchestrator details)

### 1.9 Proxy Blueprint

**File**: `function/blueprints/proxy_bp.py`

Forwards HTTP requests to Docker apps using `httpx`:
- Target URL from environment: `ORCHESTRATOR_URL`, `WORKER_URL`
- Forwards relevant headers (Content-Type, Authorization, X-Correlation-ID)
- 30 second timeout
- Health check endpoint pings both targets

### 1.10 RBAC Configuration

**Not Yet Configured** - Required before production:

| Role | Target | Purpose |
|------|--------|---------|
| PostgreSQL **read-only** access | `dagapp` schema | Status queries (no write access) |
| Network access to orchestrator | Orchestrator HTTP API | Forward mutations via proxy |
| Managed Identity | Function App | Authentication |

### 1.11 Remaining Tasks

- [ ] Local testing with `func start`
- [ ] Verify `.funcignore` excludes correctly
- [ ] Test all 22 endpoints
- [ ] Azure deployment test
- [ ] RBAC configuration

---

## 2. Stability (Priority 1)

### 2.1 Workflow Version Pinning

**Status**: TODO

**Problem**: Job stores `workflow_id` but not version. If workflow changes, running jobs use new definition.

**Impact**: In-flight jobs could break if workflow is updated.

#### Design

Add `workflow_version` column to `dag_jobs`:

```sql
ALTER TABLE dagapp.dag_jobs
ADD COLUMN workflow_version INTEGER DEFAULT 1;
```

Update Job model:

```python
# core/models/job.py
class Job(BaseModel):
    # ...existing...
    workflow_version: int = 1
```

Store version on job creation:

```python
# services/job_service.py
async def create_job(self, workflow_id: str, input_params: Dict, ...) -> Job:
    workflow = self._workflow_service.get(workflow_id)
    job = Job(
        job_id=uuid.uuid4().hex,
        workflow_id=workflow_id,
        workflow_version=workflow.version,  # NEW
        # ...
    )
```

Load correct version when processing:

```python
# orchestrator/loop.py
async def _process_job(self, job: Job) -> None:
    workflow = self._workflow_service.get(
        job.workflow_id,
        version=job.workflow_version
    )
```

#### Task Checklist

| Task | Status | File |
|------|--------|------|
| Add workflow_version to Job model | [ ] | `core/models/job.py` |
| Include in Job repo queries | [ ] | `repositories/job_repo.py` |
| Store version on job creation | [ ] | `services/job_service.py` |
| Load correct version when processing | [ ] | `orchestrator/loop.py` |
| Update workflow_service.get() for version | [ ] | `services/workflow_service.py` |

### 2.2 Transaction Wrapping

**Status**: TODO

**Problem**: Multi-step updates done in separate queries. Crash between steps causes inconsistent state.

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

```python
# orchestrator/loop.py
async def _dispatch_node(self, job, node, workflow) -> bool:
    async with self._pool.connection() as conn:
        async with conn.transaction():
            await self._node_repo.mark_dispatched(
                node.job_id, node.node_id, task_id, conn=conn
            )
            if job.status == JobStatus.PENDING:
                await self._job_repo.update_status(
                    job.job_id, JobStatus.RUNNING, conn=conn
                )
    # Send to queue AFTER transaction commits
    await self._publisher.publish(message, queue_name)
```

Update repositories to accept optional connection parameter:

```python
# repositories/node_repo.py
async def mark_dispatched(
    self, job_id, node_id, task_id,
    conn: Optional[AsyncConnection] = None,
) -> bool:
    async def _execute(c: AsyncConnection):
        # ... existing logic using c instead of self._pool

    if conn:
        return await _execute(conn)
    else:
        async with self._pool.connection() as c:
            return await _execute(c)
```

#### Task Checklist

| Task | Status | File |
|------|--------|------|
| Add optional conn param to job_repo | [ ] | `repositories/job_repo.py` |
| Add optional conn param to node_repo | [ ] | `repositories/node_repo.py` |
| Add optional conn param to task_repo | [ ] | `repositories/task_repo.py` |
| Wrap dispatch in transaction | [ ] | `orchestrator/loop.py` |
| Wrap result processing in transaction | [ ] | `services/node_service.py` |

---

## 3. Parallel Patterns

### 3.1 Fan-Out Implementation

**Status**: DONE (06 FEB 2026)

**Use Case**: Tiled raster processing, chunked vector uploads, parallel analysis.

#### Workflow YAML Syntax

```yaml
nodes:
  split:
    type: fan_out
    source: "{{ nodes.prepare.output.echoed_params.item_list }}"
    task:
      handler: echo
      queue: dag-worker-tasks
      params:
        item_value: "{{ item }}"       # Current array element
        item_index: "{{ index }}"      # 0-based position
      timeout_seconds: 60
    next: aggregate
```

#### Key Components

**`FanOutTaskDef`** (`core/models/workflow.py`): Pydantic model for the `task` field on FAN_OUT nodes.

```python
class FanOutTaskDef(BaseModel):
    handler: str = Field(..., max_length=64)
    queue: str = Field(default="")  # Must be set explicitly in workflow YAML
    params: Dict[str, Any] = Field(default_factory=dict)
    timeout_seconds: int = Field(default=3600, ge=1, le=86400)
```

**`FanOutHandler`** (`orchestrator/engine/evaluator.py`): Resolves source, expands `{{ item }}`/`{{ index }}` placeholders.

**`_handle_fan_out_node()`** (`orchestrator/loop.py`): Orchestrator method that:
1. Resolves source array via Jinja2
2. Calls `FanOutHandler.expand()` for N expansion dicts
3. Second-pass resolves remaining templates (`{{ inputs.x }}`)
4. Creates N `NodeState` records (DISPATCHED, parent_node_id set, input_params stored)
5. Creates N `TaskMessage` objects, batch dispatches to Service Bus
6. Auto-completes FAN_OUT node

**Dynamic child naming**: `{parent_node_id}__{index}` (e.g., `split__0`, `split__1`)

#### Template Placeholders in Fan-Out Params

| Placeholder | Value | Example |
|-------------|-------|---------|
| `{{ item }}` | Current array element (string or scalar) | `"alpha"` |
| `{{ index }}` | 0-based position in source array | `0` |
| `{{ item.key }}` | Dict item field access | `"file1.tif"` |
| `{{ inputs.x }}` | Job input parameter (second-pass) | `"some_value"` |

#### Jinja2 Key Name Gotcha

Jinja2 resolves `{{ dict.items }}` as the built-in `dict.items()` method, NOT as `dict["items"]`. This means input/output keys that shadow Python dict method names (`items`, `keys`, `values`, `get`, `update`, `pop`) will not resolve correctly in templates.

**Workaround**: Use non-conflicting key names (e.g., `item_list` instead of `items`).

#### Task Checklist

| Task | Status | File |
|------|--------|------|
| Add parent_node_id, fan_out_index to NodeState | [x] | `core/models/node.py` |
| Add input_params to NodeState | [x] | `core/models/node.py` |
| Create FanOutTaskDef model | [x] | `core/models/workflow.py` |
| Fix FanOutHandler._resolve_source to use Jinja2 | [x] | `orchestrator/engine/evaluator.py` |
| Wire fan-out expansion into orchestrator loop | [x] | `orchestrator/loop.py` |
| Add get_children_by_parent() to NodeRepository | [x] | `repositories/node_repo.py` |
| Add all_children_terminal() to NodeRepository | [x] | `repositories/node_repo.py` |
| Persist fan-out fields in create/create_many | [x] | `repositories/node_repo.py` |
| Add FAN_OUT validation to validate_structure() | [x] | `core/models/workflow.py` |
| Dynamic child retry dispatch | [x] | `orchestrator/loop.py` |

### 3.2 Fan-In Implementation

**Status**: DONE (06 FEB 2026)

**Use Case**: Merge tiled results, aggregate parallel outputs.

```yaml
  aggregate:
    type: fan_in
    aggregation: collect    # collect | concat | sum | first | last
    next: end
```

#### How Fan-In Blocking Works

Fan-in does NOT use explicit `depends_on`. Instead, the FAN_OUT node's `next` pointer targets the FAN_IN node, creating an implicit dependency. The blocking mechanism:

1. `check_and_ready_nodes()` in `NodeService` builds a `completed_nodes` set
2. FAN_OUT nodes are excluded from `completed_nodes` until `all_children_terminal()` returns True
3. FAN_IN naturally stays PENDING (because its dependency -- the FAN_OUT node -- is not in `completed_nodes`)
4. When all children finish, FAN_OUT enters `completed_nodes`, FAN_IN becomes READY

#### Aggregation Modes

| Mode | Output Structure | Description |
|------|-----------------|-------------|
| `collect` (default) | `{"results": [{...}, ...], "count": N}` | List of all child outputs |
| `concat` | `{"results": [flattened...], "count": N}` | Flatten list values from all outputs |
| `sum` | `{"total": N, "count": N}` | Sum all numeric values across outputs |
| `first` | `{"result": {...}, "count": N}` | First child's output only |
| `last` | `{"result": {...}, "count": N}` | Last child's output only |

#### Error Handling

If any fan-out child has FAILED status, the FAN_IN node also fails with an error listing the failed child node IDs.

#### Task Checklist

| Task | Status | File |
|------|--------|------|
| Fan-in aggregation logic | [x] | `orchestrator/loop.py` `_aggregate_fan_in_results()` |
| Fan-in dispatch handler | [x] | `orchestrator/loop.py` `_handle_fan_in_node()` |
| Block FAN_IN until children terminal | [x] | `services/node_service.py` `check_and_ready_nodes()` |
| Dynamic node _dependencies_met() | [x] | `services/node_service.py` |
| Unit tests (30 tests) | [x] | `tests/test_fan_out.py` |
| Test workflow | [x] | `workflows/fan_out_test.yaml` |

---

## 4. Observability

### 4.1 Progress API

**Status**: TODO

**Goal**: Expose real-time progress for running jobs.

#### ProgressService

**File**: `services/progress_service.py` (new)

```python
class ProgressService:
    def __init__(self, node_repo, task_repo):
        self._node_repo = node_repo
        self._task_repo = task_repo

    async def get_job_progress(self, job_id: str) -> Dict[str, Any]:
        """Returns overall_percent, node status counts, current_tasks."""
        node_states = await self._node_repo.get_all_for_job(job_id)

        status_counts = {status: 0 for status in NodeStatus}
        for node in node_states:
            status_counts[node.status] += 1

        total = len(node_states)
        completed = status_counts[NodeStatus.COMPLETED] + status_counts[NodeStatus.SKIPPED]
        overall_percent = (completed / total * 100) if total > 0 else 0

        # Get current task progress for running nodes
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
```

#### API Endpoints

```python
# api/routes.py
@router.get("/jobs/{job_id}/progress")
async def get_job_progress(job_id: str) -> Dict[str, Any]: ...

@router.get("/jobs/{job_id}/nodes/{node_id}/progress")
async def get_node_progress(job_id: str, node_id: str) -> Dict[str, Any]: ...
```

#### Task Checklist

| Task | Status | File |
|------|--------|------|
| Create ProgressService | [ ] | `services/progress_service.py` |
| Add GET /jobs/{id}/progress | [ ] | `api/routes.py` |
| Add GET /jobs/{id}/nodes/{id}/progress | [ ] | `api/routes.py` |
| Add get_latest_for_task() to TaskRepository | [ ] | `repositories/task_repo.py` |

### 4.2 Metrics & Monitoring

**Status**: TODO

**Goal**: Aggregate statistics for monitoring and optimization.

#### MetricsService

**File**: `services/metrics_service.py` (new)

Key methods:

```python
class MetricsService:
    async def get_workflow_metrics(self, workflow_id=None, days=7):
        """Per-workflow: total_jobs, completed, failed, success_rate,
        avg_duration, p50_duration, p95_duration, common_failures."""

    async def get_node_metrics(self, workflow_id, days=7):
        """Per-node: total_executions, completed, failed, success_rate,
        avg_duration, total_retries."""

    async def get_failure_analysis(self, workflow_id=None, days=7, limit=20):
        """Common error messages grouped by workflow/node with occurrence counts."""

    async def get_throughput_metrics(self, hours=24, bucket_minutes=60):
        """Time-series: jobs_completed per hour, avg_duration per bucket."""
```

SQL for workflow metrics:

```sql
SELECT
    workflow_id,
    COUNT(*) as total_jobs,
    COUNT(*) FILTER (WHERE status = 'completed') as completed,
    COUNT(*) FILTER (WHERE status = 'failed') as failed,
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
GROUP BY workflow_id ORDER BY total_jobs DESC
```

#### API Endpoints

```
GET /metrics/workflows           Per-workflow statistics
GET /metrics/workflows/{id}/nodes  Per-node statistics
GET /metrics/failures            Failure pattern analysis
GET /metrics/throughput          Time-series throughput
```

#### Task Checklist

| Task | Status | File |
|------|--------|------|
| Create MetricsService | [ ] | `services/metrics_service.py` |
| Add GET /metrics/workflows | [ ] | `api/routes.py` |
| Add GET /metrics/failures | [ ] | `api/routes.py` |
| Add GET /metrics/throughput | [ ] | `api/routes.py` |
| Initialize MetricsService in main.py | [ ] | `main.py` |

### 4.3 Distributed Tracing (OpenTelemetry)

**Status**: TODO (Future/Enterprise)

Integration with Azure Application Insights via OpenTelemetry:
- Trace propagation from job creation through completion
- Correlation of orchestrator decisions with worker execution
- Performance profiling and bottleneck identification

Dependencies:
```
opentelemetry-api>=1.20.0
opentelemetry-sdk>=1.20.0
opentelemetry-exporter-otlp>=1.20.0
opentelemetry-instrumentation-fastapi>=0.41b0
opentelemetry-instrumentation-psycopg>=0.41b0
azure-monitor-opentelemetry-exporter>=1.0.0b15
```

Trace context propagation via `trace_parent` field in TaskMessage:

```python
# orchestrator/loop.py
context = trace.get_current_span().get_span_context()
trace_parent = f"00-{context.trace_id:032x}-{context.span_id:016x}-01"
message = TaskMessage(..., trace_parent=trace_parent)

# worker/executor.py
carrier = {"traceparent": message.trace_parent}
context = self._propagator.extract(carrier)
with self._tracer.start_as_current_span("execute_handler", context=context): ...
```

---

## 5. Core Engine (Remaining Phase 1 Work)

### 5.1 GeospatialAsset Integration (V0.8)

**Status**: TODO

The V0.8 design separates Requests from Jobs via GeospatialAsset:

```
dag_api_requests (N) --> dag_geospatial_assets (1) <-- (N) dag_jobs
                                │
                                └── current_job_id --> dag_jobs
```

| Task | Status | File |
|------|--------|------|
| Create GeospatialAsset model | [ ] | `core/models/geospatial_asset.py` |
| Create ApiRequest model | [ ] | `core/models/api_request.py` |
| Create AssetRevision model | [ ] | `core/models/asset_revision.py` |
| Integration with dag_jobs | [ ] | Link via asset_id + current_job_id |

### 5.2 Additional Tables (Tier 3-7)

| Task | Status | Description |
|------|--------|-------------|
| dag_approvals model | [ ] | Approval workflow |
| dag_blob_assets model | [ ] | File tracking |
| dag_external_services model | [ ] | Callback endpoints (DDH, ADF, webhooks) |
| dag_dataset_refs model | [ ] | Data lineage tracking |
| dag_promoted model | [ ] | Externally-published datasets |

Files to create:
- `core/models/approval.py`
- `core/models/blob_asset.py`
- `core/models/external_service.py`
- `core/models/dataset_ref.py`
- `core/models/promoted.py`

### 5.3 Data Integrity Gaps

**Problem**: API endpoints accepting raw dicts without Pydantic validation.

| Gap | Endpoint | Fix |
|-----|----------|-----|
| Raw dict parameter | `/api/callbacks/task-progress` | Create `TaskProgressCreate` schema |
| Raw dict parameter | `/api/test/handler` | Create `HandlerTestRequest` schema |
| Weak return types | `services/job_service.py` | Create explicit return models |

### 5.4 Configuration Defaults

| Task | Status | File |
|------|--------|------|
| TaskRoutingDefaults | [ ] | `core/config/defaults.py` |
| RasterDefaults | [ ] | `core/config/defaults.py` |
| VectorDefaults | [ ] | `core/config/defaults.py` |
| TimeoutDefaults | [ ] | `core/config/defaults.py` |

---

## 6. Worker Integration (Phase 2)

**Goal**: Existing rmhgeoapi workers can execute DAG tasks.

### 6.1 Task Message Format

Workers receive tasks via Service Bus with this JSON schema:

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
  "retry_count": 0,
  "checkpoint": null
}
```

### 6.2 Worker Result Reporting

Workers write results to `dagapp.dag_task_results`:

```python
INSERT INTO dagapp.dag_task_results (
    task_id, job_id, node_id, status, output, worker_id, reported_at
) VALUES ($1, $2, $3, $4, $5, $6, NOW())
```

### 6.3 Checkpointable Handler Interface

```python
class CheckpointableHandler(ABC):
    @abstractmethod
    async def execute(
        self,
        params: Dict[str, Any],
        checkpoint: Optional[Dict[str, Any]] = None,
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Yields:
        - {"type": "checkpoint", "phase": "...", "data": {...}}
        - {"type": "progress", "current": N, "total": M, "message": "..."}
        - {"type": "result", "output": {...}}  # Final yield
        """
```

Worker executor handles both simple and checkpointable handlers:

```python
async def execute_task(self, message: TaskMessage) -> TaskResult:
    handler = self._registry.get(message.handler)
    if isinstance(handler, CheckpointableHandler):
        return await self._execute_checkpointable(handler, message)
    else:
        return await self._execute_simple(handler, message)
```

### 6.4 Task Checklist

| Task | Status | File |
|------|--------|------|
| Define task format | [x] | `core/models/task.py` TaskMessage model |
| Worker detection | [x] | Workers process DAG TaskMessage format |
| Result reporting | [x] | Workers POST to `/api/v1/callbacks/task-result` (UPSERT) |
| Dual-mode workers | [ ] | Support both legacy and DAG |
| Function App changes | [ ] | Modify rmhazuregeoapi for DAG support |
| Docker Worker changes | [x] | rmhdagworker deployed and processing tasks |
| End-to-end test | [x] | Echo + fan-out verified in Azure (07 FEB) |

---

## 7. Real Workflows (Phase 3)

**Goal**: Production workflows running on DAG.

### 7.1 Workflows to Port

| Legacy Job Class | New YAML | Complexity |
|------------------|----------|------------|
| `ProcessRasterDocker` | `raster_processing.yaml` | Conditional routing |
| `ProcessVectorDocker` | `vector_etl.yaml` | Linear with fan-out |
| `FathomFloodProcess` | `fathom_flood.yaml` | Complex multi-stage |

### 7.2 Example Workflow (User-Defined Pipeline)

```yaml
workflow_id: my_custom_pipeline
nodes:
  validate:
    handler: raster_validate
    next: reproject
  reproject:
    handler: raster_reproject
    params:
      target_crs: "EPSG:3857"
    next: clip
  clip:
    handler: raster_clip
    params:
      boundary: "{{ inputs.aoi }}"
    next: analyze
  analyze:
    handler: stats_zonal
    params:
      zones: "{{ inputs.zones }}"
    next: END
```

### 7.3 Task Checklist

| Task | Status | File |
|------|--------|------|
| Raster workflow | [ ] | Port process_raster_docker to YAML |
| Conditional routing | [x] | Size-based routing (verified in tests) |
| Vector workflow | [ ] | Port vector_docker_etl to YAML |
| Fan-out support | [x] | 3-item fan-out verified E2E in Azure (07 FEB) |
| FATHOM workflow | [ ] | Complex multi-stage flood data |
| Template resolution | [x] | Jinja2 engine wired into dispatch (`{{ inputs.x }}`, `{{ nodes.y.output.z }}`) |
| Error handling | [ ] | Failed tasks retry correctly |
| Timeout detection | [ ] | Stuck tasks detected within 60 seconds |

---

## 8. API Integration (Phase 4)

**Goal**: External API uses DAG orchestrator.

### 8.1 Endpoints

```
POST /api/dag/submit         Submit workflow job
GET  /api/dag/jobs/{id}      Job status
GET  /api/dag/jobs/{id}/nodes  Node states
POST /api/dag/jobs/{id}/cancel Cancel job
GET  /health                 Health endpoint
```

### 8.2 Task Checklist

| Task | Status | File |
|------|--------|------|
| FastAPI setup | [ ] | Create `orchestrator/api/` module |
| Submit endpoint | [ ] | `POST /api/dag/submit` |
| Status endpoint | [ ] | `GET /api/dag/jobs/{id}` |
| Node status | [ ] | `GET /api/dag/jobs/{id}/nodes` |
| Cancel endpoint | [ ] | `POST /api/dag/jobs/{id}/cancel` |
| Health endpoint | [ ] | `GET /health` |
| Platform integration | [ ] | DDH -> DAG routing |
| Backward compat | [ ] | Legacy endpoints still work |

---

## 9. Migration & Cutover (Phase 5)

**Goal**: DAG is primary system, legacy deprecated.

### 9.1 Task Checklist

| Task | Status | Description |
|------|--------|-------------|
| Traffic migration | [ ] | Route new jobs to DAG |
| Legacy deprecation | [ ] | Mark CoreMachine deprecated |
| Monitoring | [ ] | Alerts for orchestrator health |
| Documentation | [ ] | Update all docs |
| Team training | [ ] | Knowledge transfer |
| Cleanup (optional) | [ ] | Remove legacy code |

### 9.2 Success Criteria

**MVP (Phase 3 Complete)**:
- [ ] Orchestrator runs in Azure Web App
- [ ] Can submit raster workflow via API
- [ ] Conditional routing works (size-based)
- [ ] Fan-out works (tiled rasters)
- [ ] Failures detected within 60 seconds
- [ ] No "last task turns out lights" pattern
- [ ] Existing workers process DAG tasks

**Production Ready (Phase 5 Complete)**:
- [ ] All workflows migrated to DAG
- [ ] DDH integration works
- [ ] Monitoring and alerting configured
- [ ] Documentation complete
- [ ] Legacy CoreMachine deprecated
- [ ] Team trained on new system

---

## 10. UI Implementation

**Status**: Active Development (02 FEB 2026)

The DAG Orchestrator has a solid UI foundation with templates, static assets, an abstraction layer, and working routes.

### 10.1 Current State

#### Templates (12 files, all complete)

| Template | Description |
|----------|-------------|
| `base.html` | App shell with sidebar, header, content area |
| `dashboard.html` | Stats grid, recent jobs, workflows list |
| `health.html` | Orchestrator, DB, messaging, worker, system health |
| `jobs.html` | Job list with filters, pagination |
| `job_detail.html` | Job header, node execution, task history, timeline link |
| `workflows.html` | Workflow listing with node counts |
| `workflow_detail.html` | Workflow nodes, DAG flow visualization |
| `nodes.html` | Cross-job node monitoring with filters |
| `timeline.html` | Job event timeline with filters |
| `partials/sidebar.html` | Navigation with sections |
| `partials/header.html` | Page title, mode badge, refresh |
| `partials/status_badge.html` | Reusable status badge macro |
| `partials/timeline.html` | Reusable timeline component |

#### Static Assets

| Asset | Status |
|-------|--------|
| `css/style.css` | Full dark theme (~700 lines) |
| `js/app.js` | Feather icons, auto-refresh, toast, cancelJob |

#### UI Abstraction Layer

| Component | File | Purpose |
|-----------|------|---------|
| DTOs | `ui/dto.py` | JobDTO, NodeDTO, TaskDTO, AssetDTO, JobEventDTO |
| Adapters | `ui/adapters/` | Model -> DTO conversion |
| Navigation | `ui/navigation.py` | 16 nav items with mode filtering |
| Terminology | `ui/terminology.py` | Stage<->Node, Job Type<->Workflow mapping |
| Features | `ui/features.py` | 25+ feature flags with mode support |

#### Working Routes

| Route | Template |
|-------|----------|
| `/ui/` | dashboard.html |
| `/ui/health` | health.html |
| `/ui/jobs` | jobs.html |
| `/ui/jobs/{id}` | job_detail.html |
| `/ui/jobs/{id}/timeline` | timeline.html |
| `/ui/workflows` | workflows.html |
| `/ui/workflows/{id}` | workflow_detail.html |
| `/ui/nodes` | nodes.html |

### 10.2 Remaining Work

#### Phase 2: Visualization (P1)

**DAG Graph Component** (D3.js):
- Files: `templates/partials/dag_graph.html`, `static/js/dag-graph.js`, `static/css/dag-graph.css`

```javascript
class DAGGraph {
    constructor(container, options = {}) {
        this.container = container;
        this.width = options.width || 800;
        this.height = options.height || 600;
    }
    render(workflow, nodeStates = {}) { /* D3 force-directed layout */ }
    updateNodeStatus(nodeId, status) { /* Update node color */ }
    highlightPath(nodeId) { /* Highlight path from START to nodeId */ }
}
```

CSS node status colors:
```css
.dag-node.completed { fill: var(--color-success); }
.dag-node.running { fill: var(--color-info); animation: pulse 1s infinite; }
.dag-node.failed { fill: var(--color-danger); }
.dag-node.pending { fill: var(--color-text-muted); }
```

| Task | Status |
|------|--------|
| Create DAG graph partial | [ ] |
| Implement D3.js graph | [ ] |
| Add node status colors | [ ] |
| Add edge animations | [ ] |
| Add node click handler | [ ] |
| Add zoom/pan controls | [ ] |
| Integrate into job_detail.html | [ ] |

#### Phase 3: Interactive Features (P2)

**Job Submission Page** (`templates/submit.html`):
- Workflow dropdown populated from `/api/v1/workflows`
- Dynamic params form based on workflow inputs
- JSON editor for advanced input_params editing
- Form validation and submit handler

| Task | Status |
|------|--------|
| Create submit.html template | [ ] |
| Add workflow dropdown | [ ] |
| Add dynamic params form | [ ] |
| Add form validation | [ ] |
| Add submit handler | [ ] |
| Add UI route `/ui/submit` | [ ] |

**Keyboard Shortcuts**:

| Shortcut | Action |
|----------|--------|
| `r` | Refresh current page |
| `g h` | Go to home/dashboard |
| `g j` | Go to jobs |
| `g w` | Go to workflows |
| `?` | Show keyboard shortcuts help |
| `Esc` | Close modal/cancel |

#### Phase 4: Monitoring (P3)

- **Tasks Page**: Task results listing with worker_id filter, duration histogram
- **Queues Page**: Queue depth monitoring with auto-refresh
- **Real-Time Updates**: WebSocket endpoint `/ws/jobs/{job_id}` for live node status

#### Phase 5: Polish (P4)

- Error handling: error.html, 404.html templates, retry buttons
- Loading states: skeleton loaders, spinners, inline loading
- Responsive design: mobile navigation, card view for tables
- Accessibility: ARIA labels, focus indicators, WCAG AA compliance

### 10.3 DAG Adapter Implementation

The `ui/adapters/dag.py` is currently a stub:

```python
# ui/adapters/dag.py
def job_to_dto(job: Job) -> JobDTO:
    return JobDTO(
        job_id=job.job_id,
        workflow_id=job.workflow_id,
        status=map_dag_job_status(job.status),
        result_data=job.result_data,
        error_message=job.error_message,
        created_at=job.created_at,
        completed_at=job.completed_at,
        parameters=job.input_params or {},
    )
```

| Task | Status |
|------|--------|
| Implement job_to_dto() | [ ] |
| Implement node_to_dto() | [ ] |
| Implement task_to_dto() | [ ] |
| Update __init__.py detection | [ ] |
| Add unit tests | [ ] |

### 10.4 New Backend Endpoints Needed

| Endpoint | Priority | Purpose |
|----------|----------|---------|
| `GET /api/v1/nodes` | P1 | Cross-job node monitoring |
| `GET /api/v1/tasks` | P3 | Task results listing |
| `GET /api/v1/queues/status` | P3 | Queue depth monitoring |
| `POST /api/v1/jobs/{id}/nodes/{id}/retry` | P2 | Retry single node |
| `WS /ws/jobs/{id}` | P3 | Real-time updates |

---

## 11. H3 Hexagonal Aggregation Pipeline

**Status**: Design complete (12 FEB 2026) — implementation deferred until core DAG is production-ready.

**Full design document**: `docs/HEXAGONS.md`

This section previously contained an early sketch of H3 network synthesis. The design has been significantly expanded into a standalone document covering:

- **Raster zonal statistics**: Fan-out by L3 cell, `exactextract` against rasters, parquet output (Sections 1–4)
- **Computation catalog**: Database schema defining what gets computed — recipe book in PostgreSQL, output in parquet on blob storage (Section 5)
- **Vector aggregation**: Overture Maps GeoParquet + internal PostGIS sources — point count, line length by class, polygon area, binary intersect (Section 10)
- **Cell connectivity graph**: Adjacent-cell road crossing model with deterministic boundary ownership (no shuffle), travel-time scoring, igraph analysis (Section 11)
- **Friction surface**: Combine road connectivity + raster terrain stats for travel-cost edge weights (Section 11.7)

**Key design decisions** (documented in HEXAGONS.md Section 2):
1. No H3 geometry bootstrap — `h3-py` generates cells on the fly
2. Geodesic distance everywhere — no projected CRS, no UTM zones
3. No database for computed stats — parquet is the only data layer
4. Overture GeoParquet over HTTP — no planet.osm import
5. L3 chunking for fan-out (~2,800 cells per chunk, 50–500 tasks per raster)

**Implementation phases** (P1–P11) are sequenced in HEXAGONS.md Section 12. All phases depend on the core DAG orchestrator being operational (Tiers 1–3, done) and the business domain layer (Tier 4, in progress).

---

## 12. Concurrency & Locking Reference

### 12.1 Four-Layer Locking Architecture

```
Layer 1: ORCHESTRATOR LEASE (Table-based with TTL)
├── dag_orchestrator_lease table with pulse-based expiry
├── Only one orchestrator instance runs (single-leader)
├── Automatic failover on crash (30s TTL)
└── NOTE: Replaced by multi-orchestrator in Phase 0.9

Layer 2: JOB LOCKS (Advisory locks)
├── pg_try_advisory_xact_lock('rmhdag:job:{job_id}')
├── Jobs can be safely processed in parallel
├── Non-blocking: skip job if locked
└── Transaction-scoped: auto-release

Layer 3: OPTIMISTIC LOCKING (Version column)
├── WHERE version = %(expected_version)s
├── Detect concurrent modifications, retry on conflict
└── Fine-grained: per-row protection

Layer 4: IDEMPOTENT TRANSITIONS (Already implemented)
├── WHERE status = 'ready' (conditional updates)
├── WHERE processed = false (for results)
└── State transitions are safe to retry
```

### 12.2 Why Lease-Based for Orchestrator Lock?

| Scenario | Advisory Lock | Lease-Based |
|----------|---------------|-------------|
| Normal shutdown | Released | Released |
| Container crash (SIGKILL) | Stuck for minutes | Expires in 30s |
| Network partition | Lock held indefinitely | Expires in 30s |
| ASE scales out | New instance blocked | New instance blocked |

When a container crashes in ASE, PostgreSQL advisory locks don't release immediately. TCP keepalive timeout can be 60+ seconds, during which no new orchestrator can start. The lease-based approach expires automatically.

### 12.3 Lease Table Schema

```sql
CREATE TABLE dagapp.dag_orchestrator_lease (
    lock_name       VARCHAR(64) PRIMARY KEY,
    holder_id       VARCHAR(64) NOT NULL,
    acquired_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    pulse_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    lease_ttl_sec   INTEGER NOT NULL DEFAULT 30
);
```

**Acquisition SQL** (atomic INSERT ON CONFLICT):
```sql
INSERT INTO dagapp.dag_orchestrator_lease (lock_name, holder_id, acquired_at, pulse_at)
VALUES ('orchestrator', $1, NOW(), NOW())
ON CONFLICT (lock_name) DO UPDATE
SET holder_id = EXCLUDED.holder_id, acquired_at = NOW(), pulse_at = NOW()
WHERE
    dag_orchestrator_lease.holder_id = EXCLUDED.holder_id           -- same holder
    OR dag_orchestrator_lease.pulse_at < NOW() - INTERVAL '1 second'
       * dag_orchestrator_lease.lease_ttl_sec                       -- expired
RETURNING holder_id = $1 AS acquired;
```

**Pulse SQL** (every 10 seconds):
```sql
UPDATE dagapp.dag_orchestrator_lease
SET pulse_at = NOW()
WHERE lock_name = 'orchestrator' AND holder_id = $1
RETURNING true AS pulsed;
```

**Release SQL** (graceful shutdown):
```sql
DELETE FROM dagapp.dag_orchestrator_lease
WHERE lock_name = 'orchestrator' AND holder_id = $1;
```

### 12.4 LockService Implementation

**File**: `infrastructure/locking.py`

```python
class LockService:
    JOB_LOCK_PREFIX = "rmhdag:job:"
    LEASE_TTL_SEC = 30
    PULSE_INTERVAL_SEC = 10

    def __init__(self, pool: AsyncConnectionPool):
        self.pool = pool
        self._holder_id = str(uuid.uuid4())
        self._has_lease = False
        self._pulse_task: Optional[asyncio.Task] = None

    @staticmethod
    def _hash_to_lock_id(key: str) -> int:
        h = hashlib.sha256(key.encode()).digest()[:8]
        return int.from_bytes(h, byteorder='big', signed=True)

    # Layer 1: Orchestrator Lease
    async def try_acquire_orchestrator_lease(self) -> bool: ...
    async def pulse_lease(self) -> bool: ...
    async def release_orchestrator_lease(self) -> None: ...
    async def start_pulse_task(self) -> None: ...
    async def stop_pulse_task(self) -> None: ...

    # Layer 2: Job Lock (Advisory)
    @asynccontextmanager
    async def job_lock(self, job_id: str, blocking: bool = False):
        lock_id = self._hash_to_lock_id(f"{self.JOB_LOCK_PREFIX}{job_id}")
        async with self.pool.connection() as conn:
            conn.row_factory = dict_row
            if blocking:
                await conn.execute("SELECT pg_advisory_xact_lock(%s)", (lock_id,))
                acquired = True
            else:
                result = await conn.execute(
                    "SELECT pg_try_advisory_xact_lock(%s) as acquired", (lock_id,),
                )
                row = await result.fetchone()
                acquired = row["acquired"] if row else False
            try:
                yield acquired
            finally:
                pass  # Auto-release at transaction end
```

### 12.5 OrchestratorLease Model

**File**: `core/models/lease.py`

```python
class OrchestratorLease(BaseModel):
    __sql_table__: ClassVar[str] = "dag_orchestrator_lease"
    __sql_schema__: ClassVar[str] = "dagapp"
    __sql_primary_key__: ClassVar[List[str]] = ["lock_name"]

    lock_name: str = Field(max_length=64)
    holder_id: str = Field(max_length=64)
    acquired_at: datetime = Field(default_factory=datetime.utcnow)
    pulse_at: datetime = Field(default_factory=datetime.utcnow)
    lease_ttl_sec: int = Field(default=30, ge=10, le=300)
```

### 12.6 Optimistic Locking (Version Column)

**Model** (`core/models/node.py`):
```python
class NodeState(NodeData):
    version: int = Field(default=1, description="Version for optimistic locking")
```

**Repository** (`repositories/node_repo.py`):
```python
async def update(self, node: NodeState) -> bool:
    result = await conn.execute(
        """UPDATE dagapp.dag_node_states SET
            status = %(status)s, ..., version = version + 1
        WHERE job_id = %(job_id)s AND node_id = %(node_id)s
          AND version = %(version)s""",
        {..., "version": node.version},
    )
    if result.rowcount == 0:
        logger.warning(f"Version conflict updating node {node.node_id}")
        return False
    node.version += 1
    return True
```

**Service layer conflict handling** (`services/node_service.py`):
```python
async def check_and_ready_nodes(self, job_id, workflow):
    for node in pending_nodes:
        if self._dependencies_met(node.node_id, node_def, completed_nodes, workflow):
            node.mark_ready()
            success = await self.node_repo.update(node)  # Optimistic
            if success:
                newly_ready.append(node)
            else:
                # Version conflict - another process modified this node
                # Will catch it next cycle
                logger.debug(f"Version conflict marking {node.node_id} ready")
```

### 12.7 Configuration Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `LEASE_TTL_SEC` | 30 | Lease expires after this many seconds without pulse |
| `PULSE_INTERVAL_SEC` | 10 | How often to renew the lease (TTL/3) |
| `LEASE_ACQUIRE_RETRIES` | 3 | Retry count on transient failures |
| `LEASE_ACQUIRE_DELAY_SEC` | 5 | Delay between acquisition retries |

### 12.8 Lock Placement

| Lock Type | Layer | Location | Rationale |
|-----------|-------|----------|-----------|
| Orchestrator lock | Infrastructure | `LockService` | Global singleton, infrastructure concern |
| Job lock | Infrastructure | `LockService` | Coordination mechanism |
| Version check | Repository | `NodeRepository` | Data integrity at storage boundary |
| Conflict handling | Service | `NodeService` | Business logic decides retry vs fail |

### 12.9 Testing Concurrency

```python
@pytest.mark.asyncio
async def test_orchestrator_lease_exclusive(pool):
    lock1 = LockService(pool)
    lock2 = LockService(pool)
    assert await lock1.try_acquire_orchestrator_lease() is True
    assert await lock2.try_acquire_orchestrator_lease() is False
    await lock1.release_orchestrator_lease()
    assert await lock2.try_acquire_orchestrator_lease() is True

@pytest.mark.asyncio
async def test_lease_expiry_allows_takeover(pool):
    lock1 = LockService(pool)
    assert await lock1.try_acquire_orchestrator_lease() is True
    # Simulate expiry
    async with pool.connection() as conn:
        await conn.execute("""
            UPDATE dagapp.dag_orchestrator_lease
            SET pulse_at = NOW() - INTERVAL '60 seconds'
            WHERE lock_name = 'orchestrator'
        """)
    lock2 = LockService(pool)
    assert await lock2.try_acquire_orchestrator_lease() is True

@pytest.mark.asyncio
async def test_version_conflict_detection(pool, node_repo):
    node = NodeState(job_id="job1", node_id="node1", status=NodeStatus.PENDING)
    await node_repo.create(node)
    node_a = await node_repo.get("job1", "node1")
    node_b = await node_repo.get("job1", "node1")
    node_a.mark_ready()
    assert await node_repo.update(node_a) is True   # version 1 -> 2
    node_b.mark_ready()
    assert await node_repo.update(node_b) is False   # version mismatch
```

---

## 13. Multi-Orchestrator Implementation Reference

**Status**: Phase 0.9 DONE (04 FEB 2026)

This section contains the actual implementation code for the multi-orchestrator competing consumers architecture. For the design rationale, see `docs/ARCHITECTURE.md`.

### 13.1 Core Infrastructure

#### Base Repository (`infrastructure/base_repository.py`)

Provides common patterns for all repositories:

```python
class MyRepository(AsyncBaseRepository):
    async def some_operation(self, entity_id: str):
        with self._error_context("some operation", entity_id):
            # ... database operations ...

    def update_status(self, current, new):
        self._validate_status_transition(current, new, JOB_STATUS_TRANSITIONS)
```

Features: `_error_context()`, `_validate_status_transition()`, `_log_operation()`, typed exceptions.

#### Service Bus Infrastructure (`infrastructure/service_bus.py`)

```python
from infrastructure.service_bus import (
    ServiceBusConfig, ServiceBusPublisher, ServiceBusConsumer, BatchResult,
)

# Configuration from environment
config = ServiceBusConfig.from_env()

# Publisher (singleton, thread-safe)
publisher = ServiceBusPublisher.get_instance(config)
message_id = publisher.send_message("queue-name", pydantic_model)

# Consumer (async)
consumer = ServiceBusConsumer(config, "queue-name")
await consumer.connect()
result = await consumer.receive_one(max_wait_time=5.0, message_type=MyModel)
if result:
    parsed_message, raw_message = result
    await consumer.complete(raw_message)

# Continuous consumption loop
async def handler(message, raw) -> bool:
    return True  # Complete (or False to abandon)
await consumer.consume_loop(handler, stop_event)
```

Environment Variables:
```bash
SERVICE_BUS_NAMESPACE=rmhazure.servicebus.windows.net
SERVICE_BUS_CONNECTION_STRING=  # Optional, for local dev
MANAGED_IDENTITY_CLIENT_ID=     # Optional, for managed identity
```

### 13.2 Job Model Extensions

**File**: `core/models/job.py`

```python
class Job(JobData):
    __sql_indexes__: ClassVar[List[tuple]] = [
        ("idx_dag_jobs_status", ["status"]),
        ("idx_dag_jobs_workflow", ["workflow_id"]),
        ("idx_dag_jobs_created", ["created_at"]),
        ("idx_dag_jobs_owner", ["owner_id"]),
        ("idx_dag_jobs_heartbeat", ["owner_heartbeat_at"]),
    ]

    # Ownership (multi-orchestrator)
    owner_id: Optional[str] = Field(default=None, max_length=64)
    owner_heartbeat_at: Optional[datetime] = Field(default=None)
```

### 13.3 Job Queue Message Model

**File**: `core/models/job_queue_message.py`

```python
class JobQueueMessage(BaseModel):
    """Message format for the dag-jobs queue."""
    workflow_id: str = Field(..., max_length=64)
    input_params: Dict[str, Any] = Field(default_factory=dict)
    correlation_id: Optional[str] = Field(default=None, max_length=64)
    submitted_by: Optional[str] = Field(default=None, max_length=64)
    callback_url: Optional[str] = Field(default=None, max_length=512)
    idempotency_key: Optional[str] = Field(default=None, max_length=64)
    priority: int = Field(default=0, ge=0, le=10)
    submitted_at: datetime = Field(default_factory=datetime.utcnow)
    message_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
```

### 13.4 Job Queue Repository

**File**: `repositories/job_queue_repo.py`

Thin wrapper around `ServiceBusConsumer` with `JobQueueMessage` type binding:

```python
class JobQueueRepository:
    def __init__(self, config=None):
        self._config = config or ServiceBusConfig.from_env()
        self._queue_name = os.environ.get("JOB_QUEUE_NAME")  # Required - no default
        self._consumer = None

    async def connect(self) -> None: ...
    async def close(self) -> None: ...
    async def receive_one(self, max_wait_time=5.0) -> Optional[tuple]: ...
    async def complete(self, raw_message) -> None: ...
    async def abandon(self, raw_message) -> None: ...
    async def dead_letter(self, raw_message, reason, description) -> None: ...
    async def consume_loop(self, handler, stop_event) -> None: ...
```

### 13.5 Job Repository Extensions

**File**: `repositories/job_repo.py`

Key new methods:

```python
async def create_with_owner(self, job: Job, owner_id: str) -> Job:
    """Create job with ownership claim."""

async def list_active_for_owner(self, owner_id: str, limit=100) -> List[Job]:
    """List active jobs owned by this orchestrator."""

async def update_heartbeat(self, owner_id: str) -> int:
    """Update heartbeat for all jobs owned by this orchestrator."""

async def reclaim_orphaned_jobs(
    self, new_owner_id: str, orphan_threshold_seconds=120, limit=10,
) -> List[str]:
    """Reclaim jobs with stale heartbeats (FOR UPDATE SKIP LOCKED)."""
```

Orphan reclaim uses atomic `UPDATE ... WHERE ... FOR UPDATE SKIP LOCKED ... RETURNING`:

```sql
UPDATE dagapp.dag_jobs
SET owner_id = %s, owner_heartbeat_at = NOW()
WHERE job_id IN (
    SELECT job_id FROM dagapp.dag_jobs
    WHERE status IN ('pending', 'running')
      AND owner_heartbeat_at < NOW() - INTERVAL '%s seconds'
    LIMIT %s
    FOR UPDATE SKIP LOCKED
)
RETURNING job_id
```

### 13.6 Orchestrator Loop Redesign

**File**: `orchestrator/loop.py`

The multi-instance orchestrator has four background tasks:

```python
class Orchestrator:
    POLL_INTERVAL_SEC = 1.0
    HEARTBEAT_INTERVAL_SEC = 30
    ORPHAN_SCAN_INTERVAL_SEC = 60
    ORPHAN_THRESHOLD_SEC = 120

    async def start(self):
        """No lease acquisition - multiple instances can start."""
        self._main_loop_task = asyncio.create_task(self._main_loop())
        self._queue_consumer_task = asyncio.create_task(self._queue_consumer_loop())
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        self._orphan_scan_task = asyncio.create_task(self._orphan_scan_loop())
```

**Queue Consumer** - Claims jobs from `dag-jobs` queue:
```python
async def _handle_job_submission(self, message: JobQueueMessage, raw) -> bool:
    workflow = self.workflow_service.get(message.workflow_id)
    if workflow is None:
        await self._job_queue_repo.dead_letter(raw, "WorkflowNotFound", "...")
        return True
    if message.idempotency_key:
        existing = await self._job_repo.get_by_idempotency_key(message.idempotency_key)
        if existing:
            return True  # Already processed
    job = await self.job_service.create_job_with_owner(
        workflow_id=message.workflow_id,
        input_params=message.input_params,
        owner_id=self._owner_id,
        correlation_id=message.correlation_id,
        submitted_by=message.submitted_by,
        idempotency_key=message.idempotency_key,
    )
    self._jobs_claimed += 1
    return True
```

**Main Loop** - Only processes owned jobs:
```python
async def _cycle(self):
    await self._process_results()
    active_jobs = await self._job_repo.list_active_for_owner(self._owner_id)
    for job in active_jobs:
        await self._process_job(job)
```

**Heartbeat** - Keeps ownership alive:
```python
async def _heartbeat_loop(self):
    while not self._stop_event.is_set():
        count = await self._job_repo.update_heartbeat(self._owner_id)
        await asyncio.wait_for(self._stop_event.wait(), timeout=30)
```

**Orphan Scan** - Reclaims abandoned jobs:
```python
async def _orphan_scan_loop(self):
    while not self._stop_event.is_set():
        reclaimed = await self._job_repo.reclaim_orphaned_jobs(
            new_owner_id=self._owner_id, orphan_threshold_seconds=120,
        )
        if reclaimed:
            self._jobs_reclaimed += len(reclaimed)
        await asyncio.wait_for(self._stop_event.wait(), timeout=60)
```

### 13.7 Configuration

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| `HEARTBEAT_INTERVAL_SEC` | 30 | Keep job ownership alive |
| `ORPHAN_THRESHOLD_SEC` | 120 | 2 minutes before job considered abandoned |
| `ORPHAN_SCAN_INTERVAL_SEC` | 60 | How often to check for orphans |

### 13.8 Key Bug Fixes (v0.9.1 - v0.9.3)

- **v0.9.1**: Fixed `metadata` parameter in job creation (was passing dict, needed `idempotency_key`)
- **v0.9.2**: Added `owner_id` and `owner_heartbeat_at` to UPDATE SQL in job_repo
- **v0.9.3**: Fixed metadata null handling (use `Json({})` instead of `None`)

---

## Appendix A: Completed Work Log

### Phase 0: Project Setup (DONE)

| Task | Notes |
|------|-------|
| Create repo | `rmhdagmaster` |
| Project structure | `core/`, `orchestrator/`, `workflows/` |
| Pydantic models | Job, NodeState, TaskResult, JobEvent, WorkflowDefinition |
| Base contracts | JobStatus, NodeStatus, TaskStatus enums |
| Schema generator | PydanticToSQL with __sql_* metadata |
| DDL utilities | IndexBuilder, TriggerBuilder |
| Sample workflows | hello_world.yaml, raster_processing.yaml |

### Phase 0.5: Repository Architecture Refactor (DONE)

Introduced `AsyncBaseRepository` -> `AsyncPostgreSQLRepository` -> Domain repos:
- Pool-level dict_row (impossible to forget)
- Auto Json() wrapping for dict values
- Named parameters only (`%(field)s`)
- Connection pooling for app lifetime

### Phase 0.7: Critical Fixes P0 (DONE)

| Fix | Description |
|-----|-------------|
| P0.0 Stale State Bug | Re-fetch node states before completion check |
| P0.1 Event Timeline Logging | EventRepository + EventService + emit at all lifecycle points |
| P0.2 Retry Logic | Auto-retry failed nodes up to max_retries |
| P0.3 Orchestrator Stats | GET /api/v1/orchestrator/status endpoint |

### Phase 0.8: Concurrency Control (DONE)

| Layer | Implementation |
|-------|---------------|
| Layer 1: Orchestrator Lease | dag_orchestrator_lease table with pulse-based TTL |
| Layer 2: Job Locks | pg_try_advisory_xact_lock (transaction scope) |
| Layer 3: Optimistic Locking | Version column on dag_node_states and dag_jobs |
| Layer 4: Idempotent Transitions | Conditional WHERE clauses (already existed) |

### Phase 0.8.1: Lease-Based Locking (DONE)

Replaced advisory lock with table-based lease + pulse for crash recovery. Crash recovery time reduced from minutes (TCP keepalive) to 30 seconds (lease TTL).

### Phase 0.9: Multi-Orchestrator (DONE - 04 FEB 2026)

Replaced single-leader model with competing consumers:
- dag-jobs Service Bus queue for job submission
- Job ownership model with owner_id + owner_heartbeat_at
- Each orchestrator only processes its own jobs
- Orphan recovery for jobs with stale heartbeats
- Gateway function app submits to queue

### Phase 0.9.5: Function App Gateway (DONE - 04 FEB 2026)

22-endpoint Azure Function App:
- Job submission to Service Bus
- Read-only PostgreSQL queries
- HTTP proxy to Docker apps
- Admin and health endpoints

### Phase 1.5: Advanced DAG Patterns

| Pattern | Status |
|---------|--------|
| P2.1 Conditional Routing | DONE |
| P2.4 Checkpointing | DONE |
| P2.2 Fan-Out | DONE (06 FEB 2026) |
| P2.3 Fan-In | DONE (06 FEB 2026) |

### Phase 2.0: Template Engine + Fan-Out/Fan-In (06 FEB 2026)

- Jinja2 template engine wired into dispatch path (`{{ inputs.x }}`, `{{ nodes.y.output.z }}`)
- FanOutTaskDef model, FanOutHandler.expand() with Jinja2 source resolution
- Dynamic child node creation and batch dispatch
- Fan-in aggregation (collect, concat, sum, first, last)
- 64 unit tests across 3 test files

### Phase 2.1: Azure Verification + Hardening (07 FEB 2026)

- Deployed v0.10.0 to Azure (orchestrator + worker)
- Echo test verified end-to-end via queue submission (`tools/submit_job.py`)
- Fan-out/fan-in test verified: 3-item fan-out → 8 total nodes → aggregation collected 3 results
- Bug fixes: task_repo UPSERT, job_repo orphan scan SQL, bootstrap migrations for new columns
- **No-defaults policy**: Removed all default queue names across 12 files. Missing queue config causes ValueError at startup, 503 at request time, UNHEALTHY in health checks. Affected files: `messaging/config.py`, `worker/contracts.py`, `repositories/job_queue_repo.py`, `function/config.py`, `function/blueprints/gateway_bp.py`, `gateway/routes.py`, `health/checks/startup.py`, `health/checks/worker.py`, `core/models/workflow.py`, `handlers/registry.py`, `orchestrator/loop.py`
- CLI test tool: `tools/submit_job.py` — submits JobQueueMessage to Service Bus, supports `--poll` for status tracking

### Database Schema Deployment (28 JAN 2026)

```
Host: rmhpostgres.postgres.database.azure.com
Schema: dagapp
Tables: dag_jobs, dag_node_states, dag_task_results, dag_job_events, dag_orchestrator_lease, dag_checkpoints
ENUMs: 5 types (job_status, node_status, task_status, event_type, event_status)
Indexes: 18+ indexes
Triggers: updated_at on dag_jobs, dag_node_states
```

---

## Appendix B: Handler Library

These handlers exist in rmhgeoapi and can be called from DAG workflows:

```
RASTER HANDLERS
├── raster_validate              # CRS and format detection
├── raster_process_complete      # Consolidated Docker (validate -> COG -> STAC)
├── create_cog                   # GeoTIFF creation with compression
└── extract_stac_metadata        # STAC item generation

VECTOR HANDLERS
├── vector_prepare               # Load, validate, chunk
├── vector_upload_chunk          # DELETE+INSERT for idempotency
├── vector_create_stac           # STAC item registration
└── vector_docker_complete       # Consolidated Docker handler

FATHOM ETL HANDLERS
├── fathom_tile_inventory        # Query unprocessed tiles
├── fathom_band_stack            # Stack 8 return periods into COG
├── fathom_grid_inventory        # Query Phase 1 -> Phase 2 pending
├── fathom_spatial_merge         # Merge tiles band-by-band
└── fathom_stac_register         # STAC collection/item creation

H3 AGGREGATION HANDLERS
├── h3_inventory                 # List H3 hexagon boundaries
├── h3_raster_zonal              # Zonal statistics computation
├── h3_register_dataset          # Register results to STAC
├── h3_export_dataset            # Export to cloud storage
└── h3_finalize                  # Aggregate pyramid results

STAC/CATALOG HANDLERS
├── list_raster_files            # Enumerate container files
├── extract_stac_metadata        # Bulk STAC extraction
├── stac_catalog_container       # Build STAC catalog from container
├── rebuild_stac                 # Rebuild catalog from corrupted state
└── repair_stac_items            # Fix malformed STAC items

APPROVAL/PUBLISHING HANDLERS
├── trigger_approval             # Create approval workflow
├── approve_dataset              # Mark approved, trigger ADF
├── reject_dataset               # Mark rejected
└── promote_to_external          # Publish PUBLIC data

MAINTENANCE HANDLERS
├── detect_orphan_blobs          # Find unreferenced blob files
├── register_silver_blobs        # Index silver-zone files
├── unpublish_raster             # Cascade delete raster + STAC
├── unpublish_vector             # Cascade delete vector + STAC
├── job_health_check             # Monitor stalled jobs
└── task_watchdog                # Monitor stalled tasks

FUTURE HANDLERS (to build)
├── raster_reproject             # Reproject to target CRS
├── raster_clip                  # Clip to boundary
├── raster_mosaic                # Merge multiple rasters
├── vector_simplify              # Reduce complexity
├── vector_dissolve              # Merge features
└── ml_predict                   # Run ML model
```

---

## Appendix C: Key File Locations

### Existing Files

| Component | File |
|-----------|------|
| Main loop | `orchestrator/loop.py` |
| Dependency resolution | `orchestrator/engine/evaluator.py` |
| Template resolution | `orchestrator/engine/templates.py` |
| Job model | `core/models/job.py` |
| Node model | `core/models/node.py` |
| Task result model | `core/models/task.py` |
| Event model | `core/models/events.py` |
| Lease model | `core/models/lease.py` |
| Checkpoint model | `core/models/checkpoint.py` |
| Job queue message | `core/models/job_queue_message.py` |
| Job repository | `repositories/job_repo.py` |
| Node repository | `repositories/node_repo.py` |
| Task repository | `repositories/task_repo.py` |
| Event repository | `repositories/event_repo.py` |
| Checkpoint repository | `repositories/checkpoint_repo.py` |
| Job queue repository | `repositories/job_queue_repo.py` |
| Lock service | `infrastructure/locking.py` |
| Service Bus | `infrastructure/service_bus.py` |
| Base repository | `infrastructure/base_repository.py` |
| Job service | `services/job_service.py` |
| Node service | `services/node_service.py` |
| Event service | `services/event_service.py` |
| Checkpoint service | `services/checkpoint_service.py` |
| API routes | `api/routes.py` |
| UI routes | `api/ui_routes.py` |
| Bootstrap API | `api/bootstrap_routes.py` |
| Function App entry | `function_app.py` |
| Function blueprints | `function/blueprints/*.py` |
| CLI test tool | `tools/submit_job.py` |

### Files To Be Created

| Priority | Component | File |
|----------|-----------|------|
| ~~P2.2~~ | ~~Fan-Out Logic~~ | ~~Integrated into `orchestrator/engine/evaluator.py` + `orchestrator/loop.py`~~ |
| ~~P2.3~~ | ~~Fan-In Logic~~ | ~~Integrated into `orchestrator/loop.py`~~ |
| P3.1 | Progress Service | `services/progress_service.py` |
| P3.3 | Metrics Service | `services/metrics_service.py` |
| P3.4 | Tracing Config | `infrastructure/tracing.py` |
| -- | GeospatialAsset | `core/models/geospatial_asset.py` |
| -- | API Request | `core/models/api_request.py` |
| -- | Blob Asset | `core/models/blob_asset.py` |
| -- | Config Defaults | `core/config/defaults.py` |

---

## CI/CD Pipeline

### GitHub Actions Workflow

```yaml
name: Build and Deploy
on:
  push:
    branches: [main]

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Build image
        run: docker build -t rmhdagmaster:${{ github.sha }} .
      - name: Push to ACR
        run: |
          docker tag rmhdagmaster:${{ github.sha }} rmhgeoregistry.azurecr.io/rmhdagmaster:${{ github.sha }}
          docker tag rmhdagmaster:${{ github.sha }} rmhgeoregistry.azurecr.io/rmhdagmaster:latest
          docker push --all-tags rmhgeoregistry.azurecr.io/rmhdagmaster

  deploy:
    needs: build
    runs-on: ubuntu-latest
    steps:
      - name: Deploy all apps
        run: |
          az webapp config container set --name dag-orchestrator ...
          az webapp config container set --name dag-worker-light ...
          az webapp config container set --name dag-worker-heavy ...
```

### Deployment Configuration

| App | RUN_MODE | WORKER_TYPE | WORKER_QUEUE | Instances |
|-----|----------|-------------|--------------|-----------|
| rmhdagmaster | orchestrator | - | DAG_WORKER_QUEUE=dag-worker-tasks | 1+ (always on) |
| rmhdagworker | worker | docker | WORKER_QUEUE=dag-worker-tasks | 1-5 |

**Queue Configuration** (no defaults — all must be explicitly set):
- `JOB_QUEUE_NAME=dag-jobs` — Gateway → Orchestrator job submission
- `DAG_WORKER_QUEUE=dag-worker-tasks` — Orchestrator → Worker task dispatch
- `WORKER_QUEUE=dag-worker-tasks` — Worker listens on this queue

---

*Document generated: 07 FEB 2026*
*Sources: ADVANCED.md, FUNCTION_APP_PLAN.md, TODO.md, MULTI_ORCH_IMPL_PLAN.md, UI_PLAN.md, H3.md*
