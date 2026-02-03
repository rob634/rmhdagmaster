# TODO - rmhdagmaster Implementation Plan

**Last Updated**: 02 FEB 2026
**Source**: Consolidated from EPOCH_5.md + rmhgeoapi deep review

---

## Architecture Summary

**Single Image, Multiple Deployments**

```
GitHub Push → CI/CD Build → ACR Image → Multiple Azure Web Apps
                              │
              ┌───────────────┼───────────────┐
              ▼               ▼               ▼
        dag-orchestrator  dag-worker-light  dag-worker-heavy
        RUN_MODE=orch     RUN_MODE=worker   RUN_MODE=worker
        (1 instance)      (auto-scale 0-10) (scale 1-5)
```

**Benefits**:
- One CI/CD pipeline
- One ACR image to maintain
- Version sync guaranteed
- Shared codebase (models, handlers, infrastructure)

---

## Progress Overview

| Phase | Status | Description |
|-------|--------|-------------|
| 0: Project Setup | ✅ DONE | Repo created, models defined, schema generator built |
| **0.5: Repository Refactor** | ✅ DONE | Async base class, connection pooling, auto-Json |
| **0.7: Critical Fixes (P0)** | ✅ DONE | Event logging, retry logic, stale state bug |
| **0.8: Concurrency Control** | ✅ DONE | Advisory locks, optimistic locking, version columns |
| 1: Core Engine | 🔄 IN PROGRESS | Database schema, workflow loader, evaluator |
| 2: Worker Integration | ⏳ TODO | Workers report to orchestrator |
| 3: Real Workflows | ⏳ TODO | Port raster/vector workflows |
| 4: API Integration | ⏳ TODO | HTTP endpoints for submission |
| 5: Migration & Cutover | ⏳ TODO | DAG becomes primary system |

---

## Phase 0.7: Critical Fixes (P0) 🚨 IMMEDIATE PRIORITY

**Source**: Extracted from ADVANCED.md implementation analysis

### Priority Matrix

| ID | Feature | Effort | Impact | Status |
|----|---------|--------|--------|--------|
| **P0.0** | Fix Stale State Bug | Small | **CRITICAL** | ✅ DONE |
| **P0.1** | Event Timeline Logging | Medium | High | ✅ DONE |
| **P0.2** | Retry Logic | Small | High | ✅ DONE |
| **P0.3** | Orchestrator Stats Endpoint | Small | Medium | ✅ DONE |

### P0.0: Fix Stale State Bug 🔴 CRITICAL

**Problem**: END node completes before task nodes because evaluator uses stale `node_states` from start of cycle.

**Root Cause** (`orchestrator/loop.py`):
```python
# At start of _process_job():
node_states = await node_repo.get_all_for_job(job_id)  # Snapshot

# Later, after dispatching nodes, node_states is stale
# _check_job_completion() sees stale data → END completes prematurely
```

**Fix**: Re-fetch node states before completion check (~5 lines)

| Task | Status | File |
|------|--------|------|
| Fix `_dependencies_met()` implicit dep check | ✅ DONE | `services/node_service.py` |
| Add clarifying comment in loop | ✅ DONE | `orchestrator/loop.py` |
| Add test for stale state scenario | ✅ DONE | `tests/test_integration.py` |

---

### P0.1: Event Timeline Logging

**Problem**: `JobEvent` model and `dag_job_events` table exist, but no code writes events.

**Impact**: Cannot audit execution, cannot debug silent failures.

| Task | Status | File |
|------|--------|------|
| Create EventRepository | ✅ DONE | `repositories/event_repo.py` |
| Create EventService | ✅ DONE | `services/event_service.py` |
| Add events in JobService | ✅ DONE | `services/job_service.py` |
| Add events in NodeService | ✅ DONE | `services/node_service.py` |
| Add events in Orchestrator loop | ✅ DONE | `orchestrator/loop.py` |
| Add Timeline API endpoint | ✅ DONE | `api/routes.py` |

**Events to emit**:
- `JOB_CREATED`, `JOB_STARTED`, `JOB_COMPLETED`, `JOB_FAILED`
- `NODE_READY`, `NODE_DISPATCHED`, `NODE_RUNNING`, `NODE_COMPLETED`, `NODE_FAILED`

---

### P0.2: Retry Logic

**Problem**: Fields exist (`retry_count`, `max_retries`, `prepare_retry()`), but never called.

**Impact**: Failed nodes stay failed; no automatic retry.

| Task | Status | File |
|------|--------|------|
| Add retry check after node failure | ✅ DONE | `services/node_service.py` |
| Verify prepare_retry() works | ✅ EXISTS | `core/models/node.py` |
| Read max_retries from workflow | ✅ EXISTS | `services/job_service.py` (line 296-297) |
| Add NODE_RETRY event emission | ✅ DONE | `services/node_service.py` |

---

### P0.3: Orchestrator Stats Endpoint

**Problem**: `Orchestrator.stats` property exists but no HTTP endpoint exposes it.

| Task | Status | File |
|------|--------|------|
| Enhance stats property | ✅ DONE | `orchestrator/loop.py` |
| Add tracking fields | ✅ DONE | `orchestrator/loop.py` |
| Create GET /orchestrator/status | ✅ DONE | `api/routes.py` |

---

## Phase 0.8: Concurrency Control (P1.5) 🔒 FOUNDATIONAL

**Design Reference**: See `ADVANCED.md` Section 12 for detailed implementation specs.

**Philosophy**: Even with a single orchestrator, defensive locking prevents bugs from accidental multi-instance deployment and prepares for future horizontal scaling.

### Locking Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│ Layer 1: ORCHESTRATOR LOCK                                       │
│ pg_advisory_lock('rmhdag:orchestrator')                         │
│ → Only one orchestrator instance runs                           │
├─────────────────────────────────────────────────────────────────┤
│ Layer 2: JOB LOCKS                                              │
│ pg_try_advisory_xact_lock('rmhdag:job:{job_id}')               │
│ → Jobs can be safely processed in parallel (future scaling)     │
├─────────────────────────────────────────────────────────────────┤
│ Layer 3: OPTIMISTIC LOCKING                                     │
│ WHERE version = %(expected_version)s                            │
│ → Detect concurrent modifications, retry on conflict            │
├─────────────────────────────────────────────────────────────────┤
│ Layer 4: IDEMPOTENT TRANSITIONS (already implemented)           │
│ WHERE status = 'ready' (conditional updates)                    │
│ → State transitions are safe to retry                           │
└─────────────────────────────────────────────────────────────────┘
```

### Why PostgreSQL Advisory Locks?

- **Zero external dependencies** (no Redis, etcd, Zookeeper)
- **Auto-release on crash/disconnect** (no manual cleanup)
- **Fast** (in-memory, no table writes)
- **Supports try-lock** (non-blocking, skip if busy)
- **Already have Postgres** (no new infrastructure)

### Implementation Tasks

| ID | Task | Status | File | Layer |
|----|------|--------|------|-------|
| **L1.1** | Create LockService class | ✅ DONE | `infrastructure/locking.py` | 1,2 |
| **L1.2** | Implement orchestrator lock (session-level) | ✅ DONE | `infrastructure/locking.py` | 1 |
| **L1.3** | Implement job lock context manager | ✅ DONE | `infrastructure/locking.py` | 2 |
| **L1.4** | Update Orchestrator.start() to acquire lock | ✅ DONE | `orchestrator/loop.py` | 1 |
| **L1.5** | Update Orchestrator.stop() to release lock | ✅ DONE | `orchestrator/loop.py` | 1 |
| **L1.6** | Update _process_job() to use job lock | ✅ DONE | `orchestrator/loop.py` | 2 |
| **L2.1** | Add version column to dag_node_states | ✅ DONE | `core/models/node.py` | 3 |
| **L2.2** | Add migration for version column | ✅ DONE | `api/bootstrap_routes.py` | 3 |
| **L2.3** | Update NodeRepository.update() with version check | ✅ DONE | `repositories/node_repo.py` | 3 |
| **L2.4** | Handle version conflicts in NodeService | ✅ DONE | `services/node_service.py` | 3 |
| **L3.1** | Add version column to dag_jobs | ✅ DONE | `core/models/job.py` | 3 |
| **L3.2** | Update JobRepository.update_status() with version | ✅ DONE | `repositories/job_repo.py` | 3 |

### Lock Placement Decision

| Lock Type | Location | Rationale |
|-----------|----------|-----------|
| Orchestrator lock | **Infrastructure** (`LockService`) | Global singleton, infrastructure concern |
| Job lock | **Infrastructure** (`LockService`) | Called from orchestrator loop |
| Version checking | **Repository** layer | Data integrity at storage boundary |
| Conflict handling | **Service** layer | Business logic decides retry vs fail |

**Why this placement?**

1. **LockService in Infrastructure**: Locking is an infrastructure concern like database access. It's not business logic—it's coordination mechanism. Lives alongside `AsyncPostgreSQLRepository`.

2. **Version Check in Repository**: The repository is responsible for data integrity. When it writes to the database, it should ensure the write is valid. Version checking at this layer prevents any caller (service, orchestrator) from accidentally corrupting state.

3. **Conflict Handling in Service**: When a version conflict occurs, the business logic layer decides what to do. Some operations should retry, others should log and move on. The service has the context to make this decision.

### Usage Patterns

**Orchestrator startup**:
```python
# orchestrator/loop.py
async def start(self):
    if not await self.lock_service.try_acquire_orchestrator_lock():
        raise RuntimeError("Another orchestrator instance is running")
    # ... continue startup
```

**Job processing**:
```python
# orchestrator/loop.py
async def _process_job(self, job: Job):
    async with self.lock_service.job_lock(job.job_id) as acquired:
        if not acquired:
            return  # Skip, locked by another process
        await self._process_job_internal(job)
```

**Node update with version**:
```python
# repositories/node_repo.py
async def update(self, node: NodeState) -> bool:
    result = await conn.execute(
        "UPDATE ... SET version = version + 1 WHERE version = %(version)s",
        {..., "version": node.version}
    )
    return result.rowcount > 0  # False = version conflict
```

---

## Phase 0.85: Stability (P1)

| ID | Feature | Effort | Impact | Status |
|----|---------|--------|--------|--------|
| **P1.1** | Workflow Version Pinning | Small | Medium | ⏳ TODO |
| **P1.2** | Transaction Wrapping | Medium | Medium | ⏳ TODO |

### P1.1: Workflow Version Pinning

**Problem**: Job stores `workflow_id` but not version. If workflow changes, running jobs use new definition.

| Task | Status | File |
|------|--------|------|
| Add workflow_version to Job model | ⏳ TODO | `core/models/job.py` |
| Store version on job creation | ⏳ TODO | `services/job_service.py` |
| Load correct version when processing | ⏳ TODO | `orchestrator/loop.py` |

### P1.2: Transaction Wrapping

**Problem**: Multi-step updates done in separate queries (node dispatch + job status update).

| Task | Status | File |
|------|--------|------|
| Add optional conn param to repos | ⏳ TODO | `repositories/*.py` |
| Wrap dispatch in transaction | ⏳ TODO | `orchestrator/loop.py` |
| Wrap result processing in transaction | ⏳ TODO | `services/node_service.py` |

---

## Phase 0.9: Observability (P3)

| ID | Feature | Effort | Impact | Depends On |
|----|---------|--------|--------|------------|
| **P3.1** | Progress API | Small | Medium | — |
| **P3.2** | Timeline API | Small | Medium | P0.1 |
| **P3.3** | Metrics Dashboard | Large | High | P0.1 |

### P3.1: Progress API

| Task | Status | File |
|------|--------|------|
| Create ProgressService | ⏳ TODO | `services/progress_service.py` |
| Add GET /jobs/{id}/progress | ⏳ TODO | `api/routes.py` |
| Add GET /jobs/{id}/nodes/{id}/progress | ⏳ TODO | `api/routes.py` |

### P3.2: Timeline API

| Task | Status | File |
|------|--------|------|
| Create TimelineService | ✅ DONE | `services/event_service.py` |
| Add GET /jobs/{id}/timeline | ✅ DONE | `api/routes.py` |

### P3.3: Metrics Dashboard

| Task | Status | File |
|------|--------|------|
| Create MetricsService | ⏳ TODO | `services/metrics_service.py` |
| Add GET /metrics/workflows | ⏳ TODO | `api/routes.py` |
| Add GET /metrics/failures | ⏳ TODO | `api/routes.py` |

---

## Phase 1.5: Advanced DAG Patterns (P2) 🎯 NEXT PRIORITY

**Design Reference**: See `PATTERNS.md` for detailed pattern documentation.

**Rationale**: Conditional routing + checkpointing enable serious testable workflows.
Fan-out/fan-in deferred until core patterns proven in production.

| ID | Feature | Effort | Impact | Priority | Status |
|----|---------|--------|--------|----------|--------|
| **P2.1** | Conditional Routing | Medium | High | 1st | ✅ DONE |
| **P2.4** | Checkpointing | Medium | High | 2nd | ✅ DONE |
| **P2.2** | Fan-Out | Large | High | Deferred | ⏳ TODO |
| **P2.3** | Fan-In | Medium | High | Deferred | ⏳ TODO |

### P2.1: Conditional Routing (Priority 1) ✅ COMPLETE

**Use Case**: Size-based routing, skip validation, error branching.

| Task | Status | File |
|------|--------|------|
| Define conditional node schema in workflow YAML | ✅ DONE | `core/models/workflow.py` |
| Implement evaluate_condition() | ✅ DONE | `orchestrator/engine/evaluator.py` |
| Implement skip_branch() for untaken paths | ✅ DONE | `services/node_service.py` |
| Wire up conditional routing after node completion | ✅ DONE | `orchestrator/loop.py` |
| Handle SKIPPED status in dependency resolution | ✅ DONE | `services/node_service.py` |
| Add any_of dependency support (merge after branches) | ✅ DONE | `services/node_service.py` |
| Create test workflow with conditional | ⏳ TODO | `workflows/` |

### P2.4: Checkpointing (Priority 2) ✅ COMPLETE

**Use Case**: Resume long-running tasks after failure/timeout/shutdown.

| Task | Status | File |
|------|--------|------|
| Create Checkpoint model | ✅ DONE | `core/models/checkpoint.py` |
| Add checkpoint_data to TaskMessage | ✅ DONE | `core/models/task.py` |
| Add checkpoint_id to TaskResult | ✅ DONE | `core/models/task.py` |
| Add checkpoint fields to NodeState | ✅ EXISTS | `core/models/node.py` |
| Create CheckpointRepository | ✅ DONE | `repositories/checkpoint_repo.py` |
| Create CheckpointService | ✅ DONE | `services/checkpoint_service.py` |
| Pass checkpoint to handler on retry | ✅ DONE | `orchestrator/loop.py`, `messaging/publisher.py` |
| Add checkpoint callback API endpoint | ✅ DONE | `api/routes.py` |
| Add report_checkpoint to worker reporter | ✅ DONE | `worker/reporter.py` |
| Update echo handler for checkpoint testing | ✅ DONE | `handlers/examples.py` |
| Create checkpoint test workflow | ✅ DONE | `workflows/checkpoint_test.yaml` |

**Note**: Full checkpointing flow is implemented. Handler registry already has checkpoint support via `HandlerResult.checkpoint_result()`. Worker executor handles checkpoint loading and saving. To test:
1. Deploy schema: `POST /api/v1/bootstrap/deploy?confirm=yes` (creates `dag_checkpoints` table)
2. Submit job: `POST /api/v1/jobs` with `workflow_id: checkpoint_test`, `input_params: {message: "test", steps: 5}`
3. Interrupt worker mid-execution
4. Restart worker - job should resume from last checkpoint

### P2.2: Fan-Out (Deferred)

**Use Case**: Tiled raster processing, chunked vector uploads, parallel analysis.

| Task | Status | File |
|------|--------|------|
| Add parent_node_id, fan_out_index to NodeState | ✅ EXISTS | `core/models/node.py` |
| Create FanOutExpander class | ⏳ TODO | `orchestrator/engine/fan_out.py` |
| Add fan_out context to templates | ⏳ TODO | `orchestrator/engine/templates.py` |
| Wire up expansion after fan-out node completes | ⏳ TODO | `orchestrator/loop.py` |
| Add get_by_parent() to NodeRepository | ⏳ TODO | `repositories/node_repo.py` |

### P2.3: Fan-In (Deferred)

**Use Case**: Merge tiled results, aggregate parallel outputs.

| Task | Status | File |
|------|--------|------|
| Create FanInAggregator class | ⏳ TODO | `orchestrator/engine/fan_in.py` |
| Update _dependencies_met() for dynamic nodes | ⏳ TODO | `services/node_service.py` |
| Add fan_in context to templates | ⏳ TODO | `orchestrator/engine/templates.py` |
| Collect all outputs for fan-in node | ⏳ TODO | `orchestrator/loop.py` |

---

## Future Enhancements (Aspirational)

| Feature | Description | Complexity |
|---------|-------------|------------|
| **Nested Sub-Workflows** | Invoke a workflow as a node in another workflow | High |
| **Workflow Versioning** | Pin jobs to specific workflow versions | Medium |
| **Human Approval Gates** | Pause workflow for manual approval | Medium |
| **Dynamic Timeouts** | Set timeouts based on input data | Low |

---

## Recommended Implementation Order

```
Phase A: Foundation ✅ COMPLETE
────────────────────────────────
1. P0.0 Fix Stale State Bug     ✅ DONE
2. P0.1 Event Timeline Logging  ✅ DONE
3. P0.2 Retry Logic             ✅ DONE
4. P0.3 Orchestrator Stats      ✅ DONE

Phase A.5: Concurrency Control ✅ COMPLETE
──────────────────────────────────────────
5. L1.* Orchestrator + Job Locks  ✅ DONE
6. L2.* Version columns           ✅ DONE
7. L3.* Conflict handling         ✅ DONE

Phase B: Core DAG Patterns 🎯 CURRENT
─────────────────────────────────────
8.  P2.1 Conditional Routing ← Size-based routing, branch skipping
9.  P2.4 Checkpointing       ← Resume long-running tasks

Phase B.5: Parallel Patterns (Deferred)
───────────────────────────────────────
10. P2.2 Fan-Out             ← Dynamic parallel task creation
11. P2.3 Fan-In              ← Aggregate parallel results

Phase C: Stability
──────────────────
12. P1.1 Workflow Version Pinning ← Safe deployments
13. P1.2 Transaction Wrapping     ← Data integrity

Phase D: Observability
──────────────────────
14. P3.1 Progress API    ← User visibility (P3.2 Timeline done)
15. P3.3 Metrics         ← Operations dashboard

Future: Aspirational
────────────────────
- Nested Sub-Workflows   ← Workflow composition
- Human Approval Gates   ← Manual intervention points
- Horizontal Scaling     ← Multiple orchestrators (locks enable this)
```

**See**:
- `PATTERNS.md` for pattern design documentation
- `ADVANCED.md` for detailed implementation code and specifications

---

## Phase 0: Project Setup ✅ COMPLETE

| Task | Status | Notes |
|------|--------|-------|
| Create new repo | ✅ | `rmhdagmaster` |
| Project structure | ✅ | `core/`, `orchestrator/`, `workflows/` |
| Pydantic models | ✅ | Job, NodeState, TaskResult, JobEvent, WorkflowDefinition |
| Base contracts | ✅ | JobStatus, NodeStatus, TaskStatus enums |
| Schema generator | ✅ | PydanticToSQL with __sql_* metadata pattern |
| DDL utilities | ✅ | IndexBuilder, TriggerBuilder (ported from rmhgeoapi) |
| Sample workflows | ✅ | hello_world.yaml, raster_processing.yaml |
| Documentation | ✅ | CLAUDE.md with conventions |

---

## Phase 0.5: Repository Architecture Refactor 🚨 IMMEDIATE PRIORITY

**Created**: 01 FEB 2026
**Reason**: Bug discovered where `dict` wasn't wrapped in `Json()` for psycopg3. Root cause is lack of centralized database access pattern.

### Problem Statement

Current repositories have scattered patterns:
- `dict_row` must be set per-query (easy to forget)
- `Json()` wrapping must be done manually for each dict field
- Mixed parameter styles (positional `%s` vs named `%(field)s`)
- No base class - each repo repeats connection handling
- Inconsistent error handling

### Design Goals

1. **Async-first** - All operations async (psycopg3 + psycopg_pool)
2. **Connection pooling** - Single pool for app lifetime (long-running orchestrator/worker)
3. **dict_row by default** - Set at pool level, impossible to forget
4. **Auto Json() wrapping** - Base class handles serialization for all dict values
5. **Named parameters only** - No positional `%s`, always `%(field)s`
6. **Pydantic integration** - Models define schema, repos handle DB serialization

### Class Hierarchy

```
AsyncBaseRepository (abstract)
├── Validation helpers (status transitions)
├── Error context managers
├── Logging patterns
│
    ↓ inherits

AsyncPostgreSQLRepository (concrete base)
├── Pool reference (injected at startup)
├── connection() - context manager (dict_row guaranteed)
├── execute() - INSERT/UPDATE/DELETE with auto-Json
├── fetch_one() - single row → Optional[dict]
├── fetch_all() - multiple rows → List[dict]
├── _wrap_json_params() - auto-wrap dicts in Json()
│
    ↓ inherits

Domain Repositories
├── JobRepository
├── NodeRepository
└── TaskRepository
```

### Implementation Tasks

| Task | Status | File | Description |
|------|--------|------|-------------|
| Create DatabasePool singleton | ⏳ TODO | `repositories/pool.py` | Async pool lifecycle, managed identity support |
| Create AsyncBaseRepository | ⏳ TODO | `repositories/base.py` | Abstract base with validation, logging |
| Create AsyncPostgreSQLRepository | ⏳ TODO | `repositories/base.py` | Concrete base with execute/fetch methods |
| Rewrite JobRepository | ⏳ TODO | `repositories/job_repo.py` | Extend AsyncPostgreSQLRepository |
| Rewrite NodeRepository | ⏳ TODO | `repositories/node_repo.py` | Extend AsyncPostgreSQLRepository |
| Rewrite TaskRepository | ⏳ TODO | `repositories/task_repo.py` | Extend AsyncPostgreSQLRepository |
| Update main.py lifespan | ⏳ TODO | `main.py` | Initialize pool, inject into repos |
| Update worker/main.py | ⏳ TODO | `worker/main.py` | Initialize pool for worker mode |
| Deprecate old database.py | ⏳ TODO | `repositories/database.py` | Mark deprecated, remove usage |
| Unit tests | ⏳ TODO | `tests/test_repositories.py` | Test base class, Json wrapping |

### Key Code Patterns

**Pool initialization (main.py)**:
```python
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Create pool with dict_row at pool level
    pool = await DatabasePool.initialize(
        min_size=2, max_size=10,
        kwargs={"row_factory": dict_row},  # Always set!
    )

    # Inject into repos
    app.state.job_repo = JobRepository(pool)
    app.state.node_repo = NodeRepository(pool)

    yield

    await DatabasePool.close()
```

**Base class auto-wrapping**:
```python
class AsyncPostgreSQLRepository(AsyncBaseRepository):

    def _wrap_json_params(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Auto-wrap dict values in Json() - prevents the bug we hit."""
        return {
            key: Json(value) if isinstance(value, dict) else value
            for key, value in params.items()
        }

    async def execute(self, query: str, params: Dict[str, Any]) -> int:
        wrapped = self._wrap_json_params(params)
        async with self.connection() as conn:
            result = await conn.execute(query, wrapped)
            return result.rowcount
```

**Domain repo using base class**:
```python
class JobRepository(AsyncPostgreSQLRepository):

    async def update_status(self, job_id: str, status: JobStatus, result_data: dict):
        # No need to remember Json() - base class handles it!
        await self.execute(
            """UPDATE dagapp.dag_jobs
               SET status = %(status)s, result_data = %(result_data)s
               WHERE job_id = %(job_id)s""",
            {"status": status.value, "result_data": result_data, "job_id": job_id}
        )
```

### Success Criteria

- [ ] All database operations go through base class methods
- [ ] No direct `conn.execute()` calls in domain repos
- [ ] No manual `Json()` wrapping in domain repos
- [ ] No manual `dict_row` setting anywhere
- [ ] All parameters use named style `%(field)s`
- [ ] Echo test workflow completes successfully
- [ ] Raster validate test workflow completes successfully

### Files Summary

| Action | File |
|--------|------|
| **NEW** | `repositories/pool.py` |
| **NEW** | `repositories/base.py` |
| **REWRITE** | `repositories/job_repo.py` |
| **REWRITE** | `repositories/node_repo.py` |
| **REWRITE** | `repositories/task_repo.py` |
| **MODIFY** | `main.py` |
| **MODIFY** | `worker/main.py` |
| **DEPRECATE** | `repositories/database.py` |

---

## Phase 0.6: Data Integrity Gaps 🚨 IMMEDIATE PRIORITY

**Created**: 01 FEB 2026
**Reason**: Code review identified API endpoints accepting raw dicts without Pydantic validation.

### Principle

> **All data entering Python MUST go through a Pydantic model for validation.**

This ensures:
- Type safety at system boundaries
- Clear API contracts
- Automatic validation and error messages
- Consistent serialization/deserialization

### Critical Gaps (Must Fix)

#### Gap 1: `/api/callbacks/task-progress` - Raw dict parameter

**File**: `api/routes.py` lines 372-402

```python
# CURRENT (BAD)
async def receive_task_progress(request: dict):  # ← No validation!
    result = TaskResult(
        task_id=request["task_id"],  # ← KeyError risk, no type checking
        ...
    )
```

**Fix**: Create `TaskProgressCreate` Pydantic schema

| Task | Status | File |
|------|--------|------|
| Create TaskProgressCreate schema | ⏳ TODO | `api/schemas.py` |
| Update endpoint to use schema | ⏳ TODO | `api/routes.py` |

#### Gap 2: `/api/test/handler` - Raw dict parameter

**File**: `api/routes.py` lines 419-487

```python
# CURRENT (BAD)
async def test_handler(handler_name: str, request: dict):  # ← No validation!
    params=request.get("params", {}),
```

**Fix**: Create `HandlerTestRequest` Pydantic schema

| Task | Status | File |
|------|--------|------|
| Create HandlerTestRequest schema | ⏳ TODO | `api/schemas.py` |
| Update endpoint to use schema | ⏳ TODO | `api/routes.py` |

### Medium Gaps (Should Fix)

#### Gap 3: Service return types are `Dict[str, Any]`

**File**: `services/job_service.py`

```python
# CURRENT (Weak typing)
async def get_job_with_nodes(self, job_id: str) -> Optional[Dict[str, Any]]:
    return {"job": job, "nodes": nodes}  # Types hidden
```

**Fix**: Create explicit return type

| Task | Status | File |
|------|--------|------|
| Create JobWithNodes dataclass/model | ⏳ TODO | `services/job_service.py` |
| Update return type annotations | ⏳ TODO | `services/*.py` |

### New Schemas Required

| Schema | Purpose | Fields |
|--------|---------|--------|
| `TaskProgressCreate` | POST /callbacks/task-progress | task_id, job_id, node_id, progress_current, progress_total, progress_percent, progress_message |
| `HandlerTestRequest` | POST /test/handler | params (dict), timeout_seconds (int) |

### Success Criteria

- [ ] No `request: dict` parameters in API endpoints
- [ ] All API endpoints use Pydantic request schemas
- [ ] All service methods have explicit return types
- [ ] Mypy passes with strict mode (future)

---

## Phase 1: Core Engine

### 1a: Database Schema ✅ COMPLETE

**Deployed**: 28 JAN 2026

**Connection Details**:
```
Host: rmhpostgres.postgres.database.azure.com
User: rob634
Password: B@lamb634@
Schema: dagapp
```

**Verified**:
| Component | Status | Details |
|-----------|--------|---------|
| Schema | ✅ | `dagapp` created |
| ENUMs | ✅ | 5 types (job_status, node_status, task_status, event_type, event_status) |
| Tables | ✅ | dag_jobs, dag_node_states, dag_task_results, dag_job_events |
| Indexes | ✅ | 18 indexes (PKs + custom) |
| Triggers | ✅ | updated_at triggers on dag_jobs, dag_node_states |

**Deployment Script**: `scripts/deploy_schema.py`

**Rollback** (if needed):
```sql
DROP SCHEMA dagapp CASCADE;
```

---

### 1a-2: Model Updates (From rmhgeoapi Review)

| Task | Status | Description |
|------|--------|-------------|
| Add checkpoint fields to NodeState | ⏳ TODO | checkpoint_phase, checkpoint_data, checkpoint_saved_at |
| Add progress fields to TaskResult | ⏳ TODO | progress_current, progress_total, progress_percent |
| Create OrchestrationInstruction model | ⏳ TODO | For fan-out dynamic task creation |
| Update PydanticToSQL for new fields | ⏳ TODO | Regenerate DDL |
| Deploy schema changes | ⏳ TODO | ALTER TABLE for new columns |

**Files to modify**:
- `core/models/node.py` - Add checkpoint fields
- `core/models/task.py` - Add progress fields
- `core/models/orchestration.py` - NEW: OrchestrationInstruction

---

### 1a-3: GeospatialAsset Integration (V0.8)

**Critical**: The V0.8 design in rmhgeoapi separates Requests from Jobs via GeospatialAsset.

| Task | Status | Description |
|------|--------|-------------|
| Understand V0.8 pattern | ⏳ TODO | Review V0.8_ENTITIES.md in rmhgeoapi |
| dag_geospatial_assets model | ⏳ TODO | First-class entity linking request→asset→job |
| dag_api_requests model | ⏳ TODO | Track external requests (acknowledged immediately) |
| dag_asset_revisions model | ⏳ TODO | Append-only audit log for revisions |
| Integration with dag_jobs | ⏳ TODO | Jobs link to asset via asset_id + current_job_id |

**Key Relationships**:
```
dag_api_requests (N) → dag_geospatial_assets (1) ← (N) dag_jobs
                              │
                              └── current_job_id → dag_jobs
```

**Files to create**:
- `core/models/geospatial_asset.py` - GeospatialAsset, ApprovalState, ClearanceState
- `core/models/api_request.py` - Platform request tracking
- `core/models/asset_revision.py` - Revision history

---

### 1a-4: Additional Tables (Tier 3-7)

| Task | Status | Description |
|------|--------|-------------|
| dag_approvals model | ⏳ TODO | Approval workflow (may merge into GeospatialAsset) |
| dag_blob_assets model | ⏳ TODO | File tracking (renamed from artifacts) |
| dag_external_services model | ⏳ TODO | Callback endpoints (DDH, ADF, webhooks) |
| dag_dataset_refs model | ⏳ TODO | Data lineage tracking |
| dag_promoted model | ⏳ TODO | Externally-published datasets |
| Deploy Tier 2-7 tables | ⏳ TODO | Run deploy_schema.py with new models |

**Files to create**:
- `core/models/approval.py`
- `core/models/blob_asset.py`
- `core/models/external_service.py`
- `core/models/dataset_ref.py`
- `core/models/promoted.py`

---

### 1b: Workflow Loader

| Task | Status | Description |
|------|--------|-------------|
| YAML parser | ⏳ TODO | Load YAML → WorkflowDefinition |
| Validation | ⏳ TODO | Validate structure (START/END nodes, handler refs) |
| Caching | ⏳ TODO | Cache loaded workflows in memory |
| Hot reload | ⏳ TODO | Detect YAML changes, reload |
| Unit tests | ⏳ TODO | Test loader with sample workflows |

**Files to create**:
- `orchestrator/engine/loader.py`
- `tests/test_loader.py`

---

### 1c: Template Resolution

| Task | Status | Description |
|------|--------|-------------|
| Input templates | ⏳ TODO | `{{ inputs.param_name }}` |
| Node output refs | ⏳ TODO | `{{ nodes.validate.output.blob_url }}` |
| Upstream refs | ⏳ TODO | `{{ upstream.output.cog_url }}` (for fan-in) |
| Jinja2 integration | ⏳ TODO | Use Jinja2 for template resolution |
| Unit tests | ⏳ TODO | Test template edge cases |

**Files to create**:
- `orchestrator/engine/templates.py`
- `tests/test_templates.py`

---

### 1d: DAG Evaluator

| Task | Status | Description |
|------|--------|-------------|
| Dependency resolver | ⏳ TODO | Given workflow + node states, find ready nodes |
| Topological sort | ⏳ TODO | Validate DAG has no cycles |
| Ready node detection | ⏳ TODO | All dependencies complete → node is ready |
| Conditional evaluation | ⏳ TODO | Evaluate branch conditions, skip others |
| Fan-out expansion | ⏳ TODO | Create N dynamic nodes from array |
| Fan-in detection | ⏳ TODO | All upstream nodes complete → ready |
| Unit tests | ⏳ TODO | Test complex DAG scenarios |

**Files to create**:
- `orchestrator/engine/evaluator.py`
- `tests/test_evaluator.py`

---

### 1e: Task Dispatcher

| Task | Status | Description |
|------|--------|-------------|
| Service Bus client | ⏳ TODO | Connect to existing namespace |
| Queue routing | ⏳ TODO | Route to `functionapp-tasks` or `container-tasks` |
| Message formatting | ⏳ TODO | TaskMessage → JSON → Service Bus |
| Dispatch tracking | ⏳ TODO | Update NodeState to DISPATCHED |
| Unit tests | ⏳ TODO | Mock Service Bus tests |

**Files to create**:
- `orchestrator/engine/dispatcher.py`
- `tests/test_dispatcher.py`

---

### 1f: Main Orchestrator Loop

| Task | Status | Description |
|------|--------|-------------|
| Poll for work | ⏳ TODO | Find jobs with ready nodes |
| Evaluate DAG | ⏳ TODO | Call evaluator for each job |
| Dispatch ready | ⏳ TODO | Send ready nodes to queues |
| Process results | ⏳ TODO | Poll `dag_task_results`, update NodeState |
| Complete jobs | ⏳ TODO | Detect END node reached, mark job complete |
| Error handling | ⏳ TODO | Handle failures, trigger retries |
| Graceful shutdown | ⏳ TODO | Finish current cycle, then stop |
| Logging | ⏳ TODO | Structured logging for debugging |

**Files to create**:
- `orchestrator/engine/loop.py`
- `orchestrator/main.py`

---

### 1g: Hello World Test

| Task | Status | Description |
|------|--------|-------------|
| Create hello handler | ⏳ TODO | Simple handler that echoes message |
| Submit job | ⏳ TODO | Create job with hello_world workflow |
| Verify execution | ⏳ TODO | Job completes, result captured |
| End-to-end test | ⏳ TODO | Full cycle: submit → orchestrate → execute → complete |

---

### 1h: Handler Registry (From rmhgeoapi)

| Task | Status | Description |
|------|--------|-------------|
| Port registry pattern | ⏳ TODO | @register_task decorator pattern |
| Create base handler interface | ⏳ TODO | HandlerResult, context parameter |
| Startup validation | ⏳ TODO | Fail-fast on duplicate handlers |
| Unit tests | ⏳ TODO | Test registry lookup, validation |

**Files to create**:
- `handlers/__init__.py`
- `handlers/registry.py`
- `handlers/base.py`
- `tests/test_registry.py`

---

### 1i: Configuration Defaults (From rmhgeoapi)

| Task | Status | Description |
|------|--------|-------------|
| TaskRoutingDefaults | ⏳ TODO | Light vs heavy queue routing |
| RasterDefaults | ⏳ TODO | COG settings, tiling thresholds |
| VectorDefaults | ⏳ TODO | Chunking sizes, SRID |
| STACDefaults | ⏳ TODO | STAC version, collection defaults |
| TimeoutDefaults | ⏳ TODO | Per-task-type timeouts |

**Files to create**:
- `core/config/__init__.py`
- `core/config/defaults.py`

---

### 1j: Checkpoint Manager (From rmhgeoapi)

| Task | Status | Description |
|------|--------|-------------|
| Port CheckpointManager | ⏳ TODO | Phase-based checkpoint/resume |
| SIGTERM handler | ⏳ TODO | Graceful shutdown saves checkpoint |
| Artifact validation | ⏳ TODO | Validate before saving checkpoint |
| Unit tests | ⏳ TODO | Test checkpoint save/load/resume |

**Files to create**:
- `worker/checkpoint.py`
- `tests/test_checkpoint.py`

---

### 1k: Progress Tracking

| Task | Status | Description |
|------|--------|-------------|
| Progress reporter | ⏳ TODO | Worker reports progress to DB |
| Job progress aggregation | ⏳ TODO | Aggregate node progress to job level |
| ETA calculation | ⏳ TODO | Estimate completion time |
| API endpoint | ⏳ TODO | GET /api/dag/jobs/{id}/progress |

**Files to create**:
- `worker/progress.py`
- `orchestrator/progress_aggregator.py`

---

### 1l: Infrastructure Clients

| Task | Status | Description |
|------|--------|-------------|
| Database repository | ⏳ TODO | Job, Node, Task CRUD operations |
| Service Bus client | ⏳ TODO | Send/receive with singleton pattern |
| Blob storage client | ⏳ TODO | Upload, download, copy, delete |
| PgSTAC repository | ⏳ TODO | STAC item CRUD via OGC API |
| External service client | ⏳ TODO | Callback triggers to DDH/ADF |
| Connection pooling | ⏳ TODO | PostgreSQL connection pool |

**Files to create**:
- `infrastructure/__init__.py`
- `infrastructure/database.py`
- `infrastructure/service_bus.py`
- `infrastructure/blob_storage.py`
- `infrastructure/pgstac.py`
- `infrastructure/external_services.py`
- `infrastructure/connection_pool.py`

---

### 1m: Observability (From rmhgeoapi)

| Task | Status | Description |
|------|--------|-------------|
| Structured logging | ⏳ TODO | LoggerFactory with component types |
| Application Insights | ⏳ TODO | Azure Monitor OpenTelemetry |
| Checkpoint logging | ⏳ TODO | Named checkpoints for AI queries |
| Memory/CPU tracking | ⏳ TODO | Resource usage monitoring |
| Metrics export | ⏳ TODO | Per-job metrics to blob or DB |

**Files to create**:
- `core/logging.py`
- `core/observability.py`
- `infrastructure/metrics.py`

---

## Phase 2: Worker Integration

**Goal**: Existing rmhgeoapi workers can execute DAG tasks

| Task | Status | Description |
|------|--------|-------------|
| Define task format | ⏳ TODO | Document JSON schema for DAG tasks |
| Worker detection | ⏳ TODO | Workers detect DAG vs legacy task |
| Result reporting | ⏳ TODO | Workers write to `dagapp.dag_task_results` |
| Dual-mode workers | ⏳ TODO | Support both legacy and DAG (no breaking changes) |
| Function App changes | ⏳ TODO | Modify `rmhazuregeoapi` for DAG support |
| Docker Worker changes | ⏳ TODO | Modify `rmhheavyapi` for DAG support |
| End-to-end test | ⏳ TODO | Real task execution via workers |

**Message Format (DAG)**:
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

**Worker Result Reporting**:
```python
# Worker writes this after execution
INSERT INTO dagapp.dag_task_results (
    task_id, job_id, node_id, status, output, worker_id, reported_at
) VALUES ($1, $2, $3, $4, $5, $6, NOW())
```

---

## Phase 3: Real Workflows

**Goal**: Production workflows running on DAG

| Task | Status | Description |
|------|--------|-------------|
| Raster workflow | ⏳ TODO | Port `process_raster_docker` to YAML |
| Conditional routing | ⏳ TODO | Size-based routing (FA/Docker memory/Docker mount) |
| Vector workflow | ⏳ TODO | Port `vector_docker_etl` to YAML |
| Fan-out support | ⏳ TODO | Tiled raster with N parallel tasks |
| FATHOM workflow | ⏳ TODO | Complex multi-stage flood data processing |
| Template resolution | ⏳ TODO | All `{{ }}` expressions work |
| Error handling | ⏳ TODO | Failed tasks retry correctly |
| Timeout detection | ⏳ TODO | Stuck tasks detected within 60 seconds |

**Workflows to Port**:
| Legacy Job Class | New YAML | Complexity |
|------------------|----------|------------|
| `ProcessRasterDocker` | `raster_processing.yaml` | Conditional routing |
| `ProcessVectorDocker` | `vector_etl.yaml` | Linear with fan-out |
| `FathomFloodProcess` | `fathom_flood.yaml` | Complex multi-stage |

---

## Phase 4: API Integration

**Goal**: External API uses DAG orchestrator

| Task | Status | Description |
|------|--------|-------------|
| FastAPI setup | ⏳ TODO | Create `orchestrator/api/` module |
| Submit endpoint | ⏳ TODO | `POST /api/dag/submit` |
| Status endpoint | ⏳ TODO | `GET /api/dag/jobs/{id}` |
| Node status | ⏳ TODO | `GET /api/dag/jobs/{id}/nodes` |
| Cancel endpoint | ⏳ TODO | `POST /api/dag/jobs/{id}/cancel` |
| Health endpoint | ⏳ TODO | `GET /health` |
| Platform integration | ⏳ TODO | DDH → DAG routing |
| Backward compat | ⏳ TODO | Legacy endpoints still work |

**API Endpoints**:
```
POST /api/dag/submit
  Body: { workflow_id, inputs }
  Returns: { job_id }

GET /api/dag/jobs/{job_id}
  Returns: { job_id, status, created_at, completed_at, ... }

GET /api/dag/jobs/{job_id}/nodes
  Returns: [ { node_id, status, output, ... }, ... ]

POST /api/dag/jobs/{job_id}/cancel
  Returns: { success: true }
```

---

## Phase 5: Migration & Cutover

**Goal**: DAG is primary, legacy deprecated

| Task | Status | Description |
|------|--------|-------------|
| Traffic migration | ⏳ TODO | Route new jobs to DAG |
| Legacy deprecation | ⏳ TODO | Mark CoreMachine deprecated |
| Monitoring | ⏳ TODO | Alerts for orchestrator health |
| Documentation | ⏳ TODO | Update all docs |
| Team training | ⏳ TODO | Knowledge transfer |
| Cleanup (optional) | ⏳ TODO | Remove legacy code |

---

## Future Features (Post-Launch)

| Feature | Priority | Description |
|---------|----------|-------------|
| Web UI | HIGH | Visualize DAGs, view job runs |
| Sub-workflows | MEDIUM | DAG node runs another DAG |
| Human approval | MEDIUM | Pause for manual approval |
| SLA monitoring | MEDIUM | Alert on slow workflows |
| Workflow versioning | LOW | Run old version during testing |
| Visual editor | LOW | Build workflows in browser |
| Plugin marketplace | FAR FUTURE | Pre-built nodes for common tasks |
| Custom handlers | FAR FUTURE | User-uploaded handlers (sandboxed) |

---

## Success Criteria

### MVP (Phase 3 Complete)

- [ ] Orchestrator runs in Azure Web App
- [ ] Can submit raster workflow via API
- [ ] Conditional routing works (size-based)
- [ ] Fan-out works (tiled rasters)
- [ ] Failures detected within 60 seconds (not 24 hours)
- [ ] No "last task turns out lights" pattern
- [ ] Existing workers process DAG tasks

### Production Ready (Phase 5 Complete)

- [ ] All workflows migrated to DAG
- [ ] DDH integration works
- [ ] Monitoring and alerting configured
- [ ] Documentation complete
- [ ] Legacy CoreMachine deprecated
- [ ] Team trained on new system

---

## Handler Library (Existing in rmhgeoapi)

These handlers exist in rmhgeoapi and can be called from DAG workflows:

```
RASTER HANDLERS (rmhgeoapi - ready to use)
├── raster_validate              # CRS and format detection
├── raster_process_complete      # Consolidated Docker (validate→COG→STAC)
├── create_cog                   # GeoTIFF creation with compression
└── extract_stac_metadata        # STAC item generation

VECTOR HANDLERS (rmhgeoapi - ready to use)
├── vector_prepare               # Load, validate, chunk
├── vector_upload_chunk          # DELETE+INSERT for idempotency
├── vector_create_stac           # STAC item registration
└── vector_docker_complete       # Consolidated Docker handler

FATHOM ETL HANDLERS (rmhgeoapi - ready to use)
├── fathom_tile_inventory        # Query unprocessed tiles
├── fathom_band_stack            # Stack 8 return periods into COG
├── fathom_grid_inventory        # Query Phase 1→Phase 2 pending
├── fathom_spatial_merge         # Merge tiles band-by-band
└── fathom_stac_register         # STAC collection/item creation

H3 AGGREGATION HANDLERS (rmhgeoapi - ready to use)
├── h3_inventory                 # List H3 hexagon boundaries
├── h3_raster_zonal              # Zonal statistics computation
├── h3_register_dataset          # Register results to STAC
├── h3_export_dataset            # Export to cloud storage
└── h3_finalize                  # Aggregate pyramid results

STAC/CATALOG HANDLERS (rmhgeoapi - ready to use)
├── list_raster_files            # Enumerate container files
├── extract_stac_metadata        # Bulk STAC extraction
├── stac_catalog_container       # Build STAC catalog from container
├── rebuild_stac                 # Rebuild catalog from corrupted state
└── repair_stac_items            # Fix malformed STAC items

APPROVAL/PUBLISHING HANDLERS (rmhgeoapi - ready to use)
├── trigger_approval             # Create approval workflow
├── approve_dataset              # Mark approved, trigger ADF
├── reject_dataset               # Mark rejected
└── promote_to_external          # Publish PUBLIC data

MAINTENANCE HANDLERS (rmhgeoapi - ready to use)
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

**Build Your Own Pipeline**:
```yaml
# User-defined workflow assembled from handlers
workflow_id: my_custom_pipeline
nodes:
  validate:
    handler: raster_validate
    next: reproject
  reproject:
    handler: raster_reproject      # Reusable!
    params:
      target_crs: "EPSG:3857"
    next: clip
  clip:
    handler: raster_clip           # Reusable!
    params:
      boundary: "{{ inputs.aoi }}"
    next: analyze
  analyze:
    handler: stats_zonal           # Reusable!
    params:
      zones: "{{ inputs.zones }}"
    next: END
```

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
      - name: Deploy all apps (same image, different env vars)
        run: |
          az webapp config container set --name dag-orchestrator --resource-group $RG --docker-custom-image-name rmhgeoregistry.azurecr.io/rmhdagmaster:${{ github.sha }}
          az webapp config container set --name dag-worker-light --resource-group $RG --docker-custom-image-name rmhgeoregistry.azurecr.io/rmhdagmaster:${{ github.sha }}
          az webapp config container set --name dag-worker-heavy --resource-group $RG --docker-custom-image-name rmhgeoregistry.azurecr.io/rmhdagmaster:${{ github.sha }}
```

### Deployment Configuration

| App | RUN_MODE | WORKER_TYPE | WORKER_QUEUE | Instances |
|-----|----------|-------------|--------------|-----------|
| dag-orchestrator | orchestrator | - | - | 1 (always on) |
| dag-worker-light | worker | functionapp | functionapp-tasks | 0-10 (auto) |
| dag-worker-heavy | worker | docker | container-tasks | 1-5 |

---

## Blocked Items

*(none currently)*

---

## References

- `/Users/robertharrison/python_builds/rmhgeoapi/EPOCH_5.md` - Full architecture plan
- `/Users/robertharrison/python_builds/rmhgeoapi/docs_claude/DAG_SPIKE.md` - DAG concepts
- `/Users/robertharrison/python_builds/rmhgeoapi/CLAUDE.md` - rmhgeoapi context
