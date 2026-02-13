# Workflow Version Pinning — Implementation Plan

**Created**: 11 FEB 2026
**Completed**: 11 FEB 2026
**Status**: Complete — E2E verified in Azure (v0.1.15.0)
**Tracks**: Tier 5 — Hardening (Version Pinning)

---

## Context

**Problem**: The orchestrator calls `self.workflow_service.get(job.workflow_id)` every cycle (`loop.py:411`, `loop.py:438`). If the YAML on disk changes and the service reloads, in-flight jobs silently pick up the new definition — wrong routing, missing nodes, template errors.

**Goal**: Pin the workflow definition at job creation time so (a) in-flight jobs are immune to YAML changes, and (b) every job's output carries provenance — which workflow, which version, when.

**Approach**: Two new fields on `Job`:
- `workflow_version` (INT, NOT NULL) — first-class, queryable, indexed
- `workflow_snapshot` (JSONB, NOT NULL) — full serialized `WorkflowDefinition`, used by the orchestrator instead of live cache

Schema is rebuilt from scratch via PydanticToSQL — no migration path needed.

---

## Files to Modify (7)

| # | File | Change | Status |
|---|------|--------|--------|
| 1 | `core/models/job.py` | Add `workflow_version`, `workflow_snapshot` fields + `get_pinned_workflow()` method | [x] |
| 2 | `repositories/job_repo.py` | Update `create()`, `create_with_owner()`, `_row_to_job()` to persist/read new fields | [x] |
| 3 | `services/job_service.py` | Capture version + snapshot at job creation in `create_job()` | [x] |
| 4 | `services/event_service.py` | Add `workflow_version` to `emit_job_created()` event data | [x] |
| 5 | `orchestrator/loop.py` | Add `_get_workflow_for_job()` helper, replace 2 lookup calls, enrich `_aggregate_results()` | [x] |
| 6 | `function/repositories/job_query_repo.py` | Add `workflow_version` to SELECT lists | [x] |
| 7 | `tests/test_integration.py` | Add version pinning tests + fix existing tests for new required fields | [x] |

---

## Step 1: Job Model — Add Fields

**File**: `core/models/job.py`

After `metadata` field (~line 85), add:

```python
# Workflow version pinning (captured at job creation, immutable)
workflow_version: int = Field(
    ...,
    ge=1,
    description="Workflow version pinned at job creation time"
)
workflow_snapshot: Dict[str, Any] = Field(
    ...,
    description="Serialized WorkflowDefinition captured at job creation (immutable)"
)
```

Add convenience method to the class:

```python
def get_pinned_workflow(self) -> "WorkflowDefinition":
    """Deserialize the pinned workflow snapshot."""
    from core.models.workflow import WorkflowDefinition
    return WorkflowDefinition.model_validate(self.workflow_snapshot)
```

Both fields are required (`...`) — every job must have a pinned workflow.

---

## Step 2: Job Repository — Persist + Read

**File**: `repositories/job_repo.py`

### 2a: `create()` (line 48-81)

Add to INSERT column list: `workflow_version, workflow_snapshot`
Add to VALUES: `%(workflow_version)s, %(workflow_snapshot)s`
Add to params dict:
```python
"workflow_version": job.workflow_version,
"workflow_snapshot": Json(job.workflow_snapshot),
```

### 2b: `create_with_owner()` (line 369-424)

Same as 2a — add the two columns and params.

### 2c: `_row_to_job()` (line 340-363)

Add to the Job constructor:
```python
workflow_version=row["workflow_version"],
workflow_snapshot=row["workflow_snapshot"],
```

### 2d: `update()` — NO CHANGE

These fields are immutable after creation. The existing UPDATE does not include them.

---

## Step 3: JobService — Capture at Creation

**File**: `services/job_service.py`

In `create_job()` (line 97-105), update the Job constructor:

```python
job = Job(
    job_id=job_id,
    workflow_id=workflow_id,
    workflow_version=workflow.version,                  # NEW
    workflow_snapshot=workflow.model_dump(mode="json"),  # NEW
    status=JobStatus.PENDING,
    input_params=input_params,
    submitted_by=submitted_by,
    correlation_id=correlation_id,
)
```

`model_dump(mode="json")` serializes enums as strings, datetimes as ISO — safe for JSONB. A typical workflow is ~2-3 KB.

---

## Step 4: EventService — Add Version to JOB_CREATED

**File**: `services/event_service.py`

In `emit_job_created()` (line 131-135), add `workflow_version` to the data dict:

```python
data={
    "workflow_id": workflow_id,
    "workflow_version": job.workflow_version,  # NEW
    "submitted_by": job.submitted_by,
    "correlation_id": job.correlation_id,
},
```

---

## Step 5: Orchestrator Loop — Use Pinned Snapshot

**File**: `orchestrator/loop.py`

### 5a: Add helper method

```python
def _get_workflow_for_job(self, job: Job) -> WorkflowDefinition:
    """Get the pinned workflow definition for a job."""
    return job.get_pinned_workflow()
```

### 5b: Replace 2 lookup calls

| Location | Line | Change |
|----------|------|--------|
| `_process_results()` | ~411 | `self.workflow_service.get(job.workflow_id)` → `self._get_workflow_for_job(job)` |
| `_process_job()` | ~438 | `self.workflow_service.get(job.workflow_id)` → `self._get_workflow_for_job(job)` |

Remove the null checks that follow each call (the snapshot is NOT NULL — always present).

**Do NOT change** the call in `_handle_job_submission()` (~line 277) — that validates the workflow exists before creating the job, correctly using the live cache.

### 5c: Enrich `_aggregate_results()` with provenance

In `_aggregate_results()` (line 1237-1255), after building the results dict, add:

```python
# Add provenance metadata
job = await self._job_repo.get(job_id)
if job:
    results["_provenance"] = {
        "workflow_id": job.workflow_id,
        "workflow_version": job.workflow_version,
        "job_id": job_id,
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }
```

The `_provenance` key uses a leading underscore — node IDs never start with underscore in workflow YAML.

---

## Step 6: Function App Query Repo

**File**: `function/repositories/job_query_repo.py`

Add `workflow_version` to the SELECT in `get_job()` and `get_job_by_correlation_id()`. Do NOT add `workflow_snapshot` — it's large and only needed by the orchestrator.

---

## Step 7: Tests

**File**: `tests/test_integration.py`

1. **Snapshot round-trip**: Create a `WorkflowDefinition`, serialize via `model_dump(mode="json")`, create a `Job` with the snapshot, call `get_pinned_workflow()`, verify all fields match
2. **Existing tests**: Fix any test that constructs `Job` objects directly (new required fields). Use `model_construct()` where appropriate.

---

## Schema Impact

PydanticToSQL picks up new fields automatically on `POST /api/v1/bootstrap/rebuild`:

- `workflow_version INTEGER NOT NULL`
- `workflow_snapshot JSONB NOT NULL`

---

## Verification

All checks passed on 11 FEB 2026:

1. [x] `pytest tests/ -v` — 134 tests pass (93 existing + 4 new version pinning + 37 others)
2. [x] Schema rebuild via `POST /api/v1/bootstrap/rebuild?confirm=DESTROY` — 70 statements, 0 errors
3. [x] Echo job submitted — `workflow_version: 1` and `workflow_snapshot` populated in DB
4. [x] `result_data._provenance` confirmed: `{"workflow_id": "echo_test", "workflow_version": 1, "job_id": "...", "completed_at": "..."}`
5. [x] JOB_CREATED event includes `workflow_version` in `event_data`

Deployed as v0.1.15.0 to Azure (orchestrator + worker).
