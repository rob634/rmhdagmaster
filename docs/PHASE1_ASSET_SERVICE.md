# Phase 1: GeospatialAssetService — Implementation Plan

**Created**: 09 FEB 2026
**Status**: Phase 4A + 4B COMPLETE (11 FEB 2026) — Phase 4C next
**Tracks**: Tier 4 domain service layer

---

## Overview

The `GeospatialAssetService` is the coordination layer between the domain model (Platform, GeospatialAsset, AssetVersion) and the DAG execution engine (JobService). It encodes all business rules for asset submission, processing callbacks, approval, clearance, and version resolution.

**Pattern**: Follows existing service conventions — constructor injection of `AsyncConnectionPool`, repos instantiated in `__init__`, async methods, optional `EventService` for observability.

---

## Dependencies

```
GeospatialAssetService
├── PlatformRepository      (validate platform, check required_refs)
├── AssetRepository          (get-or-create asset, clearance, soft delete)
├── AssetVersionRepository   (create version, update state, resolve latest)
├── JobService               (create DAG job for processing)
└── EventService (optional)  (emit domain events)
```

---

## Files

### New Files (2)

| # | File | Purpose |
|---|------|---------|
| 1 | `services/asset_service.py` | GeospatialAssetService — all business logic |
| 2 | `tests/test_asset_service.py` | Unit tests with mocked repos |

### Modified Files (2)

| # | File | Change |
|---|------|--------|
| 3 | `services/__init__.py` | Export GeospatialAssetService |
| 4 | `repositories/version_repo.py` | Add `get_by_job_id()` method |

### Wiring (deferred — no changes now)

| File | Change | When |
|------|--------|------|
| `main.py` | Instantiate service, inject into routes | Phase 2 (REST API) |
| `orchestrator/loop.py` | Call `on_processing_*` callbacks | Phase 3 (DAG wiring) |

---

## Step 1: Add `get_by_job_id` to AssetVersionRepository -- DONE

- [x] **File**: `repositories/version_repo.py`
- [x] **Method**: `get_by_job_id(job_id: str) -> Optional[AssetVersion]`
- [x] Queries: `WHERE current_job_id = %s LIMIT 1`
- [x] Needed by processing callbacks (DAG job completes → find which version it belongs to)

---

## Step 2: Create GeospatialAssetService -- DONE

- [x] **File**: `services/asset_service.py` (~290 lines)

### Constructor

```python
class GeospatialAssetService:
    def __init__(
        self,
        pool: AsyncConnectionPool,
        job_service: JobService,
        event_service: EventService = None,
    ):
        self.pool = pool
        self.platform_repo = PlatformRepository(pool)
        self.asset_repo = AssetRepository(pool)
        self.version_repo = AssetVersionRepository(pool)
        self.job_service = job_service
        self._event_service = event_service
```

### Methods

#### 2a: `submit_asset` — DONE

- [x] Validates platform, computes asset_id, get-or-create, version ordering, creates version + job

#### 2b: Processing callbacks — DONE

- [x] `on_processing_completed(job_id, artifacts)` — finds version by job_id, writes artifacts, transitions state
- [x] `on_processing_failed(job_id, error_message)` — finds version, writes error, transitions state
- [x] Returns None for standalone jobs (not tied to an asset)

#### 2c: Approval workflow — DONE

- [x] `approve_version` / `reject_version` — with optimistic locking

#### 2d: Clearance workflow — DONE

- [x] `mark_cleared` (UNCLEARED → OUO) / `mark_public` (OUO → PUBLIC)

#### 2e: Reprocessing — DONE

- [x] `reprocess_version` — increments revision, creates new DAG job

#### 2f: Query methods — DONE

- [x] `get_asset`, `get_version`, `get_latest_version`, `list_assets`, `list_versions`, `soft_delete_asset`

---

## Step 3: Export from services module -- DONE

- [x] **File**: `services/__init__.py`
- [x] Added import and `__all__` entry for `GeospatialAssetService`

---

## Step 4: Unit Tests -- DONE

- [x] **File**: `tests/test_asset_service.py` — 24 tests, all passing
- [x] Pattern: Mock repos, test business logic in isolation

All 24 planned test cases implemented: submit (6), callbacks (4), approval (4), clearance (4), reprocessing (3), queries (3).

---

## Prerequisites -- ALL MET

- [x] **FunctionRepository write support**: `execute_write()` + `execute_write_returning()` added to `function/repositories/base.py`. Done 11 FEB.
- [x] **Gateway config aligned**: `function/config.py` aligned with orchestrator env vars (`POSTGRES_*`), delegates to `infrastructure/auth` for managed identity. Done 11 FEB.
- [x] **Gateway RBAC**: System-assigned identity for Service Bus Data Sender, UMI for PostgreSQL read/write. Done 11 FEB.
- [ ] **Gateway deployment**: Function App settings configured but no functions published yet. Needs `func azure functionapp publish rmhdaggateway`.

---

## Verification -- PASSED

1. `pytest tests/test_asset_service.py -v` — 24 tests pass
2. `pytest tests/ -v` — 158 tests pass (134 existing + 24 new)
3. No changes to main.py; orchestrator only changed to pass `asset_id` through job creation path

---

---

## Phase 4B: Gateway Business API -- DONE (11 FEB 2026)

### New Files (3)

| # | File | Purpose |
|---|------|---------|
| 1 | `function/repositories/asset_query_repo.py` | Sync CRUD for gateway (platform, asset, version) |
| 2 | `function/blueprints/platform_bp.py` | B2B submit + status polling |
| 3 | `function/blueprints/asset_bp.py` | Asset queries, approval, clearance, soft delete (submit removed) |

### Modified Files (8)

| # | File | Change |
|---|------|--------|
| 1 | `core/models/job.py` | Added `asset_id: Optional[str]` + index |
| 2 | `core/models/job_queue_message.py` | Added `asset_id: Optional[str]` |
| 3 | `repositories/job_repo.py` | Added `asset_id` to create/create_with_owner/_row_to_job |
| 4 | `services/job_service.py` | Added `asset_id` param to `create_job()` |
| 5 | `orchestrator/loop.py` | Passes `message.asset_id` to `create_job()` |
| 6 | `function/repositories/job_query_repo.py` | Added `asset_id` to SELECT queries |
| 7 | `function/models/requests.py` | Added `request_id` to AssetSubmitRequest |
| 8 | `function/models/responses.py` | Added `request_id` to AssetSubmitResponse |

### Key Design: correlation_id vs asset_id

- `correlation_id = request_id` — B2B clients poll `GET /api/platform/status/{request_id}`
- `asset_id = computed_hash` — job→asset tracing via `WHERE asset_id = %s`
- Standalone jobs (no asset): `asset_id = NULL`, no impact

---

## Phase 4C: DAG Callback Wiring -- NOT STARTED

### What It Does

When a DAG job completes or fails, the orchestrator calls `asset_service.on_processing_completed/failed` to update the asset version with artifacts or error state.

### Hook Point

`orchestrator/loop.py:_check_job_completion()` (line ~1198):

```
COMPLETED → asset_service.on_processing_completed(job_id, result_data)
FAILED    → asset_service.on_processing_failed(job_id, error_message)
```

The service methods already exist and are tested. The wiring requires:

1. Instantiate `GeospatialAssetService` in the Orchestrator constructor (needs pool + job_service)
2. After `job_service.complete_job()` → call `asset_service.on_processing_completed(job.job_id, result_data)`
3. After `job_service.fail_job()` → call `asset_service.on_processing_failed(job.job_id, error)`
4. Both return `Optional[AssetVersion]` — None for standalone jobs (no-op, no crash)

### Files to Modify (2)

| File | Change |
|------|--------|
| `orchestrator/loop.py` | Import + instantiate GeospatialAssetService; call callbacks in `_check_job_completion()` |
| `tests/test_integration.py` | Add test for callback wiring (mock asset_service, verify calls) |

---

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| Service takes `JobService` not `JobRepository` | Job creation is multi-step (creates nodes, emits events) — reuse existing logic |
| Processing callbacks return `Optional` | Not all DAG jobs are tied to assets (standalone jobs still work) |
| Model validates state transitions | Service just calls model methods; ValueError bubbles up naturally |
| No main.py wiring yet | Service is testable standalone; wiring happens in Phase 2 (API) and Phase 3 (DAG callbacks) |
| `get_by_job_id` on version repo | Only way to find which version a DAG job belongs to when processing completes |
