# Phase 1: GeospatialAssetService — Implementation Plan

**Created**: 09 FEB 2026
**Status**: Planning
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

## Step 1: Add `get_by_job_id` to AssetVersionRepository

- [ ] **File**: `repositories/version_repo.py`
- [ ] **Method**: `get_by_job_id(job_id: str) -> Optional[AssetVersion]`
- [ ] Queries: `WHERE current_job_id = %s LIMIT 1`
- [ ] Needed by processing callbacks (DAG job completes → find which version it belongs to)

---

## Step 2: Create GeospatialAssetService

- [ ] **File**: `services/asset_service.py`

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

#### 2a: `submit_asset` — The main entry point

- [ ] **Signature**: `async def submit_asset(platform_id, platform_refs, data_type, version_label, workflow_id, input_params, submitted_by=None) -> Tuple[GeospatialAsset, AssetVersion, Job]`
- [ ] Validate platform exists and is active → `ValueError` if not
- [ ] Validate platform_refs against `platform.required_refs` → `ValueError` if missing
- [ ] Compute `asset_id` via `GeospatialAsset.compute_asset_id()`
- [ ] Get-or-create asset via `asset_repo.get_or_create()`
- [ ] Check no unresolved versions via `version_repo.has_unresolved_versions()` → `ValueError` if pending
- [ ] Get next ordinal via `version_repo.get_next_ordinal()`
- [ ] Create AssetVersion (approval=PENDING_REVIEW, processing=QUEUED)
- [ ] Create DAG job via `job_service.create_job(workflow_id, input_params, submitted_by)`
- [ ] Update version with `current_job_id` and mark processing started
- [ ] Return `(asset, version, job)`

#### 2b: Processing callbacks (DAG → domain)

- [ ] `async def on_processing_completed(job_id, artifacts: dict) -> Optional[AssetVersion]`
  - Find version by `version_repo.get_by_job_id(job_id)`
  - Return None if no version found (job not tied to an asset)
  - Write artifact fields: `blob_path`, `table_name`, `stac_item_id`, `stac_collection_id`
  - Call `version.mark_processing_completed()`
  - Save via `version_repo.update()`

- [ ] `async def on_processing_failed(job_id, error_message) -> Optional[AssetVersion]`
  - Find version by job_id
  - Return None if not found
  - Call `version.mark_processing_failed(error_message)`
  - Save via `version_repo.update()`

#### 2c: Approval workflow

- [ ] `async def approve_version(asset_id, version_ordinal, reviewer) -> AssetVersion`
  - Get version → `ValueError` if not found
  - Call `version.mark_approved(reviewer)` (model validates state transition)
  - Save via `version_repo.update()` → raise on version conflict

- [ ] `async def reject_version(asset_id, version_ordinal, reviewer, reason) -> AssetVersion`
  - Get version → `ValueError` if not found
  - Call `version.mark_rejected(reviewer, reason)`
  - Save via `version_repo.update()`

#### 2d: Clearance workflow

- [ ] `async def mark_cleared(asset_id, actor) -> GeospatialAsset`
  - Get asset → `ValueError` if not found
  - Call `asset.mark_cleared(actor)` (model validates UNCLEARED → OUO)
  - Save via `asset_repo.update()`

- [ ] `async def mark_public(asset_id, actor) -> GeospatialAsset`
  - Get asset → `ValueError` if not found
  - Call `asset.mark_public(actor)` (model validates OUO → PUBLIC)
  - Save via `asset_repo.update()`

#### 2e: Reprocessing

- [ ] `async def reprocess_version(asset_id, version_ordinal, workflow_id, input_params, submitted_by=None) -> Tuple[AssetVersion, Job]`
  - Get version → `ValueError` if not found
  - Call `version.start_reprocessing(job_id=None)` (increments revision, resets fields)
  - Create new DAG job via `job_service.create_job()`
  - Update version with new `current_job_id`
  - Save via `version_repo.update()`
  - Return `(version, job)`

#### 2f: Query methods

- [ ] `async def get_asset(asset_id) -> Optional[GeospatialAsset]`
- [ ] `async def get_version(asset_id, version_ordinal) -> Optional[AssetVersion]`
- [ ] `async def get_latest_version(asset_id) -> Optional[AssetVersion]`
  - Calls `version_repo.get_latest_approved(asset_id)`
  - This is the `version=latest` resolution for service URLs
- [ ] `async def list_assets(platform_id, include_deleted=False) -> List[GeospatialAsset]`
- [ ] `async def list_versions(asset_id) -> List[AssetVersion]`
- [ ] `async def soft_delete_asset(asset_id, actor) -> bool`

---

## Step 3: Export from services module

- [ ] **File**: `services/__init__.py`
- [ ] Add import and `__all__` entry for `GeospatialAssetService`

---

## Step 4: Unit Tests

- [ ] **File**: `tests/test_asset_service.py`
- [ ] Pattern: Mock the repos, test business logic in isolation

### Test Cases (~20 tests)

**Submit asset:**
- [ ] Happy path: new platform + new asset + first version → returns (asset, version, job)
- [ ] Idempotent: same platform_refs → same asset_id, new version ordinal
- [ ] Platform not found → ValueError
- [ ] Platform inactive → ValueError
- [ ] Missing required refs → ValueError
- [ ] Unresolved version exists → ValueError (version ordering)

**Processing callbacks:**
- [ ] on_processing_completed writes artifacts and transitions state
- [ ] on_processing_failed writes error and transitions state
- [ ] on_processing_completed with unknown job_id → returns None (no-op)

**Approval:**
- [ ] approve_version transitions PENDING_REVIEW → APPROVED
- [ ] reject_version transitions PENDING_REVIEW → REJECTED with reason
- [ ] approve already rejected → ValueError (from model)
- [ ] version not found → ValueError

**Clearance:**
- [ ] mark_cleared transitions UNCLEARED → OUO
- [ ] mark_public transitions OUO → PUBLIC
- [ ] mark_public from UNCLEARED → ValueError (from model)

**Reprocessing:**
- [ ] reprocess increments revision, creates new job
- [ ] reprocess from QUEUED → ValueError (not terminal)

**Queries:**
- [ ] get_latest_version returns highest approved ordinal
- [ ] soft_delete_asset marks deleted

---

## Verification

1. `pytest tests/test_asset_service.py -v` — new tests pass
2. `pytest tests/ -v` — all existing tests still pass (130+)
3. No changes to main.py or orchestrator yet — service is standalone

---

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| Service takes `JobService` not `JobRepository` | Job creation is multi-step (creates nodes, emits events) — reuse existing logic |
| Processing callbacks return `Optional` | Not all DAG jobs are tied to assets (standalone jobs still work) |
| Model validates state transitions | Service just calls model methods; ValueError bubbles up naturally |
| No main.py wiring yet | Service is testable standalone; wiring happens in Phase 2 (API) and Phase 3 (DAG callbacks) |
| `get_by_job_id` on version repo | Only way to find which version a DAG job belongs to when processing completes |
