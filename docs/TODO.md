# TODO - rmhdagmaster

**Last Updated**: 12 FEB 2026

> Detailed specs, code examples, and task checklists: see `IMPLEMENTATION.md`.
> System design and architecture: see `ARCHITECTURE.md`.

---

## Progress Overview

| Phase | Status | Description | IMPL Reference |
|-------|--------|-------------|----------------|
| 0: Project Setup | Done | Repo, models, schema generator | Appendix A |
| 0.5: Repository Refactor | Done | Async base, pooling, auto-Json | Appendix A |
| 0.7: Critical Fixes (P0) | Done | Event logging, retry logic, stale state bug | Appendix A |
| 0.8: Concurrency Control | Done | Advisory locks, optimistic locking, version columns | Sec 12 |
| 0.8.1: Lease-Based Locking | Done | Lease+pulse for crash recovery | Sec 12 |
| 0.9: Multi-Orchestrator | Done | Queue-based distribution, ownership, heartbeat | Sec 13 |
| 0.9.5: Function App Gateway | Done | Azure Functions gateway (22 endpoints) | Sec 1 |
| 1.5: Conditional + Checkpoint | Done | Branch routing, checkpointing | Sec 3 |
| Tier 1: E2E Execution | Done | Worker integration, template resolution, real workflows | Sec 5-7 |
| Tier 2: Parallel Processing | Done | Fan-out, fan-in, dynamic child nodes | Sec 3.1-3.2 |
| Tier 3: Retry & Timeout | Done | Timeout detection, retry wiring, blob SAS, raster validate | Sec 2 |

---

## What's Done

- Project foundation (models, schema, repos, connections)
- Orchestrator loop with heartbeat, orphan recovery, event logging
- Retry logic, optimistic locking, lease-based crash recovery
- Multi-orchestrator competing consumers architecture
- Function App gateway (22 endpoints: submit, status, admin, proxy)
- Conditional routing (size-based, branch skipping, any_of deps)
- Checkpointing (save/resume long-running tasks, SIGTERM handling)
- Timeline API (event-based audit trail)
- Orchestrator stats endpoint
- Jinja2 template engine wired into dispatch path (`{{ inputs.x }}`, `{{ nodes.y.output.z }}`)
- Fan-out: dynamic child node creation from arrays, batch dispatch
- Fan-in: aggregation of child results (collect, concat, sum, first, last)
- No-defaults policy: all queue names require explicit env vars (no silent fallbacks)
- CLI test tool (`tools/submit_job.py`) for queue-based job submission
- Echo test verified end-to-end in Azure via queue submission (v0.10.0)
- Fan-out/fan-in test verified end-to-end in Azure (3-item fan-out, 8 nodes, aggregation)
- Timeout detection: `_check_stuck_nodes()` scans DISPATCHED/RUNNING nodes per-cycle
- Retry wiring: RetryPolicy from workflow YAML applied at node creation, auto-retry on failure
- Flaky echo handler for retry testing (configurable failure rate)
- READY → FAILED state transition for pre-dispatch errors (template failures)
- Blob SAS delegation: managed identity generates ephemeral SAS tokens (no account keys)
- Raster validate handler with `_resolve_blob_path()` for container/blob_path → SAS URL
- Raster validate E2E verified in Azure: small tile + 11.3 GB WorldView-2 multispectral (8 bands, 28862x24381, uint16)
- Worker RBAC: Storage Blob Data Reader + Storage Blob Delegator assigned
- Deploy script (`scripts/deploy.sh`) for ACR build + container config + restart
- Workflow version pinning: `workflow_version` (INT) + `workflow_snapshot` (JSONB) captured at job creation
- Provenance metadata: `result_data._provenance` on every completed job (workflow_id, version, job_id, completed_at)
- In-flight jobs immune to YAML changes (orchestrator uses pinned snapshot, not live cache)
- Function App gateway config aligned with orchestrator env vars (`POSTGRES_*`, `SERVICE_BUS_FQDN`)
- Gateway managed identity auth: delegates to `infrastructure/auth` for UMI PostgreSQL access
- Function App RBAC configured: system-assigned identity for Service Bus, UMI for PostgreSQL
- GeospatialAssetService: business rules (submit, approve, reject, clearance, reprocess, callbacks) — 24 tests
- Asset read-only query endpoints on gateway (get, list, versions, latest, platforms)
- B2B status polling: `GET /api/platform/status/{request_id}` via `correlation_id`
- `asset_id` field on `dag_jobs`: nullable, indexed — links jobs to geospatial assets (standalone jobs have NULL)
- 158 unit tests across 6 test files (v0.16.0)

---

## Implementation Priority

### Tier 1: End-to-End Execution -- DONE (06 FEB 2026)

| # | Item | Status | IMPL Ref |
|---|------|--------|----------|
| 1 | **Worker Integration** | Done | Sec 6 |
| 2 | **Template Resolution** | Done -- Jinja2 engine wired into dispatch path | Sec 5 |
| 3 | **First Real Workflow** | Done -- echo, fan-out, raster validate all verified E2E in Azure (09 FEB) | Sec 7 |

### Tier 2: Parallel Processing -- DONE (06 FEB 2026)

| # | Item | Status | IMPL Ref |
|---|------|--------|----------|
| 4 | **Fan-Out** | Done -- `FanOutTaskDef` model, `FanOutHandler.expand()`, dynamic child dispatch | Sec 3.1 |
| 5 | **Fan-In** | Done -- aggregation modes (collect, concat, sum, first, last), auto-complete | Sec 3.2 |

30 unit tests in `tests/test_fan_out.py`. Test workflow: `workflows/fan_out_test.yaml`.

### Tier 3: Retry, Timeout, Error Handling -- DONE (09 FEB 2026)

| # | Item | Status | IMPL Ref |
|---|------|--------|----------|
| 6 | **Retry / Timeout / Error Handling** | Done -- timeout detection, retry wiring, flaky handler, 93 tests | Sec 2 |

29 retry/timeout tests in `tests/test_retry.py`. Test workflows: `workflows/retry_test.yaml`, `workflows/retry_fan_out_test.yaml`.

### Tier 4: Business Domain & Presentation Layer (CURRENT FOCUS)

Build the business domain layer, connect B2B applications, and add presentation metadata.

**Design**: ARCHITECTURE.md Sections 13.3, 15.6 | **Implementation plans**: `PHASE1_ASSET_SERVICE.md`, `LAYER_METADATA_IMPL.md`

**Responsibility split** (ARCHITECTURE.md Section 15.6, revised 12 FEB 2026):
- **Orchestrator** = domain authority — owns ALL mutations (business + execution state)
- **Gateway** = read-only ACL proxy — forwards B2B mutations to orchestrator HTTP API, serves read-only queries

#### Phase 4A: Asset Service (standalone, testable) -- DONE (11 FEB 2026)

| Item | What It Is | Status | Ref |
|------|-----------|--------|-----|
| GeospatialAssetService | Business rules: submit, approve, reject, clearance, reprocess | Done | `services/asset_service.py` |
| `get_by_job_id()` | Find asset version by DAG job ID (for processing callbacks) | Done | `repositories/version_repo.py` |
| Unit tests | 24 tests with mocked repos | Done | `tests/test_asset_service.py` |

#### Phase 4B: Gateway Business API -- REVISED (12 FEB 2026)

> **Architecture correction**: Phase 4B originally had the gateway writing directly to the database.
> This violated separation of concerns and introduced the `current_job_id = message_id` bug
> (Service Bus message_id != orchestrator job_id). The gateway is now read-only.
> Write methods and mutation logic are being deleted and rebuilt as orchestrator domain endpoints.

**What survived from original 4B (still valid):**
- `asset_id` on Job model (nullable field + index) — `core/models/job.py`
- Gateway read-only query endpoints (get, list, versions, latest, platforms) — `function/blueprints/asset_bp.py`
- B2B status polling (read-only) — `function/blueprints/platform_bp.py`
- AssetQueryRepository read methods — `function/repositories/asset_query_repo.py`

**What's being deleted (Phase 4B-DELETE):**
- `execute_write()` / `execute_write_returning()` on FunctionRepository
- 6 write methods on AssetQueryRepository
- Gateway submit endpoint (direct DB writes + Service Bus publish)
- Gateway mutation endpoints (approve, reject, clearance, delete doing direct DB writes)

**What replaces it (Phase 4B-REBUILD):**
- Orchestrator domain HTTP endpoints (new FastAPI router)
- Gateway mutation endpoints rewritten as proxy calls to orchestrator

#### ~~Phase 4C: DAG Callback Wiring~~ → merged into Phase 4B-REBUILD (see execution steps below)

---

## EXECUTION PLAN: Phase 4B Revision

> **Approach**: Delete first, rebuild second. No halfway states — rip out all gateway writes,
> then build the orchestrator domain endpoints, then rewire gateway as proxy.

### Pass 1: DELETE (remove all gateway write capability)

Each step is independent. Order doesn't matter but numbering is for tracking.

- [ ] **D1. `function/repositories/base.py`** — Delete `execute_write()` (lines 120-136) and `execute_write_returning()` (lines 138-154). Keep `execute_scalar`, `execute_one`, `execute_many`, `execute_count`. Update docstring to remove "Lightweight" hedge — it IS read-only, period.

- [ ] **D2. `function/repositories/asset_query_repo.py`** — Delete 6 write methods:
  - `get_or_create_asset()` (line 65)
  - `update_asset_clearance()` (line 156)
  - `soft_delete_asset()` (line 186)
  - `create_version()` (line 258)
  - `update_version_processing()` (line 281)
  - `update_version_approval()` (line 306)
  - Update class docstring: "Sync repository for asset-related queries" (remove "and writes").
  - Keep ALL read methods (`get_asset`, `list_assets`, `count_assets`, `get_version`, `list_versions`, `get_latest_approved_version`, `has_unresolved_versions`, `get_next_ordinal`, `get_platform`, `list_active_platforms`).

- [ ] **D3. `function/blueprints/platform_bp.py`** — Gut the `platform_submit` endpoint (lines 81-206). Delete everything from step 1 (validate platform) through step 8 (update version). Replace with a stub that returns 501 Not Implemented ("Awaiting orchestrator domain endpoint"). Keep `platform_status` (line 213) — it's read-only. Remove `ServiceBusPublisher` and `JobQueueMessage` imports. Remove `_compute_asset_id` helper.

- [ ] **D4. `function/blueprints/asset_bp.py`** — Gut 5 mutation endpoints. Replace each with a stub returning 501:
  - `version_approve` (line 250)
  - `version_reject` (line 305)
  - `asset_mark_cleared` (line 366)
  - `asset_mark_public` (line 416)
  - `asset_delete` (line 470)
  - Keep ALL 6 read endpoints unchanged (`asset_get`, `asset_list`, `version_list`, `version_latest`, `version_get`, `platform_list`).

- [ ] **D5. `function/config.py`** — Remove `SERVICE_BUS_FQDN` and any Service Bus connection config from gateway config. The gateway no longer publishes to Service Bus.

- [ ] **D6. Update `function/models/requests.py`** — Keep `AssetSubmitRequest`, `ApprovalRequest`, `ClearanceRequest` models (gateway still receives these from B2B, just forwards them). No deletions, but verify models don't import anything from orchestrator packages.

- [ ] **D7. Run tests** — `pytest tests/ -v`. Fix any test breakage caused by deletions. Tests in `test_asset_service.py` should be unaffected (they test the orchestrator-side service, not gateway). Tests in `test_domain_models.py` should be unaffected.

- [ ] **D8. Verify imports** — `python -c "from function.blueprints import *"` — confirm no import errors after deletions.

**Delete pass complete when**: Gateway has zero write capability. All mutation endpoints return 501. All read endpoints work. Tests pass.

---

### Pass 2: REBUILD — Orchestrator Domain Authority

Build the orchestrator-side HTTP endpoints that the gateway will proxy to.

#### Step R1: Fix `services/asset_service.py` params

- [ ] **R1a.** Add `correlation_id: Optional[str] = None` parameter to `submit_asset()` signature.
- [ ] **R1b.** Pass `asset_id=asset_id` and `correlation_id=correlation_id` to `self.job_service.create_job()` call (line 133). Currently missing — causes `dag_jobs.asset_id = NULL`.
- [ ] **R1c.** Same fix in `reprocess_version()` (line 355) — pass `asset_id` to `create_job()`.
- [ ] **R1d.** Update `tests/test_asset_service.py` to verify `asset_id` and `correlation_id` are passed through.

#### Step R2: Create orchestrator domain HTTP router

- [ ] **R2a.** Create `api/domain_routes.py` — new FastAPI router at `/api/v1/domain/`.
- [ ] **R2b.** `POST /api/v1/domain/submit` — accepts `AssetSubmitRequest` JSON, calls `asset_service.submit_asset()`, returns asset_id + version_ordinal + job_id + request_id. This is where the atomic asset→version→job creation happens.
- [ ] **R2c.** `POST /api/v1/domain/assets/{asset_id}/versions/{ordinal}/approve` — calls `asset_service.approve_version()`.
- [ ] **R2d.** `POST /api/v1/domain/assets/{asset_id}/versions/{ordinal}/reject` — calls `asset_service.reject_version()`.
- [ ] **R2e.** `POST /api/v1/domain/assets/{asset_id}/clearance/cleared` — calls `asset_service.mark_cleared()`.
- [ ] **R2f.** `POST /api/v1/domain/assets/{asset_id}/clearance/public` — calls `asset_service.mark_public()`.
- [ ] **R2g.** `DELETE /api/v1/domain/assets/{asset_id}` — calls `asset_service.soft_delete_asset()`.
- [ ] **R2h.** Register router in `main.py` (orchestrator FastAPI app).

#### Step R3: Wire `asset_service` into Orchestrator

- [ ] **R3a.** Add `asset_service: Optional[GeospatialAssetService] = None` to `Orchestrator.__init__()` (`orchestrator/loop.py` line 75).
- [ ] **R3b.** Store as `self.asset_service`.
- [ ] **R3c.** Construct `GeospatialAssetService` in `main.py` and pass to `Orchestrator()`.

#### Step R4: Wire processing callbacks into orchestrator loop

- [ ] **R4a.** In `_check_job_completion()` (`orchestrator/loop.py` line 1198), after `complete_job()` (line 1213): call `await self.asset_service.on_processing_completed(job.job_id, result_data)` (guard with `if self.asset_service:`).
- [ ] **R4b.** After `fail_job()` (line 1227): call `await self.asset_service.on_processing_failed(job.job_id, error)` (guard with `if self.asset_service:`).
- [ ] **R4c.** These calls are no-ops for standalone jobs (asset_service returns None when no version is linked).

#### Step R5: Unit tests for domain endpoints + callbacks

- [ ] **R5a.** Test `domain_routes.py` endpoints with mocked `asset_service` (happy path + validation errors + state conflicts).
- [ ] **R5b.** Test callback wiring: mock `asset_service.on_processing_completed/failed` is called when job completes/fails.
- [ ] **R5c.** Test standalone job (no asset_id) — callback returns None, no error.
- [ ] **R5d.** Run full suite: `pytest tests/ -v`.

**Rebuild pass complete when**: Orchestrator domain endpoints work. `submit_asset()` passes `asset_id` + `correlation_id`. Processing callbacks fire on job completion/failure. All tests pass.

---

### Pass 3: PROXY — Gateway forwards to orchestrator

Rewire the gateway 501 stubs to proxy through to orchestrator domain endpoints.

#### Step P1: Gateway HTTP client

- [ ] **P1a.** Create `function/services/orchestrator_client.py` — lightweight HTTP client wrapping `httpx` (sync, since Azure Functions is sync). Reads `ORCHESTRATOR_BASE_URL` from env.
- [ ] **P1b.** Methods: `submit(body) -> dict`, `approve(asset_id, ordinal, body) -> dict`, `reject(...)`, `mark_cleared(...)`, `mark_public(...)`, `delete_asset(...)`.
- [ ] **P1c.** Error handling: map orchestrator HTTP status codes to gateway responses (400→400, 404→404, 409→409, 500→502).

#### Step P2: Rewrite `platform_bp.py` submit

- [ ] **P2a.** Replace 501 stub with: validate JSON → generate `request_id` if not provided → call `orchestrator_client.submit(body)` → return B2B-filtered response (asset_id, version_ordinal, request_id, status="accepted").
- [ ] **P2b.** Gateway does NOT call `_compute_asset_id` — orchestrator computes it.
- [ ] **P2c.** Gateway does NOT import `ServiceBusPublisher` or `JobQueueMessage` — orchestrator handles queue.

#### Step P3: Rewrite `asset_bp.py` mutations

- [ ] **P3a.** `version_approve` → call `orchestrator_client.approve(asset_id, ordinal, body)`, relay response.
- [ ] **P3b.** `version_reject` → call `orchestrator_client.reject(asset_id, ordinal, body)`, relay response.
- [ ] **P3c.** `asset_mark_cleared` → call `orchestrator_client.mark_cleared(asset_id, body)`, relay response.
- [ ] **P3d.** `asset_mark_public` → call `orchestrator_client.mark_public(asset_id, body)`, relay response.
- [ ] **P3e.** `asset_delete` → call `orchestrator_client.delete_asset(asset_id, actor)`, relay response.

#### Step P4: Update gateway RBAC + config

- [ ] **P4a.** Remove `Azure Service Bus Data Sender` from gateway identity (no longer publishes).
- [ ] **P4b.** Ensure gateway has network access to orchestrator URL (VNet or public endpoint).
- [ ] **P4c.** Add `ORCHESTRATOR_BASE_URL` to `local.settings.json.example` and Azure Function App config.
- [ ] **P4d.** PostgreSQL connection for gateway should be read-only user (future hardening).

#### Step P5: Integration test

- [ ] **P5a.** Test proxy flow: gateway → orchestrator → DB → response relayed.
- [ ] **P5b.** Test error relay: orchestrator returns 409, gateway returns 409 to B2B.
- [ ] **P5c.** Test orchestrator down: gateway returns 502/503.
- [ ] **P5d.** Run full suite: `pytest tests/ -v`.

**Proxy pass complete when**: All gateway mutation endpoints proxy to orchestrator. No direct DB writes from gateway. B2B flow works end-to-end. All tests pass.

---

### Pass 4: DEPLOY + VERIFY

- [ ] **V1.** Deploy asset tables to Azure: `POST /api/v1/bootstrap/deploy?confirm=yes`
- [ ] **V2.** ACR build + deploy orchestrator with domain endpoints (`--platform linux/amd64`)
- [ ] **V3.** Deploy gateway Function App (`func azure functionapp publish`)
- [ ] **V4.** E2E test: B2B submit → gateway proxy → orchestrator creates asset+version+job → DAG runs → callback updates version → B2B polls status
- [ ] **V5.** Verify `current_job_id` on version matches actual `job_id` on dag_jobs (the bug that started all this)
- [ ] **V6.** Tag as v0.17.0

---

## Phase 4D-4F: Layer Metadata (AFTER Phase 4B revision complete)

#### Phase 4D: Layer Metadata Phase 1 (models + admin CRUD)

| Item | What It Is | Status | Ref |
|------|-----------|--------|-----|
| LayerMetadata + LayerCatalog models | Source of truth + B2C projection target | Not started | `LAYER_METADATA_IMPL.md` Phase 1 |
| Metadata repos (async + sync) | CRUD for layer_metadata, DDL for layer_catalog | Not started | `LAYER_METADATA_IMPL.md` Phase 1 |
| Admin CRUD API | Orchestrator domain endpoints for metadata (NOT gateway-local) | Not started | `LAYER_METADATA_IMPL.md` Phase 1 |
| Unit tests | ~25-30 model + repo + API tests | Not started | `LAYER_METADATA_IMPL.md` Phase 1 |

Depends on: asset tables deployed (Phase 4B-V1) — `layer_metadata.asset_id` FK references `geospatial_assets`.

#### Phase 4E: Direct Command Router + Metadata Projection

| Item | What It Is | Status | Ref |
|------|-----------|--------|-----|
| `api/command_routes.py` | FastAPI router at `/api/v1/commands/` on orchestrator | Not started | ARCHITECTURE.md Section 15.5 |
| MetadataProjectionService | Publish layer_metadata → STAC + layer_catalog | Not started | `LAYER_METADATA_IMPL.md` Phase 2 |
| Publish/unpublish commands | `POST /api/v1/commands/metadata/publish/{asset_id}` | Not started | `LAYER_METADATA_IMPL.md` Phase 2 |
| Unit tests | ~20-25 projection + command tests | Not started | `LAYER_METADATA_IMPL.md` Phase 2 |

Depends on: Phase 4D (models exist) + asset versions with artifacts (Phase 4B-R4) for meaningful projection.

#### Phase 4F: External B2C + ADF Export

| Item | What It Is | Status | Ref |
|------|-----------|--------|-----|
| External projection handler | Project layer_catalog to external B2C database | Not started | `LAYER_METADATA_IMPL.md` Phase 3 |
| ADF pipeline integration | Copy layer_catalog + pgstac to external on PUBLIC clearance | Not started | `LAYER_METADATA_IMPL.md` Phase 3 |

Depends on: Phase 4E (internal projection working) + ADF export pipeline operational.

### Tier 5: Observability & Hardening

Polish, operational visibility, and production safety.

| # | Item | What It Is | Status | Ref |
|---|------|-----------|--------|-----|
| 13 | **Version Pinning** | Pin workflow version + snapshot at job creation; provenance in outputs | Done (11 FEB) | `docs/VERSION_PINNING.md` |
| 10 | **Progress API** | Real-time job/node progress | Not started | IMPL Sec 4.1 |
| 11 | **Metrics Dashboard** | Workflow stats, failure rates | Not started | IMPL Sec 4.2 |
| 12 | **Data Integrity** | Pydantic schemas on raw dict endpoints | Not started | IMPL Sec 5.3 |
| 14 | **Transaction Wrapping** | Wrap multi-step DB updates atomically | Deprioritized (see below) |  IMPL Sec 2.2 |

**Version Pinning** (item 13) — DONE (11 FEB 2026). 7 files modified, 134 tests pass, E2E verified in Azure. See `docs/VERSION_PINNING.md`.

- [x] Add `workflow_version` (INT NOT NULL) and `workflow_snapshot` (JSONB NOT NULL) to Job model
- [x] Update JobRepository to persist/read new fields
- [x] Capture snapshot at job creation in JobService
- [x] Add `workflow_version` to JOB_CREATED event data
- [x] Orchestrator uses pinned snapshot instead of live cache
- [x] Enrich `result_data._provenance` with workflow_id, version, job_id, completion time
- [x] Update Function App query repo to expose version
- [x] Tests: snapshot round-trip, existing test fixes

**Transaction Wrapping** (item 14) — Deprioritized: the polling loop self-heals transient inconsistencies and optimistic locking prevents double-processing. Belt-and-suspenders, not a correctness fix.

### Tier 6: Migration & Future

| # | Item | What It Is | IMPL Ref |
|---|------|-----------|----------|
| 15 | **Port remaining workflows** | Vector ETL, FATHOM flood processing | Sec 7 |
| 16 | **Migration & Cutover** | Route all traffic to DAG, deprecate legacy | Sec 9 |
| 17 | **Web UI** | Visualize DAGs, view job runs | Sec 10 |
| 18 | **H3 Hexagonal Aggregation** | Raster zonal stats, vector aggregation, cell connectivity graph | `HEXAGONS.md` |

---

## Known Gaps

- [ ] Test coverage (6 test files / 158 tests for ~125 source files -- growing but still minimal)
- [ ] Function App not yet deployed to Azure (code complete, RBAC + settings configured, needs `func azure functionapp publish`)
- [x] ~~Function App RBAC not configured~~ -- Done (11 FEB 2026): system-assigned identity for Service Bus, UMI for PostgreSQL
- [ ] CI/CD pipeline not built (GitHub Actions workflow spec exists in IMPL)
- [x] ~~Retry/timeout logic not yet hardened~~ -- Done (09 FEB 2026): timeout detection, retry wiring, 93 tests
- [ ] Transaction wrapping for multi-step DB updates (crash between steps = inconsistent state)
- [ ] More raster handlers needed (COG translation, tiling, STAC registration) for full ingest pipeline
- [ ] Gateway RBAC needs tightening: remove Service Bus Data Sender, restrict PostgreSQL to read-only

---

## Success Criteria

### Tier 1 Complete -- DONE (06 FEB 2026)
- [x] Worker picks up task from Service Bus queue
- [x] Worker executes handler and writes to `dag_task_results`
- [x] Template resolution passes data between nodes
- [x] Raster validate runs end-to-end on real data (09 FEB: small tile + 11.3 GB WV2 multispectral)

### MVP (Tier 1-2 Complete) -- DONE (06 FEB 2026)
- [x] Fan-out works (tiled raster -> N parallel tasks)
- [x] Fan-in aggregates results
- [x] Failures detected within 60 seconds
- [x] Conditional routing works (size-based)

### Tier 3 Complete -- DONE (09 FEB 2026)
- [x] Timeout detection scans DISPATCHED/RUNNING nodes per orchestration cycle
- [x] RetryPolicy from workflow YAML wired to NodeState.max_retries
- [x] Auto-retry on failure (FAILED → READY → re-dispatch)
- [x] Flaky echo handler for statistical retry verification
- [x] READY → FAILED for pre-dispatch errors (template failures)
- [x] Blob SAS delegation via managed identity (no account keys)
- [x] Raster validate handler with blob reference resolution
- [x] 93 unit tests across 4 test files (now 134 as of v0.1.15.0)

### Phase 4B Revision Complete
- [ ] Gateway has zero write capability (no `execute_write`, no write methods, no Service Bus publish)
- [ ] Orchestrator domain endpoints handle all mutations (submit, approve, reject, clearance, delete)
- [ ] `submit_asset()` passes `asset_id` + `correlation_id` to `create_job()` — no more NULL
- [ ] `current_job_id` on asset_versions matches actual `job_id` on dag_jobs
- [ ] Processing callbacks fire on job completion/failure, updating version state
- [ ] Gateway proxies all mutations through to orchestrator HTTP API
- [ ] B2B submit → proxy → orchestrator → DAG → callback → poll status works E2E

### Production Ready (Tier 1-5 Complete)
- [ ] All workflows migrated to DAG
- [ ] DDH integration works
- [ ] Monitoring and alerting configured
- [ ] Legacy CoreMachine deprecated

---

## References

| Document | Purpose |
|----------|---------|
| `ARCHITECTURE.md` | System design, data flows, schemas, patterns, gateway vs orchestrator responsibility (Section 15.6) |
| `IMPLEMENTATION.md` | Detailed specs, code examples, task checklists |
| `CLAUDE.md` | Project conventions, code patterns, quick reference |
| `VERSION_PINNING.md` | Workflow version pinning implementation (Tier 5, complete) |
| `PHASE1_ASSET_SERVICE.md` | GeospatialAssetService implementation plan (Tier 4A) |
| `LAYER_METADATA_IMPL.md` | Layer metadata implementation plan (Tier 4D-4F) |
| `HEXAGONS.md` | H3 hexagonal aggregation pipeline — raster, vector, connectivity (future, after core DAG) |
| `README.md` | Entry point for new developers |
