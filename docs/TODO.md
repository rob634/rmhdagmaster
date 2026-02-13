# TODO - rmhdagmaster

**Last Updated**: 13 FEB 2026

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
| Phase 4A: Asset Service | Done | GeospatialAssetService — business rules, 26 tests | PHASE1_ASSET_SERVICE.md |
| Phase 4B: Gateway Business API | Done | 4-pass revision: DELETE→REBUILD→PROXY→DEPLOY | ARCHITECTURE.md Sec 15.6 |

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
- GeospatialAssetService: business rules (submit, approve, reject, clearance, reprocess, callbacks) — 26 tests
- Asset read-only query endpoints on gateway (get, list, versions, latest, platforms)
- B2B status polling: `GET /api/platform/status/{request_id}` via `correlation_id`
- `asset_id` field on `dag_jobs`: nullable, indexed — links jobs to geospatial assets (standalone jobs have NULL)
- Phase 4B complete: gateway write capability removed, orchestrator domain endpoints built, gateway rewired as proxy
- OrchestratorClient: sync httpx proxy for gateway→orchestrator mutations (submit, approve, reject, clearance, delete)
- Processing callbacks: `on_processing_completed/failed` fires in orchestrator loop on job completion
- Gateway query repos aligned to current Pydantic-generated schema (13 FEB 2026)
- All health endpoints now include `version` + `build_date` from `__version__.py` (unified across all 3 apps)
- Gateway deployed to Azure: `rmhdaggateway-f7epa0d6bnc6bzak.eastus-01.azurewebsites.net`
- Silent auth fallback bug fixed: gateway fails fast if managed identity auth requested but unavailable
- v0.17.0 deployed to Azure (orchestrator + worker + gateway), echo test verified E2E (13 FEB 2026)
- 190 unit tests across 8 test files

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
| Unit tests | 26 tests with mocked repos | Done | `tests/test_asset_service.py` |

#### Phase 4B: Gateway Business API -- DONE (13 FEB 2026)

> **Architecture correction**: Phase 4B originally had the gateway writing directly to the database.
> This violated separation of concerns and introduced the `current_job_id = message_id` bug
> (Service Bus message_id != orchestrator job_id). Revised as a 4-pass plan:
> DELETE → REBUILD → PROXY → DEPLOY+VERIFY.

**What was done:**
- Pass 1 (DELETE): Stripped all gateway write capability — `execute_write()`, 6 write methods, direct DB mutations, Service Bus publish
- Pass 2 (REBUILD): Orchestrator domain endpoints (`api/domain_routes.py`), `asset_service` wiring, processing callbacks (`on_processing_completed/failed`)
- Pass 3 (PROXY): `OrchestratorClient` (sync httpx), gateway mutations rewritten as proxy calls, error code relay (4xx→4xx, 5xx→502, timeout→504)
- Pass 4 (DEPLOY+VERIFY): All 3 apps deployed, echo test E2E verified, gateway query repos aligned to schema
- Silent auth fallback bug fixed (gateway `config.py` was swallowing `ImportError` and falling through to password auth)
- Gateway query repos aligned to Pydantic-generated schema (stale column names: `idempotency_key`, `node_name`, `handler_name`, `input_data`, `result_data`, `task_queue`, `message`, `details`, `timestamp` → corrected)
- All health endpoints unified: `version` + `build_date` from `__version__.py` across orchestrator, worker, and gateway

**Key files created/modified:**
- `function/services/orchestrator_client.py` (NEW) — sync httpx proxy client
- `api/domain_routes.py` (NEW) — orchestrator domain HTTP endpoints
- `tests/test_domain_routes.py` (NEW) — 17 tests for domain endpoints + callbacks
- `tests/test_gateway_proxy.py` (NEW) — 13 tests for proxy client
- `function/blueprints/platform_bp.py` — 501 stub → proxy
- `function/blueprints/asset_bp.py` — 5 stubs → proxy calls
- `function/repositories/base.py` — write methods removed
- `function/repositories/job_query_repo.py` — aligned to schema
- `function/repositories/node_query_repo.py` — aligned to schema
- `function/repositories/event_query_repo.py` — aligned to schema
- `health/router.py` — version added to health responses

#### ~~Phase 4C: DAG Callback Wiring~~ → merged into Phase 4B Pass 2 (REBUILD)

---

## EXECUTION PLAN: Phase 4B Revision -- DONE (13 FEB 2026)

> All 4 passes complete. Gateway is read-only proxy, orchestrator is domain authority, all 3 apps deployed.

### Pass 1: DELETE -- DONE (12 FEB 2026)

- [x] **D1.** `function/repositories/base.py` — Deleted `execute_write()` and `execute_write_returning()`.
- [x] **D2.** `function/repositories/asset_query_repo.py` — Deleted 6 write methods, kept all read methods.
- [x] **D3.** `function/blueprints/platform_bp.py` — Gutted submit endpoint, replaced with 501 stub.
- [x] **D4.** `function/blueprints/asset_bp.py` — Gutted 5 mutation endpoints, replaced with 501 stubs.
- [x] **D5.** `function/config.py` — Service Bus config retained (gateway_bp generic submit still uses it).
- [x] **D6.** `function/models/requests.py` — Models verified, no orchestrator imports.
- [x] **D7-D8.** Tests pass, imports clean.

### Pass 2: REBUILD -- DONE (12 FEB 2026)

- [x] **R1.** Fixed `asset_service.submit_asset()` — passes `asset_id` + `correlation_id` to `create_job()`.
- [x] **R2.** Created `api/domain_routes.py` — 7 endpoints at `/api/v1/domain/`.
- [x] **R3.** Wired `asset_service` into `Orchestrator.__init__()` and `main.py`.
- [x] **R4.** Processing callbacks: `on_processing_completed/failed` in `_check_job_completion()`.
- [x] **R5.** 17 tests in `tests/test_domain_routes.py`.

### Pass 3: PROXY -- DONE (12 FEB 2026)

- [x] **P1.** Created `function/services/orchestrator_client.py` — sync httpx client, 6 methods.
- [x] **P2.** Rewrote `platform_bp.py` submit: validates → proxies → returns B2B-filtered response.
- [x] **P3.** Rewrote 5 `asset_bp.py` mutation stubs → proxy calls.
- [x] **P4.** `ORCHESTRATOR_BASE_URL` configured on Azure Function App.
- [x] **P5.** 13 tests in `tests/test_gateway_proxy.py`.

### Pass 4: DEPLOY + VERIFY -- DONE (13 FEB 2026)

- [x] **V1.** Schema deployed via `bootstrap/rebuild` (dev DB, fresh schema with `asset_id` column).
- [x] **V2.** ACR build + deploy orchestrator + worker (`scripts/deploy.sh 0.1.17.0`).
- [x] **V3.** Gateway deployed (`func azure functionapp publish rmhdaggateway`).
- [x] **V4.** E2E echo test verified: submit → start → echo_handler → end → COMPLETED (~7s).
- [x] **V5.** Gateway read endpoints verified: `/api/status/jobs`, `/api/status/stats`, proxy endpoints.
- [x] **V6.** Tagged as v0.17.0 (commit `43110ba`).

**Post-deploy fixes (13 FEB 2026):**
- [x] Gateway auth: removed silent `except ImportError` fallback in `config.py` — now fails fast if MI unavailable.
- [x] Added `psycopg-pool` to `requirements.txt` (needed by `infrastructure/__init__.py` transitive import).
- [x] Removed `requirements.txt` from `.funcignore` (Oryx build system needs it).
- [x] Gateway query repos: aligned 3 repos to current schema (stale column names from early development).
- [x] Health endpoints: added `version` + `build_date` to all health probes (orchestrator, worker, gateway).

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

- [ ] Test coverage (8 test files / 190 tests for ~125 source files — growing but still minimal)
- [x] ~~Function App not yet deployed to Azure~~ — Done (13 FEB 2026): `rmhdaggateway` deployed, readyz green
- [x] ~~Function App RBAC not configured~~ — Done (11 FEB 2026): system-assigned identity for Service Bus, UMI for PostgreSQL
- [ ] CI/CD pipeline not built (GitHub Actions workflow spec exists in IMPL)
- [x] ~~Retry/timeout logic not yet hardened~~ — Done (09 FEB 2026): timeout detection, retry wiring, 93 tests
- [ ] Transaction wrapping for multi-step DB updates (crash between steps = inconsistent state)
- [ ] More raster handlers needed (COG translation, tiling, STAC registration) for full ingest pipeline
- [ ] Gateway RBAC needs tightening: remove Service Bus Data Sender, restrict PostgreSQL to read-only
- [ ] Workflow YAML input defaults not applied to `input_params` before template resolution (YAML says `default: 1` but `StrictUndefined` throws if param missing from `input_params`)
- [ ] Bootstrap `/migrate` hardcoded migration list needs `asset_id` column added (workaround: use `/rebuild` for dev)

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

### Phase 4B Revision Complete -- DONE (13 FEB 2026)
- [x] Gateway has zero write capability (no `execute_write`, no write methods, no Service Bus publish)
- [x] Orchestrator domain endpoints handle all mutations (submit, approve, reject, clearance, delete)
- [x] `submit_asset()` passes `asset_id` + `correlation_id` to `create_job()` — no more NULL
- [x] `current_job_id` on asset_versions matches actual `job_id` on dag_jobs
- [x] Processing callbacks fire on job completion/failure, updating version state
- [x] Gateway proxies all mutations through to orchestrator HTTP API
- [ ] B2B submit → proxy → orchestrator → DAG → callback → poll status works E2E (needs platform seed data)

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
