# TODO - rmhdagmaster

**Last Updated**: 13 FEB 2026 (Phase 4D complete, pivot to Raster ETL)

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
| Phase 4A: Asset Service | Done | GeospatialAssetService — business rules, 28 tests | PHASE1_ASSET_SERVICE.md |
| Phase 4B: Gateway Business API | Done | 4-pass revision: DELETE→REBUILD→PROXY→DEPLOY | ARCHITECTURE.md Sec 15.6 |
| Phase 4C: DAG Callback Wiring | Done | `on_processing_completed/failed` in orchestrator loop | merged into 4B Pass 2 |
| Phase 4D: Layer Metadata | Done | Models, repo, service, admin API, gateway blueprint — 34 tests | LAYER_METADATA_IMPL.md Phase 1 |
| Phase 4E-4F: Projection + Export | **On Hold** | Blocked on real ETL artifacts (STAC collections, PostGIS tables) | LAYER_METADATA_IMPL.md Phase 2-3 |
| **Raster ETL** | **NEXT** | Linear pipeline first, then conditional/tiling | see below |

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
- GeospatialAssetService: business rules (submit, approve, reject, clearance, reprocess, callbacks) — 28 tests
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
- Phase 4D: LayerMetadata + LayerCatalog models, MetadataService, orchestrator CRUD routes, gateway read + proxy blueprint
- 209 unit tests across 9 test files

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

### Tier 4: Business Domain & Presentation Layer — COMPLETE (except projection)

**Design**: ARCHITECTURE.md Sections 13.3, 15.6 | **Implementation plans**: `PHASE1_ASSET_SERVICE.md`, `LAYER_METADATA_IMPL.md`

**Responsibility split** (ARCHITECTURE.md Section 15.6, revised 12 FEB 2026):
- **Orchestrator** = domain authority — owns ALL mutations (business + execution state)
- **Gateway** = read-only ACL proxy — forwards B2B mutations to orchestrator HTTP API, serves read-only queries

#### Phase 4A: Asset Service -- DONE (11 FEB 2026)

GeospatialAssetService: submit, approve, reject, clearance, reprocess, callbacks — 28 tests.

#### Phase 4B: Gateway Business API -- DONE (13 FEB 2026)

4-pass revision: DELETE → REBUILD → PROXY → DEPLOY. Gateway is read-only proxy, orchestrator is domain authority.

#### Phase 4C: DAG Callback Wiring -- DONE (merged into 4B Pass 2)

`on_processing_completed/failed` fires in orchestrator loop on job completion.

#### Phase 4D: Layer Metadata Phase 1 -- DONE (13 FEB 2026)

| Item | What It Is | Status | Ref |
|------|-----------|--------|-----|
| LayerMetadata + LayerCatalog models | Source of truth + B2C projection target (PydanticToSQL DDL) | Done | `core/models/layer_metadata.py`, `layer_catalog.py` |
| LayerMetadataRepository | Async CRUD, optimistic locking, JSONB wrapping | Done | `repositories/metadata_repo.py` |
| MetadataService | Asset validation, zoom constraints, merge updates | Done | `services/metadata_service.py` |
| Orchestrator CRUD routes | `/api/v1/domain/metadata/` (create, read, update, delete, list) | Done | `api/metadata_routes.py` |
| Gateway metadata blueprint | Reads local, mutations proxy to orchestrator | Done | `function/blueprints/metadata_bp.py` |
| OrchestratorClient metadata | 3 proxy methods (create, update, delete) | Done | `function/services/orchestrator_client.py` |
| Unit tests | 34 tests (model, catalog, repo, service, schema) | Done | `tests/test_layer_metadata.py` |

#### Phase 4E-4F: Metadata Projection + Export -- ON HOLD

**Reason**: Projection writes to STAC collections and layer_catalog. These targets don't exist until the raster ETL pipeline produces real artifacts (COGs, STAC items, PostGIS tables). Building projection against imaginary data structures means guessing and rewriting later.

**Resume when**: Raster ETL pipeline produces real COG outputs with STAC registration. Then 4E has real targets to project into.

---

### Raster ETL Pipeline (CURRENT FOCUS)

Build a working end-to-end raster ingest workflow that produces real COG outputs and STAC catalog entries.

**Strategy**: Linear pipeline first → prove the engine works → then add conditional routing and tiling to leverage the DAG.

#### Existing Handlers (already built, in `handlers/raster/`)

| Handler Name | Purpose | Status |
|--------------|---------|--------|
| `raster.validate` | Validate raster, detect type, compression profile | Built + tested E2E |
| `raster.blob_to_mount` | Copy blob to Azure Files mount for disk-based processing | Built |
| `raster.create_cog` | Reproject + create COG with type-specific compression | Built |
| `raster.generate_tiling_scheme` | Calculate tile grid for large outputs (>1GB) | Built |
| `raster.process_tile_batch` | Process tiles with per-tile checkpointing | Built |
| `raster.stac_ensure_collection` | Create/update STAC collection (idempotent) | Built |
| `raster.stac_register_item` | Register single COG to STAC | Built |
| `raster.stac_register_items` | Register multiple tiled COGs to STAC | Built |

#### Phase R1: Simple Linear Workflow (NEXT)

Create a static, linear workflow that exercises the core pipeline end-to-end:

```
START → validate → create_cog → stac_ensure_collection → stac_register_item → END
```

| Item | What It Is | Status |
|------|-----------|--------|
| Fix workflow YAML | Existing `raster_ingest.yaml` uses wrong handler names — align to actual registered handlers | Not started |
| Wire handler params | Template expressions to pass validate output → COG input → STAC input | Not started |
| Test E2E in Azure | Submit real raster → get COG output + STAC entry | Not started |
| Fix handler bugs | Handlers were written speculatively — expect integration issues | Not started |

**Success criteria**: Submit a raster blob → validate passes → COG created in silver container → STAC collection + item registered → job COMPLETED with artifact paths.

#### Phase R2: Conditional Routing + Tiling (AFTER R1)

Leverage the DAG for size-based conditional routing:

```
START → validate → route_by_size ─┬─ small  → create_cog (memory) ────────────────────┐
                                  ├─ medium → create_cog (docker) ─────────────────────┤
                                  └─ large  → blob_to_mount → generate_tiles → fan_out ┤
                                                                                        ↓
                                                                     stac_ensure_collection → stac_register → END
```

| Item | What It Is | Status |
|------|-----------|--------|
| Fix `raster_processing.yaml` | Align handler names, wire template expressions | Not started |
| Tiling fan-out | Dynamic tile batch creation + parallel processing | Not started |
| Multi-path STAC registration | Single COG vs tiled COGs (different STAC handlers) | Not started |
| E2E with multiple raster sizes | Small (<100MB), medium (100MB-1GB), large (>1GB) | Not started |

**After R2**: Resume Phase 4E (projection) — real STAC collections and artifacts exist to project metadata into.

---

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

- [ ] Test coverage (9 test files / 209 tests for ~130 source files — growing but still minimal)
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
