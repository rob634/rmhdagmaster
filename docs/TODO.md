# TODO - rmhdagmaster

**Last Updated**: 09 FEB 2026

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
- 93 unit tests across 4 test files (v0.12.0)

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

### Tier 4: Business Domain Model & Platform Integration (CURRENT FOCUS)

Build the business domain layer (GeospatialAsset, AssetVersion, Platform Registry) and connect B2B applications to the DAG engine.

**Design**: ARCHITECTURE.md Section 13.3

| # | Item | What It Is | Status |
|---|------|-----------|--------|
| 7a | **GeospatialAsset model** | Aggregate root: identity, clearance, platform_refs | Design complete |
| 7b | **AssetVersion model** | Semantic versions: approval, processing, revision, service outputs | Design complete |
| 7c | **Platform Registry** | B2B app registration with required_refs validation | Design complete |
| 7d | **Version ordering enforcement** | Reject submissions when prior version unresolved | Design complete |
| 7e | **Service URL routing** | version=latest / version=N resolution | Design complete |
| 7f | **Service artifacts model** | Flat fields (Option A) vs child entity (Option B) -- start with A | Design complete |
| 7g | **Multi-workflow lifecycle** | ETL on submit, ADF export on public clearance -- gateway-orchestrator relay | Design complete |
| 8 | **API Integration** | platform/submit, status, approve, reject, unpublish | Not started |
| 9 | **Callback architecture** | POST results back to B2B apps on job completion | Not started |

### Tier 5: Observability & Hardening (CURRENT FOCUS)

Polish, operational visibility, and production safety.

| # | Item | What It Is | Status | Ref |
|---|------|-----------|--------|-----|
| 13 | **Version Pinning** | Pin workflow version + snapshot at job creation; provenance in outputs | In progress | `docs/VERSION_PINNING.md` |
| 10 | **Progress API** | Real-time job/node progress | Not started | IMPL Sec 4.1 |
| 11 | **Metrics Dashboard** | Workflow stats, failure rates | Not started | IMPL Sec 4.2 |
| 12 | **Data Integrity** | Pydantic schemas on raw dict endpoints | Not started | IMPL Sec 5.3 |
| 14 | **Transaction Wrapping** | Wrap multi-step DB updates atomically | Deprioritized (see below) | IMPL Sec 2.2 |

**Version Pinning** (item 13) — 7 files, implementation plan at `docs/VERSION_PINNING.md`:

- [ ] Add `workflow_version` (INT NOT NULL) and `workflow_snapshot` (JSONB NOT NULL) to Job model
- [ ] Update JobRepository to persist/read new fields
- [ ] Capture snapshot at job creation in JobService
- [ ] Add `workflow_version` to JOB_CREATED event data
- [ ] Orchestrator uses pinned snapshot instead of live cache
- [ ] Enrich `result_data._provenance` with workflow_id, version, job_id, completion time
- [ ] Update Function App query repo to expose version
- [ ] Tests: snapshot round-trip, existing test fixes

**Transaction Wrapping** (item 14) — Deprioritized: the polling loop self-heals transient inconsistencies and optimistic locking prevents double-processing. Belt-and-suspenders, not a correctness fix.

### Tier 6: Migration & Future

| # | Item | What It Is | IMPL Ref |
|---|------|-----------|----------|
| 15 | **Port remaining workflows** | Vector ETL, FATHOM flood processing | Sec 7 |
| 16 | **Migration & Cutover** | Route all traffic to DAG, deprecate legacy | Sec 9 |
| 17 | **Web UI** | Visualize DAGs, view job runs | Sec 10 |
| 18 | **H3 Network Synthesis** | Hexagonal aggregation pipelines | Sec 11 |

---

## Known Gaps

- [ ] Test coverage (4 test files / 93 tests for ~118 source files -- growing but still minimal)
- [ ] Function App not yet deployed to Azure as Function App (code complete, tested via Docker orchestrator)
- [ ] Function App RBAC not configured (Service Bus sender, PG read)
- [ ] CI/CD pipeline not built (GitHub Actions workflow spec exists in IMPL)
- [x] ~~Retry/timeout logic not yet hardened~~ -- Done (09 FEB 2026): timeout detection, retry wiring, 93 tests
- [ ] Transaction wrapping for multi-step DB updates (crash between steps = inconsistent state)
- [ ] More raster handlers needed (COG translation, tiling, STAC registration) for full ingest pipeline

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
- [x] 93 unit tests across 4 test files

### Production Ready (Tier 1-5 Complete)
- [ ] All workflows migrated to DAG
- [ ] DDH integration works
- [ ] Monitoring and alerting configured
- [ ] Legacy CoreMachine deprecated

---

## References

| Document | Purpose |
|----------|---------|
| `ARCHITECTURE.md` | System design, data flows, schemas, patterns |
| `IMPLEMENTATION.md` | Detailed specs, code examples, task checklists |
| `CLAUDE.md` | Project conventions, code patterns, quick reference |
| `README.md` | Entry point for new developers |
