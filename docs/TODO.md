# TODO - rmhdagmaster

**Last Updated**: 07 FEB 2026

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
- 64 unit tests across 3 test files

---

## Implementation Priority

### Tier 1: End-to-End Execution -- DONE (06 FEB 2026)

| # | Item | Status | IMPL Ref |
|---|------|--------|----------|
| 1 | **Worker Integration** | Done | Sec 6 |
| 2 | **Template Resolution** | Done -- Jinja2 engine wired into dispatch path | Sec 5 |
| 3 | **First Real Workflow** | In Progress -- echo + fan-out verified E2E in Azure (07 FEB); raster validate next | Sec 7 |

### Tier 2: Parallel Processing -- DONE (06 FEB 2026)

| # | Item | Status | IMPL Ref |
|---|------|--------|----------|
| 4 | **Fan-Out** | Done -- `FanOutTaskDef` model, `FanOutHandler.expand()`, dynamic child dispatch | Sec 3.1 |
| 5 | **Fan-In** | Done -- aggregation modes (collect, concat, sum, first, last), auto-complete | Sec 3.2 |

30 unit tests in `tests/test_fan_out.py`. Test workflow: `workflows/fan_out_test.yaml`.

### Tier 3: Platform Integration (CURRENT FOCUS)

Connect B2B applications to the DAG engine.

| # | Item | What It Is | IMPL Ref |
|---|------|-----------|----------|
| 6 | **Retry / Timeout / Error Handling** | Robust failure recovery, timeout detection | Sec 2 |
| 7 | **GeospatialAsset model** | Link requests -> assets -> jobs | Sec 5.1 |
| 8 | **API Integration** | External-facing submit/status/cancel on orchestrator | Sec 8 |
| 9 | **Callback architecture** | POST results back to B2B apps on job completion | Sec 8 |

### Tier 4: Observability & Hardening

Polish and operational visibility.

| # | Item | What It Is | IMPL Ref |
|---|------|-----------|----------|
| 10 | **Progress API** | Real-time job/node progress | Sec 4.1 |
| 11 | **Metrics Dashboard** | Workflow stats, failure rates | Sec 4.2 |
| 12 | **Data Integrity** | Pydantic schemas on raw dict endpoints | Sec 5.3 |
| 13 | **Version Pinning** | Snapshot workflow YAML at job creation | Sec 2.1 |
| 14 | **Transaction Wrapping** | Wrap multi-step DB updates atomically | Sec 2.2 |

### Tier 5: Migration & Future

| # | Item | What It Is | IMPL Ref |
|---|------|-----------|----------|
| 15 | **Port remaining workflows** | Vector ETL, FATHOM flood processing | Sec 7 |
| 16 | **Migration & Cutover** | Route all traffic to DAG, deprecate legacy | Sec 9 |
| 17 | **Web UI** | Visualize DAGs, view job runs | Sec 10 |
| 18 | **H3 Network Synthesis** | Hexagonal aggregation pipelines | Sec 11 |

---

## Known Gaps

- [ ] Test coverage (3 test files / 64 tests for ~118 source files -- growing but still minimal)
- [ ] Function App not yet deployed to Azure as Function App (code complete, tested via Docker orchestrator)
- [ ] Function App RBAC not configured (Service Bus sender, PG read)
- [ ] CI/CD pipeline not built (GitHub Actions workflow spec exists in IMPL)
- [ ] Retry/timeout logic not yet hardened (basic retry exists, timeout detection needs work)
- [ ] Transaction wrapping for multi-step DB updates (crash between steps = inconsistent state)

---

## Success Criteria

### Tier 1 Complete -- DONE (06 FEB 2026)
- [x] Worker picks up task from Service Bus queue
- [x] Worker executes handler and writes to `dag_task_results`
- [x] Template resolution passes data between nodes
- [ ] Raster workflow runs end-to-end on real data (pending real handler port)

### MVP (Tier 1-2 Complete) -- DONE (06 FEB 2026)
- [x] Fan-out works (tiled raster -> N parallel tasks)
- [x] Fan-in aggregates results
- [x] Failures detected within 60 seconds
- [x] Conditional routing works (size-based)

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
