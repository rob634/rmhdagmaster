# CLAUDE.md - rmhdagmaster Project Constitution

**Last Updated**: 06 FEB 2026

---

## Project Identity

**rmhdagmaster** is the DAG-based workflow orchestrator for Epoch 5 of the rmhgeoapi platform.

> **Core Philosophy**: Orchestration is its own concern — separate it from execution.

One codebase builds THREE deployment targets:

| Target | Base | Size | Purpose |
|--------|------|------|---------|
| **Function App** | Azure Functions | ~50MB | Gateway: job submission, status queries, HTTP proxy |
| **DAG Orchestrator** | python:3.12-slim | ~250MB | Coordination: main loop, heartbeat, orphan recovery |
| **DAG Worker** | osgeo/gdal:ubuntu-full | ~2-3GB | Execution: GDAL processing, heavy ETL |

For architecture details, see `docs/ARCHITECTURE.md`.

---

## Design Principles

1. **Orchestrator purity** — The orchestrator coordinates, never executes business logic.

2. **Worker idempotency** — Any task can be retried safely; workers handle duplicate delivery.

3. **State lives in Postgres** — Workers are stateless; all durable state is in the database.

4. **Repository pattern** — All database access goes through repository classes. No raw `psycopg.connect` outside the infrastructure layer.

5. **Pydantic is the schema** — Models are the single source of truth for data structures. All serialization crosses boundaries via `.model_dump()` / `.model_validate()`—never raw JSON/dict manipulation. Pydantic V2 only.

6. **Explicit over implicit** — Dependencies, queues, and handlers are declared in YAML. No magic.

7. **Fail fast, recover gracefully** — Errors surface immediately; system resumes from last known state.

8. **Observable by default** — Every state transition logs a JobEvent.

9. **Defensive concurrency** — PostgreSQL advisory locks prevent multi-instance conflicts. Optimistic locking (version columns) detects concurrent modifications. See `docs/IMPLEMENTATION.md` Section 12.

---

## Code Patterns

### Database Access (CRITICAL)

**ALL database queries MUST use `dict_row` factory.** Never use tuple indexing.

```python
# CORRECT - Always use dict_row and access by column name
async with self.pool.connection() as conn:
    conn.row_factory = dict_row
    result = await conn.execute(
        "SELECT COUNT(*) as count FROM table WHERE ...",
        (params,),
    )
    row = await result.fetchone()
    return row["count"]  # Access by name

# WRONG - Never use tuple indexing
return row[0]  # Will break if row_factory changes
```

### Named Parameters with Auto-Json

```python
# CORRECT - Named parameters with auto-wrapping
params = self._build_named_params({
    'job_id': job_id,
    'status': status.value,
    'result_data': result_data,  # Auto-wrapped with Json()
})
await conn.execute("""
    UPDATE dagapp.dag_jobs
    SET status = %(status)s, result_data = %(result_data)s
    WHERE job_id = %(job_id)s
""", params)

# WRONG - Positional parameters (causes serialization bugs)
await conn.execute(
    "UPDATE ... SET result_data = %s WHERE job_id = %s",
    (result_data, job_id)  # result_data NOT wrapped - FAILS
)
```

### Pydantic V2

All models use Pydantic V2 patterns:
- `model_config = {...}` (not `class Config:`)
- `@field_validator` (not `@validator`)
- `@computed_field` for derived properties
- `.model_dump()` (not `.dict()`)
- `.model_validate()` for deserialization

### Jinja2 Template Key Names (CRITICAL)

**Do NOT use Python dict method names as input/output keys in templates.**

Jinja2 resolves `{{ dict.items }}` as the built-in `dict.items()` method, NOT as `dict["items"]`. Reserved names that will break:

`items`, `keys`, `values`, `get`, `update`, `pop`, `clear`, `copy`, `setdefault`

```yaml
# WRONG - "items" shadows dict.items() method
params:
  items: "{{ inputs.items }}"

# CORRECT - use a non-conflicting name
params:
  item_list: "{{ inputs.item_list }}"
```

This affects `{{ inputs.* }}`, `{{ nodes.*.output.* }}`, and fan-out `source` expressions.

### Logging

```python
from core.logging import get_logger

logger = get_logger(__name__)
```

Environment: `LOG_LEVEL` (DEBUG/INFO/WARNING/ERROR), `LOG_FORMAT=json` for production.

---

## Schema Evolution (Development)

**Pydantic models are the single source of truth for database schema.**

### The Pattern

Models define SQL metadata via `ClassVar` attributes:

```python
class Job(JobData):
    # SQL DDL METADATA (ClassVar = not a model field)
    __sql_table__: ClassVar[str] = "dag_jobs"
    __sql_schema__: ClassVar[str] = "dagapp"
    __sql_primary_key__: ClassVar[List[str]] = ["job_id"]
    __sql_indexes__: ClassVar[List[tuple]] = [
        ("idx_dag_jobs_status", ["status"]),
    ]

    # Model fields as normal
    job_id: str = Field(...)
    status: JobStatus = Field(default=JobStatus.PENDING)
```

### Development Workflow

When the data model changes:

```
1. Update Pydantic model     →  core/models/*.py
2. Regenerate DDL            →  PydanticToSQL.execute(conn)
3. Postgres is now aligned   →  Schema matches Python
```

### Regenerate Schema (Dev Only)

```python
from core.schema import PydanticToSQL

generator = PydanticToSQL(schema_name="dagapp")

# Dry run - see what would execute
generator.execute(conn, dry_run=True)

# Execute DDL
generator.execute(conn, dry_run=False)
```

### Production Deployments

For QA/UAT/PROD, generate DDL scripts for DBAs:

```python
# Export DDL to file
ddl_statements = generator.generate_all_ddl()
with open("migrations/v0.2.0.sql", "w") as f:
    f.write(ddl_statements)
```

DBAs review and execute the migration script manually.

---

## Bootstrap API (Schema Management)

The `/api/v1/bootstrap/*` endpoints manage database schema lifecycle.

### Endpoint Summary

| Endpoint              | Behavior                    | Destructive? | Use Case |
|-----------------------|-----------------------------|--------------|----------|
| `GET  /status`        | Check schema status         | No           | Monitoring |
| `GET  /tables`        | List tables and row counts  | No           | Monitoring |
| `GET  /ddl`           | Preview DDL statements      | No           | Review |
| `POST /deploy`        | Create schema (idempotent)  | No           | Initial setup |
| `POST /migrate`       | Add missing columns         | No           | Schema updates |
| `POST /rebuild`       | DROP CASCADE + recreate     | **YES** ⚠️   | Dev only |

### Safety Model

```
deploy  → CREATE IF NOT EXISTS  → Safe, idempotent, run anytime
migrate → ADD COLUMN IF NOT EXISTS → Safe, only adds columns
rebuild → DROP SCHEMA CASCADE   → DESTROYS ALL DATA (dev only)
```

### Development Workflow

When you change a Pydantic model and need a fresh schema:

```bash
# Option 1: Full rebuild (destroys data)
curl -X POST "https://host/api/v1/bootstrap/rebuild?confirm=DESTROY"
# Requires: ALLOW_DESTRUCTIVE_BOOTSTRAP=true in environment

# Option 2: Add new columns only (preserves data)
curl -X POST "https://host/api/v1/bootstrap/migrate?confirm=yes"
```

### Production Workflow

1. Use `GET /ddl` to preview statements
2. Use `POST /deploy?dry_run=true` to test
3. Use `POST /deploy?confirm=yes` for initial deployment
4. Use `POST /migrate?confirm=yes` for adding new columns

**IMPORTANT**: The `/rebuild` endpoint must be REMOVED before UAT/Production.
Search for `ALLOW_DESTRUCTIVE_BOOTSTRAP` to find and remove it.

### PydanticToSQL Modes

```python
# Safe mode (default) - for deploy
generator = PydanticToSQL(schema_name="dagapp")
# Uses CREATE TYPE IF NOT EXISTS for enums

# Destructive mode - for rebuild
generator = PydanticToSQL(schema_name="dagapp", destructive=True)
# Uses DROP TYPE CASCADE for enums (destroys dependent columns!)
```

---

## Conventions

### File Header Template

```python
# ============================================================================
# CLAUDE CONTEXT - [DESCRIPTIVE_TITLE]
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: [Component type] - [Brief description]
# PURPOSE: [One sentence description]
# LAST_REVIEWED: [DD MMM YYYY]
# ============================================================================
```

### Date Format

**Use military date format**: `28 JAN 2026`

### Git Commit Format

```
Brief description

Co-Authored-By: Claude <noreply@anthropic.com>
```

---

## Quick Reference

### Model Imports

```python
from core.models import Job, NodeState, TaskMessage, TaskResult, JobEvent
from core.models import WorkflowDefinition, NodeDefinition
from core.contracts import JobStatus, NodeStatus, TaskStatus
```

### Status Checks

```python
job.is_terminal           # True if COMPLETED/FAILED/CANCELLED
node.is_terminal          # True if COMPLETED/FAILED/SKIPPED
node.is_successful        # True if COMPLETED/SKIPPED
job.can_transition_to(new_status)  # Validates state transition
```

### Run Locally

```bash
# Function App (Gateway) - requires Azure Functions Core Tools
cp local.settings.json.example local.settings.json  # Edit with your settings
func start

# Orchestrator mode (Docker app)
RUN_MODE=orchestrator python -m uvicorn main:app --host 0.0.0.0 --port 8000

# Worker mode (Docker app)
RUN_MODE=worker WORKER_TYPE=docker WORKER_QUEUE=container-tasks python -m worker.main
```

### Function App Endpoints

```
/api/livez              - Liveness probe (always 200)
/api/readyz             - Readiness probe (checks startup)
/api/gateway/submit     - Submit job to queue
/api/status/job/{id}    - Get job status
/api/status/jobs        - List jobs
/api/admin/health       - Health check with DB connectivity
/api/proxy/{target}/{p} - Forward to orchestrator/worker
```

### Python Environment

```bash
conda activate azgeo
```

---

## Related Documentation

| Document | Purpose |
|----------|---------|
| `docs/ARCHITECTURE.md` | System design, data flows, schemas, patterns, deployment |
| `docs/IMPLEMENTATION.md` | Detailed specs, code examples, task checklists |
| `docs/TODO.md` | Progress tracking and next priorities |
