# DAG Orchestrator (rmhdagmaster)

**EPOCH:** 5 - DAG ORCHESTRATION
**VERSION:** 0.1.4
**STATUS:** Production

---

## Overview

The DAG Orchestrator is the workflow execution engine for the geospatial data platform. It coordinates multi-step processing pipelines, dispatches tasks to workers, and integrates with external B2B applications.

```
┌─────────────────┐      ┌─────────────────┐      ┌─────────────────┐
│   B2B Apps      │      │  rmhdagmaster   │      │   rmhheavyapi   │
│   (DDH, etc.)   │─────►│  (Orchestrator) │─────►│    (Workers)    │
└─────────────────┘      └─────────────────┘      └─────────────────┘
                                │
                                ▼
                         ┌─────────────────┐
                         │ GeospatialAsset │
                         │   (rmhgeoapi)   │
                         └─────────────────┘
```

---

## Quick Start

### Local Development

```bash
# Set environment
export DATABASE_URL="postgresql://user:pass@localhost/postgres"
export USE_MANAGED_IDENTITY=false

# Run
uvicorn main:app --reload --port 8000
```

### Azure Deployment

```bash
# Build and push to ACR
az acr build --registry rmhazureacr --image rmhdagmaster:latest .

# Configure web app
az webapp config container set --name rmhdagmaster \
  --resource-group rmhazure_rg \
  --container-image-name rmhazureacr.azurecr.io/rmhdagmaster:latest
```

---

## Documentation

| Document | Description |
|----------|-------------|
| [ARCHITECTURE.md](docs/ARCHITECTURE.md) | System design, data flows, schemas, patterns, deployment |
| [IMPLEMENTATION.md](docs/IMPLEMENTATION.md) | Detailed specs, code examples, task checklists |
| [TODO.md](docs/TODO.md) | Progress tracking and next priorities |
| [CLAUDE.md](CLAUDE.md) | Project conventions, code patterns, quick reference |

### Critical Concepts

1. **B2B ID Decoupling**: External identifiers (DDH's dataset/resource/version) are stored as metadata, never as primary keys. See ARCHITECTURE.md Section 13.

2. **Revision vs Semantic Version**: We track operational revisions (rollback). B2B apps own semantic versioning (content lineage). These are independent concepts.

3. **Platform Registry**: New B2B integrations require only registry entries, no code changes.

---

## Project Structure

```
rmhdagmaster/
├── main.py                 # FastAPI application entry point
├── __version__.py          # Version info (single source of truth)
├── Dockerfile              # Container definition
├── requirements-orchestrator.txt
│
├── core/                   # Domain models and contracts
│   ├── models.py           # Job, NodeState, TaskMessage
│   └── contracts.py        # Enums (JobStatus, NodeStatus)
│
├── orchestrator/           # Orchestration loop
│   └── loop.py             # Main polling loop, node dispatch
│
├── services/               # Business logic
│   ├── job_service.py      # Job CRUD operations
│   ├── node_service.py     # Node state management
│   └── workflow_service.py # Workflow loading/validation
│
├── repositories/           # Data access
│   └── database.py         # Connection pool, managed identity
│
├── messaging/              # Service Bus integration
│   ├── publisher.py        # Task dispatch to workers
│   └── config.py           # Connection configuration
│
├── infrastructure/         # Platform infrastructure
│   ├── auth/               # Managed identity authentication
│   └── database_initializer.py  # Schema bootstrap
│
├── api/                    # HTTP API
│   ├── routes.py           # REST endpoints
│   ├── ui_routes.py        # Admin UI endpoints
│   └── bootstrap_routes.py # Schema deployment endpoints
│
├── workflows/              # Workflow definitions (YAML)
│   ├── echo_test.yaml      # Simple test workflow
│   ├── raster_ingest.yaml  # Raster processing pipeline
│   └── ...
│
├── templates/              # Jinja2 templates (Admin UI)
├── static/                 # CSS, JS assets
├── ui/                     # UI utilities
│
├── ARCHITECTURE.md         # System architecture (consolidated)
├── IMPLEMENTATION.md       # Implementation specs (consolidated)
└── TODO.md                 # Progress tracking
```

---

## Endpoints

### API (prefix: `/api/v1`)

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Health check |
| GET | `/workflows` | List available workflows |
| GET | `/workflows/{id}` | Get workflow definition |
| POST | `/jobs` | Create new job |
| GET | `/jobs` | List jobs (with filters) |
| GET | `/jobs/{id}` | Get job details |
| GET | `/jobs/{id}/nodes` | Get job node states |

### Admin UI (prefix: `/ui`)

| Endpoint | Description |
|----------|-------------|
| `/ui/` | Dashboard |
| `/ui/jobs` | Job list |
| `/ui/jobs/{id}` | Job detail |
| `/ui/workflows` | Workflow list |
| `/ui/health` | System health |

---

## Environment Variables

### Required

| Variable | Description |
|----------|-------------|
| `USE_MANAGED_IDENTITY` | `true` for Azure MI auth |
| `POSTGRES_HOST` | Database hostname |
| `POSTGRES_DB` | Database name |
| `SERVICE_BUS_FQDN` | Service Bus namespace (if MI) |

### Optional

| Variable | Default | Description |
|----------|---------|-------------|
| `ORCHESTRATOR_POLL_INTERVAL` | `1.0` | Polling interval (seconds) |
| `AUTO_BOOTSTRAP_SCHEMA` | `false` | Deploy schema on startup |
| `LOG_LEVEL` | `INFO` | Logging level |

See `docs/ARCHITECTURE.md` Section 15 for complete deployment and configuration details.

---

## Workflows

Workflows are defined in YAML:

```yaml
workflow_id: raster_ingest
name: Raster Ingest Pipeline
version: 1

inputs:
  - name: source_url
    type: string
    required: true

nodes:
  - id: start
    type: start

  - id: validate
    type: task
    handler: validate_raster
    depends_on: [start]

  - id: convert_cog
    type: task
    handler: convert_to_cog
    depends_on: [validate]

  - id: end
    type: end
    depends_on: [convert_cog]
```

---

## Related Projects

| Project | Purpose |
|---------|---------|
| `rmhgeoapi` | Geospatial API, asset management, STAC catalog |
| `rmhheavyapi` | Worker containers (GDAL, heavy processing) |

---

## Version History

| Version | Date | Notes |
|---------|------|-------|
| 0.1.4 | 30 JAN 2026 | Production deployment, managed identity |
| 0.1.0 | 29 JAN 2026 | Initial release |
