# Deployment Guide - rmhdagmaster

**Last Updated**: 02 FEB 2026

---

## Azure Resources

| Resource | Name | Purpose |
|----------|------|---------|
| **ACR** | `rmhazureacr.azurecr.io` | Container registry |
| **Image** | `rmhdagmaster:v0.2.x` | Orchestrator (lightweight) |
| **Image** | `rmhdagworker:v0.2.x` | Worker (heavy with GDAL) |
| **Web App** | `rmhdagmaster` | Orchestrator + API + UI |
| **Web App** | `rmhdagworker` | Heavy worker (GDAL tasks) |
| **PostgreSQL** | `rmhpostgres` | Shared database |
| **Service Bus** | `rmhazure` | Task queues |

---

## Web App URLs

| App | URL |
|-----|-----|
| Orchestrator | `https://rmhdagmaster-gcfzd5bqfxc7g7cv.eastus-01.azurewebsites.net` |
| Worker | `https://rmhdagworker-fedshwfme6drd6gq.eastus-01.azurewebsites.net` |

---

## Environment Variables

### Orchestrator (rmhdagmaster)

```bash
RUN_MODE=orchestrator
LOG_LEVEL=INFO
LOG_FORMAT=json
USE_MANAGED_IDENTITY=true
DB_ADMIN_MANAGED_IDENTITY_CLIENT_ID=a533cb80-a590-4fad-8e52-1eb1f72659d7
POSTGRES_HOST=rmhpostgres.postgres.database.azure.com
POSTGRES_DB=geopgflex
SERVICE_BUS_FQDN=rmhazure.servicebus.windows.net
ORCHESTRATOR_POLL_INTERVAL=5
```

### Worker (rmhdagworker)

```bash
RUN_MODE=worker
WORKER_TYPE=docker
WORKER_QUEUE=container-tasks
LOG_LEVEL=INFO
LOG_FORMAT=json
USE_MANAGED_IDENTITY=true
DB_ADMIN_MANAGED_IDENTITY_CLIENT_ID=a533cb80-a590-4fad-8e52-1eb1f72659d7
POSTGRES_HOST=rmhpostgres.postgres.database.azure.com
POSTGRES_DB=geopgflex
SERVICE_BUS_FQDN=rmhazure.servicebus.windows.net
DAG_CALLBACK_URL=https://rmhdagmaster-gcfzd5bqfxc7g7cv.eastus-01.azurewebsites.net/api/v1/callbacks/task-result
```

---

## Connection Details

### Database

```
Host:     rmhpostgres.postgres.database.azure.com
Database: geopgflex
Schema:   dagapp
Auth:     Managed Identity (Entra ID)
Client:   a533cb80-a590-4fad-8e52-1eb1f72659d7 (DB_ADMIN_MANAGED_IDENTITY_CLIENT_ID)
SSL:      Required
```

### Service Bus

```
Namespace:  rmhazure.servicebus.windows.net
Auth:       Managed Identity
Queues:
  - dag-jobs           (job submission)
  - functionapp-tasks  (light work)
  - container-tasks    (heavy work - GDAL)
```

### Container Registry

```
Registry:   rmhazureacr.azurecr.io
Images:
  - rmhdagmaster:v0.2.x  (orchestrator - lightweight)
  - rmhdagworker:v0.2.x  (worker - heavy with GDAL)
```

---

## Build Commands

### Build Images

```bash
# Build orchestrator (lightweight ~250MB)
az acr build --registry rmhazureacr \
  --image rmhdagmaster:v0.2.0.0 \
  -f Dockerfile .

# Build worker (heavy ~2-3GB with GDAL)
az acr build --registry rmhazureacr \
  --image rmhdagworker:v0.2.0.0 \
  -f Dockerfile.worker .
```

### Deploy to Web Apps

```bash
# Update orchestrator
az webapp config container set --name rmhdagmaster --resource-group rmhazure_rg \
  --container-image-name rmhazureacr.azurecr.io/rmhdagmaster:v0.2.0.0

# Update worker
az webapp config container set --name rmhdagworker --resource-group rmhazure_rg \
  --container-image-name rmhazureacr.azurecr.io/rmhdagworker:v0.2.0.0

# Restart to pick up new images
az webapp restart --name rmhdagmaster --resource-group rmhazure_rg
az webapp restart --name rmhdagworker --resource-group rmhazure_rg
```

---

## Two-Image Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          SHARED CODEBASE                                     │
├─────────────────────────────────────────────────────────────────────────────┤
│   core/           # Pydantic models, contracts, logging                      │
│   repositories/   # Database access (psycopg3, dict_row)                     │
│   services/       # Business logic                                           │
│   handlers/       # Task execution logic                                     │
│   worker/         # Service Bus consumer                                     │
│   orchestrator/   # DAG evaluation, dispatch                                 │
│   messaging/      # Service Bus publisher                                    │
└─────────────────────────────────────────────────────────────────────────────┘
                              │
              ┌───────────────┴───────────────┐
              ▼                               ▼
┌─────────────────────────────┐   ┌─────────────────────────────┐
│  Dockerfile                 │   │  Dockerfile.worker          │
│  (Orchestrator - Light)     │   │  (Worker - Heavy GDAL)      │
├─────────────────────────────┤   ├─────────────────────────────┤
│  Base: python:3.12-slim     │   │  Base: osgeo/gdal:full      │
│  Size: ~250MB               │   │  Size: ~2-3GB               │
│  No GDAL                    │   │  GDAL + rasterio            │
│  FastAPI + Loop             │   │  Consumer + Health          │
└─────────────────────────────┘   └─────────────────────────────┘
              │                               │
              ▼                               ▼
       rmhdagmaster:latest            rmhdagworker:latest
```

| Concern | Orchestrator | Worker |
|---------|--------------|--------|
| **Purpose** | Coordinate jobs, serve API/UI | Execute geo tasks |
| **Base Image** | `python:3.12-slim` | `osgeo/gdal:ubuntu-full` |
| **Size** | ~250MB | ~2-3GB |
| **GDAL/Rasterio** | NO | YES |
| **HTTP Server** | FastAPI (full) | aiohttp (health only) |
| **Scaling** | 1 instance | Auto-scale 0-N |

---

## Deployment Topology

```
                              GitHub Push
                                   │
                                   ▼
                    ┌──────────────────────────────────────┐
                    │  ACR Build (Two Images)              │
                    │  az acr build -f Dockerfile          │  → rmhdagmaster:latest
                    │  az acr build -f Dockerfile.worker   │  → rmhdagworker:latest
                    └──────────────────────────────────────┘
                                   │
                    ┌──────────────┴──────────────┐
                    │    Azure Container Registry │
                    │    rmhazureacr.azurecr.io   │
                    │    ├── rmhdagmaster:v0.2.x  │
                    │    └── rmhdagworker:v0.2.x  │
                    └──────────────┬──────────────┘
                                   │
                    ┌──────────────┴──────────────┐
                    ▼                             ▼
     ┌──────────────────────────┐   ┌──────────────────────────┐
     │  rmhdagmaster            │   │  rmhdagworker            │
     │  (Azure Web App)         │   │  (Azure Web App)         │
     ├──────────────────────────┤   ├──────────────────────────┤
     │  RUN_MODE=orchestrator   │   │  RUN_MODE=worker         │
     │  Always-on (1 instance)  │   │  WORKER_TYPE=docker      │
     │                          │   │  WORKER_QUEUE=           │
     │  • Owns job state        │   │    container-tasks       │
     │  • Evaluates DAG         │   │                          │
     │  • Dispatches to workers │   │  • Listens to queue      │
     │  • FastAPI endpoints     │   │  • Executes handlers     │
     │                          │   │  • Reports results to DB │
     └──────────────────────────┘   └──────────────────────────┘
```

---

## Migration Strategy

### Parallel Operation Phases

```
Phase 1: Parallel (Current)
─────────────────────────────────────
  • Both systems operational
  • New jobs can go to either system
  • Feature flag controls routing
  • app schema + dagapp schema both active

Phase 2: DAG Primary
──────────────────────────────
  • DAG handles majority of jobs
  • Legacy only for edge cases
  • Monitor for issues

Phase 3: Legacy Deprecated
────────────────────────────────────
  • All traffic to DAG
  • Legacy read-only
  • Plan cleanup timeline

Phase 4: Legacy Removed
──────────────────────────────────────
  • Remove legacy code
  • Archive app schema
  • Single system
```
