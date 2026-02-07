# Function App Implementation Plan

**Date**: 04 FEB 2026
**Epoch**: 5 - DAG ORCHESTRATION
**Status**: 🔄 REVISION NEEDED (Request/Job Separation Pattern)

---

## Design Philosophy: Request/Job Separation

**Critical Principle**: B2B applications know NOTHING about internal processes.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         B2B APPLICATION VIEW                                 │
│                                                                             │
│   B2B App submits:                    Gateway returns:                      │
│   ─────────────────                   ────────────────                      │
│   • workflow_id                       • request_id  ← Gateway generates     │
│   • input_params                      • status: "accepted"                  │
│   • submitted_by (their identity)     • submitted_at                        │
│                                                                             │
│   B2B App polls:                      Gateway returns:                      │
│   ──────────────                      ────────────────                      │
│   GET /platform/status/{request_id}   • status: pending/running/completed   │
│                                       • result (if completed)               │
│                                       • error (if failed)                   │
│                                                                             │
│   B2B NEVER sees:                                                           │
│   ───────────────                                                           │
│   • job_id (internal)                                                       │
│   • node states (internal)                                                  │
│   • orchestrator details (internal)                                         │
│   • correlation_id (internal tracking)                                      │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Flow**:
1. B2B submits job request with their identity + parameters
2. Gateway generates `request_id` (UUID), returns immediately
3. Gateway queues job with `request_id` as internal `correlation_id`
4. Orchestrator claims job, creates internal `job_id`
5. B2B polls by `request_id` - Gateway translates to internal lookup
6. B2B receives status/result - never sees `job_id`

---

## Implementation Status

| Category | Status | Notes |
|----------|--------|-------|
| Entry Point | ✅ Done | `function_app.py`, `host.json`, `.funcignore` |
| Configuration | ✅ Done | `function/config.py`, `function/startup.py` |
| Models | 🔄 Update | Need `request_id` pattern, remove B2B-visible `correlation_id` |
| Repositories | ✅ Done | `function/repositories/*.py` (4 files) |
| Blueprints | 🔄 Update | Rename to `/platform/*`, implement `request_id` generation |
| Local Testing | ⏳ TODO | Run `func start` and verify endpoints |
| Azure Deployment | ⏳ TODO | Deploy to Azure Function App |
| RBAC Configuration | ⏳ TODO | Service Bus sender, PostgreSQL read access |

### What Remains

1. **Local Testing**
   - Copy `local.settings.json.example` to `local.settings.json`
   - Configure database and Service Bus credentials
   - Run `func start` and test all 22 endpoints

2. **RBAC Configuration**
   - Assign `Azure Service Bus Data Sender` role to function app identity
   - Configure PostgreSQL access for read-only queries
   - Set up managed identity (user-assigned recommended)

3. **Azure Deployment**
   - Create Azure Function App resource
   - Deploy using `func azure functionapp publish`
   - Verify `.funcignore` excludes heavy modules correctly

---

## Overview

Create a lightweight Azure Function App that serves as the **B2B Gateway and ACL** for the DAG orchestration platform.

### Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              B2B APPLICATIONS                                │
│                     (DDH, ArcGIS, Portal, Future Apps)                       │
└────────────────────────────────┬────────────────────────────────────────────┘
                                 │
                                 │  POST /platform/submit
                                 │  GET  /platform/status/{request_id}
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         FUNCTION APP (Gateway/ACL)                           │
│                                                                             │
│   • Generates request_id (B2B tracking reference)                           │
│   • Validates requests, enforces ACL                                        │
│   • Queues jobs to Service Bus                                              │
│   • Provides status polling (translates request_id → job lookup)            │
│   • Hides all internal details from B2B                                     │
│                                                                             │
└────────────────────────────────┬────────────────────────────────────────────┘
                                 │
                                 │  dag-jobs Queue
                                 │  (correlation_id = request_id)
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         ORCHESTRATOR (Docker)                                │
│                                                                             │
│   • Claims jobs from queue (competing consumers)                            │
│   • Creates internal job_id                                                 │
│   • Manages DAG execution                                                   │
│   • Dispatches tasks to workers                                             │
│                                                                             │
└────────────────────────────────┬────────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           WORKERS (Docker)                                   │
│                                                                             │
│   • Execute handlers (GDAL, transforms, etc.)                               │
│   • Report results back to database                                         │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Deployment Targets (One Codebase)

| Target | Base Image | Size | Purpose |
|--------|-----------|------|---------|
| **Function App** | Azure Functions runtime | ~50MB | B2B Gateway, ACL, status queries |
| **DAG Orchestrator** | python:3.12-slim | ~250MB | Orchestration loop, job management |
| **DAG Worker** | osgeo/gdal:ubuntu-full | ~2-3GB | GDAL processing, heavy ETL |

### Function App Responsibilities

1. **B2B Gateway** - Single entry point for all B2B applications
2. **Request ID Generation** - Create tracking IDs for B2B (they provide nothing)
3. **Status Translation** - Convert internal job state to B2B-friendly response
4. **Abstraction Layer** - Hide job_id, nodes, orchestrator details from B2B

### What the Function App Does NOT Do

- **Authentication** - Handled by Azure Easy Auth (AAD)
- **Authorization/ACL** - Handled by Azure Easy Auth roles/claims
- **Expose internals** - No job_id, nodes, events visible to B2B

### Production vs Development

| Environment | Endpoints Available |
|-------------|---------------------|
| **Production** | `/api/platform/*` only |
| **Development** | `/api/platform/*` + `/api/ops/*` (temporary) |

The `/api/ops/*` endpoints are for development debugging only and will be removed before UAT.

---

## Design Principles

Following patterns from rmhgeoapi:

1. **Pydantic V2 Only** - All models use `model_dump()`, `model_validate()`, `ConfigDict`
2. **Repository Pattern** - All database access through repository classes
3. **dict_row Factory** - Never use tuple indexing for database results
4. **Blueprint Organization** - Group related endpoints in blueprints
5. **Conditional Registration** - Register blueprints based on app mode/config
6. **Startup Validation** - Validate environment before registering triggers
7. **Thin Tracking** - Minimal state in function app, delegate to orchestrator

---

## File Structure

```
rmhdagmaster/
├── function_app.py              # NEW - Azure Functions V2 entry point
├── host.json                    # NEW - Function app configuration
├── requirements-function.txt    # NEW - Minimal function dependencies
├── .funcignore                  # NEW - Exclude heavy modules
│
├── function/                    # NEW - Function app specific code
│   ├── __init__.py
│   ├── config.py               # Function app configuration
│   ├── startup.py              # Startup validation
│   │
│   ├── blueprints/             # HTTP endpoint blueprints
│   │   ├── __init__.py
│   │   ├── gateway_bp.py       # Job submission endpoints
│   │   ├── status_bp.py        # Status query endpoints
│   │   ├── admin_bp.py         # Admin/maintenance endpoints
│   │   └── proxy_bp.py         # HTTP proxy to Docker apps
│   │
│   ├── models/                 # Function-specific Pydantic models
│   │   ├── __init__.py
│   │   ├── requests.py         # API request models
│   │   ├── responses.py        # API response models
│   │   └── health.py           # Health check models
│   │
│   └── repositories/           # Read-only database access
│       ├── __init__.py
│       ├── base.py             # PostgreSQL base repository
│       ├── job_query_repo.py   # Job queries (read-only)
│       ├── node_query_repo.py  # Node queries (read-only)
│       ├── event_query_repo.py # Event queries (read-only)
│       └── workflow_query_repo.py  # Workflow queries
│
├── gateway/                    # EXISTING - Can be reused/adapted
│   ├── models.py              # JobSubmitRequest, JobSubmitResponse
│   └── routes.py              # FastAPI routes (reference)
│
├── core/models/               # EXISTING - Shared models
│   ├── job.py                 # Job model
│   ├── node.py                # NodeState model
│   ├── job_queue_message.py   # Queue message model
│   └── events.py              # JobEvent model
│
└── infrastructure/            # EXISTING - Can import ServiceBusPublisher
    └── service_bus.py         # ServiceBusPublisher (singleton)
```

---

## 1. Function App Entry Point

### `function_app.py`

```python
# ============================================================================
# RMHDAGMASTER - Azure Function App (Gateway)
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Gateway - Job submission and status queries
# PURPOSE: API Gateway for DAG orchestration platform
# CREATED: 04 FEB 2026
# ============================================================================
"""
RMH DAG Gateway Function App

Azure Functions V2 entry point providing:
- Job submission to dag-jobs queue
- Status queries (jobs, nodes, events)
- Health check proxying to Docker apps
- Administrative endpoints

Deployment: Azure Function App (Consumption or Premium)
"""

import azure.functions as func
import logging
import os

# Create app first (before any imports that might fail)
app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

logger = logging.getLogger(__name__)

# ============================================================================
# EARLY PROBES (Before validation - always available)
# ============================================================================

@app.route(route="livez", methods=["GET"])
def liveness_probe(req: func.HttpRequest) -> func.HttpResponse:
    """Liveness probe - always returns 200 if function is running."""
    return func.HttpResponse('{"alive": true}', status_code=200,
                             headers={"Content-Type": "application/json"})


@app.route(route="readyz", methods=["GET"])
def readiness_probe(req: func.HttpRequest) -> func.HttpResponse:
    """Readiness probe - returns 200 if startup validation passed."""
    from function.startup import STARTUP_STATE

    if STARTUP_STATE.all_passed:
        return func.HttpResponse(
            '{"ready": true}',
            status_code=200,
            headers={"Content-Type": "application/json"}
        )

    import json
    return func.HttpResponse(
        json.dumps({
            "ready": False,
            "failed_checks": STARTUP_STATE.failed_check_names()
        }),
        status_code=503,
        headers={"Content-Type": "application/json"}
    )


# ============================================================================
# STARTUP VALIDATION
# ============================================================================

from function.startup import validate_startup, STARTUP_STATE

_startup_result = validate_startup()

if not STARTUP_STATE.all_passed:
    logger.error("❌ Startup validation FAILED - limited endpoints available")
    for check in STARTUP_STATE.failed_checks():
        logger.error(f"   - {check.name}: {check.error_message}")
else:
    logger.info("✅ Startup validation PASSED")


# ============================================================================
# BLUEPRINT REGISTRATION (Conditional on startup success)
# ============================================================================

if STARTUP_STATE.all_passed:
    # Gateway endpoints (job submission)
    from function.blueprints.gateway_bp import gateway_bp
    app.register_functions(gateway_bp)
    logger.info("✅ Gateway blueprint registered")

    # Status query endpoints
    from function.blueprints.status_bp import status_bp
    app.register_functions(status_bp)
    logger.info("✅ Status blueprint registered")

    # Admin/maintenance endpoints
    from function.blueprints.admin_bp import admin_bp
    app.register_functions(admin_bp)
    logger.info("✅ Admin blueprint registered")

    # Proxy endpoints (forward to Docker apps)
    from function.blueprints.proxy_bp import proxy_bp
    app.register_functions(proxy_bp)
    logger.info("✅ Proxy blueprint registered")
else:
    logger.warning("⏭️ SKIPPING blueprint registration - startup validation failed")
    logger.warning("   Only /livez and /readyz endpoints available")


# ============================================================================
# EXPORTS
# ============================================================================

__all__ = ["app"]
```

---

## 2. Startup Validation

### `function/startup.py`

```python
"""
Startup Validation

Validates environment and dependencies before registering blueprints.
Pattern from rmhgeoapi: fail fast, log clearly, degrade gracefully.
"""

import logging
import os
from dataclasses import dataclass, field
from typing import List, Optional

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """Result of a startup validation check."""
    name: str
    passed: bool
    error_type: Optional[str] = None
    error_message: Optional[str] = None


@dataclass
class StartupState:
    """Track all startup validation checks."""
    env_vars: ValidationResult = field(default_factory=lambda: ValidationResult("env_vars", False))
    database: ValidationResult = field(default_factory=lambda: ValidationResult("database", False))
    service_bus: ValidationResult = field(default_factory=lambda: ValidationResult("service_bus", False))

    @property
    def all_passed(self) -> bool:
        return all([
            self.env_vars.passed,
            self.database.passed,
            self.service_bus.passed,
        ])

    def failed_checks(self) -> List[ValidationResult]:
        checks = [self.env_vars, self.database, self.service_bus]
        return [c for c in checks if not c.passed]

    def failed_check_names(self) -> List[str]:
        return [c.name for c in self.failed_checks()]


# Global singleton
STARTUP_STATE = StartupState()


def validate_startup() -> bool:
    """
    Run all startup validation checks.

    Returns True if all checks pass.
    """
    global STARTUP_STATE

    # 1. Environment Variables
    STARTUP_STATE.env_vars = _validate_env_vars()

    # 2. Database Connectivity (only if env vars passed)
    if STARTUP_STATE.env_vars.passed:
        STARTUP_STATE.database = _validate_database()

    # 3. Service Bus Configuration
    STARTUP_STATE.service_bus = _validate_service_bus()

    return STARTUP_STATE.all_passed


def _validate_env_vars() -> ValidationResult:
    """Validate required environment variables."""
    required = [
        "DATABASE_URL",  # or POSTGIS_HOST + POSTGIS_DATABASE
    ]

    # Check for DATABASE_URL or individual components
    has_database_url = bool(os.environ.get("DATABASE_URL"))
    has_postgis_components = all([
        os.environ.get("POSTGIS_HOST"),
        os.environ.get("POSTGIS_DATABASE"),
    ])

    if not (has_database_url or has_postgis_components):
        return ValidationResult(
            name="env_vars",
            passed=False,
            error_type="MissingEnvVar",
            error_message="DATABASE_URL or POSTGIS_HOST+POSTGIS_DATABASE required"
        )

    # Check Service Bus config
    has_sb_namespace = bool(os.environ.get("SERVICE_BUS_NAMESPACE"))
    has_sb_conn_string = bool(os.environ.get("SERVICE_BUS_CONNECTION_STRING"))

    if not (has_sb_namespace or has_sb_conn_string):
        return ValidationResult(
            name="env_vars",
            passed=False,
            error_type="MissingEnvVar",
            error_message="SERVICE_BUS_NAMESPACE or SERVICE_BUS_CONNECTION_STRING required"
        )

    return ValidationResult(name="env_vars", passed=True)


def _validate_database() -> ValidationResult:
    """Validate database connectivity."""
    try:
        from function.repositories.base import FunctionRepository

        repo = FunctionRepository()
        # Simple connectivity test
        result = repo.execute_scalar("SELECT 1")

        if result == 1:
            return ValidationResult(name="database", passed=True)
        else:
            return ValidationResult(
                name="database",
                passed=False,
                error_type="DatabaseError",
                error_message=f"Unexpected result: {result}"
            )
    except Exception as e:
        return ValidationResult(
            name="database",
            passed=False,
            error_type=type(e).__name__,
            error_message=str(e)
        )


def _validate_service_bus() -> ValidationResult:
    """Validate Service Bus configuration (not connectivity - too slow)."""
    # Just check config is present
    has_namespace = bool(os.environ.get("SERVICE_BUS_NAMESPACE"))
    has_conn_string = bool(os.environ.get("SERVICE_BUS_CONNECTION_STRING"))

    if has_namespace or has_conn_string:
        return ValidationResult(name="service_bus", passed=True)

    return ValidationResult(
        name="service_bus",
        passed=False,
        error_type="MissingConfig",
        error_message="No Service Bus configuration found"
    )
```

---

## 3. Pydantic Models

### `function/models/requests.py`

```python
"""
API Request Models

Pydantic V2 models for incoming API requests.

Design Principle: B2B apps provide ONLY their identity and job parameters.
They do not provide tracking IDs - the Gateway generates all internal references.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, ConfigDict


class PlatformSubmitRequest(BaseModel):
    """
    Request to submit a job via the platform API.

    B2B applications submit:
    - What workflow to run
    - Parameters for the workflow
    - Their identity (who is submitting)
    - Optional callback URL for completion notification

    B2B applications DO NOT provide:
    - request_id (Gateway generates this)
    - correlation_id (internal tracking)
    - job_id (internal, created by orchestrator)
    """

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "workflow_id": "raster_ingest",
                "input_params": {
                    "container_name": "bronze",
                    "blob_name": "data/satellite.tif"
                },
                "submitted_by": "DDH",
            }
        }
    )

    workflow_id: str = Field(
        ...,
        max_length=64,
        description="ID of the workflow to execute",
    )
    input_params: Dict[str, Any] = Field(
        default_factory=dict,
        description="Parameters passed to the workflow",
    )
    submitted_by: str = Field(
        ...,
        max_length=64,
        description="Identity of the submitting B2B application (e.g., 'DDH', 'ArcGIS')",
    )
    callback_url: Optional[str] = Field(
        default=None,
        max_length=512,
        description="URL to POST completion notification (webhook)",
    )
    priority: int = Field(
        default=0,
        ge=0,
        le=10,
        description="Priority (0=normal, 10=highest)",
    )
    idempotency_key: Optional[str] = Field(
        default=None,
        max_length=128,
        description="Optional key to prevent duplicate submissions. "
                    "If provided, resubmitting with same key returns existing request_id.",
    )


class BatchSubmitRequest(BaseModel):
    """Request to submit multiple jobs."""

    jobs: List[JobSubmitRequest] = Field(
        ...,
        min_length=1,
        max_length=100,
        description="List of jobs to submit (max 100)",
    )


class JobQueryRequest(BaseModel):
    """Query parameters for job listing."""

    status: Optional[str] = Field(default=None, description="Filter by status")
    workflow_id: Optional[str] = Field(default=None, description="Filter by workflow")
    limit: int = Field(default=50, ge=1, le=500, description="Max results")
    offset: int = Field(default=0, ge=0, description="Pagination offset")
    include_nodes: bool = Field(default=False, description="Include node states")


class ProxyRequest(BaseModel):
    """Request to proxy to Docker app."""

    target: str = Field(
        ...,
        pattern=r"^(orchestrator|worker)$",
        description="Target service: orchestrator or worker",
    )
    path: str = Field(
        ...,
        description="Path to forward (e.g., /health, /api/v1/jobs)",
    )
    method: str = Field(
        default="GET",
        pattern=r"^(GET|POST|PUT|DELETE|PATCH)$",
        description="HTTP method",
    )
    body: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Request body for POST/PUT/PATCH",
    )
    headers: Optional[Dict[str, str]] = Field(
        default=None,
        description="Additional headers to forward",
    )
```

### `function/models/responses.py`

```python
"""
API Response Models

Pydantic V2 models for API responses.

Design Principle: B2B apps receive ONLY what they need to track their request.
Internal details (job_id, node states, orchestrator info) are never exposed.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, ConfigDict


class PlatformSubmitResponse(BaseModel):
    """
    Response after successful job submission.

    The request_id is the ONLY identifier B2B apps need.
    They use this to poll for status and retrieve results.
    """

    request_id: str = Field(
        ...,
        description="Unique request identifier. Use this to poll for status.",
    )
    workflow_id: str = Field(..., description="Workflow that will be executed")
    submitted_at: datetime = Field(..., description="When the request was accepted")
    status: str = Field(
        default="accepted",
        description="Request status: 'accepted' means queued for processing",
    )


class PlatformBatchSubmitResponse(BaseModel):
    """Response for batch job submission."""

    submitted: int = Field(..., description="Requests successfully accepted")
    failed: int = Field(..., description="Requests that failed")
    request_ids: List[str] = Field(..., description="Request IDs for tracking")
    errors: List[str] = Field(default_factory=list, description="Error messages for failed requests")


class PlatformStatusResponse(BaseModel):
    """
    Status response for B2B polling.

    This is what B2B apps see when they poll /platform/status/{request_id}.
    Internal details (job_id, node states, orchestrator) are hidden.
    """

    request_id: str = Field(..., description="The request identifier")
    workflow_id: str = Field(..., description="Workflow being executed")
    status: str = Field(
        ...,
        description="Status: pending, running, completed, failed",
    )
    submitted_at: datetime = Field(..., description="When request was accepted")
    completed_at: Optional[datetime] = Field(
        default=None,
        description="When processing completed (if terminal)",
    )

    # Results (only populated on completion)
    result: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Result data if completed successfully",
    )
    error: Optional[str] = Field(
        default=None,
        description="Error message if failed",
    )

    # Progress (optional, for long-running jobs)
    progress: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Progress information (e.g., {'percent': 75, 'stage': 'processing'})",
    )


# =============================================================================
# INTERNAL/ADMIN MODELS (Not exposed to B2B apps)
# =============================================================================

class InternalJobStatusResponse(BaseModel):
    """
    Internal job status - for admin/debugging only.
    NOT exposed through /platform/* endpoints.
    """

    job_id: str
    workflow_id: str
    status: str
    correlation_id: Optional[str] = None  # Links to request_id
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    owner_id: Optional[str] = None
    nodes: Optional[List[Dict[str, Any]]] = None


class InternalJobListResponse(BaseModel):
    """Internal job listing - for admin only."""

    jobs: List[InternalJobStatusResponse]
    total: int
    limit: int
    offset: int


class InternalNodeStatusResponse(BaseModel):
    """Internal node status - for admin/debugging only."""

    node_id: str
    job_id: str
    node_name: str
    status: str
    handler_name: str
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    result_data: Optional[Dict[str, Any]] = None


class InternalEventListResponse(BaseModel):
    """Internal event listing - for admin/debugging only."""

    events: List[Dict[str, Any]]
    total: int


class HealthResponse(BaseModel):
    """Health check response."""

    status: str = Field(default="healthy")
    service: str = Field(default="rmhdagmaster-gateway")
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    version: str = Field(default="0.9.0")
    checks: Dict[str, bool] = Field(default_factory=dict)


class ProxyResponse(BaseModel):
    """Response from proxied request."""

    success: bool
    target: str
    path: str
    status_code: int
    body: Optional[Any] = None
    error: Optional[str] = None


class ErrorResponse(BaseModel):
    """Standard error response."""

    error: str
    details: Optional[str] = None
    code: Optional[str] = None
```

---

## 4. Repository Pattern (Read-Only)

### `function/repositories/base.py`

```python
"""
Base Repository for Function App

Lightweight PostgreSQL repository for read-only queries.
Uses psycopg3 with dict_row factory (NEVER tuple indexing).
"""

import logging
import os
from typing import Any, Dict, List, Optional

import psycopg
from psycopg.rows import dict_row

logger = logging.getLogger(__name__)


class FunctionRepository:
    """
    Base repository for function app database access.

    Design Principles:
    - Read-only queries (state changes go through orchestrator)
    - dict_row factory always (never tuple indexing)
    - Connection per query (function app pattern)
    - Managed identity support
    """

    def __init__(self, schema: str = "dagapp"):
        self.schema = schema
        self._conn_string = self._build_connection_string()

    def _build_connection_string(self) -> str:
        """Build connection string from environment."""
        # Priority 1: DATABASE_URL
        if url := os.environ.get("DATABASE_URL"):
            return url

        # Priority 2: Individual components
        host = os.environ.get("POSTGIS_HOST", "localhost")
        port = os.environ.get("POSTGIS_PORT", "5432")
        database = os.environ.get("POSTGIS_DATABASE", "dagmaster")
        user = os.environ.get("POSTGIS_USER", "postgres")
        password = os.environ.get("POSTGIS_PASSWORD", "")

        # TODO: Add managed identity token support
        # For now, use password auth
        if password:
            return f"postgresql://{user}:{password}@{host}:{port}/{database}?sslmode=require"
        else:
            return f"postgresql://{user}@{host}:{port}/{database}?sslmode=require"

    def _get_connection(self) -> psycopg.Connection:
        """Get a database connection with dict_row factory."""
        conn = psycopg.connect(self._conn_string)
        conn.row_factory = dict_row
        return conn

    def execute_scalar(self, query: str, params: tuple = ()) -> Any:
        """Execute query and return single scalar value."""
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                row = cur.fetchone()
                if row:
                    # dict_row returns dict, get first value
                    return list(row.values())[0]
                return None

    def execute_one(self, query: str, params: tuple = ()) -> Optional[Dict[str, Any]]:
        """Execute query and return single row as dict."""
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                return cur.fetchone()

    def execute_many(self, query: str, params: tuple = ()) -> List[Dict[str, Any]]:
        """Execute query and return all rows as list of dicts."""
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                return cur.fetchall()

    def execute_count(self, query: str, params: tuple = ()) -> int:
        """Execute COUNT query and return integer."""
        result = self.execute_scalar(query, params)
        return int(result) if result else 0
```

### `function/repositories/job_query_repo.py`

```python
"""
Job Query Repository

Read-only queries for job status and listing.
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from function.repositories.base import FunctionRepository

logger = logging.getLogger(__name__)


class JobQueryRepository(FunctionRepository):
    """Read-only repository for job queries."""

    TABLE = "dagapp.dag_jobs"

    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get job by ID."""
        query = f"""
            SELECT
                job_id, workflow_id, status,
                input_params, result_data, error_message,
                created_at, started_at, completed_at, updated_at,
                owner_id, owner_heartbeat_at,
                correlation_id, idempotency_key,
                version
            FROM {self.TABLE}
            WHERE job_id = %s
        """
        return self.execute_one(query, (job_id,))

    def get_job_by_correlation_id(self, correlation_id: str) -> Optional[Dict[str, Any]]:
        """Get job by correlation ID."""
        query = f"""
            SELECT * FROM {self.TABLE}
            WHERE correlation_id = %s
            ORDER BY created_at DESC
            LIMIT 1
        """
        return self.execute_one(query, (correlation_id,))

    def get_job_by_idempotency_key(self, key: str) -> Optional[Dict[str, Any]]:
        """Get job by idempotency key."""
        query = f"""
            SELECT * FROM {self.TABLE}
            WHERE idempotency_key = %s
        """
        return self.execute_one(query, (key,))

    def list_jobs(
        self,
        status: Optional[str] = None,
        workflow_id: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """List jobs with optional filters."""
        conditions = []
        params = []

        if status:
            conditions.append("status = %s")
            params.append(status)

        if workflow_id:
            conditions.append("workflow_id = %s")
            params.append(workflow_id)

        where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""

        query = f"""
            SELECT
                job_id, workflow_id, status,
                created_at, started_at, completed_at,
                error_message, owner_id
            FROM {self.TABLE}
            {where_clause}
            ORDER BY created_at DESC
            LIMIT %s OFFSET %s
        """
        params.extend([limit, offset])

        return self.execute_many(query, tuple(params))

    def count_jobs(
        self,
        status: Optional[str] = None,
        workflow_id: Optional[str] = None,
    ) -> int:
        """Count jobs with optional filters."""
        conditions = []
        params = []

        if status:
            conditions.append("status = %s")
            params.append(status)

        if workflow_id:
            conditions.append("workflow_id = %s")
            params.append(workflow_id)

        where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""

        query = f"""
            SELECT COUNT(*) as count
            FROM {self.TABLE}
            {where_clause}
        """

        return self.execute_count(query, tuple(params))

    def get_job_stats(self) -> Dict[str, int]:
        """Get job counts by status."""
        query = f"""
            SELECT status, COUNT(*) as count
            FROM {self.TABLE}
            GROUP BY status
        """
        rows = self.execute_many(query)
        return {row["status"]: row["count"] for row in rows}

    def get_active_jobs(self, owner_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get jobs that are currently being processed."""
        if owner_id:
            query = f"""
                SELECT * FROM {self.TABLE}
                WHERE status IN ('pending', 'running')
                  AND owner_id = %s
                ORDER BY created_at ASC
            """
            return self.execute_many(query, (owner_id,))
        else:
            query = f"""
                SELECT * FROM {self.TABLE}
                WHERE status IN ('pending', 'running')
                ORDER BY created_at ASC
            """
            return self.execute_many(query)
```

### `function/repositories/node_query_repo.py`

```python
"""
Node Query Repository

Read-only queries for node states.
"""

from typing import Any, Dict, List, Optional

from function.repositories.base import FunctionRepository


class NodeQueryRepository(FunctionRepository):
    """Read-only repository for node state queries."""

    TABLE = "dagapp.dag_node_states"

    def get_node(self, node_id: str) -> Optional[Dict[str, Any]]:
        """Get node by ID."""
        query = f"SELECT * FROM {self.TABLE} WHERE node_id = %s"
        return self.execute_one(query, (node_id,))

    def get_nodes_for_job(self, job_id: str) -> List[Dict[str, Any]]:
        """Get all nodes for a job."""
        query = f"""
            SELECT * FROM {self.TABLE}
            WHERE job_id = %s
            ORDER BY created_at ASC
        """
        return self.execute_many(query, (job_id,))

    def get_nodes_by_status(self, job_id: str, status: str) -> List[Dict[str, Any]]:
        """Get nodes with specific status for a job."""
        query = f"""
            SELECT * FROM {self.TABLE}
            WHERE job_id = %s AND status = %s
            ORDER BY created_at ASC
        """
        return self.execute_many(query, (job_id, status))

    def count_nodes_by_status(self, job_id: str) -> Dict[str, int]:
        """Get node counts by status for a job."""
        query = f"""
            SELECT status, COUNT(*) as count
            FROM {self.TABLE}
            WHERE job_id = %s
            GROUP BY status
        """
        rows = self.execute_many(query, (job_id,))
        return {row["status"]: row["count"] for row in rows}
```

### `function/repositories/event_query_repo.py`

```python
"""
Event Query Repository

Read-only queries for job events (audit trail).
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

from function.repositories.base import FunctionRepository


class EventQueryRepository(FunctionRepository):
    """Read-only repository for event queries."""

    TABLE = "dagapp.dag_job_events"

    def get_events_for_job(
        self,
        job_id: str,
        limit: int = 100,
        event_type: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Get events for a job."""
        if event_type:
            query = f"""
                SELECT * FROM {self.TABLE}
                WHERE job_id = %s AND event_type = %s
                ORDER BY timestamp DESC
                LIMIT %s
            """
            return self.execute_many(query, (job_id, event_type, limit))
        else:
            query = f"""
                SELECT * FROM {self.TABLE}
                WHERE job_id = %s
                ORDER BY timestamp DESC
                LIMIT %s
            """
            return self.execute_many(query, (job_id, limit))

    def get_recent_events(
        self,
        limit: int = 100,
        event_type: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Get recent events across all jobs."""
        if event_type:
            query = f"""
                SELECT * FROM {self.TABLE}
                WHERE event_type = %s
                ORDER BY timestamp DESC
                LIMIT %s
            """
            return self.execute_many(query, (event_type, limit))
        else:
            query = f"""
                SELECT * FROM {self.TABLE}
                ORDER BY timestamp DESC
                LIMIT %s
            """
            return self.execute_many(query, (limit,))

    def count_events_by_type(self, job_id: Optional[str] = None) -> Dict[str, int]:
        """Get event counts by type."""
        if job_id:
            query = f"""
                SELECT event_type, COUNT(*) as count
                FROM {self.TABLE}
                WHERE job_id = %s
                GROUP BY event_type
            """
            rows = self.execute_many(query, (job_id,))
        else:
            query = f"""
                SELECT event_type, COUNT(*) as count
                FROM {self.TABLE}
                GROUP BY event_type
            """
            rows = self.execute_many(query)

        return {row["event_type"]: row["count"] for row in rows}
```

---

## 5. Blueprints

### `function/blueprints/platform_bp.py`

```python
"""
Platform Blueprint

B2B-facing endpoints for job submission and status polling.

Design Principle:
- Gateway generates request_id (B2B never provides tracking IDs)
- B2B only sees request_id, status, and results
- Internal details (job_id, nodes, orchestrator) are hidden
"""

import azure.functions as func
import json
import logging
import os
import uuid
from datetime import datetime, timezone

from function.models.requests import PlatformSubmitRequest
from function.models.responses import PlatformSubmitResponse, PlatformStatusResponse, ErrorResponse
from function.repositories.job_query_repo import JobQueryRepository
from core.models import JobQueueMessage
from infrastructure.service_bus import ServiceBusPublisher

logger = logging.getLogger(__name__)
bp = func.Blueprint()


def _json_response(data: dict, status_code: int = 200) -> func.HttpResponse:
    """Create JSON HTTP response."""
    return func.HttpResponse(
        json.dumps(data, default=str),
        status_code=status_code,
        headers={"Content-Type": "application/json"}
    )


@bp.route(route="platform/submit", methods=["POST"])
def platform_submit(req: func.HttpRequest) -> func.HttpResponse:
    """
    Submit a job for execution.

    POST /api/platform/submit
    Body: PlatformSubmitRequest (workflow_id, input_params, submitted_by)
    Returns: PlatformSubmitResponse with request_id (202 Accepted)

    The request_id is generated by the Gateway - B2B apps never provide it.
    B2B apps use request_id to poll for status via GET /platform/status/{request_id}
    """
    try:
        body = req.get_json()
        request = PlatformSubmitRequest.model_validate(body)
    except Exception as e:
        logger.warning(f"Invalid request: {e}")
        return _json_response(
            ErrorResponse(error="Invalid request", details=str(e)).model_dump(),
            status_code=400
        )

    submitted_at = datetime.now(timezone.utc)
    queue_name = os.environ.get("JOB_QUEUE_NAME", "dag-jobs")

    # ==========================================================================
    # GATEWAY GENERATES REQUEST_ID
    # This is the only identifier B2B apps will use for tracking.
    # Internally, we use it as correlation_id to link request → job.
    # ==========================================================================
    request_id = str(uuid.uuid4())

    # Check idempotency - if same key submitted before, return existing request_id
    if request.idempotency_key:
        job_repo = JobQueryRepository()
        existing = job_repo.get_job_by_idempotency_key(request.idempotency_key)
        if existing and existing.get("correlation_id"):
            logger.info(
                f"Duplicate submission (idempotency_key={request.idempotency_key}), "
                f"returning existing request_id"
            )
            return _json_response(
                PlatformSubmitResponse(
                    request_id=existing["correlation_id"],  # correlation_id IS the request_id
                    workflow_id=existing["workflow_id"],
                    submitted_at=existing["created_at"],
                    status="accepted",
                ).model_dump(),
                status_code=200  # 200, not 202 - already processed
            )

    # Create queue message with request_id as correlation_id
    queue_message = JobQueueMessage(
        workflow_id=request.workflow_id,
        input_params=request.input_params,
        correlation_id=request_id,  # ← Gateway-generated request_id
        callback_url=request.callback_url,
        priority=request.priority,
        idempotency_key=request.idempotency_key,
        submitted_by=request.submitted_by,  # ← B2B identity
        submitted_at=submitted_at,
    )

    try:
        publisher = ServiceBusPublisher.instance()
        publisher.send_message(
            queue_name=queue_name,
            message=queue_message,
            ttl_hours=24,
        )

        response = PlatformSubmitResponse(
            request_id=request_id,  # ← Return to B2B for polling
            workflow_id=request.workflow_id,
            submitted_at=submitted_at,
            status="accepted",
        )

        logger.info(
            f"Request accepted: request_id={request_id}, "
            f"workflow={request.workflow_id}, submitted_by={request.submitted_by}"
        )

        return _json_response(response.model_dump(), status_code=202)

    except RuntimeError as e:
        logger.error(f"Failed to submit job: {e}")
        return _json_response(
            ErrorResponse(error="Failed to queue job", details=str(e)).model_dump(),
            status_code=503
        )


@bp.route(route="platform/status/{request_id}", methods=["GET"])
def platform_status(req: func.HttpRequest) -> func.HttpResponse:
    """
    Get status of a request by request_id.

    GET /api/platform/status/{request_id}
    Returns: PlatformStatusResponse

    This is the primary endpoint for B2B polling.
    Internal details (job_id, nodes, orchestrator) are NOT exposed.
    """
    request_id = req.route_params.get("request_id")

    try:
        job_repo = JobQueryRepository()

        # Look up job by correlation_id (which is the request_id)
        job = job_repo.get_job_by_correlation_id(request_id)

        if not job:
            # Job may not exist yet (still in queue)
            return _json_response(
                PlatformStatusResponse(
                    request_id=request_id,
                    workflow_id="unknown",
                    status="pending",
                    submitted_at=datetime.now(timezone.utc),  # Approximate
                    progress={"message": "Request queued, awaiting processing"},
                ).model_dump()
            )

        # Map internal status to B2B-friendly status
        internal_status = job["status"].lower()
        if internal_status in ("pending", "running"):
            b2b_status = internal_status
        elif internal_status == "completed":
            b2b_status = "completed"
        elif internal_status in ("failed", "cancelled"):
            b2b_status = "failed"
        else:
            b2b_status = internal_status

        response = PlatformStatusResponse(
            request_id=request_id,
            workflow_id=job["workflow_id"],
            status=b2b_status,
            submitted_at=job["created_at"],
            completed_at=job.get("completed_at"),
            result=job.get("result_data") if b2b_status == "completed" else None,
            error=job.get("error_message") if b2b_status == "failed" else None,
        )

        return _json_response(response.model_dump())

    except Exception as e:
        logger.error(f"Error fetching status for {request_id}: {e}")
        return _json_response(
            ErrorResponse(error="Failed to fetch status", details=str(e)).model_dump(),
            status_code=500
        )


@bp.route(route="gateway/submit/batch", methods=["POST"])
def gateway_submit_batch(req: func.HttpRequest) -> func.HttpResponse:
    """
    Submit multiple jobs in a batch.

    POST /api/gateway/submit/batch
    Body: BatchSubmitRequest
    Returns: BatchSubmitResponse (202 Accepted)
    """
    try:
        body = req.get_json()
        request = BatchSubmitRequest.model_validate(body)
    except Exception as e:
        logger.warning(f"Invalid batch request: {e}")
        return _json_response(
            ErrorResponse(error="Invalid request", details=str(e)).model_dump(),
            status_code=400
        )

    submitted_at = datetime.now(timezone.utc)
    queue_name = os.environ.get("JOB_QUEUE_NAME", "dag-jobs")

    # Create queue messages
    queue_messages = [
        JobQueueMessage(
            workflow_id=job.workflow_id,
            input_params=job.input_params,
            correlation_id=job.correlation_id,
            callback_url=job.callback_url,
            priority=job.priority,
            idempotency_key=job.idempotency_key,
            submitted_by="rmhdagmaster-gateway",
            submitted_at=submitted_at,
        )
        for job in request.jobs
    ]

    try:
        publisher = ServiceBusPublisher.instance()
        result = publisher.batch_send_messages(
            queue_name=queue_name,
            messages=queue_messages,
        )

        response = BatchSubmitResponse(
            submitted=result.messages_sent,
            failed=len(queue_messages) - result.messages_sent,
            message_ids=[],  # Batch doesn't return individual IDs
            errors=result.errors,
        )

        logger.info(f"Batch submitted: {result.messages_sent}/{len(queue_messages)} jobs")

        return _json_response(response.model_dump(), status_code=202)

    except Exception as e:
        logger.error(f"Batch submission failed: {e}")
        return _json_response(
            ErrorResponse(error="Batch submission failed", details=str(e)).model_dump(),
            status_code=503
        )


@bp.route(route="gateway/health", methods=["GET"])
def gateway_health(req: func.HttpRequest) -> func.HttpResponse:
    """
    Gateway health check.

    GET /api/gateway/health
    """
    from function.models.responses import HealthResponse

    response = HealthResponse(
        status="healthy",
        service="rmhdagmaster-gateway",
        timestamp=datetime.now(timezone.utc),
        version="0.9.0",
        checks={
            "queue_configured": bool(os.environ.get("JOB_QUEUE_NAME") or
                                     os.environ.get("SERVICE_BUS_NAMESPACE")),
        }
    )

    return _json_response(response.model_dump())
```

### `function/blueprints/status_bp.py`

```python
"""
Status Blueprint

Job and node status query endpoints.
"""

import azure.functions as func
import json
import logging

from function.models.responses import (
    JobStatusResponse, JobListResponse,
    NodeStatusResponse, EventListResponse,
    ErrorResponse
)
from function.repositories.job_query_repo import JobQueryRepository
from function.repositories.node_query_repo import NodeQueryRepository
from function.repositories.event_query_repo import EventQueryRepository

logger = logging.getLogger(__name__)
bp = func.Blueprint()


def _json_response(data: dict, status_code: int = 200) -> func.HttpResponse:
    return func.HttpResponse(
        json.dumps(data, default=str),
        status_code=status_code,
        headers={"Content-Type": "application/json"}
    )


@bp.route(route="status/job/{job_id}", methods=["GET"])
def status_job_by_id(req: func.HttpRequest) -> func.HttpResponse:
    """
    Get job status by ID.

    GET /api/status/job/{job_id}
    Query params: include_nodes=true/false
    """
    job_id = req.route_params.get("job_id")
    include_nodes = req.params.get("include_nodes", "false").lower() == "true"

    try:
        job_repo = JobQueryRepository()
        job = job_repo.get_job(job_id)

        if not job:
            return _json_response(
                ErrorResponse(error="Job not found", code="NOT_FOUND").model_dump(),
                status_code=404
            )

        # Optionally include nodes
        nodes = None
        if include_nodes:
            node_repo = NodeQueryRepository()
            nodes = node_repo.get_nodes_for_job(job_id)

        response = JobStatusResponse(
            job_id=job["job_id"],
            workflow_id=job["workflow_id"],
            status=job["status"],
            created_at=job["created_at"],
            started_at=job.get("started_at"),
            completed_at=job.get("completed_at"),
            error_message=job.get("error_message"),
            owner_id=job.get("owner_id"),
            nodes=nodes,
        )

        return _json_response(response.model_dump())

    except Exception as e:
        logger.error(f"Error fetching job {job_id}: {e}")
        return _json_response(
            ErrorResponse(error="Database error", details=str(e)).model_dump(),
            status_code=500
        )


@bp.route(route="status/jobs", methods=["GET"])
def status_jobs_list(req: func.HttpRequest) -> func.HttpResponse:
    """
    List jobs with filtering.

    GET /api/status/jobs
    Query params: status, workflow_id, limit, offset
    """
    status = req.params.get("status")
    workflow_id = req.params.get("workflow_id")
    limit = int(req.params.get("limit", "50"))
    offset = int(req.params.get("offset", "0"))

    try:
        job_repo = JobQueryRepository()

        jobs = job_repo.list_jobs(
            status=status,
            workflow_id=workflow_id,
            limit=limit,
            offset=offset,
        )

        total = job_repo.count_jobs(status=status, workflow_id=workflow_id)

        response = JobListResponse(
            jobs=[
                JobStatusResponse(
                    job_id=j["job_id"],
                    workflow_id=j["workflow_id"],
                    status=j["status"],
                    created_at=j["created_at"],
                    started_at=j.get("started_at"),
                    completed_at=j.get("completed_at"),
                    error_message=j.get("error_message"),
                    owner_id=j.get("owner_id"),
                )
                for j in jobs
            ],
            total=total,
            limit=limit,
            offset=offset,
        )

        return _json_response(response.model_dump())

    except Exception as e:
        logger.error(f"Error listing jobs: {e}")
        return _json_response(
            ErrorResponse(error="Database error", details=str(e)).model_dump(),
            status_code=500
        )


@bp.route(route="status/job/{job_id}/nodes", methods=["GET"])
def status_job_nodes(req: func.HttpRequest) -> func.HttpResponse:
    """
    Get nodes for a job.

    GET /api/status/job/{job_id}/nodes
    """
    job_id = req.route_params.get("job_id")

    try:
        node_repo = NodeQueryRepository()
        nodes = node_repo.get_nodes_for_job(job_id)

        return _json_response({"nodes": nodes, "count": len(nodes)})

    except Exception as e:
        logger.error(f"Error fetching nodes for job {job_id}: {e}")
        return _json_response(
            ErrorResponse(error="Database error", details=str(e)).model_dump(),
            status_code=500
        )


@bp.route(route="status/job/{job_id}/events", methods=["GET"])
def status_job_events(req: func.HttpRequest) -> func.HttpResponse:
    """
    Get events for a job.

    GET /api/status/job/{job_id}/events
    Query params: limit, event_type
    """
    job_id = req.route_params.get("job_id")
    limit = int(req.params.get("limit", "100"))
    event_type = req.params.get("event_type")

    try:
        event_repo = EventQueryRepository()
        events = event_repo.get_events_for_job(
            job_id=job_id,
            limit=limit,
            event_type=event_type,
        )

        return _json_response(EventListResponse(
            events=events,
            total=len(events),
        ).model_dump())

    except Exception as e:
        logger.error(f"Error fetching events for job {job_id}: {e}")
        return _json_response(
            ErrorResponse(error="Database error", details=str(e)).model_dump(),
            status_code=500
        )


@bp.route(route="status/stats", methods=["GET"])
def status_stats(req: func.HttpRequest) -> func.HttpResponse:
    """
    Get overall job statistics.

    GET /api/status/stats
    """
    try:
        job_repo = JobQueryRepository()
        stats = job_repo.get_job_stats()

        return _json_response({
            "job_counts_by_status": stats,
            "total_jobs": sum(stats.values()),
        })

    except Exception as e:
        logger.error(f"Error fetching stats: {e}")
        return _json_response(
            ErrorResponse(error="Database error", details=str(e)).model_dump(),
            status_code=500
        )


@bp.route(route="status/lookup", methods=["GET"])
def status_lookup(req: func.HttpRequest) -> func.HttpResponse:
    """
    Lookup job by correlation_id or idempotency_key.

    GET /api/status/lookup
    Query params: correlation_id OR idempotency_key
    """
    correlation_id = req.params.get("correlation_id")
    idempotency_key = req.params.get("idempotency_key")

    if not correlation_id and not idempotency_key:
        return _json_response(
            ErrorResponse(
                error="Missing parameter",
                details="Provide correlation_id or idempotency_key"
            ).model_dump(),
            status_code=400
        )

    try:
        job_repo = JobQueryRepository()

        if correlation_id:
            job = job_repo.get_job_by_correlation_id(correlation_id)
        else:
            job = job_repo.get_job_by_idempotency_key(idempotency_key)

        if not job:
            return _json_response(
                ErrorResponse(error="Job not found", code="NOT_FOUND").model_dump(),
                status_code=404
            )

        return _json_response(job)

    except Exception as e:
        logger.error(f"Error in lookup: {e}")
        return _json_response(
            ErrorResponse(error="Database error", details=str(e)).model_dump(),
            status_code=500
        )
```

### `function/blueprints/proxy_bp.py`

```python
"""
Proxy Blueprint

Forward HTTP requests to Docker apps (orchestrator, worker).
"""

import azure.functions as func
import httpx
import json
import logging
import os
from typing import Optional

from function.models.responses import ProxyResponse, ErrorResponse

logger = logging.getLogger(__name__)
bp = func.Blueprint()


def _get_target_url(target: str) -> Optional[str]:
    """Get URL for target service."""
    if target == "orchestrator":
        return os.environ.get("ORCHESTRATOR_URL", "http://localhost:8000")
    elif target == "worker":
        return os.environ.get("WORKER_URL", "http://localhost:8001")
    return None


def _json_response(data: dict, status_code: int = 200) -> func.HttpResponse:
    return func.HttpResponse(
        json.dumps(data, default=str),
        status_code=status_code,
        headers={"Content-Type": "application/json"}
    )


@bp.route(route="proxy/{target}/{*path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
def proxy_request(req: func.HttpRequest) -> func.HttpResponse:
    """
    Proxy request to Docker app.

    GET/POST/etc /api/proxy/{target}/{path}

    Examples:
      GET /api/proxy/orchestrator/health
      GET /api/proxy/orchestrator/api/v1/jobs
      POST /api/proxy/worker/api/v1/task
    """
    target = req.route_params.get("target")
    path = req.route_params.get("path", "")

    # Validate target
    base_url = _get_target_url(target)
    if not base_url:
        return _json_response(
            ErrorResponse(
                error="Invalid target",
                details=f"Target must be 'orchestrator' or 'worker', got '{target}'"
            ).model_dump(),
            status_code=400
        )

    # Build full URL
    full_url = f"{base_url}/{path}"
    if req.params:
        query_string = "&".join(f"{k}={v}" for k, v in req.params.items())
        full_url = f"{full_url}?{query_string}"

    logger.info(f"Proxying {req.method} to {target}: {path}")

    try:
        # Forward request
        with httpx.Client(timeout=30.0) as client:
            # Prepare headers (forward relevant ones)
            headers = {}
            for key in ["Content-Type", "Authorization", "X-Correlation-ID"]:
                if value := req.headers.get(key):
                    headers[key] = value

            # Make request
            response = client.request(
                method=req.method,
                url=full_url,
                headers=headers,
                content=req.get_body() if req.method in ["POST", "PUT", "PATCH"] else None,
            )

            # Return proxied response
            return func.HttpResponse(
                response.text,
                status_code=response.status_code,
                headers={"Content-Type": response.headers.get("Content-Type", "application/json")}
            )

    except httpx.ConnectError as e:
        logger.error(f"Failed to connect to {target}: {e}")
        return _json_response(
            ProxyResponse(
                success=False,
                target=target,
                path=path,
                status_code=503,
                error=f"Cannot connect to {target}: {e}"
            ).model_dump(),
            status_code=503
        )
    except httpx.TimeoutException as e:
        logger.error(f"Timeout connecting to {target}: {e}")
        return _json_response(
            ProxyResponse(
                success=False,
                target=target,
                path=path,
                status_code=504,
                error=f"Timeout connecting to {target}"
            ).model_dump(),
            status_code=504
        )
    except Exception as e:
        logger.error(f"Proxy error: {e}")
        return _json_response(
            ErrorResponse(error="Proxy error", details=str(e)).model_dump(),
            status_code=500
        )


@bp.route(route="proxy/health", methods=["GET"])
def proxy_health_all(req: func.HttpRequest) -> func.HttpResponse:
    """
    Check health of all Docker apps.

    GET /api/proxy/health
    """
    results = {}

    for target in ["orchestrator", "worker"]:
        base_url = _get_target_url(target)
        if not base_url:
            results[target] = {"status": "not_configured"}
            continue

        try:
            with httpx.Client(timeout=5.0) as client:
                response = client.get(f"{base_url}/health")
                results[target] = {
                    "status": "healthy" if response.status_code == 200 else "unhealthy",
                    "status_code": response.status_code,
                    "url": base_url,
                }
        except httpx.ConnectError:
            results[target] = {
                "status": "unreachable",
                "url": base_url,
            }
        except httpx.TimeoutException:
            results[target] = {
                "status": "timeout",
                "url": base_url,
            }
        except Exception as e:
            results[target] = {
                "status": "error",
                "error": str(e),
                "url": base_url,
            }

    return _json_response({
        "targets": results,
        "all_healthy": all(r.get("status") == "healthy" for r in results.values()),
    })
```

### `function/blueprints/admin_bp.py`

```python
"""
Admin Blueprint

Administrative and maintenance endpoints.
"""

import azure.functions as func
import json
import logging
import os
from datetime import datetime, timezone

from function.models.responses import ErrorResponse
from function.repositories.job_query_repo import JobQueryRepository
from function.repositories.event_query_repo import EventQueryRepository

logger = logging.getLogger(__name__)
bp = func.Blueprint()


def _json_response(data: dict, status_code: int = 200) -> func.HttpResponse:
    return func.HttpResponse(
        json.dumps(data, default=str),
        status_code=status_code,
        headers={"Content-Type": "application/json"}
    )


@bp.route(route="admin/health", methods=["GET"])
def admin_health(req: func.HttpRequest) -> func.HttpResponse:
    """
    Comprehensive health check.

    GET /api/admin/health
    """
    checks = {}

    # Database check
    try:
        job_repo = JobQueryRepository()
        job_repo.execute_scalar("SELECT 1")
        checks["database"] = {"status": "healthy"}
    except Exception as e:
        checks["database"] = {"status": "unhealthy", "error": str(e)}

    # Service Bus check (config only)
    sb_configured = bool(
        os.environ.get("SERVICE_BUS_NAMESPACE") or
        os.environ.get("SERVICE_BUS_CONNECTION_STRING")
    )
    checks["service_bus"] = {
        "status": "configured" if sb_configured else "not_configured"
    }

    all_healthy = all(
        c.get("status") in ("healthy", "configured")
        for c in checks.values()
    )

    return _json_response({
        "status": "healthy" if all_healthy else "degraded",
        "checks": checks,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "version": "0.9.0",
    })


@bp.route(route="admin/config", methods=["GET"])
def admin_config(req: func.HttpRequest) -> func.HttpResponse:
    """
    Show current configuration (non-sensitive).

    GET /api/admin/config
    """
    config = {
        "job_queue_name": os.environ.get("JOB_QUEUE_NAME", "dag-jobs"),
        "service_bus_namespace": os.environ.get("SERVICE_BUS_NAMESPACE", "(not set)"),
        "database_host": os.environ.get("POSTGIS_HOST", "(not set)"),
        "database_name": os.environ.get("POSTGIS_DATABASE", "(not set)"),
        "orchestrator_url": os.environ.get("ORCHESTRATOR_URL", "(not set)"),
        "worker_url": os.environ.get("WORKER_URL", "(not set)"),
    }

    return _json_response({"config": config})


@bp.route(route="admin/active-jobs", methods=["GET"])
def admin_active_jobs(req: func.HttpRequest) -> func.HttpResponse:
    """
    List currently active jobs (pending/running).

    GET /api/admin/active-jobs
    """
    try:
        job_repo = JobQueryRepository()
        active = job_repo.get_active_jobs()

        return _json_response({
            "active_jobs": active,
            "count": len(active),
        })

    except Exception as e:
        logger.error(f"Error fetching active jobs: {e}")
        return _json_response(
            ErrorResponse(error="Database error", details=str(e)).model_dump(),
            status_code=500
        )


@bp.route(route="admin/recent-events", methods=["GET"])
def admin_recent_events(req: func.HttpRequest) -> func.HttpResponse:
    """
    List recent events across all jobs.

    GET /api/admin/recent-events
    Query params: limit (default 100), event_type
    """
    limit = int(req.params.get("limit", "100"))
    event_type = req.params.get("event_type")

    try:
        event_repo = EventQueryRepository()
        events = event_repo.get_recent_events(limit=limit, event_type=event_type)

        return _json_response({
            "events": events,
            "count": len(events),
        })

    except Exception as e:
        logger.error(f"Error fetching events: {e}")
        return _json_response(
            ErrorResponse(error="Database error", details=str(e)).model_dump(),
            status_code=500
        )


@bp.route(route="admin/workflows", methods=["GET"])
def admin_workflows(req: func.HttpRequest) -> func.HttpResponse:
    """
    List available workflows.

    GET /api/admin/workflows

    Note: This queries the orchestrator for workflow definitions.
    For now, returns a static response - expand when Docker proxy is set up.
    """
    # TODO: Proxy to orchestrator for actual workflow list
    return _json_response({
        "message": "Use /api/proxy/orchestrator/api/v1/workflows for workflow list",
        "note": "This endpoint will be expanded to proxy to orchestrator"
    })
```

---

## 6. Configuration Files

### `host.json`

```json
{
  "version": "2.0",
  "logging": {
    "applicationInsights": {
      "samplingSettings": {
        "isEnabled": true,
        "excludedTypes": "Request"
      }
    },
    "logLevel": {
      "default": "Information",
      "Host.Results": "Information",
      "Function": "Information",
      "Host.Aggregator": "Warning"
    }
  },
  "extensions": {
    "http": {
      "routePrefix": "api"
    }
  },
  "functionTimeout": "00:05:00"
}
```

### `requirements-function.txt`

```
# Azure Functions Runtime
azure-functions>=1.17.0

# Service Bus Messaging
azure-servicebus>=7.11.0
azure-identity>=1.15.0

# Database
psycopg[binary]>=3.1.0

# Schema Validation
pydantic>=2.5.0

# HTTP Client (for proxy)
httpx>=0.26.0
```

### `.funcignore`

```
# =============================================================================
# FUNCIGNORE - Exclude heavy modules from Function App deployment
# =============================================================================
# The function app only needs:
# - function_app.py (entry point)
# - function/ (function-specific code)
# - core/models/ (shared Pydantic models)
# - infrastructure/service_bus.py (publisher)
# - gateway/models.py (API models)
# =============================================================================

# Docker/Container files (not needed for Functions)
Dockerfile
Dockerfile.*
*.dockerignore
entrypoint.sh

# Heavy orchestrator modules (run in Docker, not Functions)
orchestrator/
worker/
handlers/
health/
ui/

# API routes (FastAPI, not Azure Functions)
api/

# Repositories (function has its own lightweight repos)
repositories/

# Services layer (orchestrator-specific)
services/

# Templates and static (web UI for orchestrator)
templates/
static/

# Workflows YAML (loaded by orchestrator)
workflows/

# Development/Testing
tests/
docs/
*.md
!requirements-function.txt

# Python artifacts
__pycache__/
*.pyc
*.pyo
*.pyd
.Python
*.so
*.egg
*.egg-info/
dist/
build/
.eggs/

# Virtual environments
venv/
.venv/
env/
ENV/

# IDE
.vscode/
.idea/
*.swp
*.swo

# Git
.git/
.gitignore

# Other requirements (function uses requirements-function.txt)
requirements.txt
requirements-orchestrator.txt
requirements-worker.txt

# Config files for other targets
*.env
*.env.*
local.settings.json

# Misc
*.log
*.tmp
.DS_Store
Thumbs.db
```

---

## 7. Endpoint Summary

### Platform Endpoints (Production)

**These are the ONLY endpoints in production.** B2B authentication is handled by **Azure Easy Auth** - the Function App itself exposes no authentication logic or internal details.

| Method | Path | Description | Notes |
|--------|------|-------------|-------|
| POST | `/api/platform/submit` | Submit job request | Returns `request_id` |
| POST | `/api/platform/submit/batch` | Submit multiple requests | Returns list of `request_id`s |
| GET | `/api/platform/status/{request_id}` | Poll request status | B2B primary polling endpoint |
| GET | `/api/platform/health` | Platform health check | Simple health for B2B |

**What B2B apps see**:
- `request_id` - Generated by Gateway, used for all tracking
- `status` - pending, running, completed, failed
- `result` - Final output when completed
- `error` - Error message if failed

**What B2B apps NEVER see**:
- `job_id` - Internal orchestrator reference
- `correlation_id` - Internal (same as request_id, but internal name)
- Node states, events, orchestrator details

**Authentication**: Azure Easy Auth (AAD) - not handled by Function App code.

---

### Development-Only Endpoints (⚠️ TEMPORARY)

**These endpoints exist ONLY for development/debugging. They will be REMOVED before production.**

Note: `/api/admin` is reserved by Azure, so we use `/api/ops` for internal operations.

| Method | Path | Description | Remove Before |
|--------|------|-------------|---------------|
| GET | `/api/ops/health` | Comprehensive health check | UAT |
| GET | `/api/ops/config` | Show configuration | UAT |
| GET | `/api/ops/jobs` | List jobs with internal details | UAT |
| GET | `/api/ops/job/{job_id}` | Get job by internal ID | UAT |
| GET | `/api/ops/job/{job_id}/nodes` | Get node states | UAT |
| GET | `/api/ops/job/{job_id}/events` | Get event timeline | UAT |
| * | `/api/proxy/{target}/{path}` | Proxy to orchestrator/worker | UAT |

**Production Function App**: Only `/api/platform/*` endpoints. No internal visibility.

---

### Infrastructure Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/livez` | Liveness probe (always 200) |
| GET | `/api/readyz` | Readiness probe (checks startup) |

---

## 8. Environment Variables

### Required

| Variable | Description |
|----------|-------------|
| `SERVICE_BUS_NAMESPACE` | Service Bus FQDN (e.g., `mybus.servicebus.windows.net`) |
| `POSTGIS_HOST` | PostgreSQL host |
| `POSTGIS_DATABASE` | Database name |
| `POSTGIS_USER` | Database user |
| `POSTGIS_PASSWORD` | Database password (or use managed identity) |

### Optional

| Variable | Default | Description |
|----------|---------|-------------|
| `JOB_QUEUE_NAME` | `dag-jobs` | Service Bus queue for jobs |
| `ORCHESTRATOR_URL` | `http://localhost:8000` | Orchestrator base URL |
| `WORKER_URL` | `http://localhost:8001` | Worker base URL |
| `DATABASE_URL` | (built from components) | Full connection string |

---

## 9. Implementation Order

1. **Phase 1: Foundation** ✅ DONE
   - [x] Create `function/` directory structure
   - [x] Create `function/config.py`
   - [x] Create `function/startup.py`
   - [x] Create `host.json`
   - [x] Create `requirements-function.txt`
   - [x] Create `.funcignore`

2. **Phase 2: Models** ✅ DONE
   - [x] Create `function/models/requests.py`
   - [x] Create `function/models/responses.py`

3. **Phase 3: Repositories** ✅ DONE
   - [x] Create `function/repositories/base.py`
   - [x] Create `function/repositories/job_query_repo.py`
   - [x] Create `function/repositories/node_query_repo.py`
   - [x] Create `function/repositories/event_query_repo.py`

4. **Phase 4: Blueprints** ✅ DONE
   - [x] Create `function/blueprints/gateway_bp.py`
   - [x] Create `function/blueprints/status_bp.py`
   - [x] Create `function/blueprints/proxy_bp.py`
   - [x] Create `function/blueprints/admin_bp.py`

5. **Phase 5: Entry Point** ✅ DONE
   - [x] Create `function_app.py`

6. **Phase 6: Testing** ⏳ TODO
   - [ ] Local testing with `func start`
   - [ ] Verify `.funcignore` excludes correctly
   - [ ] Test all endpoints

---

## 10. Deployment

### Local Development

```bash
# Install Azure Functions Core Tools
brew install azure-functions-core-tools@4

# Start function app locally
cd /Users/robertharrison/python_builds/rmhdagmaster
func start
```

### Azure Deployment

```bash
# Deploy to Azure (creates lightweight package)
func azure functionapp publish <function-app-name>

# Verify deployment
az functionapp show --name <function-app-name> --resource-group <rg>
```

### CI/CD

- GitHub Actions workflow to build and deploy
- Use `.funcignore` to ensure lightweight package
- Target: Azure Function App (Consumption or Premium plan)

---

## Summary

This plan creates a **lightweight Azure Function App** that serves as the **B2B Gateway/ACL** for the DAG platform.

### Key Design Decisions

1. **B2B apps know nothing about internal processes**
   - They submit: `workflow_id`, `input_params`, `submitted_by`
   - They receive: `request_id` (Gateway-generated)
   - They poll: `GET /platform/status/{request_id}`
   - They NEVER see: `job_id`, `correlation_id`, nodes, events, orchestrator

2. **Gateway generates all tracking IDs**
   - `request_id` = Gateway-generated UUID
   - Used as `correlation_id` internally to link request → job
   - B2B apps don't provide ANY tracking identifiers

3. **Authentication is 100% Easy Auth**
   - Function App code has NO authentication logic
   - Azure Easy Auth (AAD) handles B2B identity verification
   - Claims/roles can be used for authorization if needed

4. **Production exposes NO internals**
   - Only `/api/platform/*` endpoints in production
   - `/api/ops/*` endpoints are temporary (removed before UAT)
   - No job_id, nodes, events, orchestrator details exposed

### Technical Implementation

- **Azure Functions V2** (blueprints, decorators, conditional registration)
- **Pydantic V2** throughout (model_dump, model_validate, ConfigDict)
- **Repository pattern** for clean database access
- **`.funcignore`** ensures lightweight deployment (~50MB vs ~250MB)

### What Remains

| Task | Status | Notes |
|------|--------|-------|
| Update `platform_bp.py` with request_id generation | 🔄 Planned | Core change |
| Update response models for B2B view | 🔄 Planned | Hide internals |
| Add `/platform/status/{request_id}` endpoint | 🔄 Planned | B2B polling |
| Rename `/admin/*` to `/ops/*` | 🔄 Planned | Azure reserved |
| Local testing | ⏳ TODO | |
| Azure deployment | ⏳ TODO | |
| Configure Easy Auth (AAD) | ⏳ TODO | B2B authentication |
| Remove `/ops/*` before UAT | ⏳ TODO | Production cleanup |
