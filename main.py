# ============================================================================
# DAG ORCHESTRATOR - MAIN APPLICATION
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Core - FastAPI application entry point
# PURPOSE: Main application with orchestration loop
# CREATED: 29 JAN 2026
# ============================================================================
"""
DAG Orchestrator Main Application

FastAPI application that:
1. Provides HTTP API for job management
2. Runs the orchestration loop in the background
3. Manages database connections and messaging

Usage:
    uvicorn main:app --host 0.0.0.0 --port 8000
"""

import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI

from __version__ import __version__, BUILD_DATE, EPOCH
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from repositories.database import init_pool, close_pool, get_pool
from services import JobService, NodeService, WorkflowService, EventService
from orchestrator import Orchestrator
from messaging import get_publisher, close_publisher
from api.routes import router, set_services
from api.ui_routes import router as ui_router, set_ui_services
from api.bootstrap_routes import router as bootstrap_router
from api.domain_routes import router as domain_router, set_domain_services

# Health check system
from health import health_router, get_registry
from health.checks.application import set_orchestrator

# Configure logging using our structured logging system
from core.logging import configure_logging, get_logger

configure_logging(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    json_output=os.environ.get("LOG_FORMAT", "").lower() == "json",
)
logger = get_logger(__name__)

# Global instances
_orchestrator: Orchestrator = None
_workflow_service: WorkflowService = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan handler.

    Initializes services on startup, cleans up on shutdown.
    """
    global _orchestrator, _workflow_service

    logger.info(f"Starting DAG Orchestrator v{__version__} (Epoch {EPOCH}, Build {BUILD_DATE})")

    # Optional: Bootstrap schema on startup (for development)
    if os.environ.get("AUTO_BOOTSTRAP_SCHEMA", "").lower() == "true":
        logger.info("Auto-bootstrap enabled, deploying schema...")
        try:
            from infrastructure import DatabaseInitializer
            initializer = DatabaseInitializer()
            result = initializer.initialize_all(dry_run=False)
            if result.success:
                logger.info("Schema bootstrap completed successfully")
            else:
                logger.warning(f"Schema bootstrap had issues: {result.errors}")
        except Exception as e:
            logger.warning(f"Schema bootstrap failed (may already exist): {e}")

    # Initialize database pool
    pool = await init_pool()
    logger.info("Database pool initialized")

    # Initialize workflow service
    workflows_dir = os.environ.get("WORKFLOWS_DIR", "./workflows")
    _workflow_service = WorkflowService(workflows_dir)
    count = _workflow_service.load_all()
    logger.info(f"Loaded {count} workflows")

    # Initialize event service (for timeline logging)
    event_service = EventService(pool)
    logger.info("Event service initialized")

    # Initialize services with event service
    job_service = JobService(pool, _workflow_service, event_service)
    node_service = NodeService(pool, _workflow_service, event_service)

    # Initialize asset service (domain authority for business objects)
    from services.asset_service import GeospatialAssetService
    asset_service = GeospatialAssetService(pool, job_service, event_service)
    logger.info("Asset service initialized")

    # Initialize orchestrator (with asset_service for processing callbacks)
    poll_interval = float(os.environ.get("ORCHESTRATOR_POLL_INTERVAL", "1.0"))
    _orchestrator = Orchestrator(pool, _workflow_service, poll_interval, event_service, asset_service)

    # Set services for API routes
    set_services(
        job_service=job_service,
        node_service=node_service,
        workflow_service=_workflow_service,
        orchestrator=_orchestrator,
        pool=pool,
        event_service=event_service,
    )

    # Set services for domain routes (business domain mutations)
    set_domain_services(asset_service=asset_service)

    # Set services for UI routes
    set_ui_services(
        job_service=job_service,
        node_service=node_service,
        workflow_service=_workflow_service,
        orchestrator=_orchestrator,
        pool=pool,
        event_service=event_service,
    )

    # Start orchestrator
    await _orchestrator.start()
    logger.info("Orchestrator started")

    # Initialize health checks
    set_orchestrator(_orchestrator)
    import health.checks  # Register all health check plugins
    get_registry().mark_initialized()
    logger.info(f"Health checks initialized ({len(get_registry())} checks registered)")

    yield

    # Shutdown
    logger.info("Shutting down DAG Orchestrator...")

    await _orchestrator.stop()
    await close_publisher()
    await close_pool()

    logger.info("DAG Orchestrator stopped")


# Create FastAPI app
app = FastAPI(
    title="DAG Orchestrator",
    description=f"Epoch {EPOCH} DAG-based workflow orchestration system",
    version=__version__,
    lifespan=lifespan,
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount static files
app.mount("/static", StaticFiles(directory="static"), name="static")

# Include health check routes (no prefix - /livez, /readyz, /health)
app.include_router(health_router)

# Include API routes
app.include_router(router, prefix="/api/v1")

# Include domain routes (business domain mutations — gateway proxies here)
app.include_router(domain_router, prefix="/api/v1")

# Include bootstrap routes (schema deployment)
app.include_router(bootstrap_router, prefix="/api/v1")

# Include UI routes
app.include_router(ui_router)


# Root endpoint
@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "service": "DAG Orchestrator",
        "version": __version__,
        "epoch": EPOCH,
        "build_date": BUILD_DATE,
        "status": "running",
        "docs": "/docs",
        "ui": "/ui",
    }


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    import uvicorn

    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "8000"))

    uvicorn.run(
        "main:app",
        host=host,
        port=port,
        reload=os.environ.get("RELOAD", "false").lower() == "true",
    )
