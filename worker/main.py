# ============================================================================
# WORKER MAIN ENTRY POINT
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Core - Worker process entry point
# PURPOSE: Start worker in standalone mode
# CREATED: 31 JAN 2026
# ============================================================================
"""
Worker Main Entry Point

Starts a DAG worker process that:
1. Loads handler modules
2. Connects to Service Bus
3. Processes tasks until shutdown

Usage:
    # With environment variables
    RUN_MODE=worker python -m worker.main

    # Or directly
    python worker/main.py

Environment Variables:
    DAG_WORKER_ID: Unique worker identifier
    DAG_WORKER_TYPE: "docker" or "functionapp"
    DAG_WORKER_QUEUE: Queue name to listen on
    DAG_SERVICEBUS_CONNECTION_STRING: Service Bus connection
    DAG_SERVICEBUS_FQDN: Service Bus namespace (if using managed identity)
    USE_MANAGED_IDENTITY: "true" to use Azure managed identity
    DAG_DB_URL: PostgreSQL connection (for direct result reporting)
    DAG_CALLBACK_URL: HTTP callback URL (alternative to DB)
    DAG_WORKER_MAX_CONCURRENT: Max concurrent task execution
    HANDLER_MODULES: Comma-separated list of handler modules to load
"""

import asyncio
import importlib
import logging
import os
import sys
from typing import List, Optional
from aiohttp import web

from worker.contracts import WorkerConfig
from worker.consumer import ServiceBusConsumer, run_consumer

# Configure logging using our structured logging system
from core.logging import configure_logging, get_logger
from __version__ import __version__, BUILD_DATE

configure_logging(
    level=os.environ.get("DAG_LOG_LEVEL", "INFO"),
    json_output=os.environ.get("DAG_LOG_FORMAT", "").lower() == "json",
)
logger = get_logger(__name__)

# Worker state for health checks
_worker_healthy = True
_worker_status = "starting"
_worker_config: Optional[WorkerConfig] = None
_consumer: Optional[ServiceBusConsumer] = None


# ============================================================================
# HEALTH SERVER (for Azure Web App probes)
# ============================================================================

async def health_handler(request):
    """
    Comprehensive health check endpoint.

    Returns version, queue configuration, and consumer status for
    orchestrator health check verification.
    """
    global _worker_healthy, _worker_status, _worker_config, _consumer

    # Build response data
    response_data = {
        "status": "healthy" if _worker_healthy else "unhealthy",
        "worker_status": _worker_status,
        "version": __version__,
        "build_date": BUILD_DATE,
        "worker_type": _worker_config.worker_type if _worker_config else "unknown",
        "queue": _worker_config.queue_name if _worker_config else "unknown",
        "worker_id": _worker_config.worker_id if _worker_config else "unknown",
        "consuming": _consumer is not None and _consumer._running if _consumer else False,
    }

    # Add stats if consumer is available
    if _consumer:
        response_data["stats"] = {
            "messages_received": _consumer._messages_received,
            "tasks_completed": _consumer._tasks_completed,
            "tasks_failed": _consumer._tasks_failed,
            "active_tasks": len(_consumer._active_tasks),
        }

    if _worker_healthy:
        return web.json_response(response_data)
    else:
        return web.json_response(response_data, status=503)


async def start_health_server(port: int = 8000):
    """Start minimal HTTP server for health probes."""
    app = web.Application()
    app.router.add_get("/", health_handler)
    app.router.add_get("/health", health_handler)
    app.router.add_get("/livez", health_handler)
    app.router.add_get("/readyz", health_handler)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info(f"Health server started on port {port}")
    return runner


# ============================================================================
# HANDLER LOADING
# ============================================================================

def load_handlers(modules: Optional[List[str]] = None) -> int:
    """
    Load handler modules to register handlers.

    Args:
        modules: List of module names to import

    Returns:
        Number of modules loaded
    """
    if modules is None:
        # Default modules
        modules = ["handlers.examples"]

        # Check for additional modules from env
        extra = os.getenv("HANDLER_MODULES", "")
        if extra:
            modules.extend(m.strip() for m in extra.split(",") if m.strip())

    loaded = 0
    for module_name in modules:
        try:
            importlib.import_module(module_name)
            logger.info(f"Loaded handler module: {module_name}")
            loaded += 1
        except ImportError as e:
            logger.warning(f"Failed to load handler module {module_name}: {e}")

    # Log registered handlers
    from handlers.registry import list_handlers
    handlers = list_handlers()
    logger.info(f"Registered {len(handlers)} handlers: {[h['name'] for h in handlers]}")

    return loaded


# ============================================================================
# MAIN
# ============================================================================

async def main() -> None:
    """Main entry point."""
    global _worker_healthy, _worker_status, _worker_config, _consumer

    logger.info("=" * 60)
    logger.info(f"DAG Worker Starting v{__version__}")
    logger.info("=" * 60)

    # Start health server for Azure probes
    health_port = int(os.environ.get("PORT", "8000"))
    health_runner = await start_health_server(health_port)

    # Load configuration
    config = WorkerConfig.from_env()
    _worker_config = config  # Set global for health endpoint
    logger.info(f"Worker ID: {config.worker_id}")
    logger.info(f"Worker Type: {config.worker_type}")
    logger.info(f"Queue: {config.queue_name}")
    logger.info(f"Max Concurrent: {config.max_concurrent_tasks}")

    # Load handlers
    load_handlers()

    # Validate configuration
    if not config.service_bus_connection and not config.service_bus_namespace:
        logger.error(
            "No Service Bus connection configured. "
            "Set DAG_SERVICEBUS_CONNECTION_STRING or DAG_SERVICEBUS_FQDN"
        )
        _worker_healthy = False
        _worker_status = "no_service_bus"
        sys.exit(1)

    if not config.database_url and not config.callback_url:
        logger.error(
            "No result reporting configured. "
            "Set DATABASE_URL or DAG_CALLBACK_URL"
        )
        _worker_healthy = False
        _worker_status = "no_result_reporter"
        sys.exit(1)

    # Mark as running
    _worker_status = "running"

    # Create consumer and track globally for health checks
    _consumer = ServiceBusConsumer(config)

    # Run consumer
    try:
        await _consumer.run()
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.exception(f"Worker failed: {e}")
        _worker_healthy = False
        _worker_status = f"error: {str(e)[:100]}"
        sys.exit(1)
    finally:
        # Cleanup health server
        await health_runner.cleanup()

    logger.info("DAG Worker stopped")


def run() -> None:
    """Synchronous entry point."""
    asyncio.run(main())


if __name__ == "__main__":
    run()
