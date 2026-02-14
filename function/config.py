# ============================================================================
# FUNCTION APP CONFIGURATION
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Gateway - Configuration management
# PURPOSE: Environment-based configuration for function app
# CREATED: 04 FEB 2026
# ============================================================================
"""
Function App Configuration

Loads configuration from environment variables with sensible defaults.
The gateway is a read-only ACL proxy — it needs database access for
queries and orchestrator URL for forwarding mutations.

Database auth uses the same env vars and auth module as the orchestrator:
- USE_MANAGED_IDENTITY=true → UMI token via infrastructure/auth
- Otherwise → POSTGRES_PASSWORD or DATABASE_URL
"""

import os
import logging
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class FunctionConfig:
    """Configuration for the function app."""

    # Database (same env vars as orchestrator: POSTGRES_*)
    database_url: Optional[str] = None
    postgres_host: str = "localhost"
    postgres_port: str = "5432"
    postgres_db: str = "postgres"
    db_schema: str = "dagapp"

    # Service Bus (used by gateway_bp generic submit — NOT by platform_bp)
    service_bus_namespace: str = ""
    service_bus_connection_string: Optional[str] = None
    job_queue_name: str = ""

    # Orchestrator (domain authority — gateway proxies mutations here)
    orchestrator_url: str = "http://localhost:8000"

    # Worker (for proxy health checks only)
    worker_url: str = "http://localhost:8001"

    # App Info
    version: str = "0.9.0"
    service_name: str = "rmhdagmaster-gateway"

    @classmethod
    def from_env(cls) -> "FunctionConfig":
        """Load configuration from environment variables."""
        return cls(
            # Database (shared DAG_ vars)
            database_url=os.environ.get("DAG_DB_URL"),
            postgres_host=os.environ.get("DAG_DB_HOST", "localhost"),
            postgres_port=os.environ.get("DAG_DB_PORT", "5432"),
            postgres_db=os.environ.get("DAG_DB_NAME", "postgres"),
            db_schema=os.environ.get("DAG_DB_SCHEMA", "dagapp"),
            # Service Bus (shared DAG_ vars)
            service_bus_namespace=os.environ.get("DAG_SERVICEBUS_FQDN", ""),
            service_bus_connection_string=os.environ.get("DAG_SERVICEBUS_CONNECTION_STRING"),
            job_queue_name=os.environ.get("DAG_JOB_QUEUE", ""),
            # Gateway-specific URLs
            orchestrator_url=os.environ.get("GATEWAY_ORCHESTRATOR_URL", "http://localhost:8000"),
            worker_url=os.environ.get("GATEWAY_WORKER_URL", "http://localhost:8001"),
            # App Info
            version=os.environ.get("APP_VERSION", "0.9.0"),
            service_name=os.environ.get("SERVICE_NAME", "rmhdagmaster-gateway"),
        )

    def get_connection_string(self) -> str:
        """
        Get database connection string.

        Delegates to infrastructure/auth for managed identity support,
        using the same auth path as the orchestrator.
        """
        # 1. Explicit DATABASE_URL overrides everything
        if self.database_url:
            return self.database_url

        # 2. Managed identity (same as orchestrator)
        use_mi = os.environ.get("USE_MANAGED_IDENTITY", "false").lower() == "true"
        if use_mi:
            from infrastructure.auth import get_postgres_connection_string
            logger.info("Gateway using Managed Identity for PostgreSQL")
            return get_postgres_connection_string()

        # 3. Password auth fallback (local dev only)
        user = os.environ.get("DAG_DB_USER", "postgres")
        password = os.environ.get("DAG_DB_PASSWORD", "")

        return (
            f"host={self.postgres_host} "
            f"port={self.postgres_port} "
            f"dbname={self.postgres_db} "
            f"user={user} "
            f"password={password} "
            f"sslmode=require"
        )

    @property
    def has_database_config(self) -> bool:
        """Check if database is configured."""
        return bool(self.database_url or self.postgres_host != "localhost")

    @property
    def has_service_bus_config(self) -> bool:
        """Check if Service Bus is configured (for gateway_bp generic submit)."""
        return bool(self.service_bus_namespace or self.service_bus_connection_string)

    @property
    def has_orchestrator_config(self) -> bool:
        """Check if orchestrator URL is configured (for domain mutation proxying)."""
        return bool(self.orchestrator_url and self.orchestrator_url != "http://localhost:8000")


# Global config singleton
_config: Optional[FunctionConfig] = None


def get_config() -> FunctionConfig:
    """Get the global configuration singleton."""
    global _config
    if _config is None:
        _config = FunctionConfig.from_env()
    return _config


__all__ = ["FunctionConfig", "get_config"]
