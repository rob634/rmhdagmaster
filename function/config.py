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
"""

import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class FunctionConfig:
    """Configuration for the function app."""

    # Database
    database_url: Optional[str] = None
    postgis_host: str = "localhost"
    postgis_port: str = "5432"
    postgis_database: str = "dagmaster"
    postgis_user: str = "postgres"
    postgis_password: str = ""
    db_schema: str = "dagapp"

    # Service Bus
    service_bus_namespace: str = ""
    service_bus_connection_string: Optional[str] = None
    job_queue_name: str = "dag-jobs"

    # Docker App URLs (for proxy)
    orchestrator_url: str = "http://localhost:8000"
    worker_url: str = "http://localhost:8001"

    # App Info
    version: str = "0.9.0"
    service_name: str = "rmhdagmaster-gateway"

    @classmethod
    def from_env(cls) -> "FunctionConfig":
        """Load configuration from environment variables."""
        return cls(
            # Database
            database_url=os.environ.get("DATABASE_URL"),
            postgis_host=os.environ.get("POSTGIS_HOST", "localhost"),
            postgis_port=os.environ.get("POSTGIS_PORT", "5432"),
            postgis_database=os.environ.get("POSTGIS_DATABASE", "dagmaster"),
            postgis_user=os.environ.get("POSTGIS_USER", "postgres"),
            postgis_password=os.environ.get("POSTGIS_PASSWORD", ""),
            db_schema=os.environ.get("DB_SCHEMA", "dagapp"),
            # Service Bus
            service_bus_namespace=os.environ.get("SERVICE_BUS_NAMESPACE", ""),
            service_bus_connection_string=os.environ.get("SERVICE_BUS_CONNECTION_STRING"),
            job_queue_name=os.environ.get("JOB_QUEUE_NAME", "dag-jobs"),
            # Docker URLs
            orchestrator_url=os.environ.get("ORCHESTRATOR_URL", "http://localhost:8000"),
            worker_url=os.environ.get("WORKER_URL", "http://localhost:8001"),
            # App Info
            version=os.environ.get("APP_VERSION", "0.9.0"),
            service_name=os.environ.get("SERVICE_NAME", "rmhdagmaster-gateway"),
        )

    def get_connection_string(self) -> str:
        """Get database connection string."""
        if self.database_url:
            return self.database_url

        if self.postgis_password:
            return (
                f"postgresql://{self.postgis_user}:{self.postgis_password}"
                f"@{self.postgis_host}:{self.postgis_port}/{self.postgis_database}"
                f"?sslmode=require"
            )
        else:
            # No password - might be using managed identity
            return (
                f"postgresql://{self.postgis_user}"
                f"@{self.postgis_host}:{self.postgis_port}/{self.postgis_database}"
                f"?sslmode=require"
            )

    @property
    def has_database_config(self) -> bool:
        """Check if database is configured."""
        return bool(self.database_url or self.postgis_host)

    @property
    def has_service_bus_config(self) -> bool:
        """Check if Service Bus is configured."""
        return bool(self.service_bus_namespace or self.service_bus_connection_string)


# Global config singleton
_config: Optional[FunctionConfig] = None


def get_config() -> FunctionConfig:
    """Get the global configuration singleton."""
    global _config
    if _config is None:
        _config = FunctionConfig.from_env()
    return _config


__all__ = ["FunctionConfig", "get_config"]
