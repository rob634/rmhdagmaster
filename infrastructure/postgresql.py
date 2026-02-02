# ============================================================================
# POSTGRESQL CONNECTION INFRASTRUCTURE
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Infrastructure - PostgreSQL connection handling
# PURPOSE: Database connectivity with managed identity support
# CREATED: 31 JAN 2026
# ============================================================================
"""
PostgreSQL Connection Infrastructure

Provides database connectivity for DAG orchestrator and workers:
- Managed identity authentication (production)
- Password authentication (development)
- Connection pooling for Docker workers
- Context managers for safe resource management

Adapted from rmhgeoapi/infrastructure/postgresql.py for DAG orchestrator.

Authentication Priority:
1. User-assigned managed identity (DB_ADMIN_MANAGED_IDENTITY_CLIENT_ID)
2. System-assigned managed identity (WEBSITE_SITE_NAME detected)
3. Password authentication (POSTGRES_PASSWORD - dev only)
"""

import os
import logging
import threading
from typing import Any, Dict, Optional
from contextlib import contextmanager

import psycopg
from psycopg.rows import dict_row

logger = logging.getLogger(__name__)


# ============================================================================
# POSTGRESQL REPOSITORY BASE
# ============================================================================

class PostgreSQLRepository:
    """
    Base repository for PostgreSQL database operations.

    Provides connection management with:
    - Managed identity authentication for Azure
    - Connection pooling for Docker workers
    - Context managers for safe resource handling

    Usage:
        repo = PostgreSQLRepository()
        with repo.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
    """

    def __init__(
        self,
        connection_string: Optional[str] = None,
        schema_name: Optional[str] = None,
    ):
        """
        Initialize PostgreSQL repository.

        Args:
            connection_string: Optional explicit connection string
            schema_name: Optional schema name for queries
        """
        self.schema_name = schema_name or os.environ.get("PGSTAC_SCHEMA", "pgstac")
        self._conn_string = connection_string
        self._conn_string_lock = threading.Lock()

    @property
    def conn_string(self) -> str:
        """Get or build connection string (lazy, thread-safe)."""
        if self._conn_string is None:
            with self._conn_string_lock:
                if self._conn_string is None:
                    self._conn_string = self._build_connection_string()
        return self._conn_string

    def _build_connection_string(self) -> str:
        """
        Build PostgreSQL connection string with authentication.

        Priority:
        1. User-assigned managed identity
        2. System-assigned managed identity
        3. Password authentication
        """
        host = os.environ.get("POSTGRES_HOST") or os.environ.get("POSTGIS_HOST")
        port = os.environ.get("POSTGRES_PORT") or os.environ.get("POSTGIS_PORT", "5432")
        database = os.environ.get("POSTGRES_DB") or os.environ.get("POSTGIS_DATABASE")

        if not host or not database:
            raise ValueError(
                "Database connection not configured. "
                "Set POSTGRES_HOST and POSTGRES_DB environment variables."
            )

        # Check for managed identity
        use_managed_identity = os.environ.get("USE_MANAGED_IDENTITY", "").lower() == "true"
        client_id = os.environ.get("DB_ADMIN_MANAGED_IDENTITY_CLIENT_ID")
        identity_name = os.environ.get("DB_ADMIN_MANAGED_IDENTITY_NAME")

        # Check for Azure environment (system-assigned identity)
        is_azure = bool(os.environ.get("WEBSITE_SITE_NAME"))

        if use_managed_identity and identity_name:
            # Managed identity authentication
            return self._build_managed_identity_connection_string(
                host=host,
                port=port,
                database=database,
                identity_name=identity_name,
                client_id=client_id,
            )
        elif is_azure and identity_name:
            # System-assigned identity in Azure
            return self._build_managed_identity_connection_string(
                host=host,
                port=port,
                database=database,
                identity_name=identity_name,
                client_id=None,
            )
        else:
            # Password authentication (development)
            return self._build_password_connection_string(
                host=host,
                port=port,
                database=database,
            )

    def _build_managed_identity_connection_string(
        self,
        host: str,
        port: str,
        database: str,
        identity_name: str,
        client_id: Optional[str] = None,
    ) -> str:
        """Build connection string using Azure Managed Identity."""
        try:
            from azure.identity import ManagedIdentityCredential
            from azure.core.exceptions import ClientAuthenticationError

            if client_id:
                logger.debug(f"Acquiring token for user-assigned identity...")
                credential = ManagedIdentityCredential(client_id=client_id)
            else:
                logger.debug("Acquiring token for system-assigned identity...")
                credential = ManagedIdentityCredential()

            # Get access token for PostgreSQL
            token_response = credential.get_token(
                "https://ossrdbms-aad.database.windows.net/.default"
            )
            token = token_response.token

            logger.debug("Token acquired successfully")

            # Build connection string with token as password
            conn_str = (
                f"host={host} "
                f"port={port} "
                f"dbname={database} "
                f"user={identity_name} "
                f"password={token} "
                f"sslmode=require"
            )

            return conn_str

        except ImportError:
            raise ImportError(
                "azure-identity package required for managed identity. "
                "Install with: pip install azure-identity"
            )
        except Exception as e:
            logger.error(f"Managed identity token acquisition failed: {e}")
            raise RuntimeError(f"Failed to acquire managed identity token: {e}")

    def _build_password_connection_string(
        self,
        host: str,
        port: str,
        database: str,
    ) -> str:
        """Build password-based connection string (development)."""
        user = os.environ.get("POSTGRES_USER") or os.environ.get("POSTGIS_USER", "postgres")
        password = os.environ.get("POSTGRES_PASSWORD") or os.environ.get("POSTGIS_PASSWORD", "")

        if not password:
            raise ValueError(
                "No authentication configured. "
                "Set USE_MANAGED_IDENTITY=true or provide POSTGRES_PASSWORD."
            )

        conn_str = (
            f"postgresql://{user}:{password}@{host}:{port}/{database}?sslmode=require"
        )

        logger.debug(f"Password connection string built for {database}")
        return conn_str

    @contextmanager
    def get_connection(self):
        """
        Context manager for PostgreSQL connections.

        Yields:
            psycopg connection with dict_row factory

        Usage:
            with repo.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
        """
        conn = None
        try:
            logger.debug("Connecting to PostgreSQL...")
            conn = psycopg.connect(self.conn_string, row_factory=dict_row)
            logger.debug("PostgreSQL connection established")
            yield conn

        except psycopg.Error as e:
            logger.error(f"PostgreSQL connection error: {e}")
            if conn:
                conn.rollback()
            raise

        finally:
            if conn:
                conn.close()

    @contextmanager
    def get_cursor(self, conn=None):
        """
        Context manager for PostgreSQL cursors.

        Args:
            conn: Optional existing connection (for transactions)

        Yields:
            psycopg cursor

        Usage:
            with repo.get_cursor() as cur:
                cur.execute("SELECT 1")
                # Auto-commits on success
        """
        if conn:
            # Use existing connection - caller controls transaction
            with conn.cursor() as cursor:
                yield cursor
        else:
            # Create new connection with auto-commit
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    yield cursor
                    conn.commit()

    def execute(self, query: str, params: tuple = None) -> None:
        """Execute a query without returning results."""
        with self.get_cursor() as cur:
            cur.execute(query, params)

    def fetch_one(self, query: str, params: tuple = None) -> Optional[Dict[str, Any]]:
        """Execute query and fetch one result."""
        with self.get_cursor() as cur:
            cur.execute(query, params)
            return cur.fetchone()

    def fetch_all(self, query: str, params: tuple = None) -> list:
        """Execute query and fetch all results."""
        with self.get_cursor() as cur:
            cur.execute(query, params)
            return cur.fetchall()


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

_default_repo: Optional[PostgreSQLRepository] = None
_repo_lock = threading.Lock()


def get_postgres_repository() -> PostgreSQLRepository:
    """Get shared PostgreSQL repository instance."""
    global _default_repo
    if _default_repo is None:
        with _repo_lock:
            if _default_repo is None:
                _default_repo = PostgreSQLRepository()
    return _default_repo


# ============================================================================
# EXPORTS
# ============================================================================

__all__ = [
    "PostgreSQLRepository",
    "get_postgres_repository",
]
