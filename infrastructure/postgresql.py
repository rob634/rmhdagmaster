# ============================================================================
# POSTGRESQL CONNECTION INFRASTRUCTURE
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Infrastructure - PostgreSQL connection handling
# PURPOSE: Database connectivity with managed identity support
# CREATED: 31 JAN 2026
# UPDATED: 02 FEB 2026 - Added async support following rmhgeoapi patterns
# ============================================================================
"""
PostgreSQL Connection Infrastructure

Provides database connectivity for DAG orchestrator and workers:
- Managed identity authentication (production)
- Password authentication (development)
- Connection pooling for Docker workers
- Context managers for safe resource management
- Both sync and async operations

Adapted from rmhgeoapi/infrastructure/postgresql.py for DAG orchestrator.

Authentication Priority:
1. User-assigned managed identity (AZURE_CLIENT_ID / DAG_DB_IDENTITY_CLIENT_ID)
2. System-assigned managed identity (WEBSITE_SITE_NAME detected)
3. Password authentication (DAG_DB_PASSWORD - dev only)
"""

import os
import logging
import threading
from typing import Any, Dict, List, Optional
from contextlib import contextmanager, asynccontextmanager

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

logger = logging.getLogger(__name__)


# ============================================================================
# POSTGRESQL REPOSITORY BASE (SYNC)
# ============================================================================

class PostgreSQLRepository:
    """
    Base repository for PostgreSQL database operations (synchronous).

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
        host = os.environ.get("DAG_DB_HOST")
        port = os.environ.get("DAG_DB_PORT", "5432")
        database = os.environ.get("DAG_DB_NAME")

        if not host or not database:
            raise ValueError(
                "Database connection not configured. "
                "Set DAG_DB_HOST and DAG_DB_NAME environment variables."
            )

        # Check for managed identity
        use_managed_identity = os.environ.get("USE_MANAGED_IDENTITY", "").lower() == "true"
        client_id = os.environ.get("DAG_DB_IDENTITY_CLIENT_ID")
        identity_name = os.environ.get("DAG_DB_IDENTITY_NAME")

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
        user = os.environ.get("DAG_DB_USER", "postgres")
        password = os.environ.get("DAG_DB_PASSWORD", "")

        if not password:
            raise ValueError(
                "No authentication configured. "
                "Set USE_MANAGED_IDENTITY=true or provide DAG_DB_PASSWORD."
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

    # Alias for rmhgeoapi compatibility
    def _get_connection(self):
        """Alias for get_connection (rmhgeoapi compatibility)."""
        return self.get_connection()

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

    def execute_composed(self, query: sql.Composed, params: tuple = None) -> None:
        """Execute a composed SQL query without returning results."""
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

    # ========================================================================
    # SCHEMA OPERATIONS
    # ========================================================================

    def check_schema_exists(self, schema_name: str) -> bool:
        """Check if a schema exists."""
        result = self.fetch_one(
            "SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = %s) as exists",
            (schema_name,)
        )
        return result["exists"] if result else False

    def check_table_exists(self, schema_name: str, table_name: str) -> bool:
        """Check if a table exists in a schema."""
        result = self.fetch_one(
            """
            SELECT EXISTS(
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
            ) as exists
            """,
            (schema_name, table_name)
        )
        return result["exists"] if result else False

    def get_tables_in_schema(self, schema_name: str) -> List[str]:
        """Get list of tables in a schema."""
        results = self.fetch_all(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s AND table_type = 'BASE TABLE'
            ORDER BY table_name
            """,
            (schema_name,)
        )
        return [r["table_name"] for r in results]

    def get_table_row_count(self, schema_name: str, table_name: str) -> int:
        """Get approximate row count for a table."""
        result = self.fetch_one(
            sql.SQL("SELECT COUNT(*) as count FROM {}.{}").format(
                sql.Identifier(schema_name),
                sql.Identifier(table_name)
            ).as_string(psycopg.connect(self.conn_string))
        )
        return result["count"] if result else 0

    def execute_ddl_statements(
        self,
        statements: List[sql.Composed],
        continue_on_error: bool = True
    ) -> Dict[str, Any]:
        """
        Execute a list of DDL statements.

        Args:
            statements: List of sql.Composed DDL statements
            continue_on_error: If True, continue executing after errors

        Returns:
            Dict with executed/skipped counts and any errors
        """
        executed = 0
        skipped = 0
        errors = []

        with self.get_connection() as conn:
            with conn.cursor() as cur:
                for stmt in statements:
                    try:
                        cur.execute(stmt)
                        executed += 1
                    except Exception as e:
                        error_msg = str(e)
                        if "already exists" in error_msg.lower():
                            skipped += 1
                        else:
                            errors.append(error_msg)
                            if not continue_on_error:
                                raise
                            logger.warning(f"DDL statement error (continuing): {e}")

                conn.commit()

        return {
            "executed": executed,
            "skipped": skipped,
            "errors": errors,
            "success": len(errors) == 0,
        }


# ============================================================================
# ASYNC POSTGRESQL REPOSITORY
# ============================================================================

class AsyncPostgreSQLRepository:
    """
    Async repository for PostgreSQL database operations.

    Uses the async connection pool from repositories.database for proper
    Azure AD authentication in async contexts.

    Usage:
        repo = AsyncPostgreSQLRepository()
        result = await repo.execute_ddl_statements(statements)
    """

    def __init__(self, schema_name: str = "dagapp"):
        """
        Initialize async PostgreSQL repository.

        Args:
            schema_name: Schema name for operations
        """
        self.schema_name = schema_name

    async def _get_pool(self):
        """Get the async connection pool."""
        from repositories.database import get_pool
        return await get_pool()

    @asynccontextmanager
    async def get_connection(self):
        """
        Async context manager for PostgreSQL connections.

        Yields:
            Async psycopg connection with dict_row factory
        """
        pool = await self._get_pool()
        async with pool.connection() as conn:
            conn.row_factory = dict_row
            yield conn

    async def check_schema_exists(self, schema_name: str) -> bool:
        """Check if a schema exists."""
        async with self.get_connection() as conn:
            result = await conn.execute(
                "SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = %s) as exists",
                (schema_name,)
            )
            row = await result.fetchone()
            return row["exists"] if row else False

    async def check_table_exists(self, schema_name: str, table_name: str) -> bool:
        """Check if a table exists in a schema."""
        async with self.get_connection() as conn:
            result = await conn.execute(
                """
                SELECT EXISTS(
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = %s AND table_name = %s
                ) as exists
                """,
                (schema_name, table_name)
            )
            row = await result.fetchone()
            return row["exists"] if row else False

    async def get_tables_in_schema(self, schema_name: str) -> List[str]:
        """Get list of tables in a schema."""
        async with self.get_connection() as conn:
            result = await conn.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = %s AND table_type = 'BASE TABLE'
                ORDER BY table_name
                """,
                (schema_name,)
            )
            rows = await result.fetchall()
            return [r["table_name"] for r in rows]

    async def get_enum_types(self, schema_name: str) -> List[str]:
        """Get list of enum types in a schema."""
        async with self.get_connection() as conn:
            result = await conn.execute(
                """
                SELECT typname
                FROM pg_type t
                JOIN pg_namespace n ON t.typnamespace = n.oid
                WHERE n.nspname = %s AND t.typtype = 'e'
                ORDER BY typname
                """,
                (schema_name,)
            )
            rows = await result.fetchall()
            return [r["typname"] for r in rows]

    async def get_table_info(self, schema_name: str) -> Dict[str, Dict[str, Any]]:
        """Get table info including column counts."""
        async with self.get_connection() as conn:
            result = await conn.execute(
                """
                SELECT
                    t.table_name,
                    (SELECT COUNT(*) FROM information_schema.columns c
                     WHERE c.table_schema = t.table_schema
                     AND c.table_name = t.table_name) as column_count
                FROM information_schema.tables t
                WHERE t.table_schema = %s AND t.table_type = 'BASE TABLE'
                ORDER BY t.table_name
                """,
                (schema_name,)
            )
            rows = await result.fetchall()
            return {
                r["table_name"]: {"columns": r["column_count"]}
                for r in rows
            }

    async def get_table_row_count(self, schema_name: str, table_name: str) -> int:
        """Get row count for a table."""
        async with self.get_connection() as conn:
            try:
                result = await conn.execute(
                    sql.SQL("SELECT COUNT(*) as count FROM {}.{}").format(
                        sql.Identifier(schema_name),
                        sql.Identifier(table_name)
                    )
                )
                row = await result.fetchone()
                return row["count"] if row else -1
            except Exception:
                return -1  # Table doesn't exist

    async def execute_ddl_statements(
        self,
        statements: List[sql.Composed],
        continue_on_error: bool = True
    ) -> Dict[str, Any]:
        """
        Execute a list of DDL statements asynchronously.

        Args:
            statements: List of sql.Composed DDL statements
            continue_on_error: If True, continue executing after errors

        Returns:
            Dict with executed/skipped counts and any errors
        """
        executed = 0
        skipped = 0
        errors = []

        async with self.get_connection() as conn:
            for stmt in statements:
                try:
                    await conn.execute(stmt)
                    executed += 1
                except Exception as e:
                    error_msg = str(e)
                    if "already exists" in error_msg.lower():
                        skipped += 1
                    else:
                        errors.append(error_msg)
                        if not continue_on_error:
                            raise
                        logger.warning(f"DDL statement error (continuing): {e}")

        return {
            "executed": executed,
            "skipped": skipped,
            "errors": errors,
            "success": len(errors) == 0,
        }


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

_default_repo: Optional[PostgreSQLRepository] = None
_repo_lock = threading.Lock()


def get_postgres_repository() -> PostgreSQLRepository:
    """Get shared PostgreSQL repository instance (sync)."""
    global _default_repo
    if _default_repo is None:
        with _repo_lock:
            if _default_repo is None:
                _default_repo = PostgreSQLRepository()
    return _default_repo


def get_async_postgres_repository(schema_name: str = "dagapp") -> AsyncPostgreSQLRepository:
    """Get async PostgreSQL repository instance."""
    return AsyncPostgreSQLRepository(schema_name=schema_name)


# ============================================================================
# EXPORTS
# ============================================================================

__all__ = [
    "PostgreSQLRepository",
    "AsyncPostgreSQLRepository",
    "get_postgres_repository",
    "get_async_postgres_repository",
]
