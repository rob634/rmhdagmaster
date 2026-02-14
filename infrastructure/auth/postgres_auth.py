# ============================================================================
# POSTGRESQL OAUTH AUTHENTICATION
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# PURPOSE: Managed Identity authentication for Azure PostgreSQL
# CREATED: 29 JAN 2026
# ============================================================================
"""
PostgreSQL OAuth authentication for DAG Orchestrator.

Acquires OAuth tokens for Azure Database for PostgreSQL using Managed Identity.
Tokens are cached and refreshed automatically before expiry.

Authentication Flow:
-------------------
1. Orchestrator starts → get_postgres_connection_string() called
2. ManagedIdentityCredential acquires token for PostgreSQL scope
3. Token cached with expiry time
4. Token refreshed when within 5 minutes of expiry
5. Connection string returned with current token as password

Environment Variables:
---------------------
USE_MANAGED_IDENTITY=true (required for UMI auth)
AZURE_CLIENT_ID=<guid>  # User-assigned MI client ID
DAG_DB_IDENTITY_NAME=<identity-name>  # PostgreSQL user name
DAG_DB_HOST=<server>.postgres.database.azure.com
DAG_DB_NAME=<database>
DAG_DB_PORT=5432

For password auth (local development):
DAG_DB_USER=<user>
DAG_DB_PASSWORD=<password>
USE_MANAGED_IDENTITY=false

Usage:
------
```python
from infrastructure.auth import get_postgres_connection_string

# Get connection string with current token
conn_str = get_postgres_connection_string()

# Use with psycopg
conn = psycopg.connect(conn_str)
```
"""

import os
import logging
from datetime import datetime, timezone
from typing import Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# OAuth scope for Azure Database for PostgreSQL
POSTGRES_SCOPE = "https://ossrdbms-aad.database.windows.net/.default"

# Refresh tokens when less than 5 minutes until expiry
TOKEN_REFRESH_BUFFER_SECS = 300


@dataclass
class TokenCache:
    """Simple in-memory token cache."""
    token: Optional[str] = None
    expires_at: Optional[datetime] = None

    def get_if_valid(self, min_ttl_seconds: int = 0) -> Optional[str]:
        """Get token if valid and has sufficient TTL."""
        if not self.token or not self.expires_at:
            return None

        now = datetime.now(timezone.utc)
        remaining = (self.expires_at - now).total_seconds()

        if remaining <= min_ttl_seconds:
            return None

        return self.token

    def set(self, token: str, expires_at: datetime) -> None:
        """Cache a new token."""
        self.token = token
        self.expires_at = expires_at

    def invalidate(self) -> None:
        """Clear the cache."""
        self.token = None
        self.expires_at = None

    def ttl_seconds(self) -> float:
        """Get remaining TTL in seconds."""
        if not self.expires_at:
            return 0
        return (self.expires_at - datetime.now(timezone.utc)).total_seconds()


# Global token cache
_token_cache = TokenCache()


def _get_env(key: str, default: str = "") -> str:
    """Get environment variable with default."""
    return os.environ.get(key, default)


def _use_managed_identity() -> bool:
    """Check if managed identity authentication is enabled."""
    return _get_env("USE_MANAGED_IDENTITY", "false").lower() == "true"


def get_postgres_token() -> Optional[str]:
    """
    Get PostgreSQL OAuth token using Managed Identity.

    Uses caching with automatic refresh when token is within 5 minutes of expiry.

    Returns:
        OAuth bearer token for Azure Database for PostgreSQL.

    Raises:
        Exception: If token acquisition fails.
    """
    if not _use_managed_identity():
        logger.debug("Managed identity disabled, using password auth")
        return None

    # Check cache first
    cached = _token_cache.get_if_valid(min_ttl_seconds=TOKEN_REFRESH_BUFFER_SECS)
    if cached:
        ttl = _token_cache.ttl_seconds()
        logger.debug(f"Using cached PostgreSQL token, TTL: {ttl:.0f}s")
        return cached

    # Acquire new token
    host = _get_env("DAG_DB_HOST", "localhost")
    database = _get_env("DAG_DB_NAME", "postgres")
    identity_name = _get_env("DAG_DB_IDENTITY_NAME", "")

    logger.info("=" * 60)
    logger.info("Acquiring PostgreSQL OAuth token...")
    logger.info(f"Host: {host}")
    logger.info(f"Database: {database}")
    logger.info(f"Identity: {identity_name}")
    logger.info("=" * 60)

    try:
        from azure.identity import ManagedIdentityCredential, DefaultAzureCredential
        from azure.core.exceptions import ClientAuthenticationError

        # Use user-assigned MI if client ID is set
        client_id = _get_env("AZURE_CLIENT_ID") or _get_env("DAG_DB_IDENTITY_CLIENT_ID")

        if client_id:
            logger.info(f"Using user-assigned Managed Identity: {client_id[:8]}...")
            credential = ManagedIdentityCredential(client_id=client_id)
        else:
            logger.info("Using DefaultAzureCredential (system MI or az login)")
            credential = DefaultAzureCredential()

        token_response = credential.get_token(POSTGRES_SCOPE)

        access_token = token_response.token
        expires_at = datetime.fromtimestamp(token_response.expires_on, tz=timezone.utc)

        # Cache the token
        _token_cache.set(access_token, expires_at)

        logger.info(f"PostgreSQL token acquired, expires: {expires_at.isoformat()}")
        logger.debug(f"Token length: {len(access_token)} characters")

        return access_token

    except ClientAuthenticationError as e:
        logger.error("=" * 60)
        logger.error("FAILED TO GET POSTGRESQL OAUTH TOKEN")
        logger.error("=" * 60)
        logger.error(f"Error: {e}")
        logger.error("")
        logger.error("Troubleshooting:")
        logger.error("  - Verify Managed Identity is assigned to Web App")
        logger.error("  - Verify database user exists and matches MI name")
        logger.error(f"  - Expected user: {identity_name}")
        logger.error("  - Run: SELECT * FROM pgaadauth_list_principals();")
        logger.error("=" * 60)
        raise

    except Exception as e:
        logger.error(f"PostgreSQL token acquisition failed: {type(e).__name__}: {e}")
        raise


def get_postgres_connection_string() -> str:
    """
    Build PostgreSQL connection string with OAuth token or password.

    Returns:
        PostgreSQL connection string in psycopg format.

    Raises:
        ValueError: If no authentication method is configured.
        Exception: If token acquisition fails.
    """
    host = _get_env("DAG_DB_HOST", "localhost")
    port = _get_env("DAG_DB_PORT", "5432")
    database = _get_env("DAG_DB_NAME", "postgres")

    if not _use_managed_identity():
        # Fall back to password auth
        user = _get_env("DAG_DB_USER", "postgres")
        password = _get_env("DAG_DB_PASSWORD", "")

        if not password:
            # Check for DATABASE_URL
            database_url = _get_env("DAG_DB_URL")
            if database_url:
                return database_url

            raise ValueError(
                "No PostgreSQL authentication configured. "
                "Set USE_MANAGED_IDENTITY=true or provide DAG_DB_PASSWORD or DAG_DB_URL"
            )

        return (
            f"host={host} "
            f"port={port} "
            f"dbname={database} "
            f"user={user} "
            f"password={password} "
            f"sslmode=require"
        )

    # Get OAuth token
    token = get_postgres_token()
    if not token:
        raise ValueError("Failed to acquire PostgreSQL OAuth token")

    # Build connection string with token as password
    identity_name = _get_env("DAG_DB_IDENTITY_NAME", "")

    return (
        f"host={host} "
        f"port={port} "
        f"dbname={database} "
        f"user={identity_name} "
        f"password={token} "
        f"sslmode=require"
    )


def get_postgres_token_status() -> dict:
    """
    Get PostgreSQL token status for health checks.

    Returns:
        Dict with token status information.
    """
    if not _use_managed_identity():
        return {
            "auth_type": "password",
            "token_cached": False,
        }

    return {
        "auth_type": "managed_identity",
        "token_cached": _token_cache.token is not None,
        "ttl_seconds": _token_cache.ttl_seconds() if _token_cache.token else 0,
        "expires_at": _token_cache.expires_at.isoformat() if _token_cache.expires_at else None,
    }
