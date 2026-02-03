# ============================================================================
# DISTRIBUTED LOCKING SERVICE
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Infrastructure - Concurrency control
# PURPOSE: PostgreSQL advisory locks for orchestrator coordination
# CREATED: 02 FEB 2026
# ============================================================================
"""
Distributed Locking Service

Uses PostgreSQL advisory locks for coordination:
- Session-level locks for long-running processes (orchestrator)
- Transaction-level locks for job processing
- Optimistic locking via version numbers for fine-grained control

Advisory locks are:
- Fast (in-memory, no disk I/O)
- Auto-release on disconnect (crash-safe)
- Support non-blocking try_lock semantics
- 64-bit key space

Lock Layers:
- Layer 1: Orchestrator lock - only one orchestrator runs
- Layer 2: Job lock - process jobs safely in parallel (future scaling)
- Layer 3: Version columns (handled in repositories, not here)
- Layer 4: Conditional WHERE clauses (already exists)

Usage:
    from infrastructure.locking import LockService

    lock_service = LockService(pool)

    # Orchestrator startup
    if not await lock_service.try_acquire_orchestrator_lock():
        raise RuntimeError("Another orchestrator is running")

    # Job processing
    async with lock_service.job_lock(job_id) as acquired:
        if acquired:
            await process_job(job)
"""

import hashlib
import logging
from contextlib import asynccontextmanager
from typing import Optional

from psycopg_pool import AsyncConnectionPool

logger = logging.getLogger(__name__)


class LockService:
    """
    PostgreSQL-based distributed locking.

    Provides two types of locks:
    1. Orchestrator lock (session-level): Ensures single orchestrator
    2. Job locks (transaction-level): Per-job coordination

    All locks use PostgreSQL advisory locks which:
    - Are fast (in-memory)
    - Auto-release on disconnect
    - Support non-blocking acquisition
    """

    # Lock namespace prefixes (hashed to int8 for pg_advisory_lock)
    ORCHESTRATOR_LOCK = "rmhdag:orchestrator"
    JOB_LOCK_PREFIX = "rmhdag:job:"

    def __init__(self, pool: AsyncConnectionPool):
        """
        Initialize lock service.

        Args:
            pool: Database connection pool
        """
        self.pool = pool
        self._orchestrator_conn = None  # Held connection for orchestrator lock

    @staticmethod
    def _hash_to_lock_id(key: str) -> int:
        """
        Convert string key to int64 for PostgreSQL advisory lock.

        PostgreSQL advisory locks use bigint keys. We hash our string
        keys to get consistent int64 values.

        Args:
            key: String key to hash

        Returns:
            Signed int64 suitable for pg_advisory_lock
        """
        # Use first 8 bytes of SHA256, interpret as signed int64
        h = hashlib.sha256(key.encode()).digest()[:8]
        return int.from_bytes(h, byteorder='big', signed=True)

    # =========================================================================
    # LAYER 1: ORCHESTRATOR LOCK (Session-level)
    # =========================================================================

    async def try_acquire_orchestrator_lock(self) -> bool:
        """
        Try to acquire the global orchestrator lock.

        Only one orchestrator should run at a time. This lock is held
        for the lifetime of the orchestrator process via a dedicated
        connection that stays open.

        The lock auto-releases if:
        - release_orchestrator_lock() is called
        - The connection is closed
        - The process crashes

        Returns:
            True if lock acquired, False if another orchestrator holds it
        """
        lock_id = self._hash_to_lock_id(self.ORCHESTRATOR_LOCK)

        # Get a dedicated connection to hold the session-level lock
        # This connection must stay open for the lock to be held
        self._orchestrator_conn = await self.pool.getconn()

        try:
            result = await self._orchestrator_conn.execute(
                "SELECT pg_try_advisory_lock(%s) as acquired",
                (lock_id,),
            )
            row = await result.fetchone()
            # Handle both dict_row and tuple row factories
            if row:
                acquired = row["acquired"] if hasattr(row, 'keys') else row[0]
            else:
                acquired = False

            if acquired:
                logger.info(
                    f"Acquired orchestrator lock (lock_id={lock_id})"
                )
            else:
                logger.warning(
                    "Failed to acquire orchestrator lock - "
                    "another instance may be running"
                )
                # Return connection since we didn't get the lock
                await self.pool.putconn(self._orchestrator_conn)
                self._orchestrator_conn = None

            return acquired

        except Exception as e:
            logger.error(f"Error acquiring orchestrator lock: {e}")
            if self._orchestrator_conn:
                await self.pool.putconn(self._orchestrator_conn)
                self._orchestrator_conn = None
            return False

    async def release_orchestrator_lock(self) -> None:
        """
        Release the orchestrator lock.

        Called during graceful shutdown. Also releases automatically
        if the connection closes unexpectedly.
        """
        if self._orchestrator_conn:
            lock_id = self._hash_to_lock_id(self.ORCHESTRATOR_LOCK)
            try:
                await self._orchestrator_conn.execute(
                    "SELECT pg_advisory_unlock(%s)",
                    (lock_id,),
                )
                logger.info(
                    f"Released orchestrator lock (lock_id={lock_id})"
                )
            except Exception as e:
                logger.warning(f"Error releasing orchestrator lock: {e}")
            finally:
                try:
                    await self.pool.putconn(self._orchestrator_conn)
                except Exception:
                    pass  # Best effort cleanup
                self._orchestrator_conn = None

    @property
    def has_orchestrator_lock(self) -> bool:
        """Check if this service holds the orchestrator lock."""
        return self._orchestrator_conn is not None

    # =========================================================================
    # LAYER 2: JOB LOCK (Transaction-level)
    # =========================================================================

    @asynccontextmanager
    async def job_lock(self, job_id: str, blocking: bool = False):
        """
        Context manager for job-level locking.

        Uses transaction-level advisory locks which auto-release when
        the connection context exits.

        Args:
            job_id: Job to lock
            blocking: If True, wait for lock. If False, return immediately
                     with acquired=False if lock unavailable.

        Yields:
            bool: True if lock acquired, False if not (only when blocking=False)

        Usage:
            async with lock_service.job_lock(job_id) as acquired:
                if acquired:
                    await process_job(job)
                else:
                    logger.debug("Job locked, skipping")
        """
        lock_id = self._hash_to_lock_id(f"{self.JOB_LOCK_PREFIX}{job_id}")

        async with self.pool.connection() as conn:
            if blocking:
                # Blocking: wait for lock (use with caution)
                await conn.execute(
                    "SELECT pg_advisory_xact_lock(%s)",
                    (lock_id,),
                )
                acquired = True
                logger.debug(f"Acquired blocking lock for job {job_id}")
            else:
                # Non-blocking: return immediately if lock unavailable
                result = await conn.execute(
                    "SELECT pg_try_advisory_xact_lock(%s) as acquired",
                    (lock_id,),
                )
                row = await result.fetchone()
                # Handle both dict_row and tuple row factories
                if row:
                    acquired = row["acquired"] if hasattr(row, 'keys') else row[0]
                else:
                    acquired = False

                if acquired:
                    logger.debug(f"Acquired lock for job {job_id}")
                else:
                    logger.debug(
                        f"Job {job_id} locked by another process, skipping"
                    )

            try:
                yield acquired
            finally:
                # Transaction-level locks auto-release when connection
                # context exits, but we log for debugging
                if acquired:
                    logger.debug(f"Released lock for job {job_id}")

    async def is_job_locked(self, job_id: str) -> bool:
        """
        Check if a job is currently locked (without acquiring).

        Note: This is a point-in-time check. The lock status may change
        immediately after this returns.

        Args:
            job_id: Job to check

        Returns:
            True if job is locked by any process
        """
        lock_id = self._hash_to_lock_id(f"{self.JOB_LOCK_PREFIX}{job_id}")

        async with self.pool.connection() as conn:
            # Try to acquire, then immediately release if successful
            result = await conn.execute(
                "SELECT pg_try_advisory_lock(%s) as acquired",
                (lock_id,),
            )
            row = await result.fetchone()
            # Handle both dict_row and tuple row factories
            if row:
                acquired = row["acquired"] if hasattr(row, 'keys') else row[0]
            else:
                acquired = False

            if acquired:
                # We got it, so it wasn't locked - release immediately
                await conn.execute(
                    "SELECT pg_advisory_unlock(%s)",
                    (lock_id,),
                )
                return False
            else:
                # Couldn't acquire, so it's locked
                return True


class LockNotAcquired(Exception):
    """
    Raised when a required lock cannot be acquired.

    Use this when blocking=False and you need to signal the failure
    as an exception rather than a boolean return.
    """

    def __init__(self, lock_type: str, key: str):
        self.lock_type = lock_type
        self.key = key
        super().__init__(f"Failed to acquire {lock_type} lock for {key}")


# ============================================================================
# EXPORTS
# ============================================================================

__all__ = ['LockService', 'LockNotAcquired']
