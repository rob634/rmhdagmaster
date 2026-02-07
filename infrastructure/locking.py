# ============================================================================
# DISTRIBUTED LOCKING SERVICE
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Infrastructure - Concurrency control (PARTIALLY DEPRECATED)
# PURPOSE: Advisory locks for job coordination
# CREATED: 02 FEB 2026
# UPDATED: 04 FEB 2026 - Layer 1 DEPRECATED (multi-orchestrator replaces lease)
# ============================================================================
"""
Distributed Locking Service

DEPRECATION NOTICE (04 FEB 2026):
    Layer 1 (Orchestrator Lease) is DEPRECATED and should not be used.
    The multi-orchestrator architecture replaces the single-leader lease model
    with competing consumers and owner_id-based job ownership.

    See: orchestrator/loop.py for the new multi-instance design.
    See: docs/MULTI_ORCH_IMPL_PLAN.md for architecture details.

Layer 1: Orchestrator Lease (DEPRECATED)
    - DO NOT USE - retained for backwards compatibility only
    - The new architecture allows multiple orchestrators to run simultaneously
    - Jobs are owned via owner_id column, not global lease

Layer 2: Job Locks (Advisory locks) - ACTIVE
    - Transaction-level locks for job processing
    - Fast (in-memory, no disk I/O)
    - Auto-release at transaction end
    - May be useful for future horizontal scaling within a single orchestrator

Current Usage (multi-orchestrator):
    The orchestrator no longer uses LockService for startup.
    Job locks may still be used for fine-grained coordination if needed.

    # Job processing with advisory lock (optional, for extra safety)
    async with lock_service.job_lock(job_id) as acquired:
        if acquired:
            await process_job(job)
"""

import asyncio
import hashlib
import logging
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional

from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

logger = logging.getLogger(__name__)


class LockService:
    """
    PostgreSQL-based distributed locking.

    Provides two types of locks:
    1. Orchestrator lease (table-based): Ensures single orchestrator with crash recovery
    2. Job locks (advisory): Per-job coordination for future horizontal scaling
    """

    # Job lock namespace prefix (hashed to int8 for pg_advisory_lock)
    JOB_LOCK_PREFIX = "rmhdag:job:"

    # Lease configuration
    LEASE_TTL_SEC = 30          # Lease expires after 30 seconds without pulse
    PULSE_INTERVAL_SEC = 10     # Renew lease every 10 seconds (TTL/3)

    def __init__(self, pool: AsyncConnectionPool):
        """
        Initialize lock service.

        Args:
            pool: Database connection pool
        """
        self.pool = pool
        self._holder_id = str(uuid.uuid4())  # Unique ID for this instance
        self._has_lease = False
        self._pulse_task: Optional[asyncio.Task] = None

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
        h = hashlib.sha256(key.encode()).digest()[:8]
        return int.from_bytes(h, byteorder='big', signed=True)

    # =========================================================================
    # LAYER 1: ORCHESTRATOR LEASE (DEPRECATED - Do not use)
    # =========================================================================
    # The multi-orchestrator architecture uses owner_id-based job ownership
    # instead of global lease. These methods are retained for backwards
    # compatibility but will log deprecation warnings.
    # =========================================================================

    async def try_acquire_orchestrator_lease(self) -> bool:
        """
        DEPRECATED: Try to acquire the orchestrator lease.

        WARNING: This method is deprecated. The multi-orchestrator architecture
        uses owner_id-based job ownership instead of global lease. Multiple
        orchestrators can now run simultaneously without coordination.

        Succeeds if:
        - No lease exists (first orchestrator)
        - Existing lease has expired (previous holder crashed/stopped pulsing)
        - We already hold the lease (reconnecting after brief disconnect)

        Returns:
            True if lease acquired, False if another orchestrator holds active lease
        """
        import warnings
        warnings.warn(
            "try_acquire_orchestrator_lease is deprecated. "
            "Use multi-orchestrator with owner_id-based job ownership instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        try:
            async with self.pool.connection() as conn:
                conn.row_factory = dict_row
                result = await conn.execute(
                    """
                    INSERT INTO dagapp.dag_orchestrator_lease
                        (lock_name, holder_id, acquired_at, pulse_at, lease_ttl_sec)
                    VALUES
                        ('orchestrator', %(holder_id)s, NOW(), NOW(), %(ttl)s)
                    ON CONFLICT (lock_name) DO UPDATE
                    SET
                        holder_id = EXCLUDED.holder_id,
                        acquired_at = NOW(),
                        pulse_at = NOW()
                    WHERE
                        -- Same holder reconnecting
                        dag_orchestrator_lease.holder_id = EXCLUDED.holder_id
                        -- OR lease has expired (pulse_at + ttl < now)
                        OR dag_orchestrator_lease.pulse_at <
                           NOW() - INTERVAL '1 second' * dag_orchestrator_lease.lease_ttl_sec
                    RETURNING holder_id = %(holder_id)s AS acquired
                    """,
                    {"holder_id": self._holder_id, "ttl": self.LEASE_TTL_SEC},
                )
                row = await result.fetchone()

                if row and row["acquired"]:
                    self._has_lease = True
                    logger.info(
                        f"Acquired orchestrator lease "
                        f"(holder_id={self._holder_id[:8]}..., ttl={self.LEASE_TTL_SEC}s)"
                    )
                    return True
                else:
                    # Check who holds the lease and when it expires
                    info = await conn.execute(
                        """
                        SELECT holder_id, pulse_at, lease_ttl_sec,
                               pulse_at + INTERVAL '1 second' * lease_ttl_sec AS expires_at
                        FROM dagapp.dag_orchestrator_lease
                        WHERE lock_name = 'orchestrator'
                        """
                    )
                    lease_row = await info.fetchone()
                    if lease_row:
                        logger.warning(
                            f"Failed to acquire orchestrator lease - held by {lease_row['holder_id'][:8]}..., "
                            f"expires at {lease_row['expires_at']}"
                        )
                    else:
                        logger.warning(
                            "Failed to acquire orchestrator lease - unexpected state"
                        )
                    return False

        except Exception as e:
            logger.error(f"Error acquiring orchestrator lease: {e}")
            return False

    async def pulse_lease(self) -> bool:
        """
        DEPRECATED: Renew the orchestrator lease (send pulse).

        WARNING: This method is deprecated. Use multi-orchestrator instead.

        Called periodically (every PULSE_INTERVAL_SEC) to keep the lease alive.
        If this fails, we've lost the lease and should stop orchestrating.

        Returns:
            True if pulse succeeded, False if we lost the lease
        """
        import warnings
        warnings.warn(
            "pulse_lease is deprecated. Use multi-orchestrator with heartbeat instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        try:
            async with self.pool.connection() as conn:
                conn.row_factory = dict_row
                result = await conn.execute(
                    """
                    UPDATE dagapp.dag_orchestrator_lease
                    SET pulse_at = NOW()
                    WHERE lock_name = 'orchestrator' AND holder_id = %(holder_id)s
                    RETURNING true AS pulsed
                    """,
                    {"holder_id": self._holder_id},
                )
                row = await result.fetchone()

                if row and row["pulsed"]:
                    logger.debug(f"Lease pulse sent (holder_id={self._holder_id[:8]}...)")
                    return True
                else:
                    logger.error(
                        f"Lease pulse failed - lost the lease! "
                        f"(holder_id={self._holder_id[:8]}...)"
                    )
                    self._has_lease = False
                    return False

        except Exception as e:
            logger.error(f"Error pulsing lease: {e}")
            return False

    async def release_orchestrator_lease(self) -> None:
        """
        DEPRECATED: Release the orchestrator lease (graceful shutdown).

        WARNING: This method is deprecated. Use multi-orchestrator instead.

        Called during normal shutdown. Allows new orchestrator to acquire
        immediately without waiting for TTL expiry.
        """
        import warnings
        warnings.warn(
            "release_orchestrator_lease is deprecated. "
            "Multi-orchestrator uses owner_id-based ownership.",
            DeprecationWarning,
            stacklevel=2,
        )
        if self._has_lease:
            try:
                async with self.pool.connection() as conn:
                    await conn.execute(
                        """
                        DELETE FROM dagapp.dag_orchestrator_lease
                        WHERE lock_name = 'orchestrator' AND holder_id = %(holder_id)s
                        """,
                        {"holder_id": self._holder_id},
                    )
                logger.info(
                    f"Released orchestrator lease (holder_id={self._holder_id[:8]}...)"
                )
            except Exception as e:
                logger.warning(f"Error releasing lease: {e}")
            finally:
                self._has_lease = False

    async def start_pulse_task(self) -> None:
        """
        DEPRECATED: Start the background pulse task.

        WARNING: This method is deprecated. Use multi-orchestrator instead.

        Must be called after successfully acquiring the lease.
        The pulse task keeps the lease alive by updating pulse_at periodically.
        """
        import warnings
        warnings.warn(
            "start_pulse_task is deprecated. "
            "Multi-orchestrator uses heartbeat loop instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        if self._pulse_task is None and self._has_lease:
            self._pulse_task = asyncio.create_task(self._pulse_loop())
            logger.info(
                f"Started lease pulse task (interval={self.PULSE_INTERVAL_SEC}s)"
            )

    async def stop_pulse_task(self) -> None:
        """
        DEPRECATED: Stop the background pulse task.

        WARNING: This method is deprecated. Use multi-orchestrator instead.

        Called during shutdown before releasing the lease.
        """
        import warnings
        warnings.warn(
            "stop_pulse_task is deprecated. "
            "Multi-orchestrator uses heartbeat loop instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        if self._pulse_task:
            self._pulse_task.cancel()
            try:
                await self._pulse_task
            except asyncio.CancelledError:
                pass
            self._pulse_task = None
            logger.info("Stopped lease pulse task")

    async def _pulse_loop(self) -> None:
        """
        Background loop that sends pulse every PULSE_INTERVAL_SEC.

        If pulse fails, sets _has_lease to False which should trigger
        orchestrator shutdown.
        """
        while True:
            try:
                await asyncio.sleep(self.PULSE_INTERVAL_SEC)
                if self._has_lease:
                    success = await self.pulse_lease()
                    if not success:
                        # Lost the lease - this is serious
                        logger.error(
                            "Lost orchestrator lease - orchestrator should shut down"
                        )
                        break
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in pulse loop: {e}")

    @property
    def has_orchestrator_lease(self) -> bool:
        """
        DEPRECATED: Check if this service holds the orchestrator lease.

        WARNING: This property is deprecated. Multi-orchestrator uses
        owner_id-based job ownership - no global lease required.
        """
        return self._has_lease

    @property
    def holder_id(self) -> str:
        """
        DEPRECATED: Get this instance's unique holder ID.

        WARNING: This property is deprecated. Use Orchestrator.owner_id instead.
        """
        return self._holder_id

    async def get_lease_info(self) -> Optional[dict]:
        """
        DEPRECATED: Get current lease information.

        WARNING: This method is deprecated. Use orchestrator.stats instead.

        Returns:
            Dict with lease details or None if no lease exists
        """
        import warnings
        warnings.warn(
            "get_lease_info is deprecated. "
            "Use Orchestrator.stats for instance information.",
            DeprecationWarning,
            stacklevel=2,
        )
        try:
            async with self.pool.connection() as conn:
                conn.row_factory = dict_row
                result = await conn.execute(
                    """
                    SELECT
                        lock_name,
                        holder_id,
                        acquired_at,
                        pulse_at,
                        lease_ttl_sec,
                        pulse_at + INTERVAL '1 second' * lease_ttl_sec AS expires_at,
                        CASE
                            WHEN pulse_at + INTERVAL '1 second' * lease_ttl_sec < NOW()
                            THEN true ELSE false
                        END AS is_expired
                    FROM dagapp.dag_orchestrator_lease
                    WHERE lock_name = 'orchestrator'
                    """
                )
                row = await result.fetchone()
                if row:
                    return {
                        "lock_name": row["lock_name"],
                        "holder_id": row["holder_id"],
                        "acquired_at": row["acquired_at"].isoformat() if row["acquired_at"] else None,
                        "pulse_at": row["pulse_at"].isoformat() if row["pulse_at"] else None,
                        "expires_at": row["expires_at"].isoformat() if row["expires_at"] else None,
                        "lease_ttl_sec": row["lease_ttl_sec"],
                        "is_expired": row["is_expired"],
                        "is_held_by_me": row["holder_id"] == self._holder_id,
                    }
                return None
        except Exception as e:
            logger.error(f"Error getting lease info: {e}")
            return None

    # =========================================================================
    # LAYER 2: JOB LOCK (Transaction-level advisory locks)
    # =========================================================================

    @asynccontextmanager
    async def job_lock(self, job_id: str, blocking: bool = False):
        """
        Context manager for job-level locking.

        Uses transaction-level advisory locks which auto-release when
        the connection context exits. Fast and suitable for short-lived
        job processing operations.

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
            conn.row_factory = dict_row
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
                acquired = row["acquired"] if row else False

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
            conn.row_factory = dict_row
            # Try to acquire, then immediately release if successful
            result = await conn.execute(
                "SELECT pg_try_advisory_lock(%s) as acquired",
                (lock_id,),
            )
            row = await result.fetchone()
            acquired = row["acquired"] if row else False

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
