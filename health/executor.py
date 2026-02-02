# ============================================================================
# HEALTH CHECK EXECUTOR
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Infrastructure - Parallel health check execution
# PURPOSE: Execute health checks with timeouts and aggregation
# CREATED: 31 JAN 2026
# ============================================================================
"""
Health Check Executor

Executes health checks with:
- Parallel execution (grouped by priority tiers)
- Per-check timeouts
- Overall execution timeout
- Result aggregation with 'worst wins' semantics

Execution Strategy:
1. Group checks by priority tier
2. Execute each tier sequentially (to catch cascading failures)
3. Within each tier, execute checks in parallel
4. Aggregate results after each tier
5. Optional early termination on critical failure
"""

import asyncio
import logging
import time
from typing import Dict, List, Optional, Set
from concurrent.futures import ThreadPoolExecutor

from health.core import (
    HealthStatus,
    HealthCheckResult,
    HealthCheckPlugin,
    AggregatedHealthResult,
)
from health.registry import HealthCheckRegistry, get_registry

logger = logging.getLogger(__name__)


class HealthCheckExecutor:
    """
    Executes health checks with parallel execution and timeouts.

    Checks are grouped by priority and executed tier-by-tier.
    Within each tier, checks run in parallel for performance.
    """

    # Priority tiers for grouping parallel execution
    TIER_BOUNDARIES = [15, 25, 35, 45, 100]

    def __init__(
        self,
        registry: Optional[HealthCheckRegistry] = None,
        overall_timeout: float = 60.0,
        max_parallel: int = 10,
    ):
        """
        Initialize executor.

        Args:
            registry: Health check registry (uses global if None)
            overall_timeout: Max total execution time
            max_parallel: Max concurrent checks per tier
        """
        self.registry = registry or get_registry()
        self.overall_timeout = overall_timeout
        self.max_parallel = max_parallel
        self._thread_pool = ThreadPoolExecutor(max_workers=max_parallel)

    async def execute_all(
        self,
        early_terminate: bool = False,
    ) -> AggregatedHealthResult:
        """
        Execute all registered health checks.

        Args:
            early_terminate: Stop on first unhealthy result

        Returns:
            Aggregated result with all check outcomes
        """
        start_time = time.monotonic()
        checks = self.registry.get_checks_by_priority()

        if not checks:
            return AggregatedHealthResult(
                status=HealthStatus.HEALTHY,
                checks={},
                total_duration_ms=0.0,
            )

        results: Dict[str, HealthCheckResult] = {}

        # Group checks by priority tier
        tiers = self._group_by_tier(checks)

        for tier_priority, tier_checks in sorted(tiers.items()):
            # Check overall timeout
            elapsed = time.monotonic() - start_time
            if elapsed >= self.overall_timeout:
                logger.warning(
                    f"Health check overall timeout ({self.overall_timeout}s) exceeded"
                )
                for check in tier_checks:
                    results[check.name] = HealthCheckResult.unhealthy(
                        "Skipped: overall timeout exceeded"
                    )
                continue

            # Execute tier in parallel
            tier_results = await self._execute_tier(
                tier_checks,
                remaining_timeout=self.overall_timeout - elapsed,
            )
            results.update(tier_results)

            # Early termination if requested
            if early_terminate:
                tier_statuses = [r.status for r in tier_results.values()]
                if HealthStatus.UNHEALTHY in tier_statuses:
                    logger.info("Early termination: unhealthy check detected")
                    break

        total_duration_ms = (time.monotonic() - start_time) * 1000
        overall_status = HealthStatus.aggregate([r.status for r in results.values()])

        return AggregatedHealthResult(
            status=overall_status,
            checks=results,
            total_duration_ms=total_duration_ms,
        )

    async def execute_required(self) -> AggregatedHealthResult:
        """
        Execute only checks required for /readyz.

        Faster than execute_all() for readiness probes.
        """
        start_time = time.monotonic()
        checks = self.registry.get_required_checks()

        results = await self._execute_tier(
            checks,
            remaining_timeout=self.overall_timeout,
        )

        total_duration_ms = (time.monotonic() - start_time) * 1000
        overall_status = HealthStatus.aggregate([r.status for r in results.values()])

        return AggregatedHealthResult(
            status=overall_status,
            checks=results,
            total_duration_ms=total_duration_ms,
        )

    async def execute_single(
        self,
        name: str,
    ) -> Optional[HealthCheckResult]:
        """Execute a single check by name."""
        check = self.registry.get(name)
        if check is None:
            return None

        return await self._execute_check(check)

    async def _execute_tier(
        self,
        checks: List[HealthCheckPlugin],
        remaining_timeout: float,
    ) -> Dict[str, HealthCheckResult]:
        """Execute a tier of checks in parallel."""
        if not checks:
            return {}

        # Limit parallelism
        semaphore = asyncio.Semaphore(self.max_parallel)

        async def run_with_semaphore(check: HealthCheckPlugin):
            async with semaphore:
                return check.name, await self._execute_check(check)

        # Create tasks
        tasks = [
            asyncio.create_task(run_with_semaphore(check))
            for check in checks
        ]

        # Wait with overall timeout
        try:
            done, pending = await asyncio.wait(
                tasks,
                timeout=remaining_timeout,
                return_when=asyncio.ALL_COMPLETED,
            )

            # Cancel any pending tasks
            for task in pending:
                task.cancel()

        except Exception as e:
            logger.error(f"Error executing health check tier: {e}")
            return {
                check.name: HealthCheckResult.from_exception(e)
                for check in checks
            }

        # Collect results
        results = {}
        for task in done:
            try:
                name, result = task.result()
                results[name] = result
            except Exception as e:
                logger.error(f"Error getting health check result: {e}")

        # Add timeout results for pending
        for task in pending:
            # Find which check this was
            for check in checks:
                if check.name not in results:
                    results[check.name] = HealthCheckResult.unhealthy(
                        f"Timeout after {check.timeout_seconds}s"
                    )

        return results

    async def _execute_check(
        self,
        check: HealthCheckPlugin,
    ) -> HealthCheckResult:
        """Execute a single check with timeout."""
        start_time = time.monotonic()

        try:
            # Run check with timeout
            result = await asyncio.wait_for(
                check.check(),
                timeout=check.timeout_seconds,
            )

            # Add timing
            result.duration_ms = (time.monotonic() - start_time) * 1000

            logger.debug(
                f"Health check {check.name}: {result.status.value} "
                f"({result.duration_ms:.1f}ms)"
            )

            return result

        except asyncio.TimeoutError:
            duration_ms = (time.monotonic() - start_time) * 1000
            logger.warning(
                f"Health check {check.name} timed out after {check.timeout_seconds}s"
            )
            result = HealthCheckResult.unhealthy(
                f"Timeout after {check.timeout_seconds}s"
            )
            result.duration_ms = duration_ms
            return result

        except Exception as e:
            duration_ms = (time.monotonic() - start_time) * 1000
            logger.error(f"Health check {check.name} failed: {e}")
            result = HealthCheckResult.from_exception(e)
            result.duration_ms = duration_ms
            return result

    def _group_by_tier(
        self,
        checks: List[HealthCheckPlugin],
    ) -> Dict[int, List[HealthCheckPlugin]]:
        """Group checks by priority tier."""
        tiers: Dict[int, List[HealthCheckPlugin]] = {}

        for check in checks:
            # Find tier for this priority
            tier = 0
            for boundary in self.TIER_BOUNDARIES:
                if check.priority <= boundary:
                    tier = boundary
                    break

            if tier not in tiers:
                tiers[tier] = []
            tiers[tier].append(check)

        return tiers


# ============================================================================
# EXPORTS
# ============================================================================

__all__ = [
    "HealthCheckExecutor",
]
