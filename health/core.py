# ============================================================================
# HEALTH CHECK CORE TYPES
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Infrastructure - Base classes for health checks
# PURPOSE: Health check plugin interfaces and result types
# CREATED: 31 JAN 2026
# ============================================================================
"""
Health Check Core Types

Defines the plugin interface and result types for health checks.

Status Hierarchy (worst wins):
- healthy: All systems operational
- degraded: Operational with warnings (non-blocking issues)
- unhealthy: Critical failure (blocks operations)

Categories (execution order by priority):
1. Startup (10): Basic process checks
2. Infrastructure (20): Storage, messaging
3. Database (30): PostgreSQL, pgSTAC
4. Application (40): Orchestrator, workflows
5. External (50): Optional external services
"""

from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime


class HealthStatus(str, Enum):
    """Health check status values."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"

    def __lt__(self, other: "HealthStatus") -> bool:
        """Enable comparison for 'worst wins' aggregation."""
        order = {
            HealthStatus.HEALTHY: 0,
            HealthStatus.DEGRADED: 1,
            HealthStatus.UNHEALTHY: 2,
        }
        return order[self] < order[other]

    @classmethod
    def aggregate(cls, statuses: List["HealthStatus"]) -> "HealthStatus":
        """Aggregate multiple statuses (worst wins)."""
        if not statuses:
            return cls.HEALTHY
        return max(statuses)


class HealthCheckCategory(str, Enum):
    """Health check categories with default priorities."""
    STARTUP = "startup"           # Priority 10: Basic process checks
    INFRASTRUCTURE = "infrastructure"  # Priority 20: Storage, messaging
    DATABASE = "database"         # Priority 30: PostgreSQL, pgSTAC
    APPLICATION = "application"   # Priority 40: Orchestrator state
    EXTERNAL = "external"         # Priority 50: Optional external services

    @property
    def default_priority(self) -> int:
        """Get default priority for category."""
        priorities = {
            HealthCheckCategory.STARTUP: 10,
            HealthCheckCategory.INFRASTRUCTURE: 20,
            HealthCheckCategory.DATABASE: 30,
            HealthCheckCategory.APPLICATION: 40,
            HealthCheckCategory.EXTERNAL: 50,
        }
        return priorities[self]


@dataclass
class HealthCheckResult:
    """Result from a single health check."""
    status: HealthStatus
    message: Optional[str] = None
    details: Dict[str, Any] = field(default_factory=dict)
    duration_ms: float = 0.0
    checked_at: datetime = field(default_factory=datetime.utcnow)

    @classmethod
    def healthy(cls, message: str = None, **details) -> "HealthCheckResult":
        """Create healthy result."""
        return cls(status=HealthStatus.HEALTHY, message=message, details=details)

    @classmethod
    def degraded(cls, message: str, **details) -> "HealthCheckResult":
        """Create degraded result."""
        return cls(status=HealthStatus.DEGRADED, message=message, details=details)

    @classmethod
    def unhealthy(cls, message: str, **details) -> "HealthCheckResult":
        """Create unhealthy result."""
        return cls(status=HealthStatus.UNHEALTHY, message=message, details=details)

    @classmethod
    def from_exception(cls, e: Exception) -> "HealthCheckResult":
        """Create unhealthy result from exception."""
        return cls(
            status=HealthStatus.UNHEALTHY,
            message=str(e),
            details={"exception_type": type(e).__name__},
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON response."""
        result = {
            "status": self.status.value,
            "duration_ms": round(self.duration_ms, 2),
        }
        if self.message:
            result["message"] = self.message
        if self.details:
            result["details"] = self.details
        return result


@dataclass
class AggregatedHealthResult:
    """Aggregated result from multiple health checks."""
    status: HealthStatus
    checks: Dict[str, HealthCheckResult]
    total_duration_ms: float
    checked_at: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON response."""
        return {
            "status": self.status.value,
            "checks": {
                name: result.to_dict()
                for name, result in self.checks.items()
            },
            "total_duration_ms": round(self.total_duration_ms, 2),
            "checked_at": self.checked_at.isoformat() + "Z",
        }


class HealthCheckPlugin(ABC):
    """
    Base class for health check plugins.

    Subclass and implement check() to create custom health checks.
    Use @register_check decorator or manual registration.

    Attributes:
        name: Unique identifier for the check
        category: Check category (determines priority)
        priority: Execution priority (lower runs first)
        timeout_seconds: Max execution time before timeout
        required_for_ready: If True, failure blocks /readyz

    Example:
        @register_check(category="database", priority=30)
        class PostgresCheck(HealthCheckPlugin):
            name = "postgres"
            timeout_seconds = 5.0

            async def check(self) -> HealthCheckResult:
                try:
                    await db.execute("SELECT 1")
                    return HealthCheckResult.healthy()
                except Exception as e:
                    return HealthCheckResult.unhealthy(str(e))
    """

    name: str = "unnamed"
    category: HealthCheckCategory = HealthCheckCategory.APPLICATION
    priority: int = 50
    timeout_seconds: float = 10.0
    required_for_ready: bool = True

    @abstractmethod
    async def check(self) -> HealthCheckResult:
        """
        Execute health check.

        Returns:
            HealthCheckResult with status and optional details
        """
        pass

    def __init_subclass__(cls, **kwargs):
        """Set default priority from category if not specified."""
        super().__init_subclass__(**kwargs)
        if cls.priority == 50 and hasattr(cls, "category"):
            cls.priority = cls.category.default_priority
