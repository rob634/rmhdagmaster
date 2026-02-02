# ============================================================================
# HEALTH CHECK REGISTRY
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Infrastructure - Health check plugin registration
# PURPOSE: Register and discover health check plugins
# CREATED: 31 JAN 2026
# ============================================================================
"""
Health Check Registry

Manages registration and discovery of health check plugins.

Usage:
    # Decorator registration
    @register_check(category="database")
    class PostgresCheck(HealthCheckPlugin):
        ...

    # Manual registration
    registry = get_registry()
    registry.register(PostgresCheck())

    # Get checks for execution
    checks = registry.get_checks_by_priority()
"""

import logging
from typing import Dict, List, Optional, Type

from health.core import (
    HealthCheckPlugin,
    HealthCheckCategory,
)

logger = logging.getLogger(__name__)


class HealthCheckRegistry:
    """
    Registry for health check plugins.

    Maintains a collection of health checks organized by category
    and priority. Supports both class-based and instance registration.
    """

    def __init__(self):
        self._checks: Dict[str, HealthCheckPlugin] = {}
        self._initialized = False

    def register(self, check: HealthCheckPlugin) -> None:
        """
        Register a health check plugin instance.

        Args:
            check: Plugin instance to register

        Raises:
            ValueError: If check with same name already registered
        """
        if check.name in self._checks:
            logger.warning(f"Overwriting health check: {check.name}")

        self._checks[check.name] = check
        logger.debug(
            f"Registered health check: {check.name} "
            f"(category={check.category.value}, priority={check.priority})"
        )

    def register_class(
        self,
        check_class: Type[HealthCheckPlugin],
        **kwargs
    ) -> HealthCheckPlugin:
        """
        Instantiate and register a health check class.

        Args:
            check_class: Plugin class to instantiate
            **kwargs: Arguments passed to constructor

        Returns:
            The instantiated plugin
        """
        instance = check_class(**kwargs)
        self.register(instance)
        return instance

    def unregister(self, name: str) -> bool:
        """
        Remove a health check by name.

        Returns:
            True if check was removed
        """
        if name in self._checks:
            del self._checks[name]
            return True
        return False

    def get(self, name: str) -> Optional[HealthCheckPlugin]:
        """Get health check by name."""
        return self._checks.get(name)

    def get_all(self) -> List[HealthCheckPlugin]:
        """Get all registered checks."""
        return list(self._checks.values())

    def get_checks_by_priority(self) -> List[HealthCheckPlugin]:
        """Get all checks sorted by priority (lower first)."""
        return sorted(self._checks.values(), key=lambda c: c.priority)

    def get_checks_by_category(
        self,
        category: HealthCheckCategory
    ) -> List[HealthCheckPlugin]:
        """Get checks for a specific category."""
        return [
            c for c in self._checks.values()
            if c.category == category
        ]

    def get_required_checks(self) -> List[HealthCheckPlugin]:
        """Get checks required for /readyz."""
        return [
            c for c in self._checks.values()
            if c.required_for_ready
        ]

    def clear(self) -> None:
        """Remove all registered checks."""
        self._checks.clear()
        self._initialized = False

    @property
    def is_initialized(self) -> bool:
        """Check if registry has been initialized with checks."""
        return self._initialized

    def mark_initialized(self) -> None:
        """Mark registry as initialized."""
        self._initialized = True

    def __len__(self) -> int:
        return len(self._checks)

    def __contains__(self, name: str) -> bool:
        return name in self._checks


# ============================================================================
# GLOBAL REGISTRY & DECORATOR
# ============================================================================

_registry: Optional[HealthCheckRegistry] = None


def get_registry() -> HealthCheckRegistry:
    """Get the global health check registry."""
    global _registry
    if _registry is None:
        _registry = HealthCheckRegistry()
    return _registry


def register_check(
    category: str = None,
    priority: int = None,
    timeout_seconds: float = None,
    required_for_ready: bool = None,
):
    """
    Decorator to register a health check class.

    Args:
        category: Override category (string or HealthCheckCategory)
        priority: Override priority (lower runs first)
        timeout_seconds: Override timeout
        required_for_ready: Override if required for /readyz

    Example:
        @register_check(category="database", priority=30)
        class PostgresCheck(HealthCheckPlugin):
            name = "postgres"

            async def check(self) -> HealthCheckResult:
                ...
    """
    def decorator(cls: Type[HealthCheckPlugin]) -> Type[HealthCheckPlugin]:
        # Apply overrides to class
        if category is not None:
            if isinstance(category, str):
                cls.category = HealthCheckCategory(category)
            else:
                cls.category = category

        if priority is not None:
            cls.priority = priority
        elif hasattr(cls, "category"):
            # Use category's default priority if not set
            cls.priority = cls.category.default_priority

        if timeout_seconds is not None:
            cls.timeout_seconds = timeout_seconds

        if required_for_ready is not None:
            cls.required_for_ready = required_for_ready

        # Register instance
        registry = get_registry()
        registry.register_class(cls)

        return cls

    return decorator


# ============================================================================
# EXPORTS
# ============================================================================

__all__ = [
    "HealthCheckRegistry",
    "get_registry",
    "register_check",
]
