# ============================================================================
# STARTUP VALIDATION
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Gateway - Startup validation
# PURPOSE: Validate environment before registering blueprints
# CREATED: 04 FEB 2026
# ============================================================================
"""
Startup Validation

Validates environment and dependencies before registering blueprints.
Pattern from rmhgeoapi: fail fast, log clearly, degrade gracefully.

If validation fails, only /livez and /readyz endpoints are available.
"""

import logging
from dataclasses import dataclass, field
from typing import List, Optional

from function.config import get_config

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """Result of a startup validation check."""

    name: str
    passed: bool
    error_type: Optional[str] = None
    error_message: Optional[str] = None


@dataclass
class StartupState:
    """Track all startup validation checks."""

    env_vars: ValidationResult = field(
        default_factory=lambda: ValidationResult("env_vars", False, "NotRun", "Validation not yet run")
    )
    database: ValidationResult = field(
        default_factory=lambda: ValidationResult("database", False, "NotRun", "Validation not yet run")
    )
    service_bus: ValidationResult = field(
        default_factory=lambda: ValidationResult("service_bus", False, "NotRun", "Validation not yet run")
    )

    @property
    def all_passed(self) -> bool:
        """Check if all validations passed."""
        return all([
            self.env_vars.passed,
            self.database.passed,
            self.service_bus.passed,
        ])

    def failed_checks(self) -> List[ValidationResult]:
        """Get list of failed validation checks."""
        checks = [self.env_vars, self.database, self.service_bus]
        return [c for c in checks if not c.passed]

    def failed_check_names(self) -> List[str]:
        """Get names of failed checks."""
        return [c.name for c in self.failed_checks()]

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "all_passed": self.all_passed,
            "checks": {
                "env_vars": {
                    "passed": self.env_vars.passed,
                    "error": self.env_vars.error_message if not self.env_vars.passed else None,
                },
                "database": {
                    "passed": self.database.passed,
                    "error": self.database.error_message if not self.database.passed else None,
                },
                "service_bus": {
                    "passed": self.service_bus.passed,
                    "error": self.service_bus.error_message if not self.service_bus.passed else None,
                },
            },
        }


# Global singleton
STARTUP_STATE = StartupState()


def validate_startup() -> bool:
    """
    Run all startup validation checks.

    Returns True if all checks pass.
    Updates global STARTUP_STATE with results.
    """
    global STARTUP_STATE

    logger.info("Starting validation checks...")

    # 1. Environment Variables
    STARTUP_STATE.env_vars = _validate_env_vars()
    if STARTUP_STATE.env_vars.passed:
        logger.info("  [PASS] Environment variables")
    else:
        logger.error(f"  [FAIL] Environment variables: {STARTUP_STATE.env_vars.error_message}")

    # 2. Database Connectivity (only if env vars passed)
    if STARTUP_STATE.env_vars.passed:
        STARTUP_STATE.database = _validate_database()
        if STARTUP_STATE.database.passed:
            logger.info("  [PASS] Database connectivity")
        else:
            logger.error(f"  [FAIL] Database connectivity: {STARTUP_STATE.database.error_message}")
    else:
        STARTUP_STATE.database = ValidationResult(
            name="database",
            passed=False,
            error_type="Skipped",
            error_message="Skipped due to env_vars failure",
        )

    # 3. Service Bus Configuration
    STARTUP_STATE.service_bus = _validate_service_bus()
    if STARTUP_STATE.service_bus.passed:
        logger.info("  [PASS] Service Bus configuration")
    else:
        logger.error(f"  [FAIL] Service Bus configuration: {STARTUP_STATE.service_bus.error_message}")

    # Summary
    if STARTUP_STATE.all_passed:
        logger.info("All validation checks PASSED")
    else:
        failed = STARTUP_STATE.failed_check_names()
        logger.error(f"Validation FAILED: {failed}")

    return STARTUP_STATE.all_passed


def _validate_env_vars() -> ValidationResult:
    """Validate required environment variables."""
    config = get_config()

    # Check for database configuration
    if not config.has_database_config:
        return ValidationResult(
            name="env_vars",
            passed=False,
            error_type="MissingEnvVar",
            error_message="DATABASE_URL or POSTGIS_HOST required",
        )

    # Check for Service Bus configuration
    if not config.has_service_bus_config:
        return ValidationResult(
            name="env_vars",
            passed=False,
            error_type="MissingEnvVar",
            error_message="SERVICE_BUS_NAMESPACE or SERVICE_BUS_CONNECTION_STRING required",
        )

    return ValidationResult(name="env_vars", passed=True)


def _validate_database() -> ValidationResult:
    """Validate database connectivity."""
    try:
        from function.repositories.base import FunctionRepository

        repo = FunctionRepository()
        result = repo.execute_scalar("SELECT 1")

        if result == 1:
            return ValidationResult(name="database", passed=True)
        else:
            return ValidationResult(
                name="database",
                passed=False,
                error_type="UnexpectedResult",
                error_message=f"Expected 1, got {result}",
            )
    except Exception as e:
        return ValidationResult(
            name="database",
            passed=False,
            error_type=type(e).__name__,
            error_message=str(e),
        )


def _validate_service_bus() -> ValidationResult:
    """
    Validate Service Bus configuration.

    Note: We only validate config is present, not connectivity.
    Connectivity check would be too slow for cold start.
    """
    config = get_config()

    if config.has_service_bus_config:
        return ValidationResult(name="service_bus", passed=True)

    return ValidationResult(
        name="service_bus",
        passed=False,
        error_type="MissingConfig",
        error_message="No Service Bus configuration found",
    )


__all__ = ["STARTUP_STATE", "validate_startup", "ValidationResult", "StartupState"]
