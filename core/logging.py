# ============================================================================
# STRUCTURED LOGGING
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Core - Structured logging with context
# PURPOSE: Consistent, queryable logging across all components
# CREATED: 31 JAN 2026
# ============================================================================
"""
Structured Logging

Provides structured, JSON-formatted logging for the DAG orchestrator.
Integrates with Azure Application Insights for monitoring.

Features:
- Component-based loggers
- Contextual fields (job_id, node_id, task_id)
- JSON output for log aggregation
- Named checkpoints for AI-assisted debugging

Usage:
    from core.logging import get_logger, LogContext

    logger = get_logger("orchestrator.loop")

    with LogContext(job_id="job-123"):
        logger.info("Processing job", extra={"node_count": 5})
"""

import json
import logging
import os
import sys
import threading
from contextlib import contextmanager
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Any, Dict, Optional, Union
from enum import Enum


class ComponentType(str, Enum):
    """Component types for logging categorization."""
    ORCHESTRATOR = "orchestrator"
    WORKER = "worker"
    API = "api"
    REPOSITORY = "repository"
    SERVICE = "service"
    HANDLER = "handler"
    MESSAGING = "messaging"
    INFRASTRUCTURE = "infrastructure"


@dataclass
class LogContext:
    """
    Context for structured logging.

    Thread-local storage for contextual fields.
    """
    job_id: Optional[str] = None
    node_id: Optional[str] = None
    task_id: Optional[str] = None
    workflow_id: Optional[str] = None
    correlation_id: Optional[str] = None
    worker_id: Optional[str] = None
    component: Optional[str] = None
    operation: Optional[str] = None
    extra: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dict, excluding None values."""
        result = {}
        for key, value in asdict(self).items():
            if value is not None and key != "extra":
                result[key] = value
        if self.extra:
            result.update(self.extra)
        return result


# Thread-local context storage
_context_stack = threading.local()


def _get_context_stack() -> list:
    """Get thread-local context stack."""
    if not hasattr(_context_stack, "stack"):
        _context_stack.stack = []
    return _context_stack.stack


def get_current_context() -> LogContext:
    """Get current logging context."""
    stack = _get_context_stack()
    if stack:
        return stack[-1]
    return LogContext()


@contextmanager
def log_context(**kwargs):
    """
    Context manager for adding logging context.

    Args:
        **kwargs: Context fields to add

    Example:
        with log_context(job_id="job-123", node_id="validate"):
            logger.info("Processing node")
    """
    # Merge with parent context
    parent = get_current_context()
    new_context = LogContext(
        job_id=kwargs.get("job_id", parent.job_id),
        node_id=kwargs.get("node_id", parent.node_id),
        task_id=kwargs.get("task_id", parent.task_id),
        workflow_id=kwargs.get("workflow_id", parent.workflow_id),
        correlation_id=kwargs.get("correlation_id", parent.correlation_id),
        worker_id=kwargs.get("worker_id", parent.worker_id),
        component=kwargs.get("component", parent.component),
        operation=kwargs.get("operation", parent.operation),
        extra={**parent.extra, **kwargs.get("extra", {})},
    )

    stack = _get_context_stack()
    stack.append(new_context)
    try:
        yield new_context
    finally:
        stack.pop()


class StructuredFormatter(logging.Formatter):
    """
    JSON formatter for structured logging.

    Outputs log records as JSON for easy parsing by log aggregators.
    """

    def __init__(
        self,
        include_timestamp: bool = True,
        include_level: bool = True,
        include_logger: bool = True,
        include_context: bool = True,
    ):
        super().__init__()
        self.include_timestamp = include_timestamp
        self.include_level = include_level
        self.include_logger = include_logger
        self.include_context = include_context

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON."""
        log_data: Dict[str, Any] = {}

        if self.include_timestamp:
            log_data["timestamp"] = datetime.utcnow().isoformat() + "Z"

        if self.include_level:
            log_data["level"] = record.levelname

        if self.include_logger:
            log_data["logger"] = record.name

        # Message
        log_data["message"] = record.getMessage()

        # Include context
        if self.include_context:
            context = get_current_context()
            context_dict = context.to_dict()
            if context_dict:
                log_data["context"] = context_dict

        # Include extra fields from record
        if hasattr(record, "extra") and record.extra:
            log_data["data"] = record.extra

        # Include exception info
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        # Source location
        log_data["source"] = {
            "file": record.filename,
            "line": record.lineno,
            "function": record.funcName,
        }

        return json.dumps(log_data, default=str)


class HumanFormatter(logging.Formatter):
    """
    Human-readable formatter for development.

    Includes context fields inline for easy reading.
    """

    def format(self, record: logging.LogRecord) -> str:
        """Format log record for human reading."""
        # Base format
        timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        level = record.levelname.ljust(8)

        # Get context
        context = get_current_context()
        context_parts = []
        if context.job_id:
            context_parts.append(f"job={context.job_id}")
        if context.node_id:
            context_parts.append(f"node={context.node_id}")
        if context.task_id:
            context_parts.append(f"task={context.task_id}")

        context_str = f" [{', '.join(context_parts)}]" if context_parts else ""

        # Message
        message = record.getMessage()

        # Extra data
        extra_str = ""
        if hasattr(record, "extra") and record.extra:
            extra_str = f" {record.extra}"

        # Format
        result = f"{timestamp} {level} {record.name}{context_str}: {message}{extra_str}"

        # Exception
        if record.exc_info:
            result += f"\n{self.formatException(record.exc_info)}"

        return result


class ContextLogger(logging.LoggerAdapter):
    """
    Logger adapter that includes context in log records.

    Automatically includes thread-local context in all log messages.
    """

    def process(self, msg, kwargs):
        """Process log record to include context."""
        # Get current context
        context = get_current_context()

        # Merge extra fields
        extra = kwargs.get("extra", {})
        extra.update(context.to_dict())

        # Store as attribute for formatter access
        kwargs["extra"] = {"extra": extra}

        return msg, kwargs


def get_logger(
    name: str,
    component: Optional[ComponentType] = None,
) -> ContextLogger:
    """
    Get a context-aware logger.

    Args:
        name: Logger name (e.g., "orchestrator.loop")
        component: Optional component type for categorization

    Returns:
        ContextLogger instance
    """
    base_logger = logging.getLogger(name)
    adapter = ContextLogger(base_logger, {"component": component})
    return adapter


def configure_logging(
    level: Union[str, int] = "INFO",
    json_output: bool = False,
    include_source: bool = True,
) -> None:
    """
    Configure logging for the application.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR)
        json_output: Use JSON format (for production)
        include_source: Include source file/line info
    """
    # Determine level
    if isinstance(level, str):
        level = getattr(logging, level.upper(), logging.INFO)

    # Choose formatter
    if json_output or os.getenv("LOG_FORMAT", "").lower() == "json":
        formatter = StructuredFormatter(include_context=True)
    else:
        formatter = HumanFormatter()

    # Configure root logger
    root = logging.getLogger()
    root.setLevel(level)

    # Remove existing handlers
    for handler in root.handlers[:]:
        root.removeHandler(handler)

    # Add stream handler
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    root.addHandler(handler)


# ============================================================================
# CHECKPOINT LOGGING
# ============================================================================

def log_checkpoint(
    name: str,
    data: Optional[Dict[str, Any]] = None,
    logger: Optional[logging.Logger] = None,
) -> None:
    """
    Log a named checkpoint for AI-assisted debugging.

    Checkpoints are named markers that can be queried to
    understand execution flow.

    Args:
        name: Checkpoint name (e.g., "job_started", "node_completed")
        data: Optional checkpoint data
        logger: Optional specific logger to use
    """
    if logger is None:
        logger = logging.getLogger("checkpoint")

    checkpoint_data = {
        "checkpoint": name,
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }

    context = get_current_context()
    if context.job_id:
        checkpoint_data["job_id"] = context.job_id
    if context.node_id:
        checkpoint_data["node_id"] = context.node_id
    if context.task_id:
        checkpoint_data["task_id"] = context.task_id

    if data:
        checkpoint_data["data"] = data

    logger.info(f"CHECKPOINT: {name}", extra={"extra": checkpoint_data})


# ============================================================================
# EXPORTS
# ============================================================================

__all__ = [
    "ComponentType",
    "LogContext",
    "StructuredFormatter",
    "HumanFormatter",
    "ContextLogger",
    "get_logger",
    "configure_logging",
    "log_context",
    "get_current_context",
    "log_checkpoint",
]
