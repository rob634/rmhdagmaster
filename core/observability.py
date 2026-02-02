# ============================================================================
# OBSERVABILITY
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Core - Application Insights and metrics
# PURPOSE: Telemetry, tracing, and metrics collection
# CREATED: 31 JAN 2026
# ============================================================================
"""
Observability

Provides integration with Azure Application Insights for:
- Distributed tracing
- Custom metrics
- Dependency tracking
- Exception logging

Also supports OpenTelemetry for vendor-neutral observability.

Usage:
    from core.observability import get_tracer, track_metric

    tracer = get_tracer()

    with tracer.start_span("process_job") as span:
        span.set_attribute("job_id", job_id)
        # do work
        track_metric("jobs_processed", 1)
"""

import os
import logging
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Union

logger = logging.getLogger(__name__)


# ============================================================================
# CONFIGURATION
# ============================================================================

@dataclass
class ObservabilityConfig:
    """Configuration for observability."""
    # Application Insights
    app_insights_connection_string: Optional[str] = None
    app_insights_instrumentation_key: Optional[str] = None

    # OpenTelemetry
    otlp_endpoint: Optional[str] = None
    otlp_headers: Dict[str, str] = field(default_factory=dict)

    # Service info
    service_name: str = "dag-orchestrator"
    service_version: str = "1.0.0"
    environment: str = "development"

    # Sampling
    sample_rate: float = 1.0  # 1.0 = 100% sampling

    # Feature flags
    enable_tracing: bool = True
    enable_metrics: bool = True
    enable_logging: bool = True

    @classmethod
    def from_env(cls) -> "ObservabilityConfig":
        """Create config from environment variables."""
        return cls(
            app_insights_connection_string=os.getenv("APPLICATIONINSIGHTS_CONNECTION_STRING"),
            app_insights_instrumentation_key=os.getenv("APPINSIGHTS_INSTRUMENTATIONKEY"),
            otlp_endpoint=os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
            service_name=os.getenv("OTEL_SERVICE_NAME", "dag-orchestrator"),
            service_version=os.getenv("SERVICE_VERSION", "1.0.0"),
            environment=os.getenv("ENVIRONMENT", "development"),
            sample_rate=float(os.getenv("OTEL_SAMPLE_RATE", "1.0")),
            enable_tracing=os.getenv("ENABLE_TRACING", "true").lower() == "true",
            enable_metrics=os.getenv("ENABLE_METRICS", "true").lower() == "true",
        )


# ============================================================================
# SPAN / TRACING
# ============================================================================

@dataclass
class Span:
    """
    Represents a tracing span.

    Lightweight wrapper that works with or without OpenTelemetry.
    """
    name: str
    trace_id: Optional[str] = None
    span_id: Optional[str] = None
    parent_span_id: Optional[str] = None
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None
    attributes: Dict[str, Any] = field(default_factory=dict)
    events: List[Dict[str, Any]] = field(default_factory=list)
    status: str = "OK"
    status_message: Optional[str] = None

    # Internal OpenTelemetry span reference
    _otel_span: Any = None

    def set_attribute(self, key: str, value: Any) -> None:
        """Set a span attribute."""
        self.attributes[key] = value
        if self._otel_span:
            self._otel_span.set_attribute(key, value)

    def add_event(self, name: str, attributes: Optional[Dict[str, Any]] = None) -> None:
        """Add an event to the span."""
        event = {
            "name": name,
            "timestamp": time.time(),
            "attributes": attributes or {},
        }
        self.events.append(event)
        if self._otel_span:
            self._otel_span.add_event(name, attributes=attributes)

    def set_status(self, status: str, message: Optional[str] = None) -> None:
        """Set span status."""
        self.status = status
        self.status_message = message
        if self._otel_span:
            # Map to OpenTelemetry status
            pass  # Simplified for now

    def end(self) -> None:
        """End the span."""
        self.end_time = time.time()
        if self._otel_span:
            self._otel_span.end()

    @property
    def duration_ms(self) -> float:
        """Duration in milliseconds."""
        end = self.end_time or time.time()
        return (end - self.start_time) * 1000

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dict."""
        return {
            "name": self.name,
            "trace_id": self.trace_id,
            "span_id": self.span_id,
            "parent_span_id": self.parent_span_id,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration_ms": self.duration_ms,
            "attributes": self.attributes,
            "events": self.events,
            "status": self.status,
            "status_message": self.status_message,
        }


class Tracer:
    """
    Tracer for creating spans.

    Works with or without OpenTelemetry.
    """

    def __init__(self, name: str = "dag-orchestrator"):
        self.name = name
        self._otel_tracer = None
        self._initialize_otel()

    def _initialize_otel(self) -> None:
        """Try to initialize OpenTelemetry tracer."""
        try:
            from opentelemetry import trace
            self._otel_tracer = trace.get_tracer(self.name)
        except ImportError:
            logger.debug("OpenTelemetry not available, using local tracing")

    @contextmanager
    def start_span(
        self,
        name: str,
        attributes: Optional[Dict[str, Any]] = None,
    ):
        """
        Start a new span.

        Args:
            name: Span name
            attributes: Initial attributes

        Yields:
            Span instance
        """
        span = Span(name=name)

        if attributes:
            for key, value in attributes.items():
                span.set_attribute(key, value)

        # If OpenTelemetry is available, use it
        if self._otel_tracer:
            try:
                with self._otel_tracer.start_as_current_span(name) as otel_span:
                    span._otel_span = otel_span
                    if attributes:
                        for key, value in attributes.items():
                            otel_span.set_attribute(key, value)
                    yield span
                    return
            except Exception as e:
                logger.debug(f"OpenTelemetry span failed: {e}")

        # Fallback to local span
        try:
            yield span
        finally:
            span.end()
            logger.debug(f"Span completed: {name} ({span.duration_ms:.2f}ms)")


# ============================================================================
# METRICS
# ============================================================================

@dataclass
class MetricPoint:
    """A single metric data point."""
    name: str
    value: float
    timestamp: float = field(default_factory=time.time)
    tags: Dict[str, str] = field(default_factory=dict)
    unit: str = ""


class MetricsCollector:
    """
    Collects and exports metrics.

    Supports counters, gauges, and histograms.
    """

    def __init__(self):
        self._metrics: List[MetricPoint] = []
        self._counters: Dict[str, float] = {}
        self._gauges: Dict[str, float] = {}
        self._otel_meter = None
        self._initialize_otel()

    def _initialize_otel(self) -> None:
        """Try to initialize OpenTelemetry meter."""
        try:
            from opentelemetry import metrics
            self._otel_meter = metrics.get_meter("dag-orchestrator")
        except ImportError:
            logger.debug("OpenTelemetry metrics not available")

    def counter(
        self,
        name: str,
        value: float = 1.0,
        tags: Optional[Dict[str, str]] = None,
    ) -> None:
        """
        Increment a counter metric.

        Args:
            name: Metric name
            value: Value to add (default 1)
            tags: Optional tags
        """
        key = f"{name}:{tags}" if tags else name
        self._counters[key] = self._counters.get(key, 0) + value

        self._metrics.append(MetricPoint(
            name=name,
            value=value,
            tags=tags or {},
        ))

        logger.debug(f"Metric counter: {name}={value}")

    def gauge(
        self,
        name: str,
        value: float,
        tags: Optional[Dict[str, str]] = None,
    ) -> None:
        """
        Set a gauge metric.

        Args:
            name: Metric name
            value: Current value
            tags: Optional tags
        """
        key = f"{name}:{tags}" if tags else name
        self._gauges[key] = value

        self._metrics.append(MetricPoint(
            name=name,
            value=value,
            tags=tags or {},
        ))

        logger.debug(f"Metric gauge: {name}={value}")

    def histogram(
        self,
        name: str,
        value: float,
        tags: Optional[Dict[str, str]] = None,
        unit: str = "ms",
    ) -> None:
        """
        Record a histogram value.

        Args:
            name: Metric name
            value: Value to record
            tags: Optional tags
            unit: Unit of measurement
        """
        self._metrics.append(MetricPoint(
            name=name,
            value=value,
            tags=tags or {},
            unit=unit,
        ))

        logger.debug(f"Metric histogram: {name}={value}{unit}")

    @contextmanager
    def timer(self, name: str, tags: Optional[Dict[str, str]] = None):
        """
        Context manager for timing operations.

        Args:
            name: Metric name
            tags: Optional tags

        Yields:
            None
        """
        start = time.time()
        try:
            yield
        finally:
            duration_ms = (time.time() - start) * 1000
            self.histogram(name, duration_ms, tags=tags, unit="ms")

    def get_metrics(self) -> List[MetricPoint]:
        """Get all collected metrics."""
        return self._metrics.copy()

    def clear(self) -> None:
        """Clear collected metrics."""
        self._metrics.clear()


# ============================================================================
# GLOBAL INSTANCES
# ============================================================================

_config: Optional[ObservabilityConfig] = None
_tracer: Optional[Tracer] = None
_metrics: Optional[MetricsCollector] = None


def initialize(config: Optional[ObservabilityConfig] = None) -> None:
    """
    Initialize observability.

    Args:
        config: Optional config (uses env vars if not provided)
    """
    global _config, _tracer, _metrics

    _config = config or ObservabilityConfig.from_env()
    _tracer = Tracer(_config.service_name)
    _metrics = MetricsCollector()

    logger.info(
        f"Observability initialized: service={_config.service_name}, "
        f"tracing={_config.enable_tracing}, metrics={_config.enable_metrics}"
    )


def get_tracer() -> Tracer:
    """Get the global tracer."""
    global _tracer
    if _tracer is None:
        initialize()
    return _tracer


def get_metrics() -> MetricsCollector:
    """Get the global metrics collector."""
    global _metrics
    if _metrics is None:
        initialize()
    return _metrics


def track_metric(
    name: str,
    value: float = 1.0,
    tags: Optional[Dict[str, str]] = None,
) -> None:
    """
    Track a metric (convenience function).

    Args:
        name: Metric name
        value: Value
        tags: Optional tags
    """
    get_metrics().counter(name, value, tags)


# ============================================================================
# DECORATORS
# ============================================================================

def traced(name: Optional[str] = None):
    """
    Decorator to trace a function.

    Args:
        name: Optional span name (defaults to function name)
    """
    def decorator(func: Callable) -> Callable:
        span_name = name or func.__name__

        if asyncio_iscoroutinefunction(func):
            async def async_wrapper(*args, **kwargs):
                with get_tracer().start_span(span_name) as span:
                    try:
                        result = await func(*args, **kwargs)
                        span.set_status("OK")
                        return result
                    except Exception as e:
                        span.set_status("ERROR", str(e))
                        raise
            return async_wrapper
        else:
            def sync_wrapper(*args, **kwargs):
                with get_tracer().start_span(span_name) as span:
                    try:
                        result = func(*args, **kwargs)
                        span.set_status("OK")
                        return result
                    except Exception as e:
                        span.set_status("ERROR", str(e))
                        raise
            return sync_wrapper

    return decorator


def asyncio_iscoroutinefunction(func) -> bool:
    """Check if function is async."""
    import asyncio
    return asyncio.iscoroutinefunction(func)


# ============================================================================
# EXPORTS
# ============================================================================

__all__ = [
    "ObservabilityConfig",
    "Span",
    "Tracer",
    "MetricPoint",
    "MetricsCollector",
    "initialize",
    "get_tracer",
    "get_metrics",
    "track_metric",
    "traced",
]
