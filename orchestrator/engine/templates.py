# ============================================================================
# TEMPLATE RESOLUTION ENGINE
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Core - Template resolution with Jinja2
# PURPOSE: Resolve {{ }} expressions in workflow parameters
# CREATED: 31 JAN 2026
# ============================================================================
"""
Template Resolution Engine

Resolves template expressions in workflow node parameters.

Supported patterns:
- {{ inputs.param_name }} - Job input parameters
- {{ nodes.node_id.output.field }} - Output from completed nodes
- {{ nodes.node_id.status }} - Status of a node
- {{ upstream.output.field }} - For fan-in, output from upstream node
- {{ env.VAR_NAME }} - Environment variables

Examples:
    params:
      blob_url: "{{ inputs.source_url }}"
      validated_crs: "{{ nodes.validate.output.crs }}"
      all_chunks: "{{ upstream.output.chunk_urls }}"
"""

import os
import re
import logging
from typing import Any, Dict, List, Optional, Union
from jinja2 import Environment, BaseLoader, TemplateSyntaxError, UndefinedError, StrictUndefined

logger = logging.getLogger(__name__)


class TemplateResolver:
    """
    Jinja2-based template resolver for workflow parameters.

    Thread-safe, can be reused across multiple resolutions.
    """

    def __init__(self):
        """Initialize the template resolver with Jinja2 environment."""
        self._env = Environment(
            loader=BaseLoader(),
            autoescape=False,
            # Keep undefined as undefined for error detection
            undefined=StrictUndefined,
        )
        # Simple pattern for quick detection
        self._template_pattern = re.compile(r'\{\{.*?\}\}')

    def resolve(
        self,
        params: Dict[str, Any],
        context: "TemplateContext",
    ) -> Dict[str, Any]:
        """
        Resolve all template expressions in a params dict.

        Args:
            params: Dictionary containing template expressions
            context: Template context with inputs, nodes, etc.

        Returns:
            New dict with all templates resolved

        Raises:
            TemplateResolutionError: If template cannot be resolved
        """
        return self._resolve_value(params, context.to_dict())

    def _resolve_value(
        self,
        value: Any,
        context: Dict[str, Any],
    ) -> Any:
        """Recursively resolve template expressions in a value."""
        if isinstance(value, str):
            return self._resolve_string(value, context)
        elif isinstance(value, dict):
            return {k: self._resolve_value(v, context) for k, v in value.items()}
        elif isinstance(value, list):
            return [self._resolve_value(item, context) for item in value]
        else:
            return value

    def _resolve_string(self, value: str, context: Dict[str, Any]) -> Any:
        """Resolve template expressions in a string value."""
        # Quick check: if no template markers, return as-is
        if '{{' not in value:
            return value

        # Check if the entire string is a single template expression
        # This allows returning non-string types (lists, dicts, etc.)
        stripped = value.strip()
        if stripped.startswith('{{') and stripped.endswith('}}'):
            # Count the braces to ensure it's a single expression
            inner = stripped[2:-2].strip()
            if '{{' not in inner and '}}' not in inner:
                try:
                    template = self._env.from_string(value)
                    result = template.render(context)
                    # Try to evaluate as Python literal for complex types
                    return self._maybe_parse_result(result)
                except (TemplateSyntaxError, UndefinedError) as e:
                    raise TemplateResolutionError(f"Failed to resolve '{value}': {e}")

        # Multiple expressions or mixed content - return as string
        try:
            template = self._env.from_string(value)
            return template.render(context)
        except (TemplateSyntaxError, UndefinedError) as e:
            raise TemplateResolutionError(f"Failed to resolve '{value}': {e}")

    def _maybe_parse_result(self, result: str) -> Any:
        """Try to parse result as Python literal (for lists, dicts, etc.)."""
        result = result.strip()
        if not result:
            return result

        # If it looks like a Python literal, try to parse it
        if (result.startswith('[') and result.endswith(']')) or \
           (result.startswith('{') and result.endswith('}')):
            try:
                import ast
                return ast.literal_eval(result)
            except (ValueError, SyntaxError):
                pass

        # Try to parse as number
        try:
            if '.' in result:
                return float(result)
            return int(result)
        except ValueError:
            pass

        # Return as string
        return result

    def has_templates(self, params: Dict[str, Any]) -> bool:
        """Check if params contain any template expressions."""
        return self._check_for_templates(params)

    def _check_for_templates(self, value: Any) -> bool:
        """Recursively check for template expressions."""
        if isinstance(value, str):
            return bool(self._template_pattern.search(value))
        elif isinstance(value, dict):
            return any(self._check_for_templates(v) for v in value.values())
        elif isinstance(value, list):
            return any(self._check_for_templates(item) for item in value)
        return False


class TemplateContext:
    """
    Context for template resolution.

    Provides access to:
    - inputs: Job input parameters
    - nodes: Completed node outputs and status
    - upstream: For fan-in nodes, the upstream node's output
    - env: Environment variables
    """

    def __init__(
        self,
        inputs: Optional[Dict[str, Any]] = None,
        nodes: Optional[Dict[str, "NodeContext"]] = None,
        upstream: Optional["NodeContext"] = None,
        env_prefix: str = "DAG_",
    ):
        """
        Initialize template context.

        Args:
            inputs: Job input parameters
            nodes: Map of node_id -> NodeContext for completed nodes
            upstream: For fan-in, the specific upstream node context
            env_prefix: Prefix for environment variables (default: DAG_)
        """
        self.inputs = inputs or {}
        self.nodes = nodes or {}
        self.upstream = upstream
        self.env_prefix = env_prefix

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dict for Jinja2 rendering."""
        result = {
            "inputs": self.inputs,
            "nodes": {
                node_id: ctx.to_dict()
                for node_id, ctx in self.nodes.items()
            },
            "env": _EnvAccessor(self.env_prefix),
        }

        if self.upstream:
            result["upstream"] = self.upstream.to_dict()

        return result

    @classmethod
    def from_job(
        cls,
        job_params: Dict[str, Any],
        node_outputs: Dict[str, Dict[str, Any]],
        node_statuses: Optional[Dict[str, str]] = None,
    ) -> "TemplateContext":
        """
        Create context from job data.

        Args:
            job_params: Job input parameters
            node_outputs: Map of node_id -> output dict
            node_statuses: Optional map of node_id -> status string

        Returns:
            TemplateContext instance
        """
        nodes = {}
        for node_id, output in node_outputs.items():
            status = (node_statuses or {}).get(node_id, "completed")
            nodes[node_id] = NodeContext(output=output, status=status)

        return cls(inputs=job_params, nodes=nodes)


class NodeContext:
    """Context for a single node's output and status."""

    def __init__(
        self,
        output: Optional[Dict[str, Any]] = None,
        status: str = "pending",
    ):
        self.output = output or {}
        self.status = status

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dict for Jinja2 rendering."""
        return {
            "output": self.output,
            "status": self.status,
        }


class _EnvAccessor:
    """Accessor for environment variables with optional prefix."""

    def __init__(self, prefix: str = ""):
        self._prefix = prefix

    def __getattr__(self, name: str) -> str:
        """Get environment variable, with or without prefix."""
        # Try with prefix first
        value = os.environ.get(f"{self._prefix}{name}")
        if value is not None:
            return value

        # Try without prefix
        value = os.environ.get(name)
        if value is not None:
            return value

        raise AttributeError(f"Environment variable not found: {name}")


class TemplateResolutionError(Exception):
    """Raised when template resolution fails."""
    pass


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

_resolver: Optional[TemplateResolver] = None


def get_resolver() -> TemplateResolver:
    """Get shared template resolver instance."""
    global _resolver
    if _resolver is None:
        _resolver = TemplateResolver()
    return _resolver


def resolve_params(
    params: Dict[str, Any],
    job_params: Dict[str, Any],
    node_outputs: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """
    Convenience function to resolve params.

    Args:
        params: Parameters with template expressions
        job_params: Job input parameters
        node_outputs: Optional map of node_id -> output dict

    Returns:
        Resolved parameters
    """
    context = TemplateContext.from_job(
        job_params=job_params,
        node_outputs=node_outputs or {},
    )
    return get_resolver().resolve(params, context)


# ============================================================================
# EXPORTS
# ============================================================================

__all__ = [
    "TemplateResolver",
    "TemplateContext",
    "NodeContext",
    "TemplateResolutionError",
    "get_resolver",
    "resolve_params",
]
