# ============================================================================
# ORCHESTRATOR ENGINE
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Core - Engine components
# PURPOSE: DAG evaluation, template resolution, dispatching
# CREATED: 31 JAN 2026
# ============================================================================
"""
Orchestrator Engine Components

- templates: Jinja2-based template resolution
- evaluator: DAG dependency resolution and conditional evaluation
"""

from orchestrator.engine.templates import (
    TemplateResolver,
    TemplateContext,
    NodeContext,
    TemplateResolutionError,
    get_resolver,
    resolve_params,
)
from orchestrator.engine.evaluator import (
    DAGEvaluator,
    DependencyGraph,
    EvaluationResult,
    ConditionResult,
    GraphBuilder,
    ConditionEvaluator,
    FanOutHandler,
    get_evaluator,
    find_ready_nodes,
    validate_workflow,
)

__all__ = [
    # Templates
    "TemplateResolver",
    "TemplateContext",
    "NodeContext",
    "TemplateResolutionError",
    "get_resolver",
    "resolve_params",
    # Evaluator
    "DAGEvaluator",
    "DependencyGraph",
    "EvaluationResult",
    "ConditionResult",
    "GraphBuilder",
    "ConditionEvaluator",
    "FanOutHandler",
    "get_evaluator",
    "find_ready_nodes",
    "validate_workflow",
]
