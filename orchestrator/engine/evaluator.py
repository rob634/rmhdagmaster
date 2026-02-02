# ============================================================================
# DAG EVALUATOR
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Core - DAG dependency resolution and evaluation
# PURPOSE: Determine ready nodes, evaluate conditionals, handle fan-out
# CREATED: 31 JAN 2026
# ============================================================================
"""
DAG Evaluator

Core logic for DAG traversal and dependency resolution.

Features:
- Dependency graph construction
- Topological sort validation (cycle detection)
- Ready node detection
- Conditional branch evaluation
- Fan-out expansion

The evaluator is stateless - it takes workflow definitions and node states
as input and returns decisions about what should happen next.
"""

import logging
import operator
import re
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

from core.models import WorkflowDefinition, NodeDefinition, NodeState, NodeType
from core.contracts import NodeStatus

logger = logging.getLogger(__name__)


# ============================================================================
# DATA STRUCTURES
# ============================================================================

@dataclass
class DependencyGraph:
    """
    Dependency graph for a workflow.

    Nodes are connected by edges representing dependencies.
    A -> B means "B depends on A" (A must complete before B).
    """
    # Node ID -> List of nodes that depend on it
    forward_edges: Dict[str, List[str]] = field(default_factory=lambda: defaultdict(list))

    # Node ID -> List of nodes it depends on
    backward_edges: Dict[str, List[str]] = field(default_factory=lambda: defaultdict(list))

    # All node IDs
    nodes: Set[str] = field(default_factory=set)

    def add_edge(self, from_node: str, to_node: str) -> None:
        """Add a dependency edge: to_node depends on from_node."""
        self.forward_edges[from_node].append(to_node)
        self.backward_edges[to_node].append(from_node)
        self.nodes.add(from_node)
        self.nodes.add(to_node)

    def get_dependencies(self, node_id: str) -> List[str]:
        """Get nodes that this node depends on."""
        return self.backward_edges.get(node_id, [])

    def get_dependents(self, node_id: str) -> List[str]:
        """Get nodes that depend on this node."""
        return self.forward_edges.get(node_id, [])


@dataclass
class EvaluationResult:
    """Result of DAG evaluation."""
    # Nodes that are ready to execute
    ready_nodes: List[str] = field(default_factory=list)

    # Nodes that should be skipped (conditional branch not taken)
    skip_nodes: List[str] = field(default_factory=list)

    # Fan-out expansions: (parent_node, task_template, items)
    fan_out_expansions: List[Tuple[str, NodeDefinition, List[Any]]] = field(
        default_factory=list
    )

    # Errors encountered during evaluation
    errors: List[str] = field(default_factory=list)


@dataclass
class ConditionResult:
    """Result of condition evaluation."""
    matched: bool
    next_node: Optional[str] = None
    skip_nodes: List[str] = field(default_factory=list)


# ============================================================================
# GRAPH BUILDER
# ============================================================================

class GraphBuilder:
    """Builds dependency graph from workflow definition."""

    def build(self, workflow: WorkflowDefinition) -> DependencyGraph:
        """
        Build dependency graph from workflow.

        Args:
            workflow: Workflow definition

        Returns:
            DependencyGraph instance
        """
        graph = DependencyGraph()

        for node_id, node_def in workflow.nodes.items():
            graph.nodes.add(node_id)

            # Add edges from 'next' pointers (forward edges)
            if node_def.next:
                next_nodes = (
                    [node_def.next] if isinstance(node_def.next, str)
                    else node_def.next
                )
                for next_node in next_nodes:
                    graph.add_edge(node_id, next_node)

            # Add edges from conditional branches
            if node_def.branches:
                for branch in node_def.branches:
                    if branch.next:
                        graph.add_edge(node_id, branch.next)

            # Add edges from explicit depends_on
            if node_def.depends_on:
                if node_def.depends_on.all_of:
                    for dep in node_def.depends_on.all_of:
                        graph.add_edge(dep, node_id)
                if node_def.depends_on.any_of:
                    for dep in node_def.depends_on.any_of:
                        graph.add_edge(dep, node_id)

        return graph


# ============================================================================
# TOPOLOGICAL SORT / CYCLE DETECTION
# ============================================================================

class TopologicalSorter:
    """Validates DAG structure and provides topological ordering."""

    def validate(self, graph: DependencyGraph) -> Tuple[bool, List[str], Optional[str]]:
        """
        Validate that graph is a DAG (no cycles).

        Args:
            graph: Dependency graph

        Returns:
            Tuple of (is_valid, sorted_nodes, error_message)
        """
        in_degree = {node: 0 for node in graph.nodes}

        # Calculate in-degrees
        for node in graph.nodes:
            for dep in graph.get_dependencies(node):
                if dep in in_degree:
                    in_degree[node] += 1

        # Start with nodes that have no dependencies
        queue = deque([node for node, degree in in_degree.items() if degree == 0])
        sorted_nodes = []

        while queue:
            node = queue.popleft()
            sorted_nodes.append(node)

            # Reduce in-degree for dependents
            for dependent in graph.get_dependents(node):
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)

        if len(sorted_nodes) != len(graph.nodes):
            # Cycle detected
            remaining = [n for n in graph.nodes if n not in sorted_nodes]
            return False, sorted_nodes, f"Cycle detected involving nodes: {remaining}"

        return True, sorted_nodes, None


# ============================================================================
# CONDITION EVALUATOR
# ============================================================================

class ConditionEvaluator:
    """
    Evaluates conditional expressions.

    Supports:
    - Comparison operators: ==, !=, <, >, <=, >=
    - Logical operators: and, or, not
    - Field access: output.field_name
    - Type checks: is_null, is_not_null
    """

    # Operator mapping
    OPERATORS = {
        "==": operator.eq,
        "!=": operator.ne,
        "<": operator.lt,
        ">": operator.gt,
        "<=": operator.le,
        ">=": operator.ge,
        "in": lambda a, b: a in b,
        "not_in": lambda a, b: a not in b,
        "contains": lambda a, b: b in a if isinstance(a, (str, list, dict)) else False,
        "starts_with": lambda a, b: a.startswith(b) if isinstance(a, str) else False,
        "ends_with": lambda a, b: a.endswith(b) if isinstance(a, str) else False,
    }

    def evaluate(
        self,
        condition: str,
        context: Dict[str, Any],
    ) -> bool:
        """
        Evaluate a condition expression.

        Args:
            condition: Condition string (e.g., "output.size > 1000")
            context: Evaluation context (output, inputs, etc.)

        Returns:
            True if condition is met
        """
        if not condition:
            return True  # Empty condition is always true

        try:
            # Handle simple boolean checks
            if condition.lower() == "true":
                return True
            if condition.lower() == "false":
                return False

            # Handle is_null / is_not_null
            if condition.startswith("is_null("):
                field = condition[8:-1].strip()
                value = self._get_value(field, context)
                return value is None

            if condition.startswith("is_not_null("):
                field = condition[12:-1].strip()
                value = self._get_value(field, context)
                return value is not None

            # Handle comparison expressions
            return self._evaluate_comparison(condition, context)

        except Exception as e:
            logger.warning(f"Failed to evaluate condition '{condition}': {e}")
            return False

    def _evaluate_comparison(
        self,
        condition: str,
        context: Dict[str, Any],
    ) -> bool:
        """Evaluate a comparison expression."""
        # Try to parse as: left_side operator right_side
        for op_str, op_func in self.OPERATORS.items():
            # Use word boundaries for multi-char operators
            pattern = rf'\s+{re.escape(op_str)}\s+' if len(op_str) > 2 else re.escape(op_str)
            parts = re.split(pattern, condition, maxsplit=1)

            if len(parts) == 2:
                left_expr = parts[0].strip()
                right_expr = parts[1].strip()

                left_value = self._get_value(left_expr, context)
                right_value = self._parse_literal(right_expr, context)

                return op_func(left_value, right_value)

        # No operator found - treat as boolean field access
        value = self._get_value(condition, context)
        return bool(value)

    def _get_value(self, expr: str, context: Dict[str, Any]) -> Any:
        """Get value from context using dot notation."""
        parts = expr.split(".")
        value = context

        for part in parts:
            if isinstance(value, dict):
                value = value.get(part)
            elif hasattr(value, part):
                value = getattr(value, part)
            else:
                return None

            if value is None:
                return None

        return value

    def _parse_literal(self, expr: str, context: Dict[str, Any]) -> Any:
        """Parse a literal value or field reference."""
        expr = expr.strip()

        # String literal
        if (expr.startswith('"') and expr.endswith('"')) or \
           (expr.startswith("'") and expr.endswith("'")):
            return expr[1:-1]

        # Numeric literal
        try:
            if "." in expr:
                return float(expr)
            return int(expr)
        except ValueError:
            pass

        # Boolean literal
        if expr.lower() == "true":
            return True
        if expr.lower() == "false":
            return False
        if expr.lower() in ("null", "none"):
            return None

        # Field reference
        return self._get_value(expr, context)


# ============================================================================
# FAN-OUT HANDLER
# ============================================================================

class FanOutHandler:
    """Handles fan-out node expansion."""

    def expand(
        self,
        node_def: NodeDefinition,
        context: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """
        Expand a fan-out node into multiple task instances.

        Args:
            node_def: Fan-out node definition
            context: Context with inputs and node outputs

        Returns:
            List of task parameter dicts, one per expansion
        """
        if node_def.type != NodeType.FAN_OUT:
            return []

        # Get the source array
        source_expr = node_def.source
        if not source_expr:
            logger.warning(f"Fan-out node missing 'source' field")
            return []

        # Resolve source expression
        items = self._resolve_source(source_expr, context)
        if not isinstance(items, list):
            logger.warning(f"Fan-out source '{source_expr}' is not a list")
            return []

        # Get task template
        task_def = node_def.task
        if not task_def:
            logger.warning(f"Fan-out node missing 'task' template")
            return []

        # Expand each item
        expansions = []
        for index, item in enumerate(items):
            params = self._expand_params(task_def.params, item, index, context)
            expansions.append({
                "index": index,
                "item": item,
                "params": params,
                "handler": task_def.handler,
                "queue": task_def.queue,
                "timeout_seconds": task_def.timeout_seconds,
            })

        logger.info(f"Fan-out expanded to {len(expansions)} tasks")
        return expansions

    def _resolve_source(
        self,
        source_expr: str,
        context: Dict[str, Any],
    ) -> Any:
        """Resolve source expression to get the array."""
        # Simple dot notation resolution
        parts = source_expr.replace("{{ ", "").replace(" }}", "").split(".")
        value = context

        for part in parts:
            if isinstance(value, dict):
                value = value.get(part)
            else:
                return None

        return value

    def _expand_params(
        self,
        params: Dict[str, Any],
        item: Any,
        index: int,
        context: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Expand parameters with item and index substitution."""
        result = {}

        for key, value in params.items():
            if isinstance(value, str):
                # Replace {{ item }} and {{ index }}
                value = value.replace("{{ item }}", str(item))
                value = value.replace("{{ index }}", str(index))
                # Also handle dict items
                if isinstance(item, dict):
                    for item_key, item_value in item.items():
                        value = value.replace(
                            f"{{{{ item.{item_key} }}}}",
                            str(item_value)
                        )
            elif isinstance(value, dict):
                value = self._expand_params(value, item, index, context)

            result[key] = value

        return result


# ============================================================================
# MAIN EVALUATOR
# ============================================================================

class DAGEvaluator:
    """
    Main DAG evaluator.

    Combines all evaluation logic to determine:
    - Which nodes are ready to execute
    - Which nodes should be skipped
    - How to expand fan-out nodes
    """

    def __init__(self):
        self.graph_builder = GraphBuilder()
        self.topo_sorter = TopologicalSorter()
        self.condition_evaluator = ConditionEvaluator()
        self.fan_out_handler = FanOutHandler()

    def validate_workflow(
        self,
        workflow: WorkflowDefinition,
    ) -> Tuple[bool, Optional[str]]:
        """
        Validate a workflow definition.

        Checks for:
        - Exactly one START node
        - At least one END node
        - No cycles
        - All referenced nodes exist

        Args:
            workflow: Workflow to validate

        Returns:
            Tuple of (is_valid, error_message)
        """
        # Use workflow's built-in validation
        errors = workflow.validate_structure()
        if errors:
            return False, "; ".join(errors)

        # Build and validate graph
        graph = self.graph_builder.build(workflow)
        is_valid, _, error = self.topo_sorter.validate(graph)

        if not is_valid:
            return False, error

        return True, None

    def find_ready_nodes(
        self,
        workflow: WorkflowDefinition,
        node_states: Dict[str, NodeState],
    ) -> EvaluationResult:
        """
        Find nodes that are ready to execute.

        A node is ready when:
        1. It's not already completed/dispatched/running
        2. All its dependencies are completed

        Args:
            workflow: Workflow definition
            node_states: Current state of all nodes

        Returns:
            EvaluationResult with ready nodes
        """
        result = EvaluationResult()

        # Build dependency graph
        graph = self.graph_builder.build(workflow)

        # Get completed nodes
        completed_nodes = {
            node_id for node_id, state in node_states.items()
            if state.status in (NodeStatus.COMPLETED, NodeStatus.SKIPPED)
        }

        # Get non-ready nodes (pending only)
        pending_nodes = {
            node_id for node_id, state in node_states.items()
            if state.status == NodeStatus.PENDING
        }

        # Check each pending node
        for node_id in pending_nodes:
            node_def = workflow.nodes.get(node_id)
            if not node_def:
                continue

            # Check if all dependencies are met
            if self._dependencies_met(node_id, node_def, graph, completed_nodes, node_states):
                result.ready_nodes.append(node_id)

        return result

    def evaluate_conditional(
        self,
        node_def: NodeDefinition,
        output: Optional[Dict[str, Any]],
        workflow: WorkflowDefinition,
    ) -> ConditionResult:
        """
        Evaluate a conditional node and determine next path.

        Args:
            node_def: Conditional node definition
            output: Output from the node (or upstream node)
            workflow: Workflow definition

        Returns:
            ConditionResult with matched branch and nodes to skip
        """
        result = ConditionResult(matched=False)

        if node_def.type != NodeType.CONDITIONAL:
            return result

        if not node_def.branches:
            return result

        # Build context for evaluation
        context = {"output": output or {}}

        # Evaluate each branch in order
        all_next_nodes = []
        matched_next = None

        for branch in node_def.branches:
            all_next_nodes.append(branch.next)

            if matched_next is not None:
                continue  # Already matched, but collect all for skip list

            if branch.default:
                # Default branch - use if no other matches
                if matched_next is None:
                    matched_next = branch.next
                    result.matched = True
                continue

            # Evaluate condition
            if self.condition_evaluator.evaluate(branch.condition, context):
                matched_next = branch.next
                result.matched = True

        result.next_node = matched_next

        # All branches except the matched one should be skipped
        if matched_next:
            result.skip_nodes = [n for n in all_next_nodes if n != matched_next and n]

        return result

    def expand_fan_out(
        self,
        node_def: NodeDefinition,
        job_params: Dict[str, Any],
        node_outputs: Dict[str, Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """
        Expand a fan-out node.

        Args:
            node_def: Fan-out node definition
            job_params: Job input parameters
            node_outputs: Outputs from completed nodes

        Returns:
            List of expansion dicts
        """
        context = {
            "inputs": job_params,
            "nodes": node_outputs,
        }
        return self.fan_out_handler.expand(node_def, context)

    def _dependencies_met(
        self,
        node_id: str,
        node_def: NodeDefinition,
        graph: DependencyGraph,
        completed_nodes: Set[str],
        node_states: Dict[str, NodeState],
    ) -> bool:
        """Check if all dependencies for a node are met."""
        # Check explicit depends_on
        if node_def.depends_on:
            if node_def.depends_on.all_of:
                # All must be complete
                for dep in node_def.depends_on.all_of:
                    if dep not in completed_nodes:
                        return False

            if node_def.depends_on.any_of:
                # At least one must be complete
                if not any(dep in completed_nodes for dep in node_def.depends_on.any_of):
                    return False

            return True

        # Check implicit dependencies from graph
        dependencies = graph.get_dependencies(node_id)
        if dependencies:
            for dep in dependencies:
                if dep not in completed_nodes:
                    return False

        # START nodes are always ready if no explicit dependencies
        if node_def.type == NodeType.START:
            return True

        # If no dependencies defined, node is ready
        return len(dependencies) == 0 or all(d in completed_nodes for d in dependencies)


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

_evaluator: Optional[DAGEvaluator] = None


def get_evaluator() -> DAGEvaluator:
    """Get shared evaluator instance."""
    global _evaluator
    if _evaluator is None:
        _evaluator = DAGEvaluator()
    return _evaluator


def find_ready_nodes(
    workflow: WorkflowDefinition,
    node_states: Dict[str, NodeState],
) -> List[str]:
    """
    Convenience function to find ready nodes.

    Args:
        workflow: Workflow definition
        node_states: Current node states

    Returns:
        List of ready node IDs
    """
    result = get_evaluator().find_ready_nodes(workflow, node_states)
    return result.ready_nodes


def validate_workflow(workflow: WorkflowDefinition) -> Tuple[bool, Optional[str]]:
    """
    Convenience function to validate a workflow.

    Args:
        workflow: Workflow to validate

    Returns:
        Tuple of (is_valid, error_message)
    """
    return get_evaluator().validate_workflow(workflow)


# ============================================================================
# EXPORTS
# ============================================================================

__all__ = [
    "DependencyGraph",
    "EvaluationResult",
    "ConditionResult",
    "GraphBuilder",
    "TopologicalSorter",
    "ConditionEvaluator",
    "FanOutHandler",
    "DAGEvaluator",
    "get_evaluator",
    "find_ready_nodes",
    "validate_workflow",
]
