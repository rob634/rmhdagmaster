# ============================================================================
# CLAUDE CONTEXT - WORKFLOW DEFINITION MODEL
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Core model - Workflow template/blueprint
# PURPOSE: Define workflow structure loaded from YAML
# LAST_REVIEWED: 28 JAN 2026
# EXPORTS: WorkflowDefinition, NodeDefinition, NodeType
# DEPENDENCIES: pydantic, enum
# ============================================================================
"""
Workflow Definition Models

A WorkflowDefinition is the template/blueprint for a job.
It defines:
- What nodes exist
- What each node does (handler)
- Dependencies between nodes
- Conditional routing logic

Workflows are loaded from YAML files and cached.
Each job execution creates an instance based on a workflow.
"""

from enum import Enum
from typing import Any, Dict, List, Optional, Union
from pydantic import BaseModel, Field, field_validator


class NodeType(str, Enum):
    """Types of nodes in a workflow."""
    START = "start"              # Entry point (no handler, just marks beginning)
    END = "end"                  # Exit point (no handler, marks completion)
    TASK = "task"                # Executes a handler
    CONDITIONAL = "conditional"  # Routes based on condition
    FAN_OUT = "fan_out"          # Creates N parallel tasks from array
    FAN_IN = "fan_in"            # Waits for all/any upstream nodes


class RetryPolicy(BaseModel):
    """Retry configuration for a node."""
    max_attempts: int = Field(default=3, ge=1, le=10)
    backoff: str = Field(default="exponential", pattern="^(fixed|exponential|linear)$")
    initial_delay_seconds: int = Field(default=5, ge=1)
    max_delay_seconds: int = Field(default=300, ge=1)


class ConditionalBranch(BaseModel):
    """A branch in a conditional node."""
    name: Optional[str] = None
    condition: Optional[str] = None  # e.g., "< 100", ">= 1000"
    default: bool = False            # If true, this is the fallback branch
    next: str                        # Node to route to


class DependsOn(BaseModel):
    """
    Dependency specification for a node.

    Supports AND/OR semantics:
    - all_of: ALL listed nodes must complete (AND)
    - any_of: ANY listed node completing triggers this (OR)
    """
    all_of: Optional[List[str]] = None
    any_of: Optional[List[str]] = None

    @field_validator("all_of", "any_of", mode="before")
    @classmethod
    def handle_string_input(cls, v):
        """Allow single string as shorthand for single-item list."""
        if isinstance(v, str):
            return [v]
        return v


class NodeDefinition(BaseModel):
    """
    Definition of a single node in a workflow.

    This is the TEMPLATE - what the node does.
    NodeState (in node.py) is the INSTANCE - runtime state.
    """
    type: NodeType = Field(default=NodeType.TASK)

    # For TASK nodes
    handler: Optional[str] = Field(
        default=None,
        max_length=64,
        description="Handler function name (e.g., 'raster_cog_docker')"
    )
    queue: Optional[str] = Field(
        default="functionapp-tasks",
        description="Service Bus queue to dispatch to"
    )
    timeout_seconds: int = Field(default=3600, ge=1, le=86400)
    retry: Optional[RetryPolicy] = None
    params: Dict[str, Any] = Field(
        default_factory=dict,
        description="Parameters with {{ template }} expressions"
    )

    # Navigation
    next: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Next node(s) - string or list for fan-out"
    )
    depends_on: Optional[DependsOn] = Field(
        default=None,
        description="Explicit dependencies (for fan-in)"
    )

    # For CONDITIONAL nodes
    condition_field: Optional[str] = Field(
        default=None,
        description="Template expression for value to evaluate"
    )
    branches: Optional[List[ConditionalBranch]] = None

    # For FAN_OUT nodes
    source: Optional[str] = Field(
        default=None,
        description="Template expression returning array to fan out over"
    )
    task: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Task definition for each fan-out item"
    )

    # Documentation
    description: Optional[str] = None


class InputDefinition(BaseModel):
    """Definition of an input parameter for a workflow."""
    type: str = Field(default="string", pattern="^(string|integer|number|boolean|array|object)$")
    required: bool = True
    default: Optional[Any] = None
    description: Optional[str] = None


class WorkflowDefinition(BaseModel):
    """
    Complete workflow definition loaded from YAML.

    This is the TEMPLATE that jobs are created from.
    Immutable once loaded - changes require new version.
    """
    workflow_id: str = Field(..., max_length=64)
    name: str = Field(..., max_length=128)
    version: int = Field(default=1, ge=1)
    description: Optional[str] = None

    # Input schema
    inputs: Dict[str, InputDefinition] = Field(default_factory=dict)

    # Node definitions
    nodes: Dict[str, NodeDefinition] = Field(
        ...,
        description="Map of node_id -> NodeDefinition"
    )

    # Metadata
    is_active: bool = True
    created_at: Optional[str] = None  # ISO format
    updated_at: Optional[str] = None

    def get_start_node(self) -> str:
        """Find the START node."""
        for node_id, node in self.nodes.items():
            if node.type == NodeType.START:
                return node_id
        raise ValueError(f"Workflow {self.workflow_id} has no START node")

    def get_end_nodes(self) -> List[str]:
        """Find all END nodes."""
        return [
            node_id for node_id, node in self.nodes.items()
            if node.type == NodeType.END
        ]

    def get_node(self, node_id: str) -> NodeDefinition:
        """Get a node definition by ID."""
        if node_id not in self.nodes:
            raise KeyError(f"Node '{node_id}' not found in workflow '{self.workflow_id}'")
        return self.nodes[node_id]

    def validate_structure(self) -> List[str]:
        """
        Validate workflow structure.

        Returns list of validation errors (empty if valid).
        """
        errors = []

        # Must have exactly one START
        start_nodes = [n for n, d in self.nodes.items() if d.type == NodeType.START]
        if len(start_nodes) == 0:
            errors.append("Workflow must have a START node")
        elif len(start_nodes) > 1:
            errors.append(f"Workflow has multiple START nodes: {start_nodes}")

        # Must have at least one END
        end_nodes = [n for n, d in self.nodes.items() if d.type == NodeType.END]
        if len(end_nodes) == 0:
            errors.append("Workflow must have at least one END node")

        # All 'next' references must point to valid nodes
        for node_id, node in self.nodes.items():
            if node.next:
                next_nodes = [node.next] if isinstance(node.next, str) else node.next
                for next_id in next_nodes:
                    if next_id not in self.nodes:
                        errors.append(f"Node '{node_id}' references unknown node '{next_id}'")

        # TASK nodes must have handlers
        for node_id, node in self.nodes.items():
            if node.type == NodeType.TASK and not node.handler:
                errors.append(f"TASK node '{node_id}' must have a handler")

        # CONDITIONAL nodes must have branches
        for node_id, node in self.nodes.items():
            if node.type == NodeType.CONDITIONAL:
                if not node.branches:
                    errors.append(f"CONDITIONAL node '{node_id}' must have branches")
                elif not any(b.default for b in node.branches):
                    errors.append(f"CONDITIONAL node '{node_id}' should have a default branch")

        return errors


# ============================================================================
# SQL METADATA (for schema generation)
# ============================================================================

WorkflowDefinition.__sql_table__ = "dag_workflows"
WorkflowDefinition.__sql_schema__ = "dagapp"
WorkflowDefinition.__sql_primary_key__ = "workflow_id"
