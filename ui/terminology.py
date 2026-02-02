# ============================================================================
# UI TERMINOLOGY CONFIGURATION
# ============================================================================
# EPOCH: 4/5 - DAG PORTABLE
# STATUS: Core - Term mapping for UI consistency
# PURPOSE: Map epoch-specific terms to UI-friendly labels
# CREATED: 29 JAN 2026
# ============================================================================
"""
UI Terminology Configuration.

Maps epoch-specific terminology to UI-friendly labels. This allows templates
to use generic terms that are translated based on the current mode.

Key Mappings:
    Concept          Epoch 4 Term      DAG Term
    ──────────────────────────────────────────────
    Workflow type    Job Type          Workflow
    Workflow step    Stage             Node
    Work unit        Task              Task
    Progress unit    Stage X of Y      Node X of Y

Usage:
    from ui.terminology import get_terms

    terms = get_terms()  # Auto-detects mode

    # In template
    <h2>{{ terms.step_plural }} ({{ nodes | length }})</h2>
    {% for node in nodes %}
        <div>{{ terms.step }}: {{ node.node_id }}</div>
    {% endfor %}
"""

from dataclasses import dataclass
from typing import Optional
import os


@dataclass
class Terms:
    """
    UI terminology for the current mode.

    All fields are strings that can be used directly in templates.
    """
    # Workflow terminology
    workflow: str           # "Job Type" or "Workflow"
    workflow_plural: str    # "Job Types" or "Workflows"
    workflow_id: str        # "job_type" or "workflow_id" (field name)

    # Step terminology (Stage vs Node)
    step: str               # "Stage" or "Node"
    step_plural: str        # "Stages" or "Nodes"
    step_id: str            # "stage" or "node_id" (field name)

    # Progress terminology
    progress_label: str     # "Stage {current} of {total}" or "Node {current} of {total}"

    # Mode indicator
    mode: str               # "epoch4" or "dag"
    mode_display: str       # "Epoch 4" or "DAG Orchestrator"

    # Feature labels
    orchestrator: str       # "Core Machine" or "DAG Orchestrator"
    execution: str          # "Stage Execution" or "Node Execution"

    def format_progress(self, current: int, total: int) -> str:
        """Format progress string with current values."""
        return self.progress_label.format(current=current, total=total)


# ============================================================================
# TERM DEFINITIONS BY MODE
# ============================================================================

EPOCH4_TERMS = Terms(
    # Workflow
    workflow="Job Type",
    workflow_plural="Job Types",
    workflow_id="job_type",

    # Steps
    step="Stage",
    step_plural="Stages",
    step_id="stage",

    # Progress
    progress_label="Stage {current} of {total}",

    # Mode
    mode="epoch4",
    mode_display="Epoch 4",

    # Features
    orchestrator="Core Machine",
    execution="Stage Execution",
)


DAG_TERMS = Terms(
    # Workflow
    workflow="Workflow",
    workflow_plural="Workflows",
    workflow_id="workflow_id",

    # Steps
    step="Node",
    step_plural="Nodes",
    step_id="node_id",

    # Progress
    progress_label="Node {current} of {total}",

    # Mode
    mode="dag",
    mode_display="DAG Orchestrator",

    # Features
    orchestrator="DAG Orchestrator",
    execution="Node Execution",
)


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_current_mode() -> str:
    """
    Determine current mode from environment.

    Returns:
        "epoch4" or "dag"
    """
    ui_mode = os.environ.get("UI_MODE", "").lower()
    if ui_mode == "dag":
        return "dag"

    dag_enabled = os.environ.get("DAG_ENABLED", "").lower()
    if dag_enabled in ("true", "1", "yes"):
        return "dag"

    return "epoch4"


def get_terms(mode: Optional[str] = None) -> Terms:
    """
    Get terminology for the specified mode.

    Args:
        mode: "epoch4" or "dag" (defaults to current mode)

    Returns:
        Terms dataclass with mode-appropriate terminology
    """
    if mode is None:
        mode = get_current_mode()

    if mode == "dag":
        return DAG_TERMS
    return EPOCH4_TERMS


def get_terms_as_dict(mode: Optional[str] = None) -> dict:
    """
    Get terminology as a dictionary.

    Useful for JSON APIs or template context.

    Args:
        mode: "epoch4" or "dag"

    Returns:
        Dictionary of term mappings
    """
    terms = get_terms(mode)
    return {
        "workflow": terms.workflow,
        "workflow_plural": terms.workflow_plural,
        "workflow_id": terms.workflow_id,
        "step": terms.step,
        "step_plural": terms.step_plural,
        "step_id": terms.step_id,
        "progress_label": terms.progress_label,
        "mode": terms.mode,
        "mode_display": terms.mode_display,
        "orchestrator": terms.orchestrator,
        "execution": terms.execution,
    }


# ============================================================================
# UI TERMINOLOGY CLASS
# ============================================================================

@dataclass
class Terminology:
    """
    UI terminology configuration class.

    Compatible with ui_routes.py template context.
    """
    mode_display: str
    workflow_singular: str
    workflow_plural: str
    job_singular: str
    job_plural: str
    step_singular: str
    step_plural: str
