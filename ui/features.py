# ============================================================================
# UI FEATURE FLAGS
# ============================================================================
# EPOCH: 4/5 - DAG PORTABLE
# STATUS: Core - Feature flag configuration for UI
# PURPOSE: Enable/disable features based on mode and configuration
# CREATED: 29 JAN 2026
# ============================================================================
"""
UI Feature Flags.

Controls feature visibility and availability based on the current mode
(Epoch 4 or DAG) and environment configuration. This allows gradual
feature rollout and A/B testing.

Usage:
    from ui.features import is_enabled, get_enabled_features

    # Check single feature
    if is_enabled("dag_graph_view"):
        # Show DAG graph visualization

    # Get all enabled features for template context
    features = get_enabled_features()

    # In template
    {% if features.dag_graph_view %}
        <a href="/interface/dag/graph">View DAG Graph</a>
    {% endif %}
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Set
from enum import Enum
import os


class FeatureMode(str, Enum):
    """Feature availability mode."""
    EPOCH4_ONLY = "epoch4"      # Only available in Epoch 4
    DAG_ONLY = "dag"            # Only available in DAG mode
    BOTH = "both"               # Available in all modes
    DISABLED = "disabled"       # Disabled in all modes
    PREVIEW = "preview"         # Available only with preview flag


@dataclass
class Feature:
    """
    Feature definition.

    Attributes:
        name: Unique feature identifier (snake_case)
        label: Human-readable label for UI
        description: Detailed description of the feature
        mode: FeatureMode determining availability
        preview_key: Environment variable to enable preview features
        dependencies: Other features that must be enabled
    """
    name: str
    label: str
    description: str
    mode: FeatureMode = FeatureMode.BOTH
    preview_key: Optional[str] = None
    dependencies: Optional[List[str]] = None


# ============================================================================
# FEATURE DEFINITIONS
# ============================================================================

FEATURES: Dict[str, Feature] = {
    # -------------------------------------------------------------------------
    # CORE FEATURES - Available in all modes
    # -------------------------------------------------------------------------
    "dashboard": Feature(
        name="dashboard",
        label="Dashboard",
        description="Main dashboard with system overview",
        mode=FeatureMode.BOTH,
    ),
    "job_list": Feature(
        name="job_list",
        label="Job List",
        description="View and filter jobs",
        mode=FeatureMode.BOTH,
    ),
    "job_detail": Feature(
        name="job_detail",
        label="Job Detail",
        description="Detailed job view with progress",
        mode=FeatureMode.BOTH,
    ),
    "task_list": Feature(
        name="task_list",
        label="Task List",
        description="View and filter tasks",
        mode=FeatureMode.BOTH,
    ),
    "task_detail": Feature(
        name="task_detail",
        label="Task Detail",
        description="Detailed task view with execution info",
        mode=FeatureMode.BOTH,
    ),
    "health_check": Feature(
        name="health_check",
        label="Health Check",
        description="System health monitoring",
        mode=FeatureMode.BOTH,
    ),
    "collections": Feature(
        name="collections",
        label="Collections",
        description="Browse STAC collections",
        mode=FeatureMode.BOTH,
    ),
    "submit": Feature(
        name="submit",
        label="Submit",
        description="Submit new processing requests",
        mode=FeatureMode.BOTH,
    ),
    "logs": Feature(
        name="logs",
        label="Logs",
        description="View system logs",
        mode=FeatureMode.BOTH,
    ),
    "queues": Feature(
        name="queues",
        label="Queues",
        description="Monitor queue status",
        mode=FeatureMode.BOTH,
    ),

    # -------------------------------------------------------------------------
    # ASSET FEATURES - V0.8 GeospatialAsset
    # -------------------------------------------------------------------------
    "assets": Feature(
        name="assets",
        label="Assets",
        description="GeospatialAsset management (V0.8)",
        mode=FeatureMode.BOTH,
    ),
    "asset_detail": Feature(
        name="asset_detail",
        label="Asset Detail",
        description="Detailed asset view with state dimensions",
        mode=FeatureMode.BOTH,
        dependencies=["assets"],
    ),
    "asset_history": Feature(
        name="asset_history",
        label="Asset History",
        description="View asset revision history",
        mode=FeatureMode.BOTH,
        dependencies=["assets"],
    ),

    # -------------------------------------------------------------------------
    # VIEWER FEATURES
    # -------------------------------------------------------------------------
    "raster_viewer": Feature(
        name="raster_viewer",
        label="Raster Viewer",
        description="View raster data on map",
        mode=FeatureMode.BOTH,
    ),
    "vector_viewer": Feature(
        name="vector_viewer",
        label="Vector Viewer",
        description="View vector data on map",
        mode=FeatureMode.BOTH,
    ),
    "stac_browser": Feature(
        name="stac_browser",
        label="STAC Browser",
        description="Browse STAC catalog",
        mode=FeatureMode.BOTH,
    ),

    # -------------------------------------------------------------------------
    # DAG-SPECIFIC FEATURES - Only in DAG mode
    # -------------------------------------------------------------------------
    "workflows": Feature(
        name="workflows",
        label="Workflows",
        description="View workflow definitions",
        mode=FeatureMode.DAG_ONLY,
    ),
    "workflow_detail": Feature(
        name="workflow_detail",
        label="Workflow Detail",
        description="Detailed workflow definition view",
        mode=FeatureMode.DAG_ONLY,
        dependencies=["workflows"],
    ),
    "nodes": Feature(
        name="nodes",
        label="Nodes",
        description="View and filter nodes",
        mode=FeatureMode.DAG_ONLY,
    ),
    "node_detail": Feature(
        name="node_detail",
        label="Node Detail",
        description="Detailed node view with dependencies",
        mode=FeatureMode.DAG_ONLY,
        dependencies=["nodes"],
    ),
    "dag_graph_view": Feature(
        name="dag_graph_view",
        label="DAG Graph",
        description="Visual DAG graph with node status",
        mode=FeatureMode.DAG_ONLY,
    ),
    "dag_replay": Feature(
        name="dag_replay",
        label="DAG Replay",
        description="Replay job execution from checkpoint",
        mode=FeatureMode.DAG_ONLY,
    ),
    "fan_out_visualization": Feature(
        name="fan_out_visualization",
        label="Fan-Out View",
        description="Visualize dynamic node fan-out",
        mode=FeatureMode.DAG_ONLY,
        dependencies=["dag_graph_view"],
    ),

    # -------------------------------------------------------------------------
    # EPOCH 4 SPECIFIC FEATURES
    # -------------------------------------------------------------------------
    "stage_progress": Feature(
        name="stage_progress",
        label="Stage Progress",
        description="Stage-based progress visualization",
        mode=FeatureMode.EPOCH4_ONLY,
    ),

    # -------------------------------------------------------------------------
    # PREVIEW FEATURES - Require explicit opt-in
    # -------------------------------------------------------------------------
    "real_time_updates": Feature(
        name="real_time_updates",
        label="Real-Time Updates",
        description="WebSocket-based real-time status updates",
        mode=FeatureMode.PREVIEW,
        preview_key="ENABLE_REALTIME",
    ),
    "advanced_filters": Feature(
        name="advanced_filters",
        label="Advanced Filters",
        description="Complex filter expressions for job/task lists",
        mode=FeatureMode.PREVIEW,
        preview_key="ENABLE_ADVANCED_FILTERS",
    ),
    "bulk_operations": Feature(
        name="bulk_operations",
        label="Bulk Operations",
        description="Cancel or retry multiple jobs at once",
        mode=FeatureMode.PREVIEW,
        preview_key="ENABLE_BULK_OPS",
    ),
}


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


def is_enabled(feature_name: str, mode: Optional[str] = None) -> bool:
    """
    Check if a feature is enabled.

    Args:
        feature_name: Feature identifier
        mode: Override mode (defaults to current mode)

    Returns:
        True if feature is enabled
    """
    if feature_name not in FEATURES:
        return False

    feature = FEATURES[feature_name]

    if mode is None:
        mode = get_current_mode()

    # Check mode compatibility
    if feature.mode == FeatureMode.DISABLED:
        return False

    if feature.mode == FeatureMode.EPOCH4_ONLY and mode != "epoch4":
        return False

    if feature.mode == FeatureMode.DAG_ONLY and mode != "dag":
        return False

    if feature.mode == FeatureMode.PREVIEW:
        # Check preview environment variable
        if feature.preview_key:
            preview_enabled = os.environ.get(feature.preview_key, "").lower()
            if preview_enabled not in ("true", "1", "yes"):
                return False
        else:
            return False

    # Check dependencies
    if feature.dependencies:
        for dep in feature.dependencies:
            if not is_enabled(dep, mode):
                return False

    return True


def get_enabled_features(mode: Optional[str] = None) -> Dict[str, bool]:
    """
    Get dictionary of all features with their enabled status.

    Useful for passing to template context.

    Args:
        mode: Override mode (defaults to current mode)

    Returns:
        Dict mapping feature names to enabled status
    """
    if mode is None:
        mode = get_current_mode()

    return {
        name: is_enabled(name, mode)
        for name in FEATURES
    }


def get_enabled_feature_names(mode: Optional[str] = None) -> Set[str]:
    """
    Get set of enabled feature names.

    Args:
        mode: Override mode (defaults to current mode)

    Returns:
        Set of enabled feature names
    """
    if mode is None:
        mode = get_current_mode()

    return {
        name for name, enabled in get_enabled_features(mode).items()
        if enabled
    }


def get_features_by_mode(mode: FeatureMode) -> List[Feature]:
    """
    Get all features for a specific mode.

    Args:
        mode: FeatureMode to filter by

    Returns:
        List of Feature objects
    """
    return [f for f in FEATURES.values() if f.mode == mode]


def get_feature_info(feature_name: str) -> Optional[Dict]:
    """
    Get feature information as dictionary.

    Args:
        feature_name: Feature identifier

    Returns:
        Feature info dict or None if not found
    """
    if feature_name not in FEATURES:
        return None

    feature = FEATURES[feature_name]
    return {
        "name": feature.name,
        "label": feature.label,
        "description": feature.description,
        "mode": feature.mode.value,
        "enabled": is_enabled(feature_name),
        "dependencies": feature.dependencies or [],
    }
