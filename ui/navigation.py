# ============================================================================
# UI NAVIGATION CONFIGURATION
# ============================================================================
# EPOCH: 4/5 - DAG PORTABLE
# STATUS: Core - Configurable navigation for UI
# PURPOSE: Define nav items with conditional visibility by mode
# CREATED: 29 JAN 2026
# ============================================================================
"""
UI Navigation Configuration.

Defines navigation items for the UI with conditional visibility based on
the current mode (Epoch 4 or DAG). This allows gradual feature rollout
and mode-specific navigation.

Usage:
    from ui.navigation import get_nav_items

    # In template context
    nav_items = get_nav_items()

    # In Jinja2 template
    {% for item in nav_items %}
        <a href="{{ item.path }}" class="{% if nav_active == item.path %}active{% endif %}">
            <i class="icon-{{ item.icon }}"></i>
            {{ item.label }}
        </a>
    {% endfor %}
"""

from typing import List, Optional
from dataclasses import dataclass
from enum import Enum
import os


class UIMode(str, Enum):
    """UI mode determining which features are available."""
    EPOCH4 = "epoch4"
    DAG = "dag"
    BOTH = "both"  # Available in all modes


@dataclass
class NavItem:
    """Navigation item definition."""
    path: str
    label: str
    icon: str
    requires: UIMode = UIMode.BOTH
    section: str = "main"  # For grouping: main, data, admin, tools
    badge: Optional[str] = None  # Optional badge text (e.g., "New", "Beta")
    children: Optional[List["NavItem"]] = None


# ============================================================================
# NAVIGATION ITEMS
# ============================================================================

NAV_ITEMS: List[NavItem] = [
    # -------------------------------------------------------------------------
    # MAIN SECTION - Available in all modes
    # -------------------------------------------------------------------------
    NavItem(
        path="/interface/home",
        label="Dashboard",
        icon="home",
        section="main",
    ),
    NavItem(
        path="/interface/health",
        label="Health",
        icon="heart",
        section="main",
    ),

    # -------------------------------------------------------------------------
    # DATA SECTION - Browse and manage data
    # -------------------------------------------------------------------------
    NavItem(
        path="/interface/collections",
        label="Collections",
        icon="database",
        section="data",
    ),
    NavItem(
        path="/interface/submit",
        label="Submit",
        icon="upload",
        section="data",
    ),
    NavItem(
        path="/interface/raster/viewer",
        label="Raster Viewer",
        icon="image",
        section="data",
    ),
    NavItem(
        path="/interface/vector/viewer",
        label="Vector Viewer",
        icon="map",
        section="data",
    ),

    # -------------------------------------------------------------------------
    # JOBS & PROCESSING - Monitoring section
    # -------------------------------------------------------------------------
    NavItem(
        path="/interface/jobs",
        label="Jobs",
        icon="list",
        section="jobs",
    ),
    NavItem(
        path="/interface/tasks",
        label="Tasks",
        icon="check-square",
        section="jobs",
    ),
    NavItem(
        path="/interface/assets",
        label="Assets",
        icon="box",
        section="jobs",
        badge="V0.8",
    ),

    # -------------------------------------------------------------------------
    # DAG-SPECIFIC - Only shown in DAG mode
    # -------------------------------------------------------------------------
    NavItem(
        path="/interface/workflows",
        label="Workflows",
        icon="git-branch",
        section="dag",
        requires=UIMode.DAG,
        badge="DAG",
    ),
    NavItem(
        path="/interface/nodes",
        label="Nodes",
        icon="circle",
        section="dag",
        requires=UIMode.DAG,
    ),
    NavItem(
        path="/interface/dag/graph",
        label="DAG Graph",
        icon="share-2",
        section="dag",
        requires=UIMode.DAG,
        badge="New",
    ),

    # -------------------------------------------------------------------------
    # ADMIN SECTION
    # -------------------------------------------------------------------------
    NavItem(
        path="/interface/logs",
        label="Logs",
        icon="file-text",
        section="admin",
    ),
    NavItem(
        path="/interface/queues",
        label="Queues",
        icon="inbox",
        section="admin",
    ),
    NavItem(
        path="/interface/external-services",
        label="Services",
        icon="link",
        section="admin",
    ),

    # -------------------------------------------------------------------------
    # TOOLS SECTION - Available in all modes
    # -------------------------------------------------------------------------
    NavItem(
        path="/interface/stac",
        label="STAC Browser",
        icon="folder",
        section="tools",
    ),
]


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_current_mode() -> UIMode:
    """
    Determine current UI mode from environment.

    Checks:
    1. UI_MODE environment variable
    2. DAG_ENABLED environment variable
    3. Defaults to EPOCH4

    Returns:
        UIMode: Current mode
    """
    ui_mode = os.environ.get("UI_MODE", "").lower()
    if ui_mode == "dag":
        return UIMode.DAG

    dag_enabled = os.environ.get("DAG_ENABLED", "").lower()
    if dag_enabled in ("true", "1", "yes"):
        return UIMode.DAG

    return UIMode.EPOCH4


def get_nav_items(mode: Optional[UIMode] = None) -> List[NavItem]:
    """
    Get navigation items for the current mode.

    Filters items based on their `requires` attribute.
    Items with requires=BOTH are always included.

    Args:
        mode: UI mode to filter for (defaults to current mode)

    Returns:
        List of NavItem visible in the specified mode
    """
    if mode is None:
        mode = get_current_mode()

    return [
        item for item in NAV_ITEMS
        if item.requires == UIMode.BOTH or item.requires == mode
    ]


def get_nav_items_for_mode(mode: str) -> List[NavItem]:
    """
    Get navigation items for a specific mode by string.

    Convenience wrapper for template usage.

    Args:
        mode: "epoch4" or "dag"

    Returns:
        List of NavItem
    """
    ui_mode = UIMode.DAG if mode.lower() == "dag" else UIMode.EPOCH4
    return get_nav_items(ui_mode)


def get_nav_sections(mode: Optional[UIMode] = None) -> dict:
    """
    Get navigation items grouped by section.

    Args:
        mode: UI mode to filter for

    Returns:
        Dict mapping section name to list of NavItems
    """
    items = get_nav_items(mode)
    sections = {}

    for item in items:
        if item.section not in sections:
            sections[item.section] = []
        sections[item.section].append(item)

    return sections


def nav_item_to_dict(item: NavItem) -> dict:
    """
    Convert NavItem to dictionary for JSON serialization.

    Args:
        item: NavItem to convert

    Returns:
        Dictionary representation
    """
    return {
        "path": item.path,
        "label": item.label,
        "icon": item.icon,
        "section": item.section,
        "badge": item.badge,
        "children": [nav_item_to_dict(c) for c in item.children] if item.children else None,
    }


def get_nav_items_as_dicts(mode: Optional[UIMode] = None) -> List[dict]:
    """
    Get navigation items as dictionaries for JSON API.

    Args:
        mode: UI mode to filter for

    Returns:
        List of dictionaries
    """
    return [nav_item_to_dict(item) for item in get_nav_items(mode)]
