# UI Abstraction Layer

**EPOCH:** 4/5 - DAG PORTABLE
**STATUS:** Core
**CREATED:** 29 JAN 2026

## Purpose

This module provides a portable UI abstraction layer that allows the same templates
to work with both **Epoch 4** (stage-based) and **DAG** (node-based) orchestration systems.

The key insight is that while the underlying data models differ significantly between
epochs, the UI concepts are fundamentally similar:

| Concept | Epoch 4 | DAG (Epoch 5) |
|---------|---------|---------------|
| Workflow type | Job Type | Workflow |
| Workflow step | Stage | Node |
| Work unit | Task | Task |
| Progress | Stage X of Y | Node X of Y |

By abstracting these differences into DTOs, adapters, and terminology mappings,
we can develop the UI once and have it work across both systems.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                           TEMPLATES                                  │
│  (Jinja2 templates consume DTOs, terms, and feature flags)          │
└───────────────────────────────┬─────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        UI ABSTRACTION                                │
│  ┌──────────┐  ┌──────────┐  ┌─────────────┐  ┌──────────────┐      │
│  │   DTOs   │  │ Adapters │  │ Terminology │  │   Features   │      │
│  └──────────┘  └──────────┘  └─────────────┘  └──────────────┘      │
└───────────────────────────────┬─────────────────────────────────────┘
                                │
          ┌─────────────────────┼─────────────────────┐
          ▼                     ▼                     ▼
┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐
│    Epoch 4       │  │       DAG        │  │  GeospatialAsset │
│   (JobRecord,    │  │  (Job, NodeState,│  │    (Shared)      │
│    TaskRecord)   │  │   TaskResult)    │  │                  │
└──────────────────┘  └──────────────────┘  └──────────────────┘
```

---

## Module Structure

```
ui/
├── __init__.py          # Module exports
├── README.md            # This documentation
├── dto.py               # Data Transfer Objects (stable interfaces)
├── adapters/
│   ├── __init__.py      # Auto-detection and routing
│   ├── epoch4.py        # Epoch 4 model → DTO conversion
│   └── dag.py           # DAG model → DTO conversion (stub)
├── navigation.py        # Configurable navigation items
├── terminology.py       # Mode-specific terminology
└── features.py          # Feature flags
```

---

## Components

### 1. DTOs (`dto.py`)

**Data Transfer Objects** provide stable interfaces that templates can rely on,
regardless of the underlying data model.

#### Status Enums

```python
from ui.dto import JobStatusDTO, NodeStatusDTO, TaskStatusDTO

# Each status has properties
status = JobStatusDTO.RUNNING
status.is_terminal    # False
status.css_class      # "info"
status.display        # "Running"
status.icon           # "play-circle"
```

#### Entity DTOs

```python
from ui.dto import JobDTO, NodeDTO, TaskDTO, AssetDTO

# JobDTO - Unified job representation
job = JobDTO(
    job_id="job-123",
    workflow_id="ingest_raster",  # E4: job_type, DAG: workflow_id
    status=JobStatusDTO.RUNNING,
    current_step=2,              # E4: stage, DAG: current node index
    total_steps=5,               # E4: total_stages, DAG: total nodes
    ...
)

# Computed properties
job.progress_percent    # 40.0
job.duration_seconds    # Time from created_at to now/completed_at
job.is_complete         # False
```

### 2. Adapters (`adapters/`)

**Adapters** convert epoch-specific models to DTOs. The adapter layer auto-detects
which mode is active.

```python
from ui.adapters import job_to_dto, task_to_dto, asset_to_dto

# Works with Epoch 4 JobRecord
job_dto = job_to_dto(job_record)

# Works with DAG Job (when available)
job_dto = job_to_dto(dag_job)

# The same template code works for both:
# {{ job_dto.workflow_id }}  → "ingest_raster"
# {{ job_dto.current_step }} → 2
```

#### Epoch 4 Mappings

| JobRecord Field | JobDTO Field |
|-----------------|--------------|
| `job_id` | `job_id` |
| `job_type` | `workflow_id` |
| `status` | `status` (mapped to enum) |
| `stage` | `current_step` |
| `total_stages` | `total_steps` |
| `result_data` | `result_data` |
| `error_details` | `error_message` |

#### Stage to Node Conversion

Epoch 4 doesn't have explicit nodes, but we can create NodeDTOs from stage information:

```python
from ui.adapters import stage_to_node_dto

# Create node representation from stage
node_dto = stage_to_node_dto(
    job_id="job-123",
    stage=2,
    tasks=tasks_for_stage_2,
)
# node_dto.node_id = "stage_2"
# node_dto.status = (derived from task statuses)
```

### 3. Terminology (`terminology.py`)

**Terminology** provides mode-specific labels for UI text.

```python
from ui.terminology import get_terms

terms = get_terms()  # Auto-detects mode

# In Epoch 4:
terms.step        # "Stage"
terms.step_plural # "Stages"
terms.workflow    # "Job Type"
terms.mode_display # "Epoch 4"

# In DAG mode:
terms.step        # "Node"
terms.step_plural # "Nodes"
terms.workflow    # "Workflow"
terms.mode_display # "DAG Orchestrator"
```

#### Usage in Templates

```jinja2
<h2>{{ terms.step_plural }} ({{ nodes | length }})</h2>
<p>{{ terms.format_progress(current_step, total_steps) }}</p>
```

#### Available Terms

| Field | Epoch 4 | DAG |
|-------|---------|-----|
| `workflow` | "Job Type" | "Workflow" |
| `workflow_plural` | "Job Types" | "Workflows" |
| `step` | "Stage" | "Node" |
| `step_plural` | "Stages" | "Nodes" |
| `orchestrator` | "Core Machine" | "DAG Orchestrator" |
| `execution` | "Stage Execution" | "Node Execution" |
| `mode_display` | "Epoch 4" | "DAG Orchestrator" |

### 4. Navigation (`navigation.py`)

**Navigation** defines nav items with conditional visibility based on mode.

```python
from ui.navigation import get_nav_items, UIMode

# Get items for current mode
nav_items = get_nav_items()

# Get items for specific mode
dag_nav = get_nav_items(UIMode.DAG)
```

#### NavItem Structure

```python
@dataclass
class NavItem:
    path: str              # URL path
    label: str             # Display label
    icon: str              # Icon name (Feather icons)
    requires: UIMode       # EPOCH4, DAG, or BOTH
    section: str           # Grouping: main, data, jobs, dag, admin, tools
    badge: Optional[str]   # Badge text (e.g., "New", "Beta")
```

#### Usage in Templates

```jinja2
{% for item in nav_items %}
    {% if item.requires == 'both' or item.requires == current_mode %}
        <a href="{{ item.path }}" class="{% if nav_active == item.path %}active{% endif %}">
            <i data-feather="{{ item.icon }}"></i>
            {{ item.label }}
            {% if item.badge %}<span class="badge">{{ item.badge }}</span>{% endif %}
        </a>
    {% endif %}
{% endfor %}
```

### 5. Features (`features.py`)

**Feature flags** control feature visibility and availability.

```python
from ui.features import is_enabled, get_enabled_features

# Check single feature
if is_enabled("dag_graph_view"):
    # Show DAG visualization

# Get all features for template context
features = get_enabled_features()
```

#### Feature Modes

| Mode | Description |
|------|-------------|
| `BOTH` | Available in all modes |
| `EPOCH4_ONLY` | Only in Epoch 4 |
| `DAG_ONLY` | Only in DAG mode |
| `DISABLED` | Disabled everywhere |
| `PREVIEW` | Requires explicit opt-in via env var |

#### Usage in Templates

```jinja2
{% if features.dag_graph_view %}
    <a href="/interface/dag/graph">
        <i data-feather="share-2"></i>
        View DAG Graph
    </a>
{% endif %}

{% if features.real_time_updates %}
    <script src="/static/js/websocket.js"></script>
{% endif %}
```

---

## Environment Variables

| Variable | Values | Description |
|----------|--------|-------------|
| `UI_MODE` | `epoch4`, `dag` | Explicit mode override |
| `DAG_ENABLED` | `true`, `false` | Enable DAG mode |
| `ENABLE_REALTIME` | `true`, `false` | Preview: Real-time updates |
| `ENABLE_ADVANCED_FILTERS` | `true`, `false` | Preview: Advanced filters |
| `ENABLE_BULK_OPS` | `true`, `false` | Preview: Bulk operations |

---

## Template Integration

### Setting Up Context

```python
from ui import get_terms, get_nav_items, get_enabled_features
from ui.adapters import jobs_to_dto

def render_job_list(jobs):
    return render_template(
        "jobs/list.html",
        jobs=jobs_to_dto(jobs),
        terms=get_terms(),
        nav_items=get_nav_items(),
        features=get_enabled_features(),
        nav_active="/interface/jobs",
    )
```

### Base Template Setup

```jinja2
{# base.html #}
<!DOCTYPE html>
<html>
<head>
    <title>{{ terms.mode_display }} - {{ page_title }}</title>
</head>
<body>
    <nav>
        {% include "partials/nav.html" %}
    </nav>
    <main>
        {% block content %}{% endblock %}
    </main>
</body>
</html>
```

### Job List Template

```jinja2
{# jobs/list.html #}
{% extends "base.html" %}

{% block content %}
<h1>{{ terms.workflow_plural }}</h1>

<table>
    <thead>
        <tr>
            <th>Job ID</th>
            <th>{{ terms.workflow }}</th>
            <th>Status</th>
            <th>Progress</th>
        </tr>
    </thead>
    <tbody>
        {% for job in jobs %}
        <tr>
            <td>{{ job.job_id }}</td>
            <td>{{ job.workflow_id }}</td>
            <td>
                <span class="badge badge-{{ job.status.css_class }}">
                    {{ job.status.display }}
                </span>
            </td>
            <td>
                <div class="progress">
                    <div class="progress-bar" style="width: {{ job.progress_percent }}%"></div>
                </div>
                {{ terms.format_progress(job.current_step, job.total_steps) }}
            </td>
        </tr>
        {% endfor %}
    </tbody>
</table>
{% endblock %}
```

---

## Migration Path

### Phase 1: Current (Epoch 4)

1. All templates use DTOs from `ui.dto`
2. All data conversion uses `ui.adapters.epoch4`
3. Terminology uses Epoch 4 terms
4. DAG features are disabled

### Phase 2: DAG Preview

1. Set `DAG_ENABLED=true` for preview environments
2. DAG adapters become active when models are available
3. DAG-specific nav items appear
4. Terminology switches to DAG terms

### Phase 3: DAG Production

1. DAG is the default mode
2. Epoch 4 adapters remain for compatibility
3. Full DAG feature set enabled

---

## Adding New Features

### 1. Add Feature Flag

```python
# features.py
FEATURES["my_new_feature"] = Feature(
    name="my_new_feature",
    label="My New Feature",
    description="Does something cool",
    mode=FeatureMode.DAG_ONLY,  # or BOTH, PREVIEW, etc.
)
```

### 2. Add Navigation (if needed)

```python
# navigation.py
NAV_ITEMS.append(NavItem(
    path="/interface/my-feature",
    label="My Feature",
    icon="star",
    section="tools",
    requires=UIMode.DAG,
    badge="New",
))
```

### 3. Use in Template

```jinja2
{% if features.my_new_feature %}
    <a href="/interface/my-feature">My Feature</a>
{% endif %}
```

---

## Best Practices

### DO

- **Always use DTOs** in templates, never raw models
- **Use terminology** for any epoch-specific text
- **Check features** before showing conditional UI
- **Use adapters** for all model conversion
- **Keep templates generic** - no epoch-specific logic in HTML

### DON'T

- Don't check `job.job_type` directly (use `job.workflow_id`)
- Don't hardcode "Stage" or "Node" (use `terms.step`)
- Don't import Epoch 4 models in templates
- Don't create nav items with hardcoded mode checks

---

## Testing

```python
import os
from ui import get_terms, get_nav_items, get_enabled_features

# Test Epoch 4 mode
os.environ["UI_MODE"] = "epoch4"
terms = get_terms()
assert terms.step == "Stage"
assert terms.workflow == "Job Type"

# Test DAG mode
os.environ["UI_MODE"] = "dag"
terms = get_terms()
assert terms.step == "Node"
assert terms.workflow == "Workflow"

# Test feature flags
assert not is_enabled("dag_graph_view")  # In epoch4
os.environ["UI_MODE"] = "dag"
assert is_enabled("dag_graph_view")  # In dag
```

---

## Related Documentation

- `V0.8_ENTITIES.md` - GeospatialAsset model and DAG enhancements
- `rmhdagmaster/ARCHITECTURE.md` - DAG orchestration architecture
- `rmhdagmaster/TODO.md` - Implementation roadmap
