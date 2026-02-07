# UI Implementation Plan - rmhdagmaster

**Created**: 02 FEB 2026
**Status**: Active Development

---

## Executive Summary

The DAG Orchestrator has a solid UI foundation with an abstraction layer (`ui/`), base templates, and working routes. This document outlines the complete implementation plan to deliver a production-ready monitoring dashboard.

---

## Current State Analysis

### What Exists

#### Templates (12 files)

| Template | Status | Description |
|----------|--------|-------------|
| `base.html` | ✅ Complete | App shell with sidebar, header, content area |
| `dashboard.html` | ✅ Complete | Stats grid, recent jobs, workflows list |
| `health.html` | ✅ Complete | Orchestrator, DB, messaging, worker, system health |
| `jobs.html` | ✅ Complete | Job list with filters, pagination |
| `job_detail.html` | ✅ Complete | Job header, node execution, task history, timeline link |
| `workflows.html` | ✅ Complete | Workflow listing with node counts |
| `workflow_detail.html` | ✅ Complete | Workflow nodes, DAG flow visualization |
| `nodes.html` | ✅ Complete | Cross-job node monitoring with filters |
| `timeline.html` | ✅ Complete | Job event timeline with filters |
| `partials/sidebar.html` | ✅ Complete | Navigation with sections |
| `partials/header.html` | ✅ Complete | Page title, mode badge, refresh |
| `partials/status_badge.html` | ✅ Complete | Reusable status badge macro |
| `partials/timeline.html` | ✅ Complete | Reusable timeline component |

#### Static Assets (2 files)

| Asset | Status | Description |
|-------|--------|-------------|
| `css/style.css` | ✅ Complete | Full dark theme (~700 lines) |
| `js/app.js` | ✅ Basic | Feather icons, auto-refresh, toast, cancelJob |

#### UI Abstraction Layer (Complete)

| Component | File | Purpose |
|-----------|------|---------|
| DTOs | `ui/dto.py` | JobDTO, NodeDTO, TaskDTO, AssetDTO, JobEventDTO |
| Adapters | `ui/adapters/` | Model → DTO conversion (Epoch 4 done, DAG stub) |
| Navigation | `ui/navigation.py` | 16 nav items with mode filtering |
| Terminology | `ui/terminology.py` | Stage↔Node, Job Type↔Workflow mapping |
| Features | `ui/features.py` | 25+ feature flags with mode support |

#### API Routes (Working)

| Route | Method | Description |
|-------|--------|-------------|
| `/ui/` | GET | Dashboard |
| `/ui/health` | GET | Health page |
| `/ui/jobs` | GET | Jobs list |
| `/ui/jobs/{id}` | GET | Job detail |
| `/ui/workflows` | GET | Workflows (uses dashboard.html) |
| `/ui/nodes` | GET | Nodes (uses dashboard.html) |

#### Backend API (Ready for UI)

| Endpoint | UI Support |
|----------|------------|
| `GET /api/v1/orchestrator/status` | ✅ Ready |
| `GET /api/v1/workflows` | ✅ Ready |
| `GET /api/v1/jobs` | ✅ Ready |
| `GET /api/v1/jobs/{id}` | ✅ Ready |
| `POST /api/v1/jobs/{id}/cancel` | ✅ Ready |
| `GET /api/v1/jobs/{id}/timeline` | ✅ Ready |
| `GET /api/v1/jobs/{id}/nodes/{id}/events` | ✅ Ready |

---

## What's Missing

### Templates Needed

| Priority | Template | Purpose | Status |
|----------|----------|---------|--------|
| P0 | `workflows.html` | List workflow definitions | ✅ Complete |
| P0 | `workflow_detail.html` | Workflow nodes, DAG visualization | ✅ Complete |
| P1 | `nodes.html` | All nodes across jobs (monitoring) | ✅ Complete |
| P1 | `timeline.html` | Job event timeline view | ✅ Complete |
| P2 | `submit.html` | Submit new job form | ⏳ TODO |
| P2 | `dag_graph.html` | Interactive DAG visualization | ⏳ TODO |
| P3 | `tasks.html` | Task list (worker results) | ⏳ TODO |
| P3 | `queues.html` | Queue depth monitoring | ⏳ TODO |

### Partials Needed

| Priority | Partial | Purpose | Status |
|----------|---------|---------|--------|
| P1 | `partials/dag_graph.html` | D3.js DAG visualization component | ⏳ TODO |
| P1 | `partials/timeline.html` | Event timeline component | ✅ Complete |
| P2 | `partials/progress_bar.html` | Animated progress bar | ⏳ TODO |
| P2 | `partials/toast.html` | Toast notification component | ⏳ TODO |
| P3 | `partials/modal.html` | Modal dialog component | ⏳ TODO |

### JavaScript Needed

| Priority | Feature | Description |
|----------|---------|-------------|
| P1 | DAG visualization | D3.js force-directed graph |
| P1 | WebSocket connection | Real-time updates (optional) |
| P2 | Form validation | Job submission validation |
| P2 | Keyboard shortcuts | Navigation, refresh |
| P3 | Dark/light theme toggle | User preference |

### CSS Needed

| Priority | Feature | Description |
|----------|---------|-------------|
| P1 | DAG graph styles | Node colors, edges, animations |
| P2 | Form styles | Input, select, validation states |
| P2 | Modal styles | Overlay, dialog, close button |
| P3 | Print styles | Print-friendly job reports |

---

## Implementation Phases

### Phase 1: Core Pages (P0)

Complete the essential pages for monitoring.

#### 1.1 Workflows Page

**File**: `templates/workflows.html`

| Task | Status | Description |
|------|--------|-------------|
| Create workflows.html template | ⏳ TODO | Table with workflow_id, name, nodes, version |
| Add workflow count in header | ⏳ TODO | "Workflows (5)" |
| Add node count per workflow | ⏳ TODO | Show total nodes |
| Add "View" action button | ⏳ TODO | Link to workflow detail |
| Update ui_routes.py | ⏳ TODO | Render workflows.html instead of dashboard |

**Template Structure**:
```jinja2
{% extends "base.html" %}
{% block content %}
<div class="card">
    <div class="card-header">
        <h3>Workflows ({{ workflows | length }})</h3>
    </div>
    <div class="card-body">
        <table>
            <thead>
                <tr>
                    <th>Workflow ID</th>
                    <th>Name</th>
                    <th>Nodes</th>
                    <th>Version</th>
                    <th>Actions</th>
                </tr>
            </thead>
            <tbody>
                {% for wf in workflows %}
                <tr>
                    <td><a href="/ui/workflows/{{ wf.id }}">{{ wf.id }}</a></td>
                    <td>{{ wf.name | default('-') }}</td>
                    <td>{{ wf.node_count }}</td>
                    <td><span class="badge badge-secondary">v{{ wf.version }}</span></td>
                    <td>
                        <a href="/ui/workflows/{{ wf.id }}" class="btn btn-icon">
                            <i data-feather="eye"></i>
                        </a>
                    </td>
                </tr>
                {% endfor %}
            </tbody>
        </table>
    </div>
</div>
{% endblock %}
```

#### 1.2 Workflow Detail Page

**File**: `templates/workflow_detail.html`

| Task | Status | Description |
|------|--------|-------------|
| Create workflow_detail.html | ⏳ TODO | Workflow header with metadata |
| Display node list | ⏳ TODO | All nodes with handler, next |
| Show node dependencies | ⏳ TODO | Visual dependency chain |
| Add "Run Workflow" button | ⏳ TODO | Link to submit with workflow pre-selected |
| Add API route | ⏳ TODO | `/ui/workflows/{workflow_id}` |

#### 1.3 Update Nodes Page

**File**: `templates/nodes.html`

| Task | Status | Description |
|------|--------|-------------|
| Create nodes.html template | ⏳ TODO | Cross-job node monitoring |
| Add status filters | ⏳ TODO | Filter by RUNNING, FAILED, etc. |
| Add job link | ⏳ TODO | Link back to parent job |
| Add duration column | ⏳ TODO | Show execution time |
| Add API route | ⏳ TODO | Need backend endpoint for cross-job nodes |

---

### Phase 2: Visualization (P1)

Add visual representations of DAG state.

#### 2.1 DAG Graph Component

**Files**:
- `templates/partials/dag_graph.html`
- `static/js/dag-graph.js`
- `static/css/dag-graph.css`

| Task | Status | Description |
|------|--------|-------------|
| Create DAG graph partial | ⏳ TODO | SVG container, legend |
| Implement D3.js graph | ⏳ TODO | Force-directed layout |
| Add node status colors | ⏳ TODO | Green=completed, blue=running, etc. |
| Add edge animations | ⏳ TODO | Flow animation on edges |
| Add node click handler | ⏳ TODO | Show node details on click |
| Add zoom/pan controls | ⏳ TODO | Mouse wheel zoom, drag pan |
| Add layout toggle | ⏳ TODO | Force vs hierarchical layout |

**JavaScript Structure** (`static/js/dag-graph.js`):
```javascript
class DAGGraph {
    constructor(container, options = {}) {
        this.container = container;
        this.width = options.width || 800;
        this.height = options.height || 600;
        this.svg = null;
        this.simulation = null;
    }

    render(workflow, nodeStates = {}) {
        // Convert workflow nodes to D3 format
        // Create force simulation
        // Draw nodes and edges
        // Apply status colors
    }

    updateNodeStatus(nodeId, status) {
        // Update node color based on status
    }

    highlightPath(nodeId) {
        // Highlight path from START to nodeId
    }
}
```

**CSS Structure** (`static/css/dag-graph.css`):
```css
.dag-container { /* SVG container */ }
.dag-node { /* Base node styles */ }
.dag-node.completed { fill: var(--color-success); }
.dag-node.running { fill: var(--color-info); animation: pulse 1s infinite; }
.dag-node.failed { fill: var(--color-danger); }
.dag-node.pending { fill: var(--color-text-muted); }
.dag-edge { stroke: var(--color-border); }
.dag-edge.active { stroke: var(--color-primary); animation: flow 1s linear infinite; }
```

#### 2.2 Timeline Component ✅ COMPLETE

**Files**:
- `templates/partials/timeline.html` ✅
- `templates/timeline.html` ✅

| Task | Status | Description |
|------|--------|-------------|
| Create timeline partial | ✅ Done | Vertical timeline with events |
| Add event icons | ✅ Done | Icon per event type |
| Add event details expansion | ✅ Done | Click to expand event_data |
| Add timestamp formatting | ✅ Done | Relative time + absolute on hover |
| Add filtering | ✅ Done | Filter by event_type, status, node_id |
| Create full timeline page | ✅ Done | Job timeline with filters |
| Add UI route | ✅ Done | `/ui/jobs/{job_id}/timeline` |
| Add timeline link to job_detail | ✅ Done | Timeline button in job header |

**Template Structure**:
```jinja2
<div class="timeline">
    {% for event in events %}
    <div class="timeline-item {{ event.event_status }}">
        <div class="timeline-marker">
            <i data-feather="{{ event_icons[event.event_type] }}"></i>
        </div>
        <div class="timeline-content">
            <div class="timeline-header">
                <span class="timeline-title">{{ event.event_type }}</span>
                <span class="timeline-time" title="{{ event.created_at }}">
                    {{ event.created_at | relative_time }}
                </span>
            </div>
            {% if event.node_id %}
            <div class="timeline-node">Node: {{ event.node_id }}</div>
            {% endif %}
            {% if event.duration_ms %}
            <div class="timeline-duration">Duration: {{ event.duration_ms }}ms</div>
            {% endif %}
            {% if event.error_message %}
            <div class="timeline-error">{{ event.error_message }}</div>
            {% endif %}
        </div>
    </div>
    {% endfor %}
</div>
```

#### 2.3 Integrate DAG Graph into Job Detail

| Task | Status | Description |
|------|--------|-------------|
| Add DAG graph to job_detail.html | ⏳ TODO | Above node list |
| Pass workflow definition to template | ⏳ TODO | For graph rendering |
| Add auto-update on job status change | ⏳ TODO | Refresh graph colors |

---

### Phase 3: Interactive Features (P2)

Add user interactions beyond viewing.

#### 3.1 Job Submission Page

**File**: `templates/submit.html`

| Task | Status | Description |
|------|--------|-------------|
| Create submit.html template | ⏳ TODO | Form with workflow selector |
| Add workflow dropdown | ⏳ TODO | Populated from /api/v1/workflows |
| Add dynamic params form | ⏳ TODO | Based on workflow inputs |
| Add JSON editor for advanced | ⏳ TODO | Raw input_params editing |
| Add form validation | ⏳ TODO | Required fields, types |
| Add submit handler | ⏳ TODO | POST to /api/v1/jobs |
| Add success redirect | ⏳ TODO | Redirect to job detail |
| Add UI route | ⏳ TODO | `/ui/submit` |

**Form Structure**:
```jinja2
<form id="submit-job-form" method="POST" action="/api/v1/jobs">
    <div class="form-group">
        <label for="workflow_id">Workflow</label>
        <select name="workflow_id" id="workflow_id" required>
            {% for wf in workflows %}
            <option value="{{ wf.id }}">{{ wf.name }} ({{ wf.id }})</option>
            {% endfor %}
        </select>
    </div>

    <div id="workflow-params">
        <!-- Dynamically populated based on workflow selection -->
    </div>

    <div class="form-group">
        <label for="correlation_id">Correlation ID (optional)</label>
        <input type="text" name="correlation_id" id="correlation_id" placeholder="External reference">
    </div>

    <button type="submit" class="btn btn-primary">
        <i data-feather="play"></i>
        Submit Job
    </button>
</form>
```

#### 3.2 Enhanced Job Actions

| Task | Status | Description |
|------|--------|-------------|
| Add retry failed nodes button | ⏳ TODO | Retry single node |
| Add restart job button | ⏳ TODO | Create new job with same params |
| Add confirmation modals | ⏳ TODO | Confirm cancel, retry |
| Add bulk cancel | ⏳ TODO | Cancel multiple selected jobs |

#### 3.3 Keyboard Shortcuts

**File**: `static/js/keyboard.js`

| Shortcut | Action |
|----------|--------|
| `r` | Refresh current page |
| `g h` | Go to home/dashboard |
| `g j` | Go to jobs |
| `g w` | Go to workflows |
| `?` | Show keyboard shortcuts help |
| `/` | Focus search (future) |
| `Esc` | Close modal/cancel |

| Task | Status | Description |
|------|--------|-------------|
| Create keyboard.js | ⏳ TODO | Keyboard shortcut handler |
| Add shortcut overlay | ⏳ TODO | Show on `?` |
| Integrate into base.html | ⏳ TODO | Load keyboard.js |

---

### Phase 4: Monitoring Enhancements (P3)

Advanced monitoring and debugging features.

#### 4.1 Tasks Page

**File**: `templates/tasks.html`

| Task | Status | Description |
|------|--------|-------------|
| Create tasks.html template | ⏳ TODO | Task results listing |
| Add worker_id filter | ⏳ TODO | Filter by worker |
| Add status filter | ⏳ TODO | Filter by task status |
| Add duration histogram | ⏳ TODO | Visual distribution |
| Add backend endpoint | ⏳ TODO | `/api/v1/tasks` (needs implementation) |

#### 4.2 Queues Page

**File**: `templates/queues.html`

| Task | Status | Description |
|------|--------|-------------|
| Create queues.html template | ⏳ TODO | Queue depth monitoring |
| Show queue names | ⏳ TODO | dag-worker-tasks, etc. |
| Show message counts | ⏳ TODO | Active, scheduled, dead-letter |
| Add refresh interval | ⏳ TODO | Auto-refresh every 5s |
| Add backend endpoint | ⏳ TODO | `/api/v1/queues/status` (needs implementation) |

#### 4.3 Real-Time Updates (Optional)

| Task | Status | Description |
|------|--------|-------------|
| Add WebSocket endpoint | ⏳ TODO | `/ws/jobs/{job_id}` |
| Create WebSocket client | ⏳ TODO | `static/js/websocket.js` |
| Add connection indicator | ⏳ TODO | Show connected/disconnected |
| Update job status in real-time | ⏳ TODO | No page refresh needed |
| Update node status in real-time | ⏳ TODO | Node colors update live |

---

### Phase 5: Polish (P4)

Final touches for production readiness.

#### 5.1 Error Handling

| Task | Status | Description |
|------|--------|-------------|
| Create error.html template | ⏳ TODO | Generic error page |
| Create 404.html template | ⏳ TODO | Not found page |
| Add error boundary in routes | ⏳ TODO | Catch exceptions, show friendly error |
| Add retry button on errors | ⏳ TODO | "Try again" button |

#### 5.2 Loading States

| Task | Status | Description |
|------|--------|-------------|
| Add skeleton loaders | ⏳ TODO | Placeholder while loading |
| Add loading spinner | ⏳ TODO | Full-page loader |
| Add inline loading | ⏳ TODO | Button loading state |

#### 5.3 Responsive Design

| Task | Status | Description |
|------|--------|-------------|
| Test on mobile | ⏳ TODO | Verify layout on phones |
| Add mobile navigation | ⏳ TODO | Hamburger menu |
| Optimize tables for mobile | ⏳ TODO | Card view on small screens |

#### 5.4 Accessibility

| Task | Status | Description |
|------|--------|-------------|
| Add ARIA labels | ⏳ TODO | Screen reader support |
| Add focus indicators | ⏳ TODO | Keyboard navigation |
| Ensure color contrast | ⏳ TODO | WCAG AA compliance |
| Add skip-to-content link | ⏳ TODO | Keyboard accessibility |

---

## File Structure (Target)

```
templates/
├── base.html                    ✅ EXISTS
├── dashboard.html               ✅ EXISTS
├── health.html                  ✅ EXISTS
├── jobs.html                    ✅ EXISTS
├── job_detail.html              ✅ EXISTS
├── workflows.html               ⏳ TODO (P0)
├── workflow_detail.html         ⏳ TODO (P0)
├── nodes.html                   ⏳ TODO (P1)
├── timeline.html                ⏳ TODO (P1)
├── submit.html                  ⏳ TODO (P2)
├── tasks.html                   ⏳ TODO (P3)
├── queues.html                  ⏳ TODO (P3)
├── error.html                   ⏳ TODO (P4)
├── 404.html                     ⏳ TODO (P4)
└── partials/
    ├── sidebar.html             ✅ EXISTS
    ├── header.html              ✅ EXISTS
    ├── status_badge.html        ✅ EXISTS
    ├── dag_graph.html           ⏳ TODO (P1)
    ├── timeline.html            ⏳ TODO (P1)
    ├── progress_bar.html        ⏳ TODO (P2)
    ├── modal.html               ⏳ TODO (P2)
    └── toast.html               ⏳ TODO (P2)

static/
├── css/
│   ├── style.css                ✅ EXISTS
│   └── dag-graph.css            ⏳ TODO (P1)
└── js/
    ├── app.js                   ✅ EXISTS
    ├── dag-graph.js             ⏳ TODO (P1)
    ├── keyboard.js              ⏳ TODO (P2)
    └── websocket.js             ⏳ TODO (P3)
```

---

## Backend API Requirements

### Existing Endpoints (Ready)

| Endpoint | UI Consumer |
|----------|-------------|
| `GET /api/v1/orchestrator/status` | Health page |
| `GET /api/v1/workflows` | Workflows page, Submit form |
| `GET /api/v1/workflows/{id}` | Workflow detail |
| `GET /api/v1/jobs` | Jobs list |
| `GET /api/v1/jobs/{id}` | Job detail |
| `POST /api/v1/jobs` | Submit form |
| `POST /api/v1/jobs/{id}/cancel` | Job actions |
| `GET /api/v1/jobs/{id}/timeline` | Timeline page |

### New Endpoints Needed

| Endpoint | Priority | Purpose |
|----------|----------|---------|
| `GET /api/v1/nodes` | P1 | Cross-job node monitoring |
| `GET /api/v1/tasks` | P3 | Task results listing |
| `GET /api/v1/queues/status` | P3 | Queue depth monitoring |
| `POST /api/v1/jobs/{id}/nodes/{id}/retry` | P2 | Retry single node |
| `WS /ws/jobs/{id}` | P3 | Real-time updates |

---

## UI Routes Updates Needed

### Existing Routes (api/ui_routes.py)

| Route | Template | Status |
|-------|----------|--------|
| `/ui/` | dashboard.html | ✅ Working |
| `/ui/health` | health.html | ✅ Working |
| `/ui/jobs` | jobs.html | ✅ Working |
| `/ui/jobs/{id}` | job_detail.html | ✅ Working |
| `/ui/jobs/{id}/timeline` | timeline.html | ✅ Working |
| `/ui/workflows` | workflows.html | ✅ Working |
| `/ui/workflows/{id}` | workflow_detail.html | ✅ Working |
| `/ui/nodes` | nodes.html | ✅ Working |

### Routes to Add (Future)

| Route | Template | Priority |
|-------|----------|----------|
| `/ui/submit` | submit.html | P2 |
| `/ui/tasks` | tasks.html | P3 |
| `/ui/queues` | queues.html | P3 |

---

## DAG Adapter Implementation

The `ui/adapters/dag.py` is currently a stub. It needs implementation.

| Task | Status | Description |
|------|--------|-------------|
| Implement `job_to_dto()` | ⏳ TODO | Convert Job → JobDTO |
| Implement `node_to_dto()` | ⏳ TODO | Convert NodeState → NodeDTO |
| Implement `task_to_dto()` | ⏳ TODO | Convert TaskResult → TaskDTO |
| Update `__init__.py` detection | ⏳ TODO | Auto-detect DAG models |
| Add unit tests | ⏳ TODO | Test adapter conversions |

**Implementation**:
```python
# ui/adapters/dag.py

from core.models import Job, NodeState, TaskResult
from core.contracts import JobStatus, NodeStatus, TaskStatus
from ui.dto import JobDTO, NodeDTO, TaskDTO, JobStatusDTO, NodeStatusDTO, TaskStatusDTO

def job_to_dto(job: Job) -> JobDTO:
    return JobDTO(
        job_id=job.job_id,
        workflow_id=job.workflow_id,
        status=map_dag_job_status(job.status),
        current_step=1,  # Calculate from nodes
        total_steps=1,   # From workflow
        completed_nodes=None,  # Calculate
        result_data=job.result_data,
        error_message=job.error_message,
        created_at=job.created_at,
        completed_at=job.completed_at,
        parameters=job.input_params or {},
    )

def node_to_dto(node: NodeState) -> NodeDTO:
    return NodeDTO(
        node_id=node.node_id,
        job_id=node.job_id,
        status=map_dag_node_status(node.status),
        handler=None,  # Needs workflow lookup
        task_id=node.task_id,
        retry_count=node.retry_count,
        output=node.output,
        error_message=node.error_message,
        created_at=node.created_at,
        started_at=node.started_at,
        completed_at=node.completed_at,
    )
```

---

## Testing Strategy

### Unit Tests

| Test | File | Description |
|------|------|-------------|
| DTO creation | `tests/test_ui_dto.py` | Test all DTO constructors |
| Adapter conversion | `tests/test_ui_adapters.py` | Test model → DTO |
| Status mapping | `tests/test_ui_adapters.py` | Test status enum mapping |

### Integration Tests

| Test | File | Description |
|------|------|-------------|
| UI routes | `tests/test_ui_routes.py` | Test all UI endpoints |
| Template rendering | `tests/test_ui_routes.py` | Verify templates render |
| API integration | `tests/test_ui_routes.py` | Test data flows through |

### Manual Testing

| Test | Description |
|------|-------------|
| Dashboard loads | Verify stats, recent jobs, workflows |
| Health shows correct status | All indicators green/red appropriately |
| Jobs list filters work | Status and workflow filters |
| Job detail shows nodes | All nodes with correct status |
| Cancel job works | Job transitions to CANCELLED |

---

## Recommended Implementation Order

```
Week 1: Core Pages (P0)
────────────────────────
Day 1-2: workflows.html + route
Day 3-4: workflow_detail.html + route
Day 5:   Fix nodes.html placeholder

Week 2: Visualization (P1)
──────────────────────────
Day 1-2: DAG graph component (D3.js)
Day 3:   Integrate into workflow_detail
Day 4:   Timeline component
Day 5:   Integrate into job_detail

Week 3: Interactive (P2)
────────────────────────
Day 1-2: submit.html + form logic
Day 3:   Keyboard shortcuts
Day 4:   Enhanced job actions
Day 5:   Testing & fixes

Week 4: Monitoring & Polish (P3-P4)
───────────────────────────────────
Day 1:   Tasks page (if API ready)
Day 2:   Queues page (if API ready)
Day 3:   Error pages & loading states
Day 4:   Responsive design review
Day 5:   Final testing & docs
```

---

## Dependencies

### External Libraries

| Library | Version | Purpose |
|---------|---------|---------|
| D3.js | 7.x | DAG visualization |
| Feather Icons | 4.x | ✅ Already included |

### CDN Links (add to base.html)

```html
<!-- D3.js for DAG visualization -->
<script src="https://unpkg.com/d3@7"></script>
```

---

## Success Criteria

### MVP (Phase 1-2 Complete)

- [ ] All navigation items link to working pages
- [ ] Workflows page shows all loaded workflows
- [ ] Workflow detail shows node list
- [ ] DAG graph renders workflow structure
- [ ] Timeline shows job events
- [ ] No JavaScript console errors

### Production Ready (All Phases)

- [ ] Job submission works end-to-end
- [ ] Real-time updates (or fast polling)
- [ ] All error states handled gracefully
- [ ] Mobile-responsive layout
- [ ] Keyboard navigation works
- [ ] Performance acceptable (< 1s page loads)

---

## Related Documentation

| Document | Purpose |
|----------|---------|
| `ui/README.md` | UI abstraction layer documentation |
| `docs/ARCHITECTURE.md` | System architecture |
| `TODO.md` | Overall project implementation plan |
| `CLAUDE.md` | Project conventions |
