# PATTERNS.md - Advanced DAG Execution Patterns

**Last Updated**: 02 FEB 2026

This document describes advanced DAG execution patterns that extend the basic linear workflow model. These are the **priority P2 features** for enabling complex real-world workflows.

---

## Document Hierarchy

```
CLAUDE.md          ← Project constitution, coding standards
├── ARCHITECTURE.md ← System design, components, data flows
├── LOOP.md         ← Step-by-step orchestrator execution trace
├── PATTERNS.md     ← THIS FILE: Advanced DAG patterns (P2)
├── ADVANCED.md     ← Detailed implementation specs
└── TODO.md         ← Implementation status and roadmap
```

---

## Pattern Overview

| Pattern | Priority | Status | Use Case |
|---------|----------|--------|----------|
| **Conditional Routing** | P2.1 | TODO | Branch based on runtime data |
| **Fan-Out** | P2.2 | TODO | Process N items in parallel |
| **Fan-In** | P2.2 | TODO | Aggregate parallel results |
| **Nested Sub-Workflows** | Future | Aspirational | Reusable workflow components |

---

## 1. Conditional Routing

### Problem

A workflow needs to take different paths based on runtime data:
- Skip validation if file is pre-validated
- Route large files to heavy workers, small files to light workers
- Branch to error handling vs success path

### Pattern

```yaml
nodes:
  validate:
    handler: raster.validate
    next: [route_by_size]

  route_by_size:
    type: conditional
    condition: "{{ nodes.validate.output.file_size_mb }} > 100"
    on_true: process_heavy      # Large file → container worker
    on_false: process_light     # Small file → function app

  process_heavy:
    handler: raster.process
    queue: container-tasks
    next: [merge]

  process_light:
    handler: raster.process
    queue: functionapp-tasks
    next: [merge]

  merge:
    depends_on:
      any_of: [process_heavy, process_light]  # Whichever ran
    handler: finalize
```

### State Changes

```
                    validate
                        │
                        ▼
                  route_by_size (evaluates condition)
                   /         \
          (true) /           \ (false)
               ▼               ▼
        process_heavy    process_light
               │               │
               │    SKIPPED    │
               ▼       ▼       ▼
                    merge
```

When a branch is NOT taken:
- Nodes on that branch are marked `SKIPPED`
- `SKIPPED` counts as "successful" for dependency resolution
- `any_of` dependencies are satisfied by either branch completing

### Dependency Checker Impact

```python
def _dependencies_met(node_id, node_def, completed_nodes, workflow):
    # completed_nodes now includes SKIPPED status
    completed_or_skipped = {
        n.node_id for n in all_nodes
        if n.status in (NodeStatus.COMPLETED, NodeStatus.SKIPPED)
    }

    # For any_of: at least one must be complete (not skipped)
    if node_def.depends_on.any_of:
        actually_completed = {
            n.node_id for n in all_nodes
            if n.status == NodeStatus.COMPLETED
        }
        if not any(n in actually_completed for n in any_of):
            return False
```

### Implementation Files

| File | Changes |
|------|---------|
| `orchestrator/engine/evaluator.py` | Add `evaluate_condition()` |
| `services/node_service.py` | Add `skip_branch()` for untaken paths |
| `orchestrator/loop.py` | Call evaluator after conditional node completes |
| `core/models/node.py` | Ensure SKIPPED status works in state machine |

---

## 2. Fan-Out (Dynamic Parallelism)

### Problem

A workflow needs to process a variable number of items in parallel:
- Process each tile in a raster (could be 10 or 10,000 tiles)
- Upload each chunk of a large vector dataset
- Run the same analysis on N input files

The number of items isn't known until runtime.

### Pattern

```yaml
nodes:
  generate_tiles:
    handler: raster.generate_tiling_scheme
    # Output: {"tiles": [{x:0,y:0,z:5}, {x:1,y:0,z:5}, ...]}
    fan_out:
      output_key: tiles           # Array in output to iterate
      target_node: process_tile   # Node to instantiate per item

  process_tile:
    type: fan_out_target          # Special type: created dynamically
    handler: raster.process_tile
    params:
      tile: "{{ fan_out.item }}"  # Current item from array
      index: "{{ fan_out.index }}" # 0, 1, 2, ...
      total: "{{ fan_out.total }}" # Total count
    next: [merge_tiles]

  merge_tiles:
    type: fan_in                  # Waits for ALL dynamic instances
    depends_on:
      all_of: [process_tile]      # Wait for all instances
    handler: raster.merge_results
    params:
      tile_outputs: "{{ fan_in.outputs }}"  # Array of all outputs
```

### State Changes

```
        generate_tiles
              │
              │ fan_out.output_key = "tiles"
              │ tiles = [{...}, {...}, {...}]
              ▼
    ┌─────────┼─────────┐
    ▼         ▼         ▼
process_tile process_tile process_tile
  (index=0)   (index=1)   (index=2)
    │         │         │
    └─────────┼─────────┘
              ▼
        merge_tiles (fan_in)
```

### Dynamic Node Creation

When `generate_tiles` completes:

```python
# Fan-out expansion creates new NodeState records
original_output = generate_tiles.output  # {"tiles": [...]}
items = original_output["tiles"]

for index, item in enumerate(items):
    # Create dynamic node instance
    node_state = NodeState(
        job_id=job.job_id,
        node_id=f"process_tile_{index}",  # Unique ID
        parent_node_id="process_tile",    # Links to template
        fan_out_index=index,
        status=NodeStatus.PENDING,
    )
    await node_repo.create(node_state)
```

### Database Schema

```sql
-- dag_node_states additions for fan-out
ALTER TABLE dagapp.dag_node_states ADD COLUMN parent_node_id VARCHAR(64);
ALTER TABLE dagapp.dag_node_states ADD COLUMN fan_out_index INTEGER;

-- Example dynamic nodes
INSERT INTO dag_node_states (job_id, node_id, parent_node_id, fan_out_index, status)
VALUES
  ('job123', 'process_tile_0', 'process_tile', 0, 'pending'),
  ('job123', 'process_tile_1', 'process_tile', 1, 'pending'),
  ('job123', 'process_tile_2', 'process_tile', 2, 'pending');
```

### Template Context

The `fan_out` context is available in params:

```python
fan_out_context = {
    "item": items[index],      # Current item from array
    "index": index,            # 0-based position
    "total": len(items),       # Total count
    "parent_node": "generate_tiles",
}
```

### Implementation Files

| File | Changes |
|------|---------|
| `core/models/node.py` | Add `parent_node_id`, `fan_out_index` fields |
| `orchestrator/engine/fan_out.py` | NEW: `FanOutExpander` class |
| `orchestrator/engine/templates.py` | Add `fan_out.*` context |
| `orchestrator/loop.py` | Call expander after fan-out node completes |
| `repositories/node_repo.py` | Support creating dynamic nodes |

---

## 3. Fan-In (Aggregation)

### Problem

After parallel processing, collect all results into a single node:
- Merge processed tiles back into a single raster
- Aggregate statistics from N parallel workers
- Wait for variable number of dynamic nodes

### Pattern

Fan-in is the complement to fan-out. A node with `type: fan_in` waits for ALL instances of a dynamically-created node set.

```yaml
merge_tiles:
  type: fan_in
  depends_on:
    all_of: [process_tile]  # Waits for ALL process_tile_* instances
  handler: raster.merge
  params:
    outputs: "{{ fan_in.outputs }}"  # Array of all outputs
```

### Dependency Resolution

The challenge: at workflow definition time, we don't know how many `process_tile_*` nodes will exist.

```python
def _check_fan_in_ready(node_def, job_id, workflow):
    """Check if all dynamic instances of parent node are complete."""

    # Find the parent node being fanned-in
    parent_node_id = node_def.depends_on.all_of[0]  # "process_tile"

    # Query for all dynamic instances
    dynamic_nodes = await node_repo.get_by_parent(job_id, parent_node_id)

    if not dynamic_nodes:
        # Fan-out hasn't happened yet
        return False

    # All must be complete
    return all(
        n.status in (NodeStatus.COMPLETED, NodeStatus.SKIPPED)
        for n in dynamic_nodes
    )
```

### Fan-In Context

When the fan-in node runs, it has access to all outputs:

```python
fan_in_context = {
    "outputs": [
        {"tile_path": "/tmp/tile_0.tif", "stats": {...}},
        {"tile_path": "/tmp/tile_1.tif", "stats": {...}},
        {"tile_path": "/tmp/tile_2.tif", "stats": {...}},
    ],
    "count": 3,
    "parent_node": "process_tile",
}
```

### Implementation Files

| File | Changes |
|------|---------|
| `orchestrator/engine/fan_in.py` | NEW: `FanInAggregator` class |
| `services/node_service.py` | Update `_dependencies_met()` for fan-in |
| `orchestrator/engine/templates.py` | Add `fan_in.*` context |
| `repositories/node_repo.py` | Add `get_by_parent()` query |

---

## 4. Complete Example: Tiled Raster Processing

This example shows all patterns working together:

```yaml
workflow_id: raster_tiled_processing
name: "Tiled Raster Processing with Size-Based Routing"
version: "1.0"

nodes:
  start:
    type: start
    next: [validate]

  # Step 1: Validate and get file info
  validate:
    handler: raster.validate
    next: [route_by_size]

  # Step 2: Conditional routing based on size
  route_by_size:
    type: conditional
    condition: "{{ nodes.validate.output.file_size_mb }} > 500"
    on_true: generate_tiles    # Large → tile and parallelize
    on_false: process_direct   # Small → process directly

  # Branch A: Direct processing for small files
  process_direct:
    handler: raster.create_cog
    queue: container-tasks
    next: [finalize]

  # Branch B: Tiled processing for large files
  generate_tiles:
    handler: raster.generate_tiling_scheme
    params:
      tile_size_mb: 50
    fan_out:
      output_key: tiles
      target_node: process_tile

  process_tile:
    type: fan_out_target
    handler: raster.process_tile
    queue: container-tasks
    params:
      tile: "{{ fan_out.item }}"
    next: [merge_tiles]

  merge_tiles:
    type: fan_in
    depends_on:
      all_of: [process_tile]
    handler: raster.merge_tiles
    params:
      tile_outputs: "{{ fan_in.outputs }}"
    next: [finalize]

  # Step 3: Finalize (both branches converge)
  finalize:
    depends_on:
      any_of: [process_direct, merge_tiles]
    handler: raster.stac_register
    next: [end]

  end:
    type: end
```

### Execution Flow (Large File)

```
start
  │
validate ──────────────────────────────────────┐
  │                                            │
route_by_size                                  │
  │ (file_size_mb=750 > 500 → true)            │
  ▼                                            │
generate_tiles                                 │ SKIPPED
  │ output: {tiles: [{...}, {...}, ...]}       │
  │                                            ▼
  ├──────┬──────┬──────┐              process_direct
  ▼      ▼      ▼      ▼                       │
tile_0 tile_1 tile_2 tile_3                    │
  │      │      │      │                       │
  └──────┴──────┴──────┘                       │
           │                                   │
     merge_tiles ◀─────────────────────────────┘
           │
      finalize
           │
          end
```

---

## 5. Future: Nested Sub-Workflows (Aspirational)

### Vision

Encapsulate reusable workflow patterns as sub-workflows:

```yaml
# Main workflow
nodes:
  validate:
    handler: validate
    next: [process_raster]

  process_raster:
    type: sub_workflow
    workflow_id: standard_raster_pipeline  # Reference another workflow
    params:
      input_path: "{{ nodes.validate.output.path }}"
    next: [publish]
```

### Why This Is Aspirational

Sub-workflows require:
- Workflow-in-workflow job tracking
- Output mapping between parent and child
- Error propagation across workflow boundaries
- Shared vs isolated state decisions
- Complexity in visualization and debugging

This is deferred until the core patterns (conditional, fan-out, fan-in) are solid.

---

## Implementation Priority

Based on real-world workflow needs:

| Priority | Pattern | Reason |
|----------|---------|--------|
| **1st** | Conditional Routing | Size-based routing needed immediately |
| **2nd** | Fan-Out | Tiled processing is a common pattern |
| **3rd** | Fan-In | Required to complete fan-out workflows |
| **Later** | Sub-Workflows | Nice-to-have, not blocking |

---

## Related Documentation

| Document | Content |
|----------|---------|
| `ADVANCED.md` | Detailed implementation code and specs |
| `TODO.md` | Implementation status tracking |
| `LOOP.md` | How basic orchestration works |
| `docs/ARCHITECTURE.md` | System architecture overview |
