# EPOCH 5: DAG-Based Workflow Orchestration

**Created**: 27 JAN 2026
**Status**: Research & Design
**Author**: Robert Harrison + Claude
**Prerequisites**: Read `docs_claude/DAG_SPIKE.md` for foundational concepts

---

## Executive Summary

**Epoch 5 represents a fundamental architectural shift**: separating orchestration from execution.

| Epoch | Core Architecture | Insight |
|-------|-------------------|---------|
| 1-3 | Previous iterations | - |
| 4 | Function App + Docker workers with distributed orchestration | "Build a proper geospatial platform on Azure" |
| **5** | Dedicated orchestrator + dumb workers | "Orchestration is its own concern - separate it from execution" |

**Why now?** The current "last task turns out the lights" pattern is fundamentally fragile. We've built defensive coding (janitor, heartbeats, atomic SQL) around a broken pattern. DAG-based orchestration with a dedicated orchestrator is how serious distributed systems solve this problem.

**Resource context**: We have virtually unlimited Azure resources (ASE infrastructure). Scaling to zero is NOT a priority. Always-on services are acceptable. Use the right tool for the job.

---

## Table of Contents

1. [Why Epoch 5?](#why-epoch-5)
2. [The Problem with "Last Task Turns Out the Lights"](#the-problem-with-last-task-turns-out-the-lights)
3. [Motivation Assessment](#motivation-assessment)
4. [Current State Analysis](#current-state-analysis)
5. [Architecture: Dedicated Orchestrator](#architecture-dedicated-orchestrator)
6. [Workflow Definition Format](#workflow-definition-format)
7. [Illustration: Why DAGs Matter Even for Linear Workflows](#illustration-why-dags-matter-even-for-linear-workflows)
8. [The DAG Engine](#the-dag-engine)
9. [Integration with Workers](#integration-with-workers)
10. [Code Reuse Assessment](#code-reuse-assessment)
11. [High-Level Project Plan](#high-level-project-plan)
12. [Implementation Phases](#implementation-phases)

---

## Why Epoch 5?

### Epochs as Page-1 Rewrites

Each epoch has represented a fundamental rethinking of architecture:

- **Epoch 4** (current): Successfully built a geospatial platform on Azure Functions + Docker. Linear stages, "last task turns out lights" fan-in, CoreMachine as orchestrator-within-workers.

- **Epoch 5** (proposed): Recognize that **orchestration is a separate concern** from execution. Stop pretending workers can coordinate themselves. Build a dedicated orchestrator.

### The Function-App-Only Mindset

Epoch 4 architecture reflects:

> "I have a hammer (Azure Functions), everything is a nail (stateless HTTP/queue trigger)"

This led to:
- **Stages** as a poor man's dependencies
- **"Last task turns out lights"** as poor man's fan-in
- **Janitor service** as poor man's health monitoring
- **CoreMachine in workers** as poor man's orchestrator

**It works**, but it's accidental complexity from fitting a square peg into a round hole.

### Epoch 5 Insight

```
Epoch 4: Workers ARE the orchestration (distributed state, defensive coding)
         Every worker has CoreMachine, every worker makes decisions

Epoch 5: Workers EXECUTE, Orchestrator ORCHESTRATES (separation of concerns)
         Workers are dumb. One orchestrator makes all decisions.
```

---

## The Problem with "Last Task Turns Out the Lights"

### What It Is

Current fan-in pattern for stage completion:

```python
# When a task completes, it checks: "Am I the last one?"
UPDATE tasks
SET status = 'completed'
WHERE task_id = $1
RETURNING (
    SELECT COUNT(*) = 0
    FROM tasks
    WHERE parent_job_id = $2
    AND stage = $3
    AND status != 'completed'
) as is_last_task;

# If is_last_task, advance to next stage
```

### Why It's Fragile

This pattern has fundamental failure modes:

| Failure Mode | What Happens | Current Mitigation | Is It Sufficient? |
|--------------|--------------|-------------------|-------------------|
| **Race condition** | Two tasks complete simultaneously, both see themselves as "last" | Atomic SQL `UPDATE ... RETURNING` | Mostly, but complex |
| **Partial failure** | 1 of 10 tasks fails, 9 complete, stage never advances | Janitor marks stale after timeout | Requires manual intervention |
| **Lost completion** | Task completes, worker dies before DB update | Janitor detects via `last_pulse` | 30+ minute detection delay |
| **Worker death** | Task "running" but worker is dead | 24-hour timeout for Docker | Job stuck for hours |
| **Message loss** | Completion message lost in Service Bus | None | Job hangs forever |
| **State divergence** | DB says "running", queue is empty | None | Silent failure |
| **Retry storms** | Failed task retries while others complete | Retry counter | Can still cause duplicates |

### The Deeper Problem

**We're doing distributed consensus without a consensus protocol.**

Every worker independently:
1. Completes its task
2. Checks if it's the last one
3. Decides whether to advance the job

This is fundamentally unsafe. We've papered over it with:
- Atomic SQL (handles simple races)
- Janitor service (detects zombies, eventually)
- Heartbeats (detects dead workers, eventually)
- Retry limits (prevents infinite loops)

**These are band-aids, not solutions.** The correct solution is: **one orchestrator makes all decisions**.

### Why Frameworks Exist

Temporal, Airflow, Durable Functions exist because orchestration is a solved problem:

```
What looks simple:           What actually happens:

A → B → C                   A completes
                            ↓
                            Message to B queued
                            ↓
                            B starts... worker dies mid-execution
                            ↓
                            Is B running? Failed? Completed?
                            ↓
                            B retries, maybe completes twice
                            ↓
                            Two messages to C
                            ↓
                            C runs twice, corrupts state
```

Frameworks handle:
- **Exactly-once semantics** (or at-least-once with idempotency)
- **Durable state** that survives crashes
- **Dependency resolution** with complex graphs
- **Single source of truth** for job state
- **Recovery** that doesn't require human intervention

---

## Motivation Assessment

### Why Explore DAGs?

| # | Motivation | Assessment |
|---|------------|------------|
| 1 | **Learning** - Understand how serious distributed systems operate, vendor-agnostic | ✅ VALID - DAGs are foundational to Spark, Kubernetes, CI/CD, build systems |
| 2 | **Conditionals exist** - We already have conditional routing, just handled ad-hoc | ✅ CORRECT - Hidden complexity that should be declared explicitly |
| 3 | **Template-driven workflows** - Could workflow definitions be simplified with graph representation? | ✅ REAL OPPORTUNITY - Move from Python classes to YAML/JSON definitions |
| 4 | **Future plugin architecture** - Configurable DAGs as safer alternative to custom code | ⚠️ DISTANT BUT PLAUSIBLE - DAGs enable this without arbitrary code execution |

### Motivation 1: Learning (Always Valid)

DAGs are foundational to:
- Spark/Dask execution plans
- Kubernetes operator reconciliation
- Build systems (Make, Bazel, Gradle)
- CI/CD pipelines (GitHub Actions, Azure DevOps)
- Neural network computation graphs

Understanding DAGs vendor-agnostic means understanding the *pattern*, not just "how to use Airflow."

### Motivation 2: Conditionals Already Exist

We have conditionals, they're just **buried in procedural code** rather than **declared in workflow structure**:

**Current: Conditional hidden in routing logic**
```python
def route_task(job_params):
    if size < 100_MB:
        return "functionapp-tasks"
    elif size < 1_GB:
        return "container-tasks"  # in-memory mode
    else:
        return "container-tasks"  # mount mode
```

**DAG: Conditional is part of workflow definition**
```yaml
route_by_size:
  type: conditional
  branches:
    - condition: "size < 100MB"
      next: fa_process
    - condition: "100MB <= size < 1GB"
      next: docker_memory
    - default: true
      next: docker_mount
```

**FATHOM file discovery** is another example - it's **dynamic DAG construction**, we just don't call it that.

### Motivation 3: Template-Driven Workflows

**Current state** - workflows are Python classes:
```python
class ProcessRasterDocker(JobBaseMixin, JobBase):
    job_type = "process_raster_docker"
    stages = [
        {"number": 1, "name": "validate", ...},
        {"number": 2, "name": "create_cog", ...},
        {"number": 3, "name": "create_stac", ...}
    ]
```

**Could become** - workflows defined in data:
```yaml
name: process_raster_docker
nodes:
  validate:
    handler: raster_validate
    next: route_by_size
  route_by_size:
    type: conditional
    branches:
      - condition: "{{ size_mb < 100 }}"
        next: process_fa
      - condition: "{{ size_mb < 1000 }}"
        next: process_docker_memory
      - default: true
        next: process_docker_mount
  # ... etc
```

**Benefits:**
- Workflows defined in data (YAML/JSON), not code
- Non-engineers can read/modify workflows
- Version control workflow definitions separately from handlers
- Generate documentation/diagrams automatically

### Motivation 4: Plugin Architecture (Future)

The spectrum of client customization:

| Level | Risk | Example |
|-------|------|---------|
| **1. Parameters** | None | Client chooses compression level, CRS |
| **2. Pre-built node selection** | Low | Client picks "COG" or "GeoParquet" output |
| **3. Configurable DAGs** | Medium | Client connects pre-built nodes in custom order |
| **4. Custom code** | NIGHTMARE | Client uploads Python that runs on your infra |

Configurable DAGs (Level 3) is the sweet spot for future extensibility.

---

## Current State Analysis

### Current Architecture (Epoch 4)

```
┌─────────────────────────────────────────────────────────────────┐
│           EPOCH 4: DISTRIBUTED ORCHESTRATION                     │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   Every worker contains CoreMachine                              │
│   Every worker makes orchestration decisions                     │
│   State is distributed across workers + database                 │
│                                                                  │
│   ┌─────────────────────────────────────────────────────────┐   │
│   │                    FUNCTION APP                          │   │
│   │                                                          │   │
│   │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐      │   │
│   │  │   Worker    │  │   Worker    │  │   Worker    │      │   │
│   │  │ +CoreMachine│  │ +CoreMachine│  │ +CoreMachine│      │   │
│   │  │             │  │             │  │             │      │   │
│   │  │ "Am I last?"│  │ "Am I last?"│  │ "Am I last?"│      │   │
│   │  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘      │   │
│   │         │                │                │              │   │
│   └─────────┼────────────────┼────────────────┼──────────────┘   │
│             │                │                │                   │
│             ▼                ▼                ▼                   │
│   ┌─────────────────────────────────────────────────────────┐   │
│   │                     POSTGRESQL                           │   │
│   │                                                          │   │
│   │   jobs: stage=2, status=processing                       │   │
│   │   tasks: [completed, completed, running, running]        │   │
│   │                                                          │   │
│   │   "Who knows the truth? Race to find out!"               │   │
│   │                                                          │   │
│   └─────────────────────────────────────────────────────────┘   │
│                                                                  │
│   ALSO: Janitor timer runs every 5 min to detect zombies        │
│   ALSO: Heartbeats (last_pulse) to detect dead workers          │
│   ALSO: Atomic SQL to handle completion races                    │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘

Problems:
- Multiple decision-makers (race conditions)
- Defensive coding everywhere
- Failure detection is slow (minutes to hours)
- Complex mental model ("who decides what?")
```

### What Current Model Cannot Express

**1. Cross-stage dependencies:**
```
    Stage 2: [Create COG A] [Create COG B]
                ↓               ↓
    Stage 3: [Index A to STAC]  │
                ↓               │
    Stage 4: ←──────────────────┘ [Merge A + B into Mosaic]

    Merge needs BOTH "Index A" AND "Create COG B" - different stages.
    Current model: impossible.
```

**2. Conditional branching:**
```
                [Validate]
                    │
            ┌───────┼───────┐
            ▼       ▼       ▼
        [< 100MB] [med]   [> 1GB]
            │       │         │
            ▼       ▼         ▼
        [FA]    [Docker]   [Docker+Mount]

    Current model: Buried in routing code, not visible in job definition.
```

**3. Dynamic fan-out:**
```
    [Discover Files] → Found 47 files → [Process F1] [F2] ... [F47] → [Finalize]

    Current model: Stages must be defined at job creation time.
```

---

## Architecture: Dedicated Orchestrator

### The Epoch 5 Model

```
┌─────────────────────────────────────────────────────────────────┐
│           EPOCH 5: CENTRALIZED ORCHESTRATION                     │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   ONE orchestrator makes ALL decisions                           │
│   Workers are DUMB - just execute and report                     │
│   State lives in ONE place                                       │
│                                                                  │
│   ┌─────────────────────────────────────────────────────────┐   │
│   │              DAG ORCHESTRATOR                            │   │
│   │              (Docker Web App, always-on)                 │   │
│   │                                                          │   │
│   │   ┌─────────────────────────────────────────────────┐   │   │
│   │   │                 DAG ENGINE                       │   │   │
│   │   │                                                  │   │   │
│   │   │  - Loads workflow definitions                    │   │   │
│   │   │  - Creates DAG instances for jobs                │   │   │
│   │   │  - Evaluates dependencies                        │   │   │
│   │   │  - Dispatches ready nodes to queues              │   │   │
│   │   │  - Receives completion notifications             │   │   │
│   │   │  - Handles failures and retries                  │   │   │
│   │   │  - ONE source of truth                           │   │   │
│   │   │                                                  │   │   │
│   │   └─────────────────────────────────────────────────┘   │   │
│   │                          │                               │   │
│   │                          ▼                               │   │
│   │   ┌──────────────┐   ┌──────────────┐                   │   │
│   │   │  PostgreSQL  │   │  Service Bus │                   │   │
│   │   │  (state)     │   │  (dispatch)  │                   │   │
│   │   └──────────────┘   └──────────────┘                   │   │
│   │                                                          │   │
│   └─────────────────────────────────────────────────────────┘   │
│                              │                                   │
│                              │ Dispatch tasks                    │
│                              ▼                                   │
│   ┌─────────────────────────────────────────────────────────┐   │
│   │                      WORKERS                             │   │
│   │                      (Dumb executors)                    │   │
│   │                                                          │   │
│   │   ┌─────────────┐              ┌─────────────┐          │   │
│   │   │ Function App│              │Docker Worker│          │   │
│   │   │             │              │             │          │   │
│   │   │ - Pick task │              │ - Pick task │          │   │
│   │   │ - Execute   │              │ - Execute   │          │   │
│   │   │ - Report    │              │ - Report    │          │   │
│   │   │             │              │             │          │   │
│   │   │ NO CoreMachine             │ NO CoreMachine         │   │
│   │   │ NO "Am I last?"            │ NO decisions │         │   │
│   │   │ NO orchestration           │ Just execute │         │   │
│   │   └─────────────┘              └─────────────┘          │   │
│   │                                                          │   │
│   └─────────────────────────────────────────────────────────┘   │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘

Benefits:
- Single decision-maker (no races)
- Clear responsibility (orchestrator orchestrates, workers work)
- Fast failure detection (orchestrator monitors actively)
- Simple mental model ("orchestrator decides everything")
```

### Why Dedicated Orchestrator (Not Event-Driven)

Given unlimited Azure resources, the calculus changes:

| Consideration | Event-Driven | Dedicated Orchestrator |
|---------------|--------------|------------------------|
| **Scales to zero** | ✅ Yes | ❌ No (always-on) |
| **Simplicity** | ⚠️ Distributed triggers | ✅ One process, one loop |
| **Debugging** | ⚠️ "Which trigger fired?" | ✅ "Check orchestrator logs" |
| **State management** | ⚠️ Reconstruct from events | ✅ Orchestrator owns state |
| **Failure detection** | ⚠️ Timeout-based | ✅ Active monitoring |
| **Race conditions** | ⚠️ Still possible | ✅ Eliminated |

**With unlimited resources, "always-on" is not a cost concern.** Simplicity and reliability win.

### Orchestrator Responsibilities

The DAG Orchestrator handles:

| Responsibility | Description |
|----------------|-------------|
| **Workflow loading** | Read YAML/JSON definitions, parse into DAG structure |
| **Instance creation** | Create runtime instance when job submitted |
| **Dependency evaluation** | Determine which nodes can execute |
| **Task dispatch** | Send tasks to appropriate queue (FA or Docker) |
| **Completion handling** | Receive results, update state, evaluate next nodes |
| **Failure handling** | Retry policies, failure propagation, alerting |
| **Timeout monitoring** | Detect stuck tasks without waiting for janitor |
| **Conditional evaluation** | Evaluate branch conditions, route accordingly |

### Worker Responsibilities (Simplified!)

Workers become much simpler:

| Responsibility | Description |
|----------------|-------------|
| **Pick task from queue** | Receive Service Bus message |
| **Execute handler** | Run the actual work (COG creation, ETL, etc.) |
| **Report result** | POST to orchestrator: success/failure + output |

**Workers do NOT:**
- Track job state
- Decide what runs next
- Check if they're "last"
- Advance stages
- Handle fan-in logic

---

## Workflow Definition Format

### YAML Structure

```yaml
# workflows/raster_processing_v1.yaml
workflow_id: raster_processing_v1
name: Raster Processing Pipeline
version: 1
description: |
  Validates raster, routes by size, creates COG, registers to STAC.
  Supports three processing paths based on file size.

# Input schema (validated on submission)
inputs:
  container_name:
    type: string
    required: true
  blob_name:
    type: string
    required: true
  collection_id:
    type: string
    required: true
    default: "default-collection"

# Node definitions
nodes:
  # ─────────────────────────────────────────────────────────────
  # START
  # ─────────────────────────────────────────────────────────────
  START:
    type: start
    next: validate

  # ─────────────────────────────────────────────────────────────
  # VALIDATION (runs on Function App - lightweight)
  # ─────────────────────────────────────────────────────────────
  validate:
    type: task
    handler: raster_validate
    queue: functionapp-tasks
    timeout_seconds: 300
    retry:
      max_attempts: 3
      backoff: exponential
    params:
      container_name: "{{ inputs.container_name }}"
      blob_name: "{{ inputs.blob_name }}"
    next: route_by_size

  # ─────────────────────────────────────────────────────────────
  # CONDITIONAL ROUTING
  # ─────────────────────────────────────────────────────────────
  route_by_size:
    type: conditional
    description: Route to appropriate processor based on file size
    condition_field: "{{ nodes.validate.output.size_mb }}"
    branches:
      - name: small
        condition: "< 100"
        next: process_fa
      - name: medium
        condition: "< 1000"
        next: process_docker_memory
      - name: large
        default: true
        next: process_docker_mount

  # ─────────────────────────────────────────────────────────────
  # PROCESSING PATHS (mutually exclusive)
  # ─────────────────────────────────────────────────────────────
  process_fa:
    type: task
    handler: raster_cog_functionapp
    queue: functionapp-tasks
    timeout_seconds: 600
    params:
      source_url: "{{ nodes.validate.output.blob_url }}"
      collection_id: "{{ inputs.collection_id }}"
    next: register_stac

  process_docker_memory:
    type: task
    handler: raster_cog_docker
    queue: container-tasks
    timeout_seconds: 1800
    params:
      source_url: "{{ nodes.validate.output.blob_url }}"
      collection_id: "{{ inputs.collection_id }}"
      use_mount: false
    next: register_stac

  process_docker_mount:
    type: task
    handler: raster_cog_docker
    queue: container-tasks
    timeout_seconds: 7200
    params:
      source_url: "{{ nodes.validate.output.blob_url }}"
      collection_id: "{{ inputs.collection_id }}"
      use_mount: true
    next: register_stac

  # ─────────────────────────────────────────────────────────────
  # STAC REGISTRATION (fan-in from any processing path)
  # ─────────────────────────────────────────────────────────────
  register_stac:
    type: task
    handler: stac_register
    queue: functionapp-tasks
    timeout_seconds: 300
    # Fan-in: runs when ANY upstream completes (OR semantics)
    depends_on:
      any_of:  # OR semantics - whichever branch ran
        - process_fa
        - process_docker_memory
        - process_docker_mount
    params:
      cog_url: "{{ upstream.output.cog_url }}"
      collection_id: "{{ inputs.collection_id }}"
      metadata: "{{ upstream.output.metadata }}"
    next: END

  # ─────────────────────────────────────────────────────────────
  # END
  # ─────────────────────────────────────────────────────────────
  END:
    type: end
```

### Fan-Out Example (Tiled Raster)

```yaml
# workflows/raster_tiled_v1.yaml
nodes:
  validate:
    type: task
    handler: raster_validate
    next: plan_tiles

  plan_tiles:
    type: task
    handler: raster_plan_tiles
    description: Determines tile grid, outputs list of tile specs
    next: create_tiles

  create_tiles:
    type: fan_out
    description: Dynamic fan-out - creates N parallel tasks
    source: "{{ nodes.plan_tiles.output.tiles }}"  # Array of tile specs
    task:
      handler: raster_cog_docker
      queue: container-tasks
      params:
        tile_spec: "{{ item }}"  # Each item from the array
    next: register_stac

  register_stac:
    type: task
    handler: stac_register_collection
    # Fan-in: ALL tiles must complete (AND semantics)
    depends_on:
      all_of: create_tiles  # Wait for ALL dynamic tasks
    params:
      tile_results: "{{ nodes.create_tiles.outputs }}"  # Array of results
    next: END
```

### Dependency Semantics

```yaml
# OR semantics (any upstream completes)
depends_on:
  any_of:
    - node_a
    - node_b
    - node_c

# AND semantics (all upstream complete)
depends_on:
  all_of:
    - node_a
    - node_b
    - node_c

# Mixed (A OR B) AND C
depends_on:
  all_of:
    - any_of:
        - node_a
        - node_b
    - node_c
```

---

## Illustration: Why DAGs Matter Even for Linear Workflows

### The process_raster Job

The current `process_raster` job is essentially linear:

```
validate_input → reproject → cog_translate → update_stac
```

This is the **simplest possible DAG** - a straight chain. But expressing it explicitly as a DAG still provides major benefits.

### As a YAML Workflow Definition

```yaml
name: process_raster
version: 1
description: |
  Simple raster processing: validate, reproject to 4326,
  create COG, register to STAC catalog.

inputs:
  container_name:
    type: string
    required: true
  file_name:
    type: string
    required: true
  dataset_id:
    type: string
    required: true

nodes:
  START:
    type: start
    next: validate_input

  validate_input:
    type: task
    handler: raster_validate
    queue: functionapp-tasks
    timeout_seconds: 300
    params:
      container_name: "{{ inputs.container_name }}"
      file_name: "{{ inputs.file_name }}"
      dataset_id: "{{ inputs.dataset_id }}"
    next: reproject

  reproject:
    type: task
    handler: raster_reproject
    queue: container-tasks
    timeout_seconds: 3600
    retry:
      max_attempts: 3
      backoff: exponential
    params:
      target_crs: "EPSG:4326"
      source_path: "{{ nodes.validate_input.output.validated_path }}"
    next: cog_translate

  cog_translate:
    type: task
    handler: raster_cog_translate
    queue: container-tasks
    timeout_seconds: 7200
    retry:
      max_attempts: 3
      backoff: exponential
    params:
      compression: "DEFLATE"
      tile_size: 512
      input_path: "{{ nodes.reproject.output.reprojected_path }}"
    next: update_stac

  update_stac:
    type: task
    handler: stac_register_item
    queue: functionapp-tasks
    timeout_seconds: 300
    params:
      cog_path: "{{ nodes.cog_translate.output.cog_path }}"
      bbox: "{{ nodes.cog_translate.output.bbox }}"
      properties: "{{ nodes.validate_input.output.metadata }}"
    next: END

  END:
    type: end
```

### Visual Representation

```
┌─────────────────┐
│     START       │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ validate_input  │  ← Function App (lightweight)
│                 │    Timeout: 5 min
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│    reproject    │  ← Docker Worker (heavy)
│                 │    Timeout: 60 min, 3 retries
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  cog_translate  │  ← Docker Worker (heavy)
│                 │    Timeout: 2 hours, 3 retries
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   update_stac   │  ← Function App (lightweight)
│                 │    Timeout: 5 min
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│      END        │
└─────────────────┘
```

### Benefit 1: Granular Retries (Critical for FATHOM 8TB Dataset)

With the current system, if `cog_translate` fails after 90 minutes of work:

```
CURRENT (Epoch 4):
├── Job fails entirely
├── Must restart from beginning (validate again, reproject again)
├── 90 minutes of cog_translate work lost
├── For FATHOM: potentially hours of reprocessing per retry
└── Manual intervention often required
```

With DAG:

```
EPOCH 5:
├── validate_input: completed ✓ (output stored)
├── reproject: completed ✓ (output stored)
├── cog_translate: FAILED ✗
│   └── Retry 1: starts from cog_translate (not validate!)
│   └── Uses reproject.output.reprojected_path from database
├── update_stac: pending (waiting)
└── 90 minutes preserved, not wasted
```

**For an 8TB FATHOM dataset**, this means:
- If tile #47 of 100 fails during COG creation → retry only tile #47
- If STAC registration fails → retry STAC registration, not the entire pipeline
- Network blip during upload → retry upload, not reprocessing

### Benefit 2: Explicit Data Flow

The `params_from_node` pattern makes data dependencies explicit:

```yaml
cog_translate:
  params:
    input_path: "{{ nodes.reproject.output.reprojected_path }}"
    #            ↑ Explicit: "I need this output from reproject"
```

**Current system**: Data flow is implicit in handler code, hard to trace.

**DAG system**: One glance at YAML shows exactly what each node needs and produces.

### Benefit 3: Independent Timeout/Retry per Node

```yaml
validate_input:
  timeout_seconds: 300        # 5 min - should be fast
  retry: none                 # Validation shouldn't need retries

cog_translate:
  timeout_seconds: 7200       # 2 hours - large files need time
  retry:
    max_attempts: 3           # Transient failures happen
    backoff: exponential      # Don't hammer on failure
```

**Current system**: One timeout for entire job, retry logic scattered in code.

### Benefit 4: Easy Extensibility

Adding parallel preview generation becomes trivial:

```yaml
# BEFORE: Linear chain
validate → reproject → cog_translate → update_stac

# AFTER: Add preview in parallel (zero changes to existing nodes!)
nodes:
  reproject:
    next: [cog_translate, generate_preview]  # Fan out to both

  generate_preview:           # NEW - runs in parallel with cog_translate
    type: task
    handler: raster_thumbnail
    queue: functionapp-tasks
    params:
      source_path: "{{ nodes.reproject.output.reprojected_path }}"

  update_stac:
    depends_on:
      all_of: [cog_translate, generate_preview]  # Wait for BOTH
    params:
      preview_url: "{{ nodes.generate_preview.output.thumbnail_url }}"
```

Visual:

```
         ┌─────────────────┐
         │ validate_input  │
         └────────┬────────┘
                  │
                  ▼
         ┌─────────────────┐
         │    reproject    │
         └────────┬────────┘
                  │
        ┌─────────┴─────────┐
        │                   │
        ▼                   ▼
┌───────────────┐   ┌───────────────┐
│generate_preview   │ cog_translate │
│ (Function App)│   │(Docker Worker)│
└───────┬───────┘   └───────┬───────┘
        │                   │
        └─────────┬─────────┘
                  │
                  ▼
         ┌─────────────────┐
         │   update_stac   │
         └─────────────────┘
```

The orchestrator handles the fan-out and fan-in automatically. No code changes in handlers.

### Benefit 5: State Visibility

Every node has independent status in the database:

```sql
SELECT node_id, status, started_at, completed_at, retry_count
FROM app.dag_node_states
WHERE instance_id = 'job-xyz';

 node_id        | status    | started_at          | completed_at        | retry_count
----------------+-----------+---------------------+---------------------+-------------
 validate_input | completed | 2026-01-28 10:00:00 | 2026-01-28 10:00:15 | 0
 reproject      | completed | 2026-01-28 10:00:15 | 2026-01-28 10:15:30 | 0
 cog_translate  | running   | 2026-01-28 10:15:31 | NULL                | 1
 update_stac    | pending   | NULL                | NULL                | 0
```

**Current system**: "Is the job on stage 2 or 3? Did task 5 complete?" - requires detective work.

**DAG system**: Each node's state is independently queryable with full history.

### Summary: Even Linear Workflows Benefit

| Aspect | Current (Epoch 4) | DAG (Epoch 5) |
|--------|-------------------|---------------|
| **Retry granularity** | Entire job | Individual node |
| **State visibility** | Job-level status | Per-node status |
| **Data flow** | Implicit in code | Explicit in YAML |
| **Timeouts** | One per job | Per node |
| **Adding parallelism** | Code rewrite | YAML change |
| **Failure recovery** | Start over | Resume from failure |

For large datasets like **FATHOM (8TB)**, granular retries alone justify the DAG approach. Hours of processing aren't wasted on transient failures.

---

## The DAG Engine

### Core Components

```
┌─────────────────────────────────────────────────────────────────┐
│                       DAG ENGINE                                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   ┌─────────────────┐   ┌─────────────────┐                     │
│   │ WorkflowLoader  │   │ InstanceManager │                     │
│   │                 │   │                 │                     │
│   │ - Load YAML     │   │ - Create inst   │                     │
│   │ - Validate      │   │ - Track state   │                     │
│   │ - Cache         │   │ - Node states   │                     │
│   └────────┬────────┘   └────────┬────────┘                     │
│            │                     │                               │
│            └──────────┬──────────┘                               │
│                       │                                          │
│                       ▼                                          │
│            ┌─────────────────────┐                               │
│            │    DAGEvaluator     │                               │
│            │                     │                               │
│            │ - Dependency check  │                               │
│            │ - Find ready nodes  │                               │
│            │ - Eval conditionals │                               │
│            │ - Resolve templates │                               │
│            └──────────┬──────────┘                               │
│                       │                                          │
│                       ▼                                          │
│            ┌─────────────────────┐                               │
│            │   TaskDispatcher    │                               │
│            │                     │                               │
│            │ - Build task msg    │                               │
│            │ - Send to queue     │                               │
│            │ - Track dispatch    │                               │
│            └─────────────────────┘                               │
│                                                                  │
│   ┌─────────────────────────────────────────────────────────┐   │
│   │                    MAIN LOOP                             │   │
│   │                                                          │   │
│   │   while True:                                            │   │
│   │       # 1. Check for completed tasks (poll or webhook)   │   │
│   │       completions = get_task_completions()               │   │
│   │                                                          │   │
│   │       # 2. Update node states                            │   │
│   │       for task in completions:                           │   │
│   │           update_node_state(task)                        │   │
│   │                                                          │   │
│   │       # 3. Evaluate all active DAG instances             │   │
│   │       for instance in get_active_instances():            │   │
│   │           ready_nodes = evaluate(instance)               │   │
│   │           for node in ready_nodes:                       │   │
│   │               dispatch(node)                             │   │
│   │                                                          │   │
│   │       # 4. Check for timeouts                            │   │
│   │       check_timeouts()                                   │   │
│   │                                                          │   │
│   │       sleep(1)  # Or use event-driven with fallback poll │   │
│   │                                                          │   │
│   └─────────────────────────────────────────────────────────┘   │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Database Schema

```sql
-- ============================================================
-- DAG WORKFLOW DEFINITIONS
-- ============================================================
CREATE TABLE app.dag_workflows (
    workflow_id VARCHAR(64) PRIMARY KEY,
    name VARCHAR(128) NOT NULL,
    description TEXT,
    definition JSONB NOT NULL,      -- Full YAML converted to JSON
    version INTEGER DEFAULT 1,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- DAG INSTANCES (one per job execution)
-- ============================================================
CREATE TABLE app.dag_instances (
    instance_id VARCHAR(64) PRIMARY KEY,  -- Same as job_id
    workflow_id VARCHAR(64) REFERENCES app.dag_workflows(workflow_id),
    status VARCHAR(20) DEFAULT 'running',  -- running, completed, failed, cancelled
    input_params JSONB NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    error TEXT
);

CREATE INDEX idx_dag_instances_status ON app.dag_instances(status);
CREATE INDEX idx_dag_instances_workflow ON app.dag_instances(workflow_id);

-- ============================================================
-- NODE STATES (state of each node in each instance)
-- ============================================================
CREATE TABLE app.dag_node_states (
    instance_id VARCHAR(64) REFERENCES app.dag_instances(instance_id),
    node_id VARCHAR(64),
    status VARCHAR(20) DEFAULT 'pending',
    -- pending: waiting for dependencies
    -- ready: dependencies met, awaiting dispatch
    -- dispatched: sent to queue
    -- running: worker picked it up
    -- completed: finished successfully
    -- failed: finished with error
    -- skipped: branch not taken
    task_id VARCHAR(64),            -- Links to dispatched task
    dispatched_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    output JSONB,                    -- Result data for downstream nodes
    error TEXT,
    retry_count INTEGER DEFAULT 0,
    PRIMARY KEY (instance_id, node_id)
);

CREATE INDEX idx_dag_node_status ON app.dag_node_states(status);
CREATE INDEX idx_dag_node_instance ON app.dag_node_states(instance_id);

-- ============================================================
-- TASK RESULTS (workers report here, orchestrator reads)
-- ============================================================
CREATE TABLE app.dag_task_results (
    task_id VARCHAR(64) PRIMARY KEY,
    instance_id VARCHAR(64),
    node_id VARCHAR(64),
    status VARCHAR(20),              -- completed, failed
    output JSONB,
    error TEXT,
    worker_id VARCHAR(64),           -- Which worker executed
    reported_at TIMESTAMPTZ DEFAULT NOW(),
    processed BOOLEAN DEFAULT false  -- Orchestrator sets true after processing
);

CREATE INDEX idx_dag_task_results_unprocessed
    ON app.dag_task_results(processed) WHERE processed = false;
```

### Orchestrator Main Loop

```python
# dag_orchestrator/main.py
"""
DAG Orchestrator - The brain of Epoch 5.

Runs as a dedicated Docker Web App, always-on.
Single instance (or leader-elected for HA).
"""

import asyncio
import logging
from datetime import datetime, timedelta

from .loader import WorkflowLoader
from .evaluator import DAGEvaluator
from .dispatcher import TaskDispatcher
from .instance_manager import InstanceManager

logger = logging.getLogger(__name__)


class DAGOrchestrator:
    """
    Central orchestrator for all DAG workflow execution.

    Responsibilities:
    - Monitor for task completions
    - Evaluate DAG dependencies
    - Dispatch ready tasks
    - Handle failures and timeouts
    """

    def __init__(self, db_pool, service_bus_client):
        self.db = db_pool
        self.loader = WorkflowLoader()
        self.dispatcher = TaskDispatcher(service_bus_client)
        self.instance_manager = InstanceManager(db_pool)
        self.running = False

    async def run(self):
        """Main orchestrator loop."""
        self.running = True
        logger.info("DAG Orchestrator starting...")

        while self.running:
            try:
                # 1. Process completed tasks
                await self._process_completions()

                # 2. Evaluate all active instances
                await self._evaluate_active_instances()

                # 3. Check for timeouts
                await self._check_timeouts()

                # 4. Brief sleep (or replace with event-driven)
                await asyncio.sleep(1)

            except Exception as e:
                logger.exception(f"Orchestrator loop error: {e}")
                await asyncio.sleep(5)  # Back off on error

    async def _process_completions(self):
        """Process task completion reports from workers."""

        # Get unprocessed completions
        completions = await self.db.fetch("""
            SELECT task_id, instance_id, node_id, status, output, error
            FROM app.dag_task_results
            WHERE processed = false
            ORDER BY reported_at
            LIMIT 100
        """)

        for completion in completions:
            try:
                await self._handle_completion(completion)
            except Exception as e:
                logger.exception(f"Error handling completion {completion['task_id']}: {e}")

    async def _handle_completion(self, completion):
        """Handle a single task completion."""

        instance_id = completion['instance_id']
        node_id = completion['node_id']
        status = completion['status']

        # Update node state
        if status == 'completed':
            await self.instance_manager.mark_node_completed(
                instance_id=instance_id,
                node_id=node_id,
                output=completion['output']
            )
        else:
            await self.instance_manager.mark_node_failed(
                instance_id=instance_id,
                node_id=node_id,
                error=completion['error']
            )

        # Mark completion as processed
        await self.db.execute("""
            UPDATE app.dag_task_results
            SET processed = true
            WHERE task_id = $1
        """, completion['task_id'])

    async def _evaluate_active_instances(self):
        """Evaluate all running DAG instances for ready nodes."""

        instances = await self.db.fetch("""
            SELECT instance_id, workflow_id, input_params
            FROM app.dag_instances
            WHERE status = 'running'
        """)

        for instance in instances:
            await self._evaluate_instance(instance)

    async def _evaluate_instance(self, instance):
        """Evaluate a single DAG instance."""

        instance_id = instance['instance_id']
        workflow_id = instance['workflow_id']

        # Load workflow definition
        workflow = self.loader.load(workflow_id)

        # Get current node states
        node_states = await self.instance_manager.get_node_states(instance_id)

        # Build execution context
        context = {
            'inputs': instance['input_params'],
            'nodes': {
                node_id: {'output': state.output}
                for node_id, state in node_states.items()
                if state.output
            }
        }

        # Evaluate DAG
        evaluator = DAGEvaluator(workflow, node_states)
        ready_nodes = evaluator.get_ready_nodes()

        # Dispatch ready nodes
        for node_id in ready_nodes:
            node_def = workflow['nodes'][node_id]
            await self._dispatch_node(instance_id, node_id, node_def, context)

        # Check if DAG is complete
        if evaluator.is_complete():
            await self.instance_manager.mark_instance_completed(instance_id)
        elif evaluator.is_failed():
            await self.instance_manager.mark_instance_failed(instance_id)

    async def _dispatch_node(self, instance_id, node_id, node_def, context):
        """Dispatch a node for execution."""

        node_type = node_def.get('type', 'task')

        # Handle special node types
        if node_type == 'start':
            await self.instance_manager.mark_node_completed(instance_id, node_id)
            return

        if node_type == 'end':
            await self.instance_manager.mark_node_completed(instance_id, node_id)
            return

        if node_type == 'conditional':
            await self._handle_conditional(instance_id, node_id, node_def, context)
            return

        if node_type == 'fan_out':
            await self._handle_fan_out(instance_id, node_id, node_def, context)
            return

        # Regular task node
        handler = node_def['handler']
        queue = node_def.get('queue', 'functionapp-tasks')
        params = self._resolve_params(node_def.get('params', {}), context)

        task_id = await self.dispatcher.dispatch(
            instance_id=instance_id,
            node_id=node_id,
            handler=handler,
            queue=queue,
            params=params,
            timeout_seconds=node_def.get('timeout_seconds', 3600)
        )

        await self.instance_manager.mark_node_dispatched(
            instance_id=instance_id,
            node_id=node_id,
            task_id=task_id
        )

    async def _handle_conditional(self, instance_id, node_id, node_def, context):
        """Evaluate a conditional node and mark appropriate branch."""

        evaluator = DAGEvaluator({}, {})
        next_node = evaluator.evaluate_condition(node_def, context)

        # Mark this conditional node as completed
        await self.instance_manager.mark_node_completed(
            instance_id=instance_id,
            node_id=node_id,
            output={'branch_taken': next_node}
        )

        # Mark untaken branches as skipped
        for branch in node_def.get('branches', []):
            branch_next = branch.get('next')
            if branch_next and branch_next != next_node:
                # This branch was not taken - skip its target
                # (Actually, we don't skip the target - it just won't have
                #  its dependency met from this path)
                pass

    async def _handle_fan_out(self, instance_id, node_id, node_def, context):
        """Handle dynamic fan-out - create N parallel tasks."""

        source_path = node_def['source']
        items = self._resolve_template(source_path, context)

        if not isinstance(items, list):
            raise ValueError(f"Fan-out source must be a list, got {type(items)}")

        # Create a sub-node for each item
        for i, item in enumerate(items):
            sub_node_id = f"{node_id}_{i}"
            sub_context = {**context, 'item': item, 'index': i}

            task_def = node_def['task']
            handler = task_def['handler']
            queue = task_def.get('queue', 'container-tasks')
            params = self._resolve_params(task_def.get('params', {}), sub_context)

            task_id = await self.dispatcher.dispatch(
                instance_id=instance_id,
                node_id=sub_node_id,
                handler=handler,
                queue=queue,
                params=params
            )

            # Track the sub-node
            await self.instance_manager.create_dynamic_node(
                instance_id=instance_id,
                node_id=sub_node_id,
                parent_node_id=node_id,
                task_id=task_id
            )

        # Mark fan-out node as completed (it just created the sub-nodes)
        await self.instance_manager.mark_node_completed(
            instance_id=instance_id,
            node_id=node_id,
            output={'fan_out_count': len(items)}
        )

    async def _check_timeouts(self):
        """Check for tasks that have exceeded their timeout."""

        timed_out = await self.db.fetch("""
            SELECT ns.instance_id, ns.node_id, ns.task_id, ns.dispatched_at,
                   w.definition->'nodes'->ns.node_id->>'timeout_seconds' as timeout
            FROM app.dag_node_states ns
            JOIN app.dag_instances di ON ns.instance_id = di.instance_id
            JOIN app.dag_workflows w ON di.workflow_id = w.workflow_id
            WHERE ns.status IN ('dispatched', 'running')
            AND ns.dispatched_at < NOW() - INTERVAL '1 second' *
                COALESCE((w.definition->'nodes'->ns.node_id->>'timeout_seconds')::int, 3600)
        """)

        for node in timed_out:
            logger.warning(f"Node timeout: {node['instance_id']}/{node['node_id']}")
            await self.instance_manager.mark_node_failed(
                instance_id=node['instance_id'],
                node_id=node['node_id'],
                error=f"Task timed out after {node['timeout']} seconds"
            )

    def _resolve_params(self, params: dict, context: dict) -> dict:
        """Resolve template expressions in parameters."""
        resolved = {}
        for key, value in params.items():
            if isinstance(value, str):
                resolved[key] = self._resolve_template(value, context)
            elif isinstance(value, dict):
                resolved[key] = self._resolve_params(value, context)
            else:
                resolved[key] = value
        return resolved

    def _resolve_template(self, template: str, context: dict):
        """Resolve {{ }} template expressions."""
        if not isinstance(template, str):
            return template
        if not template.startswith('{{') or not template.endswith('}}'):
            return template

        path = template[2:-2].strip()
        parts = path.split('.')

        value = context
        for part in parts:
            if isinstance(value, dict):
                value = value.get(part)
            else:
                return None

        return value
```

---

## Integration with Workers

### Worker Changes

Workers become dramatically simpler. They no longer contain CoreMachine or orchestration logic.

**Before (Epoch 4):**
```python
# Worker had CoreMachine, decided what to do next
class TaskHandler:
    def __init__(self):
        self.core_machine = CoreMachine()

    async def handle(self, task):
        result = await self.execute(task)

        # Worker decides what happens next!
        await self.core_machine.complete_task(task.id, result)
        # CoreMachine checks "am I last?", advances stages, etc.
```

**After (Epoch 5):**
```python
# Worker is dumb - just execute and report
class TaskHandler:
    def __init__(self, orchestrator_url: str):
        self.orchestrator_url = orchestrator_url

    async def handle(self, message):
        task = parse_task_message(message)

        try:
            result = await self.execute(task)
            await self.report_completion(task, 'completed', result)
        except Exception as e:
            await self.report_completion(task, 'failed', error=str(e))

    async def report_completion(self, task, status, result=None, error=None):
        """Report to orchestrator - that's it, we're done."""
        async with aiohttp.ClientSession() as session:
            await session.post(
                f"{self.orchestrator_url}/api/task/complete",
                json={
                    'task_id': task.task_id,
                    'instance_id': task.instance_id,
                    'node_id': task.node_id,
                    'status': status,
                    'output': result,
                    'error': error,
                    'worker_id': self.worker_id
                }
            )
        # OR: Write directly to dag_task_results table
```

### Task Message Format

```json
{
    "task_id": "task-abc123",
    "instance_id": "job-xyz789",
    "node_id": "create_cog",
    "handler": "raster_cog_docker",
    "params": {
        "source_url": "https://storage.../input.tif",
        "collection_id": "my-collection",
        "use_mount": true
    },
    "timeout_seconds": 7200,
    "dispatched_at": "2026-01-27T12:00:00Z"
}
```

### Completion Report Format

```json
{
    "task_id": "task-abc123",
    "instance_id": "job-xyz789",
    "node_id": "create_cog",
    "status": "completed",
    "output": {
        "cog_url": "https://storage.../output.tif",
        "cog_size_mb": 450,
        "metadata": { ... }
    },
    "worker_id": "docker-worker-1",
    "duration_seconds": 245
}
```

### Queue Routing

The workflow definition specifies which queue each task type uses:

```yaml
nodes:
  validate:
    handler: raster_validate
    queue: functionapp-tasks    # Lightweight - Function App

  create_cog:
    handler: raster_cog_docker
    queue: container-tasks      # Heavy - Docker worker
```

The orchestrator dispatches to the specified queue. Workers listen to their respective queues.

---

## Implementation Phases

### Phase 0: Spike (1 week)

**Goal**: Prove the concept works

| Task | Description |
|------|-------------|
| Minimal orchestrator | FastAPI app with main loop |
| Hardcoded workflow | One workflow, in Python dict |
| SQLite state | No PostgreSQL yet |
| Local queues | Python queues, not Service Bus |
| One handler | hello_world only |

**Deliverable**: Can run `validate → hello_world → done` locally.

### Phase 1: Foundation (2 weeks)

**Goal**: Production-ready orchestrator skeleton

| Task | Description |
|------|-------------|
| Docker Web App | Deploy orchestrator to Azure |
| PostgreSQL schema | `dag_workflows`, `dag_instances`, `dag_node_states` |
| YAML loader | Parse workflow definitions |
| Service Bus dispatch | Real queue integration |
| Health endpoint | `/health` for monitoring |

**Deliverable**: Orchestrator running in Azure, can dispatch to existing queues.

### Phase 2: Worker Integration (2 weeks)

**Goal**: Workers report to orchestrator

| Task | Description |
|------|-------------|
| Completion endpoint | `POST /api/task/complete` |
| Worker modifications | Remove CoreMachine, add reporting |
| Result storage | `dag_task_results` table |
| Template resolution | `{{ inputs.x }}`, `{{ nodes.y.output.z }}` |

**Deliverable**: End-to-end flow works: submit → orchestrate → execute → complete.

### Phase 3: Migration (2-3 weeks)

**Goal**: Convert existing workflows

| Task | Description |
|------|-------------|
| `process_raster_docker` | Full workflow with conditional routing |
| `vector_docker_etl` | Linear workflow |
| Fan-out support | Tiled raster workflow |
| Parallel execution | Verify multiple tasks run concurrently |

**Deliverable**: Existing workloads running on DAG orchestrator.

### Phase 4: Cutover (1 week)

**Goal**: DAG is primary, old system deprecated

| Task | Description |
|------|-------------|
| Disable old CoreMachine | Remove "last task" logic |
| Remove janitor workarounds | No longer needed |
| Documentation | Update all docs |
| Monitoring | Alerts for orchestrator health |

**Deliverable**: Epoch 5 is live.

### Future Phases

| Feature | Description |
|---------|-------------|
| Visual DAG editor | Web UI to build workflows |
| Sub-workflows | DAG node that runs another DAG |
| Human approval nodes | Pause for manual approval |
| SLA monitoring | Alert if workflow exceeds expected duration |
| Workflow versioning | Run old version while new version in testing |

---

## Summary

### Epoch 5 Core Principles

1. **Separation of concerns**: Orchestrator orchestrates, workers work
2. **Single source of truth**: Orchestrator owns all state
3. **Declarative workflows**: YAML, not Python classes
4. **Explicit dependencies**: DAG edges, not hidden in code
5. **No "last task" games**: One decision-maker, no races

### What We're Building

| Component | Technology | Purpose |
|-----------|------------|---------|
| DAG Orchestrator | Docker Web App (always-on) | Central brain |
| Workflow definitions | YAML files | Declarative workflows |
| State storage | PostgreSQL | Durable state |
| Task dispatch | Service Bus | Queue routing |
| Workers | Function App + Docker | Dumb executors |

### What We're Eliminating

| Epoch 4 Pattern | Why It's Problematic | Epoch 5 Replacement |
|-----------------|----------------------|---------------------|
| "Last task turns out lights" | Race conditions, fragile | Single orchestrator decides |
| Janitor service | Band-aid for detection failures | Orchestrator monitors actively |
| CoreMachine in workers | Distributed decisions | Centralized decisions |
| Linear stages | Can't express complex deps | DAG with any dependencies |
| Hidden conditionals | Hard to understand/modify | Explicit in workflow YAML |

---

## Code Reuse Assessment

### What Can Be Reused?

**Short answer**: ~70-80% of code stays. Handlers and infrastructure are reusable. Core orchestration logic (CoreMachine, stages, "last task") gets replaced.

```
┌─────────────────────────────────────────────────────────────────┐
│                    CODE REUSE BREAKDOWN                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   KEEP AS-IS (70-80%)              REWRITE/REPLACE (20-30%)     │
│   ─────────────────────            ────────────────────────      │
│                                                                  │
│   ✅ All handlers                  ❌ CoreMachine                │
│   ✅ Service Bus clients           ❌ Job class definitions      │
│   ✅ PostgreSQL connections        ❌ JobBaseMixin               │
│   ✅ Storage clients               ❌ Stage advancement logic    │
│   ✅ STAC integration              ❌ "Last task" SQL            │
│   ✅ Docker worker infra           ❌ Task creation in workers   │
│   ✅ Function App infra                                          │
│   ✅ All business logic                                          │
│   ✅ Existing tables (evolve)      ➕ NEW: Orchestrator service  │
│                                    ➕ NEW: DAG tables            │
│                                    ➕ NEW: YAML definitions      │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Detailed Inventory

#### KEEP: Handlers (The Bulk of Your Code)

These are **pure execution logic** - they don't care who called them:

```python
# services/handlers/ - ALL OF THIS STAYS
handler_process_raster_complete.py   # COG creation logic ✅
handler_vector_docker_complete.py    # Vector ETL logic ✅
handler_raster_validate.py           # Validation logic ✅
handler_stac_*.py                    # STAC registration ✅

# The handler does work and returns a result
# It doesn't know or care about DAGs, stages, or orchestration
```

**Change needed**: Instead of calling `CoreMachine.complete_task()`, handlers return result to caller. That's ~5 lines per handler.

#### KEEP: Infrastructure

```python
# All of this stays exactly as-is
services/storage/          # Azure Blob clients ✅
services/database/         # PostgreSQL connections ✅
services/stac/             # STAC catalog operations ✅
services/queue/            # Service Bus send/receive ✅
config/                    # Configuration ✅
util_logger.py             # Logging ✅
```

#### KEEP: Docker Worker + Function App Structure

```python
# Docker worker - keeps listening to queue
docker_service.py          # Queue listener loop ✅
                          # Just change what happens after task completes

# Function App - keeps HTTP endpoints
triggers/                  # HTTP triggers stay ✅
                          # Submit goes to orchestrator instead of CoreMachine
```

#### REPLACE: CoreMachine + Job Definitions

This is the **~20-30% that changes**:

```python
# GOES AWAY or MAJOR REWRITE
core/machine.py            # CoreMachine class ❌
core/jobs/*.py             # Job class definitions ❌
jobs/base.py               # JobBase ❌
jobs/mixins.py             # JobBaseMixin ❌

# This pattern disappears:
class ProcessRasterDocker(JobBaseMixin, JobBase):
    stages = [
        {"number": 1, "name": "validate", ...},
        {"number": 2, "name": "create_cog", ...},
    ]
```

**Replaced by**: YAML workflow definitions + DAG evaluator

#### NEW: Orchestrator Service

```python
# NEW Web App (can be small, ~1000-2000 lines total)
dag_orchestrator/
├── main.py              # FastAPI app + main loop
├── evaluator.py         # DAG dependency logic
├── loader.py            # YAML parser
├── dispatcher.py        # Send to Service Bus
└── models.py            # Pydantic models

# NEW: Workflow definitions
workflows/
├── raster_processing.yaml
├── vector_etl.yaml
└── raster_tiled.yaml
```

#### EVOLVE: Database Schema

```sql
-- KEEP existing tables (jobs, tasks still useful for history)
-- ADD new tables alongside

-- NEW
CREATE TABLE app.dag_workflows (...);
CREATE TABLE app.dag_instances (...);
CREATE TABLE app.dag_node_states (...);
CREATE TABLE app.dag_task_results (...);

-- EXISTING (keep for backward compat during migration)
app.jobs      -- Can map to dag_instances
app.tasks     -- Can map to dag_node_states
```

### What Changes in Workers

#### Before (Current)

```python
# docker_service.py - current pattern
async def handle_task(message):
    task = parse_message(message)
    handler = get_handler(task.task_type)

    # Execute
    result = await handler.execute(task.parameters)

    # Worker does orchestration! ❌
    await core_machine.complete_task(
        task_id=task.task_id,
        result=result
    )
    # CoreMachine checks "am I last?", advances stage, etc.
```

#### After (Epoch 5)

```python
# docker_service.py - new pattern
async def handle_task(message):
    task = parse_message(message)
    handler = get_handler(task.handler)  # Note: 'handler' not 'task_type'

    # Execute (SAME AS BEFORE)
    result = await handler.execute(task.params)

    # Just report completion ✅
    await report_to_orchestrator(
        task_id=task.task_id,
        instance_id=task.instance_id,
        node_id=task.node_id,
        status='completed',
        output=result
    )
    # That's it. Orchestrator decides what's next.


async def report_to_orchestrator(task_id, instance_id, node_id, status, output):
    """Write to results table (orchestrator polls this)."""
    await db.execute("""
        INSERT INTO app.dag_task_results
        (task_id, instance_id, node_id, status, output, reported_at)
        VALUES ($1, $2, $3, $4, $5, NOW())
    """, task_id, instance_id, node_id, status, json.dumps(output))
```

**Net change to docker_service.py**: ~50 lines modified, remove CoreMachine import.

#### What Changes in Handlers

**Before and After**: Essentially unchanged. Handlers already return results.

```python
# handler_process_raster_complete.py - NO CHANGES NEEDED
class ProcessRasterHandler:
    async def execute(self, params):
        # ... 500 lines of COG creation logic ... (UNCHANGED)

        return {
            "cog_url": cog_url,
            "metadata": metadata
        }
        # Handler just returns result - same as before
```

### Lines of Code Estimate

| Category | Lines of Code (Estimate) | What Happens |
|----------|-------------------------|--------------|
| **Handlers** | ~5,000 | Keep 95%, minor changes |
| **Infrastructure** | ~3,000 | Keep 100% |
| **CoreMachine + Jobs** | ~2,000 | Replace entirely |
| **New Orchestrator** | ~1,500 | New code |
| **Worker changes** | ~100 | Small modifications |
| **Triggers/API** | ~1,000 | Minor changes to submit |

**It's not a complete rewrite.** The handlers are the valuable part and they're untouched. The orchestration layer (CoreMachine) is what we're replacing.

---

## High-Level Project Plan

### Project Structure: Parallel Development

The Epoch 5 orchestrator will be developed as a **separate project** that can run alongside the existing system (rmhgeoapi). This allows:

1. Continued development/bugfixes on Epoch 4 (rmhgeoapi)
2. Independent development of Epoch 5 orchestrator
3. Gradual migration without big-bang cutover
4. Easy rollback if issues discovered

```
┌─────────────────────────────────────────────────────────────────┐
│                 PARALLEL PROJECT STRUCTURE                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   EXISTING: rmhgeoapi/                                          │
│   ────────────────────                                          │
│   - Function App (keeps running)                                 │
│   - Docker Worker (keeps running)                                │
│   - CoreMachine (legacy, eventually deprecated)                  │
│   - All handlers (SHARED - copied or imported)                   │
│                                                                  │
│   NEW: rmhgeo-orchestrator/ (or epoch5-orchestrator/)           │
│   ─────────────────────────────────────────────────             │
│   - DAG Orchestrator Web App                                     │
│   - Workflow YAML definitions                                    │
│   - DAG evaluator, dispatcher, etc.                              │
│   - Shares database with rmhgeoapi                               │
│   - Shares Service Bus with rmhgeoapi                            │
│                                                                  │
│   RUNTIME:                                                       │
│   ────────                                                       │
│   ┌─────────────┐  ┌─────────────┐  ┌─────────────┐             │
│   │  Function   │  │   Docker    │  │    DAG      │             │
│   │    App      │  │   Worker    │  │Orchestrator │             │
│   │ (rmhgeoapi) │  │ (rmhgeoapi) │  │   (NEW)     │             │
│   └──────┬──────┘  └──────┬──────┘  └──────┬──────┘             │
│          │                │                │                     │
│          └────────────────┼────────────────┘                     │
│                           │                                      │
│                           ▼                                      │
│               ┌───────────────────────┐                         │
│               │   Shared PostgreSQL   │                         │
│               │   Shared Service Bus  │                         │
│               └───────────────────────┘                         │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Phase 0: Project Setup (Week 1)

**Goal**: New project scaffolded, can deploy to Azure

| Task | Description | Deliverable |
|------|-------------|-------------|
| Create new repo | `rmhgeo-orchestrator` or similar | Empty repo with README |
| Project structure | FastAPI app scaffold | `main.py`, `requirements.txt`, `Dockerfile` |
| Local dev setup | Docker Compose for local testing | Can run orchestrator locally |
| Azure Web App | Create `rmhgeo-orchestrator` Web App in ASE | Deployed, shows health endpoint |
| Database access | Connect to existing PostgreSQL | Can query existing tables |
| Service Bus access | Connect to existing namespace | Can send messages |

**Deliverable**: Empty orchestrator running in Azure, can connect to shared resources.

### Phase 1: Core Engine (Weeks 2-3)

**Goal**: DAG evaluator works, can run simple workflow

| Task | Description | Deliverable |
|------|-------------|-------------|
| Database schema | Create `dag_*` tables | Schema deployed |
| Workflow loader | Parse YAML definitions | Can load workflow files |
| DAG evaluator | Dependency resolution logic | Unit tests pass |
| Instance manager | Create/track DAG instances | Can create instance in DB |
| Main loop | Poll for work, evaluate, dispatch | Loop runs without crashing |
| Hello World workflow | Simplest possible workflow | `START → hello → END` |

**Deliverable**: Can submit a workflow, orchestrator runs it, completes.

### Phase 2: Worker Integration (Weeks 4-5)

**Goal**: Existing workers can execute DAG tasks

| Task | Description | Deliverable |
|------|-------------|-------------|
| Task message format | Define DAG task structure | JSON schema documented |
| Completion reporting | Workers write to `dag_task_results` | Results appear in DB |
| Worker modifications | Fork or modify docker_service.py | Workers handle DAG tasks |
| Dual-mode workers | Support both legacy and DAG tasks | No breaking changes |
| End-to-end test | Submit → Orchestrate → Execute → Complete | Full cycle works |

**Deliverable**: Real tasks execute on existing workers, orchestrated by new system.

### Phase 3: Real Workflows (Weeks 6-8)

**Goal**: Production workflows running on DAG

| Task | Description | Deliverable |
|------|-------------|-------------|
| Raster workflow YAML | Port `process_raster_docker` | Conditional routing works |
| Vector workflow YAML | Port `vector_docker_etl` | Linear workflow works |
| Fan-out support | Tiled raster with N tiles | Dynamic task creation works |
| Template resolution | `{{ inputs.x }}` syntax | Parameters flow correctly |
| Error handling | Failed tasks, retries | Failures handled gracefully |
| Timeout detection | Stuck task detection | No infinite hangs |

**Deliverable**: Can process real rasters and vectors via DAG orchestrator.

### Phase 4: API Integration (Weeks 9-10)

**Goal**: External API uses DAG orchestrator

| Task | Description | Deliverable |
|------|-------------|-------------|
| Submit endpoint | `POST /api/dag/submit` | Can submit via HTTP |
| Status endpoint | `GET /api/dag/status/{id}` | Can check progress |
| Platform integration | `/api/platform/submit` routes to DAG | DDH workflow works |
| Backward compat | Legacy endpoints still work | No breaking changes |

**Deliverable**: DDH can use new orchestrator, old endpoints still work.

### Phase 5: Migration & Cutover (Weeks 11-12)

**Goal**: DAG is primary, legacy deprecated

| Task | Description | Deliverable |
|------|-------------|-------------|
| Traffic migration | Route all new jobs to DAG | All jobs use DAG |
| Legacy deprecation | Mark CoreMachine deprecated | Warning logs |
| Monitoring | Alerts for orchestrator health | PagerDuty/alerts configured |
| Documentation | Update all docs | CLAUDE.md, README updated |
| Cleanup | Remove legacy code (optional) | Cleaner codebase |

**Deliverable**: Epoch 5 is production system.

### Future Phases (Post-Launch)

| Feature | Description | Priority |
|---------|-------------|----------|
| Web UI | Visualize DAGs, view runs | HIGH |
| Sub-workflows | DAG node runs another DAG | MEDIUM |
| Human approval | Pause for manual approval | MEDIUM |
| SLA monitoring | Alert on slow workflows | MEDIUM |
| Workflow versioning | Run old version during testing | LOW |
| Visual editor | Build workflows in browser | LOW |

---

## New Project Structure

```
rmhgeo-orchestrator/
├── README.md
├── CLAUDE.md                    # Project context for Claude
├── requirements.txt
├── Dockerfile
├── docker-compose.yml           # Local dev with DB, Service Bus emulator
│
├── orchestrator/
│   ├── __init__.py
│   ├── main.py                  # FastAPI app entry point
│   ├── config.py                # Configuration (reuse patterns from rmhgeoapi)
│   │
│   ├── api/
│   │   ├── __init__.py
│   │   ├── submit.py            # POST /api/dag/submit
│   │   ├── status.py            # GET /api/dag/status/{id}
│   │   └── health.py            # GET /health
│   │
│   ├── engine/
│   │   ├── __init__.py
│   │   ├── evaluator.py         # DAG dependency resolution
│   │   ├── dispatcher.py        # Send tasks to Service Bus
│   │   ├── loader.py            # Load YAML workflows
│   │   └── loop.py              # Main orchestrator loop
│   │
│   ├── models/
│   │   ├── __init__.py
│   │   ├── workflow.py          # Workflow definition models
│   │   ├── instance.py          # DAG instance models
│   │   └── node.py              # Node state models
│   │
│   └── db/
│       ├── __init__.py
│       ├── connection.py        # PostgreSQL connection
│       ├── schema.py            # Table definitions
│       └── queries.py           # SQL queries
│
├── workflows/                   # YAML workflow definitions
│   ├── hello_world.yaml
│   ├── raster_processing.yaml
│   ├── raster_tiled.yaml
│   └── vector_etl.yaml
│
├── tests/
│   ├── test_evaluator.py
│   ├── test_loader.py
│   └── test_integration.py
│
└── scripts/
    ├── deploy.sh                # Deploy to Azure
    └── migrate_schema.py        # Create DAG tables
```

---

## Key Design Decisions

### Decision 1: Separate Project vs Monorepo

**Decision**: Separate project (new repo)

**Rationale**:
- Clean separation of concerns
- Independent deployment cycles
- Can evolve independently
- Easy to reason about
- Can share code via packages if needed later

### Decision 2: Shared vs Separate Database

**Decision**: Shared database (same PostgreSQL)

**Rationale**:
- Single source of truth
- Can reference existing tables (collections, etc.)
- No data sync issues
- New tables in same `app` schema (or new `dag` schema)

### Decision 3: Workers: Fork vs Modify

**Decision**: Modify existing workers to support dual-mode

**Rationale**:
- Avoid code duplication
- Handlers are reusable as-is
- Workers detect task type and route accordingly
- Gradual migration possible

### Decision 4: Orchestrator Deployment

**Decision**: Docker Web App in existing ASE

**Rationale**:
- Consistent with existing infrastructure
- No new VM management
- Uses existing networking, security
- Can scale via App Service plan

---

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Orchestrator becomes bottleneck | LOW | HIGH | Design for horizontal scale from start |
| Database contention | LOW | MEDIUM | Use separate tables, proper indexes |
| Migration breaks existing jobs | MEDIUM | HIGH | Dual-mode workers, gradual migration |
| Complexity underestimated | MEDIUM | MEDIUM | Start with simplest workflow, iterate |
| Scope creep | HIGH | MEDIUM | Strict MVP definition, future phases |

---

## Success Criteria

### MVP (Phase 3 Complete)

- [ ] Orchestrator runs in Azure Web App
- [ ] Can submit raster workflow via API
- [ ] Conditional routing works (size-based)
- [ ] Fan-out works (tiled rasters)
- [ ] Failures detected within 60 seconds (not 24 hours)
- [ ] No "last task turns out lights" pattern
- [ ] Existing workers process DAG tasks

### Production Ready (Phase 5 Complete)

- [ ] All workflows migrated to DAG
- [ ] DDH integration works
- [ ] Monitoring and alerting configured
- [ ] Documentation complete
- [ ] Legacy CoreMachine deprecated
- [ ] Team trained on new system

---

## References

- `docs_claude/DAG_SPIKE.md` - Foundational concepts
- [Temporal.io - How it works](https://temporal.io/how-it-works)
- [Airflow Architecture](https://airflow.apache.org/docs/apache-airflow/stable/concepts/overview.html)
- [AWS Step Functions](https://docs.aws.amazon.com/step-functions/latest/dg/welcome.html)

---

*Document created: 27 JAN 2026*
*Revised: 27 JAN 2026 - Reframed as Epoch transition, added dedicated orchestrator as primary architecture, honest assessment of "last task turns out lights" problems*
*Revised: 27 JAN 2026 - Added code reuse assessment and high-level project plan for parallel development*
*Revised: 28 JAN 2026 - Added "Illustration: Why DAGs Matter Even for Linear Workflows" section with process_raster example, emphasizing granular retries (critical for FATHOM 8TB) and extensibility*
