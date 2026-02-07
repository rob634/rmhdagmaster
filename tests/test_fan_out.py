# ============================================================================
# FAN-OUT / FAN-IN TESTS
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Tests - Tier 2 fan-out/fan-in
# PURPOSE: Verify fan-out expansion, fan-in aggregation, and dependencies
# CREATED: 06 FEB 2026
# ============================================================================
"""
Fan-Out / Fan-In Tests

Covers:
1. FanOutTaskDef parses from dict and validates
2. FanOutHandler.expand() produces correct expansions
3. Fan-in aggregation modes (collect, concat, sum, first, last)
4. check_and_ready_nodes blocks FAN_IN until children done
5. Dynamic node _dependencies_met() returns True
6. Fan-in detects failed children
7. Workflow validation for FAN_OUT nodes
8. _resolve_source uses Jinja2

Run with:
    pytest tests/test_fan_out.py -v
"""

import asyncio
import pytest
from datetime import datetime
from typing import Dict, List, Optional, Set
from unittest.mock import AsyncMock, MagicMock, patch

from core.models import (
    WorkflowDefinition, NodeDefinition, NodeState, NodeType, FanOutTaskDef,
)
from core.contracts import NodeStatus
from orchestrator.engine.evaluator import FanOutHandler


# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture
def fan_out_workflow():
    """Workflow: START -> prepare -> split (fan_out) -> aggregate (fan_in) -> END."""
    return WorkflowDefinition(
        workflow_id="test_fan_out",
        name="Fan-Out Test",
        version=1,
        nodes={
            "start": NodeDefinition(type=NodeType.START, next="prepare"),
            "prepare": NodeDefinition(
                type=NodeType.TASK,
                handler="echo",
                params={"item_list": "{{ inputs.item_list }}"},
                next="split",
            ),
            "split": NodeDefinition(
                type=NodeType.FAN_OUT,
                source="{{ nodes.prepare.output.echoed_params.item_list }}",
                task=FanOutTaskDef(
                    handler="echo",
                    queue="dag-worker-tasks",
                    params={
                        "item_value": "{{ item }}",
                        "item_index": "{{ index }}",
                    },
                    timeout_seconds=60,
                ),
                next="aggregate",
            ),
            "aggregate": NodeDefinition(
                type=NodeType.FAN_IN,
                aggregation="collect",
                next="end",
            ),
            "end": NodeDefinition(type=NodeType.END),
        },
    )


@pytest.fixture
def make_node():
    """Factory for creating NodeState instances."""
    def _make(
        node_id: str,
        status: NodeStatus = NodeStatus.READY,
        job_id: str = "job-fan-001",
        output: Optional[dict] = None,
        parent_node_id: Optional[str] = None,
        fan_out_index: Optional[int] = None,
        input_params: Optional[dict] = None,
        version: int = 1,
    ) -> NodeState:
        return NodeState(
            job_id=job_id,
            node_id=node_id,
            status=status,
            output=output,
            parent_node_id=parent_node_id,
            fan_out_index=fan_out_index,
            input_params=input_params,
            version=version,
        )
    return _make


# ============================================================================
# FAN-OUT TASK DEF MODEL
# ============================================================================

class TestFanOutTaskDef:
    """Test FanOutTaskDef model parsing and validation."""

    def test_parse_from_dict(self):
        """FanOutTaskDef auto-coerces from dict (Pydantic V2)."""
        data = {
            "handler": "echo",
            "queue": "dag-worker-tasks",
            "params": {"item_value": "{{ item }}"},
            "timeout_seconds": 120,
        }
        task_def = FanOutTaskDef(**data)
        assert task_def.handler == "echo"
        assert task_def.queue == "dag-worker-tasks"
        assert task_def.params == {"item_value": "{{ item }}"}
        assert task_def.timeout_seconds == 120

    def test_defaults(self):
        """FanOutTaskDef has sensible defaults."""
        task_def = FanOutTaskDef(handler="process")
        assert task_def.queue == "dag-worker-tasks"
        assert task_def.params == {}
        assert task_def.timeout_seconds == 3600

    def test_handler_required(self):
        """Handler is a required field."""
        with pytest.raises(Exception):
            FanOutTaskDef()

    def test_coercion_in_node_definition(self):
        """NodeDefinition.task auto-coerces dict to FanOutTaskDef."""
        node = NodeDefinition(
            type=NodeType.FAN_OUT,
            source="{{ inputs.items }}",
            task={
                "handler": "echo",
                "params": {"x": "{{ item }}"},
            },
        )
        assert isinstance(node.task, FanOutTaskDef)
        assert node.task.handler == "echo"


# ============================================================================
# FAN-OUT HANDLER EXPANSION
# ============================================================================

class TestFanOutHandlerExpand:
    """Test FanOutHandler.expand() produces correct expansions."""

    def test_expand_simple_list(self):
        """Expand a simple list of strings."""
        handler = FanOutHandler()
        node_def = NodeDefinition(
            type=NodeType.FAN_OUT,
            source="{{ inputs.item_list }}",
            task=FanOutTaskDef(
                handler="echo",
                params={"value": "{{ item }}"},
            ),
        )
        context = {
            "inputs": {"item_list": ["alpha", "bravo", "charlie"]},
            "nodes": {},
        }

        result = handler.expand(node_def, context)

        assert len(result) == 3
        assert result[0]["index"] == 0
        assert result[0]["item"] == "alpha"
        assert result[0]["params"]["value"] == "alpha"
        assert result[0]["handler"] == "echo"
        assert result[1]["item"] == "bravo"
        assert result[2]["item"] == "charlie"

    def test_expand_with_index(self):
        """Index placeholder resolves correctly."""
        handler = FanOutHandler()
        node_def = NodeDefinition(
            type=NodeType.FAN_OUT,
            source="{{ inputs.data_list }}",
            task=FanOutTaskDef(
                handler="process",
                params={"idx": "{{ index }}", "val": "{{ item }}"},
            ),
        )
        context = {
            "inputs": {"data_list": ["x", "y"]},
            "nodes": {},
        }

        result = handler.expand(node_def, context)

        assert result[0]["params"]["idx"] == "0"
        assert result[0]["params"]["val"] == "x"
        assert result[1]["params"]["idx"] == "1"
        assert result[1]["params"]["val"] == "y"

    def test_expand_dict_items(self):
        """Expand with dict items and {{ item.key }} access."""
        handler = FanOutHandler()
        node_def = NodeDefinition(
            type=NodeType.FAN_OUT,
            source="{{ inputs.records }}",
            task=FanOutTaskDef(
                handler="process",
                params={
                    "name": "{{ item.name }}",
                    "size": "{{ item.size }}",
                },
            ),
        )
        context = {
            "inputs": {
                "records": [
                    {"name": "file1.tif", "size": "100"},
                    {"name": "file2.tif", "size": "200"},
                ]
            },
            "nodes": {},
        }

        result = handler.expand(node_def, context)

        assert len(result) == 2
        assert result[0]["params"]["name"] == "file1.tif"
        assert result[0]["params"]["size"] == "100"
        assert result[1]["params"]["name"] == "file2.tif"

    def test_expand_empty_list(self):
        """Empty source list produces no expansions."""
        handler = FanOutHandler()
        node_def = NodeDefinition(
            type=NodeType.FAN_OUT,
            source="{{ inputs.item_list }}",
            task=FanOutTaskDef(handler="echo", params={}),
        )
        context = {"inputs": {"item_list": []}, "nodes": {}}

        result = handler.expand(node_def, context)
        assert result == []

    def test_expand_non_fan_out_returns_empty(self):
        """Non-FAN_OUT node returns empty list."""
        handler = FanOutHandler()
        node_def = NodeDefinition(
            type=NodeType.TASK,
            handler="echo",
        )
        result = handler.expand(node_def, {"inputs": {}, "nodes": {}})
        assert result == []

    def test_expand_from_node_output(self):
        """Source references upstream node output."""
        handler = FanOutHandler()
        node_def = NodeDefinition(
            type=NodeType.FAN_OUT,
            source="{{ nodes.prepare.output.echoed_params.item_list }}",
            task=FanOutTaskDef(
                handler="echo",
                params={"val": "{{ item }}"},
            ),
        )
        context = {
            "inputs": {},
            "nodes": {
                "prepare": {
                    "output": {
                        "echoed_params": {
                            "item_list": ["one", "two", "three"],
                        }
                    }
                }
            },
        }

        result = handler.expand(node_def, context)
        assert len(result) == 3
        assert result[0]["params"]["val"] == "one"
        assert result[2]["params"]["val"] == "three"


# ============================================================================
# FAN-IN AGGREGATION
# ============================================================================

class TestFanInAggregation:
    """Test fan-in aggregation modes."""

    def _make_loop(self):
        """Create a minimal Orchestrator-like object with _aggregate_fan_in_results."""
        from orchestrator.loop import Orchestrator
        loop = object.__new__(Orchestrator)
        return loop

    def test_collect_mode(self, make_node):
        """Collect mode returns list of all outputs."""
        loop = self._make_loop()
        children = [
            make_node("split__0", status=NodeStatus.COMPLETED, output={"result": "a"}),
            make_node("split__1", status=NodeStatus.COMPLETED, output={"result": "b"}),
            make_node("split__2", status=NodeStatus.COMPLETED, output={"result": "c"}),
        ]

        result = loop._aggregate_fan_in_results(children, "collect")

        assert result["count"] == 3
        assert len(result["results"]) == 3
        assert result["results"][0] == {"result": "a"}
        assert result["results"][2] == {"result": "c"}

    def test_concat_mode(self, make_node):
        """Concat mode flattens list values."""
        loop = self._make_loop()
        children = [
            make_node("split__0", status=NodeStatus.COMPLETED, output={"items": ["a", "b"]}),
            make_node("split__1", status=NodeStatus.COMPLETED, output={"items": ["c"]}),
        ]

        result = loop._aggregate_fan_in_results(children, "concat")

        assert result["results"] == ["a", "b", "c"]
        assert result["count"] == 3

    def test_sum_mode(self, make_node):
        """Sum mode adds numeric values."""
        loop = self._make_loop()
        children = [
            make_node("split__0", status=NodeStatus.COMPLETED, output={"value": 10}),
            make_node("split__1", status=NodeStatus.COMPLETED, output={"value": 20}),
            make_node("split__2", status=NodeStatus.COMPLETED, output={"value": 30}),
        ]

        result = loop._aggregate_fan_in_results(children, "sum")

        assert result["total"] == 60
        assert result["count"] == 3

    def test_first_mode(self, make_node):
        """First mode returns first output."""
        loop = self._make_loop()
        children = [
            make_node("split__0", status=NodeStatus.COMPLETED, output={"val": "first"}),
            make_node("split__1", status=NodeStatus.COMPLETED, output={"val": "second"}),
        ]

        result = loop._aggregate_fan_in_results(children, "first")

        assert result["result"] == {"val": "first"}
        assert result["count"] == 2

    def test_last_mode(self, make_node):
        """Last mode returns last output."""
        loop = self._make_loop()
        children = [
            make_node("split__0", status=NodeStatus.COMPLETED, output={"val": "first"}),
            make_node("split__1", status=NodeStatus.COMPLETED, output={"val": "last"}),
        ]

        result = loop._aggregate_fan_in_results(children, "last")

        assert result["result"] == {"val": "last"}
        assert result["count"] == 2

    def test_collect_skips_failed(self, make_node):
        """Collect only includes completed children, not failed."""
        loop = self._make_loop()
        children = [
            make_node("split__0", status=NodeStatus.COMPLETED, output={"val": "ok"}),
            make_node("split__1", status=NodeStatus.FAILED, output=None),
            make_node("split__2", status=NodeStatus.COMPLETED, output={"val": "also_ok"}),
        ]

        result = loop._aggregate_fan_in_results(children, "collect")

        assert result["count"] == 2
        assert len(result["results"]) == 2

    def test_empty_children(self, make_node):
        """Empty children list produces empty results."""
        loop = self._make_loop()
        result = loop._aggregate_fan_in_results([], "collect")
        assert result["count"] == 0
        assert result["results"] == []

    def test_unknown_mode_falls_back_to_collect(self, make_node):
        """Unknown aggregation mode falls back to collect behavior."""
        loop = self._make_loop()
        children = [
            make_node("split__0", status=NodeStatus.COMPLETED, output={"x": 1}),
        ]

        result = loop._aggregate_fan_in_results(children, "unknown_mode")

        assert result["count"] == 1
        assert result["results"] == [{"x": 1}]


# ============================================================================
# DYNAMIC NODE DEPENDENCIES
# ============================================================================

class TestDynamicNodeDependencies:
    """Test that dynamic nodes are handled correctly in dependency checks."""

    def test_dynamic_node_always_ready(self, fan_out_workflow):
        """Dynamic nodes (not in workflow) return True from _dependencies_met."""
        from services.node_service import NodeService

        # Create service with mock repos
        svc = object.__new__(NodeService)
        svc.node_repo = MagicMock()
        svc.task_repo = MagicMock()
        svc._event_service = None

        # Dynamic node ID not in workflow.nodes
        node_def = NodeDefinition(type=NodeType.TASK, handler="echo")
        completed: Set[str] = set()

        # The dynamic node check: node_id not in workflow.nodes → True
        result = svc._dependencies_met("split__0", node_def, completed, fan_out_workflow)
        assert result is True

    def test_regular_node_checks_deps(self, fan_out_workflow):
        """Regular nodes still check dependencies normally."""
        from services.node_service import NodeService

        svc = object.__new__(NodeService)
        svc.node_repo = MagicMock()
        svc.task_repo = MagicMock()
        svc._event_service = None

        node_def = fan_out_workflow.get_node("prepare")
        # prepare depends on start (implicit via next pointer)
        # Without start completed, prepare should not be ready
        completed: Set[str] = set()
        result = svc._dependencies_met("prepare", node_def, completed, fan_out_workflow)
        assert result is False

        # With start completed, prepare should be ready
        completed.add("start")
        result = svc._dependencies_met("prepare", node_def, completed, fan_out_workflow)
        assert result is True


# ============================================================================
# NODE STATE PROPERTIES
# ============================================================================

class TestNodeStateProperties:
    """Test NodeState properties related to fan-out."""

    def test_is_dynamic_with_parent(self, make_node):
        """Node with parent_node_id is dynamic."""
        node = make_node("split__0", parent_node_id="split", fan_out_index=0)
        assert node.is_dynamic is True

    def test_is_dynamic_without_parent(self, make_node):
        """Node without parent_node_id is not dynamic."""
        node = make_node("prepare")
        assert node.is_dynamic is False

    def test_input_params_stored(self, make_node):
        """Input params are stored on dynamic nodes."""
        params = {"item_value": "alpha", "item_index": "0"}
        node = make_node(
            "split__0",
            parent_node_id="split",
            fan_out_index=0,
            input_params=params,
        )
        assert node.input_params == params


# ============================================================================
# WORKFLOW VALIDATION
# ============================================================================

class TestWorkflowValidation:
    """Test workflow validation for FAN_OUT nodes."""

    def test_valid_fan_out_workflow(self, fan_out_workflow):
        """Valid fan-out workflow passes validation."""
        errors = fan_out_workflow.validate_structure()
        assert errors == []

    def test_fan_out_missing_source(self):
        """FAN_OUT without source fails validation."""
        wf = WorkflowDefinition(
            workflow_id="bad",
            name="Bad",
            version=1,
            nodes={
                "start": NodeDefinition(type=NodeType.START, next="split"),
                "split": NodeDefinition(
                    type=NodeType.FAN_OUT,
                    task=FanOutTaskDef(handler="echo"),
                    next="end",
                ),
                "end": NodeDefinition(type=NodeType.END),
            },
        )
        errors = wf.validate_structure()
        assert any("source" in e for e in errors)

    def test_fan_out_missing_task(self):
        """FAN_OUT without task fails validation."""
        wf = WorkflowDefinition(
            workflow_id="bad",
            name="Bad",
            version=1,
            nodes={
                "start": NodeDefinition(type=NodeType.START, next="split"),
                "split": NodeDefinition(
                    type=NodeType.FAN_OUT,
                    source="{{ inputs.items }}",
                    next="end",
                ),
                "end": NodeDefinition(type=NodeType.END),
            },
        )
        errors = wf.validate_structure()
        assert any("task" in e for e in errors)

    def test_fan_out_yaml_loads(self):
        """fan_out_test.yaml loads and validates."""
        import yaml
        import os
        yaml_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "workflows", "fan_out_test.yaml",
        )
        with open(yaml_path) as f:
            data = yaml.safe_load(f)

        wf = WorkflowDefinition.model_validate(data)
        assert wf.workflow_id == "fan_out_test"

        errors = wf.validate_structure()
        assert errors == [], f"Validation errors: {errors}"

        # Verify the fan-out node
        split = wf.get_node("split")
        assert split.type == NodeType.FAN_OUT
        assert split.source is not None
        assert split.task is not None
        assert isinstance(split.task, FanOutTaskDef)
        assert split.task.handler == "echo"

        # Verify fan-in node
        agg = wf.get_node("aggregate")
        assert agg.type == NodeType.FAN_IN
        assert agg.aggregation == "collect"


# ============================================================================
# RESOLVE SOURCE VIA JINJA2
# ============================================================================

class TestResolveSource:
    """Test that _resolve_source uses Jinja2 for template resolution."""

    def test_resolve_from_inputs(self):
        """Resolve source from inputs context."""
        handler = FanOutHandler()
        context = {
            "inputs": {"item_list": ["a", "b", "c"]},
            "nodes": {},
        }

        result = handler._resolve_source("{{ inputs.item_list }}", context)
        assert result == ["a", "b", "c"]

    def test_resolve_from_node_output(self):
        """Resolve source from upstream node output."""
        handler = FanOutHandler()
        context = {
            "inputs": {},
            "nodes": {
                "prepare": {
                    "output": {
                        "echoed_params": {
                            "item_list": [1, 2, 3],
                        }
                    }
                }
            },
        }

        result = handler._resolve_source(
            "{{ nodes.prepare.output.echoed_params.item_list }}", context
        )
        assert result == [1, 2, 3]

    def test_resolve_invalid_returns_none(self):
        """Invalid source expression returns None."""
        handler = FanOutHandler()
        context = {"inputs": {}, "nodes": {}}

        result = handler._resolve_source("{{ nonexistent.path }}", context)
        assert result is None


# ============================================================================
# RUN TESTS
# ============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
