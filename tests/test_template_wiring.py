# ============================================================================
# TEMPLATE WIRING TESTS
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Tests - Template resolution in dispatch path
# PURPOSE: Verify the Jinja2 engine is wired into publisher and loop
# CREATED: 06 FEB 2026
# ============================================================================
"""
Template Wiring Tests

Verifies that the proper Jinja2 template engine (orchestrator/engine/templates.py)
is wired into the task dispatch path (messaging/publisher.py).

Tests:
1. create_task_message resolves {{ inputs.x }} from job params
2. create_task_message resolves {{ nodes.X.output.Y }} from completed nodes
3. Missing node outputs raise TemplateResolutionError
4. Backward compat: callers without node_outputs still work
5. Nested template expressions resolve correctly

Run with:
    pytest tests/test_template_wiring.py -v
"""

import pytest
from unittest.mock import MagicMock

from core.models import NodeState, WorkflowDefinition, NodeDefinition, NodeType
from core.contracts import NodeStatus
from messaging.publisher import TaskPublisher
from orchestrator.engine.templates import TemplateResolutionError


# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture
def publisher():
    """Create a TaskPublisher with a mock config (no real Service Bus)."""
    config = MagicMock()
    return TaskPublisher(config)


@pytest.fixture
def simple_workflow():
    """Single-node workflow: START -> echo -> END."""
    return WorkflowDefinition(
        workflow_id="test_simple",
        name="Simple Test",
        version=1,
        nodes={
            "start": NodeDefinition(type=NodeType.START, next="echo"),
            "echo": NodeDefinition(
                type=NodeType.TASK,
                handler="echo",
                params={"message": "{{ inputs.message }}", "count": "{{ inputs.count }}"},
                next="end",
            ),
            "end": NodeDefinition(type=NodeType.END),
        },
    )


@pytest.fixture
def pipeline_workflow():
    """Two-node pipeline: START -> step_one -> step_two -> END."""
    return WorkflowDefinition(
        workflow_id="test_pipeline",
        name="Pipeline Test",
        version=1,
        nodes={
            "start": NodeDefinition(type=NodeType.START, next="step_one"),
            "step_one": NodeDefinition(
                type=NodeType.TASK,
                handler="echo",
                params={"message": "{{ inputs.message }}"},
                next="step_two",
            ),
            "step_two": NodeDefinition(
                type=NodeType.TASK,
                handler="echo",
                params={
                    "from_upstream": "{{ nodes.step_one.output.echoed_params.message }}",
                    "original": "{{ inputs.message }}",
                },
                next="end",
            ),
            "end": NodeDefinition(type=NodeType.END),
        },
    )


@pytest.fixture
def node_for(request):
    """Factory for creating NodeState instances."""
    def _make(node_id, job_id="job-test-001"):
        return NodeState(
            job_id=job_id,
            node_id=node_id,
            status=NodeStatus.READY,
        )
    return _make


# ============================================================================
# INPUT TEMPLATE RESOLUTION
# ============================================================================

class TestInputTemplateResolution:
    """Test that {{ inputs.x }} resolves from job params."""

    def test_resolve_inputs(self, publisher, simple_workflow, node_for):
        """Basic input template resolution."""
        node = node_for("echo")
        job_params = {"message": "hello world", "count": 3}

        task = publisher.create_task_message(
            node=node,
            workflow=simple_workflow,
            job_params=job_params,
        )

        assert task.params["message"] == "hello world"
        # Template engine may return int for numeric values
        assert task.params["count"] == 3

    def test_static_params_pass_through(self, publisher, node_for):
        """Params without templates pass through unchanged."""
        workflow = WorkflowDefinition(
            workflow_id="test_static",
            name="Static Test",
            version=1,
            nodes={
                "start": NodeDefinition(type=NodeType.START, next="task"),
                "task": NodeDefinition(
                    type=NodeType.TASK,
                    handler="echo",
                    params={"fixed": "value", "number": 42},
                    next="end",
                ),
                "end": NodeDefinition(type=NodeType.END),
            },
        )

        task = publisher.create_task_message(
            node=node_for("task"),
            workflow=workflow,
            job_params={},
        )

        assert task.params["fixed"] == "value"
        assert task.params["number"] == 42


# ============================================================================
# NODE-TO-NODE TEMPLATE RESOLUTION
# ============================================================================

class TestNodeOutputResolution:
    """Test that {{ nodes.X.output.Y }} resolves from completed node outputs."""

    def test_resolve_upstream_output(self, publisher, pipeline_workflow, node_for):
        """Resolve templates that reference a completed node's output."""
        node = node_for("step_two")
        job_params = {"message": "hello world"}
        node_outputs = {
            "step_one": {
                "echoed_params": {"message": "hello world"}
            }
        }

        task = publisher.create_task_message(
            node=node,
            workflow=pipeline_workflow,
            job_params=job_params,
            node_outputs=node_outputs,
        )

        assert task.params["from_upstream"] == "hello world"
        assert task.params["original"] == "hello world"

    def test_missing_node_output_raises(self, publisher, pipeline_workflow, node_for):
        """Referencing a non-existent node output raises TemplateResolutionError."""
        node = node_for("step_two")
        job_params = {"message": "hello"}
        # step_one NOT in node_outputs — template cannot resolve
        node_outputs = {}

        with pytest.raises(TemplateResolutionError):
            publisher.create_task_message(
                node=node,
                workflow=pipeline_workflow,
                job_params=job_params,
                node_outputs=node_outputs,
            )

    def test_mixed_inputs_and_node_outputs(self, publisher, pipeline_workflow, node_for):
        """Templates mixing {{ inputs.x }} and {{ nodes.y.output.z }} resolve."""
        node = node_for("step_two")
        job_params = {"message": "original-msg"}
        node_outputs = {
            "step_one": {
                "echoed_params": {"message": "upstream-msg"}
            }
        }

        task = publisher.create_task_message(
            node=node,
            workflow=pipeline_workflow,
            job_params=job_params,
            node_outputs=node_outputs,
        )

        assert task.params["from_upstream"] == "upstream-msg"
        assert task.params["original"] == "original-msg"


# ============================================================================
# BACKWARD COMPATIBILITY
# ============================================================================

class TestBackwardCompatibility:
    """Ensure callers without node_outputs still work."""

    def test_no_node_outputs_param(self, publisher, simple_workflow, node_for):
        """Calling without node_outputs works for simple input templates."""
        node = node_for("echo")
        job_params = {"message": "test", "count": 1}

        task = publisher.create_task_message(
            node=node,
            workflow=simple_workflow,
            job_params=job_params,
        )

        assert task.params["message"] == "test"

    def test_none_node_outputs(self, publisher, simple_workflow, node_for):
        """Explicitly passing node_outputs=None works."""
        node = node_for("echo")
        job_params = {"message": "test", "count": 1}

        task = publisher.create_task_message(
            node=node,
            workflow=simple_workflow,
            job_params=job_params,
            node_outputs=None,
        )

        assert task.params["message"] == "test"


# ============================================================================
# TASK MESSAGE METADATA
# ============================================================================

class TestTaskMessageMetadata:
    """Verify task message fields are set correctly."""

    def test_task_id_format(self, publisher, simple_workflow, node_for):
        """Task ID follows {job_id}_{node_id}_{retry_count} format."""
        node = node_for("echo")
        node.retry_count = 2

        task = publisher.create_task_message(
            node=node,
            workflow=simple_workflow,
            job_params={"message": "test", "count": 1},
        )

        assert task.task_id == "job-test-001_echo_2"
        assert task.handler == "echo"
        assert task.job_id == "job-test-001"
        assert task.node_id == "echo"
        assert task.retry_count == 2

    def test_checkpoint_data_passed_through(self, publisher, simple_workflow, node_for):
        """Checkpoint data is passed to TaskMessage."""
        node = node_for("echo")
        checkpoint = {"phase_name": "step_3", "state_data": {"next_step": 4}}

        task = publisher.create_task_message(
            node=node,
            workflow=simple_workflow,
            job_params={"message": "test", "count": 1},
            checkpoint_data=checkpoint,
        )

        assert task.checkpoint_data == checkpoint


# ============================================================================
# RUN TESTS
# ============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
