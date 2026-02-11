# ============================================================================
# INTEGRATION TESTS
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Tests - End-to-end integration tests
# PURPOSE: Verify full workflow: submit → dispatch → execute → complete
# CREATED: 31 JAN 2026
# ============================================================================
"""
Integration Tests

End-to-end tests for the DAG orchestrator.

These tests verify:
1. Job submission via API
2. Orchestrator dispatch loop
3. Worker execution
4. Result reporting
5. Job completion

Run with:
    pytest tests/test_integration.py -v
"""

import asyncio
import pytest
from datetime import datetime
from typing import Dict, Any
from unittest.mock import AsyncMock, MagicMock, patch

# Import test subjects
from core.models import Job, NodeState, TaskResult, WorkflowDefinition, NodeDefinition, NodeType
from core.models.workflow import DependsOn
from core.contracts import JobStatus, NodeStatus, TaskStatus
from handlers.registry import register_handler, clear_handlers, HandlerContext, HandlerResult
from worker.contracts import DAGTaskMessage, DAGTaskResult, TaskResultStatus
from worker.executor import TaskExecutor
from orchestrator.engine.templates import resolve_params, TemplateContext
from orchestrator.engine.evaluator import DAGEvaluator, find_ready_nodes


# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture(autouse=True)
def clean_handlers():
    """Clear handlers before and after each test."""
    clear_handlers()
    yield
    clear_handlers()


@pytest.fixture
def sample_workflow() -> WorkflowDefinition:
    """Create a sample workflow for testing."""
    return WorkflowDefinition(
        workflow_id="test_workflow",
        name="Test Workflow",
        version=1,
        nodes={
            "START": NodeDefinition(type=NodeType.START, next="process"),
            "process": NodeDefinition(
                type=NodeType.TASK,
                handler="echo",
                params={"message": "{{ inputs.message }}"},
                next="END",
            ),
            "END": NodeDefinition(type=NodeType.END),
        },
    )


@pytest.fixture
def sample_job(sample_workflow) -> Job:
    """Create a sample job for testing."""
    return Job(
        job_id="job-test-123",
        workflow_id=sample_workflow.workflow_id,
        workflow_version=sample_workflow.version,
        workflow_snapshot=sample_workflow.model_dump(mode="json"),
        status=JobStatus.PENDING,
        input_params={"message": "Hello, World!"},
    )


@pytest.fixture
def sample_node_states(sample_job) -> Dict[str, NodeState]:
    """Create sample node states for testing."""
    return {
        "START": NodeState(
            job_id=sample_job.job_id,
            node_id="START",
            status=NodeStatus.READY,
        ),
        "process": NodeState(
            job_id=sample_job.job_id,
            node_id="process",
            status=NodeStatus.PENDING,
        ),
        "END": NodeState(
            job_id=sample_job.job_id,
            node_id="END",
            status=NodeStatus.PENDING,
        ),
    }


# ============================================================================
# TEMPLATE RESOLUTION TESTS
# ============================================================================

class TestTemplateResolution:
    """Tests for template resolution."""

    def test_resolve_input_params(self):
        """Test resolving input parameters."""
        params = {"greeting": "{{ inputs.name }}"}
        job_params = {"name": "Alice"}

        resolved = resolve_params(params, job_params)

        assert resolved["greeting"] == "Alice"

    def test_resolve_nested_params(self):
        """Test resolving nested parameters."""
        params = {
            "config": {
                "host": "{{ inputs.host }}",
                "port": "{{ inputs.port }}",
            }
        }
        job_params = {"host": "localhost", "port": "8080"}

        resolved = resolve_params(params, job_params)

        assert resolved["config"]["host"] == "localhost"
        # Template resolver may convert numeric strings to numbers
        assert str(resolved["config"]["port"]) == "8080"

    def test_resolve_node_output(self):
        """Test resolving node output references."""
        params = {"file_url": "{{ nodes.upload.output.url }}"}
        job_params = {}
        node_outputs = {"upload": {"url": "https://example.com/file.zip"}}

        resolved = resolve_params(params, job_params, node_outputs)

        assert resolved["file_url"] == "https://example.com/file.zip"

    def test_no_templates(self):
        """Test params without templates pass through."""
        params = {"static": "value", "number": 42}

        resolved = resolve_params(params, {})

        assert resolved["static"] == "value"
        assert resolved["number"] == 42


# ============================================================================
# DAG EVALUATOR TESTS
# ============================================================================

class TestDAGEvaluator:
    """Tests for DAG evaluation."""

    def test_find_ready_nodes_start(self, sample_workflow, sample_node_states):
        """Test finding ready nodes when START is PENDING."""
        # Change START to PENDING to test evaluation
        sample_node_states["START"].status = NodeStatus.PENDING

        evaluator = DAGEvaluator()

        result = evaluator.find_ready_nodes(sample_workflow, sample_node_states)

        # START has no dependencies, so it should be ready
        assert "START" in result.ready_nodes

    def test_find_ready_nodes_after_start(self, sample_workflow, sample_node_states):
        """Test finding ready nodes after START completes."""
        # Mark START as completed
        sample_node_states["START"].status = NodeStatus.COMPLETED

        evaluator = DAGEvaluator()
        result = evaluator.find_ready_nodes(sample_workflow, sample_node_states)

        # 'process' should now be ready
        assert "process" in result.ready_nodes

    def test_validate_workflow_valid(self, sample_workflow):
        """Test workflow validation for valid workflow."""
        evaluator = DAGEvaluator()

        is_valid, error = evaluator.validate_workflow(sample_workflow)

        assert is_valid is True
        assert error is None

    def test_validate_workflow_missing_start(self):
        """Test workflow validation rejects missing START."""
        workflow = WorkflowDefinition(
            workflow_id="bad_workflow",
            name="Bad Workflow",
            nodes={
                "process": NodeDefinition(type=NodeType.TASK, handler="test", next="END"),
                "END": NodeDefinition(type=NodeType.END),
            },
        )
        evaluator = DAGEvaluator()

        is_valid, error = evaluator.validate_workflow(workflow)

        assert is_valid is False
        assert "START" in error or "start" in error.lower()


# ============================================================================
# HANDLER REGISTRY TESTS
# ============================================================================

class TestHandlerRegistry:
    """Tests for handler registry."""

    def test_register_handler(self):
        """Test registering a handler."""
        @register_handler("test_handler")
        async def test_handler(ctx: HandlerContext) -> HandlerResult:
            return HandlerResult.success_result({"test": True})

        from handlers.registry import get_handler
        handler = get_handler("test_handler")

        assert handler is not None
        assert handler.__name__ == "test_handler"

    def test_register_duplicate_raises(self):
        """Test duplicate handler registration raises."""
        @register_handler("duplicate")
        async def handler1(ctx: HandlerContext) -> HandlerResult:
            return HandlerResult.success_result()

        from handlers.registry import DuplicateHandlerError

        with pytest.raises(DuplicateHandlerError):
            @register_handler("duplicate")
            async def handler2(ctx: HandlerContext) -> HandlerResult:
                return HandlerResult.success_result()

    def test_get_handler_not_found(self):
        """Test getting non-existent handler returns None."""
        from handlers.registry import get_handler

        handler = get_handler("nonexistent")

        assert handler is None


# ============================================================================
# WORKER EXECUTOR TESTS
# ============================================================================

class TestWorkerExecutor:
    """Tests for worker task execution."""

    def test_execute_success(self):
        """Test successful task execution."""
        async def run_test():
            # Register a test handler
            @register_handler("test_success")
            async def test_success(ctx: HandlerContext) -> HandlerResult:
                return HandlerResult.success_result({"result": "ok"})

            executor = TaskExecutor(worker_id="test-worker")

            task = DAGTaskMessage(
                task_id="task-1",
                job_id="job-1",
                node_id="test",
                handler="test_success",
                params={},
            )

            result = await executor.execute(task)

            assert result.status == TaskResultStatus.COMPLETED
            assert result.output == {"result": "ok"}
            assert result.worker_id == "test-worker"

        asyncio.run(run_test())

    def test_execute_failure(self):
        """Test failed task execution."""
        async def run_test():
            @register_handler("test_failure")
            async def test_failure(ctx: HandlerContext) -> HandlerResult:
                return HandlerResult.failure_result("Something went wrong")

            executor = TaskExecutor(worker_id="test-worker")

            task = DAGTaskMessage(
                task_id="task-1",
                job_id="job-1",
                node_id="test",
                handler="test_failure",
                params={},
            )

            result = await executor.execute(task)

            assert result.status == TaskResultStatus.FAILED
            assert "Something went wrong" in result.error_message

        asyncio.run(run_test())

    def test_execute_handler_not_found(self):
        """Test execution with missing handler."""
        async def run_test():
            executor = TaskExecutor(worker_id="test-worker")

            task = DAGTaskMessage(
                task_id="task-1",
                job_id="job-1",
                node_id="test",
                handler="nonexistent_handler",
                params={},
            )

            result = await executor.execute(task)

            assert result.status == TaskResultStatus.FAILED
            assert "not found" in result.error_message.lower()

        asyncio.run(run_test())

    def test_execute_timeout(self):
        """Test task timeout."""
        async def run_test():
            @register_handler("test_slow")
            async def test_slow(ctx: HandlerContext) -> HandlerResult:
                await asyncio.sleep(10)  # Longer than timeout
                return HandlerResult.success_result()

            executor = TaskExecutor(worker_id="test-worker")

            task = DAGTaskMessage(
                task_id="task-1",
                job_id="job-1",
                node_id="test",
                handler="test_slow",
                params={},
                timeout_seconds=1,  # 1 second timeout
            )

            result = await executor.execute(task)

            assert result.status == TaskResultStatus.FAILED
            assert "timed out" in result.error_message.lower()

        asyncio.run(run_test())


# ============================================================================
# DAG TASK MESSAGE TESTS
# ============================================================================

class TestDAGTaskMessage:
    """Tests for DAG task message serialization."""

    def test_to_json_and_back(self):
        """Test message serialization round-trip."""
        original = DAGTaskMessage(
            task_id="task-123",
            job_id="job-456",
            node_id="process",
            handler="test_handler",
            params={"key": "value"},
            timeout_seconds=300,
            retry_count=1,
        )

        json_str = original.to_json()
        restored = DAGTaskMessage.from_json(json_str)

        assert restored.task_id == original.task_id
        assert restored.job_id == original.job_id
        assert restored.node_id == original.node_id
        assert restored.handler == original.handler
        assert restored.params == original.params
        assert restored.timeout_seconds == original.timeout_seconds
        assert restored.retry_count == original.retry_count

    def test_detect_dag_message(self):
        """Test detecting DAG message type."""
        from worker.contracts import detect_message_type, TaskMessageType

        dag_msg = {
            "task_id": "123",
            "job_id": "456",
            "node_id": "test",
            "handler": "test_handler",
        }

        assert detect_message_type(dag_msg) == TaskMessageType.DAG

    def test_detect_legacy_message(self):
        """Test detecting legacy message type."""
        from worker.contracts import detect_message_type, TaskMessageType

        legacy_msg = {
            "job_id": "123",
            "task_type": "process_raster",
            "stage": "1",
        }

        assert detect_message_type(legacy_msg) == TaskMessageType.LEGACY


# ============================================================================
# NODE SERVICE DEPENDENCY TESTS
# ============================================================================

class TestNodeServiceDependencies:
    """Tests for node service dependency checking (P0.0 stale state fix)."""

    def test_dependencies_met_checks_implicit_next_pointers(self):
        """
        Test that _dependencies_met() correctly checks implicit dependencies
        from 'next' pointers. This is the P0.0 fix for the stale state bug.

        Bug scenario: END node would become READY prematurely because the
        implicit dependency check wasn't implemented.
        """
        from services.node_service import NodeService

        # Create workflow: START -> process -> END
        workflow = WorkflowDefinition(
            workflow_id="test_implicit_deps",
            name="Test Implicit Dependencies",
            nodes={
                "START": NodeDefinition(type=NodeType.START, next="process"),
                "process": NodeDefinition(
                    type=NodeType.TASK,
                    handler="echo",
                    next="END",
                ),
                "END": NodeDefinition(type=NodeType.END),
            },
        )

        # Create a mock node service (we only need the _dependencies_met method)
        node_service = NodeService.__new__(NodeService)

        # Scenario 1: No nodes completed yet
        completed_nodes = set()
        end_def = workflow.nodes["END"]

        # END should NOT be ready - process hasn't completed
        result = node_service._dependencies_met("END", end_def, completed_nodes, workflow)
        assert result is False, "END should not be ready when process hasn't completed"

        # Scenario 2: START completed, process pending
        completed_nodes = {"START"}

        # process should be ready - START completed (implicit dep via next)
        process_def = workflow.nodes["process"]
        result = node_service._dependencies_met("process", process_def, completed_nodes, workflow)
        assert result is True, "process should be ready after START completes"

        # END should still NOT be ready
        result = node_service._dependencies_met("END", end_def, completed_nodes, workflow)
        assert result is False, "END should not be ready when process hasn't completed"

        # Scenario 3: Both START and process completed
        completed_nodes = {"START", "process"}

        # END should now be ready
        result = node_service._dependencies_met("END", end_def, completed_nodes, workflow)
        assert result is True, "END should be ready after process completes"

    def test_dependencies_met_with_parallel_tasks(self):
        """
        Test dependency checking with parallel tasks leading to fan-in.
        """
        from services.node_service import NodeService

        # Create workflow: START -> [task_a, task_b] (parallel) -> END
        workflow = WorkflowDefinition(
            workflow_id="test_parallel",
            name="Test Parallel Tasks",
            nodes={
                "START": NodeDefinition(type=NodeType.START, next=["task_a", "task_b"]),
                "task_a": NodeDefinition(type=NodeType.TASK, handler="echo", next="END"),
                "task_b": NodeDefinition(type=NodeType.TASK, handler="echo", next="END"),
                "END": NodeDefinition(type=NodeType.END),
            },
        )

        node_service = NodeService.__new__(NodeService)
        end_def = workflow.nodes["END"]

        # Only task_a completed - END should not be ready (task_b still pending)
        completed_nodes = {"START", "task_a"}
        result = node_service._dependencies_met("END", end_def, completed_nodes, workflow)
        assert result is False, "END should not be ready when only task_a completed"

        # Both tasks completed - END should be ready
        completed_nodes = {"START", "task_a", "task_b"}
        result = node_service._dependencies_met("END", end_def, completed_nodes, workflow)
        assert result is True, "END should be ready when both tasks complete"

    def test_explicit_depends_on_takes_precedence(self):
        """
        Test that explicit depends_on takes precedence over implicit next pointers.
        """
        from services.node_service import NodeService

        # Create workflow with explicit depends_on
        workflow = WorkflowDefinition(
            workflow_id="test_explicit",
            name="Test Explicit Dependencies",
            nodes={
                "START": NodeDefinition(type=NodeType.START, next="task_a"),
                "task_a": NodeDefinition(type=NodeType.TASK, handler="echo", next="task_b"),
                "task_b": NodeDefinition(
                    type=NodeType.TASK,
                    handler="echo",
                    # Explicit depends_on - only needs task_a, not START
                    depends_on=DependsOn(all_of=["task_a"]),
                    next="END",
                ),
                "END": NodeDefinition(type=NodeType.END),
            },
        )

        node_service = NodeService.__new__(NodeService)
        task_b_def = workflow.nodes["task_b"]

        # task_a completed - task_b should be ready (explicit depends_on satisfied)
        completed_nodes = {"task_a"}
        result = node_service._dependencies_met("task_b", task_b_def, completed_nodes, workflow)
        assert result is True, "task_b should be ready when explicit dependency satisfied"


# ============================================================================
# END-TO-END SIMULATION
# ============================================================================

class TestRetryLogic:
    """Tests for P0.2 retry logic."""

    def test_prepare_retry_resets_to_ready(self):
        """Test that prepare_retry() correctly resets a failed node."""
        node = NodeState(
            job_id="job-1",
            node_id="test-node",
            status=NodeStatus.FAILED,
            retry_count=0,
            max_retries=3,
            error_message="Something went wrong",
            task_id="task-123",
        )

        # Should be able to retry
        assert node.prepare_retry() is True
        assert node.status == NodeStatus.READY
        assert node.retry_count == 1
        assert node.task_id is None
        assert node.error_message is None

    def test_prepare_retry_respects_max_retries(self):
        """Test that prepare_retry() fails when max retries exceeded."""
        node = NodeState(
            job_id="job-1",
            node_id="test-node",
            status=NodeStatus.FAILED,
            retry_count=3,
            max_retries=3,
            error_message="Something went wrong",
        )

        # Should not be able to retry
        assert node.prepare_retry() is False
        assert node.status == NodeStatus.FAILED  # Unchanged
        assert node.retry_count == 3  # Unchanged

    def test_retry_count_increments_each_attempt(self):
        """Test that retry_count increments with each retry."""
        node = NodeState(
            job_id="job-1",
            node_id="test-node",
            status=NodeStatus.FAILED,
            retry_count=0,
            max_retries=3,
        )

        # First retry
        assert node.prepare_retry() is True
        assert node.retry_count == 1

        # Simulate another failure
        node.status = NodeStatus.FAILED

        # Second retry
        assert node.prepare_retry() is True
        assert node.retry_count == 2

        # Simulate another failure
        node.status = NodeStatus.FAILED

        # Third retry
        assert node.prepare_retry() is True
        assert node.retry_count == 3

        # Simulate another failure
        node.status = NodeStatus.FAILED

        # Fourth retry should fail (max is 3)
        assert node.prepare_retry() is False
        assert node.retry_count == 3  # Unchanged


class TestVersionPinning:
    """Tests for workflow version pinning (Tier 5)."""

    def test_snapshot_round_trip(self, sample_workflow):
        """Serialize a workflow to snapshot, create a Job, deserialize back."""
        snapshot = sample_workflow.model_dump(mode="json")

        job = Job(
            job_id="job-pin-001",
            workflow_id=sample_workflow.workflow_id,
            workflow_version=sample_workflow.version,
            workflow_snapshot=snapshot,
            status=JobStatus.PENDING,
            input_params={},
        )

        # Deserialize back via get_pinned_workflow()
        restored = job.get_pinned_workflow()

        assert restored.workflow_id == sample_workflow.workflow_id
        assert restored.version == sample_workflow.version
        assert len(restored.nodes) == len(sample_workflow.nodes)
        for node_id, node_def in sample_workflow.nodes.items():
            assert node_id in restored.nodes
            assert restored.nodes[node_id].type == node_def.type

    def test_workflow_version_required(self):
        """Job requires workflow_version — omitting it raises ValidationError."""
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            Job(
                job_id="job-no-version",
                workflow_id="test",
                status=JobStatus.PENDING,
                input_params={},
                workflow_snapshot={"workflow_id": "test", "name": "t", "nodes": {}},
            )

    def test_workflow_snapshot_required(self):
        """Job requires workflow_snapshot — omitting it raises ValidationError."""
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            Job(
                job_id="job-no-snapshot",
                workflow_id="test",
                workflow_version=1,
                status=JobStatus.PENDING,
                input_params={},
            )

    def test_pinned_workflow_preserves_node_types(self, sample_workflow):
        """Verify node types survive serialization round-trip."""
        snapshot = sample_workflow.model_dump(mode="json")
        job = Job(
            job_id="job-pin-002",
            workflow_id=sample_workflow.workflow_id,
            workflow_version=sample_workflow.version,
            workflow_snapshot=snapshot,
            status=JobStatus.PENDING,
            input_params={},
        )

        restored = job.get_pinned_workflow()

        assert restored.nodes["START"].type == NodeType.START
        assert restored.nodes["process"].type == NodeType.TASK
        assert restored.nodes["process"].handler == "echo"
        assert restored.nodes["END"].type == NodeType.END


class TestEndToEnd:
    """End-to-end simulation tests."""

    def test_full_job_lifecycle(self, sample_workflow):
        """Simulate a full job lifecycle without real DB/Service Bus."""
        async def run_test():
            # Register handler
            @register_handler("echo")
            async def echo_handler(ctx: HandlerContext) -> HandlerResult:
                return HandlerResult.success_result({
                    "echoed": ctx.params.get("message"),
                })

            # 1. Create job (simulated)
            job = Job(
                job_id="job-e2e-test",
                workflow_id=sample_workflow.workflow_id,
                workflow_version=sample_workflow.version,
                workflow_snapshot=sample_workflow.model_dump(mode="json"),
                status=JobStatus.PENDING,
                input_params={"message": "Hello, E2E!"},
            )

            # 2. Create node states
            node_states = {
                "START": NodeState(
                    job_id=job.job_id,
                    node_id="START",
                    status=NodeStatus.PENDING,
                ),
                "process": NodeState(
                    job_id=job.job_id,
                    node_id="process",
                    status=NodeStatus.PENDING,
                ),
                "END": NodeState(
                    job_id=job.job_id,
                    node_id="END",
                    status=NodeStatus.PENDING,
                ),
            }

            # 3. Evaluate - START should be ready (no dependencies)
            evaluator = DAGEvaluator()
            result = evaluator.find_ready_nodes(sample_workflow, node_states)
            assert "START" in result.ready_nodes

            # 4. Complete START node (START/END auto-complete, skip dispatch/running)
            node_states["START"].status = NodeStatus.COMPLETED
            node_states["START"].output = {}

            # 5. Evaluate again - process should be ready
            result = evaluator.find_ready_nodes(sample_workflow, node_states)
            assert "process" in result.ready_nodes

            # 6. Resolve templates for process node
            process_def = sample_workflow.nodes["process"]
            resolved_params = resolve_params(
                process_def.params,
                job.input_params,
            )
            assert resolved_params["message"] == "Hello, E2E!"

            # 7. Create task message
            task = DAGTaskMessage(
                task_id=f"{job.job_id}_process_0",
                job_id=job.job_id,
                node_id="process",
                handler=process_def.handler,
                params=resolved_params,
            )

            # 8. Execute task
            executor = TaskExecutor(worker_id="test-worker")
            task_result = await executor.execute(task)

            assert task_result.status == TaskResultStatus.COMPLETED
            assert task_result.output["echoed"] == "Hello, E2E!"

            # 9. Update node state (simulate dispatch -> run -> complete)
            node_states["process"].status = NodeStatus.COMPLETED
            node_states["process"].output = task_result.output

            # 10. Evaluate - END should be ready
            result = evaluator.find_ready_nodes(sample_workflow, node_states)
            assert "END" in result.ready_nodes

            # 11. Complete END (auto-complete for END nodes)
            node_states["END"].status = NodeStatus.COMPLETED
            node_states["END"].output = {}

            # 12. All nodes terminal - job complete
            assert all(n.is_terminal for n in node_states.values())

            # 13. Mark job complete (go through RUNNING first)
            job.mark_started()
            job.mark_completed({"process": task_result.output})
            assert job.status == JobStatus.COMPLETED

        asyncio.run(run_test())


# ============================================================================
# RUN TESTS
# ============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
