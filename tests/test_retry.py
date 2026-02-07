# ============================================================================
# RETRY / TIMEOUT TESTS
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Tests - Tier 3 retry and timeout handling
# PURPOSE: Verify timeout detection, retry logic, and flaky handler behavior
# CREATED: 07 FEB 2026
# ============================================================================
"""
Retry / Timeout Tests

Covers:
1. Flaky echo handler (configurable failure rate)
2. Timeout detection for stuck DISPATCHED/RUNNING nodes
3. Auto-retry after timeout (if retries remaining)
4. Max retries exhaustion → node stays FAILED
5. RetryPolicy wiring from workflow YAML to NodeState.max_retries
6. NodeState.prepare_retry() behavior
7. Integration: timeout → retry → dispatch cycle

Run with:
    pytest tests/test_retry.py -v
"""

import asyncio
import pytest
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set
from unittest.mock import AsyncMock, MagicMock, patch

from core.models import (
    WorkflowDefinition, NodeDefinition, NodeState, NodeType, FanOutTaskDef,
)
from core.models.workflow import RetryPolicy
from core.contracts import NodeStatus
from handlers.registry import HandlerContext, HandlerResult


# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture
def retry_workflow():
    """Workflow: START -> flaky_step (with retry) -> END."""
    return WorkflowDefinition(
        workflow_id="test_retry",
        name="Retry Test",
        version=1,
        nodes={
            "start": NodeDefinition(type=NodeType.START, next="flaky_step"),
            "flaky_step": NodeDefinition(
                type=NodeType.TASK,
                handler="flaky_echo",
                params={"failure_rate": "0.5", "message": "test"},
                retry=RetryPolicy(
                    max_attempts=5,
                    backoff="fixed",
                    initial_delay_seconds=1,
                ),
                timeout_seconds=30,
                next="end",
            ),
            "end": NodeDefinition(type=NodeType.END),
        },
    )


@pytest.fixture
def fan_out_retry_workflow():
    """Workflow with fan-out and flaky children."""
    return WorkflowDefinition(
        workflow_id="test_fan_out_retry",
        name="Fan-Out Retry Test",
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
                    handler="flaky_echo",
                    queue="dag-worker-tasks",
                    params={
                        "item_value": "{{ item }}",
                        "failure_rate": "0.3",
                    },
                    timeout_seconds=15,
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
        status: NodeStatus = NodeStatus.PENDING,
        job_id: str = "job-retry-001",
        output: Optional[dict] = None,
        retry_count: int = 0,
        max_retries: int = 3,
        dispatched_at: Optional[datetime] = None,
        started_at: Optional[datetime] = None,
        parent_node_id: Optional[str] = None,
        fan_out_index: Optional[int] = None,
        task_id: Optional[str] = None,
        version: int = 1,
    ) -> NodeState:
        return NodeState(
            job_id=job_id,
            node_id=node_id,
            status=status,
            output=output,
            retry_count=retry_count,
            max_retries=max_retries,
            dispatched_at=dispatched_at,
            started_at=started_at,
            parent_node_id=parent_node_id,
            fan_out_index=fan_out_index,
            task_id=task_id,
            version=version,
        )
    return _make


# ============================================================================
# FLAKY ECHO HANDLER
# ============================================================================

class TestFlakyEchoHandler:
    """Test the flaky_echo handler."""

    def test_always_succeeds_at_zero_rate(self):
        """flaky_echo with failure_rate=0.0 always succeeds."""
        from handlers.examples import flaky_echo_handler

        ctx = HandlerContext(
            task_id="task-001",
            job_id="job-001",
            node_id="node-001",
            handler="flaky_echo",
            params={"failure_rate": "0.0", "message": "test"},
            timeout_seconds=60,
            retry_count=0,
        )

        # Run 20 times to be sure
        for _ in range(20):
            result = asyncio.run(flaky_echo_handler(ctx))
            assert result.success is True
            assert "echoed_params" in result.output
            assert result.output["handler"] == "flaky_echo"

    def test_always_fails_at_full_rate(self):
        """flaky_echo with failure_rate=1.0 always fails."""
        from handlers.examples import flaky_echo_handler

        ctx = HandlerContext(
            task_id="task-001",
            job_id="job-001",
            node_id="node-001",
            handler="flaky_echo",
            params={"failure_rate": "1.0"},
            timeout_seconds=60,
            retry_count=0,
        )

        for _ in range(20):
            result = asyncio.run(flaky_echo_handler(ctx))
            assert result.success is False
            assert "Random failure" in result.error_message

    def test_default_failure_rate_is_twenty_percent(self):
        """Default failure rate is 0.2."""
        from handlers.examples import flaky_echo_handler

        ctx = HandlerContext(
            task_id="task-001",
            job_id="job-001",
            node_id="node-001",
            handler="flaky_echo",
            params={},  # No failure_rate → default 0.2
            timeout_seconds=60,
            retry_count=0,
        )

        # Run 100 times, should have mix of success and failure
        results = [asyncio.run(flaky_echo_handler(ctx)) for _ in range(100)]
        successes = sum(1 for r in results if r.success)
        failures = sum(1 for r in results if not r.success)

        # With 0.2 failure rate over 100 runs, expect ~80 successes, ~20 failures
        # Allow wide margin for randomness
        assert successes > 50, f"Too few successes: {successes}/100"
        assert failures > 5, f"Too few failures: {failures}/100"

    def test_retry_count_in_output(self):
        """Success output includes retry_count."""
        from handlers.examples import flaky_echo_handler

        ctx = HandlerContext(
            task_id="task-001",
            job_id="job-001",
            node_id="node-001",
            handler="flaky_echo",
            params={"failure_rate": "0.0"},
            timeout_seconds=60,
            retry_count=3,
        )

        result = asyncio.run(flaky_echo_handler(ctx))
        assert result.success is True
        assert result.output["retry_count"] == 3

    def test_handler_is_registered(self):
        """flaky_echo is in the handler registry."""
        from handlers.registry import get_handler, clear_handlers
        # Re-register handlers (other tests may have cleared the registry)
        clear_handlers()
        import importlib
        import handlers.examples
        importlib.reload(handlers.examples)

        handler = get_handler("flaky_echo")
        assert handler is not None


# ============================================================================
# NODE STATE RETRY BEHAVIOR
# ============================================================================

class TestNodeStateRetry:
    """Test NodeState retry mechanics."""

    def test_prepare_retry_increments_count(self, make_node):
        """prepare_retry() increments retry_count and resets to READY."""
        node = make_node("n1", status=NodeStatus.FAILED, max_retries=3, retry_count=0)
        assert node.prepare_retry() is True
        assert node.retry_count == 1
        assert node.status == NodeStatus.READY

    def test_prepare_retry_clears_error(self, make_node):
        """prepare_retry() clears error_message, task_id, timestamps."""
        node = make_node(
            "n1",
            status=NodeStatus.FAILED,
            max_retries=3,
            retry_count=0,
            task_id="old-task",
        )
        node.error_message = "Some error"
        node.dispatched_at = datetime.utcnow()
        node.started_at = datetime.utcnow()
        node.completed_at = datetime.utcnow()

        assert node.prepare_retry() is True
        assert node.error_message is None
        assert node.task_id is None
        assert node.dispatched_at is None
        assert node.started_at is None
        assert node.completed_at is None

    def test_prepare_retry_fails_when_exhausted(self, make_node):
        """prepare_retry() returns False when retry_count >= max_retries."""
        node = make_node("n1", status=NodeStatus.FAILED, max_retries=3, retry_count=3)
        assert node.prepare_retry() is False
        assert node.status == NodeStatus.FAILED

    def test_prepare_retry_fails_if_not_failed(self, make_node):
        """prepare_retry() returns False if node is not in FAILED state."""
        node = make_node("n1", status=NodeStatus.RUNNING, max_retries=3, retry_count=0)
        assert node.prepare_retry() is False

    def test_can_transition_failed_to_ready_with_retries(self, make_node):
        """FAILED → READY allowed when retries remain."""
        node = make_node("n1", status=NodeStatus.FAILED, max_retries=3, retry_count=2)
        assert node.can_transition_to(NodeStatus.READY) is True

    def test_cannot_transition_failed_to_ready_exhausted(self, make_node):
        """FAILED → READY blocked when max retries exhausted."""
        node = make_node("n1", status=NodeStatus.FAILED, max_retries=3, retry_count=3)
        assert node.can_transition_to(NodeStatus.READY) is False

    def test_dispatched_can_transition_to_failed(self, make_node):
        """DISPATCHED → FAILED allowed (for timeout)."""
        node = make_node("n1", status=NodeStatus.DISPATCHED)
        assert node.can_transition_to(NodeStatus.FAILED) is True

    def test_running_can_transition_to_failed(self, make_node):
        """RUNNING → FAILED allowed (for timeout or error)."""
        node = make_node("n1", status=NodeStatus.RUNNING)
        assert node.can_transition_to(NodeStatus.FAILED) is True


# ============================================================================
# RETRY POLICY WIRING
# ============================================================================

class TestRetryPolicyWiring:
    """Test that RetryPolicy from workflow YAML is applied to NodeState."""

    def test_retry_max_attempts_applied(self, retry_workflow):
        """Nodes created with retry.max_attempts get that as max_retries."""
        from services.job_service import JobService

        # Create a minimal mock
        mock_pool = MagicMock()
        mock_workflow_svc = MagicMock()
        mock_workflow_svc.get_or_raise.return_value = retry_workflow

        job_service = JobService.__new__(JobService)
        job_service.workflow_service = mock_workflow_svc

        nodes = job_service._create_node_states("job-001", retry_workflow, {})

        # Find the flaky_step node
        flaky_node = next(n for n in nodes if n.node_id == "flaky_step")
        assert flaky_node.max_retries == 5  # max_attempts from RetryPolicy

    def test_default_max_retries_without_policy(self, retry_workflow):
        """Nodes without retry policy get default max_retries=3."""
        from services.job_service import JobService

        job_service = JobService.__new__(JobService)

        # Workflow where no node has retry policy
        simple_workflow = WorkflowDefinition(
            workflow_id="simple",
            name="Simple",
            version=1,
            nodes={
                "start": NodeDefinition(type=NodeType.START, next="task"),
                "task": NodeDefinition(
                    type=NodeType.TASK,
                    handler="echo",
                    next="end",
                ),
                "end": NodeDefinition(type=NodeType.END),
            },
        )

        nodes = job_service._create_node_states("job-001", simple_workflow, {})
        task_node = next(n for n in nodes if n.node_id == "task")
        assert task_node.max_retries == 3  # default

    def test_start_node_is_ready(self, retry_workflow):
        """START nodes are created with READY status."""
        from services.job_service import JobService

        job_service = JobService.__new__(JobService)
        nodes = job_service._create_node_states("job-001", retry_workflow, {})

        start_node = next(n for n in nodes if n.node_id == "start")
        assert start_node.status == NodeStatus.READY

    def test_other_nodes_are_pending(self, retry_workflow):
        """Non-START nodes are created with PENDING status."""
        from services.job_service import JobService

        job_service = JobService.__new__(JobService)
        nodes = job_service._create_node_states("job-001", retry_workflow, {})

        for node in nodes:
            if node.node_id != "start":
                assert node.status == NodeStatus.PENDING, f"{node.node_id} should be PENDING"


# ============================================================================
# TIMEOUT DETECTION
# ============================================================================

class TestTimeoutDetection:
    """Test _check_stuck_nodes() in orchestrator loop."""

    def _make_orchestrator(self):
        """Create a minimal Orchestrator for testing."""
        from orchestrator.loop import Orchestrator

        mock_pool = MagicMock()
        mock_workflow_svc = MagicMock()
        orch = Orchestrator.__new__(Orchestrator)
        orch.pool = mock_pool
        orch.node_service = MagicMock()
        orch._event_service = None
        orch.DEFAULT_TASK_TIMEOUT_SEC = 3600
        return orch

    def test_dispatched_node_past_timeout(self, retry_workflow, make_node):
        """Node DISPATCHED past timeout_seconds is marked FAILED then retried."""
        orch = self._make_orchestrator()

        # Node dispatched 60 seconds ago with 30s timeout
        node = make_node(
            "flaky_step",
            status=NodeStatus.DISPATCHED,
            dispatched_at=datetime.utcnow() - timedelta(seconds=60),
            task_id="task-001",
            max_retries=3,
            retry_count=0,
        )

        # Mock node_repo
        mock_node_repo = AsyncMock()
        mock_node_repo.get_all_for_job.return_value = [node]
        mock_node_repo.update.return_value = True

        from core.models import Job
        job = Job.model_construct(job_id="job-retry-001", workflow_id="test_retry")

        with patch("orchestrator.loop.NodeRepository", return_value=mock_node_repo):
            count = asyncio.run(orch._check_stuck_nodes(job, retry_workflow))

        assert count == 1
        # Node should be READY (failed + retried)
        assert node.status == NodeStatus.READY
        assert node.retry_count == 1

    def test_running_node_past_timeout(self, retry_workflow, make_node):
        """Node RUNNING past timeout_seconds is marked FAILED then retried."""
        orch = self._make_orchestrator()

        node = make_node(
            "flaky_step",
            status=NodeStatus.RUNNING,
            dispatched_at=datetime.utcnow() - timedelta(seconds=120),
            started_at=datetime.utcnow() - timedelta(seconds=60),
            task_id="task-001",
            max_retries=3,
            retry_count=0,
        )

        mock_node_repo = AsyncMock()
        mock_node_repo.get_all_for_job.return_value = [node]
        mock_node_repo.update.return_value = True

        from core.models import Job
        job = Job.model_construct(job_id="job-retry-001", workflow_id="test_retry")

        with patch("orchestrator.loop.NodeRepository", return_value=mock_node_repo):
            count = asyncio.run(orch._check_stuck_nodes(job, retry_workflow))

        assert count == 1
        assert node.status == NodeStatus.READY
        assert node.retry_count == 1

    def test_node_within_timeout_not_touched(self, retry_workflow, make_node):
        """Node within timeout is not affected."""
        orch = self._make_orchestrator()

        # Node dispatched 10 seconds ago with 30s timeout
        node = make_node(
            "flaky_step",
            status=NodeStatus.DISPATCHED,
            dispatched_at=datetime.utcnow() - timedelta(seconds=10),
            task_id="task-001",
        )

        mock_node_repo = AsyncMock()
        mock_node_repo.get_all_for_job.return_value = [node]

        from core.models import Job
        job = Job.model_construct(job_id="job-retry-001", workflow_id="test_retry")

        with patch("orchestrator.loop.NodeRepository", return_value=mock_node_repo):
            count = asyncio.run(orch._check_stuck_nodes(job, retry_workflow))

        assert count == 0
        assert node.status == NodeStatus.DISPATCHED  # Unchanged

    def test_timed_out_node_max_retries_exhausted(self, retry_workflow, make_node):
        """Timed-out node with max retries exhausted stays FAILED."""
        orch = self._make_orchestrator()

        node = make_node(
            "flaky_step",
            status=NodeStatus.DISPATCHED,
            dispatched_at=datetime.utcnow() - timedelta(seconds=60),
            task_id="task-001",
            max_retries=3,
            retry_count=3,  # Already exhausted
        )

        mock_node_repo = AsyncMock()
        mock_node_repo.get_all_for_job.return_value = [node]
        mock_node_repo.update.return_value = True

        from core.models import Job
        job = Job.model_construct(job_id="job-retry-001", workflow_id="test_retry")

        with patch("orchestrator.loop.NodeRepository", return_value=mock_node_repo):
            count = asyncio.run(orch._check_stuck_nodes(job, retry_workflow))

        assert count == 1
        assert node.status == NodeStatus.FAILED  # Stays FAILED
        assert "timed out" in node.error_message.lower()

    def test_dynamic_child_uses_parent_timeout(self, fan_out_retry_workflow, make_node):
        """Dynamic child node uses parent FAN_OUT task def's timeout."""
        orch = self._make_orchestrator()

        # Dynamic child dispatched 20 seconds ago — parent task timeout is 15s
        node = make_node(
            "split__0",
            status=NodeStatus.DISPATCHED,
            dispatched_at=datetime.utcnow() - timedelta(seconds=20),
            task_id="task-001",
            parent_node_id="split",
            fan_out_index=0,
            max_retries=3,
            retry_count=0,
        )

        mock_node_repo = AsyncMock()
        mock_node_repo.get_all_for_job.return_value = [node]
        mock_node_repo.update.return_value = True

        from core.models import Job
        job = Job.model_construct(job_id="job-retry-001", workflow_id="test_retry")

        with patch("orchestrator.loop.NodeRepository", return_value=mock_node_repo):
            count = asyncio.run(orch._check_stuck_nodes(job, fan_out_retry_workflow))

        assert count == 1
        assert node.status == NodeStatus.READY  # Failed + retried
        assert node.retry_count == 1
        assert node.error_message is None  # Cleared by prepare_retry()

    def test_pending_and_completed_nodes_ignored(self, retry_workflow, make_node):
        """Only DISPATCHED/RUNNING nodes are checked for timeout."""
        orch = self._make_orchestrator()

        nodes = [
            make_node("start", status=NodeStatus.COMPLETED),
            make_node("flaky_step", status=NodeStatus.PENDING),
            make_node("end", status=NodeStatus.PENDING),
        ]

        mock_node_repo = AsyncMock()
        mock_node_repo.get_all_for_job.return_value = nodes

        from core.models import Job
        job = Job.model_construct(job_id="job-retry-001", workflow_id="test_retry")

        with patch("orchestrator.loop.NodeRepository", return_value=mock_node_repo):
            count = asyncio.run(orch._check_stuck_nodes(job, retry_workflow))

        assert count == 0

    def test_default_timeout_for_unknown_node(self, make_node):
        """Nodes not in workflow definition use DEFAULT_TASK_TIMEOUT_SEC."""
        orch = self._make_orchestrator()
        orch.DEFAULT_TASK_TIMEOUT_SEC = 60  # 60 seconds

        # Workflow with no matching node for "mystery_node"
        workflow = WorkflowDefinition(
            workflow_id="test",
            name="Test",
            version=1,
            nodes={
                "start": NodeDefinition(type=NodeType.START, next="end"),
                "end": NodeDefinition(type=NodeType.END),
            },
        )

        # Node dispatched 120 seconds ago — exceeds 60s default
        node = make_node(
            "mystery_node",
            status=NodeStatus.DISPATCHED,
            dispatched_at=datetime.utcnow() - timedelta(seconds=120),
            task_id="task-001",
            max_retries=3,
            retry_count=0,
        )

        mock_node_repo = AsyncMock()
        mock_node_repo.get_all_for_job.return_value = [node]
        mock_node_repo.update.return_value = True

        from core.models import Job
        job = Job.model_construct(job_id="job-001", workflow_id="test")

        with patch("orchestrator.loop.NodeRepository", return_value=mock_node_repo):
            count = asyncio.run(orch._check_stuck_nodes(job, workflow))

        assert count == 1

    def test_timeout_emits_events(self, retry_workflow, make_node):
        """Timeout detection emits NODE_FAILED and NODE_RETRY events."""
        orch = self._make_orchestrator()
        orch._event_service = AsyncMock()

        node = make_node(
            "flaky_step",
            status=NodeStatus.DISPATCHED,
            dispatched_at=datetime.utcnow() - timedelta(seconds=60),
            task_id="task-001",
            max_retries=3,
            retry_count=0,
        )

        mock_node_repo = AsyncMock()
        mock_node_repo.get_all_for_job.return_value = [node]
        mock_node_repo.update.return_value = True

        from core.models import Job
        job = Job.model_construct(job_id="job-retry-001", workflow_id="test_retry")

        with patch("orchestrator.loop.NodeRepository", return_value=mock_node_repo):
            asyncio.run(orch._check_stuck_nodes(job, retry_workflow))

        # Should have emitted both NODE_FAILED and NODE_RETRY
        orch._event_service.emit_node_failed.assert_called_once()
        orch._event_service.emit_node_retry.assert_called_once()


# ============================================================================
# WORKFLOW LOADING
# ============================================================================

class TestRetryWorkflowLoading:
    """Test that retry workflows load correctly."""

    def test_retry_test_workflow_loads(self):
        """workflows/retry_test.yaml loads and validates."""
        from services.workflow_service import WorkflowService
        svc = WorkflowService()
        workflow = svc.get("retry_test")
        assert workflow is not None
        assert workflow.workflow_id == "retry_test"

        # Check flaky_step has retry policy
        flaky = workflow.nodes["flaky_step"]
        assert flaky.retry is not None
        assert flaky.retry.max_attempts == 5
        assert flaky.retry.backoff == "fixed"
        assert flaky.timeout_seconds == 30

    def test_retry_fan_out_workflow_loads(self):
        """workflows/retry_fan_out_test.yaml loads and validates."""
        from services.workflow_service import WorkflowService
        svc = WorkflowService()
        workflow = svc.get("retry_fan_out_test")
        assert workflow is not None
        assert workflow.workflow_id == "retry_fan_out_test"

        # Check fan-out task has flaky_echo handler
        split = workflow.nodes["split"]
        assert split.type == NodeType.FAN_OUT
        assert split.task.handler == "flaky_echo"
        assert split.task.timeout_seconds == 30

    def test_retry_test_validates(self):
        """retry_test workflow passes structural validation."""
        from services.workflow_service import WorkflowService
        svc = WorkflowService()
        workflow = svc.get("retry_test")
        errors = workflow.validate_structure()
        assert errors == [], f"Validation errors: {errors}"

    def test_retry_fan_out_validates(self):
        """retry_fan_out_test workflow passes structural validation."""
        from services.workflow_service import WorkflowService
        svc = WorkflowService()
        workflow = svc.get("retry_fan_out_test")
        errors = workflow.validate_structure()
        assert errors == [], f"Validation errors: {errors}"
