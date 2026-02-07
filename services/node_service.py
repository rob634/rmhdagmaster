# ============================================================================
# NODE SERVICE
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Core - Node state management
# PURPOSE: Manage node transitions, check dependencies, handle results
# CREATED: 29 JAN 2026
# ============================================================================
"""
Node Service

Manages node state transitions:
- Check dependencies and mark nodes READY
- Dispatch nodes (mark DISPATCHED)
- Process task results (mark COMPLETED/FAILED)
- Handle retries
"""

import logging
from datetime import datetime
from typing import Dict, Any, Optional, List, Set, Tuple

from psycopg_pool import AsyncConnectionPool

from core.models import NodeState, TaskResult, WorkflowDefinition, NodeDefinition, NodeType
from core.contracts import NodeStatus, TaskStatus
from repositories import NodeRepository, TaskResultRepository
from .workflow_service import WorkflowService

logger = logging.getLogger(__name__)


class NodeService:
    """Service for node state management."""

    def __init__(
        self,
        pool: AsyncConnectionPool,
        workflow_service: WorkflowService,
        event_service: "EventService" = None,
    ):
        """
        Initialize node service.

        Args:
            pool: Database connection pool
            workflow_service: Workflow definition service
            event_service: Optional event service for timeline logging
        """
        self.pool = pool
        self.workflow_service = workflow_service
        self.node_repo = NodeRepository(pool)
        self.task_repo = TaskResultRepository(pool)
        self._event_service = event_service

    async def get_ready_nodes(self, job_id: str) -> List[NodeState]:
        """
        Get nodes that are ready for dispatch.

        Args:
            job_id: Job identifier

        Returns:
            List of READY nodes
        """
        return await self.node_repo.get_ready_nodes(job_id)

    async def check_and_ready_nodes(
        self,
        job_id: str,
        workflow: WorkflowDefinition,
    ) -> List[NodeState]:
        """
        Check pending nodes and mark them READY if dependencies are met.

        Uses optimistic locking: if a version conflict occurs, the node
        is skipped for this cycle (will be retried next cycle).

        Args:
            job_id: Job identifier
            workflow: Workflow definition for dependency info

        Returns:
            List of nodes that were successfully transitioned to READY
        """
        # Get all nodes for this job
        all_nodes = await self.node_repo.get_all_for_job(job_id)
        nodes_by_id = {n.node_id: n for n in all_nodes}

        # Find completed node IDs
        # For FAN_OUT nodes: only count as completed if ALL children are also terminal
        completed_nodes: Set[str] = set()
        for n in all_nodes:
            if n.status not in (NodeStatus.COMPLETED, NodeStatus.SKIPPED):
                continue
            # Check if this is a FAN_OUT node with pending children
            node_def = workflow.nodes.get(n.node_id)
            if node_def and node_def.type == NodeType.FAN_OUT:
                children_done = await self.node_repo.all_children_terminal(
                    job_id, n.node_id
                )
                if children_done:
                    completed_nodes.add(n.node_id)
                # else: FAN_OUT stays out of completed_nodes, blocking FAN_IN
            else:
                completed_nodes.add(n.node_id)

        # Check each pending node
        newly_ready: List[NodeState] = []
        pending_nodes = await self.node_repo.get_pending_nodes(job_id)

        for node in pending_nodes:
            node_def = workflow.nodes.get(node.node_id)
            if node_def is None:
                continue

            # Check if dependencies are met
            if self._dependencies_met(node.node_id, node_def, completed_nodes, workflow):
                node.mark_ready()

                # Optimistic update with version check (Layer 3)
                success = await self.node_repo.update(node)
                if success:
                    newly_ready.append(node)
                    logger.debug(f"Node {node.node_id} is now READY")

                    # Emit NODE_READY event
                    if self._event_service:
                        deps = list(completed_nodes) if completed_nodes else []
                        await self._event_service.emit_node_ready(node, deps)
                else:
                    # Version conflict - another process modified this node
                    # This is fine - will retry next cycle
                    logger.debug(
                        f"Version conflict marking node {node.node_id} ready, "
                        f"will retry next cycle"
                    )

        return newly_ready

    async def mark_dispatched(
        self,
        job_id: str,
        node_id: str,
        task_id: str,
    ) -> bool:
        """
        Mark a node as dispatched.

        Args:
            job_id: Job identifier
            node_id: Node identifier
            task_id: Assigned task ID

        Returns:
            True if update succeeded
        """
        success = await self.node_repo.mark_dispatched(job_id, node_id, task_id)
        if success:
            logger.info(f"Node {node_id} dispatched as task {task_id}")
        return success

    async def process_task_result(
        self,
        result: TaskResult,
        workflow: WorkflowDefinition,
    ) -> NodeState:
        """
        Process a task result and update node state.

        Args:
            result: TaskResult from worker
            workflow: Workflow definition

        Returns:
            Updated NodeState
        """
        node = await self.node_repo.get(result.job_id, result.node_id)
        if node is None:
            raise ValueError(
                f"Node not found: {result.job_id}/{result.node_id}"
            )

        if result.status == TaskStatus.COMPLETED:
            # Ensure node transitions through RUNNING before COMPLETED
            if node.status == NodeStatus.DISPATCHED:
                node.mark_running()
            node.mark_completed(result.output)
            logger.info(f"Node {node.node_id} completed successfully")

            # Emit NODE_COMPLETED event
            if self._event_service:
                output_keys = list(result.output.keys()) if result.output else []
                duration_ms = result.execution_duration_ms
                await self._event_service.emit_node_completed(node, duration_ms, output_keys)

        elif result.status == TaskStatus.FAILED:
            node.mark_failed(result.error_message or "Unknown error")
            logger.warning(f"Node {node.node_id} failed: {result.error_message}")

            # Check if we should retry
            can_retry = node.retry_count < node.max_retries

            # Emit NODE_FAILED event
            if self._event_service:
                await self._event_service.emit_node_failed(
                    node, result.error_message or "Unknown error", can_retry
                )

            # Automatic retry: if retries remaining, prepare for retry
            if can_retry:
                if node.prepare_retry():
                    logger.info(
                        f"Node {node.node_id} prepared for retry "
                        f"(attempt {node.retry_count}/{node.max_retries})"
                    )

                    # Emit NODE_RETRY event
                    if self._event_service:
                        await self._event_service.emit_node_retry(node)

        else:
            # RUNNING status - update started_at
            if node.status == NodeStatus.DISPATCHED:
                node.mark_running()
                logger.debug(f"Node {node.node_id} is now running")

                # Emit NODE_STARTED event
                if self._event_service:
                    await self._event_service.emit_node_started(
                        node.job_id, node.node_id, node.task_id, result.worker_id
                    )

        # Update node with version check (Layer 3)
        success = await self.node_repo.update(node)
        if not success:
            # Version conflict - this is rare for task results since the node
            # should be in DISPATCHED/RUNNING state which isn't normally modified
            # by other processes. Log warning and don't mark result as processed
            # so it will be retried.
            logger.warning(
                f"Version conflict updating node {node.node_id} with task result, "
                f"will retry on next cycle"
            )
            return node

        # Mark the task result as processed
        await self.task_repo.mark_processed(result.task_id)

        return node

    async def retry_failed_node(
        self,
        job_id: str,
        node_id: str,
    ) -> Optional[NodeState]:
        """
        Retry a failed node if retries remaining.

        Args:
            job_id: Job identifier
            node_id: Node identifier

        Returns:
            Updated NodeState if retry prepared, None if max retries exceeded
        """
        node = await self.node_repo.get(job_id, node_id)
        if node is None:
            return None

        if node.prepare_retry():
            success = await self.node_repo.update(node)
            if success:
                logger.info(
                    f"Node {node_id} prepared for retry "
                    f"(attempt {node.retry_count}/{node.max_retries})"
                )
                return node
            else:
                logger.warning(
                    f"Version conflict preparing retry for node {node_id}"
                )
                return None

        logger.warning(f"Node {node_id} has no retries remaining")
        return None

    async def get_node_summary(self, job_id: str) -> Dict[str, int]:
        """
        Get summary of node statuses for a job.

        Args:
            job_id: Job identifier

        Returns:
            Dict mapping status to count
        """
        return await self.node_repo.count_by_status(job_id)

    async def get_unprocessed_results(self, limit: int = 100) -> List[TaskResult]:
        """
        Get unprocessed task results.

        The orchestration loop calls this to find work.

        Args:
            limit: Maximum results

        Returns:
            List of unprocessed TaskResult instances
        """
        return await self.task_repo.get_unprocessed(limit)

    def _dependencies_met(
        self,
        node_id: str,
        node_def: NodeDefinition,
        completed_nodes: Set[str],
        workflow: WorkflowDefinition,
    ) -> bool:
        """
        Check if a node's dependencies are met.

        Dependencies come from:
        1. Explicit depends_on in node definition
        2. Implicit from 'next' pointers (reverse lookup)

        Args:
            node_id: ID of the node being checked
            node_def: Node definition from workflow
            completed_nodes: Set of node IDs that are completed/skipped
            workflow: Workflow definition

        Returns:
            True if all dependencies are satisfied
        """
        # Dynamic nodes (created by fan-out) have no graph-level dependencies
        # Their "dependency" (the fan-out expansion) was satisfied at creation time
        if node_id not in workflow.nodes:
            return True

        # Check explicit dependencies
        if node_def.depends_on:
            if node_def.depends_on.all_of:
                # All listed nodes must be complete
                if not all(n in completed_nodes for n in node_def.depends_on.all_of):
                    return False

            if node_def.depends_on.any_of:
                # At least one must be complete
                if not any(n in completed_nodes for n in node_def.depends_on.any_of):
                    return False

            return True

        # Check implicit dependencies (nodes that point to this one via 'next')
        # Build list of nodes that must complete before this node
        implicit_deps: Set[str] = set()
        for other_id, other_def in workflow.nodes.items():
            if other_def.next:
                next_nodes = (
                    [other_def.next]
                    if isinstance(other_def.next, str)
                    else other_def.next
                )
                # If other_node.next points to us, we depend on other_node
                if node_id in next_nodes:
                    implicit_deps.add(other_id)

            # Also check conditional branches
            if other_def.branches:
                for branch in other_def.branches:
                    if branch.next == node_id:
                        implicit_deps.add(other_id)

        # If we have implicit dependencies, check they're all completed
        if implicit_deps:
            if not all(dep in completed_nodes for dep in implicit_deps):
                return False

        return True

    def get_next_nodes(
        self,
        node_id: str,
        workflow: WorkflowDefinition,
        output: Optional[Dict[str, Any]] = None,
    ) -> List[str]:
        """
        Get the next nodes to execute after a node completes.

        Handles conditional routing based on output.

        Args:
            node_id: Completed node ID
            workflow: Workflow definition
            output: Output from the completed node

        Returns:
            List of next node IDs
        """
        node_def = workflow.nodes.get(node_id)
        if node_def is None:
            return []

        # Handle conditional nodes
        if node_def.type.value == "conditional" and node_def.branches:
            return self._evaluate_condition(node_def, output)

        # Handle regular 'next' pointer
        if node_def.next:
            if isinstance(node_def.next, str):
                return [node_def.next]
            return node_def.next

        return []

    def _evaluate_condition(
        self,
        node_def: NodeDefinition,
        output: Optional[Dict[str, Any]],
    ) -> List[str]:
        """
        Evaluate conditional branches and return the matching branch target.

        Uses ConditionEvaluator for proper condition parsing and evaluation.

        Args:
            node_def: Conditional node definition
            output: Output from the node that feeds into the conditional

        Returns:
            List with single next node ID, or empty if no match
        """
        if not node_def.branches or not node_def.condition_field:
            return []

        # Get the value to evaluate from the condition_field
        # The condition_field may be a template like "size_mb" or a direct field name
        value = None
        if output:
            # Try direct field access first
            if node_def.condition_field in output:
                value = output[node_def.condition_field]
            else:
                # Try dot notation (e.g., "metadata.size")
                parts = node_def.condition_field.split(".")
                current = output
                for part in parts:
                    if isinstance(current, dict) and part in current:
                        current = current[part]
                    else:
                        current = None
                        break
                value = current

        # Create condition evaluator (local import to avoid circular dependency)
        from orchestrator.engine.evaluator import ConditionEvaluator
        evaluator = ConditionEvaluator()

        # Evaluate branches in order, first match wins
        default_branch = None
        for branch in node_def.branches:
            if branch.default:
                default_branch = branch.next
                continue

            if branch.condition and value is not None:
                # Build condition string: "value <condition>"
                # Conditions are like "< 100", ">= 1000"
                condition_str = branch.condition.strip()

                # Check if condition already has the comparison format
                # or if we need to prepend the value
                if condition_str.startswith(("<", ">", "=", "!")):
                    # Condition like "< 100" - compare value against RHS
                    full_condition = f"value {condition_str}"
                    context = {"value": value}
                else:
                    # Condition might be a direct expression
                    full_condition = condition_str
                    context = {"value": value, "output": output or {}}

                if evaluator.evaluate(full_condition, context):
                    logger.debug(
                        f"Conditional branch matched: {branch.name or branch.next} "
                        f"(condition: {condition_str}, value: {value})"
                    )
                    return [branch.next]

        # No condition matched, use default
        if default_branch:
            logger.debug(f"Conditional using default branch: {default_branch}")
            return [default_branch]

        logger.warning(
            f"No branch matched for conditional "
            f"(condition_field: {node_def.condition_field}, value: {value})"
        )
        return []

    async def evaluate_and_route_conditional(
        self,
        job_id: str,
        conditional_node_id: str,
        workflow: WorkflowDefinition,
        job_params: Dict[str, Any],
        node_outputs: Dict[str, Dict[str, Any]],
    ) -> Tuple[Optional[str], List[str]]:
        """
        Evaluate a conditional node and determine routing.

        This is called when a conditional node's dependencies are met.
        Conditionals don't dispatch to workers - they evaluate immediately.

        Args:
            job_id: Job identifier
            conditional_node_id: The conditional node to evaluate
            workflow: Workflow definition
            job_params: Job input parameters (for template resolution)
            node_outputs: Outputs from completed nodes (for template resolution)

        Returns:
            Tuple of (taken_branch_node_id, list_of_skipped_node_ids)
        """
        node_def = workflow.nodes.get(conditional_node_id)
        if not node_def or node_def.type.value != "conditional":
            return None, []

        # Resolve condition_field template if needed
        # Local imports to avoid circular dependency
        from orchestrator.engine.templates import TemplateContext, get_resolver

        condition_field = node_def.condition_field
        if condition_field and "{{" in condition_field:
            # Resolve the template to get actual field path
            context = TemplateContext.from_job(job_params, node_outputs)
            resolved = get_resolver().resolve(
                {"field": condition_field}, context
            )
            condition_field = resolved.get("field", condition_field)

        # Get the output from the node that feeds into this conditional
        # This is typically the node that points to this conditional via 'next'
        upstream_output = self._get_upstream_output(
            conditional_node_id, workflow, node_outputs
        )

        # Temporarily set resolved condition_field for evaluation
        original_field = node_def.condition_field
        # Extract just the field name if it's a template like {{ nodes.validate.output.size_mb }}
        if original_field and "output." in original_field:
            # Extract field name after 'output.'
            import re
            match = re.search(r'output\.(\w+)', original_field)
            if match:
                node_def.condition_field = match.group(1)

        # Evaluate which branch to take
        taken_branches = self._evaluate_condition(node_def, upstream_output)

        # Restore original
        node_def.condition_field = original_field

        taken_branch = taken_branches[0] if taken_branches else None

        # Determine which branches to skip
        skipped_nodes = []
        if taken_branch and node_def.branches:
            all_branch_targets = {b.next for b in node_def.branches if b.next}
            untaken_targets = all_branch_targets - {taken_branch}
            skipped_nodes = list(untaken_targets)

        return taken_branch, skipped_nodes

    def _get_upstream_output(
        self,
        node_id: str,
        workflow: WorkflowDefinition,
        node_outputs: Dict[str, Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        """
        Get output from the node that feeds into a given node.

        Looks for nodes that have this node as their 'next' target.
        """
        for other_id, other_def in workflow.nodes.items():
            if other_def.next:
                next_nodes = (
                    [other_def.next] if isinstance(other_def.next, str)
                    else other_def.next
                )
                if node_id in next_nodes:
                    return node_outputs.get(other_id)
        return None

    async def skip_untaken_branches(
        self,
        job_id: str,
        nodes_to_skip: List[str],
        workflow: WorkflowDefinition,
    ) -> List[str]:
        """
        Mark nodes as SKIPPED when they're on untaken conditional branches.

        Also recursively skips downstream nodes that are ONLY reachable
        via the skipped nodes (not reachable via any other path).

        Args:
            job_id: Job identifier
            nodes_to_skip: Initial list of node IDs to skip (branch targets)
            workflow: Workflow definition

        Returns:
            List of all node IDs that were skipped
        """
        if not nodes_to_skip:
            return []

        # Get all nodes for this job
        all_nodes = await self.node_repo.get_all_for_job(job_id)
        nodes_by_id = {n.node_id: n for n in all_nodes}

        # Build dependency graph (local import to avoid circular dependency)
        from orchestrator.engine.evaluator import GraphBuilder
        graph_builder = GraphBuilder()
        graph = graph_builder.build(workflow)

        # Find all nodes exclusively reachable via the nodes to skip
        all_to_skip = set(nodes_to_skip)
        all_to_skip.update(
            self._find_exclusive_descendants(nodes_to_skip, workflow, graph)
        )

        # Skip each node
        skipped = []
        for node_id in all_to_skip:
            node = nodes_by_id.get(node_id)
            if node and node.status == NodeStatus.PENDING:
                node.mark_skipped()
                success = await self.node_repo.update(node)
                if success:
                    skipped.append(node_id)
                    logger.info(f"Skipped node {node_id} (untaken branch)")

                    # Emit NODE_SKIPPED event
                    if self._event_service:
                        await self._event_service.emit_node_skipped(
                            node, reason="Conditional branch not taken"
                        )

        return skipped

    def _find_exclusive_descendants(
        self,
        root_nodes: List[str],
        workflow: WorkflowDefinition,
        graph,
    ) -> Set[str]:
        """
        Find nodes that are ONLY reachable via the given root nodes.

        A node is "exclusive" if removing the root nodes from the graph
        would make it unreachable from START.

        Args:
            root_nodes: Nodes being skipped
            workflow: Workflow definition
            graph: Pre-built dependency graph

        Returns:
            Set of node IDs that are exclusively reachable via root_nodes
        """
        root_set = set(root_nodes)

        # Find all nodes reachable from START without going through root_nodes
        start_node = workflow.get_start_node()
        reachable_without_roots = self._find_reachable(
            start_node, workflow, exclude=root_set
        )

        # All nodes that are NOT reachable without roots should be skipped
        # (except the roots themselves which are handled separately)
        all_node_ids = set(workflow.nodes.keys())
        exclusive = all_node_ids - reachable_without_roots - root_set

        # Don't skip END nodes
        end_nodes = set(workflow.get_end_nodes())
        exclusive -= end_nodes

        return exclusive

    def _find_reachable(
        self,
        start_node: str,
        workflow: WorkflowDefinition,
        exclude: Set[str],
    ) -> Set[str]:
        """
        Find all nodes reachable from start_node, excluding certain nodes.

        Uses BFS traversal following 'next' pointers and branch targets.
        """
        reachable = set()
        queue = [start_node]

        while queue:
            node_id = queue.pop(0)

            if node_id in reachable or node_id in exclude:
                continue

            reachable.add(node_id)

            node_def = workflow.nodes.get(node_id)
            if not node_def:
                continue

            # Follow 'next' pointers
            if node_def.next:
                next_nodes = (
                    [node_def.next] if isinstance(node_def.next, str)
                    else node_def.next
                )
                queue.extend(next_nodes)

            # Follow branch targets
            if node_def.branches:
                for branch in node_def.branches:
                    if branch.next:
                        queue.append(branch.next)

        return reachable
