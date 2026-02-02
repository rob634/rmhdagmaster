# ============================================================================
# WORKFLOW SERVICE
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Core - Workflow definition management
# PURPOSE: Load and cache workflow definitions
# CREATED: 29 JAN 2026
# ============================================================================
"""
Workflow Service

Loads workflow definitions from YAML files and provides
lookup capabilities. Caches loaded workflows.

Workflow files are stored in the workflows/ directory.
"""

import logging
import os
from pathlib import Path
from typing import Dict, Optional, List
import yaml

from core.models import WorkflowDefinition, NodeDefinition, NodeType

logger = logging.getLogger(__name__)


class WorkflowService:
    """Service for loading and managing workflow definitions."""

    def __init__(self, workflows_dir: Optional[str] = None):
        """
        Initialize workflow service.

        Args:
            workflows_dir: Directory containing workflow YAML files.
                          Defaults to ./workflows/
        """
        if workflows_dir:
            self.workflows_dir = Path(workflows_dir)
        else:
            self.workflows_dir = Path(__file__).parent.parent / "workflows"

        self._cache: Dict[str, WorkflowDefinition] = {}
        self._loaded = False

    def load_all(self) -> int:
        """
        Load all workflow definitions from the workflows directory.

        Returns:
            Number of workflows loaded
        """
        if not self.workflows_dir.exists():
            logger.warning(f"Workflows directory not found: {self.workflows_dir}")
            return 0

        count = 0
        for yaml_file in self.workflows_dir.glob("*.yaml"):
            try:
                workflow = self._load_yaml(yaml_file)
                self._cache[workflow.workflow_id] = workflow
                count += 1
                logger.info(f"Loaded workflow: {workflow.workflow_id} v{workflow.version}")
            except Exception as e:
                logger.error(f"Failed to load {yaml_file}: {e}")

        # Also load .yml files
        for yaml_file in self.workflows_dir.glob("*.yml"):
            try:
                workflow = self._load_yaml(yaml_file)
                self._cache[workflow.workflow_id] = workflow
                count += 1
                logger.info(f"Loaded workflow: {workflow.workflow_id} v{workflow.version}")
            except Exception as e:
                logger.error(f"Failed to load {yaml_file}: {e}")

        self._loaded = True
        logger.info(f"Loaded {count} workflows from {self.workflows_dir}")
        return count

    def get(self, workflow_id: str) -> Optional[WorkflowDefinition]:
        """
        Get a workflow definition by ID.

        Args:
            workflow_id: Workflow identifier

        Returns:
            WorkflowDefinition or None if not found
        """
        if not self._loaded:
            self.load_all()

        return self._cache.get(workflow_id)

    def get_or_raise(self, workflow_id: str) -> WorkflowDefinition:
        """
        Get a workflow definition, raising if not found.

        Args:
            workflow_id: Workflow identifier

        Returns:
            WorkflowDefinition

        Raises:
            KeyError if workflow not found
        """
        workflow = self.get(workflow_id)
        if workflow is None:
            raise KeyError(f"Workflow not found: {workflow_id}")
        return workflow

    def list_all(self) -> List[WorkflowDefinition]:
        """
        List all loaded workflows.

        Returns:
            List of WorkflowDefinition instances
        """
        if not self._loaded:
            self.load_all()

        return list(self._cache.values())

    def register(self, workflow: WorkflowDefinition) -> None:
        """
        Register a workflow definition (for testing or programmatic use).

        Args:
            workflow: WorkflowDefinition to register
        """
        # Validate before registering
        errors = workflow.validate_structure()
        if errors:
            raise ValueError(f"Invalid workflow: {errors}")

        self._cache[workflow.workflow_id] = workflow
        logger.info(f"Registered workflow: {workflow.workflow_id}")

    def _load_yaml(self, path: Path) -> WorkflowDefinition:
        """
        Load a workflow from YAML file.

        Args:
            path: Path to YAML file

        Returns:
            WorkflowDefinition instance
        """
        with open(path) as f:
            data = yaml.safe_load(f)

        # Convert node dicts to NodeDefinition objects
        if "nodes" in data:
            nodes = {}
            for node_id, node_data in data["nodes"].items():
                # Handle node type
                if "type" in node_data:
                    node_data["type"] = NodeType(node_data["type"])
                nodes[node_id] = NodeDefinition(**node_data)
            data["nodes"] = nodes

        workflow = WorkflowDefinition(**data)

        # Validate structure
        errors = workflow.validate_structure()
        if errors:
            raise ValueError(f"Invalid workflow in {path}: {errors}")

        return workflow

    def reload(self) -> int:
        """
        Reload all workflows from disk.

        Returns:
            Number of workflows loaded
        """
        self._cache.clear()
        self._loaded = False
        return self.load_all()
