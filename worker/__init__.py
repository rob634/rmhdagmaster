# ============================================================================
# WORKER MODULE
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Core - Worker execution components
# PURPOSE: Task execution, checkpoint management, progress tracking
# CREATED: 31 JAN 2026
# ============================================================================
"""
Worker Module

Components for task execution on worker nodes:
- contracts: Message schemas and worker configuration
- checkpoint: Checkpoint management for resumable tasks
- progress: Progress tracking and reporting
- executor: Task execution engine
- reporter: Result reporting (DB or HTTP)
- consumer: Service Bus message consumer
- main: Worker entry point
"""

from worker.checkpoint import (
    CheckpointManager,
    Checkpoint,
    CheckpointError,
)
from worker.progress import (
    ProgressTracker,
    ProgressReport,
    report_progress,
)
from worker.contracts import (
    DAGTaskMessage,
    DAGTaskResult,
    TaskResultStatus,
    TaskMessageType,
    WorkerConfig,
    detect_message_type,
)
from worker.executor import (
    TaskExecutor,
    BatchExecutor,
    ExecutionContext,
)
from worker.reporter import (
    ResultReporter,
    DatabaseReporter,
    HTTPReporter,
    create_reporter,
)
from worker.consumer import (
    ServiceBusConsumer,
    run_consumer,
)

__all__ = [
    # Checkpoint
    "CheckpointManager",
    "Checkpoint",
    "CheckpointError",
    # Progress
    "ProgressTracker",
    "ProgressReport",
    "report_progress",
    # Contracts
    "DAGTaskMessage",
    "DAGTaskResult",
    "TaskResultStatus",
    "TaskMessageType",
    "WorkerConfig",
    "detect_message_type",
    # Executor
    "TaskExecutor",
    "BatchExecutor",
    "ExecutionContext",
    # Reporter
    "ResultReporter",
    "DatabaseReporter",
    "HTTPReporter",
    "create_reporter",
    # Consumer
    "ServiceBusConsumer",
    "run_consumer",
]
