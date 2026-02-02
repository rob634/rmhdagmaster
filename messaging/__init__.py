# ============================================================================
# MESSAGING MODULE
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Core - Azure Service Bus integration
# PURPOSE: Dispatch tasks to workers, receive results
# CREATED: 29 JAN 2026
# ============================================================================
"""
Messaging Module

Provides Azure Service Bus integration for task dispatch and result handling.

Usage:
    from messaging import TaskPublisher, get_publisher

    publisher = await get_publisher()
    await publisher.dispatch_task(task_message)
"""

from .publisher import TaskPublisher, get_publisher, close_publisher
from .config import MessagingConfig

__all__ = [
    "TaskPublisher",
    "get_publisher",
    "close_publisher",
    "MessagingConfig",
]
