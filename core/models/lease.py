# ============================================================================
# ORCHESTRATOR LEASE MODEL
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Core - Lease-based orchestrator coordination
# PURPOSE: Table-based lease with TTL for crash-safe leader election
# CREATED: 03 FEB 2026
# ============================================================================
"""
Orchestrator Lease Model

Table-based lease with pulse/TTL for crash-safe orchestrator coordination.
Replaces advisory locks for Layer 1 (orchestrator lock) to handle container
crashes in Azure App Service Environment where TCP keepalive timeout can
take minutes to release advisory locks.

Key properties:
- Lease expires automatically if not renewed within TTL
- New orchestrator can acquire expired lease immediately
- Graceful shutdown releases lease explicitly
- Crash recovery in ~30 seconds (not minutes)
"""

from datetime import datetime
from typing import ClassVar, List

from pydantic import BaseModel, Field


class OrchestratorLease(BaseModel):
    """
    Orchestrator lease for leader election.

    Only one orchestrator can hold the lease at a time.
    The holder must send periodic pulses to keep the lease alive.
    If pulse_at + lease_ttl_sec < NOW(), the lease is considered expired
    and can be acquired by a new orchestrator.

    Table: dagapp.dag_orchestrator_lease (single row)
    """

    # SQL DDL Metadata
    __sql_table__: ClassVar[str] = "dag_orchestrator_lease"
    __sql_schema__: ClassVar[str] = "dagapp"
    __sql_primary_key__: ClassVar[List[str]] = ["lock_name"]

    lock_name: str = Field(
        default="orchestrator",
        max_length=64,
        description="Lock identifier (always 'orchestrator' for the main lock)"
    )
    holder_id: str = Field(
        max_length=64,
        description="UUID of the instance holding the lease"
    )
    acquired_at: datetime = Field(
        default_factory=datetime.utcnow,
        description="When the lease was first acquired by this holder"
    )
    pulse_at: datetime = Field(
        default_factory=datetime.utcnow,
        description="Last pulse timestamp - must be renewed within TTL"
    )
    lease_ttl_sec: int = Field(
        default=30,
        ge=10,
        le=300,
        description="Lease TTL in seconds - expires if no pulse within this time"
    )

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "lock_name": "orchestrator",
                    "holder_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
                    "acquired_at": "2026-02-03T12:00:00Z",
                    "pulse_at": "2026-02-03T12:05:30Z",
                    "lease_ttl_sec": 30,
                }
            ]
        }
    }

    def is_expired(self, now: datetime = None) -> bool:
        """
        Check if the lease has expired.

        Args:
            now: Current time (defaults to utcnow)

        Returns:
            True if lease has expired (pulse_at + ttl < now)
        """
        if now is None:
            now = datetime.utcnow()

        from datetime import timedelta
        expiry = self.pulse_at + timedelta(seconds=self.lease_ttl_sec)
        return now > expiry


# ============================================================================
# EXPORTS
# ============================================================================

__all__ = ['OrchestratorLease']
