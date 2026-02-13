# ============================================================================
# CLAUDE CONTEXT - METADATA QUERY REPOSITORY (GATEWAY)
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Gateway - Read-only queries for layer metadata
# PURPOSE: Database access for layer metadata read endpoints
# CREATED: 13 FEB 2026
# ============================================================================
"""
Metadata Query Repository (Gateway)

Read-only repository for layer metadata queries.
The gateway NEVER writes to the database — all mutations go through
the orchestrator's domain HTTP API via OrchestratorClient.

Uses dict_row (from base class).
"""

import logging
from typing import Any, Dict, List, Optional

from function.repositories.base import FunctionRepository

logger = logging.getLogger(__name__)


class MetadataQueryRepository(FunctionRepository):
    """Read-only repository for layer metadata queries."""

    TABLE_METADATA = "dagapp.layer_metadata"

    def get_metadata(self, asset_id: str) -> Optional[Dict[str, Any]]:
        """Get layer metadata by asset_id."""
        return self.execute_one(
            f"SELECT * FROM {self.TABLE_METADATA} WHERE asset_id = %s",
            (asset_id,),
        )

    def list_metadata(
        self,
        data_type: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """List layer metadata with optional data_type filter."""
        conditions: List[str] = []
        params: List[Any] = []

        if data_type:
            conditions.append("data_type = %s")
            params.append(data_type)

        where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""

        query = f"""
            SELECT * FROM {self.TABLE_METADATA}
            {where_clause}
            ORDER BY sort_order, display_name
            LIMIT %s OFFSET %s
        """
        params.extend([limit, offset])
        return self.execute_many(query, tuple(params))

    def count_metadata(self, data_type: Optional[str] = None) -> int:
        """Count layer metadata entries with optional data_type filter."""
        conditions: List[str] = []
        params: List[Any] = []

        if data_type:
            conditions.append("data_type = %s")
            params.append(data_type)

        where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""

        return self.execute_count(
            f"SELECT COUNT(*) AS count FROM {self.TABLE_METADATA} {where_clause}",
            tuple(params),
        )


__all__ = ["MetadataQueryRepository"]
