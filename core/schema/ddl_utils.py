# ============================================================================
# CLAUDE CONTEXT - DDL UTILITIES
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Core - DRY utilities for SQL DDL generation
# PURPOSE: Index, trigger, comment, and constraint builders using psycopg.sql
# LAST_REVIEWED: 28 JAN 2026
# EXPORTS: IndexBuilder, TriggerBuilder, CommentBuilder, SchemaUtils, TYPE_MAP
# DEPENDENCIES: psycopg
# ============================================================================
"""
DDL Utilities - Shared SQL Generation Patterns.

Ported from rmhgeoapi for consistent DDL generation.
All methods return psycopg.sql.Composed objects for safe execution.
No string concatenation - full SQL composition for injection safety.

Usage:
    from core.schema.ddl_utils import IndexBuilder, TriggerBuilder

    # Create a B-tree index
    idx = IndexBuilder.btree('app', 'dag_jobs', ['status'])
    cursor.execute(idx)

    # Create updated_at trigger
    stmts = TriggerBuilder.updated_at('app', 'dag_jobs')
    for stmt in stmts:
        cursor.execute(stmt)
"""

from typing import List, Optional, Union, Sequence
from psycopg import sql


# ============================================================================
# TYPE MAPPING
# ============================================================================

TYPE_MAP = {
    # Python native types
    str: "VARCHAR",
    int: "INTEGER",
    float: "DOUBLE PRECISION",
    bool: "BOOLEAN",
    dict: "JSONB",
    list: "JSONB",

    # String representations
    'str': 'VARCHAR',
    'string': 'TEXT',
    'object': 'TEXT',
    'int': 'INTEGER',
    'int64': 'BIGINT',
    'float': 'DOUBLE PRECISION',
    'float64': 'DOUBLE PRECISION',
    'bool': 'BOOLEAN',
    'boolean': 'BOOLEAN',
    'datetime': 'TIMESTAMPTZ',
    'datetime64': 'TIMESTAMPTZ',

    # Explicit PostgreSQL types (pass-through)
    'TEXT': 'TEXT',
    'VARCHAR': 'VARCHAR',
    'INTEGER': 'INTEGER',
    'BIGINT': 'BIGINT',
    'BOOLEAN': 'BOOLEAN',
    'JSONB': 'JSONB',
    'TIMESTAMPTZ': 'TIMESTAMPTZ',
    'SERIAL': 'SERIAL',
}


def get_postgres_type(python_type) -> str:
    """
    Map Python type to PostgreSQL type.

    Args:
        python_type: Python type or string representation

    Returns:
        PostgreSQL type string
    """
    if python_type in TYPE_MAP:
        return TYPE_MAP[python_type]

    type_str = str(python_type).lower()
    for key, pg_type in TYPE_MAP.items():
        if isinstance(key, str) and key.lower() in type_str:
            return pg_type

    return 'TEXT'


# ============================================================================
# INDEX BUILDER
# ============================================================================

class IndexBuilder:
    """
    Builder for PostgreSQL index DDL statements.

    All methods are static and return sql.Composed objects.
    """

    @staticmethod
    def _normalize_columns(columns: Union[str, Sequence[str]]) -> List[str]:
        """Convert single column or sequence to list."""
        if isinstance(columns, str):
            return [columns]
        return list(columns)

    @staticmethod
    def _generate_index_name(
        table: str,
        columns: List[str],
        prefix: str = 'idx',
        suffix: str = ''
    ) -> str:
        """Generate conventional index name."""
        col_part = '_'.join(columns)
        name = f"{prefix}_{table}_{col_part}"
        if suffix:
            name = f"{name}_{suffix}"
        return name

    @staticmethod
    def btree(
        schema: str,
        table: str,
        columns: Union[str, Sequence[str]],
        name: Optional[str] = None,
        descending: bool = False,
        partial_where: Optional[str] = None
    ) -> sql.Composed:
        """
        Create B-tree index.

        Args:
            schema: Schema name
            table: Table name
            columns: Column name(s) to index
            name: Optional custom index name
            descending: If True, create DESC index
            partial_where: Optional WHERE clause for partial index

        Returns:
            sql.Composed CREATE INDEX statement
        """
        cols = IndexBuilder._normalize_columns(columns)
        idx_name = name or IndexBuilder._generate_index_name(
            table, cols, suffix='desc' if descending else ''
        )

        if descending:
            col_parts = [
                sql.SQL("{} DESC").format(sql.Identifier(c))
                for c in cols
            ]
        else:
            col_parts = [sql.Identifier(c) for c in cols]

        col_sql = sql.SQL(", ").join(col_parts)

        stmt = sql.SQL("CREATE INDEX IF NOT EXISTS {name} ON {schema}.{table} ({columns})").format(
            name=sql.Identifier(idx_name),
            schema=sql.Identifier(schema),
            table=sql.Identifier(table),
            columns=col_sql
        )

        if partial_where:
            stmt = sql.SQL("{} WHERE {}").format(stmt, sql.SQL(partial_where))

        return stmt

    @staticmethod
    def gin(
        schema: str,
        table: str,
        column: str,
        name: Optional[str] = None,
        partial_where: Optional[str] = None
    ) -> sql.Composed:
        """
        Create GIN index (for JSONB, arrays).

        Args:
            schema: Schema name
            table: Table name
            column: Column to index
            name: Optional custom index name
            partial_where: Optional WHERE clause

        Returns:
            sql.Composed CREATE INDEX statement
        """
        idx_name = name or IndexBuilder._generate_index_name(table, [column])

        stmt = sql.SQL(
            "CREATE INDEX IF NOT EXISTS {name} ON {schema}.{table} USING GIN ({column})"
        ).format(
            name=sql.Identifier(idx_name),
            schema=sql.Identifier(schema),
            table=sql.Identifier(table),
            column=sql.Identifier(column)
        )

        if partial_where:
            stmt = sql.SQL("{} WHERE {}").format(stmt, sql.SQL(partial_where))

        return stmt

    @staticmethod
    def unique(
        schema: str,
        table: str,
        columns: Union[str, Sequence[str]],
        name: Optional[str] = None,
        partial_where: Optional[str] = None
    ) -> sql.Composed:
        """
        Create unique index.

        Args:
            schema: Schema name
            table: Table name
            columns: Column name(s) for unique constraint
            name: Optional custom index name
            partial_where: Optional WHERE clause

        Returns:
            sql.Composed CREATE UNIQUE INDEX statement
        """
        cols = IndexBuilder._normalize_columns(columns)
        idx_name = name or IndexBuilder._generate_index_name(table, cols, prefix='idx_unique')

        col_sql = sql.SQL(", ").join(sql.Identifier(c) for c in cols)

        stmt = sql.SQL(
            "CREATE UNIQUE INDEX IF NOT EXISTS {name} ON {schema}.{table} ({columns})"
        ).format(
            name=sql.Identifier(idx_name),
            schema=sql.Identifier(schema),
            table=sql.Identifier(table),
            columns=col_sql
        )

        if partial_where:
            stmt = sql.SQL("{} WHERE {}").format(stmt, sql.SQL(partial_where))

        return stmt


# ============================================================================
# TRIGGER BUILDER
# ============================================================================

class TriggerBuilder:
    """
    Builder for PostgreSQL trigger DDL statements.
    """

    @staticmethod
    def updated_at_function(schema: str) -> sql.Composed:
        """
        Create the update_updated_at_column() trigger function.
        """
        return sql.SQL("""
            CREATE OR REPLACE FUNCTION {schema}.update_updated_at_column()
            RETURNS TRIGGER
            LANGUAGE plpgsql
            AS $$
            BEGIN
                NEW.updated_at = NOW();
                RETURN NEW;
            END;
            $$
        """).format(schema=sql.Identifier(schema))

    @staticmethod
    def updated_at_trigger(
        schema: str,
        table: str,
        trigger_name: Optional[str] = None
    ) -> List[sql.Composed]:
        """
        Create trigger that calls update_updated_at_column() on UPDATE.

        Returns DROP + CREATE for idempotency.
        """
        trig_name = trigger_name or f"trg_{table}_updated_at"

        drop_stmt = sql.SQL("DROP TRIGGER IF EXISTS {name} ON {schema}.{table}").format(
            name=sql.Identifier(trig_name),
            schema=sql.Identifier(schema),
            table=sql.Identifier(table)
        )

        create_stmt = sql.SQL("""
            CREATE TRIGGER {name}
            BEFORE UPDATE ON {schema}.{table}
            FOR EACH ROW
            EXECUTE FUNCTION {schema}.update_updated_at_column()
        """).format(
            name=sql.Identifier(trig_name),
            schema=sql.Identifier(schema),
            table=sql.Identifier(table)
        )

        return [drop_stmt, create_stmt]

    @staticmethod
    def updated_at(schema: str, table: str) -> List[sql.Composed]:
        """
        Create complete updated_at trigger setup for a table.
        """
        stmts = [TriggerBuilder.updated_at_function(schema)]
        stmts.extend(TriggerBuilder.updated_at_trigger(schema, table))
        return stmts


# ============================================================================
# COMMENT BUILDER
# ============================================================================

class CommentBuilder:
    """
    Builder for PostgreSQL COMMENT statements.
    """

    @staticmethod
    def table(schema: str, table: str, comment: str) -> sql.Composed:
        """Add comment to table."""
        return sql.SQL("COMMENT ON TABLE {}.{} IS {}").format(
            sql.Identifier(schema),
            sql.Identifier(table),
            sql.Literal(comment)
        )

    @staticmethod
    def column(schema: str, table: str, column: str, comment: str) -> sql.Composed:
        """Add comment to column."""
        return sql.SQL("COMMENT ON COLUMN {}.{}.{} IS {}").format(
            sql.Identifier(schema),
            sql.Identifier(table),
            sql.Identifier(column),
            sql.Literal(comment)
        )


# ============================================================================
# SCHEMA UTILITIES
# ============================================================================

class SchemaUtils:
    """
    Utility methods for schema-level DDL operations.
    """

    @staticmethod
    def create_schema(schema: str, comment: Optional[str] = None) -> List[sql.Composed]:
        """
        Create schema with optional comment.
        """
        stmts = [
            sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                sql.Identifier(schema)
            )
        ]

        if comment:
            stmts.append(CommentBuilder.table(schema, schema, comment))

        return stmts

    @staticmethod
    def set_search_path(schema: str, include_public: bool = True) -> sql.Composed:
        """
        Set search_path to include schema.
        """
        if include_public:
            return sql.SQL("SET search_path TO {}, public").format(
                sql.Identifier(schema)
            )
        else:
            return sql.SQL("SET search_path TO {}").format(
                sql.Identifier(schema)
            )


# ============================================================================
# EXPORTS
# ============================================================================

__all__ = [
    'TYPE_MAP',
    'get_postgres_type',
    'IndexBuilder',
    'TriggerBuilder',
    'CommentBuilder',
    'SchemaUtils',
]
