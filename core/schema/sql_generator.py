# ============================================================================
# CLAUDE CONTEXT - PYDANTIC TO SQL GENERATOR
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# STATUS: Core - DDL generation from Pydantic models
# PURPOSE: Generate PostgreSQL CREATE statements from Pydantic models
# LAST_REVIEWED: 02 FEB 2026
# EXPORTS: PydanticToSQL
# DEPENDENCIES: pydantic, psycopg
# ============================================================================
"""
Pydantic to PostgreSQL Schema Generator.

Generates PostgreSQL DDL statements from Pydantic models.
Pydantic models are the SINGLE SOURCE OF TRUTH for schema.

Model Metadata Convention:
    Models define SQL metadata via ClassVar attributes:
    - __sql_table__: Table name
    - __sql_schema__: Schema name
    - __sql_primary_key__: Primary key column(s) - string or list
    - __sql_foreign_keys__: Dict of {column: "schema.table(column)"}
    - __sql_indexes__: List of index definitions
    - __sql_serial_columns__: Columns that should be SERIAL

Usage:
    generator = PydanticToSQL(schema_name="app")
    statements = generator.generate_all()
    for stmt in statements:
        cursor.execute(stmt)
"""

import re
import logging
from typing import Dict, List, Optional, Type, Any, Union, get_args, get_origin
from datetime import datetime
from enum import Enum
from pydantic import BaseModel
from pydantic.fields import FieldInfo
from psycopg import sql
from annotated_types import MaxLen

from core.schema.ddl_utils import IndexBuilder, TriggerBuilder, SchemaUtils

# Setup logger
logger = logging.getLogger(__name__)


class PydanticToSQL:
    """
    Convert Pydantic models to PostgreSQL DDL statements.

    Analyzes Pydantic models with __sql_* metadata and generates
    corresponding PostgreSQL CREATE TABLE statements.
    """

    TYPE_MAP = {
        str: "VARCHAR",
        int: "INTEGER",
        float: "DOUBLE PRECISION",
        bool: "BOOLEAN",
        datetime: "TIMESTAMPTZ",
        dict: "JSONB",
        Dict: "JSONB",
        list: "JSONB",
        List: "JSONB",
    }

    def __init__(self, schema_name: str = "app", destructive: bool = False):
        """
        Initialize the generator.

        Args:
            schema_name: Default PostgreSQL schema name
            destructive: If True, use DROP+CREATE for enums (data loss risk).
                        If False (default), use CREATE IF NOT EXISTS (safe).
        """
        self.schema_name = schema_name
        self.destructive = destructive
        self.enums: Dict[str, Type[Enum]] = {}

    # =========================================================================
    # METADATA EXTRACTION
    # =========================================================================

    @staticmethod
    def get_model_metadata(model: Type[BaseModel]) -> Dict[str, Any]:
        """
        Extract SQL DDL metadata from a Pydantic model.

        Looks for __sql_* attributes (which Python mangles to _ClassName__sql_*).

        Args:
            model: Pydantic model class

        Returns:
            Dict with table, schema, primary_key, foreign_keys, indexes, serial_columns
        """
        # Try both mangled and unmangled attribute names
        def get_attr(name: str, default=None):
            mangled = f"_{model.__name__}__{name}"
            return getattr(model, mangled, getattr(model, f"__{name}", getattr(model, name, default)))

        metadata = {
            "table": get_attr("sql_table__", get_attr("sql_table")),
            "schema": get_attr("sql_schema__", get_attr("sql_schema", "app")),
            "primary_key": get_attr("sql_primary_key__", get_attr("sql_primary_key", [])),
            "foreign_keys": get_attr("sql_foreign_keys__", get_attr("sql_foreign_keys", {})),
            "indexes": get_attr("sql_indexes__", get_attr("sql_indexes", [])),
            "serial_columns": get_attr("sql_serial_columns__", get_attr("sql_serial_columns", [])),
        }

        # Normalize primary_key to list
        if isinstance(metadata["primary_key"], str):
            metadata["primary_key"] = [metadata["primary_key"]]

        return metadata

    # =========================================================================
    # TYPE CONVERSION
    # =========================================================================

    def python_type_to_sql(self, field_type: Type, field_info: FieldInfo) -> str:
        """
        Convert Python type to PostgreSQL type.

        Args:
            field_type: Python type from Pydantic model
            field_info: Pydantic field information

        Returns:
            PostgreSQL type string
        """
        actual_type = field_type
        origin = get_origin(field_type)

        # Unwrap Optional/Union
        if origin is Union:
            args = get_args(field_type)
            if type(None) in args:
                actual_type = args[0] if args[0] is not type(None) else args[1]
        elif origin in (dict, Dict):
            return "JSONB"
        elif origin in (list, List):
            return "JSONB"

        # Handle string with max_length
        if actual_type == str:
            max_length = None
            if hasattr(field_info, 'metadata') and field_info.metadata:
                for constraint in field_info.metadata:
                    if isinstance(constraint, MaxLen):
                        max_length = constraint.max_length
                        break
            if max_length:
                return f"VARCHAR({max_length})"
            return "VARCHAR"

        # Handle Enums
        if isinstance(actual_type, type) and issubclass(actual_type, Enum):
            enum_name = re.sub(r'(?<!^)(?=[A-Z])', '_', actual_type.__name__).lower()
            self.enums[enum_name] = actual_type
            return enum_name

        # Standard type mapping
        sql_type = self.TYPE_MAP.get(actual_type)
        if sql_type:
            return sql_type

        return "JSONB"

    # =========================================================================
    # ENUM GENERATION
    # =========================================================================

    def generate_enum(self, enum_name: str, enum_class: Type[Enum], schema: str) -> List[sql.Composed]:
        """
        Generate PostgreSQL ENUM type DDL.

        Behavior depends on self.destructive:
        - destructive=True: DROP CASCADE + CREATE (destroys dependent columns!)
        - destructive=False: CREATE with DO block to skip if exists (safe)
        """
        values_list = [member.value for member in enum_class]
        values_sql = sql.SQL(', ').join(sql.Literal(v) for v in values_list)

        if self.destructive:
            # DESTRUCTIVE: Drop and recreate (use only for development rebuild)
            return [
                sql.SQL("DROP TYPE IF EXISTS {}.{} CASCADE").format(
                    sql.Identifier(schema),
                    sql.Identifier(enum_name)
                ),
                sql.SQL("CREATE TYPE {}.{} AS ENUM ({})").format(
                    sql.Identifier(schema),
                    sql.Identifier(enum_name),
                    values_sql
                )
            ]
        else:
            # SAFE: Only create if not exists (PostgreSQL 9.1+ DO block)
            # This avoids dropping columns that depend on the enum
            values_str = ', '.join(f"'{v}'" for v in values_list)
            do_block = f"""
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = '{enum_name}' AND typnamespace = (SELECT oid FROM pg_namespace WHERE nspname = '{schema}')) THEN
        CREATE TYPE "{schema}"."{enum_name}" AS ENUM ({values_str});
    END IF;
END$$
"""
            return [sql.SQL(do_block)]

    # =========================================================================
    # TABLE GENERATION
    # =========================================================================

    def generate_table(self, model: Type[BaseModel]) -> sql.Composed:
        """
        Generate CREATE TABLE DDL from a Pydantic model.

        Args:
            model: Pydantic model with __sql_* metadata

        Returns:
            sql.Composed CREATE TABLE statement
        """
        meta = self.get_model_metadata(model)
        table_name = meta["table"]
        schema_name = meta["schema"]
        primary_key = meta["primary_key"]
        foreign_keys = meta["foreign_keys"]
        serial_columns = meta["serial_columns"]

        if not table_name:
            raise ValueError(f"Model {model.__name__} missing __sql_table__ attribute")

        logger.debug(f"Generating table {schema_name}.{table_name} from {model.__name__}")

        columns = []
        constraints = []

        for field_name, field_info in model.model_fields.items():
            field_type = field_info.annotation
            sql_type_str = self.python_type_to_sql(field_type, field_info)

            # Check if Optional
            is_optional = False
            origin = get_origin(field_type)
            if origin is Union:
                args = get_args(field_type)
                if type(None) in args:
                    is_optional = True

            # Handle SERIAL columns
            if field_name in serial_columns:
                sql_type_str = "SERIAL"

            # Build column definition
            column_parts = [
                sql.Identifier(field_name),
                sql.SQL(" ")
            ]

            # Handle enum types with schema qualification
            if sql_type_str in self.enums:
                column_parts.extend([
                    sql.Identifier(schema_name),
                    sql.SQL("."),
                    sql.Identifier(sql_type_str)
                ])
            else:
                column_parts.append(sql.SQL(sql_type_str))

            # NOT NULL constraint (skip for SERIAL and PK columns)
            if not is_optional and field_name not in primary_key and sql_type_str != "SERIAL":
                column_parts.extend([sql.SQL(" "), sql.SQL("NOT NULL")])

            # Handle defaults
            if field_info.default is not None and field_info.default != ...:
                if isinstance(field_info.default, Enum):
                    column_parts.extend([
                        sql.SQL(" DEFAULT "),
                        sql.Literal(field_info.default.value),
                        sql.SQL("::"),
                        sql.Identifier(schema_name),
                        sql.SQL("."),
                        sql.Identifier(sql_type_str)
                    ])
                elif isinstance(field_info.default, str):
                    column_parts.extend([sql.SQL(" DEFAULT "), sql.Literal(field_info.default)])
                elif isinstance(field_info.default, (int, float)):
                    column_parts.extend([sql.SQL(" DEFAULT "), sql.Literal(field_info.default)])
                elif isinstance(field_info.default, bool):
                    column_parts.extend([sql.SQL(" DEFAULT "), sql.SQL("true" if field_info.default else "false")])

            # Handle default_factory for timestamps and dicts
            if hasattr(field_info, 'default_factory') and field_info.default_factory is not None:
                if field_name in ["created_at", "updated_at"]:
                    column_parts.extend([sql.SQL(" DEFAULT NOW()")])
                elif sql_type_str == "JSONB":
                    column_parts.extend([sql.SQL(" DEFAULT '{}'")])

            # Ensure timestamp defaults
            if field_name == "created_at" and " DEFAULT" not in str(sql.SQL("").join(column_parts)):
                column_parts.extend([sql.SQL(" DEFAULT NOW()")])
            if field_name == "updated_at" and " DEFAULT" not in str(sql.SQL("").join(column_parts)):
                column_parts.extend([sql.SQL(" DEFAULT NOW()")])

            columns.append(sql.SQL("").join(column_parts))

        # PRIMARY KEY constraint
        if len(primary_key) == 1:
            constraints.append(
                sql.SQL("PRIMARY KEY ({})").format(sql.Identifier(primary_key[0]))
            )
        elif len(primary_key) > 1:
            pk_columns = sql.SQL(", ").join(sql.Identifier(col) for col in primary_key)
            constraints.append(
                sql.SQL("PRIMARY KEY ({})").format(pk_columns)
            )

        # FOREIGN KEY constraints
        for fk_column, fk_reference in foreign_keys.items():
            match = re.match(r"(\w+)\.(\w+)\((\w+)\)", fk_reference)
            if match:
                ref_schema, ref_table, ref_column = match.groups()
                constraints.append(
                    sql.SQL("FOREIGN KEY ({}) REFERENCES {}.{} ({}) ON DELETE CASCADE").format(
                        sql.Identifier(fk_column),
                        sql.Identifier(ref_schema),
                        sql.Identifier(ref_table),
                        sql.Identifier(ref_column)
                    )
                )

        # Combine columns and constraints
        all_parts = columns + constraints

        return sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} ({})").format(
            sql.Identifier(schema_name),
            sql.Identifier(table_name),
            sql.SQL(", ").join(all_parts)
        )

    # =========================================================================
    # INDEX GENERATION
    # =========================================================================

    def generate_indexes(self, model: Type[BaseModel]) -> List[sql.Composed]:
        """
        Generate CREATE INDEX statements from a Pydantic model's __sql_indexes__.

        Args:
            model: Pydantic model with __sql_indexes__ attribute

        Returns:
            List of sql.Composed CREATE INDEX statements
        """
        meta = self.get_model_metadata(model)
        table_name = meta["table"]
        schema_name = meta["schema"]
        indexes = meta.get("indexes", [])

        result = []

        for idx_def in indexes:
            # Handle tuple format: (name, columns) or (name, columns, partial_where)
            if isinstance(idx_def, tuple):
                name = idx_def[0]
                columns = idx_def[1] if len(idx_def) > 1 else []
                partial_where = idx_def[2] if len(idx_def) > 2 else None
                index_type = "btree"
                descending = False
            # Handle dict format
            elif isinstance(idx_def, dict):
                columns = idx_def.get("columns", [])
                name = idx_def.get("name")
                partial_where = idx_def.get("partial_where")
                descending = idx_def.get("descending", False)
                index_type = idx_def.get("type", "btree")
            else:
                continue

            if not columns or not name:
                continue

            # Ensure columns is a list
            if isinstance(columns, str):
                columns = [columns]

            if index_type == "gin":
                result.append(IndexBuilder.gin(schema_name, table_name, columns[0], name=name))
            else:
                result.append(IndexBuilder.btree(
                    schema_name, table_name, columns,
                    name=name,
                    partial_where=partial_where,
                    descending=descending
                ))

        return result

    # =========================================================================
    # COMPLETE SCHEMA GENERATION
    # =========================================================================

    def generate_drop_schema(self) -> sql.Composed:
        """
        Generate DROP SCHEMA CASCADE statement.

        WARNING: This destroys ALL data in the schema!
        Only use for development rebuild.
        """
        return sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
            sql.Identifier(self.schema_name)
        )

    def generate_all(self) -> List[sql.Composed]:
        """
        Generate complete DDL for all DAG models.

        Returns:
            List of sql.Composed statements ready for execution
        """
        from core.models import Job, NodeState, JobEvent
        from core.models.task import TaskResult
        from core.contracts import JobStatus, NodeStatus, TaskStatus

        statements = []

        # Schema creation
        statements.append(
            sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                sql.Identifier(self.schema_name)
            )
        )

        # Search path
        statements.append(SchemaUtils.set_search_path(self.schema_name))

        # Generate ENUMs
        statements.extend(self.generate_enum("job_status", JobStatus, self.schema_name))
        statements.extend(self.generate_enum("node_status", NodeStatus, self.schema_name))
        statements.extend(self.generate_enum("task_status", TaskStatus, self.schema_name))

        # Import event enums
        from core.models.events import EventType, EventStatus
        statements.extend(self.generate_enum("event_type", EventType, self.schema_name))
        statements.extend(self.generate_enum("event_status", EventStatus, self.schema_name))

        # Generate tables
        statements.append(self.generate_table(Job))
        statements.append(self.generate_table(NodeState))
        statements.append(self.generate_table(TaskResult))
        statements.append(self.generate_table(JobEvent))

        # Generate indexes
        statements.extend(self.generate_indexes(Job))
        statements.extend(self.generate_indexes(NodeState))
        statements.extend(self.generate_indexes(TaskResult))
        statements.extend(self.generate_indexes(JobEvent))

        # Generate updated_at triggers
        statements.append(TriggerBuilder.updated_at_function(self.schema_name))
        statements.extend(TriggerBuilder.updated_at_trigger(self.schema_name, "dag_jobs"))
        statements.extend(TriggerBuilder.updated_at_trigger(self.schema_name, "dag_node_states"))

        logger.info(f"Generated {len(statements)} DDL statements for schema {self.schema_name}")
        return statements

    def execute(self, conn, dry_run: bool = False) -> int:
        """
        Execute all DDL statements.

        Args:
            conn: psycopg connection
            dry_run: If True, log statements but don't execute

        Returns:
            Number of statements executed
        """
        statements = self.generate_all()

        if dry_run:
            for stmt in statements:
                logger.info(f"[DRY RUN] {stmt.as_string(conn)[:100]}...")
            return len(statements)

        with conn.cursor() as cur:
            for stmt in statements:
                cur.execute(stmt)

        logger.info(f"Executed {len(statements)} DDL statements")
        return len(statements)


# ============================================================================
# EXPORTS
# ============================================================================

__all__ = ['PydanticToSQL']
