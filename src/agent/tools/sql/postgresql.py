"""PostgreSQL tools for the Universal Agent.

Provides PostgreSQL operations:
- SQL query execution (read-only SELECT)
- Schema inspection (tables, columns, types, constraints)
- SQL statement execution (INSERT, UPDATE, DELETE, DDL)

These tools are injected automatically when a PostgreSQL connector is
attached to a job. See knowledge-base/knowledge/datasources.md.
"""

import json
import logging
from typing import Any, List

from langchain_core.tools import tool

from agent.tools.context import ToolContext

from shared.tool_catalog.definitions import (
    SQL_TOOLS_METADATA as SQL_TOOLS_METADATA,
)

logger = logging.getLogger(__name__)


# Tool metadata for registry
# Phase availability: domain tools are tactical-only


def create_postgresql_tools(context: ToolContext) -> List[Any]:
    """Create PostgreSQL tools with injected context.

    Args:
        context: ToolContext with dependencies (must include a PostgreSQL connector)

    Returns:
        List of LangChain tool functions

    Raises:
        ValueError: If a PostgreSQL connector is not available in context
    """
    conn = context.connection_for("sql")
    if not conn:
        raise ValueError("PostgreSQL connector not available in context")

    @tool
    def sql_query(query: str) -> str:
        """Execute a read-only SQL query against the PostgreSQL connector.

        Args:
            query: A valid SQL SELECT query

        Returns:
            String representation of query results (up to 100 rows)

        Use this to explore data, run aggregations, and inspect records.
        Only SELECT queries are allowed. Use sql_execute for writes.
        """
        if not conn:
            return "Error: No PostgreSQL connection available"

        try:
            with conn.cursor() as cur:
                # Use a read-only transaction
                cur.execute("SET TRANSACTION READ ONLY")
                cur.execute(query)

                if cur.description is None:
                    conn.rollback()
                    return "Query executed but returned no result set. Use sql_execute for write operations."

                columns = [desc[0] for desc in cur.description]
                rows = cur.fetchmany(100)
                total_available = cur.rowcount if cur.rowcount >= 0 else len(rows)

                conn.rollback()  # End the read-only transaction

                if not rows:
                    return f"Query returned 0 rows.\nColumns: {', '.join(columns)}"

                # Format as table
                formatted = [f"Columns: {', '.join(columns)}", ""]
                for i, row in enumerate(rows, 1):
                    row_dict = dict(zip(columns, row))
                    # Convert non-serializable types to strings
                    for k, v in row_dict.items():
                        if not isinstance(v, (str, int, float, bool, type(None))):
                            row_dict[k] = str(v)
                    formatted.append(f"Row {i}: {json.dumps(row_dict, default=str)}")

                result_str = "\n".join(formatted)

                if total_available > 100:
                    result_str += f"\n\n... showing 100 of {total_available} rows"

                return result_str

        except Exception as e:
            # Ensure we don't leave a broken transaction
            try:
                conn.rollback()
            except Exception:
                pass
            return f"Error executing query: {str(e)}"

    @tool
    def sql_schema(table_name: str = "") -> str:
        """Inspect the PostgreSQL datasource schema.

        Args:
            table_name: Optional table name. If empty, lists all tables.
                If provided, shows columns, types, and constraints for that table.

        Returns:
            Schema information as formatted text
        """
        if not conn:
            return "Error: No PostgreSQL connection available"

        try:
            with conn.cursor() as cur:
                if not table_name:
                    # List all tables in non-system schemas
                    cur.execute("""
                        SELECT table_schema, table_name, table_type
                        FROM information_schema.tables
                        WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
                        ORDER BY table_schema, table_name
                    """)
                    rows = cur.fetchall()

                    if not rows:
                        return "No tables found in the database."

                    result = f"Tables ({len(rows)}):\n\n"
                    current_schema = None
                    for schema, name, ttype in rows:
                        if schema != current_schema:
                            current_schema = schema
                            result += f"Schema: {schema}\n"
                        result += f"  - {name} ({ttype.lower()})\n"

                    return result

                else:
                    # Describe specific table
                    # Parse schema.table or default to public
                    if "." in table_name:
                        schema, tbl = table_name.split(".", 1)
                    else:
                        schema, tbl = "public", table_name

                    # Columns
                    cur.execute(
                        """
                        SELECT column_name, data_type, is_nullable,
                               column_default, character_maximum_length
                        FROM information_schema.columns
                        WHERE table_schema = %s AND table_name = %s
                        ORDER BY ordinal_position
                    """,
                        (schema, tbl),
                    )
                    columns = cur.fetchall()

                    if not columns:
                        return f"Table '{table_name}' not found."

                    result = f"Table: {schema}.{tbl}\n\nColumns ({len(columns)}):\n"
                    for col_name, dtype, nullable, default, max_len in columns:
                        type_str = dtype
                        if max_len:
                            type_str += f"({max_len})"
                        null_str = "NULL" if nullable == "YES" else "NOT NULL"
                        default_str = f" DEFAULT {default}" if default else ""
                        result += (
                            f"  - {col_name}: {type_str} {null_str}{default_str}\n"
                        )

                    # Constraints (primary key, foreign keys, unique)
                    cur.execute(
                        """
                        SELECT tc.constraint_name, tc.constraint_type,
                               kcu.column_name,
                               ccu.table_schema AS ref_schema,
                               ccu.table_name AS ref_table,
                               ccu.column_name AS ref_column
                        FROM information_schema.table_constraints tc
                        JOIN information_schema.key_column_usage kcu
                            ON tc.constraint_name = kcu.constraint_name
                            AND tc.table_schema = kcu.table_schema
                        LEFT JOIN information_schema.constraint_column_usage ccu
                            ON tc.constraint_name = ccu.constraint_name
                            AND tc.table_schema = ccu.table_schema
                        WHERE tc.table_schema = %s AND tc.table_name = %s
                        ORDER BY tc.constraint_type, tc.constraint_name
                    """,
                        (schema, tbl),
                    )
                    constraints = cur.fetchall()

                    if constraints:
                        result += f"\nConstraints ({len(constraints)}):\n"
                        for (
                            cname,
                            ctype,
                            col,
                            ref_schema,
                            ref_table,
                            ref_col,
                        ) in constraints:
                            if ctype == "FOREIGN KEY":
                                result += f"  - {cname} ({ctype}): {col} -> {ref_schema}.{ref_table}.{ref_col}\n"
                            else:
                                result += f"  - {cname} ({ctype}): {col}\n"

                    # Indexes
                    cur.execute(
                        """
                        SELECT indexname, indexdef
                        FROM pg_indexes
                        WHERE schemaname = %s AND tablename = %s
                    """,
                        (schema, tbl),
                    )
                    indexes = cur.fetchall()

                    if indexes:
                        result += f"\nIndexes ({len(indexes)}):\n"
                        for idx_name, idx_def in indexes:
                            result += f"  - {idx_name}: {idx_def}\n"

                    conn.rollback()  # Clean transaction state
                    return result

        except Exception as e:
            try:
                conn.rollback()
            except Exception:
                pass
            return f"Error inspecting schema: {str(e)}"

    @tool
    def sql_execute(statement: str) -> str:
        """Execute a write SQL statement against the PostgreSQL datasource.

        Args:
            statement: A valid SQL statement (INSERT, UPDATE, DELETE, CREATE, ALTER, DROP, etc.)

        Returns:
            Result message with affected row count

        Use this for data modifications and DDL operations.
        For SELECT queries, use sql_query instead.
        """
        if not conn:
            return "Error: No PostgreSQL connection available"

        try:
            with conn.cursor() as cur:
                cur.execute(statement)
                rowcount = cur.rowcount
                conn.commit()

                if rowcount >= 0:
                    return f"Statement executed successfully. Rows affected: {rowcount}"
                else:
                    return "Statement executed successfully."

        except Exception as e:
            try:
                conn.rollback()
            except Exception:
                pass
            return f"Error executing statement: {str(e)}"

    return [
        sql_query,
        sql_schema,
        sql_execute,
    ]
