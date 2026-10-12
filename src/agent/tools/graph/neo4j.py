"""Neo4j graph tools for the Universal Agent.

Provides Neo4j graph operations:
- Cypher query execution (read-only)
- Cypher statement execution (write)
- Schema inspection

These tools are injected automatically when a Neo4j datasource is
attached to a job. See knowledge-base/knowledge/datasources.md.
"""

import logging
from typing import Any, Dict, List, Optional

from langchain_core.tools import tool

from agent.tools.context import ToolContext

from shared.tool_catalog.definitions import (
    GRAPH_TOOLS_METADATA as GRAPH_TOOLS_METADATA,
)

logger = logging.getLogger(__name__)


# Cypher keywords that indicate a write operation
_WRITE_KEYWORDS = (
    "CREATE",
    "MERGE",
    "DELETE",
    "DETACH",
    "SET",
    "REMOVE",
    "DROP",
    "CALL",
)


# Tool metadata for registry
# Phase availability: domain tools are tactical-only


def create_neo4j_tools(context: ToolContext) -> List[Any]:
    """Create Neo4j graph tools with injected context.

    Args:
        context: ToolContext with dependencies (must include a Neo4j connector)

    Returns:
        List of LangChain tool functions

    Raises:
        ValueError: If Neo4j database not available in context
    """
    neo4j = context.connection_for("graph")
    if not neo4j:
        raise ValueError("Neo4j connector not available in context")

    # Schema cache
    _schema_cache: Optional[Dict] = None

    def get_schema() -> Dict:
        nonlocal _schema_cache
        if _schema_cache is None and neo4j:
            _schema_cache = (
                neo4j.get_schema()
                if hasattr(neo4j, "get_schema")
                else neo4j.get_database_schema()
            )
        return _schema_cache or {}

    def _format_results(results: list) -> str:
        """Format query results as a string."""
        if not results:
            return "Query executed successfully but returned no results."

        limited_results = results[:50]
        formatted = []
        for i, record in enumerate(limited_results, 1):
            formatted.append(f"Record {i}: {record}")

        result_str = "\n".join(formatted)

        if len(results) > 50:
            result_str += f"\n\n... and {len(results) - 50} more results"

        return result_str

    @tool
    def cypher_query(query: str) -> str:
        """Execute a read-only Cypher query against the Neo4j database.

        Args:
            query: A valid Cypher query string (MATCH, RETURN, etc.)

        Returns:
            String representation of query results (up to 50 records)

        Use this to explore the graph, find entities, check relationships.
        Only read queries are allowed. Use cypher_execute for writes.
        """
        if not neo4j:
            return "Error: No Neo4j connection available"

        # A friendly early refusal for the obvious cases. The enforcement is
        # the read-access session below: the server refuses every write in
        # it (Neo.ClientError.Statement.AccessMode), however it is spelled.
        upper = query.strip().upper()
        if any(upper.startswith(kw) or f" {kw} " in upper for kw in _WRITE_KEYWORDS):
            return (
                "Error: Write operations are not allowed in cypher_query. "
                "Use cypher_execute for CREATE, MERGE, DELETE, SET, etc."
            )

        try:
            results = neo4j.execute_read(query)
            return _format_results(results)
        except Exception as e:
            return f"Error executing query: {str(e)}"

    @tool
    def cypher_execute(statement: str) -> str:
        """Execute a write Cypher statement against the Neo4j database.

        Args:
            statement: A valid Cypher statement (CREATE, MERGE, DELETE, SET, etc.)

        Returns:
            Result message or string representation of results

        Use this for graph modifications: creating nodes/relationships,
        updating properties, deleting elements.
        For read-only queries, use cypher_query instead.
        """
        if not neo4j:
            return "Error: No Neo4j connection available"

        try:
            results = neo4j.execute_query(statement)
            if results:
                return _format_results(results)
            return "Statement executed successfully."
        except Exception as e:
            return f"Error executing statement: {str(e)}"

    @tool
    def get_database_schema() -> str:
        """Get the Neo4j database schema.

        Returns:
            Schema information with node labels, relationship types, properties
        """
        schema = get_schema()
        if not schema:
            return "Error: Could not retrieve schema"

        schema_str = "Neo4j Database Schema:\n\n"
        schema_str += f"Node Labels ({len(schema.get('node_labels', []))}):\n"
        for label in schema.get("node_labels", []):
            schema_str += f"  - {label}\n"

        schema_str += (
            f"\nRelationship Types ({len(schema.get('relationship_types', []))}):\n"
        )
        for rel_type in schema.get("relationship_types", []):
            schema_str += f"  - {rel_type}\n"

        schema_str += f"\nProperty Keys ({len(schema.get('property_keys', []))}):\n"
        for prop in schema.get("property_keys", [])[:30]:
            schema_str += f"  - {prop}\n"

        if len(schema.get("property_keys", [])) > 30:
            schema_str += f"  ... and {len(schema['property_keys']) - 30} more\n"

        return schema_str

    return [
        cypher_query,
        cypher_execute,
        get_database_schema,
    ]
