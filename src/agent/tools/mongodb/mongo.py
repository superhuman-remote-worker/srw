"""MongoDB tools for the Universal Agent.

Provides MongoDB operations:
- Document querying with filters
- Aggregation pipelines
- Schema inspection (collections, sample fields, indexes)
- Document insertion
- Document updates

These tools are injected automatically when a MongoDB datasource is
attached to a job. See knowledge-base/knowledge/datasources.md.
"""

import json
import logging
from typing import Any, Dict, List

from langchain_core.tools import tool

from agent.tools.context import ToolContext

from shared.tool_catalog.definitions import (
    MONGODB_TOOLS_METADATA as MONGODB_TOOLS_METADATA,
)

logger = logging.getLogger(__name__)


# Custom JSON encoder for MongoDB types (ObjectId, datetime, etc.)
class _MongoJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        # Handle ObjectId, datetime, and other BSON types
        return str(obj)


def _json_dumps(obj: Any) -> str:
    """Serialize MongoDB documents to JSON string."""
    return json.dumps(obj, cls=_MongoJSONEncoder, ensure_ascii=False)


def _parse_json(s: str, label: str = "input") -> Any:
    """Parse a JSON string, returning a helpful error message on failure."""
    try:
        return json.loads(s)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in {label}: {e}")


# Tool metadata for registry
# Phase availability: domain tools are tactical-only


def create_mongo_tools(context: ToolContext) -> List[Any]:
    """Create MongoDB tools with injected context.

    Args:
        context: ToolContext with dependencies (must include a MongoDB connector)

    Returns:
        List of LangChain tool functions

    Raises:
        ValueError: If a MongoDB connector is not available in context
    """
    db = context.connection_for("mongodb")
    if not db:
        raise ValueError("MongoDB connector not available in context")

    @tool
    def mongo_query(collection: str, filter: str = "{}", limit: int = 50) -> str:
        """Query documents from a MongoDB collection.

        Args:
            collection: Collection name to query
            filter: JSON string with MongoDB query filter (e.g. '{"status": "active"}')
            limit: Maximum number of documents to return (default 50, max 100)

        Returns:
            String representation of matching documents
        """
        if not db:
            return "Error: No MongoDB connection available"

        try:
            query_filter = _parse_json(filter, "filter")
            limit = min(limit, 100)

            coll = db[collection]
            cursor = coll.find(query_filter).limit(limit)
            docs = list(cursor)

            if not docs:
                return f"No documents found in '{collection}' matching filter: {filter}"

            formatted = [f"Found {len(docs)} document(s) in '{collection}':\n"]
            for i, doc in enumerate(docs, 1):
                formatted.append(f"Document {i}: {_json_dumps(doc)}")

            return "\n".join(formatted)

        except ValueError as e:
            return f"Error: {str(e)}"
        except Exception as e:
            return f"Error querying collection: {str(e)}"

    @tool
    def mongo_aggregate(collection: str, pipeline: str) -> str:
        """Run an aggregation pipeline on a MongoDB collection.

        Args:
            collection: Collection name
            pipeline: JSON string with aggregation pipeline array
                (e.g. '[{"$match": {"status": "active"}}, {"$group": {"_id": "$type", "count": {"$sum": 1}}}]')

        Returns:
            String representation of aggregation results
        """
        if not db:
            return "Error: No MongoDB connection available"

        try:
            pipeline_list = _parse_json(pipeline, "pipeline")
            if not isinstance(pipeline_list, list):
                return "Error: Pipeline must be a JSON array of stages"

            coll = db[collection]
            results = list(coll.aggregate(pipeline_list))

            if not results:
                return f"Aggregation on '{collection}' returned no results."

            # Limit output
            limited = results[:100]
            formatted = [f"Aggregation results ({len(results)} total):\n"]
            for i, doc in enumerate(limited, 1):
                formatted.append(f"Result {i}: {_json_dumps(doc)}")

            if len(results) > 100:
                formatted.append(f"\n... showing 100 of {len(results)} results")

            return "\n".join(formatted)

        except ValueError as e:
            return f"Error: {str(e)}"
        except Exception as e:
            return f"Error running aggregation: {str(e)}"

    @tool
    def mongo_schema(collection: str = "") -> str:
        """Inspect the MongoDB database schema.

        Args:
            collection: Optional collection name. If empty, lists all collections.
                If provided, shows sample fields, indexes, and document count.

        Returns:
            Schema information as formatted text
        """
        if not db:
            return "Error: No MongoDB connection available"

        try:
            if not collection:
                # List all collections
                collections = db.list_collection_names()
                if not collections:
                    return "No collections found in the database."

                result = f"Collections ({len(collections)}):\n\n"
                for name in sorted(collections):
                    try:
                        count = db[name].estimated_document_count()
                        result += f"  - {name} ({count} documents)\n"
                    except Exception:
                        result += f"  - {name}\n"

                return result

            else:
                # Describe specific collection
                coll = db[collection]

                # Document count
                try:
                    count = coll.estimated_document_count()
                except Exception:
                    count = "unknown"

                result = f"Collection: {collection}\nDocuments: {count}\n"

                # Sample fields from a few documents
                sample_docs = list(coll.find().limit(5))
                if sample_docs:
                    all_fields: Dict[str, set] = {}
                    for doc in sample_docs:
                        for key, value in doc.items():
                            type_name = type(value).__name__
                            if key not in all_fields:
                                all_fields[key] = set()
                            all_fields[key].add(type_name)

                    result += f"\nFields (from {len(sample_docs)} sample documents):\n"
                    for field_name, types in sorted(all_fields.items()):
                        type_str = ", ".join(sorted(types))
                        result += f"  - {field_name}: {type_str}\n"
                else:
                    result += "\nNo documents to sample fields from.\n"

                # Indexes
                try:
                    indexes = list(coll.list_indexes())
                    if indexes:
                        result += f"\nIndexes ({len(indexes)}):\n"
                        for idx in indexes:
                            keys = dict(idx.get("key", {}))
                            unique = " (unique)" if idx.get("unique") else ""
                            result += f"  - {idx['name']}: {keys}{unique}\n"
                except Exception as e:
                    result += f"\nError listing indexes: {str(e)}\n"

                return result

        except Exception as e:
            return f"Error inspecting schema: {str(e)}"

    @tool
    def mongo_insert(collection: str, documents: str) -> str:
        """Insert one or more documents into a MongoDB collection.

        Args:
            collection: Collection name
            documents: JSON string - single document object or array of documents
                (e.g. '{"name": "test"}' or '[{"name": "a"}, {"name": "b"}]')

        Returns:
            Result message with inserted document count and IDs
        """
        if not db:
            return "Error: No MongoDB connection available"

        try:
            parsed = _parse_json(documents, "documents")

            coll = db[collection]

            if isinstance(parsed, list):
                if not parsed:
                    return "Error: Empty document array"
                result = coll.insert_many(parsed)
                ids = [str(oid) for oid in result.inserted_ids]
                return f"Inserted {len(ids)} document(s) into '{collection}'.\nIDs: {', '.join(ids)}"
            elif isinstance(parsed, dict):
                result = coll.insert_one(parsed)
                return f"Inserted 1 document into '{collection}'.\nID: {str(result.inserted_id)}"
            else:
                return "Error: Documents must be a JSON object or array of objects"

        except ValueError as e:
            return f"Error: {str(e)}"
        except Exception as e:
            return f"Error inserting documents: {str(e)}"

    @tool
    def mongo_update(collection: str, filter: str, update: str) -> str:
        """Update documents in a MongoDB collection.

        Args:
            collection: Collection name
            filter: JSON string with query filter to match documents
                (e.g. '{"status": "draft"}')
            update: JSON string with update operations
                (e.g. '{"$set": {"status": "published"}}')

        Returns:
            Result message with matched and modified counts
        """
        if not db:
            return "Error: No MongoDB connection available"

        try:
            query_filter = _parse_json(filter, "filter")
            update_ops = _parse_json(update, "update")

            coll = db[collection]
            result = coll.update_many(query_filter, update_ops)

            return (
                f"Update on '{collection}': "
                f"matched {result.matched_count}, "
                f"modified {result.modified_count} document(s)."
            )

        except ValueError as e:
            return f"Error: {str(e)}"
        except Exception as e:
            return f"Error updating documents: {str(e)}"

    return [
        mongo_query,
        mongo_aggregate,
        mongo_schema,
        mongo_insert,
        mongo_update,
    ]
