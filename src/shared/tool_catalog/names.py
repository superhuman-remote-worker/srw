"""Canonical names used by both tool metadata and runtime configuration."""

APP_GUIDE_LOADER_TOOL = "read_product_guide"
PRODUCT_CAPABILITIES_TOOL_NAME = "get_product_capabilities"
#: The model's own way into project memory (append-only context injection,
#: D25/D30): pushed memories arrive as context, this tool pulls more. Its
#: results count as present memories, so a fetched memory is not pushed again
#: (``agent.core.context_injection.scan_presence``).
MEMORY_SEARCH_TOOL_NAME = "memory_search"
