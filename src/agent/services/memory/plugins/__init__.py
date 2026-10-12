"""Built-in memory plugins.

Modules in this package call ``register_memory_plugin`` at import time;
importing the package (done by src/agent/services/memory/__init__.py) populates
MEMORY_PLUGIN_REGISTRY — the same import-time registration pattern as
src/tools/registry.py.

Phase-1 transplant slices land the legacy current-behaviour plugins here
(two-tier recall retriever, KB hybrid-search retriever, the extraction/
curation writers); Phase 3+ adds the measured improvements (reranker,
gate, bounded core). See knowledge-base/knowledge/features/agent_memory_overhaul.md §5.
"""

from agent.services.memory.plugins import bounded  # noqa: F401  (import-time registration)
from agent.services.memory.plugins import gate  # noqa: F401  (import-time registration)
from agent.services.memory.plugins import legacy  # noqa: F401  (import-time registration)
from agent.services.memory.plugins import legacy_writers  # noqa: F401  (import-time registration)
from agent.services.memory.plugins import memory_search  # noqa: F401  (import-time registration)
from agent.services.memory.plugins import reranker  # noqa: F401  (import-time registration)
