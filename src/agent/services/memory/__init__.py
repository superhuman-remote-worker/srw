"""MemoryManager seam — single entry point for agent memory.

Design: knowledge-base/knowledge/features/agent_memory_overhaul.md. Phase-1 kernel: data
vocabulary (types), plugin protocols, registry, and the binder
(MemoryManager.from_config). Current behaviour transplants into
registered plugins in the follow-up slices; until the cutover flag
(memory.manager.enabled) flips, nothing in production constructs this.
"""

from agent.services.memory.manager import MemoryManager
from agent.services.memory.registry import (
    MEMORY_PLUGIN_REGISTRY,
    MemoryPluginSpec,
    UnknownMemoryPluginError,
    available_memory_plugins,
    register_memory_plugin,
)
from agent.services.memory.types import (
    CAPTURE_KINDS,
    AssembleRequest,
    AssembleStats,
    BucketRef,
    Candidate,
    CaptureEvent,
    InjectionBlock,
    MemoryPayload,
    MemoryPipelineError,
    MemoryRuntime,
    Query,
    RetrievalResult,
    Scored,
    TaskFrame,
    TransientScorerError,
)

# Importing the plugins package registers the built-in plugins (same
# import-time registration as src/tools/registry.py). Empty until the
# Phase-1 transplant slices land.
from agent.services.memory import plugins  # noqa: F401  (import for side effect)

__all__ = [
    "MemoryManager",
    "MEMORY_PLUGIN_REGISTRY",
    "MemoryPluginSpec",
    "UnknownMemoryPluginError",
    "available_memory_plugins",
    "register_memory_plugin",
    "CAPTURE_KINDS",
    "AssembleRequest",
    "AssembleStats",
    "BucketRef",
    "Candidate",
    "CaptureEvent",
    "InjectionBlock",
    "MemoryPayload",
    "MemoryPipelineError",
    "MemoryRuntime",
    "Query",
    "RetrievalResult",
    "Scored",
    "TaskFrame",
    "TransientScorerError",
]
