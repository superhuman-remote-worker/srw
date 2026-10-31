"""Plugin registry for the MemoryManager pipeline.

Same registration pattern as TOOL_REGISTRY (src/agent/tools/registry.py): a
module-level dict that plugin modules populate at import time, resolved
by name from config. ``memory.pipeline`` entries in YAML are names into
this registry; MemoryManager.from_config() binds them (P6 — the manager
is a binder, the registry is the driver catalog).

Usage:
    from agent.services.memory.registry import register_memory_plugin

    @register_memory_plugin("retriever", "recall_hybrid",
                            description="RecallStore.retrieve, budget-fit")
    def _build(runtime: MemoryRuntime) -> Retriever:
        return RecallHybridRetriever(runtime.recall_store, ...)

Resolution failures are loud by design: an unknown name in config is a
misconfiguration, not a degradation — it must fail at bind time with the
available names in the message.
"""

import logging
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Tuple

logger = logging.getLogger(__name__)

#: The pipeline stages a plugin can register for (== memory.pipeline keys).
PLUGIN_KINDS = ("retriever", "scorer", "policy", "writer", "extension")

PluginFactory = Callable[[Any], Any]  # (MemoryRuntime) -> plugin instance


class UnknownMemoryPluginError(LookupError):
    """A memory.pipeline entry names a plugin that isn't registered."""


@dataclass(frozen=True)
class MemoryPluginSpec:
    """Registry entry: how to build one named plugin."""

    kind: str
    name: str
    factory: PluginFactory
    description: str = ""


# Master registry: kind -> name -> spec. Populated at import time by the
# plugin modules (src/services/memory/plugins/), mirroring TOOL_REGISTRY.
MEMORY_PLUGIN_REGISTRY: Dict[str, Dict[str, MemoryPluginSpec]] = {
    kind: {} for kind in PLUGIN_KINDS
}


#: Plugin names a release removed, with the reason. A stored or frozen config
#: that still names one (a resumed job's ``resolved_config``, a copied expert)
#: binds without it instead of failing at bind time.
RETIRED_MEMORY_PLUGINS: Dict[Tuple[str, str], str] = {
    ("writer", "memory_assembler"): "the memory assembler was removed",
}


def register_memory_plugin(
    kind: str, name: str, *, description: str = ""
) -> Callable[[PluginFactory], PluginFactory]:
    """Decorator registering a plugin factory under (kind, name).

    Duplicate registration raises immediately — two modules claiming the
    same name is a programming error that must surface at import time.
    """
    if kind not in PLUGIN_KINDS:
        raise ValueError(
            f"Unknown memory plugin kind '{kind}'. Valid kinds: {PLUGIN_KINDS}"
        )

    def _decorator(factory: PluginFactory) -> PluginFactory:
        existing = MEMORY_PLUGIN_REGISTRY[kind].get(name)
        if existing is not None and existing.factory is not factory:
            raise ValueError(
                f"Memory plugin {kind}/'{name}' is already registered "
                f"(by {existing.factory!r})"
            )
        MEMORY_PLUGIN_REGISTRY[kind][name] = MemoryPluginSpec(
            kind=kind, name=name, factory=factory, description=description
        )
        logger.debug("Registered memory plugin %s/%s", kind, name)
        return factory

    return _decorator


def resolve_memory_plugin(kind: str, name: str) -> MemoryPluginSpec:
    """Look up a registered plugin spec, failing loudly with alternatives."""
    if kind not in PLUGIN_KINDS:
        raise ValueError(
            f"Unknown memory plugin kind '{kind}'. Valid kinds: {PLUGIN_KINDS}"
        )
    spec = MEMORY_PLUGIN_REGISTRY[kind].get(name)
    if spec is None:
        available = sorted(MEMORY_PLUGIN_REGISTRY[kind]) or ["<none registered>"]
        raise UnknownMemoryPluginError(
            f"No memory {kind} named '{name}' is registered. "
            f"Available {kind}s: {', '.join(available)}. "
            f"Check memory.pipeline in the agent config."
        )
    return spec


def available_memory_plugins(
    kind: Optional[str] = None,
) -> Dict[str, Dict[str, MemoryPluginSpec]]:
    """Copy of the registry (one kind or all) — for status/docs surfaces."""
    if kind is not None:
        if kind not in PLUGIN_KINDS:
            raise ValueError(
                f"Unknown memory plugin kind '{kind}'. Valid kinds: {PLUGIN_KINDS}"
            )
        return {kind: dict(MEMORY_PLUGIN_REGISTRY[kind])}
    return {k: dict(v) for k, v in MEMORY_PLUGIN_REGISTRY.items()}
