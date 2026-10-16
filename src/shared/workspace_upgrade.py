"""Names both sides of the stateless workspace upgrade share (C1; C2 reuses them).

The orchestrator writes ``workspace_container.seed_from`` in the transition and
ships the old lite workspace beside ``attach`` in the claim bundle; the
claimant declares that it can seed one, copies the files, then clears the key.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

WORKSPACE_UPGRADE_VERB = "upgrade-to-workspace"
# The canonical control payload: ``{target_tier?, template?, runtime_generation}``.
WORKSPACE_UPGRADE_PAYLOAD_KEYS = frozenset(
    {"target_tier", "template", "runtime_generation"}
)
SEED_FROM_KEY = "seed_from"
WORKSPACE_UPGRADE_SEED_KEY = "workspace_upgrade_seed"
WORKSPACE_UPGRADE_SEED_CAPABILITY = "workspace_upgrade_seed.v1"
WORKSPACE_UPGRADE_SEED_VERSION = 1


def metadata_object(value: Any) -> dict[str, Any]:
    """A thread's metadata as a dict; JSON text and junk degrade to ``{}``."""
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (TypeError, ValueError):
            return {}
    return dict(value) if isinstance(value, Mapping) else {}


def workspace_upgrade_seed_from(metadata: Any) -> dict[str, Any] | None:
    """The pending upgrade's copy source, or None when no copy is owed."""
    container = metadata_object(metadata).get("workspace_container")
    if not isinstance(container, Mapping):
        return None
    seed = container.get(SEED_FROM_KEY)
    return dict(seed) if isinstance(seed, Mapping) else None


__all__ = [
    "SEED_FROM_KEY",
    "WORKSPACE_UPGRADE_PAYLOAD_KEYS",
    "WORKSPACE_UPGRADE_SEED_CAPABILITY",
    "WORKSPACE_UPGRADE_SEED_KEY",
    "WORKSPACE_UPGRADE_SEED_VERSION",
    "WORKSPACE_UPGRADE_VERB",
    "metadata_object",
    "workspace_upgrade_seed_from",
]
