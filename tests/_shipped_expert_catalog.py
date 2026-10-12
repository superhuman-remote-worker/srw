"""The shared Expert Catalog exactly as a fresh installation seeds it.

``seed_bundled_expert_manifests`` saves every shipped ``config/experts/*`` and
``config/subagents/*`` document unchanged into the shared Catalog. Tests that
compare a catalogue selection with its inline copy serve that Catalog from the
same files, so roster references resolve as they do on a real deployment.
"""

from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from shared.manifests import parse_documents

CONFIG_DIR = Path(__file__).parents[1] / "config"
CATALOG_SCOPE = {"kind": "Catalog", "name": "shared"}


def shipped_catalog() -> dict[str, dict[str, Any]]:
    """``{resource name: saved resource}`` for every shipped Expert document."""
    resources: dict[str, dict[str, Any]] = {}
    for group in ("experts", "subagents"):
        for path in sorted((CONFIG_DIR / group).glob("*/config.yaml")):
            document = parse_documents(path.read_text(encoding="utf-8"))[0]
            name = document["metadata"]["name"]
            resources[name] = {
                "id": str(uuid5(NAMESPACE_URL, f"srw-test-catalog:{name}")),
                "document": document,
                "revision": "sha256:" + "1" * 64,
                "resource_version": 1,
                "owner_id": None,
                "linked_id": None,
            }
    return resources


def authored_runtime_config(name: str) -> dict[str, Any]:
    """A shipped Expert's ``spec.runtime.config``: what an inline copy carries."""
    return deepcopy(shipped_catalog()[name]["document"]["spec"]["runtime"]["config"])


def serve_shipped_catalog(monkeypatch) -> dict[str, dict[str, Any]]:
    """Answer ``ManifestStore.by_name`` for the shared Catalog from the files."""
    from orchestrator.services.manifest_store import ManifestStore

    catalog = shipped_catalog()

    async def by_name(self, kind, scope, name, *, revision=None):
        if kind == "Expert" and scope == CATALOG_SCOPE and name in catalog:
            return deepcopy(catalog[name])
        return None

    monkeypatch.setattr(ManifestStore, "by_name", by_name)
    return catalog
