"""Per-row provenance for Helm-reconciled configuration.

Framework-free contract shared by the seed Job, the DB layer, the admin API
and tests. Four system-scoped tables carry it — ``system_api_keys``,
``llm_endpoints``, ``models`` and ``system_settings`` (app migration 0243):

* ``source`` — who wrote the row last. ``helm`` = the ``llm.seed`` Job,
  ``ui`` = an admin through the Cockpit / REST API, ``default`` = a boot-time
  seeder shipped in the image (Tavily/SearXNG/Crawl4AI/ElevenLabs promotion,
  data migrations).
* ``helm_value_hash`` — digest of the value Helm last applied, so a re-run
  with unchanged values is a no-op and an admin override is detectable
  without storing the chart's plaintext beside the live one.
* ``source_updated_at`` — when ``source`` was last written.

Whether a row is *managed* (re-applied on every ``helm upgrade``) is not a
row flag: the seed Job records the set of entries flagged ``reconcile: true``
in the ``system_settings`` row ``helm.reconcile`` (the manifest), and the API
derives ``managed_by_helm`` from it. Removing the flag from values and
upgrading rewrites the manifest, which is the only way to release a row.
Design: knowledge-base/knowledge/features/helm_managed_settings.md.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

SOURCE_HELM = "helm"
SOURCE_UI = "ui"
SOURCE_DEFAULT = "default"
SOURCES: frozenset[str] = frozenset({SOURCE_DEFAULT, SOURCE_HELM, SOURCE_UI})

# ``seeded_from`` / ``updated_by`` breadcrumb the llm.seed Job stamps on every
# row it writes. Kept identical to ``orchestrator.seed.llm_config.SEEDED_FROM_TAG``
# (pinned by a test) so provenance can be derived from the breadcrumb alone.
HELM_SEED_BREADCRUMB = "helm:llm.seed"

# ``updated_by`` breadcrumb on a required-capability default pin the system
# chose on its own (``orchestrator.services.readiness.auto_pin_required_defaults``).
# ``source`` is ``default``; the breadcrumb is what tells the seed Job it may
# replace the pin with a declared ``llm.seed.defaults`` entry and tells the
# admin UI to label it as automatic. An admin re-selecting it clears the mark.
AUTO_PIN_BREADCRUMB = "auto:required-default"

# system_settings key holding the reconcile manifest the seed Job writes.
RECONCILE_MANIFEST_KEY = "helm.reconcile"

# Manifest sections, each a sorted list of identities:
#   systemApiKeys   -> provider              ("openai")
#   systemEndpoints -> endpoint label        ("MiniMax")
#   models          -> "<anchor>/<model_id>" ("openai/gpt-5-mini",
#                                             "endpoint:MiniMax/MiniMax-M3")
#   defaults        -> kind                  ("chat")
MANIFEST_SECTIONS = ("systemApiKeys", "systemEndpoints", "models", "defaults")


def provenance_from_breadcrumb(seeded_from: str | None) -> str:
    """Derive ``source`` for a row written by a caller that only knows the
    legacy ``seeded_from`` / ``updated_by`` breadcrumb.

    Mirrors the backfill in migration 0243 exactly: the llm.seed tag means
    Helm, any other breadcrumb means an image-shipped seeder, none means an
    admin.
    """
    if not seeded_from:
        return SOURCE_UI
    if seeded_from.startswith(HELM_SEED_BREADCRUMB):
        return SOURCE_HELM
    return SOURCE_DEFAULT


def value_hash(value: Any) -> str:
    """Stable digest of a non-secret declaration (any JSON-serialisable shape).

    Canonical JSON (sorted keys, no whitespace) so two renders of the same
    values.yaml hash identically regardless of key order.
    Credential-bearing seed entries use the seeder's keyed digest instead;
    an unkeyed hash would allow offline guessing of low-entropy credentials.
    """
    canonical = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def model_identity(provider_kind: str, anchor: str, model_id: str) -> str:
    """Manifest identity of a catalog row.

    ``anchor`` is the provider name for ``provider_kind='system'`` and the
    endpoint *label* for ``provider_kind='endpoint'`` — labels, not UUIDs, so
    the manifest reads like the values file that produced it.
    """
    if provider_kind == "endpoint":
        return f"endpoint:{anchor}/{model_id}"
    return f"{anchor}/{model_id}"


def empty_manifest() -> dict[str, list[str]]:
    return {section: [] for section in MANIFEST_SECTIONS}


def is_managed(manifest: dict[str, Any] | None, section: str, identity: str) -> bool:
    """Whether ``identity`` is reconciled by Helm according to ``manifest``."""
    if not manifest:
        return False
    entries = manifest.get(section)
    if not isinstance(entries, list):
        return False
    return identity in entries


def annotate(
    row: dict[str, Any],
    *,
    manifest: dict[str, Any] | None,
    section: str,
    identity: str,
) -> dict[str, Any]:
    """Add the provenance fields the admin UI renders to an API row.

    ``managed_by_helm`` comes from the manifest; ``helm_drift`` is true when a
    managed row was last written by someone other than the seed Job — the
    next ``helm upgrade`` will revert it.
    """
    source = row.get("source")
    managed = is_managed(manifest, section, identity)
    row["source"] = source
    row["managed_by_helm"] = managed
    row["helm_drift"] = bool(managed and source != SOURCE_HELM)
    return row
