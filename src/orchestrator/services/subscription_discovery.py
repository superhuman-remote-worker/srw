"""Discovery + catalog reconciliation for the subscription proxy.

``GET {base_url}/models`` is the proxy's *advertised inference inventory* — a
routable-right-now list, not a successful inference test and not a quota
guarantee (upstream deliberately keeps quota-cooled models in it). It also
carries almost nothing: ``id``, ``object``, ``created``, ``owned_by``. Two
management reads supply the rest:

* ``/v0/management/auth-files/models?name=…`` attributes each advertised model
  to the connected credentials that can serve it — the source provenance a
  pooled catalog row needs, and the only thing that can tell "Claude via
  Anthropic" from "Claude via Antigravity".
* ``/v0/management/model-definitions/{channel}`` supplies the static context /
  output limits and display names. Suggestions about models we already know are
  routable — never extra models to import.

Everything here is read-only. Registration is a separate, explicit admin action
(:func:`import_candidates`) that never deletes and never overwrites an admin's
own edits.

Design: knowledge-base/knowledge/features/subscription_proxy.md §7.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping

from orchestrator.services.subscription_providers import (
    client_protocol_for_channels,
)
from orchestrator.services.subscriptions import (
    SubscriptionAccount,
    SubscriptionProxyError,
    account_model_map,
    channel_model_definitions,
    invalidate_account_cache,
    management_request,
)
from shared.subscription_routing import (
    PROTOCOL_OPENAI_CHAT,
    RoutingMetadata,
    merge_routing_into_params,
    routing_from_params,
    routing_params_block,
)

logger = logging.getLogger(__name__)

#: A candidate SRW can register as a chat model.
SUPPORT_SUPPORTED = "supported"
#: Advertised, but the modality is one SRW has no workflow for (image/video
#: generation). Shown so the inventory is honest; never bulk-imported.
SUPPORT_UNSUPPORTED_MODALITY = "unsupported_modality"
#: Advertised and probably usable, but protocol/source could not be resolved
#: with confidence. Registrable one at a time, flagged for review.
SUPPORT_NEEDS_REVIEW = "needs_review"

# Image/video generation models CLIProxyAPI advertises on the same /v1/models
# list as chat models. Exact IDs from the pinned build's registry builtins
# (internal/registry/model_definitions.go) plus the one image entry in its
# bundled models.json. The name heuristic below catches future siblings; this
# list keeps today's exactly right regardless of naming.
_KNOWN_NON_CHAT_MODELS = frozenset(
    {
        "gpt-image-1.5",
        "gpt-image-2",
        "grok-imagine-image",
        "grok-imagine-image-quality",
        "grok-imagine-video",
        "grok-imagine-video-1.5-preview",
        "gemini-3.1-flash-image",
    }
)

# Name patterns for generative-media and non-chat model families. Deliberately
# anchored on segment boundaries so a chat model that merely *mentions* vision
# ("vision-pro-chat") is not swept up.
_NON_CHAT_PATTERNS = (
    re.compile(r"(?:^|[-_/])image(?:$|[-_.])"),
    re.compile(r"(?:^|[-_/])video(?:$|[-_.])"),
    re.compile(r"imagine"),
    re.compile(r"(?:^|[-_/])(?:sora|veo|imagen|dall-?e)(?:$|[-_.])"),
    re.compile(r"(?:^|[-_/])(?:tts|whisper|embedding|rerank)(?:$|[-_.])"),
)


def is_chat_candidate(model_id: str, definition: Mapping[str, Any] | None) -> bool:
    """Whether SRW can offer ``model_id`` as a chat model.

    A static definition with neither a context window nor a completion cap is
    corroborating evidence for a media model (that is exactly how the proxy
    describes ``gemini-3.1-flash-image``), but on its own it only means
    "metadata missing" — so it is used to confirm a name match, never to reject
    a model by itself.
    """
    name = model_id.strip().lower()
    if name in _KNOWN_NON_CHAT_MODELS:
        return False
    return not any(pattern.search(name) for pattern in _NON_CHAT_PATTERNS)


@dataclass
class ModelCandidate:
    """One advertised model, attributed and classified."""

    model_id: str
    display_label: str
    owned_by: str | None = None
    #: Upstream channels that can serve it (provenance; may be several).
    sources: tuple[str, ...] = ()
    #: SRW provider keys behind those channels.
    providers: tuple[str, ...] = ()
    #: Opaque ids of the accounts that advertised it.
    account_ids: tuple[str, ...] = ()
    client_protocol: str | None = None
    context_window: int | None = None
    max_output_tokens: int | None = None
    family: str | None = None
    capabilities: tuple[str, ...] = ("chat", "auxiliary")
    support: str = SUPPORT_SUPPORTED
    #: Human-readable reason when support is not ``supported``.
    support_reason: str | None = None
    registered: bool = False
    registered_catalog_id: str | None = None
    #: True when the row exists but its stored routing disagrees with what
    #: discovery now resolves. Surfaced, never silently rewritten.
    routing_drift: bool = False

    def to_public(self) -> dict[str, Any]:
        return {
            "id": self.model_id,
            "display_label": self.display_label,
            "owned_by": self.owned_by,
            "sources": list(self.sources),
            "providers": list(self.providers),
            "account_ids": list(self.account_ids),
            "client_protocol": self.client_protocol,
            "context_window": self.context_window,
            "max_output_tokens": self.max_output_tokens,
            "family": self.family,
            "capability_hints": list(self.capabilities),
            "support": self.support,
            "support_reason": self.support_reason,
            "registered": self.registered,
            "catalog_id": self.registered_catalog_id,
            "routing_drift": self.routing_drift,
        }


@dataclass
class DiscoveryResult:
    ok: bool
    probe_url: str
    error: str | None = None
    candidates: list[ModelCandidate] = field(default_factory=list)
    #: Accounts whose per-credential inventory could not be read. Their models
    #: still appear (from /v1/models) but without attribution.
    unreadable_account_ids: list[str] = field(default_factory=list)
    #: True when attribution ran at all. False => sources are unknown, and the
    #: UI must say so instead of implying a model has no source.
    attribution_complete: bool = True

    def to_public(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "probe_url": self.probe_url,
            "error": self.error,
            "models": [c.to_public() for c in self.candidates],
            "unreadable_account_ids": list(self.unreadable_account_ids),
            "attribution_complete": self.attribution_complete,
        }


def _as_int(value: Any) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


async def _definitions_for_channels(
    channels: Iterable[str],
) -> dict[str, dict[str, Any]]:
    """Merge the static definitions of every connected channel by model id.

    Collisions are real (``claude-sonnet-4-6`` is published by both the
    ``claude`` and ``antigravity`` channels). First write wins and the values we
    read from it — context window, output cap — are only ever *suggestions*, so
    a collision cannot mis-route anything; it can at most suggest the other
    channel's limit, which the admin sees and can edit before importing.
    """
    merged: dict[str, dict[str, Any]] = {}
    for channel in dict.fromkeys(channels):
        for model_id, definition in (await channel_model_definitions(channel)).items():
            merged.setdefault(model_id, definition)
    return merged


async def discover_subscription_models(
    *,
    base_url: str,
    api_key: str | None,
    catalog_rows: Iterable[Mapping[str, Any]],
) -> DiscoveryResult:
    """Enumerate + attribute the models the proxy currently advertises.

    ``catalog_rows`` are the existing ``models`` rows on this endpoint; they
    decide the ``registered`` flag and expose routing drift. A failed
    discovery returns ``ok=False`` and an empty candidate list — never an
    authoritative "the proxy has no models".
    """
    from orchestrator.services.llm_endpoint_probe import probe_endpoint_models

    probe = await probe_endpoint_models(base_url=base_url, api_key=api_key)
    if not probe.ok:
        return DiscoveryResult(
            ok=False, probe_url=probe.probe_url, error=probe.error, candidates=[]
        )

    attribution: dict[str, list[SubscriptionAccount]] = {}
    unreadable: list[str] = []
    attribution_complete = True
    try:
        attribution, unreadable = await account_model_map()
    except SubscriptionProxyError as exc:
        # The inference list succeeded but enrichment did not: expose the
        # incomplete metadata rather than dropping models or inventing sources.
        attribution_complete = False
        logger.info("subscription discovery: attribution unavailable (%s)", exc.message)

    channels = {
        account.channel
        for accounts in attribution.values()
        for account in accounts
        if account.channel
    }
    definitions = await _definitions_for_channels(sorted(channels))

    by_model_id = {
        str(row.get("model_id")): row
        for row in catalog_rows
        if row.get("model_id") is not None
    }

    candidates: list[ModelCandidate] = []
    for entry in probe.models:
        model_id = entry.get("id")
        if not isinstance(model_id, str) or not model_id:
            continue
        accounts = attribution.get(model_id, [])
        sources = tuple(dict.fromkeys(a.channel for a in accounts if a.channel))
        providers = tuple(
            dict.fromkeys(
                a.provider_key for a in accounts if a.provider_key is not None
            )
        )
        definition = definitions.get(model_id)
        candidate = ModelCandidate(
            model_id=model_id,
            display_label=(definition or {}).get("display_name") or model_id,
            owned_by=entry.get("owned_by"),
            sources=sources,
            providers=providers,
            account_ids=tuple(a.account_id for a in accounts),
            context_window=_as_int((definition or {}).get("context_length"))
            or _as_int((definition or {}).get("inputTokenLimit"))
            or entry.get("context_window"),
            max_output_tokens=_as_int((definition or {}).get("max_completion_tokens"))
            or _as_int((definition or {}).get("outputTokenLimit")),
            family=entry.get("family"),
        )

        if not is_chat_candidate(model_id, definition):
            candidate.support = SUPPORT_UNSUPPORTED_MODALITY
            candidate.support_reason = "media_generation"
            candidate.capabilities = ()
        else:
            protocol = client_protocol_for_channels(sources)
            candidate.client_protocol = protocol
            if protocol is None:
                candidate.support = SUPPORT_NEEDS_REVIEW
                candidate.support_reason = (
                    "unknown_source" if not sources else "mixed_source_protocols"
                )

        existing = by_model_id.get(model_id)
        if existing is not None:
            candidate.registered = True
            candidate.registered_catalog_id = str(existing.get("id"))
            stored = routing_from_params(existing.get("params_json"))
            candidate.routing_drift = _has_routing_drift(stored, candidate)
        candidates.append(candidate)

    candidates.sort(key=lambda c: (c.support != SUPPORT_SUPPORTED, c.model_id))
    return DiscoveryResult(
        ok=True,
        probe_url=probe.probe_url,
        candidates=candidates,
        unreadable_account_ids=unreadable,
        attribution_complete=attribution_complete and not unreadable,
    )


def _has_routing_drift(stored: RoutingMetadata, candidate: ModelCandidate) -> bool:
    """Whether a registered row's routing disagrees with fresh discovery.

    Only a *contradiction* counts. A row with no recorded protocol is not
    drifting — it is a pre-feature row whose protocol the migration pinned by
    behaviour — and a row whose sources are a superset of what is connected
    right now is not drifting either (an account can be temporarily
    disconnected without that being a catalog defect).
    """
    if (
        stored.client_protocol
        and candidate.client_protocol
        and stored.client_protocol != candidate.client_protocol
    ):
        return True
    if (
        stored.subscription_sources
        and candidate.sources
        and not set(candidate.sources).issubset(set(stored.subscription_sources))
    ):
        return True
    return False


@dataclass
class ImportOutcome:
    created: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    rejected: list[dict[str, str]] = field(default_factory=list)

    def to_public(self) -> dict[str, Any]:
        return {
            "created": list(self.created),
            "skipped": list(self.skipped),
            "rejected": list(self.rejected),
        }


def _capped_context_window(candidate: ModelCandidate, cap: int | None) -> int | None:
    """The window to store for ``candidate`` under an admin's bulk cap.

    A cap only ever lowers the window. An advertised maximum is clamped to it.
    With no advertised maximum the row would fall back to its family default,
    so the cap is stored only when that default exceeds it (or is unknown);
    otherwise NULL keeps the row tracking the family matrix.
    """
    if cap is None:
        return candidate.context_window
    if candidate.context_window:
        return min(cap, candidate.context_window)
    from shared.runtime.core.model_registry import _family_context_window

    family_window = _family_context_window(candidate.model_id)
    if family_window and family_window <= cap:
        return None
    return cap


async def import_candidates(
    *,
    db: Any,
    endpoint_id: str,
    candidates: Iterable[ModelCandidate],
    requested_ids: Iterable[str] | None,
    include_review: bool,
    context_window_cap: int | None = None,
) -> ImportOutcome:
    """Register the administrator's selection as catalog rows. Idempotent.

    Rules, in the order they bite:

    * A model already registered on this endpoint is skipped, never re-inserted
      and never rewritten — that is what preserves manual labels, capability
      choices, limits and enabled state across a rediscovery.
    * A media-generation model is rejected, even if explicitly selected: SRW
      would register it as a chat model and every dispatch to it would fail.
    * A ``needs_review`` model is rejected unless the caller opted in, and is
      then stored with ``needs_review`` on its routing block plus the safe
      Chat Completions protocol, so nothing silently inherits Codex behaviour.
    * ``context_window_cap`` lowers a row's window, never raises it (see
      :func:`_capped_context_window`).
    """
    from shared.runtime.core.model_registry import family_of

    wanted = None if requested_ids is None else {str(i) for i in requested_ids}
    outcome = ImportOutcome()

    for candidate in candidates:
        if wanted is not None and candidate.model_id not in wanted:
            continue
        if candidate.support == SUPPORT_UNSUPPORTED_MODALITY:
            outcome.rejected.append(
                {"id": candidate.model_id, "reason": "unsupported_modality"}
            )
            continue
        if candidate.registered:
            outcome.skipped.append(candidate.model_id)
            continue
        if candidate.support == SUPPORT_NEEDS_REVIEW and not include_review:
            outcome.rejected.append(
                {
                    "id": candidate.model_id,
                    "reason": candidate.support_reason or "needs_review",
                }
            )
            continue

        needs_review = candidate.support == SUPPORT_NEEDS_REVIEW
        protocol = candidate.client_protocol or PROTOCOL_OPENAI_CHAT
        params: dict[str, Any] = {}
        if candidate.max_output_tokens:
            params["max_output_tokens"] = candidate.max_output_tokens
        params = merge_routing_into_params(
            params,
            routing_params_block(
                client_protocol=protocol,
                subscription_sources=candidate.sources,
                needs_review=needs_review,
            ),
        )
        try:
            row = await db.create_model(
                provider_kind="endpoint",
                provider_ref=endpoint_id,
                model_id=candidate.model_id,
                display_label=candidate.display_label,
                capabilities=list(candidate.capabilities or ("chat", "auxiliary")),
                family=candidate.family or family_of(candidate.model_id),
                context_window=_capped_context_window(candidate, context_window_cap),
                params_json=params or None,
                enabled=True,
                seeded_from="subscription-proxy:discover",
                on_conflict_do_nothing=True,
            )
        except Exception as exc:  # pragma: no cover - DB-shape dependent
            logger.warning(
                "subscription import failed for %s: %s", candidate.model_id, exc
            )
            outcome.rejected.append(
                {"id": candidate.model_id, "reason": "insert_failed"}
            )
            continue
        if row is None:
            outcome.skipped.append(candidate.model_id)
        else:
            outcome.created.append(candidate.model_id)

    if outcome.created:
        invalidate_account_cache()
    return outcome


async def advertised_model_ids() -> set[str]:
    """Model IDs the proxy currently advertises (empty set on any failure).

    Used by ``/api/models`` to badge subscription-backed catalog rows. A
    failure here degrades a badge, so it must never raise.
    """
    try:
        response = await management_request("GET", "/v1/models", timeout=2.0)
    except SubscriptionProxyError:
        return set()
    try:
        payload = response.json()
    except ValueError:
        return set()
    data = payload.get("data") if isinstance(payload, Mapping) else None
    ids: set[str] = set()
    for item in data or []:
        if isinstance(item, Mapping) and isinstance(item.get("id"), str):
            ids.add(item["id"])
        elif isinstance(item, str):
            ids.add(item)
    return ids
