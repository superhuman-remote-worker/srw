"""Subscription-proxy routing contract (shared, dependency-free).

One CLIProxyAPI deployment fronts every connected *subscription* account
(ChatGPT/Codex, Claude Code, Antigravity, Grok Build, Kimi Code). SRW reaches
all of them through a single ``llm_endpoints`` transport row. What used to
identify that row — the literal label ``codex-proxy`` or a ``codex-proxy``
substring in its hostname — is not identity: renaming the label or the
Kubernetes Service silently re-routed every model on it through the generic
OpenAI factory. This module holds the explicit metadata that replaces those
guesses:

``transport_kind``
    A stable marker on the endpoint row (``llm_endpoints.transport_kind``),
    written by the seeder and the migration. Independent of label/hostname.

``client_protocol``
    The API schema SRW *sends*. Today one of ``openai-responses`` (the
    OpenAI Responses API, which carries reasoning summaries and is the
    established Codex path) or ``openai-chat`` (Chat Completions). The proxy
    converts to whatever the upstream account actually speaks — an OpenAI
    wire protocol does NOT mean the upstream model is an OpenAI model.

``subscription_sources``
    Which upstream credential channel(s) can serve the model. Provenance,
    and the key for limits that are genuinely provider-specific (the Codex
    context clamp). A model can be served by more than one channel, so this
    is a set, not a scalar; ``owned_by`` alone never identifies it.

Per-model values live in a namespaced ``models.params_json['routing']`` block
and are lifted into typed ``ModelMeta`` fields at resolution time — storing
JSON the resolver ignores would not implement routing.

Verified against the pinned upstream (``docker.io/eceasy/cli-proxy-api``
v7.2.110) on 2026-09-07; see
knowledge-base/knowledge/features/subscription_proxy.md §2.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Optional

# ---------------------------------------------------------------------------
# Endpoint transport identity
# ---------------------------------------------------------------------------

#: Value stored in ``llm_endpoints.transport_kind`` for the subscription proxy.
#: The one authoritative marker — never infer from label or hostname again.
SUBSCRIPTION_PROXY_TRANSPORT = "subscription-proxy"

#: Canonical label for the seeded endpoint row after the rename.
SUBSCRIPTION_PROXY_ENDPOINT_LABEL = "subscription-proxy"

#: Pre-rename label. Still matched (never re-created) so an upgrade finds the
#: existing row instead of inserting a duplicate, and so a rollback that
#: restores the old label keeps resolving.
LEGACY_CODEX_PROXY_ENDPOINT_LABEL = "codex-proxy"

#: Labels that identify the shared subscription endpoint when the stable
#: ``transport_kind`` marker has not been backfilled yet (pre-migration
#: readers, a hand-created row, a rolled-back schema).
SUBSCRIPTION_PROXY_LABELS = frozenset(
    {SUBSCRIPTION_PROXY_ENDPOINT_LABEL, LEGACY_CODEX_PROXY_ENDPOINT_LABEL}
)

# ---------------------------------------------------------------------------
# Client protocols
# ---------------------------------------------------------------------------

#: OpenAI Responses API (``POST {base_url}/responses``). SRW's ``codex``
#: factory; the only path that requests + captures a reasoning summary.
PROTOCOL_OPENAI_RESPONSES = "openai-responses"

#: OpenAI Chat Completions (``POST {base_url}/chat/completions``). SRW's
#: generic ``openai`` factory.
PROTOCOL_OPENAI_CHAT = "openai-chat"

CLIENT_PROTOCOLS = frozenset({PROTOCOL_OPENAI_RESPONSES, PROTOCOL_OPENAI_CHAT})

#: Agent-side LLM factory that serves each client protocol.
_PROTOCOL_FACTORY = {
    PROTOCOL_OPENAI_RESPONSES: "codex",
    PROTOCOL_OPENAI_CHAT: "openai",
}

# ---------------------------------------------------------------------------
# Upstream credential channels
# ---------------------------------------------------------------------------
#
# These are CLIProxyAPI's own ``auth.Provider`` values as written by its OAuth
# handlers at v7.2.110 (``internal/api/handlers/management/
# auth_files_provider_oauth.go``) and surfaced as ``provider``/``type`` on
# ``GET /v0/management/auth-files``. Note that the *login* provider and the
# *credential* channel differ for Anthropic: the OAuth session is registered
# as ``anthropic`` while the saved record's provider is ``claude``.

CHANNEL_CODEX = "codex"
CHANNEL_CLAUDE = "claude"
CHANNEL_ANTIGRAVITY = "antigravity"
CHANNEL_XAI = "xai"
CHANNEL_KIMI = "kimi"

KNOWN_CHANNELS = frozenset(
    {
        CHANNEL_CODEX,
        CHANNEL_CLAUDE,
        CHANNEL_ANTIGRAVITY,
        CHANNEL_XAI,
        CHANNEL_KIMI,
    }
)

#: Aliases upstream accepts / emits for the same channel. Kept small and
#: explicit: an unknown channel is preserved verbatim rather than guessed at.
_CHANNEL_ALIASES = {
    "openai": CHANNEL_CODEX,
    "chatgpt": CHANNEL_CODEX,
    "anthropic": CHANNEL_CLAUDE,
    "claude-code": CHANNEL_CLAUDE,
    "anti-gravity": CHANNEL_ANTIGRAVITY,
    "x-ai": CHANNEL_XAI,
    "x.ai": CHANNEL_XAI,
    "grok": CHANNEL_XAI,
    "moonshot": CHANNEL_KIMI,
}


def normalize_channel(value: Optional[str]) -> Optional[str]:
    """Canonicalize an upstream channel slug; ``None`` for empty input.

    Unrecognized slugs are lower-cased and returned as-is — a proxy plugin may
    add channels we do not know about, and dropping them would silently erase
    provenance.
    """
    if value is None:
        return None
    slug = value.strip().lower()
    if not slug:
        return None
    return _CHANNEL_ALIASES.get(slug, slug)


def normalize_channels(values: Optional[Iterable[Any]]) -> tuple[str, ...]:
    """Canonicalize + de-duplicate a channel list, preserving first-seen order."""
    if not values:
        return ()
    seen: list[str] = []
    for raw in values:
        if not isinstance(raw, str):
            continue
        slug = normalize_channel(raw)
        if slug and slug not in seen:
            seen.append(slug)
    return tuple(seen)


# ---------------------------------------------------------------------------
# Per-model routing metadata
# ---------------------------------------------------------------------------

#: Namespace inside ``models.params_json`` that carries routing metadata.
ROUTING_PARAMS_KEY = "routing"


@dataclass(frozen=True)
class RoutingMetadata:
    """Validated routing block read off a catalog row's ``params_json``.

    ``client_protocol`` is ``None`` when the row predates this feature (or an
    admin cleared it) — callers then fall back to the legacy transport
    inference, which is what keeps an upgraded Codex install behaving
    identically.
    """

    client_protocol: Optional[str] = None
    subscription_sources: tuple[str, ...] = ()
    #: True when discovery could not resolve protocol/source with confidence
    #: and an administrator should review the row before relying on it.
    needs_review: bool = False

    @property
    def is_empty(self) -> bool:
        return (
            self.client_protocol is None
            and not self.subscription_sources
            and not self.needs_review
        )


EMPTY_ROUTING = RoutingMetadata()


def routing_from_params(params_json: Any) -> RoutingMetadata:
    """Read the ``routing`` block from a catalog row's ``params_json``.

    Tolerant by design: a row whose ``params_json`` is a raw JSON string, a
    non-dict, or carries a bogus protocol resolves to "no explicit routing"
    rather than raising — the caller then keeps the legacy inference. Never
    invent a protocol from a malformed value.
    """
    if not isinstance(params_json, Mapping):
        return EMPTY_ROUTING
    block = params_json.get(ROUTING_PARAMS_KEY)
    if not isinstance(block, Mapping):
        return EMPTY_ROUTING

    protocol = block.get("client_protocol")
    if (
        not isinstance(protocol, str)
        or protocol.strip().lower() not in CLIENT_PROTOCOLS
    ):
        protocol = None
    else:
        protocol = protocol.strip().lower()

    sources = block.get("subscription_sources")
    if isinstance(sources, str):
        sources = [sources]
    elif not isinstance(sources, (list, tuple)):
        sources = None

    return RoutingMetadata(
        client_protocol=protocol,
        subscription_sources=normalize_channels(sources),
        needs_review=bool(block.get("needs_review")),
    )


def routing_params_block(
    *,
    client_protocol: Optional[str] = None,
    subscription_sources: Optional[Iterable[str]] = None,
    needs_review: bool = False,
) -> dict[str, Any]:
    """Build the ``params_json['routing']`` value for a catalog write.

    Only keys with real content are emitted, so a row never carries an empty
    ``subscription_sources: []`` that would read as "no source" instead of
    "source unknown".
    """
    block: dict[str, Any] = {}
    if client_protocol:
        protocol = client_protocol.strip().lower()
        if protocol not in CLIENT_PROTOCOLS:
            raise ValueError(
                f"Unknown client_protocol {client_protocol!r}; "
                f"expected one of {sorted(CLIENT_PROTOCOLS)}"
            )
        block["client_protocol"] = protocol
    channels = normalize_channels(subscription_sources)
    if channels:
        block["subscription_sources"] = list(channels)
    if needs_review:
        block["needs_review"] = True
    return block


def merge_routing_into_params(
    params_json: Any,
    routing: Mapping[str, Any],
) -> dict[str, Any]:
    """Return ``params_json`` with its ``routing`` block replaced.

    Every other key an admin set (``temperature``, ``max_output_tokens``, …)
    is preserved — rediscovery must not clobber manual catalog edits.
    """
    base: dict[str, Any] = dict(params_json) if isinstance(params_json, Mapping) else {}
    if routing:
        base[ROUTING_PARAMS_KEY] = dict(routing)
    else:
        base.pop(ROUTING_PARAMS_KEY, None)
    return base


# ---------------------------------------------------------------------------
# Resolution helpers
# ---------------------------------------------------------------------------


def is_subscription_endpoint(
    *,
    transport_kind: Optional[str] = None,
    label: Optional[str] = None,
    base_url: Optional[str] = None,
) -> bool:
    """Whether an endpoint row is the shared subscription proxy.

    Priority is deliberate: the explicit ``transport_kind`` marker first, the
    well-known labels next (covers a pre-migration read), and only then the
    legacy hostname sniff (covers a row an operator created by hand against
    the in-cluster ``*-codex-proxy`` Service before this feature existed).
    """
    if (
        transport_kind
        and transport_kind.strip().lower() == SUBSCRIPTION_PROXY_TRANSPORT
    ):
        return True
    if label and label.strip().lower() in SUBSCRIPTION_PROXY_LABELS:
        return True
    if base_url:
        host = base_url.lower()
        if LEGACY_CODEX_PROXY_ENDPOINT_LABEL in host:
            return True
        if SUBSCRIPTION_PROXY_ENDPOINT_LABEL in host:
            return True
    return False


def factory_provider_for_protocol(protocol: Optional[str]) -> Optional[str]:
    """Agent-side LLM factory name for a client protocol (``None`` on miss)."""
    if not protocol:
        return None
    return _PROTOCOL_FACTORY.get(protocol.strip().lower())


def protocol_for_factory_provider(provider: Optional[str]) -> Optional[str]:
    """Inverse of :func:`factory_provider_for_protocol`."""
    if not provider:
        return None
    slug = provider.strip().lower()
    for protocol, factory in _PROTOCOL_FACTORY.items():
        if factory == slug:
            return protocol
    return None


def applies_codex_context_cap(
    provider: Optional[str],
    subscription_sources: Iterable[str] = (),
) -> bool:
    """Whether the Codex-surface context clamp applies to this route.

    The clamp is a property of *OpenAI's Codex/ChatGPT-OAuth backend*, not of
    the Responses API and not of the proxy. So:

    - A route whose sources include ``codex`` is clamped.
    - A route with *known, non-codex* sources (Grok Build, Kimi, Claude Code,
      Antigravity) is not — those have their own limits.
    - A route with **unknown** sources that still resolved onto the ``codex``
      factory is clamped, because that is exactly the pre-feature Codex
      install whose behaviour must not change on upgrade.
    """
    channels = tuple(subscription_sources or ())
    if CHANNEL_CODEX in channels:
        return True
    if channels:
        return False
    return (provider or "").strip().lower() == "codex"


# ---------------------------------------------------------------------------
# Claude thinking visibility
# ---------------------------------------------------------------------------
#
# CLIProxyAPI's Claude executor sends a fixed ``Anthropic-Beta`` list on every
# upstream call, and that list contains ``redact-thinking-2026-02-12``. With
# that beta on, Anthropic returns **signature-only thinking blocks** — a
# ``thinking`` block whose text is empty — so a reasoning turn is billed
# (``usage.output_tokens_details.thinking_tokens`` is non-zero) but nothing
# readable ever reaches SRW. The executor's own ``ensureClaudeThinkingDisplay``
# tries to counteract this by defaulting ``thinking.display`` to
# ``"summarized"``; measured against a live Claude Code account on 2026-09-07
# that does NOT defeat the beta.
#
# The one lever a client has is the request's own ``Anthropic-Beta`` header:
# the executor *replaces* its default list with an inbound header (re-adding
# ``oauth-*`` and ``interleaved-thinking-*`` if absent), while ``betas`` in the
# body can only ever add. So SRW restates the pinned executor list minus the
# redaction beta and changes exactly one thing.
#
# This is a pin on an upstream implementation detail: it is copied from
# ``internal/runtime/executor/claude_executor_request.go`` at
# ``docker.io/eceasy/cli-proxy-api:v7.2.110``. Re-check it when the proxy pin
# moves — a beta added upstream is one this header would suppress.
#
# Re-checked at v7.3.13 (2026-09-22): the header no longer *replaces* the list.
# The executor assembles Claude Code 2.1.258's per-request betas itself, drops
# every beta it manages from an inbound header, and forwards only unmanaged
# ones — here just ``token-efficient-tools-2026-03-28``. It also sets
# ``thinking.display: summarized`` whenever ``reasoning_effort`` is present and
# omits the redaction beta once a display is set, so it should produce visible
# thinking with no header at all. The header stays because deployments still
# pin older proxies separately (prod-private is on v7.1.39), and there it is
# the only thing between SRW and signature-only thinking. Retire it once no
# supported deployment runs below v7.3.x and
# ``scripts/subscription-proxy-probe.py opus-5-noheader`` reports VISIBLE.
CLAUDE_VISIBLE_THINKING_BETAS: tuple[str, ...] = (
    "claude-code-20250219",
    "oauth-2025-04-20",
    "interleaved-thinking-2025-05-14",
    "context-management-2025-06-27",
    "prompt-caching-scope-2026-01-05",
    "structured-outputs-2025-12-15",
    "fast-mode-2026-02-01",
    "token-efficient-tools-2026-03-28",
)

#: Beta the list above deliberately omits (kept named so a drift check can
#: assert its absence rather than matching the whole string).
CLAUDE_REDACT_THINKING_BETA = "redact-thinking-2026-02-12"

ANTHROPIC_BETA_HEADER = "Anthropic-Beta"


def subscription_request_headers(
    *,
    transport_kind: Optional[str] = None,
    label: Optional[str] = None,
    base_url: Optional[str] = None,
    subscription_sources: Iterable[str] = (),
) -> dict[str, str]:
    """Transport headers a subscription-proxy route needs, if any.

    Today that is exactly one case: a route served by a Claude Code credential
    needs an ``Anthropic-Beta`` header that omits the redaction beta, or its
    reasoning comes back as empty thinking blocks. Everything else gets ``{}``
    — a Codex/Grok/Kimi/Antigravity route must not carry Anthropic headers, and
    neither must a model on an ordinary endpoint.

    Fails closed: an unknown source, an empty source list, or a non-subscription
    endpoint all yield no headers, so a row that predates routing metadata keeps
    its current behaviour instead of inheriting Anthropic's.
    """
    if CHANNEL_CLAUDE not in normalize_channels(subscription_sources):
        return {}
    if not is_subscription_endpoint(
        transport_kind=transport_kind, label=label, base_url=base_url
    ):
        return {}
    return {ANTHROPIC_BETA_HEADER: ",".join(CLAUDE_VISIBLE_THINKING_BETAS)}
