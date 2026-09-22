"""Registry of the subscription providers SRW can connect through the proxy.

One entry per **product** an administrator can sign in to in
Settings → AI Subscriptions. Each entry binds together four things that are
deliberately *not* the same string:

``key``
    SRW's stable provider id. Used in API payloads and i18n keys; never sent
    upstream.

``login_path`` / ``login_flow``
    The CLIProxyAPI management handler that starts the flow and the shape of
    that flow. Verified against the pinned image
    (``docker.io/eceasy/cli-proxy-api:v7.2.110``) on 2026-09-07 —
    ``internal/api/server_management.go`` registers exactly five built-in
    provider handlers (v7.3.13 adds Devin, Meta and a second Kimi route that
    SRW does not wire; the five below are unchanged), and
    ``internal/api/handlers/management/auth_files_provider_oauth.go`` shows
    which of them wait on a browser callback (Codex, Claude, Antigravity) and
    which run a device-authorization poll (Grok Build, Kimi Code).

``callback_provider``
    The provider slug the shared callback endpoint accepts for this flow.
    It is *not* always the login key nor the credential channel: Anthropic
    registers its OAuth session as ``anthropic`` while the saved credential's
    provider is ``claude``. Device flows have no callback at all.

``channels``
    The ``auth-files`` ``provider``/``type`` values whose credentials belong to
    this connection — the model-provenance key.

Availability is decided from this table and the configured proxy, never by
calling an auth-url handler: those handlers *start a real authorization
session* (and, for a browser flow, bind a local callback port) as a side
effect of being asked whether they exist.

Design: knowledge-base/knowledge/features/subscription_proxy.md §5.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from shared.subscription_routing import (
    CHANNEL_ANTIGRAVITY,
    CHANNEL_CLAUDE,
    CHANNEL_CODEX,
    CHANNEL_KIMI,
    CHANNEL_XAI,
    PROTOCOL_OPENAI_CHAT,
    PROTOCOL_OPENAI_RESPONSES,
)

#: Browser OAuth: the provider redirects to a localhost callback. The proxy
#: runs the redirect target itself; SRW additionally offers a paste-the-URL
#: field because in a Kubernetes deployment the redirect lands on the
#: *administrator's* localhost, not the proxy pod's.
LOGIN_FLOW_BROWSER = "browser"

#: Device authorization: the proxy polls the provider. No callback, no URL to
#: paste — SRW shows the verification link, the user code and the expiry.
LOGIN_FLOW_DEVICE = "device"


@dataclass(frozen=True)
class SubscriptionProvider:
    """One connectable subscription product."""

    key: str
    #: Product label. English source string; the cockpit renders its own
    #: translated label from ``subscriptions.providers.<key>.label``.
    label: str
    #: Vendor, for grouping in the UI.
    vendor: str
    login_path: str
    login_flow: str
    #: Slug accepted by ``POST /v0/management/oauth-callback`` for this flow.
    #: ``None`` for device flows, which never receive a callback.
    callback_provider: str | None
    #: Upstream credential channels that belong to this connection.
    channels: tuple[str, ...]
    #: Client protocol SRW sends for models discovered on these channels.
    client_protocol: str
    #: Whether SRW has a working provider-specific usage/quota reader.
    has_usage_reader: bool = False
    #: Whether SRW has *live-verified* inference through this connection.
    #: Set only after a real account has passed the §9 integration checks;
    #: the UI surfaces the difference rather than implying every listed
    #: product is proven.
    inference_verified: bool = False
    #: Free-form notes rendered as a caveat in the connect dialog.
    notes: tuple[str, ...] = field(default_factory=tuple)


SUBSCRIPTION_PROVIDERS: tuple[SubscriptionProvider, ...] = (
    SubscriptionProvider(
        key="openai-codex",
        label="OpenAI · ChatGPT / Codex",
        vendor="openai",
        login_path="/v0/management/codex-auth-url",
        login_flow=LOGIN_FLOW_BROWSER,
        callback_provider="codex",
        channels=(CHANNEL_CODEX,),
        # The established SRW path: Responses carries the reasoning summary
        # that Chat Completions drops. Do not change without re-verifying.
        client_protocol=PROTOCOL_OPENAI_RESPONSES,
        has_usage_reader=True,
        inference_verified=True,
    ),
    SubscriptionProvider(
        key="anthropic-claude-code",
        label="Anthropic · Claude Code",
        vendor="anthropic",
        login_path="/v0/management/anthropic-auth-url",
        login_flow=LOGIN_FLOW_BROWSER,
        # The OAuth session registers as ``anthropic``; the saved credential's
        # channel is ``claude``. Both spellings normalize upstream, but the
        # callback must carry the *session* provider.
        callback_provider="anthropic",
        channels=(CHANNEL_CLAUDE,),
        # The proxy's Claude executor translates from whatever the client
        # sends (``internal/translator/claude/openai/{chat-completions,
        # responses}`` are both registered at v7.2.110). Chat Completions is
        # SRW's generic, best-exercised client path.
        client_protocol=PROTOCOL_OPENAI_CHAT,
    ),
    SubscriptionProvider(
        key="google-antigravity",
        label="Google · Antigravity",
        vendor="google",
        login_path="/v0/management/antigravity-auth-url",
        login_flow=LOGIN_FLOW_BROWSER,
        callback_provider="antigravity",
        channels=(CHANNEL_ANTIGRAVITY,),
        client_protocol=PROTOCOL_OPENAI_CHAT,
        notes=(
            "Antigravity access is a separate product entitlement — a Google "
            "API key does not grant it.",
        ),
    ),
    SubscriptionProvider(
        key="xai-grok-build",
        label="xAI · Grok Build",
        vendor="xai",
        login_path="/v0/management/xai-auth-url",
        login_flow=LOGIN_FLOW_DEVICE,
        callback_provider=None,
        channels=(CHANNEL_XAI,),
        # The xAI executor's native target is the Responses format
        # (``prepareResponsesRequestTo(..., sdktranslator.FormatCodex)``), so
        # sending Responses avoids a lossy chat→responses conversion.
        client_protocol=PROTOCOL_OPENAI_RESPONSES,
        notes=(
            "Grok Build is its own entitlement; an ordinary Grok subscription "
            "does not enable it.",
        ),
    ),
    SubscriptionProvider(
        key="kimi-code",
        label="Moonshot · Kimi Code",
        vendor="moonshot",
        login_path="/v0/management/kimi-auth-url",
        login_flow=LOGIN_FLOW_DEVICE,
        callback_provider=None,
        channels=(CHANNEL_KIMI,),
        # The Kimi executor posts Chat Completions upstream
        # (``kimiauth.KimiAPIBaseURL + "/v1/chat/completions"``).
        client_protocol=PROTOCOL_OPENAI_CHAT,
    ),
)

PROVIDERS_BY_KEY: dict[str, SubscriptionProvider] = {
    provider.key: provider for provider in SUBSCRIPTION_PROVIDERS
}

#: Reverse index: upstream credential channel -> owning provider.
PROVIDER_BY_CHANNEL: dict[str, SubscriptionProvider] = {
    channel: provider
    for provider in SUBSCRIPTION_PROVIDERS
    for channel in provider.channels
}


def get_provider(key: str | None) -> SubscriptionProvider | None:
    """Look up a provider by SRW key (``None`` for unknown/empty)."""
    if not key:
        return None
    return PROVIDERS_BY_KEY.get(key.strip().lower())


def provider_for_channel(channel: str | None) -> SubscriptionProvider | None:
    """Owning provider for an upstream credential channel, if we know it."""
    if not channel:
        return None
    return PROVIDER_BY_CHANNEL.get(channel.strip().lower())


def client_protocol_for_channels(channels: tuple[str, ...]) -> str | None:
    """Protocol to use for a model served by ``channels``.

    A pooled route needs **one** protocol across every eligible account. When
    the connected channels disagree (a model advertised by both Claude Code and
    Grok Build, say) we return ``None`` rather than picking the first — the
    caller then marks the candidate for review instead of silently attaching
    one provider's behaviour to a mixed route.
    """
    protocols = {
        provider.client_protocol
        for provider in (provider_for_channel(c) for c in channels)
        if provider is not None
    }
    if len(protocols) == 1:
        return protocols.pop()
    return None
