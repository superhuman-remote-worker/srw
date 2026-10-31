"""Routing metadata resolution for the subscription proxy.

The regression these guard against is the one the feature doc opens with:
identity used to be the endpoint's *label* (``codex-proxy``) or a substring of
its *hostname*, so renaming either would silently move every attached model off
the Responses factory and strip its reasoning summaries — or, in the other
direction, attach Codex-only context limits to a Grok Build route that happens
to share the same endpoint.
"""

from __future__ import annotations

import pytest

from shared.runtime.core import model_registry
from shared.runtime.core.model_registry import (
    _cap_context_window,
    _catalog_row_to_meta,
    ModelMeta,
    _endpoint_factory_provider,
    effective_client_protocol,
)
from shared.subscription_routing import (
    ANTHROPIC_BETA_HEADER,
    CLAUDE_REDACT_THINKING_BETA,
    CLAUDE_VISIBLE_THINKING_BETAS,
    PROTOCOL_OPENAI_CHAT,
    PROTOCOL_OPENAI_RESPONSES,
    SUBSCRIPTION_PROXY_TRANSPORT,
    is_subscription_endpoint,
    subscription_request_headers,
    merge_routing_into_params,
    normalize_channel,
    routing_from_params,
    routing_params_block,
    validate_routing_params,
)


def _catalog_row(**overrides):
    row = {
        "provider_kind": "endpoint",
        "provider_ref": "11111111-1111-1111-1111-111111111111",
        "endpoint_id": "11111111-1111-1111-1111-111111111111",
        "endpoint_label": "subscription-proxy",
        "endpoint_base_url": "http://srw-codex-proxy:8317/v1",
        "endpoint_transport_kind": SUBSCRIPTION_PROXY_TRANSPORT,
        "model_id": "gpt-5.6-sol",
        "display_label": "GPT 5.6 Sol",
        "family": "codex",
        "capability": "chat",
        "params_json": None,
    }
    row.update(overrides)
    return row


class TestEndpointIdentity:
    def test_transport_marker_beats_an_unrecognizable_label(self):
        assert is_subscription_endpoint(
            transport_kind=SUBSCRIPTION_PROXY_TRANSPORT,
            label="Our shared AI logins",
            base_url="https://ai.internal.example/v1",
        )

    def test_legacy_label_still_matches_before_the_backfill(self):
        assert is_subscription_endpoint(label="codex-proxy")

    def test_renamed_label_matches(self):
        assert is_subscription_endpoint(label="subscription-proxy")

    def test_hostname_fallback_for_a_hand_created_row(self):
        assert is_subscription_endpoint(base_url="http://srw-codex-proxy:8317/v1")

    def test_an_ordinary_endpoint_is_not_a_subscription(self):
        assert not is_subscription_endpoint(
            label="Local Gemma", base_url="http://vllm.ai.svc:8000/v1"
        )


class TestFactorySelection:
    def test_explicit_chat_protocol_wins_over_the_transport(self):
        """This is the whole point: Claude Code over Chat Completions must be
        able to live on the same endpoint as Codex over Responses."""
        routing = routing_from_params(
            {"routing": {"client_protocol": PROTOCOL_OPENAI_CHAT}}
        )
        assert (
            _endpoint_factory_provider(
                "http://srw-codex-proxy:8317/v1",
                "subscription-proxy",
                transport_kind=SUBSCRIPTION_PROXY_TRANSPORT,
                routing=routing,
            )
            == "openai"
        )

    def test_explicit_responses_protocol_selects_the_codex_factory(self):
        routing = routing_from_params(
            {"routing": {"client_protocol": PROTOCOL_OPENAI_RESPONSES}}
        )
        assert (
            _endpoint_factory_provider(
                "https://ai.internal.example/v1", "anything", routing=routing
            )
            == "codex"
        )

    def test_subscription_transport_without_protocol_keeps_responses(self):
        """The upgrade safety net: a pre-feature row must not change behaviour."""
        assert (
            _endpoint_factory_provider(
                "https://ai.internal.example/v1",
                "Renamed by an admin",
                transport_kind=SUBSCRIPTION_PROXY_TRANSPORT,
            )
            == "codex"
        )

    def test_plain_endpoint_defaults_to_chat_completions(self):
        assert (
            _endpoint_factory_provider("http://vllm.ai.svc:8000/v1", "Local Gemma")
            == "openai"
        )

    def test_a_bogus_stored_protocol_falls_back_instead_of_guessing(self):
        routing = routing_from_params({"routing": {"client_protocol": "grpc"}})
        assert routing.client_protocol is None
        assert (
            _endpoint_factory_provider(
                "http://vllm.ai.svc:8000/v1", "Local Gemma", routing=routing
            )
            == "openai"
        )


class TestContextCapScoping:
    """The Codex clamp belongs to OpenAI's Codex backend, not to the proxy."""

    def test_codex_source_is_clamped(self):
        assert _cap_context_window("codex", 1_000_000, "gpt-5.4", ("codex",)) == 400_000

    def test_unknown_source_on_the_codex_factory_keeps_the_legacy_clamp(self):
        assert _cap_context_window("codex", 1_000_000, "gpt-5.4", ()) == 400_000

    def test_grok_build_keeps_its_own_window_on_the_same_protocol(self):
        assert (
            _cap_context_window(
                "codex", 2_000_000, "grok-4.20-0309-reasoning", ("xai",)
            )
            == 2_000_000
        )

    def test_claude_code_keeps_its_own_window(self):
        assert (
            _cap_context_window("openai", 1_000_000, "claude-opus-4-8", ("claude",))
            == 1_000_000
        )

    def test_cap_is_a_ceiling_not_a_floor(self):
        assert (
            _cap_context_window("codex", 128_000, "gpt-5.3-codex-spark", ("codex",))
            == 128_000
        )

    def test_disabling_the_cap_restores_the_declared_window(self, monkeypatch):
        monkeypatch.setenv("CODEX_CONTEXT_WINDOW_CAP", "0")
        assert (
            _cap_context_window("codex", 1_000_000, "gpt-5.4", ("codex",)) == 1_000_000
        )


class TestCatalogRowResolution:
    def test_legacy_row_resolves_exactly_as_before(self):
        """No routing metadata + subscription transport => Responses + clamp."""
        meta = _catalog_row_to_meta(_catalog_row(context_window=1_000_000))
        assert meta.provider == "codex"
        assert meta.client_protocol == PROTOCOL_OPENAI_RESPONSES
        assert meta.context_window == 400_000
        assert meta.subscription_sources == ()

    def test_backfilled_row_resolves_identically(self):
        """What migration 0228 writes must be a behavioural no-op."""
        meta = _catalog_row_to_meta(
            _catalog_row(
                context_window=1_000_000,
                params_json={"routing": {"client_protocol": PROTOCOL_OPENAI_RESPONSES}},
            )
        )
        assert meta.provider == "codex"
        assert meta.context_window == 400_000

    def test_grok_row_shares_the_endpoint_without_inheriting_codex_limits(self):
        meta = _catalog_row_to_meta(
            _catalog_row(
                model_id="grok-4.3",
                family="default",
                context_window=1_000_000,
                params_json={
                    "routing": {
                        "client_protocol": PROTOCOL_OPENAI_RESPONSES,
                        "subscription_sources": ["xai"],
                    }
                },
            )
        )
        assert meta.provider == "codex"  # Responses factory
        assert meta.subscription_sources == ("xai",)
        assert meta.context_window == 1_000_000  # not clamped to 400k

    def test_kimi_row_uses_chat_completions(self):
        meta = _catalog_row_to_meta(
            _catalog_row(
                model_id="kimi-k2.6",
                family="default",
                context_window=262_144,
                params_json={
                    "routing": {
                        "client_protocol": PROTOCOL_OPENAI_CHAT,
                        "subscription_sources": ["kimi"],
                    }
                },
            )
        )
        assert meta.provider == "openai"
        assert meta.client_protocol == PROTOCOL_OPENAI_CHAT
        assert meta.context_window == 262_144

    def test_transport_kind_is_carried_onto_the_meta(self):
        meta = _catalog_row_to_meta(_catalog_row())
        assert meta.transport_kind == SUBSCRIPTION_PROXY_TRANSPORT

    def test_system_anchored_rows_carry_no_subscription_metadata(self):
        meta = _catalog_row_to_meta(
            {
                "provider_kind": "system",
                "provider_ref": "anthropic",
                "model_id": "claude-opus-4-8",
                "display_label": "Claude Opus 4.8",
                "family": "claude-opus",
                "capability": "chat",
                "context_window": 1_000_000,
                "params_json": None,
            }
        )
        assert meta.provider == "anthropic"
        assert meta.transport_kind is None
        assert meta.subscription_sources == ()
        assert meta.context_window == 1_000_000


class TestParamsBlock:
    def test_round_trips(self):
        block = routing_params_block(
            client_protocol=PROTOCOL_OPENAI_CHAT, subscription_sources=["Claude", "xai"]
        )
        routing = routing_from_params({"routing": block})
        assert routing.client_protocol == PROTOCOL_OPENAI_CHAT
        assert routing.subscription_sources == ("claude", "xai")

    def test_rejects_an_unknown_protocol(self):
        with pytest.raises(ValueError):
            routing_params_block(client_protocol="grpc")

    def test_empty_sources_are_omitted_not_stored_as_empty(self):
        assert routing_params_block(client_protocol=PROTOCOL_OPENAI_CHAT) == {
            "client_protocol": PROTOCOL_OPENAI_CHAT
        }

    def test_merge_preserves_other_admin_params(self):
        merged = merge_routing_into_params(
            {"temperature": 0.0, "max_output_tokens": 4096},
            routing_params_block(client_protocol=PROTOCOL_OPENAI_CHAT),
        )
        assert merged["temperature"] == 0.0
        assert merged["max_output_tokens"] == 4096
        assert merged["routing"]["client_protocol"] == PROTOCOL_OPENAI_CHAT

    def test_non_dict_params_do_not_raise(self):
        assert routing_from_params('{"routing": {}}').client_protocol is None
        assert routing_from_params(None).is_empty


class TestChannelNormalization:
    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("anthropic", "claude"),
            ("openai", "codex"),
            ("Grok", "xai"),
            ("x.ai", "xai"),
            ("moonshot", "kimi"),
            ("antigravity", "antigravity"),
        ],
    )
    def test_aliases(self, raw, expected):
        assert normalize_channel(raw) == expected

    def test_unknown_channels_survive_rather_than_vanish(self):
        assert normalize_channel("some-plugin") == "some-plugin"

    def test_blank_is_none(self):
        assert normalize_channel("  ") is None
        assert normalize_channel(None) is None


def test_module_still_exports_the_legacy_label():
    """Older imports (and the cockpit's legacy branch) still resolve."""
    assert model_registry.CODEX_PROXY_ENDPOINT_LABEL == "codex-proxy"


class TestClaudeThinkingVisibilityHeaders:
    """A Claude Code route needs its own ``Anthropic-Beta`` list.

    CLIProxyAPI's Claude executor always sends ``redact-thinking-…`` upstream
    unless the inbound request carries a beta list of its own, and under that
    beta Anthropic returns signature-only thinking blocks — reasoning that is
    billed (``thinking_tokens`` > 0) but arrives empty. Measured against a live
    Claude Code account on 2026-09-07: with the beta, 0 characters of
    reasoning; without it, a real summary at every effort level.
    """

    def test_claude_route_gets_a_beta_list_without_the_redaction_beta(self):
        headers = subscription_request_headers(
            transport_kind=SUBSCRIPTION_PROXY_TRANSPORT,
            subscription_sources=["claude"],
        )
        assert CLAUDE_REDACT_THINKING_BETA not in headers[ANTHROPIC_BETA_HEADER]
        # …and still carries the betas the executor would otherwise have sent,
        # because an inbound header REPLACES the executor's list rather than
        # extending it.
        assert "claude-code-20250219" in headers[ANTHROPIC_BETA_HEADER]
        assert "prompt-caching-scope-2026-01-05" in headers[ANTHROPIC_BETA_HEADER]

    def test_the_pinned_list_never_contains_the_redaction_beta(self):
        """Drift guard for the constant itself, independent of the resolver."""
        assert CLAUDE_REDACT_THINKING_BETA not in CLAUDE_VISIBLE_THINKING_BETAS

    @pytest.mark.parametrize("channel", ["codex", "xai", "kimi", "antigravity"])
    def test_other_accounts_on_the_same_endpoint_get_nothing(self, channel):
        """One transport, many accounts: an Anthropic header on a Codex route
        is exactly the label-inference bug this feature removed."""
        assert (
            subscription_request_headers(
                transport_kind=SUBSCRIPTION_PROXY_TRANSPORT,
                subscription_sources=[channel],
            )
            == {}
        )

    def test_alias_spelling_is_resolved(self):
        """The login provider is ``anthropic``; the credential channel is
        ``claude``. Either spelling must reach the same route."""
        assert subscription_request_headers(
            transport_kind=SUBSCRIPTION_PROXY_TRANSPORT,
            subscription_sources=["anthropic"],
        )

    def test_pooled_route_including_claude_still_gets_it(self):
        assert subscription_request_headers(
            transport_kind=SUBSCRIPTION_PROXY_TRANSPORT,
            subscription_sources=["xai", "claude"],
        )

    def test_unattributed_row_gets_nothing(self):
        """Fails closed — a row imported before routing metadata existed keeps
        the behaviour it has today rather than inheriting Anthropic's."""
        assert (
            subscription_request_headers(
                transport_kind=SUBSCRIPTION_PROXY_TRANSPORT, subscription_sources=[]
            )
            == {}
        )

    def test_claude_on_an_ordinary_endpoint_gets_nothing(self):
        """The header describes the *proxy*, not the vendor: a Claude model on
        some other OpenAI-compatible gateway must not receive it."""
        assert (
            subscription_request_headers(
                transport_kind=None,
                label="Local Router",
                base_url="https://ai.example.test/v1",
                subscription_sources=["claude"],
            )
            == {}
        )

    def test_legacy_label_still_resolves(self):
        """A pre-migration row (label only, no transport marker) is still the
        subscription proxy."""
        assert subscription_request_headers(
            label="codex-proxy", subscription_sources=["claude"]
        )


class TestAnthropicMessages:
    def test_anthropic_messages_selects_the_anthropic_factory(self):
        row = _catalog_row(
            params_json={"routing": {"client_protocol": "anthropic-messages"}}
        )
        meta = _catalog_row_to_meta(row)
        assert meta.provider == "anthropic"
        assert meta.client_protocol == "anthropic-messages"

    def test_effective_protocol_falls_back_to_the_provider(self):
        def meta(provider, family):
            return ModelMeta(
                model_id="m", provider=provider, family=family, display_name="m"
            )

        assert effective_client_protocol(meta("anthropic", "claude")) == (
            "anthropic-messages"
        )
        assert effective_client_protocol(meta("codex", "gpt")) == "openai-responses"
        assert effective_client_protocol(meta("openrouter", "x")) == "openai-chat"
        assert effective_client_protocol(meta("google", "g")) == "google-native"

    def test_a_garbage_stored_protocol_resolves_to_the_provider_default(self):
        row = _catalog_row(params_json={"routing": {"client_protocol": "grpc"}})
        assert (
            effective_client_protocol(_catalog_row_to_meta(row)) == "openai-responses"
        )

    def test_validate_routing_params_refuses_unknown_and_non_endpoint_rows(self):
        validate_routing_params(
            {"routing": {"client_protocol": "anthropic-messages"}},
            provider_kind="endpoint",
        )
        with pytest.raises(ValueError, match="client_protocol"):
            validate_routing_params(
                {"routing": {"client_protocol": "grpc"}}, provider_kind="endpoint"
            )
        with pytest.raises(ValueError, match="endpoint"):
            validate_routing_params(
                {"routing": {"client_protocol": "anthropic-messages"}},
                provider_kind="system",
            )
        validate_routing_params(None, provider_kind="system")
        validate_routing_params({"max_output_tokens": 5}, provider_kind="system")
