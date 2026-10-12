"""Discovery, attribution and catalog reconciliation for the subscription proxy.

``/v1/models`` is an advertised inventory with four fields; everything that
makes a candidate registrable — which account serves it, which protocol that
account needs, what its real limits are — comes from enrichment. The rules
tested here are the ones that keep a bulk import honest:

* a media-generation model advertised beside the chat models is never imported
  as a chat model, even when explicitly selected;
* an already-registered model is skipped, never rewritten, so a rediscovery
  cannot clobber an admin's label/limits/enabled state;
* a failed enrichment yields *incomplete metadata*, never an authoritative
  empty inventory and never an invented source.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from orchestrator.services import subscription_discovery as discovery
from orchestrator.services import subscriptions
from orchestrator.services.llm_endpoint_probe import ProbeResult
from orchestrator.services.subscriptions import (
    SubscriptionProxyError,
    account_from_entry,
)
from shared.subscription_routing import (
    PROTOCOL_OPENAI_CHAT,
    PROTOCOL_OPENAI_RESPONSES,
)

ENDPOINT_ID = "11111111-1111-1111-1111-111111111111"


def _account(name, provider):
    account = account_from_entry(
        {"name": name, "provider": provider, "status": "active"}
    )
    assert account is not None
    return account


def _probe(model_ids):
    return ProbeResult(
        ok=True,
        status=200,
        error=None,
        probe_url="http://proxy/v1/models",
        models=[
            {
                "id": model_id,
                "owned_by": "openai",
                "capability_hints": ["chat", "auxiliary"],
                "family": None,
                "context_window": None,
            }
            for model_id in model_ids
        ],
    )


@pytest.fixture
def wired(monkeypatch):
    """Patch the three upstream reads discovery composes."""
    state = {
        "probe": _probe([]),
        "attribution": ({}, []),
        "definitions": {},
    }

    async def fake_probe(*, base_url, api_key, **kwargs):
        return state["probe"]

    async def fake_attribution():
        result = state["attribution"]
        if isinstance(result, Exception):
            raise result
        return result

    async def fake_definitions(channel):
        return state["definitions"].get(channel, {})

    monkeypatch.setattr(
        "orchestrator.services.llm_endpoint_probe.probe_endpoint_models", fake_probe
    )
    monkeypatch.setattr(discovery, "account_model_map", fake_attribution)
    monkeypatch.setattr(discovery, "channel_model_definitions", fake_definitions)
    return state


async def _discover(catalog_rows=()):
    return await discovery.discover_subscription_models(
        base_url="http://proxy/v1", api_key=None, catalog_rows=catalog_rows
    )


class TestModalityClassification:
    @pytest.mark.parametrize(
        "model_id",
        [
            "gpt-image-2",
            "gpt-image-1.5",
            "grok-imagine-image",
            "grok-imagine-video-1.5-preview",
            "gemini-3.1-flash-image",
        ],
    )
    def test_known_media_models_are_not_chat(self, model_id):
        assert not discovery.is_chat_candidate(model_id, None)

    @pytest.mark.parametrize(
        "model_id",
        ["gpt-5.6-sol", "claude-opus-4-8", "grok-4.3", "kimi-k2.6", "gemini-3-flash"],
    )
    def test_chat_models_are_chat(self, model_id):
        assert discovery.is_chat_candidate(model_id, None)

    def test_embedding_and_speech_models_are_not_chat(self):
        assert not discovery.is_chat_candidate("text-embedding-3-large", None)
        assert not discovery.is_chat_candidate("whisper-1", None)

    def test_a_name_that_merely_contains_a_word_is_not_swept_up(self):
        assert discovery.is_chat_candidate("vision-pro-chat", None)


class TestAttribution:
    @pytest.mark.asyncio
    async def test_sources_and_protocol_come_from_the_serving_account(self, wired):
        wired["probe"] = _probe(["kimi-k2.6"])
        wired["attribution"] = ({"kimi-k2.6": [_account("kimi-1.json", "kimi")]}, [])
        result = await _discover()
        candidate = result.candidates[0]
        assert candidate.sources == ("kimi",)
        assert candidate.providers == ("kimi-code",)
        assert candidate.client_protocol == PROTOCOL_OPENAI_CHAT
        assert candidate.support == discovery.SUPPORT_SUPPORTED

    @pytest.mark.asyncio
    async def test_codex_keeps_the_responses_protocol(self, wired):
        wired["probe"] = _probe(["gpt-5.6-sol"])
        wired["attribution"] = (
            {"gpt-5.6-sol": [_account("codex-a.json", "codex")]},
            [],
        )
        candidate = (await _discover()).candidates[0]
        assert candidate.client_protocol == PROTOCOL_OPENAI_RESPONSES

    @pytest.mark.asyncio
    async def test_a_model_served_by_two_accounts_keeps_both_sources(self, wired):
        wired["probe"] = _probe(["claude-sonnet-4-6"])
        wired["attribution"] = (
            {
                "claude-sonnet-4-6": [
                    _account("claude-a.json", "claude"),
                    _account("claude-b.json", "claude"),
                ]
            },
            [],
        )
        candidate = (await _discover()).candidates[0]
        assert candidate.sources == ("claude",)
        assert len(candidate.account_ids) == 2

    @pytest.mark.asyncio
    async def test_conflicting_protocols_across_sources_need_review(self, wired):
        """Claude Code wants Chat Completions, Grok Build wants Responses. A
        pooled route cannot silently pick one."""
        wired["probe"] = _probe(["shared-model"])
        wired["attribution"] = (
            {
                "shared-model": [
                    _account("claude-a.json", "claude"),
                    _account("xai-a.json", "xai"),
                ]
            },
            [],
        )
        candidate = (await _discover()).candidates[0]
        assert candidate.support == discovery.SUPPORT_NEEDS_REVIEW
        assert candidate.support_reason == "mixed_source_protocols"
        assert candidate.client_protocol is None

    @pytest.mark.asyncio
    async def test_unattributed_model_needs_review(self, wired):
        wired["probe"] = _probe(["mystery-model"])
        wired["attribution"] = ({}, [])
        candidate = (await _discover()).candidates[0]
        assert candidate.support == discovery.SUPPORT_NEEDS_REVIEW
        assert candidate.support_reason == "unknown_source"

    @pytest.mark.asyncio
    async def test_failed_enrichment_reports_incomplete_not_empty(self, wired):
        wired["probe"] = _probe(["gpt-5.6-sol"])
        wired["attribution"] = SubscriptionProxyError("management API down")
        result = await _discover()
        assert result.ok is True
        assert len(result.candidates) == 1
        assert result.attribution_complete is False

    @pytest.mark.asyncio
    async def test_unreadable_account_is_named(self, wired):
        wired["probe"] = _probe(["gpt-5.6-sol"])
        wired["attribution"] = ({}, ["acct-1"])
        result = await _discover()
        assert result.unreadable_account_ids == ["acct-1"]
        assert result.attribution_complete is False

    @pytest.mark.asyncio
    async def test_failed_probe_is_not_an_empty_inventory(self, wired):
        wired["probe"] = ProbeResult(
            ok=False, status=502, error="boom", probe_url="http://proxy/v1/models"
        )
        result = await _discover()
        assert result.ok is False
        assert result.candidates == []


class TestDefinitionEnrichment:
    @pytest.mark.asyncio
    async def test_limits_and_label_come_from_the_static_definition(self, wired):
        wired["probe"] = _probe(["grok-4.3"])
        wired["attribution"] = ({"grok-4.3": [_account("xai-a.json", "xai")]}, [])
        wired["definitions"] = {
            "xai": {
                "grok-4.3": {
                    "id": "grok-4.3",
                    "display_name": "Grok 4.3",
                    "context_length": 1_000_000,
                    "max_completion_tokens": 65536,
                }
            }
        }
        candidate = (await _discover()).candidates[0]
        assert candidate.display_label == "Grok 4.3"
        assert candidate.context_window == 1_000_000
        assert candidate.max_output_tokens == 65536


class TestRegistrationState:
    @pytest.mark.asyncio
    async def test_registered_rows_are_identified(self, wired):
        wired["probe"] = _probe(["gpt-5.6-sol"])
        wired["attribution"] = (
            {"gpt-5.6-sol": [_account("codex-a.json", "codex")]},
            [],
        )
        candidate = (
            await _discover(catalog_rows=[{"id": "cat-1", "model_id": "gpt-5.6-sol"}])
        ).candidates[0]
        assert candidate.registered is True
        assert candidate.registered_catalog_id == "cat-1"

    @pytest.mark.asyncio
    async def test_a_backfilled_row_is_not_reported_as_drifting(self, wired):
        wired["probe"] = _probe(["gpt-5.6-sol"])
        wired["attribution"] = (
            {"gpt-5.6-sol": [_account("codex-a.json", "codex")]},
            [],
        )
        candidate = (
            await _discover(
                catalog_rows=[
                    {
                        "id": "cat-1",
                        "model_id": "gpt-5.6-sol",
                        "params_json": {
                            "routing": {"client_protocol": PROTOCOL_OPENAI_RESPONSES}
                        },
                    }
                ]
            )
        ).candidates[0]
        assert candidate.routing_drift is False

    @pytest.mark.asyncio
    async def test_contradicting_protocol_is_surfaced_as_drift(self, wired):
        wired["probe"] = _probe(["kimi-k2.6"])
        wired["attribution"] = ({"kimi-k2.6": [_account("kimi-1.json", "kimi")]}, [])
        candidate = (
            await _discover(
                catalog_rows=[
                    {
                        "id": "cat-1",
                        "model_id": "kimi-k2.6",
                        "params_json": {
                            "routing": {"client_protocol": PROTOCOL_OPENAI_RESPONSES}
                        },
                    }
                ]
            )
        ).candidates[0]
        assert candidate.routing_drift is True

    @pytest.mark.asyncio
    async def test_a_disconnected_account_is_not_drift(self, wired):
        """A stored source that is temporarily offline is not a catalog defect."""
        wired["probe"] = _probe(["claude-sonnet-4-6"])
        wired["attribution"] = (
            {"claude-sonnet-4-6": [_account("claude-a.json", "claude")]},
            [],
        )
        candidate = (
            await _discover(
                catalog_rows=[
                    {
                        "id": "cat-1",
                        "model_id": "claude-sonnet-4-6",
                        "params_json": {
                            "routing": {
                                "client_protocol": PROTOCOL_OPENAI_CHAT,
                                "subscription_sources": ["claude", "antigravity"],
                            }
                        },
                    }
                ]
            )
        ).candidates[0]
        assert candidate.routing_drift is False


class TestImport:
    def _db(self):
        db = AsyncMock()
        db.create_model = AsyncMock(return_value={"id": "new-row"})
        return db

    def _candidate(self, **overrides):
        base = dict(
            model_id="kimi-k2.6",
            display_label="Kimi K2.6",
            sources=("kimi",),
            providers=("kimi-code",),
            client_protocol=PROTOCOL_OPENAI_CHAT,
            context_window=262_144,
            max_output_tokens=65536,
        )
        base.update(overrides)
        return discovery.ModelCandidate(**base)

    @pytest.mark.asyncio
    async def test_add_all_registers_supported_models_with_routing(self):
        db = self._db()
        outcome = await discovery.import_candidates(
            db=db,
            endpoint_id=ENDPOINT_ID,
            candidates=[self._candidate()],
            requested_ids=None,
            include_review=False,
        )
        assert outcome.created == ["kimi-k2.6"]
        kwargs = db.create_model.await_args.kwargs
        assert kwargs["provider_kind"] == "endpoint"
        assert kwargs["provider_ref"] == ENDPOINT_ID
        assert kwargs["model_id"] == "kimi-k2.6"
        assert kwargs["context_window"] == 262_144
        assert kwargs["on_conflict_do_nothing"] is True
        routing = kwargs["params_json"]["routing"]
        assert routing["client_protocol"] == PROTOCOL_OPENAI_CHAT
        assert routing["subscription_sources"] == ["kimi"]
        assert kwargs["params_json"]["max_output_tokens"] == 65536

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("advertised", "cap", "stored"),
        [
            (262_144, 200_000, 200_000),  # the cap lowers the advertised maximum
            (131_072, 200_000, 131_072),  # ...and never raises it
        ],
    )
    async def test_context_window_cap_clamps_the_advertised_maximum(
        self, advertised, cap, stored
    ):
        db = self._db()
        await discovery.import_candidates(
            db=db,
            endpoint_id=ENDPOINT_ID,
            candidates=[self._candidate(context_window=advertised)],
            requested_ids=None,
            include_review=False,
            context_window_cap=cap,
        )
        assert db.create_model.await_args.kwargs["context_window"] == stored

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("family_window", "stored"),
        [
            (128_000, None),  # the family default already fits: keep tracking it
            (1_000_000, 200_000),  # the family default exceeds the cap: pin the cap
            (None, 200_000),  # unknown family default: pin the cap
        ],
    )
    async def test_context_window_cap_without_an_advertised_maximum(
        self, monkeypatch, family_window, stored
    ):
        from shared.runtime.core import model_registry

        monkeypatch.setattr(
            model_registry, "_family_context_window", lambda _model_id: family_window
        )
        db = self._db()
        await discovery.import_candidates(
            db=db,
            endpoint_id=ENDPOINT_ID,
            candidates=[self._candidate(context_window=None)],
            requested_ids=None,
            include_review=False,
            context_window_cap=200_000,
        )
        assert db.create_model.await_args.kwargs["context_window"] == stored

    @pytest.mark.asyncio
    async def test_add_selected_ignores_the_rest(self):
        db = self._db()
        outcome = await discovery.import_candidates(
            db=db,
            endpoint_id=ENDPOINT_ID,
            candidates=[self._candidate(), self._candidate(model_id="kimi-k2.5")],
            requested_ids=["kimi-k2.5"],
            include_review=False,
        )
        assert outcome.created == ["kimi-k2.5"]
        assert db.create_model.await_count == 1

    @pytest.mark.asyncio
    async def test_registered_rows_are_skipped_not_rewritten(self):
        db = self._db()
        outcome = await discovery.import_candidates(
            db=db,
            endpoint_id=ENDPOINT_ID,
            candidates=[self._candidate(registered=True)],
            requested_ids=None,
            include_review=False,
        )
        assert outcome.skipped == ["kimi-k2.6"]
        db.create_model.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_media_models_are_refused_even_when_named(self):
        db = self._db()
        outcome = await discovery.import_candidates(
            db=db,
            endpoint_id=ENDPOINT_ID,
            candidates=[
                self._candidate(
                    model_id="gpt-image-2",
                    support=discovery.SUPPORT_UNSUPPORTED_MODALITY,
                    support_reason="media_generation",
                    capabilities=(),
                )
            ],
            requested_ids=["gpt-image-2"],
            include_review=False,
        )
        assert outcome.rejected == [
            {"id": "gpt-image-2", "reason": "unsupported_modality"}
        ]
        db.create_model.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_needs_review_is_excluded_from_add_all(self):
        db = self._db()
        outcome = await discovery.import_candidates(
            db=db,
            endpoint_id=ENDPOINT_ID,
            candidates=[
                self._candidate(
                    model_id="mystery",
                    client_protocol=None,
                    support=discovery.SUPPORT_NEEDS_REVIEW,
                    support_reason="unknown_source",
                )
            ],
            requested_ids=None,
            include_review=False,
        )
        assert outcome.created == []
        assert outcome.rejected == [{"id": "mystery", "reason": "unknown_source"}]

    @pytest.mark.asyncio
    async def test_opting_in_to_review_stores_the_flag_and_a_neutral_protocol(self):
        db = self._db()
        await discovery.import_candidates(
            db=db,
            endpoint_id=ENDPOINT_ID,
            candidates=[
                self._candidate(
                    model_id="mystery",
                    client_protocol=None,
                    sources=(),
                    support=discovery.SUPPORT_NEEDS_REVIEW,
                    support_reason="unknown_source",
                )
            ],
            requested_ids=["mystery"],
            include_review=True,
        )
        routing = db.create_model.await_args.kwargs["params_json"]["routing"]
        # Never Responses: an unknown route must not inherit Codex behaviour.
        assert routing["client_protocol"] == PROTOCOL_OPENAI_CHAT
        assert routing["needs_review"] is True
        assert "subscription_sources" not in routing

    @pytest.mark.asyncio
    async def test_a_conflicting_insert_counts_as_skipped(self):
        db = self._db()
        db.create_model = AsyncMock(return_value=None)
        outcome = await discovery.import_candidates(
            db=db,
            endpoint_id=ENDPOINT_ID,
            candidates=[self._candidate()],
            requested_ids=None,
            include_review=False,
        )
        assert outcome.skipped == ["kimi-k2.6"]
        assert outcome.created == []


class TestAdvertisedModelIds:
    @pytest.mark.asyncio
    async def test_unreachable_proxy_degrades_to_empty(self, monkeypatch):
        async def boom(*args, **kwargs):
            raise SubscriptionProxyError("down")

        monkeypatch.setattr(subscriptions, "management_request", boom)
        monkeypatch.setattr(discovery, "management_request", boom)
        assert await discovery.advertised_model_ids() == set()
