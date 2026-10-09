"""A model that cannot run is refused with its name, never routed by a guess.

Design: knowledge-base/knowledge/features/unavailable_model_handling.md.
Incident: main-dev sessions 0592c419 / 9c19d0eb (2026-10-08) — an account
default naming the disabled ``MiniMax-M3`` reached the agent with no route and
the user saw OpenAI's ``Incorrect API key provided: not-needed`` 401.
"""

from __future__ import annotations

import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import httpx
import pytest
from fastapi import HTTPException

from orchestrator.services import dispatch_credentials as dc
from orchestrator.services import model_availability as ma
from orchestrator.services import stateless_input_admission as sia
from orchestrator.services.config_resolver import inject_blob_credentials
from shared.runtime.core.model_registry import ModelMeta, UnknownModelError

ENDPOINT_ID = "aac23bfd-ff30-4c00-a794-003033042ea6"
PROXY_URL = "http://srw-codex-proxy:8317/v1"

_METAS = {
    "gpt-6-astra": ModelMeta(
        model_id="gpt-6-astra",
        provider="openai",
        family="default",
        display_name="gpt-6-astra",
        origin="catalog",
        endpoint_id=ENDPOINT_ID,
    ),
}

# Catalog rows regardless of ``enabled`` (store.catalog_model_states).
_CATALOG = {
    "MiniMax-M3": [{"enabled": False, "capabilities": ["chat", "auxiliary", "vision"]}],
    "chat-only": [{"enabled": True, "capabilities": ["chat"]}],
    "gpt-6-astra": [{"enabled": True, "capabilities": ["chat", "auxiliary"]}],
}


async def _resolve(model_id, user_id=None, capability="chat"):
    meta = _METAS.get(model_id)
    if meta is None:
        raise UnknownModelError(model_id)
    return meta


def _store(**overrides):
    async def catalog_model_states(model_id):
        return list(_CATALOG.get(model_id, []))

    async def get_user_llm_endpoint(endpoint_id):
        if endpoint_id == ENDPOINT_ID:
            return {"id": ENDPOINT_ID, "base_url": PROXY_URL, "api_key": "proxy-key"}
        return None

    store = SimpleNamespace(
        catalog_model_states=AsyncMock(side_effect=catalog_model_states),
        get_user_llm_endpoint=AsyncMock(side_effect=get_user_llm_endpoint),
        resolve_api_keys_for_job=AsyncMock(return_value={"openai": "sk-real-openai"}),
        resolve_default_for_capability=AsyncMock(return_value=None),
        get_user=AsyncMock(return_value={"is_admin": False}),
    )
    for key, value in overrides.items():
        setattr(store, key, value)
    return store


def _deps(store=None):
    return dc.DispatchCredentialDependencies(
        store=store or _store(),
        logger=logging.getLogger("test.unavailable_model"),
        resolve_model=AsyncMock(side_effect=_resolve),
    )


# ---------------------------------------------------------------------------
# Availability lookup and messages
# ---------------------------------------------------------------------------


class TestReasons:
    @pytest.mark.asyncio
    async def test_disabled_capability_and_unknown_are_told_apart(self):
        store = _store()
        assert await ma.unresolved_model_reason(store, "MiniMax-M3") == "disabled"
        assert await ma.unresolved_model_reason(store, "chat-only") == "capability"
        assert await ma.unresolved_model_reason(store, "nope") == "unknown"

    @pytest.mark.asyncio
    async def test_a_failed_lookup_only_changes_the_wording(self):
        store = _store(catalog_model_states=AsyncMock(side_effect=RuntimeError("db")))
        assert await ma.unresolved_model_reason(store, "MiniMax-M3") == "unknown"
        assert await ma.unresolved_model_reason(object(), "MiniMax-M3") == "unknown"

    @pytest.mark.asyncio
    async def test_a_resolvable_model_has_no_reason(self):
        reason = await ma.model_unavailable_reason(
            "gpt-6-astra",
            user_id="u",
            capability="chat",
            store=_store(),
            resolve_model=AsyncMock(side_effect=_resolve),
        )
        assert reason is None

    def test_explicit_transport_keeps_an_unknown_model_but_never_a_disabled_one(self):
        section = {"model": "x", "base_url": "http://self-hosted/v1"}
        assert ma.keeps_explicit_transport(section, "unknown") is True
        assert ma.keeps_explicit_transport({"provider": "openai"}, "capability") is True
        assert ma.keeps_explicit_transport(section, "disabled") is False
        assert ma.keeps_explicit_transport({"model": "x"}, "unknown") is False


class TestMessages:
    def test_disabled_main_model_names_the_model_and_where_to_change_it(self):
        text = ma.ModelUnavailable(
            [ma.UnavailableModel("llm", "MiniMax-M3", "disabled")]
        ).message(where=ma.WHERE_SESSION)
        assert text == (
            "The model `MiniMax-M3` (main model) is no longer available. Choose "
            "another model in this session's settings, or ask your administrator."
        )

    def test_admins_get_the_re_enable_hint(self):
        refusal = ma.ModelUnavailable(
            [ma.UnavailableModel("auxiliary", "MiniMax-M3", "disabled")]
        )
        assert "Admin → Models" in refusal.message(is_admin=True)
        assert "Admin → Models" not in refusal.message(is_admin=False)

    def test_unknown_and_roster_slots_read_naturally(self):
        text = ma.render_unavailable_message(
            [ma.UnavailableModel("subagents.roster.reader.llm", "ghost", "unknown")],
            where=ma.WHERE_JOB,
        )
        assert text == (
            "The model `ghost` (helper agent reader) is not configured on this "
            "installation. Choose another model in the job's configuration."
        )

    def test_the_message_fits_the_chat_banner(self):
        entries = [
            ma.UnavailableModel(f"subagents.roster.agent{i}.llm", "m" * 40, "disabled")
            for i in range(10)
        ]
        assert len(ma.render_unavailable_message(entries)) <= 240

    def test_detail_is_the_object_the_cockpit_reads(self):
        detail = ma.ModelUnavailable(
            [ma.UnavailableModel("llm", "MiniMax-M3", "disabled")]
        ).detail(where=ma.WHERE_SESSION)
        assert detail["code"] == "model.unavailable"
        assert detail["entries"] == [
            {"slot": "llm", "model": "MiniMax-M3", "reason": "disabled"}
        ]
        assert "MiniMax-M3" in detail["message"]


class TestSlots:
    def test_inherited_and_sentinel_slots_are_marked(self):
        config = {
            "llm": {"model": "a", "summarization": {"model": "b"}},
            "auxiliary": {"model": "c"},
            "subagents": {
                "llm": {"model": "inherit"},
                "roster": {
                    "reader": {"llm": {"model": "a", "_inherit_llm": True}},
                    "pinned": {"llm": {"model": "d"}},
                },
            },
        }
        slots = {s.label: s for s in ma.model_slots(config)}
        assert set(slots) == {
            "llm",
            "llm.summarization",
            "auxiliary",
            "subagents.llm",
            "subagents.roster.reader.llm",
            "subagents.roster.pinned.llm",
        }
        assert slots["subagents.roster.reader.llm"].inherited is True
        assert ma.slot_model(slots["subagents.llm"]) is None
        assert slots["auxiliary"].capability == "auxiliary"

    @pytest.mark.asyncio
    async def test_unavailable_slots_skips_inherited_and_explicit_routes(self):
        config = {
            "llm": {"model": "MiniMax-M3"},
            "auxiliary": {"model": "gpt-6-astra"},
            "subagents": {
                "roster": {
                    "reader": {"llm": {"model": "MiniMax-M3", "_inherit_llm": True}},
                    "local": {
                        "llm": {"model": "self-hosted", "base_url": "http://x/v1"}
                    },
                }
            },
        }
        found = await ma.unavailable_slots(
            config,
            user_id="u",
            store=_store(),
            resolve_model=AsyncMock(side_effect=_resolve),
        )
        assert [e.as_dict() for e in found] == [
            {"slot": "llm", "model": "MiniMax-M3", "reason": "disabled"}
        ]


# ---------------------------------------------------------------------------
# Credential injection: no guessing, one refusal
# ---------------------------------------------------------------------------


class TestInjection:
    @pytest.mark.asyncio
    async def test_a_disabled_gpt_model_is_not_sent_to_openai(self):
        """The prefix map used to route any ``gpt-*`` miss to OpenAI with the
        stored OpenAI key."""
        _CATALOG["gpt-retired"] = [{"enabled": False, "capabilities": ["chat"]}]
        try:
            section: dict = {}
            reason = await dc.inject_model_credentials(
                section=section,
                model_id="gpt-retired",
                user_id="u",
                resolved_keys={"openai": "sk-real-openai"},
                dependencies=_deps(),
            )
        finally:
            del _CATALOG["gpt-retired"]
        assert reason == "disabled"
        assert section == {}

    @pytest.mark.asyncio
    async def test_a_disabled_model_is_refused_even_with_a_caller_route(self):
        section = {"base_url": "http://caller/v1", "api_key": "k"}
        reason = await dc.inject_model_credentials(
            section=section,
            model_id="MiniMax-M3",
            user_id="u",
            resolved_keys={},
            dependencies=_deps(),
        )
        assert reason == "disabled"

    @pytest.mark.asyncio
    async def test_session_injector_reports_every_slot_at_once(self):
        override = {
            "llm": {"model": "MiniMax-M3"},
            "auxiliary": {"model": "chat-only"},
            "subagents": {
                "roster": {
                    "reader": {"llm": {"model": "MiniMax-M3", "_inherit_llm": True}},
                    "critic": {"llm": {"model": "nope"}},
                }
            },
        }
        with patch(
            "orchestrator.services.capability_credentials.resolve_capability_credentials",
            AsyncMock(return_value=None),
        ):
            with pytest.raises(ma.ModelUnavailable) as raised:
                await dc.inject_thread_dispatch_credentials(
                    override, user_id="u", dependencies=_deps()
                )
        assert [e.as_dict() for e in raised.value.entries] == [
            {"slot": "llm", "model": "MiniMax-M3", "reason": "disabled"},
            {"slot": "auxiliary", "model": "chat-only", "reason": "capability"},
            {
                "slot": "subagents.roster.critic.llm",
                "model": "nope",
                "reason": "unknown",
            },
        ]
        # Nothing was routed by a guess.
        assert "provider" not in override["llm"]
        assert "api_key" not in override["llm"]

    @pytest.mark.asyncio
    async def test_a_fallback_copy_only_logs(self, caplog):
        override = {"llm": {"model": "MiniMax-M3"}}
        with patch(
            "orchestrator.services.capability_credentials.resolve_capability_credentials",
            AsyncMock(return_value=None),
        ):
            with caplog.at_level(logging.WARNING, logger="test.unavailable_model"):
                out = await dc.inject_thread_dispatch_credentials(
                    override, user_id="u", strict=False, dependencies=_deps()
                )
        assert out["llm"] == {"model": "MiniMax-M3"}
        assert any("fallback copy" in r.getMessage() for r in caplog.records)

    @pytest.mark.asyncio
    async def test_the_delivery_seam_refuses(self):
        """``inject_blob_credentials`` is what every session and job delivery
        goes through; the refusal leaves it unchanged."""
        blob = {"agent": {"llm": {"model": "MiniMax-M3", "base_url": None}}}
        deps = _deps()
        with patch(
            "orchestrator.services.capability_credentials.resolve_capability_credentials",
            AsyncMock(return_value=None),
        ):
            with pytest.raises(ma.ModelUnavailable):
                await inject_blob_credentials(
                    blob,
                    lambda co: dc.inject_thread_dispatch_credentials(
                        co, user_id="u", dependencies=deps
                    ),
                )
            delivered = await inject_blob_credentials(
                {"agent": {"llm": {"model": "gpt-6-astra"}}},
                lambda co: dc.inject_thread_dispatch_credentials(
                    co, user_id="u", dependencies=deps
                ),
            )
        assert delivered["agent"]["llm"]["base_url"] == PROXY_URL


# ---------------------------------------------------------------------------
# Stateless input admission
# ---------------------------------------------------------------------------


def _execution(model: str) -> dict:
    return {"harness_adapter": "srw", "resolved": {"agent": {"llm": {"model": model}}}}


class TestStatelessAdmission:
    @pytest.fixture
    def snapshot(self, monkeypatch):
        from orchestrator.services import manifest_execution_snapshot as mes

        holder: dict = {}

        async def read_execution(store, kind, work_id):
            assert kind == "Session"
            return holder.get("execution")

        monkeypatch.setattr(mes, "read_execution", read_execution)
        monkeypatch.setattr(
            mes,
            "srw_snapshot_config",
            lambda execution: (execution["resolved"], {}),
        )
        return holder

    def _deps(self, store=None):
        return sia.StatelessInputDependencies(
            store=store or _store(),
            schedule_stateless_workspace_ensure=lambda thread_id: None,
            resolve_model=AsyncMock(side_effect=_resolve),
        )

    @pytest.mark.asyncio
    async def test_refuses_before_storing_with_the_model_named(self, snapshot):
        snapshot["execution"] = _execution("MiniMax-M3")
        with pytest.raises(HTTPException) as raised:
            await sia.refuse_unavailable_session_models(
                {"id": "t", "user_id": "u"}, dependencies=self._deps()
            )
        assert raised.value.status_code == 409
        detail = raised.value.detail
        assert detail["code"] == "model.unavailable"
        assert "`MiniMax-M3` (main model) is no longer available" in detail["message"]
        assert "this session's settings" in detail["message"]

    @pytest.mark.asyncio
    async def test_admins_are_told_they_can_re_enable_it(self, snapshot):
        snapshot["execution"] = _execution("MiniMax-M3")
        store = _store(get_user=AsyncMock(return_value={"is_admin": True}))
        with pytest.raises(HTTPException) as raised:
            await sia.refuse_unavailable_session_models(
                {"id": "t", "user_id": "u"}, dependencies=self._deps(store)
            )
        assert "Admin → Models" in raised.value.detail["message"]

    @pytest.mark.asyncio
    async def test_a_runnable_model_and_a_legacy_row_pass(self, snapshot):
        snapshot["execution"] = _execution("gpt-6-astra")
        await sia.refuse_unavailable_session_models(
            {"id": "t", "user_id": "u"}, dependencies=self._deps()
        )
        snapshot["execution"] = None
        await sia.refuse_unavailable_session_models(
            {"id": "t", "user_id": "u"}, dependencies=self._deps()
        )

    @pytest.mark.asyncio
    async def test_a_failing_check_admits(self, snapshot, monkeypatch):
        from orchestrator.services import manifest_execution_snapshot as mes

        async def broken(*_a, **_k):
            raise RuntimeError("db down")

        monkeypatch.setattr(mes, "read_execution", broken)
        await sia.refuse_unavailable_session_models(
            {"id": "t", "user_id": "u"}, dependencies=self._deps()
        )


# ---------------------------------------------------------------------------
# Jobs: the dispatcher asks before its claim (S4)
# ---------------------------------------------------------------------------


class TestJobCheckBeforeClaim:
    """What the start bundle would refuse, asked before the dispatcher's claim
    (k3d gate G6, 2026-10-09: with completion commands on, the post-claim
    refusal only logged and the job was claimed again every lease expiry)."""

    @pytest.fixture
    def snapshot(self, monkeypatch):
        from orchestrator.services import job_start_bundle as jsb
        from shared.runtime.core import model_registry

        monkeypatch.setattr(model_registry, "lookups_registered", lambda: True)
        monkeypatch.setattr(
            model_registry, "resolve_model", AsyncMock(side_effect=_resolve)
        )
        holder: dict = {"execution": None}

        async def read_execution(store, kind, work_id):
            assert (kind, work_id) == ("Job", "j")
            return holder["execution"]

        monkeypatch.setattr(jsb, "read_execution", read_execution)
        monkeypatch.setattr(
            jsb, "srw_snapshot_config", lambda execution: (execution["resolved"], {})
        )
        return holder

    @staticmethod
    def _deps(store=None, *, experts_db=True):
        return SimpleNamespace(
            store=store or _store(),
            logger=logging.getLogger("test.unavailable_model"),
            is_experts_db_enabled=lambda: experts_db,
        )

    @staticmethod
    async def _check(job, deps):
        from orchestrator.services import job_start_bundle as jsb

        return await jsb.unavailable_models_before_claim(
            {"id": "j", "user_id": "u", **job}, dependencies=deps
        )

    @pytest.mark.asyncio
    async def test_a_frozen_disabled_model_is_refused_with_the_job_message(
        self, snapshot
    ):
        snapshot["execution"] = _execution("MiniMax-M3")
        refusal = await self._check({}, self._deps())
        assert [entry.as_dict() for entry in refusal.entries] == [
            {"slot": "llm", "model": "MiniMax-M3", "reason": "disabled"}
        ]
        assert refusal.message(where=ma.WHERE_JOB) == (
            "The model `MiniMax-M3` (main model) is no longer available. Choose "
            "another model in the job's configuration, or ask your administrator."
        )

    @pytest.mark.asyncio
    async def test_a_frozen_unknown_model_is_refused_the_same_way(self, snapshot):
        snapshot["execution"] = _execution("gpt-9-preview")
        refusal = await self._check({}, self._deps())
        assert [entry.as_dict() for entry in refusal.entries] == [
            {"slot": "llm", "model": "gpt-9-preview", "reason": "unknown"}
        ]

    @pytest.mark.asyncio
    async def test_frozen_models_that_can_run_pass(self, snapshot):
        snapshot["execution"] = _execution("gpt-6-astra")
        assert await self._check({}, self._deps()) is None

    @pytest.mark.asyncio
    async def test_every_frozen_slot_is_checked_but_an_inherited_label(self, snapshot):
        """The start bundle checks the auxiliary and roster slots too; a roster
        entry that inherits its parent's model is not checked by name (D5)."""
        snapshot["execution"] = {
            "resolved": {
                "agent": {
                    "llm": {"model": "gpt-6-astra"},
                    "auxiliary": {"model": "MiniMax-M3"},
                    "subagents": {
                        "roster": {
                            "critic": {
                                "llm": {"model": "MiniMax-M3", "_inherit_llm": True}
                            }
                        }
                    },
                }
            }
        }
        refusal = await self._check({}, self._deps())
        assert [entry.as_dict() for entry in refusal.entries] == [
            {"slot": "auxiliary", "model": "MiniMax-M3", "reason": "disabled"}
        ]

    @pytest.mark.asyncio
    async def test_a_historical_job_pin_is_refused(self, snapshot):
        refusal = await self._check(
            {"config_override": '{"llm": {"model": "MiniMax-M3"}}'}, self._deps()
        )
        assert [entry.model for entry in refusal.entries] == ["MiniMax-M3"]

    @pytest.mark.asyncio
    async def test_a_historical_expert_pin_is_refused_unless_the_job_overrides_it(
        self, snapshot
    ):
        store = _store(
            get_expert_by_id=AsyncMock(
                return_value={"config": {"llm": {"model": "MiniMax-M3"}}}
            )
        )
        refusal = await self._check({"expert_id": "e"}, self._deps(store))
        assert [entry.model for entry in refusal.entries] == ["MiniMax-M3"]
        store.get_expert_by_id.assert_awaited_once_with("e")

        overridden = {
            "expert_id": "e",
            "config_override": {"llm": {"model": "gpt-6-astra"}},
        }
        assert await self._check(overridden, self._deps(store)) is None
        # Without the experts DB the start bundle never reads the expert.
        assert (
            await self._check({"expert_id": "e"}, self._deps(store, experts_db=False))
            is None
        )

    @pytest.mark.asyncio
    async def test_a_historical_phase_tier_is_read_as_the_start_bundle_reads_it(
        self, snapshot
    ):
        beside = {"llm": {"model": "gpt-6-astra", "tactical": {"model": "MiniMax-M3"}}}
        # Experts DB on: resolve_config drops a tier beside an explicit model...
        assert await self._check({"config_override": beside}, self._deps()) is None
        # ...and lifts a lone one into llm.model.
        lone = {"llm": {"strategic": {"model": "MiniMax-M3"}}}
        refusal = await self._check({"config_override": lone}, self._deps())
        assert [entry.as_dict() for entry in refusal.entries] == [
            {"slot": "llm", "model": "MiniMax-M3", "reason": "disabled"}
        ]
        # Experts DB off: the flat override is delivered, and refused, as it is.
        refusal = await self._check(
            {"config_override": beside}, self._deps(experts_db=False)
        )
        assert [entry.as_dict() for entry in refusal.entries] == [
            {"slot": "llm.tactical", "model": "MiniMax-M3", "reason": "disabled"}
        ]

    @pytest.mark.asyncio
    async def test_an_account_default_is_never_refused_here(self, snapshot):
        """It falls back to the system default when the job is resolved (S2)."""
        store = _store(
            get_user_settings=AsyncMock(return_value={"default_model": "MiniMax-M3"})
        )
        assert await self._check({"config_override": {}}, self._deps(store)) is None
        store.get_user_settings.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_failing_check_lets_the_start_bundle_decide(
        self, snapshot, monkeypatch, caplog
    ):
        from orchestrator.services import job_start_bundle as jsb

        async def broken(*_a, **_k):
            raise RuntimeError("db down")

        monkeypatch.setattr(jsb, "read_execution", broken)
        with caplog.at_level(logging.WARNING, logger="test.unavailable_model"):
            assert await self._check({}, self._deps()) is None
        assert "model check before the claim failed for job j" in caplog.text

    @pytest.mark.asyncio
    async def test_an_unconfigured_registry_judges_nothing(self, snapshot, monkeypatch):
        from shared.runtime.core import model_registry

        snapshot["execution"] = _execution("MiniMax-M3")
        monkeypatch.setattr(model_registry, "lookups_registered", lambda: False)
        assert await self._check({}, self._deps()) is None


# ---------------------------------------------------------------------------
# Agent: the claim refusal is parsed; the OpenAI client refuses a missing route
# ---------------------------------------------------------------------------


class TestAgentSide:
    def test_claim_bundle_409_carries_the_refusal_message(self):
        from agent.api.orchestrator_client import _model_unavailable_message

        request = httpx.Request("GET", "http://o/internal/units/u/claim-bundle")
        refused = httpx.Response(
            409,
            json={"detail": {"code": "model.unavailable", "message": "The model `X`…"}},
            request=request,
        )
        generic = httpx.Response(
            409, json={"detail": "Attach assembly refused"}, request=request
        )
        assert _model_unavailable_message(refused) == "The model `X`…"
        assert _model_unavailable_message(generic) is None

    @pytest.mark.asyncio
    async def test_routeless_openai_client_refuses_without_a_request(self, monkeypatch):
        from shared.runtime.core.llm_retry import _classify_llm_error
        from shared.runtime.core.loader import LLMConfig, create_llm
        from shared.runtime.core.transport_resolution import ModelRouteMissing

        monkeypatch.delenv("OPENAI_API_KEY", raising=False)

        async def tripwire(*_a, **_k):  # pragma: no cover - must never run
            raise AssertionError("a request was sent")

        monkeypatch.setattr(httpx.AsyncClient, "send", tripwire)
        llm = create_llm(LLMConfig(model="MiniMax-M3", temperature=0.0))
        with pytest.raises(ModelRouteMissing) as raised:
            await llm.ainvoke("hi")
        assert str(raised.value).startswith("Model `MiniMax-M3` has no configured")
        assert _classify_llm_error(raised.value) == "permanent"

    def test_a_routed_or_keyed_client_is_untouched(self, monkeypatch):
        from shared.runtime.core.loader import LLMConfig, create_llm

        monkeypatch.delenv("OPENAI_API_KEY", raising=False)
        routed = create_llm(
            LLMConfig(model="m", temperature=0.0, base_url=PROXY_URL, provider="openai")
        )
        keyed = create_llm(LLMConfig(model="m", temperature=0.0, api_key="sk-x"))
        assert routed.route_missing_model is None
        assert keyed.route_missing_model is None


# ---------------------------------------------------------------------------
# S2: a stale account default falls back to the system default, with a notice
# ---------------------------------------------------------------------------


class TestAccountDefaultFallback:
    @pytest.fixture
    def registry(self, monkeypatch):
        """Install the fake registry as the live module resolver."""
        from shared.runtime.core import model_registry

        monkeypatch.setattr(model_registry, "lookups_registered", lambda: True)
        monkeypatch.setattr(
            model_registry, "resolve_model", AsyncMock(side_effect=_resolve)
        )

    def _store(self, settings, *, system=("gpt-6-astra", "gpt-6-astra")):
        chat, aux = system

        async def default_for(capability):
            return {"chat": chat, "auxiliary": aux}.get(capability)

        return _store(
            get_user_settings=AsyncMock(return_value=settings),
            resolve_default_for_capability=AsyncMock(side_effect=default_for),
        )

    @pytest.mark.asyncio
    async def test_the_10_08_account_falls_back_and_says_so(self, registry):
        from orchestrator.services import session_config_resolution as scr

        notices: list = []
        layer = await scr.resolve_default_models(
            "u",
            dependencies=SimpleNamespace(
                store=self._store({"default_model": "MiniMax-M3"})
            ),
            notices=notices,
        )
        assert layer["llm"]["model"] == "gpt-6-astra"
        assert notices == [
            {
                "slot": "llm",
                "skipped": "MiniMax-M3",
                "reason": "disabled",
                "used": "gpt-6-astra",
                "source": "account.default_model",
            }
        ]
        assert ma.render_fallback_notice(notices[0]) == (
            "Your default model `MiniMax-M3` is no longer available, so this "
            "session uses `gpt-6-astra`. Choose a new default in Settings → "
            "Preferences."
        )

    @pytest.mark.asyncio
    async def test_a_valid_preference_still_wins(self, registry):
        from orchestrator.services import session_config_resolution as scr

        _METAS["muse"] = ModelMeta(
            model_id="muse", provider="openai", family="x", display_name="muse"
        )
        try:
            notices: list = []
            layer = await scr.resolve_default_models(
                "u",
                dependencies=SimpleNamespace(
                    store=self._store({"default_model": "muse"})
                ),
                notices=notices,
            )
        finally:
            del _METAS["muse"]
        assert layer["llm"]["model"] == "muse"
        assert notices == []

    @pytest.mark.asyncio
    async def test_without_a_system_default_the_choice_is_kept_to_be_refused(
        self, registry
    ):
        """Falling back to nothing would leave the base YAML placeholder, and
        the refusal would name a model the user never chose."""
        from orchestrator.services import session_config_resolution as scr

        layer = await scr.resolve_default_models(
            "u",
            dependencies=SimpleNamespace(
                store=self._store({"default_model": "MiniMax-M3"}, system=(None, None))
            ),
        )
        assert layer["llm"]["model"] == "MiniMax-M3"

    @pytest.mark.asyncio
    async def test_an_unconfigured_registry_judges_nothing(self, monkeypatch):
        from orchestrator.services import session_config_resolution as scr
        from shared.runtime.core import model_registry

        monkeypatch.setattr(model_registry, "lookups_registered", lambda: False)
        layer = await scr.resolve_default_models(
            "u",
            dependencies=SimpleNamespace(
                store=self._store({"default_model": "MiniMax-M3"})
            ),
        )
        assert layer["llm"]["model"] == "MiniMax-M3"

    @pytest.mark.asyncio
    async def test_session_model_preference_falls_back_too(self, registry):
        from orchestrator.services import session_config_resolution as scr

        settings = {"persistent_agent": {"model": "MiniMax-M3"}}
        notices: list = []
        layer = await scr.resolve_session_account_defaults(
            "u",
            settings,
            dependencies=SimpleNamespace(store=self._store(settings)),
            notices=notices,
        )
        assert layer["llm"]["model"] == "gpt-6-astra"
        assert [n["source"] for n in notices] == ["account.persistent_agent.model"]

    @pytest.mark.asyncio
    async def test_the_create_form_shows_the_skipped_preference(self, registry):
        from orchestrator.services.expert_catalog import ExpertCatalogService

        service = ExpertCatalogService.__new__(ExpertCatalogService)
        service.store = self._store({"default_model": "MiniMax-M3"})
        effective = await service.compute_expert_effective_models({}, "u")
        assert effective["model"]["model"] == "gpt-6-astra"
        assert effective["model"]["source"] == "system_default"
        assert effective["model"]["skipped_account_default"] == {
            "model": "MiniMax-M3",
            "reason": "disabled",
        }

    @pytest.mark.asyncio
    async def test_the_legacy_job_path_skips_it(self, registry):
        from orchestrator.services import job_dispatch_credentials as jdc

        store = self._store({})
        kept = await jdc._usable_account_default(
            "MiniMax-M3",
            "chat",
            job_id="j",
            user_id="u",
            store=store,
            logger=logging.getLogger("test"),
        )
        assert kept is None
