"""Pins for the session config_name plumbing + detach-then-delete fixes.

Covers the three holes found during the memory-overhaul Phase-1 closure
step 1 (2026-06-11):

- Hole A: a bare ``POST /api/persistent/threads`` must land on the
  persistent base config, not the worker one
  (knowledge-base/knowledge/issues/session_config_name_plumbing.md).
- Hole B: the idle-pool ``/session/attach`` path must carry the thread's
  config_name (orchestrator side) and resolve it as the session base
  (agent side) — otherwise pool-attached sessions bind the worker memory
  pipeline and silently lose ``teardown_extractor``.
- B11 k8s route: the user-facing thread DELETE must give a live session
  agent the chance to terminate (final memory capture + git push) before
  the workspace and pod are torn down (memory_bugs.md B11 addendum).
"""

from tests import _b09_control_seams as control_seams

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.services import (  # noqa: E402
    thread_datasource_authorization,
)
from fastapi import HTTPException

import orchestrator.main as orch_main

# R1.B05 lane P: the tier constants keep their owner; main no longer
# re-exports them.
from orchestrator.services import session_workspace_policy
from orchestrator.services import session_tool_policy
from orchestrator.services import thread_workspace_delivery
from orchestrator.routers import (
    agent_thread_workspace as agent_thread_workspace_routes,
)
from orchestrator.services.cloud.protected_reader_authority import (
    ProtectedNextcloudReaderGrantPlan,
)
from orchestrator.services.cloud_staging.source_identity import (
    ProtectedMountSourceIdentity,
)
from orchestrator.services.pinned_retirement import PinnedRetirementOperations
from agent.api.persistent_app import _load_expert_config
from shared.runtime.core.tool_policy import (
    ToolPolicyError,
    validate_tool_override_fragment,
)
from shared.runtime_actor import RuntimeActorContext
from orchestrator.services import session_attach_payload


_ATTACH_THREAD_ID = "10000000-0000-4000-8000-000000000001"
_ATTACH_AGENT_ID = "20000000-0000-4000-8000-000000000002"
_ATTACH_RUNTIME_GENERATION = "30000000-0000-4000-8000-000000000003"
_ATTACH_TOKEN = "40000000-0000-4000-8000-000000000004"


class _FakeResponse:
    def __init__(self, status_code: int = 200):
        self.status_code = status_code
        self.text = ""


class _FakeAsyncClient:
    """httpx.AsyncClient stand-in recording the last POST."""

    calls: list = []
    response_status: int = 200
    response_text: str = ""
    raise_on_post: Exception | None = None

    def __init__(self, **kwargs):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def post(self, url, json=None):
        if _FakeAsyncClient.raise_on_post is not None:
            raise _FakeAsyncClient.raise_on_post
        _FakeAsyncClient.calls.append({"url": url, "json": json})
        response = _FakeResponse(_FakeAsyncClient.response_status)
        response.text = _FakeAsyncClient.response_text
        return response


@pytest.fixture(autouse=True)
def _reset_fake_client():
    _FakeAsyncClient.calls = []
    _FakeAsyncClient.response_status = 200
    _FakeAsyncClient.response_text = ""
    _FakeAsyncClient.raise_on_post = None
    yield


@asynccontextmanager
async def _noop_thread_datasource_lock():
    yield


@asynccontextmanager
async def _owned_workspace_lifecycle_lock(*_args, **_kwargs):
    yield True


@pytest.fixture(autouse=True)
def _patch_thread_datasource_delivery_lock():
    with patch.object(
        orch_main.postgres_db,
        "thread_datasource_lock",
        side_effect=lambda _thread_id: _noop_thread_datasource_lock(),
    ):
        yield


_REAL_RESERVE_SESSION_ATTACH_BINDING = orch_main._reserve_session_attach_binding


@pytest.fixture(autouse=True)
def _patch_session_attach_reservation():
    """Payload-focused tests do not need a live DB reservation."""
    target = orch_main._PinnedSessionMutationTarget(
        agent={
            "id": _ATTACH_AGENT_ID,
            "pod_ip": "10.0.0.1",
            "pod_port": 8001,
        },
        binding=None,
        recipient={
            "expected_thread_id": _ATTACH_THREAD_ID,
            "expected_agent_id": _ATTACH_AGENT_ID,
            "expected_pod_uid": None,
            "expected_process_generation": "test-process-generation",
        },
        process_generation="test-process-generation",
        runtime_generation=_ATTACH_RUNTIME_GENERATION,
        attach_token=_ATTACH_TOKEN,
    )
    with (
        patch.object(
            orch_main,
            "_reserve_session_attach_binding",
            AsyncMock(return_value=_ATTACH_TOKEN),
        ),
        patch.object(
            orch_main,
            "_release_session_attach_binding",
            AsyncMock(return_value="released"),
        ),
        patch.object(
            orch_main,
            "_prepare_pinned_session_mutation_target",
            AsyncMock(return_value=target),
        ),
        patch.object(
            orch_main,
            "_pinned_session_mutation_target_is_current",
            AsyncMock(return_value=True),
        ),
    ):
        yield


@pytest.fixture(autouse=True)
def _patch_session_runtime_actor_mint():
    """Config-plumbing tests stop at the server-derived actor delivery seam."""

    actor = RuntimeActorContext(
        caller_kind="human",
        thread_id=_ATTACH_THREAD_ID,
        access_credential="sra_" + ("A" * 43),
        refresh_credential="srr_" + ("B" * 43),
    )
    with patch.object(
        orch_main,
        "mint_thread_runtime_actor",
        AsyncMock(return_value=actor),
    ):
        yield


class TestThreadCreateDefault:
    """Hole A: the request-model default."""

    def test_bare_thread_create_defaults_to_persistent_config(self):
        assert orch_main.ThreadCreateRequest().config_name == "session_base"

    def test_explicit_datasource_default_request_is_distinct_from_omission(self):
        request = orch_main.ThreadCreateRequest(use_datasource_defaults=True)

        assert request.use_datasource_defaults is True
        assert "datasource_ids" not in request.model_fields_set

    def test_datasource_defaults_and_explicit_selection_are_mutually_exclusive(self):
        with pytest.raises(ValueError, match="mutually exclusive"):
            orch_main.ThreadCreateRequest(
                use_datasource_defaults=True,
                datasource_ids=[],
            )


class TestJobCreateDatasourceDefaults:
    def test_explicit_datasource_default_request_is_distinct_from_omission(self):
        request = orch_main.JobCreate(
            description="defaulted job",
            use_datasource_defaults=True,
        )

        assert request.use_datasource_defaults is True
        assert "datasource_ids" not in request.model_fields_set

    def test_datasource_defaults_and_explicit_selection_are_mutually_exclusive(self):
        with pytest.raises(ValueError, match="mutually exclusive"):
            orch_main.JobCreate(
                description="ambiguous job",
                use_datasource_defaults=True,
                datasource_ids=[],
            )


class TestSessionWorkspaceBackendOverride:
    """The New Session 'Backend' selector must reach create_thread.

    Regression pin for the dropped-backend bug (found 2026-06-20 live-testing
    the workspace-tier-upgrade feature): ThreadCreateRequest declared no
    config_override field, so the cockpit's
    ``{"config_override": {"workspace": {"backend": "virtual"}}}`` was silently
    discarded by Pydantic and every session booted the default (sandbox) —
    making lite/VM sessions uncreatable from the UI.
    """

    def test_request_model_accepts_config_override(self):
        req = orch_main.ThreadCreateRequest(
            config_override={"workspace": {"backend": "virtual"}}
        )
        assert req.config_override == {"workspace": {"backend": "virtual"}}

    def test_bare_request_has_no_config_override(self):
        assert orch_main.ThreadCreateRequest().config_override is None

    @pytest.mark.parametrize("backend", ["sandbox", "virtual", "none"])
    def test_creatable_backends_pass_through(self, backend):
        ws = orch_main._validated_session_workspace_override(
            {"workspace": {"backend": backend, "max_read_words": 5}}
        )
        assert ws == {"backend": backend, "max_read_words": 5}

    def test_vm_backend_accepted_at_create(self):
        # vm is now selectable at creation (operator-gated + provisioned in
        # create_thread). Validation lets it through with its sizing sub-dict;
        # the permission/provisioner gate lives downstream, not here.
        # knowledge-base/knowledge/features/session_create_on_vm.md
        ws = orch_main._validated_session_workspace_override(
            {"workspace": {"backend": "vm", "vm": {"cpu_cores": 4, "memory": "8Gi"}}}
        )
        assert ws == {"backend": "vm", "vm": {"cpu_cores": 4, "memory": "8Gi"}}

    def test_vm_not_in_default_chain_set(self):
        # vm must remain a per-session opt-in, never an implicit/saved default:
        # it is excluded from SESSION_WORKSPACE_BACKENDS (the default-chain +
        # settings-PATCH set) but present in the create-time allowlist.
        assert "vm" not in session_workspace_policy.SESSION_WORKSPACE_BACKENDS
        assert "vm" in session_workspace_policy.SESSION_CREATE_WORKSPACE_BACKENDS

    def test_unknown_backend_rejected(self):
        with pytest.raises(orch_main.HTTPException) as exc:
            orch_main._validated_session_workspace_override(
                {"workspace": {"backend": "bogus"}}
            )
        assert exc.value.status_code == 400

    def test_absent_fragment_returns_none(self):
        assert orch_main._validated_session_workspace_override(None) is None
        assert orch_main._validated_session_workspace_override({}) is None
        assert (
            orch_main._validated_session_workspace_override({"llm": {"model": "m"}})
            is None
        )

    def test_workspace_without_backend_passes_through(self):
        # Word-limit-only tweaks (no tier change) are honored, not rejected.
        ws = orch_main._validated_session_workspace_override(
            {"workspace": {"max_read_words": 10}}
        )
        assert ws == {"max_read_words": 10}


class TestSessionReadyTimeout:
    """VM-aware readiness budget for the session-start paths
    (knowledge-base/knowledge/features/session_create_on_vm.md)."""

    def test_non_vm_uses_fast_default(self, monkeypatch):
        monkeypatch.delenv("WS_READY_TIMEOUT_S", raising=False)
        assert session_workspace_policy.session_ready_timeout_s("sandbox") == 180
        assert session_workspace_policy.session_ready_timeout_s("virtual") == 180
        assert session_workspace_policy.session_ready_timeout_s(None) == 180

    def test_vm_uses_extended_budget(self, monkeypatch):
        monkeypatch.delenv("VM_WS_READY_TIMEOUT_S", raising=False)
        assert session_workspace_policy.session_ready_timeout_s("vm") == 960

    def test_budgets_are_env_tunable(self, monkeypatch):
        monkeypatch.setenv("WS_READY_TIMEOUT_S", "200")
        monkeypatch.setenv("VM_WS_READY_TIMEOUT_S", "1200")
        assert session_workspace_policy.session_ready_timeout_s("sandbox") == 200
        assert session_workspace_policy.session_ready_timeout_s("vm") == 1200

    def test_vm_budget_exceeds_non_vm(self):
        # Nested-budget invariant: the server ready wait must outlast a cold VM
        # boot, so vm must be strictly larger than the sandbox default.
        assert session_workspace_policy.session_ready_timeout_s(
            "vm"
        ) > session_workspace_policy.session_ready_timeout_s("sandbox")


class TestThreadWorkspaceBackend:
    """_thread_workspace_backend extracts a thread's stored backend so the
    resume path (_do_prepare) can size the VM budget without the config_override."""

    def test_dict_metadata(self):
        thread = {"metadata": {"config_override": {"workspace": {"backend": "vm"}}}}
        assert orch_main._thread_workspace_backend(thread) == "vm"

    def test_json_string_metadata(self):
        import json

        thread = {
            "metadata": json.dumps(
                {"config_override": {"workspace": {"backend": "sandbox"}}}
            )
        }
        assert orch_main._thread_workspace_backend(thread) == "sandbox"

    def test_missing_or_malformed_returns_none(self):
        assert orch_main._thread_workspace_backend({}) is None
        assert orch_main._thread_workspace_backend({"metadata": "not-json"}) is None
        assert orch_main._thread_workspace_backend(None) is None
        assert (
            orch_main._thread_workspace_backend({"metadata": {"config_override": {}}})
            is None
        )


class TestSessionWorkspaceBackendDefaultChain:
    """Instant-landing defaults chain (knowledge-base/knowledge/features/instant_landing_session.md):
    explicit request > owner's saved ``persistent_agent.workspace_backend`` >
    platform default (virtual). Sessions are never implicitly sandbox."""

    def test_platform_default_is_virtual(self):
        assert session_workspace_policy.SESSION_DEFAULT_WORKSPACE_BACKEND == "virtual"
        assert (
            session_workspace_policy.SESSION_DEFAULT_WORKSPACE_BACKEND
            in session_workspace_policy.SESSION_WORKSPACE_BACKENDS
        )

    def test_no_settings_falls_back_to_platform_default(self):
        assert orch_main._default_session_workspace_backend({}) == "virtual"
        assert orch_main._default_session_workspace_backend(None) == "virtual"

    @pytest.mark.parametrize("backend", ["sandbox", "virtual", "none"])
    def test_saved_user_default_wins_over_platform_default(self, backend):
        assert (
            orch_main._default_session_workspace_backend({"workspace_backend": backend})
            == backend
        )

    @pytest.mark.parametrize("junk", ["vm", "bogus", "", 3, {"backend": "vm"}])
    def test_junk_saved_value_falls_back_to_platform_default(self, junk):
        # Legacy/hand-edited settings rows must not brick session creation.
        assert (
            orch_main._default_session_workspace_backend({"workspace_backend": junk})
            == "virtual"
        )

    def test_settings_patch_accepts_valid_workspace_backend(self):
        upd = orch_main.UserSettingsUpdate(
            persistent_agent={"workspace_backend": "sandbox", "model": "m"}
        )
        assert upd.persistent_agent == {"workspace_backend": "sandbox", "model": "m"}

    @pytest.mark.parametrize("bad", ["vm", "bogus", ""])
    def test_settings_patch_rejects_invalid_workspace_backend(self, bad):
        with pytest.raises(ValueError):
            orch_main.UserSettingsUpdate(persistent_agent={"workspace_backend": bad})

    def test_settings_patch_leaves_other_keys_free_form(self):
        # persistent_agent stays a free dict for other keys. `greeting` is the
        # deliberate example: it is a legacy key from a removed control, and a
        # stored blob that still carries one must round-trip rather than 422 —
        # nothing reads it any more.
        upd = orch_main.UserSettingsUpdate(
            persistent_agent={"headless_mode": "eager", "greeting": "hi"}
        )
        assert upd.persistent_agent == {"headless_mode": "eager", "greeting": "hi"}

    def test_settings_patch_round_trips_communication(self):
        # Regression: the cockpit's Communication card PATCHes this whole
        # sub-object. Until 2026-08-23 the model did not declare the field, so
        # Pydantic dropped it and the endpoint 400'd with "No settings
        # provided" — every channel toggle, quiet-hours window and reply-
        # delivery choice was silently discarded for every user.
        payload = {
            "delivery": {"async_reply": "llm_triage", "urgent_override": True},
            "channels": {
                "email": False,
                "cockpit": True,
                "ntfy": False,
                "slack_webhook": False,
                "discord_webhook": False,
            },
            "quiet_hours": {
                "enabled": True,
                "start": "22:00",
                "end": "08:00",
                "timezone": "Europe/Berlin",
            },
        }
        upd = orch_main.UserSettingsUpdate(communication=payload)
        assert upd.communication == payload
        assert "communication" in upd.model_fields_set

    def test_settings_patch_communication_survives_the_endpoint_filter(self):
        # The endpoint drops keys that are None and unset; a communication-only
        # PATCH must survive it, or update_user_preferences raises 400.
        upd = orch_main.UserSettingsUpdate(communication={"channels": {"email": False}})
        settings = {
            k: v
            for k, v in upd.model_dump().items()
            if v is not None or k in upd.model_fields_set
        }
        assert settings == {"communication": {"channels": {"email": False}}}

    @pytest.mark.parametrize("bad", ["off", 0, 1, None, "true"])
    def test_settings_patch_rejects_non_boolean_channel(self, bad):
        # Readers gate on channels.get(name, True); a truthy non-bool would
        # leave the channel on after the user switched it off.
        with pytest.raises(ValueError):
            orch_main.UserSettingsUpdate(communication={"channels": {"email": bad}})

    @pytest.mark.parametrize("key", ["delivery", "channels", "quiet_hours"])
    def test_settings_patch_rejects_non_object_communication_subkey(self, key):
        with pytest.raises(ValueError):
            orch_main.UserSettingsUpdate(communication={key: "nope"})

    def test_settings_patch_leaves_unknown_communication_keys_free_form(self):
        upd = orch_main.UserSettingsUpdate(
            communication={"channels": {"email": True}, "future_knob": 7}
        )
        assert upd.communication["future_knob"] == 7

    def test_settings_patch_round_trips_the_preference_matrix(self):
        # D9: categories[category][channel] overrides the channel default.
        payload = {
            "channels": {"email": True},
            "categories": {"review_queue": {"email": False, "ntfy": True}},
            "escalation_minutes": 10,
        }
        upd = orch_main.UserSettingsUpdate(communication=payload)
        assert upd.communication == payload

    @pytest.mark.parametrize("bad", ["off", 0, 1, None, "true"])
    def test_settings_patch_rejects_non_boolean_matrix_cell(self, bad):
        with pytest.raises(ValueError):
            orch_main.UserSettingsUpdate(
                communication={"categories": {"review_queue": {"email": bad}}}
            )

    @pytest.mark.parametrize("bad", ["nope", ["email"], 3])
    def test_settings_patch_rejects_non_object_matrix(self, bad):
        with pytest.raises(ValueError):
            orch_main.UserSettingsUpdate(communication={"categories": bad})
        with pytest.raises(ValueError):
            orch_main.UserSettingsUpdate(communication={"categories": {"x": bad}})

    @pytest.mark.parametrize("bad", [0, -5, 1441, "5", True, 2.5])
    def test_settings_patch_bounds_escalation_minutes(self, bad):
        with pytest.raises(ValueError):
            orch_main.UserSettingsUpdate(communication={"escalation_minutes": bad})
        assert (
            orch_main.UserSettingsUpdate(
                communication={"escalation_minutes": 1}
            ).communication["escalation_minutes"]
            == 1
        )

    @pytest.mark.parametrize("lang", ["en", "de-DE"])
    def test_settings_patch_round_trips_language(self, lang):
        # Same regression as communication: I18nService.setLanguage PATCHes
        # {language} on its own, so an undeclared field meant a 400 and the
        # locale choice never survived a reload.
        upd = orch_main.UserSettingsUpdate(language=lang)
        settings = {
            k: v
            for k, v in upd.model_dump().items()
            if v is not None or k in upd.model_fields_set
        }
        assert settings == {"language": lang}

    @pytest.mark.parametrize("bad", ["", "a", "de_DE", "not a tag", "x" * 40])
    def test_settings_patch_rejects_junk_language(self, bad):
        with pytest.raises(ValueError):
            orch_main.UserSettingsUpdate(language=bad)

    def test_resolved_preference_defaults_surface_workspace_backend(self):
        # The Settings UI shows the resolved system default as the placeholder;
        # it must match what create_thread will actually apply.
        import asyncio

        with patch.object(
            orch_main,
            "postgres_db",
            SimpleNamespace(
                resolve_default_for_capability=AsyncMock(return_value=None)
            ),
        ):
            resolved = asyncio.run(orch_main._resolve_preference_defaults())
        assert (
            resolved["persistent_agent"]["workspace_backend"]
            == session_workspace_policy.SESSION_DEFAULT_WORKSPACE_BACKEND
        )

    def test_fleet_management_tools_override_passes_through(self):
        tools = orch_main._validated_session_fleet_tools_override(
            {"tools": {"orchestrator": []}}
        )
        assert tools == []

    def test_session_tool_group_overrides_pass_through(self):
        tools = session_tool_policy.validated_tool_overrides(
            {"tools": {"orchestrator": [], "agent_catalog": [], "workflows": []}}
        )
        assert tools == {"orchestrator": [], "agent_catalog": [], "workflows": []}

    def test_absent_fleet_management_tools_override_returns_none(self):
        assert orch_main._validated_session_fleet_tools_override(None) is None
        assert orch_main._validated_session_fleet_tools_override({}) is None
        assert (
            orch_main._validated_session_fleet_tools_override(
                {"tools": {"research": []}}
            )
            is None
        )

    def test_invalid_fleet_management_tools_override_rejected(self):
        with pytest.raises(orch_main.HTTPException) as exc:
            orch_main._validated_session_fleet_tools_override(
                {"tools": {"orchestrator": "disabled"}}
            )
        assert exc.value.status_code == 400

    def test_invalid_agent_catalog_tools_override_rejected(self):
        with pytest.raises(orch_main.HTTPException) as exc:
            session_tool_policy.validated_tool_overrides(
                {"tools": {"agent_catalog": "disabled"}}
            )
        assert exc.value.status_code == 400
        assert "agent_catalog" in exc.value.detail

    def test_invalid_workflows_tools_override_rejected(self):
        with pytest.raises(orch_main.HTTPException) as exc:
            session_tool_policy.validated_tool_overrides(
                {"tools": {"workflows": "disabled"}}
            )
        assert exc.value.status_code == 400
        assert "workflows" in exc.value.detail

    @pytest.mark.parametrize(
        ("group", "injected"),
        [
            ("orchestrator", "run_command"),
            ("agent_catalog", "create_job"),
            ("workflows", "get_skill"),
            ("canvas", "run_command"),
        ],
    )
    def test_cross_category_session_tool_override_rejected(self, group, injected):
        with pytest.raises(orch_main.HTTPException) as exc:
            session_tool_policy.validated_tool_overrides({"tools": {group: [injected]}})
        assert exc.value.status_code == 400
        assert group in exc.value.detail
        assert injected in exc.value.detail

    def test_known_session_tool_override_names_are_accepted(self):
        assert session_tool_policy.validated_tool_overrides(
            {
                "tools": {
                    "orchestrator": ["get_session_context"],
                    "agent_catalog": ["list_skills"],
                    "workflows": ["list_automations"],
                    "canvas": ["get_canvas", "set_canvas", "clear_canvas"],
                }
            }
        ) == {
            "orchestrator": ["get_session_context"],
            "agent_catalog": ["list_skills"],
            "workflows": ["list_automations"],
            "canvas": ["get_canvas", "set_canvas", "clear_canvas"],
        }

    def test_shared_validator_rejects_cross_category_canvas_name(self):
        with pytest.raises(ToolPolicyError, match="run_command"):
            validate_tool_override_fragment({"tools": {"canvas": ["run_command"]}})

    def test_shared_validator_honors_every_category_not_four(self):
        """Was ``..._ignores_non_session_categories``, and that was the defect.

        The boundary accepted a restriction on ``shell`` (and seven other
        categories the New Session form renders) and threw it away.
        """
        assert validate_tool_override_fragment(
            {
                "tools": {
                    "canvas": ["get_canvas"],
                    "shell": ["run_command"],
                    "research": [],
                }
            }
        ) == {"canvas": ["get_canvas"], "shell": ["run_command"], "research": []}

    def test_fleet_management_disabled_detection(self):
        assert orch_main._fleet_management_explicitly_disabled(
            {"tools": {"orchestrator": []}}
        )
        assert not orch_main._fleet_management_explicitly_disabled(
            {"tools": {"orchestrator": ["get_session_context"]}}
        )
        assert not orch_main._fleet_management_explicitly_disabled({})

    def test_agent_catalog_disabled_detection(self):
        assert orch_main._agent_catalog_explicitly_disabled(
            {"tools": {"agent_catalog": []}}
        )
        assert not orch_main._agent_catalog_explicitly_disabled(
            {"tools": {"agent_catalog": ["list_skills"]}}
        )
        assert not orch_main._agent_catalog_explicitly_disabled({})

    def test_workflows_disabled_detection(self):
        assert orch_main._workflows_explicitly_disabled({"tools": {"workflows": []}})
        assert not orch_main._workflows_explicitly_disabled(
            {"tools": {"workflows": ["list_automations"]}}
        )
        assert not orch_main._workflows_explicitly_disabled({})

    def test_session_tool_group_disabled_markers(self):
        markers = orch_main._session_tool_group_disabled_markers(
            {"tools": {"orchestrator": [], "agent_catalog": [], "workflows": []}}
        )
        assert markers == {
            "_fleet_management_disabled": True,
            "_agent_catalog_disabled": True,
            "_workflows_disabled": True,
        }


class TestSendSessionAttachPayload:
    """Hole B, orchestrator side: the attach payload carries config_name."""

    thread_id = _ATTACH_THREAD_ID
    agent_id = _ATTACH_AGENT_ID
    generation = _ATTACH_RUNTIME_GENERATION
    attach_token = _ATTACH_TOKEN

    def _thread(self, **overrides):
        thread = {
            "id": self.thread_id,
            "execution_lane": "pinned",
            "status": "created",
            "agent_id": self.agent_id,
            "runtime_generation": self.generation,
            "runtime_attach_token": self.attach_token,
            "runtime_retirement_token": None,
            "user_id": None,
            "metadata": {},
        }
        thread.update(overrides)
        return thread

    @pytest.mark.asyncio
    @pytest.mark.parametrize("execution_lane", ["stateless", "future-lane"])
    async def test_attach_refuses_every_non_pinned_lane_before_http(
        self, execution_lane
    ):
        thread = self._thread(execution_lane=execution_lane)
        with patch.object(
            orch_main.postgres_db,
            "get_thread",
            AsyncMock(return_value=thread),
        ):
            ok = await orch_main._send_session_attach(
                {"id": self.agent_id, "pod_ip": "10.0.0.1", "pod_port": 8001},
                self.thread_id,
            )

        assert ok is False
        assert _FakeAsyncClient.calls == []

    @pytest.mark.asyncio
    async def test_attach_reservation_miss_never_reaches_http(self):
        """A lost lane/detachment CAS cannot start an agent-side attach."""
        thread = self._thread()
        with (
            patch.object(
                orch_main.postgres_db,
                "get_thread",
                AsyncMock(return_value=thread),
            ),
            patch.object(
                session_attach_payload,
                "assemble_session_attach_payload",
                AsyncMock(
                    return_value={
                        "thread_id": self.thread_id,
                        "session_runtime_generation": self.generation,
                    }
                ),
            ),
            patch.object(
                orch_main,
                "_reserve_session_attach_binding",
                AsyncMock(return_value=None),
            ) as reserve,
            patch.object(orch_main.httpx, "AsyncClient", _FakeAsyncClient),
        ):
            ok = await orch_main._send_session_attach(
                {"id": self.agent_id, "pod_ip": "10.0.0.1", "pod_port": 8001},
                self.thread_id,
            )

        assert ok is False
        reserve.assert_awaited_once_with(
            self.agent_id,
            self.thread_id,
            expected_runtime_generation=self.generation,
        )
        assert _FakeAsyncClient.calls == []

    @pytest.mark.asyncio
    async def test_thread_reservation_refusal_never_claims_delivery(self):
        result = SimpleNamespace(bound=False, state="refused", attach_token=None)
        with patch.object(
            orch_main,
            "reserve_pinned_warm_agent_binding",
            AsyncMock(return_value=result),
        ) as reserve:
            reserved = await _REAL_RESERVE_SESSION_ATTACH_BINDING(
                self.agent_id,
                self.thread_id,
                expected_runtime_generation=self.generation,
            )

        assert reserved is None
        reserve.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_pending_warm_protection_fences_fallback_delivery(self):
        result = SimpleNamespace(bound=False, state="pending", attach_token=None)
        with patch.object(
            orch_main,
            "reserve_pinned_warm_agent_binding",
            AsyncMock(return_value=result),
        ):
            with pytest.raises(orch_main._WarmBindingReservationPending):
                await _REAL_RESERVE_SESSION_ATTACH_BINDING(
                    self.agent_id,
                    self.thread_id,
                    expected_runtime_generation=self.generation,
                )

    @pytest.mark.asyncio
    async def test_bound_warm_protection_returns_exact_attach_token(self):
        result = SimpleNamespace(
            bound=True,
            state="bound",
            attach_token=self.attach_token,
        )
        with patch.object(
            orch_main,
            "reserve_pinned_warm_agent_binding",
            AsyncMock(return_value=result),
        ):
            reserved = await _REAL_RESERVE_SESSION_ATTACH_BINDING(
                self.agent_id,
                self.thread_id,
                expected_runtime_generation=self.generation,
            )

        assert reserved == self.attach_token

    @pytest.mark.asyncio
    async def test_successful_reservation_precedes_http_delivery(self):
        thread = self._thread()
        order: list[str] = []

        async def reserve(*_args, **_kwargs):
            order.append("reserve")
            return self.attach_token

        async def post(_self, url, json=None):
            assert order == ["reserve"]
            order.append("http")
            _FakeAsyncClient.calls.append({"url": url, "json": json})
            return _FakeResponse(200)

        with (
            patch.object(
                orch_main.postgres_db,
                "get_thread",
                AsyncMock(return_value=thread),
            ),
            patch.object(
                session_attach_payload,
                "assemble_session_attach_payload",
                AsyncMock(
                    return_value={
                        "thread_id": self.thread_id,
                        "session_runtime_generation": self.generation,
                    }
                ),
            ),
            patch.object(
                orch_main,
                "_reserve_session_attach_binding",
                AsyncMock(side_effect=reserve),
            ),
            patch.object(_FakeAsyncClient, "post", post),
            patch.object(orch_main.httpx, "AsyncClient", _FakeAsyncClient),
        ):
            ok = await orch_main._send_session_attach(
                {"id": self.agent_id, "pod_ip": "10.0.0.1", "pod_port": 8001},
                self.thread_id,
            )

        assert ok is True
        assert order == ["reserve", "http"]

    @pytest.mark.asyncio
    async def test_409_retains_reservation_because_same_thread_is_ambiguous(self):
        thread = self._thread()
        _FakeAsyncClient.response_status = 409
        with (
            patch.object(
                orch_main.postgres_db,
                "get_thread",
                AsyncMock(return_value=thread),
            ),
            patch.object(
                session_attach_payload,
                "assemble_session_attach_payload",
                AsyncMock(
                    return_value={
                        "thread_id": self.thread_id,
                        "session_runtime_generation": self.generation,
                    }
                ),
            ),
            patch.object(orch_main.httpx, "AsyncClient", _FakeAsyncClient),
            patch.object(
                orch_main,
                "_reserve_session_attach_binding",
                AsyncMock(return_value=self.attach_token),
            ) as reserve,
        ):
            ok = await orch_main._send_session_attach(
                {"id": self.agent_id, "pod_ip": "10.0.0.1", "pod_port": 8001},
                self.thread_id,
            )

        assert ok is True
        reserve.assert_awaited_once_with(
            self.agent_id,
            self.thread_id,
            expected_runtime_generation=self.generation,
        )
        assert [call["url"] for call in _FakeAsyncClient.calls] == [
            "http://10.0.0.1:8001/session/attach"
        ]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status_code", [422, 500])
    async def test_ambiguous_http_failure_retains_reservation(self, status_code):
        thread = self._thread()
        _FakeAsyncClient.response_status = status_code
        with (
            patch.object(
                orch_main.postgres_db,
                "get_thread",
                AsyncMock(return_value=thread),
            ),
            patch.object(
                session_attach_payload,
                "assemble_session_attach_payload",
                AsyncMock(
                    return_value={
                        "thread_id": self.thread_id,
                        "session_runtime_generation": self.generation,
                    }
                ),
            ),
            patch.object(orch_main.httpx, "AsyncClient", _FakeAsyncClient),
        ):
            ok = await orch_main._send_session_attach(
                {"id": self.agent_id, "pod_ip": "10.0.0.1", "pod_port": 8001},
                self.thread_id,
            )

        assert ok is True

    @pytest.mark.asyncio
    async def test_ambiguous_attach_log_does_not_echo_secret_response(self, caplog):
        private_material = "-----BEGIN OPENSSH PRIVATE KEY-----hidden"
        thread = self._thread()
        _FakeAsyncClient.response_status = 422
        _FakeAsyncClient.response_text = private_material
        with (
            patch.object(
                orch_main.postgres_db,
                "get_thread",
                AsyncMock(return_value=thread),
            ),
            patch.object(
                session_attach_payload,
                "assemble_session_attach_payload",
                AsyncMock(
                    return_value={
                        "thread_id": self.thread_id,
                        "session_runtime_generation": self.generation,
                    }
                ),
            ),
            patch.object(orch_main.httpx, "AsyncClient", _FakeAsyncClient),
        ):
            ok = await orch_main._send_session_attach(
                {"id": self.agent_id, "pod_ip": "10.0.0.1", "pod_port": 8001},
                self.thread_id,
            )

        assert ok is True
        assert private_material not in caplog.text
        assert "ambiguous attach response 422" in caplog.text

    @pytest.mark.asyncio
    async def test_transport_failure_retains_reservation(self):
        thread = self._thread()
        _FakeAsyncClient.raise_on_post = TimeoutError("response lost")
        with (
            patch.object(
                orch_main.postgres_db,
                "get_thread",
                AsyncMock(return_value=thread),
            ),
            patch.object(
                session_attach_payload,
                "assemble_session_attach_payload",
                AsyncMock(
                    return_value={
                        "thread_id": self.thread_id,
                        "session_runtime_generation": self.generation,
                    }
                ),
            ),
            patch.object(orch_main.httpx, "AsyncClient", _FakeAsyncClient),
        ):
            ok = await orch_main._send_session_attach(
                {"id": self.agent_id, "pod_ip": "10.0.0.1", "pod_port": 8001},
                self.thread_id,
            )

        assert ok is True

    @pytest.mark.asyncio
    async def test_detach_save_cannot_finish_before_inflight_delivery(self):
        gate = asyncio.Lock()
        delivery_started = asyncio.Event()
        release_delivery = asyncio.Event()
        order: list[str] = []

        @asynccontextmanager
        async def shared_lock():
            async with gate:
                yield

        async def fake_delivery(*_args, **_kwargs):
            delivery_started.set()
            await release_delivery.wait()
            order.append("delivery")
            return True

        async def save_detach():
            async with orch_main.postgres_db.thread_datasource_lock(self.thread_id):
                order.append("save")

        with (
            patch.object(
                orch_main.postgres_db,
                "thread_datasource_lock",
                side_effect=lambda _thread_id: shared_lock(),
            ),
            patch.object(
                orch_main,
                "_send_session_attach_locked",
                AsyncMock(side_effect=fake_delivery),
            ),
        ):
            attach_task = asyncio.create_task(
                orch_main._send_session_attach(
                    {
                        "id": self.agent_id,
                        "pod_ip": "10.0.0.1",
                        "pod_port": 8001,
                    },
                    self.thread_id,
                )
            )
            await delivery_started.wait()
            save_task = asyncio.create_task(save_detach())
            await asyncio.sleep(0)
            assert not save_task.done()

            release_delivery.set()
            assert await attach_task is True
            await save_task

        assert order == ["delivery", "save"]

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("stored_narration", "expected_narration"),
        [(None, "silent"), ("verbose", "verbose")],
        ids=["legacy-inherited", "materialized-control-scalar"],
    )
    async def test_attach_overlays_first_class_control_scalars_on_resolved_config(
        self, stored_narration, expected_narration
    ):
        thread = self._thread(
            user_id="owner-1",
            permission_mode="autonomous",
            narration_mode=stored_narration,
        )
        resolved = {
            "agent": {
                "interactive": {
                    "permission_mode": "supervised",
                    "narration_mode": "silent",
                }
            }
        }
        with (
            patch.object(
                orch_main.postgres_db,
                "get_thread",
                AsyncMock(return_value=thread),
            ),
            patch.object(
                orch_main,
                "_inject_lite_workspace_config",
                side_effect=lambda value, **_kwargs: value,
            ),
            patch.object(orch_main, "_thread_project_ids", AsyncMock(return_value=[])),
            patch.object(
                orch_main,
                "_revalidate_thread_project_ids",
                AsyncMock(return_value=[]),
            ),
            patch.object(
                orch_main,
                "_resolve_authorized_thread_datasources",
                AsyncMock(return_value=[]),
            ),
            patch.object(
                orch_main, "_resolve_session_config", AsyncMock(return_value=resolved)
            ),
        ):
            payload = await session_attach_payload.assemble_session_attach_payload(
                self.thread_id,
                dependencies=orch_main._session_attach_payload_dependencies(),
            )

        interactive = payload["resolved_config"]["agent"]["interactive"]
        assert interactive == {
            "permission_mode": "autonomous",
            "narration_mode": expected_narration,
        }

    @pytest.mark.asyncio
    async def test_attach_materializes_scalars_in_legacy_config_fallback(self):
        thread = self._thread(
            user_id="owner-1",
            permission_mode="auto_accept",
            narration_mode="auto",
        )
        with (
            patch.object(
                orch_main.postgres_db,
                "get_thread",
                AsyncMock(return_value=thread),
            ),
            patch.object(
                orch_main,
                "_inject_lite_workspace_config",
                side_effect=lambda value, **_kwargs: value,
            ),
            patch.object(orch_main, "_thread_project_ids", AsyncMock(return_value=[])),
            patch.object(
                orch_main,
                "_revalidate_thread_project_ids",
                AsyncMock(return_value=[]),
            ),
            patch.object(
                orch_main,
                "_resolve_authorized_thread_datasources",
                AsyncMock(return_value=[]),
            ),
            patch.object(
                orch_main, "_resolve_session_config", AsyncMock(return_value=None)
            ),
        ):
            payload = await session_attach_payload.assemble_session_attach_payload(
                self.thread_id,
                config_override={"llm": {"model": "m"}},
                dependencies=orch_main._session_attach_payload_dependencies(),
            )

        assert payload["resolved_config"] is None
        assert payload["config_override"]["interactive"] == {
            "permission_mode": "auto_accept",
            "narration_mode": "auto",
        }

    @pytest.mark.asyncio
    async def test_virtual_binding_generation_is_not_a_physical_attach_identity(self):
        thread = self._thread(
            user_id="owner-1",
            metadata={
                "config_override": {"workspace": {"backend": "virtual"}},
                "workspace_container": {
                    "git_remote_url": "https://git.invalid/project.git",
                    "repo_name": "project",
                },
                "_workspace_binding": {
                    "generation": "50000000-0000-4000-8000-000000000001",
                    "kind": "virtual",
                    "backing_id": "rclone:test-backing",
                    "ssh_host_key_fingerprint": None,
                },
            },
        )
        with (
            patch.object(
                orch_main.postgres_db,
                "get_thread",
                AsyncMock(return_value=thread),
            ),
            patch.object(
                orch_main,
                "_inject_lite_workspace_config",
                side_effect=lambda value, **_kwargs: value,
            ),
            patch.object(orch_main, "_thread_project_ids", AsyncMock(return_value=[])),
            patch.object(
                orch_main,
                "_revalidate_thread_project_ids",
                AsyncMock(return_value=[]),
            ),
            patch.object(
                orch_main,
                "_resolve_authorized_thread_datasources",
                AsyncMock(return_value=[]),
            ),
            patch.object(
                orch_main, "_resolve_session_config", AsyncMock(return_value=None)
            ),
        ):
            payload = await session_attach_payload.assemble_session_attach_payload(
                self.thread_id,
                config_override={"workspace": {"backend": "virtual"}},
                dependencies=orch_main._session_attach_payload_dependencies(),
            )

        assert payload is not None
        assert payload["workspace_generation"] is None
        assert payload["workspace_runtime_incarnation"] is None

    @pytest.mark.asyncio
    async def test_remote_binding_still_requires_a_physical_runtime_pair(self):
        thread = self._thread(
            user_id="owner-1",
            metadata={
                "config_override": {"workspace": {"backend": "sandbox"}},
                "workspace_container": {"provisioner": "k8s"},
                "_workspace_binding": {
                    "generation": "50000000-0000-4000-8000-000000000001",
                    "kind": "remote",
                    "backing_id": "k8s-pvc:test",
                    "ssh_host_key_fingerprint": "SHA256:test",
                },
            },
        )
        with (
            patch.object(
                orch_main.postgres_db,
                "get_thread",
                AsyncMock(return_value=thread),
            ),
            patch.object(
                orch_main,
                "_inject_lite_workspace_config",
                side_effect=lambda value, **_kwargs: value,
            ),
            patch.object(orch_main, "_thread_project_ids", AsyncMock(return_value=[])),
            patch.object(
                orch_main,
                "_revalidate_thread_project_ids",
                AsyncMock(return_value=[]),
            ),
            patch.object(
                orch_main,
                "_resolve_authorized_thread_datasources",
                AsyncMock(return_value=[]),
            ),
            patch.object(
                orch_main, "_resolve_session_config", AsyncMock(return_value=None)
            ),
        ):
            payload = await session_attach_payload.assemble_session_attach_payload(
                self.thread_id,
                config_override={"workspace": {"backend": "sandbox"}},
                dependencies=orch_main._session_attach_payload_dependencies(),
            )

        assert payload is None

    @pytest.mark.asyncio
    async def test_payload_carries_config_name(self):
        _FakeAsyncClient.response_status = 500
        thread = self._thread()
        with (
            patch.object(
                orch_main.postgres_db,
                "get_thread",
                AsyncMock(return_value=thread),
            ),
            patch.object(
                orch_main,
                "_thread_project_ids",
                AsyncMock(return_value=[]),
            ),
            patch.object(orch_main, "_is_experts_db_enabled", return_value=False),
            patch.object(orch_main.httpx, "AsyncClient", _FakeAsyncClient),
        ):
            ok = await orch_main._send_session_attach(
                {"id": self.agent_id, "pod_ip": "10.0.0.1", "pod_port": 8001},
                self.thread_id,
                {"llm": {"model": "m"}},
                ["p1"],
                datasources=None,
                config_name="session_base",
            )
        assert ok is True
        assert len(_FakeAsyncClient.calls) == 1
        call = _FakeAsyncClient.calls[0]
        assert call["url"] == "http://10.0.0.1:8001/session/attach"
        assert call["json"]["config_name"] == "session_base"
        assert call["json"]["thread_id"] == self.thread_id
        assert call["json"]["runtime_actor"]["caller_kind"] == "human"
        assert call["json"]["runtime_actor"]["access_credential"].startswith("sra_")

    @pytest.mark.asyncio
    async def test_attach_refuses_revoked_persisted_datasource_before_http(self):
        datasource_id = "11111111-2222-3333-4444-555555555555"
        thread = self._thread(
            user_id="aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            metadata={"datasource_ids": [datasource_id]},
        )
        denied = HTTPException(
            status_code=403,
            detail="One or more selected connectors are unavailable",
        )

        with (
            patch.object(
                orch_main.postgres_db,
                "get_thread",
                AsyncMock(return_value=thread),
            ),
            # R1.B06: `resolve_authorized_thread_datasources` moved into
            # services/thread_datasource_authorization and now calls its
            # module-local revalidator, so the double belongs there —
            # patching main would be green but inert.
            patch.object(
                thread_datasource_authorization,
                "revalidate_thread_datasource_selection",
                AsyncMock(side_effect=denied),
            ) as revalidate,
            patch.object(
                orch_main,
                "_thread_project_ids",
                AsyncMock(return_value=[]),
            ),
            patch.object(
                orch_main,
                "_revalidate_thread_project_ids",
                AsyncMock(return_value=[]),
            ),
            patch.object(orch_main.httpx, "AsyncClient", _FakeAsyncClient),
        ):
            ok = await orch_main._send_session_attach(
                {"id": self.agent_id, "pod_ip": "10.0.0.1", "pod_port": 8001},
                self.thread_id,
                {},
                [],
                datasources=[{"type": "kb", "datasource_id": datasource_id}],
                config_name="persistent_defaults",
            )

        assert ok is False
        # R1.B06: the call now carries the service's injected ``dependencies``.
        # Assert the subject, not the plumbing.
        revalidate.assert_awaited_once()
        assert revalidate.await_args.args == (thread, [datasource_id])
        assert revalidate.await_args.kwargs["target_project_ids"] == []
        assert _FakeAsyncClient.calls == []

    @pytest.mark.asyncio
    async def test_attach_refuses_revoked_native_project_before_http(self):
        project_id = "99999999-2222-3333-4444-555555555555"
        thread = self._thread(
            user_id="aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        )
        denied = HTTPException(
            status_code=403,
            detail="One or more attached projects are unavailable",
        )

        with (
            patch.object(
                orch_main.postgres_db,
                "get_thread",
                AsyncMock(return_value=thread),
            ),
            # R1.B06: `resolve_authorized_thread_datasources` moved into
            # services/thread_datasource_authorization and now calls its
            # module-local revalidator, so the double belongs there —
            # patching main would be green but inert.
            patch.object(
                thread_datasource_authorization,
                "revalidate_thread_datasource_selection",
                AsyncMock(return_value=([], {})),
            ),
            patch.object(
                orch_main,
                "_thread_project_ids",
                AsyncMock(return_value=[project_id]),
            ),
            patch.object(
                orch_main,
                "_revalidate_thread_project_ids",
                AsyncMock(side_effect=denied),
            ) as revalidate,
            patch.object(orch_main.httpx, "AsyncClient", _FakeAsyncClient),
        ):
            ok = await orch_main._send_session_attach(
                {"id": self.agent_id, "pod_ip": "10.0.0.1", "pod_port": 8001},
                self.thread_id,
                {},
                [project_id],
                datasources=None,
                config_name="persistent_defaults",
            )

        assert ok is False
        revalidate.assert_awaited_once_with(thread, [project_id])
        assert _FakeAsyncClient.calls == []

    @pytest.mark.asyncio
    async def test_attach_discards_caller_payload_and_resolves_current_selection(self):
        stale_id = "11111111-2222-4333-8444-555555555555"
        current_id = "66666666-7777-4888-8999-aaaaaaaaaaaa"
        project_id = "99999999-2222-4333-8444-555555555555"
        thread = self._thread(
            user_id="aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
            metadata={"datasource_ids": [current_id]},
        )
        current_row = {
            "id": current_id,
            "type": "kb",
            "name": "Current KB",
            "description": None,
            "connection_url": None,
            "credentials": {},
            "config": {},
            "project_read_only": True,
            "policy_revision": 2,
        }
        resolver = AsyncMock(return_value=[current_row])

        _FakeAsyncClient.response_status = 500
        with (
            patch.object(
                orch_main.postgres_db,
                "get_thread",
                AsyncMock(return_value=thread),
            ),
            patch.object(
                orch_main,
                "_thread_project_ids",
                AsyncMock(return_value=[project_id]),
            ),
            patch.object(
                orch_main,
                "_revalidate_thread_project_ids",
                AsyncMock(return_value=[project_id]),
            ),
            # R1.B06: `resolve_authorized_thread_datasources` moved into
            # services/thread_datasource_authorization and now calls its
            # module-local revalidator, so the double belongs there —
            # patching main would be green but inert.
            patch.object(
                thread_datasource_authorization,
                "revalidate_thread_datasource_selection",
                AsyncMock(return_value=([current_id], {current_id: 2})),
            ),
            patch.object(
                orch_main.postgres_db,
                "resolve_datasources_for_thread",
                resolver,
            ),
            patch.object(orch_main, "_is_experts_db_enabled", return_value=False),
            patch.object(orch_main.httpx, "AsyncClient", _FakeAsyncClient),
        ):
            ok = await orch_main._send_session_attach(
                {"id": self.agent_id, "pod_ip": "10.0.0.1", "pod_port": 8001},
                self.thread_id,
                {},
                ["stale-project"],
                datasources=[{"type": "kb", "datasource_id": stale_id}],
                config_name="persistent_defaults",
            )

        assert ok is True
        resolver.assert_awaited_once_with(
            datasource_ids=[current_id],
            project_ids=[project_id],
        )
        payload = _FakeAsyncClient.calls[0]["json"]
        assert payload["project_ids"] == [project_id]
        assert payload["datasources"][0]["datasource_id"] == current_id
        assert stale_id not in str(payload["datasources"])


class TestColdSessionDatasourceDelivery:
    @pytest.mark.asyncio
    async def test_pinned_workspace_owner_is_exact_and_strict_mode_is_staged(
        self, monkeypatch
    ):
        agent_a = "00000000-0000-0000-0000-0000000000a1"
        agent_b = "00000000-0000-0000-0000-0000000000b2"
        generation = "00000000-0000-4000-8000-0000000000d4"
        attach_token = "00000000-0000-4000-8000-0000000000e5"
        thread = {
            "id": "00000000-0000-0000-0000-0000000000c3",
            "execution_lane": "pinned",
            "status": "created",
            "agent_id": agent_b,
            "runtime_generation": generation,
            "runtime_attach_token": attach_token,
            "runtime_retirement_token": None,
        }
        reciprocal = AsyncMock(return_value=True)
        with patch.object(
            orch_main.postgres_db,
            "pinned_thread_agent_is_reciprocal",
            reciprocal,
        ):
            assert (
                await orch_main._require_pinned_workspace_credential_owner(
                    thread, agent_b, generation, attach_token
                )
                == agent_b
            )
            with pytest.raises(HTTPException) as stale:
                await orch_main._require_pinned_workspace_credential_owner(
                    thread, agent_a, generation, attach_token
                )
            assert stale.value.status_code == 409
            assert stale.value.detail["code"] == "pinned_runtime_identity_mismatch"

            # A compatibility override remains available for isolated rollback
            # tests, but the post-0185 application default is strict.
            monkeypatch.setenv("REQUIRE_PINNED_STATUS_IDENTITY", "false")
            assert (
                await orch_main._require_pinned_workspace_credential_owner(
                    thread, None, None, None
                )
                is None
            )
            monkeypatch.setenv("REQUIRE_PINNED_STATUS_IDENTITY", "true")
            with pytest.raises(HTTPException) as missing:
                await orch_main._require_pinned_workspace_credential_owner(
                    thread, None, None, None
                )
            assert missing.value.detail["code"] == "pinned_status_identity_required"

            monkeypatch.delenv("REQUIRE_PINNED_STATUS_IDENTITY", raising=False)
            protected = {
                **thread,
                "metadata": {"protected_cloud": True},
            }
            with pytest.raises(HTTPException) as protected_missing:
                await orch_main._require_pinned_workspace_credential_owner(
                    protected, None, None, None
                )
            assert (
                protected_missing.value.detail["code"]
                == "pinned_status_identity_required"
            )

        reciprocal.assert_awaited_once_with(
            thread["id"],
            agent_b,
            expected_runtime_generation=generation,
            expected_attach_token=attach_token,
            expected_ro_mount_id=None,
            expected_ro_engage_attempt=None,
            expected_ro_grant_handle=None,
            expected_ro_reader_id=None,
            expected_ro_webdav_url=None,
        )

    @pytest.mark.asyncio
    async def test_old_runtime_crossing_end_resume_cannot_receive_successor_credentials(
        self,
    ):
        agent_a = "00000000-0000-0000-0000-0000000000a1"
        agent_b = "00000000-0000-0000-0000-0000000000b2"
        thread_id = "00000000-0000-0000-0000-0000000000c3"
        generation_a = "00000000-0000-4000-8000-0000000000d4"
        generation_b = "00000000-0000-4000-8000-0000000000d5"
        attach_a = "00000000-0000-4000-8000-0000000000e6"
        entry = {
            "id": thread_id,
            "execution_lane": "pinned",
            "status": "created",
            "agent_id": agent_a,
            "runtime_generation": generation_a,
            "runtime_attach_token": attach_a,
            "runtime_retirement_token": None,
            "user_id": None,
            "project_id": None,
            "metadata": {"config_override": {"llm": {"api_key": "secret-sentinel"}}},
        }
        successor = {
            **entry,
            "status": "active",
            "agent_id": agent_b,
            "runtime_generation": generation_b,
            "runtime_attach_token": "00000000-0000-4000-8000-0000000000e7",
            "metadata": {},
        }

        with (
            patch.object(
                orch_main.postgres_db,
                "get_thread",
                AsyncMock(side_effect=[entry, successor]),
            ),
            patch.object(
                orch_main.postgres_db,
                "pinned_thread_agent_is_reciprocal",
                AsyncMock(return_value=True),
            ),
            patch.object(
                orch_main.postgres_db,
                "list_thread_mounts",
                AsyncMock(return_value=[]),
            ),
            patch.object(orch_main, "_thread_project_ids", AsyncMock(return_value=[])),
            patch.object(
                orch_main,
                "_revalidate_thread_project_ids",
                AsyncMock(return_value=[]),
            ),
            patch.object(
                thread_workspace_delivery,
                "agent_canvas_workspace_capabilities",
                return_value=(False, False, False),
            ),
            patch.object(
                orch_main, "_resolve_thread_datasources", AsyncMock(return_value=None)
            ),
            patch.object(
                orch_main, "_build_agent_cloud_mount", AsyncMock(return_value=None)
            ),
            patch.object(orch_main, "_build_agent_cloud_sync", return_value=None),
            patch.object(
                orch_main,
                "_inject_thread_dispatch_credentials",
                AsyncMock(return_value={"llm": {"api_key": "secret-sentinel"}}),
            ),
            patch.object(
                orch_main, "_resolve_session_config", AsyncMock(return_value=None)
            ),
            patch.object(
                orch_main,
                "_resolve_thread_repositories",
                AsyncMock(return_value=None),
            ),
            patch.object(
                orch_main.postgres_db,
                "managed_repository_authorities_are_current",
                AsyncMock(return_value=True),
            ),
            patch.object(
                orch_main,
                "_inject_lite_workspace_config",
                side_effect=lambda value, **_kwargs: value,
            ),
        ):
            with pytest.raises(HTTPException) as denied:
                await control_seams.agent_get_thread_workspace_locked(
                    thread_id,
                    presented_agent_id=agent_a,
                    presented_runtime_generation=generation_a,
                    presented_attach_token=attach_a,
                )

        assert denied.value.status_code == 409
        assert denied.value.detail["code"] == "pinned_runtime_identity_mismatch"
        assert "secret-sentinel" not in str(denied.value.detail)

    @pytest.mark.asyncio
    async def test_prepared_protected_reader_attempt_is_fenced_at_response_boundary(
        self,
    ):
        """A same-G A1 -> A2 grant rotation cannot leak A1 credentials.

        The expensive response is prepared from A1. Immediately before the
        final joined owner/RO query, another replica has published A2 for the
        same thread generation. The response must be refused, not splice A1's
        password into a thread snapshot that merely still has the same G.
        """

        thread_id = "00000000-0000-4000-8000-0000000000c3"
        agent_id = "00000000-0000-4000-8000-0000000000a1"
        generation = "00000000-0000-4000-8000-0000000000d4"
        attach_token = "00000000-0000-4000-8000-0000000000e5"
        attempt_a1 = "00000000-0000-4000-8000-0000000000f1"
        attempt_a2 = "00000000-0000-4000-8000-0000000000f2"
        backend_instance_id = "00000000-0000-4000-8000-000000000061"
        project_id = "00000000-0000-4000-8000-000000000062"
        mount_rows = [
            {
                "id": "00000000-0000-4000-8000-000000000071",
                "mount_kind": "project",
                "backend_id": "nextcloud",
                "backend_instance_id": backend_instance_id,
                "source_kind": "project_folder",
                "source_ref": project_id,
                "target_path": "projects/proj",
                "cloud_handle": (
                    '{"backend":"nextcloud","native_id":"42",'
                    '"vendor_meta":{"mountpoint":"Proj"}}'
                ),
            }
        ]
        source = ProtectedMountSourceIdentity.from_mount_row(mount_rows[0])
        assert source is not None
        plan = ProtectedNextcloudReaderGrantPlan(
            engage_attempt=attempt_a1,
            backend_instance_id=backend_instance_id,
            source=source,
        )
        ro_a1 = {
            "id": "00000000-0000-4000-8000-000000000081",
            "selected_mount_id": mount_rows[0]["id"],
            "thread_id": thread_id,
            "user_id": "user-1",
            "backend": "nextcloud",
            "backend_instance_id": backend_instance_id,
            "reader_id": plan.reader_id,
            "grant_group_id": plan.group_id,
            "grant_handle": plan.grant_handle,
            "grant_handle_sha256": plan.grant_handle_sha256,
            "source_binding": source.binding,
            "source_binding_sha256": source.sha256,
            "credentials": "credential-a1-sentinel",
            "webdav_url": (
                "https://nc.internal/remote.php/dav/files/"
                f"{plan.reader_id}/{plan.mountpoint}/"
            ),
            "auth_kind": "basic",
            "status": "active",
            "etag_baseline": {},
            "runtime_generation": generation,
            "engage_attempt": attempt_a1,
        }
        workspace = {
            "status": "ready",
            # A ready pinned sandbox workspace declares its provisioner; the
            # attach path refuses an undeclared one (503) before it reaches
            # this credential boundary.
            "provisioner": "k8s",
            "pod_ip": "10.42.0.10",
            "pod_port": 30022,
            "_canvas_workspace_generation": ("00000000-0000-4000-8000-000000000091"),
            "_runtime_incarnation": "00000000-0000-4000-8000-000000000092",
        }
        attestation = orch_main.WorkspaceRuntimeAttestation(
            backing_id="k8s-pvc:agent-workspaces:pvc-uid-a1",
            workspace_generation=workspace["_canvas_workspace_generation"],
            runtime_incarnation=workspace["_runtime_incarnation"],
            ssh_host_key_fingerprint="SHA256:trusted-a1",
            host="workspace-session.agent-workspaces.svc.cluster.local",
            pod_ip=workspace["pod_ip"],
            port=workspace["pod_port"],
        )
        thread = {
            "id": thread_id,
            "execution_lane": "pinned",
            "status": "created",
            "agent_id": agent_id,
            "runtime_generation": generation,
            "runtime_attach_token": attach_token,
            "runtime_retirement_token": None,
            "user_id": "user-1",
            "project_id": None,
            "metadata": {
                "protected_cloud": True,
                "config_override": {"workspace": {"backend": "sandbox"}},
                "workspace_container": workspace,
                "_workspace_binding": {
                    "generation": workspace["_canvas_workspace_generation"],
                    "kind": "remote",
                    "backing_id": "k8s-pvc:agent-workspaces:pvc-uid-a1",
                    "ssh_host_key_fingerprint": "SHA256:trusted-a1",
                    "runtime_incarnation": workspace["_runtime_incarnation"],
                },
            },
        }
        state = {"attempt": attempt_a1}

        async def get_thread(_thread_id):
            return thread

        async def reciprocal(_thread_id, _agent_id, **kwargs):
            expected_attempt = kwargs.get("expected_ro_engage_attempt")
            if expected_attempt is not None:
                # The peer publication lands after A1 was prepared but before
                # the joined final owner/RO snapshot evaluates its predicates.
                state["attempt"] = attempt_a2
            return expected_attempt is None or expected_attempt == state["attempt"]

        reciprocal_check = AsyncMock(side_effect=reciprocal)
        with (
            patch.object(
                orch_main.postgres_db,
                "get_thread",
                AsyncMock(side_effect=get_thread),
            ),
            patch.object(
                orch_main.container_provisioner,
                "attest_workspace_runtime",
                AsyncMock(return_value=attestation),
            ),
            patch.object(
                orch_main.postgres_db,
                "pinned_thread_agent_is_reciprocal",
                reciprocal_check,
            ),
            patch.object(
                orch_main,
                "_protected_cloud_delivery_state",
                AsyncMock(return_value=("ready", None)),
            ),
            patch.object(
                orch_main.postgres_db,
                "get_ro_mount_by_thread",
                AsyncMock(return_value=ro_a1),
            ),
            patch.object(
                orch_main.postgres_db,
                "list_thread_mounts",
                AsyncMock(return_value=mount_rows),
            ),
            patch.object(orch_main, "_thread_project_ids", AsyncMock(return_value=[])),
            patch.object(
                orch_main,
                "_revalidate_thread_project_ids",
                AsyncMock(return_value=[]),
            ),
            patch.object(
                thread_workspace_delivery,
                "agent_canvas_workspace_capabilities",
                return_value=(False, False, False),
            ),
            patch.object(
                orch_main, "_resolve_thread_datasources", AsyncMock(return_value=None)
            ),
            patch.object(
                orch_main, "_build_agent_cloud_mount", AsyncMock(return_value=None)
            ),
            patch.object(orch_main, "_build_agent_cloud_sync", return_value=None),
            patch.object(
                orch_main,
                "_inject_thread_dispatch_credentials",
                AsyncMock(return_value={"workspace": {"backend": "sandbox"}}),
            ),
            patch.object(
                orch_main, "_resolve_session_config", AsyncMock(return_value=None)
            ),
            patch.object(
                orch_main, "_resolve_thread_repositories", AsyncMock(return_value=None)
            ),
            patch.object(
                orch_main.postgres_db,
                "managed_repository_authorities_are_current",
                AsyncMock(return_value=True),
            ),
            patch.object(
                orch_main,
                "_inject_lite_workspace_config",
                side_effect=lambda value, **_kwargs: value,
            ),
        ):
            with pytest.raises(HTTPException) as denied:
                await control_seams.agent_get_thread_workspace_locked(
                    thread_id,
                    presented_agent_id=agent_id,
                    presented_runtime_generation=generation,
                    presented_attach_token=attach_token,
                )

        assert denied.value.status_code == 409
        assert denied.value.detail["code"] == "pinned_runtime_identity_mismatch"
        assert "credential-a1-sentinel" not in str(denied.value.detail)
        assert reciprocal_check.await_count == 2
        assert (
            reciprocal_check.await_args_list[-1].kwargs["expected_ro_engage_attempt"]
            == attempt_a1
        )

    @pytest.mark.asyncio
    async def test_live_detach_commits_before_cold_handler_refetches(self):
        thread_id = _ATTACH_THREAD_ID
        datasource_a = "11111111-2222-4333-8444-555555555555"
        state = {"datasource_ids": [datasource_a]}
        gate = asyncio.Lock()
        writer_entered = asyncio.Event()
        allow_writer_commit = asyncio.Event()

        @asynccontextmanager
        async def shared_lock():
            async with gate:
                yield

        async def current_thread(_thread_id):
            # Return a fresh row so a pre-lock read would retain A even after
            # the writer replaces the canonical metadata with [].
            return {
                "id": thread_id,
                "execution_lane": "pinned",
                "status": "created",
                "agent_id": _ATTACH_AGENT_ID,
                "runtime_generation": _ATTACH_RUNTIME_GENERATION,
                "runtime_attach_token": _ATTACH_TOKEN,
                "runtime_retirement_token": None,
                "user_id": None,
                "project_id": None,
                "metadata": {"datasource_ids": list(state["datasource_ids"])},
            }

        async def save_detach():
            async with orch_main.postgres_db.thread_datasource_lock(thread_id):
                assert state["datasource_ids"] == [datasource_a]
                writer_entered.set()
                await allow_writer_commit.wait()
                state["datasource_ids"] = []

        get_thread = AsyncMock(side_effect=current_thread)
        with (
            patch.object(
                orch_main.postgres_db,
                "thread_datasource_lock",
                side_effect=lambda _thread_id: shared_lock(),
            ),
            patch.object(orch_main.postgres_db, "get_thread", get_thread),
            patch.object(
                orch_main.postgres_db,
                "pinned_thread_agent_is_reciprocal",
                AsyncMock(return_value=True),
            ),
            patch.object(
                orch_main.postgres_db,
                "list_thread_mounts",
                AsyncMock(return_value=[]),
            ),
            patch.object(orch_main, "require_internal", AsyncMock()),
            patch.object(orch_main, "_thread_project_ids", AsyncMock(return_value=[])),
            patch.object(
                thread_workspace_delivery,
                "agent_canvas_workspace_capabilities",
                return_value=(False, False, False),
            ),
            patch.object(
                orch_main, "_build_agent_cloud_mount", AsyncMock(return_value=None)
            ),
            patch.object(orch_main, "_build_agent_cloud_sync", return_value=None),
            patch.object(
                orch_main, "_resolve_session_config", AsyncMock(return_value=None)
            ),
            patch.object(
                orch_main,
                "_inject_lite_workspace_config",
                side_effect=lambda value, **_kwargs: value,
            ),
        ):
            writer = asyncio.create_task(save_detach())
            await writer_entered.wait()
            cold_response = asyncio.create_task(
                # R1.B05 moved this route to `routers/agent_thread_workspace`.
                # It resolves its collaborators from the application handling
                # the request, so the fake request carries the same factory the
                # real app registers — which is what makes the `orch_main`
                # patches above still reach the handler.
                agent_thread_workspace_routes.agent_get_thread_workspace(
                    SimpleNamespace(
                        headers={
                            "X-Agent-ID": _ATTACH_AGENT_ID,
                            "X-Session-Runtime-Generation": (
                                _ATTACH_RUNTIME_GENERATION
                            ),
                            "X-Session-Runtime-Attach-Token": _ATTACH_TOKEN,
                        },
                        app=SimpleNamespace(
                            state=SimpleNamespace(
                                thread_workspace_delivery_dependencies_factory=(
                                    orch_main._thread_workspace_delivery_dependencies
                                )
                            )
                        ),
                    ),
                    thread_id,
                )
            )
            await asyncio.sleep(0)

            # The old implementation read A before waiting on any lock.  The
            # cold handler must not touch canonical state while Save owns it.
            get_thread.assert_not_awaited()

            allow_writer_commit.set()
            await writer
            response = await cold_response

        assert response["datasources"] is None
        assert datasource_a not in str(response)
        assert get_thread.await_count == 2
        assert all(call.args == (thread_id,) for call in get_thread.await_args_list)

    @pytest.mark.asyncio
    async def test_attach_discards_stale_payload_after_detach_all(self):
        stale_id = "11111111-2222-4333-8444-555555555555"
        thread = {
            "id": _ATTACH_THREAD_ID,
            "execution_lane": "pinned",
            "status": "created",
            "agent_id": _ATTACH_AGENT_ID,
            "runtime_generation": _ATTACH_RUNTIME_GENERATION,
            "runtime_attach_token": _ATTACH_TOKEN,
            "runtime_retirement_token": None,
            "user_id": None,
            "metadata": {"datasource_ids": []},
        }

        _FakeAsyncClient.response_status = 500
        with (
            patch.object(
                orch_main.postgres_db,
                "get_thread",
                AsyncMock(return_value=thread),
            ),
            patch.object(
                orch_main,
                "_thread_project_ids",
                AsyncMock(return_value=[]),
            ),
            patch.object(
                orch_main.postgres_db,
                "resolve_datasources_for_thread",
                AsyncMock(return_value=[]),
            ),
            patch.object(orch_main, "_is_experts_db_enabled", return_value=False),
            patch.object(orch_main.httpx, "AsyncClient", _FakeAsyncClient),
        ):
            ok = await orch_main._send_session_attach(
                {
                    "id": _ATTACH_AGENT_ID,
                    "pod_ip": "10.0.0.1",
                    "pod_port": 8001,
                },
                _ATTACH_THREAD_ID,
                {},
                [],
                datasources=[{"type": "kb", "datasource_id": stale_id}],
                config_name="persistent_defaults",
            )

        assert ok is True
        assert _FakeAsyncClient.calls[0]["json"]["datasources"] is None

    def test_warm_resume_uses_canonical_thread_mount_projects(self):
        import inspect

        source = inspect.getsource(orch_main.thread_resume_operations.resume_thread)
        assert "pids = await _thread_project_ids(tid)" in source
        assert 'pids = thread.get("project_ids")' not in source


class TestAttachRoutesForwardConfigName:
    """Hole B: BOTH /session/attach routes must forward config_name.

    The job pool runs the dual app, dedicated session pods run the
    persistent app — each registers its own /session/attach. The first
    live verify missed the dual route (it answered 200 and silently
    dropped config_name), so pin both at source level.
    """

    def test_both_attach_routes_forward_config_name(self):
        import inspect

        import agent.api.dual_app as dual_app
        import agent.api.persistent_app as papp

        assert 'config_name=request.get("config_name")' in inspect.getsource(dual_app)
        assert '"config_name": request.get("config_name")' in inspect.getsource(
            papp._admit_pool_session_attach
        )

    def test_dual_detach_uses_rest_detach_reason(self):
        """Both detach routes must terminate with the documented
        "rest_detach" reason — the dual route used the "legacy" shim,
        which broke the greppable Terminate(rest_detach) signal."""
        import inspect

        import agent.api.dual_app as dual_app

        assert '_terminate_session("rest_detach")' in inspect.getsource(dual_app)


class TestLoadExpertConfig:
    """Hole B, agent side: the named config resolves with its own pipeline."""

    def test_persistent_name_yields_persistent_pipeline(self):
        cfg = _load_expert_config("persistent_defaults")
        assert cfg.memory.pipeline.writers == [
            "persistent_interval_extractor",
            "pre_compaction_extractor",
            "teardown_extractor",
        ]

    def test_worker_name_yields_worker_pipeline(self):
        cfg = _load_expert_config("defaults")
        assert "teardown_extractor" not in cfg.memory.pipeline.writers
        assert "interval_extractor" in cfg.memory.pipeline.writers

    def test_unknown_name_raises(self):
        with pytest.raises(Exception):
            _load_expert_config("no-such-config-xyz")


class TestDetachAgentSession:
    """B11 k8s route: detach-then-delete preconditions and outcomes."""

    def _db(self, thread, agent_row):
        return SimpleNamespace(
            get_thread=AsyncMock(return_value=thread),
            fetchrow=AsyncMock(return_value=agent_row),
        )

    @pytest.mark.asyncio
    async def test_no_bound_agent_skips_without_http(self):
        db = self._db({"id": "t1", "agent_id": None}, None)
        with (
            patch.object(orch_main, "postgres_db", db),
            patch.object(orch_main.httpx, "AsyncClient", _FakeAsyncClient),
        ):
            assert await control_seams.detach_agent_session("t1") is False
        assert _FakeAsyncClient.calls == []

    @pytest.mark.asyncio
    async def test_non_session_agent_skips_without_http(self):
        """Offline/ready agents (the orphan-reaper case) must not stall."""
        db = self._db(
            {"id": "t1", "agent_id": "a1"},
            {"pod_ip": "10.0.0.2", "pod_port": 8001, "status": "ready"},
        )
        with (
            patch.object(orch_main, "postgres_db", db),
            patch.object(orch_main.httpx, "AsyncClient", _FakeAsyncClient),
        ):
            assert await control_seams.detach_agent_session("t1") is False
        assert _FakeAsyncClient.calls == []

    @pytest.mark.asyncio
    async def test_live_session_agent_detaches(self):
        thread = {
            "id": _ATTACH_THREAD_ID,
            "agent_id": _ATTACH_AGENT_ID,
            "execution_lane": "pinned",
            "status": "active",
            "runtime_generation": _ATTACH_RUNTIME_GENERATION,
            "runtime_attach_token": _ATTACH_TOKEN,
            "runtime_retirement_token": None,
        }
        db = self._db(
            thread,
            {"pod_ip": "10.0.0.2", "pod_port": 8001, "status": "session"},
        )
        db.get_pinned_session_binding = AsyncMock(
            return_value=SimpleNamespace(
                agent_status="session",
                pod_namespace="srw",
                pod_ip="10.0.0.2",
                pod_port=8001,
                session_identity_fingerprint="sha256:" + "a" * 64,
            )
        )
        with (
            patch.object(orch_main, "postgres_db", db),
            patch.object(orch_main.httpx, "AsyncClient", _FakeAsyncClient),
        ):
            assert await control_seams.detach_agent_session(_ATTACH_THREAD_ID) is True
        assert _FakeAsyncClient.calls[0]["url"] == "http://10.0.0.2:8001/session/detach"

    @pytest.mark.asyncio
    async def test_http_failure_is_contained(self):
        """Teardown must proceed even when the agent is unreachable."""
        _FakeAsyncClient.raise_on_post = ConnectionError("refused")
        db = self._db(
            {"id": "t1", "agent_id": "a1"},
            {"pod_ip": "10.0.0.2", "pod_port": 8001, "status": "session"},
        )
        with (
            patch.object(orch_main, "postgres_db", db),
            patch.object(orch_main.httpx, "AsyncClient", _FakeAsyncClient),
        ):
            assert await control_seams.detach_agent_session("t1") is False

    @pytest.mark.asyncio
    async def test_release_runs_detach_before_workspace_cleanup(self):
        """Ordering pin: the agent must get its terminate (git push needs
        the workspace alive) before the workspace archive/cleanup step."""
        order: list = []

        async def _detach(thread_id, timeout=150.0, **_kwargs):
            order.append("detach")
            return True

        async def _archive(thread_id, entity_type, *, reclaim_volume=True, **_kwargs):
            order.append("workspace")

        provisioner = SimpleNamespace(
            is_available=True,
            delete_agent_pod_by_thread=AsyncMock(
                side_effect=lambda tid: order.append("pod") or True
            ),
        )
        with (
            patch.object(
                orch_main.thread_retirement_operations, "detach_agent_session", _detach
            ),
            patch.object(
                orch_main.thread_retirement_operations,
                "archive_and_cleanup_workspace",
                _archive,
            ),
            patch.object(orch_main, "agent_provisioner", provisioner),
        ):
            await control_seams.release_thread_resources("t1")
        assert order == ["detach", "workspace", "pod"]


class TestEndedSessionKeepsItsVolume:
    """THE session-durability invariant, end to end: **an ended thread is
    RESUMABLE**, so ending or idling a session must never delete its workspace
    volume. Only a hard delete reclaims it.

    Now that session workspaces are PVC-backed, every teardown path that used to
    be a harmless pod delete is a potential data-destruction path. Three callers
    reach the same teardown for entirely different reasons: the user-facing
    DELETE (soft end vs ?permanent=true), the agent's idle-archive after 30 idle
    minutes, and the stale-agent sweeper when a pod merely goes offline. Only
    the first of those, with ?permanent=true, means "destroy this user's data" —
    and ``resume_thread`` requires exactly the 'ended' status the other two
    write. The whole chain is pinned here (end_thread →
    _release_thread_resources → _archive_and_cleanup_workspace →
    release_workspace) because a default flipped at any one link silently
    deletes workspaces at the far end.
    """

    thread_id = "50000000-0000-4000-8000-000000000001"
    runtime_generation = "50000000-0000-4000-8000-000000000002"
    retirement_token = "50000000-0000-4000-8000-000000000003"
    workspace_generation = "50000000-0000-4000-8000-000000000004"
    workspace_runtime = "50000000-0000-4000-8000-000000000005"
    workspace_pvc_uid = "50000000-0000-4000-8000-000000000006"

    def _thread(self):
        return {
            "id": self.thread_id,
            "user_id": "u1",
            "agent_id": None,
            "execution_lane": "pinned",
            "status": "created",
            "runtime_generation": self.runtime_generation,
            "runtime_attach_token": None,
            "runtime_retirement_token": None,
            "metadata": {
                "workspace_container": {
                    "status": "ready",
                    "provisioner": "k8s",
                    "_canvas_workspace_generation": self.workspace_generation,
                    "_runtime_incarnation": self.workspace_runtime,
                },
                "_workspace_binding": {
                    "generation": self.workspace_generation,
                    "kind": "remote",
                    "backing_id": f"k8s-pvc:{self.workspace_pvc_uid}",
                },
            },
        }

    async def _run_end_thread(
        self, *, permanent: bool, observed_pvc_uid: str | None = "captured"
    ):
        """Drive the real DELETE endpoint, capturing the reclaim decision.

        Returns ``(result, release_workspace, db)`` so the assertion reaches
        the exact Kubernetes actuator that owns PVC retention/deletion.
        """
        thread = self._thread()
        retirement = {
            "state": "pending",
            "token": self.retirement_token,
            "generation": self.runtime_generation,
            "permanent": permanent,
            "authorized_at": None,
            "context": {
                "thread_id": self.thread_id,
                "generation": self.runtime_generation,
                "settle_status": "ended",
                "runtime_authority_exposed": False,
                "agent_id": None,
                "runtime_attach_token": None,
                "agent": {},
                "agent_pod": {},
                "route": {},
                "workspace_container": thread["metadata"]["workspace_container"],
                "workspace_binding": thread["metadata"]["_workspace_binding"],
                "workspace_backend": "sandbox",
                "vm": {},
                "protected_ro": {},
            },
        }

        db = SimpleNamespace(
            # No admitted pinned idle stop: permanent End has nothing to join
            # and the durable idle-operation probe finds no physical-stop row.
            reserve_pinned_thread_idle_terminal_end=AsyncMock(return_value="none"),
            fetchrow=AsyncMock(return_value=None),
            begin_pinned_thread_retirement=AsyncMock(return_value=retirement),
            authorize_pinned_thread_retirement=AsyncMock(return_value=True),
            settle_pinned_thread_retirement=AsyncMock(return_value=True),
            delete_thread=AsyncMock(),
            get_thread=AsyncMock(return_value=thread),
            merge_thread_config_override=AsyncMock(),
            try_thread_advisory_lock=MagicMock(
                side_effect=_owned_workspace_lifecycle_lock
            ),
            pinned_retirement_external_cleanup_complete=AsyncMock(return_value=False),
            clear_pinned_retirement_physical_runtime_endpoint=AsyncMock(
                return_value=True
            ),
        )
        teardown_identity = SimpleNamespace(
            pod_uid=self.workspace_runtime,
            pvc_uid=(
                self.workspace_pvc_uid
                if observed_pvc_uid == "captured"
                else observed_pvc_uid
            ),
        )
        release_workspace = AsyncMock(return_value=True)
        provisioner = SimpleNamespace(
            is_available=True,
            capture_workspace_teardown_identity=AsyncMock(
                return_value=teardown_identity
            ),
            release_workspace=release_workspace,
        )
        with (
            patch.object(
                orch_main,
                "require_thread_owner",
                AsyncMock(return_value=({"sub": "u1"}, thread)),
            ),
            patch.object(
                orch_main.thread_retirement_operations,
                "thread_turn_in_flight",
                AsyncMock(return_value=False),
            ),
            patch.object(orch_main, "_conclude_conference_if_any", AsyncMock()),
            patch.object(orch_main, "postgres_db", db),
            patch.object(
                PinnedRetirementOperations,
                "pinned_retirement_is_current",
                AsyncMock(return_value=True),
            ),
            patch.object(
                PinnedRetirementOperations,
                "_pinned_retirement_is_current",
                AsyncMock(return_value=True),
            ),
            patch.object(
                PinnedRetirementOperations,
                "_reconcile_workspace_provision_intent_for_retirement",
                AsyncMock(return_value=False),
            ),
            patch.object(
                PinnedRetirementOperations,
                "_stop_captured_retirement_agent",
                AsyncMock(),
            ),
            patch.object(
                PinnedRetirementOperations,
                "_reconcile_agent_workspace_claim_for_retirement",
                AsyncMock(),
            ),
            patch.object(
                orch_main,
                "session_router",
                SimpleNamespace(teardown_route=AsyncMock(return_value=True)),
            ),
            patch.object(orch_main, "container_provisioner", provisioner),
            patch.object(
                orch_main, "snapshot_service", SimpleNamespace(is_available=False)
            ),
            patch.object(
                orch_main, "gitea_client", SimpleNamespace(is_initialized=False)
            ),
        ):
            result = await control_seams.end_thread(
                self.thread_id,
                SimpleNamespace(),
                permanent=permanent,
                force=True,
            )
        return result, release_workspace, db

    @pytest.mark.asyncio
    async def test_soft_end_keeps_the_workspace_volume(self):
        """A soft end leaves the thread in 'ended', which /resume accepts — so
        reclaiming here would hand the user an empty workspace on a session they
        never deleted. Same reasoning that already keeps the Gitea repo."""
        result, release_workspace, db = await self._run_end_thread(permanent=False)

        assert result == {"status": "ended"}
        assert release_workspace.await_args.kwargs["reclaim_volume"] is False
        db.settle_pinned_thread_retirement.assert_awaited_once_with(
            self.thread_id,
            token=self.retirement_token,
            generation=self.runtime_generation,
            final_status="ended",
            staged_event=None,
        )
        db.delete_thread.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_permanent_delete_reclaims_the_workspace_volume(self):
        """The other half — without this the invariant would be trivially
        satisfiable by never reclaiming anything, and every deleted session
        would leak a 10Gi volume until the namespace quota rejects new ones."""
        result, release_workspace, db = await self._run_end_thread(permanent=True)

        assert result == {"status": "deleted"}
        assert release_workspace.await_args.kwargs["reclaim_volume"] is True
        db.delete_thread.assert_awaited_once_with(
            self.thread_id,
            expected_runtime_retirement_token=self.retirement_token,
            expected_runtime_generation=self.runtime_generation,
        )

    @pytest.mark.asyncio
    async def test_permanent_retry_accepts_captured_workspace_volume_already_absent(
        self,
    ):
        """A lost response after exact PVC deletion must replay to completion."""

        result, release_workspace, db = await self._run_end_thread(
            permanent=True,
            observed_pvc_uid=None,
        )

        assert result == {"status": "deleted"}
        assert release_workspace.await_args.kwargs["teardown_identity"].pvc_uid is None
        db.delete_thread.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_permanent_retry_refuses_replacement_workspace_volume(self):
        """A present same-name PVC with another UID is never adopted."""

        with pytest.raises(HTTPException) as exc:
            await self._run_end_thread(
                permanent=True,
                observed_pvc_uid="70000000-0000-4000-8000-000000000007",
            )

        assert exc.value.status_code == 503

    @pytest.mark.asyncio
    async def test_stateless_end_refuses_unattested_workspace_before_cleanup(self):
        """Legacy rows cannot enter terminal cleanup without runtime authority."""
        thread = {
            "id": "t1",
            "user_id": "u1",
            "agent_id": None,
            "execution_lane": "stateless",
            "metadata": {
                "config_override": {"workspace": {"backend": "sandbox"}},
                "workspace_container": {
                    "status": "ready",
                    "provisioner": "k8s",
                },
            },
        }

        db = SimpleNamespace(
            begin_stateless_thread_workspace_retirement=AsyncMock(),
            get_thread=AsyncMock(return_value=thread),
            stateless_session_workspace_ensure_lock=MagicMock(
                side_effect=_owned_workspace_lifecycle_lock
            ),
        )
        with (
            patch.object(
                orch_main,
                "require_thread_owner",
                AsyncMock(return_value=({"sub": "u1"}, thread)),
            ),
            patch.object(orch_main, "_conclude_conference_if_any", AsyncMock()),
            patch.object(
                orch_main.thread_retirement_operations,
                "release_thread_resources",
                AsyncMock(),
            ) as release,
            patch.object(orch_main, "postgres_db", db),
            patch.object(
                orch_main, "snapshot_service", SimpleNamespace(is_available=False)
            ),
            patch.object(
                orch_main, "gitea_client", SimpleNamespace(is_initialized=False)
            ),
            pytest.raises(HTTPException) as exc,
        ):
            await control_seams.end_thread(
                "t1", SimpleNamespace(), permanent=False, force=True
            )

        assert exc.value.status_code == 409
        db.begin_stateless_thread_workspace_retirement.assert_not_awaited()
        release.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_waiting_duplicate_end_does_not_retire_freshly_resumed_thread(self):
        initial = {
            "id": "t1",
            "user_id": "u1",
            "agent_id": None,
            "status": "ended",
            "execution_lane": "stateless",
            "metadata": {
                "config_override": {"workspace": {"backend": "sandbox"}},
                "workspace_container": {
                    "status": "ready",
                    "provisioner": "k8s",
                },
            },
        }
        resumed = {**initial, "status": "created"}
        begin = AsyncMock(return_value=True)
        release = AsyncMock()
        db = SimpleNamespace(
            begin_stateless_thread_workspace_retirement=begin,
            finish_stateless_thread_workspace_retirement=AsyncMock(return_value=True),
            get_thread=AsyncMock(return_value=resumed),
            merge_thread_config_override=AsyncMock(),
            stateless_session_workspace_ensure_lock=MagicMock(
                side_effect=_owned_workspace_lifecycle_lock
            ),
        )
        with (
            patch.object(
                orch_main,
                "require_thread_owner",
                AsyncMock(return_value=({"sub": "u1"}, initial)),
            ),
            patch.object(
                orch_main.thread_retirement_operations,
                "thread_turn_in_flight",
                AsyncMock(return_value=False),
            ),
            patch.object(orch_main, "_conclude_conference_if_any", AsyncMock()),
            patch.object(
                orch_main.thread_retirement_operations,
                "release_thread_resources",
                release,
            ),
            patch.object(orch_main, "postgres_db", db),
            pytest.raises(HTTPException) as exc,
        ):
            await control_seams.end_thread(
                "t1", SimpleNamespace(), permanent=False, force=True
            )

        assert exc.value.status_code == 409
        begin.assert_not_awaited()
        release.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_release_defaults_to_keeping_the_volume(self):
        """The default is the safe one, so a caller that forgets the argument
        leaks a volume (recoverable) rather than destroying one (not).

        The two live callers both pass it explicitly today — end_thread forwards
        ?permanent, the stale-agent sweeper hardcodes False — so this pins the
        fallback the next caller will inherit. It matters because that caller is
        most likely another "the agent went offline" path, which says nothing
        about the user's intent to keep their files."""
        captured: dict = {}

        async def _archive(thread_id, entity_type, *, reclaim_volume=True, **_kwargs):
            captured["entity_type"] = entity_type
            captured["reclaim_volume"] = reclaim_volume

        with (
            patch.object(
                orch_main.thread_retirement_operations,
                "detach_agent_session",
                AsyncMock(),
            ),
            patch.object(
                orch_main.thread_retirement_operations,
                "archive_and_cleanup_workspace",
                _archive,
            ),
            patch.object(
                orch_main, "agent_provisioner", SimpleNamespace(is_available=False)
            ),
            patch.object(
                orch_main, "persistent_provisioner", SimpleNamespace(is_available=False)
            ),
        ):
            await control_seams.release_thread_resources("t1")

        assert captured == {"entity_type": "threads", "reclaim_volume": False}

    @pytest.mark.asyncio
    async def test_release_forwards_an_explicit_reclaim(self):
        captured: dict = {}

        async def _archive(thread_id, entity_type, *, reclaim_volume=True, **_kwargs):
            captured["reclaim_volume"] = reclaim_volume

        with (
            patch.object(
                orch_main.thread_retirement_operations,
                "detach_agent_session",
                AsyncMock(),
            ),
            patch.object(
                orch_main.thread_retirement_operations,
                "archive_and_cleanup_workspace",
                _archive,
            ),
            patch.object(
                orch_main, "agent_provisioner", SimpleNamespace(is_available=False)
            ),
            patch.object(
                orch_main, "persistent_provisioner", SimpleNamespace(is_available=False)
            ),
        ):
            await control_seams.release_thread_resources("t1", reclaim_volume=True)

        assert captured["reclaim_volume"] is True

    @pytest.mark.asyncio
    async def test_archive_forwards_the_decision_to_the_provisioner(self):
        """The last link: the flag has to survive all the way to the k8s call
        that actually deletes the PVC, keyed on the session owner."""
        # Same import path main.py uses — `services.*` and `orchestrator.services.*`
        # load as distinct modules, and the dataclass equality below needs the
        # class identity to match.
        from orchestrator.services.workspace_lifecycle import WorkspaceOwner

        for reclaim in (False, True):
            thread = {
                "id": "t1",
                "metadata": {"workspace_container": {"status": "ready"}},
            }
            provisioner = SimpleNamespace(
                is_available=True,
                capture_terminal_workspace_identity=AsyncMock(
                    return_value=SimpleNamespace(
                        pod_uid="pod-uid", pvc_uid="pvc-uid", service_uid="svc-uid"
                    )
                ),
                release_workspace=AsyncMock(return_value=True),
            )
            with (
                patch.object(
                    orch_main,
                    "postgres_db",
                    SimpleNamespace(get_thread=AsyncMock(return_value=thread)),
                ),
                patch.object(orch_main, "container_provisioner", provisioner),
                patch.object(
                    orch_main, "vm_provisioner", SimpleNamespace(is_available=False)
                ),
            ):
                await control_seams.archive_and_cleanup_workspace(
                    "t1", entity_type="threads", reclaim_volume=reclaim
                )

            provisioner.release_workspace.assert_awaited_once_with(
                WorkspaceOwner.session("t1"),
                reclaim_volume=reclaim,
                teardown_identity=(
                    provisioner.capture_terminal_workspace_identity.return_value
                ),
                strict=True,
            )

    @pytest.mark.asyncio
    async def test_archive_refuses_false_kubernetes_teardown_result(self):
        from orchestrator.services.workspace_lifecycle import WorkspaceOwner

        thread = {
            "id": "t1",
            "metadata": {"workspace_container": {"status": "ready"}},
        }
        identity = SimpleNamespace(
            pod_uid="pod-uid", pvc_uid="pvc-uid", service_uid="svc-uid"
        )
        provisioner = SimpleNamespace(
            is_available=True,
            capture_terminal_workspace_identity=AsyncMock(return_value=identity),
            release_workspace=AsyncMock(return_value=False),
        )
        with (
            patch.object(
                orch_main,
                "postgres_db",
                SimpleNamespace(get_thread=AsyncMock(return_value=thread)),
            ),
            patch.object(orch_main, "container_provisioner", provisioner),
            patch.object(
                orch_main, "vm_provisioner", SimpleNamespace(is_available=False)
            ),
        ):
            with pytest.raises(RuntimeError, match="exact teardown is incomplete"):
                await control_seams.archive_and_cleanup_workspace(
                    "t1", entity_type="threads", reclaim_volume=False
                )

        provisioner.release_workspace.assert_awaited_once_with(
            WorkspaceOwner.session("t1"),
            reclaim_volume=False,
            teardown_identity=identity,
            strict=True,
        )
