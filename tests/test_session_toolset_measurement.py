"""The tool-groups read serves the AGENT's answer, and says when it cannot.

D6: the resolved toolset comes from the agent, not from an orchestrator-side
re-implementation. These tests hold the two halves of that:

* when a pod has reported, its answer wins outright — including where it
  DISAGREES with the merged config, which is the whole point (the runtime
  injection layer is invisible to a config-only view);
* when no pod has reported, the answer is labelled a prediction and carries a
  reason, because a forecast served as a measurement is the original defect at
  a new seam.
"""

import asyncio
import json
from contextlib import ExitStack
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.routers import thread_session
from orchestrator.schemas.thread_session import ToolGroupPreviewRequest
from shared.runtime.core.session_tool_overrides import SESSION_TOOL_OVERRIDE_NAMES
from shared.runtime.core.tool_report import (
    MEASURED_ORIGINS,
    ORIGIN_AGENT,
    ORIGIN_AGENT_PARTIAL,
    ORIGIN_PREDICTION,
    REPORT_VERSION,
    STATE_ON,
    STATE_UNAVAILABLE,
    report_categories,
)
from orchestrator.application import transport as transport_composition

_AGENT_ROW = {"id": "agent-1", "pod_ip": "10.0.0.9", "pod_port": 8001}

#: The tool view's own module-level collaborators (deployment gate, policy
#: merge) are looked up here at call time, so they are patched here.
_TOOL_VIEW = "orchestrator.services.session_tool_view"


@pytest.fixture(autouse=True)
def account_workspace_defaults(fake_db):
    fake_db.get_user_settings.return_value = {}
    fake_db.resolve_default_for_capability.return_value = None


def _approved(user):
    """A stand-in with the REAL ``require_approved_user(request, db)`` arity.

    An ``AsyncMock`` swallows any signature, which is how the preview route
    shipped calling it with one argument and passed every unit test — the live
    gate caught it. A plain function does not.
    """

    async def _require_approved_user(request, db):
        return user

    return _require_approved_user


def _thread(metadata=None, agent_id=None, config_name=None):
    from tests.conftest import _TID_A, _UID_A

    return {
        "id": _TID_A,
        "user_id": _UID_A,
        "title": "thread A",
        "config_name": config_name,
        "project_id": None,
        "agent_id": agent_id,
        "metadata": metadata or {},
    }


class _Response:
    def __init__(self, status_code=200, payload=None, raises=False):
        self.status_code = status_code
        self._payload = payload
        self._raises = raises

    def json(self):
        if self._raises:
            raise ValueError("not json")
        return self._payload


class _Client:
    """Minimal ``httpx.AsyncClient`` stand-in with a per-path route table."""

    def __init__(self, routes, calls):
        self._routes = routes
        self._calls = calls

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def get(self, url):
        self._calls.append(url)
        for suffix, result in self._routes.items():
            if url.endswith(suffix):
                if isinstance(result, Exception):
                    raise result
                return result
        return _Response(status_code=404, payload={})


def _agent_http(routes):
    """Patch the toolset probe's ``httpx.AsyncClient``; return the call list."""
    calls: list[str] = []
    return patch(
        "orchestrator.services.agent_toolset_probe.httpx.AsyncClient",
        MagicMock(side_effect=lambda **kw: _Client(routes, calls)),
    ), calls


def _app_dependencies():
    """The application's own dependency composition, rebuilt per call.

    ``main._thread_session_dependencies()`` reads the app-owned collaborators
    (``postgres_db``, ``require_approved_user``, ``_user_experts_enabled``,
    ``_resolve_runner_grants``, ...) at call time, so patches of those still
    steer the extracted route. Build it INSIDE the patch context.
    """
    import orchestrator.main as orch_main

    return transport_composition.thread_session_dependencies(
        orch_main.app.state.resources
    )


async def _get_tool_groups(thread_id, request):
    return await thread_session.get_thread_tool_groups(
        thread_id, request, dependencies=_app_dependencies()
    )


async def _preview_tool_groups(body, request):
    return await thread_session.preview_tool_groups(
        body, request, dependencies=_app_dependencies()
    )


def _report(categories, *, observed_at="2026-08-02T12:00:00Z", backend=None):
    tools = [n for names in categories.values() for n in names]
    return {
        "attached": True,
        "thread_id": "t",
        "report": {
            "version": REPORT_VERSION,
            "thread_id": "t",
            "observed_at": observed_at,
            "backend": backend,
            "tools": tools,
            "categories": categories,
        },
    }


async def _call(user, db, thread_row, fake_request, *, routes=None, grants=None):
    """Drive the endpoint with a fake agent transport and explicit grants."""
    db.get_thread = AsyncMock(return_value=thread_row)
    db.get_agent = AsyncMock(return_value=_AGENT_ROW)
    http_patch, calls = _agent_http(routes or {})
    with ExitStack() as stack:
        stack.enter_context(
            patch("orchestrator.security.auth.require_approved_user", _approved(user))
        )
        stack.enter_context(
            patch(
                "orchestrator.security.access.require_approved_user",
                AsyncMock(return_value=user),
            )
        )
        stack.enter_context(
            patch("orchestrator.main.app.state.resources.postgres_db", db)
        )
        stack.enter_context(
            patch(f"{_TOOL_VIEW}.is_experts_db_enabled", MagicMock(return_value=True))
        )
        stack.enter_context(
            patch(
                "orchestrator.services.grant_enforcement.user_experts_enabled",
                AsyncMock(return_value=True),
            )
        )
        stack.enter_context(
            patch(
                "orchestrator.services.grant_enforcement.resolve_runner_grants",
                AsyncMock(return_value=grants),
            )
        )
        stack.enter_context(http_patch)
        result = await _get_tool_groups(str(thread_row["id"]), fake_request)
    return result, calls


# =============================================================================
# Measured
# =============================================================================
class TestMeasuredAnswer:
    @pytest.mark.asyncio
    async def test_agent_answer_is_labelled_and_timestamped(
        self, user_a, fake_db, fake_request
    ):
        result, _ = await _call(
            user_a,
            fake_db,
            _thread(agent_id="agent-1"),
            fake_request,
            routes={
                "/session/toolset": _Response(
                    payload=_report({"research": ["web_search"]})
                )
            },
        )
        assert result["origin"] == ORIGIN_AGENT
        assert result["observed_at"] == "2026-08-02T12:00:00Z"
        assert result["prediction_reason"] is None
        assert result["degraded_reason"] is None
        assert result["categories"]["research"]["state"] == STATE_ON

    @pytest.mark.asyncio
    async def test_the_agent_overrules_the_merged_config(
        self, user_a, fake_db, fake_request
    ):
        """THE regression this task exists for.

        ``session_base`` ships ``tools.job_control: []``, so every
        config-derived view says job control is off. If the agent bound
        job-control tools anyway — which the runtime injection layer does whenever
        the group is enabled — the endpoint must say ON.
        """
        result, _ = await _call(
            user_a,
            fake_db,
            _thread(agent_id="agent-1"),
            fake_request,
            routes={
                "/session/toolset": _Response(
                    payload=_report({"job_control": ["create_job"]})
                )
            },
        )
        assert result["tool_groups"]["job_control"] is True
        assert result["categories"]["job_control"]["state"] == STATE_ON
        # And the disagreement is visible rather than hidden.
        assert result["categories"]["job_control"]["configured"] == []

    @pytest.mark.asyncio
    async def test_tool_groups_is_derived_from_the_measurement(
        self, user_a, fake_db, fake_request
    ):
        result, _ = await _call(
            user_a,
            fake_db,
            _thread(agent_id="agent-1"),
            fake_request,
            routes={
                "/session/toolset": _Response(
                    payload=_report({"canvas": ["set_canvas"]})
                )
            },
        )
        assert set(result["tool_groups"]) == set(SESSION_TOOL_OVERRIDE_NAMES)
        assert result["tool_groups"]["canvas"] is True
        assert result["tool_groups"]["workflows"] is False

    @pytest.mark.asyncio
    async def test_every_category_is_answered_not_just_the_closed_four(
        self, user_a, fake_db, fake_request
    ):
        result, _ = await _call(
            user_a,
            fake_db,
            _thread(agent_id="agent-1"),
            fake_request,
            routes={"/session/toolset": _Response(payload=_report({}))},
        )
        assert set(report_categories()) <= set(result["categories"])
        assert len(result["categories"]) >= 24


class TestBackendCapabilitiesRideTheReport:
    """The tier gate is a fact only the agent holds; carry it, do not re-derive.

    The live gate on k3d saw a no-shell session bind 40 tools where the
    config-only prediction said 60 — 14 of the 20 missing were exactly the
    ``browser_direct`` + ``git`` names ``filter_tools_by_backend`` drops. With
    the agent's reported capabilities in hand those read as *unavailable, wrong
    workspace tier* instead of a bare "off" the user would try to tick.
    """

    @pytest.mark.asyncio
    async def test_a_no_shell_tier_reads_unavailable_not_off(
        self, user_a, fake_db, fake_request
    ):
        result, _ = await _call(
            user_a,
            fake_db,
            _thread(agent_id="agent-1"),
            fake_request,
            routes={
                "/session/toolset": _Response(
                    payload=_report(
                        {},
                        backend={
                            "supports_shell": False,
                            "supports_file_tools": True,
                            "supports_canvas_presentation": True,
                        },
                    )
                )
            },
            grants=None,
        )
        assert result["backend"]["supports_shell"] is False
        for category in ("shell", "git", "browser_direct"):
            entry = result["categories"][category]
            assert entry["state"] == STATE_UNAVAILABLE, category
            assert entry["decided_by"] == "backend", category
            assert "workspace tier" in entry["reason"], category

    @pytest.mark.asyncio
    async def test_a_shell_capable_tier_imposes_no_backend_reason(
        self, user_a, fake_db, fake_request
    ):
        result, _ = await _call(
            user_a,
            fake_db,
            _thread(agent_id="agent-1"),
            fake_request,
            routes={
                "/session/toolset": _Response(
                    payload=_report(
                        {},
                        backend={
                            "supports_shell": True,
                            "supports_file_tools": True,
                            "supports_canvas_presentation": True,
                        },
                    )
                )
            },
            grants=None,
        )
        # The tier does not gate it...
        assert result["categories"]["git"]["decided_by"] != "backend"
        assert "workspace tier" not in (result["categories"]["git"]["reason"] or "")
        # ...but session_base grants git tools the agent did not bind, and that
        # shortfall is still reported rather than smoothed into a plain "off".
        assert result["categories"]["git"]["state"] == STATE_UNAVAILABLE

    @pytest.mark.asyncio
    async def test_a_prediction_reports_no_backend(self, user_a, fake_db, fake_request):
        """No workspace is provisioned yet, so claiming a tier would be a guess."""
        result, _ = await _call(user_a, fake_db, _thread(), fake_request)
        assert result["backend"] is None


# =============================================================================
# Prediction — and saying so
# =============================================================================
class TestPredictionIsLabelled:
    @pytest.mark.asyncio
    async def test_no_agent_means_prediction_with_a_reason(
        self, user_a, fake_db, fake_request
    ):
        result, calls = await _call(user_a, fake_db, _thread(), fake_request)
        assert result["origin"] == ORIGIN_PREDICTION
        assert result["observed_at"] is None
        assert "no agent" in result["prediction_reason"]
        assert calls == [], "must not probe when nothing is bound"

    @pytest.mark.asyncio
    async def test_unreachable_agent_degrades_to_a_labelled_prediction(
        self, user_a, fake_db, fake_request
    ):
        result, _ = await _call(
            user_a,
            fake_db,
            _thread(agent_id="agent-1"),
            fake_request,
            routes={"/session/toolset": RuntimeError("connection refused")},
        )
        assert result["origin"] == ORIGIN_PREDICTION
        assert "did not answer" in result["prediction_reason"]

    @pytest.mark.asyncio
    async def test_detached_agent_is_not_an_empty_measurement(
        self, user_a, fake_db, fake_request
    ):
        """A pod with no session bound has nothing to measure — say that."""
        result, _ = await _call(
            user_a,
            fake_db,
            _thread(agent_id="agent-1"),
            fake_request,
            routes={
                "/session/toolset": _Response(
                    payload={"attached": False, "thread_id": None, "report": None}
                )
            },
        )
        assert result["origin"] == ORIGIN_PREDICTION
        assert "not attached" in result["prediction_reason"]

    @pytest.mark.asyncio
    async def test_a_terminal_agent_row_is_not_probed_at_all(
        self, user_a, fake_db, fake_request
    ):
        """A dead pod's row keeps its ``pod_ip``, and the pane blocks on us."""
        fake_db.get_thread = AsyncMock(return_value=_thread(agent_id="agent-1"))
        fake_db.get_agent = AsyncMock(return_value={**_AGENT_ROW, "status": "offline"})
        http_patch, calls = _agent_http({})
        with (
            patch(
                "orchestrator.security.auth.require_approved_user", _approved(user_a)
            ),
            patch(
                "orchestrator.security.access.require_approved_user",
                AsyncMock(return_value=user_a),
            ),
            patch("orchestrator.main.app.state.resources.postgres_db", fake_db),
            patch(f"{_TOOL_VIEW}.is_experts_db_enabled", MagicMock(return_value=True)),
            patch(
                "orchestrator.services.grant_enforcement.user_experts_enabled",
                AsyncMock(return_value=True),
            ),
            patch(
                "orchestrator.services.grant_enforcement.resolve_runner_grants",
                AsyncMock(return_value=None),
            ),
            http_patch,
        ):
            result = await _get_tool_groups(str(_thread()["id"]), fake_request)

        assert result["origin"] == ORIGIN_PREDICTION
        assert "offline" in result["prediction_reason"]
        assert calls == [], "no HTTP to a pod that is known gone"

    @pytest.mark.asyncio
    async def test_a_freshly_attached_agent_is_still_probed(
        self, user_a, fake_db, fake_request
    ):
        """``ready`` persists for up to one heartbeat after attach.

        Gating the probe on ``status == "session"`` would report a prediction
        for the first minute of every session — the exact class of silently
        wrong answer this endpoint exists to remove.
        """
        fake_db.get_thread = AsyncMock(return_value=_thread(agent_id="agent-1"))
        fake_db.get_agent = AsyncMock(return_value={**_AGENT_ROW, "status": "ready"})
        http_patch, calls = _agent_http(
            {
                "/session/toolset": _Response(
                    payload=_report({"research": ["web_search"]})
                )
            }
        )
        with (
            patch(
                "orchestrator.security.auth.require_approved_user", _approved(user_a)
            ),
            patch(
                "orchestrator.security.access.require_approved_user",
                AsyncMock(return_value=user_a),
            ),
            patch("orchestrator.main.app.state.resources.postgres_db", fake_db),
            patch(f"{_TOOL_VIEW}.is_experts_db_enabled", MagicMock(return_value=True)),
            patch(
                "orchestrator.services.grant_enforcement.user_experts_enabled",
                AsyncMock(return_value=True),
            ),
            patch(
                "orchestrator.services.grant_enforcement.resolve_runner_grants",
                AsyncMock(return_value=None),
            ),
            http_patch,
        ):
            result = await _get_tool_groups(str(_thread()["id"]), fake_request)

        assert result["origin"] == ORIGIN_AGENT
        assert calls

    @pytest.mark.asyncio
    async def test_unreadable_report_is_refused_not_half_read(
        self, user_a, fake_db, fake_request
    ):
        result, _ = await _call(
            user_a,
            fake_db,
            _thread(agent_id="agent-1"),
            fake_request,
            routes={
                "/session/toolset": _Response(
                    payload={
                        "attached": True,
                        "report": {"version": 999, "categories": {}},
                    }
                )
            },
        )
        assert result["origin"] == ORIGIN_PREDICTION
        assert "could not be read" in result["prediction_reason"]

    @pytest.mark.asyncio
    async def test_a_prediction_carries_no_configured_shadow(
        self, user_a, fake_db, fake_request
    ):
        """Nothing to compare against, so the field is absent by shape."""
        result, _ = await _call(user_a, fake_db, _thread(), fake_request)
        assert "configured" not in result["categories"]["research"]


class TestOlderAgentImageFallback:
    @pytest.mark.asyncio
    async def test_status_tools_are_a_measurement_but_a_DEGRADED_one(
        self, user_a, fake_db, fake_request
    ):
        """``/status`` already publishes the bound names; use them — and say so.

        An agent image predating ``/session/toolset`` is not a reason to
        downgrade to a guess. But it IS a reason to stop short of the full
        contract: no timestamp, no workspace capabilities, no agent-side
        categorisation. ``agent_partial`` is how a caller tells, because
        ``observed_at`` alone cannot — it is legitimately null here.
        """
        result, calls = await _call(
            user_a,
            fake_db,
            _thread(agent_id="agent-1"),
            fake_request,
            routes={
                "/session/toolset": _Response(status_code=404, payload={}),
                "/status": _Response(payload={"tools": ["web_search", "read_file"]}),
            },
        )
        assert result["origin"] == ORIGIN_AGENT_PARTIAL
        assert result["origin"] in MEASURED_ORIGINS
        assert result["observed_at"] is None, "no timestamp is available on this path"
        assert result["backend"] is None
        assert result["prediction_reason"] is None, "this IS measured"
        assert "predates" in result["degraded_reason"]
        assert result["categories"]["research"]["tools"] == ["web_search"]
        assert result["categories"]["workspace"]["tools"] == ["read_file"]
        assert any(u.endswith("/status") for u in calls)

    @pytest.mark.asyncio
    async def test_a_degraded_measurement_never_promises_an_unverifiable_on(
        self, user_a, fake_db, fake_request
    ):
        """The harm the third origin exists to bound.

        With no ``backend`` the composer cannot name the tier gate, so a naive
        renderer would show ``git``/``browser_direct`` as an ordinary unticked
        checkbox on a no-shell session — a MEASUREMENT saying "tick this and it
        will work" when it cannot. The bound-none rule catches it anyway.
        """
        result, _ = await _call(
            user_a,
            fake_db,
            _thread(agent_id="agent-1"),
            fake_request,
            routes={
                "/session/toolset": _Response(status_code=404, payload={}),
                "/status": _Response(payload={"tools": ["read_file"]}),
            },
            grants=None,
        )
        for category in ("git", "browser_direct"):
            entry = result["categories"][category]
            assert entry["state"] == STATE_UNAVAILABLE, category
            assert entry["settable"] is False, category
            assert entry["reason"], category

    @pytest.mark.asyncio
    async def test_neither_endpoint_answering_is_a_prediction(
        self, user_a, fake_db, fake_request
    ):
        result, _ = await _call(
            user_a,
            fake_db,
            _thread(agent_id="agent-1"),
            fake_request,
            routes={
                "/session/toolset": _Response(status_code=404, payload={}),
                "/status": _Response(status_code=500, payload={}),
            },
        )
        assert result["origin"] == ORIGIN_PREDICTION


# =============================================================================
# Unavailable, with a reason
# =============================================================================
class TestUnavailableCarriesAReason:
    @pytest.mark.asyncio
    async def test_missing_shell_grant_reads_unavailable_not_merely_off(
        self, user_a, fake_db, fake_request
    ):
        """D4/1.4: a silent unticked box is what left the user with no way in."""
        result, _ = await _call(
            user_a,
            fake_db,
            _thread(agent_id="agent-1"),
            fake_request,
            routes={"/session/toolset": _Response(payload=_report({}))},
            grants={"shell_tools": False, "browser": True, "datasource_tools": True},
        )
        shell = result["categories"]["shell"]
        assert shell["state"] == STATE_UNAVAILABLE
        assert shell["settable"] is False
        assert "shell_tools" in shell["reason"]

    @pytest.mark.asyncio
    async def test_admin_bypass_imposes_no_grant_restriction(
        self, user_a, fake_db, fake_request
    ):
        result, _ = await _call(
            user_a,
            fake_db,
            _thread(agent_id="agent-1"),
            fake_request,
            routes={"/session/toolset": _Response(payload=_report({}))},
            grants=None,
        )
        assert result["categories"]["shell"]["reason"] is None

    @pytest.mark.asyncio
    async def test_a_failed_grant_lookup_never_fabricates_a_denial(
        self, user_a, fake_db, fake_request
    ):
        """The PDP enforces; this only explains. Inventing a denial is its own
        D1 violation, so a lookup failure must read as no restriction."""
        fake_db.get_thread = AsyncMock(return_value=_thread())
        with (
            patch(
                "orchestrator.security.auth.require_approved_user", _approved(user_a)
            ),
            patch(
                "orchestrator.security.access.require_approved_user",
                AsyncMock(return_value=user_a),
            ),
            patch("orchestrator.main.app.state.resources.postgres_db", fake_db),
            patch(f"{_TOOL_VIEW}.is_experts_db_enabled", MagicMock(return_value=True)),
            patch(
                "orchestrator.services.grant_enforcement.user_experts_enabled",
                AsyncMock(return_value=True),
            ),
            patch(
                "orchestrator.services.grant_enforcement.resolve_runner_grants",
                AsyncMock(side_effect=RuntimeError("db down")),
            ),
        ):
            result = await _get_tool_groups(str(_thread()["id"]), fake_request)

        assert result["categories"]["shell"]["reason"] is None


# =============================================================================
# The error path keeps a measurement it already has
# =============================================================================
class TestResolveFailureWithAMeasurement:
    @pytest.mark.asyncio
    async def test_a_broken_resolve_does_not_discard_the_agent_answer(
        self, user_a, fake_db, fake_request
    ):
        """The resolve is only the EXPLANATION once a measurement exists.

        A session that is running has, by construction, already resolved — so
        a resolve failing now says nothing about what the agent holds.
        """
        fake_db.get_thread = AsyncMock(return_value=_thread(agent_id="agent-1"))
        fake_db.get_agent = AsyncMock(return_value=_AGENT_ROW)
        http_patch, _calls = _agent_http(
            {
                "/session/toolset": _Response(
                    payload=_report({"research": ["web_search"]})
                )
            }
        )
        with (
            patch(
                "orchestrator.security.auth.require_approved_user", _approved(user_a)
            ),
            patch(
                "orchestrator.security.access.require_approved_user",
                AsyncMock(return_value=user_a),
            ),
            patch("orchestrator.main.app.state.resources.postgres_db", fake_db),
            patch(f"{_TOOL_VIEW}.is_experts_db_enabled", MagicMock(return_value=True)),
            patch(
                "orchestrator.services.grant_enforcement.user_experts_enabled",
                AsyncMock(return_value=True),
            ),
            patch(
                "orchestrator.services.grant_enforcement.resolve_runner_grants",
                AsyncMock(return_value=None),
            ),
            patch(
                f"{_TOOL_VIEW}.merged_session_tool_policy",
                MagicMock(side_effect=RuntimeError("boom")),
            ),
            http_patch,
        ):
            result = await _get_tool_groups(str(_thread()["id"]), fake_request)

        assert result["source"] == "error"
        assert result["origin"] == ORIGIN_AGENT
        assert result["categories"]["research"]["state"] == STATE_ON


# =============================================================================
# The creation form's read — a prediction by construction
# =============================================================================
class TestPreviewEndpoint:
    @pytest.mark.asyncio
    async def test_always_a_prediction(self, user_a, fake_db, fake_request):
        with (
            patch(
                "orchestrator.security.auth.require_approved_user", _approved(user_a)
            ),
            patch("orchestrator.main.app.state.resources.postgres_db", fake_db),
            patch(
                "orchestrator.services.grant_enforcement.resolve_runner_grants",
                AsyncMock(return_value=None),
            ),
        ):
            result = await _preview_tool_groups(
                ToolGroupPreviewRequest(config_name="session_base"), fake_request
            )

        assert result["origin"] == ORIGIN_PREDICTION
        assert result["observed_at"] is None
        assert result["prediction_reason"]
        assert set(report_categories()) <= set(result["categories"])

    @pytest.mark.asyncio
    async def test_reflects_the_requested_override(self, user_a, fake_db, fake_request):
        with (
            patch(
                "orchestrator.security.auth.require_approved_user", _approved(user_a)
            ),
            patch("orchestrator.main.app.state.resources.postgres_db", fake_db),
            patch(
                "orchestrator.services.grant_enforcement.resolve_runner_grants",
                AsyncMock(return_value=None),
            ),
        ):
            on = await _preview_tool_groups(
                ToolGroupPreviewRequest(
                    config_name="session_base",
                    config_override={"tools": {"canvas": ["get_canvas", "set_canvas"]}},
                ),
                fake_request,
            )
            off = await _preview_tool_groups(
                ToolGroupPreviewRequest(
                    config_name="session_base",
                    config_override={"tools": {"canvas": []}},
                ),
                fake_request,
            )

        assert on["tool_groups"]["canvas"] is True
        assert on["categories"]["canvas"]["decided_by"] == "request"
        assert off["tool_groups"]["canvas"] is False

    @pytest.mark.asyncio
    async def test_worker_and_session_get_different_answers(
        self, user_a, fake_db, fake_request
    ):
        """A job preview must answer for ``worker_base``, not ``session_base``.

        What actually carries the difference is the BASE, and the distinction is
        worth stating because the first version of this test asserted it via
        ``expert_type`` and passed with ``expert_type`` hardcoded back to
        ``"session"`` — measured, not assumed: ``resolve_config`` with
        ``worker`` vs ``session`` yields byte-identical ``tools`` on both bases,
        because that argument selects PROMPT leaves and nothing in the toolset.
        See :meth:`test_expert_type_does_not_currently_shape_the_toolset`.

        So this pins the base routing, and the mutation that breaks it is
        defaulting a worker request to ``session_base``.
        """
        with (
            patch(
                "orchestrator.security.auth.require_approved_user", _approved(user_a)
            ),
            patch("orchestrator.main.app.state.resources.postgres_db", fake_db),
            patch(
                "orchestrator.services.grant_enforcement.resolve_runner_grants",
                AsyncMock(return_value=None),
            ),
        ):
            session = await _preview_tool_groups(
                ToolGroupPreviewRequest(expert_type="session"), fake_request
            )
            worker = await _preview_tool_groups(
                ToolGroupPreviewRequest(expert_type="worker"), fake_request
            )

        def bound(view):
            return {
                k: tuple(v["tools"])
                for k, v in view["categories"].items()
                if v["tools"]
            }

        assert bound(session) != bound(worker), (
            "worker and session predictions are identical, so expert_type is "
            "not reaching resolve_config"
        )
        # `core` is the clearest divider: worker jobs carry the phase/todo tools
        # and a session does not.
        assert worker["categories"]["core"]["tools"]
        assert not session["categories"]["core"]["tools"]
        assert "job" in worker["prediction_reason"]

    @pytest.mark.asyncio
    async def test_worker_defaults_its_base_without_a_config_name(
        self, user_a, fake_db, fake_request
    ):
        """No `config_name` on the job form must mean worker_base, not session_base."""
        seen: dict = {}

        def _spy(**kwargs):
            seen.update(kwargs)
            return {}, {}

        with (
            patch(
                "orchestrator.security.auth.require_approved_user", _approved(user_a)
            ),
            patch("orchestrator.main.app.state.resources.postgres_db", fake_db),
            patch(
                "orchestrator.services.grant_enforcement.resolve_runner_grants",
                AsyncMock(return_value=None),
            ),
            patch(
                f"{_TOOL_VIEW}.merged_session_tool_policy",
                MagicMock(side_effect=_spy),
            ),
        ):
            await _preview_tool_groups(
                ToolGroupPreviewRequest(expert_type="worker"), fake_request
            )

        assert seen["base_config_name"] == "worker_base"
        assert seen["expert_type"] == "worker"

    def test_expert_type_is_the_role_and_wins_over_a_root_base_name(self):
        """The honest scope of the ``expert_type`` parameter, post U1 WP2.

        Until the root split it was threaded through only because passing
        ``resolve_config`` the truthful value beat passing it a lie: the BASE
        made a job preview correct, and a tripwire pinned that ``expert_type``
        changed nothing. That day came: ``expert_type`` is now the ROLE the
        config resolves for. On a root name the role wins (``worker_base`` +
        ``session`` answers as the session base — the roots are one thing in
        different roles), and on a bundled expert the chain is re-rooted onto
        the role's overlay. The preview endpoint threads it on every call (the
        job form sends ``worker``), so its routing stays correct by
        construction; ``config_name`` alone would not be.
        """
        import os

        os.environ.setdefault("VECTOR_DB_URL", "postgresql://t:t@localhost:5432/t")
        from orchestrator.services.config_resolver import resolve_config

        def tools_for(base: str, expert_type: str) -> dict:
            capture: dict = {}
            resolve_config(
                base_config_name=base,
                base_defaults=None,
                expert_row=None,
                project_overrides=None,
                request_override=None,
                expert_type=expert_type,
                capture=capture,
            )
            merged = (capture.get("merged_fragment") or {}).get("tools") or {}
            return {k: tuple(v) for k, v in merged.items() if v}

        worker = tools_for("worker_base", "worker")
        session = tools_for("session_base", "session")
        assert worker != session
        assert "core" in worker and "core" not in session
        for base in ("worker_base", "session_base"):
            assert tools_for(base, "worker") == worker, (
                f"{base} + worker must answer as the worker base"
            )
            assert tools_for(base, "session") == session, (
                f"{base} + session must answer as the session base"
            )
        # A session-authored bundled expert previewed for a job gains the
        # worker's phase-loop group underneath its own declarations.
        assert "core" in tools_for("assistant", "worker")
        assert "core" not in tools_for("assistant", "session")

    @pytest.mark.asyncio
    async def test_an_unresolvable_config_is_refused_not_guessed(
        self, user_a, fake_db, fake_request
    ):
        from fastapi import HTTPException

        with (
            patch(
                "orchestrator.security.auth.require_approved_user", _approved(user_a)
            ),
            patch("orchestrator.main.app.state.resources.postgres_db", fake_db),
            patch(
                f"{_TOOL_VIEW}.merged_session_tool_policy",
                MagicMock(side_effect=RuntimeError("boom")),
            ),
        ):
            with pytest.raises(HTTPException) as exc:
                await _preview_tool_groups(
                    ToolGroupPreviewRequest(config_name="session_base"), fake_request
                )
        assert exc.value.status_code == 422

    @pytest.mark.asyncio
    async def test_requires_an_approved_user(self, fake_db, fake_request):
        from fastapi import HTTPException

        with (
            patch(
                "orchestrator.security.auth.require_approved_user",
                AsyncMock(side_effect=HTTPException(status_code=403)),
            ),
            patch("orchestrator.main.app.state.resources.postgres_db", fake_db),
        ):
            with pytest.raises(HTTPException) as exc:
                await _preview_tool_groups(ToolGroupPreviewRequest(), fake_request)
        assert exc.value.status_code == 403


class TestEnumerateOnlyRidesBothReads:
    """A settings surface cannot offer "shell on" unless it is told the names.

    ``shell`` refuses ``tools.shell: true`` at the write boundary, so the only
    payload that enables it is an enumeration. Serving that enumeration from
    the registry is what stops the cockpit growing a fifth hand-maintained
    tool-name list in the change that deletes four — and it has to ride BOTH
    reads, because the New Session form (preview) is the surface where "shell
    becomes settable" is actually visible.
    """

    @pytest.mark.asyncio
    async def test_the_thread_read_serves_it(self, user_a, fake_db, fake_request):
        result, _ = await _call(user_a, fake_db, _thread(), fake_request)
        assert result["enumerate_only"]["shell"]

    @pytest.mark.asyncio
    async def test_the_preview_read_serves_it(self, user_a, fake_db, fake_request):
        with (
            patch(
                "orchestrator.security.auth.require_approved_user", _approved(user_a)
            ),
            patch("orchestrator.main.app.state.resources.postgres_db", fake_db),
            patch(
                "orchestrator.services.grant_enforcement.resolve_runner_grants",
                AsyncMock(return_value=None),
            ),
        ):
            result = await _preview_tool_groups(
                ToolGroupPreviewRequest(config_name="session_base"), fake_request
            )
        assert result["enumerate_only"]["shell"]

    @pytest.mark.asyncio
    async def test_it_is_the_registry_answer_not_a_transcription(
        self, user_a, fake_db, fake_request
    ):
        from shared.runtime.core.tool_policy import enumerate_only_members

        result, _ = await _call(user_a, fake_db, _thread(), fake_request)
        assert result["enumerate_only"] == enumerate_only_members()

    @pytest.mark.asyncio
    async def test_what_it_prescribes_is_what_the_write_boundary_accepts(
        self, user_a, fake_db, fake_request
    ):
        """The round trip the cockpit actually performs."""
        from shared.runtime.core.tool_policy import validate_tool_override_fragment

        result, _ = await _call(user_a, fake_db, _thread(), fake_request)
        for category, names in result["enumerate_only"].items():
            accepted = validate_tool_override_fragment(
                {"tools": {category: {"only": names}}}
            )
            assert accepted[category] == names


class TestPreviewModelsTheLegacyPathToo:
    """The creation form must predict the path a created session would take.

    With experts off, the agent APPENDS the compatibility groups unless an
    explicit ``[]`` disables them — the opposite of the resolved path for an
    unset group. A preview that always predicted ``resolved`` would tell the
    user "Fleet Management: off" and then hand them a session that has it,
    which is this series' own defect rebuilt in the surface that predicts it.
    """

    async def _preview(self, user, db, req, *, experts, override=None):
        with (
            patch("orchestrator.security.auth.require_approved_user", _approved(user)),
            patch("orchestrator.main.app.state.resources.postgres_db", db),
            patch(
                f"{_TOOL_VIEW}.is_experts_db_enabled",
                MagicMock(return_value=experts),
            ),
            patch(
                "orchestrator.services.grant_enforcement.user_experts_enabled",
                AsyncMock(return_value=experts),
            ),
            patch(
                "orchestrator.services.grant_enforcement.resolve_runner_grants",
                AsyncMock(return_value=None),
            ),
        ):
            return await _preview_tool_groups(
                ToolGroupPreviewRequest(
                    config_name="session_base", config_override=override
                ),
                req,
            )

    @pytest.mark.asyncio
    async def test_legacy_deployment_predicts_the_groups_ON(
        self, user_a, fake_db, fake_request
    ):
        result = await self._preview(user_a, fake_db, fake_request, experts=False)
        assert result["source"] == "legacy"
        assert result["tool_groups"]["orchestrator"] is True
        assert result["tool_groups"]["workflows"] is True

    @pytest.mark.asyncio
    async def test_resolved_deployment_predicts_them_OFF(
        self, user_a, fake_db, fake_request
    ):
        """The same base config, the opposite answer — which is the whole point."""
        result = await self._preview(user_a, fake_db, fake_request, experts=True)
        assert result["source"] == "resolved"
        assert result["tool_groups"]["orchestrator"] is False
        assert result["tool_groups"]["workflows"] is False

    @pytest.mark.asyncio
    async def test_legacy_still_honours_an_explicit_disable(
        self, user_a, fake_db, fake_request
    ):
        result = await self._preview(
            user_a,
            fake_db,
            fake_request,
            experts=False,
            override={"tools": {"orchestrator": []}},
        )
        assert result["tool_groups"]["orchestrator"] is False

    @pytest.mark.asyncio
    async def test_it_is_still_a_prediction_on_the_legacy_path(
        self, user_a, fake_db, fake_request
    ):
        result = await self._preview(user_a, fake_db, fake_request, experts=False)
        assert result["origin"] == ORIGIN_PREDICTION
        assert result["degraded_reason"] is None


class TestOneDeadlineForTheWholeProbe:
    """404-then-/status is TWO requests, and it is the normal path today.

    ``httpx``'s timeout is per-operation, so a per-request bound lets the
    fallback cost a second full budget — and lets a trickling pod exceed either
    without tripping one. The settings pane blocks on this endpoint.
    """

    @pytest.mark.asyncio
    async def test_a_slow_two_hop_probe_is_cut_off_and_labelled(
        self, user_a, fake_db, fake_request
    ):
        from orchestrator.services import agent_toolset_probe

        async def _crawl(url):
            await asyncio.sleep(5)
            return _Response(payload={})

        client = MagicMock()
        client.__aenter__ = AsyncMock(return_value=client)
        client.__aexit__ = AsyncMock(return_value=False)
        client.get = AsyncMock(side_effect=_crawl)

        fake_db.get_thread = AsyncMock(return_value=_thread(agent_id="agent-1"))
        fake_db.get_agent = AsyncMock(return_value=_AGENT_ROW)
        started = asyncio.get_event_loop().time()
        with (
            patch(
                "orchestrator.security.auth.require_approved_user", _approved(user_a)
            ),
            patch(
                "orchestrator.security.access.require_approved_user",
                AsyncMock(return_value=user_a),
            ),
            patch("orchestrator.main.app.state.resources.postgres_db", fake_db),
            patch(f"{_TOOL_VIEW}.is_experts_db_enabled", MagicMock(return_value=True)),
            patch(
                "orchestrator.services.grant_enforcement.user_experts_enabled",
                AsyncMock(return_value=True),
            ),
            patch(
                "orchestrator.services.grant_enforcement.resolve_runner_grants",
                AsyncMock(return_value=None),
            ),
            # R1.B05 moved the probe to `services/agent_toolset_probe`, which
            # reads its own module constant. Patching the budget on `main`
            # would leave this case green while the probe ran unbounded.
            patch.object(agent_toolset_probe, "AGENT_TOOLSET_BUDGET_S", 0.2),
            patch(
                "orchestrator.services.agent_toolset_probe.httpx.AsyncClient",
                MagicMock(return_value=client),
            ),
        ):
            result = await _get_tool_groups(str(_thread()["id"]), fake_request)
        elapsed = asyncio.get_event_loop().time() - started

        assert result["origin"] == ORIGIN_PREDICTION
        assert "budget" in result["prediction_reason"]
        assert elapsed < 2.0, f"probe was not bounded (took {elapsed:.2f}s)"

    @pytest.mark.asyncio
    async def test_a_booting_agent_is_not_probed(self, user_a, fake_db, fake_request):
        """Registered but not serving: nothing bound, so nothing to measure."""
        fake_db.get_thread = AsyncMock(return_value=_thread(agent_id="agent-1"))
        fake_db.get_agent = AsyncMock(return_value={**_AGENT_ROW, "status": "booting"})
        http_patch, calls = _agent_http({})
        with (
            patch(
                "orchestrator.security.auth.require_approved_user", _approved(user_a)
            ),
            patch(
                "orchestrator.security.access.require_approved_user",
                AsyncMock(return_value=user_a),
            ),
            patch("orchestrator.main.app.state.resources.postgres_db", fake_db),
            patch(f"{_TOOL_VIEW}.is_experts_db_enabled", MagicMock(return_value=True)),
            patch(
                "orchestrator.services.grant_enforcement.user_experts_enabled",
                AsyncMock(return_value=True),
            ),
            patch(
                "orchestrator.services.grant_enforcement.resolve_runner_grants",
                AsyncMock(return_value=None),
            ),
            http_patch,
        ):
            result = await _get_tool_groups(str(_thread()["id"]), fake_request)

        assert result["origin"] == ORIGIN_PREDICTION
        assert "booting" in result["prediction_reason"]
        assert calls == []


# =============================================================================
# The agent's side of the wire
# =============================================================================
class TestAgentToolsetRoute:
    def test_reports_nothing_to_measure_when_unattached(self):
        import agent.api.persistent_app as pa

        with patch.object(pa, "_session", None), patch.object(pa, "_thread_id", "t9"):
            payload = pa._session_toolset_report()
        assert payload["attached"] is False
        assert payload["report"] is None

    def test_reads_the_live_tool_objects(self):
        import agent.api.persistent_app as pa

        session = MagicMock()
        session.thread_id = "t9"
        session.tools = [MagicMock(name="a"), MagicMock(name="b")]
        session.tools[0].name = "read_file"
        session.tools[1].name = "web_search"
        session.workspace_manager.backend = None

        with (
            patch.object(pa, "_session", session),
            patch.object(pa, "_thread_id", "t9"),
        ):
            payload = pa._session_toolset_report()

        assert payload["attached"] is True
        assert payload["report"]["tools"] == ["read_file", "web_search"]
        assert payload["report"]["categories"]["workspace"] == ["read_file"]

    def test_round_trips_into_the_orchestrator_reader(self):
        """The two ends of the wire agree — no hand transcription between them."""
        import agent.api.persistent_app as pa
        from shared.runtime.core.tool_report import read_agent_toolset_report

        session = MagicMock()
        session.thread_id = "t9"
        tool = MagicMock()
        tool.name = "web_search"
        session.tools = [tool]
        session.workspace_manager.backend = None

        with (
            patch.object(pa, "_session", session),
            patch.object(pa, "_thread_id", "t9"),
        ):
            payload = pa._session_toolset_report()

        assert read_agent_toolset_report(payload["report"]) == {
            "research": ["web_search"]
        }

    def test_payload_is_json_serialisable(self):
        import agent.api.persistent_app as pa

        session = MagicMock()
        session.thread_id = "t9"
        tool = MagicMock()
        tool.name = "web_search"
        session.tools = [tool]
        session.workspace_manager.backend = None

        with (
            patch.object(pa, "_session", session),
            patch.object(pa, "_thread_id", "t9"),
        ):
            payload = pa._session_toolset_report()

        assert json.loads(json.dumps(payload))["report"]["tools"] == ["web_search"]


class TestBothAgentAppsRegisterTheRoute:
    """Dedicated session pods run ``--mode persistent``; pool pods run ``dual``.

    ``/session/status`` exists only on the dual app, so
    ``_thread_turn_in_flight`` 404s against every dedicated session pod — a
    live bug. A toolset route missing from either app would silently downgrade
    that whole population to a prediction, which is the same class of quiet
    wrong answer this task exists to remove.
    """

    @pytest.mark.parametrize("factory", ["persistent", "dual"])
    def test_session_toolset_is_registered(self, factory):
        if factory == "persistent":
            from agent.api.persistent_app import create_persistent_app

            app = create_persistent_app("config/session_base.yaml")
        else:
            from agent.api.dual_app import create_dual_app

            app = create_dual_app()
        paths = {getattr(r, "path", "") for r in app.routes}
        assert "/session/toolset" in paths


class TestPreviewRoster:
    """The New Session form's read carries the same ``subagents`` summary as
    the thread endpoint, so the Delegation row can warn BEFORE the session
    exists that the picked expert has nothing to delegate to."""

    @pytest.mark.asyncio
    async def test_preview_reports_the_expert_roster(
        self, user_a, fake_db, fake_request
    ):
        from pathlib import Path
        from uuid import UUID

        import yaml

        from shared.manifests.resolution import content_revision
        from tests.conftest import _UID_A

        document = yaml.safe_load(
            (
                Path(__file__).resolve().parents[1]
                / "config/subagents/reader/config.yaml"
            ).read_text()
        )
        description = "Reader from the saved Catalog revision."
        document["spec"]["runtime"]["config"]["config"]["description"] = description
        catalog = {
            "id": UUID("44444444-2222-3333-4444-555555555555"),
            "owner_id": None,
            "document": document,
            "resolved": document,
            "resource_version": 2,
            "revision": content_revision(document["spec"]),
        }

        async def stored_definition(query, *args):
            if "FROM srw_resources" in query:
                assert args == ("Expert", "Catalog", "shared", "subagent-reader")
                return catalog
            return None

        fake_db.fetchrow = AsyncMock(side_effect=stored_definition)
        expert_id = "11111111-2222-3333-4444-555555555555"
        fake_db.get_expert_by_id = AsyncMock(
            return_value={
                "id": expert_id,
                "user_id": _UID_A,
                "name": "rostered",
                "config": {
                    "subagents": {
                        "default": "reader",
                        "roster": {"reader": {"$ref": "subagents/reader"}},
                    }
                },
            }
        )
        with (
            patch(
                "orchestrator.security.auth.require_approved_user",
                AsyncMock(return_value=user_a),
            ),
            patch("orchestrator.main.app.state.resources.postgres_db", fake_db),
            patch(f"{_TOOL_VIEW}.is_experts_db_enabled", MagicMock(return_value=True)),
            patch(
                "orchestrator.services.grant_enforcement.user_experts_enabled",
                AsyncMock(return_value=True),
            ),
            patch(
                "orchestrator.services.grant_enforcement.resolve_runner_grants",
                AsyncMock(return_value=None),
            ),
        ):
            with_roster = await _preview_tool_groups(
                ToolGroupPreviewRequest(
                    config_name="session_base", expert_id=expert_id
                ),
                fake_request,
            )
            bare = await _preview_tool_groups(
                ToolGroupPreviewRequest(config_name="session_base"), fake_request
            )

        assert with_roster["subagents"]["default"] == "reader"
        assert [e["name"] for e in with_roster["subagents"]["roster"]] == ["reader"]
        assert with_roster["subagents"]["roster"][0]["ref"] == "subagents/reader"
        assert with_roster["subagents"]["roster"][0]["description"] == description
        assert bare["subagents"] == {"default": None, "roster": []}


@pytest.mark.asyncio
@pytest.mark.parametrize("backend", ["none", "virtual", "sandbox"])
async def test_explicit_workspace_preview_overrides_expert_recommendation(
    user_a, fake_db, fake_request, backend
):
    selected = (
        None if backend == "none" else {"template": {"inline": {"backend": backend}}}
    )
    with (
        patch("orchestrator.security.auth.require_approved_user", _approved(user_a)),
        patch("orchestrator.main.app.state.resources.postgres_db", fake_db),
        patch(
            "orchestrator.services.grant_enforcement.resolve_runner_grants",
            AsyncMock(return_value=None),
        ),
    ):
        result = await _preview_tool_groups(
            ToolGroupPreviewRequest(
                config_name="session_base",
                workspace=selected,
                workspace_preference="sandbox",
                config_override={"tools": {"shell": ["run_command"]}},
            ),
            fake_request,
        )
    assert result["workspace"] == {
        "backend": backend,
        "source": "request",
        "binding": selected,
    }
    assert result["origin"] == ORIGIN_PREDICTION
    if backend != "sandbox":
        assert result["categories"]["shell"]["state"] == STATE_UNAVAILABLE
        assert result["categories"]["shell"]["decided_by"] == "backend"
