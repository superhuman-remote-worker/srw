"""GET /api/persistent/threads/{id}/tool-groups — the Settings→Tools truth source.

Regression cover for the "checkbox ticked, agent has no tools" bug: the cockpit
used to derive tool-group enablement from the thread's ``config_override``
alone, where an unset group reads as enabled. The merged config says the
opposite — ``config/session_base.yaml`` ships ``tools.orchestrator: []`` — so a
stock session rendered a ticked Fleet Management box while the agent bound zero
fleet tools.

The endpoint answers from the same layering the agent hydrates, and reports
WHICH agent path it modelled (``resolved`` / ``legacy`` / ``error``), because
the legacy fallback genuinely disagrees with the resolved path for an unset
group.
"""

from contextlib import ExitStack
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from shared.runtime.core.session_tool_overrides import SESSION_TOOL_OVERRIDE_NAMES
from orchestrator.routers import thread_session
from orchestrator.services import session_config_resolution

#: The tool view's own module-level collaborators (deployment gate, policy
#: merge) are looked up here at call time, so they are patched here.
_TOOL_VIEW = "orchestrator.services.session_tool_view"


def _patch_caller_and_db(user: dict, db):
    stack = ExitStack()
    stack.enter_context(
        patch("orchestrator.main.require_approved_user", AsyncMock(return_value=user))
    )
    stack.enter_context(
        patch(
            "orchestrator.security.access.require_approved_user",
            AsyncMock(return_value=user),
        )
    )
    stack.enter_context(patch("orchestrator.main.postgres_db", db))
    return stack


def _thread(metadata=None, config_name=None, project_id=None) -> dict:
    """A thread row shaped like the real one; ``_TID_A`` is user_a's."""
    from tests.conftest import _TID_A, _UID_A

    return {
        "id": _TID_A,
        "user_id": _UID_A,
        "title": "thread A",
        "config_name": config_name,
        "project_id": project_id,
        "metadata": metadata or {},
    }


async def _get_tool_groups(thread_id, request):
    """Drive the route through the application's own dependency composition.

    ``main._thread_session_dependencies()`` is rebuilt per call and reads the
    app-owned collaborators (``postgres_db``, ``_user_experts_enabled``,
    ``_resolve_runner_grants``, ...) at call time, so patches of those still
    steer the extracted route.
    """
    import orchestrator.main as orch_main

    return await thread_session.get_thread_tool_groups(
        thread_id, request, dependencies=orch_main._thread_session_dependencies()
    )


async def _call(user, db, thread_row, fake_request, *, experts=True):
    db.get_thread = AsyncMock(return_value=thread_row)
    with (
        _patch_caller_and_db(user, db),
        patch(f"{_TOOL_VIEW}.is_experts_db_enabled", MagicMock(return_value=experts)),
        patch(
            "orchestrator.main._user_experts_enabled", AsyncMock(return_value=experts)
        ),
    ):
        return await _get_tool_groups(str(thread_row["id"]), fake_request)


# =============================================================================
# Resolved path — the merged config decides
# =============================================================================


class TestResolvedPath:
    @pytest.mark.asyncio
    async def test_reports_base_yaml_disabled_groups(
        self, user_a, fake_db, fake_request
    ):
        """THE regression: a stock session has fleet/catalog/workflows OFF."""
        result = await _call(user_a, fake_db, _thread(), fake_request)

        assert result["source"] == "resolved"
        assert result["tool_groups"] == {
            "orchestrator": False,
            "job_control": False,
            "job_inspection": False,
            "agent_catalog": False,
            "workflows": False,
            "canvas": True,
            # session_base declares `catalog_authoring: [ ]`, and turning it on
            # additionally needs the capability grant.
            "catalog_authoring": False,
        }

    @pytest.mark.asyncio
    async def test_reported_shape_is_the_closed_vocabulary(
        self, user_a, fake_db, fake_request
    ):
        result = await _call(user_a, fake_db, _thread(), fake_request)
        assert set(result["tool_groups"]) == set(SESSION_TOOL_OVERRIDE_NAMES)

    @pytest.mark.asyncio
    async def test_honors_request_override_reenable(
        self, user_a, fake_db, fake_request
    ):
        """A live Settings toggle lands in config_override — it must be read."""
        override = {
            "tools": {
                "orchestrator": sorted(SESSION_TOOL_OVERRIDE_NAMES["orchestrator"])
            }
        }
        result = await _call(
            user_a,
            fake_db,
            _thread(metadata={"config_override": override}),
            fake_request,
        )

        assert result["tool_groups"]["orchestrator"] is True
        # The groups the override didn't name stay at the base default.
        assert result["tool_groups"]["workflows"] is False

    @pytest.mark.asyncio
    async def test_honors_expert_fragment_disable(self, user_a, fake_db, fake_request):
        """Pins that the expert layer is still merged (canvas is on in the base)."""
        from tests.conftest import _UID_A

        expert_id = "11111111-2222-3333-4444-555555555555"
        fake_db.get_expert_by_id = AsyncMock(
            return_value={
                "id": expert_id,
                "user_id": _UID_A,
                "name": "no-canvas",
                "config": {"tools": {"canvas": []}},
            }
        )
        result = await _call(
            user_a,
            fake_db,
            _thread(metadata={"expert_id": expert_id}),
            fake_request,
        )

        assert result["tool_groups"]["canvas"] is False

    @pytest.mark.asyncio
    @pytest.mark.parametrize("as_json_string", [False, True])
    async def test_honors_project_expert_link_override(
        self, user_a, fake_db, fake_request, as_json_string
    ):
        """Pins the project layer, incl. the JSON-string delivery shape."""
        import json

        from tests.conftest import _PID_A, _UID_A

        expert_id = "11111111-2222-3333-4444-555555555555"
        link_override = {
            "tools": {"workflows": sorted(SESSION_TOOL_OVERRIDE_NAMES["workflows"])}
        }
        fake_db.get_expert_by_id = AsyncMock(
            return_value={"id": expert_id, "user_id": _UID_A, "name": "e", "config": {}}
        )
        fake_db.get_project_expert_link = AsyncMock(
            return_value={
                "config_override": (
                    json.dumps(link_override) if as_json_string else link_override
                )
            }
        )
        result = await _call(
            user_a,
            fake_db,
            _thread(metadata={"expert_id": expert_id}, project_id=_PID_A),
            fake_request,
        )

        assert result["tool_groups"]["workflows"] is True

    @pytest.mark.asyncio
    async def test_uuid_config_name_falls_back_to_session_base(
        self, user_a, fake_db, fake_request
    ):
        """A cockpit-conflated expert UUID must not be loaded as a config path."""
        result = await _call(
            user_a,
            fake_db,
            _thread(config_name="11111111-2222-3333-4444-555555555555"),
            fake_request,
        )

        assert result["source"] == "resolved"
        assert result["tool_groups"]["orchestrator"] is False


# =============================================================================
# The skip ledger — what the lean resolve leaves out must not move tools.*
# =============================================================================


class TestLeanResolveFidelity:
    @pytest.mark.parametrize(
        "expert_row,project_overrides,request_override",
        [
            (None, None, None),
            (None, None, {"tools": {"job_control": ["create_job"]}}),
            ({"config": {"tools": {"canvas": []}}}, None, None),
            (None, {"tools": {"workflows": ["get_project_loop"]}}, None),
            (
                {"config": {"tools": {"agent_catalog": ["get_skill"]}}},
                None,
                {"tools": {"workflows": []}},
            ),
        ],
    )
    def test_lean_resolve_matches_full_resolve_markers(
        self, expert_row, project_overrides, request_override
    ):
        """The endpoint's answer must equal what the attach path would mark.

        This is what pins the skip ledger in ``_merged_session_tool_groups``:
        the reference run below layers in ``base_defaults`` (the biggest skip)
        and derives enablement the way attach does, via the disable markers.
        If account defaults or the settings matrix ever learn to emit
        ``tools``, this fails instead of silently drifting the checkbox.
        """
        import orchestrator.main as orch_main
        from orchestrator.services.config_resolver import resolve_config

        capture: dict = {}
        resolve_config(
            base_config_name="session_base",
            base_defaults={
                "llm": {"model": "gpt-5.6-sol", "temperature": 0.2},
                "interactive": {"permission_mode": "autonomous"},
                "workspace": {"backend": "sandbox"},
            },
            expert_row=expert_row,
            project_overrides=project_overrides,
            request_override=request_override,
            expert_type="session",
            capture=capture,
        )
        markers = orch_main._session_tool_group_disabled_markers(
            capture["merged_fragment"]
        )
        marker_names = orch_main._SESSION_TOOL_DISABLED_MARKERS
        # Keyed on the MARKER set, not on SESSION_TOOL_OVERRIDE_NAMES. A marker
        # exists to tell the legacy agent not to re-add a canonical list, so only
        # groups that agent knows about have one; a presentation group added later
        # (catalog_authoring) has no marker because no deployed agent re-adds it.
        # Iterating the vocabulary here asserted a marker per checkbox, which is
        # the same conflation `LEGACY_APPENDED_GROUPS` exists to break.
        reference = {
            group: marker_names[group] not in markers for group in marker_names
        }

        lean = orch_main._merged_session_tool_groups(
            base_config_name="session_base",
            expert_row=expert_row,
            project_overrides=project_overrides,
            request_override=request_override,
        )

        assert {g: lean[g] for g in marker_names} == reference
        # The resolved path answers for every presentation group, marker or not.
        assert set(lean) == set(SESSION_TOOL_OVERRIDE_NAMES)

    @pytest.mark.asyncio
    async def test_session_account_defaults_never_carry_tools(self, fake_db):
        """Pins the largest skip: account defaults cannot move a tool group."""
        import orchestrator.main as orch_main
        from tests.conftest import _UID_A

        fake_db.get_user_settings = AsyncMock(
            return_value={
                "persistent_agent": {
                    "model": "gpt-5.6-sol",
                    "temperature": 0.4,
                    "permission_mode": "autonomous",
                    "workspace_backend": "sandbox",
                    "tools": {"job_control": ["create_job"]},
                }
            }
        )
        with patch("orchestrator.main.postgres_db", fake_db):
            defaults = await session_config_resolution.resolve_session_account_defaults(
                str(_UID_A), dependencies=orch_main._session_config_dependencies()
            )

        assert "tools" not in (defaults or {})


# =============================================================================
# Legacy path — experts off, where an unset group is ENABLED
# =============================================================================


class TestLegacyPath:
    @pytest.mark.asyncio
    async def test_absent_group_is_enabled(self, user_a, fake_db, fake_request):
        """The opposite of the resolved path — and why we report ``source``.

        With experts off the agent takes the config_name + config_override
        fallback, which appends the canonical lists whenever no explicit
        ``[]`` marker is present. Reporting the resolved answer here would be
        just as wrong as the bug we're fixing, in the other direction.
        """
        result = await _call(user_a, fake_db, _thread(), fake_request, experts=False)

        assert result["source"] == "legacy"
        assert result["tool_groups"] == {
            "orchestrator": True,
            "job_control": True,
            "job_inspection": True,
            "agent_catalog": True,
            "workflows": True,
            "canvas": True,
            # The odd one out, and deliberately so: the inversion this test
            # documents comes from the legacy agent re-adding canonical lists
            # when no disable marker is present, and no deployed agent image
            # re-adds `catalog_authoring` — it did not exist when they were
            # built. So an unset catalog_authoring is OFF on both paths. If this
            # ever flips to True, the endpoint is promising a write capability
            # the agent will not bind.
            "catalog_authoring": False,
        }

    @pytest.mark.asyncio
    async def test_explicit_empty_override_disables(
        self, user_a, fake_db, fake_request
    ):
        result = await _call(
            user_a,
            fake_db,
            _thread(metadata={"config_override": {"tools": {"workflows": []}}}),
            fake_request,
            experts=False,
        )

        assert result["tool_groups"]["workflows"] is False
        assert result["tool_groups"]["orchestrator"] is True

    @pytest.mark.asyncio
    async def test_canvas_follows_the_base_yaml(self, user_a, fake_db, fake_request):
        """Canvas is strip-only on the legacy path — no append, unlike the rest.

        ``worker_base`` declares no canvas group, so canvas must read disabled
        there while the appending groups still read enabled.
        """
        result = await _call(
            user_a,
            fake_db,
            _thread(config_name="worker_base"),
            fake_request,
            experts=False,
        )

        assert result["tool_groups"]["canvas"] is False
        assert result["tool_groups"]["orchestrator"] is True

    @pytest.mark.asyncio
    async def test_user_experts_kill_switch_selects_legacy(
        self, user_a, fake_db, fake_request
    ):
        fake_db.get_thread = AsyncMock(return_value=_thread())
        with (
            _patch_caller_and_db(user_a, fake_db),
            patch(f"{_TOOL_VIEW}.is_experts_db_enabled", MagicMock(return_value=True)),
            patch(
                "orchestrator.main._user_experts_enabled", AsyncMock(return_value=False)
            ),
        ):
            result = await _get_tool_groups(str(_thread()["id"]), fake_request)

        assert result["source"] == "legacy"


# =============================================================================
# Failure + auth
# =============================================================================


class TestFailureModes:
    @pytest.mark.asyncio
    async def test_resolve_failure_reports_error_not_a_guess(
        self, user_a, fake_db, fake_request
    ):
        """A resolve error REFUSES the attach, so there is no answer to report.

        Falling back to a config_override-shaped guess here would reintroduce
        the bug, so the endpoint says so and lets the client use its own
        defaults.
        """
        fake_db.get_thread = AsyncMock(return_value=_thread())
        with (
            _patch_caller_and_db(user_a, fake_db),
            patch(f"{_TOOL_VIEW}.is_experts_db_enabled", MagicMock(return_value=True)),
            patch(
                "orchestrator.main._user_experts_enabled", AsyncMock(return_value=True)
            ),
            patch(
                f"{_TOOL_VIEW}.merged_session_tool_policy",
                MagicMock(side_effect=RuntimeError("boom")),
            ),
        ):
            result = await _get_tool_groups(str(_thread()["id"]), fake_request)

        assert result["source"] == "error"
        assert result["tool_groups"] is None

    @pytest.mark.asyncio
    async def test_cross_user_blocked(self, user_b, fake_db, fake_request):
        sentinel = MagicMock(side_effect=AssertionError("called past gate"))
        with (
            _patch_caller_and_db(user_b, fake_db),
            patch(f"{_TOOL_VIEW}.merged_session_tool_policy", sentinel),
        ):
            with pytest.raises(HTTPException) as exc:
                await _get_tool_groups(str(_thread()["id"]), fake_request)

        assert exc.value.status_code == 403
        sentinel.assert_not_called()

    @pytest.mark.asyncio
    async def test_orphan_thread_blocked(self, user_a, fake_db, fake_request):
        orphan_id = "ccc55555-5555-5555-5555-555555555555"
        fake_db.get_thread = AsyncMock(
            return_value={"id": orphan_id, "user_id": None, "title": "orphan"}
        )
        sentinel = MagicMock(side_effect=AssertionError("called past gate"))
        with (
            _patch_caller_and_db(user_a, fake_db),
            patch(f"{_TOOL_VIEW}.merged_session_tool_policy", sentinel),
        ):
            with pytest.raises(HTTPException) as exc:
                await _get_tool_groups(orphan_id, fake_request)

        assert exc.value.status_code == 403
        sentinel.assert_not_called()


# =============================================================================
# Acknowledged grant drift must be reflected, not just enforced silently
# =============================================================================


class TestAcknowledgedGrantDriftReportedNotJustEnforced:
    """Round-2 finding: ``_merged_session_tool_policy`` used to call
    ``resolve_config`` WITHOUT the ``grant_strip`` hook
    ``_resolve_session_config`` applies, so this endpoint reported the
    PRE-strip merge. ``catalog_authoring`` is both a closed session tool
    group and a key ``strip_to_grants`` deletes, so once a user acknowledged
    losing that grant, the settings checkbox kept reading "on" even though
    the delivered agent blob no longer carried it — the report disagreed
    with what the session actually got.
    """

    @pytest.mark.asyncio
    async def test_acknowledged_catalog_authoring_reports_unavailable_not_on(
        self, user_a, fake_db, fake_request
    ):
        thread = _thread(
            metadata={
                "config_override": {"tools": {"catalog_authoring": ["create_expert"]}},
                "config_drift_ack": {"grant:catalog_authoring": "revoked"},
            }
        )
        # A real (non-admin) owner row, so _resolve_runner_grants actually
        # resolves grants instead of admin-bypassing to None.
        fake_db.get_user = AsyncMock(
            return_value={"id": str(user_a["id"]), "is_admin": False}
        )

        with patch(
            "orchestrator.main._resolve_runner_grants",
            AsyncMock(return_value={"catalog_authoring": False}),
        ):
            result = await _call(user_a, fake_db, thread, fake_request)

        assert result["categories"]["catalog_authoring"]["state"] == "unavailable"
        assert result["tool_groups"]["catalog_authoring"] is False

    @pytest.mark.asyncio
    async def test_unacknowledged_catalog_authoring_still_reports_on(
        self, user_a, fake_db, fake_request
    ):
        """Control case: with NOTHING acknowledged, grant_strip has nothing
        to strip (acknowledged_grant_keys is empty), so the merged config's
        own value still decides — pinning that the fix above narrows the
        report correctly rather than blanket-hiding the category."""
        thread = _thread(
            metadata={
                "config_override": {"tools": {"catalog_authoring": ["create_expert"]}},
            }
        )
        fake_db.get_user = AsyncMock(
            return_value={"id": str(user_a["id"]), "is_admin": False}
        )

        with patch(
            "orchestrator.main._resolve_runner_grants",
            AsyncMock(return_value={"catalog_authoring": True}),
        ):
            result = await _call(user_a, fake_db, thread, fake_request)

        assert result["categories"]["catalog_authoring"]["state"] == "on"
        assert result["tool_groups"]["catalog_authoring"] is True


class TestAdminViewUsesTheOwnersGrants:
    """An admin viewing another user's session must see THAT owner's grants —
    both for the ``unavailable`` explanation and for the acknowledged-drift
    strip — never their own; the owner-vs-caller fix the view mirrors."""

    @pytest.mark.asyncio
    async def test_every_grant_lookup_names_the_thread_owner(
        self, user_admin, fake_db, fake_request
    ):
        from tests.conftest import _UID_A

        thread = _thread(
            metadata={"config_drift_ack": {"grant:catalog_authoring": "revoked"}}
        )
        resolve = AsyncMock(return_value={"catalog_authoring": False})
        with patch("orchestrator.main._resolve_runner_grants", resolve):
            await _call(user_admin, fake_db, thread, fake_request)

        # One lookup explains ``unavailable``, one builds the drift strip.
        assert resolve.await_count == 2
        assert {c.kwargs["runner_user_id"] for c in resolve.await_args_list} == {
            str(_UID_A)
        }


# =============================================================================
# The explicit-grant gate — names alone are not a binding
# =============================================================================
class TestDelegationGate:
    """``delegate_agent`` + control plane bind only when ``tools.delegation``
    names them AND ``delegation.enabled`` is true (the factory returns ``[]``
    otherwise). The prediction used to report the names regardless — "5
    predicted" for a tick that changed nothing (main-dev thread 54e31e45:
    "11 configured tool(s) did not bind"). It must report the category as the
    factory will bind it, and leave it settable so the tick (which now writes
    the gate too) is a promise the runtime keeps."""

    _NAMES = [
        "delegate_agent",
        "list_agents",
        "message_agent",
        "stop_agent",
        "wait_agent",
    ]

    @pytest.mark.asyncio
    async def test_names_without_the_gate_predict_off_and_settable(
        self, user_a, fake_db, fake_request
    ):
        result = await _call(
            user_a,
            fake_db,
            _thread(
                metadata={"config_override": {"tools": {"delegation": self._NAMES}}}
            ),
            fake_request,
        )
        entry = result["categories"]["delegation"]
        assert entry["state"] == "off"
        assert entry["tools"] == []
        assert entry["settable"] is True

    @pytest.mark.asyncio
    async def test_names_with_the_gate_predict_on(self, user_a, fake_db, fake_request):
        result = await _call(
            user_a,
            fake_db,
            _thread(
                metadata={
                    "config_override": {
                        "tools": {"delegation": self._NAMES},
                        "delegation": {"enabled": True},
                    }
                }
            ),
            fake_request,
        )
        entry = result["categories"]["delegation"]
        assert entry["state"] == "on"
        assert sorted(entry["tools"]) == sorted(self._NAMES)

    @pytest.mark.asyncio
    async def test_gate_applies_on_the_legacy_path_too(
        self, user_a, fake_db, fake_request
    ):
        result = await _call(
            user_a,
            fake_db,
            _thread(
                metadata={"config_override": {"tools": {"delegation": self._NAMES}}}
            ),
            fake_request,
            experts=False,
        )
        assert result["source"] == "legacy"
        assert result["categories"]["delegation"]["tools"] == []
        on = await _call(
            user_a,
            fake_db,
            _thread(
                metadata={
                    "config_override": {
                        "tools": {"delegation": self._NAMES},
                        "delegation": {"enabled": True},
                    }
                }
            ),
            fake_request,
            experts=False,
        )
        assert sorted(on["categories"]["delegation"]["tools"]) == sorted(self._NAMES)


# =============================================================================
# The roster a Delegation tick reaches
# =============================================================================


class TestRosterReport:
    """Both tool-groups reads carry ``subagents``: the roster the resolve
    materialised, or an EMPTY one. The seeded ``assistant`` shipped without a
    roster, so a ticked Delegation bound a ``delegate_agent`` whose every call
    errored ("this expert has no roster", main-dev thread 54e31e45,
    2026-09-07); the pane could not warn because nothing told it."""

    _ROSTER = {
        "subagents": {
            "default": "explorer",
            "roster": {
                "explorer": {"$ref": "subagents/explorer"},
                "scout": {
                    "description": "Inline scout.",
                    "tools": {"workspace": ["read_file"]},
                    "write_policy": "none",
                },
            },
        }
    }

    @pytest.mark.asyncio
    async def test_reports_the_materialised_roster(self, user_a, fake_db, fake_request):
        from pathlib import Path
        from uuid import UUID

        import yaml

        from shared.manifests.resolution import content_revision
        from tests.conftest import _UID_A

        document = yaml.safe_load(
            (
                Path(__file__).resolve().parents[1]
                / "config/subagents/explorer/config.yaml"
            ).read_text()
        )
        description = "Read-only investigator from the saved Catalog revision."
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
                assert args == ("Expert", "Catalog", "shared", "subagent-explorer")
                return catalog
            return None

        fake_db.fetchrow = AsyncMock(side_effect=stored_definition)
        expert_id = "11111111-2222-3333-4444-555555555555"
        fake_db.get_expert_by_id = AsyncMock(
            return_value={
                "id": expert_id,
                "user_id": _UID_A,
                "name": "rostered",
                "config": self._ROSTER,
            }
        )
        result = await _call(
            user_a,
            fake_db,
            _thread(metadata={"expert_id": expert_id}),
            fake_request,
        )
        summary = result["subagents"]
        assert summary["default"] == "explorer"
        by_name = {e["name"]: e for e in summary["roster"]}
        assert set(by_name) == {"explorer", "scout"}
        # A library `$ref` is materialised: the pane shows what the model sees.
        assert by_name["explorer"]["ref"] == "subagents/explorer"
        assert by_name["explorer"]["description"].startswith("Read-only investigator")
        assert by_name["explorer"]["description"] == description
        assert by_name["scout"] == {
            "name": "scout",
            "description": "Inline scout.",
            "ref": None,
        }

    @pytest.mark.asyncio
    async def test_an_expert_without_a_roster_reports_an_empty_one(
        self, user_a, fake_db, fake_request
    ):
        """``[]`` is the answer that lets the pane warn — distinct from the
        no-claim ``None`` of a failed resolve."""
        from tests.conftest import _UID_A

        expert_id = "11111111-2222-3333-4444-555555555555"
        fake_db.get_expert_by_id = AsyncMock(
            return_value={
                "id": expert_id,
                "user_id": _UID_A,
                "name": "bare",
                "config": {"tools": {"delegation": ["delegate_agent"]}},
            }
        )
        result = await _call(
            user_a,
            fake_db,
            _thread(
                metadata={
                    "expert_id": expert_id,
                    "config_override": {"delegation": {"enabled": True}},
                }
            ),
            fake_request,
        )
        assert result["subagents"] == {"default": None, "roster": []}
        assert result["categories"]["delegation"]["state"] == "on"

    @pytest.mark.asyncio
    async def test_legacy_path_reports_an_empty_roster(
        self, user_a, fake_db, fake_request
    ):
        result = await _call(user_a, fake_db, _thread(), fake_request, experts=False)
        assert result["source"] == "legacy"
        assert result["subagents"] == {"default": None, "roster": []}
