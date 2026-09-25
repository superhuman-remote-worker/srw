"""The officer's post — O3/O4 of knowledge-base/knowledge/features/officer_post.md.

Lifecycle endpoints (commission / decommission / hold / release), the PATCH
editor with §7's validation matrix + effect labels, the row-only
communication policy, the while-vacant ledger, and the ``end_thread``
rerouting that makes a direct DELETE on an officer thread and the
decommission endpoint one funnel.

Endpoint sections exercise the handlers directly with ``main``'s globals
monkeypatched (the house pattern — full TestClient auth wiring is k3d-smoke
territory). Real-Postgres sections cover the new ``project_officers``
ledger/queue helpers via the testcontainers pattern from
test_officer_post.py and skip cleanly without a local container engine.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from fastapi import HTTPException

import orchestrator.main as orch_main
from orchestrator.main import app
from orchestrator.database.postgres import PostgresDB
from orchestrator.routers import officers as officers_router
from orchestrator.routers.officers import (
    commission_project_officer,
    decommission_project_officer,
    hold_project_officer,
    patch_project_officer,
    recycle_project_officer,
    release_project_officer,
)
from orchestrator.schemas.officer_post import (
    OfficerDecommissionRequest,
    OfficerHoldRequest,
)
from orchestrator.services import (
    officer_notices,
    officer_post_lifecycle,
    session_wake,
)
from orchestrator.services.officer_post_lifecycle import (
    OfficerPostLifecycleDependencies,
)
from orchestrator.services.officer_post_policy import (
    OfficerPostPolicyDependencies,
    check_officer_sleep_bounds as _check_officer_sleep_bounds,
    validated_officer_post_patch as _validated_officer_post_patch,
)
from orchestrator.services.officer_post_views import (
    OfficerPostViewDependencies,
    officer_editor_block,
    officer_spend_today,
    while_vacant_view,
)
from orchestrator.services.persistent_recycler import PersistentRecycleResult
from tests._route_inventory import mounted_routes
from orchestrator.application import controls as controls_composition
from orchestrator.application import sessions as sessions_composition
from orchestrator.application.resources import bound
from orchestrator.services import officer_conference as officer_conference_module
from orchestrator.services import (
    officer_post_lifecycle as officer_post_lifecycle_module,
)
from orchestrator.services import (
    persistent_provisioner as persistent_provisioner_module,
)
from orchestrator.services import pinned_retirement as pinned_retirement_module
from orchestrator.services import session_wake as session_wake_module
from orchestrator.services import thread_admission as thread_admission_module
from orchestrator.services import thread_retirement as thread_retirement_module

REPO_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_FILE = REPO_ROOT / "src" / "orchestrator" / "database" / "schema_current.sql"

PROJECT_ID = str(uuid4())
THREAD_ID = str(uuid4())
RETIREMENT_GENERATION = str(uuid4())
RETIREMENT_TOKEN = str(uuid4())


# =========================================================================
# Route registration
# =========================================================================


class TestRoutesRegistered:
    def test_lifecycle_routes_are_wired(self):
        # The Post's nine declarations live on a mounted router now, and this
        # FastAPI puts one opaque ``_IncludedRouter`` in ``app.routes`` per
        # include — walking ``app.routes`` directly would report every one of
        # them missing. ``mounted_routes`` is the reading that survives both
        # representations.
        registered = mounted_routes(app)
        expected = {
            ("POST", "/api/projects/{project_id}/officer/commission"),
            ("POST", "/api/projects/{project_id}/officer/decommission"),
            ("POST", "/api/projects/{project_id}/officer/hold"),
            ("POST", "/api/projects/{project_id}/officer/release"),
            ("POST", "/api/projects/{project_id}/officer/recycle"),
            ("PATCH", "/api/projects/{project_id}/officer"),
            ("GET", "/api/projects/{project_id}/officer"),
        }
        missing = [r for r in expected if r not in registered]
        assert not missing, f"missing officer post routes: {missing}"


# =========================================================================
# §7 validation matrix — the pure validator
# =========================================================================


class TestPatchValidator:
    def test_none_body_is_empty(self):
        assert _validated_officer_post_patch(None) == ({}, None, {})

    def test_non_object_body_400s(self):
        with pytest.raises(HTTPException) as exc:
            _validated_officer_post_patch(["nope"])
        assert exc.value.status_code == 400

    def test_unknown_field_400s(self):
        with pytest.raises(HTTPException) as exc:
            _validated_officer_post_patch({"slotz": {}})
        assert exc.value.status_code == 400
        assert "slotz" in str(exc.value.detail)

    def test_typoed_kit_400s_via_validate_slots_spec(self):
        with pytest.raises(HTTPException) as exc:
            _validated_officer_post_patch({"slots": {"line": {"count": "many"}}})
        assert exc.value.status_code == 400

    def test_valid_kit_lands_under_officer(self):
        fragment, comm, effects = _validated_officer_post_patch(
            {"slots": {"line": {"count": 2, "backend": "sandbox"}}}
        )
        assert fragment["officer"]["slots"]["line"]["count"] == 2
        assert comm is None
        assert effects == {"slots": "next dispatch"}

    def test_int_fields_coerce_and_clamp(self):
        fragment, _, _ = _validated_officer_post_patch(
            {"max_actions_per_wake": "4", "daily_token_ceiling": -5}
        )
        assert fragment["officer"]["max_actions_per_wake"] == 4
        assert fragment["officer"]["daily_token_ceiling"] == 0

    def test_int_garbage_400s(self):
        with pytest.raises(HTTPException) as exc:
            _validated_officer_post_patch({"max_concurrent_workers": "lots"})
        assert exc.value.status_code == 400

    def test_auto_pull_is_strict_boolean_and_null_clears_off(self):
        fragment, _, effects = _validated_officer_post_patch({"auto_pull": True})
        assert fragment == {"officer": {"auto_pull": True}}
        assert effects == {"auto_pull": "next dispatch"}
        fragment, _, _ = _validated_officer_post_patch({"auto_pull": None})
        assert fragment == {"officer": {"auto_pull": False}}
        for bad in (1, "true", [], {}):
            with pytest.raises(HTTPException) as exc:
                _validated_officer_post_patch({"auto_pull": bad})
            assert exc.value.status_code == 400

    @pytest.mark.parametrize("value", [12.5, "12.5"])
    def test_worker_spend_ceiling_is_optional_positive_usd(self, value):
        fragment, _, effects = _validated_officer_post_patch(
            {"worker_spend_ceiling_daily": value}
        )
        assert fragment == {"officer": {"worker_spend_ceiling_daily": 12.5}}
        assert effects == {"worker_spend_ceiling_daily": "next dispatch"}
        fragment, _, _ = _validated_officer_post_patch(
            {"worker_spend_ceiling_daily": None}
        )
        assert fragment == {"officer": {"worker_spend_ceiling_daily": None}}

    @pytest.mark.parametrize("value", [0, -1, True, "nan", "inf", "nope"])
    def test_worker_spend_ceiling_rejects_invalid_values(self, value):
        with pytest.raises(HTTPException) as exc:
            _validated_officer_post_patch({"worker_spend_ceiling_daily": value})
        assert exc.value.status_code == 400

    def test_brain_maps_to_llm_fragment(self):
        fragment, _, effects = _validated_officer_post_patch(
            {"brain": {"model": "MiniMax-M3", "reasoning_level": "high"}}
        )
        assert fragment["llm"] == {"model": "MiniMax-M3", "reasoning_level": "high"}
        assert effects["brain"] == "next respawn"

    def test_brain_unknown_key_400s(self):
        with pytest.raises(HTTPException) as exc:
            _validated_officer_post_patch({"brain": {"model": "x", "iq": 200}})
        assert exc.value.status_code == 400

    def test_brain_bad_reasoning_vocabulary_400s(self):
        with pytest.raises(HTTPException) as exc:
            _validated_officer_post_patch({"brain": {"reasoning_level": "galaxy"}})
        assert exc.value.status_code == 400

    def test_brain_empty_or_non_object_400s(self):
        for bad in ({}, "MiniMax-M3"):
            with pytest.raises(HTTPException) as exc:
                _validated_officer_post_patch({"brain": bad})
            assert exc.value.status_code == 400

    def test_communication_policy_is_separate_row_only(self):
        fragment, comm, effects = _validated_officer_post_patch(
            {
                "communication_policy": {
                    "worker_messages": "officer_first",
                    "officer_response_minutes": 15,
                }
            }
        )
        # Never enters the config fragment that gets mirrored to the thread.
        assert fragment == {}
        assert comm == {
            "worker_messages": "officer_first",
            "officer_response_minutes": 15,
        }
        assert effects["communication_policy"] == "next worker message"

    def test_communication_policy_vocabulary_400s(self):
        with pytest.raises(HTTPException) as exc:
            _validated_officer_post_patch(
                {"communication_policy": {"worker_messages": "carrier_pigeon"}}
            )
        assert exc.value.status_code == 400

    @pytest.mark.parametrize("minutes", [4, 121, 0, -1, "soon"])
    def test_officer_response_minutes_bounds_400(self, minutes):
        with pytest.raises(HTTPException) as exc:
            _validated_officer_post_patch(
                {"communication_policy": {"officer_response_minutes": minutes}}
            )
        assert exc.value.status_code == 400

    @pytest.mark.parametrize("minutes", [5, 120])
    def test_officer_response_minutes_bounds_ok(self, minutes):
        _, comm, _ = _validated_officer_post_patch(
            {"communication_policy": {"officer_response_minutes": minutes}}
        )
        assert comm == {"officer_response_minutes": minutes}

    def test_communication_policy_unknown_key_400s(self):
        with pytest.raises(HTTPException) as exc:
            _validated_officer_post_patch(
                {"communication_policy": {"smoke_signals": True}}
            )
        assert exc.value.status_code == 400

    def test_effect_labels_cover_the_whole_table(self):
        body = {
            "slots": {"line": {"count": 1}},
            "auto_pull": False,
            "worker_spend_ceiling_daily": 12.5,
            "max_concurrent_workers": 2,
            "daily_token_ceiling": 1,
            "sleep_min_minutes": 5,
            "sleep_max_minutes": 60,
            "max_actions_per_wake": 3,
            "brain": {"model": "m"},
        }
        _, _, effects = _validated_officer_post_patch(body)
        assert effects == {
            "slots": "next dispatch",
            "auto_pull": "next dispatch",
            "worker_spend_ceiling_daily": "next dispatch",
            "max_concurrent_workers": "next dispatch",
            "daily_token_ceiling": "next delivery",
            "sleep_min_minutes": "next sleep filing + watchdog immediately",
            "sleep_max_minutes": "next sleep filing + watchdog immediately",
            "max_actions_per_wake": "next respawn",
            "brain": "next respawn",
        }

    # --- null-as-clear (O5 card contract): explicit null reverts a field ---

    def test_slots_null_clears_to_flat_cap(self):
        fragment, _, effects = _validated_officer_post_patch({"slots": None})
        assert fragment == {"officer": {"slots": None}}
        assert effects == {"slots": "next dispatch"}

    def test_int_field_null_clears_to_default(self):
        fragment, _, _ = _validated_officer_post_patch({"daily_token_ceiling": None})
        assert fragment == {"officer": {"daily_token_ceiling": None}}

    def test_brain_null_clears_both_overrides(self):
        fragment, _, _ = _validated_officer_post_patch({"brain": None})
        assert fragment == {"llm": {"model": None, "reasoning_level": None}}

    def test_brain_member_null_clears_that_member(self):
        fragment, _, _ = _validated_officer_post_patch(
            {"brain": {"model": None, "reasoning_level": "high"}}
        )
        assert fragment == {"llm": {"model": None, "reasoning_level": "high"}}

    def test_communication_policy_null_is_refused_with_an_explanation(self):
        with pytest.raises(HTTPException) as exc:
            _validated_officer_post_patch({"communication_policy": None})
        assert exc.value.status_code == 400
        assert "cannot be cleared" in str(exc.value.detail)


class TestSleepBounds:
    def test_min_over_standing_max_400s(self):
        with pytest.raises(HTTPException) as exc:
            _check_officer_sleep_bounds(
                {"sleep_max_minutes": 60}, {"sleep_min_minutes": 90}
            )
        assert exc.value.status_code == 400

    def test_max_under_standing_min_400s(self):
        with pytest.raises(HTTPException) as exc:
            _check_officer_sleep_bounds(
                {"sleep_min_minutes": 10}, {"sleep_max_minutes": 5}
            )
        assert exc.value.status_code == 400

    def test_defaults_apply_when_nothing_stands(self):
        # Default min is 5 — a lone max of 4 violates it.
        with pytest.raises(HTTPException):
            _check_officer_sleep_bounds({}, {"sleep_max_minutes": 4})

    def test_consistent_pair_passes(self):
        _check_officer_sleep_bounds(
            {}, {"sleep_min_minutes": 10, "sleep_max_minutes": 20}
        )
        _check_officer_sleep_bounds(
            {"sleep_max_minutes": 60}, {"sleep_min_minutes": 60}
        )

    def test_no_sleep_keys_never_checks(self):
        _check_officer_sleep_bounds(
            {"sleep_min_minutes": 90, "sleep_max_minutes": 6}, {}
        )

    def test_null_clear_reverts_to_the_default_bound(self):
        # Clearing min (null → default 5) while the row max stands at 60: fine.
        _check_officer_sleep_bounds(
            {"sleep_max_minutes": 60}, {"sleep_min_minutes": None}
        )
        # Clearing max (null → default 60) against a standing min of 90: 400.
        with pytest.raises(HTTPException):
            _check_officer_sleep_bounds(
                {"sleep_min_minutes": 90}, {"sleep_max_minutes": None}
            )


# =========================================================================
# GET card shape helpers (O5 contract)
# =========================================================================


class TestEditorBlock:
    def test_seeds_every_editor_field_from_config(self):
        block = officer_editor_block(
            {
                "slots": {"line": {"count": 2}},
                "sleep_min_minutes": 10,
                "sleep_max_minutes": 45,
                "daily_token_ceiling": 500000,
                "max_actions_per_wake": 6,
                "max_concurrent_workers": 3,
            },
            {"model": "MiniMax-M3", "reasoning_level": "high"},
        )
        assert block["model"] == "MiniMax-M3"
        assert block["reasoning_level"] == "high"
        assert block["slots"] == {"line": {"count": 2}}
        assert block["sleep_minutes"] == {"min": 10, "max": 45}
        assert block["sleep_min_minutes"] == 10
        assert block["sleep_max_minutes"] == 45
        assert block["daily_token_ceiling"] == 500000
        assert block["max_actions_per_wake"] == 6
        assert block["max_concurrent_workers"] == 3

    def test_unset_numerics_stay_null_not_materialized(self):
        block = officer_editor_block({}, {})
        assert block["daily_token_ceiling"] is None
        assert block["max_actions_per_wake"] is None
        assert block["max_concurrent_workers"] is None
        assert block["slots"] is None
        assert block["model"] is None
        # The display pair still carries the effective defaults.
        assert block["sleep_minutes"] == {"min": 5, "max": 60}


class TestWhileVacantView:
    def test_maps_description_to_title_and_counts_dropped(self):
        view = while_vacant_view(
            {
                "while_vacant": [
                    {"job_id": "j1", "status": "completed", "description": "d1"},
                    {"job_id": "j2", "status": "failed", "title": "explicit"},
                ],
                "while_vacant_dropped": 3,
            }
        )
        assert view["entries"][0]["title"] == "d1"
        assert view["entries"][1]["title"] == "explicit"
        assert view["dropped"] == 3

    def test_empty_or_garbage_state_is_the_empty_shape(self):
        assert while_vacant_view(None) == {"entries": [], "dropped": 0}
        assert while_vacant_view({"while_vacant": "nope"}) == {
            "entries": [],
            "dropped": 0,
        }


# =========================================================================
# Endpoint harness — main globals monkeypatched
# =========================================================================


def _post_row(**over):
    row = {
        "project_id": PROJECT_ID,
        "thread_id": None,
        "config_override": {},
        "communication_policy": {
            "worker_messages": "user_direct",
            "officer_response_minutes": 15,
        },
        "state": {},
        "incarnations": [],
        "updated_at": datetime(2026, 8, 15, 12, 0, tzinfo=timezone.utc),
    }
    row.update(over)
    return row


def _officer_thread(**over):
    t = {
        "id": THREAD_ID,
        "status": "active",
        "project_id": PROJECT_ID,
        "agent_id": None,
        "runtime_generation": RETIREMENT_GENERATION,
        "runtime_retirement_token": None,
        "metadata": {"config_override": {"officer": {"enabled": True}}},
    }
    t.update(over)
    return t


@pytest.fixture
def db(monkeypatch):
    db = MagicMock()
    db.get_or_create_project_officer = AsyncMock(return_value=_post_row())
    db.get_project_officer = AsyncMock(return_value=_post_row())
    db.get_officer_thread_for_project = AsyncMock(return_value=None)
    db.merge_project_officer_config = AsyncMock(return_value=_post_row())
    db.merge_project_officer_communication_policy = AsyncMock(return_value=_post_row())
    db.merge_project_officer_state = AsyncMock(return_value=_post_row())
    db.append_project_officer_incarnation = AsyncMock(return_value=_post_row())
    db.clear_project_officer_thread = AsyncMock(return_value=_post_row())
    db.fold_project_officer_wake_queue = AsyncMock(
        return_value={"folded": 0, "deleted": 0, "dropped": 0}
    )
    db.update_project_officer_post = AsyncMock(
        return_value={
            "post": _post_row(),
            "thread": None,
            "applied_to_thread": False,
        }
    )
    db.set_project_officer_hold = AsyncMock(
        return_value={"thread": _officer_thread(), "previous_hold": None, "routes": []}
    )
    db.decommission_project_officer = AsyncMock(
        return_value={
            "transitioned": False,
            "already_decommissioned": False,
            "vacant": True,
            "routes": [],
            "folded": 0,
            "deleted": 0,
            "dropped": 0,
            "harvested": False,
            "incarnation": None,
        }
    )
    db.drain_project_officer_while_vacant = AsyncMock(
        return_value={"entries": [], "dropped": 0}
    )
    db.merge_thread_config_override = AsyncMock(return_value=True)
    db.merge_thread_officer_state = AsyncMock(return_value=True)
    db.enqueue_session_wake_event = AsyncMock(return_value=True)
    db.confirm_project_officer_incarnation = AsyncMock(return_value=True)
    db.get_thread = AsyncMock(return_value=None)
    # The lifecycle fake has no legacy Kubernetes rows. Keep the rollout
    # reconciler's async inventory boundary explicit: a bare MagicMock would
    # manufacture awaitable-looking methods and turn "no candidates" into a
    # false adoption failure before the test reaches its Officer handoff.
    db.list_legacy_pinned_warm_binding_candidates = AsyncMock(return_value=[])
    db.list_expired_pinned_warm_binding_protections = AsyncMock(return_value=[])
    db.list_legacy_pinned_agent_k8s_authority_candidates = AsyncMock(return_value=[])
    # Commissioning is gated on unattended_operations; these tests are about
    # lifecycle mechanics, so the grant is held. TestCommissionCapabilityGate
    # below flips it to False and asserts the refusal.
    db.user_can_run_unattended_operations = AsyncMock(return_value=True)
    # ``_end_thread_flow`` is still main's, and TestEndThreadReroute drives it
    # through this global.
    monkeypatch.setattr(orch_main.app.state.resources, "postgres_db", db)
    return db


@pytest.fixture
def deps(db) -> OfficerPostLifecycleDependencies:
    """The Post's lifecycle collaborators, taken explicitly. Every field is
    bound to what ``main._officer_post_lifecycle_dependencies()`` binds, so an
    unsteered test runs the same collaborator the application does; a test
    steers ``create_thread`` / ``end_thread_flow`` / the recycler / the release
    fence HERE, because rebinding them on ``orchestrator.main`` no longer
    reaches the operation."""
    return OfficerPostLifecycleDependencies(
        store=db,
        persistent_provisioner=persistent_provisioner_module.persistent_provisioner,
        persistent_thread_recycler=orch_main.app.state.resources.persistent_thread_recycler,
        policy=OfficerPostPolicyDependencies(
            auto_pull_release_enabled=(
                lambda: orch_main.app.state.resources.settings.officer_auto_pull_release_enabled
            )
        ),
        kick_officer_event_drain=session_wake_module.kick_event_drain,
        deliver_officer_note=session_wake_module.deliver_officer_note,
        create_thread=bound(
            thread_admission_module.create_thread,
            sessions_composition.thread_admission_dependencies,
            orch_main.app.state.resources,
        ),
        end_thread_flow=controls_composition.thread_retirement_operations(
            orch_main.app.state.resources
        ).end_thread_flow,
    )


@pytest.fixture
def req(deps):
    """A request whose application resolves the Post's dependencies, so the
    route declarations — and the owner gate each one calls — stay live."""
    request = MagicMock()
    request.app.state.officer_post_lifecycle_dependencies_factory = lambda: deps
    return request


@pytest.fixture
def as_project_admin(monkeypatch):
    gate = AsyncMock(
        return_value=({"id": str(uuid4()), "is_admin": True}, {"name": "Throwaway"})
    )
    monkeypatch.setattr(officers_router, "require_project_owner", gate)
    return gate


@pytest.fixture
def quiet_side_channels(monkeypatch, deps):
    monkeypatch.setattr(
        officer_notices, "inject_officer_notice", AsyncMock(return_value=False)
    )
    deps.kick_officer_event_drain = MagicMock()


class TestAdminGate:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "call",
        [
            lambda request: commission_project_officer(request, PROJECT_ID, None),
            lambda request: decommission_project_officer(request, PROJECT_ID, None),
            lambda request: hold_project_officer(request, PROJECT_ID, None),
            lambda request: release_project_officer(request, PROJECT_ID),
            lambda request: recycle_project_officer(request, PROJECT_ID),
            lambda request: patch_project_officer(
                request, PROJECT_ID, {"max_actions_per_wake": 1}
            ),
        ],
    )
    async def test_every_endpoint_sits_behind_project_owner(
        self, monkeypatch, db, req, call
    ):
        gate = AsyncMock(
            side_effect=HTTPException(status_code=403, detail="Project owner required")
        )
        monkeypatch.setattr(officers_router, "require_project_owner", gate)
        with pytest.raises(HTTPException) as exc:
            await call(req)
        assert exc.value.status_code == 403
        gate.assert_awaited_once()
        db.merge_project_officer_config.assert_not_awaited()
        db.merge_thread_config_override.assert_not_awaited()


class TestHoldRelease:
    @pytest.mark.asyncio
    async def test_hold_on_vacant_post_400s(
        self, db, as_project_admin, quiet_side_channels, req
    ):
        with pytest.raises(HTTPException) as exc:
            await hold_project_officer(req, PROJECT_ID, None)
        assert exc.value.status_code == 400
        db.merge_thread_config_override.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_release_on_vacant_post_400s(
        self, db, as_project_admin, quiet_side_channels, req
    ):
        with pytest.raises(HTTPException) as exc:
            await release_project_officer(req, PROJECT_ID)
        assert exc.value.status_code == 400

    @pytest.mark.asyncio
    async def test_hold_stamps_maintenance_without_thread_id(
        self, db, as_project_admin, quiet_side_channels, req
    ):
        db.get_officer_thread_for_project = AsyncMock(return_value=_officer_thread())
        out = await hold_project_officer(
            req, PROJECT_ID, OfficerHoldRequest(note="quarterly maintenance")
        )
        assert out["status"] == "held"
        db.set_project_officer_hold.assert_awaited_once()
        args, kwargs = db.set_project_officer_hold.await_args
        assert args == (PROJECT_ID,)
        assert kwargs["expected_thread_id"] == THREAD_ID
        assert kwargs["route_reason"] == "officer_hold"
        hold = kwargs["hold"]
        assert hold["kind"] == "maintenance"
        assert hold["note"] == "quarterly maintenance"
        assert hold["since"]
        # The load-bearing absence: a thread_id key is what the watchdog's
        # stale-conference self-heal keys on — maintenance must never carry it.
        assert "thread_id" not in hold

    @pytest.mark.asyncio
    async def test_second_hold_400s_and_does_not_clobber(
        self, db, as_project_admin, quiet_side_channels, req
    ):
        held = _officer_thread(
            metadata={
                "config_override": {
                    "officer": {"enabled": True, "hold": {"kind": "maintenance"}}
                }
            }
        )
        db.get_officer_thread_for_project = AsyncMock(return_value=held)
        with pytest.raises(HTTPException) as exc:
            await hold_project_officer(req, PROJECT_ID, None)
        assert exc.value.status_code == 400
        db.merge_thread_config_override.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_release_clears_via_json_null_and_kicks_drain(
        self, db, as_project_admin, req, deps, monkeypatch
    ):
        monkeypatch.setattr(
            officer_notices, "inject_officer_notice", AsyncMock(return_value=True)
        )
        kick = MagicMock()
        deps.kick_officer_event_drain = kick
        held = _officer_thread(
            metadata={
                "config_override": {
                    "officer": {
                        "enabled": True,
                        "hold": {"kind": "maintenance", "since": "x", "note": ""},
                    }
                }
            }
        )
        db.get_officer_thread_for_project = AsyncMock(return_value=held)
        out = await release_project_officer(req, PROJECT_ID)
        assert out["status"] == "released"
        db.set_project_officer_hold.assert_awaited_once_with(
            PROJECT_ID,
            expected_thread_id=THREAD_ID,
            hold=None,
        )
        kick.assert_called_once()

    @pytest.mark.asyncio
    async def test_release_when_not_held_400s(
        self, db, as_project_admin, quiet_side_channels, req
    ):
        db.get_officer_thread_for_project = AsyncMock(return_value=_officer_thread())
        with pytest.raises(HTTPException) as exc:
            await release_project_officer(req, PROJECT_ID)
        assert exc.value.status_code == 400
        db.merge_thread_config_override.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_generic_release_cannot_clear_recycler_owned_hold(
        self, db, as_project_admin, quiet_side_channels, req
    ):
        held = _officer_thread(
            metadata={
                "config_override": {
                    "officer": {
                        "enabled": True,
                        "hold": {
                            "kind": "maintenance",
                            "_persistent_recycle_generation": "server-owned",
                        },
                    }
                }
            }
        )
        db.get_officer_thread_for_project = AsyncMock(return_value=held)
        with pytest.raises(HTTPException) as exc:
            await release_project_officer(req, PROJECT_ID)
        assert exc.value.status_code == 409
        db.set_project_officer_hold.assert_not_awaited()


class TestOfficerRecycle:
    @pytest.mark.asyncio
    async def test_owner_action_delegates_to_shared_recycler(
        self, db, as_project_admin, req, deps
    ):
        db.get_officer_thread_for_project = AsyncMock(return_value=_officer_thread())
        recycler = MagicMock()
        recycler.observe = AsyncMock(return_value=None)
        recycler.request_and_reconcile = AsyncMock(
            return_value=PersistentRecycleResult(
                THREAD_ID, "recycling", "provisioning", "generation-hidden"
            )
        )
        provisioner = MagicMock(is_available=True, expected_build_sha="new")
        deps.persistent_thread_recycler = recycler
        deps.persistent_provisioner = provisioner

        result = await recycle_project_officer(req, PROJECT_ID)

        assert result == {
            "thread_id": THREAD_ID,
            "state": "recycling",
            "phase": "provisioning",
            "failure_class": None,
        }
        assert "generation" not in result
        recycler.request_and_reconcile.assert_awaited_once_with(
            thread_id=THREAD_ID,
            reason="operator_requested",
            expected_build_sha="new",
            observation=None,
            expected_project_id=PROJECT_ID,
        )


class TestPatchEndpoint:
    @pytest.mark.asyncio
    async def test_release_gate_refuses_true_before_any_post_write(
        self, db, as_project_admin, quiet_side_channels, req, deps
    ):
        deps.policy.auto_pull_release_enabled = lambda: False
        with pytest.raises(HTTPException) as exc:
            await patch_project_officer(req, PROJECT_ID, {"auto_pull": True})
        assert exc.value.status_code == 409
        assert "not released" in str(exc.value.detail)
        db.update_project_officer_post.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_release_gate_always_permits_disable_and_spend_edits(
        self, db, as_project_admin, quiet_side_channels, req, deps
    ):
        deps.policy.auto_pull_release_enabled = lambda: False
        await patch_project_officer(
            req,
            PROJECT_ID,
            {"auto_pull": False, "worker_spend_ceiling_daily": 19.5},
        )
        db.update_project_officer_post.assert_awaited_once_with(
            PROJECT_ID,
            config_updates={
                "officer": {
                    "auto_pull": False,
                    "worker_spend_ceiling_daily": 19.5,
                }
            },
            communication_policy_patch=None,
        )

    @pytest.mark.asyncio
    async def test_released_enable_mirrors_all_three_control_layers(
        self, db, as_project_admin, quiet_side_channels, req, deps
    ):
        deps.policy.auto_pull_release_enabled = lambda: True
        db.update_project_officer_post = AsyncMock(
            return_value={
                "post": _post_row(
                    thread_id=THREAD_ID,
                    config_override={
                        "officer": {
                            "auto_pull": True,
                            "worker_spend_ceiling_daily": 19.5,
                            "slots": {
                                "research": {
                                    "count": 1,
                                    "category": "researcher",
                                    "spend_ceiling_daily": 7.25,
                                }
                            },
                        }
                    },
                ),
                "thread": _officer_thread(),
                "applied_to_thread": True,
            }
        )
        out = await patch_project_officer(
            req,
            PROJECT_ID,
            {
                "auto_pull": True,
                "worker_spend_ceiling_daily": 19.5,
                "slots": {
                    "research": {
                        "count": 1,
                        "category": "researcher",
                        "spend_ceiling_daily": 7.25,
                    }
                },
            },
        )
        fragment = db.update_project_officer_post.await_args.kwargs["config_updates"]
        assert fragment["officer"] == {
            "auto_pull": True,
            "worker_spend_ceiling_daily": 19.5,
            "slots": {
                "research": {
                    "count": 1,
                    "category": "researcher",
                    "spend_ceiling_daily": 7.25,
                }
            },
        }
        assert out["effects"] == {
            "auto_pull": "next dispatch",
            "worker_spend_ceiling_daily": "next dispatch",
            "slots": "next dispatch",
        }

    @pytest.mark.asyncio
    async def test_vacant_post_writes_the_row_only(
        self, db, as_project_admin, quiet_side_channels, req
    ):
        out = await patch_project_officer(req, PROJECT_ID, {"daily_token_ceiling": 5})
        db.update_project_officer_post.assert_awaited_once_with(
            PROJECT_ID,
            config_updates={"officer": {"daily_token_ceiling": 5}},
            communication_policy_patch=None,
        )
        db.merge_thread_config_override.assert_not_awaited()
        assert out["commissioned"] is False
        assert out["applied_to_thread"] is False
        assert out["effects"] == {"daily_token_ceiling": "next delivery"}

    @pytest.mark.asyncio
    async def test_commissioned_post_mirrors_to_thread_and_notices_not_wakes(
        self, db, as_project_admin, req, monkeypatch
    ):
        notice = AsyncMock(return_value=True)
        monkeypatch.setattr(officer_notices, "inject_officer_notice", notice)
        officer = _officer_thread()
        db.update_project_officer_post = AsyncMock(
            return_value={
                "post": _post_row(
                    thread_id=THREAD_ID,
                    config_override={
                        "officer": {"slots": {"line": {"count": 1}}},
                        "llm": {"reasoning_level": "high"},
                    },
                ),
                "thread": officer,
                "applied_to_thread": True,
            }
        )
        out = await patch_project_officer(
            req,
            PROJECT_ID,
            {"slots": {"line": {"count": 1}}, "brain": {"reasoning_level": "high"}},
        )
        db.update_project_officer_post.assert_awaited_once()
        _, kwargs = db.update_project_officer_post.await_args
        fragment = kwargs["config_updates"]
        assert fragment["officer"]["slots"]["line"]["count"] == 1
        assert fragment["llm"]["reasoning_level"] == "high"
        notice.assert_awaited_once()
        # Deliberately NOT a wake (§7) — no event enqueued.
        db.enqueue_session_wake_event.assert_not_awaited()
        assert out["applied_to_thread"] is True
        assert out["effects"]["brain"] == "next respawn"

    @pytest.mark.asyncio
    async def test_communication_policy_is_never_mirrored_to_the_thread(
        self, db, as_project_admin, quiet_side_channels, req
    ):
        await patch_project_officer(
            req,
            PROJECT_ID,
            {"communication_policy": {"worker_messages": "officer_first"}},
        )
        db.update_project_officer_post.assert_awaited_once_with(
            PROJECT_ID,
            config_updates=None,
            communication_policy_patch={"worker_messages": "officer_first"},
        )
        db.merge_project_officer_config.assert_not_awaited()
        db.merge_thread_config_override.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_empty_body_400s(
        self, db, as_project_admin, quiet_side_channels, req
    ):
        with pytest.raises(HTTPException) as exc:
            await patch_project_officer(req, PROJECT_ID, {})
        assert exc.value.status_code == 400

    @pytest.mark.asyncio
    async def test_min_over_max_400s_against_the_standing_row(
        self, db, as_project_admin, quiet_side_channels, req
    ):
        db.get_or_create_project_officer = AsyncMock(
            return_value=_post_row(
                config_override={"officer": {"sleep_max_minutes": 30}}
            )
        )
        with pytest.raises(HTTPException) as exc:
            await patch_project_officer(req, PROJECT_ID, {"sleep_min_minutes": 45})
        assert exc.value.status_code == 400
        db.merge_project_officer_config.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_shrinking_below_in_flight_is_allowed(
        self, db, as_project_admin, quiet_side_channels, req
    ):
        """Drain semantics, decided (§7): the 409 lives at the next dispatch,
        never at the form save."""
        db.get_officer_thread_for_project = AsyncMock(return_value=_officer_thread())
        out = await patch_project_officer(
            req, PROJECT_ID, {"slots": {"line": {"count": 0}}}
        )
        assert out["status"] == "updated"


class TestCommissionCapabilityGate:
    """Commissioning is gated on the `unattended_operations` capability grant
    (knowledge-history/done/unattended_operations_grant.md). The config PDP refuses
    `officer.enabled` downstream too, but only after the kit has been written —
    so the point of this gate is that it fires FIRST and touches nothing."""

    @pytest.mark.asyncio
    async def test_missing_grant_403s_before_anything_mutates(
        self, db, as_project_admin, quiet_side_channels, req, deps
    ):
        create = AsyncMock()
        deps.create_thread = create
        db.user_can_run_unattended_operations = AsyncMock(return_value=False)

        with pytest.raises(HTTPException) as exc:
            await commission_project_officer(req, PROJECT_ID, None)

        assert exc.value.status_code == 403
        assert "unattended_operations" in str(exc.value.detail)
        create.assert_not_awaited()
        db.update_project_officer_post.assert_not_awaited()
        db.merge_project_officer_config.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_grant_is_resolved_against_this_project(
        self, db, as_project_admin, quiet_side_channels, req, deps
    ):
        """Project scope is the axis an operator most wants ("this team may run
        officers, that one may not"), so the project id must reach the read —
        dropping it would silently reduce the key to a user-only capability."""
        deps.create_thread = AsyncMock()
        db.user_can_run_unattended_operations = AsyncMock(return_value=False)

        with pytest.raises(HTTPException):
            await commission_project_officer(req, PROJECT_ID, None)

        _user, project_id = db.user_can_run_unattended_operations.await_args.args
        assert project_id == PROJECT_ID


class TestCommissionEndpoint:
    @pytest.mark.asyncio
    async def test_dark_release_gate_refuses_commission_true_without_mutation(
        self, db, as_project_admin, quiet_side_channels, req, deps
    ):
        deps.policy.auto_pull_release_enabled = lambda: False
        create = AsyncMock()
        deps.create_thread = create
        with pytest.raises(HTTPException) as exc:
            await commission_project_officer(req, PROJECT_ID, {"auto_pull": True})
        assert exc.value.status_code == 409
        create.assert_not_awaited()
        db.get_or_create_project_officer.assert_not_awaited()
        db.update_project_officer_post.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_dark_release_gate_refuses_recommission_of_standing_true(
        self, db, as_project_admin, quiet_side_channels, req, deps
    ):
        deps.policy.auto_pull_release_enabled = lambda: False
        create = AsyncMock()
        deps.create_thread = create
        db.get_or_create_project_officer = AsyncMock(
            return_value=_post_row(config_override={"officer": {"auto_pull": True}})
        )
        with pytest.raises(HTTPException) as exc:
            await commission_project_officer(req, PROJECT_ID, None)
        assert exc.value.status_code == 409
        create.assert_not_awaited()
        db.update_project_officer_post.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("legacy_true", ["true", "True", 1])
    async def test_dark_release_gate_refuses_legacy_truthy_recommission(
        self, db, as_project_admin, quiet_side_channels, legacy_true, req, deps
    ):
        deps.policy.auto_pull_release_enabled = lambda: False
        create = AsyncMock()
        deps.create_thread = create
        db.get_or_create_project_officer = AsyncMock(
            return_value=_post_row(
                config_override={"officer": {"auto_pull": legacy_true}}
            )
        )
        with pytest.raises(HTTPException) as exc:
            await commission_project_officer(req, PROJECT_ID, None)
        assert exc.value.status_code == 409
        create.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_standing_officer_409s_before_any_create(
        self, db, as_project_admin, quiet_side_channels, req, deps
    ):
        create = AsyncMock()
        deps.create_thread = create
        db.get_officer_thread_for_project = AsyncMock(return_value=_officer_thread())
        with pytest.raises(HTTPException) as exc:
            await commission_project_officer(req, PROJECT_ID, None)
        assert exc.value.status_code == 409
        create.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_released_commission_carries_auto_pull_and_spend_layers(
        self, db, as_project_admin, quiet_side_channels, req, deps
    ):
        deps.policy.auto_pull_release_enabled = lambda: True
        new_tid = str(uuid4())

        async def create_with_continuity(create_request, _request):
            create_request._officer_commission_result = {
                "brief_enqueued": True,
                "while_vacant": [],
                "while_vacant_dropped": 0,
                "state_restored": False,
            }
            return {"thread_id": new_tid, "status": "created"}

        create = AsyncMock(side_effect=create_with_continuity)
        deps.create_thread = create
        original = _post_row(config_override={"officer": {}})
        commissioned_config = {
            "officer": {
                "auto_pull": True,
                "worker_spend_ceiling_daily": 20.0,
                "slots": {
                    "research": {
                        "count": 1,
                        "category": "researcher",
                        "spend_ceiling_daily": 8.5,
                    }
                },
            }
        }
        updated = _post_row(config_override=commissioned_config)
        db.get_or_create_project_officer = AsyncMock(return_value=original)
        db.update_project_officer_post = AsyncMock(
            return_value={
                "post": updated,
                "thread": None,
                "applied_to_thread": False,
            }
        )

        await commission_project_officer(
            req,
            PROJECT_ID,
            {
                "auto_pull": True,
                "worker_spend_ceiling_daily": 20,
                "slots": {
                    "research": {
                        "count": 1,
                        "category": "researcher",
                        "spend_ceiling_daily": 8.5,
                    }
                },
            },
        )

        db.update_project_officer_post.assert_awaited_once_with(
            PROJECT_ID,
            config_updates=commissioned_config,
            communication_policy_patch=None,
            expected_vacant_updated_at=original["updated_at"],
        )
        create_request = create.await_args.args[0]
        officer = create_request.config_override["officer"]
        assert "auto_pull" not in officer
        assert "worker_spend_ceiling_daily" not in officer
        assert "slots" not in officer
        assert create_request._officer_post_config_snapshot == commissioned_config

    @pytest.mark.asyncio
    async def test_bad_kit_400s_before_any_create(
        self, db, as_project_admin, quiet_side_channels, req, deps
    ):
        create = AsyncMock()
        deps.create_thread = create
        with pytest.raises(HTTPException) as exc:
            await commission_project_officer(
                req, PROJECT_ID, {"slots": {"line": {"count": "many"}}}
            )
        assert exc.value.status_code == 400
        create.assert_not_awaited()
        db.merge_project_officer_config.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_commission_goes_through_the_funnel_with_the_rows_kit(
        self, db, as_project_admin, quiet_side_channels, req, deps
    ):
        new_tid = str(uuid4())
        continuity = {
            "brief_enqueued": True,
            "while_vacant": [{"job_id": "j1", "status": "completed"}],
            "while_vacant_dropped": 2,
            "state_restored": True,
            "payload": {"vacant_since": "2026-08-10T00:00:00+00:00"},
        }

        async def create_with_continuity(create_request, _request):
            create_request._officer_commission_result = continuity
            return {"thread_id": new_tid, "status": "created"}

        create = AsyncMock(side_effect=create_with_continuity)
        deps.create_thread = create
        row = _post_row(
            config_override={
                "officer": {
                    "enabled": True,
                    "conference": False,
                    "slots": {"line": {"count": 2}},
                    "sleep_min_minutes": 5,
                },
                "llm": {"model": "MiniMax-M3", "reasoning_level": "high"},
                "workspace": {"backend": "sandbox"},
                "interactive": {"permission_mode": "autonomous"},
            },
            state={"sitrep_fingerprints": {"j": "fp"}, "while_vacant": [{"x": 1}]},
            incarnations=[
                {
                    "thread_id": str(uuid4()),
                    "decommissioned_at": "2026-08-10T00:00:00+00:00",
                }
            ],
        )
        db.get_or_create_project_officer = AsyncMock(return_value=row)
        db.update_project_officer_post = AsyncMock(
            return_value={"post": row, "thread": None, "applied_to_thread": False}
        )
        db.get_project_officer = AsyncMock(return_value=row)
        db.drain_project_officer_while_vacant = AsyncMock(
            return_value={
                "entries": [{"job_id": "j1", "status": "completed"}],
                "dropped": 2,
            }
        )

        body = {"max_actions_per_wake": 4}
        out = await commission_project_officer(req, PROJECT_ID, body)

        # Body validated + merged into the row under the post lock FIRST.
        db.update_project_officer_post.assert_awaited_once_with(
            PROJECT_ID,
            config_updates={"officer": {"max_actions_per_wake": 4}},
            communication_policy_patch=None,
            expected_vacant_updated_at=row["updated_at"],
        )
        # One funnel: the create-thread endpoint, with the row's kit.
        create_request = create.await_args.args[0]
        assert create_request.project_id == PROJECT_ID
        assert create_request.title == "Centurion — Throwaway"
        assert create_request.model == "MiniMax-M3"
        assert create_request.reasoning_level == "high"
        assert create_request.permission_mode == "autonomous"
        # The expert IS the job surface. Without an explicit config_name the
        # request falls to session_base and the officer boots with NO
        # job_control plane — he cannot dispatch, steer, approve or read
        # evidence. Found live on the Resavio change of command 2026-08-15:
        # the endpoint-commissioned officer had 34 tools, none of which could
        # create a job.
        assert create_request.config_name == "centurion"
        # An officer is headless: a supervised gate can never be answered, so
        # every tool call parks the turn and he executes nothing at all. The
        # row pinned "autonomous" here, so it travels; the absent case is
        # covered by test_headless_officer_defaults_to_autonomous below.
        officer_frag = create_request.config_override["officer"]
        assert officer_frag["enabled"] is True
        assert "conference" not in officer_frag
        assert "slots" not in officer_frag
        assert create_request._officer_post_config_snapshot["officer"]["slots"][
            "line"
        ] == {"count": 2}
        assert create_request.config_override["workspace"] == {"backend": "sandbox"}

        # Continuity was already restored/drained/enqueued inside registration's
        # post-locked transaction. The endpoint performs no split follow-up writes.
        assert create_request._officer_commission_result == continuity
        db.merge_thread_officer_state.assert_not_awaited()
        db.drain_project_officer_while_vacant.assert_not_awaited()
        db.enqueue_session_wake_event.assert_not_awaited()
        db.confirm_project_officer_incarnation.assert_awaited_once_with(
            PROJECT_ID, new_tid
        )
        assert out["status"] == "commissioned"
        assert out["thread_id"] == new_tid
        assert out["brief_enqueued"] is True
        assert out["while_vacant"] == 1

    @pytest.mark.asyncio
    async def test_headless_officer_defaults_to_autonomous(
        self, db, as_project_admin, quiet_side_channels, req, deps
    ):
        """A post that pins no permission mode must NOT inherit the create
        endpoint's ``supervised`` default.

        An officer runs headless — there is no session in which a human could
        answer a permission prompt — so a supervised gate parks every turn on
        its first tool call and strips the rest as orphans. He then cycles
        forever executing nothing: no reads, no dispatches, no sleep filed,
        empty assistant text, zero tool results. Observed live on the Resavio
        change of command 2026-08-15, where it read as a model defect rather
        than a config one. An explicitly pinned mode still wins.
        """
        new_tid = str(uuid4())

        async def create_with_continuity(create_request, _request):
            create_request._officer_commission_result = {
                "brief_enqueued": True,
                "while_vacant": [],
                "while_vacant_dropped": 0,
                "state_restored": False,
            }
            return {"thread_id": new_tid, "status": "created"}

        create = AsyncMock(side_effect=create_with_continuity)
        deps.create_thread = create
        # No "interactive" block at all — the shape a fresh post has.
        row = _post_row(config_override={"officer": {"enabled": True}})
        db.get_or_create_project_officer = AsyncMock(return_value=row)
        db.update_project_officer_post = AsyncMock(
            return_value={"post": row, "thread": None, "applied_to_thread": False}
        )
        db.get_project_officer = AsyncMock(return_value=row)

        await commission_project_officer(req, PROJECT_ID, None)

        create_request = create.await_args.args[0]
        assert create_request.permission_mode == "autonomous"
        assert create_request.config_name == "centurion"

    @pytest.mark.asyncio
    async def test_commission_asks_the_funnel_for_connector_defaults(
        self, db, as_project_admin, quiet_side_channels, req, deps
    ):
        """The third field this endpoint forgot, after config_name and
        permission_mode.

        It hand-builds the funnel request, so anything the cockpit supplies and
        this call site does not is silently absent. Without the flag the post
        falls to ``omitted_compat`` and persists ``datasource_ids: []`` — and an
        empty list is authoritative on the inheritance path, so it is not a
        blank to be filled in later but a standing instruction to attach
        nothing. Found live on Better Resavio 2026-08-15: the officer's workers
        came up with no repository checkout and he idled a whole watch.
        """
        new_tid = str(uuid4())

        async def create_with_continuity(create_request, _request):
            create_request._officer_commission_result = {
                "brief_enqueued": True,
                "while_vacant": [],
                "while_vacant_dropped": 0,
                "state_restored": False,
            }
            return {"thread_id": new_tid, "status": "created"}

        create = AsyncMock(side_effect=create_with_continuity)
        deps.create_thread = create
        row = _post_row(config_override={"officer": {"enabled": True}})
        db.get_or_create_project_officer = AsyncMock(return_value=row)
        db.update_project_officer_post = AsyncMock(
            return_value={"post": row, "thread": None, "applied_to_thread": False}
        )
        db.get_project_officer = AsyncMock(return_value=row)

        await commission_project_officer(req, PROJECT_ID, None)

        create_request = create.await_args.args[0]
        assert create_request.use_datasource_defaults is True
        assert "datasource_ids" not in create_request.model_fields_set

    @pytest.mark.asyncio
    async def test_cleared_row_fields_do_not_travel_to_the_funnel(
        self, db, as_project_admin, quiet_side_channels, req, deps
    ):
        """After a PATCH null-clear the row carries JSON nulls; commission
        must drop them — the funnel spells 'default' by omission, and its
        validator 400s on null ints/slots."""
        new_tid = str(uuid4())

        async def create_with_continuity(create_request, _request):
            create_request._officer_commission_result = {
                "brief_enqueued": True,
                "while_vacant": [],
                "while_vacant_dropped": 0,
                "state_restored": False,
            }
            return {"thread_id": new_tid, "status": "created"}

        create = AsyncMock(side_effect=create_with_continuity)
        deps.create_thread = create
        row = _post_row(
            config_override={
                "officer": {
                    "enabled": True,
                    "slots": None,
                    "daily_token_ceiling": None,
                    "max_actions_per_wake": 2,
                },
                "llm": {"model": None, "reasoning_level": None},
            }
        )
        db.get_or_create_project_officer = AsyncMock(return_value=row)
        db.get_project_officer = AsyncMock(return_value=row)
        db.update_project_officer_post = AsyncMock(
            return_value={"post": row, "thread": None, "applied_to_thread": False}
        )

        await commission_project_officer(req, PROJECT_ID, None)

        create_request = create.await_args.args[0]
        officer_frag = create_request.config_override["officer"]
        assert "slots" not in officer_frag
        assert "daily_token_ceiling" not in officer_frag
        assert officer_frag["max_actions_per_wake"] == 2
        assert officer_frag["enabled"] is True
        assert create_request.model is None
        assert create_request.reasoning_level is None

    @pytest.mark.asyncio
    async def test_funnel_409_propagates(
        self, db, as_project_admin, quiet_side_channels, req, deps
    ):
        """The registration claim inside the funnel is the rival authority —
        commission inherits its 409 instead of double-claiming."""
        create = AsyncMock(
            side_effect=HTTPException(status_code=409, detail="already commissioned")
        )
        deps.create_thread = create
        with pytest.raises(HTTPException) as exc:
            await commission_project_officer(req, PROJECT_ID, None)
        assert exc.value.status_code == 409
        db.enqueue_session_wake_event.assert_not_awaited()


class TestDecommissionEndpoint:
    @pytest.mark.asyncio
    async def test_vacant_post_is_an_idempotent_success(
        self, db, as_project_admin, quiet_side_channels, req
    ):
        out = await decommission_project_officer(req, PROJECT_ID, None)
        assert out == {
            "status": "decommissioned",
            "already_vacant": True,
            "incarnations": [],
        }
        db.decommission_project_officer.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_in_flight_jobs_warn_is_a_200_without_force(
        self, db, as_project_admin, quiet_side_channels, req, deps
    ):
        """The warning is a 200 body (O5 card contract) — an error status
        would route to the card's failure path instead of the leave-running
        confirmation flow. Nothing is touched until force."""
        db.get_or_create_project_officer = AsyncMock(
            return_value=_post_row(thread_id=THREAD_ID)
        )
        db.get_thread = AsyncMock(return_value=_officer_thread())
        jobs = [{"job_id": "j1", "status": "processing", "slot": "line", "title": "d"}]
        flow = AsyncMock(
            return_value={
                "status": "in_flight",
                "warning": "1 job(s) in flight; retry with force=true",
                "in_flight_jobs": jobs,
            }
        )
        deps.end_thread_flow = flow
        out = await decommission_project_officer(req, PROJECT_ID, None)
        assert out["status"] == "in_flight"
        assert out["in_flight_jobs"] == jobs
        assert "force=true" in out["warning"]
        flow.assert_awaited_once()
        assert flow.await_args.kwargs["force"] is False
        assert flow.await_args.kwargs["officer_post_required"] is True
        db.clear_project_officer_thread.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_force_proceeds_through_the_end_funnel_leaving_jobs(
        self, db, as_project_admin, quiet_side_channels, req, deps
    ):
        db.get_or_create_project_officer = AsyncMock(
            return_value=_post_row(thread_id=THREAD_ID)
        )
        db.get_thread = AsyncMock(return_value=_officer_thread())
        jobs = [{"job_id": "j1", "status": "processing", "slot": "line", "title": "d"}]
        flow = AsyncMock(
            return_value={
                "status": "ended",
                "_officer_handoff": {"in_flight_jobs": jobs},
            }
        )
        deps.end_thread_flow = flow
        out = await decommission_project_officer(
            req, PROJECT_ID, OfficerDecommissionRequest(force=True)
        )
        flow.assert_awaited_once()
        args, kwargs = flow.await_args
        assert args[0] == THREAD_ID
        assert kwargs["permanent"] is False
        assert kwargs["force"] is True
        assert kwargs["officer_retire_reason"] == "decommissioned"
        # Warned, not cancelled: the jobs ride along in the response.
        assert out["in_flight_jobs"] == jobs
        assert out["status"] == "decommissioned"

    @pytest.mark.asyncio
    async def test_no_jobs_needs_no_force(
        self, db, as_project_admin, quiet_side_channels, req, deps
    ):
        db.get_or_create_project_officer = AsyncMock(
            return_value=_post_row(thread_id=THREAD_ID)
        )
        db.get_thread = AsyncMock(return_value=_officer_thread())
        flow = AsyncMock(
            return_value={
                "status": "ended",
                "_officer_handoff": {"in_flight_jobs": []},
            }
        )
        deps.end_thread_flow = flow
        out = await decommission_project_officer(req, PROJECT_ID, None)
        assert out["status"] == "decommissioned"
        assert flow.await_args.kwargs["officer_retire_reason"] == "decommissioned"

    @pytest.mark.asyncio
    async def test_server_owned_runtime_state_is_not_reported_as_harvested_memory(
        self, db, as_project_admin, quiet_side_channels, req, deps
    ):
        db.get_or_create_project_officer = AsyncMock(
            return_value=_post_row(thread_id=THREAD_ID)
        )
        db.get_thread = AsyncMock(return_value=_officer_thread())
        db.get_project_officer = AsyncMock(
            return_value={
                "state": {
                    "runtime_actor_incident": {"status": "resolved"},
                    "runtime_actor_verification": {"state": "completed"},
                    "while_vacant": [{"kind": "wake"}],
                    "while_vacant_dropped": 1,
                },
                "incarnations": [{"thread_id": THREAD_ID}],
            }
        )
        deps.end_thread_flow = AsyncMock(
            return_value={
                "status": "ended",
                "_officer_handoff": {"in_flight_jobs": []},
            }
        )

        out = await decommission_project_officer(req, PROJECT_ID, None)

        assert out["status"] == "decommissioned"
        assert out["harvested"] is False

    @pytest.mark.asyncio
    async def test_already_ended_link_folds_without_the_end_flow(
        self, db, as_project_admin, quiet_side_channels, req, deps, monkeypatch
    ):
        db.get_or_create_project_officer = AsyncMock(
            return_value=_post_row(thread_id=THREAD_ID)
        )
        db.get_thread = AsyncMock(return_value=_officer_thread(status="ended"))
        fold = AsyncMock(return_value={"harvested": True, "folded": 1, "deleted": 2})
        monkeypatch.setattr(officer_post_lifecycle, "decommission_officer_post", fold)
        flow = AsyncMock()
        deps.end_thread_flow = flow
        out = await decommission_project_officer(req, PROJECT_ID, None)
        fold.assert_awaited_once()
        flow.assert_not_awaited()
        assert out["already_ended"] is True

    @pytest.mark.asyncio
    async def test_missing_thread_row_uses_the_atomic_handoff(
        self, db, as_project_admin, quiet_side_channels, req
    ):
        db.get_or_create_project_officer = AsyncMock(
            return_value=_post_row(thread_id=THREAD_ID)
        )
        db.get_thread = AsyncMock(return_value=None)
        db.decommission_project_officer = AsyncMock(
            return_value={
                "transitioned": True,
                "already_decommissioned": False,
                "vacant": True,
                "routes": [],
                "folded": 0,
                "deleted": 0,
                "dropped": 0,
                "harvested": False,
                "incarnation": {"thread_id": THREAD_ID},
            }
        )
        out = await decommission_project_officer(req, PROJECT_ID, None)
        db.decommission_project_officer.assert_awaited_once_with(
            PROJECT_ID,
            THREAD_ID,
            reason="decommissioned",
            force=False,
            allow_orphan_retirement=False,
        )
        db.clear_project_officer_thread.assert_not_awaited()
        db.append_project_officer_incarnation.assert_not_awaited()
        assert out["status"] == "decommissioned"


# =========================================================================
# The stand-down reroute — one funnel for DELETE and decommission
# =========================================================================


class TestDecommissionHygieneHelper:
    @pytest.mark.asyncio
    async def test_registered_officer_runs_harvest_fold_unlink_incarnation(
        self, db, deps
    ):
        created_at = datetime(2026, 8, 1, 12, 0, tzinfo=timezone.utc)
        thread = _officer_thread(created_at=created_at)
        thread["metadata"]["officer_state"] = {
            "sitrep_fingerprints": {"j": "fp"},
            "digest": [{"subject": "s"}],
        }
        db.decommission_project_officer = AsyncMock(
            return_value={
                "transitioned": True,
                "already_decommissioned": False,
                "vacant": True,
                "routes": [],
                "folded": 1,
                "deleted": 2,
                "dropped": 0,
                "harvested": True,
                "incarnation": {
                    "thread_id": THREAD_ID,
                    "commissioned_at": created_at.isoformat(),
                    "reason": "decommissioned",
                },
            }
        )
        out = await officer_post_lifecycle.decommission_officer_post(
            thread, reason="decommissioned", dependencies=deps
        )
        assert out is not None
        db.decommission_project_officer.assert_awaited_once_with(
            PROJECT_ID,
            THREAD_ID,
            reason="decommissioned",
            force=False,
            allow_orphan_retirement=False,
        )
        assert out["harvested"] is True
        assert out["folded"] == 1
        assert out["deleted"] == 2
        db.merge_project_officer_state.assert_not_awaited()
        db.fold_project_officer_wake_queue.assert_not_awaited()
        db.clear_project_officer_thread.assert_not_awaited()
        db.append_project_officer_incarnation.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_unregistered_officer_thread_touches_nothing(self, db, deps):
        """A legacy enabled thread that never claimed the post must not
        harvest over another incarnation's state."""
        db.get_project_officer = AsyncMock(
            return_value=_post_row(thread_id=str(uuid4()))
        )
        out = await officer_post_lifecycle.decommission_officer_post(
            _officer_thread(), reason="retired", dependencies=deps
        )
        assert out["transitioned"] is False
        db.decommission_project_officer.assert_awaited_once_with(
            PROJECT_ID,
            THREAD_ID,
            reason="retired",
            force=False,
            allow_orphan_retirement=False,
        )
        db.merge_project_officer_state.assert_not_awaited()
        db.clear_project_officer_thread.assert_not_awaited()
        db.append_project_officer_incarnation.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_projectless_thread_is_a_noop(self, db, deps):
        out = await officer_post_lifecycle.decommission_officer_post(
            _officer_thread(project_id=None), reason="retired", dependencies=deps
        )
        assert out is None
        db.get_project_officer.assert_not_awaited()


class TestEndThreadReroute:
    @pytest.fixture(autouse=True)
    def exact_retirement(self, db, monkeypatch):
        async def begin_retirement(*_args, permanent, settle_status, **_kwargs):
            return {
                "state": "pending",
                "token": RETIREMENT_TOKEN,
                "generation": RETIREMENT_GENERATION,
                "permanent": permanent,
                "authorized_at": None,
                "context": {
                    "thread_id": THREAD_ID,
                    "settle_status": settle_status,
                    "runtime_authority_exposed": False,
                },
            }

        lock = AsyncMock()
        lock.__aenter__.return_value = True
        lock.__aexit__.return_value = False
        db.begin_pinned_thread_retirement = AsyncMock(side_effect=begin_retirement)
        db.abort_pinned_thread_retirement = AsyncMock(return_value=True)
        db.authorize_pinned_thread_retirement = AsyncMock(return_value=True)
        db.settle_pinned_thread_retirement = AsyncMock(return_value=True)
        db.try_thread_advisory_lock = MagicMock(return_value=lock)
        # End recovers the idle physical-Pod-stop requirement from durable
        # ``vm_idle_operations`` authority, never from the caller. No open
        # pinned idle operation exists here, so this is a plain owner End.
        db.reserve_pinned_thread_idle_terminal_end = AsyncMock(return_value="none")
        db.fetchrow = AsyncMock(return_value=None)
        monkeypatch.setattr(
            pinned_retirement_module.PinnedRetirementOperations,
            "pinned_retirement_is_current",
            AsyncMock(return_value=True),
        )
        monkeypatch.setattr(
            pinned_retirement_module.PinnedRetirementOperations,
            "cleanup_pinned_thread_retirement",
            AsyncMock(),
        )

    @pytest.mark.asyncio
    async def test_direct_delete_on_an_officer_routes_through_decommission(
        self, db, monkeypatch
    ):
        """The officer branch of ``end_thread``'s stand-down IS decommission
        step 2-3-5 (officer_post.md §5) — reason 'retired' by default."""
        monkeypatch.setattr(
            thread_retirement_module,
            "thread_turn_in_flight",
            AsyncMock(return_value=False),
        )
        monkeypatch.setattr(
            thread_retirement_module,
            "release_thread_resources",
            AsyncMock(),
        )
        monkeypatch.setattr(
            officer_conference_module, "conclude_conference_if_any", AsyncMock()
        )
        db.get_project_officer = AsyncMock(return_value=_post_row(thread_id=THREAD_ID))
        db.decommission_project_officer = AsyncMock(
            return_value={
                "transitioned": True,
                "already_decommissioned": False,
                "vacant": True,
                "routes": [],
                "folded": 0,
                "deleted": 0,
                "dropped": 0,
                "harvested": True,
                "incarnation": {"thread_id": THREAD_ID, "reason": "retired"},
            }
        )
        db.end_thread = AsyncMock()
        thread = _officer_thread(execution_lane="pinned")
        thread["metadata"]["officer_state"] = {"pages": {"count": 1}}
        db.get_thread = AsyncMock(return_value=thread)

        out = await controls_composition.thread_retirement_operations(
            orch_main.app.state.resources
        ).end_thread_flow(THREAD_ID, thread, permanent=False, force=False)

        assert out == {"status": "ended"}
        # The post handoff and server-owned runtime disable are one database
        # transaction; End cannot retire the thread after a partial handoff.
        db.decommission_project_officer.assert_awaited_once_with(
            PROJECT_ID,
            THREAD_ID,
            reason="retired",
            force=False,
            allow_orphan_retirement=True,
            retirement_token=RETIREMENT_TOKEN,
            retirement_generation=RETIREMENT_GENERATION,
            retirement_settle_status="ended",
        )
        db.merge_thread_config_override.assert_not_awaited()
        db.merge_project_officer_state.assert_not_awaited()
        db.fold_project_officer_wake_queue.assert_not_awaited()
        db.clear_project_officer_thread.assert_not_awaited()
        db.append_project_officer_incarnation.assert_not_awaited()
        db.end_thread.assert_not_awaited()
        db.settle_pinned_thread_retirement.assert_awaited_once_with(
            THREAD_ID,
            token=RETIREMENT_TOKEN,
            generation=RETIREMENT_GENERATION,
            final_status="ended",
            staged_event=None,
        )

    @pytest.mark.asyncio
    async def test_authoritative_handoff_failure_blocks_the_end(self, db, monkeypatch):
        monkeypatch.setattr(
            thread_retirement_module,
            "thread_turn_in_flight",
            AsyncMock(return_value=False),
        )
        monkeypatch.setattr(
            thread_retirement_module,
            "release_thread_resources",
            AsyncMock(),
        )
        monkeypatch.setattr(
            officer_conference_module, "conclude_conference_if_any", AsyncMock()
        )
        monkeypatch.setattr(
            officer_post_lifecycle_module,
            "decommission_officer_post",
            AsyncMock(side_effect=RuntimeError("post table on fire")),
        )
        db.end_thread = AsyncMock()
        thread = _officer_thread(execution_lane="pinned")
        db.get_thread = AsyncMock(return_value=thread)
        with pytest.raises(RuntimeError, match="post table on fire"):
            await controls_composition.thread_retirement_operations(
                orch_main.app.state.resources
            ).end_thread_flow(THREAD_ID, thread, permanent=False, force=False)
        db.end_thread.assert_not_awaited()
        db.settle_pinned_thread_retirement.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_direct_end_returns_post_locked_in_flight_warning_without_cleanup(
        self, db, monkeypatch
    ):
        jobs = [
            {
                "job_id": "j1",
                "status": "pending_review",
                "slot": "line",
                "title": "still owned",
            }
        ]
        db.decommission_project_officer = AsyncMock(
            return_value={
                "transitioned": False,
                "blocked_by_in_flight": True,
                "vacant": False,
                "routes": [],
                "in_flight_jobs": jobs,
            }
        )
        monkeypatch.setattr(
            thread_retirement_module,
            "thread_turn_in_flight",
            AsyncMock(return_value=False),
        )
        release = AsyncMock()
        conclude = AsyncMock()
        monkeypatch.setattr(
            thread_retirement_module, "release_thread_resources", release
        )
        monkeypatch.setattr(
            officer_conference_module, "conclude_conference_if_any", conclude
        )
        db.end_thread = AsyncMock()
        thread = _officer_thread(execution_lane="pinned")
        db.get_thread = AsyncMock(return_value=thread)

        out = await controls_composition.thread_retirement_operations(
            orch_main.app.state.resources
        ).end_thread_flow(
            THREAD_ID,
            thread,
            permanent=False,
            force=False,
        )

        assert out["status"] == "in_flight"
        assert out["in_flight_jobs"] == jobs
        db.decommission_project_officer.assert_awaited_once_with(
            PROJECT_ID,
            THREAD_ID,
            reason="retired",
            force=False,
            allow_orphan_retirement=True,
            retirement_token=RETIREMENT_TOKEN,
            retirement_generation=RETIREMENT_GENERATION,
            retirement_settle_status="ended",
        )
        release.assert_not_awaited()
        conclude.assert_not_awaited()
        db.end_thread.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_direct_end_accepts_atomic_orphan_retirement(self, db, monkeypatch):
        db.decommission_project_officer = AsyncMock(
            return_value={
                "transitioned": False,
                "orphan_retired": True,
                "vacant": False,
                "routes": [],
                "in_flight_jobs": [],
            }
        )
        monkeypatch.setattr(
            thread_retirement_module,
            "thread_turn_in_flight",
            AsyncMock(return_value=False),
        )
        monkeypatch.setattr(
            thread_retirement_module,
            "release_thread_resources",
            AsyncMock(),
        )
        monkeypatch.setattr(
            officer_conference_module, "conclude_conference_if_any", AsyncMock()
        )
        db.end_thread = AsyncMock()
        thread = _officer_thread(execution_lane="pinned")
        db.get_thread = AsyncMock(return_value=thread)

        out = await controls_composition.thread_retirement_operations(
            orch_main.app.state.resources
        ).end_thread_flow(
            THREAD_ID,
            thread,
            permanent=False,
            force=False,
        )

        assert out == {"status": "ended"}
        db.merge_thread_config_override.assert_not_awaited()
        db.end_thread.assert_not_awaited()
        db.settle_pinned_thread_retirement.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_direct_end_and_explicit_decommission_share_one_transition(
        self, db, req, as_project_admin, quiet_side_channels, monkeypatch
    ):
        """Both public controls reach the same post/thread transaction."""
        monkeypatch.setattr(
            thread_retirement_module,
            "thread_turn_in_flight",
            AsyncMock(return_value=False),
        )
        monkeypatch.setattr(
            thread_retirement_module,
            "release_thread_resources",
            AsyncMock(),
        )
        monkeypatch.setattr(
            officer_conference_module, "conclude_conference_if_any", AsyncMock()
        )
        db.get_or_create_project_officer = AsyncMock(
            return_value=_post_row(thread_id=THREAD_ID)
        )
        thread = _officer_thread(execution_lane="pinned")
        db.get_thread = AsyncMock(return_value=thread)
        db.get_project_officer = AsyncMock(return_value=_post_row())
        db.end_thread = AsyncMock()
        db.decommission_project_officer = AsyncMock(
            return_value={
                "transitioned": True,
                "already_decommissioned": False,
                "vacant": True,
                "routes": [],
                "folded": 0,
                "deleted": 0,
                "dropped": 0,
                "harvested": False,
                "incarnation": {"thread_id": THREAD_ID, "reason": "retired"},
            }
        )

        await controls_composition.thread_retirement_operations(
            orch_main.app.state.resources
        ).end_thread_flow(
            THREAD_ID,
            thread,
            permanent=False,
            force=False,
            officer_retire_reason="retired",
        )
        direct_call = db.decommission_project_officer.await_args

        db.decommission_project_officer.reset_mock()
        await decommission_project_officer(
            req,
            PROJECT_ID,
            OfficerDecommissionRequest(reason="retired"),
        )
        endpoint_call = db.decommission_project_officer.await_args

        assert direct_call.args == endpoint_call.args == (PROJECT_ID, THREAD_ID)
        assert direct_call.kwargs == {
            "reason": "retired",
            "force": False,
            "allow_orphan_retirement": True,
            "retirement_token": RETIREMENT_TOKEN,
            "retirement_generation": RETIREMENT_GENERATION,
            "retirement_settle_status": "ended",
        }
        assert endpoint_call.kwargs == {
            "reason": "retired",
            "force": False,
            "allow_orphan_retirement": False,
            "retirement_token": RETIREMENT_TOKEN,
            "retirement_generation": RETIREMENT_GENERATION,
            "retirement_settle_status": "ended",
        }

    @pytest.mark.asyncio
    async def test_notifier_failure_does_not_falsify_committed_handoff(
        self, db, deps, monkeypatch
    ):
        from orchestrator.services import message_routing

        route = {
            "route_id": str(uuid4()),
            "job_id": str(uuid4()),
            "user_delivery_at": None,
        }
        db.decommission_project_officer = AsyncMock(
            return_value={
                "transitioned": True,
                "already_decommissioned": False,
                "vacant": True,
                "routes": [route],
                "folded": 0,
                "deleted": 0,
                "dropped": 0,
                "harvested": False,
                "incarnation": {"thread_id": THREAD_ID},
            }
        )

        async def fail_after_commit(*args, **kwargs):
            db.decommission_project_officer.assert_awaited_once()
            return False

        deliver = AsyncMock(side_effect=fail_after_commit)
        monkeypatch.setattr(message_routing, "deliver_route_to_user", deliver)

        out = await officer_post_lifecycle.decommission_officer_post(
            _officer_thread(), reason="decommissioned", dependencies=deps
        )

        assert out["transitioned"] is True
        assert out["routes_staged"] == 1
        assert out["routes_delivered"] == 0
        assert route["user_delivery_at"] is None
        deliver.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_notifier_exception_does_not_falsify_committed_handoff(
        self, db, deps, monkeypatch
    ):
        from orchestrator.services import message_routing

        route = {
            "route_id": str(uuid4()),
            "job_id": str(uuid4()),
            "user_delivery_at": None,
        }
        db.decommission_project_officer = AsyncMock(
            return_value={
                "transitioned": True,
                "already_decommissioned": False,
                "vacant": True,
                "routes": [route],
                "folded": 0,
                "deleted": 0,
                "dropped": 0,
                "harvested": False,
                "incarnation": {"thread_id": THREAD_ID},
            }
        )
        deliver = AsyncMock(side_effect=RuntimeError("notifier unavailable"))
        monkeypatch.setattr(message_routing, "deliver_route_to_user", deliver)

        out = await officer_post_lifecycle.decommission_officer_post(
            _officer_thread(), reason="decommissioned", dependencies=deps
        )

        assert out["transitioned"] is True
        assert out["routes_staged"] == 1
        assert out["routes_delivered"] == 0
        assert route["user_delivery_at"] is None
        deliver.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_plain_session_end_skips_officer_hygiene(self, db, monkeypatch):
        monkeypatch.setattr(
            thread_retirement_module,
            "thread_turn_in_flight",
            AsyncMock(return_value=False),
        )
        monkeypatch.setattr(
            thread_retirement_module,
            "release_thread_resources",
            AsyncMock(),
        )
        monkeypatch.setattr(
            officer_conference_module, "conclude_conference_if_any", AsyncMock()
        )
        hygiene = AsyncMock()
        monkeypatch.setattr(
            officer_post_lifecycle_module,
            "decommission_officer_post",
            hygiene,
        )
        db.end_thread = AsyncMock()
        plain = {
            "id": THREAD_ID,
            "project_id": PROJECT_ID,
            "status": "active",
            "execution_lane": "pinned",
            "runtime_generation": RETIREMENT_GENERATION,
            "runtime_retirement_token": None,
            "metadata": {},
        }
        db.get_thread = AsyncMock(return_value=plain)
        await controls_composition.thread_retirement_operations(
            orch_main.app.state.resources
        ).end_thread_flow(THREAD_ID, plain, permanent=False, force=False)
        hygiene.assert_not_awaited()
        db.merge_thread_config_override.assert_not_awaited()


# =========================================================================
# While-vacant ledger leg in the wake decision function
# =========================================================================


class TestWhileVacantLeg:
    def _db(self, officer=None):
        db = MagicMock()
        db.get_job = AsyncMock(
            return_value={
                "id": "j1",
                "project_id": PROJECT_ID,
                "description": "review the quarterly numbers",
            }
        )
        db.route_project_officer_job_transition = AsyncMock(
            return_value={
                "destination": "wake" if officer else "while_vacant",
                "thread_id": THREAD_ID if officer else None,
                "enqueued": bool(officer),
                "appended": not bool(officer),
            }
        )
        db.get_officer_thread_for_project = AsyncMock(return_value=officer)
        db.append_project_officer_while_vacant = AsyncMock(return_value=_post_row())
        db.enqueue_session_wake_event = AsyncMock(return_value=True)
        return db

    @pytest.mark.asyncio
    async def test_vacant_post_records_the_transition_instead_of_dropping(self):
        db = self._db(officer=None)
        ok = await session_wake._notify_project_officer_of_job(db, "j1", "completed")
        assert ok is False
        db.route_project_officer_job_transition.assert_awaited_once_with(
            PROJECT_ID,
            job_id="j1",
            status="completed",
            description="review the quarterly numbers",
            dedup_key="j1:completed",
        )
        db.get_officer_thread_for_project.assert_not_awaited()
        db.append_project_officer_while_vacant.assert_not_awaited()
        db.enqueue_session_wake_event.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_commissioned_post_enqueues_and_skips_the_ledger(self):
        db = self._db(officer={"id": THREAD_ID})
        ok = await session_wake._notify_project_officer_of_job(db, "j1", "paused")
        assert ok is True
        db.route_project_officer_job_transition.assert_awaited_once_with(
            PROJECT_ID,
            job_id="j1",
            status="paused",
            description="review the quarterly numbers",
            dedup_key="j1:paused",
        )
        db.append_project_officer_while_vacant.assert_not_awaited()
        db.enqueue_session_wake_event.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_ledger_failure_never_raises_into_the_completion_path(self):
        db = self._db(officer=None)
        db.route_project_officer_job_transition = AsyncMock(
            side_effect=RuntimeError("boom")
        )
        assert (
            await session_wake._notify_project_officer_of_job(db, "j1", "failed")
            is False
        )

    def test_commission_brief_is_never_debounced(self):
        assert session_wake.OFFICER_DEBOUNCE_BY_SOURCE["commission"] == 0


# =========================================================================
# spend_today (§8)
# =========================================================================


def _view_deps(usage_ledger) -> OfficerPostViewDependencies:
    """The card's read collaborators. The metering ledger is a dependency
    field now, so a suite wires it here instead of rebinding
    ``orchestrator.main.usage_ledger``."""
    return OfficerPostViewDependencies(
        store=MagicMock(),
        vector_store=MagicMock(),
        usage_ledger=usage_ledger,
        persistent_provisioner=MagicMock(),
        auto_pull_release_enabled=lambda: False,
        persistent_agent_reconciliation_enabled=lambda: False,
        find_open_conference_thread=AsyncMock(return_value=None),
    )


class TestSpendToday:
    @pytest.mark.asyncio
    async def test_vacant_post_is_zero_spend(self):
        out = await officer_spend_today(None, 5_000_000, dependencies=_view_deps(None))
        assert out == {"tokens": 0, "ceiling": 5_000_000}

    @pytest.mark.asyncio
    async def test_commissioned_sums_token_categories_over_today(self):
        ledger = MagicMock()
        ledger.is_available = True
        ledger.query_usage = AsyncMock(
            return_value={
                "by_category": [
                    {"unit": "prompt-token", "quantity": 1_200_000},
                    {"unit": "cached-prompt-token", "quantity": 250_000},
                    {"unit": "completion-token", "quantity": 50_000},
                    {"unit": "requests", "quantity": 999},
                ]
            }
        )
        out = await officer_spend_today(
            THREAD_ID, 5_000_000, dependencies=_view_deps(ledger)
        )
        assert out == {"tokens": 1_500_000, "ceiling": 5_000_000}
        kwargs = ledger.query_usage.await_args.kwargs
        assert kwargs["ref_id"] == THREAD_ID
        assert kwargs["from_ts"].date() == datetime.now(timezone.utc).date()
        assert kwargs["to_ts"] - kwargs["from_ts"] < timedelta(days=1)

    @pytest.mark.asyncio
    async def test_metering_down_reads_none_not_zero(self):
        ledger = MagicMock()
        ledger.is_available = True
        ledger.query_usage = AsyncMock(side_effect=RuntimeError("metering down"))
        out = await officer_spend_today(THREAD_ID, 0, dependencies=_view_deps(ledger))
        assert out == {"tokens": None, "ceiling": 0}

    @pytest.mark.asyncio
    async def test_no_ledger_wired_reads_none(self):
        out = await officer_spend_today(THREAD_ID, 100, dependencies=_view_deps(None))
        assert out == {"tokens": None, "ceiling": 100}


# =========================================================================
# Real Postgres — the new post ledger/queue helpers
# =========================================================================

testcontainers_postgres = pytest.importorskip(
    "testcontainers.postgres", reason="testcontainers not installed"
)
PostgresContainer = testcontainers_postgres.PostgresContainer


@pytest.fixture(scope="module")
def pg_dsn():
    try:
        container = PostgresContainer("postgres:15")
        container.start()
    except Exception as exc:  # docker/podman not available on this runner
        pytest.skip(f"local Postgres container unavailable: {exc}")
    try:
        yield container.get_connection_url().replace(
            "postgresql+psycopg2", "postgresql"
        )
    finally:
        container.stop()


@pytest_asyncio.fixture(scope="module")
async def _schema_applied(pg_dsn):
    import asyncpg

    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(SCHEMA_FILE.read_text())
    finally:
        await conn.close()


@pytest_asyncio.fixture
async def pg(pg_dsn, _schema_applied, monkeypatch):
    monkeypatch.setenv("EXPERTS_DB_ENABLED", "false")
    store = PostgresDB(
        connection_string=pg_dsn,
        min_connections=1,
        max_connections=4,
    )
    await store.connect()
    async with store.acquire() as conn:
        await conn.execute(
            "TRUNCATE project_officers, session_wake_events, jobs, threads, "
            "projects CASCADE"
        )
    try:
        yield store
    finally:
        await store.close()


async def _seed_project(pg: PostgresDB, name: str = "lifecycle-test") -> str:
    project_id = uuid4()
    async with pg.acquire() as conn:
        await conn.execute(
            "INSERT INTO projects (id, name) VALUES ($1, $2)", project_id, name
        )
    return str(project_id)


async def _seed_thread(pg: PostgresDB, project_id: str, **cols) -> str:
    thread_id = uuid4()
    async with pg.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO threads (id, project_id, status, metadata)
            VALUES ($1, $2, $3, $4::jsonb)
            """,
            thread_id,
            UUID(project_id),
            cols.get("status", "active"),
            json.dumps(cols.get("metadata") or {}),
        )
    return str(thread_id)


class TestWhileVacantRingHelpers:
    @pytest.mark.asyncio
    async def test_append_caps_at_twenty_and_counts_the_dropped(self, pg):
        project_id = await _seed_project(pg)
        for i in range(25):
            row = await pg.append_project_officer_while_vacant(
                project_id, [{"job_id": f"j{i}", "status": "completed"}]
            )
        ledger = row["state"]["while_vacant"]
        assert len(ledger) == 20
        # Drop-oldest: j0..j4 fell off, j24 is the newest survivor.
        assert ledger[0]["job_id"] == "j5"
        assert ledger[-1]["job_id"] == "j24"
        assert row["state"]["while_vacant_dropped"] == 5

    @pytest.mark.asyncio
    async def test_append_self_heals_a_missing_post(self, pg):
        project_id = await _seed_project(pg)  # direct INSERT — no post row
        row = await pg.append_project_officer_while_vacant(
            project_id, [{"job_id": "j1", "status": "failed"}]
        )
        assert row is not None
        assert row["state"]["while_vacant"] == [{"job_id": "j1", "status": "failed"}]

    @pytest.mark.asyncio
    async def test_append_refuses_garbage_and_empty(self, pg):
        assert await pg.append_project_officer_while_vacant("nope", [{"a": 1}]) is None
        assert (
            await pg.append_project_officer_while_vacant(str(uuid4()), [{"a": 1}])
            is None
        )
        project_id = await _seed_project(pg)
        assert await pg.append_project_officer_while_vacant(project_id, []) is None

    @pytest.mark.asyncio
    async def test_drain_reads_and_clears_atomically(self, pg):
        project_id = await _seed_project(pg)
        await pg.append_project_officer_while_vacant(
            project_id,
            [{"job_id": "j1", "status": "completed"}],
        )
        await pg.append_project_officer_while_vacant(
            project_id,
            [{"job_id": "j2", "status": "paused"}],
        )
        drained = await pg.drain_project_officer_while_vacant(project_id)
        assert [e["job_id"] for e in drained["entries"]] == ["j1", "j2"]
        assert drained["dropped"] == 0
        # Second drain: nothing left — the brief consumed it exactly once.
        again = await pg.drain_project_officer_while_vacant(project_id)
        assert again == {"entries": [], "dropped": 0}
        row = await pg.get_project_officer(project_id)
        assert row["state"]["while_vacant"] == []
        assert row["state"]["while_vacant_dropped"] == 0

    @pytest.mark.asyncio
    async def test_drain_on_absent_post_is_empty(self, pg):
        assert await pg.drain_project_officer_while_vacant(str(uuid4())) == {
            "entries": [],
            "dropped": 0,
        }


class TestQueueFold:
    @pytest.mark.asyncio
    async def test_job_events_fold_into_the_ledger_and_the_rest_die(self, pg):
        project_id = await _seed_project(pg)
        thread_id = await _seed_thread(pg, project_id)
        await pg.register_project_officer_thread(project_id, thread_id)
        assert await pg.enqueue_session_wake_event(
            thread_id,
            source="job_transition",
            dedup_key="aaaa:completed",
            payload={"job_id": "aaaa", "status": "completed", "description": "d1"},
            project_id=project_id,
        )
        assert await pg.enqueue_session_wake_event(
            thread_id,
            source="job_transition",
            dedup_key="bbbb:paused",
            payload={"job_id": "bbbb", "status": "paused", "description": "d2"},
            project_id=project_id,
        )
        assert await pg.enqueue_session_wake_event(
            thread_id,
            source="timer",
            dedup_key="timer",
            payload={"minutes": 30},
            fire_at=datetime.now(timezone.utc) + timedelta(minutes=30),
        )
        assert await pg.enqueue_session_wake_event(
            thread_id, source="fleet", dedup_key="upgrade-1", payload={}
        )

        out = await pg.fold_project_officer_wake_queue(project_id, thread_id)

        assert out == {"folded": 2, "deleted": 4, "dropped": 0}
        row = await pg.get_project_officer(project_id)
        ledger = row["state"]["while_vacant"]
        assert {e["job_id"] for e in ledger} == {"aaaa", "bbbb"}
        assert all(e.get("at") for e in ledger)
        async with pg.acquire() as conn:
            remaining = await conn.fetchval(
                "SELECT COUNT(*) FROM session_wake_events WHERE thread_id = $1",
                UUID(thread_id),
            )
        assert remaining == 0

    @pytest.mark.asyncio
    async def test_fold_respects_the_ring_cap(self, pg):
        project_id = await _seed_project(pg)
        thread_id = await _seed_thread(pg, project_id)
        await pg.register_project_officer_thread(project_id, thread_id)
        # 18 already on the ledger + 4 folded = 22 → 2 dropped.
        await pg.append_project_officer_while_vacant(
            project_id,
            [{"job_id": f"old{i}", "status": "completed"} for i in range(18)],
        )
        for i in range(4):
            assert await pg.enqueue_session_wake_event(
                thread_id,
                source="job_transition",
                dedup_key=f"new{i}:completed",
                payload={"job_id": f"new{i}", "status": "completed"},
                project_id=project_id,
            )
        out = await pg.fold_project_officer_wake_queue(project_id, thread_id)
        assert out["folded"] == 4
        assert out["dropped"] == 2
        row = await pg.get_project_officer(project_id)
        assert len(row["state"]["while_vacant"]) == 20
        assert row["state"]["while_vacant_dropped"] == 2

    @pytest.mark.asyncio
    async def test_empty_queue_folds_to_nothing(self, pg):
        project_id = await _seed_project(pg)
        thread_id = await _seed_thread(pg, project_id)
        out = await pg.fold_project_officer_wake_queue(project_id, thread_id)
        assert out == {"folded": 0, "deleted": 0, "dropped": 0}


class TestCommunicationPolicyMerge:
    @pytest.mark.asyncio
    async def test_partial_patch_keeps_the_other_key(self, pg):
        project_id = await _seed_project(pg)
        await pg.get_or_create_project_officer(project_id)
        row = await pg.merge_project_officer_communication_policy(
            project_id, {"worker_messages": "officer_first"}
        )
        assert row["communication_policy"] == {
            "worker_messages": "officer_first",
            "officer_response_minutes": 15,
        }
        row = await pg.merge_project_officer_communication_policy(
            project_id, {"officer_response_minutes": 30}
        )
        assert row["communication_policy"] == {
            "worker_messages": "officer_first",
            "officer_response_minutes": 30,
        }

    @pytest.mark.asyncio
    async def test_absent_post_is_none(self, pg):
        assert (
            await pg.merge_project_officer_communication_policy(
                str(uuid4()), {"worker_messages": "user_direct"}
            )
            is None
        )
