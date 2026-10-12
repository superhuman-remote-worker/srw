"""Acknowledged drift is skipped at attach; unacknowledged drift still denies.

Round-1 fix: ``_strip_acknowledged_grants`` is SYNC and pure (no grant
resolution inside it) so it can run as ``resolve_config``'s ``grant_strip``
hook on the fully-merged ``data`` — not just on the detached
``capture["merged_fragment"]`` copy the PDP evaluates. Grants are resolved by
the caller (``_resolve_session_config``) and handed in directly; "admin
bypass" is therefore the CALLER's concern now (it never builds a grant_strip
closure when ``_resolve_runner_grants`` returns ``None``), not this helper's
— see ``test_resolve_session_config_strips_the_delivered_blob_not_just_the_capture``
below for the wiring, and ``tests/test_session_toolset_measurement.py`` for
existing broad coverage of ``_resolve_runner_grants``'s admin short-circuit.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from tests import _b09_control_seams as control_seams

import orchestrator.main
from orchestrator.services.grant_enforcement import (
    strip_acknowledged_grants as _strip_acknowledged_grants,
)
from orchestrator.services.config_drift import (
    acknowledged_drift_ids,
    acknowledged_grant_keys,
    strip_acknowledged,
)
from orchestrator.application import preparation as preparation_composition
from orchestrator.services import (
    session_config_resolution as session_config_resolution_module,
)


DS_GONE = "d7555d5d-ce46-49e2-b1fa-8235d720badc"
DS_OK = "2991589e-249d-4cca-98ce-780db69b2520"
OWNER = "11111111-1111-4111-8111-111111111111"


def test_acknowledged_drift_ids_reads_metadata():
    metadata = {"config_drift_ack": {f"connector:{DS_GONE}": "deleted"}}
    assert acknowledged_drift_ids(metadata) == {f"connector:{DS_GONE}"}


def test_acknowledged_drift_ids_tolerates_a_json_string():
    """asyncpg hands back JSONB as a raw string; a guard that only accepts dict
    silently disables the feature."""
    metadata = '{"config_drift_ack": {"connector:x": "deleted"}}'
    assert acknowledged_drift_ids(metadata) == {"connector:x"}


def test_acknowledged_drift_ids_missing_key_is_empty():
    assert acknowledged_drift_ids({}) == set()


def test_strip_acknowledged_removes_only_acked_ids():
    result = strip_acknowledged(
        [DS_OK, DS_GONE], {f"connector:{DS_GONE}"}, prefix="connector"
    )
    assert result == [DS_OK]


def test_strip_acknowledged_leaves_unacked_ids_in_place():
    result = strip_acknowledged([DS_OK, DS_GONE], set(), prefix="connector")
    assert result == [DS_OK, DS_GONE]


def test_acknowledged_grant_keys_unprefixes_only_grants():
    metadata = {
        "config_drift_ack": {
            "grant:shell_tools": "revoked",
            f"connector:{DS_GONE}": "deleted",
        }
    }
    assert acknowledged_grant_keys(metadata) == {"shell_tools"}


def test_unacknowledged_grant_violation_is_not_stripped():
    """Acknowledging ONE grant must never smuggle a different one through.
    With shell_tools acked but vm_workspace also violating, the fragment must
    come back untouched so the dispatch PEP still denies."""
    merged = {"tools": {"shell": True}, "workspace": {"backend": "vm"}}
    grants = {"shell_tools": False, "vm_workspace": False}

    result = _strip_acknowledged_grants(merged, grants, {"shell_tools"})

    assert result == merged


def test_fully_acknowledged_grant_violations_are_stripped():
    merged = {"tools": {"shell": True}, "workspace": {"backend": "vm"}}
    grants = {"shell_tools": False, "vm_workspace": False}

    result = _strip_acknowledged_grants(merged, grants, {"shell_tools", "vm_workspace"})

    assert "shell" not in result.get("tools", {})
    assert "backend" not in result.get("workspace", {})
    # The original must not be mutated in place — callers reuse it.
    assert merged["tools"]["shell"] is True


def test_no_violations_returns_the_fragment_unchanged():
    merged = {"tools": {"shell": True}}

    result = _strip_acknowledged_grants(merged, {"shell_tools": True}, {"shell_tools"})

    assert result == merged


@pytest.mark.asyncio
async def test_resolve_session_config_strips_the_delivered_blob_not_just_the_capture():
    """MANDATORY end-to-end regression for the round-1 privilege-escalation
    finding: every earlier grant test called the stripping helper in
    isolation, which is exactly why the leak shipped — the helper reassigned
    ``_cap["merged_fragment"]`` (a detached deepcopy taken purely for the PDP)
    while the actually-delivered blob was already built from
    ``resolve_config``'s internal ``data`` beforehand. A user who acknowledged
    ``shell_tools`` stopped getting denied, but the agent still hydrated
    ``tools.shell`` with the full tool list — the capability the grant
    revoked came back.

    Drives ``_resolve_session_config`` for real (``resolve_config`` itself is
    NOT mocked — that is precisely the code path the bug lived in) and
    asserts both halves: no ``GrantDenied``, AND the returned blob's
    ``agent.tools.shell`` is empty.
    """
    thread = {"id": "11111111-1111-4111-8111-111111111111", "user_id": "u1"}
    metadata = {"config_drift_ack": {"grant:shell_tools": "revoked"}}

    with (
        patch(
            "orchestrator.main.app.state.resources.postgres_db.fetchrow",
            AsyncMock(return_value=None),
        ),
        patch(
            "orchestrator.services.deployment_gates.is_experts_db_enabled",
            return_value=True,
        ),
        patch(
            "orchestrator.services.grant_enforcement.user_experts_enabled",
            AsyncMock(return_value=True),
        ),
        # R1.B05 lane P: same-module sibling of the resolve once main
        # delegates to ``services.session_config_resolution`` — patch both so
        # the stub is reached before AND after the extraction.
        patch(
            "orchestrator.services.session_config_resolution"
            ".resolve_session_account_defaults",
            AsyncMock(return_value={}),
        ),
        # R1.B12: the session config dependencies reach the skill gatherer
        # through ``catalogue.expert_catalog_service(resources)`` per call, so
        # the double replaces the service method, not a main wrapper.
        patch(
            "orchestrator.services.expert_catalog.ExpertCatalogService"
            ".gather_in_scope_skills",
            AsyncMock(return_value={}),
        ),
        patch(
            "orchestrator.services.thread_mount_rows.thread_project_ids",
            AsyncMock(return_value=[]),
        ),
        patch(
            "orchestrator.services.grant_enforcement.resolve_runner_grants",
            AsyncMock(return_value={"shell_tools": False}),
        ),
        patch(
            "orchestrator.services.dispatch_credentials.inject_thread_dispatch_credentials",
            AsyncMock(side_effect=lambda co, **_kw: co),
        ),
    ):
        delivered = await session_config_resolution_module.resolve_session_config(
            thread,
            metadata,
            config_override={"tools": {"shell": ["run_command"]}},
            dependencies=preparation_composition.session_config_dependencies(
                orchestrator.main.app.state.resources
            ),
        )

    assert delivered is not None
    assert not delivered["agent"]["tools"].get("shell")


def _ds_row(ds_id: str, *, owner: str = OWNER) -> dict:
    return {
        "id": ds_id,
        "type": "postgresql",
        "created_by": owner,
        "is_global": False,
        "scope_mode": "all",
        "policy_revision": 1,
        "job_id": None,
        "project_ids": [],
        "config": {},
    }


def _owner_row(owner: str = OWNER) -> dict:
    return {"id": owner, "is_admin": False, "is_approved": True}


class TestConnectorStripIsConditionalOnCurrentAvailability:
    """Round-2 finding: ``strip_acknowledged`` used to run unconditionally at
    the attach sites, so an acknowledged connector id was dropped FOREVER —
    even after the row came back — because nothing ever prunes
    ``config_drift_ack``. Spec §3.2 explicitly promises the opposite: "If the
    connector is recreated or the grant re-issued, the item returns
    automatically on the next attach — no repair step." Grants already
    behaved this way (``_strip_acknowledged_grants`` re-evaluates violations
    first); these two tests pin the same discipline for connectors, applied
    where the strip now actually happens: :func:`_strip_still_denied_ack`,
    called from ``_revalidate_thread_datasource_selection``.
    """

    @pytest.mark.asyncio
    async def test_recovered_acknowledged_connector_is_used_again(self):
        thread = {
            "id": "22222222-2222-4222-8222-222222222222",
            "user_id": OWNER,
            "metadata": {"config_drift_ack": {f"connector:{DS_OK}": "deleted"}},
        }
        db = AsyncMock()
        db.get_user = AsyncMock(return_value=_owner_row())
        db.get_datasource_policy_rows = AsyncMock(return_value=[_ds_row(DS_OK)])

        with patch("orchestrator.main.app.state.resources.postgres_db", db):
            (
                selected,
                revisions,
            ) = await control_seams.revalidate_thread_datasource_selection(
                thread, [DS_OK], target_project_ids=[]
            )

        assert selected == [DS_OK]
        assert revisions == {DS_OK: 1}

    @pytest.mark.asyncio
    async def test_still_unavailable_acknowledged_connector_is_still_dropped(self):
        """The converse, guarding against weakening fail-closed: an
        acknowledged id that is STILL unavailable must still be dropped
        (not silently kept forever, and not silently denying the rest of
        the selection either)."""
        thread = {
            "id": "22222222-2222-4222-8222-222222222222",
            "user_id": OWNER,
            "metadata": {"config_drift_ack": {f"connector:{DS_GONE}": "deleted"}},
        }
        db = AsyncMock()
        db.get_user = AsyncMock(return_value=_owner_row())
        db.get_datasource_policy_rows = AsyncMock(return_value=[])  # still gone

        with patch("orchestrator.main.app.state.resources.postgres_db", db):
            (
                selected,
                revisions,
            ) = await control_seams.revalidate_thread_datasource_selection(
                thread, [DS_GONE], target_project_ids=[]
            )

        assert selected == []
        assert revisions == {}

    @pytest.mark.asyncio
    async def test_unacknowledged_denial_still_fails_the_whole_selection(self):
        """Fail-closed sanity check: a denied id that was NEVER acknowledged
        must still deny the whole call, even alongside a fine, unrelated id —
        the new per-item classify-then-narrow step must not turn this into a
        silent partial selection."""
        from fastapi import HTTPException

        thread = {
            "id": "22222222-2222-4222-8222-222222222222",
            "user_id": OWNER,
            "metadata": {},
        }
        db = AsyncMock()
        db.get_user = AsyncMock(return_value=_owner_row())
        db.get_datasource_policy_rows = AsyncMock(return_value=[_ds_row(DS_OK)])

        with patch("orchestrator.main.app.state.resources.postgres_db", db):
            with pytest.raises(HTTPException) as exc:
                await control_seams.revalidate_thread_datasource_selection(
                    thread, [DS_OK, DS_GONE], target_project_ids=[]
                )

        assert exc.value.status_code == 403


@pytest.mark.asyncio
async def test_strip_still_denied_ack_translates_unavailable_to_403():
    """D: ``classify_datasource_selection`` can raise
    ``DatasourceUnavailableError`` directly — a malformed stored id, or a
    vanished/unapproved owner — rather than returning a per-item verdict,
    because those checks run BEFORE the per-item loop.
    ``_strip_still_denied_ack`` called it unwrapped, so with a non-empty ack
    map that path raised past every FastAPI handler and reached the ASGI
    layer as an unhandled 500 where every sibling authorizer 403s. This
    reproduces it with the ack map non-empty (so the call is even reached at
    all — see the early-return in _strip_still_denied_ack) and a malformed
    id inside the STORED selection being revalidated.
    """
    from fastapi import HTTPException

    thread = {
        "id": "22222222-2222-4222-8222-222222222222",
        "user_id": OWNER,
        "metadata": {"config_drift_ack": {f"connector:{DS_OK}": "deleted"}},
    }
    db = AsyncMock()
    db.get_user = AsyncMock(return_value=_owner_row())

    with patch("orchestrator.main.app.state.resources.postgres_db", db):
        with pytest.raises(HTTPException) as exc:
            await control_seams.revalidate_thread_datasource_selection(
                thread, ["not-a-uuid"], target_project_ids=[]
            )

    assert exc.value.status_code == 403
    assert exc.value.detail == "One or more selected connectors are unavailable"
