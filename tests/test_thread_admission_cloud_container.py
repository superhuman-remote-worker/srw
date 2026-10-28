"""A Session whose cloud folders Virtual cannot serve gets a Container.

The rule of 2026-10-21 (a Virtual Session mounts no cloud folder, so a
defaulted Virtual with a cloud connector became a Container) was narrowed on
2026-10-28 (features/cloud_folders_on_virtual_sessions.md, ruling 8): a
Virtual Session now serves a project folder and the session outbox itself,
in the ``cloud_remote`` form, so a defaulted Virtual becomes a Container only
when a selected folder's requested level is not offered on Virtual (a
``protected`` folder) or its provider has no Virtual delivery yet. The
left-out record is written only for those rows.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException

from orchestrator.schemas.thread_admission import ThreadCreateRequest
from orchestrator.services import thread_admission as ta
from tests.test_b06_lane_b_thread_admission import (
    FOLDER,
    PROJECT,
    USER,
    _deps,
    _folder_row,
)

KB = "99999999-9999-4999-8999-999999999999"


def _kb_row() -> dict[str, Any]:
    return {**_folder_row(), "id": KB, "type": "kb", "managed_key": None}


@pytest.fixture(autouse=True)
def virtual_by_default(monkeypatch):
    monkeypatch.setenv("WORKSPACE_DEFAULTS", '{"sessions": "virtual"}')
    monkeypatch.delenv("WORKSPACE_BUILTIN_TEMPLATES", raising=False)


def _deps_with(
    rows: list[dict[str, Any]],
    *,
    containers: bool = True,
    container_granted: bool = True,
):
    async def grants(fragment, **_kw):
        backend = (fragment.get("workspace") or {}).get("backend")
        if backend == "sandbox" and not container_granted:
            raise HTTPException(status_code=422, detail="workspace tier not granted")

    return _deps(
        store={
            "resolve_datasources_for_thread": AsyncMock(return_value=rows),
            "get_user_project_roles": AsyncMock(return_value={}),
        },
        container_provisioner=SimpleNamespace(
            is_available=containers, in_cluster=containers
        ),
        enforce_session_create_grants=AsyncMock(side_effect=grants),
    )


def _backend(plan) -> str | None:
    return (plan.config_override.get("workspace") or {}).get("backend")


async def _plan(deps, **fields: Any):
    body = ThreadCreateRequest(project_id=PROJECT, **fields)
    return await ta.resolve_thread_creation_plan(body, USER, dependencies=deps)


def _opencloud_folder_row() -> dict[str, Any]:
    """A folder on a provider the Virtual delivery does not serve yet."""
    row = _folder_row()
    row["config"] = {**row["config"], "backend_id": "opencloud"}
    return row


@pytest.mark.asyncio
async def test_a_session_with_its_cloud_folder_stays_virtual():
    """Virtual serves the folder itself now (the cloud_remote form)."""
    plan = await _plan(_deps_with([_folder_row()]), datasource_ids=[FOLDER])

    assert _backend(plan) == "virtual"
    assert plan.selected_datasource_ids == [FOLDER]
    assert _left_out(plan) == []


@pytest.mark.asyncio
async def test_a_protected_request_for_its_cloud_folder_gets_a_container():
    """The one level Virtual cannot serve: the user's explicit safety choice."""
    plan = await _plan(
        _deps_with([_folder_row()]),
        datasource_ids=[FOLDER],
        datasource_access={FOLDER: "protected"},
    )

    assert _backend(plan) == "sandbox"
    assert plan.selected_datasource_ids == [FOLDER]


@pytest.mark.asyncio
async def test_a_provider_without_a_virtual_delivery_gets_a_container():
    """OpenCloud's bearer token reaches a Virtual Session only in the feature's
    second run: until then its folders take a Container as before."""
    plan = await _plan(_deps_with([_opencloud_folder_row()]), datasource_ids=[FOLDER])

    assert _backend(plan) == "sandbox"
    assert plan.selected_datasource_ids == [FOLDER]


@pytest.mark.asyncio
async def test_a_session_without_cloud_connectors_stays_virtual():
    plan = await _plan(_deps_with([_kb_row()]), datasource_ids=[KB])

    assert _backend(plan) == "virtual"


@pytest.mark.asyncio
async def test_an_explicit_virtual_choice_is_kept():
    # The user chose the tier: their choice, even for a provider Virtual
    # does not serve yet; the folder is selected and named at delivery.
    plan = await _plan(
        _deps_with([_opencloud_folder_row()]),
        datasource_ids=[FOLDER],
        workspace={"template": {"inline": {"backend": "virtual"}}},
    )

    assert _backend(plan) == "virtual"
    assert plan.selected_datasource_ids == [FOLDER]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "unavailable",
    [{"containers": False}, {"container_granted": False}],
    ids=["no-container-provisioner", "container-not-granted"],
)
async def test_without_an_allowed_container_the_session_stays_virtual(unavailable):
    plan = await _plan(
        _deps_with([_opencloud_folder_row()], **unavailable), datasource_ids=[FOLDER]
    )

    assert _backend(plan) == "virtual"


OUTBOX = "12121212-1212-4121-8121-121212121212"


def _outbox_row() -> dict[str, Any]:
    return {
        "id": OUTBOX,
        "name": "Session outbox",
        "type": "cloud_outbox",
        "managed_key": f"cloud-outbox:{USER['id']}",
        "created_by": USER["id"],
        "auto_attach": True,
        "scope_mode": "all",
        "policy_revision": 1,
        "project_ids": [],
        "config": {},
    }


def _defaults_deps(**over: Any):
    deps = _deps_with([_outbox_row()], **over)
    deps.store.list_default_datasource_candidates = AsyncMock(
        return_value=[_outbox_row()]
    )
    return deps


def _left_out(plan) -> list[dict[str, Any]]:
    status = plan.create_kwargs["initial_metadata"].get("cloud_mount_status") or {}
    return status.get("excluded") or []


@pytest.mark.asyncio
async def test_the_owners_default_outbox_stays_virtual():
    """The new-chat draft's case: no connector list, the defaults attach the
    owner's outbox (dev's fa2a9885 got none on Virtual); Virtual serves it
    itself now, so the Session starts in seconds, as a Virtual."""
    plan = await _plan(_defaults_deps(), use_datasource_defaults=True)

    assert _backend(plan) == "virtual"
    assert plan.selected_datasource_ids == [OUTBOX]
    assert _left_out(plan) == []


def _tier_refuses_the_outbox(deps):
    """A connector the Virtual tier itself refuses (a driver without the
    cloud_remote form): the selection drops it on Virtual."""
    deps.authorize_thread_datasource_selection.side_effect = (
        lambda _u, ids, *, workspace_backend, **_kw: (
            [] if workspace_backend == "virtual" else list(ids),
            {},
        )
    )
    deps.store.list_default_datasource_candidates = AsyncMock(
        return_value=[_outbox_row()]
    )
    return deps


def _outbox_on_opencloud(deps):
    """The owner's cloud is OpenCloud, whose outbox the Virtual delivery does
    not serve yet (the bearer token follows in the feature's second run)."""
    from dataclasses import replace

    return replace(
        deps,
        main_cloud_router=SimpleNamespace(
            active_instance_id="instance-1",
            for_owner=lambda _user: SimpleNamespace(backend_id="opencloud"),
        ),
    )


@pytest.mark.asyncio
async def test_a_virtual_session_records_the_cloud_folders_it_leaves_out():
    """A connector the Virtual tier refuses, with no Container allowed: it is
    recorded as left out, so the session's cloud notice and the agent name
    it, never silent. (A folder Virtual admits but cannot bind, a provider
    gap, is named at each delivery instead, by the planner's exclusions.)"""
    plan = await _plan(
        _tier_refuses_the_outbox(_defaults_deps(container_granted=False)),
        datasource_ids=[OUTBOX],
    )

    assert _backend(plan) == "virtual"
    assert plan.selected_datasource_ids == []
    assert _left_out(plan) == [
        {
            "source_ref": OUTBOX,
            "mount_kind": "cloud_outbox",
            "reason": "mount_unsupported",
            "detail": "virtual_workspace",
            "title": "Session outbox",
        }
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("unavailable", "expected"),
    [({}, True), ({"containers": False}, False), ({"container_granted": False}, False)],
    ids=["container", "no-container-provisioner", "container-not-granted"],
)
async def test_the_new_session_form_predicts_the_container(unavailable, expected):
    """The form's preview asks admission's own rule for the defaults it
    submits: an outbox on a provider Virtual does not serve yet still takes
    a Container."""
    deps = _outbox_on_opencloud(_defaults_deps(**unavailable))

    assert (
        await ta.cloud_folders_need_a_container(
            USER, project_id=PROJECT, dependencies=deps
        )
        is expected
    )


@pytest.mark.asyncio
async def test_the_new_session_form_predicts_virtual_for_the_default_outbox():
    deps = _defaults_deps()

    assert (
        await ta.cloud_folders_need_a_container(
            USER, project_id=PROJECT, dependencies=deps
        )
        is False
    )


@pytest.mark.asyncio
async def test_the_form_predicts_the_container_for_a_protected_level():
    deps = _deps_with([_folder_row()])
    assert (
        await ta.cloud_folders_need_a_container(
            USER,
            project_id=PROJECT,
            datasource_ids=[FOLDER],
            datasource_access={FOLDER: "protected"},
            dependencies=deps,
        )
        is True
    )
    assert (
        await ta.cloud_folders_need_a_container(
            USER,
            project_id=PROJECT,
            datasource_ids=[FOLDER],
            datasource_access={FOLDER: "read_write"},
            dependencies=deps,
        )
        is False
    )


@pytest.mark.asyncio
async def test_the_new_session_form_predicts_virtual_without_cloud_connectors():
    deps = _deps_with([_kb_row()])
    deps.store.list_default_datasource_candidates = AsyncMock(return_value=[_kb_row()])

    assert (
        await ta.cloud_folders_need_a_container(
            USER, project_id=PROJECT, dependencies=deps
        )
        is False
    )


@pytest.mark.asyncio
async def test_a_refused_project_predicts_no_container():
    deps = _defaults_deps()
    deps.authorize_thread_project_ids.side_effect = HTTPException(
        status_code=403, detail="nope"
    )

    assert (
        await ta.cloud_folders_need_a_container(
            USER, project_id=PROJECT, dependencies=deps
        )
        is False
    )


@pytest.mark.asyncio
async def test_the_form_predicts_from_the_connectors_it_submits():
    """Unticked in the form, the cloud folder takes no Container; ticked, a
    folder Virtual cannot serve does, even where it isn't a default."""
    deps = _deps_with([_opencloud_folder_row()])
    deps.store.list_default_datasource_candidates = AsyncMock(return_value=[_kb_row()])
    assert (
        await ta.cloud_folders_need_a_container(
            USER, project_id=PROJECT, datasource_ids=[FOLDER], dependencies=deps
        )
        is True
    )

    deps = _deps_with([_kb_row()])
    assert (
        await ta.cloud_folders_need_a_container(
            USER, project_id=PROJECT, datasource_ids=[KB], dependencies=deps
        )
        is False
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("unavailable", "expected"),
    [({}, True), ({"containers": False}, False), ({"container_granted": False}, False)],
    ids=["container", "no-container-provisioner", "container-not-granted"],
)
async def test_the_form_knows_whether_a_cloud_folder_can_take_a_container(
    unavailable, expected
):
    deps = _deps_with([], **unavailable)

    assert (
        await ta.cloud_folders_can_take_a_container(
            USER, project_id=PROJECT, dependencies=deps
        )
        is expected
    )


def _listing_deps(**over: Any):
    """Defaults hold the owner's outbox; the store resolves the ids asked."""
    deps = _defaults_deps(**over)
    rows = {OUTBOX: _outbox_row(), KB: _kb_row()}
    deps.store.resolve_datasources_for_thread = AsyncMock(
        side_effect=lambda datasource_ids=None, **_kw: [
            rows[cid] for cid in datasource_ids or [] if cid in rows
        ]
    )
    return deps


@pytest.mark.asyncio
async def test_a_chosen_virtual_with_a_list_records_the_defaults_it_cannot_serve():
    """The cockpit always sends a list and greys out what a chosen Virtual
    can't serve: a default the Virtual tier refuses is still named as left
    out, so the notice, the agent and an upgrade know it (owner 2026-10-21).
    The owner's outbox is served on Virtual now, so it is not among them."""
    plan = await _plan(
        _listing_deps(),
        datasource_ids=[KB],
        workspace={"template": {"inline": {"backend": "virtual"}}},
    )

    assert _backend(plan) == "virtual"
    assert plan.selected_datasource_ids == [KB]
    assert _left_out(plan) == []

    plan = await _plan(
        _outbox_on_opencloud(_listing_deps()),
        datasource_ids=[KB],
        workspace={"template": {"inline": {"backend": "virtual"}}},
    )
    assert [entry["source_ref"] for entry in _left_out(plan)] == [OUTBOX]


@pytest.mark.asyncio
async def test_a_list_beside_a_defaulted_tier_is_the_users_own_choice():
    """Unticked in the New Session form: no Container and nothing recorded."""
    plan = await _plan(_listing_deps(), datasource_ids=[KB])

    assert _backend(plan) == "virtual"
    assert plan.selected_datasource_ids == [KB]
    assert _left_out(plan) == []
