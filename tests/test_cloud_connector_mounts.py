"""A session's cloud folders are its attached cloud connectors (main cloud 3.5).

Pinned here: each attached ``cloud_folder`` / ``cloud_outbox`` connector is
bound on its own (mount id = connector id, the project's folder under the
project's name, the outbox at ``cloud/outbox``); one that cannot be mounted
is excluded with a closed reason and nothing takes its place; an explicit
empty selection mounts nothing; a protected session names the others
``beside_protected``; End rebuilds the old rule only for a session the old
rule mounted (R1); a session created before the rule converts once; and the
outbox's folder is created, recorded and its share retried.
"""

from __future__ import annotations

import dataclasses
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from orchestrator.services import agent_cloud_mounts as mounts
from orchestrator.services import cloud_connector_mounts as rule
from orchestrator.services import session_cloud_preparation as prep
from orchestrator.services.cloud import RcloneMountSpec
from orchestrator.services.cloud.retry import BucketFull

USER = "00000000-0000-4000-8000-0000000000a1"
OTHER = "00000000-0000-4000-8000-0000000000a2"
PROJECT = "00000000-0000-4000-8000-0000000000b1"
OTHER_PROJECT = "00000000-0000-4000-8000-0000000000b2"
INSTANCE = "00000000-0000-4000-8000-00000000c10d"
FOLDER = "00000000-0000-4000-8000-0000000000f1"
FOLDER_2 = "00000000-0000-4000-8000-0000000000f3"
OUTBOX = "00000000-0000-4000-8000-0000000000f2"
THREAD = "00000000-0000-4000-8000-0000000000d1"
GENERATION = "22222222-2222-4222-8222-222222222222"


def _folder_row(
    cid=FOLDER, project=PROJECT, *, name="Acme Launch cloud folder", **over
):
    row = {
        "id": cid,
        "type": "cloud_folder",
        "name": name,
        "config": {
            "folder_kind": "project",
            "project_id": project,
            "backend_id": "nextcloud",
            "backend_instance_id": INSTANCE,
            "folder_handle": "stale-handle",
        },
        "managed_key": f"project-cloud:{project}",
        "read_only": None,
        "project_read_only": None,
    }
    row.update(over)
    return row


def _outbox_row(owner=USER, **over):
    row = {
        "id": OUTBOX,
        "type": "cloud_outbox",
        "name": "Session outbox",
        "config": {},
        "managed_key": f"cloud-outbox:{owner}",
        "created_by": owner,
        "read_only": None,
        "project_read_only": None,
    }
    row.update(over)
    return row


class Store:
    """The reads the mount rule makes, and the writes conversion makes."""

    def __init__(self, rows=(), projects=None, threads=None):
        self.rows = {row["id"]: dict(row) for row in rows}
        self.projects = (
            projects
            if projects is not None
            else {PROJECT: _project(PROJECT, "Acme Launch")}
        )
        self.threads = threads or {}
        self.resolved: list[list[str]] = []
        self.selections: list[tuple[str, list[str], dict]] = []
        self.metadata: list[tuple[str, dict]] = []
        self.main_cloud: list[dict] = []

    async def resolve_datasources_for_thread(self, *, datasource_ids, project_ids):
        self.resolved.append(list(datasource_ids))
        # Like the real resolver: no managed key, and its own order.
        return [
            {k: v for k, v in self.rows[cid].items() if k != "managed_key"}
            for cid in sorted(datasource_ids)
            if cid in self.rows
        ]

    async def get_datasource(self, cid):
        row = self.rows.get(cid)
        return (
            {k: v for k, v in row.items() if k != "project_read_only"} if row else None
        )

    async def get_datasource_by_managed_key(self, key):
        return next(
            (dict(r) for r in self.rows.values() if r.get("managed_key") == key), None
        )

    async def get_project(self, project_id):
        return self.projects.get(project_id)

    async def get_user(self, user_id):
        return {"id": user_id, "email": "u@example.com", "display_name": "U"}

    async def set_thread_datasource_ids(self, thread_id, ids, **kwargs):
        self.selections.append((thread_id, list(ids), kwargs))
        thread = self.threads.setdefault(thread_id, {"id": thread_id, "metadata": {}})
        thread["metadata"] = {**thread["metadata"], "datasource_ids": list(ids)}
        return True

    async def add_thread_datasource_ids(
        self, thread_id, added, *, datasource_policy_revisions, provenance=None
    ):
        """Like the real store: appended to the selection as it is now."""
        thread = self.threads.setdefault(thread_id, {"id": thread_id, "metadata": {}})
        current = list(thread["metadata"].get("datasource_ids") or [])
        selected = current + [cid for cid in added if cid not in current]
        thread["metadata"] = {**thread["metadata"], "datasource_ids": selected}
        self.selections.append(
            (
                thread_id,
                selected,
                {
                    "datasource_policy_revisions": dict(datasource_policy_revisions),
                    "provenance": provenance,
                },
            )
        )
        return selected

    async def merge_thread_cloud_metadata(self, thread_id, patch):
        self.metadata.append((thread_id, dict(patch)))
        thread = self.threads.setdefault(thread_id, {"id": thread_id, "metadata": {}})
        thread["metadata"] = {**thread["metadata"], **patch}
        return True

    async def get_thread(self, thread_id):
        thread = self.threads.get(thread_id)
        return dict(thread) if thread else None

    async def update_thread_main_cloud(self, thread_id, **kwargs):
        self.main_cloud.append({"thread_id": thread_id, **kwargs})
        thread = self.threads.setdefault(thread_id, {"id": thread_id, "metadata": {}})
        thread["main_cloud_session_handle"] = kwargs["session_handle"]
        thread["main_cloud_share_handle"] = kwargs.get("share_handle")
        return True


def _project(pid, name, handle="project-handle", instance=INSTANCE):
    return {
        "id": pid,
        "name": name,
        "main_cloud_backend": "nextcloud",
        "main_cloud_backend_instance_id": instance,
        "main_cloud_folder_handle": handle,
    }


class Backend:
    backend_id = "nextcloud"
    backend_instance_id = INSTANCE
    is_initialized = True
    is_configured = True

    def __init__(self):
        self.specs: list[dict[str, Any]] = []

    async def build_rclone_mount_spec(
        self,
        *,
        handle,
        mount_kind,
        target_path,
        access,
        subject=None,
        prefer_public_url=False,
    ):
        self.specs.append(
            {
                "handle": handle.native_id,
                "mount_kind": mount_kind,
                "target_path": target_path,
                "access": access,
                "prefer_public_url": prefer_public_url,
            }
        )
        return RcloneMountSpec(
            source_type="webdav",
            source_config={
                "url": f"http://srw-nextcloud/dav/{handle.native_id}/",
                "vendor": "nextcloud",
                "user": "agent-service",
            },
            auth={"type": "basic", "password": "agent-pass"},
        )


def _router(backend, *, instances=(INSTANCE,)):
    def for_backend_instance(instance_id, *, expected_backend_id):
        if instance_id not in instances:
            raise RuntimeError("not cached")
        return backend

    return SimpleNamespace(
        for_thread_optional=lambda thread: backend
        if thread.get("main_cloud_backend")
        else None,
        for_backend_instance=for_backend_instance,
        active_instance_id=INSTANCE,
        for_owner=lambda owner: backend,
    )


def _deps(store, backend=None, **router_kw):
    backend = backend or Backend()
    return mounts.AgentCloudMountDependencies(
        store=store,
        cloud_router=_router(backend, **router_kw),
        cloud_tasks=SimpleNamespace(protected_engage_get=lambda key: None),
        is_protected_cloud_mode_enabled=lambda: True,
        cloud_workspace_driver=lambda: "rclone_mount",
        slugify_mount_name=lambda name: "".join(
            ch.lower() if ch.isalnum() else "_" for ch in name
        ).strip("_")
        or "project",
    )


def _thread(ids, **over):
    thread = {
        "id": THREAD,
        "user_id": USER,
        "project_id": PROJECT,
        "main_cloud_backend": "nextcloud",
        "main_cloud_backend_instance_id": INSTANCE,
        "main_cloud_session_handle": "sessions/00000000",
        "main_cloud_share_handle": "17",
        "metadata": {
            "cloud_mount_rule": "connectors",
            "cloud_outbox": {"connector_id": OUTBOX, "state": "ready"},
            "datasource_ids": list(ids),
        },
    }
    metadata = over.pop("metadata", None)
    thread.update(over)
    if metadata is not None:
        thread["metadata"] = {**thread["metadata"], **metadata}
    return thread


@pytest.fixture
def denied(monkeypatch):
    """Which connectors the owner may no longer attach (the policy itself is
    pinned in tests/test_datasource_policy.py)."""
    refused: set[str] = set()

    async def fake(thread, connector_ids, project_ids, store):
        return [
            SimpleNamespace(datasource_id=cid, denied=cid in refused)
            for cid in connector_ids
        ]

    monkeypatch.setattr(rule, "_classified", fake)
    return refused


async def _live(thread, store, *, vm=False, **deps_kw):
    deps = _deps(store, **deps_kw)
    live = await mounts._resolve_live_mount_set(
        thread, mount_rows=[], runtime_is_vm=vm, dependencies=deps
    )
    return live, deps


# --------------------------------------------------------------------------- #
# Each connector on its own
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_the_project_folder_and_the_outbox_mount_each_under_their_own_name(
    denied,
):
    store = Store([_folder_row(), _outbox_row()])
    backend = Backend()
    live, _ = await _live(_thread([FOLDER, OUTBOX]), store, backend=backend)
    assert live.excluded == [] and live.fallback is False
    by_id = {mount["mount_id"]: mount for mount in live.mounts}
    assert set(by_id) == {FOLDER, OUTBOX}
    project, outbox = by_id[FOLDER], by_id[OUTBOX]
    assert (project["workspace_name"], project["target_path"]) == (
        "acme_launch",
        "/cloud/acme_launch",
    )
    assert (project["mount_kind"], project["access"], project["source_ref"]) == (
        "project",
        "read_write",
        FOLDER,
    )
    assert (outbox["workspace_name"], outbox["target_path"]) == (
        "outbox",
        "/cloud/outbox",
    )
    assert (outbox["mount_kind"], outbox["access"]) == ("cloud_outbox", "read_write")
    # The project's current folder, not the connector's stale copy of it; the
    # outbox is the session's own folder.
    handles = {spec["target_path"]: spec["handle"] for spec in backend.specs}
    assert handles == {
        "/cloud/acme_launch": "project-handle",
        "/cloud/outbox": "sessions/00000000",
    }


@pytest.mark.asyncio
async def test_an_explicit_empty_selection_mounts_nothing_and_reads_nothing(denied):
    store = Store([_folder_row(), _outbox_row()])
    live, _ = await _live(_thread([]), store)
    assert live.mounts == [] and live.excluded == []
    assert store.resolved == []


@pytest.mark.asyncio
async def test_without_the_project_chip_nothing_takes_its_place(denied):
    store = Store([_folder_row(), _outbox_row()])
    live, _ = await _live(_thread([OUTBOX]), store)
    assert [m["mount_id"] for m in live.mounts] == [OUTBOX]
    assert live.excluded == []


@pytest.mark.asyncio
async def test_a_broken_folder_is_named_and_the_others_still_mount(denied):
    gone = {PROJECT: _project(PROJECT, "Acme Launch", instance="not-cached")}
    store = Store([_folder_row(), _outbox_row()], projects=gone)
    live, _ = await _live(_thread([FOLDER, OUTBOX]), store)
    assert [m["mount_id"] for m in live.mounts] == [OUTBOX]
    assert live.excluded == [
        {
            "source_ref": FOLDER,
            "mount_kind": "project",
            "reason": "unbuildable",
            "name": "acme_launch",
            "detail": "backend_unavailable",
            # The level it would have bound at, for the session header.
            "level": "read_write",
            "title": "Acme Launch cloud folder",
        }
    ]


@pytest.mark.asyncio
async def test_a_project_that_names_no_folder_now_mounts_nothing(denied):
    store = Store(
        [_folder_row()],
        projects={PROJECT: _project(PROJECT, "Acme Launch", handle=None)},
    )
    live, _ = await _live(_thread([FOLDER]), store)
    assert live.mounts == []
    assert (live.excluded[0]["reason"], live.excluded[0]["detail"]) == (
        "unbuildable",
        "no_transport",
    )


class GrantBackend(Backend):
    """A provider with per-folder grants (``SupportsFolderGrant``)."""

    def folder_grant_webdav_url(self, handle, principal_id):
        return f"http://srw-nextcloud/dav/groupfolders/{principal_id}/x/"

    async def ensure_folder_principal(self, handle, **kwargs):
        raise AssertionError("a mount never mints a grant")

    async def revoke_folder_principal(self, **kwargs):
        raise AssertionError("a mount never revokes a grant")

    def folder_principal_client(self, principal_id, password):
        raise AssertionError("a mount never probes")

    async def seed_canary_fixture(self, handle):
        raise AssertionError("a mount never probes")

    async def remove_canary_fixture(self, handle, fixture):
        raise AssertionError("a mount never probes")

    async def build_rclone_mount_spec(self, *, subject=None, **kwargs):
        spec = await super().build_rclone_mount_spec(subject=subject, **kwargs)
        self.specs[-1]["subject"] = subject
        return spec


READER = "srw-reader-f-" + "1" * 32
WRITER = "srw-writer-f-" + "2" * 32


def _grant(kind, principal, *, status="active", handle="project-handle"):
    import hashlib

    return {
        "id": "33333333-3333-4333-8333-333333333333",
        "kind": kind,
        "status": status,
        "datasource_id": FOLDER,
        "project_id": PROJECT,
        "user_id": USER,
        "backend_id": "nextcloud",
        "backend_instance_id": INSTANCE,
        "folder_handle_sha256": hashlib.sha256(handle.encode()).hexdigest(),
        "principal_id": principal,
        "webdav_url": "http://srw-nextcloud/dav/x/",
        "credentials": f"{kind}-password",
    }


class GrantStore(Store):
    def __init__(self, *args, grants=(), failure=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.grants = {(g["datasource_id"], g["user_id"], g["kind"]): g for g in grants}
        self.failure = failure

    async def get_live_cloud_folder_grant(self, datasource_id, user_id, kind):
        return self.grants.get((datasource_id, user_id, kind))

    async def latest_cloud_folder_grant_failure(self, datasource_id, user_id, kind):
        return self.failure


@pytest.mark.asyncio
async def test_a_root_vm_gets_its_folder_through_its_reader_and_no_outbox(denied):
    store = GrantStore(
        [_folder_row(), _outbox_row()], grants=[_grant("reader", READER)]
    )
    backend = GrantBackend()
    live, _ = await _live(_thread([FOLDER, OUTBOX]), store, vm=True, backend=backend)
    assert [(m["mount_id"], m["access"]) for m in live.mounts] == [
        (FOLDER, "read_only")
    ]
    [spec] = backend.specs
    # The reader's own credential, over the public URL: never agent-service.
    assert (spec["subject"].username, spec["subject"].credential) == (
        READER,
        "reader-password",
    )
    assert spec["prefer_public_url"] is True
    assert [(e["source_ref"], e["reason"]) for e in live.excluded] == [
        (OUTBOX, "access_unavailable")
    ]
    assert all(e.get("detail") for e in live.excluded)


@pytest.mark.asyncio
async def test_a_writer_folder_mounts_as_the_owners_writer(denied):
    store = GrantStore([_folder_row()], grants=[_grant("writer", WRITER)])
    backend = GrantBackend()
    live, _ = await _live(_thread([FOLDER]), store, backend=backend)
    assert [(m["mount_id"], m["access"]) for m in live.mounts] == [
        (FOLDER, "read_write")
    ]
    assert backend.specs[0]["subject"].username == WRITER


@pytest.mark.parametrize(
    ("vm", "grants", "failure", "reason", "detail"),
    [
        (False, [], None, "grant_unavailable", "not_ready"),
        (True, [], "probe_failed", "reader_unavailable", "probe_failed"),
        # A grant made on the folder before it moved serves nothing.
        (
            False,
            [_grant("writer", WRITER, handle="old-handle")],
            None,
            "grant_unavailable",
            "not_ready",
        ),
        # A writer never stands in for a missing reader.
        (True, [_grant("writer", WRITER)], None, "reader_unavailable", "not_ready"),
    ],
)
@pytest.mark.asyncio
async def test_without_its_grant_a_folder_is_left_out_never_the_service_account(
    denied, vm, grants, failure, reason, detail
):
    store = GrantStore([_folder_row()], grants=grants, failure=failure)
    backend = GrantBackend()
    live, _ = await _live(_thread([FOLDER]), store, vm=vm, backend=backend)
    assert live.mounts == [] and backend.specs == []
    assert [(e["reason"], e["detail"], e["name"]) for e in live.excluded] == [
        (reason, detail, "acme_launch")
    ]
    # It says the level it would have bound at: a VM's folder is its reader's.
    assert [e["level"] for e in live.excluded] == ["read_only" if vm else "read_write"]


@pytest.mark.asyncio
async def test_preparing_mints_the_bound_levels_grant_outside_the_mount(
    denied, monkeypatch
):
    from orchestrator.services.cloud import folder_grants

    calls = []

    async def ensure(store, router, target, *, user_id, kind):
        calls.append((target.datasource_id, target.folder_handle, user_id, kind))

    monkeypatch.setattr(folder_grants, "ensure_folder_grant", ensure)
    store = GrantStore([_folder_row(), _outbox_row()])
    deps = _deps(store, backend=GrantBackend())
    thread = _thread([FOLDER, OUTBOX])
    await mounts.prepare_folder_grants(
        thread, mount_rows=[], runtime_is_vm=False, dependencies=deps
    )
    await mounts.prepare_folder_grants(
        thread, mount_rows=[], runtime_is_vm=True, dependencies=deps
    )
    # The project's current folder, the session owner, the level's kind.
    assert calls == [
        (FOLDER, "project-handle", USER, "writer"),
        (FOLDER, "project-handle", USER, "reader"),
    ]
    # A provider without grants, a protected session: nothing to mint.
    calls.clear()
    await mounts.prepare_folder_grants(
        thread, mount_rows=[], runtime_is_vm=False, dependencies=_deps(store)
    )
    protected = _thread([FOLDER], metadata={"protected_cloud": True})
    await mounts.prepare_folder_grants(
        protected, mount_rows=[], runtime_is_vm=False, dependencies=deps
    )
    assert calls == []


@pytest.mark.asyncio
async def test_a_connector_the_owner_may_no_longer_attach_is_not_eligible(denied):
    denied.add(FOLDER)
    store = Store([_folder_row(), _outbox_row()])
    live, _ = await _live(_thread([FOLDER, OUTBOX]), store)
    assert [m["mount_id"] for m in live.mounts] == [OUTBOX]
    assert [(e["source_ref"], e["reason"]) for e in live.excluded] == [
        (FOLDER, "not_eligible")
    ]


@pytest.mark.parametrize(
    ("rows", "projects", "reason"),
    [
        ([_folder_row(managed_key=None)], None, "config_invalid"),
        ([_folder_row()], {}, "not_eligible"),
        ([_outbox_row(owner=OTHER)], None, "not_eligible"),
    ],
)
@pytest.mark.asyncio
async def test_why_a_connector_mounts_nothing(denied, rows, projects, reason):
    store = Store(rows, projects=projects)
    live, _ = await _live(_thread([rows[0]["id"]]), store)
    assert live.mounts == []
    assert [e["reason"] for e in live.excluded] == [reason]


@pytest.mark.asyncio
async def test_an_outbox_srw_has_not_created_says_why(denied):
    store = Store([_outbox_row()])
    thread = _thread(
        [OUTBOX],
        main_cloud_session_handle=None,
        metadata={
            "cloud_outbox": {
                "connector_id": OUTBOX,
                "state": "unavailable",
                "reason": "create_failed",
            }
        },
    )
    live, _ = await _live(thread, store)
    assert live.mounts == []
    assert live.excluded == [
        {
            "source_ref": OUTBOX,
            "mount_kind": "cloud_outbox",
            "reason": "no_session_folder",
            "name": "outbox",
            "detail": "create_failed",
            "title": "Session outbox",
        }
    ]


@pytest.mark.asyncio
async def test_names_never_collide_and_the_outbox_keeps_its_own(denied):
    projects = {
        PROJECT: _project(PROJECT, "Outbox"),
        OTHER_PROJECT: _project(OTHER_PROJECT, "outbox"),
    }
    store = Store(
        [_folder_row(), _folder_row(FOLDER_2, OTHER_PROJECT), _outbox_row()],
        projects=projects,
    )
    live, _ = await _live(_thread([FOLDER, FOLDER_2, OUTBOX]), store)
    names = {m["mount_id"]: m["workspace_name"] for m in live.mounts}
    assert names == {OUTBOX: "outbox", FOLDER: "outbox-2", FOLDER_2: "outbox-3"}


def _with_recorded_names(thread, store):
    """The thread as it is after the names its last binding recorded."""
    recorded = [
        patch[mounts.NAMES_KEY]
        for tid, patch in store.metadata
        if tid == thread["id"] and mounts.NAMES_KEY in patch
    ]
    if not recorded:
        return thread
    return {
        **thread,
        "metadata": {**thread["metadata"], mounts.NAMES_KEY: recorded[-1]},
    }


@pytest.mark.asyncio
async def test_a_folders_path_never_moves_to_another_folder(denied):
    """Two projects called "Docs". At the first attach A's writer is not
    ready: it still keeps cloud/docs, and B mounts at cloud/docs-2. At the
    next, with A's writer ready, neither moves; nor after B's project is
    renamed."""
    projects = {
        PROJECT: _project(PROJECT, "Docs"),
        OTHER_PROJECT: _project(OTHER_PROJECT, "Docs"),
    }
    store = GrantStore(
        [_folder_row(), _folder_row(FOLDER_2, OTHER_PROJECT)],
        projects=projects,
        grants=[
            _grant("writer", WRITER)
            | {"datasource_id": FOLDER_2, "project_id": OTHER_PROJECT}
        ],
    )
    thread = _thread([FOLDER, FOLDER_2])
    live, _ = await _live(thread, store, backend=GrantBackend())
    assert {m["mount_id"]: m["workspace_name"] for m in live.mounts} == {
        FOLDER_2: "docs-2"
    }
    assert [(e["source_ref"], e["name"]) for e in live.excluded] == [(FOLDER, "docs")]
    thread = _with_recorded_names(thread, store)
    store.grants[(FOLDER, USER, "writer")] = _grant("writer", WRITER)
    projects[OTHER_PROJECT] = _project(OTHER_PROJECT, "Renamed")
    live, _ = await _live(thread, store, backend=GrantBackend())
    assert {m["mount_id"]: m["workspace_name"] for m in live.mounts} == {
        FOLDER: "docs",
        FOLDER_2: "docs-2",
    }
    # A folder the session may no longer use is still named where it was.
    denied.add(FOLDER_2)
    live, _ = await _live(thread, store, backend=GrantBackend())
    assert [(e["source_ref"], e["reason"], e["name"]) for e in live.excluded] == [
        (FOLDER_2, "not_eligible", "docs-2")
    ]


@pytest.mark.asyncio
async def test_two_names_never_share_a_state_directory(denied):
    """``docs-2`` (a second "Docs") and ``docs_2`` (a project "Docs 2") map
    to one rclone state directory, config and remote: never both in a set."""
    from shared.runtime.services.cloud_mount import mount_state_name

    third = "00000000-0000-4000-8000-0000000000b3"
    folder_3 = "00000000-0000-4000-8000-0000000000f4"
    projects = {
        PROJECT: _project(PROJECT, "Docs 2"),
        OTHER_PROJECT: _project(OTHER_PROJECT, "Docs"),
        third: _project(third, "Docs"),
    }
    store = Store(
        [
            _folder_row(),
            _folder_row(FOLDER_2, OTHER_PROJECT),
            _folder_row(folder_3, third),
        ],
        projects=projects,
    )
    live, _ = await _live(_thread([FOLDER, FOLDER_2, folder_3]), store)
    names = [m["workspace_name"] for m in live.mounts]
    assert names == ["docs_2", "docs", "docs-3"]
    assert len({mount_state_name(name) for name in names}) == 3


ROOT = "00000000-0000-4000-8000-0000000000f9"


def _root_row(owner=USER, *, connected=True, revoked=False):
    config = {
        "folder_kind": "user_root",
        "project_id": PROJECT,
        "backend_id": "nextcloud",
        "backend_instance_id": INSTANCE,
    }
    credentials = {}
    if connected or revoked:
        config.update(
            {
                "folder_handle": (
                    '{"backend":"nextcloud","native_id":"home:uid-ann",'
                    '"vendor_meta":{"kind":"user_home","username":"uid-ann"}}'
                ),
                "account_id": "uid-ann",
                "grant": {"state": "revoked" if revoked else "connected"},
            }
        )
    if connected and not revoked:
        credentials = {"username": "ann", "password": "app-pw-SECRET"}
    return _folder_row(
        ROOT,
        name="My cloud",
        config=config,
        credentials=credentials,
        managed_key=f"user-cloud-root:{owner}",
        created_by=owner,
    )


class SubjectBackend(Backend):
    def __init__(self):
        super().__init__()
        self.subjects = []

    async def build_rclone_mount_spec(self, **kwargs):
        self.subjects.append(kwargs.get("subject"))
        return await super().build_rclone_mount_spec(**kwargs)


@pytest.mark.asyncio
async def test_a_user_root_mounts_as_its_user_at_my_cloud(denied):
    """Main cloud slice 4: the owner's own root, with the owner's own
    credential (the binder's subject, never a grant's or the service
    account's), at cloud/my_cloud; the credential is never printed."""
    backend = SubjectBackend()
    live, _ = await _live(_thread([ROOT]), Store([_root_row()]), backend=backend)
    assert live.excluded == []
    [mount] = live.mounts
    assert (mount["workspace_name"], mount["mount_kind"], mount["access"]) == (
        "my_cloud",
        "user_root",
        "read_write",
    )
    assert backend.specs[0]["handle"] == "home:uid-ann"
    [subject] = backend.subjects
    assert (subject.username, subject.credential) == ("ann", "app-pw-SECRET")
    assert "app-pw-SECRET" not in repr(subject)
    assert "app-pw-SECRET" not in repr(live.excluded)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("row", "reason"),
    [
        (_root_row(connected=False), "cloud_not_connected"),
        (_root_row(revoked=True), "cloud_grant_revoked"),
        # Another user's root, even one its policy let through.
        (_root_row(OTHER), "not_eligible"),
    ],
)
async def test_a_user_root_that_cannot_mount_says_why_and_nothing_takes_its_place(
    denied, row, reason
):
    live, _ = await _live(_thread([ROOT]), Store([row]))
    assert live.mounts == []
    assert [(e["reason"], e["mount_kind"], e["title"]) for e in live.excluded] == [
        (reason, "user_root", "My cloud")
    ]


@pytest.mark.asyncio
async def test_a_user_root_is_not_offered_on_a_vm(denied):
    live, _ = await _live(_thread([ROOT]), Store([_root_row()]), vm=True)
    assert live.mounts == []
    assert [e["reason"] for e in live.excluded] == ["access_unavailable"]


@pytest.mark.asyncio
async def test_the_level_the_session_resolved_binds_as_stored(denied):
    store = Store([_folder_row()])
    thread = _thread([FOLDER], metadata={"datasource_access": {FOLDER: "read_write"}})
    backend = Backend()
    live, _ = await _live(thread, store, backend=backend)
    assert [m["access"] for m in live.mounts] == ["read_write"]
    # An ordinary session never binds at the reviewed level.
    thread = _thread([FOLDER], metadata={"datasource_access": {FOLDER: "protected"}})
    live, _ = await _live(thread, store)
    assert live.mounts == []
    assert [e["reason"] for e in live.excluded] == ["access_unavailable"]


@pytest.mark.asyncio
async def test_a_protected_link_default_is_lowered_never_mounted_live(
    denied, monkeypatch
):
    store = Store([_folder_row(project_access="protected")])
    # Nextcloud's reader grant: lowered to read-only, never mounted live.
    backend = Backend()
    live, _ = await _live(_thread([FOLDER]), store, backend=backend)
    assert [m["access"] for m in live.mounts] == ["read_only"]

    from orchestrator.services.connector_drivers.cloud_folder import CloudFolderDriver

    monkeypatch.setattr(
        CloudFolderDriver,
        "offered_access",
        lambda self, row, ctx: ("read_only", "read_write")
        + (("protected",) if ctx.protected_mode_enabled else ()),
    )
    backend = Backend()
    live, _ = await _live(_thread([FOLDER]), store, backend=backend)
    assert [m["access"] for m in live.mounts] == ["read_only"]
    assert backend.specs[0]["access"] == "read_only"


@pytest.mark.asyncio
async def test_a_viewers_ceiling_takes_write_away_at_the_next_attach(monkeypatch):
    async def viewer(thread, connector_ids, project_ids, store):
        return [
            SimpleNamespace(datasource_id=cid, denied=False, access_ceiling="read_only")
            for cid in connector_ids
        ]

    monkeypatch.setattr(rule, "_classified", viewer)
    store = Store([_folder_row()])
    # Admitted as an editor: its default, read_write, stored with its origin.
    thread = _thread(
        [FOLDER],
        metadata={
            "datasource_access": {FOLDER: "read_write"},
            "datasource_selection": {
                "access": {FOLDER: {"access": "read_write", "origin": "link_default"}}
            },
        },
    )
    live, _ = await _live(thread, store)
    # Lowered to the reader's level, never written, and still mounted.
    assert [m["access"] for m in live.mounts] == ["read_only"]
    assert live.excluded == []
    # A level the session chose is bound as chosen or not at all.
    chosen = _thread(
        [FOLDER],
        metadata={
            "datasource_access": {FOLDER: "read_write"},
            "datasource_selection": {
                "access": {FOLDER: {"access": "read_write", "origin": "explicit"}}
            },
        },
    )
    live, _ = await _live(chosen, store)
    assert live.mounts == []
    assert [e["reason"] for e in live.excluded] == ["access_unavailable"]


@pytest.mark.asyncio
async def test_a_live_edit_keeps_whether_a_level_was_chosen_or_a_default():
    from orchestrator.services.connector_access import resolve_selection_access
    from shared.connectors.contract import AccessContext

    store = SimpleNamespace(get_user_project_roles=AsyncMock(return_value={}))
    rows = [{**_folder_row(), "project_access": None}]
    for previous, kept in (
        ({"access": "read_write", "origin": "link_default"}, "link_default"),
        ({"access": "read_write", "origin": "explicit"}, "explicit"),
        # Not the level kept, or no record: unknown, so a choice.
        ({"access": "read_only", "origin": "link_default"}, "materialized"),
        (None, "materialized"),
    ):
        resolved = await resolve_selection_access(
            store,
            owner_id=USER,
            datasource_ids=[FOLDER],
            project_ids=[PROJECT],
            requested={},
            ctx=AccessContext(workspace_backend="sandbox", execution="session"),
            keep={FOLDER: "read_write"},
            rows=rows,
            keep_origins={FOLDER: previous} if previous else None,
        )
        assert resolved.levels == {FOLDER: "read_write"}
        assert resolved.origins[FOLDER]["origin"] == kept


@pytest.mark.asyncio
@pytest.mark.parametrize("origin", ["link_default", "driver_default", "clamped"])
async def test_a_default_level_is_lowered_on_a_vm_never_dropped(denied, origin):
    """A sandbox session upgraded to a VM: its folder's defaulted read_write
    is resolved again for the VM, which offers read_only, instead of the
    folder being left out; a stored level of unknown origin binds as stored."""
    store = GrantStore([_folder_row()], grants=[_grant("reader", READER)])
    thread = _thread(
        [FOLDER],
        metadata={
            "datasource_access": {FOLDER: "read_write"},
            "datasource_selection": {
                "access": {FOLDER: {"access": "read_write", "origin": origin}}
            },
        },
    )
    live, _ = await _live(thread, store, vm=True, backend=GrantBackend())
    assert [m["access"] for m in live.mounts] == ["read_only"]
    unknown = _thread([FOLDER], metadata={"datasource_access": {FOLDER: "read_write"}})
    live, _ = await _live(unknown, store, vm=True, backend=GrantBackend())
    assert live.mounts == []
    assert [e["reason"] for e in live.excluded] == ["access_unavailable"]


# --------------------------------------------------------------------------- #
# The payloads
# --------------------------------------------------------------------------- #


def _ready():
    return {"workspace_container": {"status": "ready", "pod_ip": "10.42.0.10"}}


@pytest.mark.asyncio
async def test_the_in_workspace_payload_names_what_it_left_out(denied, monkeypatch):
    monkeypatch.delenv("CLOUD_RCLONE_ALLOW_CONTAINER", raising=False)
    store = Store([_folder_row(), _outbox_row()], projects={})
    thread = _thread([FOLDER, OUTBOX], main_cloud_share_handle=None)
    payload = await mounts._build_agent_cloud_mount(
        thread,
        mount_rows=[],
        metadata={**thread["metadata"], **_ready()},
        dependencies=_deps(store),
    )
    assert [m["mount_id"] for m in payload["mounts"]] == [OUTBOX]
    assert [e["reason"] for e in payload["excluded"]] == ["not_eligible"]
    assert payload["fallback"] is False and payload["outbox_shared"] is False


POD = "55555555-5555-4555-8555-555555555555"


@pytest.fixture
def plane_on(monkeypatch):
    monkeypatch.delenv("CLOUD_RCLONE_ALLOW_CONTAINER", raising=False)
    monkeypatch.setenv("CONNECTOR_IN_POD_OPENER_IMAGE", "registry.example/opener:1")
    monkeypatch.setenv("CONNECTOR_IN_POD_RCLONE_IMAGE", "registry.example/rclone:1")


def _planless_pod(**workspace):
    return {
        "workspace_container": {
            "status": "ready",
            "pod_ip": "10.42.0.10",
            "provisioner": "k8s",
            "_runtime_incarnation": POD,
            **workspace,
        }
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("record", "detail"),
    [
        (None, "needs_new_pod"),
        ({"runtime_incarnation": POD, "detail": "plan_failed"}, "plan_failed"),
        # Another Pod's failure never speaks for this one.
        ({"runtime_incarnation": "old-pod", "detail": "plan_failed"}, "needs_new_pod"),
    ],
)
async def test_a_plane_pod_without_a_plan_never_gets_a_password_in_its_workspace(
    denied, plane_on, record, detail
):
    """With the in-pod plane on, a session Pod created without a plan (it
    had nothing to mount then, its planning failed, it predates the plane)
    never gets a folder's static password in its workspace: the outbox's
    service account, a project folder's own account. Each is named with
    where and at what level it would have mounted."""
    store = Store([_folder_row(), _outbox_row()])
    backend = Backend()
    thread = _thread([FOLDER, OUTBOX])
    extra = {"cloud_mount_planless": record} if record else {}
    payload = await mounts._build_agent_cloud_mount(
        thread,
        mount_rows=[],
        metadata={**thread["metadata"], **_planless_pod(**extra)},
        dependencies=_deps(store, backend=backend),
    )
    assert payload["mounts"] == []
    assert "agent-pass" not in repr(payload)
    assert [
        (e["source_ref"], e["reason"], e["detail"], e["name"], e["level"])
        for e in payload["excluded"]
    ] == [
        (OUTBOX, "mount_unsupported", detail, "outbox", "read_write"),
        (FOLDER, "mount_unsupported", detail, "acme_launch", "read_write"),
    ]


@pytest.mark.asyncio
async def test_the_in_workspace_paths_the_plane_does_not_serve_still_mount(
    denied, plane_on, monkeypatch
):
    """A VM mounts its own folders; a workspace outside Kubernetes (the
    static pool) has no sidecars; a plane-off deployment keeps the in-pod
    path; and a bearer-token mount (OpenCloud, decision 39) never held a
    password at all."""
    store = Store([_folder_row(), _outbox_row()])
    thread = _thread([FOLDER, OUTBOX])
    deps = _deps(store)

    async def mounted(metadata):
        payload = await mounts._build_agent_cloud_mount(
            thread,
            mount_rows=[],
            metadata={**thread["metadata"], **metadata},
            dependencies=deps,
        )
        return sorted(m["mount_id"] for m in payload["mounts"])

    vm = {**_planless_pod(), "vm": {"status": "ready", "ssh_host": "vm-1"}}
    assert await mounted(vm) == [FOLDER]  # a VM is offered no outbox
    assert await mounted(_planless_pod(provisioner="docker")) == sorted(
        [FOLDER, OUTBOX]
    )
    monkeypatch.delenv("CONNECTOR_IN_POD_OPENER_IMAGE")
    assert await mounted(_planless_pod()) == sorted([FOLDER, OUTBOX])


@pytest.mark.asyncio
async def test_a_bearer_token_mount_still_mounts_in_a_planless_plane_pod(
    denied, plane_on
):
    class BearerBackend(Backend):
        async def build_rclone_mount_spec(self, **kwargs):
            await super().build_rclone_mount_spec(**kwargs)
            return RcloneMountSpec(
                source_type="webdav",
                source_config={"url": "https://oc/dav/spaces/x/", "vendor": "owncloud"},
                auth={"type": "keycloak_client_credentials", "client_id": "srw"},
            )

    store = Store([_folder_row()])
    thread = _thread([FOLDER])
    payload = await mounts._build_agent_cloud_mount(
        thread,
        mount_rows=[],
        metadata={**thread["metadata"], **_planless_pod()},
        dependencies=_deps(store, backend=BearerBackend()),
    )
    assert [m["mount_id"] for m in payload["mounts"]] == [FOLDER]


@pytest.mark.asyncio
async def test_a_planless_plane_pod_mints_no_grant_it_never_gets(
    denied, plane_on, monkeypatch
):
    from orchestrator.services.cloud import folder_grants

    calls = []

    async def ensure(store, router, target, *, user_id, kind):
        calls.append(kind)

    monkeypatch.setattr(folder_grants, "ensure_folder_grant", ensure)
    thread = _thread([FOLDER])
    store = GrantStore([_folder_row()], threads={THREAD: thread})
    store.list_thread_mounts = AsyncMock(return_value=[])
    deps = _deps(store, backend=GrantBackend())
    thread["metadata"] = {**thread["metadata"], **_planless_pod()}
    await mounts.prepare_thread_folder_grants(THREAD, dependencies=deps)
    assert calls == []
    # Off the plane the same session's workspace mounts it: its writer.
    monkeypatch.delenv("CONNECTOR_IN_POD_OPENER_IMAGE")
    await mounts.prepare_thread_folder_grants(THREAD, dependencies=deps)
    assert calls == ["writer"]


@pytest.mark.asyncio
async def test_a_folder_attached_while_the_workspace_runs_waits_for_the_next(
    denied, monkeypatch
):
    """live_attach=False: the running workspace never gets a cloud folder
    attached while it ran (next_start), nor is its grant minted; a new Pod
    (another runtime) mounts it."""
    monkeypatch.delenv("CLOUD_RCLONE_ALLOW_CONTAINER", raising=False)
    store = Store([_folder_row(), _outbox_row()])
    thread = _thread(
        [FOLDER, OUTBOX],
        metadata={"datasource_selection": {"attached_live": {FOLDER: f"pod:{POD}"}}},
    )
    workspace = _planless_pod(provisioner="docker")
    payload = await mounts._build_agent_cloud_mount(
        thread,
        mount_rows=[],
        metadata={**thread["metadata"], **workspace},
        dependencies=_deps(store),
    )
    assert [m["mount_id"] for m in payload["mounts"]] == [OUTBOX]
    assert [
        (e["source_ref"], e["reason"], e["detail"]) for e in payload["excluded"]
    ] == [(FOLDER, "mount_unsupported", "next_start")]
    later = _planless_pod(provisioner="docker", _runtime_incarnation="next-pod")
    payload = await mounts._build_agent_cloud_mount(
        thread,
        mount_rows=[],
        metadata={**thread["metadata"], **later},
        dependencies=_deps(store),
    )
    assert sorted(m["mount_id"] for m in payload["mounts"]) == sorted([FOLDER, OUTBOX])
    # A new Pod's planner is no attach: it plans the folder.
    live, _ = await _live(
        {**thread, "metadata": {**thread["metadata"], **workspace}}, store
    )
    assert sorted(m["mount_id"] for m in live.mounts) == sorted([FOLDER, OUTBOX])


@pytest.mark.parametrize("detail", ["needs_new_pod", "plan_failed", "next_start"])
def test_the_prompt_says_a_withheld_folder_mounts_when_the_workspace_starts_again(
    detail,
):
    from shared.runtime.services.cloud_mount.sidecar import excluded_row

    row = excluded_row({"reason": "mount_unsupported", "detail": detail, "name": "d"})
    assert "starts again" in row["text"] and row["reason"] == "mount_unsupported"
    plain = excluded_row({"reason": "mount_unsupported", "detail": "fuse_denied"})
    assert plain["text"].startswith("this workspace can't mount")


def test_the_prompt_says_a_virtual_session_needs_a_container_for_its_cloud():
    """A Virtual Session's left-out folders (owner 2026-10-21): the agent
    names them and the way to get them, never a workspace restart."""
    from shared.runtime.services.cloud_mount.sidecar import excluded_row

    row = excluded_row(
        {
            "reason": "mount_unsupported",
            "detail": "virtual_workspace",
            "title": "Session outbox",
            "mount_kind": "cloud_outbox",
        }
    )
    assert row["title"] == "Session outbox"
    assert "Virtual workspace" in row["text"] and "Container" in row["text"]
    assert "starts again" not in row["text"]


def test_attached_live_after_an_edit_keeps_only_this_runtimes_folders():
    metadata = {
        **_planless_pod(),
        "datasource_selection": {
            "attached_live": {
                FOLDER: f"pod:{POD}",
                FOLDER_2: "pod:old",
                OUTBOX: f"pod:{POD}",
            }
        },
    }
    rows = [_folder_row(), _folder_row(FOLDER_2), {"id": "pg", "type": "postgresql"}]
    assert rule.attached_live_after_edit(
        metadata, previous_ids={FOLDER, FOLDER_2, OUTBOX}, selected_rows=rows
    ) == {FOLDER: f"pod:{POD}"}
    # Newly added: the cloud folder waits, the database does not.
    assert rule.attached_live_after_edit(
        {**_planless_pod()}, previous_ids=set(), selected_rows=rows
    ) == {FOLDER: f"pod:{POD}", FOLDER_2: f"pod:{POD}"}
    assert (
        rule.attached_live_after_edit({}, previous_ids=set(), selected_rows=rows) == {}
    )
    vm = {"vm": {"status": "ready", "ssh_host": "vm-1", "_runtime_incarnation": "v1"}}
    assert rule.workspace_runtime_identity({**_planless_pod(), **vm}) == "vm:v1"


@pytest.mark.asyncio
async def test_an_in_workspace_set_that_mounts_nothing_still_names_each_folder(
    denied, monkeypatch
):
    """A VM whose folder's reader is not ready yet, beside an outbox no VM
    is offered: nothing mounts, and the payload still names both, with the
    name, level and detail each would have had. End has nothing to adopt."""
    store = GrantStore([_folder_row(), _outbox_row()])
    thread = _thread([FOLDER, OUTBOX])
    vm = {"vm": {"status": "ready", "ssh_host": "vm-1"}}
    metadata = {**thread["metadata"], **vm}
    deps = _deps(store, backend=GrantBackend())
    payload = await mounts._build_agent_cloud_mount(
        thread, mount_rows=[], metadata=metadata, dependencies=deps
    )
    assert payload["mounts"] == [] and "outbox_shared" not in payload
    assert [
        (e["source_ref"], e["reason"], e["name"], e.get("level"), e["title"])
        for e in payload["excluded"]
    ] == [
        (OUTBOX, "access_unavailable", "outbox", None, "Session outbox"),
        (
            FOLDER,
            "reader_unavailable",
            "acme_launch",
            "read_only",
            "Acme Launch cloud folder",
        ),
    ]
    assert all(e.get("detail") for e in payload["excluded"])
    monkeypatch.setattr(
        mounts,
        "_runtime_supports_terminal_rclone_retirement",
        lambda *args, **kwargs: True,
    )
    assert (
        await mounts._build_agent_cloud_mount(
            thread,
            mount_rows=[],
            metadata=metadata,
            terminal_retirement_token=7,
            dependencies=deps,
        )
        is None
    )
    # Nothing attached: nothing to name, no payload.
    assert (
        await mounts._build_agent_cloud_mount(
            _thread([]),
            mount_rows=[],
            metadata={**_thread([])["metadata"], **vm},
            dependencies=deps,
        )
        is None
    )


@pytest.mark.asyncio
async def test_a_protected_session_names_its_other_folders_beside_it(
    denied, monkeypatch
):
    row = {
        "status": "active",
        "backend": "nextcloud",
        "webdav_url": "https://nc/remote.php/dav/files/reader/P/",
        "reader_id": "reader",
        "credentials": "reader-secret",
        "runtime_generation": GENERATION,
    }

    async def grant(*args, **kwargs):
        return row

    monkeypatch.setattr(mounts, "_resolve_protected_grant", grant)
    monkeypatch.setattr(
        mounts,
        "_build_protected_cloud_mount",
        lambda r, *, thread_id: {"protected": True, "mounts": [{"mount_id": "p"}]},
    )
    store = Store([_folder_row(), _outbox_row()])
    thread = _thread([FOLDER, OUTBOX], runtime_generation=GENERATION)
    protected_rows = [
        {
            "mount_kind": "project",
            "source_ref": PROJECT,
            "backend_id": "nextcloud",
            "cloud_handle": "h",
        }
    ]
    # The session's protected folder (main cloud slice 5).

    async def protected_selection(_thread_id):
        return protected_rows

    store.list_protected_selection_rows = protected_selection
    payload = await mounts._build_agent_cloud_mount(
        thread,
        mount_rows=protected_rows,
        metadata={**thread["metadata"], "protected_cloud": True, **_ready()},
        dependencies=_deps(store),
    )
    # The project's own folder is the protected one; the outbox is beside it.
    assert payload["excluded"] == [
        {
            "source_ref": OUTBOX,
            "mount_kind": "cloud_outbox",
            "reason": "beside_protected",
            "name": "",
            "title": "Session outbox",
        }
    ]


@pytest.mark.asyncio
async def test_cloud_sync_sends_the_session_folder_only_with_the_outbox(denied):
    backend = SimpleNamespace(
        backend_id="nextcloud",
        is_initialized=True,
        get_session_folder_webdav_url=lambda handle: "https://nc/s/",
        cloud_sync_config=lambda url, target_user_sub=None: {"webdav_url": url},
    )
    deps = dataclasses.replace(
        _deps(Store()),
        cloud_router=SimpleNamespace(for_thread_optional=lambda t: backend),
    )
    with_outbox = mounts._build_agent_cloud_sync(_thread([OUTBOX]), dependencies=deps)
    assert with_outbox["session_folder"] == {"webdav_url": "https://nc/s/"}
    assert mounts._build_agent_cloud_sync(_thread([]), dependencies=deps) is None
    legacy = {"main_cloud_session_handle": "sessions/x", "metadata": {}}
    assert mounts._build_agent_cloud_sync(legacy, dependencies=deps)["session_folder"]


@pytest.mark.asyncio
async def test_the_sync_driver_names_each_cloud_folder_it_does_not_deliver(denied):
    # CLOUD_WORKSPACE_DRIVER=sync mounts nothing: the project's folder and
    # the owner's root are named, never silently missing; the outbox is the
    # session folder the sync already carries.
    store = Store([_folder_row(), _outbox_row(), _root_row()])
    deps = dataclasses.replace(_deps(store), cloud_workspace_driver=lambda: "sync")
    excluded = await mounts.sync_delivery_exclusions(
        _thread([FOLDER, OUTBOX, ROOT]), mount_rows=[], dependencies=deps
    )
    assert excluded == [
        {
            "source_ref": FOLDER,
            "mount_kind": "project",
            "reason": "delivery_unsupported",
            "name": "",
            "title": "Acme Launch cloud folder",
        },
        {
            "source_ref": ROOT,
            "mount_kind": "user_root",
            "reason": "delivery_unsupported",
            "name": "",
            "title": "My cloud",
        },
    ]
    assert "delivery_unsupported" in rule.EXCLUSION_REASONS
    # An older session keeps what the sync did for it from its rows; a
    # protected session has no sync at all; an explicit [] reads nothing.
    old_rule = _thread([FOLDER], metadata={"cloud_mount_rule": None})
    protected = _thread([FOLDER], metadata={"protected_cloud": True})
    for thread in (old_rule, protected, _thread([])):
        assert (
            await mounts.sync_delivery_exclusions(
                thread, mount_rows=[], dependencies=deps
            )
            == []
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("allow", "fuse_denied", "detail"),
    [
        ("false", False, "container_mounts_disabled"),
        ("true", True, "fuse_denied"),
    ],
)
async def test_a_workspace_that_can_mount_no_folder_names_each_one(
    denied, monkeypatch, allow, fuse_denied, detail
):
    """Under the rclone_mount driver a container that can mount nothing
    (CLOUD_RCLONE_ALLOW_CONTAINER off, or a custom image denied /dev/fuse)
    gets only its session folder's sync: the project's folder and the
    owner's root are named mount_unsupported, never silently missing; the
    outbox is that session folder."""
    monkeypatch.setenv("CLOUD_RCLONE_ALLOW_CONTAINER", allow)
    monkeypatch.setattr(
        mounts, "container_denies_fuse", AsyncMock(return_value=fuse_denied)
    )
    store = Store([_folder_row(), _outbox_row(), _root_row()])
    deps = _deps(store)
    thread = _thread([FOLDER, OUTBOX, ROOT])
    payload = await mounts._build_agent_cloud_mount(
        thread,
        mount_rows=[],
        metadata={**thread["metadata"], **_ready()},
        dependencies=deps,
    )
    assert payload["mounts"] == []
    assert [
        (e["source_ref"], e["reason"], e["detail"], e["title"])
        for e in payload["excluded"]
    ] == [
        (FOLDER, "mount_unsupported", detail, "Acme Launch cloud folder"),
        (ROOT, "mount_unsupported", detail, "My cloud"),
    ]
    assert "mount_unsupported" in rule.EXCLUSION_REASONS
    # End reconstructs nothing from it, and a protected session keeps its
    # own contract: no payload.
    monkeypatch.setattr(
        mounts,
        "_runtime_supports_terminal_rclone_retirement",
        lambda *args, **kwargs: False,
    )
    protected = _thread([FOLDER], metadata={"protected_cloud": True})
    for target, token in ((thread, 7), (protected, None)):
        assert (
            await mounts._build_agent_cloud_mount(
                target,
                mount_rows=[],
                metadata={**target["metadata"], **_ready()},
                terminal_retirement_token=token,
                dependencies=deps,
            )
            is None
        )


@pytest.mark.asyncio
async def test_a_workspace_still_coming_up_names_nothing_yet(denied, monkeypatch):
    """Only a runtime that can never mount names its folders: a container
    not ready yet, or a VM session's VM still booting (its own folders, even
    with container mounts off), says nothing until it is up; nor does the
    sync driver, which names its own in the delivery."""
    store = Store([_folder_row()])
    thread = _thread([FOLDER])
    booting_vm = {
        "vm": {"status": "creating"},
        "config_override": {"workspace": {"backend": "vm"}},
    }
    monkeypatch.setenv("CLOUD_RCLONE_ALLOW_CONTAINER", "true")
    creating = {"workspace_container": {"status": "creating"}}
    cases = [(creating, _deps(store))]
    cases.append(
        (
            _ready(),
            dataclasses.replace(_deps(store), cloud_workspace_driver=lambda: "sync"),
        )
    )
    for extra, deps in cases:
        assert (
            await mounts._build_agent_cloud_mount(
                thread,
                mount_rows=[],
                metadata={**thread["metadata"], **extra},
                dependencies=deps,
            )
            is None
        )
    monkeypatch.setenv("CLOUD_RCLONE_ALLOW_CONTAINER", "false")
    assert (
        await mounts._build_agent_cloud_mount(
            thread,
            mount_rows=[],
            metadata={**thread["metadata"], **booting_vm},
            dependencies=_deps(store),
        )
        is None
    )


# --------------------------------------------------------------------------- #
# End's reconstruction (R1)
# --------------------------------------------------------------------------- #


def _terminal_thread(metadata):
    runtime = "33333333-3333-4333-8333-333333333333"
    fingerprint = "SHA256:" + ("A" * 43)
    metadata = {
        **metadata,
        "workspace_container": {
            "status": "retiring_process_zero",
            "provisioner": "k8s",
            "pod_ip": "10.42.0.10",
            "port": 30022,
            "_canvas_workspace_generation": GENERATION,
            "_runtime_incarnation": runtime,
        },
        "_workspace_binding": {
            "generation": GENERATION,
            "kind": "remote",
            "backing_id": "k8s-pvc:agent-workspaces:pvc-uid",
            "ssh_host_key_fingerprint": fingerprint,
        },
        "_stateless_workspace_retirement_pending": True,
        "_stateless_claim_retirement": {
            "terminal_token": 8,
            "claimant_quiesced": True,
            "shell_retirement_required": True,
            "resident_cleanup_required": True,
            "residents_retired": False,
            "remote_retired": False,
            "permanent": True,
            "workspace_absence_proven": False,
            "workspace_generation": GENERATION,
            "endpoint_generation": GENERATION,
            "runtime_incarnation": runtime,
            "host_key_fingerprint": fingerprint,
        },
    }
    thread = _thread([], status="ended", execution_lane="stateless")
    thread["metadata"] = metadata
    return thread, metadata


@pytest.mark.asyncio
async def test_end_rebuilds_the_old_rule_only_for_a_session_it_mounted(
    denied, monkeypatch
):
    monkeypatch.delenv("CLOUD_RCLONE_ALLOW_CONTAINER", raising=False)
    store = Store([_folder_row(), _outbox_row()])
    legacy_rows = [
        {
            "id": "row-1",
            "mount_kind": "project",
            "source_ref": PROJECT,
            "target_path": "projects/acme_launch",
            "backend_id": "nextcloud",
            "backend_instance_id": INSTANCE,
            "cloud_handle": "project-handle",
        }
    ]
    # No stamp: its residents were mounted by the old rule, from its rows.
    thread, metadata = _terminal_thread({"datasource_ids": [FOLDER, OUTBOX]})
    payload = await mounts._build_agent_cloud_mount(
        thread,
        mount_rows=legacy_rows,
        metadata=metadata,
        terminal_retirement_token=8,
        dependencies=_deps(store),
    )
    assert [(m["mount_id"], m["workspace_name"]) for m in payload["mounts"]] == [
        ("row-1", "acme_launch")
    ]
    assert "excluded" not in payload
    assert store.metadata == [] and store.selections == []

    # Stamped: delivered under the connector rule, so End rebuilds that.
    thread, metadata = _terminal_thread(
        {
            "cloud_mount_rule": "connectors",
            "cloud_outbox": {"connector_id": OUTBOX, "state": "ready"},
            "datasource_ids": [FOLDER, OUTBOX],
        }
    )
    payload = await mounts._build_agent_cloud_mount(
        thread,
        mount_rows=legacy_rows,
        metadata=metadata,
        terminal_retirement_token=8,
        dependencies=_deps(store),
    )
    assert {m["mount_id"] for m in payload["mounts"]} == {FOLDER, OUTBOX}


@pytest.mark.asyncio
async def test_end_retires_what_the_workspace_was_given_not_what_is_attached_now(
    denied, monkeypatch
):
    """Each in-workspace delivery keeps its mounts on the thread, without a
    secret; End rebuilds from that record, so a folder detached, revoked or
    renamed since is still retired exactly as it was mounted."""
    import json

    from shared.runtime.services.cloud_mount import RcloneMountManager

    monkeypatch.delenv("CLOUD_RCLONE_ALLOW_CONTAINER", raising=False)
    store = Store([_folder_row(), _outbox_row()])
    thread = _thread([FOLDER, OUTBOX])
    delivered = await mounts._build_agent_cloud_mount(
        thread,
        mount_rows=[],
        metadata={**thread["metadata"], **_ready()},
        dependencies=_deps(store),
    )
    ((_tid, patch),) = [
        entry for entry in store.metadata if mounts.DELIVERED_KEY in entry[1]
    ]
    record = patch[mounts.DELIVERED_KEY]
    assert "agent-pass" not in json.dumps(record)
    assert [m["mount_id"] for m in record["mounts"]] == [
        m["mount_id"] for m in delivered["mounts"]
    ]
    # The same delivery again writes nothing.
    ((_tid, named),) = [
        entry for entry in store.metadata if mounts.NAMES_KEY in entry[1]
    ]
    store.metadata.clear()
    await mounts._build_agent_cloud_mount(
        thread,
        mount_rows=[],
        metadata={
            **thread["metadata"],
            **_ready(),
            mounts.DELIVERED_KEY: record,
            **named,
        },
        dependencies=_deps(store),
    )
    assert store.metadata == []

    # The project folder is detached since; End still rebuilds it.
    ended, metadata = _terminal_thread(
        {
            "cloud_mount_rule": "connectors",
            "datasource_ids": [OUTBOX],
            mounts.DELIVERED_KEY: record,
        }
    )
    rebuilt = await mounts._build_agent_cloud_mount(
        ended,
        mount_rows=[],
        metadata=metadata,
        terminal_retirement_token=8,
        dependencies=_deps(store),
    )
    assert [m["mount_id"] for m in rebuilt["mounts"]] == [
        m["mount_id"] for m in delivered["mounts"]
    ]
    manager = RcloneMountManager(
        thread_id=THREAD,
        cloud_cfg=rebuilt,
        workspace_backend=SimpleNamespace(
            resolve_home_path=lambda rel: f"/home/agent-host/{rel}"
        ),
        workspace_root="/home/agent-host/workspace",
    )
    for index, (given, again) in enumerate(zip(delivered["mounts"], rebuilt["mounts"])):
        assert (
            manager._state_for_mount(given, index).resident_spec_digest
            == manager._state_for_mount(again, index).resident_spec_digest
        )
    # Nothing was given last: nothing to rebuild (End's own pass retires
    # whatever an earlier claim left).
    ended, metadata = _terminal_thread(
        {
            "cloud_mount_rule": "connectors",
            "datasource_ids": [FOLDER, OUTBOX],
            mounts.DELIVERED_KEY: {"version": 1, "mounts": []},
        }
    )
    assert (
        await mounts._build_agent_cloud_mount(
            ended,
            mount_rows=[],
            metadata=metadata,
            terminal_retirement_token=8,
            dependencies=_deps(store),
        )
        is None
    )


# --------------------------------------------------------------------------- #
# The one-time conversion
# --------------------------------------------------------------------------- #


def _old_thread(**over):
    thread = {
        "id": THREAD,
        "user_id": USER,
        "project_id": PROJECT,
        "main_cloud_backend": "nextcloud",
        "main_cloud_backend_instance_id": INSTANCE,
        "main_cloud_session_handle": "sessions/00000000",
        "main_cloud_share_handle": "17",
        "metadata": {"datasource_ids": ["kb-1"]},
    }
    thread.update(over)
    return thread


_PROJECT_ROW = [{"mount_kind": "project", "source_ref": PROJECT}]


@pytest.fixture
def authorize(monkeypatch):
    calls: list[list[str]] = []
    refuse = {"value": False}

    async def fake(store, owner, owner_id, ids, project_ids, backend, **kwargs):
        from orchestrator.services.datasource_policy import DatasourceUnavailableError

        calls.append(list(ids))
        if refuse["value"]:
            raise DatasourceUnavailableError()
        return list(ids), {cid: 1 for cid in ids}

    monkeypatch.setattr(
        "orchestrator.services.datasource_policy.authorize_datasource_selection", fake
    )
    return SimpleNamespace(calls=calls, refuse=refuse)


@pytest.mark.asyncio
async def test_an_old_session_converts_once_to_what_it_mounted(denied, authorize):
    thread = _old_thread()
    store = Store([_folder_row(), _outbox_row()], threads={THREAD: dict(thread)})
    converted = await prep.convert_to_connector_rule(
        thread, mount_rows=_PROJECT_ROW, store=store
    )
    assert store.selections[0][1] == ["kb-1", FOLDER, OUTBOX]
    # Only the additions are authorized, and only theirs are snapshot.
    assert authorize.calls == [[FOLDER, OUTBOX]]
    assert store.selections[0][2]["datasource_policy_revisions"] == {
        FOLDER: 1,
        OUTBOX: 1,
    }
    metadata = converted["metadata"]
    assert metadata["cloud_mount_rule"] == "connectors"
    assert metadata["cloud_outbox"] == {"connector_id": OUTBOX, "state": "ready"}
    # Never again.
    assert (
        await prep.convert_to_connector_rule(
            converted, mount_rows=_PROJECT_ROW, store=store
        )
        is None
    )
    assert len(store.selections) == 1


@pytest.mark.asyncio
async def test_a_revoked_connector_or_a_live_edit_never_holds_the_conversion_back(
    denied, authorize
):
    """Only the additions are authorized: a connector of the selection
    revoked since stays as it is (attach reports it) and the conversion
    goes on; and the additions join the selection as it is when they land,
    so a live edit made meanwhile is kept, never undone."""
    denied.add("revoked-1")
    thread = _old_thread(metadata={"datasource_ids": ["revoked-1"]})
    # A live edit landed after the snapshot above: revoked-1 out, kb-2 in.
    stored = _old_thread(metadata={"datasource_ids": ["kb-2"]})
    store = Store([_folder_row(), _outbox_row()], threads={THREAD: stored})
    router = SimpleNamespace(active_instance_id=INSTANCE)
    converted = await prep.convert_to_connector_rule(
        thread, mount_rows=_PROJECT_ROW, store=store, cloud_router=router
    )
    assert authorize.calls == [[FOLDER, OUTBOX]]
    assert converted["metadata"]["datasource_ids"] == ["kb-2", FOLDER, OUTBOX]
    assert converted["metadata"]["cloud_mount_rule"] == "connectors"


@pytest.mark.asyncio
async def test_the_stamp_waits_for_a_connector_that_is_not_there_yet(denied, authorize):
    """A project folder in the active installation whose connector the
    backfill has not made yet, or an owner whose outbox connector is not
    there: what is there is added, the stamp waits, and the next pass
    finishes. No active main cloud: nothing is decided now."""
    router = SimpleNamespace(active_instance_id=INSTANCE)
    thread = _old_thread()
    store = Store([_outbox_row()], threads={THREAD: dict(thread)})
    converted = await prep.convert_to_connector_rule(
        thread, mount_rows=_PROJECT_ROW, store=store, cloud_router=router
    )
    assert converted["metadata"]["datasource_ids"] == ["kb-1", OUTBOX]
    assert "cloud_mount_rule" not in converted["metadata"]
    # The project's connector appears: the next pass adds it and stamps.
    store.rows[FOLDER] = _folder_row()
    again = await prep.convert_to_connector_rule(
        converted, mount_rows=_PROJECT_ROW, store=store, cloud_router=router
    )
    assert again["metadata"]["datasource_ids"] == ["kb-1", OUTBOX, FOLDER]
    assert again["metadata"]["cloud_mount_rule"] == "connectors"

    # No outbox connector for its owner yet, beside its session folder.
    store = Store([_folder_row()], threads={THREAD: dict(thread)})
    waiting = await prep.convert_to_connector_rule(
        thread, mount_rows=_PROJECT_ROW, store=store, cloud_router=router
    )
    assert "cloud_mount_rule" not in waiting["metadata"]

    # A project naming no folder, or one in another installation: nothing
    # stands for it, nothing is waited for.
    for project in (
        _project(PROJECT, "Acme Launch", handle=None),
        _project(PROJECT, "Acme Launch", instance="another-installation"),
    ):
        store = Store(
            [_outbox_row()], projects={PROJECT: project}, threads={THREAD: dict(thread)}
        )
        stamped = await prep.convert_to_connector_rule(
            thread, mount_rows=_PROJECT_ROW, store=store, cloud_router=router
        )
        assert stamped["metadata"]["cloud_mount_rule"] == "connectors"

    store = Store([_folder_row(), _outbox_row()], threads={THREAD: dict(thread)})
    assert (
        await prep.convert_to_connector_rule(
            thread,
            mount_rows=_PROJECT_ROW,
            store=store,
            cloud_router=SimpleNamespace(active_instance_id=None),
        )
        is None
    )
    assert store.selections == [] and store.metadata == []


@pytest.mark.asyncio
async def test_an_in_workspace_attach_converts_before_it_prepares_grants(
    denied, authorize, monkeypatch
):
    """The attach's pre-lock preparation converts an old session first, so
    the project folder the conversion adds gets its writer now, not one
    attach later (P3-21)."""
    from orchestrator.services.cloud import folder_grants

    minted = []

    async def ensure(store, router, target, *, user_id, kind):
        minted.append((target.datasource_id, kind))

    monkeypatch.setattr(folder_grants, "ensure_folder_grant", ensure)
    thread = _old_thread()
    store = GrantStore([_folder_row(), _outbox_row()], threads={THREAD: thread})
    store.list_thread_mounts = AsyncMock(return_value=_PROJECT_ROW)
    await mounts.prepare_thread_folder_grants(
        THREAD, dependencies=_deps(store, backend=GrantBackend())
    )
    assert store.threads[THREAD]["metadata"]["cloud_mount_rule"] == "connectors"
    assert minted == [(FOLDER, "writer")]


@pytest.mark.asyncio
async def test_conversion_adds_only_what_is_missing_and_allowed(denied, authorize):
    denied.add(OUTBOX)
    thread = _old_thread(metadata={"datasource_ids": [FOLDER]})
    store = Store([_folder_row(), _outbox_row()], threads={THREAD: dict(thread)})
    converted = await prep.convert_to_connector_rule(
        thread, mount_rows=_PROJECT_ROW, store=store
    )
    # The folder is attached already, the outbox is refused: no write but the
    # stamp.
    assert store.selections == []
    assert converted["metadata"]["cloud_mount_rule"] == "connectors"


@pytest.mark.asyncio
async def test_without_a_session_folder_no_outbox_is_added(denied, authorize):
    thread = _old_thread(main_cloud_session_handle=None)
    store = Store([_folder_row(), _outbox_row()], threads={THREAD: dict(thread)})
    converted = await prep.convert_to_connector_rule(thread, mount_rows=[], store=store)
    assert store.selections == []
    assert "cloud_outbox" not in converted["metadata"]


@pytest.mark.asyncio
async def test_a_selection_that_no_longer_authorises_converts_later(denied, authorize):
    authorize.refuse["value"] = True
    thread = _old_thread()
    store = Store([_folder_row(), _outbox_row()], threads={THREAD: dict(thread)})
    assert (
        await prep.convert_to_connector_rule(
            thread, mount_rows=_PROJECT_ROW, store=store
        )
        is None
    )
    assert store.selections == [] and store.metadata == []


@pytest.mark.parametrize(
    "metadata",
    [
        {"protected_cloud": True},
        {"config_override": {"officer": {"enabled": True}}},
        {"cloud_mount_rule": "connectors"},
    ],
)
@pytest.mark.asyncio
async def test_protected_officer_and_new_sessions_never_convert(
    denied, authorize, metadata
):
    thread = _old_thread(metadata={"datasource_ids": [], **metadata})
    store = Store([_folder_row(), _outbox_row()])
    assert (
        await prep.convert_to_connector_rule(
            thread, mount_rows=_PROJECT_ROW, store=store
        )
        is None
    )
    assert store.selections == [] and store.metadata == []


@pytest.mark.asyncio
async def test_an_old_session_converts_at_its_first_in_workspace_delivery(
    denied, authorize, monkeypatch
):
    monkeypatch.delenv("CLOUD_RCLONE_ALLOW_CONTAINER", raising=False)
    thread = _old_thread()
    store = Store([_folder_row(), _outbox_row()], threads={THREAD: dict(thread)})
    payload = await mounts._build_agent_cloud_mount(
        thread,
        mount_rows=_PROJECT_ROW,
        metadata={**thread["metadata"], **_ready()},
        dependencies=_deps(store),
    )
    assert {m["mount_id"] for m in payload["mounts"]} == {FOLDER, OUTBOX}


# --------------------------------------------------------------------------- #
# The outbox folder
# --------------------------------------------------------------------------- #


class CloudBackend(Backend):
    def __init__(self, *, share=True, full=False, fail=False, existing=()):
        super().__init__()
        self.share_ok, self.full, self.fail = share, full, fail
        self.folders: list[str] = []
        self.shares: list[str] = []
        self.existing = set(existing)

    async def ensure_session_folder(self, *, session_id, exclusive=False):
        from orchestrator.services.cloud import (
            CloudBackendError,
            CloudBackendErrorKind,
            SessionFolderHandle,
        )

        if self.fail:
            raise CloudBackendError(
                CloudBackendErrorKind.UNAVAILABLE, "down", backend="nextcloud"
            )
        if exclusive and session_id in self.existing:
            raise CloudBackendError(
                CloudBackendErrorKind.ALREADY_EXISTS, "there", backend="nextcloud"
            )
        self.existing.add(session_id)
        self.folders.append(session_id)
        return SessionFolderHandle(
            backend="nextcloud", native_id=f"sessions/{session_id}"
        )

    async def ensure_user(self, **kwargs):
        return "cloud-user"

    async def resolve_user_identity(self, email, name):
        return "cloud-user"

    async def share_session_folder(self, handle, user_id):
        from orchestrator.services.cloud import ShareHandle

        if self.full:
            raise BucketFull(120.0)
        if not self.share_ok:
            return None
        self.shares.append(handle.native_id)
        return ShareHandle(backend="nextcloud", native_id="99")


class IdentityStore(Store):
    async def get_user_cloud_identity(self, user_id):
        return {}

    async def merge_user_cloud_identity(self, *args):
        return True


@pytest.mark.asyncio
async def test_admission_creates_shares_and_records_the_outbox():
    store = IdentityStore()
    backend = CloudBackend()
    assert await prep.create_session_outbox(
        THREAD, {"id": USER}, connector_id=OUTBOX, backend=backend, store=store
    )
    # The session's full id names its folder, recorded before it is made.
    assert backend.folders == [THREAD] and backend.shares
    assert store.main_cloud[0]["share_handle"]
    assert store.main_cloud[0]["session_handle"] == f"sessions/{THREAD}"
    assert store.metadata == [
        (
            THREAD,
            {
                "cloud_outbox": {
                    "connector_id": OUTBOX,
                    "state": "creating",
                    "folder": THREAD,
                }
            },
        ),
        (THREAD, {"cloud_outbox": {"connector_id": OUTBOX, "state": "ready"}}),
    ]


@pytest.mark.asyncio
async def test_a_folder_srw_could_not_create_is_recorded_and_nothing_raises():
    store = IdentityStore()
    assert not await prep.create_session_outbox(
        THREAD,
        {"id": USER},
        connector_id=OUTBOX,
        backend=CloudBackend(fail=True),
        store=store,
    )
    assert store.main_cloud == []
    assert store.metadata[-1][1]["cloud_outbox"] == {
        "connector_id": OUTBOX,
        "state": "unavailable",
        "reason": "create_failed",
        # Its MKCOL may have landed: the next attempt takes it.
        "folder": THREAD,
    }


@pytest.mark.asyncio
async def test_a_folder_already_at_a_new_sessions_name_is_never_taken_over():
    """A new session's outbox must be new: a folder already at its name is
    not reused (and never shared), unless this session recorded it as the
    one it was creating."""
    store = IdentityStore()
    backend = CloudBackend(existing={THREAD})
    assert not await prep.create_session_outbox(
        THREAD, {"id": USER}, connector_id=OUTBOX, backend=backend, store=store
    )
    assert backend.folders == [] and backend.shares == []
    assert store.main_cloud == []
    assert store.metadata[-1][1]["cloud_outbox"] == {
        "connector_id": OUTBOX,
        "state": "unavailable",
        "reason": "folder_exists",
    }
    assert "folder_exists" in rule.OUTBOX_FAILURE_REASONS
    # An earlier attempt of this session made it, then stopped: it is taken.
    store = IdentityStore()
    assert await prep.create_session_outbox(
        THREAD,
        {"id": USER},
        connector_id=OUTBOX,
        backend=backend,
        store=store,
        recorded_folder=THREAD,
    )
    assert backend.folders == [THREAD] and backend.shares == [f"sessions/{THREAD}"]
    # The pre-plan step passes what the session recorded.
    store = IdentityStore([_outbox_row()])
    backend = CloudBackend(existing={THREAD})
    await prep.prepare_session_outbox(
        _attached(
            main_cloud_session_handle=None,
            main_cloud_share_handle=None,
            metadata={
                "cloud_outbox": {
                    "connector_id": OUTBOX,
                    "state": "creating",
                    "folder": THREAD,
                }
            },
        ),
        store=store,
        cloud_router=_router(backend),
    )
    assert backend.folders == [THREAD]


@pytest.mark.asyncio
async def test_a_full_share_limit_never_blocks_admission():
    store = IdentityStore()
    assert await prep.create_session_outbox(
        THREAD,
        {"id": USER},
        connector_id=OUTBOX,
        backend=CloudBackend(full=True),
        store=store,
    )
    assert store.main_cloud[0]["share_handle"] is None
    record = store.metadata[-1][1]["cloud_outbox"]
    assert record["state"] == "ready" and record["share_retry_at"]


class CachedIdentityStore(IdentityStore):
    """``users.cloud_identity`` with the owner's Nextcloud id cached."""

    def __init__(self, *args, cached="cloud-user"):
        super().__init__(*args)
        self.identity = {"nextcloud": {"user_id": cached}}
        self.identity_merges: list[dict] = []

    async def get_user_cloud_identity(self, user_id):
        return self.identity

    async def merge_user_cloud_identity(self, user_id, backend_id, entry):
        self.identity_merges.append(dict(entry))
        self.identity.setdefault(backend_id, {}).update(entry)
        return True


class CountingBackend(CloudBackend):
    """Counts identity lookups; refuses shares to unknown accounts."""

    def __init__(self, *, known=("cloud-user",), **kwargs):
        super().__init__(**kwargs)
        self.known = set(known)
        self.lookups = 0
        self.shared_with: list[str] = []

    async def ensure_user(self, **kwargs):
        self.lookups += 1
        return "cloud-user"

    async def resolve_user_identity(self, email, name):
        self.lookups += 1
        return "cloud-user"

    async def share_session_folder(self, handle, user_id):
        from orchestrator.services.cloud import (
            CloudBackendError,
            CloudBackendErrorKind,
        )

        self.shared_with.append(str(user_id))
        if str(user_id) not in self.known:
            raise CloudBackendError(
                CloudBackendErrorKind.NOT_FOUND, "no such user", backend="nextcloud"
            )
        return await super().share_session_folder(handle, user_id)


@pytest.mark.asyncio
async def test_admission_shares_with_the_cached_account_without_a_lookup():
    store = CachedIdentityStore()
    backend = CountingBackend()
    assert await prep.create_session_outbox(
        THREAD, {"id": USER}, connector_id=OUTBOX, backend=backend, store=store
    )
    assert backend.lookups == 0
    assert backend.shared_with == ["cloud-user"]
    assert store.main_cloud[0]["share_handle"]


@pytest.mark.asyncio
async def test_admission_on_a_cache_miss_ensures_the_account_and_caches_it():
    store = CachedIdentityStore()
    store.identity = {}
    backend = CountingBackend()
    assert await prep.create_session_outbox(
        THREAD, {"id": USER}, connector_id=OUTBOX, backend=backend, store=store
    )
    assert backend.lookups == 1
    assert store.identity["nextcloud"]["user_id"] == "cloud-user"
    assert store.main_cloud[0]["share_handle"]


@pytest.mark.asyncio
async def test_admission_heals_a_stale_cached_account():
    store = CachedIdentityStore(cached="cloud-user-before-reinstall")
    backend = CountingBackend()
    assert await prep.create_session_outbox(
        THREAD, {"id": USER}, connector_id=OUTBOX, backend=backend, store=store
    )
    assert backend.shared_with == ["cloud-user-before-reinstall", "cloud-user"]
    assert store.identity["nextcloud"]["user_id"] == "cloud-user"
    assert store.main_cloud[0]["share_handle"]
    assert store.metadata[-1][1]["cloud_outbox"] == {
        "connector_id": OUTBOX,
        "state": "ready",
    }


@pytest.mark.asyncio
async def test_a_stale_account_the_lookup_cannot_fix_leaves_the_share_for_later():
    store = CachedIdentityStore()
    backend = CountingBackend(known=())
    assert await prep.create_session_outbox(
        THREAD, {"id": USER}, connector_id=OUTBOX, backend=backend, store=store
    )
    assert backend.shared_with == ["cloud-user"]
    assert store.identity_merges == []
    assert store.main_cloud[0]["share_handle"] is None
    assert "share_retry_at" in store.metadata[-1][1]["cloud_outbox"]


def _attached(**over):
    return _thread([OUTBOX], **over)


@pytest.mark.asyncio
async def test_the_share_retry_heals_a_stale_cached_account():
    store = CachedIdentityStore([_outbox_row()], cached="cloud-user-old")
    backend = CountingBackend()
    await prep.prepare_session_outbox(
        _attached(main_cloud_share_handle=None),
        store=store,
        cloud_router=_router(backend),
    )
    assert backend.shared_with == ["cloud-user-old", "cloud-user"]
    assert store.main_cloud[0]["share_handle"]


@pytest.mark.asyncio
async def test_the_pre_plan_step_retries_a_share_that_never_landed():
    store = IdentityStore([_outbox_row()])
    backend = CloudBackend()
    await prep.prepare_session_outbox(
        _attached(main_cloud_share_handle=None),
        store=store,
        cloud_router=_router(backend),
    )
    assert backend.shares == ["sessions/00000000"]
    assert store.main_cloud[0]["share_handle"]
    assert "share_retry_at" not in store.metadata[-1][1]["cloud_outbox"]


@pytest.mark.asyncio
async def test_the_share_retry_is_throttled_and_a_full_limit_waits_for_later():
    store = IdentityStore([_outbox_row()])
    backend = CloudBackend(full=True)
    now = datetime(2026, 10, 10, 22, 0, tzinfo=timezone.utc)
    await prep.prepare_session_outbox(
        _attached(main_cloud_share_handle=None),
        store=store,
        cloud_router=_router(backend),
        now=now,
    )
    retry_at = datetime.fromisoformat(
        store.metadata[-1][1]["cloud_outbox"]["share_retry_at"]
    )
    assert retry_at == now + timedelta(seconds=prep.SHARE_RETRY_SECONDS)
    # Before then nothing is tried.
    backend.full = False
    thread = _attached(
        main_cloud_share_handle=None,
        metadata={"cloud_outbox": store.metadata[-1][1]["cloud_outbox"]},
    )
    await prep.prepare_session_outbox(
        thread, store=store, cloud_router=_router(backend), now=now
    )
    assert backend.shares == []
    await prep.prepare_session_outbox(
        thread, store=store, cloud_router=_router(backend), now=retry_at
    )
    assert backend.shares == ["sessions/00000000"]


@pytest.mark.asyncio
async def test_the_pre_plan_step_creates_a_missing_folder_and_leaves_others_alone():
    store = IdentityStore([_outbox_row()])
    backend = CloudBackend()
    await prep.prepare_session_outbox(
        _attached(main_cloud_session_handle=None, main_cloud_share_handle=None),
        store=store,
        cloud_router=_router(backend),
    )
    assert backend.folders == [THREAD]
    # Shared already, no outbox attached, or protected: nothing to do.
    for thread in (
        _attached(),
        _thread([]),
        _attached(main_cloud_share_handle=None, metadata={"protected_cloud": True}),
    ):
        backend = CloudBackend()
        await prep.prepare_session_outbox(
            thread, store=store, cloud_router=_router(backend)
        )
        assert backend.folders == [] and backend.shares == []


# --------------------------------------------------------------------------- #
# Admission: a session folder only for an attached outbox
# --------------------------------------------------------------------------- #


def _admission(store, backend, *, active=INSTANCE):
    return SimpleNamespace(
        store=store,
        main_cloud_router=SimpleNamespace(
            active_instance_id=active, for_owner=lambda user: backend
        ),
    )


@pytest.mark.parametrize(
    ("ids", "tier", "created"),
    [
        ([OUTBOX], "sandbox", True),
        ([OUTBOX, FOLDER], None, True),
        ([], "sandbox", False),
        ([FOLDER], "sandbox", False),
        # The outbox is never mounted on a root VM: no folder for it.
        ([OUTBOX], "vm", False),
    ],
)
@pytest.mark.asyncio
async def test_admission_creates_a_session_folder_only_for_the_outbox(
    ids, tier, created
):
    from orchestrator.services import thread_admission as ta

    store = IdentityStore([_outbox_row(), _folder_row()])
    backend = CloudBackend()
    await ta.setup_thread_main_cloud(
        THREAD,
        {"id": USER},
        datasource_ids=ids,
        workspace_backend=tier,
        dependencies=_admission(store, backend),
    )
    assert bool(backend.folders) is created
    assert bool(store.main_cloud) is created
    if created:
        assert store.metadata[-1][1]["cloud_outbox"]["connector_id"] == OUTBOX
        # A protected session mounts its protected folder alone: no folder.
        protected = CloudBackend()
        await ta.setup_thread_main_cloud(
            THREAD,
            {"id": USER},
            datasource_ids=ids,
            workspace_backend=tier,
            protected=True,
            dependencies=_admission(IdentityStore([_outbox_row()]), protected),
        )
        assert protected.folders == []


@pytest.mark.asyncio
async def test_admission_records_an_outbox_the_main_cloud_could_not_serve():
    from orchestrator.services import thread_admission as ta

    store = IdentityStore([_outbox_row()])
    backend = CloudBackend()
    backend.is_initialized = False
    backend.is_configured = False
    await ta.setup_thread_main_cloud(
        THREAD,
        {"id": USER},
        datasource_ids=[OUTBOX],
        dependencies=_admission(store, backend),
    )
    assert backend.folders == []
    assert store.metadata == [
        (
            THREAD,
            {
                "cloud_outbox": {
                    "connector_id": OUTBOX,
                    "state": "unavailable",
                    "reason": "backend_unavailable",
                }
            },
        )
    ]
    # Without a main cloud at all nothing is recorded.
    store = IdentityStore([_outbox_row()])
    await ta.setup_thread_main_cloud(
        THREAD,
        {"id": USER},
        datasource_ids=[OUTBOX],
        dependencies=_admission(store, CloudBackend(), active=None),
    )
    assert store.metadata == []
