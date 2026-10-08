"""Project scope off ``thread_mounts`` (main_cloud_as_connectors.md, slice 1).

A Session belongs to one project at most, and that project is
``threads.project_id``. Connector authorization reads the scope through
``thread_project_ids``, so a Session's connector eligibility must be the same
with its ``thread_mounts`` rows present, emptied, or rebuilt for something
else. Only a legacy multi-project Session (NULL column) still reads its list
from the legacy places.
"""

from __future__ import annotations

from functools import partial
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from orchestrator.services import runtime_actor
from orchestrator.services import thread_datasource_authorization as tda
from orchestrator.services import thread_mount_rows
from orchestrator.services.datasource_policy import classify_datasource_selection
from orchestrator.services.thread_mount_rows import (
    ThreadMountDependencies,
    durable_project_ids,
)

OWNER = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
THREAD = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
P = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
Q = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
DEFAULT = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"
LINKED_TO_P = "10000000-0000-4000-8000-000000000001"
NATIVE_KB_P = "10000000-0000-4000-8000-000000000002"
LINKED_TO_Q = "10000000-0000-4000-8000-000000000003"
PERSONAL = "10000000-0000-4000-8000-000000000004"
PUBLIC = "10000000-0000-4000-8000-000000000005"
ALL_CONNECTORS = [LINKED_TO_P, NATIVE_KB_P, LINKED_TO_Q, PERSONAL, PUBLIC]


def _mount(project_id: str, *, kind: str = "project") -> dict[str, Any]:
    return {
        "mount_kind": kind,
        "source_ref": project_id,
        "target_path": "" if kind == "project_default" else f"projects/{project_id}",
    }


def _policy_rows() -> dict[str, dict[str, Any]]:
    """One connector of each eligibility shape a project Session meets."""
    base = {"policy_revision": 1, "type": "webdav", "created_by": None}
    return {
        LINKED_TO_P: {
            **base,
            "id": LINKED_TO_P,
            "scope_mode": "projects",
            "project_ids": [P],
        },
        NATIVE_KB_P: {
            **base,
            "id": NATIVE_KB_P,
            "type": "kb",
            "scope_mode": "projects",
            "project_ids": [P],
            "managed_key": f"project-kb:{P}",
        },
        LINKED_TO_Q: {
            **base,
            "id": LINKED_TO_Q,
            "scope_mode": "projects",
            "project_ids": [Q],
        },
        PERSONAL: {**base, "id": PERSONAL, "scope_mode": "all", "created_by": OWNER},
        PUBLIC: {**base, "id": PUBLIC, "scope_mode": "all", "is_global": True},
    }


class _Store:
    """The few store reads thread scope and connector policy make."""

    def __init__(self, thread: dict[str, Any], mounts: list[dict[str, Any]]):
        self.thread = thread
        self.mounts = list(mounts)
        self.rows = _policy_rows()
        self.replace_thread_mounts = AsyncMock()

    async def get_thread(self, thread_id: str):
        return self.thread if thread_id == self.thread["id"] else None

    async def list_thread_mounts(self, thread_id: str):
        return list(self.mounts)

    async def get_user(self, user_id: str):
        return {"id": user_id, "is_admin": False, "is_approved": True}

    async def user_is_member_of_projects(self, user_id: str, project_ids):
        return set(project_ids) <= {P, Q, DEFAULT}

    async def get_datasource_policy_rows(self, ids, **_kwargs):
        return [self.rows[i] for i in ids if i in self.rows]

    async def resolve_datasources_for_thread(self, *, datasource_ids, project_ids):
        return [dict(self.rows[i]) for i in datasource_ids or [] if i in self.rows]


def _mount_dependencies(store: _Store) -> ThreadMountDependencies:
    return ThreadMountDependencies(
        store=store,
        cloud_router=MagicMock(),
        resolve_user_identity_cached=AsyncMock(),
        externalize_gitea_url=lambda url: url,
        resolve_authorized_thread_datasources=AsyncMock(),
        build_datasources_payload=lambda rows: rows,
        build_workspace_ssh_identities=lambda rows: [],
        cloud_workspace_driver=lambda: "sync",
    )


def _thread(project_id: str | None, **metadata: Any) -> dict[str, Any]:
    return {
        "id": THREAD,
        "user_id": OWNER,
        "project_id": project_id,
        "metadata": metadata,
    }


async def _eligibility(store: _Store) -> list[tuple[str, bool, str | None]]:
    """Each connector's verdict for the Session, through the real policy."""
    project_ids = await thread_mount_rows.thread_project_ids(
        THREAD, dependencies=_mount_dependencies(store)
    )
    verdicts, _revisions = await classify_datasource_selection(
        store, {"id": OWNER}, OWNER, ALL_CONNECTORS, project_ids, None
    )
    return [(v.datasource_id, v.denied, v.reason) for v in verdicts]


class TestDurableProjectIds:
    def test_the_column_is_the_whole_answer(self):
        thread = _thread(P, project_ids=[Q])
        assert durable_project_ids(thread, legacy_mounts=[_mount(P)]) == [P]
        assert durable_project_ids(thread, legacy_mounts=[]) == [P]

    def test_a_set_column_with_rows_for_other_projects_is_legacy(self):
        """``project_id`` plus ``project_ids`` before 2026-10-06: the rows and
        the column, never narrowed to the column."""
        mounts = [_mount(Q), _mount(P)]
        assert durable_project_ids(_thread(P), legacy_mounts=mounts) == [Q, P]
        assert durable_project_ids(_thread(P), legacy_mounts=[_mount(Q)]) == [Q, P]

    def test_no_project_is_an_empty_scope(self):
        assert durable_project_ids(_thread(None), legacy_mounts=[]) == []
        assert durable_project_ids(None) == []

    def test_a_legacy_multi_project_session_reads_its_mount_rows_first(self):
        """The historical order: the rows, as the old reader returned them."""
        mounts = [
            _mount(DEFAULT, kind="project_default"),
            _mount(Q),
            {"mount_kind": "repo", "source_ref": P},
        ]
        thread = _thread(None, project_ids=[DEFAULT, Q, P])
        assert durable_project_ids(thread, legacy_mounts=mounts) == [DEFAULT, Q]

    def test_a_legacy_session_without_rows_reads_its_metadata_list(self):
        thread = _thread(None, project_ids=[DEFAULT, Q, DEFAULT])
        assert durable_project_ids(thread, legacy_mounts=[]) == [DEFAULT, Q]

    def test_metadata_stored_as_text_is_decoded(self):
        thread = {**_thread(None), "metadata": '{"project_ids": ["%s"]}' % Q}
        assert durable_project_ids(thread) == [Q]


class TestThreadProjectIds:
    @pytest.mark.asyncio
    async def test_a_project_session_answers_the_same_without_its_mount_rows(
        self, monkeypatch
    ):
        monkeypatch.setattr(
            thread_mount_rows, "build_thread_mount_rows", AsyncMock(return_value=[])
        )
        with_rows = _Store(_thread(P), [_mount(P)])
        emptied = _Store(_thread(P), [])
        scope = partial(thread_mount_rows.thread_project_ids, THREAD)
        assert await scope(dependencies=_mount_dependencies(with_rows)) == [P]
        assert await scope(dependencies=_mount_dependencies(emptied)) == [P]

    @pytest.mark.asyncio
    async def test_rows_for_another_project_keep_a_legacy_session_wide(self):
        """The pre-2026-10-06 shape (column P, rows for P and Q) answers as it
        did: narrowing it to P would drop Q's connectors from a live session."""
        store = _Store(_thread(P), [_mount(P), _mount(Q)])
        assert await thread_mount_rows.thread_project_ids(
            THREAD, dependencies=_mount_dependencies(store)
        ) == [P, Q]

    @pytest.mark.asyncio
    async def test_the_delivery_backfill_runs_but_never_answers(self, monkeypatch):
        """Rebuilt rows feed workspace delivery only; the scope is the column."""
        build = AsyncMock(return_value=[_mount(Q)])
        monkeypatch.setattr(thread_mount_rows, "build_thread_mount_rows", build)
        store = _Store(_thread(P), [])
        assert await thread_mount_rows.thread_project_ids(
            THREAD, dependencies=_mount_dependencies(store)
        ) == [P]
        assert build.await_args.args[0] == [P]
        store.replace_thread_mounts.assert_awaited_once_with(THREAD, [_mount(Q)])

    @pytest.mark.asyncio
    async def test_a_failed_backfill_leaves_the_scope_alone(self, monkeypatch):
        monkeypatch.setattr(
            thread_mount_rows,
            "build_thread_mount_rows",
            AsyncMock(side_effect=RuntimeError("cloud is down")),
        )
        store = _Store(_thread(P), [])
        assert await thread_mount_rows.thread_project_ids(
            THREAD, dependencies=_mount_dependencies(store)
        ) == [P]
        store.replace_thread_mounts.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_existing_rows_are_not_rebuilt(self, monkeypatch):
        build = AsyncMock()
        monkeypatch.setattr(thread_mount_rows, "build_thread_mount_rows", build)
        store = _Store(_thread(P), [_mount(P)])
        await thread_mount_rows.thread_project_ids(
            THREAD, dependencies=_mount_dependencies(store)
        )
        build.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_missing_thread_has_no_scope(self):
        store = _Store(_thread(P), [_mount(P)])
        assert (
            await thread_mount_rows.thread_project_ids(
                "ffffffff-ffff-4fff-8fff-ffffffffffff",
                dependencies=_mount_dependencies(store),
            )
            == []
        )


class TestConnectorEligibilityWithoutMountRows:
    """The slice 1 gate: eligibility is unchanged with ``thread_mounts`` emptied."""

    @pytest.fixture(autouse=True)
    def _no_cloud(self, monkeypatch):
        monkeypatch.setattr(
            thread_mount_rows, "build_thread_mount_rows", AsyncMock(return_value=[])
        )

    @pytest.mark.asyncio
    async def test_a_project_session(self):
        expected = [
            (LINKED_TO_P, False, None),
            (NATIVE_KB_P, False, None),
            (LINKED_TO_Q, True, "out_of_scope"),
            (PERSONAL, False, None),
            (PUBLIC, False, None),
        ]
        assert await _eligibility(_Store(_thread(P), [_mount(P)])) == expected
        assert await _eligibility(_Store(_thread(P), [])) == expected

    @pytest.mark.asyncio
    async def test_a_session_without_a_project(self):
        expected = [
            (LINKED_TO_P, True, "out_of_scope"),
            (NATIVE_KB_P, True, "out_of_scope"),
            (LINKED_TO_Q, True, "out_of_scope"),
            (PERSONAL, False, None),
            (PUBLIC, False, None),
        ]
        assert await _eligibility(_Store(_thread(None), [])) == expected

    @pytest.mark.asyncio
    async def test_resolution_delivers_the_same_connectors(self):
        selection = [LINKED_TO_P, NATIVE_KB_P, PERSONAL, PUBLIC]
        delivered = []
        for mounts in ([_mount(P)], []):
            store = _Store(_thread(P), mounts)
            dependencies = tda.ThreadDatasourceAuthorizationDependencies(
                store=store,
                thread_project_ids=partial(
                    thread_mount_rows.thread_project_ids,
                    dependencies=_mount_dependencies(store),
                ),
            )
            rows = await tda.resolve_authorized_thread_datasources(
                store.thread, selection, dependencies=dependencies
            )
            delivered.append([row["id"] for row in rows])
        assert delivered[0] == delivered[1] == selection

    @pytest.mark.asyncio
    async def test_a_legacy_multi_project_session_keeps_its_legacy_scope(self):
        """Its list lives only in the legacy rows; slice 8 decides them."""
        store = _Store(
            _thread(None), [_mount(DEFAULT, kind="project_default"), _mount(P)]
        )
        verdicts = dict(
            (datasource_id, denied)
            for datasource_id, denied, _reason in await _eligibility(store)
        )
        # A project-scoped connector must be linked to every project.
        assert verdicts[LINKED_TO_P] is True
        assert verdicts[NATIVE_KB_P] is False


class TestRuntimeActorScope:
    @pytest.mark.asyncio
    async def test_the_runtime_actor_reads_the_column(self):
        for mounts in ([_mount(P)], []):
            db = SimpleNamespace(list_thread_mounts=AsyncMock(return_value=mounts))
            assert await runtime_actor._thread_project_ids(db, _thread(P)) == [P]

    @pytest.mark.asyncio
    async def test_the_runtime_actor_keeps_a_legacy_set_column_wide(self):
        db = SimpleNamespace(
            list_thread_mounts=AsyncMock(return_value=[_mount(P), _mount(Q)])
        )
        assert await runtime_actor._thread_project_ids(db, _thread(P)) == [P, Q]

    @pytest.mark.asyncio
    async def test_a_legacy_session_still_orders_by_its_rows(self):
        mounts = [_mount(DEFAULT, kind="project_default"), _mount(Q)]
        db = SimpleNamespace(list_thread_mounts=AsyncMock(return_value=mounts))
        assert await runtime_actor._thread_project_ids(db, _thread(None)) == [
            DEFAULT,
            Q,
        ]
