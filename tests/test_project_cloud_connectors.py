"""A project's own cloud folder connector: when it is created, repointed or
skipped, and what the startup pass counts (main cloud slice 3, 3.3).

Pure tests over a fake store. What PostgreSQL does with the row (the unique
key, the write-through, eligibility, the delete cascade) is in
``tests/test_native_cloud_connectors_real_postgres.py``.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from orchestrator.services import project_cloud_connectors as connectors

PROJECT = str(uuid4())
OWNER = str(uuid4())
EARLIER_OWNER = str(uuid4())
INSTANCE = str(uuid4())
OTHER_INSTANCE = str(uuid4())


def _project(**over: Any) -> dict[str, Any]:
    row = {
        "id": PROJECT,
        "name": "Atlas",
        "is_default": False,
        "main_cloud_backend": "nextcloud",
        "main_cloud_backend_instance_id": INSTANCE,
        "main_cloud_folder_handle": "nextcloud:41",
        "nextcloud_folder_id": 41,
    }
    row.update(over)
    return row


class _Store:
    """Rows by managed key; ``create_datasource`` honours the key's
    uniqueness the way the real ``ON CONFLICT`` does."""

    def __init__(self, *, fail: Exception | None = None) -> None:
        self.rows: dict[str, dict[str, Any]] = {}
        self.created: list[dict[str, Any]] = []
        self.updated: list[tuple[str, dict[str, Any]]] = []
        self.projects: list[dict[str, Any]] = []
        self.members: list[dict[str, Any]] = [
            {"user_id": OWNER, "role": "owner", "joined_at": "2026-10-02"},
            {"user_id": str(uuid4()), "role": "editor", "joined_at": "2026-10-01"},
            {"user_id": EARLIER_OWNER, "role": "owner", "joined_at": "2026-10-01"},
        ]
        self.fail = fail

    async def get_datasource_by_managed_key(self, key: str) -> dict[str, Any] | None:
        if self.fail:
            raise self.fail
        return self.rows.get(key)

    async def get_project_members(self, _project_id: str) -> list[dict[str, Any]]:
        return self.members

    async def create_datasource(self, **row: Any) -> dict[str, Any] | None:
        self.created.append(row)
        if row["managed_key"] in self.rows:
            return None
        self.rows[row["managed_key"]] = {"id": str(uuid4()), **row}
        return self.rows[row["managed_key"]]

    async def update_datasource(self, datasource_id: str, **fields: Any) -> bool:
        self.updated.append((datasource_id, fields))
        for row in self.rows.values():
            if row["id"] == datasource_id:
                row.update(fields)
        return True

    async def list_project_cloud_folders(self) -> list[dict[str, Any]]:
        return self.projects


async def _ensure(store, project, *, active=INSTANCE, log=None):
    return await connectors.ensure_project_cloud_connector(
        store, project, active_instance_id=active, log=log or MagicMock()
    )


class TestSkipReason:
    @pytest.mark.parametrize(
        ("over", "active", "reason"),
        [
            ({}, INSTANCE, None),
            ({"is_default": True}, INSTANCE, "skipped_default"),
            # Only a legacy Nextcloud folder id: no handle the driver can name.
            ({"main_cloud_folder_handle": None}, INSTANCE, "skipped_unrouted"),
            (
                {"main_cloud_backend": None, "main_cloud_backend_instance_id": None},
                INSTANCE,
                "skipped_unrouted",
            ),
            ({"main_cloud_backend_instance_id": None}, INSTANCE, "skipped_inactive"),
            # Another installation's folder: nothing substitutes for it.
            (
                {"main_cloud_backend_instance_id": OTHER_INSTANCE},
                INSTANCE,
                "skipped_inactive",
            ),
            ({}, None, "skipped_inactive"),
        ],
    )
    def test_reasons(self, over, active, reason):
        assert connectors.skip_reason(_project(**over), active) == reason

    def test_ids_compare_by_value(self):
        from uuid import UUID

        project = _project(main_cloud_backend_instance_id=UUID(INSTANCE))
        assert connectors.skip_reason(project, INSTANCE) is None


class TestEnsure:
    @pytest.mark.asyncio
    async def test_a_cloud_project_gets_its_row_once(self):
        store = _Store()

        assert await _ensure(store, _project()) == "created"
        assert await _ensure(store, _project()) == "present"

        [created] = store.created
        assert created == {
            "name": f"Atlas cloud folder ({PROJECT[:8]})",
            "ds_type": "cloud_folder",
            "connection_url": None,
            "description": (
                "This project's folder in the main cloud, mounted in its sessions."
            ),
            "credentials": {},
            "config": {
                "folder_kind": "project",
                "project_id": PROJECT,
                "backend_id": "nextcloud",
                "backend_instance_id": INSTANCE,
                "folder_handle": "nextcloud:41",
            },
            # The earliest-added owner, for the record.
            "created_by": EARLIER_OWNER,
            "read_only": None,
            "scope_mode": "projects",
            "auto_attach": True,
            "project_ids": [PROJECT],
            "managed_key": f"project-cloud:{PROJECT}",
        }

    @pytest.mark.asyncio
    async def test_a_listed_owner_is_used_and_none_is_none(self):
        store = _Store()
        await _ensure(store, _project(owner_id=None))
        assert store.created[0]["created_by"] is None

    @pytest.mark.asyncio
    async def test_a_concurrent_pass_that_won_counts_as_present(self):
        class _Racing(_Store):
            async def get_datasource_by_managed_key(self, key):
                return None  # read before the other pass committed

        racing = _Racing()
        racing.rows = {f"project-cloud:{PROJECT}": {"id": "winner"}}
        assert await _ensure(racing, _project()) == "present"

    @pytest.mark.asyncio
    async def test_a_repaired_folder_repoints_the_row(self):
        store = _Store()
        await _ensure(store, _project())
        row_id = store.rows[f"project-cloud:{PROJECT}"]["id"]

        moved = _project(main_cloud_folder_handle="nextcloud:57")
        assert await _ensure(store, moved) == "repointed"
        assert store.updated == [
            (
                row_id,
                {
                    "config": {
                        **store.created[0]["config"],
                        "folder_handle": "nextcloud:57",
                    }
                },
            )
        ]
        assert await _ensure(store, moved) == "present"
        assert len(store.created) == 1

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "over",
        [
            {"is_default": True},
            {"main_cloud_folder_handle": None},
            {"main_cloud_backend_instance_id": OTHER_INSTANCE},
        ],
    )
    async def test_a_skipped_project_touches_nothing(self, over):
        store = _Store(fail=AssertionError("read the store"))
        assert (await _ensure(store, _project(**over))).startswith("skipped_")
        assert store.created == []

    @pytest.mark.asyncio
    async def test_a_failure_is_logged_and_deferred_never_raised(self):
        log = MagicMock()
        store = _Store(fail=RuntimeError("db gone"))

        assert await _ensure(store, _project(), log=log) == "deferred"
        log.warning.assert_called_once()
        assert "db gone" in str(log.warning.call_args)


class TestBackfill:
    @pytest.mark.asyncio
    async def test_counts_and_a_second_pass_changes_nothing(self):
        second = str(uuid4())
        store = _Store()
        store.projects = [
            _project(owner_id=OWNER),
            _project(id=second, name="Borealis", owner_id=None),
            _project(id=str(uuid4()), main_cloud_backend_instance_id=OTHER_INSTANCE),
            _project(id=str(uuid4()), main_cloud_folder_handle=None),
        ]
        log = MagicMock()

        first = await connectors.backfill_project_cloud_connectors(
            store, active_instance_id=INSTANCE, log=log
        )
        rerun = await connectors.backfill_project_cloud_connectors(
            store, active_instance_id=INSTANCE, log=log
        )

        assert first == {
            "created": 2,
            "present": 0,
            "repointed": 0,
            "skipped_default": 0,
            "skipped_inactive": 1,
            "skipped_unrouted": 1,
            "deferred": 0,
        }
        assert rerun == {**first, "created": 0, "present": 2}
        assert log.info.call_args_list[-1].args == (
            "Project cloud folder connectors: %s",
            rerun,
        )
        log.error.assert_not_called()

    @pytest.mark.asyncio
    async def test_a_deferred_project_logs_an_error_line(self):
        store = _Store(fail=RuntimeError("db gone"))
        store.projects = [_project()]
        log = MagicMock()

        counts = await connectors.backfill_project_cloud_connectors(
            store, active_instance_id=INSTANCE, log=log
        )

        assert counts["deferred"] == 1
        log.error.assert_called_once_with("Project cloud folder connectors: %s", counts)
