"""A user's own session outbox connector (main cloud slice 6.1).

Pinned here: one ``cloud-outbox:<user>`` row per approved user while a main
cloud is configured, with its exact shape; the startup backfill is
idempotent and counts; admitting a user creates theirs; a failure never
raises.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

USER = "00000000-0000-4000-8000-0000000000a1"
OTHER = "00000000-0000-4000-8000-0000000000a2"
INSTANCE = "00000000-0000-4000-8000-00000000c10d"


class RowStore:
    def __init__(self, users=()):
        self.users = list(users)
        self.rows: dict[str, dict] = {}
        self.created: list[dict] = []

    async def get_datasource_by_managed_key(self, key):
        return next(
            (dict(r) for r in self.rows.values() if r.get("managed_key") == key), None
        )

    async def create_datasource(self, **kwargs):
        if any(
            r.get("managed_key") == kwargs["managed_key"] for r in self.rows.values()
        ):
            return None
        row = {"id": f"ds-{len(self.rows)}", "type": kwargs["ds_type"], **kwargs}
        self.rows[row["id"]] = row
        self.created.append(kwargs)
        return row

    async def list_users(self, limit=100):
        return self.users[:limit]


@pytest.mark.asyncio
async def test_every_approved_user_gets_one_outbox_and_a_second_pass_changes_nothing():
    from orchestrator.services import cloud_outbox_connectors as outboxes

    store = RowStore(
        [
            {"id": USER, "is_approved": True},
            {"id": OTHER, "is_approved": False},
        ]
    )
    first = await outboxes.backfill_cloud_outbox_connectors(
        store, active_instance_id=INSTANCE
    )
    assert first == {"created": 1, "present": 0, "deferred": 0}
    assert store.created == [
        {
            "name": "Session outbox",
            "ds_type": "cloud_outbox",
            "connection_url": None,
            "description": (
                "A folder SRW creates in the main cloud for each of your sessions "
                "and shares with you."
            ),
            "credentials": {},
            "config": {},
            "created_by": USER,
            "read_only": None,
            "scope_mode": "all",
            "auto_attach": True,
            "managed_key": f"cloud-outbox:{USER}",
        }
    ]
    second = await outboxes.backfill_cloud_outbox_connectors(
        store, active_instance_id=INSTANCE
    )
    assert second == {"created": 0, "present": 1, "deferred": 0}
    # No main cloud: nothing for an outbox to stand for.
    assert await outboxes.backfill_cloud_outbox_connectors(
        RowStore([{"id": USER, "is_approved": True}]), active_instance_id=None
    ) == {"created": 0, "present": 0, "deferred": 0}


@pytest.mark.asyncio
async def test_an_admitted_user_gets_their_outbox_and_a_failure_never_raises():
    from orchestrator.services import cloud_outbox_connectors as outboxes

    store = RowStore()
    router = SimpleNamespace(active_instance_id=INSTANCE)
    assert await outboxes.ensure_outbox_if_cloud(store, router, USER) == "created"
    assert await outboxes.ensure_outbox_if_cloud(store, router, USER) == "present"
    assert (
        await outboxes.ensure_outbox_if_cloud(
            store, SimpleNamespace(active_instance_id=None), OTHER
        )
        is None
    )

    class Broken(RowStore):
        async def create_datasource(self, **kwargs):
            raise RuntimeError("db down")

    assert await outboxes.ensure_user_outbox_connector(Broken(), USER) == "deferred"


@pytest.mark.asyncio
async def test_an_approval_gives_the_user_their_outbox():
    from orchestrator.schemas.users import AdminBulkApprove
    from orchestrator.services import user_administration as admin

    class Approving(RowStore):
        async def approve_users(self, ids, *, approved_by):
            return list(ids)

        async def get_user(self, uid):
            return {"id": uid}

    store = Approving()
    provisioned = []

    async def ensure(row):
        provisioned.append(row["id"])

    deps = SimpleNamespace(
        store=store,
        main_cloud_router=SimpleNamespace(active_instance_id=INSTANCE),
        ensure_user_provisioned=ensure,
        notification_service=SimpleNamespace(resolve_source=_noop),
        logger=SimpleNamespace(warning=lambda *a, **k: None),
    )
    result = await admin.admin_bulk_approve_users(
        body=AdminBulkApprove(user_ids=[USER]), admin={"id": OTHER}, dependencies=deps
    )
    assert result["approved_count"] == 1 and provisioned == [USER]
    assert [row["managed_key"] for row in store.created] == [f"cloud-outbox:{USER}"]


async def _noop(*args, **kwargs):
    return None
