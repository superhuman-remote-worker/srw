"""HF-6 — the two list endpoints no longer issue per-row queries.

Two N+1s were collapsed:
  * GET /api/datasources — per-row user_can_access_datasource (a
    list_datasource_projects + up to M get_user_role_in_project per row) →
    filter_visible_datasources: one membership resolution + one bulk link fetch.
  * GET /api/persistent/threads — per-thread list_thread_mounts (since main
    cloud slice 8 the list reads no mount row at all).

The correctness tests pin that filter_visible_datasources matches the per-row
gate; the call-count tests pin that the per-row methods are never touched.
"""

from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID, uuid4

import pytest

from orchestrator.security.access import filter_visible_datasources


def _ds(ds_id: UUID, created_by: UUID | None = None) -> dict:
    return {"id": ds_id, "created_by": str(created_by) if created_by else None}


def _fake_db(*, member_projects=(), links=None) -> MagicMock:
    """A db whose only real behavior is membership + the bulk link fetch.

    The per-row methods are present so a test can assert they stay untouched.
    """
    db = MagicMock()
    db.get_projects_for_user = AsyncMock(
        return_value=[{"id": p} for p in member_projects]
    )
    db.list_datasource_projects_bulk = AsyncMock(return_value=dict(links or {}))
    db.list_datasource_projects = AsyncMock(return_value=[])
    db.get_user_role_in_project = AsyncMock(return_value=None)
    return db


def _assert_no_per_row(db):
    assert db.list_datasource_projects.call_count == 0, "per-row link fetch must go"
    assert db.get_user_role_in_project.call_count == 0, "per-row role check must go"


class TestFilterVisibleDatasources:
    @pytest.mark.asyncio
    async def test_empty_rows_short_circuits(self):
        db = _fake_db()
        assert await filter_visible_datasources({"id": uuid4()}, db, []) == []
        assert db.list_datasource_projects_bulk.call_count == 0

    @pytest.mark.asyncio
    async def test_admin_sees_all_without_any_link_fetch(self):
        uid = uuid4()
        rows = [_ds(uuid4()), _ds(uuid4())]
        db = _fake_db()
        out = await filter_visible_datasources({"id": uid, "is_admin": True}, db, rows)
        assert out == rows
        # Unscoped admin short-circuits: no bulk fetch, no membership resolution.
        assert db.list_datasource_projects_bulk.call_count == 0
        assert db.get_projects_for_user.call_count == 0
        _assert_no_per_row(db)

    @pytest.mark.asyncio
    async def test_member_and_creator_and_neither(self):
        uid = uuid4()
        proj = uuid4()
        ds_linked = uuid4()  # linked to proj (member) -> visible
        ds_mine = uuid4()  # created by caller, no link -> visible
        ds_other = uuid4()  # someone else's, unlinked -> hidden
        rows = [_ds(ds_linked), _ds(ds_mine, created_by=uid), _ds(ds_other)]
        db = _fake_db(
            member_projects=[proj],
            links={str(ds_linked): [str(proj)], str(ds_other): [str(uuid4())]},
        )

        out = await filter_visible_datasources({"id": uid}, db, rows)
        got = {d["id"] for d in out}

        assert got == {ds_linked, ds_mine}
        assert ds_other not in got
        # One bulk fetch for the whole page; zero per-row queries.
        assert db.list_datasource_projects_bulk.call_count == 1
        _assert_no_per_row(db)

    @pytest.mark.asyncio
    async def test_scoped_token_requires_link_and_ignores_creator(self):
        uid = uuid4()
        scope = uuid4()
        ds_in_scope = uuid4()  # linked to the scoped project -> visible
        ds_mine_oos = uuid4()  # created by caller but NOT in scope -> hidden
        rows = [_ds(ds_in_scope), _ds(ds_mine_oos, created_by=uid)]
        db = _fake_db(
            member_projects=[scope],  # caller is a member of the scoped project
            links={str(ds_in_scope): [str(scope)], str(ds_mine_oos): [str(uuid4())]},
        )
        user = {"id": uid, "scopes": [f"project:{scope}"]}

        out = await filter_visible_datasources(user, db, rows)
        got = {d["id"] for d in out}

        assert got == {ds_in_scope}, "creator access must not survive a project scope"
        assert db.list_datasource_projects_bulk.call_count == 1
        _assert_no_per_row(db)

    @pytest.mark.asyncio
    async def test_scoped_token_non_member_sees_nothing(self):
        uid = uuid4()
        scope = uuid4()
        ds_in_scope = uuid4()
        rows = [_ds(ds_in_scope)]
        # caller is NOT a member of the scoped project
        db = _fake_db(member_projects=[], links={str(ds_in_scope): [str(scope)]})
        user = {"id": uid, "scopes": [f"project:{scope}"]}

        out = await filter_visible_datasources(user, db, rows)
        assert out == []
        _assert_no_per_row(db)


# --- bulk DB methods: single query + correct grouping (conn-mocked) ----------


def _db_with_conn(fetch_rows):
    with patch.dict("os.environ", {"DATABASE_URL": "postgresql://test"}):
        from orchestrator.database import PostgresDB

        db = PostgresDB()
    db._pool = MagicMock()
    conn = AsyncMock()
    conn.fetch = AsyncMock(return_value=fetch_rows)
    cm = AsyncMock()
    cm.__aenter__ = AsyncMock(return_value=conn)
    cm.__aexit__ = AsyncMock(return_value=False)
    db.acquire = MagicMock(return_value=cm)
    return db, conn


class TestListDatasourceProjectsBulk:
    @pytest.mark.asyncio
    async def test_groups_by_datasource_in_one_query(self):
        ds1, ds2 = uuid4(), uuid4()
        p1, p2, p3 = uuid4(), uuid4(), uuid4()
        db, conn = _db_with_conn(
            [
                {"datasource_id": ds1, "project_id": p1},
                {"datasource_id": ds1, "project_id": p2},
                {"datasource_id": ds2, "project_id": p3},
            ]
        )
        out = await db.list_datasource_projects_bulk([str(ds1), str(ds2)])
        assert out == {str(ds1): [str(p1), str(p2)], str(ds2): [str(p3)]}
        assert conn.fetch.call_count == 1, "must be a single ANY($1) query"

    @pytest.mark.asyncio
    async def test_empty_and_invalid_ids_do_not_query(self):
        db, conn = _db_with_conn([])
        assert await db.list_datasource_projects_bulk([]) == {}
        assert await db.list_datasource_projects_bulk(["not-a-uuid"]) == {}
        assert conn.fetch.call_count == 0
