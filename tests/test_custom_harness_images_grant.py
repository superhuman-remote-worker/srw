"""custom_harness_images grant: direct-path check on PostgresDB.

Gates generic manifest Job admission for admin-approved harness images.
"""

from unittest.mock import AsyncMock

import pytest

from orchestrator.database.postgres import PostgresDB

pytestmark = pytest.mark.asyncio

PROJECT_ID = "11111111-1111-1111-1111-111111111111"
EMPTY_SCOPES = {"user": [], "project": [], "global": []}
USER = {"id": "u1", "is_admin": False}


def _db(scoped):
    db = PostgresDB.__new__(PostgresDB)
    db.list_grants_for_scopes = AsyncMock(return_value=scoped)
    return db


def _row(value):
    return {"key": "custom_harness_images", "value_json": value}


async def test_admin_short_circuits_without_grant_read():
    db = _db(EMPTY_SCOPES)
    assert (
        await db.user_can_run_custom_harness_images(
            {"id": "u1", "is_admin": True}, PROJECT_ID
        )
        is True
    )
    db.list_grants_for_scopes.assert_not_awaited()


async def test_no_rows_denies_by_default():
    db = _db(EMPTY_SCOPES)
    assert await db.user_can_run_custom_harness_images(USER, PROJECT_ID) is False


async def test_user_scope_grant_allows():
    db = _db({"user": [_row(True)], "project": [], "global": []})
    assert await db.user_can_run_custom_harness_images(USER, None) is True


async def test_project_scope_grant_allows_and_is_queried():
    db = _db({"user": [], "project": [_row(True)], "global": []})
    assert await db.user_can_run_custom_harness_images(USER, PROJECT_ID) is True
    assert db.list_grants_for_scopes.await_args.kwargs["project_ids"] == [PROJECT_ID]


async def test_a_user_grant_cannot_widen_past_a_global_denial():
    db = _db({"user": [_row(True)], "project": [], "global": [_row(False)]})
    assert await db.user_can_run_custom_harness_images(USER, PROJECT_ID) is False


async def test_grant_read_failure_fails_closed():
    db = PostgresDB.__new__(PostgresDB)
    db.list_grants_for_scopes = AsyncMock(side_effect=RuntimeError("db down"))
    assert await db.user_can_run_custom_harness_images(USER, PROJECT_ID) is False
