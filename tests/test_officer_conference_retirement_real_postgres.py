"""A conference leaving service is no longer the project's open conference.

`_find_open_conference_thread` is the single-writer rule's read side: the
create path resumes whatever it returns instead of minting a rival. An
authorized retirement is irrevocable — the thread admits no further input —
so counting it keeps a project locked out of conferences for exactly as long
as that retirement takes to settle, which on a stuck runtime is forever.
"""

import json

import pytest

from orchestrator import main
from tests import test_persistent_recycler_real_postgres as fixtures
from orchestrator.application import workflows as workflows_composition
from orchestrator.services import officer_conference as officer_conference_module

db = fixtures.db
pg_dsn = fixtures.pg_dsn
_schema_applied = fixtures._schema_applied


async def _conference(db, monkeypatch):
    ids = await fixtures._seed(db, protected_agent_pod=True, workspace_claim=False)
    ids.pop("old_access")
    await db.execute(
        "UPDATE threads SET status='awaiting_user', metadata=jsonb_set(metadata,"
        "'{config_override,officer}',$2::jsonb) WHERE id=$1::uuid",
        ids["thread"],
        json.dumps({"enabled": False, "conference": True}),
    )
    monkeypatch.setattr(main.app.state.resources, "postgres_db", db)
    return ids


@pytest.mark.asyncio
async def test_open_conference_ignores_an_authorized_retirement(db, monkeypatch):
    ids = await _conference(db, monkeypatch)
    found = await officer_conference_module.find_open_conference_thread(
        ids["project"],
        dependencies=workflows_composition.officer_conference_dependencies(
            main.app.state.resources
        ),
    )
    assert found is not None and str(found["id"]) == ids["thread"]

    retirement = await db.begin_pinned_thread_retirement(ids["thread"], permanent=False)
    # A begun-but-unauthorized retirement is still abortable, so the thread
    # may yet return to service: it stays the open conference.
    found = await officer_conference_module.find_open_conference_thread(
        ids["project"],
        dependencies=workflows_composition.officer_conference_dependencies(
            main.app.state.resources
        ),
    )
    assert found is not None and str(found["id"]) == ids["thread"]

    assert await db.authorize_pinned_thread_retirement(
        ids["thread"],
        token=retirement["token"],
        generation=retirement["generation"],
        settle_status="ended",
    )
    # Authorized is irrevocable: the row is still `awaiting_user`, but it is
    # leaving service and must not block a fresh conference.
    assert (await db.get_thread(ids["thread"]))["status"] == "awaiting_user"
    assert (
        await officer_conference_module.find_open_conference_thread(
            ids["project"],
            dependencies=workflows_composition.officer_conference_dependencies(
                main.app.state.resources
            ),
        )
        is None
    )
