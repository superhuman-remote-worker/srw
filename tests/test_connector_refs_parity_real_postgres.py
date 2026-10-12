"""``execution.connectors`` binds exactly what ``datasource_ids`` binds.

The D3c review's parity matrix, kept as a regression test: for every caller
(the owner, an administrator with and without a project, a member with and
without one, a member of another project, a member removed from the project,
an unapproved member, a stranger with and without a project of their own) and
every kind of connector (private, project-only, linked with all scope, public,
the project's knowledge base), the same selection by id, by uid ref and by
name ref ends the same way, through the job funnel and the session funnel:
the same ids, or the same refusal down to its status and body.
"""

from __future__ import annotations

from functools import partial
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

from fastapi import HTTPException
import pytest

from orchestrator.schemas.thread_admission import ThreadCreateRequest
from orchestrator.services.thread_admission import select_thread_datasources
from orchestrator.services.thread_datasource_authorization import (
    ThreadDatasourceAuthorizationDependencies,
    authorize_thread_datasource_selection,
)
from tests import test_manifest_native_full_schema as full_schema
from tests.test_project_connectors_real_postgres import (
    _connector,
    _job_selection,
    _knowledge_base,
    _project,
    _ref_of,
    _user,
)

database = full_schema.database
postgres_url = full_schema.postgres_url


async def _session_selection(db, user, project, **fields):
    """The session funnel, with or without a project."""
    authorize = partial(
        authorize_thread_datasource_selection,
        dependencies=ThreadDatasourceAuthorizationDependencies(
            store=db, thread_project_ids=AsyncMock(return_value=[])
        ),
    )
    body = (
        ThreadCreateRequest(project_id=project, **fields)
        if project
        else ThreadCreateRequest(**fields)
    )
    ids, revisions, _ = await select_thread_datasources(
        body,
        user,
        thread_backend="sandbox",
        effective_project_ids=[project] if project else [],
        dependencies=SimpleNamespace(
            store=db,
            authorize_thread_datasource_selection=authorize,
            datasource_defaults_on_omission=lambda: False,
        ),
    )
    return ids, revisions


async def _outcome(call):
    try:
        ids, _ = await call
        return ("ok", tuple(ids))
    except HTTPException as exc:
        return (exc.status_code, str(exc.detail))


@pytest.mark.asyncio
@pytest.mark.parametrize("selection", [_job_selection, _session_selection])
async def test_ids_uid_refs_and_name_refs_end_the_same_way(database, selection):
    db = database
    owner = await _user(db, "Owner")
    admin = await _user(db, "Admin", admin=True)
    member = await _user(db, "Member")
    both = await _user(db, "MemberOfBoth")
    removed = await _user(db, "Removed")
    unapproved = await _user(db, "Unapproved")
    stranger = await _user(db, "Stranger")
    publisher = await _user(db, "Publisher")
    project_a = await _project(
        db,
        "A",
        owner,
        **{
            member["id"]: "editor",
            both["id"]: "editor",
            removed["id"]: "editor",
            unapproved["id"]: "editor",
        },
    )
    project_b = await _project(db, "B", owner, **{both["id"]: "editor"})
    project_s = await _project(db, "S", stranger)
    kb_a = await _knowledge_base(db, project_a, owner)
    private = await _connector(db, "Private", owner, scope_mode="all")
    linked_a = await _connector(
        db, "LinkedA", owner, scope_mode="projects", project_ids=[project_a]
    )
    linked_all = await _connector(
        db, "LinkedAll", owner, scope_mode="all", project_ids=[project_a]
    )
    public = await _connector(
        db, "Public", publisher, is_global=True, read_only=True, scope_mode="all"
    )
    await db.execute(
        "DELETE FROM project_members WHERE project_id=$1 AND user_id=$2",
        UUID(project_a),
        UUID(removed["id"]),
    )
    await db.execute(
        "UPDATE users SET is_approved=FALSE WHERE id=$1", UUID(unapproved["id"])
    )

    outcomes = []
    for label, user, project in (
        ("owner", owner, project_a),
        ("admin", admin, project_a),
        ("admin-projectless", admin, None),
        ("member", member, project_a),
        ("member-projectless", member, None),
        ("both-in-B", both, project_b),
        ("removed", removed, project_a),
        ("unapproved", {**unapproved, "is_approved": False}, project_a),
        ("stranger", stranger, project_s),
        ("stranger-projectless", stranger, None),
    ):
        for datasource_id in (private, linked_a, linked_all, public, kb_a):
            ref = await _ref_of(db, datasource_id)
            by_id = await _outcome(
                selection(db, user, project, datasource_ids=[datasource_id])
            )
            by_uid = await _outcome(
                selection(
                    db,
                    user,
                    project,
                    execution={"connectors": {"c": {"ref": {"uid": datasource_id}}}},
                )
            )
            by_name = await _outcome(
                selection(
                    db, user, project, execution={"connectors": {"c": {"ref": ref}}}
                )
            )
            outcomes.append((label, datasource_id, by_id, by_uid, by_name))
    mismatches = [case for case in outcomes if not case[2] == case[3] == case[4]]
    assert mismatches == []
    # The matrix holds both answers: some succeed, some are refused.
    results = {case[2][0] for case in outcomes}
    assert "ok" in results and 403 in results
