"""Focused policy tests for project-scoped connector defaults/authorization."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException

from orchestrator.services.datasource_policy import (
    DatasourceUnavailableError,
    DatasourceWorkspaceTierError,
    ItemVerdict,
    authorize_datasource_ids,
    authorize_datasource_selection,
    classify_datasource_selection,
    default_datasource_ids,
    default_datasource_selection,
)
from orchestrator.application import preparation as preparation_composition
from orchestrator.services import (
    job_datasource_selection as job_datasource_selection_module,
)
from orchestrator.services import (
    thread_datasource_authorization as thread_datasource_authorization_module,
)
from orchestrator.services import thread_mount_rows as thread_mount_rows_module


OWNER = "11111111-1111-4111-8111-111111111111"
OTHER = "22222222-2222-4222-8222-222222222222"
PROJECT_A = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
PROJECT_B = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
DS_OWNED = "33333333-3333-4333-8333-333333333333"
DS_SHARED = "44444444-4444-4444-8444-444444444444"
DS_NATIVE = "55555555-5555-4555-8555-555555555555"
DS_REPOSITORY = "66666666-6666-4666-8666-666666666666"
LEGACY_JOB = "77777777-7777-4777-8777-777777777777"
OTHER_JOB = "88888888-8888-4888-8888-888888888888"


def _row(
    datasource_id: str,
    *,
    owner: str | None = OWNER,
    scope: str = "all",
    projects: tuple[str, ...] = (),
    automatic: bool = False,
    public: bool = False,
    ds_type: str = "postgresql",
    config: dict | None = None,
    revision: int = 1,
    job_id: str | None = None,
) -> dict:
    return {
        "id": datasource_id,
        "type": ds_type,
        "created_by": owner,
        "is_global": public,
        "scope_mode": scope,
        "auto_attach": automatic,
        "policy_revision": revision,
        "job_id": job_id,
        "project_ids": list(projects),
        "config": config or {},
    }


def _db(rows: list[dict], *, member: bool = True):
    by_id = {row["id"]: row for row in rows}
    db = AsyncMock()
    db.get_user = AsyncMock(
        side_effect=lambda user_id: {
            "id": user_id,
            "is_approved": True,
        }
    )
    db.user_is_member_of_projects = AsyncMock(return_value=member)

    def policy_rows(ids, *, legacy_job_id=None):
        return [
            by_id[value]
            for value in ids
            if value in by_id
            and (
                not by_id[value].get("job_id")
                or by_id[value].get("job_id") == legacy_job_id
            )
        ]

    db.get_datasource_policy_rows = AsyncMock(side_effect=policy_rows)
    db.list_default_datasource_candidates = AsyncMock(return_value=list(rows))
    return db


@pytest.mark.asyncio
async def test_owner_all_scope_can_explicitly_select_projectless_connector():
    db = _db([_row(DS_OWNED)])

    result = await authorize_datasource_ids(
        db,
        {"id": OWNER, "is_admin": False},
        OWNER,
        [DS_OWNED, DS_OWNED],
        [],
        "sandbox",
    )

    assert result == [DS_OWNED]


@pytest.mark.asyncio
@pytest.mark.parametrize("backend", ["none", "virtual"])
async def test_credentials_require_shell_workspace(backend):
    db = _db([_row(DS_OWNED, ds_type="credentials")])
    with pytest.raises(DatasourceWorkspaceTierError):
        await authorize_datasource_ids(
            db, {"id": OWNER}, OWNER, [DS_OWNED], [], backend
        )


@pytest.mark.asyncio
async def test_explicit_selection_returns_revision_from_authorization_rows():
    db = _db([_row(DS_OWNED, revision=9)])

    selected, revisions = await authorize_datasource_selection(
        db,
        {"id": OWNER, "is_admin": False},
        OWNER,
        [DS_OWNED],
        [],
        "sandbox",
    )

    assert selected == [DS_OWNED]
    assert revisions == {DS_OWNED: 9}
    db.get_datasource_policy_rows.assert_awaited_once_with([DS_OWNED])


@pytest.mark.asyncio
async def test_project_scope_requires_every_work_project():
    db = _db(
        [
            _row(
                DS_OWNED,
                scope="projects",
                projects=(PROJECT_A,),
            )
        ]
    )

    with pytest.raises(DatasourceUnavailableError):
        await authorize_datasource_ids(
            db,
            {"id": OWNER},
            OWNER,
            [DS_OWNED],
            [PROJECT_A, PROJECT_B],
            "sandbox",
        )


@pytest.mark.asyncio
async def test_project_link_grant_is_target_bound_for_private_all_scope():
    db = _db(
        [
            _row(
                DS_SHARED,
                owner=OTHER,
                projects=(PROJECT_A,),
            )
        ]
    )

    assert await authorize_datasource_ids(
        db,
        {"id": OWNER},
        OWNER,
        [DS_SHARED],
        [PROJECT_A],
        "sandbox",
    ) == [DS_SHARED]
    with pytest.raises(DatasourceUnavailableError):
        await authorize_datasource_ids(
            db,
            {"id": OWNER},
            OWNER,
            [DS_SHARED],
            [PROJECT_B],
            "sandbox",
        )


@pytest.mark.asyncio
async def test_public_publishers_cannot_set_another_users_defaults():
    db = _db(
        [
            _row(
                DS_SHARED,
                owner=OTHER,
                automatic=True,
                public=True,
            )
        ]
    )

    assert await default_datasource_ids(db, OWNER, [], "sandbox") == []


@pytest.mark.asyncio
async def test_native_project_kb_is_project_managed_default():
    native = _row(
        DS_NATIVE,
        owner=None,
        scope="projects",
        projects=(PROJECT_A,),
        automatic=True,
        ds_type="kb",
        config={"native_project_id": PROJECT_A},
    )
    db = _db([native])

    assert await default_datasource_ids(
        db, OWNER, [PROJECT_A, PROJECT_B], "sandbox"
    ) == [DS_NATIVE]


@pytest.mark.asyncio
async def test_lite_tier_filters_implicit_repository_but_rejects_explicit_one():
    repository = _row(
        DS_REPOSITORY,
        automatic=True,
        ds_type="repository",
    )
    db = _db([repository])

    assert await default_datasource_ids(db, OWNER, [], "virtual") == []
    with pytest.raises(DatasourceWorkspaceTierError):
        await authorize_datasource_ids(
            db,
            {"id": OWNER},
            OWNER,
            [DS_REPOSITORY],
            [],
            "virtual",
        )


@pytest.mark.asyncio
async def test_default_selection_returns_revisions_from_decision_rows():
    selected_row = _row(DS_OWNED, automatic=True, revision=7)
    filtered_repository = _row(
        DS_REPOSITORY,
        automatic=True,
        ds_type="repository",
        revision=11,
    )
    db = _db([selected_row, filtered_repository])

    selected, revisions = await default_datasource_selection(
        db,
        OWNER,
        [],
        "virtual",
    )

    assert selected == [DS_OWNED]
    assert revisions == {DS_OWNED: 7}
    db.get_datasource_policy_rows.assert_not_awaited()


@pytest.mark.asyncio
async def test_named_owner_membership_loss_fails_closed():
    db = _db([_row(DS_OWNED, automatic=True)], member=False)

    with pytest.raises(DatasourceUnavailableError):
        await default_datasource_ids(db, OWNER, [PROJECT_A], "sandbox")


def test_delivery_snapshot_rejects_missing_or_revision_changed_rows():
    for resolved in (
        [],
        [{"id": DS_OWNED, "policy_revision": 8}],
    ):
        with pytest.raises(HTTPException) as exc:
            job_datasource_selection_module.require_exact_datasource_resolution(
                [DS_OWNED], {DS_OWNED: 9}, resolved
            )
        assert exc.value.status_code == 403
        assert exc.value.detail == "One or more selected connectors are unavailable"


def test_delivery_snapshot_rejects_duplicate_resolver_rows():
    row = {"id": DS_OWNED, "policy_revision": 9}
    with pytest.raises(HTTPException):
        job_datasource_selection_module.require_exact_datasource_resolution(
            [DS_OWNED], {DS_OWNED: 9}, [row, dict(row)]
        )


@pytest.mark.asyncio
async def test_job_delivery_authorizes_and_resolves_as_one_exact_snapshot(
    monkeypatch,
):
    import orchestrator.main as main

    db = AsyncMock()
    db.resolve_datasources_for_job = AsyncMock(return_value=[])
    monkeypatch.setattr(main.app.state.resources, "postgres_db", db)
    monkeypatch.setattr(
        job_datasource_selection_module,
        "revalidate_job_datasource_selection",
        AsyncMock(return_value=([DS_OWNED], {DS_OWNED: 3})),
    )

    with pytest.raises(HTTPException):
        await job_datasource_selection_module.resolve_authorized_job_datasources(
            {"id": LEGACY_JOB, "project_id": PROJECT_A},
            dependencies=preparation_composition.job_datasource_selection_dependencies(
                main.app.state.resources
            ),
        )

    db.resolve_datasources_for_job.assert_awaited_once_with(
        LEGACY_JOB, project_id=PROJECT_A
    )


@pytest.mark.asyncio
async def test_cold_thread_resolution_rejects_silent_deleted_connector(
    monkeypatch,
):
    import orchestrator.main as main

    db = AsyncMock()
    db.resolve_datasources_for_thread = AsyncMock(return_value=[])
    monkeypatch.setattr(main.app.state.resources, "postgres_db", db)
    monkeypatch.setattr(
        thread_datasource_authorization_module,
        "revalidate_thread_datasource_selection",
        AsyncMock(return_value=([DS_OWNED], {DS_OWNED: 3})),
    )

    with pytest.raises(HTTPException):
        await thread_mount_rows_module.resolve_thread_datasources(
            {"id": "99999999-9999-4999-8999-999999999999"},
            {"datasource_ids": [DS_OWNED]},
            project_ids=[PROJECT_A],
            dependencies=preparation_composition.thread_mount_dependencies(
                main.app.state.resources
            ),
        )


@pytest.mark.asyncio
async def test_parent_inheritance_rejects_deleted_materialized_connector(
    monkeypatch,
):
    import orchestrator.main as main

    db = AsyncMock()
    db.get_job = AsyncMock(
        return_value={
            "id": LEGACY_JOB,
            "user_id": OWNER,
            "context": {
                "datasource_selection": {
                    "datasource_ids": [DS_OWNED],
                    "policy_revisions": {DS_OWNED: 4},
                }
            },
        }
    )
    # FK CASCADE after connector deletion removed the junction row.  The
    # immutable context still proves that [] is a reduction, not an opt-out.
    db.list_job_datasource_ids = AsyncMock(return_value=[])
    monkeypatch.setattr(main.app.state.resources, "postgres_db", db)

    with pytest.raises(HTTPException) as exc:
        await job_datasource_selection_module.inherit_parent_datasource_ids(
            thread_id=None,
            parent_job_id=LEGACY_JOB,
            dependencies=preparation_composition.job_datasource_selection_dependencies(
                main.app.state.resources
            ),
        )

    assert exc.value.status_code == 403


@pytest.mark.asyncio
async def test_parent_inheritance_propagates_database_failures(monkeypatch):
    import orchestrator.main as main

    db = AsyncMock()
    db.get_thread = AsyncMock(side_effect=RuntimeError("database unavailable"))
    monkeypatch.setattr(main.app.state.resources, "postgres_db", db)

    with pytest.raises(RuntimeError, match="database unavailable"):
        await job_datasource_selection_module.inherit_parent_datasource_ids(
            thread_id="99999999-9999-4999-8999-999999999999",
            parent_job_id=LEGACY_JOB,
            dependencies=preparation_composition.job_datasource_selection_dependencies(
                main.app.state.resources
            ),
        )


@pytest.mark.asyncio
async def test_admin_effective_owner_uses_project_acl_without_membership_row():
    row = _row(DS_OWNED, automatic=True)
    db = _db([row], member=False)
    db.get_user = AsyncMock(
        return_value={"id": OWNER, "is_approved": True, "is_admin": True}
    )

    assert await default_datasource_ids(db, OWNER, [PROJECT_A], "sandbox") == [DS_OWNED]


@pytest.mark.asyncio
async def test_trusted_ownerless_inheritance_still_enforces_scope():
    db = _db(
        [
            _row(
                DS_SHARED,
                owner=None,
                scope="projects",
                projects=(PROJECT_A,),
            )
        ]
    )

    assert await authorize_datasource_ids(
        db,
        None,
        None,
        [DS_SHARED],
        [PROJECT_A],
        "sandbox",
        trusted_system_inheritance=True,
    ) == [DS_SHARED]
    with pytest.raises(DatasourceUnavailableError):
        await authorize_datasource_ids(
            db,
            None,
            None,
            [DS_SHARED],
            [PROJECT_B],
            "sandbox",
            trusted_system_inheritance=True,
        )


@pytest.mark.asyncio
async def test_ownerless_untrusted_explicit_selection_has_no_private_bypass():
    db = _db([_row(DS_SHARED, owner=None)])

    with pytest.raises(DatasourceUnavailableError):
        await authorize_datasource_ids(
            db,
            None,
            None,
            [DS_SHARED],
            [],
            "sandbox",
        )


@pytest.mark.asyncio
async def test_exact_legacy_job_binding_remains_reauthorizable_only_for_that_job():
    legacy = _row(
        DS_SHARED,
        owner=None,
        job_id=LEGACY_JOB,
    )
    db = _db([legacy])

    selected, revisions = await authorize_datasource_selection(
        db,
        {"id": OWNER},
        OWNER,
        [DS_SHARED],
        [],
        "sandbox",
        legacy_job_id=LEGACY_JOB,
    )
    assert selected == [DS_SHARED]
    assert revisions == {DS_SHARED: 1}

    with pytest.raises(DatasourceUnavailableError):
        await authorize_datasource_selection(
            db,
            {"id": OWNER},
            OWNER,
            [DS_SHARED],
            [],
            "sandbox",
            legacy_job_id=OTHER_JOB,
        )


@pytest.mark.asyncio
async def test_classify_reports_missing_row_as_deleted():
    db = _db([_row(DS_OWNED)])

    verdicts, _revisions = await classify_datasource_selection(
        db,
        {"id": OWNER, "is_admin": False},
        OWNER,
        [DS_OWNED, DS_SHARED],
        [],
        None,
    )

    assert verdicts == [
        ItemVerdict(DS_OWNED, False, None),
        ItemVerdict(DS_SHARED, True, "deleted"),
    ]


@pytest.mark.asyncio
async def test_classify_reports_unauthorized_row_as_revoked():
    db = _db([_row(DS_SHARED, owner=OTHER)])

    verdicts, _revisions = await classify_datasource_selection(
        db,
        {"id": OWNER, "is_admin": False},
        OWNER,
        [DS_SHARED],
        [],
        None,
    )

    assert verdicts == [ItemVerdict(DS_SHARED, True, "revoked")]


@pytest.mark.asyncio
async def test_classify_reports_scope_mismatch_as_out_of_scope():
    db = _db([_row(DS_OWNED, scope="projects", projects=(PROJECT_B,))])

    verdicts, _revisions = await classify_datasource_selection(
        db,
        {"id": OWNER, "is_admin": False},
        OWNER,
        [DS_OWNED],
        [PROJECT_A],
        None,
    )

    assert verdicts == [ItemVerdict(DS_OWNED, True, "out_of_scope")]


@pytest.mark.asyncio
async def test_deleted_row_still_beats_workspace_tier_error():
    """Precedence guard: a missing id must raise 403-unavailable, never the
    400-tier error from a later id. The pre-refactor code raised on the count
    mismatch before the loop, so this ordering is load-bearing."""
    db = _db([_row(DS_REPOSITORY, ds_type="repository")])

    with pytest.raises(DatasourceUnavailableError):
        await authorize_datasource_selection(
            db,
            {"id": OWNER, "is_admin": False},
            OWNER,
            [DS_SHARED, DS_REPOSITORY],
            [],
            "virtual",
        )


@pytest.mark.asyncio
async def test_authorize_still_raises_generic_error_for_missing_row():
    """The non-enumeration property create/PATCH depend on."""
    db = _db([_row(DS_OWNED)])

    with pytest.raises(DatasourceUnavailableError) as excinfo:
        await authorize_datasource_selection(
            db,
            {"id": OWNER, "is_admin": False},
            OWNER,
            [DS_OWNED, DS_SHARED],
            [],
            None,
        )

    assert str(excinfo.value) == "One or more selected connectors are unavailable"


@pytest.mark.asyncio
async def test_tier_error_wins_when_the_repository_comes_first():
    db = _db(
        [
            _row(DS_REPOSITORY, ds_type="repository"),
            _row(DS_OWNED, scope="projects", projects=(PROJECT_B,)),
        ]
    )

    with pytest.raises(DatasourceWorkspaceTierError):
        await authorize_datasource_selection(
            db,
            {"id": OWNER, "is_admin": False},
            OWNER,
            [DS_REPOSITORY, DS_OWNED],
            [PROJECT_A],
            "virtual",
        )


@pytest.mark.asyncio
async def test_out_of_scope_wins_when_it_comes_first():
    db = _db(
        [
            _row(DS_OWNED, scope="projects", projects=(PROJECT_B,)),
            _row(DS_REPOSITORY, ds_type="repository"),
        ]
    )

    with pytest.raises(DatasourceUnavailableError):
        await authorize_datasource_selection(
            db,
            {"id": OWNER, "is_admin": False},
            OWNER,
            [DS_OWNED, DS_REPOSITORY],
            [PROJECT_A],
            "virtual",
        )


@pytest.mark.asyncio
async def test_deleted_outranks_tier_error_even_when_it_comes_second():
    """The one position-independent case: the pre-refactor len() check ran
    before the loop, so a missing id wins from any position."""
    db = _db([_row(DS_REPOSITORY, ds_type="repository")])

    with pytest.raises(DatasourceUnavailableError):
        await authorize_datasource_selection(
            db,
            {"id": OWNER, "is_admin": False},
            OWNER,
            [DS_REPOSITORY, DS_SHARED],
            [],
            "virtual",
        )


@pytest.mark.asyncio
async def test_corrupt_policy_revision_denies_in_list_order():
    """A corrupt revision is denied like any other in-loop failure, so an
    EARLIER lite-tier repository still wins."""
    db = _db(
        [
            _row(DS_REPOSITORY, ds_type="repository"),
            _row(DS_OWNED, revision=0),
        ]
    )

    with pytest.raises(DatasourceWorkspaceTierError):
        await authorize_datasource_selection(
            db,
            {"id": OWNER, "is_admin": False},
            OWNER,
            [DS_REPOSITORY, DS_OWNED],
            [],
            "virtual",
        )


@pytest.mark.asyncio
async def test_corrupt_policy_revision_alone_is_a_generic_denial():
    db = _db([_row(DS_OWNED, revision=0)])

    with pytest.raises(DatasourceUnavailableError):
        await authorize_datasource_selection(
            db,
            {"id": OWNER, "is_admin": False},
            OWNER,
            [DS_OWNED],
            [],
            None,
        )
