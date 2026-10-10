"""Focused policy tests for project-scoped connector defaults/authorization."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException

from orchestrator.services.datasource_policy import (
    DatasourceExecutionKindError,
    DatasourceUnavailableError,
    DatasourceWorkspaceTierError,
    ItemVerdict,
    authorize_datasource_ids,
    authorize_datasource_selection,
    classify_datasource_selection,
    default_datasource_ids,
    default_datasource_selection,
    execution_kind_refuses,
    workspace_tier_refuses,
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
    managed_key: str | None = None,
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
        "managed_key": managed_key,
    }


def _db(rows: list[dict], *, member: bool = True, roles: dict | None = None):
    by_id = {row["id"]: row for row in rows}
    db = AsyncMock()
    db.get_user = AsyncMock(
        side_effect=lambda user_id: {
            "id": user_id,
            "is_approved": True,
        }
    )
    db.user_is_member_of_projects = AsyncMock(return_value=member)
    # The owner's role per project, for a project cloud folder's ceiling and
    # its reach past its own project.
    db.get_user_project_roles = AsyncMock(
        side_effect=lambda _user, projects: {
            project: role
            for project, role in (roles or {}).items()
            if project in projects
        }
    )

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
@pytest.mark.parametrize("backend", ["none", "virtual"])
@pytest.mark.parametrize(
    "ds_type", ["generic", "ssh_key", "kubeconfig", "generic_file"]
)
async def test_every_driver_needing_a_shell_is_refused_on_a_lite_tier(backend, ds_type):
    """The lite-tier rule is the drivers' supported_backends, not a type list:
    a generic environment used to pass admission and fail at setup."""
    db = _db([_row(DS_OWNED, ds_type=ds_type)])
    with pytest.raises(DatasourceWorkspaceTierError):
        await authorize_datasource_ids(
            db, {"id": OWNER}, OWNER, [DS_OWNED], [], backend
        )


@pytest.mark.parametrize(
    ("ds_type", "backend", "refused"),
    [
        ("repository", "virtual", True),
        ("credentials", "none", True),
        ("generic", "virtual", True),
        ("ssh_key", "none", True),
        # Credential files land in the workspace home (D1d).
        ("kubeconfig", "virtual", True),
        ("generic_file", "none", True),
        ("repository", "VIRTUAL", True),
        # Types match exactly, like the driver registry.
        ("Repository", "virtual", False),
        ("kb", "virtual", False),
        ("postgresql", "none", False),
        ("email", "virtual", False),
        ("unknown-type", "virtual", False),
        ("repository", "sandbox", False),
        ("repository", "vm", False),
        ("repository", "remote", False),
        # The attach-time revalidation passes no backend: never refused.
        ("repository", None, False),
    ],
)
def test_workspace_tier_refuses_follows_supported_backends(ds_type, backend, refused):
    assert workspace_tier_refuses({"type": ds_type}, backend) is refused


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

    assert (
        await default_datasource_ids(db, OWNER, [], "sandbox", execution_kind="session")
        == []
    )


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
        db, OWNER, [PROJECT_A, PROJECT_B], "sandbox", execution_kind="session"
    ) == [DS_NATIVE]


@pytest.mark.asyncio
async def test_native_project_cloud_folder_is_project_managed_default():
    # A key-only row (no config marker): the project's own cloud folder.
    native = _row(
        DS_NATIVE,
        owner=OTHER,
        scope="projects",
        projects=(PROJECT_A,),
        automatic=True,
        ds_type="cloud_folder",
        managed_key=f"project-cloud:{PROJECT_A}",
    )
    db = _db([native])

    assert await default_datasource_ids(
        db, OWNER, [PROJECT_A], "sandbox", execution_kind="session"
    ) == [DS_NATIVE]
    # Another project's work neither defaults to it nor may name it.
    assert (
        await default_datasource_ids(
            db, OWNER, [PROJECT_B], "sandbox", execution_kind="session"
        )
        == []
    )
    with pytest.raises(DatasourceUnavailableError):
        await authorize_datasource_ids(
            db, {"id": OWNER}, OWNER, [DS_NATIVE], [PROJECT_B], "sandbox"
        )
    # A member of its project may name it, though another user created it.
    assert await authorize_datasource_ids(
        db, {"id": OWNER}, OWNER, [DS_NATIVE], [PROJECT_A], "sandbox"
    ) == [DS_NATIVE]


def _user_root(owner: str = OWNER, **over) -> dict:
    return _row(
        DS_NATIVE,
        owner=owner,
        automatic=True,
        ds_type="cloud_folder",
        managed_key=f"user-cloud-root:{owner}",
        **over,
    )


@pytest.mark.asyncio
async def test_a_users_own_connector_is_its_users_alone():
    db = _db([_user_root(public=True, scope="projects", projects=(PROJECT_A,))])

    assert await authorize_datasource_ids(
        db, {"id": OWNER}, OWNER, [DS_NATIVE], [PROJECT_A], "sandbox"
    ) == [DS_NATIVE]
    # Not for an administrator's own work, not through a project link or
    # trusted inheritance, and not for ownerless work.
    for kwargs in (
        {"allow_admin_explicit_override": True},
        {"trusted_system_inheritance": True},
    ):
        verdicts, _ = await classify_datasource_selection(
            db,
            {"id": OTHER, "is_admin": True},
            OTHER,
            [DS_NATIVE],
            [PROJECT_A],
            "sandbox",
            **kwargs,
        )
        assert verdicts == [ItemVerdict(DS_NATIVE, True, "revoked")]
    verdicts, _ = await classify_datasource_selection(
        db,
        None,
        None,
        [DS_NATIVE],
        [PROJECT_A],
        "sandbox",
        trusted_system_inheritance=True,
    )
    assert verdicts == [ItemVerdict(DS_NATIVE, True, "revoked")]


@pytest.mark.asyncio
async def test_a_users_own_connector_defaults_only_for_its_user():
    # Even a project's chosen default (were it linked) never reaches another
    # user's work, and a key naming one user on another's row reaches nobody.
    root = _user_root(scope="projects", projects=(PROJECT_A,))
    root["project_default"] = True
    db = _db([root])
    assert await default_datasource_ids(
        db, OWNER, [PROJECT_A], "sandbox", execution_kind="session"
    ) == [DS_NATIVE]
    assert (
        await default_datasource_ids(
            db, OTHER, [PROJECT_A], "sandbox", execution_kind="session"
        )
        == []
    )
    mismatched = _user_root()
    mismatched["created_by"] = OTHER
    assert (
        await default_datasource_ids(
            _db([mismatched]), OTHER, [], "sandbox", execution_kind="session"
        )
        == []
    )


@pytest.mark.asyncio
async def test_native_only_keeps_the_works_own_and_jobs_never_get_a_folder():
    kb = _row(
        DS_NATIVE,
        owner=None,
        scope="projects",
        projects=(PROJECT_A,),
        automatic=True,
        ds_type="kb",
        managed_key=f"project-kb:{PROJECT_A}",
    )
    folder = _row(
        DS_SHARED,
        owner=OTHER,
        scope="projects",
        projects=(PROJECT_A,),
        automatic=True,
        ds_type="cloud_folder",
        managed_key=f"project-cloud:{PROJECT_A}",
    )
    other_folder = _row(
        DS_REPOSITORY,
        owner=OTHER,
        scope="projects",
        projects=(PROJECT_B,),
        automatic=True,
        ds_type="cloud_folder",
        managed_key=f"project-cloud:{PROJECT_B}",
    )
    owned = _row(DS_OWNED, automatic=True)
    db = _db([kb, folder, other_folder, owned])

    async def defaults(kind, *, native_only):
        return set(
            await default_datasource_ids(
                db,
                OWNER,
                [PROJECT_A],
                "sandbox",
                execution_kind=kind,
                native_only=native_only,
            )
        )

    assert await defaults("session", native_only=True) == {DS_NATIVE, DS_SHARED}
    assert await defaults("session", native_only=False) == {
        DS_NATIVE,
        DS_SHARED,
        DS_OWNED,
    }
    assert await defaults("job", native_only=True) == {DS_NATIVE}
    assert await defaults("job", native_only=False) == {DS_NATIVE, DS_OWNED}


@pytest.mark.asyncio
async def test_an_explicit_job_naming_a_cloud_folder_is_told_why():
    folder = _row(
        DS_NATIVE,
        owner=OTHER,
        scope="projects",
        projects=(PROJECT_A,),
        automatic=True,
        ds_type="cloud_folder",
        managed_key=f"project-cloud:{PROJECT_A}",
    )
    db = _db([folder, _row(DS_OWNED)])

    for kind in (None, "session"):
        selected, _ = await authorize_datasource_selection(
            db,
            {"id": OWNER},
            OWNER,
            [DS_OWNED, DS_NATIVE],
            [PROJECT_A],
            "sandbox",
            execution_kind=kind,
        )
        assert selected == [DS_OWNED, DS_NATIVE]
    with pytest.raises(DatasourceExecutionKindError) as refused:
        await authorize_datasource_selection(
            db,
            {"id": OWNER},
            OWNER,
            [DS_OWNED, DS_NATIVE],
            [PROJECT_A],
            "sandbox",
            execution_kind="job",
        )
    assert str(refused.value) == (
        "Cloud folder connectors attach to sessions only; a job cannot attach one"
    )
    verdicts, _ = await classify_datasource_selection(
        db,
        {"id": OWNER},
        OWNER,
        [DS_NATIVE],
        [PROJECT_A],
        "sandbox",
        execution_kind="job",
    )
    assert [(v.denied, v.reason) for v in verdicts] == [(True, "execution_kind")]
    # Authority first: another project's folder stays the generic refusal,
    # so the reason never tells a caller what it may not see.
    with pytest.raises(DatasourceUnavailableError):
        await authorize_datasource_selection(
            db,
            {"id": OWNER},
            OWNER,
            [DS_NATIVE],
            [PROJECT_B],
            "sandbox",
            execution_kind="job",
        )


@pytest.mark.asyncio
async def test_the_thread_funnel_answers_400_with_the_reason():
    folder = _row(
        DS_NATIVE,
        scope="projects",
        projects=(PROJECT_A,),
        ds_type="cloud_folder",
        managed_key=f"project-cloud:{PROJECT_A}",
    )
    dependencies = thread_datasource_authorization_module.ThreadDatasourceAuthorizationDependencies(
        store=_db([folder]), thread_project_ids=AsyncMock(return_value=[])
    )
    with pytest.raises(HTTPException) as refused:
        await thread_datasource_authorization_module.authorize_thread_datasource_selection(
            {"id": OWNER},
            [DS_NATIVE],
            workspace_backend="sandbox",
            target_project_ids=[PROJECT_A],
            execution_kind="job",
            dependencies=dependencies,
        )
    assert refused.value.status_code == 400
    assert "sessions only" in refused.value.detail


@pytest.mark.asyncio
async def test_an_inherited_selection_keeps_what_the_job_can_attach():
    folder = _row(DS_NATIVE, ds_type="cloud_folder")
    db = _db([folder, _row(DS_OWNED)])
    dependencies = job_datasource_selection_module.JobDatasourceSelectionDependencies(
        store=db,
        authorize_thread_datasource_selection=AsyncMock(),
        backend_from_override=lambda _override: None,
        revalidate_selection=AsyncMock(),
    )
    assert await job_datasource_selection_module.filter_execution_kind_datasource_ids(
        [DS_NATIVE, DS_OWNED, DS_SHARED], "job", dependencies=dependencies
    ) == [DS_OWNED, DS_SHARED]
    assert await job_datasource_selection_module.filter_execution_kind_datasource_ids(
        [DS_NATIVE, DS_OWNED], "session", dependencies=dependencies
    ) == [DS_NATIVE, DS_OWNED]


def test_the_driver_names_the_executions_a_connector_serves():
    folder = _row(DS_NATIVE, ds_type="cloud_folder")
    assert execution_kind_refuses(folder, "job")
    assert not execution_kind_refuses(folder, "session")
    assert not execution_kind_refuses(folder, None)
    assert not execution_kind_refuses(_row(DS_OWNED), "job")
    # A type no driver serves refuses nothing.
    assert not execution_kind_refuses(_row(DS_OWNED, ds_type="unknown"), "job")


@pytest.mark.asyncio
async def test_lite_tier_filters_implicit_repository_but_rejects_explicit_one():
    repository = _row(
        DS_REPOSITORY,
        automatic=True,
        ds_type="repository",
    )
    db = _db([repository])

    assert (
        await default_datasource_ids(db, OWNER, [], "virtual", execution_kind="session")
        == []
    )
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
        execution_kind="session",
    )

    assert selected == [DS_OWNED]
    assert revisions == {DS_OWNED: 7}
    db.get_datasource_policy_rows.assert_not_awaited()


@pytest.mark.asyncio
async def test_unattended_work_leaves_the_owners_outbox_out():
    outbox = _row(
        DS_NATIVE,
        automatic=True,
        ds_type="cloud_outbox",
        managed_key=f"cloud-outbox:{OWNER}",
    )
    db = _db([outbox, _row(DS_OWNED, automatic=True)])

    attended, _ = await default_datasource_selection(
        db, OWNER, [], "sandbox", execution_kind="job"
    )
    unattended, revisions = await default_datasource_selection(
        db, OWNER, [], "sandbox", execution_kind="job", unattended=True
    )

    assert attended == [DS_NATIVE, DS_OWNED]
    assert unattended == [DS_OWNED]
    assert revisions == {DS_OWNED: 1}


@pytest.mark.asyncio
async def test_named_owner_membership_loss_fails_closed():
    db = _db([_row(DS_OWNED, automatic=True)], member=False)

    with pytest.raises(DatasourceUnavailableError):
        await default_datasource_ids(
            db, OWNER, [PROJECT_A], "sandbox", execution_kind="session"
        )


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

    assert await default_datasource_ids(
        db, OWNER, [PROJECT_A], "sandbox", execution_kind="session"
    ) == [DS_OWNED]


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
