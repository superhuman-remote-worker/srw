"""``execution.connectors`` on job and session creation (slice D3c).

The request shape, the 400 for a second selector, the edge resolver (names in
a scope, ``me``, the execution's own scope, uids, the row fallback) and its
one non-enumerating refusal, the funnels taking the resolved ids down the
explicit ``datasource_ids`` branch, the project-defaults source of
``default_datasource_selection`` and the Project manifest's link entries.
The same bindings both ways, on a real database, are in
``tests/test_project_connectors_real_postgres.py``.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID

from fastapi import HTTPException
from pydantic import ValidationError
import pytest

from orchestrator.schemas.execution_selection import (
    INLINE_CONNECTOR_REFUSED,
    ExecutionSelection,
)
from orchestrator.schemas.job_create import JobCreate, public_job_create_schema
from orchestrator.schemas.thread_admission import ThreadCreateRequest
from orchestrator.services import connector_refs
from orchestrator.services.connector_refs import (
    refuse_connector_selector_conflict,
    resolve_connector_refs,
    resolve_execution_connectors,
)
from orchestrator.services.datasource_policy import (
    GENERIC_UNAVAILABLE_DETAIL,
    default_datasource_selection,
)
from orchestrator.services.job_admission import (
    JobAdmissionDependencies,
    admit_job,
)
from orchestrator.services.job_admission_datasources import (
    JobAdmissionDatasourcesDependencies,
    prepare_job_admission_datasources,
)
from orchestrator.services.job_admission_scope import JobAdmissionActor
from orchestrator.services.project_connectors import legacy_alias, link_entries
from orchestrator.services.thread_admission import (
    resolve_thread_creation_plan,
    select_thread_datasources,
)

OWNER = "11111111-1111-4111-8111-111111111111"
PROJECT = "22222222-2222-4222-8222-222222222222"
OTHER_ACCOUNT = "33333333-3333-4333-8333-333333333333"
CONNECTOR = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
SECOND = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
KB = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"


def _execution(**connectors):
    return ExecutionSelection.model_validate({"connectors": connectors})


# =============================================================================
# The request shape
# =============================================================================


@pytest.mark.parametrize(
    "selection",
    [
        {"ref": {"name": "prod-db-0123456789ab"}},
        {
            "ref": {
                "name": "prod-db-0123456789ab",
                "scope": {"kind": "Account", "name": "me"},
            }
        },
        {
            "ref": {
                "name": "kb-0123456789ab",
                "scope": {"kind": "Project", "name": PROJECT},
            }
        },
        {"ref": {"uid": CONNECTOR}},
    ],
)
def test_a_ref_by_name_in_a_scope_or_by_uid_is_accepted(selection):
    execution = _execution(db=selection)
    assert connector_refs.connector_refs(execution)["db"] == {
        key: value for key, value in selection["ref"].items()
    }


@pytest.mark.parametrize(
    ("connectors", "message"),
    [
        ({"db": {"inline": {"driver": "srw.postgresql/v1"}}}, INLINE_CONNECTOR_REFUSED),
        ({"db": {}}, "needs a ref"),
        ({"db": {"ref": {}}}, "exactly one of name or uid"),
        (
            {"db": {"ref": {"name": "a", "uid": CONNECTOR}}},
            "exactly one of name or uid",
        ),
        (
            {
                "db": {
                    "ref": {
                        "uid": CONNECTOR,
                        "scope": {"kind": "Account", "name": "me"},
                    }
                }
            },
            "uid ref has no scope",
        ),
        ({"db": {"ref": {"name": "a", "revision": "sha256:1"}}}, "Extra inputs"),
        ({"Not An Alias": {"ref": {"name": "a"}}}, "pattern"),
        ({"db": {"ref": {"name": "Upper"}}}, "pattern"),
        ({"db": {"ref": {"uid": "not-a-uuid"}}}, "uuid"),
        (
            {"db": {"ref": {"name": "a", "scope": {"kind": "Team", "name": "x"}}}},
            "Account",
        ),
    ],
)
def test_a_malformed_selection_is_refused_by_shape(connectors, message):
    with pytest.raises(ValidationError, match=message):
        _execution(**connectors)


def test_execution_carries_connectors_only():
    with pytest.raises(ValidationError, match="Extra inputs"):
        ExecutionSelection.model_validate(
            {"connectors": {}, "expert": {"ref": {"name": "developer"}}}
        )
    with pytest.raises(ValidationError, match="connectors"):
        ExecutionSelection.model_validate({})


@pytest.mark.parametrize("model", [JobCreate, ThreadCreateRequest])
def test_both_creation_bodies_accept_execution(model):
    fields = {"description": "x"} if model is JobCreate else {}
    body = model(
        **fields, execution={"connectors": {"db": {"ref": {"uid": CONNECTOR}}}}
    )
    assert "datasource_ids" not in body.model_fields_set
    assert connector_refs.connector_refs(body.execution) == {"db": {"uid": CONNECTOR}}


def test_the_public_job_schema_publishes_execution():
    assert "execution" in public_job_create_schema()["properties"]


# =============================================================================
# A second selector is a 400
# =============================================================================


@pytest.mark.parametrize(
    ("fields", "other"),
    [
        ({"datasource_ids": []}, "datasource_ids"),
        ({"datasource_ids": [CONNECTOR]}, "datasource_ids"),
        ({"use_datasource_defaults": True}, "use_datasource_defaults"),
    ],
)
@pytest.mark.parametrize("model", [JobCreate, ThreadCreateRequest])
def test_refs_with_another_selector_are_a_400(model, fields, other):
    base = {"description": "x"} if model is JobCreate else {}
    body = model(**base, **fields, execution={"connectors": {}})
    with pytest.raises(HTTPException) as refused:
        refuse_connector_selector_conflict(body)
    assert refused.value.status_code == 400
    assert other in refused.value.detail


@pytest.mark.parametrize("model", [JobCreate, ThreadCreateRequest])
def test_one_selector_is_never_a_conflict(model):
    base = {"description": "x"} if model is JobCreate else {}
    for fields in (
        {},
        {"datasource_ids": [CONNECTOR]},
        {"use_datasource_defaults": True},
        {"execution": {"connectors": {}}},
        {"execution": None, "datasource_ids": []},
    ):
        refuse_connector_selector_conflict(model(**base, **fields))


@pytest.mark.asyncio
async def test_job_admission_refuses_both_selectors_before_any_other_stage():
    dependencies = JobAdmissionDependencies(
        validate_tool_overrides=lambda value: value,
        enforce_readiness=AsyncMock(),
        scope=Mock(),
        config=Mock(),
        officer=Mock(),
        workspace=Mock(),
        datasources=Mock(),
        delivery=Mock(),
        creation=Mock(),
        redact_result=Mock(),
    )
    with pytest.raises(HTTPException) as refused:
        await admit_job(
            command=JobCreate(
                description="x",
                datasource_ids=[CONNECTOR],
                execution={"connectors": {"db": {"ref": {"uid": CONNECTOR}}}},
            ),
            actor=JobAdmissionActor(principal={"id": OWNER}),
            origin="user_rest",
            dependencies=dependencies,
        )
    assert refused.value.status_code == 400
    dependencies.enforce_readiness.assert_not_awaited()
    dependencies.scope.assert_not_called()


@pytest.mark.asyncio
async def test_session_admission_refuses_both_selectors_before_any_other_stage():
    dependencies = SimpleNamespace(authorize_thread_project_ids=AsyncMock())
    with pytest.raises(HTTPException) as refused:
        await resolve_thread_creation_plan(
            ThreadCreateRequest(
                use_datasource_defaults=True,
                execution={"connectors": {"db": {"ref": {"uid": CONNECTOR}}}},
            ),
            {"id": OWNER},
            dependencies=dependencies,
        )
    assert refused.value.status_code == 400
    dependencies.authorize_thread_project_ids.assert_not_awaited()


# =============================================================================
# The resolver
# =============================================================================


class FakeStore:
    """``ManifestStore.by_name`` and ``by_id`` over a few live resources."""

    def __init__(self, *resources):
        self.resources = list(resources)
        self.lookups = []

    def __call__(self, _db):
        return self

    async def by_name(self, kind, scope, name, *, revision=None):
        self.lookups.append((kind, dict(scope), name))
        return next(
            (
                row
                for row in self.resources
                if (row["kind"], row["scope_kind"], row["scope_name"], row["name"])
                == (kind, scope["kind"], scope["name"], name)
            ),
            None,
        )

    async def by_id(self, uid):
        return next((row for row in self.resources if str(row["id"]) == str(uid)), None)


def _connector(
    uid, name, kind="Account", scope=OWNER, *, linked=True, resource="Connector"
):
    return {
        "id": UUID(uid),
        "kind": resource,
        "scope_kind": kind,
        "scope_name": scope,
        "name": name,
        "linked_id": UUID(uid) if linked else None,
    }


@pytest.fixture
def store(monkeypatch):
    fake = FakeStore(
        _connector(CONNECTOR, "prod-db-aaaaaaaaaaaa"),
        _connector(SECOND, "reports-bbbbbbbbbbbb", scope=OTHER_ACCOUNT),
        _connector(KB, "knowledge-cccccccccccc", "Project", PROJECT),
    )
    monkeypatch.setattr(connector_refs, "ManifestStore", fake)
    return fake


async def _resolve(refs, *, owner=OWNER, project=PROJECT):
    return await resolve_connector_refs(
        object(), refs, owner_id=owner, project_id=project
    )


@pytest.mark.asyncio
async def test_names_resolve_in_their_scope_in_alias_order(store):
    refs = {
        "kb": {"name": "knowledge-cccccccccccc"},
        "mine": {
            "name": "prod-db-aaaaaaaaaaaa",
            "scope": {"kind": "Account", "name": "me"},
        },
        "theirs": {
            "name": "reports-bbbbbbbbbbbb",
            "scope": {"kind": "Account", "name": OTHER_ACCOUNT},
        },
        "again": {"uid": CONNECTOR},
    }
    # Duplicates stay: the policy collapses them as it does datasource_ids.
    assert await _resolve(refs) == [KB, CONNECTOR, SECOND, CONNECTOR]
    assert store.lookups[0][1] == {"kind": "Project", "name": PROJECT}
    assert store.lookups[1][1] == {"kind": "Account", "name": OWNER}


@pytest.mark.asyncio
async def test_an_omitted_scope_is_the_account_without_a_project(store):
    assert await _resolve({"db": {"name": "prod-db-aaaaaaaaaaaa"}}, project=None) == [
        CONNECTOR
    ]
    with pytest.raises(HTTPException) as refused:
        await _resolve({"db": {"name": "prod-db-aaaaaaaaaaaa"}})
    assert refused.value.status_code == 403


@pytest.mark.asyncio
async def test_a_uid_with_no_resource_is_the_row_itself(store):
    """A row on the legacy path has no resource yet: its id still names it,
    exactly as in datasource_ids, and the policy decides."""
    unknown = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
    assert await _resolve({"db": {"uid": unknown.upper()}}) == [unknown]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "ref",
    [
        {"name": "absent-0123456789ab"},
        {"name": "prod-db-aaaaaaaaaaaa", "scope": {"kind": "Project", "name": PROJECT}},
        {
            "name": "prod-db-aaaaaaaaaaaa",
            "scope": {"kind": "Catalog", "name": "shared"},
        },
        {"name": "unlinked-eeeeeeeeeeee"},
        {"uid": "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"},
        {"uid": "ffffffff-ffff-4fff-8fff-ffffffffffff"},
    ],
)
async def test_what_resolves_to_no_connector_is_the_policy_refusal(store, ref):
    store.resources += [
        # A Connector authored in a manifest: no datasource row behind it.
        _connector(
            "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
            "unlinked-eeeeeeeeeeee",
            "Project",
            PROJECT,
            linked=False,
        ),
        # Another kind's resource.
        _connector("ffffffff-ffff-4fff-8fff-ffffffffffff", "team", resource="Project"),
    ]
    with pytest.raises(HTTPException) as refused:
        await _resolve({"db": ref})
    assert (refused.value.status_code, refused.value.detail) == (
        403,
        GENERIC_UNAVAILABLE_DETAIL,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("scope", "message"),
    [
        (
            {"kind": "Account", "name": "someone"},
            "Account scope is named by its UUID or 'me'",
        ),
        ({"kind": "Project", "name": "team"}, "Project scope is named by its UUID"),
    ],
)
async def test_a_live_scope_is_named_by_uuid(store, scope, message):
    with pytest.raises(HTTPException, match=message) as refused:
        await _resolve({"db": {"name": "prod-db-aaaaaaaaaaaa", "scope": scope}})
    assert refused.value.status_code == 422


@pytest.mark.asyncio
async def test_me_without_an_owner_resolves_to_nothing(store):
    with pytest.raises(HTTPException) as refused:
        await _resolve(
            {
                "db": {
                    "name": "prod-db-aaaaaaaaaaaa",
                    "scope": {"kind": "Account", "name": "me"},
                }
            },
            owner=None,
        )
    assert refused.value.status_code == 403


@pytest.mark.asyncio
async def test_resolve_execution_connectors_reads_the_request_block(store):
    assert await resolve_execution_connectors(
        object(),
        _execution(db={"ref": {"uid": CONNECTOR}}),
        owner_id=OWNER,
        project_id=None,
    ) == [CONNECTOR]


# =============================================================================
# The funnels take the resolved ids down the explicit branch
# =============================================================================


@pytest.fixture
def job_dependencies():
    async def authorize(_actor, ids, **_kwargs):
        return ids, {value: 3 for value in ids}

    async def provenance(**values):
        return values

    return JobAdmissionDatasourcesDependencies(
        backend_from_override=Mock(return_value="sandbox"),
        inherit_parent_ids=AsyncMock(return_value=[SECOND]),
        filter_implicit_lite_ids=AsyncMock(return_value=[SECOND]),
        authorize_selection=AsyncMock(side_effect=authorize),
        default_selection=AsyncMock(return_value=([SECOND], {SECOND: 1})),
        defaults_on_omission=Mock(return_value=True),
        selection_provenance=AsyncMock(side_effect=provenance),
        resolve_connector_refs=AsyncMock(return_value=[CONNECTOR, KB]),
    )


async def _prepare_job(dependencies, **fields):
    return await prepare_job_admission_datasources(
        command=JobCreate(description="x", **fields),
        config_override={"workspace": {"backend": "sandbox"}},
        selection_actor={"id": OWNER},
        effective_user_id=OWNER,
        project_id=PROJECT,
        internal_call=False,
        internal_origin_bound=False,
        dependencies=dependencies,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("thread", [None, "44444444-4444-4444-8444-444444444444"])
async def test_job_refs_are_an_explicit_selection(job_dependencies, thread):
    result = await _prepare_job(
        job_dependencies,
        thread_id=thread,
        execution={"connectors": {"db": {"ref": {"uid": CONNECTOR}}}},
    )
    execution = job_dependencies.resolve_connector_refs.await_args
    assert execution.kwargs == {"owner_id": OWNER, "project_id": PROJECT}
    by_ids = await _prepare_job(
        job_dependencies, thread_id=thread, datasource_ids=[CONNECTOR, KB]
    )
    assert result == by_ids
    assert result.datasource_ids == [CONNECTOR, KB]
    assert result.provenance["origin"] == "explicit"
    for call in job_dependencies.authorize_selection.await_args_list:
        assert call.args[1] == [CONNECTOR, KB]
        assert call.kwargs["trusted_system_inheritance"] is False
    job_dependencies.inherit_parent_ids.assert_not_awaited()
    job_dependencies.default_selection.assert_not_awaited()


@pytest.mark.asyncio
async def test_job_refs_without_a_resolver_are_unavailable(job_dependencies):
    from dataclasses import replace

    with pytest.raises(HTTPException) as refused:
        await _prepare_job(
            replace(job_dependencies, resolve_connector_refs=None),
            execution={"connectors": {}},
        )
    assert refused.value.status_code == 503


@pytest.mark.asyncio
async def test_session_refs_are_an_explicit_selection(monkeypatch):
    resolver = AsyncMock(return_value=[CONNECTOR, KB])
    monkeypatch.setattr(
        "orchestrator.services.thread_admission.resolve_execution_connectors", resolver
    )

    async def authorize(_user, ids, **_kwargs):
        return ids, {value: 3 for value in ids}

    dependencies = SimpleNamespace(
        store=object(),
        authorize_thread_datasource_selection=AsyncMock(side_effect=authorize),
        datasource_defaults_on_omission=lambda: True,
    )
    ids, revisions, provenance = await select_thread_datasources(
        ThreadCreateRequest(
            execution={"connectors": {"db": {"ref": {"uid": CONNECTOR}}}}
        ),
        {"id": OWNER},
        thread_backend="sandbox",
        effective_project_ids=[PROJECT],
        dependencies=dependencies,
    )
    assert (ids, revisions) == ([CONNECTOR, KB], {CONNECTOR: 3, KB: 3})
    assert provenance["origin"] == "explicit"
    assert resolver.await_args.kwargs == {"owner_id": OWNER, "project_id": PROJECT}
    assert dependencies.authorize_thread_datasource_selection.await_args.args[1] == [
        CONNECTOR,
        KB,
    ]


# =============================================================================
# Project connector defaults in creation-time defaults
# =============================================================================


def _row(datasource_id, *, owner=OWNER, projects=(PROJECT,), automatic=False, **extra):
    return {
        "id": datasource_id,
        "type": "postgresql",
        "created_by": owner,
        "is_global": False,
        "scope_mode": "projects",
        "auto_attach": automatic,
        "policy_revision": 1,
        "project_ids": list(projects),
        "config": {},
        **extra,
    }


def _policy_db(rows):
    db = AsyncMock()
    db.get_user = AsyncMock(
        side_effect=lambda user_id: {"id": user_id, "is_approved": True}
    )
    db.user_is_member_of_projects = AsyncMock(return_value=True)
    db.list_default_datasource_candidates = AsyncMock(return_value=rows)
    return db


@pytest.mark.asyncio
async def test_a_project_default_attaches_for_every_member_while_linked():
    member = "55555555-5555-4555-8555-555555555555"
    rows = [
        _row(CONNECTOR, project_default=True),
        _row(SECOND),  # linked, but neither a default nor the member's own
        _row(KB, owner=member, projects=(), automatic=True, scope_mode="all"),
    ]
    selected, revisions = await default_datasource_selection(
        _policy_db(rows), member, [PROJECT], "sandbox"
    )
    assert selected == [CONNECTOR, KB]
    assert revisions == {CONNECTOR: 1, KB: 1}


@pytest.mark.asyncio
async def test_a_project_default_needs_its_link_to_every_target():
    rows = [
        _row(
            CONNECTOR,
            projects=("66666666-6666-4666-8666-666666666666",),
            project_default=True,
        )
    ]
    selected, _ = await default_datasource_selection(
        _policy_db(rows), OWNER, [PROJECT], "sandbox"
    )
    assert selected == []
    selected, _ = await default_datasource_selection(
        _policy_db([_row(CONNECTOR, project_default=True)]), OWNER, [], "sandbox"
    )
    assert selected == []


@pytest.mark.asyncio
async def test_a_project_default_a_lite_tier_cannot_serve_is_left_out():
    rows = [_row(CONNECTOR, project_default=True, type="repository")]
    selected, _ = await default_datasource_selection(
        _policy_db(rows), OWNER, [PROJECT], "virtual"
    )
    assert selected == []


# =============================================================================
# A Project manifest's link entries
# =============================================================================


def test_links_are_refs_with_the_row_fallback():
    rows = [
        {
            "id": UUID(CONNECTOR),
            "policy_revision": 4,
            "resource_name": "prod-db-aaaaaaaaaaaa",
            "scope_kind": "Account",
            "scope_name": OWNER,
        },
        {"id": UUID(SECOND), "policy_revision": 2, "resource_name": None},
    ]
    entries = link_entries(rows)
    assert entries == {
        "prod-db-aaaaaaaaaaaa": (
            CONNECTOR,
            {
                "ref": {
                    "name": "prod-db-aaaaaaaaaaaa",
                    "scope": {"kind": "Account", "name": OWNER},
                }
            },
        ),
        legacy_alias(SECOND): (
            SECOND,
            {
                "inline": {
                    "driver": "srw.datasource/v1",
                    "config": {"datasourceId": SECOND, "policyRevision": 2},
                }
            },
        ),
    }


def test_two_links_with_one_resource_name_get_distinct_aliases():
    rows = [
        {
            "id": UUID(value),
            "policy_revision": 1,
            "resource_name": "prod-0123456789ab",
            "scope_kind": "Account",
            "scope_name": scope,
        }
        for value, scope in ((CONNECTOR, OWNER), (SECOND, OTHER_ACCOUNT))
    ]
    assert list(link_entries(rows)) == [
        "prod-0123456789ab",
        "connector-" + SECOND.replace("-", ""),
    ]
