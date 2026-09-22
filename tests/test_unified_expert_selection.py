"""One expert catalogue, one selection parameter.

``list_experts`` returns bundled (disk) and DB experts in one list, and
``get_expert`` inspects either by the same id. Job creation used to take two
mutually-exclusive parameters for that one concept — ``config_name`` for a
bundled slug, ``expert_id`` for a DB UUID — with nothing mapping the
catalogue's ``source`` onto which one to use. The observable cost is in
knowledge-base/knowledge/issues/experts_one_catalogue_two_selection_paths.md: every job an officer
dispatched for three days landed on the application default because the only
call that looked safe was the one that named nobody.

These tests pin the unified seam at all three layers it crosses: the pure
resolver, the shared agent/MCP tool, and the REST funnel that owns the
application-default interaction.
"""

from __future__ import annotations

import json
import uuid
from contextlib import ExitStack
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from shared.expert_reference import (
    BASE_WORKER_CONFIG,
    ExpertReferenceConflict,
    looks_like_expert_uuid,
    resolve_expert_selection,
)
from shared.runtime.core.loader import canonical_config_name
from shared.orch_surface.client import AsyncCockpitClient
from agent.tools.context import ToolContext
from agent.tools.orchestrator import jobs as jobs_module
from agent.tools.orchestrator.jobs import create_orchestrator_tools


DB_EXPERT = "6a3ba4b5-0000-4000-8000-00000000abcd"
OTHER_DB_EXPERT = "11111111-2222-4333-8444-555555555555"


# ─────────────────────────────────────────────────────────────────────────
# The resolver itself: one reference in, one (base, overlay) pair out.
# ─────────────────────────────────────────────────────────────────────────


class TestResolveExpertSelection:
    def test_a_bundled_slug_is_its_own_base_and_takes_no_overlay(self):
        """The mutual-exclusion refusal existed because a DB expert resolves
        *over* a base config. Unifying the parameter means answering what the
        base is when the reference is a slug: the bundled definition IS the
        complete definition (it declares its own ``$extends``), so it becomes
        the base and nothing is layered on top."""
        choice = resolve_expert_selection(expert="developer")

        assert choice.config_name == "developer"
        assert choice.expert_id is None
        assert choice.kind == "bundled"
        assert choice.reference == "developer"

    def test_a_uuid_overlays_the_db_expert_on_the_worker_base(self):
        choice = resolve_expert_selection(expert=DB_EXPERT)

        assert choice.config_name == BASE_WORKER_CONFIG
        assert choice.expert_id == DB_EXPERT
        assert choice.kind == "db"
        assert choice.reference == DB_EXPERT

    def test_naming_nobody_leaves_the_deployment_default_in_charge(self):
        choice = resolve_expert_selection()

        assert choice.config_name == BASE_WORKER_CONFIG
        assert choice.expert_id is None
        assert choice.kind == "default"
        assert choice.reference is None

    def test_blank_and_whitespace_references_mean_nobody_was_named(self):
        assert resolve_expert_selection(expert="   ").kind == "default"
        assert resolve_expert_selection(config_name="", expert_id="").kind == "default"

    # ── deprecated aliases ────────────────────────────────────────────

    def test_deprecated_config_name_still_selects_a_bundled_expert(self):
        choice = resolve_expert_selection(config_name="developer")

        assert (choice.config_name, choice.expert_id) == ("developer", None)
        assert choice.kind == "bundled"

    def test_deprecated_expert_id_still_overlays_the_db_expert(self):
        choice = resolve_expert_selection(expert_id=DB_EXPERT)

        assert (choice.config_name, choice.expert_id) == (
            BASE_WORKER_CONFIG,
            DB_EXPERT,
        )
        assert choice.kind == "db"

    def test_the_legacy_pair_is_still_refused(self):
        """Not deleted, preserved: the two old parameters name two different
        experts, and picking one silently is how the wrong worker ships."""
        with pytest.raises(ExpertReferenceConflict) as excinfo:
            resolve_expert_selection(config_name="developer", expert_id=DB_EXPERT)

        assert "expert_id cannot be combined" in str(excinfo.value)

    def test_the_base_config_is_not_an_explicit_bundled_selection(self):
        """``worker_base`` (and its compatibility aliases) is the *absence* of
        a bundled choice, which is why the old refusal exempted it."""
        for base in ("worker_base", "default", "defaults"):
            choice = resolve_expert_selection(config_name=base, expert_id=DB_EXPERT)
            assert choice.expert_id == DB_EXPERT
            assert choice.config_name == BASE_WORKER_CONFIG

    def test_naming_the_base_through_the_new_parameter_means_the_same_thing(self):
        """``worker_base`` is not a catalogue entry, so `expert="worker_base"`
        has to read as "no choice" rather than as an unknown expert — the same
        string means the same thing in whichever parameter it arrives."""
        assert resolve_expert_selection(expert="worker_base").kind == "default"
        assert (
            resolve_expert_selection(expert="worker_base", expert_id=DB_EXPERT).kind
            == "db"
        )

    def test_the_neutral_base_names_stay_in_sync_with_the_loader(self):
        """Drift guard: the resolver hard-codes the neutral names so it can
        stay stdlib-pure, so assert they still canonicalize to the base."""
        from shared.expert_reference import BASE_WORKER_ALIASES

        for alias in BASE_WORKER_ALIASES:
            assert canonical_config_name(alias) == BASE_WORKER_CONFIG

    # ── precedence between the unified parameter and the aliases ──────

    def test_a_redundant_alias_that_agrees_is_accepted(self):
        assert (
            resolve_expert_selection(
                expert="developer", config_name="developer"
            ).config_name
            == "developer"
        )
        assert (
            resolve_expert_selection(expert=DB_EXPERT, expert_id=DB_EXPERT).expert_id
            == DB_EXPERT
        )
        # The neutral base alongside an explicit reference is not a claim.
        assert (
            resolve_expert_selection(expert=DB_EXPERT, config_name="worker_base").kind
            == "db"
        )
        assert (
            resolve_expert_selection(expert="developer", config_name="worker_base").kind
            == "bundled"
        )

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"expert": "developer", "expert_id": DB_EXPERT},
            {"expert": "developer", "config_name": "critic"},
            {"expert": DB_EXPERT, "config_name": "developer"},
            {"expert": DB_EXPERT, "expert_id": OTHER_DB_EXPERT},
        ],
    )
    def test_a_contradicting_alias_is_refused_never_silently_dropped(self, kwargs):
        """Precedence is "no precedence": two different experts in one call is
        a caller bug, and discarding one of them silently is the failure this
        whole change exists to remove."""
        with pytest.raises(ExpertReferenceConflict):
            resolve_expert_selection(**kwargs)

    def test_uuid_shape_is_what_separates_the_two_stores(self):
        assert looks_like_expert_uuid(DB_EXPERT)
        assert not looks_like_expert_uuid("developer")
        assert not looks_like_expert_uuid(None)


# ─────────────────────────────────────────────────────────────────────────
# The shared job tool (one descriptor → the agent lane AND the MCP server).
# ─────────────────────────────────────────────────────────────────────────


JOB_ID = "19707fa1-0000-4000-8000-000000000009"


class _Recorder:
    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []

    async def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        return httpx.Response(
            201, json={"id": JOB_ID, "status": "created", "description": "x"}
        )


def _install(monkeypatch: pytest.MonkeyPatch) -> tuple[_Recorder, AsyncCockpitClient]:
    recorder = _Recorder()
    client = AsyncCockpitClient(
        base_url="http://orchestrator.test",
        transport=httpx.MockTransport(recorder),
    )
    monkeypatch.setattr(jobs_module, "_get_surface_client", lambda: client)
    return recorder, client


def _create_job_tool() -> object:
    return next(
        tool
        for tool in create_orchestrator_tools(ToolContext())
        if tool.name == "create_job"
    )


async def _create(monkeypatch: pytest.MonkeyPatch, **payload):
    recorder, client = _install(monkeypatch)
    try:
        result = await _create_job_tool().ainvoke({"description": "work", **payload})
    finally:
        await client.close()
    body = json.loads(recorder.requests[-1].content) if recorder.requests else None
    return result, body


class TestCreateJobTakesOneExpertParameter:
    @pytest.mark.asyncio
    async def test_a_bundled_slug_travels_as_the_base_config(self, monkeypatch):
        _, body = await _create(monkeypatch, expert="developer")

        assert body["config_name"] == "developer"
        assert "expert_id" not in body

    @pytest.mark.asyncio
    async def test_a_db_uuid_travels_as_the_expert_overlay(self, monkeypatch):
        _, body = await _create(monkeypatch, expert=DB_EXPERT)

        assert body["config_name"] == BASE_WORKER_CONFIG
        assert body["expert_id"] == DB_EXPERT

    @pytest.mark.asyncio
    async def test_both_deprecated_aliases_still_reach_the_wire(self, monkeypatch):
        _, bundled = await _create(monkeypatch, config_name="critic")
        _, db_backed = await _create(monkeypatch, expert_id=DB_EXPERT)

        assert bundled["config_name"] == "critic"
        assert "expert_id" not in bundled
        assert db_backed["expert_id"] == DB_EXPERT
        assert db_backed["config_name"] == BASE_WORKER_CONFIG

    @pytest.mark.asyncio
    async def test_a_conflicting_pair_never_leaves_the_process(self, monkeypatch):
        recorder, client = _install(monkeypatch)
        try:
            result = await _create_job_tool().ainvoke(
                {
                    "description": "two experts",
                    "expert": "developer",
                    "expert_id": DB_EXPERT,
                }
            )
        finally:
            await client.close()

        assert result.startswith("Refusing to create job")
        assert recorder.requests == []

    @pytest.mark.asyncio
    async def test_the_result_names_the_expert_that_was_selected(self, monkeypatch):
        result, _ = await _create(monkeypatch, expert=DB_EXPERT)

        # "Config: worker_base" alone would hide which expert actually ran.
        assert f"Expert: {DB_EXPERT}" in result

    def test_expert_is_on_the_schema_beside_its_deprecated_aliases(self):
        schema = _create_job_tool().args_schema.model_json_schema()

        assert {"expert", "config_name", "expert_id"} <= set(schema["properties"])

    def test_the_parameter_documentation_points_at_the_catalogue(self):
        """``config_override`` has always said "use the list_models tool";
        worker profiles were the one selectable thing with no such pointer."""
        from shared.orch_surface.jobs import get_descriptor

        description = get_descriptor("create_job").description
        expert_doc = description.split("expert:", 1)[1].split("\n\n", 1)[0]
        assert "list_experts" in expert_doc


# ─────────────────────────────────────────────────────────────────────────
# The REST funnel: an explicit bundled expert vs. the application default.
# ─────────────────────────────────────────────────────────────────────────


USER_ID = str(uuid.uuid4())
PROJECT_ID = str(uuid.uuid4())
CREATED_JOB_ID = str(uuid.uuid4())


@pytest.fixture
def fake_request():
    return SimpleNamespace(headers={}, query_params={})


@pytest.fixture
def db():
    db = MagicMock()
    db.get_user = AsyncMock(return_value={"id": USER_ID, "is_admin": False})
    db.get_project = AsyncMock(return_value={"id": PROJECT_ID})
    db.create_job = AsyncMock(return_value={"id": CREATED_JOB_ID, "status": "created"})
    db.get_datasource = AsyncMock(return_value=None)
    db.link_datasource_to_job = AsyncMock()
    return db


def _application_default():
    from orchestrator.services.default_experts import ExpertSelection

    return ExpertSelection(
        expert={"id": DB_EXPERT, "expert_type": "worker", "owner_id": None},
        source="application",
    )


async def _rest_create(db, fake_request, body, resolver, preview=None):
    from tests._b09_control_seams import create_job

    # The work-default preview dry-runs the snapshot renderer, which lives
    # inside the (mocked) postgres create_job; stand in for its verdict.
    preview = preview or AsyncMock(return_value=[])
    patches = [
        patch("orchestrator.main.postgres_db", db),
        patch(
            "orchestrator.services.job_admission_work_expert.preview_expert_refusals",
            preview,
        ),
        patch(
            "orchestrator.main.require_approved_user",
            AsyncMock(return_value={"id": USER_ID, "is_admin": False}),
        ),
        patch("orchestrator.main.require_project_member", AsyncMock(return_value=None)),
        patch(
            "orchestrator.main._enforce_readiness_gate", AsyncMock(return_value=None)
        ),
        patch(
            "orchestrator.main._require_job_project_access",
            AsyncMock(return_value=None),
        ),
        patch("orchestrator.main._is_experts_db_enabled", MagicMock(return_value=True)),
        patch("orchestrator.main._user_experts_enabled", AsyncMock(return_value=True)),
        patch("orchestrator.main.resolve_root_expert", resolver),
        patch(
            "orchestrator.main._authorize_thread_datasource_selection",
            AsyncMock(side_effect=lambda _actor, ids, **_kw: (list(ids), {})),
        ),
        patch(
            "orchestrator.services.datasource_policy.default_datasource_selection",
            AsyncMock(return_value=([], {})),
        ),
        patch(
            "orchestrator.main._enforce_job_create_grants", AsyncMock(return_value=None)
        ),
        patch("orchestrator.services.job_provisioning.provision_job_repo", AsyncMock()),
        patch(
            "orchestrator.main.subjob_completion_operations.spawn_scholar_subjob",
            AsyncMock(return_value=None),
        ),
        patch("orchestrator.main._trigger_dispatch", MagicMock()),
    ]
    with ExitStack() as stack:
        for item in patches:
            stack.enter_context(item)
        await create_job(fake_request, body)
    return db.create_job.await_args.kwargs


class TestExplicitBundledExpertVersusApplicationDefault:
    """The collision at the heart of the issue.

    ``application_expert_defaults`` injects a DB expert whenever the caller
    names none. Combining that injected expert with a bundled slug is exactly
    what the mutual-exclusion refusal rejects — so before this change the
    officer's only non-refusing call was the one that named nobody, and every
    worker it hired was the default overlay with ``shell: []``.
    """

    @pytest.mark.asyncio
    async def test_naming_nobody_still_gets_the_application_default(
        self, db, fake_request
    ):
        from orchestrator.main import JobCreate

        resolver = AsyncMock(return_value=_application_default())
        kwargs = await _rest_create(
            db,
            fake_request,
            JobCreate(description="unspecified", project_id=PROJECT_ID),
            resolver,
        )

        resolver.assert_awaited_once()
        assert kwargs["config_name"] == BASE_WORKER_CONFIG
        assert kwargs["expert_id"] == DB_EXPERT
        assert kwargs["context"]["expert_selection"]["source"] == "application"

    @pytest.mark.asyncio
    async def test_an_explicit_bundled_expert_suppresses_the_default(
        self, db, fake_request
    ):
        from orchestrator.main import JobCreate

        resolver = AsyncMock(return_value=_application_default())
        kwargs = await _rest_create(
            db,
            fake_request,
            JobCreate(
                description="build it", project_id=PROJECT_ID, expert="developer"
            ),
            resolver,
        )

        resolver.assert_not_awaited()
        assert kwargs["config_name"] == "developer"
        assert kwargs["expert_id"] is None
        assert kwargs["context"]["expert_selection"] == {
            "source": "bundled",
            "expert": "developer",
        }

    @pytest.mark.asyncio
    async def test_an_explicit_db_expert_is_validated_not_defaulted(
        self, db, fake_request
    ):
        from orchestrator.main import JobCreate
        from orchestrator.services.default_experts import ExpertSelection

        resolver = AsyncMock(
            return_value=ExpertSelection(
                expert={"id": DB_EXPERT, "expert_type": "worker", "owner_id": USER_ID},
                source="explicit",
            )
        )
        kwargs = await _rest_create(
            db,
            fake_request,
            JobCreate(description="ask it", project_id=PROJECT_ID, expert=DB_EXPERT),
            resolver,
        )

        assert resolver.await_args.kwargs["explicit_expert_id"] == DB_EXPERT
        assert kwargs["config_name"] == BASE_WORKER_CONFIG
        assert kwargs["expert_id"] == DB_EXPERT
        assert kwargs["context"]["expert_selection"]["source"] == "explicit"

    @pytest.mark.asyncio
    async def test_an_explicit_session_expert_is_accepted_for_a_job(
        self, db, fake_request
    ):
        """Universal experts (U1 D4): an explicit DB expert of the OTHER role
        is a valid selection for a job. The resolver logs instead of refusing
        (tests/test_db_backed_default_experts.py), and the funnel persists it
        as the `worker_base` + overlay pair — `resolve_config` re-roots the
        session fragment onto the worker overlay at dispatch."""
        from orchestrator.main import JobCreate
        from orchestrator.services.default_experts import ExpertSelection

        resolver = AsyncMock(
            return_value=ExpertSelection(
                expert={"id": DB_EXPERT, "expert_type": "session", "owner_id": USER_ID},
                source="explicit",
            )
        )
        kwargs = await _rest_create(
            db,
            fake_request,
            JobCreate(
                description="cross-role", project_id=PROJECT_ID, expert=DB_EXPERT
            ),
            resolver,
        )

        assert resolver.await_args.kwargs["expert_type"] == "worker"
        assert kwargs["config_name"] == BASE_WORKER_CONFIG
        assert kwargs["expert_id"] == DB_EXPERT
        assert kwargs["context"]["expert_selection"]["source"] == "explicit"

    @pytest.mark.asyncio
    async def test_an_unknown_bundled_slug_is_refused_at_creation(
        self, db, fake_request
    ):
        """A typo in ``expert`` used to buy a job that only fails at dispatch."""
        from fastapi import HTTPException
        from orchestrator.main import JobCreate

        resolver = AsyncMock(return_value=_application_default())
        with pytest.raises(HTTPException) as excinfo:
            await _rest_create(
                db,
                fake_request,
                JobCreate(description="typo", project_id=PROJECT_ID, expert="develper"),
                resolver,
            )

        assert excinfo.value.status_code == 400
        assert "list_experts" in str(excinfo.value.detail)

    @pytest.mark.asyncio
    async def test_the_rest_body_refuses_two_experts_too(self, db, fake_request):
        from fastapi import HTTPException
        from orchestrator.main import JobCreate

        resolver = AsyncMock(return_value=_application_default())
        with pytest.raises(HTTPException) as excinfo:
            await _rest_create(
                db,
                fake_request,
                JobCreate(
                    description="two",
                    project_id=PROJECT_ID,
                    expert="developer",
                    expert_id=DB_EXPERT,
                ),
                resolver,
            )

        assert excinfo.value.status_code == 400


class TestWorkCategoryStaffsAnUnnamedWorker:
    """The same funnel, one tier further down the precedence: a job that names
    no expert but carries a work category is staffed by that category's
    default rather than the application default. See
    knowledge-base/knowledge/issues/category_expert_default_skipped_on_direct_dispatch.md."""

    @pytest.mark.asyncio
    async def test_an_executor_reaches_create_job_as_its_default_expert(
        self, db, fake_request
    ):
        from orchestrator.main import JobCreate
        from orchestrator.services.work_categories import default_expert

        resolver = AsyncMock(return_value=_application_default())
        preview = AsyncMock(return_value=[])
        kwargs = await _rest_create(
            db,
            fake_request,
            JobCreate(
                description="build it", project_id=PROJECT_ID, work_category="executor"
            ),
            resolver,
            preview,
        )

        # Bound to the application store and asked about the exact
        # override creation will persist.
        assert preview.await_args.args == (db,)
        assert preview.await_args.kwargs["config_name"] == default_expert("executor")
        assert preview.await_args.kwargs["owner_id"] == USER_ID
        assert kwargs["config_name"] == default_expert("executor")
        assert kwargs["expert_id"] is None
        assert kwargs["context"]["expert_selection"] == {
            "source": "category",
            "category": "executor",
            "expert": default_expert("executor"),
        }

    @pytest.mark.asyncio
    async def test_a_named_expert_still_outranks_the_category(self, db, fake_request):
        from orchestrator.main import JobCreate

        resolver = AsyncMock(return_value=_application_default())
        kwargs = await _rest_create(
            db,
            fake_request,
            JobCreate(
                description="look first",
                project_id=PROJECT_ID,
                expert="scholar",
                work_category="executor",
            ),
            resolver,
        )

        resolver.assert_not_awaited()
        assert (kwargs["config_name"], kwargs["expert_id"]) == ("scholar", None)
        assert kwargs["context"]["expert_selection"]["source"] == "bundled"

    @pytest.mark.asyncio
    async def test_a_refused_category_default_keeps_the_application_default(
        self, db, fake_request
    ):
        from orchestrator.main import JobCreate

        resolver = AsyncMock(return_value=_application_default())
        kwargs = await _rest_create(
            db,
            fake_request,
            JobCreate(
                description="build it", project_id=PROJECT_ID, work_category="executor"
            ),
            resolver,
            AsyncMock(return_value=["shell_tools: tools.shell requires it"]),
        )

        assert (kwargs["config_name"], kwargs["expert_id"]) == (
            BASE_WORKER_CONFIG,
            DB_EXPERT,
        )
        selection = kwargs["context"]["expert_selection"]
        assert selection["source"] == "application"
        assert selection["denied_defaults"][0]["expert"] == "engineer"
