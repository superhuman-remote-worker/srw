"""Every approved user's default project gets a knowledge vault.

A bare ``kb_write`` lands in the default project, so one without a vault
refuses every note (``no-repo``). Before this, only admin ``create_user``
provisioned one; first OIDC login, the legacy-role write-through and app-side
approval did not, so on a fresh install even the bootstrap admin could not
write a note. Pinned here:

* every admission path schedules the vault, best-effort — a failing or unbound
  provisioner never fails the login — and the Gitea account comes first, so
  the vault's owner grant finds it;
* the ensure is a no-op for a project that has a vault, and skips a project
  whose index still holds active pathless rows (a new vault's first reindex
  would archive them);
* the startup backfill provisions the eligible projects once, and its loop
  parks instead of returning, so ``run_when_leader`` never respawns it;
* a retry after an unfinished creation resumes its creation intent instead of
  colliding with it.

The SQL itself (candidates, the pathless-row predicate, the resume against real
intents) is proven in ``test_default_project_knowledge_real_postgres.py``.
"""

from __future__ import annotations

import asyncio
import functools
import logging
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from orchestrator.security import auth
from orchestrator.services import (
    access_tokens,
    application_tasks,
    default_project_knowledge as dpk,
    leader_election,
    project_provisioning,
    user_administration,
)

USER_ID = str(uuid4())
PROJECT_ID = str(uuid4())


def _user(**over: Any) -> dict[str, Any]:
    row = {
        "id": USER_ID,
        "email": "new@example.test",
        "display_name": "New",
        "preferred_username": "new",
        "keycloak_sub": "kc-new",
        "default_project_id": PROJECT_ID,
        "is_approved": True,
        "is_admin": False,
    }
    row.update(over)
    return row


class _Store:
    """The app store as the ensure and the backfill reach it."""

    def __init__(
        self,
        *,
        repos: dict[str, dict[str, list]] | None = None,
        candidates: list[dict[str, Any]] | None = None,
    ) -> None:
        self.repos = repos or {}
        self.candidates = candidates or []
        self.fetch_args: list[tuple] = []

    async def get_project_repositories(self, project_id, role=None):
        return list(self.repos.get(str(project_id), {}).get(role, []))

    async def get_project(self, project_id):
        return {"id": project_id, "name": f"P {project_id[:4]}", "is_default": True}

    async def fetch(self, query, *args):
        self.fetch_args.append(args)
        return list(self.candidates)


class _Vector:
    def __init__(self, pathless: set[str] | None = None) -> None:
        self.pathless = pathless or set()
        self.queries: list[str] = []

    async def fetchval(self, query, project_id):
        self.queries.append(query)
        return str(project_id) in self.pathless


def _deps(
    *,
    store: Any = None,
    vector: Any = None,
    forge: Any = None,
) -> project_provisioning.ProjectProvisioningDependencies:
    return project_provisioning.ProjectProvisioningDependencies(
        store=store if store is not None else _Store(),
        forge=forge
        if forge is not None
        else SimpleNamespace(is_initialized=True, is_configured=True),
        keycloak_groups=SimpleNamespace(is_initialized=False),
        main_cloud_router=SimpleNamespace(),
        logger=MagicMock(),
        repair=project_provisioning.ProjectRepairState(),
        knowledge_index=SimpleNamespace(
            vector_db=vector if vector is not None else _Vector()
        ),
    )


@pytest.fixture
def provision(monkeypatch):
    """The vault creation itself, recorded (its own tests are elsewhere)."""
    calls: list[tuple[dict, str | None]] = []

    async def _provision(project, owner_id, *, dependencies):
        calls.append((project, owner_id))
        return {"id": "datasource"}

    monkeypatch.setattr(dpk, "provision_project_knowledge_repo", _provision)
    return calls


# =============================================================================
# The ensure
# =============================================================================


class TestEnsure:
    @pytest.mark.asyncio
    async def test_a_vault_less_default_project_gets_one_owned_by_its_user(
        self, provision
    ):
        outcome = await dpk.ensure_default_project_knowledge(
            _user(), dependencies=_deps()
        )
        assert outcome == "provisioned"
        assert [(p["id"], owner) for p, owner in provision] == [(PROJECT_ID, USER_ID)]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("role", ["knowledge", "jobs"])
    async def test_an_existing_vault_is_left_alone(self, provision, role):
        # A legacy jobs-role vault counts: a new knowledge repo would take
        # precedence in resolve_kb_repo and hide every note in it.
        store = _Store(repos={PROJECT_ID: {role: [{"id": "repo"}]}})
        vector = _Vector()
        outcome = await dpk.ensure_default_project_knowledge(
            _user(), dependencies=_deps(store=store, vector=vector)
        )
        assert outcome == "present"
        assert provision == []
        assert vector.queries == []  # cheap: nothing past the repository read

    @pytest.mark.asyncio
    async def test_active_pathless_rows_are_skipped_and_logged(self, provision):
        deps = _deps(vector=_Vector(pathless={PROJECT_ID}))
        outcome = await dpk.ensure_default_project_knowledge(_user(), dependencies=deps)
        assert outcome == "legacy_rows"
        assert provision == []
        deps.logger.warning.assert_called_once()
        assert PROJECT_ID in deps.logger.warning.call_args.args

    def test_the_legacy_predicate_is_reconcile_orphans_own(self):
        import inspect

        from shared.runtime.services.knowledge_store import KnowledgeStore

        ours = inspect.getsource(dpk.project_has_unadopted_legacy_rows)
        theirs = inspect.getsource(KnowledgeStore.reconcile_orphans)
        for clause in ("project_id = $1", "path IS NULL", "status = 'active'"):
            assert clause in ours and clause in theirs, clause

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("user", "forge_ready", "expected"),
        [
            (_user(is_approved=False), True, "not_approved"),
            (_user(default_project_id=None), True, "no_project"),
            (_user(), False, "no_forge"),
        ],
    )
    async def test_nothing_is_provisioned_out_of_scope(
        self, provision, user, forge_ready, expected
    ):
        deps = _deps(forge=SimpleNamespace(is_initialized=forge_ready))
        assert (
            await dpk.ensure_default_project_knowledge(user, dependencies=deps)
            == expected
        )
        assert provision == []

    @pytest.mark.asyncio
    async def test_a_provisioning_failure_is_logged_not_raised(self, monkeypatch):
        monkeypatch.setattr(
            dpk,
            "provision_project_knowledge_repo",
            AsyncMock(side_effect=RuntimeError("gitea 500")),
        )
        deps = _deps()
        outcome = await dpk.ensure_default_project_knowledge(_user(), dependencies=deps)
        assert outcome == "failed"
        deps.logger.warning.assert_called_once()

    @pytest.mark.asyncio
    async def test_a_refused_repository_is_a_failure(self, monkeypatch):
        monkeypatch.setattr(
            dpk, "provision_project_knowledge_repo", AsyncMock(return_value=None)
        )
        outcome = await dpk.ensure_default_project_knowledge(
            _user(), dependencies=_deps()
        )
        assert outcome == "failed"

    @pytest.mark.asyncio
    async def test_concurrent_ensures_create_one_vault(self, monkeypatch):
        store = _Store()
        created: list[str] = []
        gate = asyncio.Event()

        async def _provision(project, owner_id, *, dependencies):
            await gate.wait()
            store.repos.setdefault(project["id"], {})["knowledge"] = [{"id": "r"}]
            created.append(project["id"])
            return {"id": "datasource"}

        monkeypatch.setattr(dpk, "provision_project_knowledge_repo", _provision)
        deps = _deps(store=store)
        racing = [
            asyncio.create_task(
                dpk.ensure_default_project_knowledge(_user(), dependencies=deps)
            )
            for _ in range(3)
        ]
        await asyncio.sleep(0)
        gate.set()
        outcomes = await asyncio.gather(*racing)
        assert created == [PROJECT_ID]
        assert sorted(outcomes) == ["present", "present", "provisioned"]


class TestSchedule:
    @pytest.mark.asyncio
    async def test_the_ensure_runs_in_the_background_on_the_repair_state(
        self, provision
    ):
        deps = _deps()
        assert dpk.schedule_default_project_knowledge(_user(), dependencies=deps)
        assert provision == []  # returned before any work
        (task,) = deps.repair.bg_repair_tasks
        assert await task == "provisioned"
        assert len(provision) == 1

    @pytest.mark.asyncio
    async def test_a_second_admission_inside_the_cooldown_is_throttled(self, provision):
        deps = _deps()
        assert dpk.schedule_default_project_knowledge(_user(), dependencies=deps)
        assert not dpk.schedule_default_project_knowledge(_user(), dependencies=deps)
        await asyncio.gather(*list(deps.repair.bg_repair_tasks))
        assert len(provision) == 1

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "user", [_user(is_approved=False), _user(default_project_id=None)]
    )
    async def test_nothing_is_scheduled_out_of_scope(self, provision, user):
        deps = _deps()
        assert not dpk.schedule_default_project_knowledge(user, dependencies=deps)
        assert not deps.repair.bg_repair_tasks

    def test_scheduling_never_raises(self):
        broken = SimpleNamespace(logger=MagicMock(), repair=None)
        assert not dpk.schedule_default_project_knowledge(_user(), dependencies=broken)
        broken.logger.warning.assert_called_once()


# =============================================================================
# The admission paths
# =============================================================================


@pytest.fixture
def vault(monkeypatch):
    """The application's vault scheduler, as ``create_app`` binds it."""
    scheduled = MagicMock(name="default_project_vault")
    monkeypatch.setattr(auth, "_default_project_vault_provider", scheduled)
    return scheduled


@pytest.fixture
def identity_ensures(monkeypatch):
    """The cloud and Gitea account ensures, recorded in call order."""
    order: list[str] = []

    async def _gitea(**_kwargs):
        order.append("gitea")

    async def _cloud(**_kwargs):
        order.append("cloud")

    monkeypatch.setattr(auth, "_ensure_gitea_user", _gitea)
    monkeypatch.setattr(auth, "_ensure_cloud_user", _cloud)
    monkeypatch.setattr(auth, "_ensure_user_cloud_connectors", AsyncMock())
    monkeypatch.setattr(auth, "_notify_admins_of_registration", AsyncMock())
    return order


def _claims(roles: list[str]) -> dict[str, Any]:
    return {
        "sub": "kc-new",
        "email": "new@example.test",
        "preferred_username": "new",
        "realm_access": {"roles": roles},
    }


def _first_login_db(row: dict[str, Any]) -> MagicMock:
    db = MagicMock()
    db.get_user_by_keycloak_sub = AsyncMock(return_value=None)
    db.upsert_user_from_oidc = AsyncMock(return_value=dict(row))
    db.seed_admin_grants = AsyncMock()
    return db


def _existing_db(row: dict[str, Any]) -> MagicMock:
    db = MagicMock()
    db.get_user_by_keycloak_sub = AsyncMock(return_value=dict(row))
    conn = AsyncMock()
    conn.execute = AsyncMock(return_value="UPDATE 1")
    cm = AsyncMock()
    cm.__aenter__ = AsyncMock(return_value=conn)
    cm.__aexit__ = AsyncMock(return_value=False)
    db.acquire = MagicMock(return_value=cm)
    db.seed_admin_grants = AsyncMock()
    return db


async def _settle() -> None:
    for _ in range(5):
        await asyncio.sleep(0)


class TestAdmissionPaths:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("roles", [["user"], ["admin"]])
    async def test_first_login_of_an_approved_user_schedules_the_vault_after_gitea(
        self, monkeypatch, vault, identity_ensures, roles
    ):
        # The bootstrap admin arrives this way on a fresh install.
        monkeypatch.delenv("MCP_DEV_TOKEN", raising=False)
        vault.side_effect = lambda _user: identity_ensures.append("vault")
        db = _first_login_db(_user(is_admin=roles == ["admin"]))

        user = await auth._resolve_user_from_claims(_claims(roles), db)
        await _settle()

        assert user["is_approved"] is True
        vault.assert_called_once()
        assert vault.call_args.args[0]["default_project_id"] == PROJECT_ID
        assert identity_ensures.index("gitea") < identity_ensures.index("vault")

    @pytest.mark.asyncio
    async def test_first_login_of_a_pending_registrant_provisions_nothing(
        self, vault, identity_ensures
    ):
        db = _first_login_db(_user(is_approved=False))
        user = await auth._resolve_user_from_claims(_claims([]), db)
        await _settle()
        assert user["is_approved"] is False
        vault.assert_not_called()
        assert identity_ensures == []

    @pytest.mark.asyncio
    async def test_the_legacy_role_write_through_schedules_the_vault(
        self, vault, identity_ensures
    ):
        db = _existing_db(_user(is_approved=False))
        user = await auth._resolve_user_from_claims(_claims(["user"]), db)
        assert user["is_approved"] is True
        vault.assert_called_once()
        assert vault.call_args.args[0]["is_approved"] is True

    @pytest.mark.asyncio
    async def test_an_ordinary_login_does_no_vault_work(self, vault, identity_ensures):
        # No per-request cost: an approved user's later logins never ask.
        db = _existing_db(_user(is_approved=True))
        await auth._resolve_user_from_claims(_claims(["user"]), db)
        await auth._resolve_user_from_claims(_claims([]), db)
        await _settle()
        vault.assert_not_called()

    @pytest.mark.asyncio
    async def test_a_failing_scheduler_never_fails_the_login(
        self, monkeypatch, caplog, vault, identity_ensures
    ):
        monkeypatch.delenv("MCP_DEV_TOKEN", raising=False)
        vault.side_effect = RuntimeError("no loop")
        db = _first_login_db(_user())
        with caplog.at_level(logging.WARNING, logger=auth.logger.name):
            user = await auth._resolve_user_from_claims(_claims(["user"]), db)
            await _settle()
        assert user["is_approved"] is True
        assert any("not scheduled" in r.getMessage() for r in caplog.records)

    @pytest.mark.asyncio
    async def test_an_unbound_scheduler_never_fails_the_login(
        self, monkeypatch, caplog, identity_ensures
    ):
        monkeypatch.delenv("MCP_DEV_TOKEN", raising=False)
        monkeypatch.setattr(auth, "_default_project_vault_provider", None)
        db = _first_login_db(_user())
        with caplog.at_level(logging.WARNING, logger=auth.logger.name):
            user = await auth._resolve_user_from_claims(_claims(["user"]), db)
            await _settle()
        assert user["is_approved"] is True
        assert any("unbound" in r.getMessage() for r in caplog.records)

    @pytest.mark.asyncio
    async def test_approval_schedules_the_vault_after_gitea(
        self, vault, identity_ensures
    ):
        vault.side_effect = lambda _user: identity_ensures.append("vault")
        await auth.ensure_user_provisioned(_user())
        await _settle()
        assert identity_ensures.index("gitea") < identity_ensures.index("vault")
        assert sorted(identity_ensures) == ["cloud", "gitea", "vault"]

    @pytest.mark.asyncio
    async def test_approving_a_row_without_an_oidc_identity_still_ensures_its_vault(
        self, vault, identity_ensures
    ):
        await auth.ensure_user_provisioned(_user(keycloak_sub=None))
        await _settle()
        vault.assert_called_once()
        assert identity_ensures == []  # no cloud/Gitea account without a sub

    @pytest.mark.asyncio
    async def test_admin_approval_reaches_the_vault_through_ensure_user_provisioned(
        self, vault, identity_ensures
    ):
        # The cockpit's approve path, with the ensure the application wires in.
        row = _user(keycloak_sub=None)
        store = SimpleNamespace(
            update_user=AsyncMock(return_value=True),
            get_user=AsyncMock(return_value=row),
        )
        deps = user_administration.UserAdministrationDependencies(
            store=store,
            logger=MagicMock(),
            main_cloud_router=SimpleNamespace(active_instance_id=None),
            provision_default_project_knowledge=AsyncMock(),
            ensure_user_provisioned=auth.ensure_user_provisioned,
            notification_service=SimpleNamespace(resolve_source=AsyncMock()),
            grant_project_ids=AsyncMock(return_value=[]),
            is_protected_cloud_mode_enabled=lambda: False,
            datasource_scope_auto_attach_v1_enabled=lambda: False,
            datasource_defaults_on_omission=lambda: False,
        )
        await user_administration.admin_patch_user(
            user_id=USER_ID,
            body=user_administration.AdminUserUpdate(is_approved=True),
            admin={"id": str(uuid4())},
            dependencies=deps,
        )
        await _settle()
        vault.assert_called_once_with(row)

    @pytest.mark.asyncio
    async def test_the_oauth_bridge_jit_schedules_an_approved_linked_row(self):
        from orchestrator.schemas.tokens import McpTokenCreateInternal

        scheduled = MagicMock()
        store = SimpleNamespace(
            get_user_by_keycloak_sub=AsyncMock(return_value=None),
            upsert_user_from_oidc=AsyncMock(return_value=_user()),
            create_mcp_token=AsyncMock(return_value={"id": "t", "user_id": USER_ID}),
        )
        deps = access_tokens.AccessTokenDependencies(
            store=store, schedule_default_project_knowledge=scheduled
        )
        body = McpTokenCreateInternal(
            user_sub="kc-new",
            user_email="new@example.test",
            name="n",
            token_hash="h",
            token_prefix="p",
        )
        await access_tokens.create_internal_mcp_token(body=body, dependencies=deps)
        scheduled.assert_called_once()

        # A brand-new row is pending: nothing to provision until approval.
        scheduled.reset_mock()
        store.upsert_user_from_oidc = AsyncMock(return_value=_user(is_approved=False))
        await access_tokens.create_internal_mcp_token(body=body, dependencies=deps)
        scheduled.assert_not_called()

        # A known sub is not an admission.
        store.get_user_by_keycloak_sub = AsyncMock(return_value=_user())
        await access_tokens.create_internal_mcp_token(body=body, dependencies=deps)
        scheduled.assert_not_called()

    @pytest.mark.asyncio
    async def test_a_failing_scheduler_never_fails_the_oauth_bridge(self, caplog):
        from orchestrator.schemas.tokens import McpTokenCreateInternal

        store = SimpleNamespace(
            get_user_by_keycloak_sub=AsyncMock(return_value=None),
            upsert_user_from_oidc=AsyncMock(return_value=_user()),
            create_mcp_token=AsyncMock(return_value={"id": "t", "user_id": USER_ID}),
        )
        deps = access_tokens.AccessTokenDependencies(
            store=store,
            schedule_default_project_knowledge=MagicMock(
                side_effect=RuntimeError("no deps")
            ),
        )
        body = McpTokenCreateInternal(
            user_sub="kc-new", name="n", token_hash="h", token_prefix="p"
        )
        with caplog.at_level(logging.WARNING, logger=access_tokens.logger.name):
            row = await access_tokens.create_internal_mcp_token(
                body=body, dependencies=deps
            )
        assert row["id"] == "t"
        assert any("not scheduled" in r.getMessage() for r in caplog.records)


class TestComposition:
    def test_the_access_token_port_schedules_on_its_own_application(self):
        from orchestrator.application import administration
        from orchestrator.application import projects as projects_composition

        resources = SimpleNamespace(postgres_db=object())
        deps = administration.access_token_dependencies(resources)
        schedule = deps.tokens.schedule_default_project_knowledge
        assert isinstance(schedule, functools.partial)
        assert schedule.func is projects_composition.schedule_default_project_knowledge
        assert schedule.args == (resources,)

    def test_create_app_binds_the_auth_scheduler_to_its_application(self, monkeypatch):
        monkeypatch.setenv("VECTOR_DB_URL", "postgresql://test@localhost/test")
        from orchestrator.application import create_app
        from orchestrator.application import projects as projects_composition

        seen: list[tuple] = []
        monkeypatch.setattr(
            projects_composition,
            "schedule_default_project_knowledge",
            lambda resources, user: seen.append((resources, user)),
        )
        app = create_app()
        auth._default_project_vault_provider({"id": "u"})
        assert seen == [(app.state.resources, {"id": "u"})]


# =============================================================================
# The startup backfill
# =============================================================================


def _candidate(project_id: str, user_id: str | None = None) -> dict[str, Any]:
    return {
        "id": user_id or str(uuid4()),
        "email": "c@example.test",
        "default_project_id": project_id,
        "is_approved": True,
    }


class TestBackfill:
    @pytest.mark.asyncio
    async def test_eligible_projects_are_provisioned_and_legacy_rows_skipped(
        self, provision
    ):
        eligible, legacy = str(uuid4()), str(uuid4())
        store = _Store(candidates=[_candidate(eligible), _candidate(legacy)])
        deps = _deps(store=store, vector=_Vector(pathless={legacy}))

        counts = await dpk.backfill_default_project_knowledge(dependencies=deps)

        assert [p["id"] for p, _owner in provision] == [eligible]
        assert counts["provisioned"] == 1 and counts["legacy_rows"] == 1
        # The candidate query filters on kb_reindex's vault roles.
        assert store.fetch_args == [(["knowledge", "jobs"],)]
        deps.logger.warning.assert_any_call(
            "Default project knowledge vaults: %s", counts
        )

    @pytest.mark.asyncio
    async def test_a_project_provisioned_meanwhile_is_re_checked_under_the_lock(
        self, provision
    ):
        # Listed without a vault, given one by an admission before its turn.
        project = str(uuid4())
        store = _Store(
            candidates=[_candidate(project)],
            repos={project: {"knowledge": [{"id": "r"}]}},
        )
        counts = await dpk.backfill_default_project_knowledge(
            dependencies=_deps(store=store)
        )
        assert counts["present"] == 1 and provision == []

    @pytest.mark.asyncio
    async def test_a_set_stop_ends_the_pass_between_projects(self, provision):
        stop = asyncio.Event()
        stop.set()
        store = _Store(candidates=[_candidate(str(uuid4()))])
        counts = await dpk.backfill_default_project_knowledge(
            dependencies=_deps(store=store), stop=stop
        )
        assert provision == [] and sum(counts.values()) == 0


class TestBackfillLoop:
    @pytest.mark.asyncio
    async def test_one_pass_then_it_parks_until_stop(self, monkeypatch):
        passes = AsyncMock(return_value={})
        monkeypatch.setattr(dpk, "backfill_default_project_knowledge", passes)
        stop = asyncio.Event()
        task = asyncio.create_task(
            dpk.default_project_knowledge_backfill_loop(stop, dependencies=_deps())
        )
        await asyncio.sleep(0.05)
        assert passes.await_count == 1
        assert not task.done(), "the finished loop returned instead of parking"
        stop.set()
        await asyncio.wait_for(task, timeout=1)

    @pytest.mark.asyncio
    async def test_an_unready_forge_is_waited_for(self, monkeypatch):
        passes = AsyncMock(return_value={})
        monkeypatch.setattr(dpk, "backfill_default_project_knowledge", passes)
        forge = SimpleNamespace(is_initialized=False, is_configured=True)
        attempts = 0

        async def _initialise():
            nonlocal attempts
            attempts += 1
            if attempts == 3:
                forge.is_initialized = True

        forge.ensure_initialized = _initialise
        deps = _deps(forge=forge)
        stop = asyncio.Event()
        task = asyncio.create_task(
            dpk.default_project_knowledge_backfill_loop(
                stop, dependencies=deps, retry_seconds=0.01
            )
        )
        await asyncio.sleep(0.2)
        assert attempts == 3 and passes.await_count == 1
        deps.logger.warning.assert_called_once()  # waiting is said once
        stop.set()
        await asyncio.wait_for(task, timeout=1)

    @pytest.mark.asyncio
    async def test_a_failed_pass_is_retried(self, monkeypatch):
        passes = AsyncMock(side_effect=[RuntimeError("db"), {}])
        monkeypatch.setattr(dpk, "backfill_default_project_knowledge", passes)
        deps = _deps()
        stop = asyncio.Event()
        task = asyncio.create_task(
            dpk.default_project_knowledge_backfill_loop(
                stop, dependencies=deps, retry_seconds=0.01
            )
        )
        await asyncio.sleep(0.1)
        assert passes.await_count == 2
        deps.logger.exception.assert_called_once()
        stop.set()
        await asyncio.wait_for(task, timeout=1)

    @pytest.mark.asyncio
    async def test_without_a_forge_it_parks_without_a_pass(self, monkeypatch):
        passes = AsyncMock()
        monkeypatch.setattr(dpk, "backfill_default_project_knowledge", passes)
        deps = _deps(forge=SimpleNamespace(is_initialized=False, is_configured=False))
        stop = asyncio.Event()
        task = asyncio.create_task(
            dpk.default_project_knowledge_backfill_loop(stop, dependencies=deps)
        )
        await asyncio.sleep(0.05)
        assert not task.done()
        passes.assert_not_awaited()
        stop.set()
        await asyncio.wait_for(task, timeout=1)

    @pytest.mark.asyncio
    async def test_leader_gated_it_runs_once_per_tenure_and_stops_cleanly(
        self, monkeypatch
    ):
        # Through the lifecycle's own path, with the wrapper's poll shortened
        # (tests/test_b12_leader_loop_parking.py's harness).
        monkeypatch.setattr(
            leader_election,
            "run_when_leader",
            functools.partial(leader_election.run_when_leader, poll_seconds=0.01),
        )
        passes = AsyncMock(return_value={})
        monkeypatch.setattr(dpk, "backfill_default_project_knowledge", passes)
        leader_election.is_leader.clear()
        shutdown = asyncio.Event()
        tasks = application_tasks.ApplicationTaskSet(shutdown)
        tasks.start_leader_gated(
            "default_project_knowledge_backfill",
            lambda stop: dpk.default_project_knowledge_backfill_loop(
                stop, dependencies=_deps()
            ),
        )
        try:
            await asyncio.sleep(0.05)
            passes.assert_not_awaited()  # not without leadership
            leader_election.is_leader.set()
            await asyncio.sleep(0.25)
            assert passes.await_count == 1
        finally:
            failure = await asyncio.wait_for(
                tasks.stop(["default_project_knowledge_backfill"]), timeout=5
            )
            leader_election.is_leader.clear()
        assert failure is None

    def test_the_task_is_registered_leader_gated_with_a_shutdown_slot(self):
        import ast
        import inspect

        from orchestrator.application import background_tasks

        assert (
            "default_project_knowledge_backfill"
            in background_tasks.BACKGROUND_TASK_SHUTDOWN_ORDER
        )
        source = inspect.getsource(background_tasks.start_background_tasks)
        gated = [
            node.args[0].value
            for node in ast.walk(ast.parse(source))
            if isinstance(node, ast.Call)
            and ast.unparse(node.func) == "tasks.start_leader_gated"
            and isinstance(node.args[0], ast.Constant)
        ]
        assert "default_project_knowledge_backfill" in gated


# =============================================================================
# Resuming an unfinished vault creation
# =============================================================================


class TestResume:
    def _deps(
        self, intent: Any
    ) -> project_provisioning.ProjectProvisioningDependencies:
        store = SimpleNamespace(
            get_managed_repository_creation_intent=AsyncMock(return_value=intent)
        )
        return _deps(
            store=store,
            forge=SimpleNamespace(is_initialized=True, repository_owner="srw"),
        )

    @pytest.mark.asyncio
    async def test_the_projects_own_unfinished_intent_is_resumed(self):
        authority = uuid4()
        deps = self._deps(
            {
                "authority_kind": "project_repository",
                "authority_id": authority,
                "project_id": PROJECT_ID,
                "access_mode": "none",
                "status": "pending",
            }
        )
        resumed = await project_provisioning._unfinished_knowledge_repository_id(
            "project-x-knowledge", PROJECT_ID, dependencies=deps
        )
        assert resumed == str(authority)
        deps.store.get_managed_repository_creation_intent.assert_awaited_once_with(
            "project-x-knowledge", repository_owner="srw"
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "intent",
        [
            None,
            {"authority_kind": "thread", "project_id": PROJECT_ID},
            {"authority_kind": "project_repository", "project_id": str(uuid4())},
            {
                "authority_kind": "project_repository",
                "project_id": PROJECT_ID,
                "access_mode": "write",
            },
        ],
    )
    async def test_another_scopes_intent_is_never_borrowed(self, intent):
        if intent is not None:
            intent = {"authority_id": uuid4(), "access_mode": "none", **intent}
        assert (
            await project_provisioning._unfinished_knowledge_repository_id(
                "project-x-knowledge", PROJECT_ID, dependencies=self._deps(intent)
            )
            is None
        )

    @pytest.mark.asyncio
    async def test_an_unreadable_intent_only_gives_up_the_resume(self):
        deps = _deps(store=MagicMock(), forge=SimpleNamespace(is_initialized=True))
        assert (
            await project_provisioning._unfinished_knowledge_repository_id(
                "project-x-knowledge", PROJECT_ID, dependencies=deps
            )
            is None
        )
        deps.logger.warning.assert_not_called()
