"""``config_name`` is validated where it is WRITTEN, and provisioning fails loud.

Commit 193f48552 put the pod-entrypoint allow-list
(``services.agent_pod_entrypoint.validate_config_name``) at both provisioner
boundaries.  That closed the injection, but it turned a hostile/malformed
``config_name`` from "returns None/FAILED" into "raises" — and the raise lands
in places nobody was catching:

* four fire-and-forget ``asyncio.create_task`` closures (officer commission,
  the two resume reprovision paths, the magic-link wake) where the caller has
  already answered 200, so the exception is dropped and the session simply
  never becomes ready and never reports why;
* ``POST /api/projects/{id}/officer/recycle``, which had no local handler and
  fell through to the generic ``Exception`` handler as an opaque 500.

And the root cause: the value reached the DB unvalidated in the first place —
``ThreadCreateRequest``/``JobCreate`` applied only ``canonical_config_name``,
which is an alias fold, not a charset check — so one bad create poisoned every
later resume, recycle and wake.

This file pins both halves:

* every write boundary refuses a hostile name with a 4xx and writes NOTHING;
* every legitimate name and alias form still goes through;
* each of the four detached provisioning paths records a *failed* lifecycle
  state instead of vanishing;
* the recycle route answers 4xx, not 500, for a row poisoned before the write
  boundary existed (a pre-existing bad row must fail its provisioning attempt
  loudly, not wedge the routes that list or delete it).
"""

from __future__ import annotations

from contextlib import ExitStack
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

import orchestrator.main as orch_main
from orchestrator.routers import officers as officers_router
from orchestrator.services.agent_pod_entrypoint import (
    InvalidConfigNameError,
    validate_config_name,
)
from orchestrator.services.session_runtime_admission import ThreadRuntimeAuthority


# Each of these breaks a different rule in the allow-list, and each is the
# shape the audit actually cared about: shell metacharacters, an argparse
# flag, traversal out of the config tree, and a non-ASCII lookalike.
HOSTILE_CONFIG_NAMES = [
    "scholar; touch /tmp/pwned",
    "scholar$(id)",
    "-c",
    "../../../etc/passwd",
    "config/experts/../../secrets.yaml",
    "scholar\nworker_base",
    "schölar",
]

# Everything a caller is allowed to say, including every alias form. These must
# keep working end to end — the guard is a charset check, not a whitelist of
# the four names someone remembered.
LEGITIMATE_CONFIG_NAMES = [
    "worker_base",
    "session_base",
    "subagent_base",
    "scholar",
    "centurion",
    "general-worker",
    "product-qa",
    "config/experts/scholar/config.yaml",
    "config/subagents/reader/config.yaml",
    "defaults",
    "default",
    "persistent_defaults",
    "worker_base.yaml",
    "overlays/worker",
]

THREAD_ID = "11111111-1111-4111-8111-111111111111"
USER_ID = "99999999-9999-4999-8999-999999999999"
GENERATION = "22222222-2222-4222-8222-222222222222"


class TestValidatorVocabulary:
    """The allow-list itself, before anything wires it up."""

    @pytest.mark.parametrize("name", LEGITIMATE_CONFIG_NAMES)
    def test_legitimate_selectors_pass_through_unchanged(self, name):
        assert validate_config_name(name) == name

    @pytest.mark.parametrize("name", HOSTILE_CONFIG_NAMES)
    def test_hostile_selectors_are_refused(self, name):
        with pytest.raises(InvalidConfigNameError):
            validate_config_name(name)

    def test_absent_and_empty_keep_the_caller_default(self):
        assert validate_config_name(None) is None
        assert validate_config_name("") == ""


# --------------------------------------------------------------------------- #
# Write boundaries
# --------------------------------------------------------------------------- #


def _patch_caller(user: dict, db) -> ExitStack:
    stack = ExitStack()
    stack.enter_context(
        patch("orchestrator.main.require_approved_user", AsyncMock(return_value=user))
    )
    stack.enter_context(
        patch(
            "orchestrator.security.access.require_approved_user",
            AsyncMock(return_value=user),
        )
    )
    stack.enter_context(patch("orchestrator.main.postgres_db", db))
    return stack


class TestThreadCreateWriteBoundary:
    """``POST /api/persistent/threads`` — the row every resume path reads back."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("name", HOSTILE_CONFIG_NAMES)
    async def test_hostile_name_is_refused_before_any_insert(
        self, name, user_a, fake_db, fake_request
    ):
        from orchestrator.main import ThreadCreateRequest, create_thread

        fake_db.create_thread = AsyncMock()
        fake_db.get_user_settings = AsyncMock(return_value={})
        with _patch_caller(user_a, fake_db):
            with patch("orchestrator.main._enforce_readiness_gate", AsyncMock()):
                with pytest.raises(HTTPException) as exc:
                    await create_thread(
                        ThreadCreateRequest(config_name=name), fake_request
                    )

        assert exc.value.status_code == 422
        assert "config_name" in str(exc.value.detail)
        fake_db.create_thread.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("name", LEGITIMATE_CONFIG_NAMES)
    async def test_legitimate_name_gets_past_the_boundary(
        self, name, user_a, fake_db, fake_request
    ):
        """The guard must not be the thing that stops a valid session.

        Creation goes on to need the whole session stack; this asserts only
        that it is no longer the config_name check that refuses, i.e. no 422
        naming the field.
        """
        from orchestrator.main import ThreadCreateRequest, create_thread

        fake_db.get_user_settings = AsyncMock(return_value={})
        with _patch_caller(user_a, fake_db):
            with patch("orchestrator.main._enforce_readiness_gate", AsyncMock()):
                try:
                    await create_thread(
                        ThreadCreateRequest(config_name=name), fake_request
                    )
                except HTTPException as exc:
                    assert not (
                        exc.status_code == 422 and "config_name may only" in str(exc)
                    ), f"{name} was refused by the config_name guard"
                except Exception:
                    pass


class TestAgentThreadCreateWriteBoundary:
    """``POST /api/agents/threads`` — the internal key authenticates the
    transport, not the body it carries."""

    @pytest.mark.asyncio
    async def test_hostile_name_is_refused_before_any_insert(self, fake_db):
        import orchestrator.main as main
        from orchestrator.main import AgentThreadCreateRequest
        from orchestrator.services.agent_child_threads import agent_create_thread

        fake_db.create_thread = AsyncMock()
        with patch("orchestrator.main.postgres_db", fake_db):
            with patch("orchestrator.main.require_internal", AsyncMock()):
                with pytest.raises(HTTPException) as exc:
                    await agent_create_thread(
                        MagicMock(),
                        AgentThreadCreateRequest(config_name="a; rm -rf /"),
                        dependencies=main._agent_child_threads_dependencies(),
                    )

        assert exc.value.status_code == 422
        fake_db.create_thread.assert_not_awaited()


class TestJobCreateWriteBoundary:
    """``POST /api/jobs`` — jobs.config_name is read back by dispatch, resume,
    subjob grafting and every recovery path."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("name", ["worker; id", "../../etc/shadow", "-c"])
    async def test_hostile_name_is_refused_before_any_insert(
        self, name, user_a, fake_db, fake_request
    ):
        from orchestrator.main import JobCreate
        from tests._b09_control_seams import create_job

        fake_db.create_job = AsyncMock()
        # No default project: the boundary under test must fire on a plain,
        # unscoped job create, not only inside a project.
        fake_db.get_user = AsyncMock(return_value=user_a)
        with _patch_caller(user_a, fake_db):
            with patch("orchestrator.main._enforce_readiness_gate", AsyncMock()):
                with pytest.raises(HTTPException) as exc:
                    await create_job(
                        fake_request,
                        JobCreate(description="do a thing", config_name=name),
                    )

        assert exc.value.status_code == 422
        assert "config_name" in str(exc.value.detail)
        fake_db.create_job.assert_not_awaited()


class TestProjectDefaultConfigNameWriteBoundary:
    """A project default is copied into ``jobs.config_name`` by create_job's
    legacy compatibility branch, so it is the same write boundary."""

    @pytest.mark.asyncio
    async def test_create_project_refuses_hostile_default(
        self, user_a, fake_db, fake_request
    ):
        from orchestrator.routers.projects import create_project
        from orchestrator.schemas.projects import ProjectCreate

        fake_db.create_project = AsyncMock()
        with _patch_caller(user_a, fake_db):
            with pytest.raises(HTTPException) as exc:
                await create_project(
                    ProjectCreate(
                        name="P",
                        user_id=str(user_a["id"]),
                        default_config_name="scholar && curl evil",
                    ),
                    fake_request,
                    dependencies=orch_main._projects_dependencies(),
                )

        assert exc.value.status_code == 422
        fake_db.create_project.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_patch_project_refuses_hostile_default(
        self, user_a, project_a, fake_db, fake_request
    ):
        from orchestrator.routers.projects import update_project
        from orchestrator.schemas.projects import ProjectUpdate

        fake_db.update_project = AsyncMock(return_value=True)
        with _patch_caller(user_a, fake_db):
            with patch(
                "orchestrator.main.require_project_owner",
                AsyncMock(return_value=(user_a, project_a)),
            ):
                with pytest.raises(HTTPException) as exc:
                    await update_project(
                        str(project_a["id"]),
                        ProjectUpdate(default_config_name="../../etc/passwd"),
                        fake_request,
                        dependencies=orch_main._projects_dependencies(),
                    )

        assert exc.value.status_code == 422
        fake_db.update_project.assert_not_awaited()


class TestSessionPrepareWriteBoundary:
    """``POST /api/sessions/{tid}/prepare`` — the body's own name reaches the
    provisioner, so it gets one clean 422 rather than a lifecycle failure the
    caller has to subscribe to."""

    @pytest.mark.asyncio
    async def test_hostile_body_name_is_refused_before_scheduling(
        self, user_a, thread_a
    ):
        from orchestrator.routers import sessions as sessions_router

        thread = dict(thread_a)
        thread.update(
            {
                "status": "created",
                "runtime_generation": GENERATION,
                "runtime_retirement_token": None,
            }
        )
        db = MagicMock()
        db.get_thread = AsyncMock(return_value=thread)
        scheduled: list = []

        # The router reads its store off the application answering the request.
        request = MagicMock()
        request.app.state.sessions_dependencies_factory = lambda: SimpleNamespace(
            store=db
        )

        with patch.object(
            sessions_router, "require_approved_user", AsyncMock(return_value=user_a)
        ):
            with patch.object(
                sessions_router,
                "_schedule_prepare_task",
                lambda coro: (coro.close(), scheduled.append(coro))[0],
            ):
                with pytest.raises(HTTPException) as exc:
                    await sessions_router.prepare_session(
                        request,
                        str(thread["id"]),
                        sessions_router.PrepareRequest(config_name="a b"),
                    )

        assert exc.value.status_code == 422
        assert scheduled == []


class TestAutomationExpertWriteBoundary:
    """An automation's ``expert`` becomes ``jobs.config_name`` on every future
    scheduled fire, long after the request that stored it."""

    @pytest.mark.asyncio
    async def test_hostile_expert_is_refused(self):
        from orchestrator.services.automations import (
            validate_automation_expert_selection,
        )
        from orchestrator.services.default_experts import ExpertSelectionError

        with pytest.raises(ExpertSelectionError) as exc:
            await validate_automation_expert_selection(
                MagicMock(),
                owner_id=USER_ID,
                project_id=None,
                expert="scholar; id",
                expert_id=None,
                caller_is_admin=False,
            )
        assert "config_name" in str(exc.value)

    @pytest.mark.asyncio
    async def test_legitimate_expert_still_normalizes(self):
        from orchestrator.services.automations import (
            validate_automation_expert_selection,
        )

        assert (
            await validate_automation_expert_selection(
                MagicMock(),
                owner_id=USER_ID,
                project_id=None,
                expert="defaults",
                expert_id=None,
                caller_is_admin=False,
            )
            == "worker_base"
        )


class TestBenchSpecWriteBoundary:
    """Bench run specs are persisted before any job exists, and every replicate
    hands the word to a pod entrypoint."""

    def test_task_spec_refuses_hostile_name(self):
        from pydantic import ValidationError

        from orchestrator.routers.bench import BenchTaskSpec

        with pytest.raises(ValidationError):
            BenchTaskSpec(id="t1", description="d", config_name="a;b")

    def test_arm_spec_refuses_hostile_name(self):
        from pydantic import ValidationError

        from orchestrator.routers.bench import BenchArmSpec

        with pytest.raises(ValidationError):
            BenchArmSpec(name="arm", config_name="../x", model="m")

    def test_specs_accept_bundled_selectors(self):
        from orchestrator.routers.bench import BenchArmSpec, BenchTaskSpec

        assert BenchTaskSpec(id="t1", description="d", config_name="scholar")
        assert BenchArmSpec(
            name="arm", config_name="config/experts/scholar/config.yaml", model="m"
        )


# --------------------------------------------------------------------------- #
# The four fire-and-forget provisioning closures
# --------------------------------------------------------------------------- #


def _preparable_thread(**extra) -> dict:
    thread = {
        "id": THREAD_ID,
        "user_id": USER_ID,
        "status": "created",
        "execution_lane": "pinned",
        "runtime_generation": GENERATION,
        "runtime_retirement_token": None,
        "agent_id": None,
        "config_name": "session_base",
        "metadata": {},
    }
    thread.update(extra)
    return thread


def _authority() -> ThreadRuntimeAuthority:
    return ThreadRuntimeAuthority(thread_id=THREAD_ID, generation=GENERATION)


class _LifecycleRecorder:
    """Capture ``session.lifecycle`` emissions the way the cockpit sees them."""

    def __init__(self):
        self.events: list[dict] = []

    def __call__(self, user_id, thread_id, state, **extra):
        self.events.append(
            {"user_id": user_id, "thread_id": thread_id, "state": state, **extra}
        )

    @property
    def failures(self) -> list[dict]:
        return [e for e in self.events if e["state"] == "failed"]


def _refusing_persistent_provisioner() -> MagicMock:
    prov = MagicMock()
    prov.is_available = True
    prov.expected_build_sha = "sha"
    prov.create_agent_pod = AsyncMock(
        side_effect=InvalidConfigNameError(
            "config_name may only contain letters, digits, '.', '_', '/' "
            "and '-': 'a; id'"
        )
    )
    return prov


class TestCommissionedOfficerProvisioningFailsLoudly:
    @pytest.mark.asyncio
    async def test_refused_config_name_records_a_failed_state(self, monkeypatch):
        db = MagicMock()
        db.get_thread = AsyncMock(return_value=_preparable_thread())
        monkeypatch.setattr(orch_main, "postgres_db", db)
        monkeypatch.setattr(
            orch_main, "persistent_provisioner", _refusing_persistent_provisioner()
        )
        recorder = _LifecycleRecorder()

        with patch("orchestrator.services.session_lifecycle.emit", recorder):
            # Must not raise: in production this IS the task body, and a raise
            # here is dropped by asyncio with nothing else recording it.
            await orch_main._provision_commissioned_officer(
                THREAD_ID,
                user_id=USER_ID,
                config_name="a; id",
                runtime_authority=_authority(),
            )

        assert len(recorder.failures) == 1
        failure = recorder.failures[0]
        assert failure["thread_id"] == THREAD_ID
        assert failure["session_runtime_generation"] == GENERATION
        assert "config_name" in failure["reason"]

    @pytest.mark.asyncio
    async def test_unusable_result_also_records_a_failed_state(self, monkeypatch):
        db = MagicMock()
        db.get_thread = AsyncMock(return_value=_preparable_thread())
        monkeypatch.setattr(orch_main, "postgres_db", db)
        prov = MagicMock()
        prov.create_agent_pod = AsyncMock(
            return_value=SimpleNamespace(
                usable=False,
                status=SimpleNamespace(value="failed"),
                failure_class="pvc_creation_failed",
            )
        )
        monkeypatch.setattr(orch_main, "persistent_provisioner", prov)
        recorder = _LifecycleRecorder()

        with patch("orchestrator.services.session_lifecycle.emit", recorder):
            await orch_main._provision_commissioned_officer(
                THREAD_ID,
                user_id=USER_ID,
                config_name="centurion",
                runtime_authority=_authority(),
            )

        assert len(recorder.failures) == 1
        assert "pvc_creation_failed" in recorder.failures[0]["reason"]

    @pytest.mark.asyncio
    async def test_a_dead_generation_is_not_spoken_for(self, monkeypatch):
        db = MagicMock()
        db.get_thread = AsyncMock(
            return_value=_preparable_thread(
                runtime_generation="33333333-3333-4333-8333-333333333333"
            )
        )
        monkeypatch.setattr(orch_main, "postgres_db", db)
        monkeypatch.setattr(
            orch_main, "persistent_provisioner", _refusing_persistent_provisioner()
        )
        recorder = _LifecycleRecorder()

        with patch("orchestrator.services.session_lifecycle.emit", recorder):
            await orch_main._provision_commissioned_officer(
                THREAD_ID,
                user_id=USER_ID,
                config_name="a; id",
                runtime_authority=_authority(),
            )

        assert recorder.events == []


class _CollectingCreateTask:
    """Stand-in for ``asyncio.create_task`` that keeps the coroutine.

    Production schedules these detached; running the one under test by hand is
    what lets the test observe whether it *records* its failure or vanishes.
    Anything else scheduled on the same path is closed, not run — this file has
    no interest in cloud folders or workspace reconciliation.
    """

    def __init__(self):
        self.coros: list = []

    def __call__(self, coro, *args, **kwargs):
        self.coros.append(coro)
        return SimpleNamespace(
            cancel=lambda: None,
            done=lambda: True,
            add_done_callback=lambda _cb: None,
        )

    async def drain(self, name_contains: str) -> int:
        ran = 0
        for coro in self.coros:
            if name_contains in getattr(coro, "cr_code", None).co_name:
                await coro
                ran += 1
            else:
                coro.close()
        self.coros = []
        return ran


RESUMED_GENERATION = "44444444-4444-4444-8444-444444444444"


def _resume_db(user_a, thread_row: dict):
    """``postgres_db`` stand-in that reopens ``thread_row`` on resume."""
    # AsyncMock base: resume_thread awaits a long tail of incidental reads and
    # this file is not the place to enumerate them. Only the calls the
    # provisioning fan-out actually branches on are pinned below.
    db = AsyncMock()
    db.get_thread = AsyncMock(return_value=thread_row)
    db.get_user = AsyncMock(return_value=user_a)
    db.list_thread_mounts = AsyncMock(return_value=[])

    async def _resume(thread_id: str) -> bool:
        thread_row["status"] = "created"
        thread_row["runtime_generation"] = RESUMED_GENERATION
        thread_row["agent_id"] = None
        thread_row["runtime_attach_token"] = None
        thread_row["ended_at"] = None
        return True

    db.resume_thread = AsyncMock(side_effect=_resume)
    db.get_user_settings = AsyncMock(return_value={})

    class _Lock:
        async def __aenter__(self):
            return None

        async def __aexit__(self, *args):
            return None

    db.thread_advisory_lock = lambda tid: _Lock()
    return db


def _resume_stack(user: dict, db, thread_row: dict) -> ExitStack:
    stack = ExitStack()
    stack.enter_context(
        patch(
            "orchestrator.main.require_thread_owner",
            AsyncMock(return_value=(user, thread_row)),
        )
    )
    stack.enter_context(patch("orchestrator.main.postgres_db", db))
    stack.enter_context(
        patch("orchestrator.main.ensure_session_workspace", AsyncMock())
    )
    stack.enter_context(
        patch(
            "orchestrator.main.thread_resume_operations.thread_config_drift",
            AsyncMock(return_value=[]),
        )
    )
    stack.enter_context(
        patch(
            "orchestrator.main.thread_resume_operations.await_late_cloud_setup",
            AsyncMock(),
        )
    )
    stack.enter_context(
        patch(
            "orchestrator.main._await_protected_cloud_runtime_ready",
            AsyncMock(return_value=True),
        )
    )
    stack.enter_context(
        patch(
            "orchestrator.main._find_idle_persistent_agent",
            AsyncMock(return_value=None),
        )
    )
    return stack


class TestResumeReprovisionFailsLoudly:
    """``POST /api/persistent/threads/{id}/resume`` schedules its binding work
    detached, so a raise inside it used to leave the session in limbo: the
    caller already had its 200 and nothing ever marked the thread failed."""

    @pytest.mark.asyncio
    async def test_refused_config_name_records_a_failed_state(
        self, user_a, fake_request
    ):
        from tests._b09_control_seams import resume_thread

        thread_row = _preparable_thread(
            user_id=str(user_a["id"]),
            status="ended",
            config_name="scholar; id",
        )
        db = _resume_db(user_a, thread_row)
        tasks = _CollectingCreateTask()
        recorder = _LifecycleRecorder()
        provisioner = MagicMock()
        provisioner.is_available = True
        provisioner.in_cluster = True
        provisioner.provision_agent = AsyncMock(
            side_effect=InvalidConfigNameError(
                "config_name may only contain letters, digits, '.', '_', "
                "'/' and '-': 'scholar; id'"
            )
        )

        with _resume_stack(user_a, db, thread_row):
            with patch("orchestrator.main.agent_provisioner", provisioner):
                with patch("orchestrator.main.asyncio.create_task", tasks):
                    with patch(
                        "orchestrator.services.session_lifecycle.emit", recorder
                    ):
                        await resume_thread(THREAD_ID, fake_request)
                        assert await tasks.drain("_reprovision") == 1

        assert len(recorder.failures) == 1
        failure = recorder.failures[0]
        assert failure["thread_id"] == THREAD_ID
        assert failure["session_runtime_generation"] == RESUMED_GENERATION
        assert "config_name" in failure["reason"]

    @pytest.mark.asyncio
    async def test_legacy_path_refusal_records_a_failed_state(
        self, user_a, fake_request
    ):
        from tests._b09_control_seams import resume_thread

        thread_row = _preparable_thread(
            user_id=str(user_a["id"]),
            status="ended",
            config_name="../../etc/passwd",
        )
        db = _resume_db(user_a, thread_row)
        tasks = _CollectingCreateTask()
        recorder = _LifecycleRecorder()

        with _resume_stack(user_a, db, thread_row):
            with patch(
                "orchestrator.main.agent_provisioner",
                SimpleNamespace(is_available=False, in_cluster=False),
            ):
                with patch(
                    "orchestrator.main.persistent_provisioner",
                    _refusing_persistent_provisioner(),
                ):
                    with patch("orchestrator.main.asyncio.create_task", tasks):
                        with patch(
                            "orchestrator.services.session_lifecycle.emit", recorder
                        ):
                            await resume_thread(THREAD_ID, fake_request)
                            assert await tasks.drain("_reprovision_legacy") == 1

        assert len(recorder.failures) == 1
        assert "config_name" in recorder.failures[0]["reason"]


class TestMagicLinkWakeProvisioningFailsLoudly:
    """``_create_after_magic_link`` sits lexically inside
    ``_phase5_wake_if_suspended``'s try/except, but it is scheduled as its own
    task — so that handler never sees anything it raises."""

    @pytest.mark.asyncio
    async def test_refused_config_name_records_a_failed_state(self, monkeypatch):
        thread = _preparable_thread(
            status="suspended",
            config_name="a; id",
            metadata={"workspace_container": {"status": "ready"}},
        )
        db = MagicMock()
        db.get_thread = AsyncMock(return_value=thread)
        fake_conn = MagicMock()
        fake_conn.fetchval = AsyncMock(return_value=THREAD_ID)

        class _Acquire:
            async def __aenter__(self):
                return fake_conn

            async def __aexit__(self, *args):
                return None

        db.acquire = lambda: _Acquire()
        monkeypatch.setattr(orch_main, "postgres_db", db)
        monkeypatch.setattr(
            orch_main, "persistent_provisioner", _refusing_persistent_provisioner()
        )
        monkeypatch.setattr(orch_main, "_persistent_thread_recycler", None)
        svc = MagicMock()
        svc.is_enabled = True
        monkeypatch.setattr(orch_main, "workspace_suspension_service", svc)

        tasks = _CollectingCreateTask()
        recorder = _LifecycleRecorder()
        with patch("orchestrator.main.asyncio.create_task", tasks):
            with patch("orchestrator.services.session_lifecycle.emit", recorder):
                await orch_main._phase5_wake_if_suspended(THREAD_ID)
                assert await tasks.drain("_create_after_magic_link") == 1

        assert len(recorder.failures) == 1
        assert "config_name" in recorder.failures[0]["reason"]


def _officer_recycle_request(db, recycler):
    """The Post's recycle route resolves its store, provisioner and recycler
    from ``request.app.state`` now — rebinding the three ``orchestrator.main``
    globals no longer reaches it."""
    from orchestrator.services.officer_post_lifecycle import (
        OfficerPostLifecycleDependencies,
    )
    from orchestrator.services.officer_post_policy import (
        OfficerPostPolicyDependencies,
    )

    dependencies = OfficerPostLifecycleDependencies(
        store=db,
        persistent_provisioner=MagicMock(is_available=True, expected_build_sha="sha"),
        persistent_thread_recycler=recycler,
        policy=OfficerPostPolicyDependencies(auto_pull_release_enabled=lambda: False),
        kick_officer_event_drain=MagicMock(),
        deliver_officer_note=AsyncMock(),
        create_thread=AsyncMock(),
        end_thread_flow=AsyncMock(),
    )
    request = MagicMock()
    request.app.state.officer_post_lifecycle_dependencies_factory = lambda: dependencies
    return request


class TestOfficerRecycleRouteAnswers4xx:
    """A row poisoned before the write boundary existed must fail its
    provisioning attempt with something an operator can act on."""

    @pytest.mark.asyncio
    async def test_refused_stored_config_name_is_a_422_not_a_500(self, monkeypatch):
        db = MagicMock()
        db.get_officer_thread_for_project = AsyncMock(
            return_value={"id": THREAD_ID, "project_id": "p"}
        )
        monkeypatch.setattr(
            officers_router,
            "require_project_owner",
            AsyncMock(return_value=(None, None)),
        )
        recycler = MagicMock()
        recycler.observe = AsyncMock(return_value=None)
        recycler.request_and_reconcile = AsyncMock(
            side_effect=InvalidConfigNameError(
                "config_name must not contain a '..' segment: '../../x'"
            )
        )
        request = _officer_recycle_request(db, recycler)

        with pytest.raises(HTTPException) as exc:
            await officers_router.recycle_project_officer(request, "p")

        assert exc.value.status_code == 422
        assert "'..' segment" in str(exc.value.detail)

    @pytest.mark.asyncio
    async def test_an_unexpected_error_is_still_unexpected(self, monkeypatch):
        """Only the config-name refusal is reclassified; nothing else is
        quietly downgraded to a client error."""
        db = MagicMock()
        db.get_officer_thread_for_project = AsyncMock(
            return_value={"id": THREAD_ID, "project_id": "p"}
        )
        monkeypatch.setattr(
            officers_router,
            "require_project_owner",
            AsyncMock(return_value=(None, None)),
        )
        recycler = MagicMock()
        recycler.observe = AsyncMock(return_value=None)
        recycler.request_and_reconcile = AsyncMock(side_effect=RuntimeError("boom"))
        request = _officer_recycle_request(db, recycler)

        with pytest.raises(RuntimeError):
            await officers_router.recycle_project_officer(request, "p")
