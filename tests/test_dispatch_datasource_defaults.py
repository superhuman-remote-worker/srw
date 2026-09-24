"""Inheritance is for delegation; project defaults are for dispatch.

``create_job``'s connector branch used to read "does this job have a thread or a
parent?" and, if so, inherit. That folded two different things together:

* A **delegated** subjob (critic, curator, pre-job scholar, a legacy delegation child)
  must never exceed its parent's connectors. Inheritance is the containment
  rule, and it stays.
* A thread that **commissions** fresh project work — an officer, a session — is
  not delegating its own charge. It only landed in the inheritance branch
  because it happens to be a thread.

Because that branch sat *above* the defaults branch, ``use_datasource_defaults``
was unreachable for every thread-originated job. The agent surface has always
sent the flag on omission (``orch_surface/client.py``); the server just never
looked at it.

What that cost, live on Better Resavio 2026-08-15: the officer's own thread was
created without a selection, which persists as ``datasource_ids: []`` (origin
``omitted_compat``). Every job he commissioned inherited the empty list, so his
workers came up with no ``repos/KurortEngine/`` checkout and no clone/commit/push
capability. He then — correctly — de-armed the only actionable ticket and idled
all night rather than dispatch a tester against a candidate that could not exist.
One absent field, a whole watch spent asleep.

Related: knowledge-base/knowledge/issues/commissioned_officer_boots_without_a_job_surface.md, which
is the same defect shape twice over (``config_name``, ``permission_mode``).
"""

from __future__ import annotations

import uuid
from contextlib import ExitStack
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

USER_ID = str(uuid.uuid4())
THREAD_ID = str(uuid.uuid4())
PARENT_JOB_ID = str(uuid.uuid4())
JOB_ID = str(uuid.uuid4())
PROJECT_ID = str(uuid.uuid4())

REPO_DS = str(uuid.uuid4())
PARENT_DS = str(uuid.uuid4())


@pytest.fixture
def fake_request():
    return SimpleNamespace(headers={"X-Internal-Key": "secret"}, query_params={})


@pytest.fixture
def db():
    db = MagicMock()
    db.get_thread = AsyncMock(
        return_value={"id": THREAD_ID, "user_id": USER_ID, "project_id": PROJECT_ID}
    )
    db.get_user = AsyncMock(return_value={"id": USER_ID, "is_admin": False})
    db.get_project = AsyncMock(return_value={"id": PROJECT_ID})
    db.create_job = AsyncMock(return_value={"id": JOB_ID, "status": "created"})
    db.get_job = AsyncMock(
        return_value={"id": PARENT_JOB_ID, "user_id": USER_ID, "project_id": PROJECT_ID}
    )
    db.get_datasource = AsyncMock(return_value=None)
    db.link_datasource_to_job = AsyncMock()
    return db


def _patched(db, *, inherited, defaults):
    """Stub the collaborators around the connector branch, not the branch."""
    return [
        patch("orchestrator.main.app.state.resources.postgres_db", db),
        patch(
            "orchestrator.application.access.enforce_readiness_gate",
            AsyncMock(return_value=None),
        ),
        patch(
            "orchestrator.services.thread_mount_rows.thread_project_ids",
            AsyncMock(return_value=[PROJECT_ID]),
        ),
        patch(
            "orchestrator.services.thread_project_authorization.revalidate_thread_project_ids",
            # R1.B12: the owner is bound with its ``dependencies=``.
            AsyncMock(side_effect=lambda _thread, project_ids, **_kw: project_ids),
        ),
        patch(
            "orchestrator.application.jobs.require_job_project_access",
            AsyncMock(return_value=None),
        ),
        patch(
            "orchestrator.services.deployment_gates.is_experts_db_enabled",
            MagicMock(return_value=False),
        ),
        patch(
            "orchestrator.services.job_datasource_selection.inherit_parent_datasource_ids",
            inherited,
        ),
        patch(
            "orchestrator.services.thread_datasource_authorization.authorize_thread_datasource_selection",
            AsyncMock(side_effect=lambda _actor, ids, **_kw: (list(ids), {})),
        ),
        patch(
            "orchestrator.services.datasource_policy.default_datasource_selection",
            defaults,
        ),
        patch(
            "orchestrator.services.grant_enforcement.enforce_job_create_grants",
            AsyncMock(return_value=None),
        ),
        patch("orchestrator.services.job_provisioning.provision_job_repo", AsyncMock()),
        patch(
            "orchestrator.services.subjob_completion.spawn_scholar_subjob",
            AsyncMock(return_value=None),
        ),
        patch("orchestrator.services.job_dispatcher.trigger_dispatch", MagicMock()),
    ]


async def _create(db, fake_request, body, *, inherited, defaults):
    import orchestrator.security.access as access_module
    from tests._b09_control_seams import create_job

    with ExitStack() as stack:
        stack.enter_context(patch.object(access_module, "_INTERNAL_KEY", "secret"))
        for p in _patched(db, inherited=inherited, defaults=defaults):
            stack.enter_context(p)
        await create_job(fake_request, body)
    return db.create_job.await_args.kwargs["datasource_selection_provenance"]


def _stubs():
    return (
        AsyncMock(return_value=[PARENT_DS]),
        AsyncMock(return_value=([REPO_DS], {REPO_DS: 1})),
    )


class TestDispatchResolvesProjectDefaults:
    @pytest.mark.asyncio
    async def test_a_thread_dispatch_gets_the_projects_auto_attach_defaults(
        self, db, fake_request
    ):
        """The officer's failing case, pinned.

        He passes no connectors, the client asks for defaults, and the job must
        come up with the project's repository attached — not with the empty
        list his own post happens to carry.
        """
        from orchestrator.schemas.job_create import JobCreate

        inherited, defaults = _stubs()
        selection = await _create(
            db,
            fake_request,
            JobCreate(
                description="publish the demo",
                thread_id=THREAD_ID,
                project_id=PROJECT_ID,
                use_datasource_defaults=True,
            ),
            inherited=inherited,
            defaults=defaults,
        )

        assert selection["origin"] == "default"
        assert selection["datasource_ids"] == [REPO_DS]
        inherited.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_thread_that_asks_for_nothing_still_inherits(
        self, db, fake_request
    ):
        """The flag is the opt-in, so silence keeps the old contract."""
        from orchestrator.schemas.job_create import JobCreate

        inherited, defaults = _stubs()
        selection = await _create(
            db,
            fake_request,
            JobCreate(
                description="quiet",
                thread_id=THREAD_ID,
                project_id=PROJECT_ID,
            ),
            inherited=inherited,
            defaults=defaults,
        )

        assert selection["origin"] == "inherited"
        defaults.assert_not_awaited()


class TestDelegationStillInherits:
    @pytest.mark.asyncio
    async def test_a_parented_subjob_inherits_even_when_defaults_are_requested(
        self, db, fake_request
    ):
        """Containment beats convenience.

        A critic, curator, pre-job scholar or legacy delegation child must never
        acquire a connector its parent did not have. If the defaults flag could
        reach a parented job it would become a quiet capability escalation:
        "attach whatever the project offers" applied to work that was scoped
        deliberately narrower.
        """
        from orchestrator.schemas.job_create import JobCreate

        inherited, defaults = _stubs()
        selection = await _create(
            db,
            fake_request,
            JobCreate(
                description="verify the claim",
                thread_id=THREAD_ID,
                parent_job_id=PARENT_JOB_ID,
                project_id=PROJECT_ID,
                use_datasource_defaults=True,
            ),
            inherited=inherited,
            defaults=defaults,
        )

        assert selection["origin"] == "inherited"
        assert selection["datasource_ids"] == [PARENT_DS]
        defaults.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_an_explicit_selection_still_wins_over_the_defaults_flag(
        self, db, fake_request
    ):
        """Presence — not truthiness — remains the discriminator.

        A reviewed array from the cockpit, including a deliberate ``[]``, is an
        instruction. Nothing here may quietly re-attach connectors underneath it.
        """
        from orchestrator.schemas.job_create import JobCreate

        inherited, defaults = _stubs()
        selection = await _create(
            db,
            fake_request,
            JobCreate(
                description="exactly these",
                thread_id=THREAD_ID,
                project_id=PROJECT_ID,
                datasource_ids=[],
            ),
            inherited=inherited,
            defaults=defaults,
        )

        assert selection["origin"] == "explicit"
        assert selection["datasource_ids"] == []
        defaults.assert_not_awaited()
