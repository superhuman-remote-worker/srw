"""The critic's branch handoff calls its repository effect as the application binds it.

``application/completion.py`` hands the verification workflow
``prepare_job_primary_repository_authority`` with the store and the forge
already bound. The handoff passed both again, so the call raised a TypeError,
the completion command was parked after its retries and the parent job never
settled after the critic's verdict. Other tests give the workflow an
``AsyncMock`` for this effect, which accepts any arguments; this one binds a
function with the real one's signature.
"""

from __future__ import annotations

import functools
import inspect
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from orchestrator.services import managed_repository_authority, verification_workflow


def _dependencies(store, forge, prepare):
    transaction = verification_workflow.VerificationTransactionPorts(
        revalidate_datasource_selection=AsyncMock(return_value=([], {})),
        datasource_selection_provenance=AsyncMock(return_value={}),
        resolve_workspace_contract=MagicMock(
            return_value=SimpleNamespace(assigned_backend="sandbox")
        ),
        deep_merge_dicts=MagicMock(side_effect=lambda left, right: left | right),
        is_lite_config_override=MagicMock(return_value=False),
    )
    effects = verification_workflow.VerificationEffectPorts(
        forge=forge,
        notifier=MagicMock(),
        # As application/completion.py binds it: store and forge first.
        prepare_job_repository_authority=functools.partial(prepare, store, forge),
        trigger_dispatch=MagicMock(),
        maybe_wake_session=AsyncMock(),
        kick_session_wake_drain=MagicMock(),
        trigger_curation_final_pass=AsyncMock(),
        set_target_to_autonomy_status=AsyncMock(return_value="completed"),
        escalate_target=AsyncMock(return_value="pending_review"),
        internal_resume_job=AsyncMock(return_value=True),
    )
    return verification_workflow.VerificationDependencies(
        store=store, transaction=transaction, effects=effects
    )


def _store():
    connection = MagicMock()
    connection.execute = AsyncMock(return_value="UPDATE 1")

    @asynccontextmanager
    async def acquire():
        yield connection

    store = MagicMock()
    store.acquire = acquire
    store.bind_job_managed_repository = AsyncMock(return_value=True)
    return store, connection


@pytest.mark.asyncio
async def test_the_handoff_passes_the_bound_effect_the_job_only() -> None:
    seen = []

    async def prepare(postgres_db, gitea_client, job):
        seen.append((postgres_db, gitea_client, job))
        return {
            "repo_name": "job-parent",
            "clean_repo_url": "http://gitea/srw-admin/job-parent.git",
        }

    store, connection = _store()
    forge = SimpleNamespace(
        is_initialized=True, create_branch=AsyncMock(return_value=True)
    )
    target = {"id": "11111111-aaaa-bbbb-cccc-000000000001", "branch_name": "main"}
    critic = {
        "id": "22222222-aaaa-bbbb-cccc-000000000002",
        "config_name": "critic",
        "context": {},
    }

    await verification_workflow.setup_verification_critic_workspace(
        target,
        critic,
        "critic",
        dependencies=_dependencies(store, forge, prepare),
        durable_reconcile=True,
    )

    assert seen == [(store, forge, target)]
    forge.create_branch.assert_awaited_once_with(
        "job-parent", "subjob/22222222/critic", from_branch="main"
    )
    store.bind_job_managed_repository.assert_awaited_once_with(
        critic["id"],
        repo_name="job-parent",
        clean_url="http://gitea/srw-admin/job-parent.git",
    )
    connection.execute.assert_awaited_once()


def test_the_bound_function_takes_the_store_the_forge_and_the_job() -> None:
    """The partial in application/completion.py binds exactly the first two."""
    parameters = list(
        inspect.signature(
            managed_repository_authority.prepare_job_primary_repository_authority
        ).parameters
    )
    assert parameters == ["postgres_db", "gitea_client", "job"]
