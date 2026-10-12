"""Startup abort releases the captured actor before the client is closed."""

from types import SimpleNamespace
from unittest.mock import patch

import pytest

from agent.api import persistent_app as pa
from orchestrator import main
from orchestrator.application import sessions
from orchestrator.services.session_attach_binding import release_session_attach_binding
from tests import test_persistent_recycler_real_postgres as fixtures

pg_dsn = fixtures.pg_dsn
_schema_applied = fixtures._schema_applied
db = fixtures.db


@pytest.mark.asyncio
async def test_shutdown_abort_confirms_real_dedicated_pre_setup_rotation(
    db, monkeypatch
):
    ids = await fixtures._seed(db, protected_agent_pod=True, workspace_claim=False)
    await db.execute(
        "UPDATE threads SET status='created' WHERE id=$1::uuid", ids["thread"]
    )
    await db.execute(
        "UPDATE agents SET status='booting' WHERE id=$1::uuid", ids["agent"]
    )
    generation = str((await db.get_thread(ids["thread"]))["runtime_generation"])
    receipt = dict(
        thread_id=ids["thread"],
        session_runtime_generation=generation,
        session_runtime_attach_token=ids["attach_token"],
        agent_pod_uid="old-pod",
        local_quiescence_protocol="agent_attach_not_started_v1",
    )

    async def release(thread_id, **proof):
        outcome = await release_session_attach_binding(
            ids["agent"],
            thread_id,
            expected_runtime_generation=proof.pop("session_runtime_generation"),
            expected_attach_token=proof.pop("session_runtime_attach_token"),
            expected_agent_pod_uid=proof.pop("agent_pod_uid"),
            **proof,
            dependencies=sessions.session_attach_binding_dependencies(
                main.app.state.resources
            ),
        )
        return outcome in {"released", "already_detached"}

    monkeypatch.setattr(
        pa, "_orchestrator_client", SimpleNamespace(release_thread_agent=release)
    )
    monkeypatch.setattr(pa._session_identity, "_thread_id", ids["thread"])
    monkeypatch.setattr(pa._session_identity, "_session_generation", generation)
    monkeypatch.setattr(pa._session_identity, "_attach_token", ids["attach_token"])
    monkeypatch.setattr(pa._session_attach, "_release_receipt", receipt)
    monkeypatch.setattr(pa._session_attach, "_release_restore_thread_id", None)
    monkeypatch.setattr(pa._session_attach, "_startup_task", None)
    monkeypatch.setattr(pa._session_attach, "_pool_task", None)
    with patch.object(main.app.state.resources, "postgres_db", db):
        assert await pa._session_attach.release_shutdown_receipt(timeout=5)
    current = await db.get_thread(ids["thread"])
    assert str(current["runtime_generation"]) != generation
    assert current["agent_id"] is None
    assert current["runtime_attach_token"] is None
    assert pa._session_attach.release_receipt is None
    assert pa._session_identity.thread_id is None
