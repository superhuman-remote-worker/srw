"""Dedicated Pod deletion must fence the next durable turn before HTTP drain."""

import logging
import os
import subprocess
from dataclasses import replace
from types import SimpleNamespace
from uuid import uuid4

import pytest

from agent.api.session_contract import TerminationAdmissionClosed
from agent.api.session_input import SessionInputRuntime
from agent.api.session_termination import SessionTerminationCoordinator
from agent.database.postgres_db import PostgresDB as AgentPostgresDB
from tests import test_persistent_recycler_real_postgres as fixtures
from tests.test_agent_provisioner import _bare_provisioner_for_manifest
from tests.test_session_input_runtime import _World, _runtime

pg_dsn = fixtures.pg_dsn
_schema_applied = fixtures._schema_applied
db = fixtures.db


def _manifest(*, purpose="session", thread_id=None, generation=None, attempt=None):
    return _bare_provisioner_for_manifest()._build_pod_manifest(
        pod_name="srw-agent-s-prestop-proof",
        purpose=purpose,
        thread_id=thread_id,
        config_name="persistent_defaults",
        cpu_request="100m",
        memory_request="256Mi",
        cpu_limit="1",
        memory_limit="2Gi",
        session_runtime_generation=generation,
        provision_attempt=attempt,
    )


@pytest.mark.asyncio
async def test_dedicated_prestop_fences_queued_input_before_failed_http_drain(
    db, pg_dsn, monkeypatch, tmp_path
):
    ids = await fixtures._seed(
        db, protected_agent_pod=True, workspace_claim=False, pod_uid=str(uuid4())
    )
    thread = await db.get_thread(ids["thread"])
    actor = await db.get_agent(ids["agent"])
    generation = str(thread["runtime_generation"])
    store = AgentPostgresDB(pg_dsn, min_connections=1, max_connections=2)
    await store.connect()
    coordinator = SessionTerminationCoordinator(
        SimpleNamespace(),
        logger=logging.getLogger(__name__),
        termination_queue_sentinel=object(),
    )
    sentinel = tmp_path / "srw-persistent-terminating"
    coordinator.termination_sentinel_path = sentinel
    world = _World(
        thread_id=ids["thread"],
        process_generation=str(uuid4()),
        session_generation=generation,
        attach_token=ids["attach_token"],
        agent_id=ids["agent"],
        pod_uid=actor["pod_uid"],
    )
    ports = replace(
        _runtime(world)._ports,
        session=lambda: SimpleNamespace(postgres_conn=store, turn_count=1),
        runtime_admission_closed=coordinator.termination_admission_closed,
    )
    runtime = SessionInputRuntime(ports)
    runtime.begin_attach()
    runtime.open_queue()
    try:
        first = await runtime.accept("already executing", delivery_id=str(uuid4()))
        second = await runtime.accept("queued next turn", delivery_id=str(uuid4()))
        assert await runtime.admit_delivery(
            first.delivery_id, first.claim_generation, 1
        )
        manifest = _manifest(
            thread_id=ids["thread"],
            generation=generation,
            attempt=ids["provision_attempt"],
        )
        container = manifest["spec"]["containers"][0]
        hook = container.get("lifecycle", {}).get("preStop", {}).get("exec", {})
        command = hook.get("command")
        assert command, "dedicated persistent Pod has no preStop admission fence"
        assert manifest["spec"]["terminationGracePeriodSeconds"] == 180
        assert command[:2] == ["sh", "-c"]
        assert command[2] == (
            ": > /tmp/srw-persistent-terminating; "
            "exec python -m src.api.persistent_termination"
        )
        # Execute the actual generated shell, substituting only its isolated
        # /tmp path. The Python/HTTP drain fails; shell-first refusal must hold.
        stub = tmp_path / "python"
        stub.write_text(
            "#!/bin/sh\n"
            f'test -f "{sentinel}" || exit 23\n'
            f': > "{tmp_path / "python-observed-sentinel"}"\n'
            "exit 1\n"
        )
        stub.chmod(0o755)
        result = subprocess.run(
            [
                *command[:2],
                command[2].replace("/tmp/srw-persistent-terminating", str(sentinel)),
            ],
            env={**os.environ, "PATH": f"{tmp_path}:{os.environ['PATH']}"},
            capture_output=True,
            timeout=5,
        )
        assert result.returncode == 1
        assert (tmp_path / "python-observed-sentinel").exists()
        assert coordinator.termination_admission_closed()
        assert not await runtime.admit_delivery(
            second.delivery_id, second.claim_generation, 2
        )
        with pytest.raises(TerminationAdmissionClosed):
            await runtime.accept("late input", delivery_id=str(uuid4()))
        assert await runtime.settle_delivery(first.delivery_id, first.claim_generation)
        rows = await db.fetch(
            "SELECT state,admitted_turn_number,settled_at FROM thread_input_deliveries "
            "WHERE thread_id=$1::uuid ORDER BY persisted_at",
            ids["thread"],
        )
        assert [row["state"] for row in rows] == ["settled", "queued"]
        assert [row["admitted_turn_number"] for row in rows] == [1, None]
        assert rows[0]["settled_at"] is not None and rows[1]["settled_at"] is None
        current = await db.get_thread(ids["thread"])
        assert str(current["runtime_generation"]) == generation
        assert str(current["runtime_attach_token"]) == ids["attach_token"]
        assert str(current["agent_id"]) == ids["agent"]
        assert current["runtime_retirement_token"] is None
    finally:
        await store.close()


@pytest.mark.parametrize(
    "purpose,thread_id", [("job", None), ("worker", None), ("session", None)]
)
def test_dedicated_hook_does_not_change_worker_or_threadless_pool(purpose, thread_id):
    manifest = _manifest(purpose=purpose, thread_id=thread_id)
    container = manifest["spec"]["containers"][0]
    assert "lifecycle" not in container
    assert manifest["spec"]["terminationGracePeriodSeconds"] == 180
