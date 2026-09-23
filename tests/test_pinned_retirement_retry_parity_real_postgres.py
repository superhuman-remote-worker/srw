"""The durable pinned-retirement retry versus the owner's retry, with real SQL.

R1.B10's live gate saw a soft-Ended pinned session whose permanent delete
returned the documented 503 fence. The reconciler's durable retry then refused
it every pass ("no process-zero actuator or complete captured authority ...
backend 'virtual'"), while one ordinary owner ``DELETE`` retry settled it at
once. Both reach the same End funnel; they differ only in what the durable
retry demands *before* it calls that funnel.

A same-generation soft settlement is the End funnel's own proof that the life
already reached process zero (``pinned_thread_has_prior_soft_settlement``):
Begin cleared the live owner and receipt, the generation is unchanged and
admission has been closed ever since. The owner's retry accepts it. These
cases pin that the reconciler reaches the same outcome from the same durable
state, and that a permanent retirement *without* that proof stays refused.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest

from orchestrator import main
from orchestrator.services import thread_uploads
from orchestrator.services.agent_provisioner import AgentProvisioner
from orchestrator.services.managed_repository_authority import _deploy_keypair
from orchestrator.services.session_router import SessionRouterService
from tests import test_persistent_recycler_real_postgres as fixtures

db = fixtures.db
pg_dsn = fixtures.pg_dsn
_schema_applied = fixtures._schema_applied

_VIRTUAL_BACKING = "rclone:" + "a" * 64


async def _owner_session(db, monkeypatch, *, backend: str) -> dict[str, str]:
    """One live pinned owner session (not an Officer) on ``backend``.

    ``virtual`` carries a thread repository in ``workspace_container`` exactly
    like a forge-backed harness session: the repository binding is not
    process authority, but it is not the empty shape crash recovery accepts.
    """

    ids = await fixtures._seed(db, protected_agent_pod=True, workspace_claim=False)
    ids.pop("old_access")
    config_override = {
        "officer": {"enabled": False},
        "workspace": {"backend": backend},
    }
    extra: dict = {"config_override": config_override}
    if backend == "virtual":
        repo_name = f"thread-{ids['thread']}"
        repo_url = f"http://gitea:3000/srw/{repo_name}.git"
        # The thread repository is attached only under an active managed
        # authority (0176); provision it the way repository preparation does.
        private_key, public_key, fingerprint = _deploy_keypair()
        authority = await db.reserve_managed_repository_authority(
            repository_owner="srw",
            repo_name=repo_name,
            authority_kind="thread",
            authority_id=ids["thread"],
            project_id=None,
            access_mode="write",
            creation_intent_id=None,
            clean_repo_url=repo_url,
            public_key=public_key,
            public_key_fingerprint=fingerprint,
            private_key=private_key,
        )
        assert await db.activate_managed_repository_authority(
            str(authority["id"]), forge_key_id=91, access_mode="write"
        )
        extra["_workspace_binding"] = {
            "generation": str(uuid4()),
            "kind": "virtual",
            "backing_id": _VIRTUAL_BACKING,
            "ssh_host_key_fingerprint": None,
        }
        extra["workspace_container"] = {
            "repo_name": repo_name,
            "git_remote_url": repo_url,
        }
    async with db.acquire() as conn:
        await conn.execute(
            "DELETE FROM project_officers WHERE thread_id=$1", UUID(ids["thread"])
        )
        await conn.execute(
            "UPDATE threads SET config_name='assistant', "
            "metadata=metadata || $2::jsonb WHERE id=$1",
            UUID(ids["thread"]),
            json.dumps(extra),
        )

    k8s = fixtures.StatefulPinnedK8sApi()
    provider = AgentProvisioner()
    provider._k8s_available = True
    provider._core_api = k8s
    route_core_api = MagicMock()
    route_networking_api = MagicMock()
    route_core_api.read_namespaced_service.side_effect = fixtures._K8sError(404)
    route_networking_api.read_namespaced_ingress.side_effect = fixtures._K8sError(404)
    monkeypatch.setattr(main, "postgres_db", db)
    monkeypatch.setattr(main, "agent_provisioner", provider)
    monkeypatch.setattr(
        main,
        "session_router",
        SessionRouterService(
            namespace="agents-a",
            ingress_host="unused.example",
            core_api=route_core_api,
            networking_api=route_networking_api,
        ),
    )
    # The forge and the virtual backing are external stores the fixture does
    # not run. The repository still goes through the real server-owned
    # revoke/contain/delete path; only the forge's HTTP answers are faked.
    forge = MagicMock()
    forge.repository_owner = "srw"
    forge.is_initialized = True
    forge.delete_repo_deploy_key = AsyncMock(return_value=True)
    forge.delete_repo = AsyncMock(return_value=True)
    monkeypatch.setattr(main, "gitea_client", forge)
    monkeypatch.setattr(
        thread_uploads,
        "purge_attested_pinned_virtual_workspace",
        AsyncMock(return_value=True),
    )
    return ids


async def _soft_end(db, ids: dict[str, str]) -> None:
    """Owner End: Begin, authorize, the live agent's ACK, then settlement."""

    soft = await db.begin_pinned_thread_retirement(ids["thread"], permanent=False)
    assert soft["state"] == "pending"
    await fixtures._authorize_and_ack(db, ids, soft)
    assert await db.settle_pinned_thread_retirement(
        ids["thread"],
        token=soft["token"],
        generation=soft["generation"],
        final_status="ended",
    )
    ended = await db.get_thread(ids["thread"])
    assert ended["status"] == "ended"
    assert ended["agent_id"] is None
    assert ended["runtime_retirement_token"] is None


async def _first_permanent_delete_fails(db, monkeypatch, ids: dict[str, str]) -> None:
    """The owner's first permanent DELETE authorizes, then hits a DB blip.

    Everything up to the final row delete runs through the real End funnel;
    the transient leaves exactly the durable state a 503 leaves in production:
    an authorized, permanent, still-pending marker on an ended thread.
    """

    real_delete = db.delete_thread
    calls = {"n": 0}

    async def _blip_once(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            raise OSError("connection reset by peer")
        return await real_delete(*args, **kwargs)

    monkeypatch.setattr(db, "delete_thread", _blip_once)
    thread = await db.get_thread(ids["thread"])
    with pytest.raises(main.HTTPException) as refused:
        await main._thread_retirement_operations().end_thread_flow(
            ids["thread"], dict(thread), permanent=True, force=False
        )
    assert refused.value.status_code == 503
    pending = await db.get_thread(ids["thread"])
    assert pending is not None
    assert pending["status"] == "ended"
    assert pending["runtime_retirement_permanent"] is True
    assert pending["runtime_retirement_authorized_at"] is not None
    assert await db.pinned_thread_has_prior_soft_settlement(
        ids["thread"],
        runtime_generation=str(pending["runtime_generation"]),
        retirement_token=str(pending["runtime_retirement_token"]),
    )


async def _durable_candidate(db, ids: dict[str, str]) -> dict:
    candidates = await db.list_retryable_pinned_retirements(grace_seconds=0)
    assert [str(candidate["id"]) for candidate in candidates] == [ids["thread"]]
    return candidates[0]


@pytest.mark.asyncio
@pytest.mark.parametrize("backend", ["none", "virtual"])
async def test_durable_retry_settles_a_soft_ended_permanent_delete(
    db, monkeypatch, backend
):
    ids = await _owner_session(db, monkeypatch, backend=backend)
    await _soft_end(db, ids)
    await _first_permanent_delete_fails(db, monkeypatch, ids)

    candidate = await _durable_candidate(db, ids)
    assert await main._retry_pending_pinned_retirement(candidate)
    assert await db.get_thread(ids["thread"]) is None
    assert await db.list_retryable_pinned_retirements(grace_seconds=0) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("backend", ["none", "virtual"])
async def test_owner_retry_settles_the_same_durable_state(db, monkeypatch, backend):
    """Control: the owner's ordinary retry completes from the identical state."""

    ids = await _owner_session(db, monkeypatch, backend=backend)
    await _soft_end(db, ids)
    await _first_permanent_delete_fails(db, monkeypatch, ids)

    thread = await db.get_thread(ids["thread"])
    result = await main._thread_retirement_operations().end_thread_flow(
        ids["thread"], dict(thread), permanent=True, force=False
    )
    assert result == {"status": "deleted"}
    assert await db.get_thread(ids["thread"]) is None


@pytest.mark.asyncio
async def test_durable_retry_keeps_refusing_a_live_permanent_delete_without_proof(
    db, monkeypatch, caplog
):
    """No soft settlement, no receipt, no actuator: the marker stays pending.

    A permanent delete of a *live* virtual session whose agent then went
    offline has neither the same-generation settlement nor a local-quiescence
    receipt, and this backing shape has no crash-recovery actuator. The retry
    must keep refusing it rather than delete a life nobody proved stopped.
    """

    ids = await _owner_session(db, monkeypatch, backend="virtual")
    permanent = await db.begin_pinned_thread_retirement(ids["thread"], permanent=True)
    assert permanent["state"] == "pending"
    assert await db.authorize_pinned_thread_retirement(
        ids["thread"],
        token=permanent["token"],
        generation=permanent["generation"],
        settle_status="ended",
    )
    await db.execute(
        "UPDATE agents SET status='offline' WHERE id=$1::uuid", ids["agent"]
    )
    assert not await db.pinned_thread_has_prior_soft_settlement(
        ids["thread"],
        runtime_generation=permanent["generation"],
        retirement_token=permanent["token"],
    )

    candidate = await _durable_candidate(db, ids)
    caplog.set_level("WARNING")
    assert not await main._retry_pending_pinned_retirement(candidate)
    assert "no process-zero actuator" in caplog.text
    pending = await db.get_thread(ids["thread"])
    assert pending is not None
    assert str(pending["runtime_retirement_token"]) == permanent["token"]
    assert pending["runtime_retirement_local_quiescence"] is None
