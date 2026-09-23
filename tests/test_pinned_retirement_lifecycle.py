"""Exact agent lifecycle projection for an authorized pinned retirement."""

import asyncio
import dataclasses
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest

# R1.B06: this operation moved to services/agent_thread_status.
from orchestrator.services import agent_thread_status  # noqa: E402
import httpx
from fastapi import FastAPI

from agent.api.orchestrator_client import OrchestratorClient

import orchestrator.main as main
from orchestrator.routers import agent_cloud_stage as agent_cloud_stage_routes
from orchestrator.services import pinned_k8s_reconciliation


THREAD_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa1"
AGENT_ID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbb2"
RUNTIME_GENERATION = "11111111-1111-4111-8111-111111111111"
ATTACH_TOKEN = "22222222-2222-4222-8222-222222222222"
RETIREMENT_TOKEN = "33333333-3333-4333-8333-333333333333"
CLAIM_ID = "44444444-4444-4444-8444-444444444444"
CLAIM_ATTEMPT = "55555555-5555-4555-8555-555555555555"
CLAIM_GENERATION = "66666666-6666-4666-8666-666666666666"
# A pinned status write must name the exact registered process; the
# request model refuses an agent_id without it.
PROCESS_GENERATION = "77777777-7777-4777-8777-777777777777"


def _k8s_reconciliation_dependencies(
    db, **provisioners
) -> pinned_k8s_reconciliation.PinnedK8sReconciliationDependencies:
    """Explicit collaborators for the pinned Kubernetes leader loops.

    A provisioner the test does not name is present but unavailable, as an
    unconfigured one is in the application.
    """

    fields = {
        "agent_provisioner": MagicMock(name="agent_provisioner", is_available=False),
        "persistent_provisioner": MagicMock(
            name="persistent_provisioner", is_available=False
        ),
        "container_provisioner": MagicMock(
            name="container_provisioner", is_available=False
        ),
    }
    fields.update(provisioners)
    return pinned_k8s_reconciliation.PinnedK8sReconciliationDependencies(
        store=db, **fields
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("permanent", [False, True])
async def test_authorized_lifecycle_projects_exact_permanent_intent(permanent):
    authorized_at = datetime(2026, 8, 26, tzinfo=timezone.utc)
    pending = {
        "status": "active",
        "agent_id": AGENT_ID,
        "runtime_generation": RUNTIME_GENERATION,
        "runtime_attach_token": ATTACH_TOKEN,
        "runtime_retirement_token": RETIREMENT_TOKEN,
        "runtime_retirement_permanent": permanent,
        "runtime_retirement_authorized_at": authorized_at,
        "runtime_retirement_context": {"settle_status": "ended"},
        "ended_at": None,
    }
    initial = {
        **pending,
        "id": THREAD_ID,
        "execution_lane": "pinned",
    }
    conn = AsyncMock()
    conn.fetchrow = AsyncMock(return_value=pending)

    @asynccontextmanager
    async def acquire():
        yield conn

    request = MagicMock()
    request.headers = {
        "X-Agent-ID": AGENT_ID,
        "X-Session-Runtime-Generation": RUNTIME_GENERATION,
        "X-Session-Runtime-Attach-Token": ATTACH_TOKEN,
    }
    internal = AsyncMock()
    with (
        patch.object(main.postgres_db, "get_thread", AsyncMock(return_value=initial)),
        patch.object(main.postgres_db, "acquire", side_effect=acquire),
    ):
        # ``require_internal`` is a dataclass FIELD DEFAULT on
        # AgentCloudStageDependencies, bound at class-creation time, so
        # patching a module attribute would never be seen. Inject it.
        response = await agent_cloud_stage_routes.agent_get_thread_lifecycle(
            request,
            THREAD_ID,
            dependencies=dataclasses.replace(
                main._agent_cloud_stage_dependencies(), require_internal=internal
            ),
        )
    internal.assert_awaited()

    query = " ".join(conn.fetchrow.await_args.args[0].split())
    assert "t.runtime_retirement_permanent" in query
    assert response["status"] == "ending"
    assert response["runtime_retirement_authorized"] is True
    assert response["retirement_permanent"] is permanent
    assert response["retirement_disposition"] == "ended"
    assert response["session_runtime_retirement_token"] == RETIREMENT_TOKEN


@pytest.mark.asyncio
async def test_hidden_preflight_does_not_advertise_permanent_end_intent():
    pending = {
        "status": "active",
        "agent_id": AGENT_ID,
        "runtime_generation": RUNTIME_GENERATION,
        "runtime_attach_token": ATTACH_TOKEN,
        "runtime_retirement_token": RETIREMENT_TOKEN,
        "runtime_retirement_permanent": True,
        "runtime_retirement_authorized_at": None,
        "runtime_retirement_context": {"settle_status": "ended"},
        "ended_at": None,
    }
    initial = {**pending, "id": THREAD_ID, "execution_lane": "pinned"}
    conn = AsyncMock()
    conn.fetchrow = AsyncMock(return_value=pending)

    @asynccontextmanager
    async def acquire():
        yield conn

    request = MagicMock()
    request.headers = {
        "X-Agent-ID": AGENT_ID,
        "X-Session-Runtime-Generation": RUNTIME_GENERATION,
        "X-Session-Runtime-Attach-Token": ATTACH_TOKEN,
    }
    internal = AsyncMock()
    with (
        patch.object(main.postgres_db, "get_thread", AsyncMock(return_value=initial)),
        patch.object(main.postgres_db, "acquire", side_effect=acquire),
    ):
        # ``require_internal`` is a dataclass FIELD DEFAULT on
        # AgentCloudStageDependencies, bound at class-creation time, so
        # patching a module attribute would never be seen. Inject it.
        response = await agent_cloud_stage_routes.agent_get_thread_lifecycle(
            request,
            THREAD_ID,
            dependencies=dataclasses.replace(
                main._agent_cloud_stage_dependencies(), require_internal=internal
            ),
        )
    internal.assert_awaited()

    assert response["status"] == "active"
    assert response["runtime_retirement_preflight"] is True
    assert response["runtime_retirement_authorized"] is False
    assert response["retirement_permanent"] is False
    assert response["retirement_disposition"] is None
    assert response["session_runtime_retirement_token"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize("through_client", [False, True])
async def test_agent_ending_installs_and_authorizes_retirement_atomically(
    monkeypatch, through_client
):
    """The exact agent endpoint never exposes a crashable hidden T."""

    thread = {
        "id": UUID(THREAD_ID),
        "status": "active",
        "execution_lane": "pinned",
        "agent_id": UUID(AGENT_ID),
        "runtime_generation": UUID(RUNTIME_GENERATION),
        "runtime_attach_token": UUID(ATTACH_TOKEN),
        "runtime_retirement_token": None,
        "metadata": {"dispatch_process_generation": PROCESS_GENERATION},
        "project_id": None,
        "title": "session",
    }
    # The endpoint reads the thread row and then the reciprocal agent row; the
    # latter carries the exact Pod UID and registered process generation.
    pod_uid = "88888888-8888-4888-8888-888888888888"
    monkeypatch.setenv("POD_UID", pod_uid)
    reciprocal_agent = {
        "pod_uid": pod_uid,
        "metadata": {"dispatch_process_generation": PROCESS_GENERATION},
    }
    conn = AsyncMock()
    conn.fetchrow = AsyncMock(side_effect=[thread, reciprocal_agent, thread, thread])
    conn.fetchval = AsyncMock(return_value=1)

    @asynccontextmanager
    async def acquire():
        yield conn

    transaction = MagicMock()
    transaction.__aenter__ = AsyncMock(return_value=None)
    transaction.__aexit__ = AsyncMock(return_value=None)
    conn.transaction = MagicMock(return_value=transaction)
    db = MagicMock()
    db.list_legacy_pinned_agent_k8s_authority_candidates = AsyncMock(return_value=[])
    db.adopt_legacy_pinned_agent_k8s_authority = AsyncMock(return_value=False)
    # 0200 runs the server-owned legacy/warm adoption reconciler before Begin
    # may install T. With nothing to adopt it is a no-op, but the seams must
    # be awaitable or Begin reports an adoption failure instead.
    db.list_legacy_pinned_warm_binding_candidates = AsyncMock(return_value=[])
    db.list_expired_pinned_warm_binding_protections = AsyncMock(return_value=[])
    db.plan_pinned_warm_binding_protection = AsyncMock(return_value=None)
    db.claim_pinned_warm_binding_effect = AsyncMock(return_value=None)
    db.publish_pinned_warm_binding_protection = AsyncMock(return_value=None)
    db.bind_pinned_warm_agent = AsyncMock(return_value=None)
    db.begin_pinned_warm_binding_release = AsyncMock(return_value=None)
    db.complete_pinned_warm_binding_release = AsyncMock(return_value=None)
    db.abort_unmodified_pinned_warm_binding = AsyncMock(return_value=None)
    db.get_pinned_warm_binding_protection = AsyncMock(return_value=None)
    db.get_pinned_warm_binding_candidate = AsyncMock(return_value=None)
    db.get_thread = AsyncMock(return_value=thread)
    db.acquire = MagicMock(side_effect=acquire)
    db.begin_pinned_thread_retirement = AsyncMock(
        return_value={
            "state": "pending",
            "token": RETIREMENT_TOKEN,
            "generation": RUNTIME_GENERATION,
            "permanent": False,
            "authorized_at": datetime(2026, 8, 26, tzinfo=timezone.utc),
        }
    )
    db.authorize_pinned_thread_retirement = AsyncMock()

    with (
        patch.object(main, "require_internal", AsyncMock()),
        patch.object(main, "postgres_db", db),
    ):
        if through_client:
            # R1.B06: the route moved to routers/agent_thread_status. Mount the
            # real router and give the app the factory it resolves through, so
            # this still exercises the wire and the internal gate.
            from orchestrator.routers import agent_thread_status as status_routes

            app = FastAPI()
            app.include_router(status_routes.router)
            app.state.agent_thread_status_dependencies_factory = (
                main._agent_thread_status_dependencies
            )
            client = OrchestratorClient(
                orchestrator_url="http://test",
                pod_ip="127.0.0.1",
                pod_port=8001,
                hostname="test-pinned-agent",
                config_name="session_base",
            )
            client.dispatch_process_generation = PROCESS_GENERATION
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app)
            ) as wire:
                client._client = wire
                response = await client.begin_thread_retirement(
                    THREAD_ID,
                    pinned_agent_id=AGENT_ID,
                    session_runtime_generation=RUNTIME_GENERATION,
                    session_runtime_attach_token=ATTACH_TOKEN,
                    retirement_disposition="ended",
                )
        else:
            response = await agent_thread_status.update_thread_status(
                THREAD_ID,
                main.AgentThreadStatusRequest(
                    status="ending",
                    agent_id=AGENT_ID,
                    process_generation=PROCESS_GENERATION,
                    pod_uid=pod_uid,
                    session_runtime_generation=RUNTIME_GENERATION,
                    session_runtime_attach_token=ATTACH_TOKEN,
                    retirement_disposition="ended",
                ),
                dependencies=main._agent_thread_status_dependencies(),
            )

    assert response == {
        "status": "ending",
        "retirement_disposition": "ended",
        "retirement_permanent": False,
        "session_runtime_retirement_token": RETIREMENT_TOKEN,
    }
    db.begin_pinned_thread_retirement.assert_awaited_once_with(
        THREAD_ID,
        permanent=False,
        settle_status="ended",
        initiator="agent",
        expected_runtime_generation=RUNTIME_GENERATION,
        expected_agent_id=AGENT_ID,
        expected_attach_token=ATTACH_TOKEN,
        authorize_immediately=True,
    )
    db.authorize_pinned_thread_retirement.assert_not_awaited()


def _claim_retirement(*, permanent: bool, status: str, pvc_uid: str | None):
    return {
        "generation": RUNTIME_GENERATION,
        "token": RETIREMENT_TOKEN,
        "permanent": permanent,
        "context": {
            "thread_id": THREAD_ID,
            "generation": RUNTIME_GENERATION,
            "agent_workspace_claim": {
                "claim_id": CLAIM_ID,
                "thread_id": THREAD_ID,
                "created_runtime_generation": CLAIM_GENERATION,
                "create_attempt": CLAIM_ATTEMPT,
                "provisioner": "agent",
                "pvc_name": "pvc-agent-s-aaaaaaaa-aaa",
                "status": status,
                "pvc_uid": pvc_uid,
                "namespace": "agents-a",
                "protection_protocol": "finalizer_v1",
            },
        },
    }


@pytest.mark.asyncio
async def test_soft_retirement_retain_publishes_exact_claim_uid():
    retirement = _claim_retirement(permanent=False, status="planned", pvc_uid=None)
    provider = MagicMock()
    provider.is_available = True
    provider.ensure_agent_workspace_claim = AsyncMock(return_value="retained-pvc-uid")
    with (
        patch.object(main, "agent_provisioner", provider),
        patch.object(
            main.postgres_db,
            "publish_pinned_agent_workspace_claim",
            AsyncMock(return_value=True),
        ) as publish,
    ):
        await main._pinned_retirement_operations().reconcile_agent_workspace_claim_for_retirement(
            retirement
        )

    provider.ensure_agent_workspace_claim.assert_awaited_once_with(
        "pvc-agent-s-aaaaaaaa-aaa",
        expected_thread_id=THREAD_ID,
        expected_runtime_generation=CLAIM_GENERATION,
        expected_claim_id=CLAIM_ID,
        expected_create_attempt=CLAIM_ATTEMPT,
        namespace="agents-a",
        expected_pvc_uid=None,
    )
    publish.assert_awaited_once_with(
        THREAD_ID,
        expected_runtime_generation=RUNTIME_GENERATION,
        expected_retirement_token=RETIREMENT_TOKEN,
        claim_id=CLAIM_ID,
        pvc_name="pvc-agent-s-aaaaaaaa-aaa",
        pvc_uid="retained-pvc-uid",
        namespace="agents-a",
    )


@pytest.mark.asyncio
async def test_permanent_retirement_deletes_original_before_pvc_fence():
    retirement = _claim_retirement(
        permanent=True, status="ready", pvc_uid="original-pvc-uid"
    )
    provider = MagicMock()
    provider.is_available = True
    provider.fence_agent_workspace_claim = AsyncMock(
        side_effect=[
            {"state": "exact_original", "pvc_uid": "original-pvc-uid"},
            {"state": "exact_fence", "pvc_uid": "fence-pvc-uid"},
        ]
    )
    provider.delete_agent_workspace_claim_exact = AsyncMock(return_value=True)
    provider.release_agent_workspace_claim_finalizer_exact = AsyncMock(
        return_value=True
    )
    db = MagicMock()
    db.revoke_pinned_agent_workspace_claim = AsyncMock(return_value=True)
    db.fetchrow = AsyncMock(return_value={"status": "revoking", "pvc_uid": None})
    db.fence_pinned_agent_workspace_claim = AsyncMock(return_value=True)
    db.get_thread = AsyncMock(
        return_value={
            "runtime_generation": RUNTIME_GENERATION,
            "runtime_retirement_token": RETIREMENT_TOKEN,
            "runtime_retirement_permanent": True,
            "runtime_retirement_authorized_at": "authorized",
        }
    )
    db.fetch = AsyncMock(return_value=[])
    with (
        patch.object(main, "agent_provisioner", provider),
        patch.object(main, "postgres_db", db),
        patch.object(main.asyncio, "sleep", AsyncMock()),
    ):
        await main._pinned_retirement_operations().reconcile_agent_workspace_claim_for_retirement(
            retirement
        )

    provider.delete_agent_workspace_claim_exact.assert_awaited_once_with(
        "pvc-agent-s-aaaaaaaa-aaa",
        expected_pvc_uid="original-pvc-uid",
        namespace="agents-a",
    )
    db.fence_pinned_agent_workspace_claim.assert_awaited_once_with(
        THREAD_ID,
        expected_runtime_generation=RUNTIME_GENERATION,
        expected_retirement_token=RETIREMENT_TOKEN,
        expected_claim_id=CLAIM_ID,
        expected_pvc_name="pvc-agent-s-aaaaaaaa-aaa",
        fence_pvc_uid="fence-pvc-uid",
    )


@pytest.mark.asyncio
async def test_permanent_retirement_accepts_exact_claim_already_reclaimed():
    """A later retirement retry treats terminal fence GC as completed."""

    retirement = _claim_retirement(
        permanent=True, status="ready", pvc_uid="original-pvc-uid"
    )
    provider = MagicMock(is_available=True)
    provider.agent_workspace_claim_authority = AsyncMock()
    provider.fence_agent_workspace_claim = AsyncMock()
    provider.delete_agent_workspace_claim_exact = AsyncMock()
    db = MagicMock()
    db.revoke_pinned_agent_workspace_claim = AsyncMock(return_value=True)
    db.fetchrow = AsyncMock(
        return_value={"status": "reclaimed", "pvc_uid": "fence-pvc-uid"}
    )
    db.get_thread = AsyncMock(
        return_value={
            "runtime_generation": RUNTIME_GENERATION,
            "runtime_retirement_token": RETIREMENT_TOKEN,
            "runtime_retirement_permanent": True,
            "runtime_retirement_authorized_at": "authorized",
        }
    )
    db.fetch = AsyncMock(return_value=[])

    with (
        patch.object(main, "agent_provisioner", provider),
        patch.object(main, "postgres_db", db),
    ):
        await main._pinned_retirement_operations().reconcile_agent_workspace_claim_for_retirement(
            retirement
        )

    provider.agent_workspace_claim_authority.assert_not_awaited()
    provider.fence_agent_workspace_claim.assert_not_awaited()
    provider.delete_agent_workspace_claim_exact.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("foreign_kind", [None, "pvc", "pod"])
async def test_restart_reconciler_promotes_only_exact_agent_create(foreign_kind):
    shutdown = asyncio.Event()
    row = {
        "attempt_id": CLAIM_ATTEMPT,
        "thread_id": THREAD_ID,
        "runtime_generation": RUNTIME_GENERATION,
        "provisioner": "agent",
        "pod_name": "srw-agent-s-fence",
        "namespace": "agents-a",
        "protection_protocol": "finalizer_v1",
        "workspace_claim": {
            "claim_id": CLAIM_ID,
            "thread_id": THREAD_ID,
            "created_runtime_generation": CLAIM_GENERATION,
            "create_attempt": CLAIM_ATTEMPT,
            "provisioner": "agent",
            "pvc_name": "pvc-agent-s-aaaaaaaa-aaa",
            "status": "planned",
            "pvc_uid": None,
            "namespace": "agents-a",
            "protection_protocol": "finalizer_v1",
        },
    }
    provider = MagicMock()
    provider.is_available = True
    provider.agent_workspace_claim_authority = AsyncMock(
        return_value=(
            {"state": "replacement", "pvc_uid": None}
            if foreign_kind == "pvc"
            else {"state": "exact_present", "pvc_uid": "observed-pvc-uid"}
        )
    )
    provider.agent_pod_provision_intent_authority = AsyncMock(
        return_value=(
            {"state": "replacement", "pod_uid": None}
            if foreign_kind == "pod"
            else {"state": "exact_present", "pod_uid": "observed-pod-uid"}
        )
    )
    db = MagicMock()

    async def _list(**_kwargs):
        shutdown.set()
        return [row]

    db.list_pinned_agent_create_intents_for_reconcile = AsyncMock(side_effect=_list)
    db.list_legacy_pinned_agent_k8s_authority_candidates = AsyncMock(return_value=[])
    db.adopt_legacy_pinned_agent_k8s_authority = AsyncMock(return_value=False)
    # 0200 runs the server-owned legacy/warm adoption reconciler before Begin
    # may install T. With nothing to adopt it is a no-op, but the seams must
    # be awaitable or Begin reports an adoption failure instead.
    db.list_legacy_pinned_warm_binding_candidates = AsyncMock(return_value=[])
    db.list_expired_pinned_warm_binding_protections = AsyncMock(return_value=[])
    db.plan_pinned_warm_binding_protection = AsyncMock(return_value=None)
    db.claim_pinned_warm_binding_effect = AsyncMock(return_value=None)
    db.publish_pinned_warm_binding_protection = AsyncMock(return_value=None)
    db.bind_pinned_warm_agent = AsyncMock(return_value=None)
    db.begin_pinned_warm_binding_release = AsyncMock(return_value=None)
    db.complete_pinned_warm_binding_release = AsyncMock(return_value=None)
    db.abort_unmodified_pinned_warm_binding = AsyncMock(return_value=None)
    db.get_pinned_warm_binding_protection = AsyncMock(return_value=None)
    db.get_pinned_warm_binding_candidate = AsyncMock(return_value=None)
    db.publish_pinned_agent_workspace_claim = AsyncMock(return_value=True)
    db.publish_pinned_agent_pod_provision_intent = AsyncMock(return_value=True)
    await pinned_k8s_reconciliation.pinned_agent_create_intent_reconciler(
        shutdown,
        dependencies=_k8s_reconciliation_dependencies(db, agent_provisioner=provider),
    )

    if foreign_kind == "pvc":
        db.publish_pinned_agent_workspace_claim.assert_not_awaited()
        provider.agent_pod_provision_intent_authority.assert_not_awaited()
        db.publish_pinned_agent_pod_provision_intent.assert_not_awaited()
        return
    db.publish_pinned_agent_workspace_claim.assert_awaited_once_with(
        THREAD_ID,
        expected_runtime_generation=RUNTIME_GENERATION,
        claim_id=CLAIM_ID,
        pvc_name="pvc-agent-s-aaaaaaaa-aaa",
        pvc_uid="observed-pvc-uid",
        namespace="agents-a",
    )
    if foreign_kind == "pod":
        db.publish_pinned_agent_pod_provision_intent.assert_not_awaited()
    else:
        db.publish_pinned_agent_pod_provision_intent.assert_awaited_once_with(
            THREAD_ID,
            expected_runtime_generation=RUNTIME_GENERATION,
            attempt_id=CLAIM_ATTEMPT,
            pod_name="srw-agent-s-fence",
            pod_uid="observed-pod-uid",
            namespace="agents-a",
        )


@pytest.mark.asyncio
async def test_leader_adopts_exact_legacy_authority_before_create_reconcile():
    shutdown = asyncio.Event()
    observed_at = datetime(2026, 8, 27, tzinfo=timezone.utc)
    row = {
        "attempt_id": CLAIM_ATTEMPT,
        "thread_id": THREAD_ID,
        "runtime_generation": RUNTIME_GENERATION,
        "provisioner": "persistent",
        "pod_name": "persistent-aaaaaaaa-aaa",
        "status": "published",
        "pod_uid": "legacy-pod-uid",
        "workspace_claim": None,
    }
    evidence = {
        "namespace": "agents-old",
        "pod_uid": "legacy-pod-uid",
        "pod_resource_version": "19",
        "pvc_uid": None,
        "pvc_resource_version": None,
        "protection_finalizer": "srw.io/pinned-authority-protection",
        "evidence_protocol": "exact_live_finalizer_v1",
        "observed_at": observed_at,
    }
    provider = MagicMock()
    provider.is_available = True
    provider.protect_legacy_pinned_agent_authority = AsyncMock(return_value=evidence)
    db = MagicMock()
    # The leader now reconciles warm-pool protection before legacy create
    # authority. Keep this fake production-shaped so an unexpected pass cannot
    # turn the test into the reconciler's normal retry loop.
    db.list_legacy_pinned_warm_binding_candidates = AsyncMock(return_value=[])
    db.list_expired_pinned_warm_binding_protections = AsyncMock(return_value=[])
    db.list_legacy_pinned_agent_k8s_authority_candidates = AsyncMock(return_value=[row])
    db.adopt_legacy_pinned_agent_k8s_authority = AsyncMock(return_value=True)
    # 0200 runs the server-owned legacy/warm adoption reconciler before Begin
    # may install T. With nothing to adopt it is a no-op, but the seams must
    # be awaitable or Begin reports an adoption failure instead.
    db.list_legacy_pinned_warm_binding_candidates = AsyncMock(return_value=[])
    db.list_expired_pinned_warm_binding_protections = AsyncMock(return_value=[])
    db.plan_pinned_warm_binding_protection = AsyncMock(return_value=None)
    db.claim_pinned_warm_binding_effect = AsyncMock(return_value=None)
    db.publish_pinned_warm_binding_protection = AsyncMock(return_value=None)
    db.bind_pinned_warm_agent = AsyncMock(return_value=None)
    db.begin_pinned_warm_binding_release = AsyncMock(return_value=None)
    db.complete_pinned_warm_binding_release = AsyncMock(return_value=None)
    db.abort_unmodified_pinned_warm_binding = AsyncMock(return_value=None)
    db.get_pinned_warm_binding_protection = AsyncMock(return_value=None)
    db.get_pinned_warm_binding_candidate = AsyncMock(return_value=None)

    async def _list_create(**_kwargs):
        shutdown.set()
        return []

    db.list_pinned_agent_create_intents_for_reconcile = AsyncMock(
        side_effect=_list_create
    )
    await asyncio.wait_for(
        pinned_k8s_reconciliation.pinned_agent_create_intent_reconciler(
            shutdown,
            dependencies=_k8s_reconciliation_dependencies(
                db, persistent_provisioner=provider
            ),
        ),
        timeout=1,
    )

    provider.protect_legacy_pinned_agent_authority.assert_awaited_once_with(row)
    db.adopt_legacy_pinned_agent_k8s_authority.assert_awaited_once_with(
        THREAD_ID,
        expected_runtime_generation=RUNTIME_GENERATION,
        attempt_id=CLAIM_ATTEMPT,
        namespace="agents-old",
        pod_uid="legacy-pod-uid",
        pod_resource_version="19",
        pvc_uid=None,
        pvc_resource_version=None,
        protection_finalizer="srw.io/pinned-authority-protection",
        evidence_protocol="exact_live_finalizer_v1",
        observed_at=observed_at,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("replacement", [False, True])
async def test_post_horizon_pod_fence_gc_is_uid_exact(replacement):
    shutdown = asyncio.Event()
    row = {
        "resource_kind": "pod",
        "authority_id": CLAIM_ATTEMPT,
        "thread_id": THREAD_ID,
        "runtime_generation": RUNTIME_GENERATION,
        "create_attempt": CLAIM_ATTEMPT,
        "provisioner": "agent",
        "resource_name": "srw-agent-s-fence",
        "resource_uid": "fence-pod-uid",
        "namespace": "agents-a",
        "protection_protocol": "finalizer_v1",
    }
    provider = MagicMock()
    provider.is_available = True
    provider.agent_pod_provision_intent_authority = AsyncMock(
        side_effect=(
            [{"state": "replacement", "pod_uid": None}]
            if replacement
            else [
                {"state": "exact_fence", "pod_uid": "fence-pod-uid"},
                {"state": "exact_absent", "pod_uid": None},
            ]
        )
    )
    provider.delete_agent_pod_exact = AsyncMock(return_value=True)
    provider.release_agent_pod_finalizer_exact = AsyncMock(return_value=True)
    db = MagicMock()

    async def _list(**_kwargs):
        shutdown.set()
        return [row]

    db.list_due_pinned_k8s_create_fences = AsyncMock(side_effect=_list)
    db.complete_pinned_k8s_create_fence_gc = AsyncMock(return_value=True)
    await pinned_k8s_reconciliation.pinned_k8s_create_fence_gc_sweeper(
        shutdown,
        dependencies=_k8s_reconciliation_dependencies(db, agent_provisioner=provider),
    )

    if replacement:
        provider.delete_agent_pod_exact.assert_not_awaited()
        db.complete_pinned_k8s_create_fence_gc.assert_not_awaited()
    else:
        provider.delete_agent_pod_exact.assert_awaited_once_with(
            "srw-agent-s-fence",
            expected_pod_uid="fence-pod-uid",
            namespace="agents-a",
        )
        db.complete_pinned_k8s_create_fence_gc.assert_awaited_once_with(
            resource_kind="pod",
            authority_id=CLAIM_ATTEMPT,
            expected_resource_uid="fence-pod-uid",
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("replacement", [False, True])
async def test_post_horizon_pvc_fence_gc_is_uid_exact(replacement):
    shutdown = asyncio.Event()
    row = {
        "resource_kind": "pvc",
        "authority_id": CLAIM_ID,
        "thread_id": THREAD_ID,
        "runtime_generation": CLAIM_GENERATION,
        "create_attempt": CLAIM_ATTEMPT,
        "provisioner": "persistent",
        "resource_name": "pvc-persistent-aaaaaaaa-aaa",
        "resource_uid": "fence-pvc-uid",
        "namespace": "agents-a",
        "protection_protocol": "finalizer_v1",
    }
    provider = MagicMock()
    provider.is_available = True
    provider.agent_workspace_claim_authority = AsyncMock(
        side_effect=(
            [{"state": "replacement", "pvc_uid": None}]
            if replacement
            else [
                {"state": "exact_fence", "pvc_uid": "fence-pvc-uid"},
                {"state": "exact_absent", "pvc_uid": None},
            ]
        )
    )
    provider.delete_agent_workspace_claim_exact = AsyncMock(return_value=True)
    provider.release_agent_workspace_claim_finalizer_exact = AsyncMock(
        return_value=True
    )
    db = MagicMock()

    async def _list(**_kwargs):
        shutdown.set()
        return [row]

    db.list_due_pinned_k8s_create_fences = AsyncMock(side_effect=_list)
    db.complete_pinned_k8s_create_fence_gc = AsyncMock(return_value=True)
    await pinned_k8s_reconciliation.pinned_k8s_create_fence_gc_sweeper(
        shutdown,
        dependencies=_k8s_reconciliation_dependencies(
            db, persistent_provisioner=provider
        ),
    )

    if replacement:
        provider.delete_agent_workspace_claim_exact.assert_not_awaited()
        db.complete_pinned_k8s_create_fence_gc.assert_not_awaited()
    else:
        provider.delete_agent_workspace_claim_exact.assert_awaited_once_with(
            "pvc-persistent-aaaaaaaa-aaa",
            expected_pvc_uid="fence-pvc-uid",
            namespace="agents-a",
        )
        db.complete_pinned_k8s_create_fence_gc.assert_awaited_once_with(
            resource_kind="pvc",
            authority_id=CLAIM_ID,
            expected_resource_uid="fence-pvc-uid",
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("replacement", [False, True])
async def test_post_horizon_workspace_fence_gc_retires_only_after_exact_delete(
    replacement,
):
    shutdown = asyncio.Event()
    row = {
        "attempt_id": CLAIM_ATTEMPT,
        "thread_id": THREAD_ID,
        "runtime_generation": RUNTIME_GENERATION,
        "namespace": "captured-namespace",
        "pod_name": "ws-thread-aaaaaaaa-aaa",
        "fence_pod_uid": "fence-pod-uid",
        "fence_pvc_uid": "fence-pvc-uid",
        "fence_configmap_uid": None,
        "fence_service_uid": "fence-service-uid",
    }
    provider = MagicMock()
    provider.is_available = True
    provider.delete_pinned_workspace_provision_fences_exact = AsyncMock(
        return_value=not replacement
    )
    db = MagicMock()
    db.list_due_pinned_k8s_create_fences = AsyncMock(return_value=[])

    async def _list_workspace(**_kwargs):
        shutdown.set()
        return [row]

    db.list_pinned_thread_workspace_provision_fences_for_gc = AsyncMock(
        side_effect=_list_workspace
    )
    db.retire_pinned_thread_workspace_provision_fence = AsyncMock(return_value=True)
    await pinned_k8s_reconciliation.pinned_k8s_create_fence_gc_sweeper(
        shutdown,
        dependencies=_k8s_reconciliation_dependencies(
            db, container_provisioner=provider
        ),
    )

    provider.delete_pinned_workspace_provision_fences_exact.assert_awaited_once_with(
        row
    )
    if replacement:
        db.retire_pinned_thread_workspace_provision_fence.assert_not_awaited()
    else:
        db.retire_pinned_thread_workspace_provision_fence.assert_awaited_once_with(
            CLAIM_ATTEMPT,
            expected_fence_pod_uid="fence-pod-uid",
            expected_fence_pvc_uid="fence-pvc-uid",
            expected_fence_configmap_uid=None,
            expected_fence_service_uid="fence-service-uid",
        )
