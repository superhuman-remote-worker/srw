"""A pinned Kubernetes session whose workspace row is not written yet reads as
"pending", which the agent waits on, never as "none", which fails its attach.

knowledge-base/knowledge/issues/protected_session_attach_races_its_workspace_create.md
"""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from agent.api import session_workspace
from orchestrator.services import thread_workspace_delivery as delivery
from orchestrator.services.protected_cloud_engage import (
    _protected_workspace_wait_payload,
)
from orchestrator.services.session_runtime_admission import thread_runtime_is_preparable
from orchestrator.services.workspace_tier_policy import thread_workspace_backend

THREAD = "11111111-1111-4111-8111-111111111111"
AGENT = "22222222-2222-4222-8222-222222222222"
RUNTIME = "33333333-3333-4333-8333-333333333333"
ATTACH = "44444444-4444-4444-8444-444444444444"


def _dependencies(thread, *, available=True, in_cluster=True):
    store = SimpleNamespace(
        get_thread=AsyncMock(side_effect=lambda _id: deepcopy(thread)),
        pinned_thread_agent_is_reciprocal=AsyncMock(return_value=True),
        list_thread_mounts=AsyncMock(return_value=[]),
        managed_repository_authorities_are_current=AsyncMock(return_value=True),
    )
    return delivery.ThreadWorkspaceDeliveryDependencies(
        store=store,
        cloud_router=SimpleNamespace(active=SimpleNamespace(is_initialized=False)),
        gitea_client=SimpleNamespace(),
        container_provisioner=SimpleNamespace(
            is_available=available, in_cluster=in_cluster
        ),
        GrantDenied=RuntimeError,
        LiteWorkspaceConfigError=ValueError,
        backend_from_override=lambda co: co["workspace"]["backend"],
        build_agent_cloud_mount=AsyncMock(return_value=None),
        build_agent_cloud_sync=lambda *a, **kw: None,
        build_protected_cloud_mount=lambda *a, **kw: None,
        cloud_workspace_driver=lambda: "sync",
        grant_violations_detail=lambda values: values,
        inject_lite_workspace_config=lambda co, **kw: co,
        inject_thread_dispatch_credentials=AsyncMock(side_effect=lambda co, **kw: co),
        protected_cloud_delivery_state=AsyncMock(return_value=("ready", None)),
        protected_workspace_wait_payload=_protected_workspace_wait_payload,
        require_pinned_status_identity=lambda: True,
        resolve_session_config=AsyncMock(return_value=None),
        resolve_thread_datasources=AsyncMock(return_value=[]),
        resolve_thread_repositories=AsyncMock(return_value=[]),
        revalidate_thread_project_ids=AsyncMock(return_value=[]),
        ro_mount_matches_protected_selection=lambda *a, **kw: True,
        schedule_stateless_workspace_ensure=lambda *a, **kw: None,
        thread_accepts_runtime=thread_runtime_is_preparable,
        thread_project_ids=AsyncMock(return_value=[]),
        thread_workspace_backend=thread_workspace_backend,
        virtual_workspace_rclone_spec=lambda: None,
        vm_workspaces_on_pod_network=lambda: True,
    )


def _thread(workspace_container=None, *, protected=False):
    metadata = {"config_override": {"workspace": {"backend": "sandbox"}}}
    if protected:
        metadata["protected_cloud"] = True
    if workspace_container is not None:
        metadata["workspace_container"] = workspace_container
    return {
        "id": THREAD,
        "status": "created",
        "execution_lane": "pinned",
        "agent_id": AGENT,
        "runtime_generation": RUNTIME,
        "runtime_attach_token": ATTACH,
        "runtime_retirement_token": None,
        "metadata": metadata,
    }


async def _deliver(dependencies):
    return await delivery.agent_get_thread_workspace_locked(
        THREAD,
        presented_agent_id=AGENT,
        presented_runtime_generation=RUNTIME,
        presented_attach_token=ATTACH,
        dependencies=dependencies,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("row", [None, {}, {"status": "none"}])
async def test_a_missing_pinned_kubernetes_row_reads_as_pending(row):
    thread = _thread(row)
    payload = await _deliver(_dependencies(thread))

    assert payload["status"] == "pending"
    # Response-local: the durable row is untouched.
    stored = thread["metadata"].get("workspace_container")
    assert stored == row


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("available", "in_cluster"), [(False, True), (True, False), (False, False)]
)
async def test_without_in_cluster_kubernetes_a_missing_row_stays_none(
    available, in_cluster
):
    """The Docker pool and an out-of-cluster kubeconfig keep today's answer."""
    payload = await _deliver(
        _dependencies(_thread(), available=available, in_cluster=in_cluster)
    )

    assert payload["status"] == "none"


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["pending", "creating", "deleted", "failed"])
async def test_a_written_row_keeps_its_own_status(status):
    payload = await _deliver(_dependencies(_thread({"status": status})))

    assert payload["status"] == status


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "row",
    [None, {"status": "pending"}, {"status": "creating"}, {"status": "deleted"}],
)
async def test_a_protected_session_waits_while_its_workspace_is_on_its_way(row):
    """Engage done, workspace not ready (a warm agent is bound before the Pod
    exists): the credential-free wait, which the agent polls on. Never a
    "ready" protected state with a workspace that is not ready, which the
    agent's protected check rejects for good."""
    thread = _thread(row, protected=True)
    dependencies = _dependencies(thread)

    payload = await _deliver(dependencies)

    assert payload == _protected_workspace_wait_payload(state="engaging")
    assert session_workspace.protected_workspace_delivery(payload) == "engaging"
    # The wait is decided before any credential is resolved.
    dependencies.resolve_thread_datasources.assert_not_awaited()
    dependencies.inject_thread_dispatch_credentials.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_protected_agent_poll_never_ends_on_the_wait():
    """Through the agent's real protected check: the wait is polled on until
    the budget runs out, never ended by ProtectedCloudUnavailable."""
    early = await _deliver(_dependencies(_thread(None, protected=True)))
    client = SimpleNamespace(get_thread_workspace=AsyncMock(return_value=early))

    result = await session_workspace.poll_workspace_ready(
        client,
        THREAD,
        timeout=0.2,
        poll_interval=0.01,
        session_runtime_generation=None,
    )

    assert result is None
    assert client.get_thread_workspace.await_count > 2


@pytest.mark.asyncio
async def test_the_agent_poll_waits_on_the_answer_until_the_row_is_ready():
    """End to end at the contract: the first read finds no row, a later one a
    ready workspace; the poll returns it instead of failing the attach."""
    early = await _deliver(_dependencies(_thread()))
    ready = {"status": "ready", "pod_ip": "10.42.0.9"}
    client = SimpleNamespace(get_thread_workspace=AsyncMock(side_effect=[early, ready]))

    result = await session_workspace.poll_workspace_ready(
        client,
        THREAD,
        timeout=10,
        poll_interval=0,
        session_runtime_generation=None,
    )

    assert result is not None
    assert result["pod_ip"] == "10.42.0.9"
