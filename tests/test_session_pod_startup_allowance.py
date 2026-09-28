"""A thread-bound session Pod attaches inside startup; its allowance must cover it.

The agent attaches a dedicated session during FastAPI lifespan startup
(``persistent_app.lifespan``), so uvicorn serves no ``/health`` until that
attach completes. A VM-tier attach waits for the VM: the agent's own bounded
budget is ``VM_UPGRADE_POLL_TIMEOUT`` (900 s) and the orchestrator waits
``session_ready_timeout_s("vm")`` (960 s, sized above it) for readiness. A
startup probe that gives up earlier makes the kubelet SIGTERM an attach that
is still progressing. uvicorn defers the signal until startup completes, and
the lifespan shutdown then ends the thread; on the owned KubeVirt gate every
new VM session (boot 2-3.5 min) ended one second after attaching (vault issue
dedicated_vm_session_ended_at_attach_by_startup_probe).

Both session-Pod builders are covered: ``AgentProvisioner`` (new dedicated
sessions) and ``PersistentProvisioner`` (Resume, wake and officers).
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from agent.api import persistent_app
from orchestrator.services import session_workspace_policy
from shared.workspace_preparation_settings import PreparationSettings
from tests.test_agent_provisioner import _fake_to_thread, _make_provisioner
from tests.test_persistent_provisioner import _make_provisioner_with_k8s

THREAD_ID = "11111111-2222-3333-4444-555555555555"
# What every agent Pod already had before its session attach: process start,
# agent initialization and registration (100 s in both builders).
PRE_ATTACH_ALLOWANCE_S = 100


def _thread_metadata(backend, *, preparation=False):
    workspace = {"backend": backend}
    if preparation:
        workspace["vm"] = {"preparation": {"recipe": "fixture"}}
    return {"config_override": {"workspace": workspace}}


def _allowance(pod_body):
    probe = pod_body["spec"]["containers"][0]["startupProbe"]
    assert probe["httpGet"]["path"] == "/health"
    return probe["failureThreshold"] * probe["periodSeconds"]


def _required(backend, *, preparation=False):
    return PRE_ATTACH_ALLOWANCE_S + session_workspace_policy.session_ready_timeout_s(
        backend, preparation=preparation
    )


async def _agent_session_pod(metadata, *, purpose="session"):
    p, _conn = _make_provisioner()
    pods = MagicMock()
    pods.items = []
    p._core_api.list_namespaced_pod.return_value = pods
    body = {}

    def _create(**kwargs):
        body.update(kwargs.get("body", {}))
        created = MagicMock()
        created.metadata.uid = "pod-uid-created"
        return created

    p._core_api.create_namespaced_pod = _create
    thread = await p._db.get_thread(THREAD_ID)
    thread["metadata"] = json.dumps(metadata)

    async def _get_thread(thread_id):
        return dict(thread) if str(thread_id) == THREAD_ID else None

    p._db.get_thread = _get_thread
    with patch(
        "orchestrator.services.agent_provisioner.asyncio.to_thread",
        side_effect=_fake_to_thread,
    ):
        name = await p.provision_agent(
            purpose=purpose, thread_id=THREAD_ID if purpose == "session" else None
        )
    assert name is not None
    return body


async def _persistent_session_pod(metadata):
    p, _ = _make_provisioner_with_k8s()
    p._db.get_thread.return_value = {
        **p._db.get_thread.return_value,
        "metadata": metadata,
    }
    created = MagicMock()
    created.metadata.uid = "pod-uid-new"
    p._core_api.create_namespaced_pod.return_value = created
    not_found = Exception("Not found")
    not_found.status = 404
    ready = MagicMock()
    ready.status.phase = "Running"
    ready.status.pod_ip = "10.0.0.5"
    ready.status.container_statuses = [MagicMock(ready=True)]
    p._core_api.read_namespaced_pod.side_effect = [not_found, ready]

    async def _to_thread(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    with patch(
        "orchestrator.services.persistent_provisioner.asyncio.to_thread",
        side_effect=_to_thread,
    ):
        await p.create_agent_pod(THREAD_ID)
    p._core_api.create_namespaced_pod.assert_called_once()
    return p._core_api.create_namespaced_pod.call_args.kwargs["body"]


def test_the_orchestrator_readiness_budget_exceeds_the_agent_attach_budget():
    """The contract the allowance builds on: the agent gives up first."""

    assert session_workspace_policy.session_ready_timeout_s("vm") > (
        persistent_app._vm_upgrade_poll_timeout
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("backend", "preparation"),
    [("vm", False), ("vm", True), ("sandbox", False)],
)
async def test_dedicated_session_pod_allows_its_attach(backend, preparation):
    body = await _agent_session_pod(_thread_metadata(backend, preparation=preparation))

    allowance = _allowance(body)
    assert allowance >= _required(backend, preparation=preparation)
    if backend == "vm":
        assert (
            allowance > PRE_ATTACH_ALLOWANCE_S + persistent_app._vm_upgrade_poll_timeout
        )
    if preparation:
        assert allowance > PreparationSettings.from_environment().wait_budget


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("backend", "preparation"),
    [("vm", False), ("vm", True), ("sandbox", False)],
)
async def test_persistent_session_pod_allows_its_attach(backend, preparation):
    body = await _persistent_session_pod(
        _thread_metadata(backend, preparation=preparation)
    )

    assert _allowance(body) >= _required(backend, preparation=preparation)


@pytest.mark.asyncio
async def test_job_pod_keeps_the_pre_attach_allowance():
    body = await _agent_session_pod({}, purpose="job")

    probe = body["spec"]["containers"][0]["startupProbe"]
    assert (probe["failureThreshold"], probe["periodSeconds"]) == (100, 1)
