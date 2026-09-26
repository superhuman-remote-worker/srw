"""Normal End must record intent while a Session creator observes readiness."""

from __future__ import annotations

import asyncio
from dataclasses import replace
import json
import sys
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

from fastapi import HTTPException, Request
import pytest

from orchestrator import main
from orchestrator.application import controls
from orchestrator.routers import thread_lifecycle
from orchestrator.services import container_provisioner as provider_module
from orchestrator.services import ssh_helpers, thread_retirement
from orchestrator.services.container_provisioner import ContainerProvisioner
from orchestrator.services.manifest_workspace_selection import (
    select_execution_workspace,
)
from orchestrator.services.session_provisioner import ensure_session_workspace
from orchestrator.services.workspace_lifecycle import EnsureOutcome
from tests import test_workspace_pull_failure_real_postgres as pull
from tests.test_pinned_failed_start_end_real_postgres import PinnedPullCluster
from tests.test_pinned_vm_initial_binding_real_postgres import _bind_protected_agent
from tests.test_sandbox_workspace_provisioner import stub_plan_inputs

pg_dsn = pull.pg_dsn
db = pull.db
_schema_applied = pull._schema_applied

IMAGE = "registry.example/active-session:waiting"


class ObservingCluster(PinnedPullCluster):
    def create_namespaced_pod(self, *, body, **kwargs):
        pod = super().create_namespaced_pod(body=body, **kwargs)
        pod.status.container_statuses[0].state.waiting.reason = "ImagePullBackOff"
        return pod


def metadata(row):
    value = row.get("metadata") or {}
    return json.loads(value) if isinstance(value, str) else value


async def setup_case(db, monkeypatch, *, lane, protected_agent):
    actor = dict(
        await db.fetchrow(
            "INSERT INTO users(display_name,is_approved,is_admin) "
            "VALUES('Active Session End owner',TRUE,TRUE) RETURNING *"
        )
    )
    db.manifest_runtime_image = "test.invalid/srw:installed"
    workspace, selection = await select_execution_workspace(
        db,
        actor,
        role="session",
        project_id=None,
        supplied=True,
        workspace={
            "template": {
                "inline": {"backend": "sandbox", "environment": {"image": IMAGE}}
            }
        },
    )
    initial = {"config_override": {"workspace": workspace}}
    if lane == "stateless":
        initial["workspace_container"] = {
            "status": "pending",
            "provisioner": "k8s",
            "_runtime_creation": {
                "generation": str(uuid4()),
                "mode": "create",
                "attempted": False,
                "replaces_uid": None,
            },
        }
    thread_id = await db.create_thread(
        user_id=str(actor["id"]),
        datasource_ids=[],
        execution_lane=lane,
        initial_metadata=initial,
        workspace_selection=selection,
    )
    if protected_agent:
        # The helper runs real plan/effect/protection/bind transitions; only
        # the exact external agent Pod observation is modeled.
        await _bind_protected_agent(db, UUID(thread_id))
    cluster = ObservingCluster()
    provider = ContainerProvisioner()
    provider._db = db
    provider._k8s_available = True
    provider._namespace = "agent-workspaces"
    provider._storage_class = "test-storage"
    provider._pvc_enabled = True
    provider._core_api = cluster
    stub_plan_inputs(monkeypatch, provider)
    for name in ("open_interval", "close_interval"):
        monkeypatch.setattr(
            provider_module.workspace_metering, name, AsyncMock(return_value=None)
        )
    monkeypatch.setattr(main.app.state.resources, "postgres_db", db)
    monkeypatch.setattr(provider_module, "container_provisioner", provider)
    deps = controls.thread_lifecycle_dependencies(main.app.state.resources)

    async def owned_actor(request, store, requested_id):
        assert requested_id == thread_id and store is db
        current = await db.get_thread(thread_id)
        assert str(current["user_id"]) == str(actor["id"])
        return actor, current

    deps = replace(deps, require_thread_owner=owned_actor)
    if protected_agent:

        class IdleAgentTransport:
            def __init__(self, **kwargs):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                return None

            async def post(self, url, *, json):
                assert url.endswith("/session/status")
                return SimpleNamespace(
                    status_code=200,
                    json=lambda: {
                        "recipient_verified": True,
                        "session_identity_fingerprint": json[
                            "session_identity_fingerprint"
                        ],
                        "thread_id": thread_id,
                        "turn_in_flight": False,
                    },
                )

        # Model only the exact recipient's HTTP idle observation. End still
        # verifies its fingerprint and current immutable retirement tuple.
        monkeypatch.setattr(thread_retirement.httpx, "AsyncClient", IdleAgentTransport)
    return SimpleNamespace(
        thread_id=thread_id, cluster=cluster, provider=provider, dependencies=deps
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "creator_active", [False, True], ids=["creator-returned", "creator-active"]
)
@pytest.mark.parametrize(
    "lane,protected_agent,observation,permanent",
    [
        pytest.param("pinned", False, "image", False, id="pinned-soft-image"),
        pytest.param("stateless", False, "image", False, id="stateless-soft-image"),
        pytest.param("pinned", True, "image", False, id="pinned-protected-soft-image"),
        pytest.param("pinned", False, "ssh", False, id="pinned-soft-ssh"),
        pytest.param("stateless", False, "ssh", False, id="stateless-soft-ssh"),
        pytest.param("pinned", False, "image", True, id="pinned-permanent-image"),
        pytest.param("stateless", False, "image", True, id="stateless-permanent-image"),
    ],
)
async def test_normal_end_during_session_readiness(
    db,
    monkeypatch,
    tmp_path,
    lane,
    protected_agent,
    observation,
    permanent,
    creator_active,
):
    case = await setup_case(db, monkeypatch, lane=lane, protected_agent=protected_agent)
    entered, ssh_started = asyncio.Event(), asyncio.Event()
    wait = case.provider._wait_for_ready
    probe_release = tmp_path / "release-ssh"
    processes = []

    async def observed_wait(*args, **kwargs):
        entered.set()
        if observation == "ssh":
            pod = case.cluster.objects["pod"]
            pod.status.phase = "Running"
            for status in pod.status.container_statuses:
                status.ready = True
                status.state = SimpleNamespace(
                    waiting=None, running=SimpleNamespace(), terminated=None
                )
        return await wait(*args, **kwargs)

    monkeypatch.setattr(case.provider, "_wait_for_ready", observed_wait)
    if observation == "ssh":
        spawn = ssh_helpers.create_owned_subprocess_exec

        async def owned_probe(*args, **kwargs):
            proc = await spawn(*args, **kwargs)
            processes.append(proc)
            ssh_started.set()
            return proc

        monkeypatch.setattr(ssh_helpers, "create_owned_subprocess_exec", owned_probe)
        monkeypatch.setattr(
            provider_module, "workspace_private_key_fingerprint", lambda _: "test-key"
        )
        monkeypatch.setattr(
            ssh_helpers,
            "build_agent_ssh_cmd",
            lambda *args, **kwargs: [
                sys.executable,
                "-c",
                "import pathlib,sys,time\np=pathlib.Path(sys.argv[1])\nwhile not p.exists(): time.sleep(0.01)\nraise SystemExit(1)",
                str(probe_release),
            ],
        )
        case.provider._ssh_auth_connect_timeout = 60
        case.provider._ssh_auth_ready_timeout = 0.1
    creator = asyncio.create_task(
        ensure_session_workspace(
            case.thread_id,
            db=db,
            provisioner=case.provider,
            suspension=SimpleNamespace(),
        )
    )
    try:
        await asyncio.wait_for(entered.wait(), 15)
        if observation == "ssh":
            await asyncio.wait_for(ssh_started.wait(), 5)
        assert not creator.done()
        original = await db.get_thread(case.thread_id)
        original_pod = case.cluster.objects["pod"].metadata.uid
        original_pvc = case.cluster.objects["pvc"].metadata.uid
        if lane == "pinned":
            intent = await db.fetchrow(
                "SELECT * FROM thread_workspace_provision_intents WHERE thread_id=$1",
                UUID(case.thread_id),
            )
            assert str(intent["pod_uid"]) == original_pod
            assert str(intent["runtime_generation"]) == str(
                original["runtime_generation"]
            )
            assert str(intent["created_agent_id"] or "") == str(
                original["agent_id"] or ""
            )
            async with db.try_thread_advisory_lock(case.thread_id) as owned:
                assert not owned
        else:
            async with db.stateless_session_workspace_ensure_lock(
                case.thread_id
            ) as owned:
                assert not owned
            async with db.workspace_runtime_mutation_lock(
                case.thread_id,
                owner_kind="thread",
                scope="workspace_container",
                wait=False,
            ) as owned:
                assert not owned
        if not creator_active:
            if observation == "ssh":
                probe_release.touch()
            else:
                case.cluster.objects["pod"].status.container_statuses[
                    0
                ].state.waiting.reason = "InvalidImageName"
            result = await asyncio.wait_for(creator, 5)
            assert result.outcome is EnsureOutcome.FAILED

        started = time.monotonic()
        try:
            response = await asyncio.wait_for(
                thread_lifecycle.end_thread(
                    case.thread_id,
                    Request(
                        {
                            "type": "http",
                            "method": "DELETE",
                            "path": f"/api/persistent/threads/{case.thread_id}",
                        }
                    ),
                    permanent=permanent,
                    force=False,
                    dependencies=case.dependencies,
                ),
                40,
            )
        except HTTPException as exc:
            current = await db.get_thread(case.thread_id)
            md = metadata(current)
            evidence = {
                "lane": lane,
                "protected_agent": protected_agent,
                "observation": observation,
                "creator_active": creator_active,
                "permanent": permanent,
                "elapsed": round(time.monotonic() - started, 3),
                "http": exc.status_code,
                "detail": exc.detail,
                "creator_still_running": not creator.done(),
                "status": current["status"],
                "same_generation": original["runtime_generation"]
                == current["runtime_generation"],
                "same_actor": original["agent_id"] == current["agent_id"],
                "same_attach": original["runtime_attach_token"]
                == current["runtime_attach_token"],
                "retirement_token": current.get("runtime_retirement_token"),
                "retirement_authorized": current.get(
                    "runtime_retirement_authorized_at"
                ),
                "stateless_retirement": md.get("_stateless_claim_retirement"),
                "same_pod": case.cluster.objects.get("pod") is not None
                and case.cluster.objects["pod"].metadata.uid == original_pod,
                "same_pvc": case.cluster.objects.get("pvc") is not None
                and case.cluster.objects["pvc"].metadata.uid == original_pvc,
            }
            pytest.fail(
                "Normal End did not accept intent: " + json.dumps(evidence, default=str)
            )
        assert response["status"] in {"ending", "ended", "deleted"}
        current = await db.get_thread(case.thread_id)
        if current is not None and response["status"] == "ending":
            assert current["runtime_retirement_authorized_at"] is not None
        elif current is not None:
            assert current["status"] == "ended"
        if not permanent and not protected_agent:
            assert case.cluster.objects["pvc"].metadata.uid == original_pvc
        if not creator_active and observation == "ssh":
            assert processes and all(proc.returncode == 1 for proc in processes)
    finally:
        probe_release.touch()
        if observation == "image" and "pod" in case.cluster.objects:
            status = case.cluster.objects["pod"].status.container_statuses[0]
            if status.state.waiting is not None:
                status.state.waiting.reason = "InvalidImageName"
        if not creator.done():
            try:
                await asyncio.wait_for(asyncio.shield(creator), 5)
            except TimeoutError:
                creator.cancel()
        await asyncio.gather(creator, return_exceptions=True)
