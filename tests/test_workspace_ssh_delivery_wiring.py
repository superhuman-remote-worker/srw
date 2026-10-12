"""C1 delivery wiring: each hop that carries or strips workspace_ssh_identities.

Review finding 5: dropping the field from the job start request or the
resume payload, ``pop`` -> ``get`` in the agent, removing the session prune,
the workspace-ready gate or the agent allowlist entry all survived the unit
tests. Each test here fails on one of those mutations.
"""

from __future__ import annotations

from contextlib import ExitStack, asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from orchestrator import main as orch_main
from orchestrator.application import controls as controls_composition
from orchestrator.services import container_provisioner as container_provisioner_module
from orchestrator.services import deployment_gates as deployment_gates_module
from orchestrator.services import grant_enforcement as grant_enforcement_module
from orchestrator.services import (
    job_datasource_selection as job_datasource_selection_module,
)
from orchestrator.services import (
    job_dispatch_credentials as job_dispatch_credentials_module,
)
from orchestrator.services import job_mutation_target as job_mutation_target_module
from orchestrator.services import job_start_bundle as job_start_bundle_module
from orchestrator.services import (
    job_workspace_authority as job_workspace_authority_module,
)
from orchestrator.services import (
    managed_repository_authority as managed_repository_authority_module,
)
from orchestrator.services import runtime_actor as runtime_actor_module
from orchestrator.services import thread_mount_rows as thread_mount_rows_module
from shared import pinned_session_identity as pinned_session_identity_module
from shared.runtime.utils.ssh_key import generate_ed25519_keypair
from tests import _b09_control_seams as control_seams
from tests.test_pr_authority_payload import (
    AGENT_ID,
    DATASOURCE_ID,
    JOB_ID,
    RUNTIME_GENERATION,
    RUNTIME_ID,
    _Client,
    _credential_passthrough,
    _job,
    _repository_row,
    _worker_actor,
)


def _ssh_repository_row() -> dict:
    return {
        **_repository_row(),
        "connection_url": "ssh://git@gitea.test:2222/acme/widget.git",
        "credentials": {
            "auth_method": "ssh",
            "ssh_key": generate_ed25519_keypair().private_key,
        },
    }


def _dispatch_patches(stack: ExitStack, row: dict) -> None:
    for target, name, value in (
        (
            orch_main.app.state.resources.postgres_db,
            "fetchrow",
            AsyncMock(return_value=None),
        ),
        (
            job_datasource_selection_module,
            "resolve_authorized_job_datasources",
            AsyncMock(return_value=[row]),
        ),
        (
            job_start_bundle_module,
            "job_project_repositories",
            AsyncMock(return_value=None),
        ),
        (
            managed_repository_authority_module,
            "authorize_job_repository_transport",
            AsyncMock(return_value=(None, None, None)),
        ),
        (
            deployment_gates_module,
            "is_experts_db_enabled",
            MagicMock(return_value=False),
        ),
        (
            grant_enforcement_module,
            "user_experts_enabled",
            AsyncMock(return_value=False),
        ),
        (
            job_dispatch_credentials_module,
            "inject_dispatch_credentials",
            AsyncMock(side_effect=_credential_passthrough),
        ),
        (
            runtime_actor_module,
            "mint_worker_runtime_actor",
            AsyncMock(return_value=_worker_actor()),
        ),
    ):
        stack.enter_context(patch.object(target, name, value))


@pytest.mark.asyncio
@pytest.mark.parametrize("ssh", [True, False])
async def test_job_start_request_carries_identities_only_when_there_are_some(ssh):
    row = _ssh_repository_row() if ssh else _repository_row()
    with ExitStack() as stack:
        _dispatch_patches(stack, row)
        request = await control_seams.build_job_start_request(_job())

    wire = request.model_dump(mode="json", exclude_none=True)
    if ssh:
        (identity,) = request.workspace_ssh_identities
        assert identity["private_key"] == row["credentials"]["ssh_key"]
        assert "ssh_key" not in request.datasources[0]["credentials"]
        assert "workspace_ssh_identities" in wire
    else:
        # Absent from the wire: a pinned agent from before C1 hashes the
        # projection it parsed and must see the bytes it always saw.
        assert request.workspace_ssh_identities is None
        assert "workspace_ssh_identities" not in wire


@pytest.mark.asyncio
@pytest.mark.parametrize("ssh", [True, False])
async def test_resume_payload_carries_identities_only_when_there_are_some(ssh):
    _Client.posts = []
    row = _ssh_repository_row() if ssh else _repository_row()
    job = _job(status="paused")
    conn = MagicMock()
    conn.fetchrow = AsyncMock(return_value=None)
    conn.execute = AsyncMock()

    @asynccontextmanager
    async def acquire():
        yield conn

    attestation = container_provisioner_module.WorkspaceRuntimeAttestation(
        backing_id=f"k8s-pvc:test:{RUNTIME_ID}",
        workspace_generation=RUNTIME_ID,
        runtime_incarnation=RUNTIME_ID,
        ssh_host_key_fingerprint="SHA256:payload-workspace",
        host="workspace.test",
        pod_ip="10.42.0.17",
        port=30022,
    )
    target = job_mutation_target_module.PinnedJobMutationTarget(
        {"id": AGENT_ID, "status": "ready", "pod_ip": "10.0.0.8", "pod_port": 8080},
        pinned_session_identity_module.PinnedJobRecipient(
            expected_agent_id=AGENT_ID,
            expected_pod_uid=None,
            expected_process_generation=RUNTIME_GENERATION,
            expected_job_id=JOB_ID,
        ),
    )
    resources = orch_main.app.state.resources
    with ExitStack() as stack:
        _dispatch_patches(stack, row)
        for owner, name, value in (
            (
                job_workspace_authority_module,
                "prepare_job_workspace_runtime",
                AsyncMock(return_value=("proceed", job, None)),
            ),
            (
                job_workspace_authority_module,
                "workspace_runtime_unchanged_before_delivery",
                AsyncMock(return_value=True),
            ),
            (
                container_provisioner_module.container_provisioner,
                "attest_workspace_runtime",
                AsyncMock(return_value=attestation),
            ),
            (
                job_workspace_authority_module,
                "pinned_k8s_job_workspace_authority_is_current",
                AsyncMock(return_value=True),
            ),
            (
                controls_composition,
                "prepare_pinned_job_mutation_target",
                AsyncMock(return_value=target),
            ),
            (
                resources.postgres_db,
                "managed_repository_authorities_are_current",
                AsyncMock(return_value=True),
            ),
            (resources.postgres_db, "update_job_status", AsyncMock()),
            (resources.postgres_db, "heartbeat", AsyncMock()),
            (resources.postgres_db, "acquire", acquire),
            (httpx, "AsyncClient", _Client),
            (resources.settings, "completion_commands_enabled", False),
        ):
            stack.enter_context(patch.object(owner, name, value))
        accepted = await control_seams.resume_job_on_agent(
            job,
            {"id": AGENT_ID, "status": "ready", "pod_ip": "10.0.0.8", "pod_port": 8080},
        )

    assert accepted is True
    (posted,) = _Client.posts
    assert posted["datasources"][0]["datasource_id"] == DATASOURCE_ID
    if ssh:
        (identity,) = posted["workspace_ssh_identities"]
        assert identity["private_key"] == row["credentials"]["ssh_key"]
        assert "ssh_key" not in posted["datasources"][0]["credentials"]
    else:
        assert "workspace_ssh_identities" not in posted


@pytest.mark.asyncio
@pytest.mark.parametrize("ready", [True, False])
async def test_thread_workspace_delivers_identities_only_to_a_ready_workspace(ready):
    from orchestrator.services import (
        stateless_workspace_scheduler as stateless_workspace_scheduler_module,
    )
    from tests.test_stateless_cloud_sync_integration import (
        _internal_workspace_response_for_lite_thread,
        _stateless_sandbox_thread,
    )

    identity = {"authority_id": DATASOURCE_ID, "private_key": "connector-key"}
    thread = _stateless_sandbox_thread()
    if not ready:
        thread["metadata"]["workspace_container"]["_snapshot_restore_required"] = True
    with (
        patch.object(
            thread_mount_rows_module,
            "resolve_thread_datasource_delivery",
            AsyncMock(return_value=([{"type": "ssh_key", "name": "K"}], [identity])),
        ),
        patch.object(
            container_provisioner_module.container_provisioner,
            "workspace_pod_live",
            AsyncMock(return_value=True),
        ),
        patch.object(
            stateless_workspace_scheduler_module,
            "schedule_stateless_workspace_ensure",
            MagicMock(),
        ),
    ):
        response = await _internal_workspace_response_for_lite_thread(thread)

    assert response["datasources"] == [{"type": "ssh_key", "name": "K"}]
    if ready:
        assert response["status"] == "ready"
        assert response["workspace_ssh_identities"] == [identity]
    else:
        assert response["status"] != "ready"
        assert "workspace_ssh_identities" not in response


@pytest.mark.asyncio
async def test_agent_pops_identities_before_anything_reads_metadata():
    """``pop``, not ``get``: metadata becomes graph state and log context."""
    from agent.agent import UniversalAgent

    class Stop(Exception):
        pass

    agent = object.__new__(UniversalAgent)
    agent._resolve_uploaded_instructions = AsyncMock(return_value=None)
    agent._hydrate_dispatched_config = AsyncMock(side_effect=Stop)
    metadata = {
        "description": "x",
        "workspace_ssh_identities": [{"private_key": "connector-key"}],
    }

    with pytest.raises(Stop):
        await UniversalAgent._setup_job_workspace(agent, JOB_ID, metadata)

    assert "workspace_ssh_identities" not in metadata
    seen = agent._hydrate_dispatched_config.await_args.args[1]
    assert "workspace_ssh_identities" not in seen


def test_agent_accepts_a_neutral_protected_wait_payload_with_the_field():
    """The agent allowlist tolerates the key (None) in a non-ready response."""
    from agent.api import session_workspace

    payload = {
        "status": "creating",
        "protected_cloud": True,
        "protected_cloud_state": "engaging",
        "protected_cloud_error_code": None,
        "managed_repository_credentials": None,
        "workspace_ssh_identities": None,
    }
    assert session_workspace.protected_workspace_delivery(payload) == "engaging"
