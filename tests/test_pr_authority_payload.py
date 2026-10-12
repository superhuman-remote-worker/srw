"""Server-owned repository datasource identity across runtime payload paths."""

from tests import _b09_control_seams as control_seams

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator import main as orch_main
from shared.runtime_actor import RuntimeActorContext
from orchestrator.services import session_attach_payload
from orchestrator.application import controls as controls_composition
from orchestrator.application import preparation as preparation_composition
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
from orchestrator.services import (
    session_config_resolution as session_config_resolution_module,
)
from orchestrator.services import (
    thread_datasource_authorization as thread_datasource_authorization_module,
)
from orchestrator.services import thread_mount_rows as thread_mount_rows_module
from orchestrator.services import (
    thread_project_authorization as thread_project_authorization_module,
)
from shared import pinned_session_identity as pinned_session_identity_module
import httpx

DATASOURCE_ID = "22222222-2222-4222-8222-222222222222"
FOREIGN_ID = "33333333-3333-4333-8333-333333333333"
JOB_ID = "11111111-1111-4111-8111-111111111111"
PROJECT_ID = "44444444-4444-4444-8444-444444444444"
AGENT_ID = "55555555-5555-4555-8555-555555555555"
RUNTIME_ID = "66666666-6666-4666-8666-666666666666"
RUNTIME_GENERATION = "88888888-8888-4888-8888-888888888888"
RUNTIME_ATTACH_TOKEN = "99999999-9999-4999-8999-999999999999"


def _repository_row() -> dict:
    return {
        "id": DATASOURCE_ID,
        "type": "repository",
        "name": "Widget",
        "description": "Exact attached repository",
        "connection_url": "https://gitea.test/acme/widget.git",
        "credentials": {"auth_method": "token", "token": "test-token"},
        "project_read_only": False,
        "config": {"forge": "gitea"},
        "default_branch": "main",
    }


def _job(*, status: str = "created") -> dict:
    return {
        "id": JOB_ID,
        "description": "publish exact PR",
        "project_id": PROJECT_ID,
        "user_id": None,
        "status": status,
        "priority": 1,
        "config_name": "worker_base",
        "config_override": {"workspace": {"backend": "sandbox"}},
        "context": {
            # Caller-authored lookalikes are not datasource authority.
            "datasource_id": FOREIGN_ID,
            "_workspace_contract": {
                "version": 1,
                "requested_backend": "sandbox",
                "assigned_backend": "sandbox",
                "assignment_source": "test",
            },
            "workspace_container": {
                "status": "ready",
                "host": "workspace.test",
                "port": 30022,
                "provisioner": "k8s",
                "_runtime_incarnation": RUNTIME_ID,
            },
        },
    }


def _worker_actor() -> RuntimeActorContext:
    return RuntimeActorContext(
        caller_kind="worker",
        project_id=PROJECT_ID,
        access_credential="sra_" + ("A" * 43),
        refresh_credential="srr_" + ("B" * 43),
    )


def _credential_passthrough(_job_row, config, **_kwargs):
    return config


@pytest.mark.asyncio
async def test_fresh_dispatch_payload_uses_resolved_repository_uuid():
    with (
        patch.object(
            orch_main.app.state.resources.postgres_db,
            "fetchrow",
            AsyncMock(return_value=None),
        ),
        patch.object(
            job_datasource_selection_module,
            "resolve_authorized_job_datasources",
            AsyncMock(return_value=[_repository_row()]),
        ),
        patch.object(
            job_start_bundle_module,
            "job_project_repositories",
            AsyncMock(return_value=None),
        ),
        patch.object(
            managed_repository_authority_module,
            "authorize_job_repository_transport",
            AsyncMock(return_value=(None, None, None)),
        ),
        patch.object(
            deployment_gates_module, "is_experts_db_enabled", return_value=False
        ),
        patch.object(
            grant_enforcement_module,
            "user_experts_enabled",
            AsyncMock(return_value=False),
        ),
        patch.object(
            job_dispatch_credentials_module,
            "inject_dispatch_credentials",
            AsyncMock(side_effect=_credential_passthrough),
        ),
        patch.object(
            runtime_actor_module,
            "mint_worker_runtime_actor",
            AsyncMock(return_value=_worker_actor()),
        ),
    ):
        request = await control_seams.build_job_start_request(_job())

    assert request is not None
    assert request.datasources[0]["datasource_id"] == DATASOURCE_ID
    assert "id" not in request.datasources[0]
    assert request.context["datasource_id"] == FOREIGN_ID


class _Response:
    status_code = 202
    text = ""


class _Client:
    posts: list[dict] = []

    def __init__(self, *_args, **_kwargs):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False

    async def post(self, _url, json=None):
        self.posts.append(json)
        return _Response()


@pytest.mark.asyncio
async def test_resume_payload_uses_resolved_repository_uuid():
    _Client.posts = []
    job = _job(status="paused")
    conn = MagicMock()
    conn.fetchrow = AsyncMock(return_value=None)
    conn.execute = AsyncMock()

    @asynccontextmanager
    async def acquire():
        yield conn

    with (
        patch.object(
            job_workspace_authority_module,
            "prepare_job_workspace_runtime",
            AsyncMock(return_value=("proceed", job, None)),
        ),
        patch.object(
            job_datasource_selection_module,
            "resolve_authorized_job_datasources",
            AsyncMock(return_value=[_repository_row()]),
        ),
        patch.object(
            job_start_bundle_module,
            "job_project_repositories",
            AsyncMock(return_value=None),
        ),
        patch.object(
            managed_repository_authority_module,
            "authorize_job_repository_transport",
            AsyncMock(return_value=(None, None, None)),
        ),
        patch.object(
            deployment_gates_module, "is_experts_db_enabled", return_value=False
        ),
        patch.object(
            grant_enforcement_module,
            "user_experts_enabled",
            AsyncMock(return_value=False),
        ),
        patch.object(
            job_dispatch_credentials_module,
            "inject_dispatch_credentials",
            AsyncMock(side_effect=_credential_passthrough),
        ),
        patch.object(
            runtime_actor_module,
            "mint_worker_runtime_actor",
            AsyncMock(return_value=_worker_actor()),
        ),
        patch.object(
            job_workspace_authority_module,
            "workspace_runtime_unchanged_before_delivery",
            AsyncMock(return_value=True),
        ),
        patch.object(
            container_provisioner_module.container_provisioner,
            "attest_workspace_runtime",
            AsyncMock(
                return_value=container_provisioner_module.WorkspaceRuntimeAttestation(
                    backing_id=f"k8s-pvc:test:{RUNTIME_ID}",
                    workspace_generation=RUNTIME_ID,
                    runtime_incarnation=RUNTIME_ID,
                    ssh_host_key_fingerprint="SHA256:payload-workspace",
                    host="workspace.test",
                    pod_ip="10.42.0.17",
                    port=30022,
                )
            ),
        ),
        patch.object(
            job_workspace_authority_module,
            "pinned_k8s_job_workspace_authority_is_current",
            AsyncMock(return_value=True),
        ),
        patch.object(
            controls_composition,
            "prepare_pinned_job_mutation_target",
            AsyncMock(
                return_value=job_mutation_target_module.PinnedJobMutationTarget(
                    {
                        "id": AGENT_ID,
                        "status": "ready",
                        "pod_ip": "10.0.0.8",
                        "pod_port": 8080,
                    },
                    pinned_session_identity_module.PinnedJobRecipient(
                        expected_agent_id=AGENT_ID,
                        expected_pod_uid=None,
                        expected_process_generation=RUNTIME_GENERATION,
                        expected_job_id=JOB_ID,
                    ),
                )
            ),
        ),
        patch.object(
            orch_main.app.state.resources.postgres_db,
            "managed_repository_authorities_are_current",
            AsyncMock(return_value=True),
        ),
        patch.object(
            orch_main.app.state.resources.postgres_db, "update_job_status", AsyncMock()
        ),
        patch.object(
            orch_main.app.state.resources.postgres_db, "heartbeat", AsyncMock()
        ),
        patch.object(orch_main.app.state.resources.postgres_db, "acquire", acquire),
        patch.object(httpx, "AsyncClient", _Client),
        patch.object(
            orch_main.app.state.resources.settings, "completion_commands_enabled", False
        ),
    ):
        accepted = await control_seams.resume_job_on_agent(
            job,
            {
                "id": AGENT_ID,
                "status": "ready",
                "pod_ip": "10.0.0.8",
                "pod_port": 8080,
            },
        )

    assert accepted is True
    assert len(_Client.posts) == 1
    assert _Client.posts[0]["datasources"][0]["datasource_id"] == DATASOURCE_ID
    assert "id" not in _Client.posts[0]["datasources"][0]


@pytest.mark.asyncio
async def test_persistent_reattach_payload_uses_resolved_repository_uuid():
    thread = {
        "id": "77777777-7777-4777-8777-777777777777",
        "user_id": None,
        "project_id": PROJECT_ID,
        "status": "active",
        "execution_lane": "pinned",
        "runtime_generation": RUNTIME_GENERATION,
        "agent_id": AGENT_ID,
        "control_admission_agent_id": AGENT_ID,
        "runtime_attach_token": RUNTIME_ATTACH_TOKEN,
        "runtime_retirement_token": None,
        "metadata": {"datasource_ids": [DATASOURCE_ID]},
        "permission_mode": "autonomous",
        "narration_mode": "silent",
    }
    actor = RuntimeActorContext(
        caller_kind="human",
        thread_id=thread["id"],
        project_id=PROJECT_ID,
        access_credential="sra_" + ("C" * 43),
        refresh_credential="srr_" + ("D" * 43),
    )
    with (
        patch.object(
            orch_main.app.state.resources.postgres_db,
            "get_thread",
            AsyncMock(return_value=thread),
        ),
        patch.object(
            thread_mount_rows_module, "thread_project_ids", AsyncMock(return_value=[])
        ),
        patch.object(
            thread_project_authorization_module,
            "revalidate_thread_project_ids",
            AsyncMock(return_value=[]),
        ),
        patch.object(
            thread_datasource_authorization_module,
            "resolve_authorized_thread_datasources",
            AsyncMock(return_value=[_repository_row()]),
        ),
        patch.object(
            session_config_resolution_module,
            "resolve_session_config",
            AsyncMock(return_value=None),
        ),
        patch.object(
            runtime_actor_module,
            "mint_thread_runtime_actor",
            AsyncMock(return_value=actor),
        ),
    ):
        payload = await session_attach_payload.assemble_session_attach_payload(
            thread["id"],
            runtime_agent_id=AGENT_ID,
            dependencies=preparation_composition.session_attach_payload_dependencies(
                orch_main.app.state.resources
            ),
        )

    assert payload is not None
    assert payload["datasources"][0]["datasource_id"] == DATASOURCE_ID
    assert "id" not in payload["datasources"][0]
