"""Connector drivers C2: every issue and revoke point is wired.

Each test fails if its hop stops delivering a lease, delivers the upstream
secret, picks the wrong owner, or stops revoking:

* pinned dispatch (``build_job_start_request``) and the stateless claim's
  deferral to its claim transaction;
* paused re-dispatch (``resume_job_on_agent``), the second credential path;
* the thread workspace delivery and the warm session attach;
* the claim transaction helper's refusal;
* a live connector detach (``apply_thread_config_update_locked``);
* the process-zero backstop at a workspace pod delete.

The SQL behind them is proven on Postgres in
``test_connector_credential_leases_real_postgres.py``.
"""

from __future__ import annotations

import contextlib
from contextlib import ExitStack, asynccontextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import asyncpg
import httpx
import pytest
from fastapi import HTTPException

from orchestrator import main as orch_main
from orchestrator.application import controls as controls_composition
from orchestrator.services import connector_credential_leases as leases
from orchestrator.services import container_provisioner as container_provisioner_module
from orchestrator.services import job_mutation_target as job_mutation_target_module
from orchestrator.services import (
    job_workspace_authority as job_workspace_authority_module,
)
from orchestrator.services import session_attach_binding as session_attach_binding
from orchestrator.services import thread_config_update as tcu
from orchestrator.services import thread_mount_rows as thread_mount_rows_module
from orchestrator.services import unit_claim_bundle
from orchestrator.services.connector_drivers import builtin_connector_drivers
from shared import pinned_session_identity as pinned_session_identity_module
from tests import _b09_control_seams as control_seams
from tests.test_pr_authority_payload import (
    AGENT_ID,
    DATASOURCE_ID,
    JOB_ID,
    RUNTIME_GENERATION,
    RUNTIME_ID,
    _Client,
    _job,
)
from tests.test_workspace_ssh_delivery_wiring import _dispatch_patches

SECRET = "upstream-secret-never-in-a-payload"
PARENT_ID = "77777777-7777-4777-8777-777777777777"
THREAD_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"


def _probe_row() -> dict[str, Any]:
    return {
        "id": DATASOURCE_ID,
        "type": "lease_probe",
        "name": "Probe",
        "description": None,
        "connection_url": None,
        "credentials": {"secret": SECRET},
        "project_read_only": False,
        "config": {"upstream": "https://upstream.invalid"},
    }


def _issued(token: str = "scl_wired") -> AsyncMock:
    async def issue(conn, *, owner, connector_id, driver, access, **_kw):
        return leases.DeliveredLease(
            id="lease-1",
            token=token,
            connector_id=connector_id,
            access=access,
            expires_at=datetime.now(timezone.utc),
            issued=True,
        )

    return AsyncMock(side_effect=issue)


@asynccontextmanager
async def _acquire():
    yield MagicMock()


def _lease_patches(stack: ExitStack, issue: AsyncMock) -> None:
    resources = orch_main.app.state.resources
    stack.enter_context(
        patch.object(
            resources, "connector_drivers", builtin_connector_drivers(lease_probe=True)
        )
    )
    stack.enter_context(patch.object(leases, "issue_or_redeliver", issue))
    stack.enter_context(patch.object(resources.postgres_db, "acquire", _acquire))


def _assert_lease_only(entry: dict[str, Any]) -> None:
    assert entry["credentials"] == {
        "lease": {"id": "lease-1", "connector_id": DATASOURCE_ID, "token": "scl_wired"}
    }
    assert SECRET not in repr(entry)


# =============================================================================
# The payload builder
# =============================================================================


@pytest.mark.parametrize("installed", [True, False])
def test_the_payload_never_carries_a_lease_drivers_secret(installed):
    from orchestrator.services.agent_datasource_payload import (
        DatasourcePayloadDependencies,
        build_datasources_payload,
    )

    deps = DatasourcePayloadDependencies(
        logger=MagicMock(),
        mcp_datasources_enabled=lambda: False,
        mcp_stdio_enabled=lambda: False,
        connector_drivers=builtin_connector_drivers(lease_probe=installed),
        workspace_ssh_known_hosts=lambda: "",
    )
    (entry,) = build_datasources_payload([_probe_row()], dependencies=deps)
    assert entry["credentials"] == {}
    assert SECRET not in repr(entry)


def test_the_payload_blanks_a_lease_entry_whatever_its_bind_returned(monkeypatch):
    from orchestrator.services.agent_datasource_payload import (
        DatasourcePayloadDependencies,
        build_datasources_payload,
    )
    from orchestrator.services.connector_drivers.lease_probe import LeaseProbeDriver

    def leaky_bind(self, row, credentials, *, ctx):
        return {"type": row["type"], "name": row["name"], "credentials": credentials}

    monkeypatch.setattr(LeaseProbeDriver, "bind", leaky_bind)
    deps = DatasourcePayloadDependencies(
        logger=MagicMock(),
        mcp_datasources_enabled=lambda: False,
        mcp_stdio_enabled=lambda: False,
        connector_drivers=builtin_connector_drivers(lease_probe=True),
        workspace_ssh_known_hosts=lambda: "",
    )
    (entry,) = build_datasources_payload([_probe_row()], dependencies=deps)
    assert entry["credentials"] == {}


# =============================================================================
# Pinned dispatch and the stateless claim
# =============================================================================


@pytest.mark.asyncio
async def test_pinned_dispatch_delivers_a_lease_never_the_secret():
    issue = _issued()
    with ExitStack() as stack:
        _dispatch_patches(stack, _probe_row())
        _lease_patches(stack, issue)
        request = await control_seams.build_job_start_request(_job())

    (entry,) = request.datasources
    _assert_lease_only(entry)
    assert SECRET not in request.model_dump_json()
    issue.assert_awaited_once()
    kwargs = issue.await_args.kwargs
    assert kwargs["owner"] == leases.LeaseOwner.job(JOB_ID)
    assert kwargs["connector_id"] == DATASOURCE_ID
    assert kwargs["driver"] == "srw.lease-probe/v1"
    assert kwargs["access"] == "ReadWrite"


@pytest.mark.asyncio
async def test_a_child_on_its_parents_workspace_receives_the_parents_lease():
    issue = _issued()
    job = _job()
    job["parent_job_id"] = PARENT_ID
    job["context"] = {**job["context"], "inherits_parent_workspace": True}
    with ExitStack() as stack:
        _dispatch_patches(stack, _probe_row())
        _lease_patches(stack, issue)
        await control_seams.build_job_start_request(job)

    assert issue.await_args.kwargs["owner"] == leases.LeaseOwner.job(PARENT_ID)


@pytest.mark.asyncio
async def test_the_stateless_claim_defers_the_lease_to_its_transaction():
    issue = _issued()
    with ExitStack() as stack:
        _dispatch_patches(stack, _probe_row())
        _lease_patches(stack, issue)
        request = await control_seams.build_job_start_request(
            _job(), persist_dispatch_state=False, deliver_connector_leases=False
        )

    issue.assert_not_awaited()
    # Even unleased, the bundle never carries the upstream secret.
    assert request.datasources[0]["credentials"] == {}
    assert SECRET not in request.model_dump_json()


@pytest.mark.asyncio
async def test_a_refused_lease_refuses_the_bundle():
    issue = AsyncMock(side_effect=leases.LeaseDeliveryError("ended"))
    with ExitStack() as stack:
        _dispatch_patches(stack, _probe_row())
        _lease_patches(stack, issue)
        request = await control_seams.build_job_start_request(_job())

    assert request is None


@pytest.mark.asyncio
async def test_the_claim_transaction_helper_refuses_generically():
    conn = MagicMock()
    with patch.object(
        leases,
        "deliver_connector_leases",
        AsyncMock(side_effect=leases.LeaseDeliveryError("no")),
    ):
        with pytest.raises(HTTPException) as exc:
            await unit_claim_bundle._deliver_claim_leases(
                conn,
                [{"type": "lease_probe"}],
                owner=leases.LeaseOwner.thread(THREAD_ID),
                refusal="Attach assembly refused",
            )
    assert (exc.value.status_code, exc.value.detail) == (409, "Attach assembly refused")


@pytest.mark.asyncio
async def test_the_claim_transaction_helper_delivers_on_the_claim_connection():
    conn = MagicMock()
    deliver = AsyncMock(return_value=1)
    entries = [{"type": "lease_probe"}]
    with patch.object(leases, "deliver_connector_leases", deliver):
        await unit_claim_bundle._deliver_claim_leases(
            conn, entries, owner=leases.LeaseOwner.thread(THREAD_ID), refusal="x"
        )
        await unit_claim_bundle._deliver_claim_leases(
            conn,
            [{"type": "generic"}],
            owner=leases.LeaseOwner.thread(THREAD_ID),
            refusal="x",
        )
    deliver.assert_awaited_once_with(
        conn, entries, owner=leases.LeaseOwner.thread(THREAD_ID)
    )


# =============================================================================
# Paused re-dispatch
# =============================================================================


async def _resume(issue: AsyncMock) -> bool:
    _Client.posts = []
    job = _job(status="paused")
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
        _dispatch_patches(stack, _probe_row())
        _lease_patches(stack, issue)
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
            (httpx, "AsyncClient", _Client),
            (resources.settings, "completion_commands_enabled", False),
        ):
            stack.enter_context(patch.object(owner, name, value))
        return await control_seams.resume_job_on_agent(
            job,
            {"id": AGENT_ID, "status": "ready", "pod_ip": "10.0.0.8", "pod_port": 8080},
        )


@pytest.mark.asyncio
async def test_resume_delivers_a_lease_in_the_resume_payload():
    issue = _issued()
    assert await _resume(issue) is True
    (posted,) = _Client.posts
    _assert_lease_only(posted["datasources"][0])
    assert issue.await_args.kwargs["owner"] == leases.LeaseOwner.job(JOB_ID)


@pytest.mark.asyncio
async def test_resume_refuses_when_no_lease_can_be_issued():
    issue = AsyncMock(side_effect=leases.LeaseDeliveryError("ended"))
    assert await _resume(issue) is False
    assert _Client.posts == []


# =============================================================================
# Sessions: the workspace delivery and the warm attach
# =============================================================================


@pytest.mark.asyncio
async def test_the_thread_workspace_delivers_the_threads_lease():
    from orchestrator.services import (
        stateless_workspace_scheduler as stateless_workspace_scheduler_module,
    )
    from tests.test_stateless_cloud_sync_integration import (
        _internal_workspace_response_for_lite_thread,
        _stateless_sandbox_thread,
    )

    thread = _stateless_sandbox_thread()
    entry = {
        "type": "lease_probe",
        "name": "Probe",
        "datasource_id": DATASOURCE_ID,
        "credentials": {},
    }
    issue = _issued()
    with ExitStack() as stack:
        _lease_patches(stack, issue)
        stack.enter_context(
            patch.object(
                thread_mount_rows_module,
                "resolve_thread_datasource_delivery",
                AsyncMock(return_value=([entry], None)),
            )
        )
        stack.enter_context(
            patch.object(
                container_provisioner_module.container_provisioner,
                "workspace_pod_live",
                AsyncMock(return_value=True),
            )
        )
        stack.enter_context(
            patch.object(
                stateless_workspace_scheduler_module,
                "schedule_stateless_workspace_ensure",
                MagicMock(),
            )
        )
        response = await _internal_workspace_response_for_lite_thread(thread)

    (delivered,) = response["datasources"]
    _assert_lease_only(delivered)
    assert issue.await_args.kwargs["owner"] == leases.LeaseOwner.thread(
        str(thread["id"])
    )


def _attach_dependencies(
    *, deliver_raises: bool
) -> tuple[session_attach_binding.SessionAttachBindingDependencies, Any]:
    thread = {
        "id": THREAD_ID,
        "execution_lane": "pinned",
        "status": "active",
        "runtime_generation": RUNTIME_GENERATION,
        "metadata": {},
        "srw_runtime": True,
    }

    @contextlib.asynccontextmanager
    async def acquire():
        yield MagicMock()

    store = SimpleNamespace(get_thread=AsyncMock(return_value=thread), acquire=acquire)
    payload = {
        "session_runtime_generation": RUNTIME_GENERATION,
        "datasources": [
            {"type": "lease_probe", "name": "Probe", "datasource_id": DATASOURCE_ID}
        ],
    }
    release = AsyncMock(return_value="released")
    deps = session_attach_binding.SessionAttachBindingDependencies(
        store=store,
        gitea_client=None,
        agent_provisioner=None,
        persistent_provisioner=None,
        reserve_pinned_warm_agent_binding=AsyncMock(),
        release_pinned_warm_binding_protection=AsyncMock(),
        await_protected_cloud_runtime_ready=AsyncMock(return_value=True),
        prepare_thread_repository_authority=AsyncMock(),
        assemble_session_attach_payload=AsyncMock(return_value=payload),
        schedule_attach_abort_successor=MagicMock(),
        prepare_pinned_session_mutation_target=AsyncMock(return_value=None),
        pinned_session_mutation_target_is_current=AsyncMock(return_value=False),
        reserve_session_attach_binding=AsyncMock(return_value="attach-token"),
        release_session_attach_binding=release,
        send_session_attach_locked=AsyncMock(),
    )
    issue = (
        AsyncMock(side_effect=leases.LeaseDeliveryError("ended"))
        if deliver_raises
        else _issued()
    )
    return deps, (payload, release, issue)


@pytest.mark.asyncio
async def test_a_warm_attach_delivers_the_threads_lease():
    deps, (payload, _release, issue) = _attach_dependencies(deliver_raises=False)
    with patch.object(leases, "issue_or_redeliver", issue):
        await session_attach_binding.send_session_attach_locked(
            {"id": AGENT_ID}, THREAD_ID, dependencies=deps
        )
    _assert_lease_only(payload["datasources"][0])
    assert issue.await_args.kwargs["owner"] == leases.LeaseOwner.thread(THREAD_ID)


@pytest.mark.asyncio
async def test_a_warm_attach_without_a_lease_releases_its_reservation():
    deps, (_payload, release, issue) = _attach_dependencies(deliver_raises=True)
    with patch.object(leases, "issue_or_redeliver", issue):
        await session_attach_binding.send_session_attach_locked(
            {"id": AGENT_ID}, THREAD_ID, dependencies=deps
        )
    release.assert_awaited_once()
    assert release.await_args.kwargs["pre_delivery"] is True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure",
    [
        asyncpg.exceptions.DeadlockDetectedError("deadlock detected"),
        ConnectionResetError("connection lost"),
    ],
    ids=["deadlock", "connection_lost"],
)
async def test_any_lease_failure_releases_the_warm_reservation(failure):
    """The attach token is reserved before the lease step: whatever the lease
    step raises, the reservation is released, never leaked."""
    deps, (_payload, release, _issue) = _attach_dependencies(deliver_raises=False)
    with patch.object(leases, "issue_or_redeliver", AsyncMock(side_effect=failure)):
        await session_attach_binding.send_session_attach_locked(
            {"id": AGENT_ID}, THREAD_ID, dependencies=deps
        )
    release.assert_awaited_once()
    assert release.await_args.kwargs["pre_delivery"] is True
    assert release.await_args.kwargs["expected_attach_token"] == "attach-token"
    deps.schedule_attach_abort_successor.assert_called_once()


# =============================================================================
# Live detach
# =============================================================================


PROBE_ID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
GENERIC_ID = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
KEPT_ID = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
REPO_ID = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"


def _detach_dependencies() -> tcu.ThreadConfigUpdateDependencies:
    from tests.test_b06_lane_b_thread_config_update import OWNER, _deps

    @contextlib.asynccontextmanager
    async def acquire():
        yield "conn"

    store = {
        "get_datasource_policy_rows": AsyncMock(
            return_value=[
                {"id": PROBE_ID, "type": "lease_probe"},
                {"id": GENERIC_ID, "type": "generic"},
                # A token repository: its stored row names no lease driver,
                # yet the git swap driver's lease is its (C3 re-review B1).
                {
                    "id": REPO_ID,
                    "type": "repository",
                    "connection_url": "https://github.com/o/r.git",
                },
            ]
        ),
        "get_user": AsyncMock(return_value={"id": OWNER}),
        "resolve_datasources_for_thread": AsyncMock(return_value=[]),
        "merge_thread_config_override": AsyncMock(return_value=True),
        "set_thread_datasource_ids": AsyncMock(return_value=True),
        "acquire": acquire,
    }
    return _deps(
        store=store,
        authorize_thread_datasource_selection=AsyncMock(return_value=([KEPT_ID], {})),
    )


@pytest.mark.asyncio
async def test_a_live_detach_revokes_every_removed_connectors_lease():
    from tests.test_b06_lane_b_thread_config_update import THREAD, _pinned_thread

    revoked: list = []

    async def revoke(conn, *, owner, connector_ids, reason="connector_detached"):
        revoked.append((conn, owner, list(connector_ids)))
        return []

    row = _pinned_thread(
        metadata={"datasource_ids": [PROBE_ID, GENERIC_ID, REPO_ID, KEPT_ID]}
    )
    with patch.object(leases, "revoke_connector_leases", revoke):
        await tcu.apply_thread_config_update_locked(
            THREAD,
            row,
            {},
            [KEPT_ID],
            request=MagicMock(),
            actor=None,
            dependencies=_detach_dependencies(),
        )

    # Every removed connector: revoking where no lease exists does nothing.
    assert revoked == [
        ("conn", leases.LeaseOwner.thread(THREAD), [PROBE_ID, GENERIC_ID, REPO_ID])
    ]


# =============================================================================
# The process-zero backstop
# =============================================================================


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("kind", "lease_kind"), [("session", "thread"), ("job", "job")]
)
async def test_the_pod_delete_backstop_revokes_only_terminal_executions(
    kind, lease_kind
):
    from orchestrator.services.workspace_lifecycle import WorkspaceOwner

    provisioner = object.__new__(container_provisioner_module.ContainerProvisioner)
    provisioner._db = SimpleNamespace(
        managed_repository_workspace_process_zero_is_current=AsyncMock(
            return_value=True
        )
    )
    backstop = AsyncMock()
    with patch.object(leases, "revoke_terminal_execution_leases_with", backstop):
        assert await provisioner._ensure_managed_repository_process_zero_before_delete(
            WorkspaceOwner(kind, THREAD_ID), MagicMock(), RUNTIME_ID
        )
    backstop.assert_awaited_once_with(
        provisioner._db, owner=leases.LeaseOwner(lease_kind, THREAD_ID)
    )
