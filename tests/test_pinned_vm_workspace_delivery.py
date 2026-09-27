"""VM physical identity survives delivery, polling, and persistent SSH setup."""

import asyncio
import logging
from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio
from fastapi import HTTPException

from agent.api import persistent_app, persistent_session
from orchestrator.services import thread_workspace_delivery as delivery
from orchestrator.services.session_runtime_admission import thread_runtime_is_preparable
from orchestrator.services.vm_provisioner import VMProvisioner
from orchestrator.services.workspace_tier_policy import thread_workspace_backend
from shared.runtime.core.backends.remote import RemoteBackend
from shared.runtime.core.workspace_backend import WorkspaceUnavailableError
from tests.test_persistent_session import _make_config

THREAD = "11111111-1111-4111-8111-111111111111"
AGENT = "22222222-2222-4222-8222-222222222222"
RUNTIME = "33333333-3333-4333-8333-333333333333"
ATTACH = "44444444-4444-4444-8444-444444444444"
GENERATION = "55555555-5555-4555-8555-555555555555"
LAUNCHER = "66666666-6666-4666-8666-666666666666"
SUCCESSOR = "77777777-7777-4777-8777-777777777777"
FINGERPRINT = "SHA256:" + "A" * 43


@pytest.fixture
def vm_delivery(monkeypatch):
    monkeypatch.setenv("VM_MODE", "same-cluster")
    vm = {
        "status": "ready",
        "provision_generation": GENERATION,
        "identity_authenticated": True,
        "identity_provision_generation": GENERATION,
        "vm_uid": "vm-uid",
        "vmi_uid": "88888888-8888-4888-8888-888888888888",
        "rootdisk_pvc_uid": "99999999-9999-4999-8999-999999999999",
        "active_pod_uid": LAUNCHER,
        "ssh_registration_id": "registration-1",
        "ssh_host_key_fingerprint": FINGERPRINT,
        "ssh_host": "10.42.1.23",
        "ssh_port": 22,
    }
    thread = {
        "id": THREAD,
        "status": "created",
        "execution_lane": "pinned",
        "agent_id": AGENT,
        "runtime_generation": RUNTIME,
        "runtime_attach_token": ATTACH,
        "runtime_retirement_token": None,
        "metadata": {"vm": vm, "config_override": {"workspace": {"backend": "vm"}}},
    }
    store = SimpleNamespace(
        get_thread=AsyncMock(side_effect=lambda _id: deepcopy(thread)),
        pinned_thread_agent_is_reciprocal=AsyncMock(return_value=True),
        list_thread_mounts=AsyncMock(return_value=[]),
        managed_repository_authorities_are_current=AsyncMock(return_value=True),
    )
    observed = {
        "_identity_authenticated": True,
        "ready": True,
        "provision_generation": GENERATION,
        "vm_uid": "vm-uid",
        "active_pod_uid": LAUNCHER,
        "pod_ip": "10.42.1.23",
        "vmi_uid": vm["vmi_uid"],
        "rootdisk_pvc_uid": vm["rootdisk_pvc_uid"],
    }
    provisioner = VMProvisioner()
    provisioner._db = store
    provisioner._controller_url = "http://vm-controller:8080"
    provisioner._http_client = MagicMock()
    provisioner._lifecycle_hmac_secret = b"test-only-attestation-secret-32bytes"
    provisioner._query_http = AsyncMock(side_effect=lambda *a, **kw: deepcopy(observed))
    deps = delivery.ThreadWorkspaceDeliveryDependencies(
        store=store,
        vm_provisioner=provisioner,
        cloud_router=SimpleNamespace(active=SimpleNamespace(is_initialized=False)),
        gitea_client=SimpleNamespace(),
        container_provisioner=None,
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
        protected_workspace_wait_payload=lambda **kw: kw,
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
    return SimpleNamespace(
        thread=thread,
        vm=vm,
        observed=observed,
        store=store,
        provisioner=provisioner,
        dependencies=deps,
    )


async def _deliver(fixture):
    return await delivery.agent_get_thread_workspace_locked(
        THREAD,
        presented_agent_id=AGENT,
        presented_runtime_generation=RUNTIME,
        presented_attach_token=ATTACH,
        dependencies=fixture.dependencies,
    )


async def _normalize(payload):
    return await persistent_app._poll_workspace_ready(
        SimpleNamespace(get_thread_workspace=AsyncMock(return_value=payload)),
        THREAD,
        timeout=1,
        require_vm=True,
    )


@pytest.mark.asyncio
async def test_attested_vm_delivery_reaches_real_remote_backend(
    vm_delivery, monkeypatch
):
    """Dropping any physical field must break SSH setup for the server contract."""
    payload = await _deliver(vm_delivery)
    normalized = await _normalize(payload)
    monkeypatch.setattr(persistent_app, "_session_runtime_generation", RUNTIME)
    monkeypatch.setattr(persistent_app, "_pinned_runtime_generation_enabled", True)
    assert (
        persistent_app._bind_attached_runtime_payload(
            normalized, protected_required=False
        )
        == RUNTIME
    )
    assert persistent_app._pinned_status_identity_advertised(normalized)
    session = persistent_session.PersistentSession(
        thread_id=THREAD,
        config=_make_config(ws_backend="vm"),
        pinned_runtime_identity_required=True,
    )
    # Keep construction/validation and identity consumers real; replace only
    # remote I/O and filesystem materialization after the SSH boundary.
    monkeypatch.setattr(RemoteBackend, "connect", lambda self: None)
    monkeypatch.setattr(RemoteBackend, "exists", lambda self, path: False)
    monkeypatch.setattr(RemoteBackend, "list_dir", lambda self, path: [])
    monkeypatch.setattr(persistent_session, "WorkspaceManager", MagicMock())
    await session._setup_workspace(workspace_override=normalized)

    remote = session._workspace_backend_for_cleanup
    assert isinstance(remote, RemoteBackend)
    assert remote.managed_repository_runtime_authority == (GENERATION, LAUNCHER)
    assert remote._expected_host_key_fingerprint == FINGERPRINT
    assert remote.host == "10.42.1.23"
    assert remote._port == 22
    assert remote._workspace_tier == "vm"
    assert not remote.supports_canvas_presentation
    assert not remote.supports_canvas_live_apps
    assert not remote.supports_canvas_shared_browser
    assert session.workspace_generation == GENERATION
    assert session.workspace_runtime_incarnation == LAUNCHER


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field",
    [
        "provision_generation",
        "active_pod_uid",
        "ssh_host_key_fingerprint",
        "identity_authenticated",
    ],
)
async def test_incomplete_admitted_vm_identity_is_refused(vm_delivery, field):
    vm_delivery.vm.pop(field)
    with pytest.raises(HTTPException) as refused:
        await _deliver(vm_delivery)
    assert refused.value.status_code in {409, 503}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("ready", False),
        ("_identity_authenticated", False),
        ("active_pod_uid", SUCCESSOR),
        ("provision_generation", SUCCESSOR),
    ],
)
async def test_unattested_or_rotated_controller_identity_is_refused(
    vm_delivery, field, value
):
    vm_delivery.observed[field] = value
    with pytest.raises(HTTPException) as refused:
        await _deliver(vm_delivery)
    assert refused.value.status_code in {409, 503}


@pytest.mark.asyncio
async def test_controller_rotation_during_payload_build_is_refused(vm_delivery):
    async def rotate(_project_ids):
        vm_delivery.observed["active_pod_uid"] = SUCCESSOR
        return []

    vm_delivery.dependencies.resolve_thread_repositories.side_effect = rotate
    with pytest.raises(HTTPException) as refused:
        await _deliver(vm_delivery)
    assert refused.value.status_code in {409, 503}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change", ["vm", "malformed_vm", "agent", "runtime", "attach", "ended", "backend"]
)
async def test_delivery_boundary_refuses_successor_authority(vm_delivery, change):
    async def rotate(_project_ids):
        if change == "vm":
            vm_delivery.vm["active_pod_uid"] = SUCCESSOR
            vm_delivery.observed["active_pod_uid"] = SUCCESSOR
        elif change == "malformed_vm":
            vm_delivery.thread["metadata"]["vm"] = "malformed"
        elif change == "agent":
            vm_delivery.thread["agent_id"] = SUCCESSOR
        elif change == "runtime":
            vm_delivery.thread["runtime_generation"] = SUCCESSOR
        elif change == "attach":
            vm_delivery.store.pinned_thread_agent_is_reciprocal.return_value = False
        elif change == "ended":
            vm_delivery.thread["status"] = "ended"
        else:
            vm_delivery.thread["metadata"]["config_override"]["workspace"][
                "backend"
            ] = "none"
        return []

    vm_delivery.dependencies.resolve_thread_repositories.side_effect = rotate
    with pytest.raises(HTTPException) as refused:
        await _deliver(vm_delivery)
    assert refused.value.status_code in {409, 503}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field",
    [
        "workspace_generation",
        "workspace_runtime_incarnation",
        "workspace_ssh_host_key_fingerprint",
    ],
)
async def test_partial_vm_payload_cannot_start_ssh(vm_delivery, monkeypatch, field):
    payload = await _deliver(vm_delivery)
    payload.pop(field)
    normalized = await _normalize(payload)
    session = persistent_session.PersistentSession(
        thread_id=THREAD,
        config=_make_config(ws_backend="vm"),
        pinned_runtime_identity_required=True,
    )
    connect = MagicMock()
    monkeypatch.setattr(RemoteBackend, "connect", connect)
    with pytest.raises(WorkspaceUnavailableError, match="orchestrator-attested"):
        await session._setup_workspace(workspace_override=normalized)
    connect.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change", ["vm", "lane", "backend", "agent", "runtime", "ended"]
)
async def test_final_read_after_attestation_refuses_changed_authority(
    vm_delivery, change
):
    async def capture(*args, **kwargs):
        if change == "vm":
            vm_delivery.vm["active_pod_uid"] = SUCCESSOR
        elif change == "lane":
            vm_delivery.thread["execution_lane"] = "stateless"
        elif change == "backend":
            vm_delivery.thread["metadata"]["config_override"]["workspace"][
                "backend"
            ] = "none"
        elif change == "agent":
            vm_delivery.thread["agent_id"] = SUCCESSOR
        elif change == "runtime":
            vm_delivery.thread["runtime_generation"] = SUCCESSOR
        else:
            vm_delivery.thread["status"] = "ended"
        return None

    vm_delivery.dependencies = replace(
        vm_delivery.dependencies, capture_session_config=capture
    )
    with pytest.raises(HTTPException) as refused:
        await _deliver(vm_delivery)
    assert refused.value.status_code == 409


@pytest.mark.asyncio
async def test_unavailable_vm_attestor_refuses_credentials(vm_delivery):
    vm_delivery.dependencies = replace(vm_delivery.dependencies, vm_provisioner=None)
    with pytest.raises(HTTPException) as refused:
        await _deliver(vm_delivery)
    assert refused.value.status_code == 503


@pytest.mark.asyncio
async def test_snapshot_rotation_before_first_attestation_is_refused(vm_delivery):
    first = deepcopy(vm_delivery.thread)
    vm_delivery.vm["active_pod_uid"] = SUCCESSOR
    vm_delivery.observed["active_pod_uid"] = SUCCESSOR
    reads = iter([first])

    async def read(_id):
        return deepcopy(next(reads, vm_delivery.thread))

    vm_delivery.store.get_thread.side_effect = read
    with pytest.raises(HTTPException) as refused:
        await _deliver(vm_delivery)
    assert refused.value.status_code == 409


@pytest_asyncio.fixture
async def vm_attach_setup(vm_delivery, monkeypatch):
    from shared.runtime.core.loader import AgentConfig
    from tests.test_managed_repository_authority import _authority, _runtime_payload

    payload = await _deliver(vm_delivery)
    credential = _runtime_payload(_authority(repo_name="thread-11111111"))
    payload["git_remote_url"] = credential["clone_url"]
    payload["managed_repository_credentials"] = [credential]
    client = SimpleNamespace(
        get_thread_workspace=AsyncMock(return_value=payload),
        adopt_session_runtime_identity=MagicMock(return_value=True),
    )
    config = AgentConfig(agent_id="test-agent", display_name="Test agent")
    config.workspace.backend = "vm"
    config.workspace.git_versioning = False
    agent = SimpleNamespace(
        config=config,
        _llm=MagicMock(),
        _tactical_llm=None,
        _auxiliary_llm=None,
        postgres_conn=None,
        vector_conn=None,
    )
    monkeypatch.setattr(persistent_app, "_agent", agent)
    monkeypatch.setattr(persistent_app, "_orchestrator_client", client)
    monkeypatch.setattr(persistent_app, "_session", None)
    monkeypatch.setattr(persistent_app, "_thread_id", None)
    monkeypatch.setattr(persistent_app, "_event_writer", None)
    monkeypatch.setattr(
        persistent_app, "_failed_attach_workspace_cleanup_context", None
    )
    monkeypatch.setattr(persistent_app, "_session_side_tasks", set())
    monkeypatch.setattr(persistent_app, "_session_runtime_generation", None)
    monkeypatch.setattr(persistent_app, "_session_runtime_attach_token", None)
    monkeypatch.setattr(persistent_app, "_pinned_runtime_generation_enabled", False)
    return credential


@pytest.mark.asyncio
async def test_vm_attach_setup_materializes_repository_bundle(
    vm_attach_setup, monkeypatch, caplog
):
    """The setup wrapper consumes credentials carried inside the VM payload."""
    credential = vm_attach_setup
    private_material = credential["private_key"]
    monkeypatch.setattr(RemoteBackend, "connect", lambda self: None)
    monkeypatch.setattr(RemoteBackend, "exists", lambda self, path: False)
    monkeypatch.setattr(RemoteBackend, "list_dir", lambda self, path: [])
    monkeypatch.setattr(
        RemoteBackend,
        "resolve_home_path",
        lambda self, path: f"/home/agent-host/{path}",
    )
    transferred = []

    def secret_transport(self, command, secret, **kwargs):
        assert private_material not in command
        if secret:
            transferred.append(bytes(secret) == private_material.encode())
        return True

    monkeypatch.setattr(RemoteBackend, "execute_with_secret_stdin", secret_transport)
    manager = MagicMock()
    monkeypatch.setattr(persistent_session, "WorkspaceManager", manager)

    class WorkspaceSetupComplete(Exception):
        pass

    async def stop_after_workspace(self, postgres_conn):
        raise WorkspaceSetupComplete

    # Stop at the first stage after real setup()/workspace creation; unrelated
    # model/tool setup and real failed-attach retirement are outside this test.
    monkeypatch.setattr(
        persistent_session.PersistentSession,
        "_seed_workspace_baseline_commit",
        stop_after_workspace,
    )
    monkeypatch.setattr(
        persistent_app, "_cleanup_failed_event_journal_attach", AsyncMock()
    )
    with pytest.raises(WorkspaceSetupComplete):
        await persistent_app._attach_session(
            THREAD,
            pinned_status_identity_contract=1,
            pinned_runtime_generation_contract=1,
            session_runtime_generation=RUNTIME,
            session_runtime_attach_token=ATTACH,
        )

    session = persistent_app._session
    remote = session._workspace_backend_for_cleanup
    assert remote.managed_repository_runtime_authority == (GENERATION, LAUNCHER)
    assert remote._expected_host_key_fingerprint == FINGERPRINT
    assert transferred == [True]
    assert "private_key" not in credential
    assert private_material not in caplog.text
    assert manager.call_args.kwargs["config"].git_remote_url == credential["clone_url"]


@pytest.mark.asyncio
@pytest.mark.parametrize("cleanup_fails", [False, True])
async def test_vm_setup_failure_is_logged_before_cleanup_settles(
    vm_attach_setup, monkeypatch, caplog, cleanup_fails
):
    """The original failure stays observable even when cleanup replaces it."""
    original = WorkspaceUnavailableError("https://user:secret-password@workspace")
    cause = RuntimeError("-----BEGIN PRIVATE KEY----- secret-cause")
    cleanup_failure = persistent_app.EventJournalUnavailable("secret-cleanup")
    cleanup_entered = asyncio.Event()
    cleanup_released = asyncio.Event()
    cleanup_calls = 0

    async def fail_workspace(self, **kwargs):
        raise original from cause

    async def cleanup(thread_id, **kwargs):
        nonlocal cleanup_calls
        cleanup_calls += 1
        if cleanup_fails and cleanup_calls <= 2:
            raise cleanup_failure
        cleanup_entered.set()
        await cleanup_released.wait()

    monkeypatch.setattr(
        persistent_session.PersistentSession, "_setup_workspace", fail_workspace
    )
    monkeypatch.setattr(persistent_app, "_cleanup_failed_event_journal_attach", cleanup)
    monkeypatch.setattr(
        persistent_app, "_EXACT_RETIREMENT_SETTLEMENT_RETRY_DELAYS", (0.0,)
    )
    caplog.set_level(logging.WARNING, logger=persistent_app.__name__)
    task = asyncio.create_task(
        persistent_app._attach_session(
            THREAD,
            pinned_status_identity_contract=1,
            pinned_runtime_generation_contract=1,
            session_runtime_generation=RUNTIME,
            session_runtime_attach_token=ATTACH,
        )
    )
    try:
        await asyncio.wait_for(cleanup_entered.wait(), timeout=2)
        records = [r for r in caplog.records if "stage=session_setup" in r.getMessage()]
        assert len(records) == 1
        record = records[0]
        assert record.levelno == logging.WARNING
        assert f"thread={THREAD}" in record.getMessage()
        assert "type=WorkspaceUnavailableError" in record.getMessage()
        assert record.args == (THREAD, "WorkspaceUnavailableError")
        assert record.exc_info is None
        assert record.exc_text is None
        assert record.stack_info is None
        for secret in (
            str(original),
            str(cause),
            str(cleanup_failure),
            vm_attach_setup["private_key"],
        ):
            assert secret not in repr(record.__dict__)
            assert secret not in caplog.text
        assert not task.done()
        assert persistent_app._session is not None
        assert persistent_app._thread_id == THREAD
        assert persistent_app._session_runtime_generation == RUNTIME
        assert persistent_app._session_runtime_attach_token == ATTACH
        assert persistent_app._pool_heartbeat_status() == "session"
        if cleanup_fails:
            assert cleanup_calls == 3
            assert "cleanup remains unproven" in caplog.text
    finally:
        cleanup_released.set()
        expected = cleanup_failure if cleanup_fails else original
        with pytest.raises(type(expected)) as raised:
            await asyncio.wait_for(task, timeout=2)
        assert raised.value is expected

    assert sum("stage=session_setup" in r.getMessage() for r in caplog.records) == 1


@pytest.mark.asyncio
async def test_cancelled_vm_setup_keeps_cancellation_without_failure_warning(
    vm_attach_setup, monkeypatch, caplog
):
    setup_entered = asyncio.Event()

    async def blocked_workspace(self, **kwargs):
        setup_entered.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(
        persistent_session.PersistentSession, "_setup_workspace", blocked_workspace
    )
    monkeypatch.setattr(
        persistent_app, "_cleanup_failed_event_journal_attach", AsyncMock()
    )
    task = asyncio.create_task(
        persistent_app._attach_session(
            THREAD,
            pinned_status_identity_contract=1,
            pinned_runtime_generation_contract=1,
            session_runtime_generation=RUNTIME,
            session_runtime_attach_token=ATTACH,
        )
    )
    try:
        await asyncio.wait_for(setup_entered.wait(), timeout=2)
    finally:
        task.cancel("test-cancellation")
        with pytest.raises(asyncio.CancelledError) as cancelled:
            await task
    assert cancelled.value.args == ("test-cancellation",)
    assert not any("stage=session_setup" in r.getMessage() for r in caplog.records)
