"""Mutation-resistant contract tests for protected workspace delivery."""

from __future__ import annotations

from agent.api import session_contract
from agent.api import session_attach
from agent.api import session_workspace
import copy
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest

from tests._session_termination_adapter import termination_target

from agent.api import persistent_app


def _protected_mount() -> dict:
    return {
        "version": 1,
        "driver": "rclone",
        "protected": True,
        "skip_workspace_links": True,
        "fallback": False,
        "overlay": {
            "lower": "/cloud/lower",
            "merged": "/cloud/merged",
            "upper": "/home/agent-host/.overlay/upper",
            "work": "/home/agent-host/.overlay/work",
            "quota_bytes": 1024,
        },
        "mounts": [
            {
                "mount_id": "reader-1",
                "mount_kind": "protected_lower",
                "backend": "nextcloud",
                "target_path": "/cloud/lower",
                "workspace_name": "lower",
                "access": "read_only",
                "source": {
                    "type": "webdav",
                    "config": {
                        "vendor": "nextcloud",
                        "url": "https://cloud.invalid/remote.php/dav/files/reader/",
                        "user": "reader",
                    },
                },
                "auth": {"type": "basic", "password": "one-use-secret"},
            }
        ],
    }


def _ready_payload() -> dict:
    return {
        "protected_cloud": True,
        "protected_cloud_state": "ready",
        "protected_cloud_error_code": None,
        "status": "ready",
        "backend": "sandbox",
        "pod_ip": "10.42.0.10",
        "pod_port": 30022,
        "pinned_status_identity_contract": 1,
        "pinned_runtime_generation_contract": 1,
        "session_runtime_generation": "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
        "workspace_generation": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        "workspace_runtime_incarnation": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        "workspace_ssh_host_key_fingerprint": "SHA256:trusted",
        "vm_status": None,
        "vm_ssh_host": None,
        "vm_ssh_port": None,
        "vm_name": None,
        "cloud_sync": None,
        "nc_session_folder": None,
        "cloud_mount": _protected_mount(),
    }


def _set_path(target: dict, path: str, value) -> None:
    parts = path.split(".")
    current = target
    for part in parts[:-1]:
        if part.isdigit():
            current = current[int(part)]
        else:
            current = current[part]
    leaf = parts[-1]
    if leaf.isdigit():
        current[int(leaf)] = value
    else:
        current[leaf] = value


def _normalized_ready(payload: dict | None = None) -> dict:
    raw = copy.deepcopy(payload or _ready_payload())
    raw["ssh_key_path"] = "/run/secrets/vm-ssh-key"
    raw["remote"] = {
        "host": raw["pod_ip"],
        "port": raw.get("pod_port") or 30022,
        "username": "agent-host",
        "key_path": raw["ssh_key_path"],
        "workspace_path": "/home/agent-host/workspace",
    }
    return raw


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"protected_cloud": False},
        {
            "protected_cloud": False,
            "protected_cloud_state": None,
            "protected_cloud_error_code": None,
            "cloud_mount": {"protected": False, "mounts": []},
        },
    ],
)
def test_ordinary_workspace_shapes_remain_ordinary(payload):
    assert session_workspace.protected_workspace_delivery(payload) == "off"


def test_protected_refusal_is_a_workspace_not_ready_failure():
    assert issubclass(
        session_contract.ProtectedCloudUnavailable,
        persistent_app.WorkspaceNotReady,
    )


@pytest.mark.parametrize("marker", [None, 0, 1, "true", {}, []])
def test_every_present_non_boolean_marker_fails_closed(marker):
    payload = _ready_payload()
    payload["protected_cloud"] = marker

    with pytest.raises(session_contract.ProtectedCloudUnavailable):
        session_workspace.protected_workspace_delivery(payload)


@pytest.mark.parametrize("protected_value", [True, 1, "true", None, {}])
def test_off_marker_rejects_non_exact_false_protected_mount_flag(protected_value):
    payload = {
        "protected_cloud": False,
        "cloud_mount": {"protected": protected_value, "mounts": []},
    }

    with pytest.raises(session_contract.ProtectedCloudUnavailable):
        session_workspace.protected_workspace_delivery(payload)


@pytest.mark.parametrize(
    "mount",
    [
        {"protected": False, "overlay": None, "mounts": []},
        {
            "protected": False,
            "mounts": [{"mount_kind": "protected_lower"}],
        },
    ],
)
def test_off_marker_rejects_every_other_protected_only_mount_shape(mount):
    with pytest.raises(session_contract.ProtectedCloudUnavailable):
        session_workspace.protected_workspace_delivery(
            {"protected_cloud": False, "cloud_mount": mount}
        )


def test_engaging_response_is_coordinate_free_and_retryable():
    payload = {
        "protected_cloud": True,
        "protected_cloud_state": "engaging",
        "protected_cloud_error_code": None,
        "status": "creating",
    }
    assert session_workspace.protected_workspace_delivery(payload) == "engaging"


@pytest.mark.parametrize(
    "field",
    [
        "pod_ip",
        "pod_name",
        "pod_port",
        "namespace",
        "vm_ssh_host",
        "vm_ssh_port",
        "vm_name",
        "ssh_key_path",
        "workspace_generation",
        "workspace_runtime_incarnation",
        "workspace_ssh_host_key_fingerprint",
        "git_remote_url",
        "managed_repository_credentials",
        "workspace_ssh_identities",
        "repositories",
        "resolved_config",
        "config_override",
        "datasources",
        "nc_session_folder",
        "cloud_sync",
        "cloud_mount",
        "remote",
    ],
)
@pytest.mark.parametrize(
    "state,status", [("engaging", "creating"), ("failed", "failed")]
)
def test_non_ready_response_rejects_each_coordinate_or_credential(field, state, status):
    payload = {
        "protected_cloud": True,
        "protected_cloud_state": state,
        "protected_cloud_error_code": "engage_failed" if state == "failed" else None,
        "status": status,
        field: "secret-sentinel",
    }

    with pytest.raises(session_contract.ProtectedCloudUnavailable) as exc:
        session_workspace.protected_workspace_delivery(payload)
    assert "secret-sentinel" not in str(exc.value)


@pytest.mark.parametrize(
    "state,status",
    [
        ("engaging", "ready"),
        ("ready", "creating"),
        ("failed", "creating"),
        (None, "ready"),
        ("ready", None),
    ],
)
def test_inconsistent_protected_state_and_workspace_status_fail(state, status):
    payload = _ready_payload()
    payload["protected_cloud_state"] = state
    payload["status"] = status

    with pytest.raises(session_contract.ProtectedCloudUnavailable):
        session_workspace.protected_workspace_delivery(payload)


def test_failed_error_code_is_allowlisted_and_secret_never_reflected():
    payload = {
        "protected_cloud": True,
        "protected_cloud_state": "failed",
        "protected_cloud_error_code": "secret-sentinel",
        "status": "failed",
    }

    with pytest.raises(session_contract.ProtectedCloudUnavailable) as exc:
        session_workspace.protected_workspace_delivery(payload)
    assert "secret-sentinel" not in str(exc.value)
    assert "engage_failed" in str(exc.value)


def test_exact_ready_contract_is_accepted():
    assert session_workspace.protected_workspace_delivery(_ready_payload()) == "ready"


@pytest.mark.parametrize(
    "field,value",
    [
        ("workspace_generation", None),
        ("workspace_generation", "not-a-uuid"),
        ("workspace_runtime_incarnation", None),
        ("workspace_runtime_incarnation", "not-a-uuid"),
        ("workspace_ssh_host_key_fingerprint", None),
        ("workspace_ssh_host_key_fingerprint", ""),
        ("pinned_runtime_generation_contract", True),
        ("pinned_runtime_generation_contract", "1"),
        ("session_runtime_generation", None),
        ("session_runtime_generation", "not-a-uuid"),
    ],
)
def test_ready_contract_requires_exact_workspace_and_runtime_identity(field, value):
    payload = _ready_payload()
    payload[field] = value

    with pytest.raises(session_contract.ProtectedCloudUnavailable):
        session_workspace.protected_workspace_delivery(payload)


def test_ready_remote_must_match_attested_pod_tuple_exactly():
    payload = _ready_payload()
    payload["ssh_key_path"] = "/run/secrets/vm-ssh-key"
    payload["remote"] = {
        "host": payload["pod_ip"],
        "port": payload["pod_port"],
        "username": "agent-host",
        "key_path": payload["ssh_key_path"],
        "workspace_path": "/home/agent-host/workspace",
    }
    assert session_workspace.protected_workspace_delivery(payload) == "ready"

    for field, value in (
        ("host", "10.42.0.99"),
        ("port", 22),
        ("username", "root"),
        ("key_path", "/tmp/key"),
        ("workspace_path", "/tmp/workspace"),
    ):
        changed = copy.deepcopy(payload)
        changed["remote"][field] = value
        with pytest.raises(session_contract.ProtectedCloudUnavailable):
            session_workspace.protected_workspace_delivery(changed)


@pytest.mark.asyncio
async def test_workspace_poll_waits_for_engage_then_preserves_ready_contract():
    engaging = {
        "protected_cloud": True,
        "protected_cloud_state": "engaging",
        "protected_cloud_error_code": None,
        "status": "creating",
    }
    client = SimpleNamespace(
        get_thread_workspace=AsyncMock(side_effect=[engaging, _ready_payload()])
    )

    normalized = await session_workspace.poll_workspace_ready(
        client,
        "thread-protected",
        timeout=1,
        poll_interval=0,
        raise_on_denied=True,
        session_runtime_generation=None,
    )

    assert normalized is not None
    assert session_workspace.protected_workspace_delivery(normalized) == "ready"
    assert normalized["status"] == "ready"
    assert normalized["pod_ip"] == "10.42.0.10"
    assert normalized["remote"]["host"] == "10.42.0.10"
    assert client.get_thread_workspace.await_count == 2


@pytest.mark.asyncio
async def test_workspace_poll_treats_failed_engage_as_terminal_without_constructing():
    client = SimpleNamespace(
        get_thread_workspace=AsyncMock(
            return_value={
                "protected_cloud": True,
                "protected_cloud_state": "failed",
                "protected_cloud_error_code": "engage_refused",
                "status": "failed",
            }
        )
    )

    with pytest.raises(
        session_contract.ProtectedCloudUnavailable, match="engage_refused"
    ):
        await session_workspace.poll_workspace_ready(
            client,
            "thread-protected",
            timeout=1,
            poll_interval=0,
            raise_on_denied=True,
            session_runtime_generation=None,
        )
    assert client.get_thread_workspace.await_count == 1


@pytest.mark.parametrize(
    "path,value",
    [
        ("backend", "vm"),
        ("backend", 1),
        ("config_override", 1),
        ("config_override", {"workspace": 1}),
        ("config_override", {"workspace": {"backend": 1}}),
        ("config_override", {"workspace": {"backend": "vm"}}),
        ("resolved_config", 1),
        ("resolved_config", {"agent": 1}),
        ("resolved_config", {"agent": {"workspace": 1}}),
        ("resolved_config", {"agent": {"workspace": {"backend": 1}}}),
        ("resolved_config", {"agent": {"workspace": {"backend": "none"}}}),
        ("vm_status", "ready"),
        ("vm_ssh_host", "vm.invalid"),
        ("vm_ssh_port", 22),
        ("vm_name", "vm-secret"),
    ],
)
def test_ready_contract_rejects_unsupported_or_malformed_backend_shapes(path, value):
    payload = _ready_payload()
    _set_path(payload, path, value)

    with pytest.raises(session_contract.ProtectedCloudUnavailable):
        session_workspace.protected_workspace_delivery(payload)


def test_ready_contract_requires_at_least_one_exact_sandbox_declaration():
    payload = _ready_payload()
    payload.pop("backend")

    with pytest.raises(session_contract.ProtectedCloudUnavailable):
        session_workspace.protected_workspace_delivery(payload)


@pytest.mark.parametrize(
    "field,value", [("cloud_sync", {}), ("nc_session_folder", "Sessions/x")]
)
def test_ready_contract_rejects_legacy_live_write_surfaces(field, value):
    payload = _ready_payload()
    payload[field] = value

    with pytest.raises(session_contract.ProtectedCloudUnavailable):
        session_workspace.protected_workspace_delivery(payload)


@pytest.mark.parametrize(
    "path,value",
    [
        ("version", True),
        ("version", 2),
        ("driver", "other"),
        ("protected", 1),
        ("skip_workspace_links", False),
        ("fallback", True),
        ("overlay.lower", "/tmp/lower"),
        ("overlay.merged", "/tmp/merged"),
        ("overlay.upper", "/home/agent-host"),
        ("overlay.work", "/cloud/lower"),
        ("overlay.quota_bytes", True),
        ("overlay.quota_bytes", 0),
        ("mounts.0.mount_id", ""),
        ("mounts.0.mount_kind", "project"),
        ("mounts.0.backend", "s3"),
        ("mounts.0.target_path", "/cloud/merged"),
        ("mounts.0.workspace_name", "cloud"),
        ("mounts.0.access", "read_write"),
        ("mounts.0.source.type", "s3"),
        ("mounts.0.source.config.vendor", "other"),
        ("mounts.0.source.config.url", 1),
        ("mounts.0.source.config.user", ""),
        ("mounts.0.auth.type", "bearer"),
        ("mounts.0.auth.password", 1),
        ("mounts.0.auth.password", ""),
    ],
)
def test_each_mount_field_that_drives_mount_or_cleanup_is_exact(path, value):
    payload = _ready_payload()
    mount = copy.deepcopy(payload["cloud_mount"])
    _set_path(mount, path, value)
    payload["cloud_mount"] = mount

    with pytest.raises(session_contract.ProtectedCloudUnavailable):
        session_workspace.protected_workspace_delivery(payload)


def test_protected_mount_rejects_a_second_live_cloud_surface():
    payload = _ready_payload()
    payload["cloud_mount"]["mounts"].append(
        {
            "mount_id": "bypass",
            "mount_kind": "session_folder",
            "backend": "nextcloud",
            "target_path": "/cloud/live",
            "access": "read_write",
        }
    )

    with pytest.raises(session_contract.ProtectedCloudUnavailable):
        session_workspace.protected_workspace_delivery(payload)


@pytest.mark.asyncio
@pytest.mark.parametrize("protected_value", [1, "true"])
async def test_mixed_off_marker_aborts_before_session_or_manager_construction(
    monkeypatch, protected_value
):
    payload = {
        "status": "ready",
        "backend": "sandbox",
        "pod_ip": "10.42.0.10",
        "pod_port": 30022,
        "remote": {"host": "10.42.0.10", "port": 30022},
        "protected_cloud": False,
        "cloud_mount": {
            "protected": protected_value,
            "overlay": _protected_mount()["overlay"],
            "mounts": _protected_mount()["mounts"],
        },
    }
    fake_agent = SimpleNamespace(
        config=object(),
        _tactical_llm=None,
        _llm=object(),
        _auxiliary_llm=object(),
        postgres_conn=None,
        vector_conn=None,
    )
    client = SimpleNamespace(get_thread_workspace=AsyncMock(return_value=payload))
    session_constructor = MagicMock()
    monkeypatch.setattr(persistent_app, "_agent", fake_agent)
    monkeypatch.setattr(persistent_app, "_orchestrator_client", client)
    monkeypatch.setattr(persistent_app, "_session", None)
    monkeypatch.setattr(persistent_app._session_identity, "_thread_id", None)

    with (
        patch.object(
            session_workspace,
            "poll_workspace_ready",
            new=AsyncMock(return_value=payload),
        ),
        patch.object(persistent_app, "PersistentSession", session_constructor),
    ):
        with pytest.raises(session_contract.ProtectedCloudUnavailable):
            await persistent_app._session_attach.attach(
                "thread-mixed",
                config_override={"workspace": {"backend": "sandbox"}},
            )

    session_constructor.assert_not_called()
    assert persistent_app._session is None
    assert persistent_app._session_identity.thread_id is None


def _runtime_client(workspace_responses: list[dict]):
    client = MagicMock()
    client.agent_id = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
    client.session_runtime_generation = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
    client.session_runtime_attach_token = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"
    client.pinned_runtime_generation_contract = True
    client.adopt_session_runtime_identity = MagicMock(return_value=True)
    client.clear_session_runtime_identity = MagicMock(return_value=True)
    client.get_thread_workspace = AsyncMock(side_effect=workspace_responses)
    return client


@pytest.mark.asyncio
async def test_dedicated_attach_initial_engaging_polls_to_ready(monkeypatch):
    engaging = {
        "protected_cloud": True,
        "protected_cloud_state": "engaging",
        "protected_cloud_error_code": None,
        "status": "creating",
    }
    ready = _ready_payload()
    # Initial peek, poll winner, pre-construction revalidation and post-setup
    # revalidation. Every credential-bearing response is the same exact
    # physical/runtime authority.
    client = _runtime_client(
        [engaging, copy.deepcopy(ready), copy.deepcopy(ready), copy.deepcopy(ready)]
    )
    client.runtime_actor = None
    effective_config = SimpleNamespace(
        llm=SimpleNamespace(model="test-model"),
        limits=SimpleNamespace(),
        extra={},
        auxiliary=None,
        workspace=SimpleNamespace(backend="sandbox"),
    )
    fake_agent = SimpleNamespace(
        config=effective_config,
        _tactical_llm=object(),
        _llm=object(),
        _auxiliary_llm=object(),
        postgres_conn=None,
        vector_conn=None,
    )
    session = MagicMock()
    session.setup = AsyncMock()
    session.recover_subagents = AsyncMock()
    session.cleanup = AsyncMock()
    session.protected_cloud_ready.return_value = True
    session.cloud_mount_manager = SimpleNamespace(active=True, mounts=[])
    session.cloud_mount_error = None
    session.workspace_manager = None
    session.workspace_sync = None
    session.tool_context = None
    session.postgres_conn = None
    constructor = MagicMock(return_value=session)

    for name, value in (
        ("_agent", fake_agent),
        ("_orchestrator_client", client),
        ("_session", None),
        ("_event_writer", None),
        ("_session_side_tasks", set()),
    ):
        monkeypatch.setattr(*termination_target(persistent_app, name), value)
    for name in (
        "_thread_id",
        "_process_generation",
        "_session_generation",
        "_attach_token",
    ):
        monkeypatch.setattr(persistent_app._session_identity, name, None)
    for name in (
        "_queue",
        "_interrupt_mode",
        "_interrupt_target_turn_id",
        "_hard_interrupt_event",
    ):
        monkeypatch.setattr(persistent_app._session_input, name, None)

    with (
        patch.object(persistent_app, "PersistentSession", constructor),
        patch.object(session_attach, "apply_session_embedding_env"),
        patch.object(persistent_app, "_wire_session_aux_archiver"),
        patch.object(persistent_app, "_restore_session_messages", new=AsyncMock()),
        patch.object(
            persistent_app,
            "_update_thread_status",
            new=AsyncMock(return_value=True),
        ) as update_status,
        patch.object(persistent_app._session_input, "reclaim_pending", new=AsyncMock()),
        patch.object(persistent_app._session_termination, "start_watchdogs"),
        patch.object(persistent_app, "_officer_cfg", return_value=None),
        patch.object(persistent_app, "_broadcast"),
        patch("agent.tools.registry.register_mcp_tools"),
        patch(
            "agent.services.knowledge.bindings.build_knowledge_bindings",
            return_value=[],
        ),
    ):
        await persistent_app._session_attach.attach(
            "thread-protected",
            pinned_runtime_generation_contract=1,
            session_runtime_generation=client.session_runtime_generation,
            session_runtime_attach_token=client.session_runtime_attach_token,
        )

    assert client.get_thread_workspace.await_count == 4
    constructor.assert_called_once()
    assert constructor.call_args.kwargs["protected_cloud_required"] is True
    session.setup.assert_awaited_once()
    assert session.setup.call_args.kwargs["cloud_mount_cfg"] == ready["cloud_mount"]
    assert session.protected_workspace_generation == ready["workspace_generation"]
    assert (
        session.protected_workspace_runtime_incarnation
        == (ready["workspace_runtime_incarnation"])
    )
    update_status.assert_awaited_once_with("active")
    assert persistent_app._session is session
    assert UUID(str(persistent_app._session_identity.process_generation))
    assert (
        persistent_app._session_identity.process_generation
        != client.session_runtime_generation
    )

    # This is a successful live attach, so invoking the delivered-attach abort
    # protocol here would now (correctly) require a real workspace process-zero
    # receipt and rotate its runtime generation.  The monkeypatch fixture owns
    # process-global restoration at test teardown instead.


def test_strict_pinned_input_identity_keeps_process_generation_separate(monkeypatch):
    client = _runtime_client([])
    monkeypatch.setattr(persistent_app, "_orchestrator_client", client)
    monkeypatch.setattr(persistent_app._session_identity, "_runtime_contract", True)
    monkeypatch.setattr(
        persistent_app._session_identity,
        "_session_generation",
        client.session_runtime_generation,
    )
    monkeypatch.setattr(
        persistent_app._session_identity,
        "_attach_token",
        client.session_runtime_attach_token,
    )
    monkeypatch.setattr(
        persistent_app._session_identity,
        "_process_generation",
        "ffffffff-ffff-4fff-8fff-ffffffffffff",
    )
    monkeypatch.setenv("POD_UID", "pod-uid")

    assert persistent_app._session_input.pinned_identity() == (
        client.agent_id,
        "pod-uid",
        "ffffffff-ffff-4fff-8fff-ffffffffffff",
        client.session_runtime_attach_token,
    )


@pytest.mark.asyncio
async def test_dedicated_attach_initial_engaging_timeout_fails_closed(monkeypatch):
    engaging = {
        "protected_cloud": True,
        "protected_cloud_state": "engaging",
        "protected_cloud_error_code": None,
        "status": "creating",
    }
    client = _runtime_client([engaging])
    client.runtime_actor = None
    constructor = MagicMock()
    poll = AsyncMock(return_value=None)
    monkeypatch.setattr(persistent_app, "_orchestrator_client", client)
    monkeypatch.setattr(persistent_app, "_session", None)
    monkeypatch.setattr(persistent_app._session_identity, "_thread_id", None)
    monkeypatch.setattr(persistent_app, "_event_writer", None)
    monkeypatch.setattr(persistent_app._session_identity, "_session_generation", None)
    monkeypatch.setattr(persistent_app._session_identity, "_attach_token", None)
    monkeypatch.setattr(persistent_app._session_attach, "_release_receipt", None)
    monkeypatch.setattr(persistent_app._session_attach, "_cleanup_context", None)
    # A real delivered runtime always has this Kubernetes identity. It is part
    # of the exact pre-setup abort receipt, so omitting it would correctly keep
    # the new cleanup owner retrying instead of returning the original timeout.
    monkeypatch.setenv("POD_UID", "pod-uid-protected-timeout")

    with (
        patch.object(session_workspace, "poll_workspace_ready", new=poll),
        patch.object(persistent_app, "PersistentSession", constructor),
    ):
        with pytest.raises(
            persistent_app.WorkspaceNotReady,
            match="No workspace container provisioned",
        ):
            await persistent_app._session_attach.attach(
                "thread-protected",
                pinned_runtime_generation_contract=1,
                session_runtime_generation=client.session_runtime_generation,
                session_runtime_attach_token=client.session_runtime_attach_token,
            )

    poll.assert_awaited_once()
    constructor.assert_not_called()
    assert persistent_app._session is None
    # The unconfirmed release still owns this exact life. Clearing identity
    # here would strand its retry and stop valid booting/ready heartbeats.
    assert persistent_app._session_identity.thread_id == "thread-protected"
    assert persistent_app._session_attach._release_receipt is not None
    client.clear_session_runtime_identity.assert_not_called()


def test_advertised_pinned_runtime_contract_requires_exact_attach_token(monkeypatch):
    monkeypatch.delenv("STATELESS_EXECUTOR", raising=False)
    client = _runtime_client([])
    client.session_runtime_attach_token = None
    monkeypatch.setattr(persistent_app, "_orchestrator_client", client)
    monkeypatch.setattr(persistent_app._session_identity, "_session_generation", None)
    monkeypatch.setattr(persistent_app._session_identity, "_attach_token", None)

    with pytest.raises(
        persistent_app.WorkspaceNotReady,
        match="generation or attach token",
    ):
        persistent_app._session_identity.adopt(
            client.session_runtime_generation,
            None,
            contract_advertised=True,
        )

    assert persistent_app._session_identity.session_generation is None
    assert persistent_app._session_identity.attach_token is None


def test_stateless_claim_uses_generation_without_pinned_attach_token(monkeypatch):
    monkeypatch.setenv("STATELESS_EXECUTOR", "1")
    client = _runtime_client([])
    client.session_runtime_attach_token = None
    monkeypatch.setattr(persistent_app, "_orchestrator_client", client)
    monkeypatch.setattr(persistent_app._session_identity, "_session_generation", None)
    monkeypatch.setattr(persistent_app._session_identity, "_attach_token", None)

    persistent_app._session_identity.adopt(
        client.session_runtime_generation,
        None,
        contract_advertised=True,
    )

    assert (
        persistent_app._session_identity.session_generation
        == client.session_runtime_generation
    )
    assert persistent_app._session_identity.attach_token is None


@pytest.mark.asyncio
async def test_workspace_identity_change_before_constructor_fails_closed(monkeypatch):
    initial = _normalized_ready()
    replacement = _ready_payload()
    replacement["pod_ip"] = "10.42.0.99"
    replacement["workspace_runtime_incarnation"] = (
        "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"
    )
    client = _runtime_client([replacement])
    fake_agent = SimpleNamespace(
        config=object(),
        _tactical_llm=None,
        _llm=object(),
        _auxiliary_llm=object(),
        postgres_conn=None,
        vector_conn=None,
    )
    constructor = MagicMock()
    monkeypatch.setattr(persistent_app, "_agent", fake_agent)
    monkeypatch.setattr(persistent_app, "_orchestrator_client", client)
    monkeypatch.setattr(persistent_app, "_session", None)
    monkeypatch.setattr(persistent_app._session_identity, "_thread_id", None)
    monkeypatch.setattr(persistent_app._session_attach, "_release_receipt", None)
    monkeypatch.setattr(persistent_app._session_attach, "_cleanup_context", None)
    monkeypatch.setenv("POD_UID", "pod-uid-identity-before")

    with (
        patch.object(
            session_workspace,
            "poll_workspace_ready",
            new=AsyncMock(return_value=initial),
        ),
        patch.object(persistent_app, "PersistentSession", constructor),
    ):
        with pytest.raises(
            session_contract.ProtectedCloudUnavailable,
            match="identity changed before setup",
        ):
            await persistent_app._session_attach.attach(
                "thread-protected",
                config_override={"workspace": {"backend": "sandbox"}},
                pinned_runtime_generation_contract=1,
                session_runtime_generation=client.session_runtime_generation,
            )

    constructor.assert_not_called()
    assert persistent_app._session is None


@pytest.mark.asyncio
async def test_workspace_identity_change_during_setup_rolls_back(monkeypatch):
    initial = _normalized_ready()
    same_before_setup = _ready_payload()
    replacement = _ready_payload()
    replacement["workspace_generation"] = "ffffffff-ffff-4fff-8fff-ffffffffffff"
    client = _runtime_client([same_before_setup, replacement])
    effective_config = SimpleNamespace(
        llm=SimpleNamespace(model="test-model"),
        limits=SimpleNamespace(),
        extra={},
        auxiliary=None,
    )
    fake_agent = SimpleNamespace(
        config=effective_config,
        _tactical_llm=object(),
        _llm=object(),
        _auxiliary_llm=object(),
        postgres_conn=None,
        vector_conn=None,
    )
    session = MagicMock()
    session.setup = AsyncMock()
    session.cleanup = AsyncMock()
    session.protected_cloud_ready.return_value = True
    # Model the strict physical cleanup receipt produced by a real session;
    # the identity-race assertion must not hang in the independent abort retry
    # owner merely because this test double omitted its post-cleanup proof.
    session.local_quiescence_protocol = "workspace_process_zero_v1"
    session.workspace_generation = initial["workspace_generation"]
    session.workspace_runtime_incarnation = initial["workspace_runtime_incarnation"]
    constructor = MagicMock(return_value=session)
    monkeypatch.setattr(persistent_app, "_agent", fake_agent)
    monkeypatch.setattr(persistent_app, "_orchestrator_client", client)
    monkeypatch.setattr(persistent_app, "_session", None)
    monkeypatch.setattr(persistent_app._session_identity, "_thread_id", None)
    monkeypatch.setattr(persistent_app._session_attach, "_release_receipt", None)
    monkeypatch.setattr(persistent_app._session_attach, "_cleanup_context", None)
    monkeypatch.setenv("POD_UID", "pod-uid-identity-during")

    with (
        patch.object(
            session_workspace,
            "poll_workspace_ready",
            new=AsyncMock(return_value=initial),
        ),
        patch.object(persistent_app, "PersistentSession", constructor),
        patch.object(
            persistent_app,
            "_llm_config_with_cache_key",
            side_effect=lambda value: value,
        ),
        patch(
            "shared.runtime.core.loader.load_config_from_resolved",
            return_value=effective_config,
        ),
        patch("shared.runtime.core.loader.create_llm", return_value=object()),
    ):
        with pytest.raises(
            session_contract.ProtectedCloudUnavailable,
            match="authority changed during setup",
        ):
            await persistent_app._session_attach.attach(
                "thread-protected",
                resolved_config={"agent": {}},
                pinned_runtime_generation_contract=1,
                session_runtime_generation=client.session_runtime_generation,
            )

    constructor.assert_called_once()
    session.setup.assert_awaited_once()
    session.cleanup.assert_awaited()
    assert persistent_app._session is None
