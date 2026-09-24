"""Focused safety tests for stateless session workspace admission."""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tests import _b09_control_seams as control_seams

# R1.B06: main no longer re-exports this. It belongs to
# services/session_class_policy and lane B's admission path imports it
# from there directly.
from orchestrator.services import session_class_policy  # noqa: E402

# R1.B06: these handlers moved to services/thread_config_update with their
# routes in routers/thread_config. main's dependency factory still reads
# main's attributes at call time, so the patches below keep steering what
# they steered before.
from orchestrator.services import thread_config_update  # noqa: E402
from fastapi import HTTPException

from orchestrator.services.stateless_workspace_gate import (
    stateless_session_workspace_check,
    stateless_workspace_check,
)
from orchestrator.services import grant_enforcement  # noqa: E402


THREAD_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
VIRTUAL_GENERATION = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
SANDBOX_GENERATION = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
SANDBOX_RUNTIME = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
CREATION_GENERATION = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"


def _thread(metadata):
    return {
        "id": THREAD_ID,
        "execution_lane": "stateless",
        "metadata": metadata,
    }


def _metadata(backend: str = "virtual", **extra):
    return {
        "config_override": {"workspace": {"backend": backend}},
        **extra,
    }


def _k8s_sandbox_metadata(*, status: str = "ready", **workspace_extra):
    workspace = {
        "status": status,
        "provisioner": "k8s",
    }
    metadata = _metadata("sandbox", workspace_container=workspace)
    if status == "ready":
        workspace.update(
            {
                "pod_ip": "10.42.0.25",
                "port": 30022,
                "pod_name": "ws-thread-aaaaaaaaaaaa",
                "namespace": "agent-workspaces",
                "_canvas_workspace_generation": SANDBOX_GENERATION,
                "_runtime_incarnation": SANDBOX_RUNTIME,
            }
        )
        metadata["_workspace_binding"] = {
            "generation": SANDBOX_GENERATION,
            "kind": "remote",
            "backing_id": "k8s-pvc:agent-workspaces:pvc-uid",
            "ssh_host_key_fingerprint": "SHA256:trusted",
        }
    workspace.update(workspace_extra)
    return metadata


def _creation_marker(*, mode="create", attempted=False, replaces_uid=None, **extra):
    return {
        "generation": CREATION_GENERATION,
        "mode": mode,
        "attempted": attempted,
        "replaces_uid": replaces_uid,
        **extra,
    }


def test_record_shaped_mapping_preserves_stateless_sandbox_authority():
    class RecordLike:
        """Match asyncpg.Record's duck type without registering as Mapping."""

        def __init__(self, values):
            self._values = values

        def get(self, key, default=None):
            return self._values.get(key, default)

    record = RecordLike(_thread(_k8s_sandbox_metadata()))

    assert stateless_session_workspace_check(record) == ("sandbox", None)


@pytest.mark.parametrize(
    "metadata",
    [
        _metadata("virtual"),
        _metadata(
            "none",
            workspace_container={
                "git_remote_url": "ssh://gitea/thread.git",
                "repo_name": "thread-aaaaaaaa",
            },
        ),
        _metadata(
            "none",
            workspace_container={"volume_reclaimed": False},
        ),
        json.dumps(
            _metadata(
                "virtual",
                vm={},
                workspace_container={},
                _workspace_binding={
                    "generation": VIRTUAL_GENERATION,
                    "kind": "virtual",
                    "backing_id": "rclone:0123456789abcdef",
                    "ssh_host_key_fingerprint": None,
                },
            )
        ),
    ],
    ids=[
        "virtual",
        "none-with-gitea",
        "none-with-settled-volume-outcome",
        "valid-virtual-binding-json",
    ],
)
def test_classifier_admits_only_unmaterialized_lite_workspaces(metadata):
    backend, refusal = stateless_workspace_check(_thread(metadata))

    assert backend in {"virtual", "none"}
    assert refusal is None


@pytest.mark.parametrize(
    "metadata",
    [
        _k8s_sandbox_metadata(),
        _k8s_sandbox_metadata(status="pending"),
        _k8s_sandbox_metadata(status="creating", pod_name="ws-thread-aaaaaaaaaaaa"),
        _k8s_sandbox_metadata(status="restoring"),
        _k8s_sandbox_metadata(status="suspended"),
        _k8s_sandbox_metadata(status="failed"),
        _k8s_sandbox_metadata(status="deleted"),
    ],
    ids=["ready", "pending", "creating", "restoring", "suspended", "failed", "deleted"],
)
def test_classifier_admits_attested_k8s_sandbox_lifecycle(metadata):
    backend, refusal = stateless_workspace_check(_thread(metadata))

    assert backend == "sandbox"
    assert refusal is None


@pytest.mark.parametrize(
    "metadata",
    [
        {**_k8s_sandbox_metadata(), "protected_cloud": True},
        _metadata("virtual", protected_cloud=True),
        _metadata("none", protected_cloud="true"),
        json.dumps(_metadata("sandbox", protected_cloud=True)),
    ],
    ids=["sandbox", "virtual", "malformed-none", "json"],
)
def test_classifier_keeps_protected_cloud_on_pinned_plane(metadata):
    backend, refusal = stateless_workspace_check(_thread(metadata))

    assert backend in {"sandbox", "virtual", "none"}
    assert refusal == "protected_cloud_requires_pinned"


def test_classifier_accepts_exact_disabled_protected_cloud_marker():
    metadata = _metadata("virtual", protected_cloud=False)

    assert stateless_workspace_check(_thread(metadata)) == ("virtual", None)


@pytest.mark.parametrize(
    "malformed",
    [None, 0, "", [], {}],
    ids=["null", "zero", "empty-string", "empty-list", "empty-map"],
)
def test_classifier_refuses_present_malformed_protected_cloud_marker(malformed):
    metadata = _metadata("virtual", protected_cloud=malformed)

    assert stateless_workspace_check(_thread(metadata)) == (
        "virtual",
        "protected_cloud_requires_pinned",
    )


@pytest.mark.parametrize("restore_required", [True, False])
def test_classifier_accepts_exact_boolean_restore_marker(restore_required):
    metadata = _k8s_sandbox_metadata(
        _snapshot_restore_required=restore_required,
    )

    assert stateless_workspace_check(_thread(metadata)) == ("sandbox", None)


@pytest.mark.parametrize(
    "malformed",
    [None, 0, "", [], {}],
    ids=["null", "zero", "empty-string", "empty-list", "empty-map"],
)
def test_classifier_refuses_malformed_restore_marker(malformed):
    metadata = _k8s_sandbox_metadata(_snapshot_restore_required=malformed)

    assert stateless_workspace_check(_thread(metadata)) == (
        "sandbox",
        "workspace_restore_marker_malformed",
    )


@pytest.mark.parametrize(
    "status", ["pending", "created", "creating", "failed", "deleted"]
)
@pytest.mark.parametrize("attempted", [False, True])
def test_classifier_accepts_exact_in_progress_creation_marker(status, attempted):
    metadata = _k8s_sandbox_metadata(
        status=status,
        _runtime_creation=_creation_marker(attempted=attempted),
    )

    assert stateless_session_workspace_check(_thread(metadata)) == ("sandbox", None)


def test_classifier_accepts_restore_creation_marker_only_with_exact_restore_intent():
    metadata = _k8s_sandbox_metadata(
        status="restoring",
        _snapshot_restore_required=True,
        _runtime_creation=_creation_marker(
            mode="restore",
            replaces_uid=SANDBOX_RUNTIME,
        ),
    )

    assert stateless_session_workspace_check(_thread(metadata)) == ("sandbox", None)


def test_classifier_refuses_ready_credentials_while_creation_marker_remains():
    metadata = _k8s_sandbox_metadata(
        _runtime_creation=_creation_marker(attempted=True),
    )

    assert stateless_session_workspace_check(_thread(metadata)) == (
        "sandbox",
        "workspace_creation_in_progress",
    )


@pytest.mark.parametrize(
    "marker",
    [
        None,
        False,
        0,
        "",
        [],
        {},
        _creation_marker(generation="EEEEEEEE-EEEE-4EEE-8EEE-EEEEEEEEEEEE"),
        _creation_marker(attempted=1),
        _creation_marker(mode="recreate"),
        _creation_marker(replaces_uid="not-a-uuid"),
        _creation_marker(extra=True),
    ],
    ids=[
        "null",
        "false",
        "zero",
        "empty-string",
        "empty-list",
        "empty-map",
        "noncanonical-generation",
        "numeric-attempted",
        "bad-mode",
        "bad-replacement-uid",
        "extra-field",
    ],
)
def test_classifier_refuses_malformed_creation_marker_at_all_statuses(marker):
    metadata = _k8s_sandbox_metadata(
        status="creating",
        _runtime_creation=marker,
    )

    assert stateless_session_workspace_check(_thread(metadata)) == (
        "sandbox",
        "workspace_creation_marker_malformed",
    )


@pytest.mark.parametrize(
    ("mode", "restore_required"),
    [("create", True), ("restore", False)],
)
def test_classifier_refuses_creation_mode_restore_intent_mismatch(
    mode, restore_required
):
    metadata = _k8s_sandbox_metadata(
        status="restoring",
        _snapshot_restore_required=restore_required,
        _runtime_creation=_creation_marker(mode=mode),
    )

    assert stateless_session_workspace_check(_thread(metadata)) == (
        "sandbox",
        "workspace_creation_marker_malformed",
    )


@pytest.mark.parametrize(
    ("metadata", "reason"),
    [
        (_metadata("sandbox"), "workspace_context_missing"),
        ({}, "declared_backend_unsupported"),
        (_metadata("virtual", vm={"status": "ready"}), "vm_context_present"),
        (_metadata("virtual", vm=[]), "vm_context_malformed"),
        (
            _metadata("virtual", workspace_container={"status": "pending"}),
            "workspace_context_present",
        ),
        (
            _metadata("virtual", workspace_container=[]),
            "workspace_context_malformed",
        ),
        (
            _metadata("none", workspace_container={"volume_reclaimed": "false"}),
            "workspace_context_malformed",
        ),
        (
            _metadata(
                "virtual",
                _workspace_binding={
                    "generation": VIRTUAL_GENERATION,
                    "kind": "remote",
                    "backing_id": "k8s-pod:srw:uid",
                    "ssh_host_key_fingerprint": "SHA256:remote",
                },
            ),
            "non_virtual_workspace_binding",
        ),
        (
            _metadata("virtual", _workspace_binding=[]),
            "non_virtual_workspace_binding",
        ),
        (
            _metadata(
                "virtual",
                _workspace_binding={
                    "generation": "not-a-uuid",
                    "kind": "virtual",
                    "backing_id": "rclone:0123",
                    "ssh_host_key_fingerprint": None,
                },
            ),
            "virtual_workspace_binding_malformed",
        ),
        (
            _metadata(
                "virtual",
                _workspace_binding={
                    "generation": VIRTUAL_GENERATION,
                    "kind": "virtual",
                    "backing_id": "rclone:0123",
                    "ssh_host_key_fingerprint": None,
                    "future_identity": "must-not-degrade-open",
                },
            ),
            "virtual_workspace_binding_malformed",
        ),
        ("not-json", "declared_backend_unsupported"),
    ],
    ids=[
        "sandbox",
        "missing-backend",
        "vm-materialized",
        "vm-malformed",
        "workspace-materialized",
        "workspace-malformed",
        "settled-volume-outcome-malformed",
        "remote-binding",
        "binding-malformed",
        "virtual-binding-malformed",
        "virtual-binding-extra-key",
        "metadata-malformed",
    ],
)
def test_classifier_refuses_non_lite_physical_and_malformed_state(metadata, reason):
    _backend, refusal = stateless_workspace_check(_thread(metadata))

    assert refusal == reason


@pytest.mark.parametrize(
    ("metadata", "reason"),
    [
        (
            _metadata(
                "sandbox",
                workspace_container={"status": "ready", "provisioner": "docker"},
            ),
            "workspace_not_k8s",
        ),
        (
            _metadata(
                "sandbox",
                workspace_container={"status": "future", "provisioner": "k8s"},
            ),
            "workspace_status_unavailable",
        ),
        (
            _k8s_sandbox_metadata(
                _runtime_incarnation="not-a-uuid",
            ),
            "workspace_runtime_incarnation_malformed",
        ),
        (
            {
                **_k8s_sandbox_metadata(),
                "_workspace_binding": {
                    "generation": SANDBOX_GENERATION,
                    "kind": "remote",
                    "backing_id": "docker:workspace-1",
                    "ssh_host_key_fingerprint": "SHA256:trusted",
                },
            },
            "remote_workspace_binding_malformed",
        ),
        (
            _k8s_sandbox_metadata(
                _canvas_workspace_generation=VIRTUAL_GENERATION,
            ),
            "workspace_generation_mismatch",
        ),
        (
            {
                **_k8s_sandbox_metadata(),
                "workspace_container": {
                    **_k8s_sandbox_metadata()["workspace_container"],
                    "_runtime_incarnation": None,
                },
            },
            "workspace_runtime_incarnation_missing",
        ),
        (
            {
                **_k8s_sandbox_metadata(),
                "workspace_container": {
                    **_k8s_sandbox_metadata()["workspace_container"],
                    "pod_ip": None,
                    "host": None,
                },
            },
            "workspace_endpoint_missing",
        ),
    ],
    ids=[
        "docker",
        "future-status",
        "runtime-malformed",
        "non-k8s-binding",
        "generation-mismatch",
        "runtime-missing",
        "endpoint-missing",
    ],
)
def test_classifier_refuses_unattested_or_non_k8s_sandbox(metadata, reason):
    backend, refusal = stateless_workspace_check(_thread(metadata))

    assert backend == "sandbox"
    assert refusal == reason


def test_session_admission_defaults_to_pinned_while_pool_is_off(monkeypatch):
    from orchestrator import main as orch_main

    monkeypatch.setattr(orch_main, "STATELESS_SESSION_ENABLED", False)

    for backend in ("sandbox", "virtual", "none", "vm"):
        assert (
            session_class_policy.resolve_thread_execution_lane(
                workspace_backend=backend,
                effective_config={"workspace": {"backend": backend}},
                dependencies=orch_main._execution_lane_dependencies(),
            )
            == "pinned"
        )


@pytest.mark.parametrize("backend", ["sandbox", "virtual", "none"])
def test_enabled_pool_auto_admits_supported_ordinary_tiers(monkeypatch, backend):
    from orchestrator import main as orch_main

    monkeypatch.setattr(orch_main, "STATELESS_SESSION_ENABLED", True)
    monkeypatch.setattr(
        orch_main,
        "container_provisioner",
        SimpleNamespace(is_available=True, in_cluster=True),
    )
    monkeypatch.setattr(
        orch_main,
        "_virtual_workspace_rclone_spec",
        lambda: {"type": "s3", "root": "workspaces"},
    )

    assert (
        session_class_policy.resolve_thread_execution_lane(
            workspace_backend=backend,
            effective_config={"workspace": {"backend": backend}},
            dependencies=orch_main._execution_lane_dependencies(),
        )
        == "stateless"
    )


def test_enabled_pool_falls_back_to_pinned_when_supported_tier_is_unready(monkeypatch):
    from orchestrator import main as orch_main

    monkeypatch.setattr(orch_main, "STATELESS_SESSION_ENABLED", True)
    monkeypatch.setattr(
        orch_main,
        "container_provisioner",
        SimpleNamespace(is_available=True, in_cluster=False),
    )
    monkeypatch.setattr(
        orch_main, "_virtual_workspace_rclone_spec", lambda: {"type": "memory"}
    )

    assert (
        session_class_policy.resolve_thread_execution_lane(
            workspace_backend="sandbox",
            effective_config={"workspace": {"backend": "sandbox"}},
            dependencies=orch_main._execution_lane_dependencies(),
        )
        == "pinned"
    )
    assert (
        session_class_policy.resolve_thread_execution_lane(
            workspace_backend="virtual",
            effective_config={"workspace": {"backend": "virtual"}},
            dependencies=orch_main._execution_lane_dependencies(),
        )
        == "pinned"
    )


def test_public_create_contract_has_no_execution_lane_selector():
    from orchestrator import main as orch_main

    assert "execution_lane" not in orch_main.ThreadCreateRequest.model_fields
    with pytest.raises(ValueError, match="orchestrator-managed"):
        orch_main.ThreadCreateRequest(execution_lane="pinned")


@pytest.mark.parametrize("backend", ["vm", "remote", "future"])
def test_enabled_pool_keeps_unsupported_tiers_pinned_on_omission(monkeypatch, backend):
    from orchestrator import main as orch_main

    monkeypatch.setattr(orch_main, "STATELESS_SESSION_ENABLED", True)

    assert (
        session_class_policy.resolve_thread_execution_lane(
            workspace_backend=backend,
            effective_config={"workspace": {"backend": backend}},
            dependencies=orch_main._execution_lane_dependencies(),
        )
        == "pinned"
    )


@pytest.mark.parametrize("officer", [{"enabled": True}, {"conference": True}])
def test_pinned_only_session_classes_do_not_auto_admit(monkeypatch, officer):
    from orchestrator import main as orch_main

    monkeypatch.setattr(orch_main, "STATELESS_SESSION_ENABLED", True)
    config = {"workspace": {"backend": "none"}, "officer": officer}

    assert (
        session_class_policy.resolve_thread_execution_lane(
            workspace_backend="none",
            effective_config=config,
            dependencies=orch_main._execution_lane_dependencies(),
        )
        == "pinned"
    )


@pytest.mark.parametrize(
    "officer",
    [
        [],
        {"enabled": None},
        {"enabled": 0},
        {"enabled": "yes"},
        {"conference": "false"},
        {"conference": 1},
    ],
)
def test_malformed_session_class_pins_and_cannot_be_materialized(monkeypatch, officer):
    from orchestrator import main as orch_main

    monkeypatch.setattr(orch_main, "STATELESS_SESSION_ENABLED", True)
    config = {"workspace": {"backend": "none"}, "officer": officer}

    assert (
        session_class_policy.resolve_thread_execution_lane(
            workspace_backend="none",
            effective_config=config,
            dependencies=orch_main._execution_lane_dependencies(),
        )
        == "pinned"
    )
    with pytest.raises(HTTPException) as exc:
        session_class_policy.materialized_session_class_override(config)
    assert exc.value.status_code == 400


def test_materialized_ordinary_class_wins_over_later_expert_and_account_changes():
    from orchestrator import main as orch_main

    materialized = {
        "officer": session_class_policy.materialized_session_class_override(
            {"officer": {"enabled": False, "conference": False}}
        )
    }
    for mutable_lower_layer in (
        {"officer": {"enabled": True}},  # account default edited later
        {"officer": {"conference": True}},  # selected expert edited later
    ):
        effective = orch_main._deep_merge_dicts(mutable_lower_layer, materialized)
        assert session_class_policy.session_class_pinned_refusal(effective) is None


@pytest.mark.asyncio
async def test_stateless_config_patch_cannot_enable_pinned_only_session_class(
    monkeypatch,
):
    from orchestrator import main as orch_main

    metadata = _k8s_sandbox_metadata()
    metadata["config_override"]["officer"] = {
        "enabled": False,
        "conference": False,
    }
    thread = {
        **_thread(metadata),
        "user_id": None,
        "project_id": None,
    }
    db = MagicMock()
    db.merge_thread_config_override = AsyncMock(return_value=True)
    db.thread_configuration_transaction.return_value.__aenter__.return_value.fetchrow = AsyncMock(
        return_value=thread
    )
    monkeypatch.setattr(orch_main, "postgres_db", db)

    with pytest.raises(HTTPException, match="pinned-only") as exc:
        await control_seams.apply_thread_config_update(
            THREAD_ID,
            thread,
            {"officer": {"enabled": True}},
            None,
            request=MagicMock(),
            actor=None,
        )

    assert exc.value.status_code == 409
    db.merge_thread_config_override.assert_not_awaited()


@pytest.mark.asyncio
async def test_stateless_vm_upgrade_refuses_before_grants_or_provisioning():
    from orchestrator import main as orch_main

    db = MagicMock()
    db.get_thread = AsyncMock(
        return_value={
            "id": THREAD_ID,
            "execution_lane": "stateless",
            "metadata": _metadata(),
        }
    )
    provisioner = MagicMock()
    provisioner.is_available = True
    provisioner.create_thread_vm = AsyncMock()
    grants = AsyncMock()

    with (
        patch.object(orch_main, "require_internal", AsyncMock()),
        patch.object(orch_main, "postgres_db", db),
        patch.object(orch_main, "vm_provisioner", provisioner),
        patch.object(grant_enforcement, "enforce_workspace_upgrade_grants", grants),
    ):
        with pytest.raises(HTTPException) as exc:
            await thread_config_update.agent_upgrade_thread_to_vm(
                MagicMock(),
                THREAD_ID,
                dependencies=orch_main._thread_config_update_dependencies(),
            )

    assert exc.value.status_code == 409
    grants.assert_not_awaited()
    provisioner.create_thread_vm.assert_not_awaited()


@pytest.mark.asyncio
async def test_stateless_sandbox_upgrade_refuses_before_any_workspace_write():
    from orchestrator import main as orch_main

    db = MagicMock()
    db.get_thread = AsyncMock(
        return_value={
            "id": THREAD_ID,
            "execution_lane": "stateless",
            "metadata": _metadata(),
        }
    )
    db.merge_thread_workspace_context = AsyncMock()
    provisioner = MagicMock()
    provisioner.is_available = True
    provisioner.in_cluster = True
    provisioner.create_workspace = AsyncMock()
    grants = AsyncMock()

    with (
        patch.object(orch_main, "require_internal", AsyncMock()),
        patch.object(orch_main, "postgres_db", db),
        patch.object(orch_main, "container_provisioner", provisioner),
        patch.object(grant_enforcement, "enforce_workspace_upgrade_grants", grants),
    ):
        with pytest.raises(HTTPException) as exc:
            await thread_config_update.agent_upgrade_thread_to_workspace(
                MagicMock(),
                THREAD_ID,
                orch_main.ThreadWorkspaceUpgradeRequest(target_tier="sandbox"),
                dependencies=orch_main._thread_config_update_dependencies(),
            )

    assert exc.value.status_code == 409
    grants.assert_not_awaited()
    db.merge_thread_workspace_context.assert_not_awaited()
    provisioner.create_workspace.assert_not_awaited()


@pytest.mark.asyncio
async def test_stateless_config_cannot_mutate_workspace_tier():
    from orchestrator import main as orch_main

    db = MagicMock()
    db.merge_thread_config_override = AsyncMock()
    audit = AsyncMock()
    thread = {
        "id": THREAD_ID,
        "user_id": None,
        "execution_lane": "stateless",
        "metadata": _metadata(),
    }

    db.thread_configuration_transaction.return_value.__aenter__.return_value.fetchrow = AsyncMock(
        return_value=thread
    )

    with (
        patch.object(orch_main, "postgres_db", db),
        patch.object(orch_main, "log_security_event", audit),
    ):
        with pytest.raises(HTTPException) as exc:
            await control_seams.apply_thread_config_update(
                THREAD_ID,
                thread,
                {"workspace": {"backend": "sandbox"}},
                None,
                request=MagicMock(),
                actor=None,
            )

    assert exc.value.status_code == 409
    db.merge_thread_config_override.assert_not_awaited()
    audit.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("target_tier", ["sandbox", "vm"])
@pytest.mark.parametrize(
    "metadata",
    [
        {
            "protected_cloud": True,
            "config_override": {"workspace": {"backend": "sandbox"}},
        },
        {"protected_cloud": "true"},
        "not-json",
        ["not", "an", "object"],
    ],
    ids=["protected", "malformed-marker", "invalid-json", "list-metadata"],
)
async def test_protected_workspace_upgrade_refuses_before_every_effect(
    target_tier, metadata
):
    """Neither upgrade endpoint may move a protected/corrupt row off its
    pinned Container contract, including the VM delegation path."""

    from orchestrator import main as orch_main

    thread = {
        "id": THREAD_ID,
        "execution_lane": "pinned",
        "metadata": metadata,
    }
    db = MagicMock()
    db.get_thread = AsyncMock(return_value=thread)
    db.merge_thread_workspace_context = AsyncMock()
    vm = MagicMock(is_available=True)
    vm.create_thread_vm = AsyncMock()
    container = MagicMock(is_available=True, in_cluster=True)
    container.create_workspace = AsyncMock()
    grants = AsyncMock()

    with (
        patch.object(orch_main, "require_internal", AsyncMock()),
        patch.object(orch_main, "postgres_db", db),
        patch.object(orch_main, "vm_provisioner", vm),
        patch.object(orch_main, "container_provisioner", container),
        patch.object(grant_enforcement, "enforce_workspace_upgrade_grants", grants),
    ):
        with pytest.raises(HTTPException) as exc:
            await thread_config_update.agent_upgrade_thread_to_workspace(
                MagicMock(),
                THREAD_ID,
                orch_main.ThreadWorkspaceUpgradeRequest(target_tier=target_tier),
                dependencies=orch_main._thread_config_update_dependencies(),
            )

    assert exc.value.status_code == 409
    assert exc.value.detail["code"].startswith("protected_cloud_")
    grants.assert_not_awaited()
    db.merge_thread_workspace_context.assert_not_awaited()
    vm.create_thread_vm.assert_not_awaited()
    container.create_workspace.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fragment",
    [
        {"workspace": {"backend": "vm"}},
        {"workspace": {"backend": "sandbox"}},
        {"officer": {"enabled": True}},
        {"officer": {"enabled": False}},
    ],
)
async def test_protected_config_cannot_mutate_runtime_class(fragment):
    from orchestrator import main as orch_main

    db = MagicMock()
    db.merge_thread_config_override = AsyncMock()
    audit = AsyncMock()
    grants = AsyncMock()
    thread = {
        "id": THREAD_ID,
        "user_id": None,
        "project_id": None,
        "execution_lane": "pinned",
        "metadata": {
            "protected_cloud": True,
            "config_override": {"workspace": {"backend": "sandbox"}},
        },
    }

    db.thread_configuration_transaction.return_value.__aenter__.return_value.fetchrow = AsyncMock(
        return_value=thread
    )

    with (
        patch.object(orch_main, "postgres_db", db),
        patch.object(orch_main, "log_security_event", audit),
        patch.object(orch_main, "_enforce_session_create_grants", grants),
    ):
        with pytest.raises(HTTPException) as exc:
            await control_seams.apply_thread_config_update(
                THREAD_ID,
                thread,
                fragment,
                None,
                request=MagicMock(),
                actor=None,
            )

    assert exc.value.status_code == 409
    assert exc.value.detail["code"] == "protected_cloud_runtime_class_fixed"
    grants.assert_not_awaited()
    db.merge_thread_config_override.assert_not_awaited()
    audit.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("metadata", ["bad-json", [], {"protected_cloud": 1}])
async def test_malformed_protected_authority_blocks_config_before_persist(metadata):
    from orchestrator import main as orch_main

    db = MagicMock()
    db.merge_thread_config_override = AsyncMock()
    audit = AsyncMock()
    thread = {
        "id": THREAD_ID,
        "user_id": None,
        "execution_lane": "pinned",
        "metadata": metadata,
    }

    db.thread_configuration_transaction.return_value.__aenter__.return_value.fetchrow = AsyncMock(
        return_value=thread
    )

    with (
        patch.object(orch_main, "postgres_db", db),
        patch.object(orch_main, "log_security_event", audit),
    ):
        with pytest.raises(HTTPException) as exc:
            await control_seams.apply_thread_config_update(
                THREAD_ID,
                thread,
                {"llm": {"temperature": 0.1}},
                None,
                request=MagicMock(),
                actor=None,
            )

    assert exc.value.status_code == 409
    assert exc.value.detail["code"] == "protected_cloud_malformed"
    db.merge_thread_config_override.assert_not_awaited()
    audit.assert_not_awaited()
