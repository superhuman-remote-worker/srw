from __future__ import annotations

from copy import deepcopy

import pytest

import orchestrator.main
from orchestrator.services.cloud_stage_authority import (
    _retirement_stage_event_from_receipt,
)
from orchestrator.services.cloud.protected_reader_authority import (
    ProtectedNextcloudReaderGrantPlan,
)
from orchestrator.services.cloud_staging.source_identity import (
    ProtectedMountSourceIdentity,
)
from orchestrator.application import controls as controls_composition


_THREAD = "11111111-1111-4111-8111-111111111111"
_USER = "22222222-2222-4222-8222-222222222222"
_GENERATION = "33333333-3333-4333-8333-333333333333"
_RETIREMENT = "44444444-4444-4444-8444-444444444444"
_MOUNT = "55555555-5555-4555-8555-555555555555"
_SELECTED_MOUNT = "66666666-6666-4666-8666-666666666666"
_ATTEMPT = "77777777-7777-4777-8777-777777777777"
_BACKEND = "88888888-8888-4888-8888-888888888888"
_SOURCE_REF = "99999999-9999-4999-8999-999999999999"
_WORKSPACE_GENERATION = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
_WORKSPACE_RUNTIME = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
_TAR_SHA256 = "c" * 64


def _receipt_fixture():
    source = ProtectedMountSourceIdentity(
        backend_instance_id=_BACKEND,
        source_ref=_SOURCE_REF,
        target_path="projects/alpha",
        native_id="17",
        mountpoint="Alpha",
    )
    plan = ProtectedNextcloudReaderGrantPlan(
        engage_attempt=_ATTEMPT,
        backend_instance_id=_BACKEND,
        source=source,
    )
    prefix = (
        f"cloud-staging/{_THREAD}/{_GENERATION}/{_WORKSPACE_GENERATION}/"
        f"6/{source.sha256}/{_TAR_SHA256}/"
    )
    summary = {
        "counts": {"added": 1, "modified": 0, "deleted": 0},
        "signature": "exact-signature",
        "tar_sha256": _TAR_SHA256,
        "source_binding": source.binding,
        "source_binding_sha256": source.sha256,
        "tar_key": f"{prefix}upper.tar",
        "manifest_key": f"{prefix}manifest.json",
    }
    captured_ro = {
        "id": _MOUNT,
        "thread_id": _THREAD,
        "user_id": _USER,
        "backend": "nextcloud",
        "backend_instance_id": _BACKEND,
        "reader_id": plan.reader_id,
        "grant_group_id": plan.group_id,
        "grant_handle": plan.grant_handle,
        "grant_handle_sha256": plan.grant_handle_sha256,
        "source_binding": source.binding,
        "source_binding_sha256": source.sha256,
        "selected_mount_id": _SELECTED_MOUNT,
        "status": "active",
        "runtime_generation": _GENERATION,
        "engage_attempt": _ATTEMPT,
        "staged_epoch": 5,
        "staged_summary": None,
        "etag_baseline": {},
    }
    current_ro = {**captured_ro, "staged_epoch": 6, "staged_summary": summary}
    receipt = {
        "version": 1,
        "kind": "uploaded",
        "runtime_generation": _GENERATION,
        "retirement_token": _RETIREMENT,
        "mount_id": _MOUNT,
        "engage_attempt": _ATTEMPT,
        "source_binding_sha256": source.sha256,
        "workspace_generation": _WORKSPACE_GENERATION,
        "workspace_runtime_incarnation": _WORKSPACE_RUNTIME,
        "expected_staged_epoch": 5,
        "staged_epoch": 6,
        "staged_summary": summary,
    }
    retirement = {
        "generation": _GENERATION,
        "token": _RETIREMENT,
        "context": {
            "thread_id": _THREAD,
            "workspace_container": {"_runtime_incarnation": _WORKSPACE_RUNTIME},
            "workspace_binding": {"generation": _WORKSPACE_GENERATION},
            "protected_ro": captured_ro,
        },
    }
    thread = {"runtime_retirement_stage_receipt": receipt}
    return retirement, thread, current_ro


def test_retirement_stage_receipt_accepts_exact_immutable_source():
    retirement, thread, row = _receipt_fixture()

    valid, event = _retirement_stage_event_from_receipt(
        retirement,
        thread,
        row,
        never_delivered_protected_reader_shape=(
            controls_composition.pinned_retirement_operations(
                orchestrator.main.app.state.resources
            ).never_delivered_protected_reader_shape
        ),
    )

    assert valid is True
    assert event == {
        "thread_id": _THREAD,
        "session_runtime_generation": _GENERATION,
        "staged_epoch": 6,
        "file_count": 1,
        "counts": {"added": 1, "modified": 0, "deleted": 0},
        "mount_id": _MOUNT,
    }


@pytest.mark.parametrize(
    "mutate",
    [
        lambda _retirement, thread, _row: thread[
            "runtime_retirement_stage_receipt"
        ].__setitem__("source_binding_sha256", "d" * 64),
        lambda _retirement, thread, _row: thread["runtime_retirement_stage_receipt"][
            "staged_summary"
        ].__setitem__("source_binding_sha256", "d" * 64),
        lambda _retirement, _thread, row: row.__setitem__(
            "source_binding_sha256", "d" * 64
        ),
    ],
)
def test_retirement_stage_receipt_rejects_source_identity_drift(mutate):
    retirement, thread, row = deepcopy(_receipt_fixture())
    mutate(retirement, thread, row)

    assert _retirement_stage_event_from_receipt(
        retirement,
        thread,
        row,
        never_delivered_protected_reader_shape=(
            controls_composition.pinned_retirement_operations(
                orchestrator.main.app.state.resources
            ).never_delivered_protected_reader_shape
        ),
    ) == (
        False,
        None,
    )
