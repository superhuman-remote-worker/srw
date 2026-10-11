"""R1.B04 lane C — agent cloud-stage authority, receipts and the internal routes.

Characterization first: before this extraction the only coverage of these nine
symbols was ``tests/cloud_staging/test_stage_triggers.py`` (the endpoint's
internal gate, flag-off, scheduling and de-dupe) and
``tests/cloud_staging/test_retirement_stage_receipt.py`` (one accept case plus
three source-drift rejections). Everything else pinned here — the whole-tuple
authority capture, the receipt accept/reject matrix including the
``never_engaged`` branch and the append-once epoch rule, the task key, the
retirement-pending broadcast suppression, and the lifecycle/retirement-outcome
refusal ordering — was untested and is written down *before* it is relied on.

The three routes are exercised both through a mounted application (path,
method, per-invocation dependency resolution) and by direct call (refusal
ordering, exact ``detail`` payloads).
"""

from __future__ import annotations

import asyncio
from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from orchestrator.routers.agent_cloud_stage import (
    AgentCloudStageDependencies,
    agent_get_thread_lifecycle,
    agent_get_thread_retirement_outcome,
    agent_trigger_cloud_stage,
    router as agent_cloud_stage_router,
)
from orchestrator.services import cloud_stage_authority
from orchestrator.services.cloud.protected_reader_authority import (
    ProtectedNextcloudReaderGrantPlan,
)
from orchestrator.services.cloud_staging.source_identity import (
    ProtectedMountSourceIdentity,
)
from tests._mounted_router import mount_router
from tests._route_inventory import mounted_route_objects


THREAD = "11111111-1111-4111-8111-111111111111"
USER = "22222222-2222-4222-8222-222222222222"
GENERATION = "33333333-3333-4333-8333-333333333333"
RETIREMENT = "44444444-4444-4444-8444-444444444444"
MOUNT = "55555555-5555-4555-8555-555555555555"
SELECTED_MOUNT = "66666666-6666-4666-8666-666666666666"
ATTEMPT = "77777777-7777-4777-8777-777777777777"
BACKEND = "88888888-8888-4888-8888-888888888888"
SOURCE_REF = "99999999-9999-4999-8999-999999999999"
WORKSPACE_GENERATION = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
WORKSPACE_RUNTIME = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
AGENT = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
ATTACH = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
TAR_SHA256 = "e" * 64
FINGERPRINT = "SHA256:" + "A" * 43


def _source() -> ProtectedMountSourceIdentity:
    return ProtectedMountSourceIdentity(
        backend_instance_id=BACKEND,
        source_ref=SOURCE_REF,
        target_path="projects/alpha",
        native_id="17",
        mountpoint="Alpha",
    )


# =============================================================================
# _capture_cloud_stage_authority — the whole-tuple check
# =============================================================================


def _authority_thread(**over):
    thread = {
        "execution_lane": "pinned",
        "runtime_generation": GENERATION,
        "agent_id": AGENT,
        "runtime_attach_token": ATTACH,
        "runtime_retirement_token": None,
        "runtime_retirement_authorized_at": None,
        "runtime_authority_exposed": True,
        "metadata": {
            "workspace_container": {
                "status": "ready",
                "_runtime_incarnation": WORKSPACE_RUNTIME,
                "_canvas_workspace_generation": WORKSPACE_GENERATION,
            },
            "_workspace_binding": {
                "kind": "remote",
                "generation": WORKSPACE_GENERATION,
                "ssh_host_key_fingerprint": FINGERPRINT,
            },
        },
    }
    thread.update(over)
    return thread


def _authority_row(**over):
    source = _source()
    row = {
        "id": MOUNT,
        "status": "active",
        "runtime_generation": GENERATION,
        "engage_attempt": ATTEMPT,
        "staged_epoch": 5,
        "source_binding": source.binding,
        "source_binding_sha256": source.sha256,
    }
    row.update(over)
    return row


class TestCaptureCloudStageAuthority:
    def test_captures_the_complete_frozen_tuple(self):
        captured = cloud_stage_authority._capture_cloud_stage_authority(
            _authority_thread(), _authority_row()
        )

        assert captured == {
            "runtime_generation": GENERATION,
            "runtime_retirement_token": None,
            "agent_id": AGENT,
            "runtime_attach_token": ATTACH,
            "workspace": {
                "status": "ready",
                "_runtime_incarnation": WORKSPACE_RUNTIME,
                "_canvas_workspace_generation": WORKSPACE_GENERATION,
            },
            "workspace_binding": {
                "kind": "remote",
                "generation": WORKSPACE_GENERATION,
                "ssh_host_key_fingerprint": FINGERPRINT,
            },
            "workspace_generation": WORKSPACE_GENERATION,
            "workspace_runtime_incarnation": WORKSPACE_RUNTIME,
            "workspace_ssh_host_key_fingerprint": FINGERPRINT,
            "mount_row_id": MOUNT,
            "engage_attempt": ATTEMPT,
            "source_binding_sha256": _source().sha256,
            "expected_staged_epoch": 5,
        }

    def test_orchestrator_owned_retirement_needs_no_agent_or_attach_token(self):
        thread = _authority_thread(
            agent_id=None,
            runtime_attach_token=None,
            runtime_retirement_token=RETIREMENT,
            runtime_retirement_authorized_at="2026-01-01T00:00:00Z",
            runtime_authority_exposed=False,
        )

        captured = cloud_stage_authority._capture_cloud_stage_authority(
            thread, _authority_row()
        )

        assert captured is not None
        assert captured["agent_id"] is None
        assert captured["runtime_attach_token"] is None
        assert captured["runtime_retirement_token"] == RETIREMENT

    @pytest.mark.parametrize(
        ("label", "thread_over", "row_over"),
        [
            ("stateless lane", {"execution_lane": "stateless"}, {}),
            ("no lane", {"execution_lane": None}, {}),
            ("non-uuid generation", {"runtime_generation": "not-a-uuid"}, {}),
            ("non-uuid attach token", {"runtime_attach_token": "nope"}, {}),
            ("no agent and no orchestrator retirement", {"agent_id": None}, {}),
            ("row not active", {}, {"status": "revoked"}),
            ("row on another generation", {}, {"runtime_generation": RETIREMENT}),
            ("non-uuid engage attempt", {}, {"engage_attempt": "attempt"}),
            ("unbindable source", {}, {"source_binding": {"version": 99}}),
            ("source sha mismatch", {}, {"source_binding_sha256": "f" * 64}),
        ],
    )
    def test_refuses_every_partial_authority(self, label, thread_over, row_over):
        assert (
            cloud_stage_authority._capture_cloud_stage_authority(
                _authority_thread(**thread_over), _authority_row(**row_over)
            )
            is None
        ), label

    @pytest.mark.parametrize(
        "metadata_over",
        [
            {"workspace_container": {"status": "provisioning"}},
            {"_workspace_binding": {"kind": "local"}},
            {"workspace_container": "not-a-dict"},
            {"_workspace_binding": "not-a-dict"},
        ],
    )
    def test_refuses_a_workspace_that_is_not_a_ready_remote_binding(
        self, metadata_over
    ):
        thread = _authority_thread()
        thread["metadata"] = {**thread["metadata"], **metadata_over}

        assert (
            cloud_stage_authority._capture_cloud_stage_authority(
                thread, _authority_row()
            )
            is None
        )

    def test_refuses_a_fingerprint_that_is_not_sha256(self):
        thread = _authority_thread()
        thread["metadata"]["_workspace_binding"]["ssh_host_key_fingerprint"] = (
            "MD5:aa:bb"
        )

        assert (
            cloud_stage_authority._capture_cloud_stage_authority(
                thread, _authority_row()
            )
            is None
        )

    def test_refuses_a_canvas_generation_that_drifted_from_the_binding(self):
        thread = _authority_thread()
        thread["metadata"]["workspace_container"]["_canvas_workspace_generation"] = (
            GENERATION
        )

        assert (
            cloud_stage_authority._capture_cloud_stage_authority(
                thread, _authority_row()
            )
            is None
        )


# =============================================================================
# _retirement_stage_event_from_receipt — append-once terminal receipt
# =============================================================================


def _uploaded_fixture():
    """The shipped happy-path fixture, mirrored from
    ``tests/cloud_staging/test_retirement_stage_receipt.py`` so this file can
    exercise the surrounding matrix without importing that module."""
    source = _source()
    plan = ProtectedNextcloudReaderGrantPlan(
        engage_attempt=ATTEMPT,
        backend_instance_id=BACKEND,
        source=source,
    )
    prefix = (
        f"cloud-staging/{THREAD}/{GENERATION}/{WORKSPACE_GENERATION}/"
        f"6/{source.sha256}/{TAR_SHA256}/"
    )
    summary = {
        "counts": {"added": 1, "modified": 0, "deleted": 0},
        "signature": "exact-signature",
        "tar_sha256": TAR_SHA256,
        "source_binding": source.binding,
        "source_binding_sha256": source.sha256,
        "tar_key": f"{prefix}upper.tar",
        "manifest_key": f"{prefix}manifest.json",
    }
    captured_ro = {
        "id": MOUNT,
        "thread_id": THREAD,
        "user_id": USER,
        "backend": "nextcloud",
        "backend_instance_id": BACKEND,
        "reader_id": plan.reader_id,
        "grant_group_id": plan.group_id,
        "grant_handle": plan.grant_handle,
        "grant_handle_sha256": plan.grant_handle_sha256,
        "source_binding": source.binding,
        "source_binding_sha256": source.sha256,
        "selected_mount_id": SELECTED_MOUNT,
        "status": "active",
        "runtime_generation": GENERATION,
        "engage_attempt": ATTEMPT,
        "staged_epoch": 5,
        "staged_summary": None,
        "etag_baseline": {},
    }
    current_ro = {**captured_ro, "staged_epoch": 6, "staged_summary": summary}
    receipt = {
        "version": 1,
        "kind": "uploaded",
        "runtime_generation": GENERATION,
        "retirement_token": RETIREMENT,
        "mount_id": MOUNT,
        "engage_attempt": ATTEMPT,
        "source_binding_sha256": source.sha256,
        "workspace_generation": WORKSPACE_GENERATION,
        "workspace_runtime_incarnation": WORKSPACE_RUNTIME,
        "expected_staged_epoch": 5,
        "staged_epoch": 6,
        "staged_summary": summary,
    }
    retirement = {
        "generation": GENERATION,
        "token": RETIREMENT,
        "context": {
            "thread_id": THREAD,
            "workspace_container": {"_runtime_incarnation": WORKSPACE_RUNTIME},
            "workspace_binding": {"generation": WORKSPACE_GENERATION},
            "protected_ro": captured_ro,
        },
    }
    thread = {"runtime_retirement_stage_receipt": receipt}
    return retirement, thread, current_ro


def _never_shape_true(*_args, **_kwargs):
    return True


def _never_shape_false(*_args, **_kwargs):
    return False


def _validate(retirement, thread, row, *, never_shape=_never_shape_false):
    return cloud_stage_authority._retirement_stage_event_from_receipt(
        retirement,
        thread,
        row,
        never_delivered_protected_reader_shape=never_shape,
    )


class TestRetirementStageEventFromReceipt:
    def test_accepts_the_exact_uploaded_receipt(self):
        retirement, thread, row = _uploaded_fixture()

        assert _validate(retirement, thread, row) == (
            True,
            {
                "thread_id": THREAD,
                "session_runtime_generation": GENERATION,
                "staged_epoch": 6,
                "file_count": 1,
                "counts": {"added": 1, "modified": 0, "deleted": 0},
                "mount_id": MOUNT,
            },
        )

    def test_a_json_string_receipt_is_parsed(self):
        import json

        retirement, thread, row = _uploaded_fixture()
        thread["runtime_retirement_stage_receipt"] = json.dumps(
            thread["runtime_retirement_stage_receipt"]
        )

        valid, event = _validate(retirement, thread, row)

        assert valid is True
        assert event["staged_epoch"] == 6

    @pytest.mark.parametrize(
        "receipt",
        ["not json at all", "[1, 2, 3]", '"a bare string"'],
    )
    def test_a_malformed_receipt_is_never_a_successful_stage(self, receipt):
        retirement, thread, row = _uploaded_fixture()
        thread["runtime_retirement_stage_receipt"] = receipt

        assert _validate(retirement, thread, row) == (False, None)

    def test_absent_receipt_is_not_a_stage(self):
        retirement, thread, row = _uploaded_fixture()
        thread["runtime_retirement_stage_receipt"] = None

        assert _validate(retirement, thread, row) == (False, None)

    def test_a_non_never_engaged_receipt_without_a_row_is_refused(self):
        retirement, thread, _row = _uploaded_fixture()

        assert _validate(retirement, thread, None) == (False, None)

    @pytest.mark.parametrize(
        "mutate",
        [
            pytest.param(
                lambda r, t, w: t["runtime_retirement_stage_receipt"].__setitem__(
                    "runtime_generation", WORKSPACE_GENERATION
                ),
                id="receipt from another runtime generation",
            ),
            pytest.param(
                lambda r, t, w: r.__setitem__("generation", WORKSPACE_GENERATION),
                id="retirement moved to another generation",
            ),
            pytest.param(
                lambda r, t, w: t["runtime_retirement_stage_receipt"].__setitem__(
                    "retirement_token", MOUNT
                ),
                id="another retirement token",
            ),
            pytest.param(
                lambda r, t, w: t["runtime_retirement_stage_receipt"].__setitem__(
                    "workspace_generation", GENERATION
                ),
                id="another workspace generation",
            ),
            pytest.param(
                lambda r, t, w: t["runtime_retirement_stage_receipt"].__setitem__(
                    "workspace_runtime_incarnation", GENERATION
                ),
                id="another workspace runtime incarnation",
            ),
            pytest.param(
                lambda r, t, w: t["runtime_retirement_stage_receipt"].__setitem__(
                    "version", 2
                ),
                id="unsupported receipt version",
            ),
            pytest.param(
                lambda r, t, w: t["runtime_retirement_stage_receipt"].__setitem__(
                    "mount_id", SELECTED_MOUNT
                ),
                id="another mount",
            ),
            pytest.param(
                lambda r, t, w: t["runtime_retirement_stage_receipt"].__setitem__(
                    "engage_attempt", GENERATION
                ),
                id="another engage attempt",
            ),
            pytest.param(
                lambda r, t, w: w.__setitem__("staged_epoch", 7),
                id="row epoch ahead of the receipt",
            ),
            pytest.param(
                lambda r, t, w: t["runtime_retirement_stage_receipt"].__setitem__(
                    "staged_epoch", 7
                ),
                id="receipt epoch is not expected + 1",
            ),
            pytest.param(
                lambda r, t, w: t["runtime_retirement_stage_receipt"].__setitem__(
                    "expected_staged_epoch", 4
                ),
                id="expected epoch disagrees with the captured row",
            ),
            pytest.param(
                lambda r, t, w: w.__setitem__("id", SELECTED_MOUNT),
                id="current row is a different mount",
            ),
            pytest.param(
                lambda r, t, w: w.__setitem__("runtime_generation", RETIREMENT),
                id="current row moved generation",
            ),
            pytest.param(
                lambda r, t, w: w.__setitem__("staged_summary", None),
                id="current row lost the summary the receipt claims",
            ),
            pytest.param(
                lambda r, t, w: t["runtime_retirement_stage_receipt"][
                    "staged_summary"
                ].__setitem__("tar_key", "cloud-staging/elsewhere/upper.tar"),
                id="tar key outside the derived prefix",
            ),
            pytest.param(
                lambda r, t, w: t["runtime_retirement_stage_receipt"][
                    "staged_summary"
                ].__setitem__("tar_sha256", "f" * 63),
                id="tar sha is not 64 hex chars",
            ),
            pytest.param(
                lambda r, t, w: t["runtime_retirement_stage_receipt"].__setitem__(
                    "kind", "teleported"
                ),
                id="unknown receipt kind",
            ),
        ],
    )
    def test_rejects_cross_generation_and_malformed_shapes(self, mutate):
        retirement, thread, row = deepcopy(_uploaded_fixture())
        mutate(retirement, thread, row)

        assert _validate(retirement, thread, row) == (False, None)

    def test_summary_drift_between_receipt_and_row_is_refused(self):
        retirement, thread, row = deepcopy(_uploaded_fixture())
        row["staged_summary"] = {
            **row["staged_summary"],
            "signature": "a different signature",
        }

        assert _validate(retirement, thread, row) == (False, None)

    def test_a_row_summary_that_is_unparseable_json_is_refused(self):
        retirement, thread, row = deepcopy(_uploaded_fixture())
        row["staged_summary"] = "{not json"

        assert _validate(retirement, thread, row) == (False, None)

    def test_empty_kind_publishes_a_zero_count_event(self):
        retirement, thread, row = deepcopy(_uploaded_fixture())
        receipt = thread["runtime_retirement_stage_receipt"]
        receipt["kind"] = "empty"
        receipt["staged_summary"] = None
        row["staged_summary"] = None

        assert _validate(retirement, thread, row) == (
            True,
            {
                "thread_id": THREAD,
                "session_runtime_generation": GENERATION,
                "staged_epoch": 6,
                "file_count": 0,
                "counts": {"added": 0, "modified": 0, "deleted": 0},
                "mount_id": MOUNT,
            },
        )

    def test_empty_kind_with_a_summary_is_refused(self):
        retirement, thread, row = deepcopy(_uploaded_fixture())
        thread["runtime_retirement_stage_receipt"]["kind"] = "empty"

        assert _validate(retirement, thread, row) == (False, None)

    def test_unchanged_kind_keeps_the_captured_epoch(self):
        retirement, thread, row = deepcopy(_uploaded_fixture())
        receipt = thread["runtime_retirement_stage_receipt"]
        summary = receipt["staged_summary"]
        receipt["kind"] = "unchanged"
        receipt["staged_epoch"] = 5
        row["staged_epoch"] = 5
        retirement["context"]["protected_ro"]["staged_epoch"] = 5
        retirement["context"]["protected_ro"]["staged_summary"] = summary

        valid, event = _validate(retirement, thread, row)

        assert valid is True
        assert event["staged_epoch"] == 5

    def test_unchanged_kind_refuses_when_the_captured_summary_differs(self):
        retirement, thread, row = deepcopy(_uploaded_fixture())
        receipt = thread["runtime_retirement_stage_receipt"]
        receipt["kind"] = "unchanged"
        receipt["staged_epoch"] = 5
        row["staged_epoch"] = 5
        retirement["context"]["protected_ro"]["staged_epoch"] = 5
        retirement["context"]["protected_ro"]["staged_summary"] = {"counts": {}}

        assert _validate(retirement, thread, row) == (False, None)


class TestNeverEngagedReceipt:
    def _fixture(self):
        retirement, thread, row = deepcopy(_uploaded_fixture())
        captured_ro = retirement["context"]["protected_ro"]
        source = _source()
        thread["runtime_retirement_stage_receipt"] = {
            "version": 1,
            "kind": "never_engaged",
            "runtime_generation": GENERATION,
            "retirement_token": RETIREMENT,
            "mount_id": MOUNT,
            "engage_attempt": ATTEMPT,
            "source_binding_sha256": source.sha256,
            "workspace_generation": None,
            "workspace_runtime_incarnation": None,
            "expected_staged_epoch": 0,
            "staged_epoch": 0,
            "staged_summary": None,
        }
        assert captured_ro["id"] == MOUNT
        return retirement, thread, row

    def test_accepts_a_proven_never_delivered_reader(self):
        retirement, thread, row = self._fixture()

        assert _validate(retirement, thread, row, never_shape=_never_shape_true) == (
            True,
            {
                "thread_id": THREAD,
                "session_runtime_generation": GENERATION,
                "staged_epoch": 0,
                "file_count": 0,
                "counts": {"added": 0, "modified": 0, "deleted": 0},
                "mount_id": MOUNT,
            },
        )

    def test_refuses_when_the_live_reader_shape_cannot_prove_it(self):
        retirement, thread, row = self._fixture()

        assert _validate(retirement, thread, row, never_shape=_never_shape_false) == (
            False,
            None,
        )

    def test_the_shape_authority_is_asked_to_require_a_revoked_current_row(self):
        retirement, thread, row = self._fixture()
        seen = {}

        def never_shape(*args, **kwargs):
            seen["args"] = args
            seen["kwargs"] = kwargs
            return True

        _validate(retirement, thread, row, never_shape=never_shape)

        assert seen["kwargs"] == {"require_current_revoked": True}
        assert seen["args"] == (retirement, thread, row)

    def test_a_never_engaged_receipt_needs_no_row(self):
        retirement, thread, _row = self._fixture()

        valid, event = _validate(
            retirement, thread, None, never_shape=_never_shape_true
        )

        assert valid is True
        assert event["mount_id"] == MOUNT

    @pytest.mark.parametrize(
        "mutate",
        [
            pytest.param(
                lambda r, t: t["runtime_retirement_stage_receipt"].__setitem__(
                    "staged_epoch", 1
                ),
                id="a never-engaged receipt cannot carry an epoch",
            ),
            pytest.param(
                lambda r, t: t["runtime_retirement_stage_receipt"].__setitem__(
                    "workspace_generation", WORKSPACE_GENERATION
                ),
                id="a never-engaged receipt names no workspace",
            ),
            pytest.param(
                lambda r, t: t["runtime_retirement_stage_receipt"].__setitem__(
                    "runtime_generation", WORKSPACE_GENERATION
                ),
                id="cross-generation never-engaged receipt",
            ),
            pytest.param(
                lambda r, t: t["runtime_retirement_stage_receipt"].__setitem__(
                    "mount_id", SELECTED_MOUNT
                ),
                id="another mount",
            ),
            pytest.param(
                lambda r, t: t["runtime_retirement_stage_receipt"].__setitem__(
                    "staged_summary", {"counts": {}}
                ),
                id="a never-engaged receipt carries no summary",
            ),
        ],
    )
    def test_refuses_malformed_never_engaged_shapes(self, mutate):
        retirement, thread, row = self._fixture()
        mutate(retirement, thread)

        assert _validate(retirement, thread, row, never_shape=_never_shape_true) == (
            False,
            None,
        )


EARLIER_GENERATION = "ffffffff-ffff-4fff-8fff-ffffffffffff"


class TestReaderlessLife:
    """A resumed life whose re-engage was refused minted no reader.

    It never had the folder mounted, so its End receipts zero even after its
    agent ran (k3d access gate, 2026-10-11). The receipt names the earlier
    row Begin captured, whose own review stays untouched.
    """

    def _fixture(self):
        retirement, thread, _row = TestNeverEngagedReceipt()._fixture()
        earlier_review = {"counts": {"added": 1, "modified": 0, "deleted": 0}}
        earlier = {
            **retirement["context"]["protected_ro"],
            "status": "revoked",
            "runtime_generation": EARLIER_GENERATION,
            "staged_epoch": 3,
            "staged_summary": earlier_review,
        }
        retirement["context"]["protected_ro"] = earlier
        retirement["context"]["protected_cloud"] = True
        thread["runtime_generation"] = GENERATION
        return retirement, thread, deepcopy(earlier)

    def test_an_earlier_revoked_reader_receipts_zero_for_this_life(self):
        retirement, thread, row = self._fixture()

        assert _validate(retirement, thread, row) == (
            True,
            {
                "thread_id": THREAD,
                "session_runtime_generation": GENERATION,
                "staged_epoch": 0,
                "file_count": 0,
                "counts": {"added": 0, "modified": 0, "deleted": 0},
                "mount_id": MOUNT,
            },
        )

    def test_a_life_with_no_reader_row_receipts_zero(self):
        retirement, thread, _row = self._fixture()
        retirement["context"]["protected_ro"] = None
        thread["runtime_retirement_stage_receipt"].update(
            mount_id=None, engage_attempt=None, source_binding_sha256=None
        )

        valid, event = _validate(retirement, thread, None)

        assert valid is True
        assert event["mount_id"] is None

    @pytest.mark.parametrize(
        "mutate",
        [
            pytest.param(
                lambda c, r: (
                    c.__setitem__("runtime_generation", GENERATION),
                    r.__setitem__("runtime_generation", GENERATION),
                ),
                id="a reader of this life",
            ),
            pytest.param(
                lambda c, r: c.__setitem__("status", "active"),
                id="captured live",
            ),
            pytest.param(
                lambda c, r: r.__setitem__("status", "engaging"),
                id="re-engaged since",
            ),
            pytest.param(
                lambda c, r: r.__setitem__("runtime_generation", GENERATION),
                id="the row moved to this life",
            ),
            pytest.param(lambda c, r: r.__setitem__("id", SELECTED_MOUNT), id="row"),
            pytest.param(
                lambda c, r: r.__setitem__("engage_attempt", SELECTED_MOUNT),
                id="attempt",
            ),
            pytest.param(
                lambda c, r: c.__setitem__("thread_id", SELECTED_MOUNT),
                id="another thread",
            ),
            pytest.param(
                lambda c, r: r.__setitem__("reader_id", "someone-else"),
                id="another grant",
            ),
        ],
    )
    def test_refuses_anything_but_the_exact_earlier_row(self, mutate):
        retirement, thread, row = self._fixture()
        mutate(retirement["context"]["protected_ro"], row)

        assert _validate(retirement, thread, row) == (False, None)

    @pytest.mark.parametrize(
        "mutate",
        [
            pytest.param(
                lambda ret, t: ret["context"].__setitem__("protected_cloud", False),
                id="not protected",
            ),
            pytest.param(
                lambda ret, t: t.__setitem__("runtime_generation", EARLIER_GENERATION),
                id="the thread moved",
            ),
        ],
    )
    def test_refuses_outside_this_protected_life(self, mutate):
        retirement, thread, row = self._fixture()
        mutate(retirement, thread)

        assert _validate(retirement, thread, row) == (False, None)

    @pytest.mark.parametrize(
        ("captured", "current"),
        [
            pytest.param("row", None, id="the captured row is gone"),
            pytest.param(None, "row", id="a row appeared"),
        ],
    )
    def test_a_missing_side_is_not_readerless(self, captured, current):
        retirement, thread, row = self._fixture()
        retirement["context"]["protected_ro"] = (
            retirement["context"]["protected_ro"] if captured else None
        )

        assert _validate(retirement, thread, row if current else None) == (
            False,
            None,
        )


# =============================================================================
# _cloud_stage_task_key / _thread_selected_vm_workspace / broadcast
# =============================================================================


class TestStageTaskKey:
    def test_vm_tier_has_no_authority_and_keys_on_the_thread(self):
        assert cloud_stage_authority._cloud_stage_task_key(THREAD, None) == (
            f"{THREAD}:vm"
        )

    def test_pinned_key_spans_both_generations_and_the_expected_epoch(self):
        key = cloud_stage_authority._cloud_stage_task_key(
            THREAD,
            {
                "runtime_generation": GENERATION,
                "workspace_generation": WORKSPACE_GENERATION,
                "expected_staged_epoch": 3,
            },
        )

        assert key == f"{THREAD}:{GENERATION}:{WORKSPACE_GENERATION}:3"

    def test_a_new_workspace_generation_is_a_different_key(self):
        first = cloud_stage_authority._cloud_stage_task_key(
            THREAD,
            {
                "runtime_generation": GENERATION,
                "workspace_generation": WORKSPACE_GENERATION,
                "expected_staged_epoch": 3,
            },
        )
        second = cloud_stage_authority._cloud_stage_task_key(
            THREAD,
            {
                "runtime_generation": GENERATION,
                "workspace_generation": GENERATION,
                "expected_staged_epoch": 3,
            },
        )

        assert first != second


def _vm_thread(**vm_over):
    """A thread whose stamped tier is VM and whose runtime is attested."""
    vm = {
        "status": "ready",
        "provision_generation": GENERATION,
        "ssh_host": "100.64.0.5",
        "ssh_ready_source": "provisioner_probe",
    }
    vm.update(vm_over)
    return {
        "metadata": {
            "config_override": {"workspace": {"backend": "vm"}},
            "vm": vm,
        }
    }


class TestThreadSelectedVmWorkspace:
    def test_reads_the_injected_provisioner_mode(self):
        assert (
            cloud_stage_authority._thread_selected_vm_workspace(
                _vm_thread(), vm_provisioner=SimpleNamespace(mode="same-cluster")
            )
            is True
        )

    def test_same_cluster_needs_the_provisioner_probe_that_external_does_not(self):
        thread = _vm_thread(ssh_ready_source=None)

        assert (
            cloud_stage_authority._thread_selected_vm_workspace(
                thread, vm_provisioner=SimpleNamespace(mode="same-cluster")
            )
            is False
        )
        assert (
            cloud_stage_authority._thread_selected_vm_workspace(
                thread, vm_provisioner=SimpleNamespace(mode="external")
            )
            is True
        )

    def test_a_sandbox_thread_is_not_a_vm_workspace(self):
        thread = {
            "metadata": {"config_override": {"workspace": {"backend": "sandbox"}}},
        }

        assert (
            cloud_stage_authority._thread_selected_vm_workspace(
                thread, vm_provisioner=SimpleNamespace(mode="same-cluster")
            )
            is False
        )


class TestBroadcastCloudStageResult:
    def test_a_retirement_pending_publication_is_not_advertised_early(
        self, monkeypatch
    ):
        feed = MagicMock()
        monkeypatch.setattr(
            "orchestrator.services.notification_feed.notification_feed", feed
        )

        cloud_stage_authority._broadcast_cloud_stage_result(
            {
                "event": {"thread_id": THREAD},
                "publication": {
                    "user_id": USER,
                    "runtime_retirement_pending": True,
                },
            }
        )

        feed.broadcast.assert_not_called()

    def test_a_settled_publication_reaches_the_owner_feed(self, monkeypatch):
        feed = MagicMock()
        monkeypatch.setattr(
            "orchestrator.services.notification_feed.notification_feed", feed
        )

        cloud_stage_authority._broadcast_cloud_stage_result(
            {"event": {"thread_id": THREAD}, "publication": {"user_id": USER}}
        )

        feed.broadcast.assert_called_once_with(
            USER, "cloud.diff_staged", {"thread_id": THREAD}
        )

    @pytest.mark.parametrize(
        "result",
        [None, {}, {"event": None}, {"event": "not a dict"}, {"event": {}}],
    )
    def test_nothing_is_broadcast_without_a_dict_event(self, result, monkeypatch):
        feed = MagicMock()
        monkeypatch.setattr(
            "orchestrator.services.notification_feed.notification_feed", feed
        )

        cloud_stage_authority._broadcast_cloud_stage_result(result)

        feed.broadcast.assert_not_called()

    def test_a_publication_without_a_user_is_dropped(self, monkeypatch):
        feed = MagicMock()
        monkeypatch.setattr(
            "orchestrator.services.notification_feed.notification_feed", feed
        )

        cloud_stage_authority._broadcast_cloud_stage_result(
            {"event": {"thread_id": THREAD}, "publication": {}}
        )

        feed.broadcast.assert_not_called()


# =============================================================================
# The routes
# =============================================================================


class FakeStageRegistry:
    """The ``stage_*`` half of the application's ``CloudTaskRegistry`` (§3.1)."""

    def __init__(self, *, preloaded: set[str] | None = None):
        self.tasks: dict[str, asyncio.Task[None]] = {}
        self.preloaded = preloaded or set()
        self.started: list[str] = []

    def stage_has(self, key: str) -> bool:
        return key in self.preloaded or key in self.tasks

    def stage_start(self, key, factory):
        if self.stage_has(key):
            return
        self.started.append(key)
        task = asyncio.get_running_loop().create_task(factory())
        self.tasks[key] = task
        task.add_done_callback(
            lambda done: self.tasks.pop(key, None)
            if self.tasks.get(key) is done
            else None
        )


class _Lock:
    def __init__(self, calls):
        self.calls = calls

    async def __aenter__(self):
        self.calls.append("lock")
        return True

    async def __aexit__(self, *exc):
        return False


def _stage_deps(**over):
    calls: list[str] = []
    store = SimpleNamespace(
        get_thread=AsyncMock(return_value=_authority_thread()),
        get_ro_mount_by_thread=AsyncMock(return_value=_authority_row()),
        thread_advisory_lock=lambda _thread_id: _Lock(calls),
        acquire=MagicMock(),
        get_pinned_thread_retirement_outcome=AsyncMock(return_value=None),
    )
    deps = AgentCloudStageDependencies(
        store=store,
        snapshots=SimpleNamespace(name="snapshots"),
        vm_provisioner=SimpleNamespace(mode="disabled"),
        cloud_tasks=FakeStageRegistry(),
        is_protected_cloud_mode_enabled=lambda: True,
        require_pinned_workspace_credential_owner=AsyncMock(),
        require_internal=AsyncMock(),
    )
    deps = replace(deps, **over)
    return deps, calls


def _request(headers=None):
    request = MagicMock()
    request.headers = headers or {}
    return request


class TestAgentTriggerCloudStage:
    @pytest.mark.asyncio
    async def test_internal_gate_fires_before_the_flag_and_any_store_read(self):
        gate = AsyncMock(side_effect=HTTPException(401, "Invalid internal key"))
        flag = MagicMock(return_value=True)
        deps, _ = _stage_deps(
            require_internal=gate, is_protected_cloud_mode_enabled=flag
        )

        with pytest.raises(HTTPException) as exc:
            await agent_trigger_cloud_stage(_request(), THREAD, dependencies=deps)

        assert exc.value.status_code == 401
        flag.assert_not_called()
        deps.store.get_thread.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_flag_off_skips_before_any_store_read(self):
        deps, _ = _stage_deps(is_protected_cloud_mode_enabled=lambda: False)

        result = await agent_trigger_cloud_stage(_request(), THREAD, dependencies=deps)

        assert result == {"skipped": "flag_off"}
        deps.store.get_thread.assert_not_awaited()
        assert deps.cloud_tasks.started == []

    @pytest.mark.asyncio
    async def test_missing_thread_is_a_404_before_the_owner_gate(self):
        owner = AsyncMock()
        deps, _ = _stage_deps(require_pinned_workspace_credential_owner=owner)
        deps.store.get_thread = AsyncMock(return_value=None)

        with pytest.raises(HTTPException) as exc:
            await agent_trigger_cloud_stage(_request(), THREAD, dependencies=deps)

        assert exc.value.status_code == 404
        assert exc.value.detail == "Thread not found"
        owner.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_owner_gate_receives_the_three_presented_headers(self):
        owner = AsyncMock()
        deps, _ = _stage_deps(require_pinned_workspace_credential_owner=owner)
        headers = {
            "X-Agent-ID": AGENT,
            "X-Session-Runtime-Generation": GENERATION,
            "X-Session-Runtime-Attach-Token": ATTACH,
        }

        await agent_trigger_cloud_stage(_request(headers), THREAD, dependencies=deps)

        owner.assert_awaited_once()
        assert owner.await_args.args[1:] == (AGENT, GENERATION, ATTACH)

    @pytest.mark.asyncio
    async def test_no_authority_and_no_vm_selection_is_a_409_code(self):
        deps, _ = _stage_deps(capture_cloud_stage_authority=lambda _t, _r: None)

        with pytest.raises(HTTPException) as exc:
            await agent_trigger_cloud_stage(_request(), THREAD, dependencies=deps)

        assert exc.value.status_code == 409
        assert exc.value.detail == {"code": "cloud_stage_authority_unavailable"}
        assert deps.cloud_tasks.started == []

    @pytest.mark.asyncio
    async def test_a_retiring_authority_refuses_rather_than_staging(self):
        deps, _ = _stage_deps(
            capture_cloud_stage_authority=lambda _t, _r: {
                "runtime_generation": GENERATION,
                "workspace_generation": WORKSPACE_GENERATION,
                "expected_staged_epoch": 1,
                "runtime_retirement_token": RETIREMENT,
            }
        )

        with pytest.raises(HTTPException) as exc:
            await agent_trigger_cloud_stage(_request(), THREAD, dependencies=deps)

        assert exc.value.status_code == 409
        assert exc.value.detail == {"code": "cloud_stage_authority_unavailable"}

    @pytest.mark.asyncio
    async def test_a_vm_thread_stages_without_an_authority(self):
        deps, _ = _stage_deps(
            capture_cloud_stage_authority=lambda _t, _r: None,
            vm_provisioner=SimpleNamespace(mode="same-cluster"),
        )
        deps.store.get_thread = AsyncMock(return_value=_vm_thread())
        stage = AsyncMock(return_value=None)

        with _patched_stage(stage):
            result = await agent_trigger_cloud_stage(
                _request(), THREAD, dependencies=deps
            )
            await _drain(deps)

        assert result == {"scheduled": True}
        assert deps.cloud_tasks.started == [f"{THREAD}:vm"]
        assert stage.await_args.kwargs["authority"] is None

    @pytest.mark.asyncio
    async def test_the_scheduled_task_stages_under_the_thread_advisory_lock(self):
        deps, calls = _stage_deps()
        stage = AsyncMock(return_value={"event": {"x": 1}, "publication": {}})

        with _patched_stage(stage):
            result = await agent_trigger_cloud_stage(
                _request(), THREAD, dependencies=deps
            )
            assert result == {"scheduled": True}
            # Registered synchronously; create_task schedules but does not run.
            assert stage.await_count == 0
            await _drain(deps)

        assert calls == ["lock"]
        assert stage.await_args.kwargs == {
            "thread_id": THREAD,
            "postgres_db": deps.store,
            "snapshot_service": deps.snapshots,
            "authority": cloud_stage_authority._capture_cloud_stage_authority(
                _authority_thread(), _authority_row()
            ),
            "vm_provisioner": deps.vm_provisioner,
        }

    @pytest.mark.asyncio
    async def test_a_duplicate_ping_for_the_same_key_schedules_nothing(self):
        deps, _ = _stage_deps()
        key = cloud_stage_authority._cloud_stage_task_key(
            THREAD,
            cloud_stage_authority._capture_cloud_stage_authority(
                _authority_thread(), _authority_row()
            ),
        )
        deps.cloud_tasks.preloaded.add(key)
        stage = AsyncMock()

        with _patched_stage(stage):
            result = await agent_trigger_cloud_stage(
                _request(), THREAD, dependencies=deps
            )

        assert result == {"scheduled": True}
        assert deps.cloud_tasks.started == []
        stage.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_the_broadcast_runs_after_the_lock_is_released(self, monkeypatch):
        deps, calls = _stage_deps()
        published = []
        monkeypatch.setattr(
            cloud_stage_authority,
            "_broadcast_cloud_stage_result",
            lambda result: published.append(result),
        )
        stage = AsyncMock(return_value={"event": {"ok": True}, "publication": {}})

        with _patched_stage(stage):
            await agent_trigger_cloud_stage(_request(), THREAD, dependencies=deps)
            await _drain(deps)

        assert published == [{"event": {"ok": True}, "publication": {}}]


def _patched_stage(stage_mock):
    from unittest.mock import patch

    return patch(
        "orchestrator.services.cloud_staging.stage.stage_thread_cloud_diff",
        stage_mock,
    )


async def _drain(deps):
    for task in list(deps.cloud_tasks.tasks.values()):
        await task
    await asyncio.sleep(0)


class TestAgentGetThreadLifecycle:
    @pytest.mark.asyncio
    async def test_internal_gate_fires_before_the_store(self):
        gate = AsyncMock(side_effect=HTTPException(401, "Invalid internal key"))
        deps, _ = _stage_deps(require_internal=gate)

        with pytest.raises(HTTPException) as exc:
            await agent_get_thread_lifecycle(_request(), THREAD, dependencies=deps)

        assert exc.value.status_code == 401
        deps.store.get_thread.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_missing_thread_is_a_404(self):
        deps, _ = _stage_deps()
        deps.store.get_thread = AsyncMock(return_value=None)

        with pytest.raises(HTTPException) as exc:
            await agent_get_thread_lifecycle(_request(), THREAD, dependencies=deps)

        assert exc.value.status_code == 404

    @pytest.mark.asyncio
    async def test_a_stateless_thread_skips_the_pinned_owner_gate(self):
        owner = AsyncMock()
        deps, _ = _stage_deps(require_pinned_workspace_credential_owner=owner)
        deps.store.get_thread = AsyncMock(
            return_value={
                "execution_lane": "stateless",
                "status": "active",
                "agent_id": AGENT,
                "runtime_generation": GENERATION,
                "runtime_attach_token": ATTACH,
                "runtime_retirement_token": None,
                "ended_at": None,
            }
        )

        result = await agent_get_thread_lifecycle(_request(), THREAD, dependencies=deps)

        owner.assert_not_awaited()
        assert result["status"] == "active"
        assert result["runtime_retirement_pending"] is False
        assert result["session_runtime_retirement_token"] is None

    @pytest.mark.asyncio
    async def test_a_pinned_thread_is_re_read_and_re_checked_after_the_gate(self):
        owner = AsyncMock()
        first = _authority_thread(status="active", ended_at=None)
        second = _authority_thread(status="active", ended_at=None)
        deps, _ = _stage_deps(require_pinned_workspace_credential_owner=owner)
        deps.store.get_thread = AsyncMock(side_effect=[first, second])

        await agent_get_thread_lifecycle(_request(), THREAD, dependencies=deps)

        assert deps.store.get_thread.await_count == 2
        assert owner.await_count == 2
        # The second (final) check is the one made against the re-read row.
        assert owner.await_args_list[1].args[0] is second

    @pytest.mark.asyncio
    async def test_a_retiring_pinned_thread_requires_a_full_uuid_identity(self):
        deps, _ = _stage_deps()
        deps.store.get_thread = AsyncMock(
            return_value=_authority_thread(runtime_retirement_token=RETIREMENT)
        )

        with pytest.raises(HTTPException) as exc:
            await agent_get_thread_lifecycle(
                _request({"X-Agent-ID": "not-a-uuid"}), THREAD, dependencies=deps
            )

        assert exc.value.status_code == 409
        assert exc.value.detail == {"code": "pinned_runtime_identity_required"}

    @pytest.mark.asyncio
    async def test_a_retiring_pinned_thread_that_does_not_match_is_a_409(self):
        deps, _ = _stage_deps()
        deps.store.get_thread = AsyncMock(
            return_value=_authority_thread(runtime_retirement_token=RETIREMENT)
        )
        deps.store.acquire = _acquire_returning(None)

        with pytest.raises(HTTPException) as exc:
            await agent_get_thread_lifecycle(
                _request(
                    {
                        "X-Agent-ID": AGENT,
                        "X-Session-Runtime-Generation": GENERATION,
                        "X-Session-Runtime-Attach-Token": ATTACH,
                    }
                ),
                THREAD,
                dependencies=deps,
            )

        assert exc.value.status_code == 409
        assert exc.value.detail == {"code": "pinned_runtime_identity_mismatch"}

    @pytest.mark.asyncio
    async def test_an_authorized_retirement_reports_ending_and_the_token(self):
        deps, _ = _stage_deps()
        deps.store.get_thread = AsyncMock(
            return_value=_authority_thread(runtime_retirement_token=RETIREMENT)
        )
        deps.store.acquire = _acquire_returning(
            {
                "status": "active",
                "agent_id": AGENT,
                "runtime_generation": GENERATION,
                "runtime_attach_token": ATTACH,
                "runtime_retirement_token": RETIREMENT,
                "runtime_retirement_permanent": True,
                "runtime_retirement_authorized_at": "2026-01-01T00:00:00Z",
                "runtime_retirement_context": '{"settle_status": "ended"}',
                "ended_at": None,
            }
        )

        result = await agent_get_thread_lifecycle(
            _request(
                {
                    "X-Agent-ID": AGENT,
                    "X-Session-Runtime-Generation": GENERATION,
                    "X-Session-Runtime-Attach-Token": ATTACH,
                }
            ),
            THREAD,
            dependencies=deps,
        )

        assert result["status"] == "ending"
        assert result["runtime_retirement_authorized"] is True
        assert result["runtime_retirement_preflight"] is False
        assert result["retirement_permanent"] is True
        assert result["retirement_disposition"] == "ended"
        assert result["session_runtime_retirement_token"] == RETIREMENT


def _acquire_returning(row):
    class _Acquire:
        async def __aenter__(self):
            conn = SimpleNamespace(fetchrow=AsyncMock(return_value=row))
            return conn

        async def __aexit__(self, *exc):
            return False

    return lambda: _Acquire()


class TestAgentGetThreadRetirementOutcome:
    def _headers(self, **over):
        headers = {
            "X-Agent-ID": AGENT,
            "X-Session-Runtime-Generation": GENERATION,
            "X-Session-Runtime-Attach-Token": ATTACH,
            "X-Session-Runtime-Retirement-Token": RETIREMENT,
            "X-Retirement-Disposition": "ended",
            "X-Retirement-Permanent": "true",
        }
        headers.update(over)
        return headers

    @pytest.mark.asyncio
    async def test_internal_gate_fires_before_the_header_validation(self):
        gate = AsyncMock(side_effect=HTTPException(401, "Invalid internal key"))
        deps, _ = _stage_deps(require_internal=gate)

        with pytest.raises(HTTPException) as exc:
            await agent_get_thread_retirement_outcome(
                _request({}), THREAD, dependencies=deps
            )

        assert exc.value.status_code == 401

    @pytest.mark.asyncio
    async def test_a_missing_permanent_header_refuses_before_any_store_read(self):
        deps, _ = _stage_deps()

        with pytest.raises(HTTPException) as exc:
            await agent_get_thread_retirement_outcome(
                _request(self._headers(**{"X-Retirement-Permanent": "maybe"})),
                THREAD,
                dependencies=deps,
            )

        assert exc.value.status_code == 409
        assert exc.value.detail == {
            "code": "pinned_retirement_outcome_identity_required"
        }
        deps.store.get_pinned_thread_retirement_outcome.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_recorded_outcome_is_returned_verbatim(self):
        deps, _ = _stage_deps()
        deps.store.get_pinned_thread_retirement_outcome = AsyncMock(
            return_value={"status": "ended", "retirement_disposition": "ended"}
        )

        result = await agent_get_thread_retirement_outcome(
            _request(self._headers()), THREAD, dependencies=deps
        )

        assert result == {"status": "ended", "retirement_disposition": "ended"}
        deps.store.get_thread.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_still_pending_identical_attempt_answers_ending(self):
        deps, _ = _stage_deps()
        deps.store.get_thread = AsyncMock(
            return_value={
                "runtime_generation": GENERATION,
                "runtime_retirement_token": RETIREMENT,
                "agent_id": AGENT,
                "runtime_attach_token": ATTACH,
                "runtime_retirement_permanent": True,
                "runtime_retirement_authorized_at": "2026-01-01T00:00:00Z",
                "runtime_retirement_context": {"settle_status": "ended"},
            }
        )

        result = await agent_get_thread_retirement_outcome(
            _request(self._headers()), THREAD, dependencies=deps
        )

        assert result == {
            "status": "ending",
            "retirement_disposition": "ended",
            "retirement_permanent": True,
        }

    @pytest.mark.asyncio
    async def test_a_successor_generation_is_never_the_callers_settlement(self):
        deps, _ = _stage_deps()
        deps.store.get_thread = AsyncMock(
            return_value={
                "runtime_generation": WORKSPACE_GENERATION,
                "runtime_retirement_token": RETIREMENT,
                "agent_id": AGENT,
                "runtime_attach_token": ATTACH,
                "runtime_retirement_permanent": True,
                "runtime_retirement_authorized_at": "2026-01-01T00:00:00Z",
                "runtime_retirement_context": {"settle_status": "ended"},
            }
        )

        with pytest.raises(HTTPException) as exc:
            await agent_get_thread_retirement_outcome(
                _request(self._headers()), THREAD, dependencies=deps
            )

        assert exc.value.status_code == 409
        assert exc.value.detail == {"code": "pinned_retirement_outcome_unproven"}

    @pytest.mark.asyncio
    async def test_a_generic_404_is_unproven_not_a_success(self):
        deps, _ = _stage_deps()
        deps.store.get_thread = AsyncMock(return_value=None)

        with pytest.raises(HTTPException) as exc:
            await agent_get_thread_retirement_outcome(
                _request(self._headers()), THREAD, dependencies=deps
            )

        assert exc.value.status_code == 409
        assert exc.value.detail == {"code": "pinned_retirement_outcome_unproven"}


# =============================================================================
# Wire: routes are mounted where they were, dependencies resolve per request
# =============================================================================


class TestAgentCloudStageWire:
    def test_routes_keep_their_paths_and_methods(self):
        app = mount_router(agent_cloud_stage_router)
        # ``app.routes`` is not the API on FastAPI >= 0.139: an include is one
        # ``_IncludedRouter`` wrapper with no path or methods of its own, so the
        # obvious comprehension silently sees zero of these routes. The pin is
        # ``fastapi>=0.109.0``, so a fresh install resolves the new shape while a
        # long-lived venv sits on the old one — read it through the helper.
        declared = {
            (route.path, tuple(sorted(route.methods)))
            for route in mounted_route_objects(app)
        }

        assert (
            "/api/agents/threads/{thread_id}/cloud-stage",
            ("POST",),
        ) in declared
        assert ("/api/agents/threads/{thread_id}/lifecycle", ("GET",)) in declared
        assert (
            "/api/agents/threads/{thread_id}/retirement-outcome",
            ("GET",),
        ) in declared

    def test_a_rebound_singleton_is_observed_by_the_next_dependency_build(self):
        first, _ = _stage_deps(is_protected_cloud_mode_enabled=lambda: False)
        second, _ = _stage_deps(is_protected_cloud_mode_enabled=lambda: False)
        second.store.get_thread = AsyncMock(return_value=None)
        holder = {"deps": first}
        app = mount_router(
            agent_cloud_stage_router,
            factories={
                "agent_cloud_stage_dependencies_factory": lambda: holder["deps"]
            },
        )

        with TestClient(app) as client:
            assert client.post(f"/api/agents/threads/{THREAD}/cloud-stage").json() == {
                "skipped": "flag_off"
            }
            holder["deps"] = replace(
                second, is_protected_cloud_mode_enabled=lambda: True
            )
            response = client.post(f"/api/agents/threads/{THREAD}/cloud-stage")

        assert response.status_code == 404
        assert response.json() == {"detail": "Thread not found"}

    def test_the_real_internal_gate_is_the_default_and_401s_without_a_key(self):
        deps, _ = _stage_deps()
        deps = replace(
            deps,
            require_internal=AgentCloudStageDependencies.__dataclass_fields__[
                "require_internal"
            ].default,
        )
        app = mount_router(
            agent_cloud_stage_router,
            factories={"agent_cloud_stage_dependencies_factory": lambda: deps},
        )

        with TestClient(app) as client:
            response = client.get(f"/api/agents/threads/{THREAD}/lifecycle")

        assert response.status_code == 401
        assert response.json() == {"detail": "Invalid internal key"}
