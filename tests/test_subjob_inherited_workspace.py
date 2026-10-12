"""Tests for subjob workspace inheritance resolved live at dispatch.

Regression coverage for two coupled issues:

* knowledge-base/knowledge/issues/subjob_inherits_stale_workspace_container_snapshot.md — a
  scholar/critic subjob copies the parent's ``workspace_container`` / ``vm``
  context by value at spawn time. When the scholar is spawned ~3s after its
  parent (before the parent pod is ready) that snapshot is frozen at
  ``status='created'`` with no SSH host and strands the subjob at
  ``init_workspace``. ``_resolve_subjob_inherited_workspace`` re-reads the
  parent's LIVE context at dispatch and overlays it.

* knowledge-base/knowledge/issues/scholar_selfprovisioned_workspace_misclassified_as_inherited.md —
  when the parent has NO workspace at scholar-spawn (idle cluster), the scholar
  inherits nothing and self-provisions its OWN pod, writing its own
  ``workspace_container`` into context. The old resolver keyed the inherit path
  off mere *presence* of that key, so it misread the self-provisioned scholar as
  "inheriting", waited 600s on a parent workspace that never exists, then failed
  — and the dispatch-path failure never unblocked the parent, stranding it in
  ``waiting`` forever. The fix: inheriting subjobs carry an explicit
  ``inherits_parent_workspace`` flag (stamped only when they actually copy the
  parent's snapshot); the resolver gates on the flag, not on key presence; and a
  dispatch-time subjob failure routes through the parent-unblock handlers.

These tests exercise the real functions with a mocked ``postgres_db``.
"""

from __future__ import annotations

from tests import b08_completion_helpers as b08_helpers

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

import tests.conftest  # noqa: F401 — applies license/crypto/env shims + sys.path
import orchestrator.main as main
from shared.backend_kinds import LITE_BACKENDS
from orchestrator.application import preparation as preparation_composition
from orchestrator.services import container_provisioner as container_provisioner_module
from orchestrator.services import (
    job_datasource_selection as job_datasource_selection_module,
)
from orchestrator.services import job_dispatcher as job_dispatcher_module
from orchestrator.services import (
    job_workspace_authority as job_workspace_authority_module,
)
from orchestrator.services import subjob_completion as subjob_completion_module


READY_CONTAINER = {
    "status": "ready",
    "host": "workspace-parent.ns.svc.cluster.local",
    "pod_ip": "10.42.3.218",
    "port": 30022,
    "pod_name": "workspace-parent",
    "_runtime_incarnation": "11111111-1111-4111-8111-111111111111",
}
STALE_CONTAINER = {"status": "created", "pod_name": "workspace-parent"}
READY_VM = {
    "status": "ready",
    "ssh_host": "100.64.0.1",
    "pod_ip": "10.0.2.1",
    "provision_generation": "22222222-2222-4222-8222-222222222222",
}


def _stamp_workspace(row):
    if row is None:
        return None
    row = dict(row)
    context = row.get("context") or {}
    context_was_json = isinstance(context, str)
    if context_was_json:
        import json

        context = json.loads(context)
    context = dict(context)
    backend = "vm" if "vm" in context else "sandbox"
    context.setdefault(
        "_workspace_contract",
        {
            "version": 1,
            "requested_backend": backend,
            "assigned_backend": backend,
            "assignment_source": "test",
        },
    )
    row["context"] = context
    row.setdefault("config_override", {"workspace": {"backend": backend}})
    return row


def _subjob(context, *, parent_id="parent-uuid", age_s=5.0):
    """Build a subjob row like get_dispatchable_jobs returns."""
    return _stamp_workspace(
        {
            "id": "subjob-uuid",
            "parent_job_id": parent_id,
            "context": context,
            "created_at": datetime.now(timezone.utc) - timedelta(seconds=age_s),
        }
    )


def _inherited(container=None, vm=None):
    """Context of a subjob that INHERITED its parent's workspace at spawn.

    Carries the explicit ``inherits_parent_workspace`` flag (stamped by
    ``_spawn_scholar_subjob`` / ``_trigger_verification_on_complete``). Optional
    workspace snapshots here exercise compatibility with historical rows.
    Contrast a *self-provisioned* subjob, whose context carries a workspace key
    with NO flag.
    """
    ctx: dict = {"inherits_parent_workspace": True}
    if container is not None:
        ctx["workspace_container"] = container
    if vm is not None:
        ctx["vm"] = vm
    return ctx


@pytest.fixture
def patch_get_job(monkeypatch):
    """Patch main.postgres_db.get_job with an AsyncMock; return the mock."""

    def _apply(parent_row):
        mock = AsyncMock(return_value=_stamp_workspace(parent_row))
        monkeypatch.setattr(main.app.state.resources.postgres_db, "get_job", mock)
        return mock

    return _apply


class TestNonInheriting:
    """No-op paths: nothing to resolve."""

    @pytest.mark.asyncio
    async def test_no_parent_proceeds(self, patch_get_job):
        mock = patch_get_job(None)
        job = {"id": "j", "parent_job_id": None, "context": {}}
        assert await job_workspace_authority_module.resolve_subjob_inherited_workspace(
            job,
            dependencies=preparation_composition.job_workspace_authority_dependencies(
                main.app.state.resources
            ),
        ) == ("proceed", None)
        mock.assert_not_awaited()  # never touches the DB

    @pytest.mark.asyncio
    async def test_subjob_without_inherited_keys_proceeds(self, patch_get_job):
        mock = patch_get_job(None)
        job = _subjob({"scholar_target": "parent-uuid"})
        assert await job_workspace_authority_module.resolve_subjob_inherited_workspace(
            job,
            dependencies=preparation_composition.job_workspace_authority_dependencies(
                main.app.state.resources
            ),
        ) == ("proceed", None)
        mock.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_own_snapshot_already_ready_proceeds(self, patch_get_job):
        """A ready child snapshot is trusted only after parent liveness is rechecked."""
        mock = patch_get_job(
            {"status": "waiting", "context": {"workspace_container": READY_CONTAINER}}
        )
        job = _subjob(_inherited(container=dict(READY_CONTAINER)))
        assert await job_workspace_authority_module.resolve_subjob_inherited_workspace(
            job,
            dependencies=preparation_composition.job_workspace_authority_dependencies(
                main.app.state.resources
            ),
        ) == ("proceed", None)
        mock.assert_awaited_once_with("parent-uuid")


class TestSelfProvisionedDiscrimination:
    """A subjob that self-provisioned its OWN workspace (no inherit flag) must
    ride the normal dispatch path — never wait on, or fail against, a parent
    workspace that will never exist.

    Regression for
    knowledge-base/knowledge/issues/scholar_selfprovisioned_workspace_misclassified_as_inherited.md.
    The old resolver keyed off presence of ``workspace_container`` / ``vm`` and
    so misclassified these as inheriting.
    """

    @pytest.mark.asyncio
    async def test_self_provisioned_ready_container_proceeds_without_flag(
        self, patch_get_job
    ):
        # Scholar self-provisioned: it wrote its OWN ready container into context
        # but carries NO inherits_parent_workspace flag. Must proceed WITHOUT even
        # consulting the (workspace-less) parent.
        mock = patch_get_job(None)
        job = _subjob({"workspace_container": dict(READY_CONTAINER)})
        assert await job_workspace_authority_module.resolve_subjob_inherited_workspace(
            job,
            dependencies=preparation_composition.job_workspace_authority_dependencies(
                main.app.state.resources
            ),
        ) == ("proceed", None)
        mock.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_self_provisioned_creating_container_proceeds_not_waits(
        self, patch_get_job
    ):
        # The exact shape that used to strand: a self-provisioned pod mid-creation
        # (status=creating) with no flag. Old code waited on the parent for 600s
        # then failed; new code proceeds immediately down the normal path.
        mock = patch_get_job({"status": "waiting", "context": {}})
        job = _subjob(
            {
                "workspace_container": {
                    "status": "creating",
                    "pod_name": "workspace-self",
                }
            }
        )
        assert await job_workspace_authority_module.resolve_subjob_inherited_workspace(
            job,
            dependencies=preparation_composition.job_workspace_authority_dependencies(
                main.app.state.resources
            ),
        ) == ("proceed", None)
        mock.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_self_provisioned_vm_proceeds_without_flag(self, patch_get_job):
        mock = patch_get_job(None)
        job = _subjob({"vm": {"status": "creating", "requested": True}})
        assert await job_workspace_authority_module.resolve_subjob_inherited_workspace(
            job,
            dependencies=preparation_composition.job_workspace_authority_dependencies(
                main.app.state.resources
            ),
        ) == ("proceed", None)
        mock.assert_not_awaited()


class TestContainerInheritance:
    @pytest.mark.asyncio
    async def test_exact_legacy_parent_runtime_is_adopted_under_parent_owner(
        self, monkeypatch
    ):
        from orchestrator.services.container_provisioner import (
            WorkspaceRuntimeAttestation,
        )

        parent_id = "11111111-1111-4111-8111-111111111111"
        old_runtime = {
            "status": "ready",
            "provisioner": "k8s",
            "pod_ip": "10.42.3.218",
            "port": 30022,
            "host": "workspace-parent.ns.svc.cluster.local",
        }
        parent = {
            "id": parent_id,
            "status": "waiting",
            "execution_lane": "pinned",
            "config_override": {"workspace": {"backend": "container"}},
            # Exact prior-release row: no contract and no runtime UID.
            "context": {"workspace_container": dict(old_runtime)},
        }
        attestation = WorkspaceRuntimeAttestation(
            backing_id="k8s-pvc:test:22222222-2222-4222-8222-222222222222",
            workspace_generation="22222222-2222-4222-8222-222222222222",
            runtime_incarnation="33333333-3333-4333-8333-333333333333",
            ssh_host_key_fingerprint="SHA256:" + ("a" * 43),
            host=old_runtime["host"],
            pod_ip=old_runtime["pod_ip"],
            port=old_runtime["port"],
        )
        state = {"row": deepcopy(parent)}

        async def _get_job(_job_id):
            return deepcopy(state["row"])

        async def _adopt(_job_id, **kwargs):
            current_workspace = state["row"]["context"]["workspace_container"]
            if current_workspace != kwargs["expected_workspace"]:
                return False
            state["row"]["context"]["workspace_container"] = deepcopy(
                kwargs["adopted_workspace"]
            )
            return True

        get_job = AsyncMock(side_effect=_get_job)
        monkeypatch.setattr(main.app.state.resources.postgres_db, "get_job", get_job)
        reserve = AsyncMock(
            return_value={
                "id": "44444444-4444-4444-8444-444444444444",
                "reservation_generation": 1,
                "claim_token": 1,
            }
        )
        monkeypatch.setattr(
            main.app.state.resources.postgres_db,
            "reserve_managed_repository_workspace_creation",
            reserve,
        )
        authorize = AsyncMock(return_value=True)
        monkeypatch.setattr(
            main.app.state.resources.postgres_db,
            "authorize_managed_repository_workspace_creation_runtime",
            authorize,
        )
        settle = AsyncMock(return_value=True)
        monkeypatch.setattr(
            main.app.state.resources.postgres_db,
            "settle_managed_repository_workspace_creation_reservation",
            settle,
        )
        monkeypatch.setattr(
            main.app.state.resources.postgres_db,
            "abort_managed_repository_workspace_creation_reservation",
            AsyncMock(return_value=True),
        )
        cas = AsyncMock(side_effect=_adopt)
        monkeypatch.setattr(
            main.app.state.resources.postgres_db,
            "adopt_legacy_k8s_job_workspace_runtime",
            cas,
        )
        attest = AsyncMock(return_value=attestation)
        monkeypatch.setattr(
            container_provisioner_module.container_provisioner,
            "attest_workspace_runtime",
            attest,
        )
        child = _subjob(_inherited(container=dict(old_runtime)), parent_id=parent_id)

        assert await job_workspace_authority_module.resolve_subjob_inherited_workspace(
            child,
            dependencies=preparation_composition.job_workspace_authority_dependencies(
                main.app.state.resources
            ),
        ) == (
            "proceed",
            None,
        )

        assert (
            child["context"]["workspace_container"]["_runtime_incarnation"]
            == attestation.runtime_incarnation
        )
        assert attest.await_count == 3
        assert all(call.args[0].id == parent_id for call in attest.await_args_list)
        cas.assert_awaited_once()
        reserve.assert_awaited_once()
        authorize.assert_awaited_once()
        settle.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_flag_only_child_overlays_parent_ready_container(self, patch_get_job):
        get_parent = patch_get_job(
            {"status": "waiting", "context": {"workspace_container": READY_CONTAINER}}
        )
        job = _subjob(_inherited())

        assert await job_workspace_authority_module.resolve_subjob_inherited_workspace(
            job,
            dependencies=preparation_composition.job_workspace_authority_dependencies(
                main.app.state.resources
            ),
        ) == (
            "proceed",
            None,
        )

        get_parent.assert_awaited_once_with("parent-uuid")
        assert job["context"]["workspace_container"] == READY_CONTAINER

    @pytest.mark.asyncio
    async def test_stale_snapshot_overlays_parent_ready_container(self, patch_get_job):
        patch_get_job(
            {"status": "waiting", "context": {"workspace_container": READY_CONTAINER}}
        )
        job = _subjob(_inherited(container=dict(STALE_CONTAINER)))
        result = await job_workspace_authority_module.resolve_subjob_inherited_workspace(
            job,
            dependencies=preparation_composition.job_workspace_authority_dependencies(
                main.app.state.resources
            ),
        )
        assert result == ("proceed", None)
        # In-memory context now carries the parent's ready host/pod_ip.
        assert job["context"]["workspace_container"]["status"] == "ready"
        assert job["context"]["workspace_container"]["host"] == READY_CONTAINER["host"]

    @pytest.mark.asyncio
    async def test_parent_still_provisioning_waits(self, patch_get_job):
        patch_get_job(
            {
                "status": "waiting",
                "context": {"workspace_container": {"status": "creating"}},
            }
        )
        job = _subjob(_inherited(container=dict(STALE_CONTAINER)), age_s=5.0)
        assert await job_workspace_authority_module.resolve_subjob_inherited_workspace(
            job,
            dependencies=preparation_composition.job_workspace_authority_dependencies(
                main.app.state.resources
            ),
        ) == ("wait", None)
        # Inherited snapshot left untouched while waiting.
        assert job["context"]["workspace_container"] == STALE_CONTAINER

    @pytest.mark.asyncio
    async def test_wait_times_out_to_fail(self, patch_get_job):
        patch_get_job(
            {
                "status": "waiting",
                "context": {"workspace_container": {"status": "creating"}},
            }
        )
        job = _subjob(
            _inherited(container=dict(STALE_CONTAINER)),
            age_s=job_workspace_authority_module.INHERIT_WORKSPACE_MAX_WAIT_S + 60,
        )
        (
            action,
            msg,
        ) = await job_workspace_authority_module.resolve_subjob_inherited_workspace(
            job,
            dependencies=preparation_composition.job_workspace_authority_dependencies(
                main.app.state.resources
            ),
        )
        assert action == "fail"
        assert "Timed out" in msg

    @pytest.mark.asyncio
    async def test_resumed_subjob_budget_anchors_on_outage_wake(self, patch_get_job):
        """An outage-paused subjob re-dispatched hours after spawn gets a fresh
        inherit window anchored on the outage's scheduled wake
        (context.llm_outage.next_retry_at) — keyed on created_at, a resume 3h
        after spawn would insta-fail on any transiently non-ready parent
        workspace (knowledge-base/knowledge/features/llm_outage_subjob_resilience.md #5)."""
        patch_get_job(
            {
                "status": "waiting",
                "context": {"workspace_container": {"status": "creating"}},
            }
        )
        ctx = _inherited(container=dict(STALE_CONTAINER))
        ctx["llm_outage"] = {
            "attempt": 2,
            "next_retry_at": (
                datetime.now(timezone.utc) - timedelta(seconds=30)
            ).isoformat(),
        }
        job = _subjob(ctx, age_s=3 * 3600)  # spawned 3h ago, woke 30s ago
        assert await job_workspace_authority_module.resolve_subjob_inherited_workspace(
            job,
            dependencies=preparation_composition.job_workspace_authority_dependencies(
                main.app.state.resources
            ),
        ) == ("wait", None)

    @pytest.mark.asyncio
    async def test_resumed_subjob_budget_still_bounded_after_wake(self, patch_get_job):
        """The re-anchored window is still finite — a wake long past the budget
        times out to fail exactly like a fresh subjob would."""
        patch_get_job(
            {
                "status": "waiting",
                "context": {"workspace_container": {"status": "creating"}},
            }
        )
        ctx = _inherited(container=dict(STALE_CONTAINER))
        ctx["llm_outage"] = {
            "attempt": 2,
            "next_retry_at": (
                datetime.now(timezone.utc)
                - timedelta(
                    seconds=job_workspace_authority_module.INHERIT_WORKSPACE_MAX_WAIT_S
                    + 60
                )
            ).isoformat(),
        }
        job = _subjob(ctx, age_s=6 * 3600)
        (
            action,
            msg,
        ) = await job_workspace_authority_module.resolve_subjob_inherited_workspace(
            job,
            dependencies=preparation_composition.job_workspace_authority_dependencies(
                main.app.state.resources
            ),
        )
        assert action == "fail"
        assert "Timed out" in msg

    @pytest.mark.asyncio
    async def test_parent_workspace_failed_fails_fast(self, patch_get_job):
        patch_get_job(
            {
                "status": "processing",
                "context": {"workspace_container": {"status": "failed"}},
            }
        )
        job = _subjob(_inherited(container=dict(STALE_CONTAINER)))
        (
            action,
            msg,
        ) = await job_workspace_authority_module.resolve_subjob_inherited_workspace(
            job,
            dependencies=preparation_composition.job_workspace_authority_dependencies(
                main.app.state.resources
            ),
        )
        assert action == "fail"
        assert "unavailable" in msg

    @pytest.mark.asyncio
    async def test_ready_snapshot_with_dead_parent_fails_fast(self, patch_get_job):
        patch_get_job(
            {
                "status": "failed",
                "context": {"workspace_container": {"status": "deleted"}},
            }
        )
        job = _subjob(_inherited(container=dict(READY_CONTAINER)))
        (
            action,
            msg,
        ) = await job_workspace_authority_module.resolve_subjob_inherited_workspace(
            job,
            dependencies=preparation_composition.job_workspace_authority_dependencies(
                main.app.state.resources
            ),
        )
        assert action == "fail"
        assert "unavailable" in msg

    @pytest.mark.asyncio
    async def test_parent_terminal_fails_fast(self, patch_get_job):
        patch_get_job(
            {
                "status": "failed",
                "context": {"workspace_container": {"status": "creating"}},
            }
        )
        job = _subjob(_inherited(container=dict(STALE_CONTAINER)))
        (
            action,
            _,
        ) = await job_workspace_authority_module.resolve_subjob_inherited_workspace(
            job,
            dependencies=preparation_composition.job_workspace_authority_dependencies(
                main.app.state.resources
            ),
        )
        assert action == "fail"

    @pytest.mark.asyncio
    async def test_parent_missing_fails_fast(self, patch_get_job):
        patch_get_job(None)
        job = _subjob(_inherited(container=dict(STALE_CONTAINER)))
        (
            action,
            msg,
        ) = await job_workspace_authority_module.resolve_subjob_inherited_workspace(
            job,
            dependencies=preparation_composition.job_workspace_authority_dependencies(
                main.app.state.resources
            ),
        )
        assert action == "fail"
        assert "no longer exists" in msg

    @pytest.mark.asyncio
    async def test_context_as_json_string(self, patch_get_job):
        import json

        patch_get_job(
            {"status": "waiting", "context": {"workspace_container": READY_CONTAINER}}
        )
        job = _subjob(json.dumps(_inherited(container=dict(STALE_CONTAINER))))
        result = await job_workspace_authority_module.resolve_subjob_inherited_workspace(
            job,
            dependencies=preparation_composition.job_workspace_authority_dependencies(
                main.app.state.resources
            ),
        )
        assert result == ("proceed", None)
        assert job["context"]["workspace_container"]["status"] == "ready"


class TestVmInheritance:
    @pytest.mark.asyncio
    async def test_stale_vm_overlays_parent_ready_vm(self, patch_get_job):
        patch_get_job({"status": "waiting", "context": {"vm": READY_VM}})
        job = _subjob(_inherited(vm={"status": "creating", "requested": True}))
        result = await job_workspace_authority_module.resolve_subjob_inherited_workspace(
            job,
            dependencies=preparation_composition.job_workspace_authority_dependencies(
                main.app.state.resources
            ),
        )
        assert result == ("proceed", None)
        assert job["context"]["vm"]["status"] == "ready"
        assert job["context"]["vm"]["ssh_host"] == READY_VM["ssh_host"]

    @pytest.mark.asyncio
    async def test_vm_parent_not_ready_waits(self, patch_get_job):
        patch_get_job(
            {"status": "waiting", "context": {"vm": {"status": "provisioning"}}}
        )
        job = _subjob(
            _inherited(vm={"status": "creating", "requested": True}), age_s=5.0
        )
        assert await job_workspace_authority_module.resolve_subjob_inherited_workspace(
            job,
            dependencies=preparation_composition.job_workspace_authority_dependencies(
                main.app.state.resources
            ),
        ) == ("wait", None)


class TestFailSubjobUnblocksParent:
    """A subjob failed at dispatch time (e.g. it cannot inherit its parent's
    workspace) must also unblock its parent, which _spawn_scholar_subjob left
    held in 'waiting'. Otherwise the parent strands forever — the secondary bug
    in scholar_selfprovisioned_workspace_misclassified_as_inherited.md.
    """

    @pytest.fixture
    def patch_db(self, monkeypatch):
        """Record status transitions + context merges; parent starts 'waiting'."""
        calls = {"status": [], "merge": []}

        async def fake_update_status(job_id, status=None, **kw):
            calls["status"].append((job_id, status, kw.get("error_message")))

        async def fake_merge(job_id, delta):
            calls["merge"].append((job_id, delta))

        parent_row = {"id": "parent-uuid", "status": "waiting", "context": {}}

        monkeypatch.setattr(
            main.app.state.resources.postgres_db,
            "update_job_status",
            AsyncMock(side_effect=fake_update_status),
        )
        monkeypatch.setattr(
            main.app.state.resources.postgres_db,
            "get_job",
            AsyncMock(return_value=parent_row),
        )
        monkeypatch.setattr(
            main.app.state.resources.postgres_db,
            "merge_job_context",
            AsyncMock(side_effect=fake_merge),
        )
        monkeypatch.setattr(
            job_dispatcher_module, "trigger_dispatch", lambda *a, **k: None
        )
        return calls

    @pytest.mark.asyncio
    async def test_failed_scholar_unblocks_waiting_parent(self, patch_db):
        job = _subjob({"scholar_target": "parent-uuid"})
        job["id"] = "scholar-uuid"
        job["status"] = "created"
        job["creation_order"] = None

        await job_workspace_authority_module.fail_subjob_and_unblock_parent(
            job,
            "cannot inherit: boom",
            dependencies=preparation_composition.job_workspace_authority_dependencies(
                main.app.state.resources
            ),
        )

        # 1. Scholar marked failed with the diagnostic message.
        assert ("scholar-uuid", "failed", "cannot inherit: boom") in patch_db["status"]
        # 2. Parent flipped waiting -> created (the unblock).
        assert ("parent-uuid", "created", None) in patch_db["status"]
        # 3. scholar_failed recorded on the parent context.
        assert ("parent-uuid", {"scholar_failed": True}) in patch_db["merge"]

    @pytest.mark.asyncio
    async def test_non_scholar_subjob_still_marked_failed(self, patch_db):
        # A subjob that isn't a scholar/delegation child (no scholar_target, no
        # creation_order) is still failed; there is simply no parent to unblock.
        job = _subjob({"verification_target": "parent-uuid"})
        job["id"] = "critic-uuid"
        job["status"] = "created"
        job["creation_order"] = None

        await job_workspace_authority_module.fail_subjob_and_unblock_parent(
            job,
            "workspace gone",
            dependencies=preparation_composition.job_workspace_authority_dependencies(
                main.app.state.resources
            ),
        )

        assert ("critic-uuid", "failed", "workspace gone") in patch_db["status"]
        # No unblock: parent was never transitioned to created.
        assert not any(s == "created" for _, s, _ in patch_db["status"])

    @pytest.mark.asyncio
    async def test_stale_stateless_failure_does_not_unblock_parent(self, monkeypatch):
        job = _subjob({"scholar_target": "parent-uuid"})
        job.update(
            {
                "id": "scholar-uuid",
                "status": "created",
                "execution_lane": "stateless",
                "creation_order": None,
            }
        )
        update = AsyncMock(return_value=False)
        scholar = AsyncMock()
        delegation = AsyncMock()
        monkeypatch.setattr(
            main.app.state.resources.postgres_db, "update_job_status", update
        )
        monkeypatch.setattr(
            subjob_completion_module,
            "handle_scholar_completion",
            scholar,
        )
        monkeypatch.setattr(
            subjob_completion_module,
            "handle_delegation_child_completion",
            delegation,
        )

        await job_workspace_authority_module.fail_subjob_and_unblock_parent(
            job,
            "cannot inherit",
            dependencies=preparation_composition.job_workspace_authority_dependencies(
                main.app.state.resources
            ),
        )

        update.assert_awaited_once_with(
            "scholar-uuid",
            status="failed",
            error_message="cannot inherit",
            expected_status="created",
        )
        scholar.assert_not_awaited()
        delegation.assert_not_awaited()


class TestScholarMaterializationFailure:
    @pytest.mark.asyncio
    async def test_policy_failure_releases_waiting_parent(self, monkeypatch):
        from orchestrator.database.postgres import (
            DatasourceMaterializationAuthorizationError,
        )
        from orchestrator.services import completion

        parent_id = "11111111-1111-4111-8111-111111111111"
        owner_id = "22222222-2222-4222-8222-222222222222"
        datasource_id = "33333333-3333-4333-8333-333333333333"
        job = {
            "id": parent_id,
            "user_id": owner_id,
            "description": "Research this",
            "context": {"workspace_container": dict(READY_CONTAINER)},
            "config_override": {"workspace": {"backend": "sandbox"}},
            "project_id": None,
            "parent_job_id": None,
        }
        job = _stamp_workspace(job)

        monkeypatch.setattr(
            completion,
            "resolve_scholar_config_from_disk",
            lambda *_args, **_kwargs: {
                "enabled": True,
                "scholar_config": "scholar",
            },
        )
        monkeypatch.setattr(
            completion,
            "format_scholar_instructions",
            lambda **_kwargs: "instructions",
        )
        monkeypatch.setattr(
            job_datasource_selection_module,
            "revalidate_job_datasource_selection",
            AsyncMock(return_value=([datasource_id], {datasource_id: 4})),
        )
        monkeypatch.setattr(
            job_datasource_selection_module,
            "datasource_selection_provenance",
            AsyncMock(return_value={"datasource_ids": [datasource_id]}),
        )
        monkeypatch.setattr(
            main.app.state.resources.postgres_db,
            "get_user",
            AsyncMock(return_value={"id": owner_id}),
        )
        update_status = AsyncMock()
        merge_context = AsyncMock()
        monkeypatch.setattr(
            main.app.state.resources.postgres_db, "update_job_status", update_status
        )
        monkeypatch.setattr(
            main.app.state.resources.postgres_db, "merge_job_context", merge_context
        )
        monkeypatch.setattr(
            main.app.state.resources.postgres_db,
            "create_job",
            AsyncMock(
                side_effect=DatasourceMaterializationAuthorizationError(
                    "authority changed"
                )
            ),
        )
        dispatch = MagicMock()
        monkeypatch.setattr(job_dispatcher_module, "trigger_dispatch", dispatch)

        with pytest.raises(DatasourceMaterializationAuthorizationError):
            await b08_helpers.spawn_scholar_subjob(job, "worker", {}, {})

        assert [call.kwargs["status"] for call in update_status.await_args_list] == [
            "waiting",
            "created",
        ]
        merge_context.assert_awaited_once_with(parent_id, {"scholar_failed": True})
        dispatch.assert_called_once()


# =============================================================================
# Dispatch backstop predicate — mirrors _dispatch_job_to_agent (main.py).
# A workspace-backed job with no injected SSH remote hard-fails the agent at
# init_workspace; the dispatcher refuses to POST it. Lite tiers are exempt.
# =============================================================================


def _dispatch_would_refuse(config_override: dict | None) -> bool:
    """Replica of the backstop condition in _dispatch_job_to_agent."""
    ws_final = (config_override or {}).get("workspace", {})
    backend_final = ws_final.get("backend")
    return backend_final not in LITE_BACKENDS and not ws_final.get("remote")


class TestDispatchBackstopPredicate:
    def test_refuses_sandbox_without_remote(self):
        assert _dispatch_would_refuse({"workspace": {"backend": "sandbox"}}) is True

    def test_refuses_vm_without_remote(self):
        assert _dispatch_would_refuse({"workspace": {"backend": "vm"}}) is True

    def test_refuses_no_config_at_all(self):
        # backend defaults to sandbox agent-side → still needs remote.
        assert _dispatch_would_refuse(None) is True
        assert _dispatch_would_refuse({}) is True

    def test_allows_sandbox_with_remote(self):
        co = {
            "workspace": {"backend": "sandbox", "remote": {"host": "h", "port": 30022}}
        }
        assert _dispatch_would_refuse(co) is False

    def test_allows_vm_with_remote(self):
        co = {"workspace": {"backend": "vm", "remote": {"host": "h"}}}
        assert _dispatch_would_refuse(co) is False

    def test_exempts_lite_backends(self):
        assert _dispatch_would_refuse({"workspace": {"backend": "virtual"}}) is False
        assert _dispatch_would_refuse({"workspace": {"backend": "none"}}) is False
