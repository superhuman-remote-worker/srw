"""Tests for Phase 1: a pre-agent scholar provisions its PARENT's shared
workspace under the parent's identity, instead of self-provisioning a throwaway
pod.

Design: knowledge-base/knowledge/issues/scholar_selfprovisioned_workspace_misclassified_as_inherited.md
(Phase 1 — one parent-owned workspace). On an idle cluster the parent holds no
workspace when the scholar is dispatched. Rather than the scholar creating
`workspace-<scholarId>` (a wasted second pod that the parent then can't inherit),
the scholar drives `ensure_workspace(WorkspaceOwner.job(<parentId>))` — creating
the ONE pod `workspace-<parentId>`, whose ready context lands on the PARENT's row
automatically — then promotes itself to a normal inheriting subjob and rides it
via the already-shipped worktree machinery.

These tests exercise the three new helpers directly with mocked collaborators.
"""

from __future__ import annotations

from orchestrator.services.job_workspace_runtime import scholar_provision_parent_id
from orchestrator.services.workspace_lifecycle import EnsureOutcome
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import tests.conftest  # noqa: F401 — license/crypto/env shims + sys.path
import orchestrator.main as main


def _ensure(outcome, status):
    """An ensure_workspace result. Use EnsureOutcome so the `is` identity
    checks inside the helper match (the enum is import-path sensitive under the
    conftest sys.path shims)."""
    return SimpleNamespace(outcome=outcome, status=status)


# --------------------------------------------------------------------------- #
# _scholar_provision_parent_id — pure marker extraction
# --------------------------------------------------------------------------- #
class TestScholarProvisionMarker:
    def test_marker_present_returns_parent_id(self):
        job = {
            "id": "scholar",
            "context": {"provisions_parent_workspace": "parent-uuid"},
        }
        assert scholar_provision_parent_id(job) == "parent-uuid"

    def test_marker_present_in_json_string_context(self):
        job = {"id": "s", "context": json.dumps({"provisions_parent_workspace": "p2"})}
        assert scholar_provision_parent_id(job) == "p2"

    def test_no_marker_returns_none(self):
        assert scholar_provision_parent_id({"id": "s", "context": {}}) is None

    def test_inheriting_scholar_is_not_a_provisioner(self):
        # An inheriting subjob (flag + copied container) must NOT be treated as a
        # provisioner — it rides an existing pod, it does not create one.
        job = {
            "id": "s",
            "context": {
                "inherits_parent_workspace": True,
                "workspace_container": {"status": "ready"},
            },
        }
        assert scholar_provision_parent_id(job) is None


# --------------------------------------------------------------------------- #
# _scholar_should_provision_parent_container — pure backend gate.
# (k8s/in-cluster enforcement lives at the dispatch seam, not here.)
# --------------------------------------------------------------------------- #
class TestScholarShouldProvision:
    def test_sandbox_backend_provisions(self):
        assert (
            main._scholar_should_provision_parent_container(
                {"workspace": {"backend": "sandbox"}}
            )
            is True
        )

    def test_unset_backend_provisions(self):
        # No explicit backend defaults to the sandbox container path.
        assert main._scholar_should_provision_parent_container({}) is True
        assert main._scholar_should_provision_parent_container(None) is True

    def test_vm_backend_does_not_provision(self):
        # VM/remote parents keep today's behavior (out of Slice 1 scope).
        assert (
            main._scholar_should_provision_parent_container(
                {"workspace": {"backend": "vm"}}
            )
            is False
        )
        assert (
            main._scholar_should_provision_parent_container(
                {"workspace": {"backend": "remote"}}
            )
            is False
        )

    def test_lite_backend_does_not_provision(self):
        assert (
            main._scholar_should_provision_parent_container(
                {"workspace": {"backend": "virtual"}}
            )
            is False
        )
        assert (
            main._scholar_should_provision_parent_container(
                {"workspace": {"backend": "none"}}
            )
            is False
        )


# --------------------------------------------------------------------------- #
# _provision_parent_workspace_for_scholar — the wait/promoted/fail state machine
# --------------------------------------------------------------------------- #
class _FakeConn:
    def __init__(self):
        self.executed = []

    async def execute(self, query, *args):
        self.executed.append((query, args))


class _FakeAcquire:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *a):
        return False


READY_CONTAINER = {
    "status": "ready",
    "host": "workspace-parent.srw.svc.cluster.local",
    "pod_ip": "10.42.0.9",
    "port": 30022,
    "pod_name": "workspace-parent1234",
}


class TestProvisionParentWorkspaceForScholar:
    @pytest.fixture
    def scholar_job(self):
        return {"id": "aabbccdd-1111-2222-3333-444455556666", "config_name": "scholar"}

    @pytest.fixture
    def wire(self, monkeypatch):
        """Patch DB + ensure_workspace + unblock; return handles for assertions."""
        state = {"merge": [], "conn": _FakeConn(), "fail": AsyncMock()}

        monkeypatch.setattr(
            main.postgres_db,
            "merge_job_context",
            AsyncMock(
                side_effect=lambda jid, delta: state["merge"].append((jid, delta))
            ),
        )
        monkeypatch.setattr(
            main.postgres_db, "acquire", lambda: _FakeAcquire(state["conn"])
        )
        monkeypatch.setattr(main, "_fail_subjob_and_unblock_parent", state["fail"])
        return state

    @pytest.mark.asyncio
    async def test_pending_returns_wait(self, monkeypatch, scholar_job, wire):
        parent = {
            "id": "parent-uuid",
            "context": {"workspace_container": {"status": "creating"}},
            "config_override": {},
        }
        monkeypatch.setattr(main.postgres_db, "get_job", AsyncMock(return_value=parent))
        monkeypatch.setattr(
            main,
            "ensure_workspace",
            AsyncMock(return_value=_ensure(EnsureOutcome.PENDING, "creating")),
        )

        result = await main._provision_parent_workspace_for_scholar(
            scholar_job, "parent-uuid"
        )

        assert result == "wait"
        assert state_unchanged(wire)

    @pytest.mark.asyncio
    async def test_failed_returns_fail_and_unblocks(
        self, monkeypatch, scholar_job, wire
    ):
        parent = {"id": "parent-uuid", "context": {}, "config_override": {}}
        monkeypatch.setattr(main.postgres_db, "get_job", AsyncMock(return_value=parent))
        monkeypatch.setattr(
            main,
            "ensure_workspace",
            AsyncMock(return_value=_ensure(EnsureOutcome.FAILED, "failed")),
        )

        result = await main._provision_parent_workspace_for_scholar(
            scholar_job, "parent-uuid"
        )

        assert result == "fail"
        wire["fail"].assert_awaited_once()
        assert not wire["merge"]  # not promoted

    @pytest.mark.asyncio
    async def test_missing_parent_returns_fail_without_provisioning(
        self, monkeypatch, scholar_job, wire
    ):
        monkeypatch.setattr(main.postgres_db, "get_job", AsyncMock(return_value=None))
        ensure = AsyncMock()
        monkeypatch.setattr(main, "ensure_workspace", ensure)

        result = await main._provision_parent_workspace_for_scholar(
            scholar_job, "gone-uuid"
        )

        assert result == "fail"
        wire["fail"].assert_awaited_once()
        ensure.assert_not_awaited()  # never tried to provision a dead parent

    @pytest.mark.asyncio
    async def test_ready_promotes_scholar_to_inherit(
        self, monkeypatch, scholar_job, wire
    ):
        # Initial read: parent still creating (no host). Fresh read after ready:
        # the parent row now carries the ready container that create_workspace wrote.
        parent_creating = {
            "id": "parent-uuid",
            "context": {"workspace_container": {"status": "creating"}},
            "config_override": {"workspace": {"container": {"cpu": "500m"}}},
        }
        parent_ready = {
            "id": "parent-uuid",
            "context": {"workspace_container": dict(READY_CONTAINER)},
            "config_override": {},
        }
        monkeypatch.setattr(
            main.postgres_db,
            "get_job",
            AsyncMock(side_effect=[parent_creating, parent_ready]),
        )
        monkeypatch.setattr(
            main,
            "ensure_workspace",
            AsyncMock(return_value=_ensure(EnsureOutcome.READY, "ready")),
        )

        result = await main._provision_parent_workspace_for_scholar(
            scholar_job, "parent-uuid"
        )

        assert result == "promoted"
        wire["fail"].assert_not_awaited()
        # Persist only the inherit discriminator. Dispatch re-reads and overlays
        # the parent's ready runtime in memory; copying it here would claim
        # parent-owned Kubernetes authority on the child row.
        assert len(wire["merge"]) == 1
        jid, delta = wire["merge"][0]
        assert jid == scholar_job["id"]
        assert delta == {"inherits_parent_workspace": True}
        # worktree_path persisted for the injector: <scholarId[:8]>-<config_name>.
        assert len(wire["conn"].executed) == 1
        query, args = wire["conn"].executed[0]
        assert "worktree_path" in query
        assert args[0] == "/home/agent-host/workspace/worktrees/aabbccdd-scholar"
        assert args[1] == scholar_job["id"]


def state_unchanged(wire) -> bool:
    """No promotion side effects happened."""
    return (
        not wire["merge"] and not wire["conn"].executed and not wire["fail"].await_count
    )
