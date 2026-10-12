import pytest

from orchestrator.services.workspace_lifecycle import WorkspaceOwner  # noqa: E402


def test_owner_job_naming_and_labels():
    o = WorkspaceOwner.job("abcdef0123456789")
    assert o.kind == "job"
    assert o.pod_name == "workspace-abcdef012345"  # 12-char id truncation
    assert o.label_key == "srw/job-id"
    assert o.component_label == "workspace"
    assert o.network_tier_kind == "job"


def test_owner_session_naming_and_labels():
    o = WorkspaceOwner.session("abcdef0123456789")
    assert o.pod_name == "ws-thread-abcdef012345"
    assert o.label_key == "srw/thread-id"
    assert o.component_label == "thread-workspace"
    assert o.network_tier_kind == "thread"


def test_owner_is_frozen_hashable():
    o = WorkspaceOwner.session("t1")
    with pytest.raises(Exception):
        o.id = "t2"  # frozen
    assert {o: 1}[WorkspaceOwner.session("t1")] == 1  # hashable by (kind, id)


# =============================================================================
# Tests for ensure_workspace (uses services.* path — conftest puts orchestrator/
# on sys.path, so this resolves without the orchestrator. prefix and avoids the
# dual-class-object problem that breaks assert_called_with equality checks).
# =============================================================================

import asyncio  # noqa: E402 (placed here to keep module-level imports above)
from unittest.mock import AsyncMock  # noqa: E402

from orchestrator.services.workspace_lifecycle import (  # noqa: E402
    EnsureOutcome,
    WorkspaceOwner as _WO,
    ensure_workspace,
)

CREATION_GENERATION = "33333333-3333-4333-8333-333333333333"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status,expect_create,outcome",
    [
        (None, True, EnsureOutcome.PENDING),
        ("", True, EnsureOutcome.PENDING),
        ("deleted", True, EnsureOutcome.PENDING),  # no live workspace -> recreate
        ("none", True, EnsureOutcome.PENDING),  # no live workspace -> recreate
        ("failed", True, EnsureOutcome.PENDING),  # SESSION self-heals
        ("created", False, EnsureOutcome.PENDING),  # transient in-progress
        ("creating", False, EnsureOutcome.PENDING),
        ("restoring", False, EnsureOutcome.PENDING),
        ("pending", True, EnsureOutcome.PENDING),
        ("ready", False, EnsureOutcome.READY),
        ("weird-unknown", False, EnsureOutcome.PENDING),  # truly unknown -> wait
    ],
)
async def test_ensure_session_transitions(status, expect_create, outcome):
    prov = AsyncMock()
    prov.create_workspace = AsyncMock(return_value=True)
    prov.create_pinned_thread_workspace = AsyncMock(return_value=True)
    susp = AsyncMock()
    susp.restore = AsyncMock(return_value=True)
    res = await ensure_workspace(
        _WO.session("t1"),
        provisioner=prov,
        suspension=susp,
        current_status=status,
    )
    assert res.outcome == outcome
    if expect_create:
        prov.create_pinned_thread_workspace.assert_awaited_once_with("t1")
    else:
        prov.create_pinned_thread_workspace.assert_not_awaited()
    prov.create_workspace.assert_not_awaited()


@pytest.mark.asyncio
async def test_ensure_job_failed_surfaces_FAILED_not_recreate():
    """Jobs must NOT auto-retry a failed workspace (preserves dispatcher fail-the-job)."""
    prov = AsyncMock()
    prov.create_workspace = AsyncMock(return_value=True)
    susp = AsyncMock()
    res = await ensure_workspace(
        _WO.job("j1"),
        provisioner=prov,
        suspension=susp,
        current_status="failed",
    )
    assert res.outcome == EnsureOutcome.FAILED
    prov.create_workspace.assert_not_called()


@pytest.mark.asyncio
async def test_ensure_suspended_kicks_off_restore_fire_and_forget():
    prov = AsyncMock()
    susp = AsyncMock()
    susp.restore = AsyncMock(return_value=True)
    owner = _WO.session("t1")
    res = await ensure_workspace(
        owner, provisioner=prov, suspension=susp, current_status="suspended"
    )
    assert res.outcome == EnsureOutcome.PENDING
    await asyncio.sleep(0)  # let the fire-and-forget restore task run
    susp.restore.assert_awaited_once_with(owner)
    prov.create_workspace.assert_not_called()


@pytest.mark.asyncio
async def test_ensure_suspended_job_also_fire_and_forget():
    """The suspended->restore branch is owner-agnostic; jobs restore too."""
    prov = AsyncMock()
    susp = AsyncMock()
    susp.restore = AsyncMock(return_value=True)
    owner = _WO.job("j1")
    res = await ensure_workspace(
        owner, provisioner=prov, suspension=susp, current_status="suspended"
    )
    assert res.outcome == EnsureOutcome.PENDING
    await asyncio.sleep(0)
    susp.restore.assert_awaited_once_with(owner)
    prov.create_workspace.assert_not_called()


@pytest.mark.asyncio
async def test_ensure_create_failure_returns_FAILED():
    prov = AsyncMock()
    prov.create_workspace = AsyncMock(return_value=False)
    prov.create_pinned_thread_workspace = AsyncMock(return_value=False)
    susp = AsyncMock()
    res = await ensure_workspace(
        _WO.session("t1"),
        provisioner=prov,
        suspension=susp,
        current_status=None,
    )
    assert res.outcome == EnsureOutcome.FAILED
    prov.create_pinned_thread_workspace.assert_awaited_once_with("t1")
    prov.create_workspace.assert_not_awaited()


# =============================================================================
# 'ready' drift check: a workspace marked ready whose pod is actually gone must
# be recreated (design "ready, pod missing → treat as failed → recreate"),
# while a probe that can't tell must NEVER false-recreate a healthy workspace.
# =============================================================================


@pytest.mark.asyncio
async def test_ensure_ready_recreates_when_pod_confirmed_dead():
    """ready + probe says the pod is gone/tombstone (False) → recreate."""
    prov = AsyncMock()
    prov.create_workspace = AsyncMock(return_value=True)
    prov.create_pinned_thread_workspace = AsyncMock(return_value=True)
    prov.workspace_pod_live = AsyncMock(return_value=False)
    susp = AsyncMock()
    owner = _WO.session("t1")
    res = await ensure_workspace(
        owner, provisioner=prov, suspension=susp, current_status="ready"
    )
    assert res.outcome == EnsureOutcome.PENDING  # _create → creating/PENDING
    prov.workspace_pod_live.assert_awaited_once_with(owner)
    prov.create_pinned_thread_workspace.assert_awaited_once_with("t1")
    prov.create_workspace.assert_not_awaited()


@pytest.mark.asyncio
async def test_required_ready_adopts_only_exact_live_runtime():
    prov = AsyncMock()
    prov.create_workspace = AsyncMock(return_value=True)
    prov.delete_workspace = AsyncMock(return_value=True)
    prov.workspace_pod_authority = AsyncMock(return_value="exact_live")
    owner = _WO.session("t1")
    runtime_uid = "11111111-1111-4111-8111-111111111111"

    res = await ensure_workspace(
        owner,
        provisioner=prov,
        suspension=AsyncMock(),
        current_status="ready",
        expected_runtime_incarnation=runtime_uid,
        require_runtime_incarnation=True,
    )

    assert res.outcome == EnsureOutcome.READY
    prov.workspace_pod_authority.assert_awaited_once_with(
        owner,
        expected_runtime_incarnation=runtime_uid,
    )
    prov.delete_workspace.assert_not_awaited()
    prov.create_workspace.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "authority", ["exact_absent", "replacement", "unknown", "malformed"]
)
async def test_required_cached_runtime_non_authoritative_states_hold_without_effects(
    authority,
):
    prov = AsyncMock()
    prov.create_workspace = AsyncMock(return_value=True)
    prov.delete_workspace = AsyncMock(return_value=True)
    prov.workspace_pod_authority = AsyncMock(return_value=authority)
    owner = _WO.session("t1")
    runtime_uid = "11111111-1111-4111-8111-111111111111"

    res = await ensure_workspace(
        owner,
        provisioner=prov,
        suspension=AsyncMock(),
        current_status="ready",
        expected_runtime_incarnation=runtime_uid,
        require_runtime_incarnation=True,
    )

    assert res.outcome == EnsureOutcome.PENDING
    assert res.status == "ready"
    prov.workspace_pod_authority.assert_awaited_once_with(
        owner,
        expected_runtime_incarnation=runtime_uid,
    )
    prov.delete_workspace.assert_not_awaited()
    prov.create_workspace.assert_not_awaited()


@pytest.mark.asyncio
async def test_required_cached_runtime_probe_exception_holds_without_effects():
    prov = AsyncMock()
    prov.create_workspace = AsyncMock(return_value=True)
    prov.delete_workspace = AsyncMock(return_value=True)
    prov.workspace_pod_authority = AsyncMock(side_effect=RuntimeError("api down"))

    res = await ensure_workspace(
        _WO.session("t1"),
        provisioner=prov,
        suspension=AsyncMock(),
        current_status="ready",
        expected_runtime_incarnation="11111111-1111-4111-8111-111111111111",
        require_runtime_incarnation=True,
    )

    assert res.outcome == EnsureOutcome.PENDING
    prov.delete_workspace.assert_not_awaited()
    prov.create_workspace.assert_not_awaited()


@pytest.mark.asyncio
async def test_required_exact_terminal_deletes_by_uid_before_recreate():
    effects = []

    async def authority(*_args, **_kwargs):
        effects.append("probe")
        return "exact_terminal"

    async def prepare(*_args, **_kwargs):
        effects.append("prepare")
        return CREATION_GENERATION

    async def create(*_args, **_kwargs):
        effects.append("create")
        return True

    prov = AsyncMock()
    prov.workspace_pod_authority = AsyncMock(side_effect=authority)
    prov.prepare_stateless_workspace_recreation = AsyncMock(side_effect=prepare)
    prov.create_workspace = AsyncMock(side_effect=create)
    owner = _WO.session("t1")
    runtime_uid = "11111111-1111-4111-8111-111111111111"

    res = await ensure_workspace(
        owner,
        provisioner=prov,
        suspension=AsyncMock(),
        current_status="ready",
        expected_runtime_incarnation=runtime_uid,
        require_runtime_incarnation=True,
    )

    assert res.outcome == EnsureOutcome.PENDING
    assert effects == ["probe", "prepare", "create"]
    prov.prepare_stateless_workspace_recreation.assert_awaited_once_with(
        owner,
        expected_runtime_incarnation=runtime_uid,
        mode="create",
    )
    prov.create_workspace.assert_awaited_once_with(
        owner,
        stateless_creation_generation=CREATION_GENERATION,
        allow_stateless_create=True,
    )


@pytest.mark.asyncio
async def test_required_exact_terminal_delete_conflict_never_creates():
    prov = AsyncMock()
    prov.workspace_pod_authority = AsyncMock(return_value="exact_terminal")
    prov.prepare_stateless_workspace_recreation = AsyncMock(return_value=None)
    prov.create_workspace = AsyncMock(return_value=True)

    res = await ensure_workspace(
        _WO.session("t1"),
        provisioner=prov,
        suspension=AsyncMock(),
        current_status="ready",
        expected_runtime_incarnation="11111111-1111-4111-8111-111111111111",
        require_runtime_incarnation=True,
    )

    assert res.outcome == EnsureOutcome.PENDING
    prov.create_workspace.assert_not_awaited()


@pytest.mark.asyncio
async def test_required_exact_live_nonready_holds_without_name_only_adoption():
    prov = AsyncMock()
    prov.workspace_pod_authority = AsyncMock(return_value="exact_live")
    prov.delete_workspace = AsyncMock(return_value=True)
    prov.create_workspace = AsyncMock(return_value=True)
    owner = _WO.session("t1")

    res = await ensure_workspace(
        owner,
        provisioner=prov,
        suspension=AsyncMock(),
        current_status="creating",
        expected_runtime_incarnation="11111111-1111-4111-8111-111111111111",
        require_runtime_incarnation=True,
    )

    assert res.outcome == EnsureOutcome.PENDING
    prov.delete_workspace.assert_not_awaited()
    prov.create_workspace.assert_not_awaited()


@pytest.mark.asyncio
async def test_required_exact_live_nonready_continues_by_uid_and_generation():
    prov = AsyncMock()
    prov.workspace_pod_authority = AsyncMock(return_value="exact_live")
    prov.continue_stateless_workspace_creation = AsyncMock(return_value=True)
    owner = _WO.session("t1")
    runtime_uid = "11111111-1111-4111-8111-111111111111"

    res = await ensure_workspace(
        owner,
        provisioner=prov,
        suspension=AsyncMock(),
        current_status="creating",
        expected_runtime_incarnation=runtime_uid,
        require_runtime_incarnation=True,
        stateless_creation_generation=CREATION_GENERATION,
    )

    assert res.outcome == EnsureOutcome.PENDING
    assert res.status == "creating"
    prov.continue_stateless_workspace_creation.assert_awaited_once_with(
        owner,
        generation=CREATION_GENERATION,
        expected_runtime_incarnation=runtime_uid,
    )


@pytest.mark.asyncio
async def test_required_missing_runtime_incarnation_rebinds_without_phase_probe():
    """A stateless legacy Ready row cannot be blessed by name/phase alone."""
    prov = AsyncMock()
    prov.create_workspace = AsyncMock(return_value=True)
    prov.workspace_pod_live = AsyncMock(return_value=True)
    owner = _WO.session("t1")

    res = await ensure_workspace(
        owner,
        provisioner=prov,
        suspension=AsyncMock(),
        current_status="ready",
        require_runtime_incarnation=True,
        stateless_creation_generation=CREATION_GENERATION,
        allow_stateless_create=True,
    )

    assert res.outcome == EnsureOutcome.PENDING
    prov.workspace_pod_live.assert_not_awaited()
    prov.create_workspace.assert_awaited_once_with(
        owner,
        stateless_creation_generation=CREATION_GENERATION,
        allow_stateless_create=True,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["created", "creating", "pending"])
async def test_required_runtime_reenters_interrupted_in_progress_create(status):
    """An abandoned readiness waiter must not strand a durable queued turn."""
    prov = AsyncMock()
    prov.create_workspace = AsyncMock(return_value=True)
    owner = _WO.session("t1")

    res = await ensure_workspace(
        owner,
        provisioner=prov,
        suspension=AsyncMock(),
        current_status=status,
        require_runtime_incarnation=True,
        stateless_creation_generation=CREATION_GENERATION,
        allow_stateless_create=True,
    )

    assert res.outcome == EnsureOutcome.PENDING
    prov.create_workspace.assert_awaited_once_with(
        owner,
        stateless_creation_generation=CREATION_GENERATION,
        allow_stateless_create=True,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["suspended", "restoring"])
async def test_required_runtime_phase_without_restore_marker_holds(status):
    """A phase string alone cannot authorize snapshot extraction."""
    owner = _WO.session("t1")
    susp = AsyncMock()
    susp.restore = AsyncMock(return_value=True)

    res = await ensure_workspace(
        owner,
        provisioner=AsyncMock(),
        suspension=susp,
        current_status=status,
        require_runtime_incarnation=True,
    )

    assert res.outcome == EnsureOutcome.PENDING
    assert res.status == status
    susp.restore.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [None, "", "deleted", "failed", "created", "ready"])
async def test_required_runtime_failed_restore_retries_snapshot_not_plain_create(
    status,
):
    """A failed extract must not publish an empty workspace on the next poll."""
    owner = _WO.session("t1")
    prov = AsyncMock()
    susp = AsyncMock()
    susp.restore = AsyncMock(return_value=False)

    res = await ensure_workspace(
        owner,
        provisioner=prov,
        suspension=susp,
        current_status=status,
        require_runtime_incarnation=True,
        snapshot_restore_required=True,
        stateless_creation_generation=CREATION_GENERATION,
        allow_stateless_create=True,
    )

    assert res.outcome == EnsureOutcome.PENDING
    assert res.status == "failed"
    susp.restore.assert_awaited_once_with(
        owner,
        stateless_creation_generation=CREATION_GENERATION,
        allow_stateless_create=True,
    )
    prov.create_workspace.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("authority", "expected_effects", "expected_status"),
    [
        ("exact_live", ["probe", "continue"], "creating"),
        ("exact_terminal", ["probe", "prepare", "restore_new"], "restoring"),
        ("exact_absent", ["probe"], "deleted"),
        ("replacement", ["probe"], "deleted"),
        ("unknown", ["probe"], "deleted"),
        ("malformed", ["probe"], "deleted"),
    ],
)
async def test_durable_restore_intent_obeys_cached_runtime_authority(
    authority,
    expected_effects,
    expected_status,
):
    owner = _WO.session("t1")
    runtime_uid = "11111111-1111-4111-8111-111111111111"
    effects = []

    async def probe(*_args, **_kwargs):
        effects.append("probe")
        return authority

    async def prepare(*_args, **_kwargs):
        effects.append("prepare")
        return CREATION_GENERATION

    async def continuation(*_args, **_kwargs):
        effects.append("continue")
        return True

    async def restore(*_args, **kwargs):
        effects.append(
            "restore_exact"
            if kwargs.get("expected_runtime_incarnation") == runtime_uid
            else "restore_new"
        )
        return True

    prov = AsyncMock()
    prov.workspace_pod_authority = AsyncMock(side_effect=probe)
    prov.prepare_stateless_workspace_recreation = AsyncMock(side_effect=prepare)
    prov.continue_stateless_workspace_creation = AsyncMock(side_effect=continuation)
    prov.create_workspace = AsyncMock(return_value=True)
    susp = AsyncMock()
    susp.restore = AsyncMock(side_effect=restore)

    res = await ensure_workspace(
        owner,
        provisioner=prov,
        suspension=susp,
        current_status="deleted",
        expected_runtime_incarnation=runtime_uid,
        require_runtime_incarnation=True,
        snapshot_restore_required=True,
        stateless_creation_generation=CREATION_GENERATION,
        allow_stateless_create=True,
    )

    assert res.outcome == EnsureOutcome.PENDING
    assert res.status == expected_status
    assert effects == expected_effects
    prov.workspace_pod_authority.assert_awaited_once_with(
        owner,
        expected_runtime_incarnation=runtime_uid,
    )
    if authority == "exact_live":
        prov.continue_stateless_workspace_creation.assert_awaited_once_with(
            owner,
            generation=CREATION_GENERATION,
            expected_runtime_incarnation=runtime_uid,
        )
        susp.restore.assert_not_awaited()
        prov.prepare_stateless_workspace_recreation.assert_not_awaited()
    elif authority == "exact_terminal":
        prov.prepare_stateless_workspace_recreation.assert_awaited_once_with(
            owner,
            expected_runtime_incarnation=runtime_uid,
            mode="restore",
        )
        susp.restore.assert_awaited_once_with(
            owner,
            stateless_creation_generation=CREATION_GENERATION,
            allow_stateless_create=True,
        )
    else:
        prov.prepare_stateless_workspace_recreation.assert_not_awaited()
        susp.restore.assert_not_awaited()
    prov.create_workspace.assert_not_awaited()


@pytest.mark.asyncio
async def test_durable_restore_probe_exception_holds_without_effects():
    owner = _WO.session("t1")
    prov = AsyncMock()
    prov.workspace_pod_authority = AsyncMock(side_effect=RuntimeError("api down"))
    prov.delete_workspace = AsyncMock(return_value=True)
    susp = AsyncMock()
    susp.restore = AsyncMock(return_value=True)

    res = await ensure_workspace(
        owner,
        provisioner=prov,
        suspension=susp,
        current_status="deleted",
        expected_runtime_incarnation="11111111-1111-4111-8111-111111111111",
        require_runtime_incarnation=True,
        snapshot_restore_required=True,
    )

    assert res.outcome == EnsureOutcome.PENDING
    assert res.status == "deleted"
    prov.delete_workspace.assert_not_awaited()
    susp.restore.assert_not_awaited()


@pytest.mark.asyncio
async def test_durable_restore_terminal_delete_conflict_holds_without_restore():
    owner = _WO.session("t1")
    prov = AsyncMock()
    prov.workspace_pod_authority = AsyncMock(return_value="exact_terminal")
    prov.prepare_stateless_workspace_recreation = AsyncMock(return_value=None)
    susp = AsyncMock()
    susp.restore = AsyncMock(return_value=True)

    res = await ensure_workspace(
        owner,
        provisioner=prov,
        suspension=susp,
        current_status="deleted",
        expected_runtime_incarnation="11111111-1111-4111-8111-111111111111",
        require_runtime_incarnation=True,
        snapshot_restore_required=True,
        stateless_creation_generation=CREATION_GENERATION,
        allow_stateless_create=True,
    )

    assert res.outcome == EnsureOutcome.PENDING
    assert res.status == "deleted"
    susp.restore.assert_not_awaited()


@pytest.mark.asyncio
async def test_durable_restore_without_cached_runtime_creates_restore_target():
    owner = _WO.session("t1")
    prov = AsyncMock()
    prov.workspace_pod_authority = AsyncMock(return_value="replacement")
    susp = AsyncMock()
    susp.restore = AsyncMock(return_value=True)

    res = await ensure_workspace(
        owner,
        provisioner=prov,
        suspension=susp,
        current_status="deleted",
        require_runtime_incarnation=True,
        snapshot_restore_required=True,
        stateless_creation_generation=CREATION_GENERATION,
        allow_stateless_create=True,
    )

    assert res.outcome == EnsureOutcome.PENDING
    assert res.status == "restoring"
    susp.restore.assert_awaited_once_with(
        owner,
        stateless_creation_generation=CREATION_GENERATION,
        allow_stateless_create=True,
    )
    prov.workspace_pod_authority.assert_not_awaited()


@pytest.mark.asyncio
async def test_non_boolean_restore_intent_cannot_bypass_cached_runtime_authority():
    owner = _WO.session("t1")
    prov = AsyncMock()
    prov.workspace_pod_authority = AsyncMock(return_value="replacement")
    prov.delete_workspace = AsyncMock(return_value=True)
    prov.create_workspace = AsyncMock(return_value=True)
    susp = AsyncMock()
    susp.restore = AsyncMock(return_value=True)

    res = await ensure_workspace(
        owner,
        provisioner=prov,
        suspension=susp,
        current_status="deleted",
        expected_runtime_incarnation="11111111-1111-4111-8111-111111111111",
        require_runtime_incarnation=True,
        snapshot_restore_required=1,
    )

    assert res.outcome == EnsureOutcome.PENDING
    prov.workspace_pod_authority.assert_awaited_once()
    susp.restore.assert_not_awaited()
    prov.delete_workspace.assert_not_awaited()
    prov.create_workspace.assert_not_awaited()


@pytest.mark.asyncio
async def test_ensure_ready_no_recreate_when_pod_live():
    """ready + probe says the pod is live (True) → READY, no recreate."""
    prov = AsyncMock()
    prov.create_workspace = AsyncMock(return_value=True)
    prov.workspace_pod_live = AsyncMock(return_value=True)
    susp = AsyncMock()
    res = await ensure_workspace(
        _WO.session("t1"), provisioner=prov, suspension=susp, current_status="ready"
    )
    assert res.outcome == EnsureOutcome.READY
    prov.create_workspace.assert_not_called()


@pytest.mark.asyncio
async def test_ensure_ready_trusts_db_when_probe_unknown():
    """ready + probe can't tell (None: non-k8s / transient) → READY, no
    recreate — never false-recreate a healthy workspace on a probe blip."""
    prov = AsyncMock()
    prov.create_workspace = AsyncMock(return_value=True)
    prov.workspace_pod_live = AsyncMock(return_value=None)
    susp = AsyncMock()
    res = await ensure_workspace(
        _WO.session("t1"), provisioner=prov, suspension=susp, current_status="ready"
    )
    assert res.outcome == EnsureOutcome.READY
    prov.create_workspace.assert_not_called()


@pytest.mark.asyncio
async def test_ensure_ready_trusts_db_when_provisioner_has_no_probe():
    """A provisioner without workspace_pod_live (e.g. a non-container backend)
    keeps the original behavior: ready → READY, no probe, no recreate."""
    prov = AsyncMock(spec=["create_workspace"])
    prov.create_workspace = AsyncMock(return_value=True)
    susp = AsyncMock()
    res = await ensure_workspace(
        _WO.session("t1"), provisioner=prov, suspension=susp, current_status="ready"
    )
    assert res.outcome == EnsureOutcome.READY
    prov.create_workspace.assert_not_called()


# =============================================================================
# D3: only a caller that fails the Job on FAILED (the dispatcher) opts a Job
# create in to failing on an exited container; every other creator keeps the
# legacy readiness timeout, which settles the creation.
# =============================================================================


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("current_status", "pod_live"),
    [(None, None), ("deleted", None), ("ready", False)],
    ids=["fresh", "deleted", "ready-drift"],
)
@pytest.mark.parametrize("opted_in", [True, False], ids=["dispatcher", "other"])
async def test_only_an_opted_in_job_create_is_asked_to_fail_on_an_exit(
    current_status, pod_live, opted_in
):
    prov = AsyncMock()
    prov.create_workspace = AsyncMock(return_value=True)
    prov.workspace_pod_live = AsyncMock(return_value=pod_live)
    owner = _WO.job("j1")

    res = await ensure_workspace(
        owner,
        provisioner=prov,
        suspension=AsyncMock(),
        current_status=current_status,
        **({"fail_on_exited_container": True} if opted_in else {}),
    )

    assert res.outcome == EnsureOutcome.PENDING
    if opted_in:
        prov.create_workspace.assert_awaited_once_with(
            owner, fail_on_exited_container=True
        )
    else:
        prov.create_workspace.assert_awaited_once_with(owner)


@pytest.mark.asyncio
async def test_a_session_create_never_forwards_the_exit_opt_in():
    prov = AsyncMock()
    prov.create_workspace = AsyncMock(return_value=True)
    prov.create_pinned_thread_workspace = AsyncMock(return_value=True)

    await ensure_workspace(
        _WO.session("t1"),
        provisioner=prov,
        suspension=AsyncMock(),
        current_status=None,
        fail_on_exited_container=True,
    )
    await ensure_workspace(
        _WO.session("t2"),
        provisioner=prov,
        suspension=AsyncMock(),
        current_status=None,
        stateless_creation_generation=CREATION_GENERATION,
        allow_stateless_create=True,
        fail_on_exited_container=True,
    )

    prov.create_pinned_thread_workspace.assert_awaited_once_with("t1")
    prov.create_workspace.assert_awaited_once_with(
        _WO.session("t2"),
        stateless_creation_generation=CREATION_GENERATION,
        allow_stateless_create=True,
    )
