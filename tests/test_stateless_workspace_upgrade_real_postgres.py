"""Stateless workspace upgrade (C1) against the production schema snapshot.

The transition is one UPDATE on a lite stateless row; every production trigger
stays enabled. The create path is the real one, with only Kubernetes faked.
"""

from __future__ import annotations

import json
import os
from uuid import uuid4

import pytest

from orchestrator.services.manifest_workspace_selection import (
    select_execution_workspace,
)
from orchestrator.services.stateless_workspace_gate import stateless_workspace_check
from shared.workspace_defaults import ENV_NAME as WORKSPACE_DEFAULTS
from tests import test_stateless_workspace_continuation_real_postgres as continuation

actor = continuation.actor
database = continuation.database
postgres_url = continuation.postgres_url
IMAGE = continuation.IMAGE
VIRTUAL_BACKING = "rclone:" + "a" * 64


def metadata(thread):
    value = thread["metadata"]
    return json.loads(value) if isinstance(value, str) else dict(value)


async def _none_session_workspace(database, actor):
    # The manifest WorkspaceSpec has no "none" backend, so a None Session
    # comes from the defaults chain, as on an installation that defaults
    # Sessions to none.
    previous = os.environ.get(WORKSPACE_DEFAULTS)
    os.environ[WORKSPACE_DEFAULTS] = json.dumps({"sessions": "none"})
    try:
        workspace, selection = await select_execution_workspace(
            database, actor, role="session", project_id=None
        )
    finally:
        if previous is None:
            os.environ.pop(WORKSPACE_DEFAULTS, None)
        else:
            os.environ[WORKSPACE_DEFAULTS] = previous
    assert workspace == {"backend": "none"}, workspace
    return workspace, selection


async def lite_session(database, actor, *, backend="virtual", bind=True):
    if backend == "none":
        workspace, selection = await _none_session_workspace(database, actor)
    else:
        workspace, selection = await select_execution_workspace(
            database,
            actor,
            role="session",
            project_id=None,
            supplied=True,
            workspace={"template": {"inline": {"backend": backend}}},
        )
    thread_id = await database.create_thread(
        user_id=str(actor["id"]),
        datasource_ids=[],
        execution_lane="stateless",
        initial_metadata={"config_override": {"workspace": workspace}},
        workspace_selection=selection,
    )
    if bind and backend == "virtual":
        await database.bind_thread_workspace_backing(
            thread_id, backing_kind="virtual", backing_id=VIRTUAL_BACKING
        )
    return thread_id


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "shape", ["virtual_bound", "virtual_unbound", "none", "empty_container"]
)
async def test_the_guards_admit_the_upgrade_envelope(database, actor, shape):
    """Probe B (spec "One transition"): with the runtime generation kept, the
    0198/0223 guards (as patched by 0332) admit the container envelope."""
    backend = "none" if shape == "none" else "virtual"
    thread_id = await lite_session(
        database, actor, backend=backend, bind=shape == "virtual_bound"
    )
    if shape == "empty_container":
        await database.execute(
            "UPDATE threads SET metadata = jsonb_set(metadata, "
            "'{workspace_container}', '{}'::jsonb) WHERE id = $1::uuid",
            thread_id,
        )
    row = await database.get_thread(thread_id)
    generation = str(row["runtime_generation"])
    next_metadata = metadata(row)
    binding = next_metadata.pop("_workspace_binding", None)
    next_metadata["config_override"]["workspace"]["backend"] = "sandbox"
    next_metadata["workspace_container"] = {
        "status": "pending",
        "provisioner": "k8s",
        "_runtime_creation": {
            "generation": generation,
            "mode": "create",
            "attempted": False,
            "replaces_uid": None,
        },
        "upgrade_config": {"image": IMAGE},
        "upgrade_sources": None,
        "seed_from": {
            "backend": backend,
            "prefix": f"threads/{thread_id}/",
            "binding": binding,
        },
    }

    await database.execute(
        "UPDATE threads SET metadata = $2::jsonb WHERE id = $1::uuid",
        thread_id,
        json.dumps(next_metadata),
    )

    after = await database.get_thread(thread_id)
    assert str(after["runtime_generation"]) == generation
    assert stateless_workspace_check(after) == ("sandbox", None)


from orchestrator.database.stateless_workspace_upgrade import (  # noqa: E402
    UPGRADE_NOT_POSSIBLE,
    StatelessUpgradeRefused,
    begin_stateless_session_workspace_upgrade,
)

UPGRADE_CONFIG = {"image": IMAGE, "cpu": 1, "memory": "3Gi", "storage": "15Gi"}
UPGRADE_SOURCES = {
    "tier": "explicit",
    "template": "explicit",
    "template_name": "container-minimal",
}


async def transition(database, thread_id, **row_overrides):
    async with database.acquire() as conn:
        async with conn.transaction():
            row = dict(
                await conn.fetchrow(
                    "SELECT id, status, execution_lane, metadata, runtime_generation "
                    "FROM threads WHERE id = $1::uuid FOR UPDATE",
                    thread_id,
                )
            )
            row.update(row_overrides)
            return await begin_stateless_session_workspace_upgrade(
                conn,
                row,
                upgrade_config=UPGRADE_CONFIG,
                upgrade_sources=UPGRADE_SOURCES,
            )


@pytest.mark.asyncio
async def test_transition_turns_a_lite_session_into_a_pending_container_session(
    database, actor
):
    thread_id = await lite_session(database, actor)
    before = await database.get_thread(thread_id)
    binding = metadata(before)["_workspace_binding"]

    await transition(database, thread_id)

    after = await database.get_thread(thread_id)
    meta = metadata(after)
    generation = str(before["runtime_generation"])
    assert str(after["runtime_generation"]) == generation
    assert meta["config_override"]["workspace"]["backend"] == "sandbox"
    assert "_workspace_binding" not in meta
    assert meta["workspace_container"] == {
        "status": "pending",
        "provisioner": "k8s",
        "_runtime_creation": {
            "generation": generation,
            "mode": "create",
            "attempted": False,
            "replaces_uid": None,
        },
        "upgrade_config": UPGRADE_CONFIG,
        "upgrade_sources": UPGRADE_SOURCES,
        "seed_from": {
            "backend": "virtual",
            "prefix": f"threads/{thread_id}/",
            "binding": binding,
        },
    }
    assert stateless_workspace_check(after) == ("sandbox", None)


@pytest.mark.asyncio
async def test_a_none_session_records_a_none_source(database, actor):
    thread_id = await lite_session(database, actor, backend="none")
    await transition(database, thread_id)
    seed = metadata(await database.get_thread(thread_id))["workspace_container"][
        "seed_from"
    ]
    assert seed == {
        "backend": "none",
        "prefix": f"threads/{thread_id}/",
        "binding": None,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        "sandbox_backend",
        "vm_context",
        "stop_marker",
        "foreign_container_keys",
        "stale_generation",
    ],
)
async def test_transition_refuses_rows_it_cannot_upgrade(database, actor, change):
    thread_id = await lite_session(database, actor)
    overrides = {}
    if change == "stale_generation":
        overrides["runtime_generation"] = str(uuid4())
    else:
        patch = {
            "sandbox_backend": (
                "jsonb_set(metadata, '{config_override,workspace,backend}', "
                "'\"sandbox\"'::jsonb)"
            ),
            "vm_context": 'metadata || \'{"vm": {"status": "failed"}}\'::jsonb',
            "stop_marker": (
                "metadata || '{\"_stateless_claim_retirement\": {}}'::jsonb"
            ),
            "foreign_container_keys": (
                "jsonb_set(metadata, '{workspace_container}', "
                '\'{"repo_name": "thread-x"}\'::jsonb)'
            ),
        }[change]
        overrides["metadata"] = await database.fetchval(
            f"SELECT {patch} FROM threads WHERE id = $1::uuid", thread_id
        )
    before = await database.get_thread(thread_id)

    with pytest.raises(StatelessUpgradeRefused) as refused:
        await transition(database, thread_id, **overrides)

    assert str(refused.value) == UPGRADE_NOT_POSSIBLE
    assert await database.get_thread(thread_id) == before


from orchestrator.services.thread_control_inbox import (  # noqa: E402
    ControlAdmissionError,
    ControlAdmissionNotReady,
    admit_thread_control,
)
from shared.run_queue import (  # noqa: E402
    UNIT_KIND_SESSION_TURN,
    claim_unit,
    complete_unit,
    record_input_seq,
)

UPGRADE_RECORD = {"upgrade_config": UPGRADE_CONFIG, "upgrade_sources": UPGRADE_SOURCES}


async def admit_upgrade(database, actor, thread_id, *, client_request_id=None):
    thread = await database.get_thread(thread_id)
    generation = str(thread["runtime_generation"])
    return await admit_thread_control(
        database,
        thread_id=thread_id,
        owner_user_id=actor["id"],
        client_request_id=client_request_id or uuid4(),
        verb="upgrade-to-workspace",
        payload={"runtime_generation": generation, "target_tier": "sandbox"},
        requested_by="real-pg",
        expected_runtime_generation=generation,
        workspace_upgrade=UPGRADE_RECORD,
    )


@pytest.mark.asyncio
async def test_admission_commits_the_transition_the_request_and_the_wake(
    database, actor
):
    thread_id = await lite_session(database, actor)
    request_id = uuid4()

    admitted = await admit_upgrade(
        database, actor, thread_id, client_request_id=request_id
    )

    meta = metadata(await database.get_thread(thread_id))
    assert meta["workspace_container"]["status"] == "pending"
    assert "seed_from" in meta["workspace_container"]
    request = await database.fetchrow(
        "SELECT verb, payload, outcome FROM thread_control_requests WHERE id = $1",
        admitted.id,
    )
    assert request["verb"] == "upgrade-to-workspace" and request["outcome"] is None
    queue = await database.fetchrow(
        "SELECT state, control_input_seq FROM run_queue WHERE unit_id = $1::uuid",
        thread_id,
    )
    assert queue["state"] == "queued"
    assert int(queue["control_input_seq"]) == admitted.request_seq

    replay = await admit_upgrade(
        database, actor, thread_id, client_request_id=request_id
    )
    assert replay.duplicate is True
    assert metadata(await database.get_thread(thread_id)) == meta


@pytest.mark.asyncio
async def test_input_committed_first_defers_the_upgrade(database, actor):
    """Review Focus 1."""
    thread_id = await lite_session(database, actor)
    async with database.acquire() as conn:
        await record_input_seq(
            conn, unit_id=thread_id, unit_kind=UNIT_KIND_SESSION_TURN, input_seq=5
        )
    before = await database.get_thread(thread_id)

    with pytest.raises(ControlAdmissionNotReady, match="pending input"):
        await admit_upgrade(database, actor, thread_id)

    assert await database.get_thread(thread_id) == before
    assert not await database.fetchval(
        "SELECT count(*) FROM thread_control_requests WHERE thread_id = $1::uuid",
        thread_id,
    )


@pytest.mark.asyncio
async def test_a_leased_turn_defers_the_upgrade(database, actor):
    thread_id = await lite_session(database, actor)
    async with database.acquire() as conn:
        await record_input_seq(
            conn, unit_id=thread_id, unit_kind=UNIT_KIND_SESSION_TURN, input_seq=5
        )
        assert await claim_unit(conn, unit_kind=UNIT_KIND_SESSION_TURN, pod_name="p-1")

    with pytest.raises(ControlAdmissionNotReady, match="completing a turn"):
        await admit_upgrade(database, actor, thread_id)


@pytest.mark.asyncio
async def test_input_after_the_transition_keeps_a_valid_container_row(database, actor):
    """Review Focus 1: a message after the upgrade waits for the claim that
    attaches the container; the row stays admissible at every boundary."""
    thread_id = await lite_session(database, actor)
    await admit_upgrade(database, actor, thread_id)
    async with database.acquire() as conn:
        state = await record_input_seq(
            conn, unit_id=thread_id, unit_kind=UNIT_KIND_SESSION_TURN, input_seq=9
        )
    assert state == "queued"
    assert stateless_workspace_check(await database.get_thread(thread_id)) == (
        "sandbox",
        None,
    )


@pytest.mark.asyncio
async def test_end_first_refuses_the_upgrade(database, actor):
    thread_id = await lite_session(database, actor)
    begun = await database.begin_stateless_thread_workspace_retirement(
        thread_id, force=False, permanent=False
    )
    assert begun["state"] != "busy"

    # End committed first: the row is ended, so admission refuses before
    # the transition (no request, no wake).
    with pytest.raises(ControlAdmissionError, match="not currently able"):
        await admit_upgrade(database, actor, thread_id)


from datetime import datetime, timezone  # noqa: E402
from types import SimpleNamespace  # noqa: E402
from unittest.mock import AsyncMock, MagicMock  # noqa: E402

from orchestrator.database.container_startup_stage import (  # noqa: E402
    ScheduledAt,
    StageBudgets,
    StartupAttention,
    Unscheduled,
)
from orchestrator.services import container_provisioner as provisioner_module  # noqa: E402
from orchestrator.services.container_provisioner import ContainerProvisioner  # noqa: E402
from orchestrator.services.session_provisioner import ensure_session_workspace  # noqa: E402
from orchestrator.services.stateless_workspace_upgrade import (  # noqa: E402
    workspace_upgrade_view,
)
from orchestrator.services.workspace_lifecycle import EnsureOutcome  # noqa: E402
from tests import test_stateless_failed_start_end_real_postgres as lifecycle  # noqa: E402
from tests.test_sandbox_workspace_provisioner import stub_plan_inputs  # noqa: E402


def upgrade_provisioner(database, monkeypatch):
    """The continuation suite's provisioner setup (only Kubernetes and SSH fake)."""
    cluster = continuation.DelayedWorkspaceCluster()
    provisioner = ContainerProvisioner()
    provisioner._db = database
    provisioner._k8s_available = True
    provisioner._namespace = "agent-workspaces"
    provisioner._storage_class = "test-storage"
    provisioner._pvc_enabled = True
    provisioner._core_api = cluster
    stub_plan_inputs(monkeypatch, provisioner)
    for name in ("open_interval", "close_interval"):
        monkeypatch.setattr(
            provisioner_module.workspace_metering, name, AsyncMock(return_value=None)
        )
    monkeypatch.setattr(
        provisioner_module,
        "wait_for_agent_ssh",
        AsyncMock(return_value=(True, 1, None)),
    )
    monkeypatch.setattr(
        provisioner_module,
        "workspace_private_key_fingerprint",
        lambda _: continuation.FINGERPRINT,
    )
    monkeypatch.setattr(
        provisioner_module,
        "_isolated_pod_exec",
        lambda *a, **k: f"256 {continuation.FINGERPRINT} workspace (ED25519)",
    )
    return provisioner, cluster


async def complete_a_lite_turn(database, thread_id):
    """One lite turn through the queue: its input, claim and completion."""
    async with database.acquire() as conn:
        await record_input_seq(
            conn, unit_id=thread_id, unit_kind=UNIT_KIND_SESSION_TURN, input_seq=1
        )
        claim = await claim_unit(conn, unit_kind=UNIT_KIND_SESSION_TURN, pod_name="p-1")
        assert claim is not None and str(claim.unit_id) == str(thread_id)
        assert (
            await complete_unit(
                conn,
                unit_id=thread_id,
                lease_token=claim.lease_token,
                consumed_seq=claim.input_seq,
            )
            == "done"
        )


async def upgraded_create(
    database, actor, monkeypatch, *, first_wait, bind=True, earlier_turn=None
):
    """``earlier_turn(thread_id)`` runs on the lite Session before the upgrade."""
    thread_id = await lite_session(database, actor, bind=bind)
    if earlier_turn is not None:
        await earlier_turn(thread_id)
    await admit_upgrade(database, actor, thread_id)
    provisioner, cluster = upgrade_provisioner(database, monkeypatch)
    suspension = SimpleNamespace()
    with monkeypatch.context() as waiter:
        if first_wait == "not_ready":
            waiter.setattr(provisioner, "_wait_for_ready", AsyncMock(return_value=None))
        else:
            wait_for_ready = provisioner._wait_for_ready

            async def ready_on_first_wait(*args, **kwargs):
                cluster.become_ready()
                return await wait_for_ready(*args, **kwargs)

            waiter.setattr(provisioner, "_wait_for_ready", ready_on_first_wait)
        first = await ensure_session_workspace(
            thread_id, db=database, provisioner=provisioner, suspension=suspension
        )
    return SimpleNamespace(
        thread_id=thread_id,
        first=first,
        provisioner=provisioner,
        cluster=cluster,
        suspension=suspension,
        pod_uid=cluster.objects["pod"].metadata.uid,
        pvc_uid=cluster.objects["pvc"].metadata.uid,
    )


@pytest.mark.asyncio
async def test_upgrade_create_uses_the_template_and_keeps_the_seed_marker_through_ready(
    database, actor, monkeypatch
):
    case = await upgraded_create(database, actor, monkeypatch, first_wait="ready")

    # Like the continuation suite's ready case: the first ensure reports the
    # create it started; the row and the receipt show Ready.
    assert case.first.outcome == EnsureOutcome.PENDING
    assert case.cluster.objects["pod"].spec.containers[0]["image"] == IMAGE
    thread = await database.get_thread(case.thread_id)
    meta = metadata(thread)
    workspace = meta["workspace_container"]
    assert workspace["status"] == "ready"
    assert "_runtime_creation" not in workspace
    assert meta["_workspace_binding"]["kind"] == "remote"
    assert workspace["seed_from"]["backend"] == "virtual"
    assert workspace["upgrade_config"] == UPGRADE_CONFIG
    receipt = await continuation.reservation(database, case.thread_id)
    assert receipt["phase"] == "settled"
    assert workspace_upgrade_view(thread, None) == {
        "target_tier": "sandbox",
        "phase": "copying",
    }


@pytest.mark.asyncio
async def test_continuation_runner_finds_the_upgrade_create_on_the_initial_route(
    database, actor, monkeypatch
):
    """Review Focus 3: a restart mid-create resumes through `initial`; the
    binding moved under seed_from doesn't hold it."""
    case = await upgraded_create(database, actor, monkeypatch, first_wait="not_ready")
    assert case.first.outcome == EnsureOutcome.PENDING

    page = await database.list_current_session_creation_candidates()

    routes = {
        str(candidate.cursor.thread_id): candidate.route
        for candidate in page.candidates
    }
    assert routes.get(str(case.thread_id)) == "initial"


@pytest.mark.asyncio
async def test_an_upgrade_create_in_startup_attention_projects_a_failed_upgrade(
    database, actor, monkeypatch
):
    monkeypatch.setenv("CONTAINER_STARTUP_STAGE_AUTHORITY_ENABLED", "true")
    case = await upgraded_create(database, actor, monkeypatch, first_wait="not_ready")
    receipt = await continuation.reservation(database, case.thread_id)
    observe = dict(
        owner_kind="thread",
        owner_id=case.thread_id,
        reservation_id=str(receipt["id"]),
        claim_token=int(receipt["claim_token"]),
        pod_uid=case.pod_uid,
    )
    assert await database.observe_container_startup(
        **observe, observation=Unscheduled("scheduling_other"), adopt_if_unmarked=True
    )
    assert await database.observe_container_startup(
        **observe,
        observation=ScheduledAt(datetime.now(timezone.utc)),
        budgets=StageBudgets(ready_seconds=180, pull_seconds=None, ssh_seconds=30),
    )
    assert await database.observe_container_startup(
        **observe, observation=StartupAttention("invalid_image")
    )

    receipt = await continuation.reservation(database, case.thread_id)
    assert receipt["startup_protocol_version"] == 1
    views = await database.container_workspace_creation_views(
        "thread", [case.thread_id]
    )
    view = next(iter(views.values()))
    assert (view["state"], view["reason_code"]) == ("attention", "invalid_image")
    assert workspace_upgrade_view(await database.get_thread(case.thread_id), view) == {
        "target_tier": "sandbox",
        "phase": "failed",
        "reason_code": "invalid_image",
    }


from fastapi import HTTPException  # noqa: E402

from agent.api import turn_executor as executor_module  # noqa: E402
from orchestrator.services import (  # noqa: E402
    dispatch_credentials,
    session_attach_payload,
    unit_claim_bundle,
)
from shared.run_queue import (  # noqa: E402
    PARK_REASON_RETRY_EXHAUSTED,
    transient_release_backoff_seconds,
)
from shared.session_retirement import update_stateless_claim_status  # noqa: E402
from shared.workspace_upgrade import WORKSPACE_UPGRADE_SEED_CAPABILITY  # noqa: E402

CLAIMANT_POD = "srw-agent-stateless-c1"
CLAIMANT_POD_UID = "11111111-2222-4333-8444-555555555555"
OBJECT_STORE = {"type": "s3", "root": "srw-workspaces", "config": {"endpoint": "e"}}


class PastTheUpgradeGate(Exception):
    """The first collaborator after the upgrade gate ran."""


async def claim_now(database, thread_id):
    """The next claim of this Session's unit, as an executor makes it."""
    async with database.acquire() as conn:
        await conn.execute(
            "UPDATE run_queue SET run_after = now() WHERE unit_id = $1::uuid",
            thread_id,
        )
        claim = await claim_unit(
            conn,
            unit_kind=UNIT_KIND_SESSION_TURN,
            pod_name=CLAIMANT_POD,
            affinity_grace_seconds=0.0,
        )
    assert claim is None or str(claim.unit_id) == str(thread_id)
    return claim


#: An attach the assembly can serve a lite Session with.
LITE_ATTACH = {"datasources": []}


def upgrade_claimant(database, monkeypatch, *, attach=None):
    """A claimant that can seed an upgrade: the real executor release and claim
    bundle, with only the credential injection and the attach assembly faked.
    ``attach`` is what the assembly returns."""
    monkeypatch.setattr(
        "orchestrator.services.virtual_workspace.virtual_workspace_rclone_spec",
        lambda: OBJECT_STORE,
    )
    inject = AsyncMock(side_effect=lambda co, **_: co)
    monkeypatch.setattr(
        dispatch_credentials, "inject_thread_dispatch_credentials", inject
    )
    # The live refusal before Ready: the workspace identity pair is incomplete.
    assembly = AsyncMock(return_value=attach)
    monkeypatch.setattr(
        session_attach_payload, "assemble_session_attach_payload", assembly
    )
    dependencies = SimpleNamespace(
        db=database,
        recovery_store=None,
        thread_project_ids=AsyncMock(return_value=[]),
        thread_has_knowledge_scope=AsyncMock(return_value=False),
        dispatch_credential_dependencies=lambda: None,
        session_attach_payload_dependencies=lambda: None,
        session_subagent_fanout=lambda _lane: False,
    )
    executor = executor_module.StatelessTurnExecutor(
        pod_name=CLAIMANT_POD, pod_uid=CLAIMANT_POD_UID
    )
    monkeypatch.setattr(
        executor_module.StatelessTurnExecutor, "_db", property(lambda _: database)
    )
    monkeypatch.setattr(executor_module, "_pa", lambda: MagicMock())
    monkeypatch.setattr(executor, "_quiesce_claim_before_transition", AsyncMock())
    monkeypatch.setattr(executor, "_ack_terminal_claim_loss", AsyncMock())
    monkeypatch.setattr(executor, "_clear_claim_tool_effect", MagicMock())

    async def bundle(claim):
        return await unit_claim_bundle.claim_bundle_for_unit(
            str(claim.unit_id),
            lease_token=claim.lease_token,
            pod_name=CLAIMANT_POD,
            pod_uid=CLAIMANT_POD_UID,
            dependencies=dependencies,
            claimant_capabilities=frozenset({WORKSPACE_UPGRADE_SEED_CAPABILITY}),
        )

    return SimpleNamespace(
        executor=executor, bundle=bundle, inject=inject, assembly=assembly
    )


async def back_off_before_ready(database, claimant, thread_id, *, claims):
    """``claims`` pre-Ready claims, each refused (425) and released as the
    executor releases a bundle refusal."""
    for _ in range(claims):
        claim = await claim_now(database, thread_id)
        assert claim is not None
        with pytest.raises(HTTPException) as refused:
            await claimant.bundle(claim)
        assert refused.value.status_code == 425
        await claimant.executor._release(
            claim, reason=f"bundle_{refused.value.status_code}"
        )


async def serve_a_lite_turn(database, claimant, thread_id):
    """One served lite turn: the bundle stamps its claim, attach and the turn's
    end publish the Session's presence, and the turn completes."""
    async with database.acquire() as conn:
        await record_input_seq(
            conn, unit_id=thread_id, unit_kind=UNIT_KIND_SESSION_TURN, input_seq=1
        )
    claim = await claim_now(database, thread_id)
    assert claim is not None
    await claimant.bundle(claim)
    for status in ("active", "awaiting_user"):
        assert await update_stateless_claim_status(
            database, thread_id=thread_id, lease_token=claim.lease_token, status=status
        )
    async with database.acquire() as conn:
        assert (
            await complete_unit(
                conn,
                unit_id=thread_id,
                lease_token=claim.lease_token,
                consumed_seq=claim.input_seq,
            )
            == "done"
        )


@pytest.mark.asyncio
async def test_claims_before_ready_back_off_without_parking_the_upgrade(
    database, actor, monkeypatch
):
    """F1: the upgrade's control wakes the queue at once, and until Ready
    writes the binding the attach assembly refuses every claim (409), which
    executors count toward the park budget (5 in about 3.4 min live). The
    bundle answers 425 instead; the executor's own release for it re-queues
    with the capped transient backoff and never parks, and the first claim
    after Ready goes past the gate."""
    case = await upgraded_create(
        database, actor, monkeypatch, first_wait="not_ready", bind=False
    )
    claimant = upgrade_claimant(database, monkeypatch)

    max_attempts = await database.fetchval(
        "SELECT max_attempts FROM run_queue WHERE unit_id = $1::uuid", case.thread_id
    )
    for attempt in range(1, max_attempts + 2):
        claim = await claim_now(database, case.thread_id)
        assert claim is not None, f"claim {attempt}: the unit parked"
        with pytest.raises(HTTPException) as refused:
            await claimant.bundle(claim)
        # turn_executor ~:4240: a bundle refusal releases as bundle_<status>.
        await claimant.executor._release(
            claim, reason=f"bundle_{refused.value.status_code}"
        )
        queue = await database.fetchrow(
            "SELECT state, park_reason, attach_failures, last_error, "
            "EXTRACT(EPOCH FROM run_after - queued_at) AS backoff "
            "FROM run_queue WHERE unit_id = $1::uuid",
            case.thread_id,
        )
        assert (
            queue["state"],
            queue["park_reason"],
            queue["attach_failures"],
            queue["last_error"],
        ) == ("queued", None, 0, None), (attempt, refused.value.detail)
        assert (refused.value.status_code, refused.value.detail) == (
            425,
            "Workspace upgrade is still creating the container",
        )
        assert float(queue["backoff"]) == pytest.approx(
            transient_release_backoff_seconds(claim.attempts_since_completion),
            abs=1.0,
        )
    claimant.inject.assert_not_awaited()
    claimant.assembly.assert_not_awaited()
    assert not await database.fetchval(
        "SELECT count(*) FROM thread_events WHERE thread_id = $1::uuid",
        case.thread_id,
    )
    pending = metadata(await database.get_thread(case.thread_id))
    assert "_stateless_active_claim" not in pending
    assert pending["workspace_container"]["status"] != "ready"

    case.cluster.become_ready()
    await ensure_session_workspace(
        case.thread_id,
        db=database,
        provisioner=case.provisioner,
        suspension=case.suspension,
    )
    ready = metadata(await database.get_thread(case.thread_id))
    assert ready["workspace_container"]["status"] == "ready"
    assert ready["workspace_container"]["seed_from"]["backend"] == "virtual"
    claimant.inject.side_effect = PastTheUpgradeGate
    with pytest.raises(PastTheUpgradeGate):
        await claimant.bundle(await claim_now(database, case.thread_id))


from orchestrator.services.thread_retirement import end_thread_flow  # noqa: E402

END_AUTHORITY_CHANGED = "Stateless retirement authority is malformed or changed"


async def end(database, case, *, force):
    return await end_thread_flow(
        case.thread_id,
        await database.get_thread(case.thread_id),
        permanent=False,
        force=force,
        dependencies=lifecycle.retirement_dependencies(database, case),
    )


async def queue_row(database, thread_id):
    return dict(
        await database.fetchrow(
            "SELECT state, lease_token, leased_by FROM run_queue "
            "WHERE unit_id = $1::uuid",
            thread_id,
        )
    )


async def stateless_receipts(database, thread_id):
    rows = await database.fetch(
        "SELECT runtime_incarnation FROM managed_repository_process_zero_receipts "
        "WHERE owner_kind='thread' AND owner_id=$1::uuid "
        "AND scope='stateless_workspace'",
        thread_id,
    )
    return [str(row["runtime_incarnation"]) for row in rows]


async def assert_forced_end_retires_the_create(database, case):
    """The precedent's End checks (test_stateless_failed_start_end_real_postgres
    ::test_normal_end_retires_exact_initial_pull_failed_workspace), the
    upgrade's record, and the queue the settle path leaves."""
    before = metadata(await database.get_thread(case.thread_id))["workspace_container"]
    prior_token = (await queue_row(database, case.thread_id))["lease_token"]

    assert await end(database, case, force=True) == {"status": "ended"}

    assert "pod" not in case.cluster.objects
    assert "service" not in case.cluster.objects
    assert case.cluster.objects["pvc"].metadata.uid == case.pvc_uid
    ended = await database.get_thread(case.thread_id)
    assert ended["status"] == "ended"
    workspace = metadata(ended)["workspace_container"]
    assert "_runtime_creation" not in workspace
    assert workspace["upgrade_config"] == UPGRADE_CONFIG
    assert workspace["seed_from"] == before["seed_from"]
    assert "_stateless_active_claim" not in metadata(ended)
    settled = metadata(ended)["_stateless_workspace_retirement_settled"]
    assert settled["runtime_incarnation"] == case.pod_uid
    assert settled["terminal_token"] == prior_token + 1
    assert await stateless_receipts(database, case.thread_id) == [case.pod_uid]
    assert await queue_row(database, case.thread_id) == {
        "state": "done",
        "lease_token": prior_token + 1,
        "leased_by": None,
    }


async def retirement_state(database, case):
    """What an End changes: the Kubernetes objects, the thread status, the
    queue row and the process-zero receipts."""
    return {
        "objects": sorted(case.cluster.objects),
        "status": (await database.get_thread(case.thread_id))["status"],
        "queue": await queue_row(database, case.thread_id),
        "receipts": await stateless_receipts(database, case.thread_id),
    }


@pytest.mark.asyncio
async def test_end_during_the_upgrade_create_retires_the_initial_creation(
    database, actor, monkeypatch
):
    """Review Focus 2: the unconsumed upgrade control makes a normal End busy;
    a forced End retires the exact Pod and keeps the template and the source."""
    case = await upgraded_create(database, actor, monkeypatch, first_wait="not_ready")

    with pytest.raises(HTTPException) as busy:
        await end(database, case, force=False)
    assert busy.value.status_code == 409
    assert busy.value.detail["code"] == "stateless_end_busy"

    await assert_forced_end_retires_the_create(database, case)


@pytest.mark.asyncio
@pytest.mark.parametrize("claims", [1, 3])
async def test_forced_end_after_pre_ready_claims_retires_the_upgrade_create(
    database, actor, monkeypatch, claims
):
    """F2: each pre-Ready claim takes a lease token before its 425 release, so
    the create's End admission sees a nonzero token on an unleased queue."""
    case = await upgraded_create(database, actor, monkeypatch, first_wait="not_ready")
    claimant = upgrade_claimant(database, monkeypatch)
    await back_off_before_ready(database, claimant, case.thread_id, claims=claims)
    assert await queue_row(database, case.thread_id) == {
        "state": "queued",
        "lease_token": claims,
        "leased_by": None,
    }

    await assert_forced_end_retires_the_create(database, case)


@pytest.mark.asyncio
async def test_forced_end_retires_the_upgrade_create_after_an_earlier_lite_turn(
    database, actor, monkeypatch
):
    """The lite turn before the upgrade leaves lease token 1 on the queue."""
    case = await upgraded_create(
        database,
        actor,
        monkeypatch,
        first_wait="not_ready",
        earlier_turn=lambda thread_id: complete_a_lite_turn(database, thread_id),
    )
    assert (await queue_row(database, case.thread_id))["lease_token"] == 1

    await assert_forced_end_retires_the_create(database, case)


@pytest.mark.asyncio
@pytest.mark.parametrize("claims", [0, 2])
async def test_forced_end_retires_the_upgrade_create_of_a_session_that_served_a_lite_turn(
    database, actor, monkeypatch, claims
):
    """A served lite turn leaves its claim record and the Session's presence
    (awaiting_user); with no lease on the queue that claim can't still run."""
    claimant = upgrade_claimant(database, monkeypatch, attach=LITE_ATTACH)
    case = await upgraded_create(
        database,
        actor,
        monkeypatch,
        first_wait="not_ready",
        earlier_turn=lambda thread_id: serve_a_lite_turn(database, claimant, thread_id),
    )
    await back_off_before_ready(database, claimant, case.thread_id, claims=claims)
    served = await database.get_thread(case.thread_id)
    assert served["status"] == "awaiting_user"
    assert metadata(served)["_stateless_active_claim"]["lease_token"] == 1
    assert await queue_row(database, case.thread_id) == {
        "state": "queued",
        "lease_token": 1 + claims,
        "leased_by": None,
    }

    await assert_forced_end_retires_the_create(database, case)


@pytest.mark.asyncio
async def test_resume_after_the_forced_end_restarts_the_upgrade_create(
    database, actor, monkeypatch
):
    """Resume of the ended upgrade keeps the template and the copy source: the
    next create uses the template on the kept volume, and its claims wait for
    Ready again."""
    claimant = upgrade_claimant(database, monkeypatch, attach=LITE_ATTACH)
    case = await upgraded_create(
        database,
        actor,
        monkeypatch,
        first_wait="not_ready",
        earlier_turn=lambda thread_id: serve_a_lite_turn(database, claimant, thread_id),
    )
    await back_off_before_ready(database, claimant, case.thread_id, claims=2)
    await assert_forced_end_retires_the_create(database, case)
    ended = await database.get_thread(case.thread_id)

    assert await lifecycle.resume_case(database, case, actor) == {
        "status": "created",
        "thread_id": case.thread_id,
    }

    resumed = await database.get_thread(case.thread_id)
    workspace = metadata(resumed)["workspace_container"]
    assert resumed["runtime_generation"] != ended["runtime_generation"]
    assert workspace["_runtime_creation"] == {
        "generation": str(resumed["runtime_generation"]),
        "mode": "create",
        "attempted": False,
        "replaces_uid": None,
    }
    assert workspace["upgrade_config"] == UPGRADE_CONFIG
    assert workspace["seed_from"] == metadata(ended)["workspace_container"]["seed_from"]
    # The upgrade's control is still unconsumed, so Resume re-queues the unit.
    assert (await queue_row(database, case.thread_id))["state"] == "queued"
    with monkeypatch.context() as waiter:
        waiter.setattr(
            case.provisioner, "_wait_for_ready", AsyncMock(return_value=None)
        )
        await ensure_session_workspace(
            case.thread_id,
            db=database,
            provisioner=case.provisioner,
            suspension=case.suspension,
        )
    assert case.cluster.pod_create_calls == 2
    assert case.cluster.objects["pod"].spec.containers[0]["image"] == IMAGE
    assert case.cluster.objects["pvc"].metadata.uid == case.pvc_uid
    with pytest.raises(HTTPException) as refused:
        await claimant.bundle(await claim_now(database, case.thread_id))
    assert (refused.value.status_code, refused.value.detail) == (
        425,
        "Workspace upgrade is still creating the container",
    )


@pytest.mark.asyncio
async def test_forced_end_retires_the_upgrade_create_of_a_parked_queue(
    database, actor, monkeypatch
):
    """Before F1 the bundle refused every pre-Ready claim with a 409, which the
    executor counts toward the park budget (the live Session parked at token
    5); any other deterministic bundle refusal still does. Today's bundle
    answers 425 before Ready, so the releases name the 409 themselves; the
    release, the park and its turn.parked frame are the executor's own."""
    case = await upgraded_create(database, actor, monkeypatch, first_wait="not_ready")
    claimant = upgrade_claimant(database, monkeypatch)
    max_attempts = await database.fetchval(
        "SELECT max_attempts FROM run_queue WHERE unit_id = $1::uuid", case.thread_id
    )
    for _ in range(max_attempts):
        claim = await claim_now(database, case.thread_id)
        assert claim is not None
        await claimant.executor._release(claim, reason="bundle_409")
    parked = await database.fetchrow(
        "SELECT state, park_reason, lease_token, leased_by FROM run_queue "
        "WHERE unit_id = $1::uuid",
        case.thread_id,
    )
    assert dict(parked) == {
        "state": "parked",
        "park_reason": PARK_REASON_RETRY_EXHAUSTED,
        "lease_token": max_attempts,
        "leased_by": None,
    }

    await assert_forced_end_retires_the_create(database, case)


@pytest.mark.asyncio
async def test_forced_end_still_refuses_a_claimed_create_without_the_upgrade_marker(
    database, actor, monkeypatch
):
    """Without seed_from a claimed create is a native container create with
    queue history, which End still refuses before any effect."""
    case = await upgraded_create(database, actor, monkeypatch, first_wait="not_ready")
    claimant = upgrade_claimant(database, monkeypatch)
    await back_off_before_ready(database, claimant, case.thread_id, claims=1)
    await database.execute(
        "UPDATE threads SET metadata = metadata #- '{workspace_container,seed_from}' "
        "WHERE id = $1::uuid",
        case.thread_id,
    )
    before = await retirement_state(database, case)

    with pytest.raises(HTTPException) as refused:
        await end(database, case, force=True)

    assert (refused.value.status_code, refused.value.detail) == (
        503,
        END_AUTHORITY_CHANGED,
    )
    assert await retirement_state(database, case) == before
    assert before["status"] == "created" and before["receipts"] == []


@pytest.mark.asyncio
@pytest.mark.parametrize("served_turn", [False, True])
async def test_end_refuses_while_a_pre_ready_claim_holds_the_lease(
    database, actor, monkeypatch, served_turn
):
    claimant = upgrade_claimant(database, monkeypatch, attach=LITE_ATTACH)
    case = await upgraded_create(
        database,
        actor,
        monkeypatch,
        first_wait="not_ready",
        earlier_turn=(
            (lambda thread_id: serve_a_lite_turn(database, claimant, thread_id))
            if served_turn
            else None
        ),
    )
    claim = await claim_now(database, case.thread_id)
    assert claim is not None
    before = await retirement_state(database, case)
    assert before["queue"] == {
        "state": "leased",
        "lease_token": claim.lease_token,
        "leased_by": CLAIMANT_POD,
    }
    assert {"pod", "pvc", "service"} <= set(before["objects"])

    with pytest.raises(HTTPException) as busy:
        await end(database, case, force=False)
    assert busy.value.status_code == 409
    assert busy.value.detail["code"] == "stateless_end_busy"
    assert busy.value.detail["queue_state"] == "leased"
    with pytest.raises(HTTPException) as refused:
        await end(database, case, force=True)
    assert (refused.value.status_code, refused.value.detail) == (
        503,
        END_AUTHORITY_CHANGED,
    )

    assert await retirement_state(database, case) == before
    assert before["receipts"] == []


@pytest.mark.asyncio
async def test_the_fenced_seed_clear_passes_every_guard_and_drops_only_the_marker(
    database, actor, monkeypatch
):
    from agent.database.postgres_db import PostgresDB as AgentDB
    from shared.run_queue import fence_lease

    case = await upgraded_create(database, actor, monkeypatch, first_wait="ready")
    before = metadata(await database.get_thread(case.thread_id))
    async with database.acquire() as conn:
        claimed = await claim_unit(
            conn, unit_kind=UNIT_KIND_SESSION_TURN, pod_name="p-1"
        )
    assert claimed is not None

    async with database.acquire() as conn:
        async with conn.transaction():
            assert await fence_lease(
                conn, unit_id=case.thread_id, lease_token=claimed.lease_token
            )
            cleared = await conn.fetchval(
                AgentDB._CLEAR_WORKSPACE_UPGRADE_SEED_SQL,
                case.thread_id,
                json.dumps(before["workspace_container"]["seed_from"]),
            )

    assert cleared == 1
    after = metadata(await database.get_thread(case.thread_id))
    expected = dict(before["workspace_container"])
    expected.pop("seed_from")
    assert after["workspace_container"] == expected
    assert after["_workspace_binding"] == before["_workspace_binding"]


@pytest.mark.asyncio
async def test_the_claimant_clears_the_marker_only_under_its_exact_lease(
    database, actor, monkeypatch
):
    """The agent's fenced clear and pending read: a stale token writes nothing."""
    from agent.api.lease_context import LeaseHandle, LeaseLostError, current_lease
    from agent.database.postgres_db import PostgresDB as AgentDB

    case = await upgraded_create(database, actor, monkeypatch, first_wait="ready")
    meta = metadata(await database.get_thread(case.thread_id))
    seed_from = meta["workspace_container"]["seed_from"]
    async with database.acquire() as conn:
        claimed = await claim_unit(
            conn, unit_kind=UNIT_KIND_SESSION_TURN, pod_name="p-1"
        )
    assert claimed is not None
    agent_db = AgentDB.__new__(AgentDB)
    agent_db._pool = database._pool
    agent_db._queries = {}
    assert await agent_db.workspace_upgrade_seed_pending(case.thread_id) is True

    lease = LeaseHandle()
    reset = current_lease.set(lease)
    try:
        lease.update(case.thread_id, claimed.lease_token - 1)
        with pytest.raises(LeaseLostError):
            await agent_db.clear_workspace_upgrade_seed(
                case.thread_id, expected_seed_from=seed_from
            )
        assert await agent_db.workspace_upgrade_seed_pending(case.thread_id) is True

        lease.update(case.thread_id, claimed.lease_token)
        assert await agent_db.clear_workspace_upgrade_seed(
            case.thread_id, expected_seed_from=seed_from
        )
        assert not await agent_db.clear_workspace_upgrade_seed(
            case.thread_id, expected_seed_from=seed_from
        )
    finally:
        current_lease.reset(reset)
    assert await agent_db.workspace_upgrade_seed_pending(case.thread_id) is False
