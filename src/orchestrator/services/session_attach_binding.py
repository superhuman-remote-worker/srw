"""Binding, delivering and aborting one pinned session attach.

Extracted verbatim from ``orchestrator.main`` (R1.B06 lane A, census group
``R_ATTACH``). This module owns the *atomic* half of the pinned attach plane:
who is bound to whom, proved in one statement, and how a failed attach gives
that ownership back. The restartable recovery that follows an abort lives in
``session_attach_recovery`` — different failure semantics, deliberately not
merged.

Atomicity is the contract (port contract §P8). Nothing here may be split into
two round trips, widened into a larger transaction, or have a read moved out
from under ``FOR UPDATE``:

* :func:`bind_registered_persistent_agent` writes the thread side and pairs
  ``agents.thread_id`` **in the same transaction**; deferred reciprocal
  constraints validate the final state. The conditional ``UPDATE`` re-asserts
  the lane, the admissible statuses, the runtime generation and the absence of
  a retirement token, so an out-of-band lane edit landing between precheck and
  bind fails closed. A lost reciprocal update raises rather than returning a
  half-bound success.
* :func:`reserve_session_attach_binding` reserves **before** HTTP delivery.
  That ordering is what makes the "flip only while detached" transition
  atomic: either the lane flip wins while ``agent_id`` is NULL, or the
  reservation wins while the lane is still pinned. Any post-plan error is
  ambiguous, never a refusal — it raises
  :class:`WarmBindingReservationPending` so no caller provisions a competing
  runtime against ownership a durable plan may already hold.
* :func:`release_session_attach_binding` keeps the generation rotation, the
  reciprocal agent release and the durable append-only outcome inside the same
  transaction as its two ``FOR UPDATE`` reads. Clearing pointers inside G1
  would be an ABA — the same pool process/thread pair can recur and a delayed
  failure would then clear G2 — so a successful abort rotates the generation
  and resets the monotonic exposure bit in one proof-bearing transaction. Any
  CAS loss anywhere in that sequence rolls all three back and reports
  ``"unsafe"``.

The refusal shapes are contract too (§P7). ``release_session_attach_binding``
returns ``"unsafe"`` — never a silent success — for a status mismatch, a moved
pod UID, an unparseable metadata blob, a warm protection row that does not
match on every one of its nine identity columns, a workspace tuple that is
half-present, a delivered abort without process-zero proof, a VM/remote
backend whose cleanup needs the orchestrator's own actuator, and any
already-admitted input or control request. ``pre_delivery`` is server-only and
means the HTTP payload never crossed; every other caller must present strict
process-zero proof bound to the captured physical workspace.

:func:`acknowledge_retiring_failed_attach` is the one path that does *not*
rotate: if owner End installed a retirement token after delivery but before
setup finished, the same exact agent appends its process-zero proof to that
retirement instead. It re-reads a settled outcome on every early return so a
replayed acknowledgement is idempotent rather than a refusal.

:func:`send_session_attach_locked` is the sequencer. It re-reads the thread and
re-proves lane, generation and protected-cloud readiness after **every** await
boundary, and once a reservation exists every failure route goes through
release-then-schedule-successor. An unconfirmed release returns ``True``
(delivery ambiguous, do not create a fallback) rather than ``False``; that
asymmetry prevents a double owner and is preserved exactly.

Collaborators arrive through :class:`SessionAttachBindingDependencies`, rebuilt
per invocation. Several of them are this module's own siblings, injected rather
than called directly because the attach suites patch them on
``orchestrator.main`` to drive individual branches; resolving them in this
module's namespace would make those patches green but inert (§P3).
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Literal, Optional
from uuid import UUID, uuid4

import httpx

from orchestrator.services import connector_credential_leases
from orchestrator.services.container_provisioner import (
    WORKSPACE_RUNTIME_INCARNATION_KEY,
)
from orchestrator.services.managed_repository_authority import (
    ManagedRepositoryAuthorityError,
)
from orchestrator.services.session_runtime_admission import (
    same_thread_runtime_authority,
    thread_runtime_authority,
)
from orchestrator.services.session_runtime_identity import (
    agent_sha_is_current,
    expected_agent_shas,
    thread_uses_pinned_execution,
)
from shared.run_queue import LANE_PINNED
from shared.session_subagent_batch import (
    SESSION_SUBAGENT_BATCH_SETTLE_CONTRACT,
    SESSION_SUBAGENT_BATCH_SETTLE_CONTRACT_KEY,
    SESSION_SUBAGENT_FANOUT_KEY,
)

logger = logging.getLogger(__name__)

SessionAttachReleaseOutcome = Literal[
    "released",
    "already_detached",
    "retirement_acknowledged",
    "unsafe",
]


class WarmBindingReservationPending(RuntimeError):
    """A durable warm protection plan must settle before fallback creation."""


@dataclass(frozen=True)
class SessionAttachBindingDependencies:
    """Collaborators for one attach binding operation, per invocation.

    Main-namespace singletons and cross-batch callables only; nothing here is
    captured at import (port contract §P1).

    * ``store`` — main's ``postgres_db``.
    * ``gitea_client`` — rebound on main by several suites; passed straight
      through to ``prepare_thread_repository_authority``.
    * ``agent_provisioner`` / ``persistent_provisioner`` — the two Pod
      authorities the warm-binding reservation and release hand their
      finalizer work to.
    * ``reserve_pinned_warm_agent_binding`` /
      ``release_pinned_warm_binding_protection`` — ``services``
      ``pinned_agent_authority``, reached through main's names because the
      plumbing suite patches them there to drive the bound/refused/pending
      states.
    * ``await_protected_cloud_runtime_ready`` (B04) — ``(thread_id, *,
      timeout_s, allow_schedule) -> Awaitable[bool]``. An admission
      prerequisite, checked inside the sender so no caller can omit it.
    * ``prepare_thread_repository_authority`` — ``(store, gitea_client,
      thread) -> Awaitable[None]``, raising ``ManagedRepositoryAuthorityError``.
    * ``assemble_session_attach_payload`` (B05) — main's
      ``_assemble_session_attach_payload``; owns the payload build and every
      fail-closed rule inside it.
    * ``schedule_attach_abort_successor`` — this lane's recovery module,
      reached through main's bridge (§P9 pairing lives there, not here).
    * ``prepare_pinned_session_mutation_target`` /
      ``pinned_session_mutation_target_is_current`` — this lane's recipient
      module, reached through main's bridges for the same patching reason.
    * ``reserve_session_attach_binding`` / ``release_session_attach_binding``
      / ``send_session_attach_locked`` — this module's own functions, reached
      through main's bridges so a patch there steers the sequencer.
    * ``session_subagent_fanout`` — ``(lane) -> bool``, the operator's session
      fan-out switch (``DeploymentSettings.session_subagent_fanout``), read at
      every attach; off when a composition does not wire it.
    """

    store: Any
    gitea_client: Any
    agent_provisioner: Any
    persistent_provisioner: Any
    reserve_pinned_warm_agent_binding: Callable[..., Awaitable[Any]]
    release_pinned_warm_binding_protection: Callable[..., Awaitable[Any]]
    await_protected_cloud_runtime_ready: Callable[..., Awaitable[bool]]
    prepare_thread_repository_authority: Callable[..., Awaitable[Any]]
    assemble_session_attach_payload: Callable[..., Awaitable[Optional[dict[str, Any]]]]
    schedule_attach_abort_successor: Callable[..., Any]
    prepare_pinned_session_mutation_target: Callable[..., Awaitable[Any]]
    pinned_session_mutation_target_is_current: Callable[..., Awaitable[bool]]
    reserve_session_attach_binding: Callable[..., Awaitable[Optional[str]]]
    release_session_attach_binding: Callable[
        ..., Awaitable[SessionAttachReleaseOutcome]
    ]
    send_session_attach_locked: Callable[..., Awaitable[bool]]
    session_subagent_fanout: Callable[[str], bool] = lambda _lane: False


async def bind_registered_persistent_agent(
    thread_id: str,
    agent_id: str,
    expected_agent_id: str | None,
    expected_runtime_generation: str,
    *,
    dependencies: SessionAttachBindingDependencies,
) -> str | None:
    """Lane-qualified final half of persistent-agent registration.

    A new dedicated agent is first inserted unbound. The thread side is written
    first here, then ``agents.thread_id`` is paired in the same transaction;
    deferred reciprocal constraints validate the final state. The advisory
    lock serializes sanctioned transitions, while the conditional update also
    fails closed if an out-of-band lane edit lands between precheck and bind.
    """
    attach_token = str(uuid4())
    async with dependencies.store.acquire() as conn:
        async with conn.transaction():
            if expected_agent_id is None:
                bound = await conn.execute(
                    "UPDATE threads SET agent_id = $2, "
                    "runtime_attach_token = $5::uuid, "
                    "control_admission_agent_id = NULL "
                    "WHERE id = $1 AND execution_lane = $3 AND agent_id IS NULL "
                    "AND status IN ('created','active','awaiting_user','suspended') "
                    "AND runtime_generation = $4::uuid "
                    "AND runtime_retirement_token IS NULL",
                    thread_id,
                    agent_id,
                    "pinned",
                    expected_runtime_generation,
                    attach_token,
                )
            else:
                bound = await conn.execute(
                    "UPDATE threads SET agent_id = $2, "
                    "runtime_attach_token = $6::uuid, "
                    "control_admission_agent_id = NULL "
                    "WHERE id = $1 AND execution_lane = $3 AND agent_id = $4 "
                    "AND status IN ('created','active','awaiting_user','suspended') "
                    "AND runtime_generation = $5::uuid "
                    "AND runtime_retirement_token IS NULL",
                    thread_id,
                    agent_id,
                    "pinned",
                    expected_agent_id,
                    expected_runtime_generation,
                    attach_token,
                )
            if bound != "UPDATE 1":
                return None
            reciprocal = await conn.execute(
                "UPDATE agents SET thread_id=$2::uuid "
                "WHERE id=$1::uuid "
                "AND (thread_id IS NULL OR thread_id=$2::uuid) "
                "AND current_job_id IS NULL",
                agent_id,
                thread_id,
            )
            if reciprocal != "UPDATE 1":
                raise RuntimeError("agent is no longer available for exact binding")
            return attach_token


async def find_idle_persistent_agent(
    *, dependencies: SessionAttachBindingDependencies
) -> Optional[dict]:
    """Find an idle persistent or dual-mode agent in the pool.

    Returns the agent row dict or None if no idle agents are available.
    An agent is idle when: agent_mode in ('persistent', 'dual'),
    status in ('ready'), and no thread currently attached.

    Agents whose build SHA doesn't match the current expected images
    are skipped here; the lifecycle reconciler is responsible for
    actually draining them.
    """
    try:
        rows = await dependencies.store.fetch(
            """
            SELECT id, pod_ip, pod_port, hostname, status, config_name,
                   metadata
            FROM agents
            WHERE agent_mode IN ('persistent', 'dual')
              AND status IN ('ready')
              AND thread_id IS NULL
            ORDER BY last_heartbeat DESC
            LIMIT 10
            """,
        )
        for row in rows:
            agent = dict(row)
            meta = agent.get("metadata") or {}
            if isinstance(meta, str):
                try:
                    meta = json.loads(meta)
                except (json.JSONDecodeError, ValueError):
                    meta = {}
            if agent_sha_is_current(meta):
                return agent
            logger.debug(
                "Skipping stale agent %s (build_sha=%s, expected=%s)",
                agent["id"],
                meta.get("build_sha", ""),
                expected_agent_shas(),
            )
        return None
    except Exception:
        logger.exception("Failed to find idle persistent agent")
        return None


async def send_session_attach(
    agent: dict,
    thread_id: str,
    config_override: Optional[dict] = None,
    project_ids: Optional[list] = None,
    datasources: Optional[list] = None,
    config_name: Optional[str] = None,
    expected_runtime_generation: str | None = None,
    *,
    dependencies: SessionAttachBindingDependencies,
) -> bool:
    """Serialize connector selection with the complete attach delivery."""
    async with dependencies.store.thread_datasource_lock(thread_id):
        return await dependencies.send_session_attach_locked(
            agent,
            thread_id,
            config_override=config_override,
            project_ids=project_ids,
            datasources=datasources,
            config_name=config_name,
            expected_runtime_generation=expected_runtime_generation,
        )


async def reserve_session_attach_binding(
    agent_id: str,
    thread_id: str,
    *,
    expected_runtime_generation: str,
    dependencies: SessionAttachBindingDependencies,
) -> str | None:
    """Atomically reserve both sides of a pinned warm-pool binding.

    Reservation happens before HTTP delivery.  That ordering makes the
    documented "flip only while detached" transition test atomic: either the
    lane flip wins while ``agent_id`` is NULL, or this reservation wins while
    the lane is still pinned.  We never need to tear down an already-started
    session merely because a post-delivery lane CAS lost.
    """
    try:
        result = await dependencies.reserve_pinned_warm_agent_binding(
            dependencies.store,
            agent_provisioner=dependencies.agent_provisioner,
            persistent_provisioner=dependencies.persistent_provisioner,
            thread_id=thread_id,
            agent_id=agent_id,
            expected_runtime_generation=expected_runtime_generation,
        )
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        # A durable plan may already own a finalizer effect.  Treat every
        # post-plan error as ambiguous; the leader reconciler will either bind
        # or release it and no caller may provision a competing runtime.
        logger.exception(
            "Warm session attach reservation became ambiguous for agent %s / "
            "thread %s: %s",
            agent_id,
            thread_id,
            exc,
        )
        raise WarmBindingReservationPending from exc
    if result.bound:
        return result.attach_token
    if result.state == "pending":
        raise WarmBindingReservationPending
    return None


async def release_session_attach_binding(
    agent_id: str,
    thread_id: str,
    *,
    expected_runtime_generation: str,
    expected_attach_token: str,
    pre_delivery: bool = False,
    expected_agent_pod_uid: str | None = None,
    local_runtime_quiesced: bool = False,
    local_quiescence_protocol: str | None = None,
    workspace_generation: str | None = None,
    workspace_runtime_incarnation: str | None = None,
    dependencies: SessionAttachBindingDependencies,
) -> SessionAttachReleaseOutcome:
    """Abort one exact failed attach before it can admit paid/user work.

    Clearing pointers inside G1 is an ABA: the same pool process/thread pair
    can recur and a delayed failure can then clear G2. Successful abort rotates
    the thread generation and resets the monotonic exposure bit in one
    proof-bearing transaction. ``pre_delivery`` is server-only and means the
    HTTP payload never crossed; the agent route must instead present strict
    process-zero proof bound to the captured physical workspace.
    """

    class _AttachAbortCASLost(RuntimeError):
        pass

    store = dependencies.store
    warm_release_id: str | None = None
    try:
        async with store.acquire() as conn:
            async with conn.transaction():
                thread = await conn.fetchrow(
                    "SELECT agent_id, status, metadata, runtime_generation, "
                    "runtime_attach_token, runtime_retirement_token, "
                    "runtime_authority_exposed FROM threads "
                    "WHERE id = $1 FOR UPDATE",
                    thread_id,
                )
                agent = await conn.fetchrow(
                    "SELECT thread_id, current_job_id, status, agent_mode, hostname, pod_uid "
                    "FROM agents "
                    "WHERE id = $1 FOR UPDATE",
                    agent_id,
                )
                prior = await conn.fetchrow(
                    "SELECT successor_generation FROM "
                    "thread_runtime_attach_abort_outcomes "
                    "WHERE thread_id=$1::uuid AND runtime_generation=$2::uuid "
                    "AND runtime_attach_token=$3::uuid AND agent_id=$4::uuid",
                    thread_id,
                    expected_runtime_generation,
                    expected_attach_token,
                    agent_id,
                )
                if prior is not None:
                    return "already_detached"
                # Everything below intentionally remains inside the same
                # transaction as the two FOR UPDATE reads.  The generation
                # rotation, reciprocal agent release, and durable outcome are
                # one indivisible authority transition; a failure at any
                # point rolls all three back.
                thread_matches = bool(
                    thread is not None
                    and str(thread.get("agent_id") or "") == agent_id
                    and str(thread.get("runtime_generation") or "")
                    == expected_runtime_generation
                    and str(thread.get("runtime_attach_token") or "")
                    == expected_attach_token
                    and thread.get("runtime_retirement_token") is None
                )
                agent_matches = bool(
                    agent is not None and str(agent.get("thread_id") or "") == thread_id
                )
                if not thread_matches or not agent_matches:
                    return "unsafe"
                if (
                    str(thread.get("status") or "") != "created"
                    or thread.get("runtime_authority_exposed") is not True
                    or agent.get("current_job_id") is not None
                ):
                    return "unsafe"
                current_pod_uid = str(agent.get("pod_uid") or "")
                if not current_pod_uid or (
                    not pre_delivery
                    and current_pod_uid != str(expected_agent_pod_uid or "")
                ):
                    return "unsafe"

                metadata = thread.get("metadata") or {}
                if isinstance(metadata, str):
                    try:
                        metadata = json.loads(metadata)
                    except (TypeError, ValueError):
                        return "unsafe"
                if not isinstance(metadata, dict):
                    return "unsafe"
                updated_metadata = dict(metadata)
                agent_pod_marker = metadata.get("agent_pod")
                if agent_pod_marker not in (None, {}):
                    if not (
                        isinstance(agent_pod_marker, dict)
                        and str(agent_pod_marker.get("pod_name") or "")
                        == str(agent.get("hostname") or "")
                        and str(agent_pod_marker.get("pod_uid") or "")
                        == current_pod_uid
                    ):
                        return "unsafe"
                    # The exact G1 Pod is released back to its agent-side
                    # lifecycle; it is not G2 thread authority. Clear only
                    # the marker proven reciprocal to the captured agent in
                    # this same rotation transaction—never UID-delete it.
                    updated_metadata.pop("agent_pod", None)
                # Dedicated actors advertise booting/ready before setup. Only
                # their exact protected create-intent Pod plus a monotonic
                # pre-setup proof may release that delivered life. Other status
                # mismatches still refuse; zero-admission is repeated below.
                observed_status = str(agent.get("status") or "")
                dedicated_pre_setup = bool(
                    not pre_delivery
                    and local_runtime_quiesced
                    and local_quiescence_protocol == "agent_attach_not_started_v1"
                    and observed_status in {"booting", "ready"}
                    and str(agent.get("agent_mode") or "") == "persistent"
                    and isinstance(agent_pod_marker, dict)
                    and agent_pod_marker.get("protection_protocol") == "finalizer_v1"
                    and str(agent_pod_marker.get("runtime_generation") or "")
                    == expected_runtime_generation
                    and not agent_pod_marker.get("warm_binding_protection")
                )
                if dedicated_pre_setup:
                    # Provisioner ownership lives in the immutable create
                    # intent, not in the published Pod marker.
                    dedicated_pre_setup = bool(
                        await conn.fetchval(
                            "SELECT EXISTS (SELECT 1 FROM thread_agent_pod_provision_intents "
                            "WHERE attempt_id::text=$1 AND thread_id=$2::uuid "
                            "AND runtime_generation=$3::uuid AND provisioner='persistent' "
                            "AND status='published' AND pod_name=$4 AND pod_uid=$5 "
                            "AND namespace=$6 AND protection_protocol='finalizer_v1')",
                            str(agent_pod_marker.get("provision_attempt") or ""),
                            thread_id,
                            expected_runtime_generation,
                            str(agent.get("hostname") or ""),
                            current_pod_uid,
                            str(agent_pod_marker.get("namespace") or ""),
                        )
                    )
                if observed_status != "session" and not dedicated_pre_setup:
                    return "unsafe"
                warm_binding = None
                if isinstance(agent_pod_marker, dict) and str(
                    agent_pod_marker.get("warm_binding_protection") or ""
                ):
                    try:
                        warm_release_id = str(
                            UUID(str(agent_pod_marker.get("warm_binding_protection")))
                        )
                    except (TypeError, ValueError):
                        return "unsafe"
                    warm_binding = await conn.fetchrow(
                        "SELECT * FROM "
                        "thread_agent_warm_binding_protections "
                        "WHERE protection_id=$1::uuid FOR UPDATE",
                        warm_release_id,
                    )
                    if not (
                        warm_binding is not None
                        and str(warm_binding["status"]) == "bound"
                        and str(warm_binding["source"]) == "attach"
                        and str(warm_binding["thread_id"]) == thread_id
                        and str(warm_binding["runtime_generation"])
                        == expected_runtime_generation
                        and str(warm_binding["runtime_attach_token"])
                        == expected_attach_token
                        and str(warm_binding["agent_id"]) == agent_id
                        and str(warm_binding["pod_name"])
                        == str(agent.get("hostname") or "")
                        and str(warm_binding["pod_uid"]) == current_pod_uid
                        and str(warm_binding["namespace"])
                        == str(agent_pod_marker.get("namespace") or "")
                    ):
                        return "unsafe"
                config = metadata.get("config_override") or {}
                workspace_cfg = (
                    config.get("workspace") if isinstance(config, dict) else {}
                )
                backend = (
                    str(
                        (workspace_cfg or {}).get("backend")
                        if isinstance(workspace_cfg, dict)
                        else ""
                    )
                    or "sandbox"
                )
                ws = metadata.get("workspace_container") or {}
                binding = metadata.get("_workspace_binding") or {}
                if not isinstance(ws, dict) or not isinstance(binding, dict):
                    return "unsafe"
                captured_workspace_generation = str(binding.get("generation") or "")
                captured_workspace_runtime = str(
                    (
                        ws.get("_docker_workspace_lease_id")
                        if ws.get("provisioner") == "docker"
                        else ws.get(WORKSPACE_RUNTIME_INCARNATION_KEY)
                    )
                    or ""
                )
                if bool(captured_workspace_generation) != bool(
                    captured_workspace_runtime
                ):
                    return "unsafe"
                if pre_delivery:
                    release_kind = "server_pre_delivery"
                    proof_protocol = "pre_delivery_no_payload_v1"
                else:
                    if not local_runtime_quiesced:
                        return "unsafe"
                    release_kind = "process_zero"
                    if local_quiescence_protocol == "agent_attach_not_started_v1":
                        # The dual agent may reject the delivered claim before
                        # its monotonic setup-started latch flips (for example,
                        # runtime-actor bind refusal).  No session/backend/task
                        # exists to run workspace cleanup yet. The agent route
                        # owns that one-way local latch; the DB additionally
                        # proves this G admitted no input/control authority.
                        proof_protocol = "agent_attach_not_started_v1"
                    elif backend == "sandbox" and captured_workspace_generation:
                        proof_protocol = "workspace_process_zero_v1"
                    elif backend in {"sandbox", "virtual", "none"} and not (
                        captured_workspace_generation or captured_workspace_runtime
                    ):
                        proof_protocol = "agent_runtime_zero_v1"
                    else:
                        # VM/remote cleanup requires the orchestrator's exact
                        # actuator stop/snapshot proof, never an agent assertion.
                        return "unsafe"
                    if local_quiescence_protocol != proof_protocol:
                        return "unsafe"
                    if str(workspace_generation or "") != captured_workspace_generation:
                        return "unsafe"
                    if (
                        str(workspace_runtime_incarnation or "")
                        != captured_workspace_runtime
                    ):
                        return "unsafe"

                admitted_input = await conn.fetchval(
                    "SELECT EXISTS (SELECT 1 FROM thread_input_deliveries "
                    "WHERE thread_id=$1::uuid AND owner_agent_id=$2::uuid "
                    "AND owner_pod_uid=$3 "
                    "AND (state IN ('admitted','settled') OR "
                    "($4::boolean AND state IN ('owned','queued'))))",
                    thread_id,
                    agent_id,
                    current_pod_uid,
                    proof_protocol == "agent_attach_not_started_v1",
                )
                admitted_control = await conn.fetchval(
                    "SELECT EXISTS (SELECT 1 FROM thread_control_requests "
                    "WHERE thread_id=$1::uuid AND runtime_generation=$2::uuid)",
                    thread_id,
                    expected_runtime_generation,
                )
                if admitted_input or admitted_control:
                    return "unsafe"

                # Local pre-setup zero says nothing about an external workspace
                # create already admitted by this thread. Reservation and UID
                # publication take the same thread lock: either publication
                # settles first, or the captured life stays bound for normal
                # retirement. Never rotate away from an unresolved obligation.
                if await conn.fetchval(
                    "SELECT EXISTS (SELECT 1 FROM thread_workspace_provision_intents "
                    "WHERE thread_id=$1::uuid AND status IN ('planned','revoking','fenced'))",
                    thread_id,
                ):
                    return "unsafe"

                successor_generation = str(uuid4())
                receipt = {
                    "version": 1,
                    "runtime_generation": expected_runtime_generation,
                    "successor_generation": successor_generation,
                    "agent_id": agent_id,
                    "runtime_attach_token": expected_attach_token,
                    "agent_pod_uid": current_pod_uid,
                    "release_kind": release_kind,
                    "quiescence_protocol": proof_protocol,
                    "workspace_generation": captured_workspace_generation or None,
                    "workspace_runtime_incarnation": (
                        captured_workspace_runtime or None
                    ),
                }
                if warm_binding is not None:
                    releasing = await conn.execute(
                        "UPDATE thread_agent_warm_binding_protections SET "
                        "status='releasing',"
                        "release_started_at=transaction_timestamp() "
                        "WHERE protection_id=$1::uuid AND status='bound'",
                        warm_release_id,
                    )
                    if releasing != "UPDATE 1":
                        raise _AttachAbortCASLost
                thread_updated = await conn.execute(
                    "UPDATE threads SET agent_id=NULL, runtime_attach_token=NULL, "
                    "control_admission_agent_id=NULL, "
                    "runtime_generation=$5::uuid, runtime_authority_exposed=false, "
                    "runtime_attach_abort_receipt=$6::jsonb, metadata=$7::jsonb "
                    "WHERE id=$1::uuid AND agent_id=$2::uuid "
                    "AND status='created' AND runtime_generation=$3::uuid "
                    "AND runtime_attach_token=$4::uuid "
                    "AND runtime_retirement_token IS NULL",
                    thread_id,
                    agent_id,
                    expected_runtime_generation,
                    expected_attach_token,
                    successor_generation,
                    json.dumps(receipt, sort_keys=True, separators=(",", ":")),
                    json.dumps(updated_metadata, sort_keys=True, separators=(",", ":")),
                )
                agent_updated = await conn.execute(
                    "UPDATE agents SET thread_id=NULL, status=$4 "
                    "WHERE id=$1::uuid AND thread_id=$2::uuid "
                    "AND pod_uid=$3 AND current_job_id IS NULL AND status::text=$5",
                    agent_id,
                    thread_id,
                    current_pod_uid,
                    "draining" if warm_binding is not None else "ready",
                    observed_status,
                )
                if thread_updated != "UPDATE 1" or agent_updated != "UPDATE 1":
                    raise _AttachAbortCASLost
                outcome_inserted = await conn.execute(
                    "INSERT INTO thread_runtime_attach_abort_outcomes ("
                    "thread_id, runtime_generation, runtime_attach_token, "
                    "agent_id, agent_pod_uid, successor_generation, release_kind, "
                    "quiescence_protocol, workspace_generation, "
                    "workspace_runtime_incarnation) VALUES ("
                    "$1::uuid,$2::uuid,$3::uuid,$4::uuid,$5,$6::uuid,$7,$8,"
                    "$9::uuid,$10::uuid) ON CONFLICT DO NOTHING",
                    thread_id,
                    expected_runtime_generation,
                    expected_attach_token,
                    agent_id,
                    current_pod_uid,
                    successor_generation,
                    release_kind,
                    proof_protocol,
                    captured_workspace_generation or None,
                    captured_workspace_runtime or None,
                )
                if outcome_inserted != "INSERT 0 1":
                    raise _AttachAbortCASLost
        if warm_release_id is not None:
            try:
                await dependencies.release_pinned_warm_binding_protection(
                    store,
                    protection_id=warm_release_id,
                    agent_provisioner=dependencies.agent_provisioner,
                    persistent_provisioner=dependencies.persistent_provisioner,
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "Warm Pod finalizer release remains durable after attach "
                    "abort (thread=%s protection=%s)",
                    thread_id,
                    warm_release_id,
                )
        return "released"
    except _AttachAbortCASLost:
        return "unsafe"


async def acknowledge_retiring_failed_attach(
    agent_id: str,
    thread_id: str,
    *,
    expected_runtime_generation: str,
    expected_attach_token: str,
    expected_agent_pod_uid: str,
    local_quiescence_protocol: str,
    workspace_generation: str | None,
    workspace_runtime_incarnation: str | None,
    dependencies: SessionAttachBindingDependencies,
) -> bool:
    """Route one exact failed-attach proof into an existing retirement.

    Owner End may install and authorize T after an attach payload is delivered
    but before the agent finishes setup.  Normal attach abort must then refuse
    to rotate G or schedule a successor.  The same exact agent can instead
    append its process-zero proof to T; every authority and physical-identity
    predicate is repeated by the receipt transaction.
    """

    store = dependencies.store

    async def _settled_readback() -> bool:
        return await store.has_exact_pinned_runtime_retirement_outcome(
            thread_id,
            runtime_generation=expected_runtime_generation,
            agent_id=agent_id,
            runtime_attach_token=expected_attach_token,
        )

    thread = await store.get_thread(thread_id)
    if not isinstance(thread, Mapping):
        return await _settled_readback()
    retirement_token = str(thread.get("runtime_retirement_token") or "")
    context = thread.get("runtime_retirement_context") or {}
    if isinstance(context, str):
        try:
            context = json.loads(context)
        except (TypeError, ValueError):
            return False
    if not retirement_token or not isinstance(context, Mapping):
        return await _settled_readback()
    settle_status = str(context.get("settle_status") or "")
    if settle_status not in {"ended", "suspended"}:
        return await _settled_readback()

    if local_quiescence_protocol == "agent_attach_not_started_v1" and context.get(
        "workspace_backend"
    ) in {"vm", "remote"}:
        # An agent that never entered setup may request the normal captured
        # actuator. It cannot attest VM writers, and nomination is not release.
        await store.request_pinned_pre_setup_retirement(
            thread_id,
            runtime_generation=expected_runtime_generation,
            runtime_attach_token=expected_attach_token,
            retirement_token=retirement_token,
            agent_id=agent_id,
            pod_uid=expected_agent_pod_uid,
        )
        return await _settled_readback()

    receipt_protocol = local_quiescence_protocol
    if local_quiescence_protocol == "agent_attach_not_started_v1":
        # The agent owns this monotonic pre-setup latch.  Under T, zero
        # admitted input/control plus the captured physical tuple derives the
        # ordinary retirement protocol without changing its receipt schema.
        receipt_protocol = (
            "workspace_process_zero_v1"
            if workspace_generation and workspace_runtime_incarnation
            else "agent_runtime_zero_v1"
        )
    receipt = await store.acknowledge_pinned_thread_local_quiescence(
        thread_id,
        expected_runtime_generation=expected_runtime_generation,
        expected_retirement_token=retirement_token,
        expected_agent_id=agent_id,
        expected_attach_token=expected_attach_token,
        expected_settle_status=settle_status,
        expected_quiescence_protocol=receipt_protocol,
        expected_workspace_generation=workspace_generation,
        expected_workspace_runtime_incarnation=workspace_runtime_incarnation,
        expected_agent_pod_uid=expected_agent_pod_uid,
        require_zero_admission=True,
    )
    return receipt is not None or await _settled_readback()


async def send_session_attach_locked(
    agent: dict,
    thread_id: str,
    config_override: Optional[dict] = None,
    project_ids: Optional[list] = None,
    datasources: Optional[list] = None,
    config_name: Optional[str] = None,
    expected_runtime_generation: str | None = None,
    *,
    dependencies: SessionAttachBindingDependencies,
) -> bool:
    """Send a session attach request to an idle persistent agent.

    ``config_name`` is the thread's config — pool pods boot as workers
    (``worker_base``), so the agent must re-resolve the session base config
    from this name instead of its boot config
    (knowledge-base/knowledge/issues/session_config_name_plumbing.md, hole B).

    ``project_ids``/``datasources`` are accepted for caller compatibility but
    ignored: the assembly recomputes both from the thread's current state
    (they are mutable authorization grants — see
    ``_assemble_session_attach_payload``, which owns the payload build and
    every fail-closed rule).

    Returns True once the agent accepted the session *or* delivery became
    ambiguous after the DB reservation.  Callers must not provision a fallback
    executor on that outcome.  False means no reservation remains.
    """
    del project_ids, datasources  # recomputed inside the assembly (see docstring)
    store = dependencies.store
    thread = await store.get_thread(thread_id)
    runtime_authority = thread_runtime_authority(thread)
    if not thread_uses_pinned_execution(thread) or runtime_authority is None:
        logger.warning(
            "Session attach: refusing pinned delivery for thread %s on "
            "execution lane %r",
            thread_id,
            thread.get("execution_lane") if thread else None,
        )
        return False
    if (
        expected_runtime_generation is not None
        and runtime_authority.generation != expected_runtime_generation
    ):
        return False
    # A protected reader is an admission prerequisite, not work an already
    # reserved warm agent should wait on.  Keep this authoritative gate inside
    # the sender so Docker/create and future callers cannot omit it.
    if not await dependencies.await_protected_cloud_runtime_ready(
        thread_id,
        timeout_s=0,
        allow_schedule=False,
    ):
        return False
    thread = await store.get_thread(thread_id)
    if not thread_uses_pinned_execution(thread) or not same_thread_runtime_authority(
        thread, runtime_authority
    ):
        return False
    try:
        await dependencies.prepare_thread_repository_authority(
            store, dependencies.gitea_client, thread
        )
    except ManagedRepositoryAuthorityError as exc:
        logger.warning(
            "Session attach: repository authority unavailable for thread %s (%s)",
            thread_id,
            exc.code,
        )
        return False
    thread = await store.get_thread(thread_id)
    if not thread_uses_pinned_execution(thread) or not same_thread_runtime_authority(
        thread, runtime_authority
    ):
        return False
    # Repository authority preparation is another await boundary.  A revoked
    # reader/current mount selection must fail before either side of the warm
    # reservation is written.
    if not await dependencies.await_protected_cloud_runtime_ready(
        thread_id,
        timeout_s=0,
        allow_schedule=False,
    ):
        return False
    thread = await store.get_thread(thread_id)
    if not thread_uses_pinned_execution(thread) or not same_thread_runtime_authority(
        thread, runtime_authority
    ):
        return False
    agent_id = str(agent["id"])
    try:
        attach_token = await dependencies.reserve_session_attach_binding(
            agent_id,
            thread_id,
            expected_runtime_generation=runtime_authority.generation,
        )
    except WarmBindingReservationPending:
        logger.warning(
            "Warm session attach protection remains pending for agent %s / "
            "thread %s; refusing a competing runtime",
            agent_id,
            thread_id,
        )
        return True
    if attach_token is None:
        return False
    payload = await dependencies.assemble_session_attach_payload(
        thread_id,
        config_override=config_override,
        config_name=config_name,
        runtime_agent_id=agent_id,
    )
    if payload is not None:
        # Credential leases (connector drivers C2), under the datasource lock
        # the caller holds: a warm re-attach or a recycled pod receives the
        # thread's same token; a failed attach revokes nothing.
        try:
            await connector_credential_leases.deliver_connector_leases_with(
                store,
                payload.get("datasources"),
                owner=connector_credential_leases.LeaseOwner.thread(thread_id),
            )
        except connector_credential_leases.LeaseDeliveryError:
            logger.warning(
                "Session attach: connector leases unavailable for thread %s",
                thread_id,
                exc_info=True,
            )
            payload = None
    if payload is None:
        try:
            release = await dependencies.release_session_attach_binding(
                agent_id,
                thread_id,
                expected_runtime_generation=runtime_authority.generation,
                expected_attach_token=attach_token,
                pre_delivery=True,
            )
        except Exception:
            # A failed release is ambiguous ownership, just like a failed HTTP
            # attach. Retain/fence it for the reconciler; never provision a
            # second runtime against an ownership state we could not clear.
            logger.exception(
                "Session attach assembly failed and reservation release was "
                "ambiguous for agent %s / thread %s",
                agent_id,
                thread_id,
            )
            return True
        if release in {"released", "already_detached"}:
            dependencies.schedule_attach_abort_successor(
                thread_id,
                retired_runtime_generation=runtime_authority.generation,
                retired_attach_token=attach_token,
                retired_agent_id=agent_id,
            )
        return release not in {"released", "already_detached"}

    payload_generation = str(payload.get("session_runtime_generation") or "")
    if payload_generation != runtime_authority.generation:
        release = await dependencies.release_session_attach_binding(
            agent_id,
            thread_id,
            expected_runtime_generation=runtime_authority.generation,
            expected_attach_token=attach_token,
            pre_delivery=True,
        )
        if release in {"released", "already_detached"}:
            dependencies.schedule_attach_abort_successor(
                thread_id,
                retired_runtime_generation=runtime_authority.generation,
                retired_attach_token=attach_token,
                retired_agent_id=agent_id,
            )
        return release not in {"released", "already_detached"}
    payload["session_runtime_attach_token"] = attach_token
    # This orchestrator settles an abandoned delegation turn as one batch
    # (parallel_subagents.md §12). Both pinned /session/attach handlers read
    # named fields, so the key is safe for agents that predate it; it is NOT
    # part of the shared payload, which the stateless executor splats into
    # ``_attach_session`` (the claim bundle advertises it beside ``attach``).
    payload[SESSION_SUBAGENT_BATCH_SETTLE_CONTRACT_KEY] = (
        SESSION_SUBAGENT_BATCH_SETTLE_CONTRACT
    )
    # The operator's fan-out switch for the pinned lane (§12, D5), read at
    # this attach; handlers that predate it ignore the named field.
    payload[SESSION_SUBAGENT_FANOUT_KEY] = bool(
        dependencies.session_subagent_fanout(LANE_PINNED)
    )

    current = await store.get_thread(thread_id)
    if (
        not thread_uses_pinned_execution(current)
        or not same_thread_runtime_authority(current, runtime_authority)
        or str(current.get("agent_id") or "") != agent_id
        or str(current.get("runtime_attach_token") or "") != attach_token
    ):
        release = await dependencies.release_session_attach_binding(
            agent_id,
            thread_id,
            expected_runtime_generation=runtime_authority.generation,
            expected_attach_token=attach_token,
            pre_delivery=True,
        )
        if release in {"released", "already_detached"}:
            dependencies.schedule_attach_abort_successor(
                thread_id,
                retired_runtime_generation=runtime_authority.generation,
                retired_attach_token=attach_token,
                retired_agent_id=agent_id,
            )
        # An unsafe/unconfirmed release still owns enough authority that a
        # fallback runtime would create a double owner.  Report the delivery
        # as ambiguous and leave the reconciler/process latch to fence it.
        return release not in {"released", "already_detached"}

    target = await dependencies.prepare_pinned_session_mutation_target(
        thread_id=thread_id,
        agent_id=agent_id,
        runtime_generation=runtime_authority.generation,
        attach_token=attach_token,
    )
    if target is None:
        try:
            release = await dependencies.release_session_attach_binding(
                agent_id,
                thread_id,
                expected_runtime_generation=runtime_authority.generation,
                expected_attach_token=attach_token,
                pre_delivery=True,
            )
        except Exception:
            logger.exception(
                "Session attach recipient proof failed and reservation release "
                "was ambiguous for agent %s / thread %s",
                agent_id,
                thread_id,
            )
            return True
        if release in {"released", "already_detached"}:
            dependencies.schedule_attach_abort_successor(
                thread_id,
                retired_runtime_generation=runtime_authority.generation,
                retired_attach_token=attach_token,
                retired_agent_id=agent_id,
            )
        return release not in {"released", "already_detached"}

    payload["_recipient"] = target.recipient
    agent_url = (
        f"http://{target.agent['pod_ip']}:"
        f"{int(target.agent.get('pod_port') or 8001)}/session/attach"
    )
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(agent_url, json=payload)
        target_current = await dependencies.pinned_session_mutation_target_is_current(
            target
        )
        if not target_current:
            logger.error(
                "Session attach response for agent %s lost exact recipient "
                "authority; retaining reservation for reconciliation",
                agent_id,
            )
            return True
        if response.status_code == 200:
            logger.info(
                "Assigned thread %s to persistent agent %s (%s:%s)",
                thread_id,
                target.agent["id"],
                target.agent["pod_ip"],
                target.agent["pod_port"],
            )
            return True
        logger.error(
            "Persistent agent %s returned ambiguous attach response %s; "
            "retaining reservation to prevent duplicate execution",
            target.agent["id"],
            response.status_code,
        )
        return True
    except Exception:
        logger.exception(
            "Session attach delivery to agent %s is ambiguous; retaining "
            "the DB reservation to prevent duplicate execution",
            target.agent["id"],
        )
        return True
