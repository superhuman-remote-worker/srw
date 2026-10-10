"""Runtime owner of session attachment.

One :class:`SessionAttachCoordinator` per runtime owns attaching a thread's
session -- dedicated at boot, warm/pool after ``/session/attach``, dual after
its ``/session/attach`` and stateless under the executor's claim -- and
everything an attach leaves behind when it fails:

- the attach sequence itself: identity adoption before the first await, the
  workspace readiness and identity fences, the one-way setup boundary,
  construction, runtime wiring, workspace population and the ordered
  publication that ends in an open input queue (see :meth:`attach`);
- the construction-only cleanup context of an exact delivered attach, the
  failed-attach cleanup that proves local/remote writers zero, and the
  retained release receipt that may rotate the delivered generation only once
  that proof exists;
- the pool admission claim and its background attach transaction;
- the session delegation advertisement an attach runs with.

Identity lives in :class:`~agent.api.session_identity.SessionIdentityRuntime`
and input state in :class:`~agent.api.session_input.SessionInputRuntime`; the
coordinator reaches both, and everything the runtime still owns (the session
slot, the event journal, watchers, watchdogs, lifecycle writes, restore, loop
start, cloud sync builders), through :class:`SessionAttachPorts` -- every one a
call-time provider, never a captured value.

This module does not import the runtime that composes it, an application
factory, the loop or the worker graph (import contract and boundary guard).
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import os
import time
from dataclasses import dataclass
from functools import partial
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

from agent.api.lease_context import LeaseHandle, LeaseLostError
from agent.api.orchestrator_client import (
    SessionEnded,
    SessionEnding,
    SessionGrantDenied,
)
from agent.api.session_contract import (
    EventJournalUnavailable,
    ProtectedCloudUnavailable,
    WorkspaceNotReady,
)
from agent.api.session_identity import (
    SessionIdentityRuntime,
    canonical_runtime_generation,
    pinned_runtime_generation_advertised,
    pinned_status_identity_advertised,
)
from agent.api.session_input import SessionInputRuntime
from agent.api.session_workspace import (
    ATTACH_WORKSPACE_IDENTITY_UNSET,
    assert_attach_workspace_payload,
    assert_attach_workspace_tier,
    canonical_attach_workspace_identity,
    protected_workspace_delivery,
    protected_workspace_identity,
    protected_workspace_marker,
    protected_mount_payload,
)
from shared.runtime.core.loader import (
    normalize_delegation_block,
    normalize_llm_tiers,
)
from shared.runtime.core.tool_policy import normalize_tool_policy
from shared.runtime_actor import RuntimeActorContext
from shared.session_attach_cleanup_identity import PreSetupWorkspaceIdentity
from shared.session_subagent_batch import (
    SESSION_SUBAGENT_BATCH_SETTLE_CONTRACT,
    SESSION_SUBAGENT_BATCH_SETTLE_CONTRACT_KEY,
    SESSION_SUBAGENT_FANOUT_KEY,
)

# Records keep the runtime's logger name (the moved code logged there).
logger = logging.getLogger("agent.api.persistent_app")

# Backoff between attempts of an exact failed-attach cleanup or release: the
# first retry is immediate, later ones cap at a few seconds.
EXACT_SETTLEMENT_RETRY_DELAYS = (0.0, 0.25, 1.0, 3.0)


def subagent_batch_settle_advertised(value: Any) -> bool:
    """Accept only the exact numeric v1 batch-settle capability (§12)."""

    return bool(type(value) is int and value == SESSION_SUBAGENT_BATCH_SETTLE_CONTRACT)


def subagent_fanout_advertised(value: Any) -> bool:
    """The orchestrator's fan-out switch counts only as a literal ``true``."""

    return value is True


def session_subagent_advertisement(
    batch_settle_contract: Any,
    fanout: Any,
    workspace_responses: Tuple[Any, ...],
    *,
    from_workspace: bool,
) -> Tuple[bool, bool]:
    """The fan-out advertisement a session attach runs with (§12).

    The pushed pinned ``/session/attach`` body and the stateless claim bundle
    pass both values as keywords. A pinned pod that attaches itself receives
    neither: it reads them from the newest ready workspace response it
    fetched (``workspace_responses``, newest first), exactly like
    ``pinned_status_identity_contract``. The initial VM wait payload carries
    neither key; the ready payload that follows it does. A stateless attach
    never falls back: its claim bundle is the only per-claim authority, and a
    warm session is re-applied from the next bundle.
    """

    if from_workspace:
        newest = next(
            (
                response
                for response in workspace_responses
                if isinstance(response, dict)
            ),
            None,
        )
        if newest is not None:
            if batch_settle_contract is None:
                batch_settle_contract = newest.get(
                    SESSION_SUBAGENT_BATCH_SETTLE_CONTRACT_KEY
                )
            if fanout is None:
                fanout = newest.get(SESSION_SUBAGENT_FANOUT_KEY)
    return (
        subagent_batch_settle_advertised(batch_settle_contract),
        subagent_fanout_advertised(fanout),
    )


def session_backend_is_lite(config: Optional[Dict[str, Any]]) -> bool:
    """True if a resolved session config / override selects a lite tier
    (``virtual``/``none``).

    Lite tiers run with no workspace pod, so the session attach must skip the
    workspace-readiness poll (which would otherwise raise ``WorkspaceNotReady``
    for a pod that never exists) and let ``PersistentSession._setup_workspace``
    build the object-store backend from the injected mounts (the lite tiers
    have no SSH workspace pod — no_workspace_agent_mode.md §4).
    """
    if not isinstance(config, dict):
        return False
    from agent.core.backends.factory import LITE_BACKENDS

    # config_override is flat ({workspace: ...}); a resolved_config blob nests
    # the agent config under "agent".
    ws = config.get("workspace") or (config.get("agent") or {}).get("workspace") or {}
    return ws.get("backend") in LITE_BACKENDS


def session_backend_is_vm(config: Optional[Dict[str, Any]]) -> bool:
    """True if a resolved session config / override selects the VM tier
    (``vm``, or its legacy ``remote`` alias).

    A vm-tier session's workspace is a KubeVirt VM, and its readiness poll must
    accept ONLY that VM: a sandbox container is ready in seconds while a cold VM
    boot takes minutes, so a container that exists for any reason would always
    win the race and silently attach the session to the wrong tier
    (knowledge-base/knowledge/issues/session_vm_backend_never_attaches.md Defect 2).

    Same dual-shape contract as :func:`session_backend_is_lite`.
    """
    if not isinstance(config, dict):
        return False
    from agent.core.backends.factory import VM_BACKENDS

    ws = config.get("workspace") or (config.get("agent") or {}).get("workspace") or {}
    return ws.get("backend") in VM_BACKENDS


def apply_datasource_enrichment_to_resolved(
    resolved_config: Optional[Dict[str, Any]],
    ds_tool_categories: Dict[str, List[str]],
) -> None:
    """Fold datasource-derived config into an orchestrator-resolved blob.

    Hydration (``load_config_from_resolved``) deliberately skips the
    config_override merge, so the datasource tool categories applied to
    config_override during attach never reach a hydrated session. Merge them
    into the blob's ``agent["tools"]`` in place instead.

    No-op when ``resolved_config`` is absent or malformed.
    """
    if not resolved_config:
        return
    agent_dict = resolved_config.get("agent")
    if not isinstance(agent_dict, dict):
        return
    if ds_tool_categories:
        agent_tools = agent_dict.get("tools")
        agent_tools = dict(agent_tools) if isinstance(agent_tools, dict) else {}
        agent_tools.update(ds_tool_categories)
        agent_dict["tools"] = agent_tools


MEMORY_EMBEDDING_ENV_KEYS = (
    "EMBEDDING_PROVIDER",
    "EMBEDDING_MODEL",
    "EMBEDDING_BASE_URL",
    "EMBEDDING_API_KEY",
    "RERANK_MODEL",
    "RERANK_BASE_URL",
    "RERANK_API_KEY",
)


def apply_session_embedding_env(env_keys: Optional[Dict[str, Any]]) -> None:
    """Replace the process embedding profile with this attach's snapshot.

    Scrub-on-claim (stateless_agents.md §5.6 — M3 deliverable D, and a live
    pinned-lane pod-reuse leak): the KB path (``apply_kb_embedding_env``) was
    deliberately hardened pop-first for pod reuse; the memory path was not —
    it pushed ``EMBEDDING_API_KEY`` into process-global ``os.environ`` and
    never popped it, so a following tenant whose config omitted ``env_keys``
    skipped the block and inherited the prior tenant's key + un-reset
    singleton. Symmetric now: at EVERY attach, unconditionally pop all
    memory-embedding keys and null the memory-embedding singleton BEFORE
    applying the new ``env_keys`` (which then re-set them only if provided).

    Acceptance (tests/test_turn_executor.py scrub matrix): after an attach
    with tenant-A env_keys followed by an attach with tenant-B env_keys
    absent, ``os.environ`` carries no A values and the singleton is None.
    """
    for k in MEMORY_EMBEDDING_ENV_KEYS:
        os.environ.pop(k, None)
    from shared.runtime.services import embedding_service as _embedding_module

    _embedding_module._embedding_service = None
    # KB path: already a complete pop-first attach-time snapshot.
    _embedding_module.apply_kb_embedding_env(env_keys)
    if env_keys:
        for k in MEMORY_EMBEDDING_ENV_KEYS:
            value = env_keys.get(k)
            if value is not None:
                os.environ[k] = str(value)
        if any(
            k in env_keys
            for k in MEMORY_EMBEDDING_ENV_KEYS + _embedding_module.KB_EMBEDDING_ENV_KEYS
        ):
            logger.info(
                "Embedding overrides applied: memory_model=%s, kb_model=%s",
                os.environ.get("EMBEDDING_MODEL"),
                os.environ.get("KB_EMBEDDING_MODEL"),
            )


async def strict_cleanup_partial_attach_local_resources(
    context: dict[str, Any],
) -> None:
    """Close every datasource/MCP owner created before session construction."""

    resources: list[Any] = []
    seen: set[int] = set()
    for registry_name in ("datasources", "datasource_clients"):
        registry = context.get(registry_name)
        if not isinstance(registry, dict):
            continue
        for resource in registry.values():
            if resource is None or id(resource) in seen:
                continue
            seen.add(id(resource))
            resources.append(resource)
    for resource in resources:
        try:
            aclose = getattr(resource, "aclose", None)
            if callable(aclose):
                result = aclose()
                if inspect.isawaitable(result):
                    await result
                continue
            close = getattr(resource, "close", None)
            if callable(close):
                result = close()
                if inspect.isawaitable(result):
                    await result
        except Exception as exc:
            raise EventJournalUnavailable(
                "partial attach local datasource owner did not quiesce"
            ) from exc
    context["datasources"] = {}
    context["datasource_clients"] = {}


async def strict_cleanup_partial_sandbox_workspace(
    context: dict[str, Any],
) -> str:
    """Prove all writers zero on an attested sandbox before G rotation."""

    remote = context.get("remote")
    generation = canonical_runtime_generation(context.get("workspace_generation"))
    incarnation = canonical_runtime_generation(
        context.get("workspace_runtime_incarnation")
    )
    fingerprint = context.get("workspace_ssh_host_key_fingerprint")
    thread_id = context.get("thread_id")
    if not (
        isinstance(remote, dict)
        and isinstance(remote.get("host"), str)
        and remote["host"].strip()
        and type(remote.get("port", 22)) is int
        and 1 <= remote.get("port", 22) <= 65535
        and isinstance(thread_id, str)
        and thread_id
        and generation is not None
        and incarnation is not None
        and isinstance(fingerprint, str)
        and fingerprint.strip()
    ):
        raise EventJournalUnavailable(
            "partial sandbox attach lacks exact workspace cleanup authority"
        )

    from shared.runtime.core.backends.remote import RemoteBackend

    try:
        backend = RemoteBackend(
            host=remote["host"],
            port=remote.get("port", 22),
            username=remote.get("username", "agent-host"),
            key_path=remote.get("key_path", "/run/secrets/vm-ssh-key"),
            workspace_path=remote.get("workspace_path", "/home/agent-host/workspace"),
            job_id=thread_id,
            connect_timeout=remote.get("connect_timeout", 30),
            max_retries=remote.get("max_retries", 5),
            retry_timeouts_as_booting=remote.get("retry_timeouts_as_booting", False),
            sudo_action="freeze",
            workspace_generation=generation,
            runtime_incarnation=incarnation,
            expected_host_key_fingerprint=fingerprint,
            workspace_tier="sandbox",
        )
    except Exception as exc:
        raise EventJournalUnavailable(
            "partial sandbox cleanup authority is malformed"
        ) from exc

    loop = asyncio.get_running_loop()
    try:
        await loop.run_in_executor(None, backend.connect)
        protocol = await loop.run_in_executor(
            None, backend.protected_workspace_zero_cleanup_strict
        )
        if protocol != "workspace_process_zero_v1":
            raise EventJournalUnavailable(
                "partial sandbox workspace process-zero proof is unavailable"
            )
        return protocol
    except EventJournalUnavailable:
        raise
    except Exception as exc:
        raise EventJournalUnavailable(
            "partial sandbox workspace did not quiesce"
        ) from exc
    finally:
        try:
            await loop.run_in_executor(None, backend.disconnect)
        except Exception:
            logger.warning(
                "Partial sandbox cleanup transport did not disconnect",
                exc_info=True,
            )


@dataclass(frozen=True, slots=True)
class SessionAttachPorts:
    """Call-time collaborators of the attach coordinator.

    Owners and process singletons are providers read at each use. The rest
    name the runtime's own operations an attach performs on state the runtime
    still owns (moved by R3.3c/R3.4): the session slot, turn flags, the event
    journal, watchers and watchdogs, the lifecycle CAS, restore, loop start and
    the cloud sync builders.
    """

    # Owners and process singletons.
    identity: Callable[[], SessionIdentityRuntime]
    input_runtime: Callable[[], SessionInputRuntime]
    agent: Callable[[], Any]
    orchestrator_client: Callable[[], Any]
    stateless_mode: Callable[[], bool]
    lease: Callable[[], Optional[LeaseHandle]]
    # The attached session slot.
    session: Callable[[], Any]
    publish_session: Callable[[Any], None]
    session_factory: Callable[..., Any]
    # Authority callbacks the constructed session is wired with.
    provider_admission: Callable[..., Any]
    effect_authority: Callable[..., Any]
    settlement_authority: Callable[..., Any]
    retirement_authorized: Callable[..., Any]
    terminate_failed_attach_if_authorized: Callable[..., Awaitable[bool]]
    subagent_event_available: Callable[..., Any]
    # Runtime state an attach resets or clears.
    reset_turn_state: Callable[[], None]
    reset_draft_title: Callable[[], None]
    set_cloud_sync_retry_pending: Callable[[bool], None]
    close_runtime_authorization: Callable[[], None]
    clear_canvas: Callable[[], None]
    clear_subscribers: Callable[[], None]
    side_tasks_active: Callable[[], bool]
    quiesce_side_tasks: Callable[[], Awaitable[None]]
    pending_drain_suspend: Callable[[], Optional[dict[str, Any]]]
    # The ordered event journal.
    event_writer: Callable[[], Any]
    discard_event_writer: Callable[[], None]
    reset_journal_cursor: Callable[[], None]
    open_event_journal: Callable[[], Awaitable[None]]
    events_epoch: Callable[[], int]
    # Watchers, watchdogs and publication.
    stop_interrupt_watcher: Callable[[], Awaitable[None]]
    stop_control_watcher: Callable[[], Awaitable[None]]
    stop_and_join_watchdogs: Callable[[], Awaitable[None]]
    start_watchdogs: Callable[[], None]
    update_thread_status: Callable[..., Awaitable[bool]]
    restore_messages: Callable[[], Awaitable[None]]
    broadcast: Callable[[str, dict[str, Any]], Any]
    officer_config: Callable[[], Any]
    loop_running: Callable[[], bool]
    ensure_loop_started: Callable[[str], bool]
    wire_aux_archiver: Callable[[], None]
    emit_citation_verdict: Callable[..., Any]
    emit_canvas_event: Callable[..., Any]
    # Workspace and configuration collaborators.
    poll_workspace_ready: Callable[..., Awaitable[Optional[Dict[str, Any]]]]
    load_expert_config: Callable[[str], Any]
    apply_session_tool_group_markers: Callable[..., None]
    llm_config_with_cache_key: Callable[[Any], Any]
    build_sync_coordinator: Callable[..., Any]
    legacy_nc_cloud_cfg: Callable[[str], Dict[str, Any]]
    # Process-global dynamic tools (the MCP entries of the tool registry).
    register_mcp_tools: Callable[[Any], None]


@dataclass(frozen=True, slots=True)
class PoolAttachAdmission:
    """The synchronous answer to a pool ``/session/attach``."""

    status_code: int
    body: dict[str, Any]


def cloud_mount_payload(workspace: dict[str, Any]) -> dict[str, Any] | None:
    """The cloud mount config a workspace payload carries: the in-workspace
    ``cloud_mount``, or the ``cloud_mount_sidecar`` of a Pod whose folders its
    sidecars mounted (connector drivers D7)."""
    return workspace.get("cloud_mount") or workspace.get("cloud_mount_sidecar")


class SessionAttachCoordinator:
    """Attach sequence, failed-attach cleanup, release receipts and the pool
    admission claim of one runtime."""

    def __init__(
        self, ports: SessionAttachPorts, *, logger: logging.Logger | None = None
    ) -> None:
        self._ports = ports
        self._logger = logger or logging.getLogger("agent.api.persistent_app")
        # Pool admission: claimed synchronously, finished in the background.
        self._pool_lock = asyncio.Lock()
        self._pool_claim: Optional[str] = None
        self._pool_claim_generation: Optional[str] = None
        self._pool_claim_token: Optional[str] = None
        self._pool_task: Optional[asyncio.Task[None]] = None
        self._startup_task: Optional[asyncio.Task[None]] = None
        # Exact proof retained between exception-safe attach rollback and the
        # orchestrator's generation-rotating release CAS. Never inferred from
        # a swallowed cleanup error or an absent session.
        self._release_receipt: Optional[dict[str, Any]] = None
        self._release_restore_thread_id: Optional[str] = None
        # Mutable only while one exact delivered attach is constructing: the
        # one-way setup boundary plus the attested workspace coordinates that
        # prove a partial sandbox runtime writer-free. Never logged (it holds
        # transport paths).
        self._cleanup_context: Optional[dict[str, Any]] = None
        # The newest heartbeat's fan-out advertisement for a pinned session
        # (batch settle, switch), held until the next turn start
        # (parallel_subagents.md §14.2 P5). None = nothing to apply.
        self._heartbeat_subagent_advertisement: Optional[tuple[Any, Any]] = None
        # Background reports of the sidecar cloud folders' state (D7), held
        # so the loop does not drop them mid-flight.
        self._report_tasks: set[asyncio.Task[None]] = set()

    # --- Views ----------------------------------------------------------------

    @property
    def _identity(self) -> SessionIdentityRuntime:
        return self._ports.identity()

    @property
    def _input(self) -> SessionInputRuntime:
        return self._ports.input_runtime()

    @property
    def _client(self) -> Any:
        return self._ports.orchestrator_client()

    @property
    def _agent(self) -> Any:
        return self._ports.agent()

    @property
    def _session(self) -> Any:
        return self._ports.session()

    @property
    def pool_claim(self) -> tuple[Optional[str], Optional[str], Optional[str]]:
        return (self._pool_claim, self._pool_claim_generation, self._pool_claim_token)

    @property
    def pool_task(self) -> Optional[asyncio.Task[None]]:
        return self._pool_task

    @property
    def release_receipt(self) -> Optional[dict[str, Any]]:
        return self._release_receipt

    @property
    def cleanup_context(self) -> Optional[dict[str, Any]]:
        return self._cleanup_context

    # --- Workspace reads of one attach -----------------------------------------

    def _ending_fence(self) -> dict[str, Any]:
        """A pinned attach stops at its life's first ending refusal.

        Stateless attaches keep the historical reads: their claim's lease is
        the authority the End fences.
        """

        return {} if self._ports.stateless_mode() else {"raise_on_ending": True}

    def _ending_outcome(self, ending: SessionEnding) -> BaseException:
        """Classify an ending refusal against this attach's own generation."""

        own = self._identity.session_generation
        named = canonical_runtime_generation(ending.runtime_generation)
        if named is not None and own is not None and named != own:
            self._logger.warning(
                "Attach superseded while waiting for its workspace "
                "(thread=%s): another session life is ending",
                self._identity.thread_id,
            )
            return WorkspaceNotReady(
                "Workspace runtime generation changed during attach"
            )
        self._logger.info(
            "Session life retirement began before the attach completed "
            "(thread=%s disposition=%s) — stopping the attach",
            self._identity.thread_id,
            ending.retirement_disposition,
        )
        return ending

    async def _read_workspace(self, thread_id: str) -> Any:
        """One workspace read of this attach, fenced by the ending refusal."""

        try:
            return await self._client.get_thread_workspace(
                thread_id, **self._ending_fence()
            )
        except SessionEnding as ending:
            outcome = self._ending_outcome(ending)
            if outcome is ending:
                raise
            raise outcome from ending

    async def _report_sidecar_mounts(
        self, watcher: Any, report: list[dict[str, Any]]
    ) -> None:
        """Publish the sidecar cloud folders' state (D7): a cockpit event at
        once, and the orchestrator's record for this Pod in the background,
        so a slow orchestrator never holds the attach. Never raises."""
        self._ports.broadcast(
            "cloud_mount.status",
            {
                "mounts": report,
                "excluded": list(watcher.excluded),
                "protected": bool(watcher.cloud_cfg.get("protected")),
            },
        )
        thread_id = self._identity.thread_id
        report_status = getattr(self._client, "report_cloud_mount_status", None)
        if not (
            thread_id
            and watcher.fingerprint
            and watcher.runtime_incarnation
            and callable(report_status)
        ):
            return

        async def send() -> None:
            try:
                accepted = await report_status(
                    str(thread_id),
                    fingerprint=watcher.fingerprint,
                    pod_uid=watcher.runtime_incarnation,
                    mounts=report,
                )
                if not accepted:
                    self._logger.warning(
                        "The orchestrator did not keep the cloud folders' state"
                    )
            except Exception as exc:
                self._logger.warning(
                    "Could not report the cloud folders' state: %s",
                    type(exc).__name__,
                )

        task = asyncio.create_task(send(), name="cloud-mount-status-report")
        self._report_tasks.add(task)
        task.add_done_callback(self._report_tasks.discard)

    async def _poll_workspace(self, thread_id: str, **kwargs: Any) -> Any:
        """The readiness poll of this attach, fenced by the ending refusal."""

        try:
            return await self._ports.poll_workspace_ready(
                self._client,
                thread_id,
                session_runtime_generation=self._identity.session_generation,
                **kwargs,
                **self._ending_fence(),
            )
        except SessionEnding as ending:
            outcome = self._ending_outcome(ending)
            if outcome is ending:
                raise
            raise outcome from ending

    def retain_release_receipt(self, receipt: dict[str, Any]) -> bool:
        """Install one immutable attach-abort proof without replacing a claimant.

        Dual mode can prove that actor binding failed before its one-way setup latch
        flipped; the normal attach rollback installs a stronger process-zero proof.
        In both cases a delayed failure from G1 must never replace G2's retained
        proof, so equality is benign/idempotent and every other incumbent wins.
        """

        if not isinstance(receipt, dict):
            return False
        incumbent = self._release_receipt
        if incumbent is not None:
            return incumbent == receipt
        self._release_receipt = dict(receipt)
        return True

    def pool_heartbeat_status(self) -> str:
        """Only an unbound process with no pending attach may advertise idle."""

        return (
            "ready"
            if self._identity.thread_id is None
            and (self._startup_task is None or self._startup_task.done())
            and self._session is None
            and self._pool_claim is None
            and self._ports.pending_drain_suspend() is None
            and self._release_receipt is None
            else "session"
        )

    def apply_subagent_advertisement(
        self, batch_settle_contract: Any, fanout: Any
    ) -> bool:
        """Re-apply one claim's fan-out advertisement to the attached session.

        The stateless executor calls this at every claim: a warm session skips
        attach, and the orchestrator's operator switch must still reach it at
        once (parallel_subagents.md §12). Returns True when a value changed.
        """

        session = self._session
        if session is None:
            return False
        batch_settle = subagent_batch_settle_advertised(batch_settle_contract)
        fanout_allowed = subagent_fanout_advertised(fanout)
        changed = session.apply_subagent_fanout_advertisement(
            batch_settle_contract=batch_settle,
            fanout=fanout_allowed,
        )
        if changed:
            self._logger.info(
                "Session delegation advertisement re-applied: thread=%s "
                "batch_settle=%s fanout=%s",
                self._identity.thread_id,
                batch_settle,
                fanout_allowed,
            )
        return changed

    def hold_heartbeat_subagent_advertisement(self, response: Any) -> None:
        """Hold a heartbeat response's fan-out advertisement for the next turn.

        A pinned runtime claims its inputs from Postgres, so the heartbeat is
        the only orchestrator response a running pinned session receives
        (parallel_subagents.md §14.2 P5). It carries both keys for a pinned
        session; an older orchestrator, or one answering for an agent with no
        pinned thread, sends neither, and that means no change, never off.
        The newest response replaces an older held one. Nothing is applied
        here: the switch must not move under a running turn.
        """

        if not isinstance(response, dict):
            return
        if (
            SESSION_SUBAGENT_BATCH_SETTLE_CONTRACT_KEY not in response
            or SESSION_SUBAGENT_FANOUT_KEY not in response
        ):
            return
        self._heartbeat_subagent_advertisement = (
            response[SESSION_SUBAGENT_BATCH_SETTLE_CONTRACT_KEY],
            response[SESSION_SUBAGENT_FANOUT_KEY],
        )

    def apply_heartbeat_subagent_advertisement(self) -> bool:
        """Apply the held heartbeat advertisement once, at a pinned turn start.

        Called before the turn reads its tool binding, so a value that arrives
        mid-turn reaches the next turn and never the running one; a batch
        already running is not stopped. The stateless lane never takes it:
        its claim bundle is the per-claim authority. Returns True when a value
        changed (``apply_subagent_advertisement`` logs the change).
        """

        held = self._heartbeat_subagent_advertisement
        if held is None or self._session is None:
            return False
        self._heartbeat_subagent_advertisement = None
        if self._ports.stateless_mode():
            return False
        return self.apply_subagent_advertisement(*held)

    def runtime_actor_for_attach(
        self,
        payload: Optional[Dict[str, Any]],
    ) -> RuntimeActorContext | None:
        """Resolve one actor object shared by maintenance and every session tool."""

        actor = RuntimeActorContext.from_payload(payload)
        if payload is not None and actor is None:
            raise RuntimeError("Malformed server-derived runtime actor context")
        client = self._client
        if actor is None and client is not None:
            # Dedicated runtime clients receive the actor during registration.
            actor = getattr(client, "runtime_actor", None)
        elif actor is not None and client is not None:
            # Pool/stateless attach receives its actor in the server payload. The
            # heartbeat maintenance channel and the session/tool bindings must
            # share this exact mutable object so a rotation cannot leave tools on
            # the predecessor bearer.
            adopt = getattr(client, "adopt_runtime_actor", None)
            if callable(adopt):
                adopt(actor)
            else:  # deliberately tiny dry-run/test adapters
                client.runtime_actor = actor
        return actor

    def clear_runtime_actor(self) -> None:
        """Drop project authority at the common session teardown boundary."""

        client = self._client
        clear = getattr(client, "clear_runtime_actor", None) if client else None
        if callable(clear):
            clear()

    @property
    def startup_task(self) -> asyncio.Task[None] | None:
        """Startup construction or its uncertain cleanup remains tracked."""
        return self._startup_task

    def start_dedicated_attach(
        self, thread_id: str, *, on_failure: Callable[[str, Exception], Awaitable[None]]
    ) -> asyncio.Task[None]:
        """Own construction after registration while process health can be served."""
        if self._startup_task is not None and not self._startup_task.done():
            raise RuntimeError(
                "Dedicated attach construction already owns this process"
            )
        self._startup_task = asyncio.create_task(
            self.run_dedicated_attach(thread_id, on_failure=on_failure),
            name=f"dedicated-session-attach:{thread_id}",
        )
        return self._startup_task

    async def run_dedicated_attach(
        self, thread_id: str, *, on_failure: Callable[[str, Exception], Awaitable[None]]
    ) -> None:
        """Run one exact attach; the termination owner makes failure exit decisions."""
        try:
            await self.attach(thread_id)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await on_failure(thread_id, exc)

    async def stop_startup_attach(self, *, cleanup_timeout: float = 5.0) -> bool:
        """Bound dedicated and pool cleanup; pending work requires recovery proof.

        Both tasks own the same attach transaction. Cancellation requests its
        rollback; only a completed join permits shutdown to touch that session.
        A repeated stop must not inject another cancellation into the rollback.
        """
        tasks = {
            task for task in (self._startup_task, self._pool_task) if task is not None
        }
        if not tasks:
            return True
        pending = {task for task in tasks if not task.done()}
        if pending:
            self._logger.info(
                "Shutting down attach: cancellation requested (dedicated=%s pool=%s)",
                self._startup_task in pending,
                self._pool_task in pending,
            )
        for task in pending:
            if not task.cancelling():
                task.cancel()
        if pending:
            await asyncio.wait(pending, timeout=max(0.0, cleanup_timeout))
        done = {task for task in tasks if task.done()}
        await asyncio.gather(*done, return_exceptions=True)
        if self._startup_task in done:
            self._startup_task = None
        if self._pool_task in done:
            self._pool_task = None
        if done != tasks:
            self._logger.warning(
                "Attach cleanup still owns unproven work; remote quiescence refused"
            )
            return False
        self._logger.info("Shutting down attach: cleanup joined")
        return True

    async def attach(
        self,
        thread_id: str,
        config_override: Optional[Dict[str, Any]] = None,
        resolved_config: Optional[Dict[str, Any]] = None,
        project_ids: Optional[List[str]] = None,
        datasources: Optional[List[Dict[str, Any]]] = None,
        config_name: Optional[str] = None,
        runtime_actor: Optional[Dict[str, Any]] = None,
        pinned_status_identity_contract: Any = None,
        pinned_runtime_generation_contract: Any = None,
        session_runtime_generation: Any = None,
        session_runtime_attach_token: Any = None,
        conversation_revision: Any = None,
        events_epoch: Any = None,
        workspace_generation: Any = ATTACH_WORKSPACE_IDENTITY_UNSET,
        workspace_runtime_incarnation: Any = ATTACH_WORKSPACE_IDENTITY_UNSET,
        session_subagent_batch_settle_contract: Any = None,
        session_subagent_fanout: Any = None,
    ) -> None:
        """Exception-safe attach transaction around the full construction tail."""

        if self._release_receipt is not None:
            raise WorkspaceNotReady("Prior attach release remains unconfirmed")
        previous_thread_id = self._identity.thread_id
        try:
            await self._attach_inner(
                thread_id=thread_id,
                config_override=config_override,
                resolved_config=resolved_config,
                project_ids=project_ids,
                datasources=datasources,
                config_name=config_name,
                runtime_actor=runtime_actor,
                pinned_status_identity_contract=pinned_status_identity_contract,
                pinned_runtime_generation_contract=pinned_runtime_generation_contract,
                session_runtime_generation=session_runtime_generation,
                session_runtime_attach_token=session_runtime_attach_token,
                conversation_revision=conversation_revision,
                events_epoch=events_epoch,
                workspace_generation=workspace_generation,
                workspace_runtime_incarnation=workspace_runtime_incarnation,
                session_subagent_batch_settle_contract=(
                    session_subagent_batch_settle_contract
                ),
                session_subagent_fanout=session_subagent_fanout,
            )
        except BaseException as exc:
            context = self._cleanup_context
            hint = exc.cleanup_identity if isinstance(exc, SessionGrantDenied) else None
            if (
                isinstance(hint, PreSetupWorkspaceIdentity)
                and isinstance(context, dict)
                and context.get("setup_started") is False
                and self._session is None
                and context.get("remote") is None
                and context.get("thread_id")
                == self._identity.thread_id
                == thread_id
                == hint.thread_id
                and self._identity.session_generation == hint.session_runtime_generation
                and context.get("workspace_generation")
                in (None, "", hint.workspace_generation)
                and context.get("workspace_runtime_incarnation")
                in (None, "", hint.workspace_runtime_incarnation)
            ):
                # No construction crossed the monotonic setup boundary. Carry
                # the denied read's exact identifiers into the existing release
                # CAS; they assert no remote process-zero proof.
                context["workspace_generation"] = hint.workspace_generation
                context["workspace_runtime_incarnation"] = (
                    hint.workspace_runtime_incarnation
                )
            # Covers every post-construction await, including event-journal setup,
            # repository/message restore, lifecycle CAS, and input reclamation.
            # The helper is idempotent when the inner setup guard already ran.
            exact_identity = (
                self._identity.session_generation,
                self._identity.attach_token,
            )
            retained_receipt = self._release_receipt
            exact_receipt_exists = bool(
                isinstance(retained_receipt, dict)
                and retained_receipt.get("thread_id") == thread_id
                and retained_receipt.get("session_runtime_generation")
                == exact_identity[0]
                and retained_receipt.get("session_runtime_attach_token")
                == exact_identity[1]
            )
            if not exact_receipt_exists and (
                self._session is not None
                or self._identity.thread_id != previous_thread_id
                or (
                    self._identity.runtime_contract
                    and all(exact_identity)
                    and not exact_receipt_exists
                )
            ):
                await self.cleanup_failed_attach_until_proven(
                    thread_id, restore_thread_id=previous_thread_id
                )
            raise

    async def _attach_inner(
        self,
        thread_id: str,
        config_override: Optional[Dict[str, Any]] = None,
        resolved_config: Optional[Dict[str, Any]] = None,
        project_ids: Optional[List[str]] = None,
        datasources: Optional[List[Dict[str, Any]]] = None,
        config_name: Optional[str] = None,
        runtime_actor: Optional[Dict[str, Any]] = None,
        pinned_status_identity_contract: Any = None,
        pinned_runtime_generation_contract: Any = None,
        session_runtime_generation: Any = None,
        session_runtime_attach_token: Any = None,
        conversation_revision: Any = None,
        events_epoch: Any = None,
        workspace_generation: Any = ATTACH_WORKSPACE_IDENTITY_UNSET,
        workspace_runtime_incarnation: Any = ATTACH_WORKSPACE_IDENTITY_UNSET,
        session_subagent_batch_settle_contract: Any = None,
        session_subagent_fanout: Any = None,
    ) -> None:
        """Create and attach a PersistentSession for the given thread.

        This is the core session setup logic, extracted from the lifespan so it
        can be reused by both dedicated mode (lifespan startup) and pool mode
        (POST /session/attach).

        ``config_name`` (pool mode): the thread's config, used as the session
        base instead of the pod's boot config when provided.
        """

        expected_workspace_identity = canonical_attach_workspace_identity(
            workspace_generation,
            workspace_runtime_incarnation,
        )
        prior_thread_id = self._identity.thread_id

        self._ports.set_cloud_sync_retry_pending(False)
        # A pooled process must never carry a prior Officer's successful
        # maintenance result into the next attachment. Ordinary sessions bypass
        # this latch through ``officer_config() is None`` below.
        self._ports.close_runtime_authorization()
        self._identity.set_status_contract(
            type(pinned_status_identity_contract) is int
            and pinned_status_identity_contract == 1
        )
        runtime_contract_advertised = bool(
            type(pinned_runtime_generation_contract) is int
            and pinned_runtime_generation_contract == 1
        )
        client_generation = (
            getattr(self._client, "session_runtime_generation", None)
            if self._client is not None
            else None
        )
        if not isinstance(client_generation, str):
            client_generation = None
        client_attach_token = (
            getattr(self._client, "session_runtime_attach_token", None)
            if self._client is not None
            else None
        )
        if not isinstance(client_attach_token, str):
            client_attach_token = None
        self._identity.adopt(
            session_runtime_generation
            if session_runtime_generation is not None
            else client_generation,
            session_runtime_attach_token
            if session_runtime_attach_token is not None
            else client_attach_token,
            contract_advertised=(
                runtime_contract_advertised
                or (
                    getattr(
                        self._client,
                        "pinned_runtime_generation_contract",
                        False,
                    )
                    is True
                )
            ),
        )
        self._ports.clear_canvas()

        if self._session is not None:
            raise RuntimeError(
                f"Cannot attach thread {thread_id}: already attached to {self._identity.thread_id}"
            )
        if self._cleanup_context is not None:
            raise RuntimeError(
                "Cannot attach while a prior runtime cleanup proof remains pending"
            )

        if self._ports.side_tasks_active():
            raise RuntimeError(
                "Cannot attach a new thread while prior session tasks remain active"
            )
        self._identity.begin_attach()
        self._ports.reset_draft_title()

        # A normal detach always closes and clears the prior writer. Recover from a
        # stale writer defensively before a pool-mode reattach so no event can land
        # under the previous thread/pool identity.
        stale_writer = self._ports.event_writer()
        if stale_writer is not None:
            self._logger.warning(
                "Closing stale thread_events writer before attaching thread %s",
                thread_id,
            )
            await stale_writer.close()
            self._ports.discard_event_writer()

        self._identity.bind_thread(thread_id)
        if self._identity.runtime_contract and not self._ports.stateless_mode():
            self._cleanup_context = {
                "thread_id": thread_id,
                "setup_started": False,
                "workspace_tier": None,
                "workspace_generation": None,
                "workspace_runtime_incarnation": None,
                "workspace_ssh_host_key_fingerprint": None,
                "remote": None,
                "datasources": {},
                "datasource_clients": {},
            }

        runtime_actor_context = self.runtime_actor_for_attach(runtime_actor)

        # Determine the backend before polling: a lite (virtual/none) session has
        # NO workspace pod, so polling for one would always fail (WorkspaceNotReady).
        # The pool path passes config_override; a dedicated agent fetches it here.
        # The orchestrator attaches the lite object-store mounts to this response
        # for lite threads, so the session can build its backend without a pod.
        _rc, _co = resolved_config, config_override
        attached_workspace_generation = ""
        # Every ready workspace response this attach reads, oldest first: a
        # pinned pod that attaches itself takes the fan-out advertisement from
        # the newest one (session_subagent_advertisement).
        subagent_workspace_responses: List[Any] = []
        if _rc is None and _co is None and self._client and self._identity.thread_id:
            try:
                _peek = await self._read_workspace(self._identity.thread_id)
                if isinstance(_peek, dict):
                    peek_delivery = protected_workspace_delivery(_peek)
                    # A valid engaging response is intentionally coordinate- and
                    # credential-free, including the runtime generation.  It is a
                    # poll instruction, not an attach payload: validating ready
                    # identity here would make the dedicated path fail before
                    # the readiness poll can observe engage -> ready.
                    if peek_delivery != "engaging":
                        assert_attach_workspace_payload(
                            expected_workspace_identity,
                            _peek,
                        )
                        self._identity.adopt_workspace_payload(
                            _peek,
                            protected_required=(
                                protected_workspace_marker(_peek) == "on"
                            ),
                        )
                        self._identity.set_status_contract(
                            pinned_status_identity_advertised(_peek)
                        )
                        subagent_workspace_responses.append(_peek)
                        attached_workspace_generation = str(
                            _peek.get("workspace_generation") or ""
                        )
                    _rc = _peek.get("resolved_config")
                    _co = _peek.get("config_override")
            except (ProtectedCloudUnavailable, SessionEnded, WorkspaceNotReady):
                raise
            except Exception:
                pass
        # Check BOTH blobs: the resolved config is the agent's preferred hydration
        # source, the override is the authoritative tier — either may carry it.
        is_lite_session = session_backend_is_lite(_rc) or session_backend_is_lite(_co)
        # Same dual-blob read for the VM tier: a vm-tier session must attach to its
        # VM and never to a container that happens to be ready (Defect 2).
        is_vm_session = session_backend_is_vm(_rc) or session_backend_is_vm(_co)
        assert_attach_workspace_tier(
            expected_workspace_identity,
            is_lite_session=is_lite_session,
        )
        if self._cleanup_context is not None:
            self._cleanup_context["workspace_tier"] = (
                "vm" if is_vm_session else "virtual" if is_lite_session else "sandbox"
            )

        # Wait for workspace container (if orchestrator is provisioning one).
        # Skipped for lite tiers, which run with no pod — the session builds its
        # object-store backend from the injected mounts (persistent_session.py).
        workspace_override = None
        if not is_lite_session and self._client and self._identity.thread_id:
            workspace_override = await self._poll_workspace(
                self._identity.thread_id,
                timeout=120,
                raise_on_denied=True,
                require_vm=is_vm_session,
            )
            if workspace_override:
                assert_attach_workspace_payload(
                    expected_workspace_identity,
                    workspace_override,
                )
                self._identity.adopt_workspace_payload(
                    workspace_override,
                    protected_required=(
                        protected_workspace_marker(workspace_override) == "on"
                    ),
                )
                self._identity.set_status_contract(
                    pinned_status_identity_advertised(workspace_override)
                )
                subagent_workspace_responses.append(workspace_override)
                self._logger.info(
                    f"Workspace ready ({workspace_override.get('backend')}): "
                    f"{workspace_override['remote']['host']}"
                )
                if self._cleanup_context is not None:
                    remote = workspace_override.get("remote")
                    self._cleanup_context.update(
                        {
                            "workspace_tier": workspace_override.get("backend"),
                            "workspace_generation": workspace_override.get(
                                "workspace_generation"
                            ),
                            "workspace_runtime_incarnation": workspace_override.get(
                                "workspace_runtime_incarnation"
                            ),
                            "workspace_ssh_host_key_fingerprint": workspace_override.get(
                                "workspace_ssh_host_key_fingerprint"
                            ),
                            "remote": dict(remote)
                            if isinstance(remote, dict)
                            else None,
                        }
                    )
            elif is_vm_session:
                # Never silently downgrade a vm-tier session to a container. Say what
                # actually failed so the pod log names the real cause instead of
                # blaming a container this session was never supposed to have.
                raise WorkspaceNotReady(
                    "VM workspace never became ready for this vm-tier session "
                    "(metadata.vm did not reach status='ready' with an ssh_host "
                    "within the VM budget). Not falling back to a sandbox container."
                )
            else:
                raise WorkspaceNotReady(
                    "No workspace container provisioned for thread. "
                    "Cannot attach session without an isolated workspace."
                )
        elif is_lite_session:
            self._logger.info(
                "Lite (no-pod) session for thread %s — skipping workspace poll",
                self._identity.thread_id,
            )

        attached_workspace_generation = str(
            (workspace_override or {}).get("workspace_generation")
            or attached_workspace_generation
            or ""
        )

        # Apply config overrides, project_ids, and datasources from thread metadata
        if not config_override:
            config_override = (workspace_override or {}).get("config_override")
        if resolved_config is None:
            resolved_config = (workspace_override or {}).get("resolved_config")
        if not project_ids:
            project_ids = (workspace_override or {}).get("project_ids") or []
        cloud_mount_cfg = (
            cloud_mount_payload(workspace_override) if workspace_override else None
        )
        # Protected state is an exact three-way contract.  Always perform a fresh
        # fetch before constructing PersistentSession: an engage can be revoked or
        # fail after the readiness poll, and stale credentials must not win merely
        # because the first response already populated every optional field.
        initial_delivery = protected_workspace_delivery(workspace_override or {})
        protected_cloud = initial_delivery == "ready"
        protected_identity = (
            protected_workspace_identity(workspace_override)
            if protected_cloud and workspace_override is not None
            else None
        )
        if protected_cloud:
            self._identity.adopt_workspace_payload(
                workspace_override,
                protected_required=True,
            )
        if self._client and self._identity.thread_id:
            try:
                ws_info = await self._read_workspace(self._identity.thread_id)
                if ws_info:
                    assert_attach_workspace_payload(
                        expected_workspace_identity,
                        ws_info,
                    )
                    self._identity.adopt_workspace_payload(
                        ws_info,
                        protected_required=(
                            protected_cloud
                            or protected_workspace_marker(ws_info) == "on"
                        ),
                    )
                    self._identity.set_status_contract(
                        pinned_status_identity_advertised(ws_info)
                    )
                    subagent_workspace_responses.append(ws_info)
                    fresh_delivery = protected_workspace_delivery(ws_info)
                    if fresh_delivery == "engaging":
                        raise ProtectedCloudUnavailable(
                            "protected-cloud engage changed while attaching"
                        )
                    if protected_cloud and fresh_delivery != "ready":
                        raise ProtectedCloudUnavailable(
                            "protected-cloud authority disappeared while attaching"
                        )
                    protected_cloud = fresh_delivery == "ready"
                    attached_workspace_generation = (
                        attached_workspace_generation
                        or str(ws_info.get("workspace_generation") or "")
                    )
                    if not config_override:
                        config_override = ws_info.get("config_override")
                    if resolved_config is None:
                        resolved_config = ws_info.get("resolved_config")
                    if not project_ids:
                        project_ids = ws_info.get("project_ids") or []
                    if not datasources:
                        datasources = ws_info.get("datasources")
                    if protected_cloud:
                        # Latest authoritative bytes replace, rather than fill, an
                        # earlier mount so a revoked/rotated reader cannot be used.
                        cloud_mount_cfg = protected_mount_payload(ws_info)
                        fresh_identity = protected_workspace_identity(ws_info)
                        if fresh_identity != protected_identity:
                            raise ProtectedCloudUnavailable(
                                "protected workspace identity changed before setup"
                            )
                    elif not cloud_mount_cfg:
                        cloud_mount_cfg = cloud_mount_payload(ws_info)
                elif protected_cloud:
                    raise ProtectedCloudUnavailable(
                        "protected-cloud workspace authority is unavailable"
                    )
            except (ProtectedCloudUnavailable, SessionEnded, WorkspaceNotReady):
                raise
            except Exception:
                if protected_cloud:
                    raise ProtectedCloudUnavailable(
                        "protected-cloud workspace revalidation failed"
                    )

        # config_override is final here (request > workspace_override > ws_info)
        # and caller-authored on every one of those routes. Strip loader-owned
        # keys ONCE, before the deep-merge onto config.extra further down: a
        # thread override carrying ``_db_prompt_keys: []`` must not unfence the
        # expert's DB prompts (security audit 2026-08-27, finding #2).
        if isinstance(config_override, dict):
            from shared.runtime.core.loader import strip_loader_owned_keys

            config_override = strip_loader_owned_keys(config_override)

        # Crossing this one-way boundary means config/datasource/session setup may
        # have created local or remote actors. Any later delivered-attach abort
        # must prove actual agent/workspace process zero; it can never downgrade
        # to `agent_attach_not_started_v1` even if construction fails before
        # PersistentSession is assigned.
        if self._cleanup_context is not None:
            self._cleanup_context["setup_started"] = True

        # Connectors, harness phase: managed connections and MCP discovery,
        # before the tool set is resolved below. The workspace phase
        # (environment, repository checkouts onto the workspace backend,
        # never the agent pod) runs once the workspace is initialized.
        from agent.connectors import (
            RuntimeContext,
            connector_registry,
            deliveries_from_payload,
        )

        connector_deliveries = deliveries_from_payload(datasources)
        connector_runtime = RuntimeContext(execution="session")
        datasources_dict = connector_runtime.connections
        datasource_clients = connector_runtime.clients
        mcp_manager = None
        if datasources:
            from agent.core.datasource_setup import datasource_tool_categories

            if self._cleanup_context is not None:
                self._cleanup_context["datasources"] = datasources_dict
                self._cleanup_context["datasource_clients"] = datasource_clients
            _t_step = time.perf_counter()
            await connector_registry().attach_harness(
                connector_deliveries, connector_runtime
            )
            self._logger.info(
                "attach step: connector harness %.2fs", time.perf_counter() - _t_step
            )
            from agent.connectors.mcp import MCP_SLOT

            mcp_manager = datasources_dict.get(MCP_SLOT)

            # Inject datasource tool categories so the correct tools are loaded
            # when config is resolved below. Shared map with the orchestrator's
            # _build_datasource_tool_override — the two previously disagreed on
            # read-write managed connectors.
            ds_tool_categories = datasource_tool_categories(datasources)
            config_override = dict(config_override or {})
            tools_override = dict(config_override.get("tools", {}))
            tools_override.update(ds_tool_categories)
            if tools_override:
                config_override["tools"] = tools_override

            # Hydrated attaches load the orchestrator-resolved blob below and
            # never touch config_override — fold the same enrichment into the
            # blob's agent dict, or a hydrated attach silently drops read-only
            # connector tools. The warm-pool path compensated
            # orchestrator-side; the dedicated-pod path did not
            # (live_session_settings.md P0.2).
            apply_datasource_enrichment_to_resolved(resolved_config, ds_tool_categories)

            self._logger.info(
                "Processed %d datasource(s) for session: %d connections",
                len(datasources),
                len(datasources_dict),
            )

        # Pool-mode agents serve sequential sessions. Replace (or clear) the
        # process-global dynamic entries before config hydration/tool loading.
        self._ports.register_mcp_tools(mcp_manager)

        effective_config = self._agent.config
        _hydrated = False
        if resolved_config:
            # Orchestrator-resolved config: the blob is the full, frozen,
            # credential-injected session config (base + expert + overrides). Hydrate
            # it directly — no config_name load, no config_override flat-merge (which
            # would degrade the resolved layers). This is the warm-pool / cold-attach
            # expert delivery channel — the fix for the 3-minute stall.
            from shared.runtime.core.loader import create_llm, load_config_from_resolved

            effective_config = load_config_from_resolved(resolved_config)
            _hydrated = True
            self._logger.info(
                "Attach: hydrated orchestrator-resolved config for thread %s "
                "(model=%s, persona_source=%s)",
                thread_id,
                effective_config.llm.model,
                effective_config.extra.get("_persona_source"),
            )
        elif config_name:
            # The thread's config beats the pod's boot config — idle-pool pods
            # boot as workers, and a session served from the worker YAML loses
            # its persistent memory pipeline (no teardown_extractor) among the
            # rest of the session profile. Fail-loud on unknown names.
            effective_config = self._ports.load_expert_config(config_name)
            self._logger.info(
                "Attach: session base config '%s' (overrides pod boot config)",
                config_name,
            )

        llm = self._agent._llm
        if _hydrated:
            # The resolved llm carries the final model + injected transport.
            llm = create_llm(
                self._ports.llm_config_with_cache_key(effective_config.llm),
                effective_config.limits,
            )
            self._logger.info(
                "Attach: built session LLM from resolved config: model=%s",
                effective_config.llm.model,
            )
        elif config_override:
            import dataclasses

            from shared.runtime.core.loader import (
                _apply_settings_matrix,
                create_llm,
                deep_merge,
                load_agent_config_from_dict,
            )

            # The legacy (experts-off) attach path reads the RAW request override
            # rather than the orchestrator's merged fragment, so it needs its own
            # normalisation — otherwise `canvas: false` never becomes the `[]` that
            # _apply_session_tool_group_markers matches on, and the group stays on.
            # Same seam for a legacy llm.strategic/tactical/subagent block.
            config_override = normalize_delegation_block(
                normalize_llm_tiers(
                    normalize_tool_policy(config_override, source="thread-override"),
                    source="thread-override",
                ),
                source="thread-override",
            )
            base_dict = dataclasses.asdict(effective_config)
            merged = deep_merge(base_dict, config_override)
            self._ports.apply_session_tool_group_markers(merged, config_override)

            # If the override changes the model, re-apply settings_matrix for the
            # new model family so temperature/top_p/limits get correct defaults.
            # Override LLM keys are treated as "explicitly set" so the matrix
            # won't overwrite them.
            if config_override.get("llm"):
                override_llm_keys = set(config_override["llm"].keys())
                _apply_settings_matrix(
                    merged, override_llm_keys, effective_config._deployment_dir
                )

            effective_config = load_agent_config_from_dict(
                merged, deployment_dir=effective_config._deployment_dir
            )
            if config_override.get("llm"):
                llm = create_llm(
                    self._ports.llm_config_with_cache_key(effective_config.llm),
                    effective_config.limits,
                )
                self._logger.info(
                    f"Config override applied: model={effective_config.llm.model}, "
                    f"temperature={effective_config.llm.temperature}"
                )

        # Task 15: thread protected_cloud into config.extra (loader.py reads
        # config.extra["_protected_cloud"] at render time), so the interactive
        # prompt's honesty block renders for this session. Applied once, after
        # effective_config is fully resolved (hydrated / config_override-merged /
        # config_name-loaded / plain boot config) rather than folded into the
        # config_override merge above — pushing it through config_override would
        # make an otherwise-empty override truthy and force every protected
        # thread through the `elif config_override:` deep-merge/rebuild branch
        # even when no other override exists.
        #
        # NEVER mutate effective_config in place here: on the plain-boot path
        # (no resolved_config / config_name / config_override) effective_config
        # IS the module-singleton self._agent.config, which pool-mode pods reuse
        # across sequential session attaches — an in-place write would leak
        # _protected_cloud into every later NON-protected session on the pod
        # (whose live cloud files really are saved, making the honesty block a
        # lie). Clone via dataclasses.replace with a copied extra dict instead;
        # the new object is assigned back to the local, so all downstream use
        # in this function picks it up. The guards skip test stubs that aren't
        # real AgentConfig dataclasses.
        import dataclasses

        if (
            protected_cloud
            and hasattr(effective_config, "extra")
            and dataclasses.is_dataclass(effective_config)
        ):
            effective_config = dataclasses.replace(
                effective_config,
                extra={**effective_config.extra, "_protected_cloud": True},
            )

        # Auxiliary LLM rebuild. The boot-time self._agent._auxiliary_llm is built from
        # config.auxiliary.model in the YAML default — for persistent sessions
        # without an override that's RedHatAI/... with no transport, which routes
        # title-generation/memory-extraction calls to api.openai.com with
        # not-needed and 401s. When the orchestrator's create_thread injection
        # (or a runtime config.update) supplies an auxiliary section, build a
        # session-scoped AuxiliaryLLM and pass it in instead of the singleton.
        auxiliary_llm = self._agent._auxiliary_llm
        if (config_override and config_override.get("auxiliary", {}).get("model")) or (
            _hydrated
            and effective_config.auxiliary
            and effective_config.auxiliary.model
        ):
            from shared.runtime.core.loader import (
                create_auxiliary_llms,
                resolve_model_settings,
            )
            from shared.runtime.services.auxiliary import AuxiliaryLLM

            aux_cfg = effective_config.auxiliary
            model_settings = resolve_model_settings(
                aux_cfg.model, effective_config._deployment_dir
            )
            aux_structured_output_method = model_settings.get(
                "structured_output_method", "json_schema"
            )
            fallback_model = effective_config.llm.model
            fallback_settings = resolve_model_settings(
                fallback_model, effective_config._deployment_dir
            )
            aux_clients = create_auxiliary_llms(
                aux_cfg, model_settings, effective_config.limits
            )
            auxiliary_llm = AuxiliaryLLM(
                llm=aux_clients.llm,
                summarization_llm=aux_clients.summarization_llm,
                max_iterations=aux_cfg.max_iterations,
                timeout=aux_cfg.timeout,
                max_context_tokens=model_settings.get("model_max_context_tokens"),
                structured_output_method=aux_structured_output_method,
                # Drop-in fallback to the main session model when the dedicated aux
                # model is unreachable — keeps compaction/memory/titles alive instead
                # of crashing the session. See
                # knowledge-base/knowledge/issues/openrouter_auxiliary_misrouted_to_openai.md.
                fallback_llm=llm,
                fallback_structured_output_method=fallback_settings.get(
                    "structured_output_method", "json_schema"
                ),
            )
            self._logger.info(
                "Auxiliary override applied: model=%s, base_url=%s",
                aux_cfg.model,
                aux_cfg.base_url or "default",
            )

        # Embedding override + scrub-on-claim (§5.6): replace the process-wide
        # embedding profile (memory + KB) with this attach's snapshot — pop-first
        # on BOTH paths, singleton nulled unconditionally. Extracted to a helper
        # so the tenant-A→tenant-B residue acceptance is unit-testable.
        _env_keys_src = (
            (effective_config.extra or {}).get("env_keys")
            if _hydrated
            else (config_override.get("env_keys") if config_override else None)
        )
        apply_session_embedding_env(_env_keys_src)

        knowledge_bindings = connector_registry().knowledge_bindings(
            connector_deliveries,
            project_ids=project_ids or [],
            runtime_actor=runtime_actor_context,
        )

        # Create PersistentSession
        live_lease = self._ports.lease()
        shell_owner_token = None
        if live_lease is not None and live_lease.active:
            if str(live_lease.unit_id) != str(self._identity.thread_id):
                raise RuntimeError(
                    "Stateless lease identity does not match the session being attached"
                )
            shell_owner_token = live_lease.lease_token

        # parallel_subagents.md §12: fan-out needs an orchestrator that can settle
        # an interrupted batch (exact int 1, like the other contracts) and its
        # operator switch for this lane (a literal true).
        subagent_batch_settle, subagent_fanout = session_subagent_advertisement(
            session_subagent_batch_settle_contract,
            session_subagent_fanout,
            tuple(reversed(subagent_workspace_responses)),
            from_workspace=not self._ports.stateless_mode(),
        )
        # This attach's advertisement supersedes one a heartbeat held for an
        # earlier binding of this process (P5); the next heartbeat brings the
        # current value again.
        self._heartbeat_subagent_advertisement = None
        self._logger.info(
            "Session delegation advertisement: thread=%s batch_settle=%s "
            "fanout=%s source=%s",
            self._identity.thread_id,
            subagent_batch_settle,
            subagent_fanout,
            "claim" if self._ports.stateless_mode() else "attach",
        )
        session = self._ports.session_factory(
            thread_id=self._identity.thread_id,
            config=effective_config,
            shell_owner_token=shell_owner_token,
            protected_cloud_required=protected_cloud,
            pinned_runtime_identity_required=bool(
                self._identity.runtime_contract and shell_owner_token is None
            ),
            orchestrator_client=self._client,
            session_parent_authority_provider=self._identity.parent_authority,
            subagent_provider_admission=self._ports.provider_admission,
            subagent_effect_authority=self._ports.effect_authority,
            subagent_settlement_authority=(self._ports.settlement_authority),
            # Bound to this life: a child of it never judges a later attach.
            subagent_retirement_authorized=partial(
                self._ports.retirement_authorized,
                self._identity.retirement_identity(),
            ),
            subagent_event_callback=self._ports.subagent_event_available,
            subagent_batch_settle_contract=subagent_batch_settle,
            subagent_fanout=subagent_fanout,
            project_ids=project_ids or [],
            datasources=datasources_dict,
            knowledge_bindings=knowledge_bindings,
            runtime_actor=runtime_actor_context,
            _datasource_clients=datasource_clients,
            # Raw payload kept as the live-change diff baseline (Slice B).
            datasource_configs=list(datasources or []),
        )
        self._ports.publish_session(session)
        self._session.execution_snapshot = (resolved_config or {}).get(
            "execution_snapshot"
        )
        # PersistentSession now owns every local/remote cleanup handle. The
        # construction-only context must not survive into pool reuse.
        self._cleanup_context = None
        if protected_identity is not None:
            self._session.protected_workspace_generation = (
                protected_identity.workspace_generation
            )
            self._session.protected_workspace_runtime_incarnation = (
                protected_identity.runtime_incarnation
            )
        if project_ids:
            self._logger.info(
                f"Session scoped to {len(project_ids)} project(s): {project_ids}"
            )
        git_remote_url = (
            workspace_override.get("git_remote_url") if workspace_override else None
        )
        _t_step = time.perf_counter()
        try:
            await self._session.setup(
                llm=llm,
                auxiliary_llm=auxiliary_llm,
                postgres_conn=self._agent.postgres_conn,
                vector_conn=getattr(self._agent, "vector_conn", None),
                workspace_override=workspace_override,
                git_remote_url=git_remote_url,
                cloud_mount_cfg=cloud_mount_cfg,
            )
            if protected_cloud:
                if self._client is None or self._identity.thread_id is None:
                    raise ProtectedCloudUnavailable(
                        "protected-cloud workspace cannot be revalidated"
                    )
                final_workspace = await self._read_workspace(self._identity.thread_id)
                if not isinstance(final_workspace, dict):
                    raise ProtectedCloudUnavailable(
                        "protected-cloud workspace authority is unavailable"
                    )
                assert_attach_workspace_payload(
                    expected_workspace_identity,
                    final_workspace,
                )
                self._identity.adopt_workspace_payload(
                    final_workspace,
                    protected_required=True,
                )
                if protected_workspace_delivery(final_workspace) != "ready":
                    raise ProtectedCloudUnavailable(
                        "protected-cloud workspace is no longer ready"
                    )
                final_mount = protected_mount_payload(final_workspace)
                final_identity = protected_workspace_identity(final_workspace)
                if (
                    final_identity != protected_identity
                    or final_mount != cloud_mount_cfg
                    or not self._session.protected_cloud_ready()
                ):
                    raise ProtectedCloudUnavailable(
                        "protected-cloud mount authority changed during setup"
                    )
        except BaseException as exc:
            if not isinstance(exc, asyncio.CancelledError):
                # Cleanup can replace this error or wait indefinitely for proof.
                # Retain only its class; messages/tracebacks can carry credentials.
                self._logger.warning(
                    "Session attach failed before cleanup "
                    "(thread=%s stage=session_setup type=%s)",
                    thread_id,
                    type(exc).__name__,
                )
            await self.cleanup_failed_attach(
                thread_id, restore_thread_id=prior_thread_id
            )
            raise
        self._logger.info(
            "attach step: session.setup %.2fs", time.perf_counter() - _t_step
        )
        # Install the lifecycle provider fence before restore/attach can invoke
        # compaction or any other auxiliary model. Turn-complete and hot-swap paths
        # call the same idempotent wiring helper again for rebuilt instances.
        self._ports.wire_aux_archiver()

        # Live citation-verdict push: let the engine's background verifier broadcast
        # pending→verified/failed so the cockpit citations panel updates in place
        # rather than only at the next per-turn refresh. Set before the first turn
        # (so it's wired before the lazily-built CitationEngine is first used).
        if self._session is not None and self._session.tool_context is not None:
            self._session.tool_context.citation_verdict_callback = (
                self._ports.emit_citation_verdict
            )
            self._session.tool_context.canvas_event_callback = (
                self._ports.emit_canvas_event
            )

        # Resolve the authoritative (generation, seq seed) before the first
        # broadcast. Clean reattaches REUSE the thread's current epoch with the
        # seq counter seeded above every previously served frame, so cached client
        # cursors stay valid and no cache-wipe cascade fires; the epoch bumps only
        # when the previous session life is terminal (see
        # _resolve_event_journal_epoch). A provisioning SSE opened against a
        # pre-bump generation uses the existing mid-stream epoch-change
        # reconciliation path.
        self._ports.reset_turn_state()
        self._ports.reset_journal_cursor()
        if self._session is not None and self._session.postgres_conn is not None:
            try:
                await self._ports.open_event_journal()
            except Exception as exc:
                self._logger.error(
                    "Event journal initialization failed; aborting session attach "
                    "(thread=%s): %s",
                    self._identity.thread_id,
                    exc,
                    exc_info=True,
                )
                await self.cleanup_failed_attach(thread_id)
                if isinstance(exc, EventJournalUnavailable):
                    raise
                raise EventJournalUnavailable(
                    "Persistent event journal initialization failed"
                ) from exc

        cloud_mount_active = bool(
            self._session.cloud_mount_manager
            and self._session.cloud_mount_manager.active
        )

        # Connectors, workspace phase: the environment file, then repository
        # checkouts (all clone/auth operations run on the workspace backend;
        # there is no agent-local clone path,
        # knowledge-base/knowledge/features/no_workspace_agent_mode.md §9.4).
        connector_runtime.workspace_manager = self._session.workspace_manager
        connector_runtime.ssh_identity_status = getattr(
            self._session, "workspace_ssh_identity_status", None
        )
        await connector_registry().attach_workspace(
            connector_deliveries, connector_runtime
        )

        # README.md workspace-facts block (connectors, materials, layout) — after
        # the workspace is initialized and repositories are cloned. Written even
        # without connectors so the file states the explicit "none" case.
        if self._session.workspace_manager:
            from agent.core.datasource_setup import inject_workspace_facts

            try:
                inject_workspace_facts(
                    datasources or [],
                    self._session.workspace_manager,
                    expert=getattr(self._session.config, "display_name", None),
                    ssh_identity_status=self._session.workspace_ssh_identity_status,
                )
            except Exception as e:
                self._logger.warning(f"Failed to write workspace facts: {e}")

        # Initialize cloud workspace sync if the orchestrator gave us a config.
        # F-C1: a protected thread NEVER adopts cloud_sync or nc_session_folder
        # from either fetch site — protected mode's only sanctioned live-write
        # surface is the capture overlay (already reflected in
        # cloud_mount_active above); letting either field through here would
        # rebuild a live agent-service WebDAV sync in every degraded-protected
        # scenario (refused engage, flag off, VM tier, overlay-failure teardown).
        suppress_disposable_cloud = bool(
            self._ports.stateless_mode()
            and getattr(getattr(effective_config, "workspace", None), "backend", None)
            == "none"
        )
        cloud_cfg = (
            None
            if cloud_mount_active or protected_cloud or suppress_disposable_cloud
            else workspace_override.get("cloud_sync")
            if workspace_override
            else None
        )
        nc_folder = (
            None
            if protected_cloud or suppress_disposable_cloud
            else workspace_override.get("nc_session_folder")
            if workspace_override
            else None
        )
        cloud_degraded_hint = False
        if not suppress_disposable_cloud and (
            not cloud_mount_active
            and (not cloud_cfg or not nc_folder)
            and self._client
            and self._identity.thread_id
        ):
            try:
                ws_info = await self._read_workspace(self._identity.thread_id)
                if ws_info:
                    assert_attach_workspace_payload(
                        expected_workspace_identity,
                        ws_info,
                    )
                    self._identity.adopt_workspace_payload(
                        ws_info,
                        protected_required=(
                            protected_cloud
                            or protected_workspace_marker(ws_info) == "on"
                        ),
                    )
                    fresh_delivery = protected_workspace_delivery(ws_info)
                    if protected_cloud:
                        if fresh_delivery != "ready":
                            raise ProtectedCloudUnavailable(
                                "protected-cloud authority changed during attach"
                            )
                        fresh_mount = protected_mount_payload(ws_info)
                        if (
                            fresh_mount != cloud_mount_cfg
                            or protected_workspace_identity(ws_info)
                            != protected_identity
                        ):
                            raise ProtectedCloudUnavailable(
                                "protected-cloud mount authority changed during attach"
                            )
                    elif fresh_delivery == "ready":
                        raise ProtectedCloudUnavailable(
                            "protected-cloud mode was enabled during attach"
                        )
                    attached_workspace_generation = (
                        attached_workspace_generation
                        or str(ws_info.get("workspace_generation") or "")
                    )
                    if not protected_cloud:
                        cloud_cfg = cloud_cfg or ws_info.get("cloud_sync")
                        nc_folder = nc_folder or ws_info.get("nc_session_folder")
                    cloud_degraded_hint = bool(ws_info.get("cloud_sync_degraded"))
            except (ProtectedCloudUnavailable, SessionEnded, WorkspaceNotReady):
                raise
            except Exception:
                # A stateless turn cannot distinguish "no cloud configured" from
                # "the credential/config boundary was unreachable" and must not
                # execute unsynced on that ambiguity. Pinned keeps the historical
                # degraded behavior and retries on its next boundary.
                if self._ports.stateless_mode():
                    self._ports.set_cloud_sync_retry_pending(True)
                if protected_cloud:
                    raise ProtectedCloudUnavailable(
                        "protected-cloud workspace revalidation failed"
                    )
        if suppress_disposable_cloud:
            # backend=none is an intentionally disposable ScratchBackend with no
            # user file tools. The orchestrator may still provision a generic
            # session cloud folder; mirroring internal scratch scaffolding into it
            # would both violate the stateless tier contract and lack a durable
            # workspace generation. Suppress both structured and legacy sync paths
            # only for stateless claims; pinned keeps its historical behavior.
            cloud_cfg = None
            nc_folder = None
            self._ports.set_cloud_sync_retry_pending(False)
        # The late credential/config fetch above is often the first place a lite
        # attach receives its binding generation. Retain the final value even when
        # no coordinator is built, so an omitted/degraded payload cannot hide a
        # pending generation row from the turn-start fail-closed check.
        self._session.cloud_sync_workspace_generation = attached_workspace_generation
        # Back-compat: translate a bare nc_session_folder into the new schema.
        # F-C1: gated on `not protected_cloud` too (defense-in-depth — nc_folder
        # is already forced None above for a protected thread, but this keeps
        # the invariant explicit at the point the shim actually fires).
        if (
            not cloud_mount_active
            and not protected_cloud
            and not cloud_cfg
            and nc_folder
        ):
            cloud_cfg = self._ports.legacy_nc_cloud_cfg(nc_folder)
        if cloud_cfg:
            try:
                self._session.workspace_sync = self._ports.build_sync_coordinator(
                    workspace_path=self._session.workspace_manager.path,
                    workspace_backend=self._session.workspace_manager.backend,
                    cloud_cfg=cloud_cfg,
                    thread_id=str(self._identity.thread_id or ""),
                    workspace_generation=attached_workspace_generation,
                )
                if self._session.workspace_sync is None:
                    raise RuntimeError("cloud sync payload resolved no usable mounts")
                if self._session.workspace_sync:
                    # Phase 1 of cloud_collaboration_model.md: turn-boundary sync,
                    # not background polling. Do one blocking initial pull to
                    # seed the workspace with current cloud-side contents before
                    # the agent starts its first turn — and raise immediately if
                    # any mount is broken, so the operator sees it before any
                    # actual work is committed.
                    #
                    # Stateless executor: SKIP this pull. Every claimed turn runs
                    # the same full pull at turn start (_run_persistent_turn's
                    # turn-boundary sync) seconds after this attach, so the
                    # attach-time pull is a duplicate full-mount walk on the
                    # claim's critical path (measured 41s of the 49s attach,
                    # 2026-08-08 baseline). Broken-mount surfacing moves to the
                    # turn's _resilient_cloud_sync path, which broadcasts
                    # workspace_sync.error and flags degradation — same operator
                    # visibility, one walk instead of two.
                    if self._ports.stateless_mode():
                        self._logger.info(
                            "attach step: initial cloud pull_all skipped "
                            "(stateless — turn-start pull covers seeding)"
                        )
                    else:
                        _t_step = time.perf_counter()
                        await self._session.workspace_sync.pull_all()
                        self._logger.info(
                            "attach step: initial cloud pull_all %.2fs",
                            time.perf_counter() - _t_step,
                        )
                    self._logger.info(
                        "Cloud workspace sync coordinator started (%d mount(s))",
                        len(self._session.workspace_sync),
                    )
            except Exception as e:
                # The coordinator build or initial pull failed. Historically this
                # was swallowed to a warning and the session then ran unsynced for
                # its entire life with no signal — the exact mechanism behind the
                # prod-private "files didn't clone, but I saw no error" incident
                # (knowledge-base/knowledge/issues/main_cloud.md Issue 13). Surface it to the cockpit
                # over the same workspace_sync.error channel the turn-loop uses
                # (_resilient_cloud_sync), so the operator sees a degraded-sync
                # state instead of silence.
                self._logger.warning(f"Failed to start cloud workspace sync: {e}")
                self._ports.broadcast(
                    "workspace_sync.error",
                    {
                        "op": "initial_pull",
                        "turn_id": 0,
                        "message": str(e),
                        "degraded": True,
                    },
                )
                self._session.workspace_sync = None
                self._ports.set_cloud_sync_retry_pending(True)
        elif cloud_degraded_hint:
            # Cloud is up but the orchestrator resolved no sync target for this
            # thread (session-folder provisioning failed upstream, so nc_session_folder
            # and the project mounts are all empty). Surface the same degraded-sync
            # state the failed-initial-pull path uses, instead of running silently
            # unsynced for the session's whole life (knowledge-base/knowledge/issues/main_cloud.md Issue 13).
            self._logger.warning(
                "Thread %s: main cloud is up but no sync target resolved — "
                "session will run unsynced.",
                self._identity.thread_id,
            )
            self._ports.broadcast(
                "workspace_sync.error",
                {
                    "op": "provision",
                    "turn_id": 0,
                    "message": "Cloud sync could not be set up for this session "
                    "(no sync target was provisioned).",
                    "degraded": True,
                },
            )
            self._ports.set_cloud_sync_retry_pending(True)

        # Mark thread as active. Stateless attach is an authorization boundary:
        # End may have fenced the queue after claim-bundle returned, so a failed
        # exact-lease CAS must abort before loop/tool admission.
        if not await self._ports.update_thread_status("active"):
            raise LeaseLostError("stateless attach lost lifecycle authority")

        # Initialize headless loop input. It survives WS reconnect so that the
        # loop can keep reading input / responding to interrupts across transport
        # churn. Cleared in _terminate_session.
        # Keep readiness closed until durable child recovery has completely
        # converged.  Publishing the queue earlier lets a concurrent status/input
        # request start the provider between two orphan reconciliations.
        self._input.begin_attach()
        self._identity.mint_process_generation()

        # Child generations survive their parent process. Reconcile predecessors
        # under this exact authority before any provider can become ready;
        # recovered background evidence joins the durable-input reclaim below.
        await self._session.recover_subagents()

        # An event an earlier process admitted and never settled is owed to
        # this one (parallel_subagents.md §14.2, P3). After recovery, whose
        # batch settle supersedes an input that delegated; before restore, so
        # restore leaves out the copy handed back and the loop runs it once.
        if not self._ports.stateless_mode():
            await self._reserve_stale_pinned_admissions()

        # Restore message history from DB (for session resume). After recovery:
        # settling an interrupted delegation turn writes one tool result per call
        # into the transcript (parallel_subagents.md §5.4, F15), and restore must
        # load them beside their calls — its tool-pairing repair and any resume
        # compaction then see complete pairs, never calls whose results land in
        # the database a moment later. The queue is still closed here, so input
        # admission keeps waiting for recovery and restore alike.
        _t_step = time.perf_counter()
        await self._ports.restore_messages()
        self._logger.info(
            "attach step: message restore %.2fs", time.perf_counter() - _t_step
        )
        self._input.open_queue()

        # Publish mount state only after the authoritative active CAS and queue
        # barrier.  An End racing message/repository restore must not observe a
        # misleading ready event from a runtime that is about to roll back.
        if cloud_mount_active:
            self._ports.broadcast(
                "cloud_mount.ready",
                {
                    "mounts": [
                        {
                            "mount_id": m.mount_id,
                            "mount_kind": m.mount_kind,
                            "target_path": m.target_path,
                            "workspace_name": m.workspace_name,
                        }
                        for m in self._session.cloud_mount_manager.mounts
                    ]
                },
            )
        elif self._session.cloud_mount_error:
            self._ports.broadcast(
                "cloud_mount.error",
                {"message": self._session.cloud_mount_error, "degraded": True},
            )
        # The watcher stays on the session when a protected session runs
        # without its cloud (decision 42): its state is reported all the same.
        sidecar_mounts = (
            getattr(self._session, "sidecar_mount_watcher", None)
            or self._session.cloud_mount_manager
        )
        if getattr(type(sidecar_mounts), "delivery", None) == "sidecar":
            # Folders the Pod's sidecars own (D7): tell the cockpit and the
            # orchestrator each one's state, and keep a pinned session's view
            # live (a stateless session reports again on its next claim).
            await self._report_sidecar_mounts(sidecar_mounts, sidecar_mounts.report())
            if not self._ports.stateless_mode():
                sidecar_mounts.start_monitor(
                    lambda report: self._report_sidecar_mounts(sidecar_mounts, report)
                )

        # Restore deliberately excludes persisted-but-unadmitted delivery rows:
        # they are executable inbox work, not passive conversation context. Claim
        # and queue them after the exact reciprocal binding is active.
        # Pinned input deliveries are owned by the reciprocal thread/agent/pod
        # binding.  A stateless turn is instead owned by its run_queue lease and
        # deliberately has no registered agent row; trying to enter the pinned
        # reclaimer here makes every pooled attach fail after all of its durable
        # setup has already completed.  The turn executor reads the stateless
        # inbox through input_seq/consumed_seq after this attach returns.
        reclaimed_input = set()
        if not self._ports.stateless_mode():
            reclaimed_input = await self._input.reclaim_pending()

        # Start self-cleanup watchdogs (PR 2): exit on boot-WS timeout or
        # out-of-band thread.status='ended'. Cancelled by _terminate_session.
        self._ports.start_watchdogs()

        # Reclaimed durable work already is input. Give it the same existing
        # loop consumer as a new REST input; a resumed life may have no socket
        # subscriber or further human input to trigger that lazy start.
        # The loop-start port still checks readiness and admission, and the
        # loop owns its normal provider/effect authority checks.
        if reclaimed_input:
            self._ports.ensure_loop_started("attach_recovered_input")

        # Officer boot self-wake (centurion.md §4): the loop starts LAZILY on
        # first input / WS attach, so a freshly booted or respawned officer would
        # otherwise park forever with restored history and no running loop. This
        # wake IS the bootstrap, and it makes any durable notices restored above
        # readable in the very first turn. Gated on the loop not already running:
        # a re-attach (e.g. a retried /session/attach POST) must not inject a
        # second boot wake — the k3d smoke produced exactly that duplicate.
        # Stateless executor pods never self-wake: turns run only under a
        # run_queue lease (officer threads stay on the pinned lane in S1).
        if (
            self._ports.officer_config() is not None
            and not self._ports.stateless_mode()
            and not self._ports.loop_running()
        ):
            self._ports.ensure_loop_started("officer_boot")
            await self._input.accept(
                "[wake: session started/restarted] You are the project officer "
                "coming back online after a start or restart. Reorient from your "
                "charter and knowledge base; recent orchestrator notices (if any) "
                "are in your history above. A fresh sitrep arrives with the next "
                "orchestrator wake. If nothing needs you now, file a sleep.",
                role="event",
            )

        self._logger.info(
            f"Session attached: thread={self._identity.thread_id} events_epoch={self._ports.events_epoch()}"
        )

    async def _reserve_stale_pinned_admissions(self) -> None:
        """Hand back, settle or park events a dead pinned process admitted.

        Runs under this attach's exact pinned identity (the one the reclaim
        below uses) and fails the attach like that reclaim does: the queue is
        still closed, so nothing is served before it converges.
        """

        session = self._session
        thread_id = self._identity.thread_id
        if session is None or session.postgres_conn is None or thread_id is None:
            return
        agent_id, pod_uid, process_generation, attach_token = (
            self._input.pinned_identity()
        )
        outcome = await session.postgres_conn.reserve_stale_pinned_admissions(
            thread_id=thread_id,
            agent_id=agent_id,
            pod_uid=pod_uid,
            runtime_generation=process_generation,
            session_runtime_generation=str(self._identity.session_generation or ""),
            runtime_attach_token=attach_token,
        )
        keys = ("settled", "history", "reserved", "parked")
        if any(outcome.get(key) for key in keys):
            self._logger.info(
                "Stale pinned admissions for thread %s: %d answered, %d history, "
                "%d served again, %d parked at the recovery bound",
                thread_id,
                *(len(outcome.get(key) or ()) for key in keys),
            )

    async def cleanup_failed_attach(
        self, thread_id: str, *, restore_thread_id: str | None = None
    ) -> dict[str, Any] | None:
        """Quiesce a partial attach and return its exact release proof.

        A delivered pinned attach owns a real G/attach reservation. Rotating that
        generation is safe only after every local/remote writer is proven zero;
        preserving the shell or swallowing writer/cleanup uncertainty would let
        old work cross into the replacement. Legacy/stateless cleanup retains its
        historical handoff behavior and returns no release receipt.
        """

        exact_generation = self._identity.session_generation
        exact_attach_token = self._identity.attach_token
        exact_pinned_attach = bool(
            self._identity.runtime_contract
            and exact_generation is not None
            and exact_attach_token is not None
        )
        release_receipt: dict[str, Any] | None = None
        cleanup_context = self._cleanup_context

        await self._ports.stop_interrupt_watcher()
        await self._ports.stop_control_watcher()
        if exact_pinned_attach:
            await self._ports.stop_and_join_watchdogs()
            await self._ports.quiesce_side_tasks()

        writer = self._ports.event_writer()
        if writer is not None:
            try:
                await writer.close()
            except Exception as exc:
                if exact_pinned_attach:
                    raise EventJournalUnavailable(
                        "partial attach event writer did not quiesce"
                    ) from exc
                self._logger.warning(
                    "Failed to close event writer after attach failure (thread=%s): %s",
                    thread_id,
                    exc,
                )
            else:
                self._ports.discard_event_writer()

        failed_session = self._session
        if failed_session is not None:
            tool_context = getattr(failed_session, "tool_context", None)
            if tool_context is not None:
                tool_context.citation_verdict_callback = None
                tool_context.canvas_event_callback = None
            try:
                # An exact delivered attach is about to rotate its G and therefore
                # must destructively retire/prove its shell and workspace writers.
                # Legacy/stateless handoff keeps the historical preserve behavior.
                await failed_session.cleanup(
                    preserve_shell=not exact_pinned_attach,
                    preserve_workspace_daemons=(
                        not exact_pinned_attach
                        and getattr(failed_session, "shell_owner_token", None)
                        is not None
                        and getattr(
                            failed_session,
                            "stateless_warm_reuse_safe",
                            True,
                        )
                        is False
                    ),
                )
            except Exception as exc:
                if exact_pinned_attach:
                    raise EventJournalUnavailable(
                        "partial attach runtime did not quiesce"
                    ) from exc
                self._logger.warning(
                    "Failed to clean partial session after event-journal error "
                    "(thread=%s): %s",
                    thread_id,
                    exc,
                )
        elif exact_pinned_attach:
            if not (
                isinstance(cleanup_context, dict)
                and cleanup_context.get("thread_id") == thread_id
            ):
                raise EventJournalUnavailable(
                    "partial attach lacks an exact local cleanup boundary"
                )
            if cleanup_context.get("setup_started") is True:
                await strict_cleanup_partial_attach_local_resources(cleanup_context)
                tier = cleanup_context.get("workspace_tier")
                if tier == "sandbox":
                    protocol = await strict_cleanup_partial_sandbox_workspace(
                        cleanup_context
                    )
                    cleanup_context["local_quiescence_protocol"] = protocol
                elif tier in {"virtual", "none"}:
                    cleanup_context["local_quiescence_protocol"] = (
                        "agent_runtime_zero_v1"
                    )
                else:
                    # VM/remote requires the orchestrator's exact actuator proof.
                    # An agent process can close only its local producers and must
                    # retain the delivered attach fence until that authority acts.
                    raise EventJournalUnavailable(
                        "partial physical attach requires actuator quiescence"
                    )
            else:
                # This monotonic branch is possible only before datasource,
                # session, backend or workspace setup begins. The server repeats
                # the zero-input/control and exact workspace-tuple predicates
                # before rotating G.
                cleanup_context["local_quiescence_protocol"] = (
                    "agent_attach_not_started_v1"
                )

        if exact_pinned_attach:
            protocol = str(
                (
                    getattr(failed_session, "local_quiescence_protocol", "")
                    if failed_session is not None
                    else (cleanup_context or {}).get("local_quiescence_protocol")
                )
                or ""
            )
            workspace_generation = (
                str(getattr(failed_session, "workspace_generation", "") or "")
                if failed_session is not None
                else str((cleanup_context or {}).get("workspace_generation") or "")
            )
            workspace_runtime_incarnation = (
                str(getattr(failed_session, "workspace_runtime_incarnation", "") or "")
                if failed_session is not None
                else str(
                    (cleanup_context or {}).get("workspace_runtime_incarnation") or ""
                )
            )
            pod_uid = str(os.environ.get("POD_UID") or "").strip()
            if (
                not pod_uid
                or protocol
                not in {
                    "workspace_process_zero_v1",
                    "agent_runtime_zero_v1",
                    "agent_attach_not_started_v1",
                }
                or bool(workspace_generation) != bool(workspace_runtime_incarnation)
                or (
                    protocol == "workspace_process_zero_v1" and not workspace_generation
                )
                or (protocol == "agent_runtime_zero_v1" and workspace_generation)
            ):
                raise EventJournalUnavailable(
                    "partial attach produced no trusted local quiescence receipt"
                )
            release_receipt = {
                "thread_id": thread_id,
                "session_runtime_generation": exact_generation,
                "session_runtime_attach_token": exact_attach_token,
                "agent_pod_uid": pod_uid,
                "local_runtime_quiesced": True,
                "local_quiescence_protocol": protocol,
                "workspace_generation": workspace_generation or None,
                "workspace_runtime_incarnation": (
                    workspace_runtime_incarnation or None
                ),
            }

        # Retain the identity that authenticated this delivered life until the
        # rotation/retirement acknowledgement confirms its obligation settled.
        # The input/session owners are closed; identity retention grants no work.
        if release_receipt is not None:
            if not self.retain_release_receipt(release_receipt):
                raise EventJournalUnavailable(
                    "partial attach release proof conflicts with another runtime"
                )
            self._release_restore_thread_id = restore_thread_id
        self._ports.publish_session(None)
        self._cleanup_context = None
        self._ports.reset_journal_cursor()
        self._ports.reset_turn_state()
        self._input.teardown()
        self._identity.clear_process_generation()
        self._ports.close_runtime_authorization()
        if release_receipt is None:
            self._identity.bind_thread(restore_thread_id)
            self._identity.set_status_contract(False)
            self._identity.clear()
            self.clear_runtime_actor()
        self._ports.clear_canvas()
        self._ports.clear_subscribers()
        self._ports.register_mcp_tools(None)
        apply_session_embedding_env(None)
        return release_receipt

    async def cleanup_failed_attach_until_proven(
        self,
        thread_id: str,
        *,
        restore_thread_id: str | None = None,
    ) -> dict[str, Any] | None:
        """Keep one delivered exact attach nonclaimable until cleanup is proven."""

        expected_session = self._session
        expected_identity = (
            self._identity.session_generation,
            self._identity.attach_token,
        )
        exact = bool(self._identity.runtime_contract and all(expected_identity))
        attempt = 0
        while True:
            try:
                # A VM attach can fail after its remote writers exist. Its
                # ordinary abort cannot mint process-zero or rotate G. If the
                # owner subsequently authorizes this exact life's End, hand
                # the still-held session to the normal terminal owner instead.
                if (
                    exact
                    and expected_session is not None
                    and self._session is expected_session
                    and getattr(expected_session, "workspace_backend_tier", None)
                    in {"vm", "remote"}
                    and await self._ports.terminate_failed_attach_if_authorized(
                        (thread_id, *expected_identity)
                    )
                ):
                    # The actuator request is durable but not a zero proof.
                    # Keep the exact process alive until Core stops its Pod.
                    await asyncio.Event().wait()
                return await self.cleanup_failed_attach(
                    thread_id,
                    restore_thread_id=restore_thread_id,
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                still_exact_owner = bool(
                    exact
                    and self._session is expected_session
                    and self._identity.session_generation == expected_identity[0]
                    and self._identity.attach_token == expected_identity[1]
                )
                if not still_exact_owner:
                    raise
                delay = EXACT_SETTLEMENT_RETRY_DELAYS[
                    min(
                        attempt + 1,
                        len(EXACT_SETTLEMENT_RETRY_DELAYS) - 1,
                    )
                ]
                attempt += 1
                self._logger.warning(
                    "Exact failed-attach cleanup remains unproven; retaining "
                    "the nonclaimable owner and retrying (thread=%s type=%s)",
                    thread_id,
                    type(exc).__name__,
                )
                if delay:
                    await asyncio.sleep(delay)

    async def release_receipt_until_confirmed(
        self,
        thread_id: str,
        *,
        runtime_generation: str | None,
        runtime_attach_token: str | None,
    ) -> bool:
        """Retry one proven delivered-attach abort without weakening its fence."""

        receipt = self._release_receipt
        if not (
            isinstance(receipt, dict)
            and receipt.get("thread_id") == thread_id
            and receipt.get("session_runtime_generation") == runtime_generation
            and receipt.get("session_runtime_attach_token") == runtime_attach_token
        ):
            return False
        client = self._client
        if client is None:
            return False
        attempt = 0
        while self._release_receipt is receipt:
            try:
                confirmed = await client.release_thread_agent(
                    thread_id,
                    session_runtime_generation=runtime_generation,
                    session_runtime_attach_token=runtime_attach_token,
                    agent_pod_uid=receipt["agent_pod_uid"],
                    local_runtime_quiesced=True,
                    local_quiescence_protocol=receipt["local_quiescence_protocol"],
                    workspace_generation=receipt.get("workspace_generation"),
                    workspace_runtime_incarnation=receipt.get(
                        "workspace_runtime_incarnation"
                    ),
                )
            except Exception as exc:
                self._logger.warning(
                    "Exact failed-attach release attempt failed (thread=%s type=%s)",
                    thread_id,
                    type(exc).__name__,
                )
                confirmed = False
            if confirmed:
                if self._release_receipt is receipt:
                    # A delayed acknowledgement of G1 cannot clear G2, its
                    # actor credential, status contract or bound thread.
                    if self._identity.clear(
                        expected_generation=runtime_generation,
                        expected_attach_token=runtime_attach_token,
                    ):
                        self._identity.bind_thread(self._release_restore_thread_id)
                        self._identity.set_status_contract(False)
                        self.clear_runtime_actor()
                    self._release_receipt = None
                    self._release_restore_thread_id = None
                return True
            delay = EXACT_SETTLEMENT_RETRY_DELAYS[
                min(attempt + 1, len(EXACT_SETTLEMENT_RETRY_DELAYS) - 1)
            ]
            attempt += 1
            await asyncio.sleep(delay)
        return False

    async def release_shutdown_receipt(self, *, timeout: float = 5.0) -> bool:
        """Boundedly confirm completed attach cleanup before closing its client.

        Cancellation of unfinished setup is never a release proof. On timeout
        the exact receipt and identity remain intact for normal reconciliation.
        """
        receipt = self._release_receipt
        if receipt is None:
            return True
        if any(
            task is not None and not task.done()
            for task in (self._startup_task, self._pool_task)
        ):
            return False
        try:
            return await asyncio.wait_for(
                self.release_receipt_until_confirmed(
                    receipt["thread_id"],
                    runtime_generation=receipt.get("session_runtime_generation"),
                    runtime_attach_token=receipt.get("session_runtime_attach_token"),
                ),
                timeout=timeout,
            )
        except asyncio.TimeoutError:
            self._logger.warning(
                "Shutdown attach release remains unconfirmed; exact obligation retained"
            )
            return False

    async def release_before_dedicated_exit(self, thread_id: str) -> None:
        """Rotate a failed dedicated attach only from its exact zero proof."""

        receipt = self._release_receipt
        if receipt is None:
            return
        if not isinstance(receipt, dict) or receipt.get("thread_id") != thread_id:
            raise EventJournalUnavailable(
                "failed attach release receipt belongs to a different runtime"
            )
        confirmed = await self.release_receipt_until_confirmed(
            thread_id,
            runtime_generation=receipt.get("session_runtime_generation"),
            runtime_attach_token=receipt.get("session_runtime_attach_token"),
        )
        if not confirmed:
            raise EventJournalUnavailable(
                "failed attach release remains unconfirmed; exit suppressed"
            )

    async def _run_pool_attach_transaction(
        self,
        thread_id: str,
        attach: Dict[str, Any],
        runtime_generation: str | None,
        attach_token: str | None,
    ) -> None:
        """Finish one synchronously claimed pool attach in the background.

        ``attach`` owns rollback of every process-global/session resource.
        This wrapper owns only the admission claim and the exact orchestrator
        thread↔agent reservation.  A failed attach releases that reservation once;
        a successful attach leaves it in place for the live session.
        """

        succeeded = False
        release_confirmed = False
        try:
            await self.attach(thread_id=thread_id, **attach)
            succeeded = True
            self._logger.info("Pool session setup complete for thread %s", thread_id)
        except asyncio.CancelledError:
            self._logger.info("Pool session setup cancelled for thread %s", thread_id)
            raise
        except BaseException as exc:
            # Do not echo an arbitrary workspace/config exception: internal
            # payloads may carry credential material.  The attach transaction logs
            # its own bounded diagnostics at the failing boundary.
            self._logger.error(
                "Pool session setup failed for thread %s (%s)",
                thread_id,
                type(exc).__name__,
            )
        finally:
            if not succeeded and self._client is not None:
                try:
                    # The server rotates G only after this exact delivered attach
                    # proves every local/workspace writer zero.  Missing or stale
                    # receipts deliberately retain the process-local claim.
                    release_confirmed = await self.release_receipt_until_confirmed(
                        thread_id,
                        runtime_generation=runtime_generation,
                        runtime_attach_token=attach_token,
                    )
                except BaseException as exc:
                    self._logger.warning(
                        "Failed to release exact pool binding for thread %s (%s)",
                        thread_id,
                        type(exc).__name__,
                    )
            async with self._pool_lock:
                if succeeded or release_confirmed:
                    if (
                        self._pool_claim == thread_id
                        and self._pool_claim_generation == runtime_generation
                        and self._pool_claim_token == attach_token
                    ):
                        self._pool_claim = None
                        self._pool_claim_generation = None
                        self._pool_claim_token = None
                    if self._pool_task is asyncio.current_task():
                        self._pool_task = None
                elif (
                    self._pool_claim == thread_id
                    and self._pool_claim_generation == runtime_generation
                    and self._pool_claim_token == attach_token
                ):
                    # Deliberately retain the claim.  The exact reservation could
                    # not be proven released, so this process must remain
                    # non-ready/nonclaimable until lifecycle reconciliation or
                    # shutdown removes it.
                    self._logger.error(
                        "Pool attach failure for thread %s retained its local "
                        "ownership fence after unconfirmed DB release",
                        thread_id,
                    )

    async def admit_pool_attach(
        self, thread_id: str, request: Dict[str, Any]
    ) -> PoolAttachAdmission:
        """Claim an idle persistent process and schedule its heavy attach.

        The claim and task are installed before returning 200.  This is the
        persistent-pool equivalent of dual mode's ``PodState.SESSION`` latch and
        is the narrow callback-deadlock break for protected workspace polling.
        The route has already validated ``thread_id`` and the recipient
        envelope.
        """

        runtime_contract = pinned_runtime_generation_advertised(request)
        generation_raw = request.get("session_runtime_generation")
        attach_token_raw = request.get("session_runtime_attach_token")
        runtime_generation = canonical_runtime_generation(generation_raw)
        attach_token = canonical_runtime_generation(attach_token_raw)
        if (
            (generation_raw is not None and runtime_generation is None)
            or (attach_token_raw is not None and attach_token is None)
            or (
                runtime_contract
                and (runtime_generation is None or attach_token is None)
            )
        ):
            return PoolAttachAdmission(
                409, {"error": "exact session runtime identity is required"}
            )

        async with self._pool_lock:
            if (
                self._identity.thread_id is not None
                or (self._startup_task is not None and not self._startup_task.done())
                or self._session is not None
                or self._pool_claim is not None
                or self._ports.pending_drain_suspend() is not None
            ):
                owner = (
                    self._identity.thread_id
                    or self._pool_claim
                    or (self._ports.pending_drain_suspend() or {}).get("thread_id")
                )
                return PoolAttachAdmission(
                    409,
                    {
                        "error": f"Already attached to thread {owner}",
                        "current_thread_id": owner,
                    },
                )

            self._pool_claim = thread_id
            self._pool_claim_generation = runtime_generation
            self._pool_claim_token = attach_token
            attach = {
                "config_override": request.get("config_override"),
                "resolved_config": request.get("resolved_config"),
                "project_ids": request.get("project_ids"),
                "datasources": request.get("datasources"),
                "config_name": request.get("config_name"),
                "runtime_actor": request.get("runtime_actor"),
                "pinned_status_identity_contract": request.get(
                    "pinned_status_identity_contract"
                ),
                "pinned_runtime_generation_contract": request.get(
                    "pinned_runtime_generation_contract"
                ),
                "session_subagent_batch_settle_contract": request.get(
                    SESSION_SUBAGENT_BATCH_SETTLE_CONTRACT_KEY
                ),
                "session_subagent_fanout": request.get(SESSION_SUBAGENT_FANOUT_KEY),
                "session_runtime_generation": runtime_generation,
                "session_runtime_attach_token": attach_token,
            }
            try:
                self._identity.adopt(
                    runtime_generation,
                    attach_token,
                    contract_advertised=runtime_contract,
                )
                self._pool_task = asyncio.create_task(
                    self._run_pool_attach_transaction(
                        thread_id,
                        attach,
                        runtime_generation,
                        attach_token,
                    ),
                    name=f"pool-session-attach:{thread_id}",
                )
            except BaseException:
                self._pool_claim = None
                self._pool_claim_generation = None
                self._pool_claim_token = None
                self._pool_task = None
                self._identity.clear(
                    expected_generation=runtime_generation,
                    expected_attach_token=attach_token,
                )
                raise

        return PoolAttachAdmission(200, {"status": "attaching", "thread_id": thread_id})
