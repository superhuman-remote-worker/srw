"""``ParentHost`` — what a child needs from whoever spawned it (U3 B.13).

The subagent runtime never assumes a worker: everything parent-specific is
behind this protocol so a persistent session (U5) is a drop-in. The worker
implementation (``WorkerHost``) is built in ``src/agent/agent.py`` (WP2); tests and
simple parents use :class:`SimpleParentHost`.
"""

from __future__ import annotations

import logging
import inspect
from dataclasses import dataclass, field
from typing import (
    Any,
    Callable,
    Dict,
    List,
    NamedTuple,
    Optional,
    Protocol,
    runtime_checkable,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ParentRef:
    """The one durable object that owns a child runtime.

    Worker children belong to a job; persistent-session children belong to a
    thread.  Keeping the discriminator beside the identifier prevents a
    session UUID from being smuggled through ``parent_job_id`` merely because
    both columns happen to be UUIDs.
    """

    kind: str
    id: str

    def __post_init__(self) -> None:
        if self.kind not in {"job", "thread"}:
            raise ValueError(f"unknown subagent parent kind: {self.kind!r}")
        object.__setattr__(self, "id", str(self.id or "").strip())


class ContextProbe(NamedTuple):
    """A snapshot of the PARENT's context accounting, read at envelope time.

    ``last_provider_input_tokens`` / ``current_token_count`` mirror
    ``ContextManager.state``; the trigger the compaction logic uses is
    ``max(local, anchor)`` and the envelope's headroom is measured against
    ``compaction_threshold_tokens`` the same way (B.5).
    """

    last_provider_input_tokens: Optional[int]
    current_token_count: int
    compaction_threshold_tokens: int
    model_max_context_tokens: int


@runtime_checkable
class ParentHost(Protocol):
    """The parent seen from a child (worker job today, session in U5)."""

    #: Exact durable parent identity.  Exactly one of job/thread is present.
    parent_ref: ParentRef
    #: UUID-shaped audit/metering correlation identity.  For a session this is
    #: the parent thread UUID; the audit store deliberately has no jobs FK.
    correlation_id: str
    #: ``lane_b`` mirrors a committed delivery into the worker's local drain;
    #: ``event`` means the durable transaction already queued session input.
    delivery_channel: str
    #: Owning user (forwarded on agent→orchestrator calls); may be ``None``.
    user_id: Optional[str]
    #: Parent ``agent_id`` — the ``agent_type`` column of the child's audit rows.
    agent_type: str
    #: The parent's ``AuxiliaryLLM`` (compaction summariser); ``None`` = the
    #: summarizer fast-fails and the child keeps its raw history.
    auxiliary_llm: Optional[Any]
    #: The parent's LIVE ``LLMConfig`` (dispatch-time credentials, a fallback
    #: model swap) — what an ``inherit`` roster entry actually runs on.
    live_llm_config: Optional[Any]
    #: Agent-side ``PostgresDB`` pool for the ledger (WP3); ``None`` in tests.
    postgres: Optional[Any]

    def provider_admission(self) -> bool:
        """Synchronous fence: ``False`` once the parent is freezing/draining —
        the child then ends at its next provider boundary without spend."""
        ...

    async def effect_authority(self) -> bool:
        """Await the parent's exact, remote execution-authority proof.

        This is the last fence before provider I/O and before a child's real
        tool invocation.  Unlike :meth:`provider_admission`, failures are
        closed: a stale owner or an unavailable authority store must never be
        mistaken for permission to spend or mutate.
        """
        ...

    async def settlement_authority(self) -> bool:
        """Prove already-admitted child state may be terminally settled.

        This narrower authority can outlive provider/tool admission while an
        exact parent owner cooperatively handles cancellation or pause.
        """
        ...

    def context_probe(self) -> Optional[ContextProbe]:
        """The parent's current context accounting (``None`` = unknown)."""
        ...

    def fork_source(self) -> List[Any]:
        """The parent's DURABLE message list to seed a ``fork=true`` child from
        (the compacted history, never a prepared/transient copy)."""
        ...

    def enqueue_event(self, text: str) -> Any:
        """Deliver a ``role=event`` notice to the parent (U4 background
        delivery), or wake a durable session inbox after its transaction.

        The return may be awaitable.  Delivery is already durable before this
        hint runs, so callers treat failures as latency-only.
        """
        ...


@dataclass
class SimpleParentHost:
    """A plain :class:`ParentHost` built from values and callables.

    The worker's ``WorkerHost`` (WP2) wires the graph's fences and probes into
    the same shape; tests hand this one static values.
    """

    job_id: str
    agent_type: str = "worker"
    thread_id: Optional[str] = None
    user_id: Optional[str] = None
    auxiliary_llm: Optional[Any] = None
    live_llm_config: Optional[Any] = None
    postgres: Optional[Any] = None
    #: The parent's audit ``metadata`` (``state["metadata"]`` in the graph);
    #: merged under the child's ``subagent_*`` keys on every tool audit row.
    audit_metadata: Dict[str, Any] = field(default_factory=dict)
    admission_fn: Optional[Callable[[], bool]] = None
    effect_authority_fn: Optional[Callable[[], Any]] = None
    probe_fn: Optional[Callable[[], Optional[ContextProbe]]] = None
    fork_source_fn: Optional[Callable[[], List[Any]]] = None
    events: List[str] = field(default_factory=list)
    delivery_channel: str = "lane_b"

    @property
    def parent_ref(self) -> ParentRef:
        return ParentRef("job", self.job_id)

    @property
    def correlation_id(self) -> str:
        return self.parent_ref.id

    def provider_admission(self) -> bool:
        if self.admission_fn is None:
            return True
        try:
            return bool(self.admission_fn())
        except Exception:
            return False

    async def effect_authority(self) -> bool:
        probe = self.effect_authority_fn
        if probe is None:
            # Lightweight/test hosts have no remote owner to prove.  Their
            # process-local fence is still consulted at both boundaries.
            return self.provider_admission()
        try:
            value = probe()
            if inspect.isawaitable(value):
                value = await value
            return bool(value) and self.provider_admission()
        except Exception:
            logger.warning(
                "subagent host: exact effect-authority probe failed — closing",
                exc_info=True,
            )
            return False

    async def settlement_authority(self) -> bool:
        """Use the ordinary local fence for lightweight/test parents."""

        return await self.effect_authority()

    def context_probe(self) -> Optional[ContextProbe]:
        if self.probe_fn is None:
            return None
        try:
            return self.probe_fn()
        except Exception:
            return None

    def fork_source(self) -> List[Any]:
        if self.fork_source_fn is None:
            return []
        return list(self.fork_source_fn() or [])

    def enqueue_event(self, text: str) -> None:
        self.events.append(text)


def _default_drain_check() -> bool:
    """The worker's drain intent (dual-mode heartbeat), read lazily so this
    module never imports the API layer; ``False`` when the seam is absent."""
    try:
        from agent.api.dual_app import is_drain_requested

        return bool(is_drain_requested())
    except Exception:
        return False


@dataclass
class WorkerHost:
    """The :class:`ParentHost` of a worker JOB (U3 WP2, plan B.13).

    Built by ``UniversalAgent._setup_job_tools`` from the job's identity and
    live objects; the per-batch facts (the durable message list, the audit
    metadata, the context probe, the admission fence) are read off the
    parent's ``ToolContext``, where the graph stamps them — so the host is
    always current without the graph knowing about hosts.
    """

    job_id: str
    agent_type: str
    tool_context: Any
    thread_id: Optional[str] = None
    user_id: Optional[str] = None
    auxiliary_llm: Optional[Any] = None
    live_llm_config: Optional[Any] = None
    postgres: Optional[Any] = None
    #: Overrides the context's ``provider_admission`` callable when set
    #: (tests); ``None`` = the context's fence, else the dual-app drain seam.
    admission_fn: Optional[Callable[[], bool]] = None
    effect_authority_fn: Optional[Callable[[], Any]] = None
    settlement_authority_fn: Optional[Callable[[], Any]] = None
    events: List[str] = field(default_factory=list)
    delivery_channel: str = "lane_b"

    @property
    def parent_ref(self) -> ParentRef:
        return ParentRef("job", self.job_id)

    @property
    def correlation_id(self) -> str:
        return self.parent_ref.id

    @property
    def audit_metadata(self) -> Dict[str, Any]:
        """The parent graph's ``state["metadata"]`` as stamped for the current
        batch; the job metadata the tools carry until the first batch."""
        stamped = getattr(self.tool_context, "_parent_audit_metadata", None)
        if isinstance(stamped, dict):
            return stamped
        fallback = getattr(self.tool_context, "_job_metadata", None)
        return dict(fallback) if isinstance(fallback, dict) else {}

    def provider_admission(self) -> bool:
        fence = self.admission_fn
        if fence is None:
            fence = getattr(self.tool_context, "provider_admission", None)
        if not callable(fence):
            return not _default_drain_check()
        try:
            return bool(fence())
        except Exception:
            logger.warning(
                "subagent host: admission fence raised — closing", exc_info=True
            )
            return False

    async def effect_authority(self) -> bool:
        """Prove the immutable worker execution against Postgres.

        Production worker contexts carry both the captured authority and the
        agent-side Postgres facade.  A partially wired production context
        fails closed; local/test parents with neither retain the process-local
        U3 behavior.
        """
        if not self.provider_admission():
            return False
        explicit = self.effect_authority_fn
        if explicit is not None:
            try:
                value = explicit()
                if inspect.isawaitable(value):
                    value = await value
                return bool(value) and self.provider_admission()
            except Exception:
                logger.warning(
                    "subagent host: exact effect-authority probe failed — closing",
                    exc_info=True,
                )
                return False

        authority = getattr(self.tool_context, "_parent_execution_authority", None)
        probe = getattr(self.postgres, "parent_execution_authority_current", None)
        if authority is None and self.postgres is None:
            return self.provider_admission()
        if authority is None or not callable(probe):
            logger.warning(
                "subagent host: exact worker authority is incompletely wired — closing"
            )
            return False
        try:
            return bool(await probe(authority)) and self.provider_admission()
        except Exception:
            logger.warning(
                "subagent host: worker execution authority is stale or unavailable",
                exc_info=True,
            )
            return False

    async def settlement_authority(self) -> bool:
        """Retain terminal-write authority after local admission closes.

        The normal effect fence intentionally includes provider admission and
        therefore closes as soon as cancellation is requested.  Durable child
        terminalization instead uses the captured worker lease/process and the
        database's preemption-aware settlement predicate.
        """

        explicit = self.settlement_authority_fn
        if explicit is not None:
            try:
                value = explicit()
                if inspect.isawaitable(value):
                    value = await value
                return bool(value)
            except Exception:
                logger.warning(
                    "subagent host: exact settlement-authority probe failed",
                    exc_info=True,
                )
                return False

        authority = getattr(self.tool_context, "_parent_execution_authority", None)
        probe = getattr(
            self.postgres, "parent_execution_settlement_authority_current", None
        )
        if authority is None and self.postgres is None:
            return await self.effect_authority()
        if authority is None or not callable(probe):
            logger.warning(
                "subagent host: exact worker settlement authority is "
                "incompletely wired — closing"
            )
            return False
        try:
            return bool(await probe(authority))
        except Exception:
            logger.warning(
                "subagent host: worker settlement authority is stale or unavailable",
                exc_info=True,
            )
            return False

    def context_probe(self) -> Optional[ContextProbe]:
        probe = getattr(self.tool_context, "parent_context_probe", None)
        if not callable(probe):
            return None
        try:
            return probe()
        except Exception:
            logger.debug("subagent host: context probe failed", exc_info=True)
            return None

    def fork_source(self) -> List[Any]:
        source = getattr(self.tool_context, "_fork_source", None)
        return list(source or [])

    def enqueue_event(self, text: str) -> None:
        # U4 wires Lane B delivery; a foreground parent only records it.
        self.events.append(text)

    @classmethod
    def from_context(cls, tool_context: Any, **overrides: Any) -> "WorkerHost":
        """A host from what a parent ``ToolContext`` already carries (the
        fallback the tool uses when agent.py has not installed one)."""
        config = getattr(tool_context, "config", None) or {}
        job_meta = getattr(tool_context, "_job_metadata", None) or {}
        values: Dict[str, Any] = {
            "job_id": str(
                overrides.pop("job_id", None)
                or getattr(tool_context, "job_id", None)
                or job_meta.get("job_id")
                or ""
            ),
            "agent_type": str(
                overrides.pop("agent_type", None) or config.get("agent_id") or "worker"
            ),
            "tool_context": tool_context,
            "thread_id": getattr(tool_context, "thread_id", None),
            "user_id": getattr(tool_context, "user_id", None),
            "auxiliary_llm": getattr(tool_context, "auxiliary_llm", None),
            "live_llm_config": getattr(tool_context, "_llm_config", None),
            "postgres": getattr(tool_context, "postgres_db", None),
        }
        values.update(overrides)
        return cls(**values)


@dataclass
class SessionHost:
    """A persistent thread as a true subagent parent (U5).

    Exact owner/lease proof remains outside this transport-neutral class and
    is injected from ``persistent_app``.  Unlike the lightweight test host,
    a missing proof is a closed boundary for a production session.
    """

    thread_id: str
    agent_type: str
    tool_context: Any
    user_id: Optional[str] = None
    auxiliary_llm: Optional[Any] = None
    live_llm_config: Optional[Any] = None
    postgres: Optional[Any] = None
    admission_fn: Optional[Callable[[], bool]] = None
    effect_authority_fn: Optional[Callable[[], Any]] = None
    settlement_authority_fn: Optional[Callable[[], Any]] = None
    retirement_authorized_fn: Optional[Callable[[], Any]] = None
    event_fn: Optional[Callable[[str], Any]] = None
    delivery_channel: str = "event"

    @property
    def parent_ref(self) -> ParentRef:
        return ParentRef("thread", self.thread_id)

    @property
    def correlation_id(self) -> str:
        return self.parent_ref.id

    @property
    def audit_metadata(self) -> Dict[str, Any]:
        stamped = getattr(self.tool_context, "_parent_audit_metadata", None)
        if isinstance(stamped, dict):
            return stamped
        return {"parent_thread_id": self.thread_id}

    def provider_admission(self) -> bool:
        if not callable(self.admission_fn):
            return False
        try:
            return bool(self.admission_fn())
        except Exception:
            logger.warning(
                "session subagent host: admission fence raised — closing",
                exc_info=True,
            )
            return False

    async def effect_authority(self) -> bool:
        if not self.provider_admission() or not callable(self.effect_authority_fn):
            return False
        try:
            value = self.effect_authority_fn()
            if inspect.isawaitable(value):
                value = await value
            return bool(value) and self.provider_admission()
        except Exception:
            logger.warning(
                "session subagent host: exact authority proof failed — closing",
                exc_info=True,
            )
            return False

    async def settlement_authority(self) -> bool:
        """Prove authority for already-admitted child settlement.

        The injected callback ignores only the local pre-retirement admission
        latch. It retains the remote generation, attach-token, pod, and
        process-termination fences.
        """

        probe = self.settlement_authority_fn or self.effect_authority_fn
        if not callable(probe):
            return False
        try:
            value = probe()
            if inspect.isawaitable(value):
                value = await value
            return bool(value)
        except Exception:
            logger.warning(
                "session subagent host: settlement-authority proof failed — closing",
                exc_info=True,
            )
            return False

    async def retirement_authorized(self) -> bool:
        """Whether the orchestrator authorized this exact session life's
        retirement, read now (not at the lifecycle watchdog's next poll).

        The runtime asks after a child write was refused for lost parent
        authority: an authorized retirement takes it for good, while an End
        still in preflight may yet abort. Unwired or failing reads say no.
        """

        probe = self.retirement_authorized_fn
        if not callable(probe):
            return False
        try:
            value = probe()
            if inspect.isawaitable(value):
                value = await value
            return value is True
        except Exception:
            logger.warning(
                "session subagent host: retirement lifecycle read failed",
                exc_info=True,
            )
            return False

    def context_probe(self) -> Optional[ContextProbe]:
        probe = getattr(self.tool_context, "parent_context_probe", None)
        if not callable(probe):
            return None
        try:
            return probe()
        except Exception:
            logger.debug("session subagent context probe failed", exc_info=True)
            return None

    def fork_source(self) -> List[Any]:
        return list(getattr(self.tool_context, "_fork_source", None) or [])

    def enqueue_event(self, text: str) -> Any:
        if not callable(self.event_fn):
            return None
        return self.event_fn(text)


__all__ = [
    "ContextProbe",
    "ParentHost",
    "ParentRef",
    "SessionHost",
    "SimpleParentHost",
    "WorkerHost",
]
