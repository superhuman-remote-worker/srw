"""``DbSubagentLedger`` — the durable side of a subagent child (U3 WP3, B.1).

One ``threads`` row of ``kind='subagent'`` per child, its transcript in
``thread_messages`` through the same serialisers a session uses
(``src/core/thread_messages``), and the lifecycle on the row's own columns:

- ``open``  → ``POST /api/agents/jobs/{job_id}/subagents`` through the
  orchestrator client. The orchestrator owns thread creation and derives the
  row's ``user_id`` / ``project_id`` from the job, which is what lets the job
  owner read the child's transcript through the ordinary thread endpoints.
  The in-process ``subagent_id`` becomes the row id, so the audit rows, the
  ``llm_requests`` rows and the thread share one identity.
- ``persist_message`` → ``save_thread_message(thread_id, **row)`` on the
  agent-side pool (idempotent upsert by the message's stable id).
- ``update`` → ``update_subagent_thread`` on the agent-side pool (guarded by
  ``kind='subagent'``). The thread ``status`` stays ``active`` while the child
  runs and becomes ``ended`` on ANY terminal kind — ``valid_thread_status``
  is never widened; the outcome lives in ``subagent_status`` (the bare kind)
  and ``subagent_outcome`` (the driver's full classification), the error in
  ``subagent_error``. Columns rather than ``metadata.subagent.*`` so the
  terminal write never crosses the ``UPDATE OF metadata`` trigger surface on
  ``threads`` (managed-repository / process-zero fences) for nothing.
- ``lookup`` → ``get_subagent_thread_by_call`` on the agent-side pool: a
  terminal row for ``(parent_job_id, parent_tool_call_id)`` is the
  rotation-surviving idempotency record the runtime replays instead of
  spending again.

``parent_iteration`` (``metadata.subagent``) is the parent's checkpointed
LLM-turn counter at spawn, read off the parent ``ToolContext``
(``_current_turn_count``, stamped by the graph's execute / tools nodes) —
the runtime itself does not know the graph's iteration (WP2 §8.1).

Foreground U3 calls retain their bounded/best-effort runtime wrapper.  The
background and fork-start paths are deliberately strict: a refused authority,
generation, create receipt, or seed batch raises before a child can spend or
later masquerade as recoverable.  A foreground child whose row could not be
created keeps no durable state at all; its in-memory record still serves that
parent process.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional, Sequence
from uuid import UUID

from langchain_core.messages import (
    AIMessage,
    BaseMessage,
    HumanMessage,
    ToolMessage,
    message_to_dict,
    messages_from_dict,
)

from agent.core.context import repair_tool_pairing
from agent.core.thread_messages import _serialize_message_row
from shared.runtime.core.context_entries import context_entry_from_row
from shared.runtime.core.message_markers import PERSIST_ROLE_CONTEXT
from shared.subagent_parent_authority import (
    ParentExecutionAuthority,
    coerce_parent_execution_authority,
)
from agent.subagents.ledger import is_terminal_status

logger = logging.getLogger(__name__)

SUBAGENT_FORK_SEED_PROVIDER_KEY = "_srw_subagent_fork_seed_v1"


class SubagentPersistenceRefused(RuntimeError):
    """A strict child write could not prove its row/generation."""


class SubagentForkSeedDecodeError(ValueError):
    """A stored lossless fork-seed envelope is malformed or unsupported."""


class SubagentTranscriptDecodeError(ValueError):
    """A durable child transcript cannot be reconstructed without data loss."""


@dataclass(frozen=True)
class RestoredSubagentTranscript:
    """Provider messages plus the monotonic durable turn cursor."""

    messages: list[BaseMessage]
    turn_number: int


def _serialize_seed_message(message: Any) -> Dict[str, Any]:
    """Serialize an already-reminted fork message without changing identity.

    The ordinary transcript serializer owns role/content normalization and
    canonical tool calls.  Fork reminting also rewrites provider-native tool
    call copies in ``additional_kwargs`` and ``response_metadata``; retain
    those exact rewritten objects so a durable restart cannot resurrect the
    parent's identities.  The database may map the stable message id onto its
    UUID primary-key representation, but this layer never mints another id.
    """

    row = _serialize_message_row(message, 0)
    additional_kwargs = getattr(message, "additional_kwargs", None)
    response_metadata = getattr(message, "response_metadata", None)
    if additional_kwargs:
        row["additional_kwargs"] = additional_kwargs
    if response_metadata:
        row["response_metadata"] = response_metadata
    # ``thread_messages.content`` is text and the ordinary transcript
    # serializer deliberately flattens provider-native list content.  Keep a
    # complete LangChain envelope in JSONB as the lossless recovery source;
    # a background-child restore must read this key instead of reconstructing
    # the seed from the diet resume columns used by ordinary sessions.
    row["provider_raw"] = {SUBAGENT_FORK_SEED_PROVIDER_KEY: message_to_dict(message)}
    return row


def restore_subagent_fork_seed_message(provider_raw: Any) -> BaseMessage | None:
    """Restore one exact fork seed from its lossless JSONB envelope.

    ``None`` means this transcript row is not a fork-seed row.  A present but
    malformed envelope raises a typed error so recovery fails closed instead
    of silently rebuilding a flattened/provider-incompatible prefix.
    """

    value = provider_raw
    if isinstance(value, (str, bytes)):
        try:
            value = json.loads(value)
        except (TypeError, ValueError) as exc:
            raise SubagentForkSeedDecodeError(
                "fork seed provider_raw is invalid JSON"
            ) from exc
    if not isinstance(value, Mapping):
        return None
    envelope = value.get(SUBAGENT_FORK_SEED_PROVIDER_KEY)
    if envelope is None:
        return None
    if not isinstance(envelope, Mapping):
        raise SubagentForkSeedDecodeError("fork seed envelope is not an object")
    try:
        restored = messages_from_dict([dict(envelope)])
    except Exception as exc:
        raise SubagentForkSeedDecodeError(
            "fork seed envelope cannot be restored"
        ) from exc
    if len(restored) != 1 or not isinstance(restored[0], BaseMessage):
        raise SubagentForkSeedDecodeError("fork seed envelope restored no message")
    return restored[0]


def restore_subagent_messages(rows: Sequence[Mapping[str, Any]]) -> list[BaseMessage]:
    """Rebuild a child's durable provider history for terminal revival.

    Fork-seed rows use their lossless ``provider_raw`` LangChain envelope; all
    later rows use the ordinary transcript columns.  The child's system prompt
    remains transient and is therefore skipped unless it is a fork seed (for
    example a compacted summary).  Malformed rows fail closed: revival must not
    silently forget evidence before spending a successor generation.  A
    ``role='context'`` row is harness context, not evidence: it comes back as
    the typed entry it was, and an unreadable one is dropped (the planner
    re-injects what is still relevant).
    """

    restored: list[BaseMessage] = []
    pending_tool_call_ids: list[str] = []
    for raw in rows:
        if not isinstance(raw, Mapping):
            raise SubagentTranscriptDecodeError("transcript row is not an object")

        exact = restore_subagent_fork_seed_message(raw.get("provider_raw"))
        if exact is not None:
            restored.append(exact)
            if isinstance(exact, AIMessage):
                pending_tool_call_ids = [
                    str(call.get("id") or "")
                    for call in (exact.tool_calls or [])
                    if call.get("id")
                ]
            elif isinstance(exact, ToolMessage):
                tool_id = str(exact.tool_call_id or "")
                if tool_id in pending_tool_call_ids:
                    pending_tool_call_ids.remove(tool_id)
            continue

        role = str(raw.get("role") or "").strip().lower()
        content = raw.get("content") or ""
        message_id = str(raw.get("id") or "").strip() or None
        identity = {"id": message_id} if message_id is not None else {}
        tool_calls = raw.get("tool_calls")
        if isinstance(tool_calls, (str, bytes)):
            try:
                tool_calls = json.loads(tool_calls)
            except (TypeError, ValueError) as exc:
                raise SubagentTranscriptDecodeError(
                    "transcript tool_calls is invalid JSON"
                ) from exc

        if role in {"human", "user", "event"}:
            restored.append(HumanMessage(content=content, **identity))
        elif role == PERSIST_ROLE_CONTEXT:
            entry = context_entry_from_row(
                raw.get("content"), raw.get("additional_kwargs"), id=message_id
            )
            if entry is None:
                logger.debug(
                    "Dropping unreadable context row %s from a child transcript",
                    message_id,
                )
            else:
                restored.append(entry)
        elif role in {"ai", "assistant"}:
            if tool_calls is not None and not isinstance(tool_calls, list):
                raise SubagentTranscriptDecodeError(
                    "assistant transcript tool_calls is not a list"
                )
            canonical: list[dict[str, Any]] = []
            for call in tool_calls or []:
                if not isinstance(call, Mapping):
                    raise SubagentTranscriptDecodeError(
                        "assistant transcript tool call is not an object"
                    )
                canonical.append(
                    {
                        "id": str(call.get("id") or ""),
                        "name": str(call.get("name") or ""),
                        "args": call.get("args")
                        if isinstance(call.get("args"), Mapping)
                        else {},
                    }
                )
            pending_tool_call_ids = [call["id"] for call in canonical if call["id"]]
            restored.append(
                AIMessage(content=content, tool_calls=canonical, **identity)
            )
        elif role == "tool":
            fallback = pending_tool_call_ids.pop(0) if pending_tool_call_ids else ""
            tool_call_id = str(raw.get("tool_call_id") or fallback)
            restored.append(
                ToolMessage(
                    content=content,
                    tool_call_id=tool_call_id,
                    **identity,
                )
            )
        elif role == "system":
            # The framework-owned child prompt is rebuilt from current config.
            continue
        else:
            raise SubagentTranscriptDecodeError(f"unsupported transcript role {role!r}")

    return list(repair_tool_pairing(restored))


class DbSubagentLedger:
    """The DB-backed :class:`~agent.subagents.ledger.SubagentLedger`."""

    def __init__(
        self,
        client: Any,
        postgres: Any,
        *,
        parent_context: Any = None,
        parent_authority: ParentExecutionAuthority | Mapping[str, Any] | None = None,
    ) -> None:
        if client is None or postgres is None:
            raise ValueError(
                "DbSubagentLedger needs the orchestrator client (row creation) "
                "and the agent-side pool (transcript + lifecycle writes)"
            )
        self.client = client
        self.postgres = postgres
        self.parent_context = parent_context
        authority_value = (
            parent_authority
            or getattr(parent_context, "_parent_execution_authority", None)
            or getattr(client, "parent_execution_authority", None)
        )
        if authority_value is None:
            raise ValueError("DbSubagentLedger needs exact parent execution authority")
        self.parent_authority = coerce_parent_execution_authority(authority_value)
        #: subagent_id → thread row id (the same value once the row exists).
        self._rows: Dict[str, str] = {}
        #: The exact run token every lifecycle mutation must match.
        self._generations: Dict[str, str] = {}
        #: Worker parent / handle retained for terminal delivery and revival.
        self._parent_jobs: Dict[str, str] = {}
        self._handles: Dict[str, str] = {}
        #: Children whose row creation definitively failed (no durable state).
        self._failed: set[str] = set()

    @classmethod
    def from_context(cls, context: Any) -> Optional["DbSubagentLedger"]:
        """The ledger of a parent ``ToolContext`` — ``None`` unless both the
        orchestrator client and the agent-side pool are on it (a test parent,
        a bare-metal agent without a DB: the runtime then uses ``NullLedger``)."""
        client = getattr(context, "orchestrator_client", None)
        postgres = getattr(context, "postgres_db", None)
        authority = getattr(context, "_parent_execution_authority", None) or getattr(
            client, "parent_execution_authority", None
        )
        if client is None or postgres is None or authority is None:
            return None
        return cls(
            client,
            postgres,
            parent_context=context,
            parent_authority=authority,
        )

    # ------------------------------------------------------------------
    # Introspection (tests, debugging)
    # ------------------------------------------------------------------

    @property
    def rows(self) -> Dict[str, str]:
        return dict(self._rows)

    @property
    def failed(self) -> set[str]:
        return set(self._failed)

    @property
    def generations(self) -> Dict[str, str]:
        return dict(self._generations)

    def thread_id_for(self, subagent_id: str) -> Optional[str]:
        return self._rows.get(str(subagent_id))

    def runtime_generation_for(self, subagent_id: str) -> Optional[str]:
        return self._generations.get(str(subagent_id))

    def _validated_live_identity(
        self, row: Mapping[str, Any]
    ) -> tuple[str, str, str, str, str] | None:
        """Parse the immutable coordinates of one recovery-roster row.

        The internal list endpoint returns the public roster shape rather than
        a raw ``threads`` row.  Recovery must not let a malformed response
        populate the mutation maps: in particular, a child generation without
        its exact parent and handle is not an actionable lease.
        """

        raw_thread_id = row.get("thread_id")
        raw_id = row.get("id")
        try:
            thread_id = str(UUID(str(raw_thread_id)))
            if raw_id is not None and UUID(str(raw_id)) != UUID(thread_id):
                return None
            generation = str(UUID(str(row.get("runtime_generation"))))
            parent_job_id = str(UUID(str(row.get("parent_job_id"))))
        except (TypeError, ValueError, AttributeError):
            return None
        if not self.parent_authority.for_job(parent_job_id):
            return None

        handle_value = row.get("handle")
        if not isinstance(handle_value, str) or not handle_value.strip():
            return None
        handle = handle_value.strip()
        if handle != handle_value or len(handle) > 120:
            return None

        # ``list_live`` is an execution/recovery surface, not a general
        # roster.  Requiring both lifecycle projections keeps an accidentally
        # broad or stale server response from becoming writable state.
        if row.get("status") not in {"queued", "running"}:
            return None
        if row.get("thread_status") not in {"created", "active"}:
            return None
        if not isinstance(row.get("run_in_background"), bool):
            return None
        subagent_type = row.get("subagent_type")
        if not isinstance(subagent_type, str) or not subagent_type.strip():
            return None
        return thread_id, thread_id, generation, parent_job_id, handle

    def _can_adopt_live_identity(
        self, identity: tuple[str, str, str, str, str]
    ) -> bool:
        child_id, thread_id, generation, parent_job_id, handle = identity
        expected = (
            (self._rows, thread_id),
            (self._generations, generation),
            (self._parent_jobs, parent_job_id),
            (self._handles, handle),
        )
        return all(mapping.get(child_id, value) == value for mapping, value in expected)

    def adopt_live(self, row: Mapping[str, Any]) -> bool:
        """Adopt one already-authorized live row into the local lease maps.

        This performs no database mutation.  Invalid rows and rows that
        conflict with an already-adopted identity leave every map untouched.
        """

        if not isinstance(row, Mapping):
            return False
        identity = self._validated_live_identity(row)
        if identity is None or not self._can_adopt_live_identity(identity):
            return False
        child_id, thread_id, generation, parent_job_id, handle = identity
        self._rows[child_id] = thread_id
        self._generations[child_id] = generation
        self._parent_jobs[child_id] = parent_job_id
        self._handles[child_id] = handle
        self._failed.discard(child_id)
        return True

    def _parent_iteration(self, fields: Dict[str, Any]) -> Optional[int]:
        explicit = fields.get("parent_iteration")
        if explicit is not None:
            try:
                return int(explicit)
            except (TypeError, ValueError):
                return None
        value = getattr(self.parent_context, "_current_turn_count", None)
        try:
            return int(value) if value is not None else None
        except (TypeError, ValueError):
            return None

    # ------------------------------------------------------------------
    # The protocol
    # ------------------------------------------------------------------

    async def open(self, subagent_id: str, **fields: Any) -> Optional[Dict[str, str]]:
        subagent_id = str(subagent_id)
        background = bool(fields.get("run_in_background", False))
        job_id = str(fields.get("parent_job_id") or "").strip()
        if not job_id:
            logger.warning(
                "subagent ledger: no parent job id for %s — keeping no durable "
                "state for this child",
                fields.get("handle") or subagent_id,
            )
            self._failed.add(subagent_id)
            if background:
                raise SubagentPersistenceRefused(
                    f"background subagent has no parent job for {subagent_id}"
                )
            return None
        if not self.parent_authority.for_job(job_id):
            logger.warning("subagent ledger: authority parent mismatch for %s", job_id)
            self._failed.add(subagent_id)
            if background:
                raise SubagentPersistenceRefused(
                    f"background subagent authority mismatch for {subagent_id}"
                )
            return None
        initial_status = str(fields.get("status") or "running").strip()
        if initial_status not in {"queued", "running"}:
            raise ValueError("a subagent must open queued or running")
        parent_tool_call_id = (
            str(fields.get("parent_tool_call_id") or "").strip() or None
        )
        created = await self.client.create_subagent_thread(
            job_id,
            parent_authority=self.parent_authority,
            subagent_id=subagent_id,
            handle=str(fields.get("handle") or ""),
            subagent_type=str(fields.get("subagent_type") or ""),
            parent_tool_call_id=parent_tool_call_id,
            parent_thread_id=fields.get("parent_thread_id") or None,
            isolation=str(fields.get("isolation") or "shared"),
            write_policy=str(fields.get("write_policy") or "none"),
            owned_paths=[str(path) for path in (fields.get("owned_paths") or [])],
            brief_description=str(fields.get("brief_description") or ""),
            parent_iteration=self._parent_iteration(fields),
            fork=bool(fields.get("fork", False)),
            run_in_background=background,
            initial_status=initial_status,
        )
        if not isinstance(created, Mapping):
            self._failed.add(subagent_id)
            if background:
                raise SubagentPersistenceRefused(
                    f"background subagent create refused for {subagent_id}"
                )
            logger.warning(
                "subagent ledger: the orchestrator did not create a fenced "
                "thread row for %s (job %s) — no transcript will be kept",
                fields.get("handle") or subagent_id,
                job_id,
            )
            return None
        thread_id = str(created.get("thread_id") or "").strip()
        generation = str(created.get("runtime_generation") or "").strip()
        try:
            exact_thread_id = str(UUID(thread_id))
            exact_subagent_id = str(UUID(subagent_id))
            exact_generation = str(UUID(generation))
        except (TypeError, ValueError, AttributeError):
            exact_thread_id = exact_subagent_id = exact_generation = ""
        if (
            not exact_thread_id
            or exact_thread_id != exact_subagent_id
            or not exact_generation
        ):
            self._failed.add(subagent_id)
            if background:
                raise SubagentPersistenceRefused(
                    "background subagent create returned a mismatched identity "
                    f"or generation for {subagent_id}"
                )
            logger.warning(
                "subagent ledger: create for %s returned a mismatched identity "
                "or generation token",
                fields.get("handle") or subagent_id,
            )
            return None
        self._rows[subagent_id] = exact_thread_id
        self._generations[subagent_id] = exact_generation
        self._parent_jobs[subagent_id] = job_id
        self._handles[subagent_id] = str(fields.get("handle") or "")
        return {
            "thread_id": exact_thread_id,
            "runtime_generation": exact_generation,
        }

    async def persist_message(
        self, subagent_id: str, msg: Any, turn_number: int
    ) -> None:
        thread_id = self._rows.get(str(subagent_id))
        generation = self._generations.get(str(subagent_id))
        parent_job_id = self._parent_jobs.get(str(subagent_id))
        if thread_id is None or generation is None or parent_job_id is None:
            return
        row = _serialize_message_row(msg, int(turn_number or 0))
        saved = await self.postgres.save_subagent_thread_message(
            thread_id=thread_id,
            parent_job_id=parent_job_id,
            parent_authority=self.parent_authority,
            runtime_generation=generation,
            **row,
        )
        if not saved:
            raise SubagentPersistenceRefused(
                f"subagent transcript generation refused for {subagent_id}"
            )

    async def persist_seed(self, subagent_id: str, messages: Sequence[Any]) -> bool:
        """Persist an entire reminted fork seed atomically before provider I/O."""

        child_key = str(subagent_id)
        thread_id = self._rows.get(child_key)
        generation = self._generations.get(child_key)
        parent_job_id = self._parent_jobs.get(child_key)
        if thread_id is None or generation is None or parent_job_id is None:
            return False
        rows = [_serialize_seed_message(message) for message in messages]
        saved = await self.postgres.save_subagent_thread_messages(
            thread_id=thread_id,
            parent_job_id=parent_job_id,
            parent_authority=self.parent_authority,
            runtime_generation=generation,
            messages=rows,
        )
        if not saved:
            raise SubagentPersistenceRefused(
                f"subagent seed generation refused for {subagent_id}"
            )
        return True

    async def update(self, subagent_id: str, **fields: Any) -> None:
        child_key = str(subagent_id)
        thread_id = self._rows.get(child_key)
        generation = self._generations.get(child_key)
        parent_job_id = self._parent_jobs.get(child_key)
        if thread_id is None or generation is None or parent_job_id is None:
            return
        kwargs: Dict[str, Any] = {}
        status = fields.get("status")
        if status is not None:
            kind = str(status)
            terminal = is_terminal_status(kind)
            kwargs["subagent_status"] = kind
            kwargs["status"] = (
                "ended" if terminal else "created" if kind == "queued" else "active"
            )
            kwargs["ended"] = terminal
        for key in ("outcome", "report_path", "error"):
            value = fields.get(key)
            if value is not None:
                kwargs[key] = str(value)
        for key in ("turns", "tokens"):
            value = fields.get(key)
            if value is not None:
                try:
                    kwargs[key] = int(value)
                except (TypeError, ValueError):
                    continue
        if not kwargs:
            return
        updated = await self.postgres.update_subagent_thread(
            thread_id,
            parent_job_id=parent_job_id,
            parent_authority=self.parent_authority,
            runtime_generation=generation,
            **kwargs,
        )
        if not updated:
            raise SubagentPersistenceRefused(
                f"subagent lifecycle generation refused for {subagent_id}"
            )

    async def terminalize_and_enqueue(
        self,
        subagent_id: str,
        *,
        delivery_id: str,
        message: str,
        timestamp: Any,
        status: str,
        **fields: Any,
    ) -> Optional[Dict[str, Any]]:
        """Opt-in background terminal write plus worker-delivery transaction.

        Foreground U3 calls continue to use :meth:`update`; they never enqueue
        a second copy of the tool result into Lane B.
        """
        child_key = str(subagent_id)
        thread_id = self._rows.get(child_key)
        generation = self._generations.get(child_key)
        parent_job_id = self._parent_jobs.get(child_key)
        if thread_id is None or generation is None or parent_job_id is None:
            return None
        kind = str(status or "").strip()
        if not is_terminal_status(kind):
            raise ValueError("terminal delivery requires a terminal child status")
        timestamp_text = (
            timestamp.isoformat() if hasattr(timestamp, "isoformat") else str(timestamp)
        )
        kwargs: Dict[str, Any] = {
            "runtime_generation": generation,
            "delivery_id": str(delivery_id),
            "message": str(message),
            "timestamp": timestamp_text,
            "subagent_status": kind,
        }
        for key in ("outcome", "report_path", "error"):
            value = fields.get(key)
            if value is not None:
                kwargs[key] = str(value)
        for key in ("turns", "tokens"):
            value = fields.get(key)
            if value is not None:
                try:
                    kwargs[key] = int(value)
                except (TypeError, ValueError):
                    continue
        return await self.client.terminalize_subagent_thread(
            parent_job_id,
            thread_id,
            parent_authority=self.parent_authority,
            **kwargs,
        )

    async def reopen(self, subagent_id: str) -> Optional[Dict[str, Any]]:
        """Rotate one terminal child and retain the successor generation."""
        child_key = str(subagent_id)
        thread_id = self._rows.get(child_key)
        generation = self._generations.get(child_key)
        parent_job_id = self._parent_jobs.get(child_key)
        if thread_id is None or generation is None or parent_job_id is None:
            return None
        result = await self.client.reopen_subagent_thread(
            parent_job_id,
            thread_id,
            parent_authority=self.parent_authority,
            runtime_generation=generation,
        )
        if result and result.get("result") == "reopened":
            successor = str(result.get("runtime_generation") or "").strip()
            if not successor:
                return None
            self._generations[child_key] = successor
        return dict(result) if result else None

    async def load_messages(self, subagent_id: str) -> RestoredSubagentTranscript:
        """Load an adopted child's complete durable transcript under authority."""

        child_key = str(subagent_id)
        thread_id = self._rows.get(child_key)
        if thread_id is None:
            raise SubagentPersistenceRefused(
                f"subagent transcript identity is unknown for {child_key}"
            )
        await self.postgres.parent_execution_authority_current(self.parent_authority)
        rows = await self.postgres.get_thread_messages_history(
            thread_id,
            limit=None,
            include_provider_raw=True,
            order_by_seq=True,
        )
        if not isinstance(rows, list):
            raise SubagentPersistenceRefused("subagent transcript is not a list")
        try:
            turn_number = max(
                (int(row.get("turn_number") or 0) for row in rows), default=0
            )
        except (TypeError, ValueError, AttributeError) as exc:
            raise SubagentTranscriptDecodeError(
                "subagent transcript has an invalid turn cursor"
            ) from exc
        return RestoredSubagentTranscript(
            messages=restore_subagent_messages(rows),
            turn_number=turn_number,
        )

    async def list_live(self, parent_job_id: str) -> list[Dict[str, Any]]:
        """Read and atomically adopt durable children for recovery bootstrap."""
        if not self.parent_authority.for_job(parent_job_id):
            raise SubagentPersistenceRefused(
                "live child roster does not match parent execution authority"
            )
        rows = await self.client.list_live_subagent_threads(
            str(parent_job_id), parent_authority=self.parent_authority
        )
        if not isinstance(rows, list):
            raise SubagentPersistenceRefused("live child roster is not a list")

        normalized: list[Dict[str, Any]] = []
        identities: list[tuple[str, str, str, str, str]] = []
        seen: set[str] = set()
        for raw_row in rows:
            if not isinstance(raw_row, Mapping):
                raise SubagentPersistenceRefused("live child roster row is malformed")
            row = dict(raw_row)
            identity = self._validated_live_identity(row)
            if (
                identity is None
                or identity[0] in seen
                or not self._can_adopt_live_identity(identity)
            ):
                raise SubagentPersistenceRefused(
                    "live child roster identity is malformed or conflicting"
                )
            seen.add(identity[0])
            normalized.append(row)
            identities.append(identity)

        # Do not seed even the first row until every response row has passed
        # validation and collision checks.
        for row, identity in zip(normalized, identities, strict=True):
            if not self.adopt_live(row):  # pragma: no cover - prevalidated above
                raise SubagentPersistenceRefused(
                    f"live child adoption changed unexpectedly for {identity[0]}"
                )
        return normalized

    async def lookup(
        self, parent_job_id: str, parent_tool_call_id: str
    ) -> Optional[Dict[str, Any]]:
        row = await self.postgres.get_subagent_thread_by_call(
            str(parent_job_id),
            str(parent_tool_call_id),
            parent_authority=self.parent_authority,
        )
        if not row:
            return None
        result = dict(row)
        self._adopt_terminal_lookup(
            result,
            parent_job_id=str(parent_job_id),
            parent_tool_call_id=str(parent_tool_call_id),
        )
        return result

    async def lookup_handle(
        self, parent_job_id: str, handle: str
    ) -> Optional[Dict[str, Any]]:
        """Resolve and adopt one terminal background child after a restart."""

        row = await self.postgres.get_subagent_thread_by_handle(
            str(parent_job_id),
            str(handle),
            parent_authority=self.parent_authority,
        )
        if not row:
            return None
        result = dict(row)
        status = str(result.get("subagent_status") or "").strip()
        if not is_terminal_status(status):
            raise SubagentPersistenceRefused(
                f"subagent {handle} still has a live durable generation"
            )
        call_id = str(result.get("parent_tool_call_id") or "").strip()
        if str(result.get("subagent_handle") or "").strip() != str(
            handle
        ).strip() or not self._adopt_terminal_lookup(
            result,
            parent_job_id=str(parent_job_id),
            parent_tool_call_id=call_id,
        ):
            raise SubagentPersistenceRefused(
                f"subagent {handle} is not an addressable background child"
            )
        return result

    def _adopt_terminal_lookup(
        self,
        row: Mapping[str, Any],
        *,
        parent_job_id: str,
        parent_tool_call_id: str,
    ) -> bool:
        """Seed the old generation maps needed by a cold terminal revival."""

        metadata = row.get("metadata")
        if isinstance(metadata, (str, bytes)):
            try:
                metadata = json.loads(metadata)
            except (TypeError, ValueError):
                return False
        spawn = metadata.get("subagent") if isinstance(metadata, Mapping) else None
        try:
            child_id = str(UUID(str(row.get("id") or row.get("thread_id"))))
            generation = str(UUID(str(row.get("runtime_generation"))))
            parent_id = str(UUID(str(row.get("parent_job_id"))))
            expected_parent = str(UUID(str(parent_job_id)))
        except (TypeError, ValueError, AttributeError):
            return False
        handle = str(row.get("subagent_handle") or row.get("handle") or "").strip()
        call_id = str(row.get("parent_tool_call_id") or "").strip()
        if (
            parent_id != expected_parent
            or not self.parent_authority.for_job(parent_id)
            or call_id != str(parent_tool_call_id or "").strip()
            or not handle
            or row.get("status") not in {"ended", None}
            or not is_terminal_status(row.get("subagent_status") or row.get("status"))
            or not isinstance(spawn, Mapping)
            or spawn.get("run_in_background") is not True
        ):
            return False
        identity = (
            child_id,
            child_id,
            generation,
            parent_id,
            handle,
        )
        if not self._can_adopt_live_identity(identity):
            return False
        self._rows[child_id] = child_id
        self._generations[child_id] = generation
        self._parent_jobs[child_id] = parent_id
        self._handles[child_id] = handle
        self._failed.discard(child_id)
        return True


__all__ = [
    "DbSubagentLedger",
    "RestoredSubagentTranscript",
    "SUBAGENT_FORK_SEED_PROVIDER_KEY",
    "SubagentForkSeedDecodeError",
    "SubagentTranscriptDecodeError",
    "SubagentPersistenceRefused",
    "restore_subagent_fork_seed_message",
    "restore_subagent_messages",
]
