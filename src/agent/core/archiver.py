"""LLM Request Archiver & Agent Auditor - stores LLM requests and agent steps in the Postgres audit store.

This module provides functionality to archive all LLM interactions and agent steps for:
- Debugging and troubleshooting
- Conversation replay and analysis
- Cost tracking and optimization
- Complete audit trails of agent execution

Usage:
    archiver = LLMArchiver.from_env()

    # Archive a request/response (existing functionality)
    archiver.archive(
        job_id="job-123",
        agent_type="creator",
        messages=[...],  # LangChain messages
        response=response,  # AIMessage
        model="gpt-4",
        latency_ms=1234,
        iteration=5,
    )

    # Audit any agent step (new functionality)
    archiver.audit_step(
        job_id="job-123",
        agent_type="creator",
        step_type="tool_call",
        node_name="tools",
        iteration=5,
        data={"tool": {"name": "read_file", "arguments": {"path": "file.txt"}}},
    )

    # Query conversation history
    history = archiver.get_conversation(job_id="job-123")

    # Query complete audit trail
    audit_trail = archiver.get_job_audit_trail(job_id="job-123")
"""

import logging
import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence

from langchain_core.messages import (
    AIMessage,
    BaseMessage,
    HumanMessage,
    SystemMessage,
    ToolMessage,
)

from shared.runtime_actor import audit_metadata_payload, redact_audit_payload_metadata

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from agent.database.audit_writer import SyncAuditWriter


def _serialize_payload(obj: Any) -> Any:
    """Serialize objects for JSONB storage in the Postgres audit store.

    Converts ``uuid.UUID`` -> str and ``datetime`` -> UTC ISO-8601 string with
    a ``Z`` suffix (microsecond precision), since JSONB has no native UUID or
    datetime type.
    """
    if isinstance(obj, uuid.UUID):
        return str(obj)
    if isinstance(obj, datetime):
        dt = (
            obj.astimezone(timezone.utc)
            if obj.tzinfo
            else obj.replace(tzinfo=timezone.utc)
        )
        return dt.isoformat().replace("+00:00", "Z")
    if isinstance(obj, dict):
        return {k: _serialize_payload(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_serialize_payload(item) for item in obj]
    return obj


# Heavy job-level blobs handed to the agent once at dispatch (it boots its config
# from them) but, left unchecked, recorded on EVERY per-event audit/LLM row — the
# graph stamps the whole job ``metadata`` on each step. On a long/loop job that
# duplicates the ~127 kB ``resolved_config`` thousands of times; a later bulk
# audit read materializes the whole pile and OOM-kills the orchestrator. These
# already live once per job in ``jobs.resolved_config`` / ``config_override`` /
# ``datasources`` / ``repositories``, so they must never be persisted per row.
# See knowledge-base/knowledge/issues/audit_metadata_config_duplication_ooms_orchestrator.md and
# knowledge-base/knowledge/features/debug_audit_view_refactor.md (Phase 0).
_HEAVY_JOB_METADATA_KEYS = (
    "resolved_config",
    "config_override",
    "datasources",
    "repositories",
)


def _lean_job_metadata(
    metadata: Optional[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    """Project safe actor identity and omit per-row copies of heavy boot blobs.

    Returns ``metadata`` unchanged (same object) when no projection is needed
    — the common case — otherwise a shallow copy of the retained fields. The agent boots
    from these keys via ``self._job_metadata`` / the dispatch ``metadata`` dict,
    never from the persisted rows, so stripping here is invisible to execution and
    only shrinks what hits the DB.
    """
    metadata = audit_metadata_payload(metadata)
    if not metadata or not any(k in metadata for k in _HEAVY_JOB_METADATA_KEYS):
        return metadata
    return {k: v for k, v in metadata.items() if k not in _HEAVY_JOB_METADATA_KEYS}


def _iso_utc_now() -> str:
    """Current UTC time as an ISO-8601 'Z' string (for JSONB ``completed_at``)."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _normalize_content(content) -> str:
    """Normalize message content to string, handling Responses API list content blocks."""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for block in content:
            if isinstance(block, str):
                parts.append(block)
            elif isinstance(block, dict) and "text" in block:
                parts.append(block["text"])
        return " ".join(parts).strip()
    return str(content) if content else ""


def _message_to_dict(msg: BaseMessage) -> Dict[str, Any]:
    """Convert a LangChain message to a serializable dict."""
    result = {
        "type": msg.__class__.__name__,
        "content": _normalize_content(msg.content),
    }

    # Add role for clarity
    if isinstance(msg, SystemMessage):
        result["role"] = "system"
    elif isinstance(msg, HumanMessage):
        result["role"] = "human"
    elif isinstance(msg, AIMessage):
        result["role"] = "assistant"
        # Include tool calls if present
        if hasattr(msg, "tool_calls") and msg.tool_calls:
            result["tool_calls"] = [
                {
                    "id": tc.get("id", ""),
                    "name": tc.get("name", ""),
                    "args": tc.get("args", {}),
                }
                for tc in msg.tool_calls
            ]
    elif isinstance(msg, ToolMessage):
        result["role"] = "tool"
        result["tool_call_id"] = getattr(msg, "tool_call_id", "")
        result["name"] = getattr(msg, "name", "")

    # Include additional_kwargs if present and non-empty
    if hasattr(msg, "additional_kwargs") and msg.additional_kwargs:
        result["additional_kwargs"] = msg.additional_kwargs

    # Include response_metadata if present (token_usage, model_name, finish_reason, etc.)
    if hasattr(msg, "response_metadata") and msg.response_metadata:
        result["response_metadata"] = msg.response_metadata

    return result


def inflight_tool_calls(messages: Sequence[BaseMessage]) -> List[Dict[str, Any]]:
    """Every unanswered tool call of the last tool-calling assistant message.

    The persistent turn loop appends the ``AIMessage`` carrying ``tool_calls`` to
    history *before* running the tools, then appends one ``ToolMessage`` per call
    as each finishes. So a tool_call on the most recent tool-calling ``AIMessage``
    that has no matching ``ToolMessage`` has not finished — the commands a
    (re)attaching client should surface as "running" while the turn is blocked,
    since that in-memory message is not persisted to the DB until the turn ends
    (so REST history can't show it). A parallel batch (a session subagent
    fan-out) has several.

    Returns ``{"id", "tool", "args"}`` per unanswered call in provider order;
    empty when nothing is outstanding.
    """
    answered: set = set()
    last_calls: Optional[List[Dict[str, Any]]] = None
    for msg in messages:
        if isinstance(msg, ToolMessage):
            tool_call_id = getattr(msg, "tool_call_id", None)
            if tool_call_id:
                answered.add(tool_call_id)
        elif isinstance(msg, AIMessage) and getattr(msg, "tool_calls", None):
            last_calls = msg.tool_calls
    if not last_calls:
        return []
    return [
        {
            "id": tc.get("id", ""),
            "tool": tc.get("name", ""),
            "args": tc.get("args", {}),
        }
        for tc in last_calls
        if tc.get("id", "") not in answered
    ]


def inflight_tool_call(messages: Sequence[BaseMessage]) -> Optional[Dict[str, Any]]:
    """Return the assistant tool call that is currently executing, or None.

    The first of :func:`inflight_tool_calls`: in the sequential tool path the
    first unanswered call of the last tool-calling ``AIMessage`` is the one
    running. Returns ``{"id", "tool", "args"}`` or None when nothing is in
    flight.
    """
    calls = inflight_tool_calls(messages)
    return calls[0] if calls else None


class LLMArchiver:
    """Archives LLM requests and responses to the Postgres audit store.

    Provides:
    - Request/response storage with full message history
    - Query by job_id, agent_type, time range
    - Conversation reconstruction for debugging
    """

    def __init__(
        self,
        writer: Optional["SyncAuditWriter"] = None,
    ):
        """Initialize the archiver over the Postgres audit backend.

        Args:
            writer: A :class:`SyncAuditWriter` bound to the audit DB. When
                ``None`` the archiver is inert — ``_ensure_connected`` fails
                and every write no-ops.
        """
        self._writer = writer
        # Per-job last-seen hash of each injected context block (todos,
        # knowledge, …), so chat_history stores an injection's full content
        # only on the turn it changes. Best-effort: lost on process restart
        # (the next turn simply re-stores full content once).
        self._context_hashes: Dict[str, Dict[str, str]] = {}

    @classmethod
    def from_env(cls) -> Optional["LLMArchiver"]:
        """Create archiver from environment variables.

        The Postgres audit store is the only backend: gated on the
        ``AUDIT_POSTGRES_*`` / ``AUDIT_DB_URL`` config consumed by
        :meth:`SyncAuditWriter.from_env`. Returns an archiver when the audit
        DB is configured, else ``None`` (so ``get_archiver()`` is ``None`` and
        all guarded call sites no-op).
        """
        from agent.database.audit_writer import SyncAuditWriter

        writer = SyncAuditWriter.from_env()
        if writer is None:
            logger.debug("Audit DB unconfigured; LLM archiving disabled")
            return None
        return cls(writer=writer)

    def _ensure_connected(self) -> bool:
        """Ensure the Postgres audit backend is ready to write.

        Returns:
            True if the writer is configured and ready, False otherwise.
        """
        return self._writer is not None and self._writer.ensure_ready()

    def _truncate_string(self, s: str, max_length: int = 500) -> str:
        """Truncate a string to max_length with ellipsis indicator."""
        if not s or len(s) <= max_length:
            return s
        return s[:max_length] + "... [truncated]"

    def archive(
        self,
        job_id: str,
        agent_type: str,
        messages: Sequence[BaseMessage],
        response: AIMessage,
        model: str,
        latency_ms: Optional[int] = None,
        iteration: Optional[int] = None,
        metadata: Optional[Dict[str, Any]] = None,
        phase: Optional[str] = None,
        phase_number: Optional[int] = None,
        tool_schemas: Optional[List[Dict[str, Any]]] = None,
        model_kwargs: Optional[Dict[str, Any]] = None,
        call_type: str = "main",
        auxiliary_metadata: Optional[Dict[str, Any]] = None,
    ) -> Optional[str]:
        """Archive an LLM request/response.

        Args:
            job_id: Job identifier
            agent_type: Agent type (e.g., "creator", "validator", "universal")
            messages: Input messages sent to LLM
            response: LLM response (AIMessage)
            model: Model name used
            latency_ms: Request latency in milliseconds
            iteration: Current iteration number
            metadata: Additional metadata to store
            phase: Current phase ("strategic" or "tactical")
            phase_number: Current phase number
            tool_schemas: Tool definition schemas sent to the LLM (OpenAI format)
            model_kwargs: Model parameters (temperature, tool_choice, etc.)
            call_type: Type of call ("main", "summarization", "memory_extraction",
                       "memory_assembly", "knowledge_curation", "vision")
            auxiliary_metadata: Optional call-type-specific context (task class,
                               trigger, iteration count, etc.)

        Returns:
            Inserted document ID, or None if archiving failed.
        """
        if not self._ensure_connected():
            return None

        try:
            # ---- shared field assembly (identical for both backends) ----
            request_data: Dict[str, Any] = {
                "messages": [_message_to_dict(m) for m in messages],
                "message_count": len(messages),
            }
            if tool_schemas:
                request_data["tools"] = tool_schemas
                request_data["tool_count"] = len(tool_schemas)
            if model_kwargs:
                request_data["model_kwargs"] = model_kwargs

            response_dict = _message_to_dict(response)

            total_input_chars = sum(
                len(_normalize_content(m.content)) for m in messages
            )
            response_chars = len(_normalize_content(response.content))
            metrics = {
                "input_chars": total_input_chars,
                "output_chars": response_chars,
                "tool_calls": len(response.tool_calls)
                if hasattr(response, "tool_calls") and response.tool_calls
                else 0,
                # Token usage from response metadata (incl. reasoning_tokens).
                # Chat Completions shape (prompt_tokens, prompt_tokens_details).
                "token_usage": getattr(response, "response_metadata", {}).get(
                    "token_usage", {}
                ),
                # LangChain's normalized usage — the ONLY home for token counts on
                # the Responses API (codex/gpt-5.x via the CLIProxyAPI proxy), whose
                # response_metadata carries no token_usage. Cached prompt tokens land
                # under input_token_details.cache_read here for BOTH APIs, so this is
                # the provider-agnostic source the metering SQL reads.
                "usage_metadata": getattr(response, "usage_metadata", None) or {},
            }

            tool_count = metrics["tool_calls"]
            iter_str = f"iter={iteration}" if iteration else ""
            latency_str = f"{latency_ms}ms" if latency_ms else "?"
            tool_str = f"{tool_count} tools" if tool_count > 0 else "no tools"
            type_str = f" | type={call_type}" if call_type != "main" else ""

            row = {
                "job_id": job_id,
                "agent_type": agent_type,
                "call_type": call_type,
                "model": model,
                "iteration": iteration,
                "timestamp": datetime.now(timezone.utc),
                "latency_ms": latency_ms,
                "request": request_data,
                "response": response_dict,
                "metadata": _serialize_payload(_lean_job_metadata(metadata))
                if metadata
                else None,
                "auxiliary_metadata": _serialize_payload(
                    audit_metadata_payload(auxiliary_metadata)
                )
                if auxiliary_metadata
                else None,
                "metrics": metrics,
            }
            request_id = self._writer.insert_llm_request(row)
            if request_id is None:
                return None
            logger.info(
                f"[LLM] {request_id} | job={job_id[:8]}... | {iter_str} | "
                f"{latency_str} | {tool_str}{type_str}"
            )
            if call_type == "main":
                self._archive_chat_entry(
                    job_id=job_id,
                    agent_type=agent_type,
                    messages=messages,
                    response=response,
                    model=model,
                    latency_ms=latency_ms,
                    iteration=iteration,
                    request_id=request_id,
                    phase=phase,
                    phase_number=phase_number,
                )
            return request_id

        except Exception as e:
            logger.warning(f"Failed to archive LLM request: {e}")
            return None

    def archive_error(
        self,
        job_id: str,
        agent_type: str,
        messages: Sequence[BaseMessage],
        model: str,
        error: str,
        error_type: str,
        *,
        latency_ms: Optional[int] = None,
        call_type: str = "main",
        auxiliary_metadata: Optional[Dict[str, Any]] = None,
    ) -> Optional[str]:
        """Archive a FAILED LLM call (no response) to the llm_requests collection.

        Mirrors :meth:`archive` for the exception path so a failed call leaves a
        queryable row (``status="error"``) instead of vanishing. The main loop
        surfaces its failures via the job's ``error_message``/status, but
        auxiliary calls are deliberately non-fatal and otherwise leave no trace
        — this is how a degraded auxiliary model becomes visible in the debug
        view. Fire-and-forget: never raises.

        See knowledge-base/knowledge/issues/surface_silent_aux_failures.md.
        """
        if not self._ensure_connected():
            return None

        try:
            request_data = {
                "messages": [_message_to_dict(m) for m in messages],
                "message_count": len(messages),
            }
            metrics = {
                "input_chars": sum(
                    len(_normalize_content(m.content)) for m in messages
                ),
                "output_chars": 0,
                "tool_calls": 0,
                "token_usage": {},
            }
            type_str = f" | type={call_type}" if call_type != "main" else ""

            # llm_requests has no status/error columns and response is
            # NOT NULL: fold the error into metadata and store an empty
            # response so a failed (usually auxiliary) call stays queryable.
            row = {
                "job_id": job_id,
                "agent_type": agent_type,
                "call_type": call_type,
                "model": model,
                "iteration": None,
                "timestamp": datetime.now(timezone.utc),
                "latency_ms": latency_ms,
                "request": request_data,
                "response": {},
                "metadata": {
                    "status": "error",
                    "error": {"type": error_type, "message": error[:2000]},
                },
                "auxiliary_metadata": _serialize_payload(
                    audit_metadata_payload(auxiliary_metadata)
                )
                if auxiliary_metadata
                else None,
                "metrics": metrics,
            }
            request_id = self._writer.insert_llm_request(row)
            if request_id is not None:
                logger.warning(
                    f"[LLM-ERR] {request_id} | job={job_id[:8]}... | "
                    f"{error_type}: {error[:120]}{type_str}"
                )
            return request_id

        except Exception as e:
            logger.warning(f"Failed to archive LLM error: {e}")
            return None

    def _archive_chat_entry(
        self,
        job_id: str,
        agent_type: str,
        messages: Sequence[BaseMessage],
        response: AIMessage,
        model: str,
        latency_ms: Optional[int],
        iteration: Optional[int],
        request_id: str,
        phase: Optional[str],
        phase_number: Optional[int],
    ) -> None:
        """Extract delta and write to chat_history collection.

        This stores only the new messages (inputs that triggered this response)
        and the LLM response, enabling a clean sequential view of conversations.

        The payload also carries injected context: the legacy transient tail
        (memory, knowledge, charter, citation feedback, supervisor guidance,
        active subagents, the App Guide turn boundary, instruction files;
        re-injected fresh every request) and typed context entries
        (``shared.runtime.core.context_entries``). The delta starts after the
        last AIMessage that is not a legacy injection: anchoring on a
        synthetic pair's AIMessage dropped the real tool results while the
        full injected block was re-stored on every turn (99% of chat_history
        bytes). Within the delta each injection is archived as a compact
        ``type="context"`` descriptor (kind/hash/chars/preview), with full
        content only on the turn its hash changes; the full payload is always
        in llm_requests via request_id.

        Args:
            job_id: Job identifier
            agent_type: Agent type
            messages: Full message list sent to LLM
            response: LLM response (AIMessage)
            model: Model name used
            latency_ms: Request latency in milliseconds
            iteration: Current iteration number
            request_id: ID linking to llm_requests collection
            phase: Current phase ("strategic" or "tactical")
            phase_number: Current phase number
        """
        from shared.runtime.core.context_entries import (
            is_context_injection,
            is_legacy_injection,
        )
        from shared.runtime.core.message_markers import is_protected_message

        try:
            conversation = [m for m in messages if not isinstance(m, SystemMessage)]

            # Find new inputs: the messages after the last real AIMessage
            # (a legacy synthetic pair's AIMessage is not one). These are the
            # messages that triggered this response.
            last_ai_idx = -1
            for i, msg in enumerate(conversation):
                if isinstance(msg, AIMessage) and not is_legacy_injection(msg):
                    last_ai_idx = i

            # Within the delta, injected context and a protected phase
            # instruction block (history, persisted in state, but not a user
            # turn) are archived as context descriptors, never as inputs.
            injected: List[BaseMessage] = []
            delivered_context: List[BaseMessage] = []
            new_inputs = []
            for msg in conversation[last_ai_idx + 1 :]:
                if is_context_injection(msg):
                    injected.append(msg)
                    continue
                if is_protected_message(msg):
                    delivered_context.append(msg)
                    continue
                content = _normalize_content(msg.content)

                input_entry: Dict[str, Any] = {
                    "type": "human" if isinstance(msg, HumanMessage) else "tool",
                    "content": content,
                    "content_preview": self._truncate_string(content, 500),
                }

                # Add tool-specific fields
                if isinstance(msg, ToolMessage):
                    input_entry["tool_call_id"] = getattr(msg, "tool_call_id", None)
                    input_entry["tool_name"] = getattr(msg, "name", None)

                new_inputs.append(input_entry)

            # Injected context frame, as compact descriptors (payload order:
            # the block sits after the conversation, so append at the end).
            new_inputs.extend(
                self._context_frame_entries(job_id, delivered_context + injected)
            )

            # Extract response
            resp_content = _normalize_content(response.content)
            response_data: Dict[str, Any] = {
                "content": resp_content,
                "content_preview": self._truncate_string(resp_content, 500),
                "has_tool_calls": bool(response.tool_calls)
                if hasattr(response, "tool_calls")
                else False,
            }

            # Add tool calls if present. args_preview (200) is the collapsed
            # one-liner; args carries the arguments up to 4000 chars so the
            # debug chat view can show real commands without opening the full
            # llm_requests row (only stored when it adds over the preview —
            # pathological cases like write_file bodies stay in llm_requests).
            if hasattr(response, "tool_calls") and response.tool_calls:
                tool_call_rows = []
                for tc in response.tool_calls:
                    args_str = str(tc.get("args", {}))
                    tc_row: Dict[str, Any] = {
                        "id": tc.get("id", ""),
                        "name": tc.get("name", ""),
                        "args_preview": self._truncate_string(args_str, 200),
                    }
                    if len(args_str) > 200:
                        tc_row["args"] = self._truncate_string(args_str, 4000)
                    tool_call_rows.append(tc_row)
                response_data["tool_calls"] = tool_call_rows

            # Extract reasoning (DeepSeek-style models with reasoning_content)
            reasoning = None
            if hasattr(response, "additional_kwargs") and response.additional_kwargs:
                reasoning_content = response.additional_kwargs.get("reasoning_content")
                if reasoning_content:
                    reasoning = {
                        "content": reasoning_content,
                        "content_preview": self._truncate_string(
                            reasoning_content, 500
                        ),
                    }

            self._writer.insert_chat_entry(
                {
                    "job_id": job_id,
                    "agent_type": agent_type,
                    "iteration": iteration,
                    "model": model,
                    "timestamp": datetime.now(timezone.utc),
                    "latency_ms": latency_ms,
                    "phase": phase,
                    "phase_number": phase_number,
                    "request_id": request_id,
                    "inputs": new_inputs,
                    "response": response_data,
                    "reasoning": reasoning,
                }
            )
            logger.debug(f"[CHAT] Archived chat entry for job {job_id[:8]}...")

        except Exception as e:
            logger.warning(f"Failed to archive chat entry: {e}")

    def _context_frame_entries(
        self, job_id: str, injected: List[BaseMessage]
    ) -> List[Dict[str, Any]]:
        """Compact ``type="context"`` input entries for the transient injections.

        One entry per injected block (the synthetic AIMessage half of a pair
        is skipped — its content lives in the paired ToolMessage). Each entry
        carries kind, content hash, size, and a 500-char preview; full content
        is included only when the hash differs from the previous archived turn
        of this job, so per-turn rows stay small while every change point
        remains reconstructable from chat_history alone.
        """
        from shared.runtime.core.context_entries import entry_kind
        from shared.runtime.core.injection_markers import (
            ACTIVE_SUBAGENTS_CONTENT_PREFIX,
            CHARTER_TOOL_CALL_ID_PREFIX,
            CITATION_FEEDBACK_TOOL_CALL_ID_PREFIX,
            GUIDANCE_TOOL_CALL_ID_PREFIX,
            INSTRUCTION_TOOL_CALL_ID_PREFIX,
            KNOWLEDGE_TOOL_CALL_ID_PREFIX,
            MEMORY_TOOL_CALL_ID_PREFIX,
            PRODUCT_GUIDE_TURN_BOUNDARY_CONTENT_PREFIX,
            TODOS_INJECTION_CONTENT_PREFIX,
        )
        from shared.runtime.core.message_markers import (
            is_protected_message,
            protected_path,
        )
        from shared.runtime.core.workspace_injection import content_hash_id

        kind_by_prefix = (
            (INSTRUCTION_TOOL_CALL_ID_PREFIX, "instruction"),
            (MEMORY_TOOL_CALL_ID_PREFIX, "memory"),
            (KNOWLEDGE_TOOL_CALL_ID_PREFIX, "knowledge"),
            (CHARTER_TOOL_CALL_ID_PREFIX, "charter"),
            (CITATION_FEEDBACK_TOOL_CALL_ID_PREFIX, "citation_feedback"),
            (GUIDANCE_TOOL_CALL_ID_PREFIX, "guidance"),
        )
        human_kind_by_prefix = (
            (TODOS_INJECTION_CONTENT_PREFIX, "todos"),
            (ACTIVE_SUBAGENTS_CONTENT_PREFIX, "subagents"),
            (PRODUCT_GUIDE_TURN_BOUNDARY_CONTENT_PREFIX, "turn_boundary"),
        )

        # Bound the per-job hash cache (worker --loop reuses the process).
        if job_id not in self._context_hashes and len(self._context_hashes) >= 64:
            self._context_hashes.pop(next(iter(self._context_hashes)))
        prev = self._context_hashes.setdefault(job_id, {})

        # Labels (instruction file paths) come from the synthetic AIMessage
        # half of each pair, keyed by tool_call_id.
        labels: Dict[str, str] = {}
        for msg in injected:
            if isinstance(msg, AIMessage) and getattr(msg, "tool_calls", None):
                for tc in msg.tool_calls:
                    path = (tc.get("args") or {}).get("path")
                    if path:
                        labels[tc.get("id", "")] = str(path)

        entries: List[Dict[str, Any]] = []
        for msg in injected:
            label: Optional[str] = None
            typed_kind = entry_kind(msg)
            if is_protected_message(msg):
                # Persistent phase instruction block (src/core/message_markers):
                # labelled by the artifact path it delivers. Mirrored in
                # src/shared/orch_surface/formatters.py::_context_label.
                kind = "phase_instruction"
                label = protected_path(msg)
            elif typed_kind is not None:
                # Typed context entry: the kind is in its metadata; the
                # citation entry keeps the legacy pair's descriptor kind.
                kind = "citation_feedback" if typed_kind == "citation" else typed_kind
            elif isinstance(msg, HumanMessage):
                content_text = msg.content if isinstance(msg.content, str) else ""
                kind = next(
                    (k for p, k in human_kind_by_prefix if content_text.startswith(p)),
                    "other",
                )
            elif isinstance(msg, ToolMessage):
                tcid = getattr(msg, "tool_call_id", "") or ""
                kind = next(
                    (k for p, k in kind_by_prefix if tcid.startswith(p)), "other"
                )
                label = labels.get(tcid)
            else:
                continue  # synthetic AIMessage half of a pair

            content = _normalize_content(msg.content)
            digest = content_hash_id(content)
            entry: Dict[str, Any] = {
                "type": "context",
                "kind": kind,
                "hash": digest,
                "chars": len(content),
                "content_preview": self._truncate_string(content, 500),
            }
            if label:
                entry["label"] = label
            # Several instruction files can be injected per turn: track each
            # by label (or content hash) so they don't clobber one another.
            key = (
                f"{kind}:{label or digest}"
                if kind in ("instruction", "phase_instruction")
                else kind
            )
            if prev.get(key) != digest:
                entry["content"] = content
            prev[key] = digest
            entries.append(entry)
        return entries

    # =========================================================================
    # Agent Audit Methods - Complete execution history tracking
    # =========================================================================

    def audit_step(
        self,
        job_id: str,
        agent_type: str,
        step_type: str,
        node_name: str,
        iteration: int,
        data: Optional[Dict[str, Any]] = None,
        latency_ms: Optional[int] = None,
        metadata: Optional[Dict[str, Any]] = None,
        phase: Optional[str] = None,
        phase_number: Optional[int] = None,
    ) -> Optional[str]:
        """Audit any step in the agent workflow.

        Args:
            job_id: Job identifier
            agent_type: Agent type (e.g., "creator", "validator")
            step_type: Type of step ("initialize", "llm_call", "llm_response",
                       "tool_call", "tool_result", "check", "routing", "error")
            node_name: Graph node name ("initialize", "process", "tools", "check")
            iteration: Current iteration number
            data: Step-specific data (llm, tool, routing, state, error info)
            latency_ms: Operation latency in milliseconds
            metadata: Additional metadata

        Returns:
            Inserted document ID, or None if audit failed.
        """
        if not self._ensure_connected():
            return None

        try:
            row = {
                "job_id": job_id,
                "agent_type": agent_type,
                "iteration": iteration,
                "step_type": step_type,
                "node_name": node_name,
                "phase": phase,
                "phase_number": phase_number,
                "timestamp": datetime.now(timezone.utc),
                "latency_ms": latency_ms,
                "payload": _serialize_payload(redact_audit_payload_metadata(data))
                if data
                else {},
                "metadata": _serialize_payload(_lean_job_metadata(metadata))
                if metadata
                else None,
            }
            audit_id = self._writer.insert_audit_pre(row)
            if audit_id is not None:
                logger.debug(
                    f"[AUDIT] {audit_id} | job={job_id[:8]}... | "
                    f"iter={iteration} | {step_type}"
                )
            return audit_id

        except Exception as e:
            logger.warning(f"Failed to audit step: {e}")
            return None

    def audit_tool_call(
        self,
        job_id: str,
        agent_type: str,
        iteration: int,
        tool_name: str,
        call_id: str,
        arguments: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None,
        phase: Optional[str] = None,
        phase_number: Optional[int] = None,
    ) -> Optional[str]:
        """Audit a tool call before execution.

        Creates a document with result fields set to null, to be updated
        via update_tool_result() after execution completes.

        Args:
            job_id: Job identifier
            agent_type: Agent type
            iteration: Current iteration number
            tool_name: Name of the tool being called
            call_id: Tool call ID from LLM
            arguments: Tool arguments
            metadata: Additional metadata

        Returns:
            Inserted document ID, or None if audit failed.
        """
        # Truncate large arguments for storage
        args_preview = {}
        for key, value in arguments.items():
            if isinstance(value, str):
                args_preview[key] = self._truncate_string(value, 200)
            elif isinstance(value, (dict, list)):
                args_str = str(value)
                args_preview[key] = self._truncate_string(args_str, 200)
            else:
                args_preview[key] = value

        return self.audit_step(
            job_id=job_id,
            agent_type=agent_type,
            step_type="tool",
            node_name="tools",
            iteration=iteration,
            data={
                "tool": {
                    "name": tool_name,
                    "call_id": call_id,
                    "arguments": args_preview,
                    # Result fields - null until update_tool_result() is called
                    "result_preview": None,
                    "result_size_bytes": None,
                    "success": None,
                    "error": None,
                },
                "started_at": datetime.now(timezone.utc),
                "completed_at": None,
            },
            metadata=metadata,
            phase=phase,
            phase_number=phase_number,
        )

    def update_tool_result(
        self,
        audit_doc_id: str,
        result: str,
        success: bool,
        latency_ms: int,
        error: Optional[str] = None,
    ) -> bool:
        """Update a tool audit document with execution result.

        Args:
            audit_doc_id: The document ID returned by audit_tool_call()
            result: Tool result content
            success: Whether tool succeeded
            latency_ms: Tool execution time
            error: Error message if failed

        Returns:
            True if update succeeded, False otherwise.
        """
        if not self._ensure_connected():
            return False

        try:
            payload = {
                "tool": {
                    "result_preview": self._truncate_string(result, 500),
                    "result_size_bytes": len(result) if result else 0,
                    "success": success,
                },
                "completed_at": _iso_utc_now(),
            }
            if error:
                payload["tool"]["error"] = self._truncate_string(error, 500)
            return self._writer.insert_audit_post(audit_doc_id, payload, latency_ms)

        except Exception as e:
            logger.warning(f"Failed to update tool result: {e}")
            return False

    def audit_llm_call(
        self,
        job_id: str,
        agent_type: str,
        iteration: int,
        model: str,
        input_message_count: int,
        state_message_count: int,
        metadata: Optional[Dict[str, Any]] = None,
        phase: Optional[str] = None,
        phase_number: Optional[int] = None,
    ) -> Optional[str]:
        """Audit an LLM call before execution.

        Creates a document with response fields set to null, to be updated
        via update_llm_response() after the LLM responds.

        Args:
            job_id: Job identifier
            agent_type: Agent type
            iteration: Current iteration number
            model: Model name
            input_message_count: Number of messages sent to LLM
            state_message_count: Total messages in conversation state
            metadata: Additional metadata

        Returns:
            Inserted document ID, or None if audit failed.
        """
        return self.audit_step(
            job_id=job_id,
            agent_type=agent_type,
            step_type="llm",
            node_name="execute",
            iteration=iteration,
            data={
                "llm": {
                    "model": model,
                    "input_message_count": input_message_count,
                    # Response fields - null until update_llm_response() is called
                    "request_id": None,
                    "response_content_preview": None,
                    "tool_calls": None,
                    "metrics": None,
                },
                "state": {
                    "message_count": state_message_count,
                },
                "started_at": datetime.now(timezone.utc),
                "completed_at": None,
            },
            metadata=metadata,
            phase=phase,
            phase_number=phase_number,
        )

    def update_llm_response(
        self,
        audit_doc_id: str,
        request_id: Optional[str],
        response_preview: str,
        tool_calls: List[Dict[str, Any]],
        output_chars: int,
        latency_ms: int,
    ) -> bool:
        """Update an LLM audit document with response data.

        Args:
            audit_doc_id: The document ID returned by audit_llm_call()
            request_id: ID linking to llm_requests collection
            response_preview: First 500 chars of response content
            tool_calls: List of tool calls in response
            output_chars: Total response character count
            latency_ms: LLM response time

        Returns:
            True if update succeeded, False otherwise.
        """
        if not self._ensure_connected():
            return False

        try:
            payload = {
                "llm": {
                    "request_id": request_id,
                    "response_content_preview": response_preview,
                    "tool_calls": tool_calls,
                    "metrics": {
                        "output_chars": output_chars,
                        "tool_call_count": len(tool_calls) if tool_calls else 0,
                    },
                },
                "completed_at": _iso_utc_now(),
            }
            return self._writer.insert_audit_post(
                audit_doc_id, payload, latency_ms, request_id=request_id
            )

        except Exception as e:
            logger.warning(f"Failed to update LLM response: {e}")
            return False

    def close(self):
        """Close the audit backend's connection."""
        if self._writer is not None:
            self._writer.close()


# Singleton instance for convenience
_default_archiver: Optional[LLMArchiver] = None


def get_archiver() -> Optional[LLMArchiver]:
    """Get or create the default LLM archiver instance.

    Returns:
        LLMArchiver instance if the audit DB is configured, None otherwise.
    """
    global _default_archiver
    if _default_archiver is None:
        _default_archiver = LLMArchiver.from_env()
    return _default_archiver


def audit_unavailable(
    *,
    job_id: str,
    agent_type: str,
    step_type: str,
    component: str,
    error: BaseException,
    node_name: str = "setup",
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    """Record that a subsystem (memory/KB) failed to initialize.

    Best-effort and never raises — safe to call from an ``except`` block. Emits
    an audit step with a custom ``step_type`` (e.g. ``memory_unavailable`` /
    ``kb_unavailable``) so the degradation is visible in the cockpit audit trail
    instead of living only in a pod-log WARNING (see
    knowledge-history/done/embedding_key_missing_silently_disables_memory_and_kb.md).
    """
    arch = get_archiver()
    if arch is None:
        return
    data: Dict[str, Any] = {
        "component": component,
        "error": str(error),
        "error_type": type(error).__name__,
    }
    if extra:
        data.update(extra)
    try:
        arch.audit_step(
            job_id=str(job_id),
            agent_type=agent_type or "",
            step_type=step_type,
            node_name=node_name,
            iteration=0,
            data=data,
        )
    except Exception:  # pragma: no cover - audit must never break the caller
        pass


def archive_llm_request(
    job_id: str,
    agent_type: str,
    messages: Sequence[BaseMessage],
    response: AIMessage,
    model: str,
    latency_ms: Optional[int] = None,
    iteration: Optional[int] = None,
    metadata: Optional[Dict[str, Any]] = None,
    phase: Optional[str] = None,
    phase_number: Optional[int] = None,
    tool_schemas: Optional[List[Dict[str, Any]]] = None,
    model_kwargs: Optional[Dict[str, Any]] = None,
    call_type: str = "main",
    auxiliary_metadata: Optional[Dict[str, Any]] = None,
) -> Optional[str]:
    """Convenience function to archive an LLM request using default archiver.

    See LLMArchiver.archive() for parameter details.
    """
    archiver = get_archiver()
    if archiver:
        return archiver.archive(
            job_id=job_id,
            agent_type=agent_type,
            messages=messages,
            response=response,
            model=model,
            latency_ms=latency_ms,
            iteration=iteration,
            metadata=metadata,
            phase=phase,
            phase_number=phase_number,
            tool_schemas=tool_schemas,
            model_kwargs=model_kwargs,
            call_type=call_type,
            auxiliary_metadata=auxiliary_metadata,
        )
    return None
