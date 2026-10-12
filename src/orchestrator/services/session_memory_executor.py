"""Strict executor for durable stateless-session memory effects.

The producer stores the exact turn boundary and its immutable memory
destination in ``completion_effects.detail``.  This module deliberately does
not infer either from the thread's current tail or current project mounts: an
effect can be delayed across a handoff or a later settings change.

The app DB outbox and vector DB are separate transactional domains.  This
executor closes their ambiguous response-loss edge with a destination ledger:
the immutable effect identity, every ``RecallStore`` mutation, and the fixed
receipt commit in one vector transaction.  A replay validates that identity
and returns the receipt without repeating any vector mutation.  Auxiliary LLM
work can still repeat when two pre-commit claimants race; the exactly-once
contract is deliberately for the durable destination effects, not provider
billing.
"""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any
from uuid import UUID, uuid4

from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, ToolMessage

from shared.runtime.core.loader import (
    LLMConfig,
    build_auxiliary_llm_config,
    create_llm,
    load_config_from_resolved,
    resolve_model_settings,
)
from shared.runtime.core.message_markers import PERSIST_ROLE_CONTEXT
from shared.runtime.services.memory_prompts import resolve_memory_extraction_prompt
from shared.runtime.services.auxiliary import AuxiliaryLLM, ExtractMemoriesTask
from shared.runtime.services.embedding_service import EmbeddingService
from shared.runtime.services.memory.ingestion import maybe_attach_ingestion_verdict
from shared.runtime.services.recall_store import RecallStore

from orchestrator.services.session_memory_effects import (
    SESSION_MEMORY_EFFECT_NAME,
    SessionMemoryEffect,
    SessionMemoryEffectPermanentError,
)

ConfigResolver = Callable[[Mapping[str, Any], str, UUID], Awaitable[Mapping[str, Any]]]

_SUPPORTED_BOUNDARY_ROLES = frozenset({"human", "user", "event"})
_SUPPORTED_TURN_ROLES = frozenset({"human", "user", "event", "ai", "assistant", "tool"})


@dataclass(frozen=True, slots=True)
class _EffectIdentity:
    producer_id: UUID
    effect_name: str
    thread_id: UUID
    input_message_id: UUID
    turn_number: int
    boundary_seq: int
    end_seq: int
    memory_scope_kind: str
    memory_scope_id: UUID


@dataclass(frozen=True, slots=True)
class _TurnSource:
    thread: dict[str, Any]
    messages: tuple[BaseMessage, ...]


_LEDGER_COLUMNS = """
    producer_id, effect_name, thread_id, input_message_id, turn_number,
    boundary_seq, end_seq, memory_scope_kind, memory_scope_id, state,
    extracted_count, stored_count, created_at, completed_at
"""

_LEDGER_SELECT_SQL = f"""
    SELECT {_LEDGER_COLUMNS}
    FROM session_memory_effect_executions
    WHERE producer_id = $1::uuid AND effect_name = $2::text
"""

_LEDGER_INSERT_SQL = """
    INSERT INTO session_memory_effect_executions (
        producer_id, effect_name, thread_id, input_message_id, turn_number,
        boundary_seq, end_seq, memory_scope_kind, memory_scope_id
    ) VALUES (
        $1::uuid, $2::text, $3::uuid, $4::uuid, $5,
        $6, $7, $8, $9::uuid
    )
    ON CONFLICT (producer_id, effect_name) DO NOTHING
    RETURNING producer_id
"""

_LEDGER_FINISH_SQL = f"""
    UPDATE session_memory_effect_executions
    SET state = 'done',
        extracted_count = $10,
        stored_count = $11,
        completed_at = now()
    WHERE producer_id = $1::uuid
      AND effect_name = $2::text
      AND thread_id = $3::uuid
      AND input_message_id = $4::uuid
      AND turn_number = $5
      AND boundary_seq = $6
      AND end_seq = $7
      AND memory_scope_kind = $8
      AND memory_scope_id = $9::uuid
      AND state = 'writing'
    RETURNING {_LEDGER_COLUMNS}
"""


def _output(
    disposition: str,
    *,
    turn_number: int,
    extracted_count: int = 0,
    stored_count: int = 0,
) -> dict[str, Any]:
    """Return the one fixed-cardinality receipt shape for every outcome."""

    return {
        "disposition": disposition,
        "turn_number": int(turn_number),
        "extracted_count": int(extracted_count),
        "stored_count": int(stored_count),
    }


def _required_uuid(value: Any, field: str) -> UUID:
    try:
        return UUID(str(value))
    except (TypeError, ValueError, AttributeError) as exc:
        raise SessionMemoryEffectPermanentError(
            f"session memory effect {field} must be a UUID"
        ) from exc


def _identity(effect: SessionMemoryEffect) -> _EffectIdentity:
    producer_id = _required_uuid(effect.producer_id, "producer_id")
    effect_name = str(effect.effect_name or "")
    if effect_name != SESSION_MEMORY_EFFECT_NAME:
        raise SessionMemoryEffectPermanentError(
            "session memory effect stable identity is unsupported"
        )
    thread_id = _required_uuid(effect.scope_id, "scope_id")
    input_message_id = _required_uuid(
        effect.detail.get("input_message_id"), "detail.input_message_id"
    )
    raw_turn = effect.detail.get("turn_number")
    if isinstance(raw_turn, bool):
        raw_turn = None
    try:
        turn_number = int(raw_turn)
    except (TypeError, ValueError) as exc:
        raise SessionMemoryEffectPermanentError(
            "session memory effect detail.turn_number must be a positive integer"
        ) from exc
    if turn_number < 1 or str(turn_number) != str(raw_turn):
        raise SessionMemoryEffectPermanentError(
            "session memory effect detail.turn_number must be a positive integer"
        )

    seq_values: dict[str, int] = {}
    for field in ("boundary_seq", "end_seq"):
        raw_seq = effect.detail.get(field)
        if isinstance(raw_seq, bool):
            raw_seq = None
        try:
            seq = int(raw_seq)
        except (TypeError, ValueError) as exc:
            raise SessionMemoryEffectPermanentError(
                f"session memory effect detail.{field} must be a positive integer"
            ) from exc
        if seq < 1 or str(seq) != str(raw_seq):
            raise SessionMemoryEffectPermanentError(
                f"session memory effect detail.{field} must be a positive integer"
            )
        seq_values[field] = seq
    if seq_values["end_seq"] < seq_values["boundary_seq"]:
        raise SessionMemoryEffectPermanentError(
            "session memory effect end_seq precedes boundary_seq"
        )

    scope_kind = str(effect.detail.get("memory_scope_kind") or "")
    if scope_kind not in {"thread", "project"}:
        raise SessionMemoryEffectPermanentError(
            "session memory effect detail.memory_scope_kind is unsupported"
        )
    scope_id = _required_uuid(
        effect.detail.get("memory_scope_id"), "detail.memory_scope_id"
    )
    if scope_kind == "thread" and scope_id != thread_id:
        raise SessionMemoryEffectPermanentError(
            "thread-scoped session memory destination must equal scope_id"
        )
    return _EffectIdentity(
        producer_id=producer_id,
        effect_name=effect_name,
        thread_id=thread_id,
        input_message_id=input_message_id,
        turn_number=turn_number,
        boundary_seq=seq_values["boundary_seq"],
        end_seq=seq_values["end_seq"],
        memory_scope_kind=scope_kind,
        memory_scope_id=scope_id,
    )


def _ledger_identity_args(identity: _EffectIdentity) -> tuple[Any, ...]:
    return (
        identity.producer_id,
        identity.effect_name,
        identity.thread_id,
        identity.input_message_id,
        identity.turn_number,
        identity.boundary_seq,
        identity.end_seq,
        identity.memory_scope_kind,
        identity.memory_scope_id,
    )


def _ledger_receipt(
    row: Mapping[str, Any], identity: _EffectIdentity
) -> dict[str, Any]:
    """Validate an immutable destination receipt before replaying it."""

    expected = {
        "producer_id": identity.producer_id,
        "effect_name": identity.effect_name,
        "thread_id": identity.thread_id,
        "input_message_id": identity.input_message_id,
        "turn_number": identity.turn_number,
        "boundary_seq": identity.boundary_seq,
        "end_seq": identity.end_seq,
        "memory_scope_kind": identity.memory_scope_kind,
        "memory_scope_id": identity.memory_scope_id,
    }
    for field, value in expected.items():
        observed = row.get(field)
        if field in {
            "producer_id",
            "thread_id",
            "input_message_id",
            "memory_scope_id",
        }:
            matches = str(observed) == str(value)
        elif field in {"turn_number", "boundary_seq", "end_seq"}:
            try:
                matches = int(observed) == int(value)
            except (TypeError, ValueError):
                matches = False
        else:
            matches = observed == value
        if not matches:
            raise SessionMemoryEffectPermanentError(
                "session memory destination ledger identity conflicts with effect"
            )

    if row.get("state") != "done" or row.get("completed_at") is None:
        raise SessionMemoryEffectPermanentError(
            "session memory destination ledger has an incomplete committed row"
        )
    try:
        extracted = int(row.get("extracted_count"))
        stored = int(row.get("stored_count"))
    except (TypeError, ValueError) as exc:
        raise SessionMemoryEffectPermanentError(
            "session memory destination ledger receipt is malformed"
        ) from exc
    if extracted < 0 or stored < 0 or stored > extracted:
        raise SessionMemoryEffectPermanentError(
            "session memory destination ledger receipt is malformed"
        )
    return _output(
        "completed",
        turn_number=identity.turn_number,
        extracted_count=extracted,
        stored_count=stored,
    )


def _json_value(value: Any, *, field: str) -> Any:
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except (TypeError, ValueError) as exc:
        raise SessionMemoryEffectPermanentError(
            f"session transcript {field} is malformed"
        ) from exc


def _messages_from_rows(rows: Sequence[Mapping[str, Any]]) -> tuple[BaseMessage, ...]:
    messages: list[BaseMessage] = []
    pending_tool_call_ids: list[str] = []
    for row in rows:
        role = str(row.get("role") or "")
        if role == PERSIST_ROLE_CONTEXT:
            # A typed context entry (memory, knowledge, guidance, ...) is
            # harness context the turn was given, not conversation: memory is
            # never extracted from it. It stays inside the frozen window.
            continue
        if role not in _SUPPORTED_TURN_ROLES:
            raise SessionMemoryEffectPermanentError(
                f"session transcript has unsupported turn role {role!r}"
            )
        content = row.get("content") or ""
        message_id = str(row.get("id") or uuid4())
        if role in _SUPPORTED_BOUNDARY_ROLES:
            messages.append(HumanMessage(content=content, id=message_id))
            continue
        if role in {"ai", "assistant"}:
            raw_calls = _json_value(row.get("tool_calls"), field="tool_calls") or []
            if not isinstance(raw_calls, list):
                raise SessionMemoryEffectPermanentError(
                    "session transcript tool_calls must be an array"
                )
            calls = []
            for call in raw_calls:
                if not isinstance(call, Mapping):
                    raise SessionMemoryEffectPermanentError(
                        "session transcript tool call must be an object"
                    )
                calls.append(
                    {
                        "id": str(call.get("id") or ""),
                        "name": str(call.get("name") or ""),
                        "args": call.get("args") or {},
                    }
                )
            pending_tool_call_ids = [call["id"] for call in calls]
            messages.append(AIMessage(content=content, tool_calls=calls, id=message_id))
            continue

        fallback_id = pending_tool_call_ids.pop(0) if pending_tool_call_ids else ""
        messages.append(
            ToolMessage(
                content=content,
                tool_call_id=str(row.get("tool_call_id") or fallback_id),
                id=message_id,
            )
        )
    return tuple(messages)


def _require_explicit_llm_transport(config: LLMConfig, *, role: str) -> None:
    """Refuse process-environment credential fallback in the resident drain."""

    if not config.model:
        raise RuntimeError(f"resolved session {role} model is missing")
    if not config.api_key:
        raise RuntimeError(
            f"resolved session {role} transport has no explicit API credential"
        )


def _build_auxiliary_llm(config: Any) -> AuxiliaryLLM:
    """Build a tenant-local aux chain with the session main model as fallback."""

    fallback_config = config.llm.get_phase_config("summarization")
    _require_explicit_llm_transport(fallback_config, role="main")
    fallback_settings = resolve_model_settings(
        fallback_config.model, config._deployment_dir
    )
    fallback_llm = create_llm(fallback_config, limits=config.limits)
    fallback_window = fallback_config.model_max_context_tokens or getattr(
        config.limits, "model_max_context_tokens", None
    )
    fallback_method = fallback_settings.get("structured_output_method", "json_schema")

    aux_config = config.auxiliary
    if not aux_config.enabled or not aux_config.model:
        return AuxiliaryLLM(
            llm=fallback_llm,
            max_iterations=aux_config.max_iterations,
            timeout=aux_config.timeout,
            max_context_tokens=fallback_window,
            structured_output_method=fallback_method,
        )

    model_settings = resolve_model_settings(aux_config.model, config._deployment_dir)
    llm_config = build_auxiliary_llm_config(aux_config, model_settings)
    _require_explicit_llm_transport(llm_config, role="auxiliary")
    return AuxiliaryLLM(
        llm=create_llm(llm_config, limits=config.limits),
        max_iterations=aux_config.max_iterations,
        timeout=aux_config.timeout,
        max_context_tokens=(llm_config.model_max_context_tokens or fallback_window),
        structured_output_method=model_settings.get(
            "structured_output_method", "json_schema"
        ),
        fallback_llm=fallback_llm,
        fallback_structured_output_method=fallback_method,
    )


def _build_embedding_service(config: Any) -> EmbeddingService:
    """Construct one explicit tenant profile without reading/mutating env."""

    env = (config.extra or {}).get("env_keys") or {}
    if not isinstance(env, Mapping):
        raise RuntimeError("resolved session env_keys must be an object")
    provider = str(env.get("EMBEDDING_PROVIDER") or "local").lower()
    model = str(env.get("EMBEDDING_MODEL") or "qwen3-embedding-8b")
    api_key = env.get("EMBEDDING_API_KEY")
    if not api_key and provider == "openrouter":
        api_key = env.get("OPENROUTER_API_KEY")
    if not api_key:
        raise RuntimeError(
            "resolved session embedding transport has no explicit API credential"
        )
    base_url = env.get("EMBEDDING_BASE_URL")
    if not base_url:
        base_url = (
            EmbeddingService.OPENROUTER_API_URL
            if provider == "openrouter"
            else EmbeddingService.OPENAI_API_URL
        )
    try:
        dimensions = int(env.get("EMBEDDING_DIMENSIONS") or 4096)
    except (TypeError, ValueError) as exc:
        raise RuntimeError("resolved session embedding dimensions are invalid") from exc
    return EmbeddingService(
        model=model,
        base_url=str(base_url),
        api_key=str(api_key),
        provider=provider,
        expected_dimensions=dimensions,
        profile_identity=(
            str(env["EMBEDDING_PROFILE_ID"])
            if env.get("EMBEDDING_PROFILE_ID")
            else None
        ),
    )


class SessionMemoryEffectExecutor:
    """Resolve and strictly execute one immutable session-turn obligation."""

    def __init__(self, app_db: Any, vector_db: Any, config_resolver: ConfigResolver):
        self._app_db = app_db
        self._vector_db = vector_db
        self._config_resolver = config_resolver

    async def _completed_vector_receipt(
        self, identity: _EffectIdentity
    ) -> dict[str, Any] | None:
        """Preflight the durable destination before any auxiliary work."""

        async with self._vector_db.acquire() as conn:
            row = await conn.fetchrow(
                _LEDGER_SELECT_SQL,
                identity.producer_id,
                identity.effect_name,
            )
        if row is None:
            return None
        return _ledger_receipt(row, identity)

    async def _load_source(
        self, identity: _EffectIdentity
    ) -> _TurnSource | dict[str, Any]:
        async with self._app_db.acquire() as conn:
            thread_row = await conn.fetchrow(
                "SELECT * FROM threads WHERE id = $1::uuid",
                identity.thread_id,
            )
            if thread_row is None:
                return _output(
                    "no_source_thread_deleted", turn_number=identity.turn_number
                )
            boundary = await conn.fetchrow(
                """
                SELECT id, seq, role, turn_number, turn_execution_id, rewound_at
                FROM thread_messages
                WHERE thread_id = $1::uuid AND id = $2::uuid
                """,
                identity.thread_id,
                identity.input_message_id,
            )
            if boundary is None:
                return _output(
                    "no_source_boundary_missing", turn_number=identity.turn_number
                )
            if boundary["rewound_at"] is not None:
                return _output(
                    "no_source_boundary_rewound", turn_number=identity.turn_number
                )
            if (
                int(boundary["seq"]) != identity.boundary_seq
                or int(boundary["turn_number"] or 0) != identity.turn_number
                or str(boundary["turn_execution_id"] or "") != str(identity.producer_id)
            ):
                raise SessionMemoryEffectPermanentError(
                    "session memory effect does not match its durable turn boundary"
                )
            rows = await conn.fetch(
                """
                SELECT id, seq, role, content, tool_calls, tool_call_id,
                       turn_number, rewound_at
                FROM thread_messages
                WHERE thread_id = $1::uuid
                  AND turn_number = $2
                  AND seq BETWEEN $3 AND $4
                ORDER BY seq ASC, id ASC
                """,
                identity.thread_id,
                identity.turn_number,
                identity.boundary_seq,
                identity.end_seq,
            )
        if not rows:
            return _output("no_source_turn_missing", turn_number=identity.turn_number)
        if any(row["rewound_at"] is not None for row in rows):
            return _output("no_source_turn_rewound", turn_number=identity.turn_number)
        if (
            str(rows[0]["id"]) != str(identity.input_message_id)
            or int(rows[0]["seq"]) != identity.boundary_seq
        ):
            raise SessionMemoryEffectPermanentError(
                "session memory effect frozen window does not start at its boundary"
            )
        if int(rows[-1]["seq"]) != identity.end_seq:
            raise SessionMemoryEffectPermanentError(
                "session memory effect frozen window does not end at its boundary"
            )
        # The exact fenced id/seq/producer tuple is the input authority. Event
        # wakes and other accepted system inputs intentionally persist a
        # transcript-specific role while entering model context as human.
        message_rows = [dict(row) for row in rows]
        message_rows[0]["role"] = "human"
        return _TurnSource(
            thread=dict(thread_row),
            messages=_messages_from_rows(message_rows),
        )

    async def _source_disposition(self, identity: _EffectIdentity) -> str | None:
        """Recheck after auxiliary work, immediately before vector mutation."""

        async with self._app_db.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT turn_number, turn_execution_id, rewound_at
                FROM thread_messages
                WHERE thread_id = $1::uuid
                  AND id = $2::uuid
                  AND seq = $3
                """,
                identity.thread_id,
                identity.input_message_id,
                identity.boundary_seq,
            )
        if row is None:
            return "no_source_boundary_missing"
        if row["rewound_at"] is not None:
            return "no_source_boundary_rewound"
        if int(row["turn_number"] or 0) != identity.turn_number or str(
            row["turn_execution_id"] or ""
        ) != str(identity.producer_id):
            raise SessionMemoryEffectPermanentError(
                "session memory effect lost its exact durable turn identity"
            )
        async with self._app_db.acquire() as conn:
            rewound = await conn.fetchval(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM thread_messages
                    WHERE thread_id = $1::uuid
                      AND turn_number = $2
                      AND seq BETWEEN $3 AND $4
                      AND rewound_at IS NOT NULL
                )
                """,
                identity.thread_id,
                identity.turn_number,
                identity.boundary_seq,
                identity.end_seq,
            )
        if rewound:
            return "no_source_turn_rewound"
        return None

    async def __call__(self, effect: SessionMemoryEffect) -> Mapping[str, Any]:
        identity = _identity(effect)
        authority_permit = effect.authority_permit
        if authority_permit is None or not callable(authority_permit):
            raise SessionMemoryEffectPermanentError(
                "session memory executor requires an exact-owner authority permit"
            )
        completed = await self._completed_vector_receipt(identity)
        if completed is not None:
            return completed
        source = await self._load_source(identity)
        if isinstance(source, dict):
            return source

        resolved = await self._config_resolver(
            source.thread,
            identity.memory_scope_kind,
            identity.memory_scope_id,
        )
        if not isinstance(resolved, Mapping) or not isinstance(
            resolved.get("agent"), Mapping
        ):
            raise RuntimeError("session memory config resolver returned no config")
        config = load_config_from_resolved(dict(resolved))
        task_config = config.auxiliary.tasks.get("extract_memories")
        if not config.memory.enabled or task_config is None or not task_config.enabled:
            return _output("memory_disabled", turn_number=identity.turn_number)

        auxiliary_llm = _build_auxiliary_llm(config)
        embedding_service = _build_embedding_service(config)
        project_id = (
            identity.memory_scope_id
            if identity.memory_scope_kind == "project"
            else None
        )
        task = ExtractMemoriesTask(
            messages=list(source.messages),
            prompt=resolve_memory_extraction_prompt(config),
            phase=0,
        )
        try:
            result = await auxiliary_llm.chain(task)
            extracted = len(result.memories)
            disposition = await self._source_disposition(identity)
            if disposition is not None:
                return _output(
                    disposition,
                    turn_number=identity.turn_number,
                    extracted_count=extracted,
                )

            # Only the ledger claimant may mutate the destination.  The
            # ON-CONFLICT statement waits for a concurrent claimant's outcome:
            # a committed winner yields its fixed receipt, while a rolled-back
            # claimant lets this transaction insert and perform the writes.
            await authority_permit()
            async with self._vector_db.acquire() as conn:
                async with conn.transaction():
                    inserted = await conn.fetchrow(
                        _LEDGER_INSERT_SQL,
                        *_ledger_identity_args(identity),
                    )
                    if inserted is None:
                        existing = await conn.fetchrow(
                            _LEDGER_SELECT_SQL,
                            identity.producer_id,
                            identity.effect_name,
                        )
                        if existing is None:
                            raise RuntimeError(
                                "session memory destination ledger conflict vanished"
                            )
                        receipt = _ledger_receipt(existing, identity)
                        # The conflict wait may outlive the app-DB claim. Even
                        # this write-free transaction must not report success
                        # on behalf of an owner that can no longer settle it.
                        await authority_permit()
                    else:
                        recall_store = RecallStore(
                            db=conn,
                            embedding_service=embedding_service,
                            job_id=identity.thread_id,
                            config=config.memory,
                            agent_id=config.agent_id,
                            project_id=project_id,
                            project_ids=(
                                [project_id] if project_id is not None else []
                            ),
                            strict_writes=True,
                        )
                        # The captured destination, not a later config toggle,
                        # is authoritative. Ingestion verdict subwrites share
                        # this strict transaction too.
                        recall_store.project_scoped = (
                            identity.memory_scope_kind == "project"
                        )
                        maybe_attach_ingestion_verdict(
                            recall_store,
                            auxiliary_llm,
                            config.memory,
                        )

                        stored = 0
                        for memory in result.memories:
                            await authority_permit()
                            memory_id = await recall_store.store(
                                content=memory.content,
                                summary=memory.summary,
                                keywords=memory.keywords,
                                importance=memory.importance,
                                memory_type=memory.type,
                                source="observer",
                                source_turn_start=identity.turn_number,
                                source_turn_end=identity.turn_number,
                                source_phase=0,
                                retrieval_messages=(memory.retrieval_messages or None),
                            )
                            # A store can include embeddings, verdict inference,
                            # and several SQL mutations. Re-fence afterwards so
                            # any observed mid-store ownership loss rolls the
                            # complete vector transaction back.
                            await authority_permit()
                            if memory_id is not None:
                                stored += 1
                        await authority_permit()
                        finished = await conn.fetchrow(
                            _LEDGER_FINISH_SQL,
                            *_ledger_identity_args(identity),
                            extracted,
                            stored,
                        )
                        if finished is None:
                            raise RuntimeError(
                                "session memory destination ledger did not finish"
                            )
                        receipt = _ledger_receipt(finished, identity)
                        # Last await before leaving the transaction context and
                        # committing the ledger plus every memory mutation.
                        await authority_permit()
        except Exception as exc:
            auxiliary_llm.health.record_failure("memory_extraction", exc)
            raise
        auxiliary_llm.health.record_success("memory_extraction")
        return receipt
