"""Focused contracts for the strict session-turn memory executor."""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest

from orchestrator.services.session_memory_effects import (
    SESSION_MEMORY_EFFECT_GROUP,
    SESSION_MEMORY_EFFECT_NAME,
    SessionMemoryEffect,
    SessionMemoryEffectLeaseLost,
    SessionMemoryEffectPermanentError,
)
from orchestrator.services.session_memory_executor import (
    SessionMemoryEffectExecutor,
    _build_embedding_service,
)

PRODUCER_ID = UUID("11111111-aaaa-4111-8111-111111111111")
THREAD_ID = UUID("22222222-bbbb-4222-8222-222222222222")
INPUT_ID = UUID("33333333-cccc-4333-8333-333333333333")
PROJECT_ID = UUID("44444444-dddd-4444-8444-444444444444")
_DEFAULT_PERMIT = object()


def _effect(
    *,
    authority_permit: object = _DEFAULT_PERMIT,
    **detail_overrides: object,
) -> SessionMemoryEffect:
    detail = {
        "input_message_id": str(INPUT_ID),
        "turn_number": 7,
        "boundary_seq": 101,
        "end_seq": 103,
        "memory_scope_kind": "project",
        "memory_scope_id": str(PROJECT_ID),
    }
    detail.update(detail_overrides)
    now = datetime.now(UTC)
    return SessionMemoryEffect(
        producer_id=str(PRODUCER_ID),
        scope_id=str(THREAD_ID),
        effect_name=SESSION_MEMORY_EFFECT_NAME,
        effect_group=SESSION_MEMORY_EFFECT_GROUP,
        attempts=1,
        max_attempts=5,
        created_at=now,
        complete_by=now + timedelta(minutes=2),
        detail=detail,
        authority_permit=(
            AsyncMock() if authority_permit is _DEFAULT_PERMIT else authority_permit
        ),
    )


class _Acquire:
    def __init__(self, conn: object) -> None:
        self._conn = conn

    async def __aenter__(self) -> object:
        return self._conn

    async def __aexit__(self, *_args: object) -> None:
        return None


class _ExecutorConnection:
    def __init__(
        self,
        *,
        thread_exists: bool = True,
        rewound: bool = False,
        end_seq: int = 103,
    ) -> None:
        self.thread_exists = thread_exists
        self.rewound = rewound
        self.end_seq = end_seq
        self.fetch_calls: list[tuple[str, tuple[object, ...]]] = []

    async def fetchrow(self, sql: str, *args: object) -> dict | None:
        if "SELECT * FROM threads" in sql:
            return (
                {
                    "id": THREAD_ID,
                    "user_id": UUID("55555555-eeee-4555-8555-555555555555"),
                    "project_id": UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
                    "metadata": {},
                }
                if self.thread_exists
                else None
            )
        if "role, turn_number" in sql:
            return {
                "id": INPUT_ID,
                "seq": 101,
                "role": "human",
                "turn_number": 7,
                "turn_execution_id": PRODUCER_ID,
                "rewound_at": datetime.now(UTC) if self.rewound else None,
            }
        if "SELECT turn_number, turn_execution_id" in sql:
            return {
                "turn_number": 7,
                "turn_execution_id": PRODUCER_ID,
                "rewound_at": None,
            }
        raise AssertionError(sql)

    async def fetch(self, sql: str, *args: object) -> list[dict]:
        self.fetch_calls.append((sql, args))
        return [
            {
                "id": INPUT_ID,
                "seq": 101,
                "role": "human",
                "content": "remember this",
                "tool_calls": None,
                "tool_call_id": None,
                "turn_number": 7,
                "rewound_at": None,
            },
            {
                "id": UUID("66666666-ffff-4666-8666-666666666666"),
                "seq": self.end_seq,
                "role": "ai",
                "content": "done",
                "tool_calls": None,
                "tool_call_id": None,
                "turn_number": 7,
                "rewound_at": None,
            },
        ]

    async def fetchval(self, sql: str, *args: object) -> bool:
        assert "seq BETWEEN $3 AND $4" in sql
        assert args == (THREAD_ID, 7, 101, 103)
        return False


class _AppDB:
    def __init__(self, conn: _ExecutorConnection) -> None:
        self.conn = conn

    def acquire(self) -> _Acquire:
        return _Acquire(self.conn)


class _Transaction:
    def __init__(self, conn: "_VectorConnection") -> None:
        self._conn = conn
        self._snapshot: dict | None = None

    async def __aenter__(self) -> None:
        self._snapshot = dict(self._conn.ledger) if self._conn.ledger else None

    async def __aexit__(self, exc_type, *_args: object) -> None:
        if exc_type is not None:
            self._conn.ledger = self._snapshot


class _VectorConnection:
    def __init__(self, ledger: dict | None = None) -> None:
        self.ledger = dict(ledger) if ledger else None

    def transaction(self) -> _Transaction:
        return _Transaction(self)

    async def fetchrow(self, sql: str, *args: object) -> dict | None:
        if "SELECT" in sql and "session_memory_effect_executions" in sql:
            return dict(self.ledger) if self.ledger else None
        if "INSERT INTO session_memory_effect_executions" in sql:
            if self.ledger is not None:
                return None
            self.ledger = {
                "producer_id": args[0],
                "effect_name": args[1],
                "thread_id": args[2],
                "input_message_id": args[3],
                "turn_number": args[4],
                "boundary_seq": args[5],
                "end_seq": args[6],
                "memory_scope_kind": args[7],
                "memory_scope_id": args[8],
                "state": "writing",
                "extracted_count": None,
                "stored_count": None,
                "created_at": datetime.now(UTC),
                "completed_at": None,
            }
            return {"producer_id": args[0]}
        if "UPDATE session_memory_effect_executions" in sql:
            if self.ledger is None or self.ledger["state"] != "writing":
                return None
            identity_fields = (
                "producer_id",
                "effect_name",
                "thread_id",
                "input_message_id",
                "turn_number",
                "boundary_seq",
                "end_seq",
                "memory_scope_kind",
                "memory_scope_id",
            )
            if any(
                self.ledger[field] != value
                for field, value in zip(identity_fields, args)
            ):
                return None
            self.ledger.update(
                state="done",
                extracted_count=args[9],
                stored_count=args[10],
                completed_at=datetime.now(UTC),
            )
            return dict(self.ledger)
        raise AssertionError(sql)


class _VectorDB:
    def __init__(self, ledger: dict | None = None) -> None:
        self.conn = _VectorConnection(ledger)

    def acquire(self) -> _Acquire:
        return _Acquire(self.conn)


def _completed_ledger(**overrides: object) -> dict:
    row = {
        "producer_id": PRODUCER_ID,
        "effect_name": SESSION_MEMORY_EFFECT_NAME,
        "thread_id": THREAD_ID,
        "input_message_id": INPUT_ID,
        "turn_number": 7,
        "boundary_seq": 101,
        "end_seq": 103,
        "memory_scope_kind": "project",
        "memory_scope_id": PROJECT_ID,
        "state": "done",
        "extracted_count": 2,
        "stored_count": 1,
        "created_at": datetime.now(UTC),
        "completed_at": datetime.now(UTC),
    }
    row.update(overrides)
    return row


def _config() -> SimpleNamespace:
    return SimpleNamespace(
        memory=SimpleNamespace(enabled=True),
        auxiliary=SimpleNamespace(
            tasks={"extract_memories": SimpleNamespace(enabled=True)}
        ),
        agent_id="session",
    )


def _memory() -> SimpleNamespace:
    return SimpleNamespace(
        content="The user likes exact boundaries.",
        summary="Exact boundaries",
        keywords=["boundary"],
        importance=0.8,
        type="factual",
        retrieval_messages=[],
    )


@pytest.mark.asyncio
async def test_executor_uses_frozen_range_and_captured_project() -> None:
    conn = _ExecutorConnection()
    vector = _VectorDB()
    resolver = AsyncMock(return_value={"agent": {}})
    aux = SimpleNamespace(
        chain=AsyncMock(return_value=SimpleNamespace(memories=[_memory()])),
        health=SimpleNamespace(record_success=MagicMock(), record_failure=MagicMock()),
    )
    store = SimpleNamespace(store=AsyncMock(return_value=PROJECT_ID))

    with (
        patch(
            "orchestrator.services.session_memory_executor.load_config_from_resolved",
            return_value=_config(),
        ),
        patch(
            "orchestrator.services.session_memory_executor._build_auxiliary_llm",
            return_value=aux,
        ),
        patch(
            "orchestrator.services.session_memory_executor._build_embedding_service",
            return_value=object(),
        ),
        patch(
            "orchestrator.services.session_memory_executor.RecallStore",
            return_value=store,
        ) as recall_cls,
        patch(
            "orchestrator.services.session_memory_executor.resolve_memory_extraction_prompt",
            return_value="extract",
        ),
    ):
        result = await SessionMemoryEffectExecutor(_AppDB(conn), vector, resolver)(
            _effect()
        )

    assert result == {
        "disposition": "completed",
        "turn_number": 7,
        "extracted_count": 1,
        "stored_count": 1,
    }
    resolver.assert_awaited_once()
    assert resolver.await_args.args[1:] == ("project", PROJECT_ID)
    assert resolver.await_args.args[0]["project_id"] != PROJECT_ID
    assert len(conn.fetch_calls) == 1
    source_sql, source_args = conn.fetch_calls[0]
    assert "seq BETWEEN $3 AND $4" in source_sql
    assert "seq >=" not in source_sql
    assert source_args == (THREAD_ID, 7, 101, 103)
    assert recall_cls.call_args.kwargs["job_id"] == THREAD_ID
    assert recall_cls.call_args.kwargs["project_id"] == PROJECT_ID
    assert recall_cls.call_args.kwargs["project_ids"] == [PROJECT_ID]
    assert recall_cls.call_args.kwargs["db"] is vector.conn
    assert recall_cls.call_args.kwargs["strict_writes"] is True
    store.store.assert_awaited_once()
    assert store.store.await_args.kwargs["source_turn_start"] == 7
    assert store.store.await_args.kwargs["source_turn_end"] == 7
    assert store.store.await_args.kwargs["content"] == _memory().content


@pytest.mark.asyncio
async def test_store_failure_propagates_instead_of_settling_success() -> None:
    vector = _VectorDB()
    resolver = AsyncMock(return_value={"agent": {}})
    aux = SimpleNamespace(
        chain=AsyncMock(return_value=SimpleNamespace(memories=[_memory()])),
        health=SimpleNamespace(record_success=MagicMock(), record_failure=MagicMock()),
    )
    store = SimpleNamespace(store=AsyncMock(side_effect=RuntimeError("vector down")))

    with (
        patch(
            "orchestrator.services.session_memory_executor.load_config_from_resolved",
            return_value=_config(),
        ),
        patch(
            "orchestrator.services.session_memory_executor._build_auxiliary_llm",
            return_value=aux,
        ),
        patch(
            "orchestrator.services.session_memory_executor._build_embedding_service",
            return_value=object(),
        ),
        patch(
            "orchestrator.services.session_memory_executor.RecallStore",
            return_value=store,
        ),
        patch(
            "orchestrator.services.session_memory_executor.resolve_memory_extraction_prompt",
            return_value="extract",
        ),
    ):
        with pytest.raises(RuntimeError, match="vector down"):
            await SessionMemoryEffectExecutor(
                _AppDB(_ExecutorConnection()), vector, resolver
            )(_effect())

    aux.health.record_failure.assert_called_once()
    aux.health.record_success.assert_not_called()
    assert vector.conn.ledger is None


@pytest.mark.asyncio
async def test_authority_loss_before_vector_transaction_starts_no_write() -> None:
    vector = _VectorDB()
    permit = AsyncMock(side_effect=SessionMemoryEffectLeaseLost("lost"))
    resolver = AsyncMock(return_value={"agent": {}})
    aux = SimpleNamespace(
        chain=AsyncMock(return_value=SimpleNamespace(memories=[_memory()])),
        health=SimpleNamespace(record_success=MagicMock(), record_failure=MagicMock()),
    )

    with (
        patch(
            "orchestrator.services.session_memory_executor.load_config_from_resolved",
            return_value=_config(),
        ),
        patch(
            "orchestrator.services.session_memory_executor._build_auxiliary_llm",
            return_value=aux,
        ),
        patch(
            "orchestrator.services.session_memory_executor._build_embedding_service",
            return_value=object(),
        ),
        patch(
            "orchestrator.services.session_memory_executor.RecallStore"
        ) as recall_cls,
        patch(
            "orchestrator.services.session_memory_executor.resolve_memory_extraction_prompt",
            return_value="extract",
        ),
    ):
        with pytest.raises(SessionMemoryEffectLeaseLost):
            await SessionMemoryEffectExecutor(
                _AppDB(_ExecutorConnection()), vector, resolver
            )(_effect(authority_permit=permit))

    assert vector.conn.ledger is None
    recall_cls.assert_not_called()


@pytest.mark.asyncio
async def test_authority_loss_after_store_rolls_back_destination_ledger() -> None:
    vector = _VectorDB()
    permit = AsyncMock(
        side_effect=[None, None, SessionMemoryEffectLeaseLost("lost after store")]
    )
    resolver = AsyncMock(return_value={"agent": {}})
    aux = SimpleNamespace(
        chain=AsyncMock(return_value=SimpleNamespace(memories=[_memory(), _memory()])),
        health=SimpleNamespace(record_success=MagicMock(), record_failure=MagicMock()),
    )
    store = SimpleNamespace(store=AsyncMock(return_value=PROJECT_ID))

    with (
        patch(
            "orchestrator.services.session_memory_executor.load_config_from_resolved",
            return_value=_config(),
        ),
        patch(
            "orchestrator.services.session_memory_executor._build_auxiliary_llm",
            return_value=aux,
        ),
        patch(
            "orchestrator.services.session_memory_executor._build_embedding_service",
            return_value=object(),
        ),
        patch(
            "orchestrator.services.session_memory_executor.RecallStore",
            return_value=store,
        ),
        patch(
            "orchestrator.services.session_memory_executor.resolve_memory_extraction_prompt",
            return_value="extract",
        ),
    ):
        with pytest.raises(SessionMemoryEffectLeaseLost):
            await SessionMemoryEffectExecutor(
                _AppDB(_ExecutorConnection()), vector, resolver
            )(_effect(authority_permit=permit))

    assert store.store.await_count == 1
    assert vector.conn.ledger is None


@pytest.mark.asyncio
async def test_authority_loss_at_commit_edge_rolls_back_finished_ledger() -> None:
    vector = _VectorDB()
    permit = AsyncMock(
        side_effect=[None, None, SessionMemoryEffectLeaseLost("lost before commit")]
    )
    resolver = AsyncMock(return_value={"agent": {}})
    aux = SimpleNamespace(
        chain=AsyncMock(return_value=SimpleNamespace(memories=[])),
        health=SimpleNamespace(record_success=MagicMock(), record_failure=MagicMock()),
    )

    with (
        patch(
            "orchestrator.services.session_memory_executor.load_config_from_resolved",
            return_value=_config(),
        ),
        patch(
            "orchestrator.services.session_memory_executor._build_auxiliary_llm",
            return_value=aux,
        ),
        patch(
            "orchestrator.services.session_memory_executor._build_embedding_service",
            return_value=object(),
        ),
        patch(
            "orchestrator.services.session_memory_executor.RecallStore"
        ) as recall_cls,
        patch(
            "orchestrator.services.session_memory_executor.resolve_memory_extraction_prompt",
            return_value="extract",
        ),
    ):
        with pytest.raises(SessionMemoryEffectLeaseLost):
            await SessionMemoryEffectExecutor(
                _AppDB(_ExecutorConnection()), vector, resolver
            )(_effect(authority_permit=permit))

    assert vector.conn.ledger is None
    recall_cls.assert_called_once()


@pytest.mark.asyncio
async def test_deleted_source_settles_with_fixed_no_source_receipt() -> None:
    resolver = AsyncMock()
    result = await SessionMemoryEffectExecutor(
        _AppDB(_ExecutorConnection(thread_exists=False)), _VectorDB(), resolver
    )(_effect())

    assert result == {
        "disposition": "no_source_thread_deleted",
        "turn_number": 7,
        "extracted_count": 0,
        "stored_count": 0,
    }
    resolver.assert_not_awaited()


@pytest.mark.asyncio
async def test_rewound_boundary_settles_without_resolving_tenant_config() -> None:
    resolver = AsyncMock()
    result = await SessionMemoryEffectExecutor(
        _AppDB(_ExecutorConnection(rewound=True)), _VectorDB(), resolver
    )(_effect())

    assert result["disposition"] == "no_source_boundary_rewound"
    assert set(result) == {
        "disposition",
        "turn_number",
        "extracted_count",
        "stored_count",
    }
    resolver.assert_not_awaited()


@pytest.mark.asyncio
async def test_completed_vector_receipt_skips_source_config_and_auxiliary_work() -> (
    None
):
    resolver = AsyncMock()
    result = await SessionMemoryEffectExecutor(
        object(), _VectorDB(_completed_ledger()), resolver
    )(_effect())

    assert result == {
        "disposition": "completed",
        "turn_number": 7,
        "extracted_count": 2,
        "stored_count": 1,
    }
    resolver.assert_not_awaited()


@pytest.mark.asyncio
async def test_vector_receipt_identity_conflict_is_permanent() -> None:
    conflict = _completed_ledger(memory_scope_id=THREAD_ID)
    with pytest.raises(
        SessionMemoryEffectPermanentError,
        match="ledger identity conflicts",
    ):
        await SessionMemoryEffectExecutor(object(), _VectorDB(conflict), AsyncMock())(
            _effect()
        )


@pytest.mark.asyncio
async def test_frozen_window_requires_exact_end_row() -> None:
    resolver = AsyncMock()
    with pytest.raises(
        SessionMemoryEffectPermanentError,
        match="does not end",
    ):
        await SessionMemoryEffectExecutor(
            _AppDB(_ExecutorConnection(end_seq=102)), _VectorDB(), resolver
        )(_effect())
    resolver.assert_not_awaited()


@pytest.mark.parametrize(
    ("field", "value"),
    [("boundary_seq", 0), ("end_seq", "nope"), ("memory_scope_kind", "current")],
)
@pytest.mark.asyncio
async def test_malformed_frozen_identity_is_permanent(
    field: str, value: object
) -> None:
    with pytest.raises(SessionMemoryEffectPermanentError):
        await SessionMemoryEffectExecutor(
            _AppDB(_ExecutorConnection()), object(), AsyncMock()
        )(_effect(**{field: value}))


_CONTEXT_TEXT = '<srw_context kind="memory">\n[m:3f9a2c] injected fact\n</srw_context>'


class _ContextRowConnection(_ExecutorConnection):
    """A frozen window with typed context entries, ending on one
    (append-only context injection: the entries of a turn are its rows)."""

    async def fetch(self, sql: str, *args: object) -> list[dict]:
        human, ai = await super().fetch(sql, *args)

        def context(seq: int, row_id: str) -> dict:
            return {
                "id": UUID(row_id),
                "seq": seq,
                "role": "context",
                "content": _CONTEXT_TEXT,
                "tool_calls": None,
                "tool_call_id": None,
                "turn_number": 7,
                "rewound_at": None,
            }

        return [
            human,
            context(102, "77777777-1111-4777-8777-777777777777"),
            {**ai, "seq": 103},
            context(104, "88888888-2222-4888-8888-888888888888"),
        ]


@pytest.mark.asyncio
async def test_context_rows_in_the_frozen_window_are_skipped() -> None:
    """Context rows are harness context, not conversation: the drain skips
    them instead of failing permanently on an unknown role, and a window
    that ends on one still matches its frozen end."""
    from orchestrator.services.session_memory_executor import _identity

    executor = SessionMemoryEffectExecutor(
        _AppDB(_ContextRowConnection()), _VectorDB(), AsyncMock()
    )

    source = await executor._load_source(_identity(_effect(end_seq=104)))

    assert [type(m).__name__ for m in source.messages] == ["HumanMessage", "AIMessage"]
    assert [m.content for m in source.messages] == ["remember this", "done"]
    assert all(_CONTEXT_TEXT not in str(m.content) for m in source.messages)


def test_unknown_roles_still_fail_permanently() -> None:
    from orchestrator.services.session_memory_executor import _messages_from_rows

    assert _messages_from_rows([{"role": "context", "content": "x"}]) == ()
    with pytest.raises(SessionMemoryEffectPermanentError, match="unsupported"):
        _messages_from_rows([{"role": "mystery", "content": "x"}])


def test_embedding_profile_is_explicit_and_does_not_touch_process_env() -> None:
    before = dict(os.environ)
    config = SimpleNamespace(
        extra={
            "env_keys": {
                "EMBEDDING_PROVIDER": "openrouter",
                "EMBEDDING_MODEL": "tenant/model",
                "EMBEDDING_BASE_URL": "https://tenant.invalid/v1",
                "EMBEDDING_API_KEY": "tenant-secret",
                "EMBEDDING_DIMENSIONS": "4096",
                "EMBEDDING_PROFILE_ID": "tenant:endpoint",
            }
        }
    )

    with patch(
        "orchestrator.services.session_memory_executor.EmbeddingService"
    ) as service_cls:
        _build_embedding_service(config)

    assert service_cls.call_args.kwargs == {
        "model": "tenant/model",
        "base_url": "https://tenant.invalid/v1",
        "api_key": "tenant-secret",
        "provider": "openrouter",
        "expected_dimensions": 4096,
        "profile_identity": "tenant:endpoint",
    }
    assert os.environ == before
