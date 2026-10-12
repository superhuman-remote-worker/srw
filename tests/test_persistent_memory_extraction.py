"""Regression tests for B1 — persistent memory extraction broken three ways.

See knowledge-base/knowledge/issues/memory_bugs.md B1. The original bug: three call sites read
phantom attributes (``extraction_interval``, ``extraction_prompt``) off
``MemoryConfig``, which only survived in tests because MagicMock configs
fabricate any attribute. Every test here that exercises extraction wiring
uses the REAL ``MemoryConfig`` dataclass so a reintroduced phantom-attribute
access fails loudly instead of being swallowed by the non-fatal handlers.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from langchain_core.messages import AIMessage, HumanMessage

from agent.api.persistent_app import _handle_archive, _handle_idle_archive
from agent.api.persistent_session import (
    PersistentSession,
    resolve_memory_extraction_prompt,
)
from shared.runtime.core.loader import AgentConfig, MemoryConfig
from agent.persistent_graph import PersistentLoopCallbacks, run_persistent_loop


def _make_callbacks(**overrides) -> PersistentLoopCallbacks:
    defaults = dict(
        get_user_input=AsyncMock(return_value="hello"),
        on_token=AsyncMock(),
        on_thinking=AsyncMock(),
        on_tool_start=AsyncMock(),
        on_tool_result=AsyncMock(),
        permission_check=AsyncMock(return_value=True),
        on_turn_start=AsyncMock(),
        on_turn_complete=AsyncMock(),
        on_error=AsyncMock(),
        check_interrupt=MagicMock(return_value=False),
        on_vm_upgrade_needed=None,
    )
    defaults.update(overrides)
    return PersistentLoopCallbacks(**defaults)


def _make_streaming_llm(content="ok"):
    llm = AsyncMock()
    llm.reasoning = None
    response = AIMessage(content=content)

    async def _astream(messages, **kw):
        yield response

    llm.astream = _astream
    llm.ainvoke = AsyncMock(return_value=response)
    return llm


def _loop_config(observer_interval: int) -> MagicMock:
    """Loop config whose memory section is the REAL MemoryConfig."""
    cfg = MagicMock()
    cfg.llm.timeout = 600
    cfg.memory = MemoryConfig(enabled=True, observer_interval=observer_interval)
    cfg.context_management.max_summary_length = 10000
    return cfg


class TestMemoryConfigContract:
    """Canary: the attrs the persistent path reads must exist on the dataclass."""

    def test_no_phantom_extraction_attrs(self):
        mc = MemoryConfig()
        assert not hasattr(mc, "extraction_interval")
        assert not hasattr(mc, "extraction_prompt")

    def test_observer_interval_is_the_real_cadence_field(self):
        assert MemoryConfig(observer_interval=3).observer_interval == 3

    def test_session_carries_prompt_field(self):
        session = PersistentSession(thread_id="t", config=MagicMock())
        assert session.memory_extraction_prompt == ""


class TestResolveMemoryExtractionPrompt:
    def test_resolves_real_prompt_through_matrix(self):
        """A bare AgentConfig resolves the actual framework prompt file."""
        cfg = AgentConfig(agent_id="t", display_name="T")
        prompt = resolve_memory_extraction_prompt(cfg)
        assert len(prompt) > 100
        assert "memory" in prompt.lower()

    def test_load_failure_returns_empty_not_raise(self):
        cfg = AgentConfig(agent_id="t", display_name="T")
        with patch(
            "shared.runtime.services.memory_prompts.load_auxiliary_prompt",
            side_effect=FileNotFoundError("missing"),
        ):
            assert resolve_memory_extraction_prompt(cfg) == ""


class TestLoopExtractionWiring:
    """B1 (b)+(c): the loop honors observer_interval and threads the prompt."""

    async def _run_loop(
        self,
        turns: int,
        observer_interval: int,
        prompt: str,
        *,
        defer_memory_extraction_to_outbox: bool = False,
    ):
        count = 0

        async def _input():
            nonlocal count
            count += 1
            if count <= turns:
                return f"turn {count}"
            raise asyncio.CancelledError

        extraction = AsyncMock()
        with patch(
            "shared.runtime.services.auxiliary.extract_and_store_memories", extraction
        ):
            await run_persistent_loop(
                llm_with_tools=_make_streaming_llm(),
                tools=[],
                context_manager=AsyncMock(
                    ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
                ),
                config=_loop_config(observer_interval),
                system_prompt="sys",
                callbacks=_make_callbacks(get_user_input=_input),
                messages=[],
                recall_store=MagicMock(),
                auxiliary_llm=MagicMock(),
                memory_extraction_prompt=prompt,
                defer_memory_extraction_to_outbox=(defer_memory_extraction_to_outbox),
            )
            # Let the fire-and-forget extraction task run
            await asyncio.sleep(0)
        return extraction

    @pytest.mark.asyncio
    async def test_fires_on_configured_observer_interval(self):
        """interval=2 fires at turn 2 — fails if the cadence falls back to 5."""
        extraction = await self._run_loop(
            turns=2, observer_interval=2, prompt="PROMPT SENTINEL"
        )
        extraction.assert_called_once()
        kwargs = extraction.call_args.kwargs
        assert kwargs["memory_extraction_prompt"] == "PROMPT SENTINEL"
        assert kwargs["source_turn_start"] == 0
        assert kwargs["source_turn_end"] == 2

    @pytest.mark.asyncio
    async def test_interval_zero_disables_extraction(self):
        extraction = await self._run_loop(turns=3, observer_interval=0, prompt="unused")
        extraction.assert_not_called()

    @pytest.mark.asyncio
    async def test_outbox_mode_skips_legacy_interval_extraction(self):
        extraction = await self._run_loop(
            turns=2,
            observer_interval=1,
            prompt="owned by outbox",
            defer_memory_extraction_to_outbox=True,
        )
        extraction.assert_not_called()

    @pytest.mark.asyncio
    async def test_outbox_mode_skips_memory_manager_capture(self):
        count = 0

        async def _input():
            nonlocal count
            count += 1
            if count == 1:
                return "turn 1"
            raise asyncio.CancelledError

        payload = MagicMock()
        payload.messages.return_value = []
        payload.blocks = []
        memory_service = MagicMock()
        memory_service.assemble = AsyncMock(return_value=payload)
        memory_service.capture = AsyncMock()
        context_manager = MagicMock()
        context_manager.should_summarize.return_value = False
        context_manager.ensure_within_limits = AsyncMock(
            side_effect=lambda m, *a, **kw: m
        )
        await run_persistent_loop(
            llm_with_tools=_make_streaming_llm(),
            tools=[],
            context_manager=context_manager,
            config=_loop_config(observer_interval=1),
            system_prompt="sys",
            callbacks=_make_callbacks(get_user_input=_input),
            messages=[],
            memory_service=memory_service,
            defer_memory_extraction_to_outbox=True,
        )

        memory_service.capture.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_outbox_mode_skips_pre_compaction_memory_capture(self):
        count = 0

        async def _input():
            nonlocal count
            count += 1
            if count == 1:
                return "turn 1"
            raise asyncio.CancelledError

        payload = MagicMock()
        payload.messages.return_value = []
        payload.blocks = []
        memory_service = MagicMock()
        memory_service.assemble = AsyncMock(return_value=payload)
        memory_service.capture = AsyncMock()
        context_manager = MagicMock()
        context_manager.should_summarize.return_value = True
        context_manager.config.keep_recent_messages = 1
        context_manager.ensure_within_limits = AsyncMock(
            side_effect=lambda m, *a, **kw: m
        )
        await run_persistent_loop(
            llm_with_tools=_make_streaming_llm(),
            tools=[],
            context_manager=context_manager,
            config=_loop_config(observer_interval=1),
            system_prompt="sys",
            callbacks=_make_callbacks(get_user_input=_input),
            messages=[],
            memory_service=memory_service,
            defer_memory_extraction_to_outbox=True,
        )

        memory_service.capture_nowait.assert_not_called()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("project_scoped", "project_ids", "expected_kind", "expected_id"),
        [
            (True, ["project-a", "project-b"], "project", "project-a"),
            (False, ["project-a"], "thread", "thread-a"),
            (True, [], "thread", "thread-a"),
        ],
    )
    async def test_outbox_callback_carries_exact_input_and_destination(
        self,
        project_scoped,
        project_ids,
        expected_kind,
        expected_id,
    ):
        count = 0

        async def _input():
            nonlocal count
            count += 1
            if count == 1:
                return {"content": "turn 1", "id": "accepted-input-a"}
            raise asyncio.CancelledError

        callbacks = _make_callbacks(get_user_input=_input)
        config = _loop_config(observer_interval=1)
        config.memory.project_scoped = project_scoped
        await run_persistent_loop(
            llm_with_tools=_make_streaming_llm(),
            tools=[],
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            config=config,
            system_prompt="sys",
            callbacks=callbacks,
            messages=[],
            project_ids=project_ids,
            memory_thread_id="thread-a",
            defer_memory_extraction_to_outbox=True,
        )

        args = callbacks.on_turn_complete.await_args.args
        assert args[2:] == (
            "accepted-input-a",
            expected_kind,
            expected_id,
        )

    @pytest.mark.asyncio
    async def test_outbox_persist_failure_suppresses_captured_turn_error(self):
        count = 0

        async def _input():
            nonlocal count
            count += 1
            if count == 1:
                return {"content": "turn 1", "id": "accepted-input-a"}
            raise asyncio.CancelledError

        llm = MagicMock()

        async def _timeout(*_args, **_kwargs):
            raise asyncio.TimeoutError
            yield  # pragma: no cover - keeps this an async generator

        llm.astream = _timeout
        llm.reasoning = None
        callbacks = _make_callbacks(
            get_user_input=_input,
            on_turn_complete=AsyncMock(side_effect=RuntimeError("persist failed")),
        )

        with (
            patch("agent.persistent_graph._SESSION_LLM_MAX_ATTEMPTS", 1),
            pytest.raises(RuntimeError, match="persist failed"),
        ):
            await run_persistent_loop(
                llm_with_tools=llm,
                tools=[],
                context_manager=AsyncMock(
                    ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
                ),
                config=_loop_config(observer_interval=1),
                system_prompt="sys",
                callbacks=callbacks,
                messages=[],
                memory_thread_id="thread-a",
                defer_memory_extraction_to_outbox=True,
            )

        callbacks.on_error.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_outbox_replays_turn_error_after_authoritative_persist(self):
        count = 0
        order = []

        async def _input():
            nonlocal count
            count += 1
            if count == 1:
                return {"content": "turn 1", "id": "accepted-input-a"}
            raise asyncio.CancelledError

        llm = MagicMock()

        async def _timeout(*_args, **_kwargs):
            raise asyncio.TimeoutError
            yield  # pragma: no cover - keeps this an async generator

        llm.astream = _timeout
        llm.reasoning = None

        async def _complete(*_args):
            order.append("persist")

        async def _error(*_args, **kwargs):
            order.append(("error", kwargs.get("turn_id")))

        callbacks = _make_callbacks(
            get_user_input=_input,
            on_turn_complete=_complete,
            on_error=_error,
        )
        with patch("agent.persistent_graph._SESSION_LLM_MAX_ATTEMPTS", 1):
            await run_persistent_loop(
                llm_with_tools=llm,
                tools=[],
                context_manager=AsyncMock(
                    ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
                ),
                config=_loop_config(observer_interval=1),
                system_prompt="sys",
                callbacks=callbacks,
                messages=[],
                memory_thread_id="thread-a",
                defer_memory_extraction_to_outbox=True,
            )

        assert order == ["persist", ("error", 1)]

    @pytest.mark.asyncio
    async def test_pinned_turn_error_keeps_historical_pre_complete_order(self):
        count = 0
        order = []

        async def _input():
            nonlocal count
            count += 1
            if count == 1:
                return "turn 1"
            raise asyncio.CancelledError

        llm = MagicMock()

        async def _timeout(*_args, **_kwargs):
            raise asyncio.TimeoutError
            yield  # pragma: no cover - keeps this an async generator

        llm.astream = _timeout
        llm.reasoning = None

        async def _complete(*_args):
            order.append("complete")

        async def _error(*_args, **_kwargs):
            order.append("error")

        callbacks = _make_callbacks(
            get_user_input=_input,
            on_turn_complete=_complete,
            on_error=_error,
        )
        with patch("agent.persistent_graph._SESSION_LLM_MAX_ATTEMPTS", 1):
            await run_persistent_loop(
                llm_with_tools=llm,
                tools=[],
                context_manager=AsyncMock(
                    ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
                ),
                config=_loop_config(observer_interval=0),
                system_prompt="sys",
                callbacks=callbacks,
                messages=[],
            )

        assert order == ["error", "complete"]

    @pytest.mark.asyncio
    async def test_real_extraction_pipeline_receives_threaded_prompt(self):
        """End-to-end in-process: the REAL extract_and_store_memories runs and
        the threaded prompt reaches the aux-LLM chain task; a memory write
        lands in the recall store."""
        from types import SimpleNamespace

        count = 0

        async def _input():
            nonlocal count
            count += 1
            if count <= 1:
                return "turn 1"
            raise asyncio.CancelledError

        memory = SimpleNamespace(
            content="user prefers tabs",
            summary="tabs",
            keywords=["tabs"],
            importance=0.6,
            type="factual",
            retrieval_messages=None,
        )
        aux = MagicMock()
        aux.chain = AsyncMock(return_value=SimpleNamespace(memories=[memory]))
        recall = MagicMock()
        recall.store = AsyncMock(return_value="mem-id-1")

        await run_persistent_loop(
            llm_with_tools=_make_streaming_llm(),
            tools=[],
            context_manager=AsyncMock(
                ensure_within_limits=AsyncMock(side_effect=lambda m, *a, **kw: m)
            ),
            config=_loop_config(observer_interval=1),
            system_prompt="sys",
            callbacks=_make_callbacks(get_user_input=_input),
            messages=[],
            recall_store=recall,
            auxiliary_llm=aux,
            memory_extraction_prompt="REAL PIPELINE PROMPT",
        )
        # Drain the fire-and-forget extraction task
        pending = [
            t
            for t in asyncio.all_tasks()
            if t is not asyncio.current_task() and not t.done()
        ]
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

        aux.chain.assert_awaited_once()
        task = aux.chain.call_args.args[0]
        assert task.system_prompt == "REAL PIPELINE PROMPT"
        recall.store.assert_awaited_once()
        assert recall.store.call_args.kwargs["source"] == "observer"
        assert recall.store.call_args.kwargs["content"] == "user prefers tabs"


class TestTeardownExtraction:
    """B1 (a): session-end and idle-archive extraction actually runs.

    The session mock carries a REAL MemoryConfig — a revert to
    ``_session.config.memory.extraction_prompt`` raises AttributeError,
    gets swallowed by the handler's non-fatal except, and these assertions
    then fail because extraction was never awaited.
    """

    def _make_session(self) -> MagicMock:
        session = MagicMock()
        session.config.memory = MemoryConfig(enabled=True)
        session.memory_service = None  # legacy path (manager flag off)
        session.memory_extraction_prompt = "TEARDOWN PROMPT"
        session.tool_context.recall_store = MagicMock()
        session.auxiliary_llm = MagicMock()
        session.messages = [HumanMessage(content="hi")]
        session.postgres_conn = None
        session.workspace_sync = None
        session.workspace_manager = None
        session.quiesce_subagents = AsyncMock()
        session.resume_subagents = AsyncMock()
        return session

    @pytest.mark.asyncio
    async def test_archive_runs_extraction_with_resolved_prompt(self):
        ws = AsyncMock()
        session = self._make_session()
        extraction = AsyncMock()
        with (
            patch(
                "agent.api.persistent_app._session_termination.begin_retirement",
                AsyncMock(return_value=True),
            ),
            patch(
                "agent.api.persistent_app._session_termination.termination_admission_closed",
                return_value=False,
            ),
            patch("agent.api.persistent_app._stateless_mode", return_value=False),
            patch("agent.api.persistent_app._session", session),
            patch("agent.api.persistent_app._session_identity._thread_id", "tid"),
            patch(
                "agent.api.persistent_app._update_thread_status",
                new=AsyncMock(return_value=True),
            ),
            patch(
                "agent.api.persistent_app._session_termination.terminate", AsyncMock()
            ),
            patch(
                "shared.runtime.services.auxiliary.extract_and_store_memories",
                extraction,
            ),
        ):
            await _handle_archive(ws)

        extraction.assert_awaited_once()
        kwargs = extraction.call_args.kwargs
        assert kwargs["memory_extraction_prompt"] == "TEARDOWN PROMPT"
        assert kwargs["messages"] == session.messages

    @pytest.mark.asyncio
    async def test_idle_archive_runs_extraction_with_resolved_prompt(self):
        session = self._make_session()
        extraction = AsyncMock()
        with (
            patch(
                "agent.api.persistent_app._session_termination.begin_retirement",
                AsyncMock(return_value=True),
            ),
            patch(
                "agent.api.persistent_app._session_termination.termination_admission_closed",
                return_value=False,
            ),
            patch("agent.api.persistent_app._stateless_mode", return_value=False),
            patch("agent.api.persistent_app._session", session),
            patch("agent.api.persistent_app._session_identity._thread_id", "tid"),
            patch("agent.api.persistent_app._broadcast"),
            patch(
                "agent.api.persistent_app._update_thread_status",
                AsyncMock(return_value=True),
            ),
            patch(
                "agent.api.persistent_app._session_termination.terminate", AsyncMock()
            ),
            patch(
                "shared.runtime.services.auxiliary.extract_and_store_memories",
                extraction,
            ),
        ):
            await _handle_idle_archive()

        extraction.assert_awaited_once()
        kwargs = extraction.call_args.kwargs
        assert kwargs["memory_extraction_prompt"] == "TEARDOWN PROMPT"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("handler", ["archive", "idle_archive"])
    async def test_stateless_teardown_skips_manager_and_legacy_extraction(
        self, handler, monkeypatch
    ):
        monkeypatch.setenv("STATELESS_EXECUTOR", "1")
        session = self._make_session()
        session.memory_service = MagicMock()
        session.memory_service.capture = AsyncMock()
        extraction = AsyncMock()
        ws = AsyncMock()
        with (
            patch("agent.api.persistent_app._session", session),
            patch("agent.api.persistent_app._session_identity._thread_id", "tid"),
            patch("agent.api.persistent_app._broadcast"),
            patch(
                "agent.api.persistent_app._session_termination.terminate", AsyncMock()
            ),
            patch(
                "shared.runtime.services.auxiliary.extract_and_store_memories",
                extraction,
            ),
        ):
            if handler == "archive":
                await _handle_archive(ws)
            else:
                await _handle_idle_archive()

        session.memory_service.capture.assert_not_awaited()
        extraction.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_stateless_generic_terminate_skips_manager_extraction(
        self, monkeypatch
    ):
        from agent.api import persistent_app as pa

        monkeypatch.setenv("STATELESS_EXECUTOR", "1")
        session = self._make_session()
        session.memory_service = MagicMock()
        session.memory_service.capture = AsyncMock()
        session.final_memory_extracted = False
        session.shell_owner_token = None
        session.quiesce_background_tasks = AsyncMock()
        session.cleanup = AsyncMock()
        session.retire_shell_owner = MagicMock()
        with (
            patch.object(pa, "_session", session),
            patch.object(pa._session_identity, "_thread_id", "tid"),
            patch.object(pa, "_loop_task", None),
            patch.object(pa, "_event_writer", None),
            patch.object(pa._session_termination, "stop_watchdogs"),
            patch.object(pa, "_stop_thread_interrupt_watcher", AsyncMock()),
            patch.object(pa, "_stop_thread_control_watcher", AsyncMock()),
            patch.object(pa, "_retire_announced_permission_rows", AsyncMock()),
            patch.object(
                pa._session_termination, "quiesce_session_side_tasks", AsyncMock()
            ),
            patch.object(pa._canvas_control, "clear_all"),
            patch.object(pa, "_subscribers", {}),
            patch.object(pa._session_termination, "max_sessions_per_process", 0),
            patch.object(pa._session_termination, "terminating", False),
        ):
            await pa._session_termination.terminate("test", mark_thread=False)

        session.memory_service.capture.assert_not_awaited()
