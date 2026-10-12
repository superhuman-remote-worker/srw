"""The streaming job path must preserve the workspace_unavailable error type.

Regression guard for knowledge-base/knowledge/issues/streaming_strips_workspace_unavailable_type.md:
a WorkspaceUnavailableError raised while the graph streams used to escape
_process_job_streaming (try/finally, no except) into the app layer's generic
``except Exception``, which reported ``{"error": {"message": str(e)}}`` — the
orchestrator's /complete recovery arm routes on ``error.type``, so the job
hard-failed and its VM was torn down instead of pause → reprovision → resume.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

import agent.agent as agent_module
from agent.agent import UniversalAgent
from shared.runtime.core.workspace_backend import (
    WorkspaceAuthenticationError,
    WorkspaceUnavailableError,
    completion_error_payload,
)


class _GraphStub:
    """Bare graph stand-in for the streaming generator's finally block."""

    _srw_memory_service = None


def _bare_agent(job_id: str = "job-under-test") -> UniversalAgent:
    """UniversalAgent with only the attributes _process_job_streaming touches."""
    agent = UniversalAgent.__new__(UniversalAgent)
    agent._graph = _GraphStub()
    agent._current_job_id = job_id
    agent._jobs_processed = 0
    agent._workspace_manager = None
    agent._auxiliary_llm = None
    agent._shell_manager = None
    agent._knowledge_graph = None
    agent._datasource_connections = {}
    agent._datasource_clients = {}
    agent._checkpoint_conn = None
    agent._checkpointer = None
    return agent


def _raising_stream(exc: Exception, states_before: int = 1):
    """run_graph_with_streaming stand-in: yield a few states, then die."""

    async def _stream(graph, graph_input, config):
        for i in range(states_before):
            yield {"iteration": i}
        raise exc

    return _stream


class TestStreamingErrorType:
    @pytest.mark.asyncio
    async def test_workspace_unavailable_yields_typed_error_state(self, monkeypatch):
        exc = WorkspaceUnavailableError("SSH command failed on 10.0.0.1: boom")
        monkeypatch.setattr(
            agent_module, "run_graph_with_streaming", _raising_stream(exc)
        )

        agent = _bare_agent()
        states = [s async for s in agent._process_job_streaming({}, {})]

        final = states[-1]
        assert final["error"]["type"] == "workspace_unavailable"
        assert final["error"]["recoverable"] is True
        assert "SSH command failed" in final["error"]["message"]
        assert final["should_stop"] is True
        assert final["job_id"] == "job-under-test"

    @pytest.mark.asyncio
    async def test_generic_exception_yields_job_error_state(self, monkeypatch):
        exc = RuntimeError("model exploded")
        monkeypatch.setattr(
            agent_module, "run_graph_with_streaming", _raising_stream(exc)
        )

        agent = _bare_agent()
        states = [s async for s in agent._process_job_streaming({}, {})]

        final = states[-1]
        assert final["error"]["type"] == "job_error"
        assert final["error"]["recoverable"] is False
        assert final["should_stop"] is True

    @pytest.mark.asyncio
    async def test_exception_does_not_escape_generator(self, monkeypatch):
        """The app layer's async-for must never see the raw exception — that is
        the exact path that strips the type (dual_app._run_job's generic
        except)."""
        exc = WorkspaceUnavailableError("gone")
        monkeypatch.setattr(
            agent_module, "run_graph_with_streaming", _raising_stream(exc)
        )

        agent = _bare_agent()
        gen = agent._process_job_streaming({}, {})
        collected = []
        # Mirrors dual_app._run_job's consumption loop.
        async for state in gen:
            collected.append(state)
        assert collected  # typed error state delivered, no raise

    @pytest.mark.asyncio
    async def test_states_before_failure_still_stream(self, monkeypatch):
        exc = WorkspaceUnavailableError("gone")
        monkeypatch.setattr(
            agent_module,
            "run_graph_with_streaming",
            _raising_stream(exc, states_before=3),
        )

        agent = _bare_agent()
        states = [s async for s in agent._process_job_streaming({}, {})]
        assert len(states) == 4  # 3 real states + the typed error state


class TestCompletionErrorPayload:
    def test_workspace_authentication_is_typed_and_nonrecoverable(self):
        payload = completion_error_payload(WorkspaceAuthenticationError("bad key"))
        assert payload == {
            "error": {
                "message": "bad key",
                "type": "workspace_authentication",
                "recoverable": False,
            }
        }

    def test_workspace_unavailable_is_typed_and_recoverable(self):
        payload = completion_error_payload(WorkspaceUnavailableError("dead"))
        assert payload == {
            "error": {
                "message": "dead",
                "type": "workspace_unavailable",
                "recoverable": True,
            }
        }

    def test_generic_exception_is_job_error(self):
        payload = completion_error_payload(ValueError("nope"))
        assert payload["error"]["type"] == "job_error"
        assert payload["error"]["recoverable"] is False
        assert payload["error"]["message"] == "nope"

    def test_app_layer_except_sites_use_the_helper(self):
        """Every app-layer except that reports an exception to /complete must
        build its payload via completion_error_payload — an inline
        ``{"error": {"message": str(e)}}`` reintroduces the type strip."""
        import pathlib

        for rel in ("src/agent/api/app.py", "src/agent/api/dual_app.py"):
            text = pathlib.Path(rel).read_text()
            assert '{"error": {"message": str(e)}}' not in text, rel
            assert "completion_error_payload" in text, rel


class TestDualAppExceptPreservesType:
    """Defense-in-depth: even if an exception DOES escape the streaming
    generator (a future regression, or a failure in the app layer itself),
    dual_app's generic except must report it with the classification intact."""

    @pytest.fixture(autouse=True)
    def _restore_dual_app_globals(self, monkeypatch):
        """Snapshot/restore the module globals this test mutates (same pattern
        as tests/test_dual_app_stop_reset.py). AGENT_LOOP=1 keeps the
        completion path from scheduling a process exit."""
        from agent.api import dual_app

        monkeypatch.setenv("AGENT_LOOP", "1")
        names = (
            "_pod_state",
            "_current_job_id",
            "_current_job_task",
            "_stop_reason",
            "_agent",
            "_orchestrator_client",
        )
        saved = {name: getattr(dual_app, name) for name in names}
        stop_req, stop_done = (
            dual_app._stop_requested.is_set(),
            dual_app._stop_completed.is_set(),
        )
        yield
        for name, val in saved.items():
            setattr(dual_app, name, val)
        (dual_app._stop_requested.set if stop_req else dual_app._stop_requested.clear)()
        (
            dual_app._stop_completed.set
            if stop_done
            else dual_app._stop_completed.clear
        )()

    @pytest.mark.asyncio
    async def test_escaping_stream_exception_reports_typed_payload(self):
        from agent.api import dual_app

        client = AsyncMock()
        client.agent_id = "agent-1"
        client.heartbeat = AsyncMock(return_value={})
        client.report_completion = AsyncMock(return_value=True)
        dual_app._orchestrator_client = client
        dual_app._pod_state = dual_app.PodState.WORKING
        dual_app._current_job_id = "job-x"
        dual_app._clear_stop()

        async def _escaping_gen():
            yield {"iteration": 0}
            raise WorkspaceUnavailableError("SSH command failed on vm: dead")

        agent = MagicMock()
        agent.process_job = AsyncMock(return_value=_escaping_gen())
        dual_app._agent = agent

        await dual_app._process_orchestrator_job("job-x", "a job description")

        client.report_completion.assert_awaited()
        reported_job_id, payload = client.report_completion.await_args.args
        assert reported_job_id == "job-x"
        assert payload["error"]["type"] == "workspace_unavailable"
        assert payload["error"]["recoverable"] is True
