"""A Session asks its user an MCP server's question (Phase C, Sessions).

knowledge-base/knowledge/features/mcp_client_2026_07_28_elicitation.md, owner
decisions of 2026-11-17 (D3, D6): in ``autonomous`` mode nobody is asked and
the connector's setting holds; in ``read_only`` and ``auto_accept`` the card
asks. The question waits in ``thread_permission_requests`` with
``kind='question'``, on the tool gate's durable wait, so only the user's
"no" is a decline and anything unanswered is a ``cancel``.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock

import pytest

from agent.tools.mcp.manager import ServerQuestion
from tests.test_thread_permissions_phase3 import _install_session, _make_postgres_conn

CONFIRM = {"value": True}


def _question(**overrides) -> ServerQuestion:
    values = dict(
        server="Civitai",
        server_slug="civitai",
        tool="mcp__civitai__generate",
        tool_call_id="call-9",
        mode="form",
        message="Spend 12 Buzz to run imageGen?",
        requested_schema={
            "type": "object",
            "properties": {"value": {"type": "boolean"}},
            "required": ["value"],
        },
        url=None,
        confirmation=CONFIRM,
        setting="decline",
        datasource_id="ds-1",
    )
    values.update(overrides)
    return ServerQuestion(**values)


def _postgres(status: str, response: dict | None = None):
    postgres = _make_postgres_conn(
        insert_returns="row-q", select_status_sequence=[status]
    )
    conn = postgres._fake_conn
    route = conn.fetchval.side_effect

    async def fetchval(sql, *args, **kwargs):
        if "SELECT response" in sql:
            return json.dumps(response) if response is not None else None
        return await route(sql, *args, **kwargs)

    conn.fetchval.side_effect = fetchval
    return postgres


class TestSessionQuestionHandler:
    def setup_method(self):
        import agent.api.persistent_app as mod

        mod._session = None
        mod._session_identity._thread_id = None
        mod._subscribers.clear()
        mod._events_epoch = 0
        mod._next_seq = 0
        mod._session_input._hard_interrupt_event = None
        mod._session_input._interrupt_mode = None
        mod._session_input._interrupt_target_turn_id = None

    def teardown_method(self):
        import agent.api.persistent_app as mod

        mod._session = None
        mod._session_identity._thread_id = None
        mod._subscribers.clear()
        mod._session_input._hard_interrupt_event = None
        mod._session_input._interrupt_mode = None
        mod._session_input._interrupt_target_turn_id = None

    @pytest.mark.asyncio
    async def test_autonomous_asks_nobody(self):
        import agent.api.persistent_app as mod

        postgres = _postgres("approved")
        _install_session(postgres_conn=postgres, permission_mode="autonomous")
        assert await mod._session_question_handler(_question()) is None
        postgres._fake_conn.fetchval.assert_not_called()

    @pytest.mark.asyncio
    async def test_the_card_is_stored_as_a_question_and_broadcast(self):
        import agent.api.persistent_app as mod

        postgres = _postgres("approved", {"content": CONFIRM, "always_accept": True})
        _install_session(postgres_conn=postgres, permission_mode="read_only")
        queue = mod._subscribe("client-Q")

        answer = await mod._session_question_handler(_question())

        assert (answer.action, answer.content, answer.always_accept) == (
            "accept",
            CONFIRM,
            True,
        )
        insert = next(
            call
            for call in postgres._fake_conn.fetchval.call_args_list
            if "INSERT" in call.args[0]
        )
        sql, *params = insert.args
        assert "kind, request" in sql
        assert params[:3] == ["thread-test", "call-9", "mcp__civitai__generate"]
        assert params[-2] == "question"
        assert json.loads(params[-1]) == {
            "server": "Civitai",
            "server_slug": "civitai",
            "datasource_id": "ds-1",
            "tool": "mcp__civitai__generate",
            "mode": "form",
            "message": "Spend 12 Buzz to run imageGen?",
            "schema": _question().requested_schema,
            "url": None,
            "confirmation": CONFIRM,
            "field_order": ["value"],
        }
        request = queue.get_nowait()
        assert request["method"] == "question.request"
        request["params"].pop("_seq", None)
        assert request["params"] == {
            "id": "call-9",
            "approval_id": "row-q",
            "server": "Civitai",
            "server_slug": "civitai",
            "tool": "mcp__civitai__generate",
            "mode": "form",
            "message": "Spend 12 Buzz to run imageGen?",
            "schema": _question().requested_schema,
            "url": None,
            "confirmation": CONFIRM,
            "field_order": ["value"],
        }
        resolved = queue.get_nowait()
        assert resolved["method"] == "question.resolved"
        resolved["params"].pop("_seq", None)
        assert resolved["params"] == {
            "id": "call-9",
            "approval_id": "row-q",
            "server": "Civitai",
            "decision": "approved",
        }

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("status", "action"),
        [("denied", "decline"), ("expired", "cancel")],
    )
    async def test_only_a_no_declines_and_no_answer_cancels(self, status, action):
        import agent.api.persistent_app as mod

        _install_session(postgres_conn=_postgres(status), permission_mode="auto_accept")
        answer = await mod._session_question_handler(_question())
        assert (answer.action, answer.content) == (action, None)

    @staticmethod
    def _retires(postgres) -> list:
        return [
            call
            for call in postgres._fake_conn.execute.call_args_list
            if "SET status = 'expired'" in call.args[0]
        ]

    @pytest.mark.asyncio
    async def test_stop_retires_the_question_and_closes_its_card(self, monkeypatch):
        """Found on k3d 2026-11-17: after Stop the row stayed pending, so the
        card came back on every reload with buttons that reached nobody."""
        import agent.api.persistent_app as mod

        postgres = _postgres("expired")
        _install_session(postgres_conn=postgres, permission_mode="auto_accept")
        monkeypatch.setattr(
            mod,
            "_wait_for_permission_resolution",
            AsyncMock(return_value="interrupted"),
        )
        queue = mod._subscribe("client-S")

        answer = await mod._session_question_handler(_question())

        assert (answer.action, answer.content) == ("cancel", None)
        (retire,) = self._retires(postgres)
        assert retire.args[1] == "row-q"
        assert "status = 'pending' AND kind = 'question'" in retire.args[0]
        assert queue.get_nowait()["method"] == "question.request"
        resolved = queue.get_nowait()
        assert resolved["method"] == "question.resolved"
        assert resolved["params"]["decision"] == "expired"
        assert queue.empty()

    @pytest.mark.asyncio
    async def test_a_graceful_stop_answers_cancel_so_the_tool_can_return(
        self, monkeypatch
    ):
        """A Stop with a tool in flight is graceful: it waits for the tool,
        which waits for the answer. Found on k3d 2026-11-17, where the card
        stayed until the pod went away."""
        import asyncio

        import agent.api.persistent_app as mod

        postgres = _postgres("expired")
        _install_session(postgres_conn=postgres, permission_mode="auto_accept")
        asked = asyncio.Event()
        inner_cancelled = []

        async def wait_forever(request_id):
            asked.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                inner_cancelled.append(request_id)
                raise

        stop = {"cause": None}
        monkeypatch.setattr(mod, "_wait_for_permission_resolution", wait_forever)
        monkeypatch.setattr(mod, "_QUESTION_INTERRUPT_POLL_SECONDS", 0.01)
        monkeypatch.setattr(
            mod._session_input, "peek_interrupt_cause", lambda: stop["cause"]
        )
        queue = mod._subscribe("client-G")

        task = asyncio.create_task(mod._session_question_handler(_question()))
        await asked.wait()
        await asyncio.sleep(0.05)
        assert not task.done()  # no Stop yet: still waiting for the user
        stop["cause"] = "user"
        answer = await asyncio.wait_for(task, timeout=2)

        assert (answer.action, answer.content) == ("cancel", None)
        assert inner_cancelled == ["row-q"]
        (retire,) = self._retires(postgres)
        assert retire.args[1] == "row-q"
        assert queue.get_nowait()["method"] == "question.request"
        resolved = queue.get_nowait()
        assert resolved["params"]["decision"] == "expired"

    @pytest.mark.asyncio
    async def test_a_cancelled_call_retires_its_question_before_unwinding(
        self, monkeypatch
    ):
        """A stateless turn's Stop cancels the running tool, so the wait never
        returns: the card must still close."""
        import asyncio

        import agent.api.persistent_app as mod

        postgres = _postgres("expired")
        _install_session(postgres_conn=postgres, permission_mode="read_only")
        waiting = asyncio.Event()

        async def wait_forever(request_id):
            waiting.set()
            await asyncio.Event().wait()

        monkeypatch.setattr(mod, "_wait_for_permission_resolution", wait_forever)
        queue = mod._subscribe("client-C")

        task = asyncio.create_task(mod._session_question_handler(_question()))
        await waiting.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        (retire,) = self._retires(postgres)
        assert retire.args[1] == "row-q"
        assert queue.get_nowait()["method"] == "question.request"
        resolved = queue.get_nowait()
        assert resolved["method"] == "question.resolved"
        assert resolved["params"]["decision"] == "expired"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", ["approved", "denied", "expired"])
    async def test_an_answered_question_is_left_as_it_is(self, status):
        import agent.api.persistent_app as mod

        postgres = _postgres(status, {"content": CONFIRM})
        _install_session(postgres_conn=postgres, permission_mode="auto_accept")
        await mod._session_question_handler(_question())
        assert self._retires(postgres) == []

    @pytest.mark.asyncio
    async def test_a_question_that_cannot_be_stored_is_declined_with_why(self):
        import agent.api.persistent_app as mod

        _install_session(postgres_conn=None, permission_mode="read_only")
        answer = await mod._session_question_handler(_question())
        assert answer.action == "decline"
        assert "could not show to the user" in answer.note

    @pytest.mark.asyncio
    async def test_a_question_without_its_call_gets_its_own_id(self):
        import agent.api.persistent_app as mod

        postgres = _postgres("denied")
        _install_session(postgres_conn=postgres, permission_mode="read_only")
        await mod._session_question_handler(_question(tool_call_id=None, tool=None))
        insert = next(
            call
            for call in postgres._fake_conn.fetchval.call_args_list
            if "INSERT" in call.args[0]
        )
        assert insert.args[2].startswith("mcp-question-")
        assert insert.args[3] == "mcp__civitai"


class TestWelcomeLists:
    def teardown_method(self):
        import agent.api.persistent_app as mod

        mod._session = None
        mod._session_identity._thread_id = None

    @pytest.mark.asyncio
    async def test_questions_ride_the_welcome_frame_and_never_as_approvals(self):
        import agent.api.persistent_app as mod

        postgres = _make_postgres_conn()
        card = {"server": "Civitai", "mode": "url", "message": "Sign in", "url": "u"}
        postgres._fake_conn.fetch = AsyncMock(
            return_value=[
                {"id": "row-1", "tool_call_id": "call-1", "request": json.dumps(card)}
            ]
        )
        _install_session(postgres_conn=postgres)

        questions = await mod._pending_question_requests()
        assert questions == [
            {
                "id": "call-1",
                "approval_id": "row-1",
                "server": "Civitai",
                "server_slug": None,
                "tool": None,
                "mode": "url",
                "message": "Sign in",
                "schema": None,
                "url": "u",
                "confirmation": None,
                "field_order": None,
            }
        ]
        assert "kind = 'question'" in postgres._fake_conn.fetch.call_args.args[0]

        postgres._fake_conn.fetch.return_value = []
        assert await mod._pending_permission_requests() == []
        assert "kind = 'tool'" in postgres._fake_conn.fetch.call_args.args[0]
