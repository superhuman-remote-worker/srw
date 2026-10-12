"""The batch settle's wire contract: request bounds, route and shared texts.

Design: knowledge-base/knowledge/features/parallel_subagents.md §5.3–§5.6 and
§6.1. The transaction itself is proven on a real Postgres in
``tests/test_session_subagent_batch_settle_pg.py``.
"""

from __future__ import annotations

from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from orchestrator.schemas.agent_child_threads import (
    AgentSessionSubagentBatchSettleRequest,
)
from orchestrator.security.access import require_internal as real_require_internal
from shared.persistent_input_delivery import InputDeliveryConflict
from shared.session_subagent_authority import (
    SessionParentAuthorityRefused,
    session_subagent_delivery_id,
)
from shared.session_subagent_batch import (
    BATCH_MAX_MEMBERS,
    BATCH_MESSAGE_MAX_CHARS,
    DECLINED_RESULT_TEXT,
    MEMBER_MESSAGE_MAX_CHARS,
    batch_continuation_text,
    not_started_result_text,
    result_metrics,
    retired_result_text,
    session_subagent_batch_delivery_id,
    session_subagent_batch_result_id,
)
from tests.test_b06_lane_c_agent_child_threads import (
    CHILD_ID,
    GENERATION,
    THREAD_ID,
    _client,
    _deps,
    _session_authority,
    _store,
)

INPUT_ID = "88888888-8888-4888-8888-888888888888"
SETTLE = f"/api/agents/threads/{THREAD_ID}/subagents/settle-batch"


def _member(**over) -> dict:
    member = {
        "thread_id": str(uuid4()),
        "runtime_generation": str(uuid4()),
        "subagent_status": "completed",
        "outcome": "completed",
        "message": "the report",
    }
    member.update(over)
    return member


def _body(members=None, **over) -> dict:
    body = {
        "parent_authority": _session_authority(),
        "parent_input_message_id": INPUT_ID,
        "parent_iteration": 3,
        "members": [_member()] if members is None else members,
    }
    body.update(over)
    return body


# --- request bounds ------------------------------------------------------------


class TestRequestBounds:
    def test_a_well_formed_request_parses(self):
        request = AgentSessionSubagentBatchSettleRequest.model_validate(_body())
        assert request.parent_iteration == 3
        assert request.parent_input_message_id == UUID(INPUT_ID)
        assert len(request.members) == 1

    def test_no_members_is_a_valid_request(self):
        request = AgentSessionSubagentBatchSettleRequest.model_validate(
            _body(members=[])
        )
        assert request.members == []

    def test_one_member_text_is_capped(self):
        AgentSessionSubagentBatchSettleRequest.model_validate(
            _body(members=[_member(message="x" * MEMBER_MESSAGE_MAX_CHARS)])
        )
        with pytest.raises(ValidationError):
            AgentSessionSubagentBatchSettleRequest.model_validate(
                _body(members=[_member(message="x" * (MEMBER_MESSAGE_MAX_CHARS + 1))])
            )

    def test_the_batch_text_is_capped_in_total(self):
        per_member = MEMBER_MESSAGE_MAX_CHARS
        fitting = BATCH_MESSAGE_MAX_CHARS // per_member
        AgentSessionSubagentBatchSettleRequest.model_validate(
            _body(members=[_member(message="x" * per_member) for _ in range(fitting)])
        )
        with pytest.raises(ValidationError, match="exceed"):
            AgentSessionSubagentBatchSettleRequest.model_validate(
                _body(
                    members=[_member(message="x" * per_member) for _ in range(fitting)]
                    + [_member(message="x")]
                )
            )

    def test_the_member_count_is_capped(self):
        with pytest.raises(ValidationError):
            AgentSessionSubagentBatchSettleRequest.model_validate(
                _body(
                    members=[
                        _member(message=None) for _ in range(BATCH_MAX_MEMBERS + 1)
                    ]
                )
            )

    def test_one_child_named_twice_is_refused(self):
        member = _member()
        with pytest.raises(ValidationError, match="twice"):
            AgentSessionSubagentBatchSettleRequest.model_validate(
                _body(members=[member, dict(member)])
            )

    @pytest.mark.parametrize("iteration", [0, -1, True, "3", 2.0])
    def test_the_parent_turn_is_an_exact_positive_integer(self, iteration):
        with pytest.raises(ValidationError):
            AgentSessionSubagentBatchSettleRequest.model_validate(
                _body(parent_iteration=iteration)
            )

    @pytest.mark.parametrize(
        "body",
        [
            _body(delivery_id=str(uuid4())),
            _body(members=[_member(handle="reader-1")]),
            _body(members=[_member(subagent_status="")]),
            _body(members=[_member(turns=-1)]),
            _body(members=[_member(thread_id="not-a-uuid")]),
        ],
    )
    def test_invented_or_malformed_fields_fail_loudly(self, body):
        with pytest.raises(ValidationError):
            AgentSessionSubagentBatchSettleRequest.model_validate(body)


# --- route -------------------------------------------------------------------


def _settle_store(result=None, **over):
    return _store(
        settle_session_subagent_batch=AsyncMock(
            return_value=result
            if result is not None
            else {"result": "applied", "delivery_id": str(uuid4()), "calls": []}
        ),
        **over,
    )


class TestRoute:
    def test_settle_batch_is_declared_before_the_child_id_route(self):
        """``settle-batch`` must not be parsed as a child thread id."""
        store = _settle_store()
        resp = _client(_deps(store=store)).post(SETTLE, json=_body())
        assert resp.status_code == 200
        store.settle_session_subagent_batch.assert_awaited_once()
        store.get_session_subagent_thread.assert_not_awaited()

    def test_it_forwards_the_turn_and_json_members(self):
        store = _settle_store()
        member = _member(turns=4, tokens=1000, report_path=".subagents/r/report.md")
        resp = _client(_deps(store=store)).post(SETTLE, json=_body(members=[member]))
        assert resp.status_code == 200
        kwargs = store.settle_session_subagent_batch.await_args.kwargs
        assert kwargs["parent_thread_id"] == THREAD_ID
        assert kwargs["parent_input_message_id"] == INPUT_ID
        assert kwargs["parent_iteration"] == 3
        assert kwargs["parent_authority"]["parent_thread_id"] == THREAD_ID
        assert kwargs["members"] == [
            {
                "thread_id": member["thread_id"],
                "runtime_generation": member["runtime_generation"],
                "subagent_status": "completed",
                "outcome": "completed",
                "turns": 4,
                "tokens": 1000,
                "report_path": ".subagents/r/report.md",
                "error": None,
                "message": "the report",
            }
        ]

    def test_it_fails_closed_without_an_internal_key(self):
        store = _settle_store()
        deps = _deps(store=store, require_internal=real_require_internal)
        resp = _client(deps).post(SETTLE, json=_body())
        assert resp.status_code == 401
        store.settle_session_subagent_batch.assert_not_awaited()

    def test_a_malformed_body_is_refused_before_the_store(self):
        store = _settle_store()
        resp = _client(_deps(store=store)).post(
            SETTLE,
            json=_body(members=[_member(message="x" * (MEMBER_MESSAGE_MAX_CHARS + 1))]),
        )
        assert resp.status_code == 422
        store.settle_session_subagent_batch.assert_not_awaited()

    @pytest.mark.parametrize(
        "result",
        ["applied", "idempotent", "already_delivered", "nothing_to_recover"],
    )
    def test_successes_return_the_result(self, result):
        body = {"result": result, "delivery_id": None, "calls": []}
        store = _settle_store(result=body)
        resp = _client(_deps(store=store)).post(SETTLE, json=_body())
        assert resp.status_code == 200
        assert resp.json() == body

    def test_a_stale_request_is_a_409_carrying_the_server_view(self):
        stale = {
            "result": "stale",
            "reason": "members_differ",
            "calls": [{"tool_call_id": "call-1", "class": "ended"}],
        }
        store = _settle_store(result=stale)
        resp = _client(_deps(store=store)).post(SETTLE, json=_body())
        assert resp.status_code == 409
        assert resp.json() == {"detail": stale}

    @pytest.mark.parametrize(
        "raised,status,detail",
        [
            (
                SessionParentAuthorityRefused("stateless_parent_not_current"),
                409,
                {
                    "code": "session_parent_authority_refused",
                    "reason": "stateless_parent_not_current",
                },
            ),
            (
                InputDeliveryConflict("stable input identity conflicts"),
                409,
                {
                    "code": "subagent_delivery_conflict",
                    "message": "stable input identity conflicts",
                },
            ),
            (
                ValueError("terminal session child retry changed its turns"),
                400,
                "terminal session child retry changed its turns",
            ),
            (RuntimeError("boom"), 500, "boom"),
        ],
    )
    def test_refusals_map_like_the_single_child_terminal(self, raised, status, detail):
        store = _store(settle_session_subagent_batch=AsyncMock(side_effect=raised))
        resp = _client(_deps(store=store)).post(SETTLE, json=_body())
        assert resp.status_code == status
        assert resp.json() == {"detail": detail}

    def test_an_unknown_session_or_input_is_a_404(self):
        store = _store(settle_session_subagent_batch=AsyncMock(return_value=None))
        resp = _client(_deps(store=store)).post(SETTLE, json=_body())
        assert resp.status_code == 404

    def test_the_live_list_adds_the_recovery_plans(self):
        plan = {
            "parent_input_message_id": INPUT_ID,
            "parent_iteration": 3,
            "supersedes_input_seq": 42,
            "delivery_id": str(uuid4()),
            "calls": [],
        }
        store = _store(
            list_live_session_subagent_recovery=AsyncMock(
                return_value={"subagents": [], "recovery_turns": [plan]}
            )
        )
        resp = _client(_deps(store=store)).post(
            f"/api/agents/threads/{THREAD_ID}/subagents/live",
            json={"parent_authority": _session_authority()},
        )
        assert resp.status_code == 200
        assert resp.json() == {
            "parent_thread_id": THREAD_ID,
            "count": 0,
            "subagents": [],
            "recovery_turns": [plan],
        }


# --- the shared contract ---------------------------------------------------------


class TestContract:
    def test_the_continuation_is_keyed_on_the_superseded_input(self):
        parent, source = uuid4(), uuid4()
        assert session_subagent_batch_delivery_id(
            parent, source
        ) == session_subagent_batch_delivery_id(str(parent), str(source).upper())
        assert session_subagent_batch_delivery_id(
            parent, source
        ) != session_subagent_batch_delivery_id(parent, uuid4())
        # Never the single-member recovery's per-child identity.
        assert session_subagent_batch_delivery_id(
            THREAD_ID, INPUT_ID
        ) != session_subagent_delivery_id(CHILD_ID, GENERATION)

    def test_each_written_result_has_its_own_stable_row_id(self):
        first = session_subagent_batch_result_id(THREAD_ID, INPUT_ID, "call-1")
        assert first == session_subagent_batch_result_id(THREAD_ID, INPUT_ID, "call-1")
        assert first != session_subagent_batch_result_id(THREAD_ID, INPUT_ID, "call-2")
        assert first != session_subagent_batch_delivery_id(THREAD_ID, INPUT_ID)

    def test_server_texts_are_pure_functions_of_durable_facts(self):
        assert not_started_result_text() == not_started_result_text()
        assert not_started_result_text().startswith("[delegate_agent: NOT STARTED]\n")
        assert DECLINED_RESULT_TEXT == "User declined this tool call."
        retired = retired_result_text(
            handle="reader-0001", subagent_type="reader", turns=3, tokens=1200
        )
        assert retired == retired_result_text(
            handle="reader-0001", subagent_type="reader", turns=3, tokens=1200
        )
        assert "Progress before it was cancelled: 3 turns, 1200 tokens." in retired

    @pytest.mark.parametrize(
        "counts,expected",
        [
            (
                dict(calls=4, interrupted=1, not_started=2, declined=0, retired=0),
                "1 of 4 delegated tasks finished and its result is above. "
                "1 was interrupted and 2 never started; each is marked. "
                "The finished result is complete and does not need to be "
                "repeated.",
            ),
            (
                dict(calls=4, interrupted=0, not_started=0, declined=0, retired=0),
                "All 4 delegated tasks finished and their results are above. "
                "The finished results are complete and do not need to be "
                "repeated.",
            ),
            (
                dict(calls=3, interrupted=1, not_started=1, declined=1, retired=0),
                "No delegated task of this turn finished. 1 was interrupted, 1 "
                "never started and 1 was declined by the user; each is marked.",
            ),
            (
                dict(calls=5, interrupted=2, not_started=0, declined=0, retired=1),
                "2 of 5 delegated tasks finished and their results are above. 2 "
                "were interrupted and 1 was cancelled when the session was "
                "stopped; each is marked.",
            ),
        ],
    )
    def test_the_continuation_counts_what_happened(self, counts, expected):
        text = batch_continuation_text(**counts)
        assert text.startswith(
            "[subagent recovery] This turn was resumed after the process running "
            "it was replaced. "
        )
        assert expected in text
        assert text.endswith("Continue with the original request.")

    def test_a_continuation_cannot_count_more_outcomes_than_calls(self):
        with pytest.raises(ValueError):
            batch_continuation_text(
                calls=1, interrupted=1, not_started=1, declined=0, retired=0
            )

    def test_result_metrics_name_a_known_class(self):
        with pytest.raises(ValueError):
            result_metrics(
                result_class="delivered", tool_call_id="call-1", delivery_id=uuid4()
            )
