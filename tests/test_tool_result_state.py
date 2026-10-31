"""The result-state marker on a tool result row (shared/tool_result_state.py).

A row that does not report what its call did carries the marker in
``thread_messages.metrics`` so the cockpit labels the call "result unknown"
or "not run" rather than "completed"
(session_unknown_tool_result_needs_admin.md).
"""

import pytest
from langchain_core.messages import ToolMessage

from agent.core.thread_messages import _serialize_message_row
from shared.tool_result_state import (
    RESULT_NOT_RUN,
    RESULT_UNKNOWN,
    TOOL_RESULT_STATE_KEY,
    tool_result_state_metrics,
)


@pytest.mark.parametrize("state", [RESULT_UNKNOWN, RESULT_NOT_RUN])
def test_a_marked_result_row_stores_its_state(state):
    msg = ToolMessage(
        content="Result unknown: lost",
        tool_call_id="c1",
        id="t1",
        additional_kwargs={TOOL_RESULT_STATE_KEY: state},
    )

    row = _serialize_message_row(msg, 2, metrics={"input_tokens": 9})

    assert row["metrics"] == {
        TOOL_RESULT_STATE_KEY: {"state": state, "tool_call_id": "c1"}
    }


def test_an_ordinary_result_row_has_no_metrics():
    msg = ToolMessage(content="ok", tool_call_id="c1", id="t1")

    assert (
        _serialize_message_row(msg, 2, metrics={"input_tokens": 9})["metrics"] is None
    )


def test_a_state_this_build_does_not_know_is_not_stored():
    msg = ToolMessage(
        content="?",
        tool_call_id="c1",
        id="t1",
        additional_kwargs={TOOL_RESULT_STATE_KEY: "finished"},
    )

    assert _serialize_message_row(msg, 2)["metrics"] is None


def test_the_marker_names_only_known_states():
    with pytest.raises(ValueError):
        tool_result_state_metrics("finished", "c1")
