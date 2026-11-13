"""The texts a Session turn writes for the tool calls it leaves un-run.

A call that never ran gets a ``not_run`` result row, so the turn leaves no
tool debt. Each text tells the model what happened and nothing more: an
unanswered approval is not a refusal
(knowledge-base/knowledge/issues/stateless_unanswered_approval_stops_chat_executors.md).
"""

from __future__ import annotations

import pytest

from agent import unrun_tool_calls as unrun
from agent.unrun_tool_calls import (
    NOT_RUN_APPROVAL_EXPIRED,
    NOT_RUN_APPROVAL_UNANSWERED,
    NOT_RUN_APPROVAL_UNAVAILABLE,
    NOT_RUN_STOPPED_AT_APPROVAL,
    NOT_RUN_STOPPED_BEFORE_START,
    SKIPPED_TURN_ENDED,
    gated_call_text,
    interrupted_call_text,
)


@pytest.mark.parametrize(
    ("status", "user_stop", "expected"),
    [
        ("interrupted", True, NOT_RUN_STOPPED_AT_APPROVAL),
        ("interrupted", False, NOT_RUN_APPROVAL_UNANSWERED),
        ("expired", False, NOT_RUN_APPROVAL_EXPIRED),
        ("unavailable", False, NOT_RUN_APPROVAL_UNAVAILABLE),
        (None, False, NOT_RUN_APPROVAL_UNANSWERED),
        (None, True, NOT_RUN_APPROVAL_UNANSWERED),
        ("later_status", False, NOT_RUN_APPROVAL_UNANSWERED),
        ("later_status", True, NOT_RUN_APPROVAL_UNANSWERED),
    ],
)
def test_gated_call_text_per_cause(status, user_stop, expected):
    assert gated_call_text(status, user_stop=user_stop) == expected


def test_interrupted_call_text():
    assert interrupted_call_text(user_stop=True) == NOT_RUN_STOPPED_BEFORE_START
    assert interrupted_call_text(user_stop=False) == SKIPPED_TURN_ENDED


def test_no_text_claims_a_refusal():
    texts = [
        value
        for name, value in vars(unrun).items()
        if name.startswith(("NOT_RUN_", "SKIPPED_")) and isinstance(value, str)
    ]
    assert len(texts) == 6
    for text in texts:
        assert "denied" not in text.lower(), text
        assert "declined" not in text.lower(), text
