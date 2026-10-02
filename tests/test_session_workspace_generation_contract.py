"""Callers must consciously provide the captured life, including legacy None."""

import inspect

from agent.api.session_workspace import poll_workspace_ready


def test_workspace_poll_requires_an_explicit_captured_generation_argument():
    generation = inspect.signature(poll_workspace_ready).parameters[
        "session_runtime_generation"
    ]
    assert generation.kind == inspect.Parameter.KEYWORD_ONLY
    assert generation.default is inspect.Parameter.empty
