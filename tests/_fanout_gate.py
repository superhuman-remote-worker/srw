"""Pin the session fan-out decision in a test (parallel_subagents §6.4, §12).

``agent.tools.delegation.fanout.session_fanout_allowed`` is the one gate. Its
inputs are the orchestrator's and change with it, so tests never set them:
they pin the decision at its use sites instead. The ``delegate_agent`` tool
binds the name at import (its description and its two refusals); the live
loop (``agent.persistent_graph``) and everything else read it through the
module at call time. The gate stays false for every parent that is not a
session, as in production.
"""

from __future__ import annotations

from typing import Any


def set_session_fanout(monkeypatch: Any, allowed: bool = True) -> None:
    from agent.tools.delegation import delegate_agent, fanout

    is_session_parent = fanout.is_session_parent

    def gate(context: Any) -> bool:
        return bool(allowed) and is_session_parent(context)

    monkeypatch.setattr(fanout, "session_fanout_allowed", gate)
    monkeypatch.setattr(delegate_agent, "session_fanout_allowed", gate)
