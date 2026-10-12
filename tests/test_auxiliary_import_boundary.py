"""Common support operations must not load agent lifecycle or tool factories."""

from pathlib import Path
import subprocess
import sys
import textwrap


def test_memory_and_remote_support_work_without_agent_runtime_imports():
    # Run outside pytest's already-imported graph/tool modules so package
    # initialization and deferred message/schema helpers exercise a cold import.
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            textwrap.dedent(
                """
                import importlib.abc
                import sys

                forbidden = (
                    "agent.tools",
                    "agent.graph",
                    "agent.persistent_graph",
                    "agent.core.context",
                    "agent.core.archiver",
                    "langgraph",
                )

                class AgentRuntimeBlocker(importlib.abc.MetaPathFinder):
                    def find_spec(self, fullname, path=None, target=None):
                        if any(
                            fullname == name or fullname.startswith(name + ".")
                            for name in forbidden
                        ):
                            raise AssertionError("agent runtime import: " + fullname)
                        return None

                sys.meta_path.insert(0, AgentRuntimeBlocker())

                from langchain_core.messages import HumanMessage, ToolMessage
                from shared.runtime.core.backends.remote import RemoteBackend
                from shared.runtime.services.auxiliary import (
                    SummarizeTask,
                    _format_messages_for_extraction,
                )
                from agent.services.memory.extraction_engine import MemoryExtractionEngine

                task = SummarizeTask(
                    "conversation", "## Objective", prior_summary="retained"
                )
                assert task.asks_for_sections
                context = task.build_context()
                assert "<prior-summary>" in context and "retained" in context
                assert RemoteBackend is not None
                assert MemoryExtractionEngine is not None

                prefixes = (
                    "instruction_inject_",
                    "memory_inject_",
                    "knowledge_inject_",
                    "charter_inject_",
                    "citation_feedback_inject_",
                )
                messages = [
                    ToolMessage(content="transient", tool_call_id=prefix + "123")
                    for prefix in prefixes
                ]
                messages.extend([
                    HumanMessage(content="<active_tasks>\\ntransient</active_tasks>"),
                    HumanMessage(content="durable conversation"),
                ])
                assert _format_messages_for_extraction(messages) == (
                    "[User] durable conversation"
                )
                """
            ),
        ],
        cwd=Path(__file__).resolve().parents[1],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
