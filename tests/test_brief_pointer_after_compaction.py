"""After a compaction a job is sent back to its brief, once.

The brief reaches the model in the kickoff message only. A summary that
replaces that message does not hold the brief word for word, and a job that
goes on from the summary does the rest of its work from a paraphrase (k3d,
2026-11-17: tasks done after a compaction failed their checks, with and
without phases, and no job opened ``task_brief.md`` again).
"""

from __future__ import annotations

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage

from agent.core.brief_pointer import (
    BRIEF_POINTER,
    BRIEF_POINTER_LEAD,
    BRIEF_SAVED_MARK,
    brief_pointer_after_compaction,
)

TOOLS = ["read_file", "write_file", "job_complete"]
KICKOFF = HumanMessage(
    content=(
        "# Task Brief\n\nDo sixteen things.\n\n"
        "Do the task in the brief now. When it is done, call `job_complete`.\n\n"
        "Your task brief is saved to `task_brief.md` in your workspace for reference."
    )
)
SUMMARY = HumanMessage(
    content="Earlier turns of this conversation were compacted to free up context."
)


def test_a_history_without_the_kickoff_gets_the_pointer():
    retained = [SystemMessage(content="system"), SUMMARY, AIMessage(content="ok")]
    assert brief_pointer_after_compaction(retained, TOOLS) == BRIEF_POINTER


def test_the_pointer_names_the_file_and_asks_for_a_read():
    assert BRIEF_POINTER.startswith(BRIEF_POINTER_LEAD)
    assert "`task_brief.md`" in BRIEF_POINTER
    assert "Read it again" in BRIEF_POINTER
    # It must not look like a kickoff message to the next check.
    assert BRIEF_SAVED_MARK not in BRIEF_POINTER


def test_a_history_that_still_holds_the_kickoff_gets_none():
    retained = [SystemMessage(content="system"), KICKOFF, AIMessage(content="ok")]
    assert brief_pointer_after_compaction(retained, TOOLS) is None


def test_the_three_kickoff_wordings_carry_the_mark():
    """The phased, the direct and the live kickoff all end the same way."""
    import inspect

    from agent import graph

    source = inspect.getsource(graph)
    # The live kickoff splits the sentence over two string literals.
    joined = source.replace('"\n                "', "")
    assert joined.count(BRIEF_SAVED_MARK) >= 3


def test_an_earlier_pointer_is_not_repeated():
    retained = [SUMMARY, HumanMessage(content=BRIEF_POINTER), AIMessage(content="ok")]
    assert brief_pointer_after_compaction(retained, TOOLS) is None


def test_a_job_that_cannot_read_a_file_gets_none():
    retained = [SUMMARY]
    assert (
        brief_pointer_after_compaction(retained, ["job_complete", "send_message"])
        is None
    )


def test_an_unknown_tool_list_still_gets_the_pointer():
    assert brief_pointer_after_compaction([SUMMARY], None) == BRIEF_POINTER
    assert brief_pointer_after_compaction([SUMMARY], []) == BRIEF_POINTER


def test_content_given_as_parts_is_read():
    kickoff = HumanMessage(content=[{"type": "text", "text": KICKOFF.content}])
    assert brief_pointer_after_compaction([kickoff], TOOLS) is None
