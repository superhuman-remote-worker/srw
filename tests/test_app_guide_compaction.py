"""The managed product guide remains callable after message compaction."""

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage

from shared.runtime.core.expert_resolution import fence_skills_menu
from shared.runtime.core.skill_resolution import (
    APP_GUIDE_LOADER_TOOL,
    add_persistent_system_skills,
    managed_product_guide_system_floor,
    managed_product_guide_turn_boundary,
)
from agent.core.context_injection import ContextSources, plan_context_entries
from shared.runtime.core.context_entries import (
    FOLDED_KEY,
    entry_body,
    entry_kind,
    fold_context_entries,
)
from shared.runtime.services.recall_store import MemoryRecord
from agent.tools.context import ToolContext
from agent.tools.product_help import create_product_help_tools


def test_app_guide_can_be_loaded_again_after_old_result_is_compacted(monkeypatch):
    monkeypatch.delenv("APP_GUIDE_BREAK_GLASS_DISABLED", raising=False)
    catalog = add_persistent_system_skills({})
    reader = create_product_help_tools(
        ToolContext(config={"_resolved_skills": catalog})
    )[0]
    old_guide_result = reader.invoke({"topic_id": "overview"})

    messages = [
        HumanMessage(content="What is SRW?", id="human-old"),
        AIMessage(
            content="",
            id="assistant-old",
            tool_calls=[
                {
                    "id": "guide-old",
                    "name": APP_GUIDE_LOADER_TOOL,
                    "args": {"topic_id": "overview"},
                }
            ],
        ),
        ToolMessage(
            content=old_guide_result,
            tool_call_id="guide-old",
            name=APP_GUIDE_LOADER_TOOL,
            id="tool-old",
        ),
        HumanMessage(content="What time is it?", id="human-recent"),
        AIMessage(
            content="",
            id="assistant-recent",
            tool_calls=[
                {
                    "id": "time-recent",
                    "name": "get_time",
                    "args": {},
                }
            ],
        ),
        ToolMessage(
            content="12:00 UTC",
            tool_call_id="time-recent",
            name="get_time",
            id="tool-recent",
        ),
    ]
    # Compaction summarises the old turn away and keeps the recent one, so
    # the old guide result is no longer in the conversation.
    compacted = messages[3:]
    assert old_guide_result not in "\n".join(
        str(message.content) for message in compacted
    )

    # Production builds this fenced catalog block afresh for each model call;
    # it is not dependent on retaining an old ToolMessage in conversation.
    current_menu = fence_skills_menu(catalog["menu"])
    assert "- app-guide [load with read_product_guide(topic_id)]" in current_menu
    current_floor = managed_product_guide_system_floor(
        catalog,
        [APP_GUIDE_LOADER_TOOL],
    )
    assert "on every relevant turn" in current_floor
    assert "summaries, memories, prior tool results" in current_floor
    assert "topic from the actual user request" in current_floor
    assert 'topic_id="index"' in current_floor

    # Resume-time memory is appended as a context entry after the user's
    # current question. A turn-boundary entry must follow it with a
    # current-digest freshness boundary so historical workspace/product facts
    # cannot become the most recent instruction.
    current_boundary = managed_product_guide_turn_boundary(
        catalog,
        [APP_GUIDE_LOADER_TOOL],
    )
    prepared = [
        SystemMessage(content=current_floor),
        *compacted,
        HumanMessage(content="What can this SRW session do now?"),
    ]
    planned = plan_context_entries(
        prepared,
        ContextSources(
            memory_records=[MemoryRecord(id=None, content="Old workspace notes")],
            turn_boundary=current_boundary,
            turn=7,
        ),
        model=None,
        max_memories=5,
    )
    assert [entry_kind(entry) for entry in planned.entries] == [
        "memory",
        "turn_boundary",
    ]
    assert "Old workspace notes" in entry_body(planned.entries[0])
    assert entry_body(planned.entries[1]) == current_boundary.strip()
    assert catalog["menu"][0]["bundle_digest"] in current_boundary

    # The request folds both entries into the user's question, the boundary
    # last, so it is the newest text the model reads.
    request = fold_context_entries([*prepared, *planned.entries])
    assert len(request) == len(prepared)
    carrier = request[-1]
    assert isinstance(carrier, HumanMessage)
    assert carrier.additional_kwargs[FOLDED_KEY] == ["memory", "turn_boundary"]
    assert carrier.content.startswith("What can this SRW session do now?")
    assert carrier.content.index("Old workspace notes") < carrier.content.index(
        current_boundary.strip()
    )
    assert carrier.content.endswith(str(planned.entries[1].content))

    current_jobs = reader.invoke({"topic_id": "jobs"})
    assert "[product guide topic: jobs]" in current_jobs
    assert "Some pause reasons are retried or redispatched" in current_jobs
    assert "Folder allowlist" not in current_jobs
    assert "OKF Root Path" not in current_jobs
