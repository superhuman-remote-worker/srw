"""Unit tests for durable stateless-worker steering identities and acks."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from shared.job_steering import (
    CheckpointSteeringAcker,
    context_delivery_key,
    queued_reply_key,
)


def test_explicit_reply_id_is_authoritative() -> None:
    first = {
        "id": "2f1b1e5b-bd38-46cf-858c-9cdf9c349600",
        "thread_id": "officer",
        "timestamp": "2026-08-10T01:02:03+00:00",
        "message": "first body",
    }
    edited_copy = {**first, "message": "different body"}

    assert queued_reply_key(first) == ("id:2f1b1e5b-bd38-46cf-858c-9cdf9c349600")
    assert queued_reply_key(edited_copy) == queued_reply_key(first)


def test_legacy_reply_key_is_deterministic_and_content_sensitive() -> None:
    first = {
        "thread_id": "officer",
        "timestamp": "2026-08-10T01:02:03+00:00",
        "message": "first body",
    }
    same = dict(reversed(list(first.items())))
    newer_same_thread = {
        **first,
        "timestamp": "2026-08-10T01:03:03+00:00",
        "message": "newer body",
    }

    assert queued_reply_key(first).startswith("legacy:")
    assert queued_reply_key(same) == queued_reply_key(first)
    assert queued_reply_key(newer_same_thread) != queued_reply_key(first)


def test_consumption_annotations_do_not_change_legacy_identity() -> None:
    reply = {
        "thread_id": "officer",
        "timestamp": "2026-08-10T01:02:03+00:00",
        "message": "body",
    }
    consumed = {
        **reply,
        "consumed_at": "2026-08-10T01:04:03+00:00",
        "consumed_checkpoint_id": "1f0d",
        "consumed_checkpoint_step": 7,
    }

    assert queued_reply_key(consumed) == queued_reply_key(reply)


def test_context_delivery_id_distinguishes_identical_repeat() -> None:
    first = context_delivery_key(
        "feedback", "try again", delivery_id="delivery-1", companion="review"
    )
    repeated = context_delivery_key(
        "feedback", "try again", delivery_id="delivery-2", companion="review"
    )

    assert first != repeated
    assert context_delivery_key("feedback", "try again", companion="review") == (
        context_delivery_key("feedback", "try again", companion="review")
    )


def _checkpoint(checkpoint_id: str = "cp-1") -> dict:
    return {
        "id": checkpoint_id,
        "channel_values": {
            "delivered_guidance_ids": ["g-1"],
            "delivered_reply_keys": ["id:r-1"],
            "delivered_feedback_keys": ["feedback:id:f-1"],
            "delivered_delegation_keys": ["delegation:id:d-1"],
        },
    }


@pytest.mark.asyncio
async def test_post_commit_acker_sends_checkpoint_proof_and_suppresses_success() -> (
    None
):
    client = AsyncMock()
    client.ack_job_guidance.return_value = True
    acker = CheckpointSteeringAcker("job-1", client)

    await acker(
        {},
        _checkpoint(),
        {"step": 4},
        {"configurable": {"checkpoint_id": "cp-1"}},
    )
    await acker(
        {},
        _checkpoint("cp-2"),
        {"step": 5},
        {"configurable": {"checkpoint_id": "cp-2"}},
    )

    client.ack_job_guidance.assert_awaited_once_with(
        "job-1",
        guidance_ids=["g-1"],
        reply_keys=["id:r-1"],
        feedback_keys=["feedback:id:f-1"],
        delegation_keys=["delegation:id:d-1"],
        checkpoint_id="cp-1",
    )


@pytest.mark.asyncio
async def test_failed_post_commit_ack_retries_on_successor_checkpoint() -> None:
    client = AsyncMock()
    client.ack_job_guidance.side_effect = [False, True]
    acker = CheckpointSteeringAcker("job-1", client)

    await acker(
        {},
        _checkpoint("cp-1"),
        {"step": 4},
        {"configurable": {"checkpoint_id": "cp-1"}},
    )
    await acker(
        {},
        _checkpoint("cp-2"),
        {"step": 5},
        {"configurable": {"checkpoint_id": "cp-2"}},
    )

    assert client.ack_job_guidance.await_count == 2
    assert client.ack_job_guidance.await_args_list[1].kwargs["checkpoint_id"] == "cp-2"


@pytest.mark.asyncio
async def test_end_reclaim_reconciles_failed_last_ack_without_another_checkpoint() -> (
    None
):
    client = AsyncMock()
    client.ack_job_guidance.side_effect = [False, True]
    first = CheckpointSteeringAcker("job-1", client)

    assert not await first.reconcile_values(
        _checkpoint("cp-end")["channel_values"],
        checkpoint_id="cp-end",
    )

    successor = CheckpointSteeringAcker("job-1", client)
    assert await successor.reconcile_values(
        _checkpoint("cp-end")["channel_values"],
        checkpoint_id="cp-end",
    )
    assert client.ack_job_guidance.await_count == 2
    assert client.ack_job_guidance.await_args_list[1].kwargs["checkpoint_id"] == (
        "cp-end"
    )


@pytest.mark.asyncio
async def test_no_absorbed_entries_means_no_ack() -> None:
    client = AsyncMock()
    acker = CheckpointSteeringAcker("job-1", client)

    await acker(
        {},
        {"id": "cp-1", "channel_values": {}},
        {"step": 1},
        {"configurable": {"checkpoint_id": "cp-1"}},
    )

    client.ack_job_guidance.assert_not_awaited()


def _append_only_execute(tmp_path, *, stateless: bool):
    """The worker execute node in append_only mode with a scripted LLM."""
    from langchain_core.messages import AIMessage

    from agent.core.workspace import WorkspaceManager
    from agent.graph import create_execute_node
    from agent.managers import TodoManager
    from agent.tools.context import ToolContext
    from tests._fs_backend import FilesystemTestBackend

    workspace = WorkspaceManager(
        job_id="job-1", base_path=tmp_path, backend=FilesystemTestBackend(tmp_path)
    )
    workspace.initialize()
    context = ToolContext(workspace_manager=workspace)
    context._stateless_worker = stateless
    requests = []

    async def ainvoke(prepared, **kwargs):
        requests.append(list(prepared))
        return AIMessage(
            content="", tool_calls=[{"name": "read_file", "args": {}, "id": "c1"}]
        )

    llm = MagicMock()
    llm.ainvoke = AsyncMock(side_effect=ainvoke)
    config = MagicMock()
    config.agent_id = "worker"
    config.extra = {}
    config.llm.model = "test-model"
    config.llm.timeout = 10.0
    config.llm.model_max_context_tokens = 100000
    config.limits.model_max_context_tokens = 100000
    config.limits.response_validation.enabled = False
    config.context_management.injection_mode = "append_only"
    config.memory.max_memories_per_entry = 5
    context_mgr = MagicMock()
    context_mgr.get_token_count.return_value = 50
    context_mgr.config.compaction_threshold_tokens = 100000
    context_mgr.config.summarization_threshold_tokens = 100000
    context_mgr.config.keep_recent_messages = 10
    context_mgr.should_summarize.return_value = False
    context_mgr.ensure_within_limits = AsyncMock(side_effect=lambda m, *a, **k: m)
    execute = create_execute_node(
        llm_with_tools=llm,
        todo_manager=TodoManager(workspace),
        memory_manager=MagicMock(),
        workspace_manager=workspace,
        config=config,
        context_mgr=context_mgr,
        retry_manager=MagicMock(),
        auxiliary_llm=None,
        summarization_prompt="",
        tool_context=context,
        tool_names=["read_file"],
    )
    return execute, requests


@pytest.mark.asyncio
@pytest.mark.parametrize("stateless", [False, True], ids=["pinned", "stateless"])
async def test_append_only_records_delivered_guidance_on_both_lanes(
    tmp_path, stateless
) -> None:
    """WP2 spec §C / B7: in append_only mode the execute node writes
    ``delivered_guidance_ids`` on both lanes and the next request filters
    them, so the entry is appended once and stays in the history. The
    stateless lane's ack is the fenced saver's: the checkpoint carrying the
    set is what it acks. The pinned lane acks itself after each answered
    request while the inbox still lists the entry."""
    from langchain_core.messages import HumanMessage

    execute, requests = _append_only_execute(tmp_path, stateless=stateless)
    pending = [{"id": "g-1", "text": "use staging", "source": "officer"}]
    ack = MagicMock()
    state = {
        "job_id": "job-1",
        "iteration": 1,
        "messages": [HumanMessage(content="hello")],
        "is_strategic_phase": False,
        "phase_number": 2,
        "metadata": {},
    }

    with (
        patch("agent.graph.get_phase_system_prompt", return_value="SYS"),
        patch("agent.graph.get_archiver", return_value=None),
        patch("agent.graph._get_pending_supervisor_guidance", return_value=pending),
        patch("agent.graph._ack_supervisor_guidance", ack),
    ):
        first = await execute(state)
        state["messages"] = state["messages"] + first["messages"]
        state["delivered_guidance_ids"] = first["delivered_guidance_ids"]
        second = await execute(state)

    needle = '<srw_context kind="guidance">'
    assert sum(str(m.content).count(needle) for m in requests[0]) == 1
    # Still once in the second request: history now, not appended again.
    assert sum(str(m.content).count(needle) for m in requests[1]) == 1
    assert first["delivered_guidance_ids"] == ["g-1"]
    assert second["delivered_guidance_ids"] == ["g-1"]
    if stateless:
        ack.assert_not_called()
    else:
        assert [c.kwargs for c in ack.call_args_list] == [{"guidance_ids": ["g-1"]}] * 2

    client = AsyncMock()
    client.ack_job_guidance.return_value = True
    acker = CheckpointSteeringAcker("job-1", client)
    assert await acker.reconcile_values(
        {"delivered_guidance_ids": first["delivered_guidance_ids"]},
        checkpoint_id="cp-1",
    )
    client.ack_job_guidance.assert_awaited_once_with(
        "job-1",
        guidance_ids=["g-1"],
        reply_keys=[],
        feedback_keys=[],
        delegation_keys=[],
        checkpoint_id="cp-1",
    )
