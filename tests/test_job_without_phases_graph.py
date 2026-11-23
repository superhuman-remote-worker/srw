"""A job without phases on the unchanged worker graph.

The resolver freezes a job without phases with one predefined to-do and a
tool list without the phase tools; the worker reads nothing else. These tests
run the compiled graph (``tests/_job_without_phases_harness.py``: real nodes,
real ``job_complete`` and ``write_file``, a scripted model, an in-memory
checkpoint) and pin what such a job does:

* it ends out of strategic phase 0 on one accepted ``job_complete``, for
  ``autonomy`` ``full`` and ``review``, with the completion report envelope
  minted;
* a model call costs exactly one tool step: ``write_file`` then
  ``job_complete`` is two calls;
* no request text it composes names ``todo_complete``, ``next_phase_todos``
  or "strategic mode" (the system prompt is another layer's);
* replies without a tool call stop at ``limits.max_plain_replies_in_a_row``,
  with the limit off they get the reminder as before, and the identical-text
  breaker still ends a job that repeats itself;
* a resume with feedback ends on one ``job_complete`` and starts a fresh
  model-call budget and plain-reply streak.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from agent.tools.core.job import _final_phase_data, _missing_deliverable_refused
from shared.runtime.services.guardrails import format_nudge
from tests._job_without_phases_harness import (
    PHASE_SKILL,
    PHASE_WORDS,
    RESUME_TODO_TEXT,
    TODO_TEXT,
    PHASED_TOOLS,
    build_job,
    job_complete_call,
    plain_reply,
    request_text,
    write_file_call,
)


@pytest.fixture(autouse=True)
def _clean_process_caches():
    _final_phase_data.clear()
    _missing_deliverable_refused.clear()
    yield
    _final_phase_data.clear()
    _missing_deliverable_refused.clear()


def _assert_ended_out_of_phase_zero(out) -> None:
    assert out["phase_number"] == 0
    assert out["is_strategic_phase"] is True
    assert out["should_stop"] is True
    assert out.get("error") is None
    # The completion report envelope was minted at END.
    assert out["client_report_id"]
    assert out["completion_report_payload"]["should_stop"] is True


def _assert_no_phase_words(requests) -> None:
    for number, request in enumerate(requests, start=1):
        text = request_text(request)
        for word in PHASE_WORDS:
            assert word not in text, f"request {number} names {word!r}"


class TestEndsOnOneJobComplete:
    @pytest.mark.asyncio
    async def test_full_autonomy_completes_in_one_model_call(self, tmp_path):
        job = build_job(tmp_path, [job_complete_call()], autonomy="full")

        out = await job.start()

        assert len(job.requests) == 1
        _assert_ended_out_of_phase_zero(out)
        assert out["goal_achieved"] is True
        assert out["todos"][0]["status"] == "completed"
        assert job.workspace.exists("output/job_completion.json")

    @pytest.mark.asyncio
    async def test_review_autonomy_freezes_for_review_in_one_model_call(self, tmp_path):
        job = build_job(tmp_path, [job_complete_call()], autonomy="review")

        out = await job.start()

        assert len(job.requests) == 1
        _assert_ended_out_of_phase_zero(out)
        assert out["goal_achieved"] is False
        assert out["freeze_data"]["freeze_type"] == "job_complete"
        assert out["completion_report_payload"]["freeze_data"]["freeze_type"] == (
            "job_complete"
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("autonomy", ["full", "review"])
    async def test_one_tool_step_then_job_complete_is_two_model_calls(
        self, tmp_path, autonomy
    ):
        job = build_job(
            tmp_path,
            [
                write_file_call("call_write"),
                job_complete_call(deliverables=["hello.txt"]),
            ],
            autonomy=autonomy,
        )

        out = await job.start()

        assert len(job.requests) == 2
        _assert_ended_out_of_phase_zero(out)
        assert job.workspace.read_file("hello.txt").startswith("Hello")
        # A short file does not refuse a job without todo_complete: the
        # warning travels with the completion instead.
        completion = out["freeze_data"]
        size = len(job.workspace.read_file("hello.txt"))
        assert completion["deliverable_warnings"] == [
            f"Deliverable 'hello.txt' appears empty or trivial ({size} bytes)"
        ]


class TestRequestTexts:
    @pytest.mark.asyncio
    async def test_no_request_names_a_phase_tool_or_strategic_mode(self, tmp_path):
        job = build_job(
            tmp_path,
            [
                plain_reply("Let me think about the greeting first."),
                write_file_call("call_write"),
                job_complete_call(),
            ],
            phase_skill_body="Work in one loop.",
        )

        await job.start()

        assert len(job.requests) == 3
        _assert_no_phase_words(job.requests)
        first = request_text(job.requests[0])
        # The phase-start skill arrives with a header that names no phase.
        assert (
            f"Instructions (from skills/{PHASE_SKILL}/SKILL.md). They apply for "
            "the whole job.\n\nWork in one loop."
        ) in first
        assert "[phase: strategic]" not in first
        # The opening message: the brief, the one task, its footer.
        assert (
            "Do the task in the brief now. When it is done, call `job_complete`."
            in (first)
        )
        assert "Current task" in first
        assert TODO_TEXT in first
        assert (
            format_nudge("todo_list_footer_job_complete", model=job.config.llm.model)
            in first
        )
        # The reminder after the plain reply points at job_complete.
        assert format_nudge(
            "todo_action_job_complete", model=job.config.llm.model
        ) in request_text(job.requests[1])

    @pytest.mark.asyncio
    async def test_the_same_job_with_the_phase_tools_keeps_todays_texts(self, tmp_path):
        """A job that plans in phases: the same script on the phased tool
        list gets the original kickoff, list header, footer and reminder."""
        job = build_job(
            tmp_path,
            [plain_reply("Thinking."), job_complete_call()],
            tool_names=PHASED_TOOLS,
            phase_skill_body="Plan first.",
        )

        await job.start()

        model = job.config.llm.model
        first = request_text(job.requests[0])
        assert (
            f"[phase: strategic] Phase instructions (from skills/{PHASE_SKILL}/"
            "SKILL.md). They apply for the whole phase.\n\nPlan first."
        ) in first
        assert "You are starting in strategic mode." in first
        assert "Current Tasks — Phase 0 (Strategic)" in first
        assert format_nudge("todo_list_footer", model=model) in first
        second = request_text(job.requests[1])
        assert format_nudge("todo_action", model=model, todo_id="todo_1") in second
        assert "invoke `todo_complete` now to record it." in second


class TestPlainReplies:
    @pytest.mark.asyncio
    async def test_stop_at_the_limit_after_two_reminders(self, tmp_path):
        job = build_job(
            tmp_path,
            [
                plain_reply("First, some thoughts."),
                plain_reply("More thoughts, worded differently."),
                plain_reply("And a third take on it."),
            ],
            limits={"max_plain_replies_in_a_row": 3},
        )

        out = await job.start()

        assert len(job.requests) == 3
        model = job.config.llm.model
        reminder = format_nudge("todo_action_job_complete", model=model)
        reminders = [m for m in out["messages"] if reminder in str(m.content)]
        assert len(reminders) == 2
        error = out["error"]
        assert error["type"] == "plain_reply_limit"
        assert error["recoverable"] is False
        assert (error["limit"], error["count"]) == (3, 3)
        assert "max_plain_replies_in_a_row" in error["message"]
        # The graph reached END: the report envelope carries the error.
        assert out["should_stop"] is True
        assert out["completion_report_payload"]["error"]["type"] == (
            "plain_reply_limit"
        )
        # A later resume starts fresh.
        assert out["plain_reply_streak"] == 0
        assert out["llm_call_baseline"] == out["iteration"] == 3
        assert out["todos"][0]["status"] != "completed"

    @pytest.mark.asyncio
    async def test_a_tool_call_resets_the_streak(self, tmp_path):
        job = build_job(
            tmp_path,
            [
                plain_reply("One."),
                plain_reply("Two."),
                write_file_call("call_write"),
                plain_reply("Three."),
                plain_reply("Four."),
                job_complete_call(),
            ],
            limits={"max_plain_replies_in_a_row": 3},
        )

        out = await job.start()

        assert len(job.requests) == 6
        assert out.get("error") is None
        assert out["goal_achieved"] is True

    @pytest.mark.asyncio
    async def test_with_the_limit_off_the_reminder_is_injected_as_before(
        self, tmp_path
    ):
        replies = [plain_reply(f"Reply number {n}.") for n in range(1, 6)]
        job = build_job(tmp_path, [*replies, job_complete_call()])

        out = await job.start()

        assert len(job.requests) == 6
        assert out.get("error") is None
        # The closing tool call reset the streak of five.
        assert out["plain_reply_streak"] == 0
        reminder = format_nudge("todo_action_job_complete", model=job.config.llm.model)
        for request in job.requests[1:]:
            assert (
                request_text(request)
                .rstrip()
                .endswith(f"Pending todos (1):\n  - todo_1: {TODO_TEXT[:80]}")
            )
            assert reminder in request_text(request)

    @pytest.mark.asyncio
    async def test_an_identical_plain_reply_still_ends_as_todays_breaker_does(
        self, tmp_path
    ):
        job = build_job(tmp_path, [plain_reply("DONE")] * 4)

        out = await job.start()

        assert len(job.requests) == 4
        assert out["error"]["type"] == "parser_failure"
        assert out["should_stop"] is True
        assert out["completion_report_payload"]["error"]["type"] == "parser_failure"


class TestMessageWhileRunning:
    """A message sent to a running job reaches the model on its next call.

    Non-urgent mail waits for a completed todo in a job that works through
    todos. Here the only todo is closed by ``job_complete``, the last call, so
    waiting for it would append the mail after the final turn.
    """

    @staticmethod
    def _fresh_reply() -> dict:
        # Just sent: the wall-clock floor (300 s) does not deliver it.
        return {
            "thread_id": "officer",
            "message": "Please also write extra.txt.",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    @pytest.mark.asyncio
    async def test_a_queued_message_is_in_the_next_request(self, tmp_path, monkeypatch):
        reply = self._fresh_reply()
        monkeypatch.setattr(
            "agent.graph._get_queued_replies", lambda job_id: [dict(reply)]
        )
        monkeypatch.setattr(
            "agent.graph._ack_supervisor_guidance", lambda *args, **kwargs: None
        )
        job = build_job(
            tmp_path,
            [
                write_file_call("call_write"),
                write_file_call("call_extra", path="extra.txt"),
                job_complete_call(),
            ],
        )

        await job.start()

        assert "Please also write extra.txt." not in request_text(job.requests[0])
        # Delivered with the first tool step, before the job's last call.
        assert "Please also write extra.txt." in request_text(job.requests[1])
        # Once: the delivered message stays in the history, it is not appended
        # again on the next tool step.
        assert request_text(job.requests[2]).count("Please also write extra.txt.") == 1
        # The wrapper does not send the job to a review step it never reaches.
        assert "Take these into account in the work in progress." in request_text(
            job.requests[1]
        )
        _assert_no_phase_words(job.requests)

    @pytest.mark.asyncio
    async def test_with_todo_steps_the_same_message_waits(self, tmp_path, monkeypatch):
        reply = self._fresh_reply()
        monkeypatch.setattr(
            "agent.graph._get_queued_replies", lambda job_id: [dict(reply)]
        )
        monkeypatch.setattr(
            "agent.graph._ack_supervisor_guidance", lambda *args, **kwargs: None
        )
        job = build_job(
            tmp_path,
            [write_file_call("call_write"), job_complete_call()],
            tool_names=PHASED_TOOLS,
        )

        await job.start()

        assert "Please also write extra.txt." not in request_text(job.requests[1])


class TestFeedbackResume:
    @pytest.mark.asyncio
    async def test_returned_job_finishes_on_one_job_complete_with_a_fresh_budget(
        self, tmp_path
    ):
        limits = {"max_llm_calls_per_job": 2, "max_plain_replies_in_a_row": 3}
        job = build_job(
            tmp_path,
            [write_file_call("call_write"), job_complete_call()],
            autonomy="review",
            limits=limits,
        )
        first = await job.start()
        assert first["freeze_data"]["freeze_type"] == "job_complete"
        assert first["iteration"] == 2  # the whole budget of the first round

        job.rebuild([plain_reply("Reading the feedback."), job_complete_call("c2")])
        out = await job.resume(
            {
                "resume_feedback": "Add a second line to hello.txt.",
                "resume_reason": "The reviewer returned the job.",
                "should_stop": False,
                "goal_achieved": False,
                "is_final_phase": False,
                "completion_decision": None,
                "client_report_id": None,
                "completion_report_payload": None,
                # A streak left over from the first round: the resume clears it.
                "plain_reply_streak": 2,
            }
        )

        # Two calls in the second round, under a cap of two per round.
        assert len(job.llms[-1].requests) == 2
        assert out.get("error") is None
        assert out["freeze_data"]["freeze_type"] == "job_complete"
        assert out["phase_number"] == 0
        assert out["llm_call_baseline"] == 2
        assert out["todos"] == [
            {**out["todos"][0], "content": RESUME_TODO_TEXT, "status": "completed"}
        ]
        resumed = request_text(job.llms[-1].requests[0])
        assert "[FEEDBACK_RESUME] The reviewer returned the job." in resumed
        assert (
            "Take the message above into account, then finish the task and call "
            "`job_complete`."
        ) in resumed
        assert RESUME_TODO_TEXT in resumed
        _assert_no_phase_words(job.llms[-1].requests)
