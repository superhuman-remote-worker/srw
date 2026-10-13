"""The voice subagent of a live-session job (live_session_jobs Part I §6.2).

The child follows a session on ``tests/_fake_live_pod.py``: the connector
pod's session API (server-sent events plus the POST controls, behind the
job's lease) and the orchestrator's journal of the session, in one fake.
"""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any, Dict
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio

import agent.subagents.live as live_mod
from agent.subagents.live import (
    DEFAULT_FOLLOW_UP,
    DEFAULT_GOODBYE,
    NOT_DELEGATED,
    LiveLease,
    LiveSessionSettings,
    LiveVoiceDriver,
    PodClient,
    Transcript,
    attach_live_session,
    is_live_entry,
    live_lease_from_payload,
)
from tests._fake_live_pod import INTERNAL_KEY, LEASE, FakeLivePod
from tests.test_subagent_background_runtime import StrictLedger
from tests.test_subagent_runtime import call, make_parent, runtime_for


def live_roster(**live: Any) -> Dict[str, Any]:
    entry: Dict[str, Any] = {
        "description": "The live voice session.",
        "driver": "live",
        "isolation": "shared",
        "write_policy": "none",
        "limits": {"return_budget_tokens": 6000},
    }
    if live:
        entry["live"] = live
    return {"roster": {"voice": entry}}


@pytest_asyncio.fixture
async def pod(monkeypatch):
    server = FakeLivePod()
    await server.start()
    monkeypatch.setenv("ORCHESTRATOR_URL", server.url)
    monkeypatch.setenv("MCP_INTERNAL_KEY", INTERNAL_KEY)
    for name, value in {
        "_BACKOFF_FIRST_S": 0.01,
        "_BACKOFF_MAX_S": 0.02,
        "_NOT_YET_WINDOW_S": 0.5,
        "_UNREACHABLE_WINDOW_S": 0.5,
        "_GOODBYE_GRACE_S": 0.05,
    }.items():
        monkeypatch.setattr(live_mod, name, value)
    try:
        yield server
    finally:
        await server.stop()


def parent_with_session(tmp_path, pod: FakeLivePod, sid: str, *, lease=LEASE, **live):
    ctx, _ = make_parent(tmp_path, subagents=live_roster(**live))
    ctx.config["live_session"] = {
        "session_id": sid,
        "connector_id": "conn-1",
        "provider_session_id": "rtc_fake",
        "endpoint_url": pod.url,
    }
    ctx._live_lease = LiveLease(
        token=lease, connector_id="conn-1", connection_url=pod.url
    )
    return ctx


async def start_voice(runtime) -> str:
    receipt = await runtime.run_background(
        call(
            "attach-1",
            subagent_type="voice",
            prompt="Handle the session.",
            run_in_background=True,
            live_attach=True,
        )
    )
    assert "queued" in receipt, receipt
    return receipt.split("[subagent ", 1)[1].split(" ", 1)[0]


def report_of(runtime, handle: str) -> str:
    return runtime._background[handle].envelope


class TestHelpers:
    def test_is_live_entry(self):
        assert is_live_entry({"driver": "live"})
        assert not is_live_entry({"driver": "llm"})
        assert not is_live_entry({"driver": "phone"})
        assert not is_live_entry(None)

    def test_settings_need_the_session(self):
        with pytest.raises(ValueError):
            LiveSessionSettings.from_config({})
        with pytest.raises(ValueError):
            LiveSessionSettings.from_config({"live_session": {"session_id": "s"}})

    def test_settings_read_the_roster_entry_and_prefer_the_payload_url(self):
        lease = LiveLease(token=LEASE, connection_url="http://pod-now:8080")
        settings = LiveSessionSettings.from_config(
            {
                "live_session": {
                    "session_id": "s",
                    "connector_id": "c",
                    "endpoint_url": "http://pod-then:8080",
                }
            },
            {
                "live": {
                    "answer_timeout_s": 12,
                    "texts": {"follow_up": "Noted.", "goodbye": "Bye."},
                }
            },
            lease,
        )
        assert settings.endpoint_url == "http://pod-now:8080"
        assert settings.answer_timeout_s == 12
        assert (settings.follow_up, settings.goodbye) == ("Noted.", "Bye.")
        assert settings.lease_token == LEASE
        assert LEASE not in repr(settings) and LEASE not in repr(lease)

    def test_defaults_are_short_english_instructions(self):
        settings = LiveSessionSettings.from_config(
            {"live_session": {"session_id": "s", "endpoint_url": "http://p"}}
        )
        assert settings.follow_up == DEFAULT_FOLLOW_UP
        assert settings.goodbye == DEFAULT_GOODBYE
        assert settings.answer_timeout_s == 25.0

    def test_lease_comes_from_the_live_connector_entry(self):
        payload = [
            {"datasource_id": "pg-1", "type": "postgresql", "credentials": {}},
            {
                "datasource_id": "conn-1",
                "type": "gpt_live_sip",
                "connection_url": "http://srw-ep-x:8080",
                "credentials": {
                    "lease": {"id": "l-1", "connector_id": "conn-1", "token": LEASE}
                },
            },
        ]
        lease = live_lease_from_payload(payload, "conn-1")
        assert lease is not None
        assert lease.token == LEASE
        assert lease.connection_url == "http://srw-ep-x:8080"
        assert live_lease_from_payload(payload, "other") is None
        assert live_lease_from_payload(payload[:1], "pg-1") is None

    def test_pod_client_repr_hides_the_lease(self):
        client = PodClient("http://pod", "s", LEASE, journal_url="http://o")
        assert LEASE not in repr(client)

    def test_transcript_groups_deltas(self):
        transcript = Transcript()
        transcript.add("caller", "Do you have ", event_id="e1")
        transcript.add("caller", "a car park?", event_id="e2")
        transcript.add("caller", "a car park?", event_id="e2")  # replay
        transcript.add("assistant", "Yes.", event_id="e3")
        assert [t.text for t in transcript.turns] == ["Do you have a car park?", "Yes."]
        assert transcript.render() == "Caller: Do you have a car park?\nAssistant: Yes."

    def test_the_request_is_the_callers_last_stretch_before_the_delegation(self):
        settings = LiveSessionSettings.from_config(
            {"live_session": {"session_id": "s", "endpoint_url": "http://p"}}
        )
        driver = LiveVoiceDriver(
            settings,
            handle="voice-1",
            subagent_type="voice",
            subagent_id="x",
            client=MagicMock(),
        )
        for speaker, text in [
            ("assistant", "Hello."),
            ("caller", "Do you have a car park?"),
            ("assistant", "Yes, behind the building."),
            ("caller", "I am ill. "),
            ("caller", "I need to cancel Tuesday's appointment."),
            ("assistant", "One moment, please."),
        ]:
            driver.transcript.add(speaker, text)
        assert (
            driver._request_upto(None)
            == "I am ill. I need to cancel Tuesday's appointment."
        )
        driver.transcript.add("caller", "And when do I leave?")
        assert driver._request_upto(None) == "And when do I leave?"


class TestLiveSubagent:
    @pytest.mark.asyncio
    async def test_question_answer_and_report(self, tmp_path, pod):
        sid = pod.open_session()
        ctx = parent_with_session(tmp_path, pod, sid)
        runtime = runtime_for(ctx, ledger=StrictLedger())
        handle = await start_voice(runtime)
        await pod.wait_for(lambda: pod.session(sid).attached == 1)

        pod.say(sid, "assistant", "Hello, you are speaking with an AI assistant.")
        pod.say(sid, "caller", "I need to cancel my appointment on Tuesday.")
        delegation_id = pod.delegate(sid)

        waited = await runtime.wait_agent(handle, 10)
        assert waited["result"] == "question"
        question = waited["question"]
        assert question["delegation_id"] == delegation_id
        assert "cancel my appointment on Tuesday" in question["request"]

        answered = await runtime.message_agent(
            handle, "The Tuesday appointment is cancelled.", reply_to=delegation_id
        )
        assert answered["result"] == "answered"
        (append,) = pod.session(sid).appends
        assert append["kind"] == "commentary"
        assert append["delegation_id"] == delegation_id
        assert append["text"] == "The Tuesday appointment is cancelled."
        assert append["event_id"].startswith("job_")

        pod.say(sid, "caller", "Thanks, bye.")
        pod.close(sid)
        waited = await runtime.wait_agent(handle, 10)
        assert waited["result"] == "ready"
        assert waited["outcome"] == "completed"
        report = report_of(runtime, handle)
        assert "Caller: I need to cancel my appointment on Tuesday." in report
        assert "## Transcript" in report
        assert "## Questions the voice handed to the back office" in report
        assert "[answered]" in report and "Ended: remote_hangup." in report
        await pod.wait_for(
            lambda: {"question", "answer"} <= {row["kind"] for row in pod.job_rows(sid)}
        )
        answer_row = next(r for r in pod.job_rows(sid) if r["kind"] == "answer")
        assert answer_row["event_id"] == append["event_id"]
        assert set(pod.authorizations) == {f"Bearer {LEASE}"}
        assert set(pod.internal_keys) == {INTERNAL_KEY}
        await runtime.close()

    @pytest.mark.asyncio
    async def test_unanswered_question_becomes_a_follow_up(self, tmp_path, pod):
        sid = pod.open_session()
        ctx = parent_with_session(tmp_path, pod, sid, answer_timeout_s=0.3)
        runtime = runtime_for(ctx, ledger=StrictLedger())
        handle = await start_voice(runtime)
        await pod.wait_for(lambda: pod.session(sid).attached == 1)
        pod.say(sid, "caller", "May I bring my dog?")
        delegation_id = pod.delegate(sid)
        await pod.wait_for(lambda: pod.session(sid).appends)
        (append,) = pod.session(sid).appends
        assert append["kind"] == "commentary"
        assert append["delegation_id"] == delegation_id
        assert append["text"] == DEFAULT_FOLLOW_UP
        pod.close(sid)
        await runtime.wait_agent(handle, 10)
        report = report_of(runtime, handle)
        assert "[follow_up]" in report
        assert "## Follow-ups to handle now" in report and "dog" in report
        await pod.wait_for(
            lambda: "follow_up" in {r["kind"] for r in pod.job_rows(sid)}
        )
        await runtime.close()

    @pytest.mark.asyncio
    async def test_the_expert_overrides_the_follow_up_text(self, tmp_path, pod):
        sid = pod.open_session()
        ctx = parent_with_session(
            tmp_path,
            pod,
            sid,
            answer_timeout_s=0.3,
            texts={"follow_up": "Say that a colleague will call back."},
        )
        runtime = runtime_for(ctx, ledger=StrictLedger())
        await start_voice(runtime)
        await pod.wait_for(lambda: pod.session(sid).attached == 1)
        pod.say(sid, "caller", "Can someone call me?")
        pod.delegate(sid)
        await pod.wait_for(lambda: pod.session(sid).appends)
        assert pod.session(sid).appends[0]["text"] == (
            "Say that a colleague will call back."
        )
        await runtime.close()

    @pytest.mark.asyncio
    async def test_the_journal_backfills_and_the_replay_is_deduplicated(
        self, tmp_path, pod
    ):
        sid = pod.open_session()
        # Said before the job got there: older than the pod's 3 s replay, so
        # only the journal still has it ...
        pod.say(sid, "caller", "I would like a call back.")
        pod.session(sid).history[-1] = (
            time.monotonic() - 10,
            pod.session(sid).history[-1][1],
        )
        # ... while the delegation is in both the journal and the replay.
        delegation_id = pod.delegate(sid)
        ctx = parent_with_session(tmp_path, pod, sid)
        runtime = runtime_for(ctx, ledger=StrictLedger())
        handle = await start_voice(runtime)

        waited = await runtime.wait_agent(handle, 10)
        assert waited["result"] == "question"
        assert waited["question"]["delegation_id"] == delegation_id
        assert waited["question"]["request"] == "I would like a call back."
        assert len(runtime._background[handle].questions) == 1
        pod.close(sid)
        await runtime.wait_agent(handle, 10)
        report = report_of(runtime, handle)
        assert report.count("Caller: I would like a call back.") == 1
        await runtime.close()

    @pytest.mark.asyncio
    async def test_a_question_the_pod_already_noted_is_a_follow_up(self, tmp_path, pod):
        sid = pod.open_session()
        pod.say(sid, "caller", "Please call me back.")
        delegation_id = pod.delegate(sid)
        pod.journal_row(
            sid,
            {
                "event_id": "fallback-1",
                "kind": "fallback",
                "speaker": "system",
                "delegation_id": delegation_id,
            },
        )
        ctx = parent_with_session(tmp_path, pod, sid)
        runtime = runtime_for(ctx, ledger=StrictLedger())
        handle = await start_voice(runtime)
        await pod.wait_for(lambda: pod.session(sid).attached == 1)
        pod.close(sid)
        waited = await runtime.wait_agent(handle, 10)
        assert waited["result"] == "ready"  # never surfaced as a live question
        assert "[follow_up] Please call me back." in report_of(runtime, handle)
        await runtime.close()

    @pytest.mark.asyncio
    async def test_steer_and_transfer(self, tmp_path, pod):
        sid = pod.open_session()
        ctx = parent_with_session(tmp_path, pod, sid)
        runtime = runtime_for(ctx, ledger=StrictLedger())
        handle = await start_voice(runtime)
        await pod.wait_for(lambda: pod.session(sid).attached == 1)

        steered = await runtime.message_agent(handle, "Ask for the caller's name.")
        assert steered["result"] == "steered"
        (append,) = pod.session(sid).appends
        assert append["kind"] == "instructions"
        assert append["delegation_id"] is None
        assert "caller's name" in append["text"]

        refused = await runtime.message_agent(
            handle, "One moment, I am putting you through.", call_action="transfer"
        )
        assert refused["transfer"]["result"] == "refused"

        pod.session(sid).transfer_target = "tel:+10000000000"
        transferred = await runtime.message_agent(
            handle, "One moment, I am putting you through.", call_action="transfer"
        )
        assert transferred["transfer"]["result"] == "transferring"
        assert pod.session(sid).transfers == [{"target": "tel:+10000000000"}]
        await pod.wait_for(lambda: "transfer" in {r["kind"] for r in pod.job_rows(sid)})
        pod.close(sid)
        await runtime.wait_agent(handle, 10)
        await runtime.close()

    @pytest.mark.asyncio
    async def test_stop_agent_says_goodbye_and_hangs_up(self, tmp_path, pod):
        sid = pod.open_session()
        ctx = parent_with_session(
            tmp_path, pod, sid, texts={"goodbye": "Say goodbye now."}
        )
        runtime = runtime_for(ctx, ledger=StrictLedger())
        handle = await start_voice(runtime)
        await pod.wait_for(lambda: pod.session(sid).attached == 1)

        stopped = await runtime.stop_agent(handle, 2.0)

        assert stopped["result"] == "stopped"
        session = pod.session(sid)
        assert session.hangups == 1 and session.status == "ended"
        goodbye = [a for a in session.appends if a["kind"] == "instructions"]
        assert goodbye and goodbye[0]["text"] == "Say goodbye now."
        await pod.wait_for(lambda: "hangup" in {r["kind"] for r in pod.job_rows(sid)})
        # stop_agent ends the job's session: the job turns strategic.
        assert ctx.consume_live_session_end() == "close_requested"
        await runtime.close()

    @pytest.mark.asyncio
    async def test_runtime_close_leaves_the_session_up(self, tmp_path, pod):
        sid = pod.open_session()
        ctx = parent_with_session(tmp_path, pod, sid)
        runtime = runtime_for(ctx, ledger=StrictLedger())
        await start_voice(runtime)
        await pod.wait_for(lambda: pod.session(sid).attached == 1)

        await runtime.close()

        await pod.wait_for(lambda: not pod.session(sid).subscribers)
        assert pod.session(sid).status == "active"
        assert pod.session(sid).hangups == 0
        # A detach is not the session's end.
        assert ctx.consume_live_session_end() is None
        assert ctx.consume_replan_request() is None

    @pytest.mark.asyncio
    async def test_delegate_agent_never_starts_a_live_entry(self, tmp_path, pod):
        sid = pod.open_session()
        ctx = parent_with_session(tmp_path, pod, sid)
        ledger = StrictLedger()
        runtime = runtime_for(ctx, ledger=ledger)

        foreground = await runtime.run_foreground(
            call("fg", subagent_type="voice", prompt="x")
        )
        background = await runtime.run_background(
            call("bg", subagent_type="voice", prompt="x", run_in_background=True)
        )
        forked = await runtime.run_background(
            call(
                "fork",
                subagent_type="voice",
                prompt="x",
                fork=True,
                run_in_background=True,
                live_attach=True,
            )
        )

        assert foreground == f"Error: voice: {NOT_DELEGATED}"
        assert background == f"Error: voice: {NOT_DELEGATED}"
        assert forked.startswith("Error:") and "no fork" in forked
        assert not ledger.opened
        assert pod.session(sid).stream_opens == 0
        await runtime.close()

    @pytest.mark.asyncio
    async def test_reply_to_is_for_live_voice_subagents_only(self, tmp_path):
        ctx, _ = make_parent(tmp_path)
        runtime = runtime_for(ctx, ledger=StrictLedger())
        result = await runtime.message_agent("explorer-0000", "x", reply_to="item_1")
        assert result["result"] in {"invalid", "not_found"}
        await runtime.close()


class TestSessionStates:
    @pytest.mark.asyncio
    async def test_a_handoff_still_on_its_way_is_retried(self, tmp_path, pod):
        """The job can be dispatched before the pod has the session: 404 while
        the journal says ringing means not yet."""
        sid = pod.open_session(unknown_for=3, journal_status="ringing")
        ctx = parent_with_session(tmp_path, pod, sid)
        runtime = runtime_for(ctx, ledger=StrictLedger())
        handle = await start_voice(runtime)

        await pod.wait_for(lambda: pod.session(sid).attached == 1)
        pod.session(sid).journal_status = None
        pod.say(sid, "caller", "Hello?")
        pod.delegate(sid)
        assert (await runtime.wait_agent(handle, 10))["result"] == "question"
        await runtime.close()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("status", "reason", "expected"),
        [
            ("ended", "remote_hangup", "remote_hangup"),
            ("failed", None, "failed"),
            ("rejected", None, "rejected"),
        ],
    )
    async def test_a_session_over_before_the_job_settles_at_once(
        self, tmp_path, pod, status, reason, expected
    ):
        """The missed-session path: the pod no longer knows the session and
        the journal says it is over; the run settles from the journal."""
        sid = pod.open_session(status=status, forgotten=True)
        pod.session(sid).close_reason = reason
        pod.say(sid, "caller", "Is anyone there?")
        ctx = parent_with_session(tmp_path, pod, sid)
        runtime = runtime_for(ctx, ledger=StrictLedger())
        handle = await start_voice(runtime)

        waited = await runtime.wait_agent(handle, 10)

        assert waited["result"] == "ready" and waited["outcome"] == "completed"
        report = report_of(runtime, handle)
        assert f"Ended: {expected}." in report
        assert "Caller: Is anyone there?" in report
        assert "comes from the session's journal" in report
        assert ctx._replan_request == f"live session ended: {expected}"
        assert ctx.consume_live_session_end() == expected
        await runtime.close()

    @pytest.mark.asyncio
    async def test_an_ended_session_past_its_replay_settles_from_the_pod(
        self, tmp_path, pod
    ):
        sid = pod.open_session()
        pod.say(sid, "caller", "Goodbye.")
        pod.close(sid)
        session = pod.session(sid)
        session.history = [(time.monotonic() - 10, e) for _, e in session.history]
        ctx = parent_with_session(tmp_path, pod, sid)
        runtime = runtime_for(ctx, ledger=StrictLedger())
        handle = await start_voice(runtime)

        waited = await runtime.wait_agent(handle, 10)

        assert waited["outcome"] == "completed"
        assert "Ended: remote_hangup." in report_of(runtime, handle)
        assert "Caller: Goodbye." in report_of(runtime, handle)
        assert ctx.consume_live_session_end() == "remote_hangup"
        await runtime.close()

    @pytest.mark.asyncio
    async def test_a_session_the_pod_never_learns_is_gone_after_the_window(
        self, tmp_path, pod
    ):
        sid = pod.open_session(forgotten=True)
        ctx = parent_with_session(tmp_path, pod, sid)
        runtime = runtime_for(ctx, ledger=StrictLedger())
        handle = await start_voice(runtime)

        waited = await runtime.wait_agent(handle, 10)

        assert waited["outcome"] == "completed"
        assert "Ended: session_gone." in report_of(runtime, handle)
        assert ctx.consume_live_session_end() == "session_gone"
        await runtime.close()

    @pytest.mark.asyncio
    async def test_a_refused_lease_ends_the_session_for_the_job(self, tmp_path, pod):
        sid = pod.open_session()
        ctx = parent_with_session(tmp_path, pod, sid, lease="scl_wrong")
        runtime = runtime_for(ctx, ledger=StrictLedger())
        handle = await start_voice(runtime)

        waited = await runtime.wait_agent(handle, 10)

        assert "Ended: not_authorized." in report_of(runtime, handle)
        assert waited["outcome"] == "completed"
        assert pod.session(sid).attached == 0
        await runtime.close()

    @pytest.mark.asyncio
    async def test_a_voice_subagent_that_fails_still_turns_the_job(self, tmp_path):
        ctx, _ = make_parent(tmp_path, subagents=live_roster())
        ctx.config["live_session"] = {"session_id": "s"}  # no endpoint anywhere
        runtime = runtime_for(ctx, ledger=StrictLedger())
        handle = await start_voice(runtime)

        waited = await runtime.wait_agent(handle, 10)

        assert waited["outcome"] == "error"
        assert ctx.consume_live_session_end() == "the voice subagent failed"
        await runtime.close()


class TestLeaseStaysInProcess:
    @pytest.mark.asyncio
    async def test_the_lease_never_reaches_logs_reports_or_the_journal(
        self, tmp_path, pod, caplog
    ):
        caplog.set_level(logging.DEBUG)
        sid = pod.open_session()
        ctx = parent_with_session(tmp_path, pod, sid)
        runtime = runtime_for(ctx, ledger=StrictLedger())
        exchange = await attach_live_session(ctx)
        handle = exchange[1].content.split("[subagent ", 1)[1].split(" ", 1)[0]
        await pod.wait_for(lambda: pod.session(sid).attached == 1)
        pod.say(sid, "caller", "What time is it?")
        delegation_id = pod.delegate(sid)
        await runtime.wait_agent(handle, 10)
        await runtime.message_agent(handle, "It is noon.", reply_to=delegation_id)
        listed = await runtime.list_agents()
        await runtime.stop_agent(handle, 2.0)
        run = runtime._background[handle]

        surfaces = [
            caplog.text,
            run.envelope,
            str(exchange),
            json.dumps(listed),
            json.dumps(pod.journal[sid]),
            runtime.active_subagents_block(),
            repr(ctx),
            json.dumps(ctx.config, default=str),
        ]
        for surface in surfaces:
            assert LEASE not in surface
        await runtime.close()


class TestLiveJob:
    """A live-session job starts with its session attached as the voice subagent."""

    @pytest.mark.asyncio
    async def test_attach_seeds_a_delegate_exchange(self, tmp_path, pod):
        from langchain_core.messages import AIMessage, ToolMessage

        sid = pod.open_session()
        ctx = parent_with_session(tmp_path, pod, sid)
        runtime = runtime_for(ctx, ledger=StrictLedger())

        exchange = await attach_live_session(ctx)

        assert isinstance(exchange[0], AIMessage)
        assert isinstance(exchange[1], ToolMessage)
        call_ = exchange[0].tool_calls[0]
        assert call_["name"] == "delegate_agent"
        assert call_["args"]["subagent_type"] == "voice"
        assert call_["args"]["run_in_background"] is True
        assert exchange[1].tool_call_id == call_["id"]
        assert "queued" in exchange[1].content
        handle = exchange[1].content.split("[subagent ", 1)[1].split(" ", 1)[0]
        assert runtime._background[handle].live_primary is True
        await pod.wait_for(lambda: pod.session(sid).attached == 1)
        pod.say(sid, "caller", "When is my appointment?")
        pod.delegate(sid)
        assert (await runtime.wait_agent(handle, 10))["result"] == "question"
        pod.close(sid)
        await runtime.wait_agent(handle, 10)
        assert ctx.consume_live_session_end() == "remote_hangup"
        await runtime.close()

    @pytest.mark.asyncio
    async def test_other_jobs_are_untouched(self, tmp_path):
        ctx, _ = make_parent(tmp_path)
        assert await attach_live_session(ctx) is None

    @pytest.mark.asyncio
    async def test_a_job_whose_expert_has_no_live_entry_turns_strategic(self, tmp_path):
        ctx, _ = make_parent(tmp_path)
        runtime_for(ctx, ledger=StrictLedger())
        ctx.config["live_session"] = {"session_id": "s", "endpoint_url": "http://p"}

        assert await attach_live_session(ctx) is None
        assert ctx.consume_live_session_end() == "the voice subagent could not start"

    @pytest.mark.asyncio
    async def test_resume_supersedes_the_old_run_and_attaches_a_new_one(
        self, tmp_path, pod
    ):
        """recover_orphans ends the predecessor's row superseded, without an
        interrupted delivery; the re-attach reads the journal from the start
        and the old handle leads to the new run."""
        sid = pod.open_session()
        pod.say(sid, "caller", "Are you still there?")
        delegation_id = pod.delegate(sid)
        ctx = parent_with_session(tmp_path, pod, sid)
        ledger = StrictLedger()
        ledger.live = [
            {
                "thread_id": "11111111-2222-4333-8444-555555555555",
                "runtime_generation": "aaaaaaaa-1111-4222-8333-444444444444",
                "handle": "voice-0ld0",
                "subagent_type": "voice",
                "run_in_background": True,
                "total_turns": 3,
            }
        ]
        runtime = runtime_for(ctx, ledger=ledger)

        recovered = await runtime.recover_orphans()

        assert recovered[0]["outcome"] == "interrupted:superseded"
        assert ledger.updates == [
            (
                "11111111-2222-4333-8444-555555555555",
                {
                    "status": "interrupted",
                    "outcome": "interrupted:superseded",
                    "turns": 3,
                    "tokens": 0,
                    "error": "superseded by a new attach to the live session",
                },
            )
        ]
        assert not ledger.terminal_calls  # no interrupted report delivered
        assert not runtime.drain_local_deliveries()

        await attach_live_session(ctx)
        (run,) = runtime._background.values()
        assert run.replaces == ["voice-0ld0"]
        assert "Reconnected to the live session" in runtime.active_subagents_block()
        assert "voice-0ld0" in runtime.active_subagents_block()
        waited = await runtime.wait_agent("voice-0ld0", 10)
        assert waited["result"] == "question"
        assert waited["handle"] == run.handle
        assert waited["question"]["delegation_id"] == delegation_id
        await runtime.close()

    def test_init_node_starts_tactical_with_the_exchange(self, tmp_path):
        from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

        from agent.graph import create_init_strategic_todos_node
        from agent.managers.todo import TodoManager

        workspace = MagicMock()
        workspace.read_file.side_effect = lambda name: (
            "A live session on the voice desk." if name == "task_brief.md" else ""
        )
        todo = TodoManager(workspace=MagicMock())
        exchange = [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "delegate_agent",
                        "args": {},
                        "id": "call_live_1",
                        "type": "tool_call",
                    }
                ],
            ),
            ToolMessage(
                content="[subagent voice-ab12 · voice · queued]",
                tool_call_id="call_live_1",
            ),
        ]
        tool_context = MagicMock(_initial_tool_exchange=exchange)
        config = MagicMock()
        config.extra = {"live_session": {"session_id": "s"}}
        config._deployment_dir = None
        config.llm.model = ""
        node = create_init_strategic_todos_node(
            workspace, todo, config, tool_context=tool_context
        )

        result = node({"job_id": "j", "phase_number": 0})

        assert result["is_strategic_phase"] is False
        assert result["phase_number"] == 0
        first = result["messages"][0]
        assert isinstance(first, HumanMessage)
        assert "You are starting in tactical mode" in first.content
        assert "Current Tasks — Phase 0 (Tactical)" in first.content
        assert result["messages"][1:] == exchange
        (live_todo,) = todo.list_all()
        assert live_todo.content.startswith(
            "Handle the live session until it ends: wait for the voice "
            "subagent's questions and answer them."
        )
        assert result["todos"][0]["content"] == live_todo.content
        assert todo.is_strategic_phase is False
        assert tool_context._initial_tool_exchange is None

    def test_voice_desk_expert(self):
        import yaml

        from shared.runtime.core.loader import load_agent_config

        path = Path(__file__).resolve().parents[1] / "config/experts/phone-desk"
        cfg = load_agent_config(str(path / "config.yaml"))
        assert cfg.delegation.enabled is True
        entry = cfg.subagents.roster["voice"]
        assert is_live_entry(entry)
        assert cfg.subagents.default is None  # never delegated by default
        assert entry["live"]["voice"] == "marin"
        assert "AI assistant" in entry["live"]["greeting"]
        assert {"wait_agent", "message_agent", "delegate_agent", "stop_agent"} <= set(
            cfg.tools.delegation
        )
        raw = yaml.safe_load((path / "config.yaml").read_text())
        authored = raw["spec"]["runtime"]["config"]["config"]
        # SRW does not restrict live jobs: no autonomy or memory overrides.
        assert "autonomy" not in authored and "memory" not in authored
        # The bundled desk starts tactical from the framework's live todos.
        assert not (path / "strategic_todos_initial.yaml").exists()


class TestAgentAttach:
    """``UniversalAgent._attach_live_session`` at a fresh start and a resume."""

    def _agent(self, context, values=None):
        from agent.agent import UniversalAgent

        agent = UniversalAgent.__new__(UniversalAgent)
        agent._tool_context = context
        agent._current_job_id = "job-1"
        agent._graph = MagicMock()
        agent._graph.aget_state = AsyncMock(return_value=MagicMock(values=values or {}))
        return agent

    @pytest.mark.asyncio
    async def test_a_fresh_start_seeds_the_exchange(self, monkeypatch):
        attach = AsyncMock(return_value=["exchange"])
        monkeypatch.setattr(live_mod, "attach_live_session", attach)
        context = MagicMock(config={"live_session": {"session_id": "s"}})
        agent = self._agent(context)

        await agent._attach_live_session({"job_id": "job-1"}, {})

        attach.assert_awaited_once_with(context)
        assert context._initial_tool_exchange == ["exchange"]
        agent._graph.aget_state.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_resume_reattaches_until_the_end_was_taken_in(self, monkeypatch):
        attach = AsyncMock(return_value=["exchange"])
        monkeypatch.setattr(live_mod, "attach_live_session", attach)
        context = MagicMock(config={"live_session": {"session_id": "s"}})
        context._initial_tool_exchange = None

        await self._agent(context)._attach_live_session(None, {})
        attach.assert_awaited_once_with(context)
        assert context._initial_tool_exchange is None  # history not reseeded

        attach.reset_mock()
        ended = {"live_session_ended": {"reason": "expired", "phase_number": 0}}
        await self._agent(context, ended)._attach_live_session(None, {})
        attach.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_other_jobs_attach_nothing(self, monkeypatch):
        attach = AsyncMock()
        monkeypatch.setattr(live_mod, "attach_live_session", attach)
        await self._agent(MagicMock(config={}))._attach_live_session({}, {})
        attach.assert_not_awaited()
