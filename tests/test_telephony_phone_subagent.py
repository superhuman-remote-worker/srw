"""P3 of phone calls: the phone subagent in the background-subagent runtime.

The child attaches to a call on ``tests/_fake_gpt_live.py``; the orchestrator's
internal call-record API is an in-memory fake.
"""

from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional

import pytest
import pytest_asyncio

import agent.subagents.phone as phone_mod
from agent.subagents.phone import (
    FOLLOW_UP_TEXT,
    PhoneCallSettings,
    Transcript,
    is_phone_entry,
)
from shared.runtime.telephony.gpt_live import (
    GptLiveClient,
    LiveEndpoints,
    build_session_config,
)
from tests._fake_gpt_live import DEFAULT_KEY, FakeGptLive
from tests.test_subagent_background_runtime import StrictLedger
from tests.test_subagent_runtime import call, make_parent, runtime_for


class FakeRecord:
    """Stands in for ``CallRecordClient`` (the orchestrator's internal API)."""

    def __init__(self, backlog: Optional[List[Dict[str, Any]]] = None) -> None:
        self.backlog = list(backlog or [])
        self.attached: List[tuple] = []
        self.posted: List[Dict[str, Any]] = []

    async def attach(self, call_id: str, job_id: Optional[str]) -> Dict[str, Any]:
        self.attached.append((call_id, job_id))
        return {"id": call_id, "status": "active"}

    async def events(self, call_id: str, after_id: int = 0) -> Dict[str, Any]:
        return {"call": {"id": call_id}, "events": list(self.backlog)}

    async def post_events(self, call_id: str, events: List[Dict[str, Any]]) -> None:
        self.posted.extend(events)

    async def close(self) -> None:
        return None

    def kinds(self) -> List[str]:
        return [e["kind"] for e in self.posted]


def phone_roster(**phone: Any) -> Dict[str, Any]:
    entry: Dict[str, Any] = {
        "description": "The live phone call.",
        "driver": "phone",
        "isolation": "shared",
        "write_policy": "none",
        "limits": {"return_budget_tokens": 6000},
    }
    if phone:
        entry["phone"] = phone
    return {"default": "phone", "roster": {"phone": entry}}


@pytest_asyncio.fixture
async def fake():
    server = FakeGptLive()
    await server.start()
    try:
        yield server
    finally:
        await server.stop()


async def accepted_call(fake: FakeGptLive) -> str:
    session_id = await fake.incoming_call()
    async with GptLiveClient(
        DEFAULT_KEY, LiveEndpoints(fake.api_base, fake.ws_base)
    ) as client:
        await client.accept(session_id, build_session_config(instructions="x"))
    return session_id


def parent_with_call(
    tmp_path,
    fake: FakeGptLive,
    session_id: str,
    *,
    answer_timeout_s: float = 25.0,
    transfer_target=None,
):
    ctx, _ = make_parent(tmp_path, subagents=phone_roster())
    ctx.config["telephony"] = {
        "call_id": "call-1",
        "session_id": session_id,
        "api_key": DEFAULT_KEY,
        "api_base": fake.api_base,
        "ws_base": fake.ws_base,
        "line": {
            "name": "Empfang",
            "answer_timeout_s": answer_timeout_s,
            "transfer_target": transfer_target,
        },
    }
    return ctx


@pytest.fixture
def record(monkeypatch):
    holder = FakeRecord()
    monkeypatch.setattr(
        phone_mod.CallRecordClient, "from_env", classmethod(lambda cls: holder)
    )
    return holder


async def start_phone(runtime) -> str:
    receipt = await runtime.run_background(
        call(
            "call-tool-1",
            subagent_type="phone",
            prompt="Handle the call.",
            run_in_background=True,
        )
    )
    assert "queued" in receipt, receipt
    return receipt.split("[subagent ", 1)[1].split(" ", 1)[0]


async def wait_attached(fake: FakeGptLive, session_id: str, count: int = 1) -> None:
    await fake.wait_until(session_id, lambda s: len(s.sockets) >= count, timeout=5)


class TestHelpers:
    def test_is_phone_entry(self):
        assert is_phone_entry({"driver": "phone"})
        assert not is_phone_entry({"driver": "llm"})
        assert not is_phone_entry(None)

    def test_settings_need_the_call(self):
        with pytest.raises(ValueError):
            PhoneCallSettings.from_config({})
        settings = PhoneCallSettings.from_config(
            {
                "telephony": {
                    "call_id": "c",
                    "session_id": "s",
                    "api_key": "k",
                    "line": {"answer_timeout_s": 12},
                }
            }
        )
        assert settings.answer_timeout_s == 12

    def test_transcript_groups_deltas(self):
        transcript = Transcript()
        transcript.add("caller", "Haben Sie ", event_id="e1")
        transcript.add("caller", "ein Schwimmbad?", event_id="e2")
        transcript.add("caller", "ein Schwimmbad?", event_id="e2")  # replay
        transcript.add("assistant", "Ja.", event_id="e3")
        assert [t.text for t in transcript.turns] == [
            "Haben Sie ein Schwimmbad?",
            "Ja.",
        ]
        assert (
            transcript.render() == "Caller: Haben Sie ein Schwimmbad?\nAssistant: Ja."
        )


class TestPhoneSubagent:
    @pytest.mark.asyncio
    async def test_question_answer_and_report(self, tmp_path, fake, record):
        session_id = await accepted_call(fake)
        ctx = parent_with_call(tmp_path, fake, session_id)
        runtime = runtime_for(ctx, ledger=StrictLedger())
        handle = await start_phone(runtime)
        await wait_attached(fake, session_id)
        assert record.attached == [("call-1", "parent-job")]

        await fake.say(
            session_id, "assistant", "Guten Tag, hier ist der digitale Assistent."
        )
        await fake.say(
            session_id, "caller", "Ich muss meinen Termin am Dienstag absagen."
        )
        delegation_id = await fake.delegate(session_id)

        waited = await runtime.wait_agent(handle, 10)
        assert waited["result"] == "question"
        question = waited["question"]
        assert question["delegation_id"] == delegation_id
        assert "Dienstag absagen" in question["request"]

        answered = await runtime.message_agent(
            handle, "Der Termin am Dienstag ist storniert.", reply_to=delegation_id
        )
        assert answered["result"] == "answered"
        sent = await fake.wait_for_command(
            session_id,
            "session.commentary.append",
            lambda c: c["delegation_id"] == delegation_id,
        )
        assert sent["content"] == "Der Termin am Dienstag ist storniert."

        await fake.say(session_id, "caller", "Danke, tschüss.")
        await fake.remote_hangup(session_id)
        waited = await runtime.wait_agent(handle, 10)
        assert waited["result"] == "ready"
        assert waited["outcome"] == "completed"
        deliveries = runtime.drain_local_deliveries()
        report = (
            deliveries[0]["message"]
            if deliveries
            else runtime._background[handle].envelope
        )
        assert "Ich muss meinen Termin am Dienstag absagen." in report
        assert "[answered]" in report
        assert "question" in record.kinds() and "answer" in record.kinds()
        await runtime.close()

    @pytest.mark.asyncio
    async def test_unanswered_question_becomes_a_follow_up(
        self, tmp_path, fake, record
    ):
        session_id = await accepted_call(fake)
        ctx = parent_with_call(tmp_path, fake, session_id, answer_timeout_s=0.3)
        runtime = runtime_for(ctx, ledger=StrictLedger())
        handle = await start_phone(runtime)
        await wait_attached(fake, session_id)
        await fake.say(session_id, "caller", "Darf ich meinen Hund mitbringen?")
        delegation_id = await fake.delegate(session_id)
        sent = await fake.wait_for_command(
            session_id,
            "session.commentary.append",
            lambda c: c["delegation_id"] == delegation_id,
        )
        assert sent["content"] == FOLLOW_UP_TEXT
        # The question was never surfaced to the parent while open, then timed out.
        await fake.remote_hangup(session_id)
        await runtime.wait_agent(handle, 10)
        report = runtime._background[handle].envelope
        assert "[follow_up]" in report
        assert "Follow-ups" in report and "Hund" in report
        assert "follow_up" in record.kinds()
        await runtime.close()

    @pytest.mark.asyncio
    async def test_backlog_question_is_raised_on_attach(
        self, tmp_path, fake, monkeypatch
    ):
        session_id = await accepted_call(fake)
        backlog = FakeRecord(
            [
                {
                    "id": 1,
                    "kind": "transcript",
                    "speaker": "caller",
                    "text": "Ich möchte einen Rückruf.",
                    "event_id": "b1",
                },
                {
                    "id": 2,
                    "kind": "delegation",
                    "delegation_id": "item_old",
                    "event_id": "b2",
                },
            ]
        )
        fake.sessions[session_id].delegations.add("item_old")
        monkeypatch.setattr(
            phone_mod.CallRecordClient, "from_env", classmethod(lambda cls: backlog)
        )
        ctx = parent_with_call(tmp_path, fake, session_id)
        runtime = runtime_for(ctx, ledger=StrictLedger())
        handle = await start_phone(runtime)
        waited = await runtime.wait_agent(handle, 10)
        assert waited["result"] == "question"
        assert waited["question"]["delegation_id"] == "item_old"
        assert waited["question"]["request"] == "Ich möchte einen Rückruf."
        await fake.remote_hangup(session_id)
        await runtime.wait_agent(handle, 10)
        await runtime.close()

    @pytest.mark.asyncio
    async def test_steer_and_transfer(self, tmp_path, fake, record):
        session_id = await accepted_call(fake)
        ctx = parent_with_call(
            tmp_path, fake, session_id, transfer_target="tel:+4960521829991"
        )
        runtime = runtime_for(ctx, ledger=StrictLedger())
        handle = await start_phone(runtime)
        await wait_attached(fake, session_id)
        steered = await runtime.message_agent(
            handle, "Fragen Sie nach dem Geburtsdatum."
        )
        assert steered["result"] == "steered"
        await fake.wait_for_command(
            session_id,
            "session.instructions.append",
            lambda c: "Geburtsdatum" in c["content"],
        )
        transferred = await runtime.message_agent(
            handle, "Ich verbinde Sie jetzt.", call_action="transfer"
        )
        assert transferred["transfer"]["result"] == "transferring"
        assert fake.session(session_id).refer_target == "tel:+4960521829991"
        await fake.remote_hangup(session_id)
        await runtime.wait_agent(handle, 10)
        await runtime.close()

    @pytest.mark.asyncio
    async def test_stop_agent_says_goodbye_and_hangs_up(
        self, tmp_path, fake, record, monkeypatch
    ):
        monkeypatch.setattr(phone_mod, "_GOODBYE_GRACE_S", 0.05)
        session_id = await accepted_call(fake)
        ctx = parent_with_call(tmp_path, fake, session_id)
        runtime = runtime_for(ctx, ledger=StrictLedger())
        handle = await start_phone(runtime)
        await wait_attached(fake, session_id)
        stopped = await runtime.stop_agent(handle, 2.0)
        assert stopped["result"] == "stopped"
        assert fake.session(session_id).status == "closed"
        goodbye = [
            c
            for c in fake.session(session_id).commands
            if c["type"] == "session.instructions.append"
        ]
        assert goodbye and "Verabschieden" in goodbye[0]["content"]
        await runtime.close()

    @pytest.mark.asyncio
    async def test_runtime_shutdown_leaves_the_call_up(self, tmp_path, fake, record):
        session_id = await accepted_call(fake)
        ctx = parent_with_call(tmp_path, fake, session_id)
        runtime = runtime_for(ctx, ledger=StrictLedger())
        await start_phone(runtime)
        await wait_attached(fake, session_id)
        await runtime.close()
        await asyncio.sleep(0.1)
        assert fake.session(session_id).status == "accepted"
        assert not fake.session(session_id).sockets

    @pytest.mark.asyncio
    async def test_phone_needs_background(self, tmp_path, fake, record):
        session_id = await accepted_call(fake)
        ctx = parent_with_call(tmp_path, fake, session_id)
        runtime = runtime_for(ctx, ledger=StrictLedger())
        answer = await runtime.run_foreground(
            call("fg", subagent_type="phone", prompt="x")
        )
        assert answer.startswith("Error:") and "run_in_background" in answer
        refused = await runtime.run_background(
            call(
                "bg",
                subagent_type="phone",
                prompt="x",
                fork=True,
                run_in_background=True,
            )
        )
        assert refused.startswith("Error:")
        await runtime.close()

    @pytest.mark.asyncio
    async def test_reply_to_is_phone_only(self, tmp_path):
        ctx, _ = make_parent(tmp_path)
        runtime = runtime_for(ctx, ledger=StrictLedger())
        result = await runtime.message_agent("explorer-0000", "x", reply_to="item_1")
        assert result["result"] in {"invalid", "not_found"}
        await runtime.close()
