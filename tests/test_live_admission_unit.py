"""Live-session admission without a database (live_session_jobs.md §3, §4.2, §6.4).

The accept payload built from the expert's ``driver: live`` roster entry, the
request contradictions admission refuses, the ``prepare_live_session`` stage
against fakes, the INSERT-transaction bind, the handoff body and the handoff
itself, and the orchestrator → pod client (control token, error mapping).
The SQL is proven in ``test_live_sessions_postgres.py``.
"""

from __future__ import annotations

import contextlib
import json
import logging
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi import HTTPException
from pydantic import ValidationError

from orchestrator.schemas.execution_selection import (
    ExecutionSelection,
    LiveSessionSelection,
)
from orchestrator.schemas.job_create import PUBLIC_JOB_CREATE_FIELDS, JobCreate
from orchestrator.security.crypto import live_control_token
from orchestrator.services.connector_service_images import ServiceImageSettings
from orchestrator.services.connector_service_launch import endpoint_url
from orchestrator.services.live import admission, pods
from orchestrator.services.live.admission import (
    LiveAdmission,
    LiveAdmissionDependencies,
    hand_off,
    handoff_body,
    prepare_live_session,
    refuse_session_contradictions,
)
from orchestrator.services.live.instructions import (
    DEFAULT_FALLBACK,
    DEFAULT_VOICE,
    LIVE_VOICE_RULES,
    MAX_GREETING_CHARS,
    MAX_INSTRUCTION_CHARS,
    LiveVoice,
    LiveVoiceError,
    build_live_voice,
    find_live_entry,
    greeting_instruction,
    live_voice_of,
    session_instructions,
)
from orchestrator.services.live.pods import LivePodClient, LivePodError, pod_endpoint
from shared.runtime.live.contract import MAX_APPEND_CHARS, SessionStatus

CONNECTOR = "00000000-0000-4000-8000-0000000000a1"
SESSION = "00000000-0000-4000-8000-0000000000b1"
JOB = "00000000-0000-4000-8000-0000000000c1"
AUTOMATION = "00000000-0000-4000-8000-0000000000d1"
PROJECT = "00000000-0000-4000-8000-0000000000e1"
DIGEST = "sha256:" + "a" * 64
GREETING = "Hello, you are speaking with an AI assistant."


def _entry(**live: Any) -> dict[str, Any]:
    block = {"persona": "You are the desk.", "facts": "Open 9-5.", "greeting": GREETING}
    block.update(live)
    return {"driver": "live", "live": block}


def _voice(**overrides: Any) -> LiveVoice:
    values = {
        "entry": "voice",
        "instructions": "INSTRUCTIONS",
        "greeting_instruction": "GREET",
        "voice": "cedar",
        "fallback": "FALLBACK",
    }
    values.update(overrides)
    return LiveVoice(**values)


def _live_job(**overrides: Any) -> JobCreate:
    body: dict[str, Any] = {
        "description": "Answer the phone",
        "live_session": True,
        "execution": {
            "connectors": {"tel": {"ref": {"name": "line"}}},
            "session": {"connector": "tel", "id": "rtc_1"},
        },
    }
    body.update(overrides)
    return JobCreate(**body)


# =============================================================================
# The accept payload (instructions.py)
# =============================================================================


class TestSessionInstructions:
    def test_persona_facts_then_the_framework_rules_last(self):
        text = session_instructions("PERSONA", "FACTS")
        assert text == f"PERSONA\n\n## Facts\nFACTS\n\n{LIVE_VOICE_RULES}"
        assert text.index("PERSONA") < text.index("FACTS") < text.index("## Rules")

    def test_facts_are_truncated_to_the_budget_with_a_warning(self, caplog):
        persona = "P" * 100
        with caplog.at_level(
            logging.WARNING, logger="orchestrator.services.live.instructions"
        ):
            text = session_instructions(persona, "F" * MAX_INSTRUCTION_CHARS, entry="v")
        assert len(text) == MAX_INSTRUCTION_CHARS
        assert text.endswith(LIVE_VOICE_RULES)
        assert any("Truncating the facts" in r.getMessage() for r in caplog.records)

    def test_facts_within_the_budget_are_kept_whole(self, caplog):
        with caplog.at_level(logging.WARNING):
            text = session_instructions("P", "F" * 1000)
        assert "F" * 1000 in text
        assert not any("Truncating" in r.getMessage() for r in caplog.records)

    def test_a_persona_over_the_budget_is_refused(self):
        with pytest.raises(LiveVoiceError, match="instruction budget"):
            session_instructions("P" * MAX_INSTRUCTION_CHARS, "")


class TestGreeting:
    def test_exact_wording(self):
        assert greeting_instruction("Hi there.") == (
            'Greet the caller now with exactly these words, then listen: "Hi there."'
        )

    def test_a_greeting_over_the_append_budget_is_refused(self):
        with pytest.raises(LiveVoiceError, match="exact-wording budget"):
            greeting_instruction("g" * MAX_GREETING_CHARS)

    def test_the_budget_is_the_append_budget(self):
        assert MAX_GREETING_CHARS == MAX_APPEND_CHARS


class TestBuildLiveVoice:
    def test_inline_texts_and_defaults(self):
        voice = build_live_voice("voice", _entry())
        assert voice.entry == "voice"
        assert voice.voice == DEFAULT_VOICE == "marin"
        assert voice.fallback == DEFAULT_FALLBACK
        assert voice.instructions == session_instructions(
            "You are the desk.", "Open 9-5."
        )
        assert voice.greeting_instruction == greeting_instruction(GREETING)

    def test_greeting_and_texts_are_stripped(self):
        voice = build_live_voice(
            "voice", _entry(greeting=f"  {GREETING}  ", persona="  P  ", facts=" F ")
        )
        assert voice.greeting_instruction == greeting_instruction(GREETING)
        assert voice.instructions.startswith("P\n\n## Facts\nF\n\n")

    def test_voice_and_fallback_from_the_entry(self):
        voice = build_live_voice(
            "voice", _entry(voice="cedar", texts={"fallback": " Please hold. "})
        )
        assert voice.voice == "cedar"
        assert voice.fallback == "Please hold."

    def test_the_fallback_is_clipped_to_the_append_budget(self):
        voice = build_live_voice(
            "voice", _entry(texts={"fallback": "x" * (MAX_APPEND_CHARS + 50)})
        )
        assert len(voice.fallback) == MAX_APPEND_CHARS

    def test_missing_persona_and_facts_are_empty(self):
        voice = build_live_voice("voice", {"live": {"greeting": GREETING}})
        assert voice.instructions.startswith("\n\n## Facts\n\n\n")

    @pytest.mark.parametrize(
        ("entry", "message"),
        [
            ({"driver": "live"}, "has no live block"),
            ({"driver": "live", "live": "text"}, "has no live block"),
            ({"live": {"persona": "p"}}, "has no greeting"),
            ({"live": {"greeting": "   "}}, "has no greeting"),
            ({"live": {"greeting": 5}}, "has no greeting"),
            ({"live": {"greeting": "g", "voice": 7}}, "voice name"),
            ({"live": {"greeting": "g", "texts": {"fallback": 7}}}, "fallback is text"),
            ({"live": {"greeting": "g", "persona": 7}}, "live.persona is text"),
        ],
    )
    def test_an_unusable_entry_is_refused(self, entry, message):
        with pytest.raises(LiveVoiceError, match=message):
            build_live_voice("voice", entry)


class TestEntryFiles:
    def test_persona_and_facts_files_beside_the_expert(self, tmp_path):
        (tmp_path / "persona.md").write_text("  File persona \n", encoding="utf-8")
        (tmp_path / "facts.md").write_text("File facts", encoding="utf-8")
        entry = {
            "_deployment_dir": str(tmp_path),
            "live": {
                "persona_file": "persona.md",
                "facts_file": "facts.md",
                "greeting": GREETING,
            },
        }
        voice = build_live_voice("voice", entry)
        assert voice.instructions.startswith("File persona\n\n## Facts\nFile facts\n\n")

    def test_a_file_in_a_subdirectory_is_allowed(self, tmp_path):
        (tmp_path / "texts").mkdir()
        (tmp_path / "texts" / "persona.md").write_text("Nested", encoding="utf-8")
        entry = {
            "_deployment_dir": str(tmp_path),
            "live": {"persona_file": "texts/persona.md", "greeting": GREETING},
        }
        assert build_live_voice("voice", entry).instructions.startswith("Nested\n\n")

    @pytest.mark.parametrize("name", ["../secret.md", "texts/../../secret.md"])
    def test_path_traversal_is_refused(self, tmp_path, name):
        expert = tmp_path / "expert"
        expert.mkdir()
        (tmp_path / "secret.md").write_text("secret", encoding="utf-8")
        entry = {
            "_deployment_dir": str(expert),
            "live": {"persona_file": name, "greeting": GREETING},
        }
        with pytest.raises(LiveVoiceError, match="file name in the expert's directory"):
            build_live_voice("voice", entry)

    def test_an_absolute_path_is_refused(self, tmp_path):
        (tmp_path / "persona.md").write_text("p", encoding="utf-8")
        entry = {
            "_deployment_dir": str(tmp_path),
            "live": {
                "persona_file": str(tmp_path / "persona.md"),
                "greeting": GREETING,
            },
        }
        with pytest.raises(LiveVoiceError, match="file name in the expert's directory"):
            build_live_voice("voice", entry)

    def test_a_symlink_out_of_the_directory_is_refused(self, tmp_path):
        expert = tmp_path / "expert"
        expert.mkdir()
        outside = tmp_path / "outside.md"
        outside.write_text("outside", encoding="utf-8")
        (expert / "persona.md").symlink_to(outside)
        entry = {
            "_deployment_dir": str(expert),
            "live": {"persona_file": "persona.md", "greeting": GREETING},
        }
        with pytest.raises(LiveVoiceError, match="names no file"):
            build_live_voice("voice", entry)

    def test_a_missing_file_is_refused(self, tmp_path):
        entry = {
            "_deployment_dir": str(tmp_path),
            "live": {"facts_file": "absent.md", "greeting": GREETING},
        }
        with pytest.raises(LiveVoiceError, match="live.facts_file names no file"):
            build_live_voice("voice", entry)

    def test_a_directory_is_no_file(self, tmp_path):
        (tmp_path / "persona.md").mkdir()
        entry = {
            "_deployment_dir": str(tmp_path),
            "live": {"persona_file": "persona.md", "greeting": GREETING},
        }
        with pytest.raises(LiveVoiceError, match="names no file"):
            build_live_voice("voice", entry)

    def test_an_entry_without_a_directory_cannot_name_a_file(self):
        entry = {"live": {"persona_file": "persona.md", "greeting": GREETING}}
        with pytest.raises(LiveVoiceError, match="no directory for"):
            build_live_voice("voice", entry)

    def test_text_and_file_are_exclusive(self, tmp_path):
        (tmp_path / "persona.md").write_text("p", encoding="utf-8")
        entry = {
            "_deployment_dir": str(tmp_path),
            "live": {
                "persona": "inline",
                "persona_file": "persona.md",
                "greeting": GREETING,
            },
        }
        with pytest.raises(LiveVoiceError, match="exclusive"):
            build_live_voice("voice", entry)


class TestFindLiveEntry:
    def test_the_roster_default_wins_when_it_is_live(self):
        agent = {
            "subagents": {
                "default": "second",
                "roster": {
                    "first": {"driver": "live", "n": 1},
                    "second": {"driver": "live", "n": 2},
                },
            }
        }
        assert find_live_entry(agent) == ("second", {"driver": "live", "n": 2})

    def test_else_the_first_live_entry(self):
        agent = {
            "subagents": {
                "default": "helper",
                "roster": {
                    "helper": {"expert": "scholar"},
                    "voice": {"driver": "live", "n": 1},
                    "other": {"driver": "live", "n": 2},
                },
            }
        }
        assert find_live_entry(agent) == ("voice", {"driver": "live", "n": 1})

    @pytest.mark.parametrize(
        "agent",
        [
            {},
            {"subagents": None},
            {"subagents": {"roster": None}},
            {"subagents": {"roster": ["voice"]}},
            {"subagents": {"roster": {"helper": {"expert": "scholar"}}}},
            {"subagents": {"roster": {"voice": "live"}}},
        ],
    )
    def test_no_live_entry(self, agent):
        assert find_live_entry(agent) is None

    def test_live_voice_of_refuses_an_expert_without_a_voice(self):
        with pytest.raises(LiveVoiceError, match="no live voice"):
            live_voice_of({"subagents": {"roster": {"h": {"expert": "scholar"}}}})

    def test_live_voice_of_builds_the_found_entry(self):
        agent = {"subagents": {"roster": {"desk": _entry(voice="ash")}}}
        voice = live_voice_of(agent)
        assert voice.entry == "desk" and voice.voice == "ash"


# =============================================================================
# The request contract (schemas + refuse_session_contradictions)
# =============================================================================


class TestRequestSchemas:
    def test_live_session_defaults_to_false(self):
        assert JobCreate(description="x").live_session is False

    def test_live_session_is_a_public_create_field(self):
        assert "live_session" in PUBLIC_JOB_CREATE_FIELDS

    def test_a_session_parses(self):
        job = _live_job()
        assert job.execution.session == LiveSessionSelection(
            connector="tel", id="rtc_1"
        )

    def test_session_extra_keys_are_forbidden(self):
        with pytest.raises(ValidationError):
            ExecutionSelection(
                connectors={}, session={"connector": "tel", "id": "rtc", "x": 1}
            )

    @pytest.mark.parametrize(
        "session",
        [
            {"connector": "tel", "id": ""},
            {"connector": "tel"},
            {"id": "rtc"},
            {"connector": "tel", "id": "r" * 201},
        ],
    )
    def test_an_incomplete_session_is_refused(self, session):
        with pytest.raises(ValidationError):
            ExecutionSelection(connectors={}, session=session)


class TestRefuseSessionContradictions:
    def test_a_plain_job_is_no_live_job(self):
        assert refuse_session_contradictions(JobCreate(description="x")) is None
        plain = JobCreate(
            description="x", execution={"connectors": {"a": {"ref": {"name": "a"}}}}
        )
        assert refuse_session_contradictions(plain) is None

    def test_a_live_job_names_its_alias_and_session(self):
        assert refuse_session_contradictions(_live_job()) == ("tel", "rtc_1")

    def test_live_without_a_session(self):
        command = _live_job(execution={"connectors": {"tel": {"ref": {"name": "l"}}}})
        with pytest.raises(HTTPException) as error:
            refuse_session_contradictions(command)
        assert error.value.status_code == 422
        assert "names its session" in error.value.detail

    def test_live_without_any_execution(self):
        command = JobCreate(description="x", live_session=True)
        with pytest.raises(HTTPException) as error:
            refuse_session_contradictions(command)
        assert error.value.status_code == 422

    def test_a_session_without_live_session(self):
        command = _live_job(live_session=False)
        with pytest.raises(HTTPException) as error:
            refuse_session_contradictions(command)
        assert error.value.status_code == 422
        assert "set live_session: true" in error.value.detail

    def test_a_session_connector_that_is_no_alias(self):
        command = _live_job(
            execution={
                "connectors": {"tel": {"ref": {"name": "line"}}},
                "session": {"connector": "phone", "id": "rtc_1"},
            }
        )
        with pytest.raises(HTTPException) as error:
            refuse_session_contradictions(command)
        assert error.value.status_code == 422
        assert "'phone' is not a key of execution.connectors" in error.value.detail


# =============================================================================
# prepare_live_session and the INSERT-transaction bind
# =============================================================================


class _Conn:
    def __init__(self, rows: list[dict | None]):
        self.fetchrow = AsyncMock(side_effect=rows)
        self.fetchval = AsyncMock()


class _Db:
    def __init__(self, *, connector: dict | None, rows: list[dict | None]):
        self.conn = _Conn(rows)
        self.get_datasource = AsyncMock(return_value=connector)

    @contextlib.asynccontextmanager
    async def acquire(self):
        yield self.conn


def _connector(ds_type: str = "gpt_live_sip", **config: Any) -> dict[str, Any]:
    return {
        "id": UUID(CONNECTOR),
        "type": ds_type,
        "config": {"model": "gpt-live-1", **config},
    }


def _session_row(**overrides: Any) -> dict[str, Any]:
    row = {
        "id": UUID(SESSION),
        "connector_id": UUID(CONNECTOR),
        "provider_session_id": "rtc_1",
        "status": "ringing",
        "job_id": None,
    }
    row.update(overrides)
    return row


@pytest.fixture
def voice_resolved(monkeypatch):
    resolve = AsyncMock(return_value=_voice())
    monkeypatch.setattr(admission, "resolve_live_voice", resolve)
    return resolve


async def _prepare(
    db, *, command=None, ids=(CONNECTOR,), datasource_ids=(CONNECTOR,), origin="api"
):
    resolve_refs = AsyncMock(return_value=list(ids))
    dependencies = LiveAdmissionDependencies(
        db=db,
        resolve_connector_refs=resolve_refs,
        pods=MagicMock(),
        session_origin=origin,
    )
    result = await prepare_live_session(
        command=command or _live_job(),
        datasource_ids=list(datasource_ids),
        effective_user_id="user-1",
        project_id=PROJECT,
        config_name="phone-desk",
        expert_id=None,
        expert_row=None,
        config_override={"autonomy": "full"},
        dependencies=dependencies,
    )
    return result, resolve_refs


class TestPrepareLiveSession:
    @pytest.mark.asyncio
    async def test_a_plain_job_needs_nothing(self, voice_resolved):
        db = _Db(connector=None, rows=[])
        result, resolve_refs = await _prepare(db, command=JobCreate(description="x"))
        assert result is None
        resolve_refs.assert_not_awaited()
        db.get_datasource.assert_not_awaited()
        voice_resolved.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_new_session_is_ensured_and_admitted(self, voice_resolved):
        db = _Db(connector=_connector(), rows=[_session_row()])
        result, resolve_refs = await _prepare(db, origin="manifest")
        assert isinstance(result, LiveAdmission)
        assert result.context_value == {
            "session_id": SESSION,
            "connector_id": CONNECTOR,
            "provider_session_id": "rtc_1",
        }
        assert result.voice == _voice()
        # Only the session's alias is resolved, in the caller's scope.
        selection = resolve_refs.await_args.args[0]
        assert list(selection.connectors) == ["tel"]
        assert resolve_refs.await_args.kwargs == {
            "owner_id": "user-1",
            "project_id": PROJECT,
        }
        # The row is created with the dependencies' origin.
        insert = db.conn.fetchrow.await_args_list[0].args
        assert "INSERT INTO live_sessions" in insert[0]
        assert insert[1:] == (UUID(CONNECTOR), "rtc_1", "manifest")
        assert voice_resolved.await_args.kwargs["config_name"] == "phone-desk"
        assert voice_resolved.await_args.kwargs["config_override"] == {
            "autonomy": "full"
        }

    @pytest.mark.asyncio
    async def test_an_existing_ringing_session_is_reused(self, voice_resolved):
        db = _Db(connector=_connector(), rows=[None, _session_row(status="active")])
        result, _ = await _prepare(db)
        assert result.session["status"] == "active"
        assert db.conn.fetchrow.await_count == 2

    @pytest.mark.asyncio
    async def test_a_connector_outside_the_jobs_connectors_is_403(self, voice_resolved):
        db = _Db(connector=_connector(), rows=[_session_row()])
        with pytest.raises(HTTPException) as error:
            await _prepare(db, datasource_ids=[str(uuid4())])
        assert error.value.status_code == 403
        db.conn.fetchrow.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_an_unresolved_alias_is_403(self, voice_resolved):
        db = _Db(connector=_connector(), rows=[])
        with pytest.raises(HTTPException) as error:
            await _prepare(db, ids=())
        assert error.value.status_code == 403

    @pytest.mark.asyncio
    @pytest.mark.parametrize("ds_type", ["postgresql", "generic", None])
    async def test_a_connector_of_no_live_driver_is_422(self, voice_resolved, ds_type):
        db = _Db(connector=_connector(ds_type), rows=[])
        with pytest.raises(HTTPException) as error:
            await _prepare(db)
        assert error.value.status_code == 422
        assert "not a live connector" in error.value.detail
        voice_resolved.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_vanished_connector_is_422(self, voice_resolved):
        db = _Db(connector=None, rows=[])
        with pytest.raises(HTTPException) as error:
            await _prepare(db)
        assert error.value.status_code == 422

    @pytest.mark.asyncio
    async def test_an_expert_without_a_live_voice_is_422(self, monkeypatch):
        monkeypatch.setattr(
            admission,
            "resolve_live_voice",
            AsyncMock(side_effect=LiveVoiceError("This expert has no live voice")),
        )
        db = _Db(connector=_connector(), rows=[_session_row()])
        with pytest.raises(HTTPException) as error:
            await _prepare(db)
        assert error.value.status_code == 422
        assert error.value.detail == "This expert has no live voice"
        db.conn.fetchrow.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_an_already_bound_session_is_409(self, voice_resolved):
        db = _Db(connector=_connector(), rows=[None, _session_row(job_id=uuid4())])
        with pytest.raises(HTTPException) as error:
            await _prepare(db)
        assert error.value.status_code == 409
        assert error.value.detail["code"] == "live_session_bound"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", ["ended", "rejected", "failed"])
    async def test_a_session_that_is_over_is_409(self, voice_resolved, status):
        db = _Db(connector=_connector(), rows=[None, _session_row(status=status)])
        with pytest.raises(HTTPException) as error:
            await _prepare(db)
        assert error.value.status_code == 409
        assert error.value.detail["code"] == "live_session_over"
        assert status in error.value.detail["message"]


class TestOnCreated:
    def _admission(self, bound: bool) -> LiveAdmission:
        sessions = SimpleNamespace(bind_job=AsyncMock(return_value=bound))
        return LiveAdmission(
            session=_session_row(),
            connector=_connector(),
            voice=_voice(),
            sessions=sessions,
        )

    @pytest.mark.asyncio
    async def test_binds_on_the_jobs_connection(self):
        live = self._admission(True)
        conn = object()
        await live.on_created(conn, JOB)
        live.sessions.bind_job.assert_awaited_once_with(conn, UUID(SESSION), JOB)

    @pytest.mark.asyncio
    async def test_a_session_bound_meanwhile_is_409(self):
        with pytest.raises(HTTPException) as error:
            await self._admission(False).on_created(object(), JOB)
        assert error.value.status_code == 409
        assert error.value.detail["code"] == "live_session_bound"


# =============================================================================
# The handoff
# =============================================================================


def _admission_for(
    connector: dict | None = None, sessions: Any = None
) -> LiveAdmission:
    return LiveAdmission(
        session=_session_row(),
        connector=connector or _connector(),
        voice=_voice(),
        sessions=sessions or MagicMock(),
    )


class TestHandoffBody:
    def test_defaults(self):
        body = handoff_body(_admission_for(), JOB)
        assert body == {
            "session_id": SESSION,
            "provider_session_id": "rtc_1",
            "job_id": JOB,
            "model": "gpt-live-1",
            "accept": {"instructions": "INSTRUCTIONS", "voice": "cedar"},
            "greeting_instruction": "GREET",
            "texts": {"fallback": "FALLBACK"},
            "limits": {"max_duration_s": 900, "no_job_fallback_s": 8},
            "transfer_target": None,
        }
        json.dumps(body)  # the pod's wire body, and the adoption record

    def test_the_connectors_limits_and_transfer_target(self):
        connector = _connector(
            max_duration_s=120, no_job_fallback_s=3, transfer_target="sip:desk@pbx"
        )
        body = handoff_body(_admission_for(connector), JOB)
        assert body["limits"] == {"max_duration_s": 120, "no_job_fallback_s": 3}
        assert body["transfer_target"] == "sip:desk@pbx"

    def test_a_connector_without_config(self):
        connector = {"id": UUID(CONNECTOR), "type": "gpt_live_sip", "config": None}
        body = handoff_body(_admission_for(connector), JOB)
        assert body["model"] == ""
        assert body["limits"] == {"max_duration_s": 900, "no_job_fallback_s": 8}


def _sessions(transition_result=(True, {"status": "active"})) -> SimpleNamespace:
    return SimpleNamespace(
        note_admission=AsyncMock(),
        transition=AsyncMock(return_value=transition_result),
    )


class TestHandOff:
    @pytest.mark.asyncio
    async def test_a_taken_session_turns_active(self):
        sessions = _sessions()
        live = _admission_for(sessions=sessions)
        pod_client = SimpleNamespace(
            handoff=AsyncMock(return_value={"status": "active"})
        )
        job = {
            "id": UUID(JOB),
            "project_id": UUID(PROJECT),
            "context": {"automation_id": AUTOMATION},
        }
        row = await hand_off(live, job, pod_client)
        assert row == {"status": "active"}
        body = handoff_body(live, JOB)
        pod_client.handoff.assert_awaited_once_with(CONNECTOR, body)
        sessions.note_admission.assert_awaited_once_with(
            SESSION,
            automation_id=AUTOMATION,
            project_id=UUID(PROJECT),
            model="gpt-live-1",
            handoff=body,
        )
        sessions.transition.assert_awaited_once_with(SESSION, SessionStatus.ACTIVE)

    @pytest.mark.asyncio
    async def test_the_automation_of_a_stored_job_row(self):
        """A job row read back with ``PostgresDB.get_job`` carries its jsonb
        context as text; the automation id must survive that."""
        sessions = _sessions()
        job = {
            "id": JOB,
            "project_id": None,
            "context": json.dumps({"automation_id": AUTOMATION}),
        }
        await hand_off(
            _admission_for(sessions=sessions), job, SimpleNamespace(handoff=AsyncMock())
        )
        assert sessions.note_admission.await_args.kwargs["automation_id"] == AUTOMATION

    @pytest.mark.asyncio
    async def test_an_explicit_automation_wins(self):
        sessions = _sessions()
        job = {"id": JOB, "context": {"automation_id": "other"}}
        await hand_off(
            _admission_for(sessions=sessions),
            job,
            SimpleNamespace(handoff=AsyncMock()),
            automation_id=AUTOMATION,
        )
        assert sessions.note_admission.await_args.kwargs["automation_id"] == AUTOMATION

    @pytest.mark.asyncio
    async def test_a_job_without_context_or_model(self):
        sessions = _sessions()
        connector = {"id": UUID(CONNECTOR), "type": "gpt_live_sip", "config": {}}
        await hand_off(
            _admission_for(connector, sessions),
            {"id": JOB, "context": None},
            SimpleNamespace(handoff=AsyncMock()),
        )
        kwargs = sessions.note_admission.await_args.kwargs
        assert kwargs["automation_id"] is None and kwargs["model"] is None

    @pytest.mark.asyncio
    async def test_a_failed_handoff_fails_the_session_and_never_raises(self):
        sessions = _sessions((True, {"status": "failed", "error": "x"}))
        pod_client = SimpleNamespace(
            handoff=AsyncMock(side_effect=LivePodError("pod busy", status=502))
        )
        row = await hand_off(_admission_for(sessions=sessions), {"id": JOB}, pod_client)
        assert row["status"] == "failed"
        sessions.transition.assert_awaited_once_with(
            SESSION, SessionStatus.FAILED, error="handoff failed: pod busy"
        )
        sessions.note_admission.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_any_other_failure_also_fails_the_session(self):
        """The job is created and bound by now: hand_off never raises."""
        sessions = _sessions((True, {"id": SESSION, "status": "failed"}))
        live = _admission_for(sessions=sessions)
        pods_ = SimpleNamespace(
            handoff=AsyncMock(side_effect=ValueError("invalid namespace"))
        )
        row = await hand_off(live, {"id": JOB}, pods_)
        assert row == {"id": SESSION, "status": "failed"}
        sessions.transition.assert_awaited_once_with(
            SESSION, SessionStatus.FAILED, error="handoff failed: ValueError"
        )

    @pytest.mark.asyncio
    async def test_an_unapplied_transition_returns_the_admitted_session(self):
        sessions = _sessions((False, None))
        live = _admission_for(sessions=sessions)
        row = await hand_off(live, {"id": JOB}, SimpleNamespace(handoff=AsyncMock()))
        assert row is live.session


# =============================================================================
# The pod client
# =============================================================================


class _Recorder:
    def __init__(self, response: httpx.Response | Exception):
        self.response = response
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if isinstance(self.response, Exception):
            raise self.response
        return self.response


@pytest.fixture
def pod_url(monkeypatch):
    endpoint = AsyncMock(return_value="http://pod.srw.svc:8080")
    monkeypatch.setattr(pods, "pod_endpoint", endpoint)
    return endpoint


def _client(response) -> tuple[LivePodClient, _Recorder]:
    recorder = _Recorder(response)
    return LivePodClient(object(), transport=httpx.MockTransport(recorder)), recorder


class TestLivePodClient:
    @pytest.mark.asyncio
    async def test_the_handoff_carries_the_control_token(self, pod_url):
        client, recorder = _client(httpx.Response(200, json={"status": "active"}))
        answer = await client.handoff(CONNECTOR, {"session_id": SESSION})
        assert answer == {"status": "active"}
        (request,) = recorder.requests
        assert request.method == "POST"
        assert str(request.url) == "http://pod.srw.svc:8080/v1/sessions"
        assert request.headers["authorization"] == (
            f"Bearer {live_control_token(CONNECTOR)}"
        )
        assert json.loads(request.content) == {"session_id": SESSION}
        pod_url.assert_awaited_once()

    def test_the_control_token_is_per_connector_and_stable(self):
        token = live_control_token(CONNECTOR)
        assert token == live_control_token(CONNECTOR)
        assert token != live_control_token(str(uuid4()))
        assert "=" not in token and len(token) == 43

    @pytest.mark.asyncio
    async def test_a_refusal_maps_its_error(self, pod_url):
        client, _ = _client(
            httpx.Response(
                502, json={"error": {"status": 486, "code": "busy", "message": "m"}}
            )
        )
        with pytest.raises(LivePodError) as error:
            await client.handoff(CONNECTOR, {})
        assert (error.value.status, error.value.code, str(error.value)) == (
            486,
            "busy",
            "m",
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "error",
        [
            {"status": "busy", "code": 486, "message": "m"},
            {"status": True, "code": ["x"], "message": "m"},
        ],
    )
    async def test_a_malformed_refusal_is_still_a_pod_error(self, pod_url, error):
        client, _ = _client(httpx.Response(502, json={"error": error}))
        with pytest.raises(LivePodError) as raised:
            await client.handoff(CONNECTOR, {})
        assert (raised.value.status, raised.value.code) == (502, None)

    @pytest.mark.asyncio
    async def test_a_refusal_without_a_body(self, pod_url):
        client, _ = _client(httpx.Response(503, content=b"not json"))
        with pytest.raises(LivePodError) as error:
            await client.handoff(CONNECTOR, {})
        assert (error.value.status, error.value.code, str(error.value)) == (
            503,
            None,
            "HTTP 503",
        )

    @pytest.mark.asyncio
    async def test_a_non_object_answer_is_empty(self, pod_url):
        client, _ = _client(httpx.Response(200, json=["x"]))
        assert await client.handoff(CONNECTOR, {}) == {}

    @pytest.mark.asyncio
    async def test_a_transport_error(self, pod_url):
        client, _ = _client(httpx.ConnectError("refused"))
        with pytest.raises(LivePodError, match="did not answer \\(ConnectError\\)"):
            await client.handoff(CONNECTOR, {})

    @pytest.mark.asyncio
    async def test_a_timeout(self, pod_url):
        client, _ = _client(httpx.ReadTimeout("slow"))
        with pytest.raises(LivePodError) as error:
            await client.handoff(CONNECTOR, {})
        assert error.value.status == 0

    @pytest.mark.asyncio
    async def test_reject_posts_the_status_code(self, pod_url):
        client, recorder = _client(httpx.Response(200, json={}))
        await client.reject(CONNECTOR, SESSION, "rtc_1", 480)
        (request,) = recorder.requests
        assert request.url.path == f"/v1/sessions/{SESSION}/reject"
        assert json.loads(request.content) == {
            "provider_session_id": "rtc_1",
            "status_code": 480,
        }
        assert request.headers["authorization"] == (
            f"Bearer {live_control_token(CONNECTOR)}"
        )

    @pytest.mark.asyncio
    async def test_no_ready_pod(self, monkeypatch):
        monkeypatch.setattr(pods, "pod_endpoint", AsyncMock(return_value=None))
        client, recorder = _client(httpx.Response(200, json={}))
        with pytest.raises(LivePodError, match="not ready"):
            await client.target(CONNECTOR)
        with pytest.raises(LivePodError, match="not ready"):
            await client.reject(CONNECTOR, SESSION, "rtc_1", 480)
        assert recorder.requests == []


class _DigestDb:
    def __init__(self, digest: str | None):
        self.fetchval = AsyncMock(return_value=digest)
        self.acquired = 0

    @contextlib.asynccontextmanager
    async def acquire(self):
        self.acquired += 1
        yield self


class TestPodEndpoint:
    @pytest.fixture
    def namespace(self, monkeypatch):
        def set_namespace(value: str) -> None:
            monkeypatch.setattr(
                "orchestrator.services.connector_service_images.service_image_settings",
                lambda: ServiceImageSettings(service_namespace=value),
            )

        return set_namespace

    @pytest.mark.asyncio
    async def test_the_ready_pods_endpoint(self, namespace):
        namespace("srw")
        db = _DigestDb(DIGEST)
        url = await pod_endpoint(db, CONNECTOR)
        assert url == endpoint_url(
            namespace="srw", connector_id=CONNECTOR, digest=DIGEST, port=8080
        )
        assert db.fetchval.await_args.args[1] == UUID(CONNECTOR)
        assert "ready_at IS NOT NULL" in db.fetchval.await_args.args[0]

    @pytest.mark.asyncio
    async def test_hosting_off(self, namespace):
        namespace("")
        db = _DigestDb(DIGEST)
        assert await pod_endpoint(db, CONNECTOR) is None
        assert db.acquired == 0

    @pytest.mark.asyncio
    async def test_no_ready_pod(self, namespace):
        namespace("srw")
        assert await pod_endpoint(_DigestDb(None), CONNECTOR) is None
