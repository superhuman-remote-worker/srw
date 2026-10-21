"""Cascade profile settings, checked at config load; local_voice_gateway.md §4.3.

Needs no Pipecat, so it runs in the main suite as well as the voice venv.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

from voice_gateway.config import load_config
from voice_gateway.engines import build_engines
from voice_gateway.engines.cascade import cascade_kind, parse_settings
from voice_gateway.engines.cascade.clauses import clause_end
from voice_gateway.engines.cascade.prompt import (
    DELEGATE_DESCRIPTION,
    exact_words,
    note,
    system_prompt,
)
from voice_gateway.engines.cascade.timings import Timings
from voice_gateway.errors import ConfigError

HAS_PIPECAT = importlib.util.find_spec("pipecat") is not None


def profile(**overrides) -> dict:
    raw = {
        "stt": {"base_url": "http://127.0.0.1:18450/v1", "model": "whisper"},
        "llm": {"base_url": "http://127.0.0.1:18451/v1/", "model": "gemma"},
        "tts": {
            "base_url": "http://127.0.0.1:18450/v1",
            "model": "piper",
            "voice": "mls",
        },
    }
    raw.update(overrides)
    return raw


class TestParseSettings:
    def test_defaults_follow_the_design_note(self):
        settings = parse_settings("cascade-cpu", profile())
        assert settings.language == "de"
        assert settings.filler == "Einen Moment, ich schaue nach."
        assert settings.barge_in.min_speech_secs == 0.5
        assert settings.barge_in.min_words == 2
        assert {"ja", "mhm", "genau"} <= set(settings.barge_in.backchannels)
        assert settings.turn.smart_turn is True
        assert settings.llm.base_url == "http://127.0.0.1:18451/v1"
        assert settings.llm.api_key is None
        assert settings.tts.response_format == "pcm"
        assert settings.tts.clause_chars == 40

    def test_key_files_are_read_and_never_shown(self, tmp_path):
        key = tmp_path / "llm.key"
        key.write_text("sk-secret-value\n")
        settings = parse_settings(
            "hosted",
            profile(
                llm={
                    "base_url": "https://api.example/v1",
                    "model": "m",
                    "api_key_file": str(key),
                }
            ),
        )
        assert settings.llm.api_key == "sk-secret-value"
        assert "sk-secret" not in repr(settings)
        key.write_text("two words\n")
        with pytest.raises(ConfigError) as error:
            parse_settings(
                "hosted",
                profile(
                    llm={
                        "base_url": "https://a.example/v1",
                        "model": "m",
                        "api_key_file": str(key),
                    }
                ),
            )
        assert "two" not in str(error.value) and str(key) not in str(error.value)

    @pytest.mark.parametrize(
        ("overrides", "message"),
        [
            ({"stt": None}, "stt must be a mapping"),
            ({"tts": {"base_url": "ftp://x/v1", "model": "m"}}, "tts.base_url"),
            ({"llm": {"base_url": "http://u:p@x/v1", "model": "m"}}, "llm.base_url"),
            ({"llm": {"base_url": "http://x/v1", "model": ""}}, "llm.model"),
            (
                {
                    "llm": {
                        "base_url": "http://x/v1",
                        "model": "m",
                        "api_key_file": "rel.key",
                    }
                },
                "absolute",
            ),
            (
                {"llm": {"base_url": "http://x/v1", "model": "m", "temperature": 3}},
                "temperature",
            ),
            (
                {
                    "tts": {
                        "base_url": "http://x/v1",
                        "model": "m",
                        "response_format": "mp3",
                    }
                },
                "pcm or wav",
            ),
            (
                {
                    "tts": {
                        "base_url": "http://x/v1",
                        "model": "m",
                        "clause_chars": -1,
                    }
                },
                "clause_chars",
            ),
            ({"turn": {"vad_stop_secs": 0}}, "vad_stop_secs"),
            (
                {"turn": {"vad_model_file": "/nonexistent/silero.onnx"}},
                "vad_model_file",
            ),
            ({"barge_in": {"backchannels": "ja"}}, "backchannels"),
            ({"language": "fr"}, "language"),
            ({"colour": "blue"}, "unknown fields: colour"),
            (
                {"stt": {"base_url": "http://x/v1", "model": "m", "beam": 5}},
                "unknown fields: beam",
            ),
        ],
    )
    def test_unusable_settings_are_refused(self, overrides, message):
        with pytest.raises(ConfigError, match=message):
            parse_settings("p", profile(**overrides))

    def test_every_stage_is_required(self):
        raw = profile()
        del raw["tts"]
        with pytest.raises(ConfigError, match="needs tts"):
            parse_settings("p", raw)

    def test_the_kind_needs_pipecat(self):
        if HAS_PIPECAT:
            assert callable(cascade_kind("p", profile(), Path(".")))
        else:
            with pytest.raises(ConfigError, match="pipecat"):
                cascade_kind("p", profile(), Path("."))

    def test_profiles_reach_the_kind_through_the_config(self, tmp_path):
        (tmp_path / "api.key").write_text("gateway-key")
        (tmp_path / "hook").write_text("whsec_" + "a2V5")
        (tmp_path / "gateway.yaml").write_text(
            "api_key_files: [api.key]\norchestrator_url: http://127.0.0.1:8085\n"
            "lines: [{name: l, connector_id: c, webhook_secret_file: hook}]\n"
            "profiles:\n  cascade-cpu:\n    engine: cascade\n    colour: blue\n"
        )
        with pytest.raises(ConfigError, match="profile cascade-cpu has unknown fields"):
            load_config(tmp_path / "gateway.yaml")
        assert set(build_engines({"s": {"engine": "scripted"}})) == {"s"}

    def test_files_resolve_against_the_config_directory(self, tmp_path):
        (tmp_path / "llm.key").write_text("stage-key")
        (tmp_path / "silero.onnx").write_bytes(b"onnx")
        raw = profile(
            llm={"base_url": "http://x/v1", "model": "m", "api_key_file": "llm.key"},
            turn={"vad_model_file": "silero.onnx"},
        )
        settings = parse_settings("p", raw, tmp_path)
        assert settings.llm.api_key == "stage-key"
        assert settings.turn.vad_model_file == str(tmp_path / "silero.onnx")
        # Without a config file (the scripts) a relative file is refused.
        with pytest.raises(ConfigError, match="absolute"):
            parse_settings("p", raw)


class TestPrompt:
    def test_the_greeting_wording_is_taken_verbatim(self):
        content = 'Greet the caller now with exactly these words, then listen: "Guten Tag, hier ist „Beispielhof“."'
        assert exact_words(content) == "Guten Tag, hier ist „Beispielhof“."
        assert exact_words("Say a short, polite goodbye to the caller now.") is None
        assert exact_words("Say exactly nothing") is None

    def test_rules_come_first_and_instructions_last(self):
        prompt = system_prompt(
            "Persona\n\n## Facts\nCheck-in 15 Uhr.", "de", "Sprich langsam."
        )
        assert prompt.startswith("# How this call works")
        assert "spoken German" in prompt
        assert prompt.index("Sprich langsam.") < prompt.index("Persona")
        assert prompt.endswith("Check-in 15 Uhr.")

    def test_notes_say_what_to_do(self):
        assert "do not answer it now" in note("thinking", "x")
        assert "follow it now" in note("instructions", "x")
        assert "in your own words" in note("commentary", "x")
        assert "without naming the back office" in note("commentary", "x")

    def test_rules_keep_the_back_office_and_handovers_off_the_line(self):
        # A1: the voice named the back office, said "Ich verbinde Sie" before
        # the back office decided, and said two holding sentences in a row.
        prompt = system_prompt("Persona", "de")
        assert "Never mention the back office" in prompt
        assert "at most one short sentence" in prompt
        assert "Never add a second holding sentence" in prompt
        assert "Ich verbinde Sie" in prompt and "decided after the check" in prompt
        assert "without saying who answered" in prompt
        assert "Do not start with a holding sentence again" in prompt
        assert "never that you connect or forward" in DELEGATE_DESCRIPTION


class TestClauses:
    @pytest.mark.parametrize(
        ("text", "end"),
        [
            # A comma, semicolon, colon or dash once enough is written.
            ("Vom 12. bis 14. März ist ein Doppelzimmer frei, ", 47),
            ("Das Frühstück gibt es täglich bis 10:30 Uhr; ", 44),
            ("Für Ihren Hund gilt Folgendes: ", 30),
            ("Die Sauna ist bis 21 Uhr geöffnet – ", 35),
            # Too short, or not at a word's end yet.
            ("Gerne, ", None),
            ("Vom 12. bis 14. März ist ein Doppelzimmer frei,", None),
            # A decimal comma or a time is no clause.
            ("Der Strom kostet pro Kilowattstunde 0,45 ", None),
            ("Das Frühstück beginnt jeden Morgen um 6:30 ", None),
            # A conjunction once twice the length is written; cut before it.
            (
                "Das Doppelzimmer kostet 129 Euro pro Nacht mit Frühstück im Haus und ",
                64,
            ),
            ("Das Zimmer ist frei und ", None),
        ],
    )
    def test_where_a_first_sentence_may_start(self, text, end):
        assert clause_end(text, 30) == end

    def test_zero_turns_it_off(self):
        assert clause_end("Vom 12. bis 14. März ist ein Doppelzimmer frei, ", 0) is None


class TestTimings:
    def test_a_turn_records_every_stage(self):
        now = [100.0]
        timings = Timings(clock=lambda: now[0])
        timings.speech_ended(99.8)  # VAD reports the end 0.2 s later
        timings.stt(310.4)
        now[0] = 100.4
        timings.turn_released()
        timings.llm_started()
        now[0] = 100.7
        timings.llm_first_token()
        now[0] = 100.9
        timings.tts_requested()
        timings.tts_first_audio(80)
        now[0] = 101.0
        timings.audio_out()
        assert timings.turns[0].as_dict() == {
            "turn_wait_ms": 600,
            "stt_ms": 310,
            "llm_first_token_ms": 300,
            "llm_first_text_ms": 500,
            "tts_first_audio_ms": 80,
            "first_audio_ms": 1200,
            "delegated": False,
        }

    def test_the_greeting_and_later_pieces_do_not_count(self):
        now = [10.0]
        timings = Timings(clock=lambda: now[0])
        timings.tts_requested()  # the greeting, before any turn
        timings.speech_ended(10.0)
        timings.llm_started()
        now[0] = 10.5
        timings.tts_requested()
        now[0] = 10.9
        timings.tts_requested()  # the second sentence
        assert timings.turns[0].llm_first_text_ms == 500
