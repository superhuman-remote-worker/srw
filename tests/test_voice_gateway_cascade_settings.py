"""Cascade profile settings, checked at config load; local_voice_gateway.md §4.3.

Needs no Pipecat, so it runs in the main suite as well as the voice venv.
"""

from __future__ import annotations

import importlib.util

import pytest

from voice_gateway.config import load_config
from voice_gateway.engines import build_engines
from voice_gateway.engines.cascade import cascade_kind, parse_settings
from voice_gateway.engines.cascade.prompt import exact_words, note, system_prompt
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
            assert callable(cascade_kind("p", profile()))
        else:
            with pytest.raises(ConfigError, match="pipecat"):
                cascade_kind("p", profile())

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
