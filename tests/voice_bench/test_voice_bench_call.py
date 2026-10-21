"""Unit tests for the bench's scripted call and addon-only mode (no Docker needed)."""

from __future__ import annotations

import array
import json
from pathlib import Path

import pytest

from tests.voice_bench import call, render_audio, srw
from tests.voice_bench.analysis import SipMessage
from tests.voice_bench.profiles import CallData, Profile
from tests.voice_bench.settings import Network

BENCH = Path(__file__).resolve().parent


def _script(**over) -> dict:
    raw = {
        "number": "107",
        "hangup_after": 60,
        "lines": [
            {"at": 5, "utterance": "a02-checkout"},
            {"at": 20, "text": "Verbinden Sie mich bitte mit der Rezeption."},
        ],
        "expect": [
            {"name": "greeting", "to": 5, "keywords": ["beispielhof"]},
            {"name": "human", "from": "human", "keywords": ["mitarbeiter"]},
        ],
    }
    raw.update(over)
    return raw


class TestScript:
    def test_a1_script_parses(self):
        script = call.load_script(BENCH / "calls" / "a1-de.yaml")
        assert script.number == "107"
        assert [line.utterance for line in script.lines][:2] == [
            "a02-checkout",
            "d03-invoice-copy",
        ]
        assert script.lines[2].text and script.lines[2].speaker == 10
        assert {e.name: e.start for e in script.expect}["human"] == "human"
        utterances = {u["id"] for u in render_audio.load_utterances()}
        assert {line.utterance for line in script.lines if line.utterance} <= utterances

    def test_defaults(self):
        script = call.parse_script(_script())
        assert script.lines[1].speaker == call.DEFAULT_SPEAKER
        assert script.expect[0].start == 0.0 and script.expect[0].end == 5.0
        assert script.expect[1].end is None

    @pytest.mark.parametrize(
        "over, message",
        [
            ({"number": "1x7"}, "number"),
            ({"hangup_after": "soon"}, "hangup_after"),
            ({"lines": []}, "needs lines"),
            (
                {"lines": [{"at": 9, "utterance": "a"}, {"at": 3, "utterance": "b"}]},
                "time order",
            ),
            ({"lines": [{"at": 60, "utterance": "a"}]}, "before hangup_after"),
            ({"lines": [{"at": 1, "utterance": "a", "text": "b"}]}, "either"),
            ({"lines": [{"at": 1, "utterance": "a", "voice": 3}]}, "takes"),
            ({"expect": [{"name": "x", "keywords": []}]}, "keywords"),
            ({"expect": [{"name": "x", "from": "later", "keywords": ["a"]}]}, "from"),
            ({"extra": 1}, "unknown"),
        ],
    )
    def test_rejects(self, over, message):
        with pytest.raises(call.ScriptError, match=message):
            call.parse_script(_script(**over))

    def test_text_lines_render_under_call(self):
        script = call.parse_script(_script())
        jobs = call.render_jobs(script)
        assert len(jobs) == 1 and jobs[0]["name"].startswith("call/")
        assert jobs[0] == render_audio.text_job(
            "Verbinden Sie mich bitte mit der Rezeption.", call.DEFAULT_SPEAKER
        )
        # Same words, other speaker: another file.
        assert (
            render_audio.text_job("x", 1)["name"]
            != render_audio.text_job("x", 2)["name"]
        )
        assert call.line_audio(script.lines[0]).name == "a02-checkout.16k.wav"
        assert call.line_audio(script.lines[1]).name == jobs[0]["name"][5:] + ".16k.wav"


class TestComposeAudio:
    def test_places_clips_in_silence(self):
        clip = array.array("h", [1000] * 4)
        track = call.compose_audio([(0.5, clip), (1.0, clip)], 2.0, rate=8)
        assert len(track) == 16
        assert list(track[4:8]) == [1000] * 4 and list(track[8:12]) == [1000] * 4
        assert not any(track[:4]) and not any(track[12:])

    def test_refuses_overlap_and_overrun(self):
        clip = array.array("h", [1] * 8)
        with pytest.raises(call.ScriptError, match="before the previous"):
            call.compose_audio([(0.0, clip), (0.5, clip)], 4.0, rate=8)
        with pytest.raises(call.ScriptError, match="past the end"):
            call.compose_audio([(1.5, clip)], 2.0, rate=8)


def _data(caller_t0=100.0, human_t=None, msgs=()) -> CallData:
    caller = [{"t": caller_t0, "type": "call_state", "state": "CONFIRMED"}]
    human = []
    if human_t is not None:
        human = [{"t": human_t, "type": "call_state", "state": "CONFIRMED"}]
    return CallData(
        profile=Profile(name="call", number="107", imitates="scripted call"),
        out=Path("."),
        standin=False,
        events={"caller": caller, "human": human},
        summary={},
        dut_msgs=list(msgs),
        keywords={},
        heard_from_dut=[],
        dut_mode="external",
    )


class TestWindowsAndTransfer:
    def test_windows_anchor_on_answer_and_human(self):
        script = call.parse_script(_script())
        placed = call.windows(script, _data(human_t=130.0))
        assert placed["heard"].start is None and placed["heard"].end is None
        assert (placed["greeting"].start, placed["greeting"].end) == (100.0, 105.0)
        assert placed["human"].start == 130.5 and placed["human"].end is None
        # No human answered: no window, and the check fails.
        unanswered = _data()
        unanswered.windows = call.windows(script, unanswered)
        assert "human" not in unanswered.windows
        found = {c.name: c for c in call.checks(script, unanswered, {"refer_to": None})}
        assert found["heard_human"].ok is False

    def test_timeline_from_whisper_offsets(self, tmp_path):
        out = tmp_path / "heard.wav.json"
        out.write_text(
            json.dumps(
                {
                    "transcription": [
                        {"offsets": {"from": 2500, "to": 6100}, "text": " Guten Tag."},
                        {"offsets": {"from": 7000, "to": 7100}, "text": " "},
                    ]
                }
            )
        )
        assert call.timeline(out, 0.0) == [
            {"from_s": 2.5, "to_s": 6.1, "text": "Guten Tag."}
        ]
        assert call.timeline(tmp_path / "missing.json", 0.0) == []

    def test_transfer_view_and_check(self):
        def msg(t, direction, start, headers, body=""):
            return SipMessage(t, direction, start, headers, body)

        common = [("Call-ID", "abc"), ("To", "<sip:1000@pbx>;tag=x")]
        msgs = [
            msg(
                120.0,
                "tx",
                "REFER sip:1000@pbx SIP/2.0",
                [*common, ("CSeq", "5 REFER"), ("Refer-To", "<sip:3000@172.30.89.10>")],
            ),
            msg(120.1, "rx", "SIP/2.0 202 Accepted", [*common, ("CSeq", "5 REFER")]),
            msg(
                121.0,
                "rx",
                "NOTIFY sip:2000@addon SIP/2.0",
                [*common, ("CSeq", "6 NOTIFY")],
                "SIP/2.0 200 OK\n",
            ),
        ]
        data = _data(human_t=121.5, msgs=msgs)
        view = call.transfer_view(data)
        assert view == {
            "refer_to": "<sip:3000@172.30.89.10>",
            "refer_at_s": 20.0,
            "refer_response": 202,
            "notify_sipfrag": ["SIP/2.0 200 OK"],
            "human_answered_at_s": 21.5,
        }
        script = call.parse_script(_script(expect=[]))
        found = {c.name: c for c in call.checks(script, data, view)}
        assert found["transfer_accepted"].ok is True


class TestAddonOnly:
    def test_prepare_addon_points_at_the_host_gateway(self, tmp_path, monkeypatch):
        monkeypatch.setattr(srw, "SRW_DIR", tmp_path / "srw")
        monkeypatch.setattr(srw, "load_secrets", lambda: {"AI_PASSWORD": "pw-line"})
        network = Network("172.30.89.0/24")
        source = srw.prepare_addon(network, "fake")
        assert source["ari_url"] == "http://172.30.89.50:8088"
        assert source["media_listen"] == "172.30.89.1:18441"
        assert source["app"] == srw.APP
        password = Path(source["password_file"])
        assert (
            password.read_text()
            == (tmp_path / "srw" / "addon" / "asterisk-password").read_text()
        )
        assert password.stat().st_mode & 0o777 == 0o600
        etc = tmp_path / "srw" / "addon" / "etc"
        assert "bindaddr = 172.30.89.50" in (etc / "http.conf").read_text()
        assert (
            "ws://172.30.89.1:18441/media"
            in (etc / "websocket_client.conf").read_text()
        )
        pjsip = (etc / "pjsip.conf").read_text()
        assert "172.30.89.10" in pjsip and "srw-fake" in pjsip
        assert "pw-line" in pjsip  # the line's registration secret, rendered in
        assert "fake" in (etc / "extensions.conf").read_text()
        saved = (tmp_path / "srw" / "gateway" / "source.yaml").read_text()
        assert "172.30.89.1:18441" in saved

    def test_media_host_override(self, tmp_path, monkeypatch):
        monkeypatch.setattr(srw, "SRW_DIR", tmp_path / "srw")
        monkeypatch.setattr(srw, "load_secrets", lambda: {"AI_PASSWORD": "pw"})
        source = srw.prepare_addon(
            Network("172.30.89.0/24"), "fake", "2001", media_host="172.30.89.7"
        )
        assert source["media_listen"] == "172.30.89.7:18441"
        pjsip = (tmp_path / "srw" / "addon" / "etc" / "pjsip.conf").read_text()
        assert "2001" in pjsip
