"""Unit tests for the WP0 voice bench's parsing and checks (no Docker needed)."""

from __future__ import annotations

import array
import importlib.util
import math
import re
from pathlib import Path

import pytest

from tests.voice_bench.analysis import (
    SipMessage,
    dbfs,
    final_status,
    initial_invite,
    keyword_found,
    missing_keywords,
    normalize,
    parse_asterisk_sip_log,
    parse_sdp,
    read_wav,
    slice_wav,
    split_sip,
    voiced_ratio,
    write_wav,
)
from tests.voice_bench.profiles import (
    PROFILES,
    CallData,
    check_dtmf_info,
    check_refer_refuse,
    check_reinvite,
    gateway_checks,
)

BENCH = Path(__file__).resolve().parent

INVITE_SDP = """v=0
o=- 1 3 IN IP4 172.30.86.10
s=Asterisk
c=IN IP4 172.30.86.10
t=0 0
m=audio 10064 RTP/AVP 8 101
a=rtpmap:8 PCMA/8000
a=rtpmap:101 telephone-event/8000
a=fmtp:101 0-16
a=ptime:20
a=sendrecv
"""

PBX_LOG = """[2026-10-21 12:56:23.700] VERBOSE[29] res_pjsip_logger.c: <--- Transmitting SIP request (489 bytes) to UDP:172.30.86.20:5060 --->
INFO sip:172.30.86.20:5060 SIP/2.0
Via: SIP/2.0/UDP 172.30.86.10:5060;rport;branch=z9hG4bKPjc451
From: "Anrufer Test" <sip:1000@172.30.86.10>;tag=88f1
To: <sip:2000@172.30.86.20;ob>;tag=ngVA
Call-ID: f5dc
CSeq: 1153 INFO
Content-Type: application/dtmf-relay
Content-Length:    24

Signal=2
Duration=200

[2026-10-21 12:56:23.701] VERBOSE[34] res_pjsip_logger.c: <--- Received SIP response (300 bytes) from UDP:172.30.86.20:5060 --->
SIP/2.0 200 OK
Call-ID: f5dc
CSeq: 1153 INFO
Content-Length:  0

[2026-10-21 12:56:23.702] VERBOSE[34] res_pjsip_logger.c: <--- Received SIP request (300 bytes) from UDP:172.30.86.40:5060 --->
BYE sip:172.30.86.10:5060 SIP/2.0
Call-ID: other
CSeq: 5 BYE

[2026-10-21 12:56:23.703] VERBOSE[1] pbx.c: Executing [s@ring-ai:1] NoOp
"""


def _msg(t, direction, start, headers, body=""):
    return SipMessage(t=t, direction=direction, start=start, headers=headers, body=body)


def _sine(rate=16000, seconds=1.0, amplitude=8000):
    return array.array(
        "h",
        (
            int(amplitude * math.sin(2 * math.pi * 440 * i / rate))
            for i in range(int(rate * seconds))
        ),
    )


class TestSdp:
    def test_codecs_and_telephone_event(self):
        sdp = parse_sdp(INVITE_SDP)
        assert sdp.connection == "172.30.86.10"
        assert sdp.audio_port == 10064
        assert sdp.voice_codecs == ["PCMA"]
        assert sdp.telephone_event
        assert sdp.direction == "sendrecv"
        assert sdp.origin_version == 3
        assert sdp.media_target == "172.30.86.10:10064"

    def test_static_payloads_without_rtpmap(self):
        sdp = parse_sdp("c=IN IP4 10.0.0.1\nm=audio 4000 RTP/AVP 0 8 9\na=sendonly\n")
        assert sdp.voice_codecs == ["PCMU", "PCMA", "G722"]
        assert not sdp.telephone_event
        assert sdp.direction == "sendonly"


class TestSipMessages:
    def test_split_sip_folds_continuations(self):
        parsed = split_sip("INVITE sip:a@b SIP/2.0\nVia: x\nSubject: one\n two\n\nbody")
        assert parsed["start"] == "INVITE sip:a@b SIP/2.0"
        assert ("Subject", "one two") in parsed["headers"]
        assert parsed["body"] == "body"

    def test_request_response_pairing(self):
        invite = _msg(
            1.0,
            "rx",
            "INVITE sip:2000@x SIP/2.0",
            [("Call-ID", "c1"), ("CSeq", "7 INVITE"), ("To", "<sip:2000@x>")],
        )
        ok = _msg(
            1.1,
            "tx",
            "SIP/2.0 200 OK",
            [("Call-ID", "c1"), ("CSeq", "7 INVITE"), ("To", "<sip:2000@x>;tag=a")],
        )
        reinvite = _msg(
            2.0,
            "rx",
            "INVITE sip:x SIP/2.0",
            [("Call-ID", "c1"), ("CSeq", "8 INVITE"), ("To", "<sip:2000@x>;tag=a")],
        )
        msgs = [invite, ok, reinvite]
        assert initial_invite(msgs) is invite
        assert final_status(msgs, invite) == 200
        assert final_status(msgs, reinvite) is None
        assert ok.method == "INVITE" and ok.status == 200

    def test_asterisk_log_keeps_dut_traffic_and_flips_direction(self):
        msgs = parse_asterisk_sip_log(PBX_LOG, exclude_hosts={"172.30.86.40"})
        assert [m.start.split()[0] for m in msgs] == ["INFO", "SIP/2.0"]
        info, ok = msgs
        assert info.direction == "rx" and ok.direction == "tx"
        assert info.body.startswith("Signal=2")
        assert final_status(msgs, info) == 200


class TestAudioAndTranscripts:
    def test_level_of_tone_and_silence(self, tmp_path):
        tone = _sine()
        silence = array.array("h", [0] * 16000)
        assert dbfs(tone) > -20
        assert dbfs(silence) == -120.0
        assert voiced_ratio(16000, tone) == 1.0
        assert voiced_ratio(16000, silence) == 0.0
        path = tmp_path / "mix.wav"
        write_wav(path, 16000, silence + tone)
        clip = tmp_path / "clip.wav"
        assert slice_wav(path, clip, 1.0, 1.5) == pytest.approx(0.5)
        rate, samples = read_wav(clip)
        assert rate == 16000 and voiced_ratio(rate, samples) == 1.0

    def test_keywords_ignore_case_space_and_punctuation(self):
        text = "Guten Tag, hier ist der Digital-Assistent vom Hotel Beispiel Hof."
        assert normalize("Straße 1") == "strasse1"
        assert keyword_found(text, "digitalassistent")
        assert keyword_found(text, "beispielhof")
        assert keyword_found(text, "Zimmer|hotel")
        assert missing_keywords(text, ["assistent", "frühstück"]) == ["frühstück"]


def _calldata(profile, msgs, events=None):
    return CallData(
        profile=PROFILES[profile],
        out=Path("."),
        standin=True,
        events={"caller": [], "dut": events or [], "human": []},
        summary={},
        dut_msgs=msgs,
        keywords={
            "caller": ["doppelzimmer"],
            "dut": ["assistent"],
            "human": ["mitarbeiter"],
        },
        heard_from_dut=["assistent"],
    )


class TestProfileChecks:
    def test_dtmf_info_reads_signals_and_digits(self):
        invite = _msg(
            0.0,
            "rx",
            "INVITE sip:2000@x SIP/2.0",
            [
                ("Call-ID", "c"),
                ("CSeq", "1 INVITE"),
                ("To", "<sip:2000@x>"),
                ("Content-Type", "application/sdp"),
            ],
            INVITE_SDP.replace("8 101", "8").replace(
                "a=rtpmap:101 telephone-event/8000\n", ""
            ),
        )
        msgs = [invite]
        events = []
        for i, digit in enumerate("2580#"):
            hdrs = [
                ("Call-ID", "c"),
                ("CSeq", f"{10 + i} INFO"),
                ("Content-Type", "application/dtmf-relay"),
            ]
            msgs.append(
                _msg(
                    1.0 + i,
                    "rx",
                    "INFO sip:x SIP/2.0",
                    hdrs,
                    f"Signal={digit}\nDuration=200",
                )
            )
            msgs.append(_msg(1.1 + i, "tx", "SIP/2.0 200 OK", hdrs[:2]))
            events.append({"type": "dtmf", "digit": digit, "method": "info"})
        checks = {
            c.name: c.ok for c in check_dtmf_info(_calldata("dtmf-info", msgs, events))
        }
        assert checks == {
            "no_telephone_event_offered": True,
            "dtmf_as_sip_info": True,
            "dut_saw_digits": True,
        }

    def test_reinvite_needs_media_moved_away_and_back(self):
        sdp_pbx = INVITE_SDP
        sdp_caller = INVITE_SDP.replace(
            "c=IN IP4 172.30.86.10", "c=IN IP4 172.30.86.40"
        )
        hdr = [("Call-ID", "c"), ("Content-Type", "application/sdp")]
        msgs = [
            _msg(
                0.0,
                "rx",
                "INVITE sip:2000@x SIP/2.0",
                [*hdr, ("CSeq", "1 INVITE"), ("To", "<sip:2000@x>")],
                sdp_pbx,
            )
        ]
        for n, (t, body) in enumerate(
            [(0.1, sdp_caller), (5.0, sdp_pbx), (8.0, sdp_caller)], start=2
        ):
            cseq = ("CSeq", f"{n} INVITE")
            msgs.append(
                _msg(
                    t,
                    "rx",
                    "INVITE sip:x SIP/2.0",
                    [*hdr, cseq, ("To", "<sip:2000@x>;tag=a")],
                    body,
                )
            )
            msgs.append(
                _msg(t + 0.01, "tx", "SIP/2.0 200 OK", [("Call-ID", "c"), cseq])
            )
        checks = {c.name: c.ok for c in check_reinvite(_calldata("reinvite", msgs))}
        assert (
            checks["reinvites_mid_call"]
            and checks["media_reanchored"]
            and checks["reinvites_accepted"]
        )


class TestSrwChecks:
    """``--dut srw``: SRW's side of the call comes from the driver's log."""

    DRIVER = [
        {
            "kind": "webhook",
            "connector": "bench-register",
            "session_id": "s",
            "sip_headers": [
                {"name": "From", "value": "<sip:1000@pbx>"},
                {"name": "To", "value": "<sip:2000@addon>"},
            ],
        },
        {"kind": "accepted", "session_id": "s", "status": 200},
        {"kind": "event", "session_id": "s", "event": "session.started"},
        {"kind": "referred", "session_id": "s", "status": 200},
        {
            "kind": "event",
            "session_id": "s",
            "event": "session.closed",
            "reason": "remote_hangup",
        },
    ]

    def _srw(self, profile, msgs, driver, human=None):
        data = _calldata(profile, msgs)
        data.standin, data.dut_mode, data.driver = False, "srw", driver
        data.events["human"] = human or []
        return data

    def test_gateway_checks_read_the_driver_log(self):
        checks = gateway_checks(self._srw("register", [], self.DRIVER))
        assert [c.ok for c in checks] == [True, True, True]
        peer = gateway_checks(self._srw("peer", [], self.DRIVER))
        assert peer[0].ok is False  # the peer call belongs to the peer line
        closed = [e for e in self.DRIVER if e.get("event") != "session.closed"]
        assert gateway_checks(self._srw("register", [], closed))[2].ok is False

    def test_refused_refer_expects_srw_to_dial_the_target(self):
        call = [("Call-ID", "c"), ("To", "<sip:2000@x>;tag=a")]
        refer = _msg(
            6.6,
            "tx",
            "REFER sip:pbx SIP/2.0",
            [*call, ("CSeq", "3 REFER"), ("Refer-To", "<sip:3000@pbx>")],
        )
        refused = _msg(6.61, "rx", "SIP/2.0 603 Decline", [*call, ("CSeq", "3 REFER")])
        dial = [("Call-ID", "d"), ("To", "<sip:3000@pbx>")]
        msgs = [refer, refused]
        for cseq, status in ((1, "401 Unauthorized"), (2, "200 OK")):
            head = [*dial, ("CSeq", f"{cseq} INVITE")]
            msgs.append(
                _msg(6.62 + cseq / 100, "tx", "INVITE sip:3000@pbx SIP/2.0", head)
            )
            msgs.append(_msg(6.63 + cseq / 100, "rx", f"SIP/2.0 {status}", head))
        human = [{"type": "call_state", "state": "CONFIRMED", "t": 6.7}]
        checks = {
            c.name: c.ok
            for c in check_refer_refuse(
                self._srw("refer-refuse", msgs, self.DRIVER, human)
            )
        }
        assert checks["refer_refused"] and checks["call_stays_up"]
        assert checks["driver_referred"] and checks["fallback_dialled"]
        assert checks["human_answered"] and "human_not_called" not in checks
        # The stand-in keeps the WP0 expectation: nobody calls the human.
        standin = {
            c.name for c in check_refer_refuse(_calldata("refer-refuse", msgs[:2]))
        }
        assert "human_not_called" in standin and "fallback_dialled" not in standin


class TestBenchConfigConsistency:
    def test_every_profile_number_and_endpoint_exists(self):
        dialplan = (BENCH / "pbx" / "extensions.conf").read_text(encoding="utf-8")
        pjsip = (BENCH / "pbx" / "pjsip.conf.in").read_text(encoding="utf-8")
        endpoints = set(re.findall(r"^\[([\w-]+)\]\(phone\)", pjsip, flags=re.M))
        for profile in PROFILES.values():
            assert re.search(
                rf"^exten => {profile.number},1,Goto\(profile-{profile.name},s,1\)",
                dialplan,
                re.M,
            )
        for endpoint in re.findall(r"ring-ai,s,1\(([\w-]+)\)", dialplan):
            assert endpoint in endpoints

    def test_ua_agent_parses_pjsua_lines(self):
        spec = importlib.util.spec_from_file_location(
            "ua_agent", BENCH / "ua" / "ua_agent.py"
        )
        ua = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(ua)
        rx = ua.SIP_START.search(
            ">>> 12:56:17.945           pjsua_core.c  .RX 556 bytes Response msg 401/REGISTER/cseq=54719 "
            "(rdata0x556d0c74c838) from UDP 172.30.86.10:5060:"
        )
        assert rx and rx["dir"] == "RX" and rx["peer"] == "172.30.86.10:5060"
        dtmf = ua.DTMF.search(
            "12:58:06.764  pjsua_app.c  .Incoming DTMF on call 0: 9, using RFC2833 method"
        )
        assert dtmf and dtmf["digit"] == "9" and dtmf["method"] == "RFC2833"
        info = ua.DTMF.search(
            "Incoming DTMF on call 0: #:duration(200), using SIP INFO method"
        )
        assert info and info["digit"] == "#" and info["duration"] == "200"
        xfer = ua.TRANSFER.search("......Call 0: transfer status=603 (Decline) [final]")
        assert xfer and xfer["code"] == "603" and xfer["final"]
        gone = ua.CALL_DISCONNECTED.search(
            "Call 0 is DISCONNECTED [reason=200 (Normal call clearing)]"
        )
        assert gone and gone["code"] == "200"
