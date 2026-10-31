"""Pure helpers for the WP0 voice bench: SIP, SDP, audio and transcripts.

Nothing here talks to Docker, so tests/voice_bench/test_voice_bench_analysis.py
can exercise it in the normal test suite.
"""

from __future__ import annotations

import array
import json
import math
import re
import unicodedata
import wave
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

# -- SIP messages ------------------------------------------------------------------


@dataclass
class SipMessage:
    """One SIP message as seen at the device under test (DUT).

    ``direction`` is from the DUT's point of view: ``rx`` = the DUT received
    it from the PBX, ``tx`` = the DUT sent it.
    """

    t: float
    direction: str
    start: str
    headers: list[tuple[str, str]]
    body: str = ""
    peer: str = ""

    @property
    def is_request(self) -> bool:
        return not self.start.startswith("SIP/2.0")

    @property
    def method(self) -> str:
        if self.is_request:
            return self.start.split(" ", 1)[0]
        return self.cseq_method

    @property
    def status(self) -> int | None:
        if self.is_request:
            return None
        parts = self.start.split(" ", 2)
        return int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else None

    def header(self, name: str) -> list[str]:
        wanted = name.lower()
        return [value for key, value in self.headers if key.lower() == wanted]

    def first(self, name: str) -> str:
        values = self.header(name)
        return values[0] if values else ""

    @property
    def call_id(self) -> str:
        return self.first("Call-ID") or self.first("i")

    @property
    def cseq(self) -> tuple[int, str]:
        raw = self.first("CSeq").split()
        if len(raw) == 2 and raw[0].isdigit():
            return int(raw[0]), raw[1].upper()
        return 0, ""

    @property
    def cseq_method(self) -> str:
        return self.cseq[1]

    @property
    def has_to_tag(self) -> bool:
        return ";tag=" in self.first("To").replace(" ", "")

    def sdp(self) -> Sdp | None:
        content_type = self.first("Content-Type").lower()
        if "application/sdp" not in content_type or not self.body.strip():
            return None
        return parse_sdp(self.body)


def messages_from_events(events: list[dict]) -> list[SipMessage]:
    """SIP messages from a ua_agent event log (the stand-in DUT's own view)."""
    out = []
    for event in events:
        if event.get("type") != "sip":
            continue
        out.append(
            SipMessage(
                t=float(event["t"]),
                direction=event["dir"],
                start=event["start"],
                headers=[(k, v) for k, v in event["headers"]],
                body=event.get("body", ""),
                peer=event.get("peer", ""),
            )
        )
    return out


_AST_LOG_LINE = re.compile(r"^\[(?P<ts>\d{4}-\d\d-\d\d \d\d:\d\d:\d\d(?:\.\d+)?)\] ")
_AST_SIP_START = re.compile(
    r"<--- (?P<what>Transmitting|Received) SIP (?:request|response) \(\d+ bytes\) "
    r"(?:to|from) (?P<transport>\w+):(?P<peer>[^ ]+) --->"
)


def parse_asterisk_sip_log(text: str, exclude_hosts: set[str]) -> list[SipMessage]:
    """SIP messages from an Asterisk ``full`` log written with pjsip debug on.

    Keeps the messages exchanged with any host not in ``exclude_hosts`` (the
    bench's own caller and human) and flips the direction to the DUT's view.
    """
    out: list[SipMessage] = []
    current: dict | None = None
    lines: list[str] = []

    def flush() -> None:
        if current is None:
            return
        raw = "\n".join(lines).strip("\n")
        if raw:
            parsed = split_sip(raw)
            out.append(
                SipMessage(
                    t=current["t"],
                    peer=current["peer"],
                    direction=current["dir"],
                    **parsed,
                )
            )

    for line in text.splitlines():
        stamp = _AST_LOG_LINE.match(line)
        if stamp:
            flush()
            current, lines = None, []
            m = _AST_SIP_START.search(line)
            if m:
                host = m["peer"].rsplit(":", 1)[0]
                if host not in exclude_hosts:
                    # The container logs in UTC.
                    when = datetime.strptime(
                        stamp["ts"][:23], "%Y-%m-%d %H:%M:%S.%f"
                    ).replace(tzinfo=timezone.utc)
                    direction = "rx" if m["what"] == "Transmitting" else "tx"
                    current = {
                        "t": when.timestamp(),
                        "peer": m["peer"],
                        "dir": direction,
                    }
            continue
        if current is not None:
            lines.append(line)
    flush()
    return out


def split_sip(text: str) -> dict:
    """Start line, headers (continuations folded) and body of a SIP message."""
    head, _, body = text.replace("\r\n", "\n").partition("\n\n")
    head_lines = head.split("\n")
    headers: list[tuple[str, str]] = []
    for line in head_lines[1:]:
        if line[:1] in (" ", "\t") and headers:
            name, value = headers[-1]
            headers[-1] = (name, value + " " + line.strip())
        elif ":" in line:
            name, _, value = line.partition(":")
            headers.append((name.strip(), value.strip()))
    return {
        "start": head_lines[0].strip(),
        "headers": headers,
        "body": body.strip("\n"),
    }


def initial_invite(msgs: list[SipMessage]) -> SipMessage | None:
    """The first dialog-creating INVITE the DUT received."""
    for msg in msgs:
        if (
            msg.direction == "rx"
            and msg.is_request
            and msg.method == "INVITE"
            and not msg.has_to_tag
        ):
            return msg
    return None


def response_to(msgs: list[SipMessage], request: SipMessage) -> list[SipMessage]:
    """Responses travelling the other way for the request's Call-ID and CSeq."""
    other = "tx" if request.direction == "rx" else "rx"
    return [
        m
        for m in msgs
        if not m.is_request
        and m.direction == other
        and m.call_id == request.call_id
        and m.cseq == request.cseq
    ]


def final_status(msgs: list[SipMessage], request: SipMessage) -> int | None:
    codes = [
        m.status for m in response_to(msgs, request) if m.status and m.status >= 200
    ]
    return codes[-1] if codes else None


# -- SDP -----------------------------------------------------------------------------


@dataclass
class Sdp:
    connection: str = ""
    audio_port: int = 0
    payloads: list[str] = field(default_factory=list)
    codecs: list[str] = field(default_factory=list)
    direction: str = "sendrecv"
    origin_version: int = 0

    @property
    def telephone_event(self) -> bool:
        return any(c.lower().startswith("telephone-event") for c in self.codecs)

    @property
    def voice_codecs(self) -> list[str]:
        return [
            c.split("/")[0].upper()
            for c in self.codecs
            if not c.lower().startswith("telephone-event")
        ]

    @property
    def media_target(self) -> str:
        return f"{self.connection}:{self.audio_port}"


_STATIC_PAYLOADS = {
    "0": "PCMU/8000",
    "8": "PCMA/8000",
    "9": "G722/8000",
    "18": "G729/8000",
}


def parse_sdp(body: str) -> Sdp:
    sdp = Sdp()
    rtpmap: dict[str, str] = {}
    in_audio = False
    for raw in body.splitlines():
        line = raw.strip()
        if line.startswith("o="):
            parts = line[2:].split()
            if len(parts) > 2 and parts[2].isdigit():
                sdp.origin_version = int(parts[2])
        elif line.startswith("c=") and (in_audio or not sdp.connection):
            parts = line[2:].split()
            if len(parts) >= 3:
                sdp.connection = parts[2]
        elif line.startswith("m="):
            parts = line[2:].split()
            in_audio = parts[0] == "audio"
            if in_audio:
                sdp.audio_port = int(parts[1])
                sdp.payloads = parts[3:]
        elif in_audio and line.startswith("a=rtpmap:"):
            pt, _, name = line[len("a=rtpmap:") :].partition(" ")
            rtpmap[pt] = name
        elif in_audio and line[2:] in ("sendrecv", "sendonly", "recvonly", "inactive"):
            sdp.direction = line[2:]
    sdp.codecs = [rtpmap.get(pt, _STATIC_PAYLOADS.get(pt, pt)) for pt in sdp.payloads]
    return sdp


# -- audio ---------------------------------------------------------------------------


def read_wav(path: Path) -> tuple[int, array.array]:
    """Mono 16-bit PCM samples of a WAV file."""
    with wave.open(str(path)) as w:
        if w.getsampwidth() != 2 or w.getnchannels() != 1:
            raise ValueError(f"{path}: expected mono 16-bit PCM")
        rate = w.getframerate()
        samples = array.array("h")
        samples.frombytes(w.readframes(w.getnframes()))
    return rate, samples


def write_wav(path: Path, rate: int, samples: array.array) -> None:
    with wave.open(str(path), "w") as w:
        w.setnchannels(1)
        w.setsampwidth(2)
        w.setframerate(rate)
        w.writeframes(samples.tobytes())


def slice_wav(
    src: Path, dst: Path, start_s: float, end_s: float | None = None
) -> float:
    """Copy [start_s, end_s) of a WAV; returns the slice length in seconds."""
    rate, samples = read_wav(src)
    first = max(0, int(start_s * rate))
    last = len(samples) if end_s is None else min(len(samples), int(end_s * rate))
    part = samples[first:last] if last > first else array.array("h")
    write_wav(dst, rate, part)
    return len(part) / rate


def dbfs(samples) -> float:
    if not samples:
        return -120.0
    mean_square = sum(s * s for s in samples) / len(samples)
    if mean_square <= 0:
        return -120.0
    return 10 * math.log10(mean_square / (32768.0**2))


def voiced_ratio(
    rate: int, samples: array.array, threshold_dbfs: float = -45.0, frame_ms: int = 20
) -> float:
    """Share of 20 ms frames louder than ``threshold_dbfs``."""
    size = max(1, rate * frame_ms // 1000)
    frames = [samples[i : i + size] for i in range(0, len(samples) - size + 1, size)]
    if not frames:
        return 0.0
    loud = sum(1 for frame in frames if dbfs(frame) > threshold_dbfs)
    return loud / len(frames)


# -- transcripts ---------------------------------------------------------------------


def normalize(text: str) -> str:
    """Lower case, ß→ss, letters and digits only (no spaces or punctuation)."""
    text = unicodedata.normalize("NFKC", text).lower().replace("ß", "ss")
    return "".join(ch for ch in text if ch.isalnum())


def keyword_found(transcript: str, keyword: str) -> bool:
    """``keyword`` may list alternatives as ``a|b``; any one is enough."""
    haystack = normalize(transcript)
    return any(
        normalize(alt) and normalize(alt) in haystack for alt in keyword.split("|")
    )


def missing_keywords(transcript: str, keywords: list[str]) -> list[str]:
    return [k for k in keywords if not keyword_found(transcript, k)]


def whisper_text(json_path: Path) -> str:
    """Joined segment texts of a whisper.cpp ``-oj`` output file."""
    data = json.loads(json_path.read_text(encoding="utf-8"))
    return " ".join(
        seg.get("text", "").strip() for seg in data.get("transcription", [])
    ).strip()


def load_events(path: Path) -> list[dict]:
    if not path.exists():
        return []
    events = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            events.append(json.loads(line))
    return events
