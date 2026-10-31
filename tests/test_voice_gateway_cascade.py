"""The cascade engine against fake OpenAI-compatible servers; local_voice_gateway.md §4.3.

Runs in the gateway's voice venv (Pipecat installed); the main suite skips
it. Caller speech is a tone and the VAD an energy threshold, so no model is
needed; the call runs in real time on a ``FileAudioPort``. Run it with

    PYTHONPATH=src .venv-voice/bin/python -m pytest --noconftest \\
        tests/test_voice_gateway_cascade*.py

(the repo's conftest needs the orchestrator's dependencies).
"""

from __future__ import annotations

import asyncio
import json
import math
import struct

import pytest
import pytest_asyncio

pytest.importorskip("pipecat")

import audioop  # noqa: E402 - audioop-lts, a Pipecat dependency on 3.13+

from aiohttp import web  # noqa: E402
from pipecat.audio.vad.vad_analyzer import VADAnalyzer  # noqa: E402

from voice_gateway.config import Line  # noqa: E402
from voice_gateway.engines.cascade import parse_settings  # noqa: E402
from voice_gateway.engines.cascade.engine import CascadeEngine  # noqa: E402
from voice_gateway.engines.cascade.fileport import FileAudioPort  # noqa: E402
from pipecat.processors.aggregators import async_tool_messages  # noqa: E402

from voice_gateway.engines.cascade.prompt import (  # noqa: E402
    ADDITION_OPEN,
    COVERED,
    HANDED_ON,
)
from voice_gateway.engines.cascade.services import (  # noqa: E402
    PHRASES,
    SpokenTextAggregator,
    cut_loop,
    note_messages,
    strip_markup,
)
from voice_gateway.engines.cascade.settings import (  # noqa: E402
    HOLD_LONGER,
    HOLD_PHRASES,
)
from voice_gateway.engines.cascade.transport import heard_prefix  # noqa: E402
from voice_gateway.session import Session  # noqa: E402


INSTRUCTIONS = (
    "Du bist die Telefonstimme vom Hotel Beispielhof.\n\n## Facts\nCheck-in ab 15 Uhr."
)
GREETING = 'Greet the caller now with exactly these words, then listen: "Guten Tag, hier ist das Hotel."'
FILLER = "Einen Moment, ich schaue nach."
GREETING_2 = (
    "Greet the caller now with exactly these words, then listen: "
    '"Guten Tag, hier ist das Hotel am 12. März. Ich bin eine KI."'
)


def tone(
    seconds: float, rate: int = 16_000, freq: float = 220.0, amp: int = 9000
) -> bytes:
    count = int(seconds * rate)
    return struct.pack(
        f"<{count}h",
        *(int(amp * math.sin(2 * math.pi * freq * i / rate)) for i in range(count)),
    )


class EnergyVAD(VADAnalyzer):
    """Speech = loud enough; the tests' caller is a tone."""

    def num_frames_required(self) -> int:
        return 320

    def voice_confidence(self, buffer) -> float:
        return 1.0 if audioop.rms(buffer, 2) > 500 else 0.0


class Fakes:
    """STT, LLM and TTS on one aiohttp server, scripted per test."""

    def __init__(self):
        self.stt_texts: list[str] = []
        self.stt_calls = 0
        self.llm_requests: list[dict] = []
        self.prewarms: list[dict] = []
        self.llm_replies: list = []  # each: str (text) or ("delegate", request[, text])
        self.tts_inputs: list[str] = []
        #: Seconds between the streamed words of a reply.
        self.word_delay = 0.0
        #: Requests for the filler (prewarmed into the phrase cache at the
        #: call's start), kept out of ``tts_inputs``.
        self.filler_requests = 0
        #: Requests for the hold sentences (prewarmed at the first
        #: delegation), kept out of ``tts_inputs`` too.
        self.hold_requests = 0
        self.ms_per_char = 40

    async def transcriptions(self, request: web.Request) -> web.Response:
        form = await request.post()
        assert form["model"] == "stt-model" and form["language"] == "de"
        self.stt_calls += 1
        text = self.stt_texts.pop(0) if self.stt_texts else ""
        return web.json_response({"text": text})

    async def completions(self, request: web.Request) -> web.StreamResponse:
        body = await request.json()
        if not body.get("stream"):
            # The engine's prewarm: one token over the fixed prefix.
            self.prewarms.append(body)
            message = {"role": "assistant", "content": "G"}
            return web.json_response(
                {
                    "id": "p",
                    "object": "chat.completion",
                    "created": 0,
                    "model": body["model"],
                    "choices": [
                        {"index": 0, "message": message, "finish_reason": "length"}
                    ],
                }
            )
        self.llm_requests.append(body)
        reply = self.llm_replies.pop(0) if self.llm_replies else "Gern."
        response = web.StreamResponse(headers={"Content-Type": "text/event-stream"})
        await response.prepare(request)

        async def send(delta, finish=None):
            chunk = {
                "id": "c",
                "object": "chat.completion.chunk",
                "created": 0,
                "model": body["model"],
                "choices": [{"index": 0, "delta": delta, "finish_reason": finish}],
            }
            await response.write(f"data: {json.dumps(chunk)}\n\n".encode())

        if isinstance(reply, tuple):
            _, ask, *said = reply
            if said:
                await send({"role": "assistant", "content": said[0]})
            call = {
                "index": 0,
                "id": f"call_{len(self.llm_requests)}",
                "type": "function",
                "function": {
                    "name": "delegate",
                    "arguments": json.dumps({"request": ask}),
                },
            }
            await send({"tool_calls": [call]})
            await send({}, "tool_calls")
        else:
            for word in reply.split(" "):
                await send({"role": "assistant", "content": word + " "})
                if self.word_delay:
                    await asyncio.sleep(self.word_delay)
            await send({}, "stop")
        await response.write(b"data: [DONE]\n\n")
        return response

    async def speech(self, request: web.Request) -> web.StreamResponse:
        body = await request.json()
        assert body["model"] == "tts-model" and body["voice"] == "speaker-1"
        if body["input"] in (FILLER, "Einen Moment, ich frage nach."):
            self.filler_requests += 1
        elif body["input"] in (*HOLD_PHRASES["de"], HOLD_LONGER["de"]):
            self.hold_requests += 1
        else:
            self.tts_inputs.append(body["input"])
        pcm = tone(len(body["input"]) * self.ms_per_char / 1000, rate=24_000, freq=330)
        response = web.StreamResponse(headers={"Content-Type": "audio/pcm"})
        await response.prepare(request)
        for at in range(0, len(pcm), 9600):
            await response.write(pcm[at : at + 9600])
        return response

    async def start(self) -> str:
        app = web.Application()
        app.router.add_post("/v1/audio/transcriptions", self.transcriptions)
        app.router.add_post("/v1/chat/completions", self.completions)
        app.router.add_post("/v1/audio/speech", self.speech)
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        return f"http://127.0.0.1:{port}/v1"


class PortSource:
    def __init__(self, port: FileAudioPort):
        self.port = port

    async def answer(self, session):
        session.audio = self.port
        self.port.start()

    async def reject(self, session, status_code):
        pass

    async def refer(self, session, target_uri):
        pass

    async def hangup(self, session):
        pass


class Call:
    def __init__(self, fakes: Fakes, base: str, **overrides):
        raw = {
            "stt": {"base_url": base, "model": "stt-model"},
            "llm": {
                "base_url": base,
                "model": "llm-model",
                "extra_body": {"cache_prompt": True},
            },
            "tts": {
                "base_url": base,
                "model": "tts-model",
                "voice": "speaker-1",
                "sample_rate": 24_000,
            },
            "turn": {
                "smart_turn": False,
                "speech_timeout_secs": 0.2,
                "vad_start_secs": 0.1,
                "vad_stop_secs": 0.2,
                "vad_min_volume": 0.0,
            },
            **overrides,
        }
        self.settings = parse_settings("cascade-test", raw)
        self.fakes = fakes
        self.port = FileAudioPort()
        self.engine: CascadeEngine | None = None
        self.events: list[dict] = []

        def factory():
            self.engine = CascadeEngine(self.settings, vad=EnergyVAD())
            return self.engine

        line = Line("test", "connector", b"key")
        self.session = Session(
            line, [], PortSource(self.port), {"cascade-test": factory}
        )

    async def accept(self):
        await self.session.control(
            "accept",
            {
                "session": {
                    "type": "live",
                    "model": "cascade-test",
                    "instructions": INSTRUCTIONS,
                    "audio": {"output": {"voice": "marin"}},
                    "delegation": {"type": "client"},
                }
            },
        )
        _, self.queue = self.session.subscribe()
        self.collector = asyncio.create_task(self._collect())

    async def _collect(self):
        while (event := await self.queue.get()) is not None:
            self.events.append(event)

    async def append(self, kind: str, content: str, delegation_id: str | None = None):
        command = {
            "type": f"session.{kind}.append",
            "event_id": f"e{len(self.events)}",
            "content": content,
        }
        if delegation_id:
            command["delegation_id"] = delegation_id
        await self.session.command(command, self.queue)

    def transcripts(self, side: str) -> list[str]:
        kind = f"session.{'input' if side == 'caller' else 'output'}_transcript.delta"
        return [e["delta"] for e in self.events if e["type"] == kind]

    def index(self, kind: str, text: str | None = None) -> int:
        for i, e in enumerate(self.events):
            if e["type"] == kind and (text is None or text in e.get("delta", "")):
                return i
        return -1

    async def until(self, check, timeout: float = 10.0):
        deadline = asyncio.get_running_loop().time() + timeout
        while not check():
            if asyncio.get_running_loop().time() > deadline:
                raise AssertionError(
                    f"timed out; events: {[e['type'] for e in self.events]}"
                )
            await asyncio.sleep(0.05)

    async def quiet(self, seconds: float = 0.6):
        """Wait until the voice has been silent for ``seconds``."""
        last = -1.0
        while True:
            await asyncio.sleep(0.05)
            if self.port.voice_playing or (
                self.engine.output and self.engine.output.audible()
            ):
                last = -1.0
                continue
            now = asyncio.get_running_loop().time()
            last = now if last < 0 else last
            if now - last >= seconds:
                return

    async def close(self):
        await self.session.close("remote_hangup")
        await self.port.close()
        self.collector.cancel()


@pytest_asyncio.fixture
async def fakes():
    PHRASES.clear()  # process-wide; each test's fake voice differs
    fakes = Fakes()
    fakes.base = await fakes.start()
    yield fakes
    await fakes.runner.cleanup()


@pytest_asyncio.fixture
async def call(fakes):
    call = Call(fakes, fakes.base)
    await call.accept()
    yield call
    await call.close()


@pytest.mark.asyncio
async def test_greeting_is_spoken_verbatim_without_the_llm(call, fakes):
    await call.append("instructions", GREETING)
    await call.until(lambda: call.transcripts("assistant"))
    assert call.transcripts("assistant") == ["Guten Tag, hier ist das Hotel."]
    assert fakes.tts_inputs[0].strip() == "Guten Tag, hier ist das Hotel."
    await call.quiet()
    assert (
        call.port.played_ms()
        >= 0.8 * len("Guten Tag, hier ist das Hotel.") * fakes.ms_per_char
    )
    assert fakes.llm_requests == []
    prewarm = fakes.prewarms[0]
    assert prewarm["max_tokens"] == 1 and prewarm["messages"][0]["role"] == "system"
    assert prewarm["tools"][0]["function"]["name"] == "delegate"


@pytest.mark.asyncio
async def test_the_greeting_goes_to_the_tts_sentence_by_sentence(call, fakes):
    await call.append("instructions", GREETING_2)
    await call.until(lambda: len(fakes.tts_inputs) >= 2)
    await call.quiet()
    assert fakes.tts_inputs == [
        "Guten Tag, hier ist das Hotel am 12. März.",
        "Ich bin eine KI.",
    ]
    assert " ".join(" ".join(call.transcripts("assistant")).split()) == (
        "Guten Tag, hier ist das Hotel am 12. März. Ich bin eine KI."
    )


@pytest.mark.asyncio
async def test_short_sentences_play_back_to_back(call, fakes):
    # Each sentence has its own TTS context; one that is not closed at once
    # held the next sentence back until Pipecat's 3 s context timeout.
    await call.append("instructions", GREETING_2)
    await call.until(lambda: len(fakes.tts_inputs) >= 2)
    await asyncio.sleep(2.0 + 0.6)
    spoken = sum(len(t) for t in fakes.tts_inputs) * fakes.ms_per_char
    assert call.port.played_ms() >= 0.9 * spoken
    assert len(call.port.voice_starts) == 1  # no silence between them


@pytest.mark.asyncio
async def test_a_fact_is_answered_by_the_voice(call, fakes):
    fakes.stt_texts = ["Ab wann kann ich einchecken?"]
    fakes.llm_replies = ["Der Check-in ist ab fünfzehn Uhr."]
    call.port.say(tone(1.0))
    await call.until(lambda: call.transcripts("assistant"))
    await call.quiet()
    assert call.transcripts("caller") == ["Ab wann kann ich einchecken?"]
    assert (
        "".join(call.transcripts("assistant")).strip()
        == "Der Check-in ist ab fünfzehn Uhr."
    )
    request = fakes.llm_requests[0]
    system = request["messages"][0]
    assert system["role"] == "system"
    assert system["content"].startswith("# How this call works")
    assert (
        system["content"].rstrip().endswith("Check-in ab 15 Uhr.")
        or "Check-in ab 15 Uhr." in system["content"]
    )
    assert request["messages"][-1] == {
        "role": "user",
        "content": "Ab wann kann ich einchecken?",
    }
    assert request["tools"][0]["function"]["name"] == "delegate"
    assert request["stream"] is True and request["cache_prompt"] is True
    turn = call.engine.timings.turns[0]
    assert turn.stt_ms is not None and turn.first_audio_ms is not None
    # Every stage of A4's breakdown is measured.
    assert turn.turn_wait_ms is not None and turn.turn_wait_ms >= 0
    assert turn.llm_first_token_ms is not None
    assert turn.llm_first_text_ms is not None
    assert turn.llm_first_text_ms >= turn.llm_first_token_ms
    assert turn.tts_first_audio_ms is not None


@pytest.mark.asyncio
async def test_a_long_first_sentence_starts_at_a_clause(call, fakes):
    fakes.stt_texts = ["Haben Sie im März ein Zimmer frei?"]
    fakes.llm_replies = [
        "Vom 12. bis 14. März ist ein Doppelzimmer frei, es kostet 129 Euro "
        "pro Nacht mit Frühstück. Soll ich es reservieren?"
    ]
    call.port.say(tone(1.0))
    await call.until(lambda: len(fakes.tts_inputs) >= 3)
    await call.quiet()
    # The clause, then the sentence's rest, then whole sentences; the
    # ordinals ("12.", "14.") end nothing.
    assert fakes.tts_inputs == [
        "Vom 12. bis 14. März ist ein Doppelzimmer frei,",
        "es kostet 129 Euro pro Nacht mit Frühstück.",
        "Soll ich es reservieren?",
    ]
    await call.until(lambda: len(call.transcripts("assistant")) >= 3)
    said = " ".join(call.transcripts("assistant"))
    assert "frei, es kostet" in " ".join(said.split())


@pytest.mark.asyncio
async def test_clause_chars_zero_waits_for_the_whole_sentence(fakes):
    call = Call(
        fakes,
        fakes.base,
        tts={
            "base_url": fakes.base,
            "model": "tts-model",
            "voice": "speaker-1",
            "sample_rate": 24_000,
            "clause_chars": 0,
        },
    )
    await call.accept()
    try:
        fakes.stt_texts = ["Haben Sie im März ein Zimmer frei?"]
        fakes.llm_replies = ["Ja, vom 12. bis 14. März ist ein Doppelzimmer frei."]
        call.port.say(tone(1.0))
        await call.until(lambda: fakes.tts_inputs)
        await call.quiet()
        assert fakes.tts_inputs == [
            "Ja, vom 12. bis 14. März ist ein Doppelzimmer frei."
        ]
    finally:
        await call.close()


def test_gemma_markup_never_reaches_the_tts():
    # Seen once in WP4's eval: the content began with empty thought markers.
    text, held = strip_markup(
        "<|channel>thought<channel|><|channel>thought<channel|><channel|>Einen Moment"
    )
    assert (text, held) == ("Einen Moment", "")
    # A marker split across chunks is held back, then dropped.
    text, held = strip_markup("Gern. <|chan")
    assert (text, held) == ("Gern. ", "<|chan")
    assert strip_markup(held + "nel>thought<channel|>Bis 11 Uhr.") == (
        "Bis 11 Uhr.",
        "",
    )
    assert strip_markup("Zimmer 3 < 4") == ("Zimmer 3 < 4", "")


def test_a_whisper_loop_is_cut_but_repeated_digits_stay():
    assert cut_loop("Ich möchte das das das das das das") == "Ich möchte das"
    assert cut_loop("Danke schön danke schön danke schön") == "Danke schön"
    # The test set: a phone number or a postcode says the same digit thrice.
    for said in (
        "Meine Nummer ist null eins fünf zwei, fünf fünf fünf zwei acht",
        "Die Postleitzahl ist 5 5 5 1 8.",
        "null null null eins",
        "zwei eins zwei eins zwei eins",
    ):
        assert cut_loop(said) == said
    assert cut_loop("null " * 9 + "eins") == "null"
    assert cut_loop("5 5 5 5 5 5 5") == "5"


@pytest.mark.asyncio
async def test_markup_in_a_reply_is_not_spoken(call, fakes):
    fakes.stt_texts = ["Wann ist Check-out?"]
    fakes.llm_replies = ["<|channel>thought<channel|><channel|>Bis 11 Uhr."]
    call.port.say(tone(1.0))
    await call.until(lambda: call.transcripts("assistant"))
    await call.quiet()
    assert "".join(call.transcripts("assistant")).strip() == "Bis 11 Uhr."
    assert fakes.tts_inputs == ["Bis 11 Uhr."]


async def _pieces(aggregator, text: str, step: int = 3) -> list[str]:
    out = []
    for at in range(0, len(text), step):
        async for piece in aggregator.aggregate(text[at : at + step]):
            out.append(piece.text)
    tail = await aggregator.flush()
    return out + ([tail.text] if tail and tail.text else [])


@pytest.mark.asyncio
async def test_only_a_reply_s_first_piece_is_cut_at_a_clause():
    aggregator = SpokenTextAggregator(clause_chars=20, language="de")
    reply = (
        "Gerne, das geht. Die Sauna ist täglich von 16 bis 21 Uhr geöffnet, "
        "für Hotelgäste kostenlos."
    )
    # "Gerne," is too short to cut; the first sentence is whole, so the
    # second, long one is not cut either.
    assert await _pieces(aggregator, reply) == [
        "Gerne, das geht.",
        "Die Sauna ist täglich von 16 bis 21 Uhr geöffnet, für Hotelgäste kostenlos.",
    ]
    # The next reply starts afresh.
    assert await _pieces(
        aggregator,
        "Das Frühstück gibt es von 6:30 bis 10:30 Uhr, am Wochenende bis 11.",
    ) == ["Das Frühstück gibt es von 6:30 bis 10:30 Uhr,", "am Wochenende bis 11."]


@pytest.mark.asyncio
async def test_delegation_follows_the_transcript_then_filler_then_phrased_answer(
    call, fakes
):
    fakes.stt_texts = ["Ist am Freitag ein Doppelzimmer frei?"]
    fakes.llm_replies = [
        ("delegate", "Doppelzimmer am Freitag frei?"),
        "Ja, am Freitag ist ein Doppelzimmer frei.",
    ]
    call.port.say(tone(1.0))
    await call.until(lambda: call.index("session.delegation.created") >= 0)
    caller = call.index("session.input_transcript.delta")
    delegation = call.events[call.index("session.delegation.created")]
    assert 0 <= caller < call.index("session.delegation.created")
    await call.until(lambda: call.transcripts("assistant"))
    assert call.transcripts("assistant") == ["Einen Moment, ich schaue nach."]
    await call.quiet()
    await call.append(
        "commentary",
        "Doppelzimmer am Freitag frei, 129 Euro.",
        delegation["delegation"]["id"],
    )
    await call.until(lambda: len(call.transcripts("assistant")) > 1)
    await call.quiet()
    assert "Doppelzimmer frei" in "".join(call.transcripts("assistant")[1:])
    answer_request = fakes.llm_requests[1]["messages"]
    tool = [m for m in answer_request if m["role"] == "tool"]
    assert tool and "129 Euro" in tool[-1]["content"]
    assert call.engine.timings.turns[0].delegated
    # The filler stands right before the call, as if the LLM had said it,
    # and nothing after the result asks for another holding sentence. With
    # the result last they stay two messages (``note_messages``).
    roles = [(m["role"], bool(m.get("tool_calls"))) for m in answer_request]
    filler = next(
        i
        for i, m in enumerate(answer_request)
        if m["role"] == "assistant" and m.get("content") == FILLER
    )
    call_message = answer_request[filler + 1]
    assert call_message["tool_calls"][0]["function"]["name"] == "delegate"
    assert roles[filler + 1 :] == [("assistant", True), ("tool", False)]


@pytest.mark.asyncio
async def test_the_filler_is_synthesised_once_and_replayed(call, fakes):
    # Prewarmed while the call starts, then played from memory: a slow TTS
    # does not keep the caller waiting for "Einen Moment, ich schaue nach."
    await call.until(lambda: fakes.filler_requests == 2)  # both holding lines
    fakes.stt_texts = ["Können Sie meine Rechnung schicken?"]
    fakes.llm_replies = [("delegate", "Rechnung schicken")]
    call.port.say(tone(1.0))
    await call.until(lambda: call.transcripts("assistant"))
    await call.quiet()
    assert call.transcripts("assistant") == [FILLER]
    assert fakes.filler_requests == 2
    assert call.port.played_ms() >= 0.8 * len(FILLER) * fakes.ms_per_char
    turn = call.engine.timings.turns[0]
    assert turn.tts_first_audio_ms == 0


@pytest.mark.asyncio
async def test_a_spoken_checking_line_replaces_the_filler(call, fakes):
    fakes.stt_texts = ["Können Sie meine Rechnung schicken?"]
    fakes.llm_replies = [("delegate", "Rechnung schicken", "Ich frage kurz nach.")]
    call.port.say(tone(1.0))
    await call.until(lambda: call.index("session.delegation.created") >= 0)
    await call.quiet()
    assert "".join(call.transcripts("assistant")).strip() == "Ich frage kurz nach."


@pytest.mark.asyncio
async def test_commentary_is_said_and_thinking_stays_silent(call, fakes):
    fakes.llm_replies = ["Ein Kollege ruft Sie heute noch zurück."]
    await call.append("thinking", "Der Anrufer ist Stammgast.")
    await asyncio.sleep(0.5)
    assert fakes.llm_requests == [] and call.transcripts("assistant") == []
    await call.append(
        "commentary", "Sag dem Anrufer, dass ein Kollege heute zurückruft."
    )
    await call.until(lambda: call.transcripts("assistant"))
    await call.quiet()
    messages = fakes.llm_requests[0]["messages"]
    notes = [m["content"] for m in messages[1:] if m["role"] == "system"]
    assert any("Stammgast" in n and "do not answer it now" in n for n in notes)
    assert any(
        "Kollege heute zurückruft" in n and "in your own words" in n for n in notes
    )
    assert "zurück" in "".join(call.transcripts("assistant"))


@pytest.mark.asyncio
async def test_steering_instructions_go_to_the_llm(call, fakes):
    fakes.llm_replies = ["Auf Wiederhören!"]
    await call.append("instructions", "Say a short, polite goodbye to the caller now.")
    await call.until(lambda: call.transcripts("assistant"))
    assert "follow it now" in fakes.llm_requests[0]["messages"][-1]["content"]


@pytest.mark.asyncio
async def test_barge_in_flushes_and_trims_the_context_to_what_was_heard(call, fakes):
    fakes.ms_per_char = 60
    long_reply = (
        "Das Frühstück gibt es von halb sieben bis halb elf im Wintergarten. "
        "Am Wochenende gibt es das Frühstück sogar bis elf Uhr für alle Gäste."
    )
    fakes.stt_texts = ["Wann gibt es Frühstück?", "Und am Sonntag?"]
    fakes.llm_replies = [long_reply, "Am Sonntag bis elf Uhr."]
    call.port.say(tone(1.0))
    await call.until(lambda: call.transcripts("assistant"))
    await asyncio.sleep(1.5)  # the caller hears part of the first sentence
    heard_before = call.port.played_ms()
    call.port.say(tone(1.0))  # talks over the voice for a second
    await call.until(lambda: len(fakes.llm_requests) == 2, timeout=10)
    played = call.port.played_ms()
    total = len(long_reply) * fakes.ms_per_char
    assert played < heard_before + 1500 < total
    messages = fakes.llm_requests[1]["messages"]
    said = [
        m["content"] for m in messages if m["role"] == "assistant" and m.get("content")
    ]
    assert said and said[-1].endswith(" …")
    assert said[-1].startswith("Das Frühstück") and "Wochenende" not in said[-1]
    assert messages[-1] == {"role": "user", "content": "Und am Sonntag?"}


@pytest.mark.asyncio
async def test_backchannel_over_the_voice_is_ignored(call, fakes):
    fakes.ms_per_char = 60
    fakes.stt_texts = ["Wann gibt es Frühstück?", "Mhm."]
    fakes.llm_replies = [
        "Das Frühstück gibt es jeden Tag von halb sieben bis halb elf im Wintergarten."
    ]
    call.port.say(tone(1.0))
    await call.until(lambda: call.transcripts("assistant"))
    await asyncio.sleep(0.8)
    call.port.say(tone(0.3))  # "mhm", shorter than the barge-in threshold
    await call.quiet(0.8)
    assert fakes.stt_calls == 2
    assert call.transcripts("caller") == ["Wann gibt es Frühstück?"]
    assert len(fakes.llm_requests) == 1
    assert call.port.played_ms() >= 0.9 * len(fakes.tts_inputs[0]) * fakes.ms_per_char


def test_heard_prefix_counts_words_by_length():
    assert heard_prefix("Guten Tag zusammen", 0.0) is None
    assert heard_prefix("Guten Tag zusammen", 0.3) == "Guten"
    assert heard_prefix("Guten Tag zusammen", 0.5) == "Guten Tag"
    assert heard_prefix("Guten Tag zusammen", 1.0) == "Guten Tag zusammen"


@pytest.mark.asyncio
async def test_a_hangup_while_the_pipeline_starts_ends_promptly(fakes):
    call = Call(fakes, fakes.base)
    await call.accept()
    started = asyncio.get_running_loop().time()
    await call.close()
    assert asyncio.get_running_loop().time() - started < 2
    assert call.engine._task.done()


# =============================================================================
# Several open requests, the line, and a long wait (owner's call 2026-10-22)
# =============================================================================


def quiet_before(call, said: list[str], n: int) -> int:
    """Milliseconds of quiet before the voice's ``n``-th sentence (the fake
    TTS makes ``ms_per_char`` of audio per character). The engine's line is
    free up to the playout lead before the audio ends: a quiet of ``x``
    seconds can measure 250 ms short."""
    starts = call.port.voice_starts
    return starts[n] - starts[n - 1] - len(said[n - 1]) * call.fakes.ms_per_char


def filler_to_hold(call) -> int:
    """Milliseconds of quiet between the filler and the hold sentence after
    it, the voice's last two starts."""
    starts = call.port.voice_starts
    return starts[-1] - starts[-2] - len(FILLER) * call.fakes.ms_per_char


def delegation_ids(call) -> list[str]:
    return [
        e["delegation"]["id"]
        for e in call.events
        if e["type"] == "session.delegation.created"
    ]


def contents(messages) -> str:
    return "\n".join(
        m["content"] for m in messages if isinstance(m.get("content"), str)
    )


async def _cut_off_then_completed(call, fakes):
    """The caller pauses, is cut off, and completes the request after the
    delegation went out; the voice hands on the rest."""
    fakes.stt_texts = [
        "Fünf Personen, drei Tage ab Montag, Montag bis",
        "Mittwoch, wahrscheinlich.",
    ]
    fakes.llm_replies = [
        ("delegate", "Fünf Personen ab Montag"),
        ("delegate", "Abreise Mittwoch"),
    ]
    call.port.say(tone(1.0))
    await call.until(lambda: len(delegation_ids(call)) == 1)
    await call.quiet()
    call.port.say(tone(1.0))
    await call.until(lambda: len(delegation_ids(call)) == 2)
    await call.quiet()
    return delegation_ids(call)


def test_the_holding_sentence_and_its_call_are_one_message():
    # Two assistant messages in a row (Pipecat's shape) made Gemma 4 loop on
    # empty thought markers when the caller spoke while the call ran.
    call = {"id": "c1", "type": "function", "function": {"name": "delegate"}}
    messages = [
        {"role": "system", "content": "rules"},
        {"role": "user", "content": "Montag bis"},
        {"role": "assistant", "content": FILLER},
        {"role": "assistant", "tool_calls": [call]},
        async_tool_messages.build_started_message("c1"),
        {"role": "user", "content": "Mittwoch."},
    ]
    out = note_messages(messages, "system")
    assert out[2] == {"role": "assistant", "content": FILLER, "tool_calls": [call]}
    assert [m["role"] for m in out] == ["system", "user", "assistant", "tool", "user"]
    # A reply without a call stays as it is.
    plain = [messages[0], messages[1], {"role": "assistant", "content": "Gern."}]
    assert note_messages(plain, "system") == plain


def test_after_the_answer_the_holding_sentence_stays_before_its_call():
    # One message, Gemma 4's template put the sentence after the tool
    # response and closed the turn; the LLM reasoned before relaying the
    # answer (8 of 10, 2.3-5.2 s; once nothing at all). Two, it went on at
    # once (0 of 10, owner's call 2026-10-23).
    call = {"id": "c1", "type": "function", "function": {"name": "delegate"}}
    messages = [
        {"role": "system", "content": "rules"},
        {"role": "user", "content": "Ist am Freitag ein Doppelzimmer frei?"},
        {"role": "assistant", "content": FILLER},
        {"role": "assistant", "tool_calls": [call]},
        {"role": "tool", "tool_call_id": "c1", "content": '"Freitag frei."'},
    ]
    assert note_messages(messages, "system") == messages
    # And so they stay for the rest of the call: the cached prefix holds.
    later = [
        *messages,
        {"role": "assistant", "content": "Ja, am Freitag ist eins frei."},
        {"role": "user", "content": "Und Samstag?"},
    ]
    assert note_messages(later, "system") == later


def test_a_running_call_says_an_addition_is_a_new_call():
    # Pipecat's "Do not call it again" made Gemma 4 answer a caller's
    # addition with another holding sentence instead of handing it on.
    started = async_tool_messages.build_started_message("c1")
    final = async_tool_messages.build_final_result_message("c1", '"frei"')
    (tool, done) = note_messages([started, final], "system")
    assert json.loads(tool["content"])["description"] == HANDED_ON
    assert async_tool_messages.parse_message(tool).kind == "started"
    assert tool["tool_call_id"] == "c1"
    assert done["content"] == final["content"] and done["role"] == "system"
    plain = {"role": "tool", "tool_call_id": "c2", "content": "129 Euro"}
    assert note_messages([plain], "system") == [plain]


@pytest.mark.asyncio
async def test_a_late_addition_is_handed_on_without_a_second_holding_sentence(
    call, fakes
):
    first, second = await _cut_off_then_completed(call, fakes)
    assert first != second
    # One holding sentence for both requests.
    assert call.transcripts("assistant") == [FILLER]
    assert call.transcripts("caller") == [
        "Fünf Personen, drei Tage ab Montag, Montag bis",
        "Mittwoch, wahrscheinlich.",
    ]
    # The LLM saw the filler with its call when the caller went on.
    request = fakes.llm_requests[1]["messages"]
    with_call = [m for m in request if m.get("tool_calls")]
    assert with_call[0]["content"] == FILLER
    assert request[-1] == {"role": "user", "content": "Mittwoch, wahrscheinlich."}
    assert set(call.engine._pending) == {first, second}


@pytest.mark.asyncio
async def test_an_answer_with_an_addition_still_open_is_marked_incomplete(call, fakes):
    first, second = await _cut_off_then_completed(call, fakes)
    fakes.llm_replies = [
        "Für fünf Personen brauchen Sie zwei Zimmer.",
        "Von Montag bis Mittwoch sind zwei Zimmer frei.",
    ]
    await call.append("commentary", "Fünf Personen brauchen zwei Zimmer.", first)
    await call.until(lambda: len(fakes.llm_requests) == 3)
    await call.quiet()
    assert ADDITION_OPEN in contents(fakes.llm_requests[2]["messages"])
    await call.append("commentary", "Mo bis Mi: zwei Zimmer frei.", second)
    await call.until(lambda: len(fakes.llm_requests) == 4)
    await call.quiet()
    last = fakes.llm_requests[3]["messages"]
    assert "zwei Zimmer frei" in contents(last)
    assert contents(last).count(ADDITION_OPEN) == 1  # only the first answer
    # The second answer comes after what the voice said to the first: in
    # place of its placeholder it read as already answered.
    said = next(
        i
        for i, m in enumerate(last)
        if m["role"] == "assistant"
        and "brauchen Sie zwei Zimmer" in str(m.get("content"))
    )
    news = next(
        i for i, m in enumerate(last) if "zwei Zimmer frei" in str(m.get("content"))
    )
    assert news > said and last[news]["role"] == "system"
    assert "Mittwoch sind zwei Zimmer frei" in "".join(call.transcripts("assistant"))
    assert call.engine._pending == {}


@pytest.mark.asyncio
async def test_an_answer_to_the_addition_settles_the_earlier_request(call, fakes):
    first, second = await _cut_off_then_completed(call, fakes)
    fakes.llm_replies = ["Von Montag bis Mittwoch sind zwei Zimmer frei."]
    await call.append("commentary", "Mo bis Mi: zwei Zimmer frei.", second)
    await call.until(lambda: len(fakes.llm_requests) == 3)
    await call.quiet()
    # One reply for both: the earlier request is settled quietly with it.
    assert len(fakes.llm_requests) == 3
    messages = contents(fakes.llm_requests[2]["messages"])
    assert COVERED in messages and "zwei Zimmer frei" in messages
    assert ADDITION_OPEN not in messages
    assert call.engine._pending == {}
    # A late answer to the earlier one is still said, as a note.
    fakes.llm_replies = ["Und das Frühstück ist inbegriffen."]
    await call.append("commentary", "Frühstück inklusive.", first)
    await call.until(lambda: len(fakes.llm_requests) == 4)
    assert "tell the caller this now" in contents(fakes.llm_requests[3]["messages"])


@pytest.mark.asyncio
async def test_a_backchannel_while_waiting_gets_no_reply(call, fakes):
    # The LLM tended to invent the open answer for a bare "Okay.".
    fakes.stt_texts = ["Können Sie meine Rechnung schicken?", "Okay.", "Hallo?"]
    fakes.llm_replies = [("delegate", "Rechnung schicken"), "Ich schaue noch nach."]
    call.port.say(tone(1.0))
    await call.until(lambda: len(delegation_ids(call)) == 1)
    await call.quiet()
    call.port.say(tone(0.8))
    await call.until(lambda: "Okay." in call.transcripts("caller"))
    await call.quiet(1.0)
    assert len(fakes.llm_requests) == 1
    call.port.say(tone(0.8))  # not a backchannel: answered
    await call.until(lambda: len(fakes.llm_requests) == 2)
    await call.until(
        lambda: "Ich schaue noch nach."
        in " ".join(call.transcripts("assistant")).replace("  ", " ")
    )


@pytest.mark.asyncio
async def test_a_markup_loop_is_cut_off(call, fakes):
    fakes.word_delay = 0.02
    fakes.stt_texts = ["Danke."]
    fakes.llm_replies = [" ".join(["<|channel>thought<channel|>"] * 100)]
    call.port.say(tone(1.0))
    await call.until(lambda: fakes.llm_requests)
    started = asyncio.get_running_loop().time()
    await call.until(lambda: not call.engine._llm.generating, timeout=5)
    # 100 markers take 2 s to stream; the engine stops reading after ~8.
    assert asyncio.get_running_loop().time() - started < 1.0
    await call.quiet()
    assert call.transcripts("assistant") == [] and fakes.tts_inputs == []


@pytest.mark.asyncio
async def test_the_voice_does_not_speak_into_the_caller(call, fakes):
    # A status line from the back office while the caller talks waits until
    # the caller's turn and the voice's reply to it are over.
    fakes.stt_texts = ["Ich hätte gern ein Zimmer für nächste Woche."]
    fakes.llm_replies = ["Gern."]
    call.port.say(tone(2.0))
    await asyncio.sleep(0.8)
    await call.append(
        "instructions",
        'Say exactly: "Ich schaue gerade im Buchungssystem nach."',
    )
    await call.until(lambda: len(call.transcripts("assistant")) >= 2, timeout=15)
    await call.quiet()
    said = [t.strip() for t in call.transcripts("assistant")]
    assert said == ["Gern.", "Ich schaue gerade im Buchungssystem nach."]
    # Nothing played while the caller's clip ran.
    assert call.port.voice_starts and call.port.voice_starts[0] > call.port.clip_ends[0]


@pytest.mark.asyncio
async def test_a_long_wait_is_bridged_with_varied_hold_sentences(fakes):
    call = Call(fakes, fakes.base, hold_after_secs=0.6, hold_max=2, hold_later_secs=1.5)
    await call.accept()
    try:
        fakes.stt_texts = ["Ist am Freitag ein Doppelzimmer frei?"]
        fakes.llm_replies = [("delegate", "Doppelzimmer Freitag")]
        call.port.say(tone(1.0))
        await call.until(lambda: len(delegation_ids(call)) == 1)
        await call.until(lambda: len(call.transcripts("assistant")) >= 4, timeout=15)
        await asyncio.sleep(3.0)  # nothing after the "longer" sentence
        await call.quiet()
        said = [t.strip() for t in call.transcripts("assistant")]
        assert said[0] == FILLER
        holds = said[1:3]
        assert all(h in HOLD_PHRASES["de"] for h in holds)
        assert holds[0] != holds[1]
        assert said[3:] == [HOLD_LONGER["de"]]
        # Never into each other: a pause of at least hold_after_secs, and
        # once the call has had hold_max holding sentences (the filler is
        # the first), hold_later_secs.
        starts = call.port.voice_starts
        assert len(starts) == 4
        quiet = [quiet_before(call, said, n) for n in (1, 2, 3)]
        assert 350 <= quiet[0] < 1250 and min(quiet[1:]) >= 1250, quiet
        # The answer ends the wait; the next wait starts afresh.
        fakes.llm_replies = ["Ja, am Freitag ist ein Doppelzimmer frei."]
        await call.append("commentary", "Freitag frei.", delegation_ids(call)[0])
        await call.until(lambda: len(call.transcripts("assistant")) == 5)
        await asyncio.sleep(1.5)
        assert len(call.transcripts("assistant")) == 5
        assert call.engine._holds == 0 and not call.engine._longer_said
        assert call.engine._call_holds == 4
    finally:
        await call.close()


@pytest.mark.asyncio
async def test_after_the_calls_first_hold_sentences_later_ones_wait_longer(fakes):
    # Owner 2026-10-23: the hold sentences came after every lookup, like a
    # loop. Once a call has had hold_max holding sentences, the filler
    # included, the next waits longer.
    call = Call(
        fakes,
        fakes.base,
        hold_after_secs=0.5,
        hold_max=2,
        hold_later_secs=2.5,
        hold_longer="",
    )
    await call.accept()
    try:
        fakes.stt_texts = ["Ist am Freitag ein Doppelzimmer frei?"]
        fakes.llm_replies = [("delegate", "Doppelzimmer Freitag")]
        call.port.say(tone(1.0))
        await call.until(lambda: len(delegation_ids(call)) == 1)
        await call.until(lambda: len(call.transcripts("assistant")) == 2)
        await call.until(lambda: len(call.port.voice_starts) == 2)
        first = filler_to_hold(call)
        fakes.llm_replies = ["Ja, am Freitag ist eins frei."]
        await call.append("commentary", "Freitag frei.", delegation_ids(call)[0])
        await call.until(lambda: len(call.transcripts("assistant")) == 3)
        await call.quiet()

        fakes.stt_texts = ["Und am Samstag?"]
        fakes.llm_replies = [("delegate", "Doppelzimmer Samstag")]
        call.port.say(tone(1.0))
        await call.until(lambda: len(delegation_ids(call)) == 2)
        await call.until(lambda: len(call.transcripts("assistant")) == 4)
        await asyncio.sleep(1.5)
        assert len(call.transcripts("assistant")) == 4  # filler only, so far
        await call.until(lambda: len(call.transcripts("assistant")) == 5, timeout=6)
        await call.quiet()
        said = [t.strip() for t in call.transcripts("assistant")]
        assert said[1] in HOLD_PHRASES["de"] and said[4] in HOLD_PHRASES["de"]
        assert said[3] == FILLER
        later = filler_to_hold(call)
        assert 250 <= first < 2250 and later >= 2250, (first, later)
    finally:
        await call.close()


@pytest.mark.asyncio
async def test_the_back_offices_line_takes_the_next_hold_sentences_place(fakes):
    # Owner's call 2026-10-23: the back office's "ich prüfe gerade den
    # Wetterdienst" came 3 s after the filler, then the hold sentences.
    call = Call(
        fakes,
        fakes.base,
        hold_after_secs=0.8,
        hold_max=2,
        hold_later_secs=2.0,
        hold_longer="",
    )
    await call.accept()
    try:
        fakes.stt_texts = ["Wie wird das Wetter nächste Woche?"]
        fakes.llm_replies = [("delegate", "Wetter nächste Woche")]
        call.port.say(tone(1.0))
        await call.until(lambda: len(delegation_ids(call)) == 1)
        await call.until(lambda: call.transcripts("assistant"))
        await call.append(
            "instructions", 'Say exactly: "Ich prüfe gerade den Wetterdienst."'
        )
        await call.until(lambda: len(call.transcripts("assistant")) == 2, timeout=5)
        await call.until(lambda: len(call.transcripts("assistant")) == 3, timeout=8)
        await call.until(lambda: len(call.port.voice_starts) == 3)
        said = [t.strip() for t in call.transcripts("assistant")]
        assert said[:2] == [FILLER, "Ich prüfe gerade den Wetterdienst."]
        assert said[2] in HOLD_PHRASES["de"]
        assert len(fakes.llm_requests) == 1
        # The line comes after the quiet a hold sentence waits for, and the
        # filler and the line are the call's hold_max: the next one waits
        # hold_later_secs.
        quiet = [quiet_before(call, said, n) for n in (1, 2)]
        assert 550 <= quiet[0] < 1750 and quiet[1] >= 1750, quiet
    finally:
        await call.close()


@pytest.mark.asyncio
async def test_an_answer_drops_the_back_offices_line_not_said_yet(fakes):
    call = Call(fakes, fakes.base, hold_after_secs=1.5)
    await call.accept()
    try:
        fakes.stt_texts = ["Wie wird das Wetter nächste Woche?"]
        fakes.llm_replies = [
            ("delegate", "Wetter nächste Woche"),
            "Nächste Woche wird es sonnig.",
        ]
        call.port.say(tone(1.0))
        await call.until(lambda: len(delegation_ids(call)) == 1)
        await call.append(
            "instructions", 'Say exactly: "Ich prüfe gerade den Wetterdienst."'
        )
        await call.append("commentary", "Sonnig.", delegation_ids(call)[0])
        await call.until(lambda: len(fakes.llm_requests) == 2)
        await asyncio.sleep(2.5)
        await call.quiet()
        said = " ".join(call.transcripts("assistant"))
        assert "sonnig" in said and "Wetterdienst" not in said
    finally:
        await call.close()


@pytest.mark.asyncio
async def test_a_note_while_the_caller_speaks_goes_into_the_reply_to_them(call, fakes):
    # Owner's call 2026-10-23: the late answer came in while the caller
    # spoke, waited for the reply to them, which guessed it, and was then
    # said again.
    fakes.stt_texts = ["Ich wollte eigentlich wissen, wie das Wetter wird."]
    fakes.llm_replies = ["Dazu habe ich leider keine verlässlichen Wetterdaten."]
    call.port.say(tone(2.0))
    await call.until(lambda: call.engine._caller_speaking)
    await call.append("commentary", "Keine verlässlichen Wetterdaten.")
    await call.until(lambda: call.transcripts("assistant"), timeout=10)
    await asyncio.sleep(1.0)
    await call.quiet()
    assert len(fakes.llm_requests) == 1
    messages = fakes.llm_requests[0]["messages"]
    note_at = next(
        i for i, m in enumerate(messages) if "Keine verlässlichen" in str(m["content"])
    )
    caller_at = next(i for i, m in enumerate(messages) if m["role"] == "user")
    assert note_at < caller_at == len(messages) - 1
    # Nothing played while the caller's clip ran.
    assert call.port.voice_starts[0] > call.port.clip_ends[0]


@pytest.mark.asyncio
async def test_a_note_taken_in_while_the_caller_speaks_is_said_without_a_reply(
    call, fakes
):
    fakes.stt_texts = [""]  # a noise: no words, no turn to reply to
    fakes.llm_replies = ["Ein Kollege ruft Sie heute noch zurück."]
    call.port.say(tone(1.5))
    await call.until(lambda: call.engine._caller_speaking)
    await call.append("commentary", "Ein Kollege ruft heute zurück.")
    await call.until(lambda: call.transcripts("assistant"), timeout=15)
    await asyncio.sleep(1.0)
    await call.quiet()
    assert len(fakes.llm_requests) == 1
    assert "zurück" in "".join(call.transcripts("assistant"))
    assert call.port.voice_starts[0] > call.port.clip_ends[0]


@pytest.mark.asyncio
async def test_hold_after_zero_never_bridges(fakes):
    call = Call(fakes, fakes.base, hold_after_secs=0)
    await call.accept()
    try:
        fakes.stt_texts = ["Ist am Freitag ein Doppelzimmer frei?"]
        fakes.llm_replies = [("delegate", "Doppelzimmer Freitag")]
        call.port.say(tone(1.0))
        await call.until(lambda: len(delegation_ids(call)) == 1)
        await asyncio.sleep(3.0)
        assert [t.strip() for t in call.transcripts("assistant")] == [FILLER]
        assert fakes.hold_requests == 0
    finally:
        await call.close()


@pytest.mark.asyncio
async def test_a_sentence_the_tts_still_synthesises_keeps_the_line_busy():
    # With a busy TTS a sentence took 7 s to start on the bench; counting
    # only audible audio, a hold sentence went out in that gap.
    import time

    from pipecat.frames.frames import AggregatedTextFrame

    from voice_gateway.engines.cascade.transport import CallOutput, transport_params

    output = CallOutput(
        FileAudioPort(),
        transport_params(),
        lead_ms=200,
        on_sentence=lambda text: None,
        on_first_audio=lambda: None,
        sentence_ms=lambda context_id: None,
    )
    assert not output.speaking()
    frame = AggregatedTextFrame("Das Doppelzimmer ist frei.", "sentence", "ctx-1")
    frame.will_be_spoken = True
    await output.write_transport_frame(frame)
    assert output.speaking() and not output.audible()
    # A TTS that never answers does not keep the line busy for good.
    assert not output.speaking(now=time.monotonic() + 16)


@pytest.mark.asyncio
async def test_no_second_holding_sentence_right_after_one(call, fakes):
    # Bench: the LLM said "Einen Moment, ich schaue nach.", the caller went
    # on before its call went out, and the call with the rest of the request
    # came seconds later: the filler then made it two holding sentences.
    fakes.stt_texts = ["Ein Zimmer für fünf Personen", "bis Mittwoch, bitte."]
    fakes.llm_replies = [FILLER, ("delegate", "Fünf Personen bis Mittwoch")]
    call.port.say(tone(1.0))
    await call.until(lambda: call.transcripts("assistant"))
    await call.quiet()
    call.port.say(tone(1.0))
    await call.until(lambda: len(delegation_ids(call)) == 1)
    await call.quiet()
    assert [t.strip() for t in call.transcripts("assistant")] == [FILLER]


async def _delegated(call, fakes, question: str) -> str:
    """The caller asks; the voice hands it on with the filler."""
    fakes.stt_texts = [question]
    fakes.llm_replies = [("delegate", question)]
    call.port.say(tone(1.0))
    await call.until(lambda: len(delegation_ids(call)) == 1)
    await call.quiet()
    return delegation_ids(call)[0]


THOUGHT = "<|channel>thought<channel|>"


@pytest.mark.asyncio
async def test_an_answer_the_reply_leaves_unsaid_is_asked_for_again(call, fakes):
    # Gemma 4 reasoned after an answer and said nothing (§9.14).
    delegation = await _delegated(call, fakes, "Gibt es Housekeeping?")
    fakes.llm_replies = [THOUGHT, "Ja, die Zimmer werden täglich gereinigt."]
    await call.append("commentary", "Zimmerreinigung täglich.", delegation)
    await call.until(lambda: "täglich" in "".join(call.transcripts("assistant")))
    await call.quiet()
    assert len(fakes.llm_requests) == 3
    again = fakes.llm_requests[2]["messages"][-1]
    assert "Zimmerreinigung täglich." in again["content"]
    assert "tell the caller this now" in again["content"]
    assert "Zimmerreinigung" not in "".join(fakes.tts_inputs)


@pytest.mark.asyncio
async def test_an_answer_left_unsaid_twice_is_spoken_as_it_came(call, fakes):
    delegation = await _delegated(call, fakes, "Wann ist die Rezeption besetzt?")
    fakes.llm_replies = [THOUGHT, THOUGHT]
    await call.append("commentary", "Die Rezeption ist bis 22 Uhr besetzt.", delegation)
    await call.until(lambda: "22 Uhr" in "".join(call.transcripts("assistant")))
    await call.quiet()
    assert len(fakes.llm_requests) == 3
    assert "Die Rezeption ist bis 22 Uhr besetzt." in fakes.tts_inputs
    assert call.engine._answer_owed is None


@pytest.mark.asyncio
async def test_a_said_answer_needs_no_net(call, fakes):
    delegation = await _delegated(call, fakes, "Gibt es Parkplätze?")
    fakes.llm_replies = ["Ja, es gibt zwanzig Parkplätze am Haus."]
    await call.append("commentary", "20 Parkplätze am Haus.", delegation)
    await call.until(lambda: "Parkplätze" in "".join(call.transcripts("assistant")))
    await call.quiet()
    await asyncio.sleep(0.5)
    assert len(fakes.llm_requests) == 2
    assert call.engine._answer_owed is None


@pytest.mark.asyncio
async def test_an_answer_with_an_addition_open_may_stay_unsaid(call, fakes):
    first, _second = await _cut_off_then_completed(call, fakes)
    fakes.llm_replies = [THOUGHT]
    await call.append("commentary", "Fünf Personen brauchen zwei Zimmer.", first)
    await call.until(lambda: len(fakes.llm_requests) == 3)
    await call.quiet()
    await asyncio.sleep(0.5)
    # "say only what it already settles, or nothing yet" (prompt rules).
    assert len(fakes.llm_requests) == 3
    assert "zwei Zimmer" not in "".join(fakes.tts_inputs)
