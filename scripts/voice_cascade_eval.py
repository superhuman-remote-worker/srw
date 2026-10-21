"""Answer or delegate: the voice LLM on the German test set; local_voice_gateway.md §2.2, §6.1 (WP3).

    PYTHONPATH=src .venv-voice/bin/python scripts/voice_cascade_eval.py \\
        --profile-file docker/voice-models/cascade-cpu.yaml [--from-audio]

Sends every ``utterances.yaml`` entry, as the caller's first turn after the
greeting, to the profile's voice LLM with the engine's real system prompt
(engine rules, then the test set's instructions, then Pipecat's async-tool
note) and the ``delegate`` tool. A reply counts as right when:

- ``answer``: no tool call, and the reply has the keywords;
- ``delegate`` / ``transfer`` / ``message``: a ``delegate`` call (the back
  office transfers and takes messages); the request's keywords are scored
  separately as "details". A voice that asks for details first (a question,
  no call) gets one more caller turn with a name and number, reported as
  "then delegated".

Wording (A1 findings, WP4) is scored on every reply: the caller never hears
the back office named; with a ``delegate`` call the voice says at most one
holding sentence and promises no transfer or forwarding ("Ich verbinde
Sie") before the back office decided. Each delegation then gets the
back office's canned answer as the tool result, and the reply that relays
it must name no back office and add no new holding sentence.

``--from-audio`` first transcribes each phone clip with the profile's STT,
so the score includes what STT makes of the test voice. Reports accuracy
per expected class and writes ``report.json`` to ``--out``.
"""

from __future__ import annotations

import argparse
import asyncio
import io
import json
import re
import time
import wave
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path

import aiohttp
import voice_cascade_testset as testset
from pipecat.processors.aggregators.async_tool_messages import ASYNC_TOOL_INSTRUCTIONS

from voice_gateway.engines.cascade import parse_settings
from voice_gateway.engines.cascade.fileport import read_wav
from voice_gateway.engines.cascade.prompt import (
    DELEGATE_TOOL,
    delegate_schema,
    system_prompt,
)
from voice_gateway.engines.cascade.services import note_messages


def tool() -> dict:
    schema = delegate_schema()
    return {
        "type": "function",
        "function": {
            "name": schema["name"],
            "description": schema["description"],
            "parameters": {
                "type": "object",
                "properties": schema["properties"],
                "required": schema["required"],
            },
        },
    }


def headers(stage) -> dict:
    return {"Authorization": f"Bearer {stage.api_key}"} if stage.api_key else {}


async def transcribe(http, settings, utterance_id: str) -> str:
    buffer = io.BytesIO()
    with wave.open(buffer, "wb") as wav:
        wav.setnchannels(1)
        wav.setsampwidth(2)
        wav.setframerate(16_000)
        wav.writeframes(read_wav(testset.clip(utterance_id)))
    form = aiohttp.FormData()
    form.add_field(
        "file", buffer.getvalue(), filename="caller.wav", content_type="audio/wav"
    )
    for key, value in (
        ("model", settings.stt.model),
        ("language", settings.language),
        ("response_format", "json"),
        ("temperature", "0"),
    ):
        form.add_field(key, value)
    async with http.post(
        settings.stt.base_url + "/audio/transcriptions",
        data=form,
        headers=headers(settings.stt),
    ) as response:
        return (await response.json(content_type=None)).get("text", "").strip()


#: Wording the caller must never hear (A1): the back office named, or a
#: transfer or forwarding promised before the back office decided.
BACK_OFFICE = re.compile(r"back[\s-]*office", re.IGNORECASE)
HANDOVER = re.compile(
    r"\b(verbinde|durchstell|stelle\s+\w+\s+durch|weiterleit|weitergeleitet"
    r"|leite\b[^.!?]*\bweiter|gebe\b[^.!?]*\bweiter|weitergeb)",
    re.IGNORECASE,
)
#: A holding phrase: the voice says it is checking instead of answering.
HOLDING = re.compile(
    r"\b(moment|schaue|prüfe|frage\b[^.!?]*\bnach|kläre|kümmere)", re.IGNORECASE
)


def sentences(text: str) -> list[str]:
    return [s for s in re.split(r"(?<=[.!?])\s+", text.strip()) if s.strip()]


def wording(reply: str, delegated: bool) -> list[str]:
    """What is wrong with a reply's wording; empty when it is fine."""
    faults = []
    if BACK_OFFICE.search(reply):
        faults.append("names_back_office")
    if delegated and HANDOVER.search(reply):
        faults.append("promises_handover")
    holding = [s for s in sentences(reply) if HOLDING.search(s) or HANDOVER.search(s)]
    if delegated and len(holding) > 1:
        faults.append("two_holding_sentences")
    return faults


def relay_wording(reply: str) -> list[str]:
    """Faults of the reply that tells the caller the back office's answer."""
    faults = []
    if not reply.strip():
        faults.append("silent")
    if BACK_OFFICE.search(reply):
        faults.append("names_back_office")
    if HOLDING.search(reply):
        faults.append("holds_again")
    return faults


#: The caller's second turn when the voice asked for details first.
FOLLOW_UP = "Mein Name ist Jana Probe, meine Nummer ist 0150 123456."


async def relay(
    http, settings, system: str, turns: list[dict], message: dict, answer: str
) -> dict:
    """The reply once the back office answered: the delegate call's result
    in place, as Pipecat writes it when the answer comes before anything else."""
    call = next(
        c for c in message["tool_calls"] if c["function"]["name"] == DELEGATE_TOOL
    )
    # Pipecat's context: what the voice said (the engine's filler when the
    # LLM said nothing), then the call on its own.
    said = (message.get("content") or "").strip() or settings.filler
    turns = [
        *turns,
        *([{"role": "assistant", "content": said}] if said else []),
        {"role": "assistant", "content": None, "tool_calls": [call]},
        {
            "role": "tool",
            "tool_call_id": call["id"],
            "content": json.dumps(answer, ensure_ascii=False),
        },
    ]
    second, _ = await ask(http, settings, system, turns)
    reply = (second.get("content") or "").strip()
    return {"reply": reply, "faults": relay_wording(reply)}


async def ask(http, settings, system: str, turns: list[dict]) -> tuple[dict, float]:
    messages = [
        {"role": "system", "content": system},
        {"role": "assistant", "content": testset.GREETING},
        *turns,
    ]
    body = {
        "model": settings.llm.model,
        "messages": note_messages(messages, settings.llm.note_role),
        "tools": [tool()],
        "temperature": settings.llm.temperature,
        "max_tokens": settings.llm.max_tokens,
        **settings.llm.extra_body,
    }
    started = time.monotonic()
    async with http.post(
        settings.llm.base_url + "/chat/completions",
        json=body,
        headers=headers(settings.llm),
    ) as response:
        response.raise_for_status()
        data = await response.json()
    return data["choices"][0]["message"], time.monotonic() - started


def judge(utterance: dict, message: dict) -> dict:
    calls = [
        c
        for c in message.get("tool_calls") or []
        if c["function"]["name"] == DELEGATE_TOOL
    ]
    reply = (message.get("content") or "").strip()
    request = ""
    if calls:
        try:
            request = json.loads(calls[0]["function"]["arguments"]).get("request", "")
        except (ValueError, AttributeError):
            request = calls[0]["function"]["arguments"]
    expect = utterance["expect"]
    keywords = utterance.get("keywords") or []
    if expect == "answer":
        right = not calls and testset.matches(reply, keywords)
        details = testset.matches(reply, keywords)
    else:
        right = bool(calls)
        details = bool(calls) and testset.matches(request + " " + reply, keywords)
    return {
        "got": "delegate" if calls else "answer",
        "right": right,
        "details": details,
        # Not delegated (yet), but asked the caller for something first.
        "asked_first": expect != "answer" and not calls and reply.endswith("?"),
        "reply": reply,
        "request": request,
        "wording": wording(reply, bool(calls)),
    }


async def run(args) -> dict:
    settings = parse_settings(
        args.profile, testset.load_profile(args.profile_file, args.profile)
    )
    system = "\n\n".join(
        [
            system_prompt(testset.instructions(), settings.language, settings.prompt),
            ASYNC_TOOL_INSTRUCTIONS,
        ]
    )
    results = []
    timeout = aiohttp.ClientTimeout(total=120)
    async with aiohttp.ClientSession(timeout=timeout) as http:
        for utterance in testset.utterances():
            said = (
                await transcribe(http, settings, utterance["id"])
                if args.from_audio
                else utterance["text"]
            )
            turns = [{"role": "user", "content": said}]
            message, seconds = await ask(http, settings, system, turns)
            verdict = judge(utterance, message)
            if verdict["got"] == "delegate":
                answer = testset.CANNED_ANSWERS.get(
                    utterance["id"], testset.DEFAULT_ANSWER
                )
                verdict["relay"] = await relay(
                    http, settings, system, turns, message, answer
                )
            if verdict["asked_first"]:
                # A message usually needs a name and a number first: does the
                # voice hand it over once the caller gave them?
                turns += [
                    {"role": "assistant", "content": verdict["reply"]},
                    {"role": "user", "content": FOLLOW_UP},
                ]
                second, _ = await ask(http, settings, system, turns)
                verdict["second_turn"] = judge(utterance, second)["got"]
            results.append(
                {
                    "id": utterance["id"],
                    "expect": utterance["expect"],
                    "said": said,
                    "seconds": round(seconds, 2),
                    **verdict,
                }
            )
            mark = "ok " if verdict["right"] else "BAD"
            shown = verdict["request"] or verdict["reply"]
            print(
                f"{mark} {utterance['id']:22s} {verdict['got']:8s} {seconds:4.1f}s | {said}\n    -> {shown}"
            )
            if verdict["got"] == "delegate" and verdict["reply"]:
                print(f"    says: {verdict['reply']}")
            if "relay" in verdict:
                print(f"    then: {verdict['relay']['reply']}")
            faults = verdict["wording"] + [
                "relay_" + f for f in verdict.get("relay", {}).get("faults", [])
            ]
            if faults:
                print(f"    WORDING {', '.join(faults)}")
    per_class = defaultdict(Counter)
    for item in results:
        per_class[item["expect"]]["n"] += 1
        per_class[item["expect"]]["right"] += item["right"]
        per_class[item["expect"]]["details"] += item["details"]
        per_class[item["expect"]]["asked"] += item["asked_first"]
        per_class[item["expect"]]["second"] += item.get("second_turn") == "delegate"
    summary = {
        expect: {
            "n": c["n"],
            "accuracy": round(c["right"] / c["n"], 2),
            "with_details": round(c["details"] / c["n"], 2),
            "asked_first": c["asked"],
            "delegated_on_second_turn": c["second"],
        }
        for expect, c in per_class.items()
    }
    total = sum(item["right"] for item in results)
    faults = Counter(f for item in results for f in item["wording"])
    faults.update(
        "relay_" + f
        for item in results
        for f in item.get("relay", {}).get("faults", [])
    )
    relayed = sum("relay" in item for item in results)
    print("\nexpect     n  right  with details  asked first  then delegated")
    for expect, row in summary.items():
        print(
            f"{expect:9s} {row['n']:2d}  {row['accuracy']:5.2f}  {row['with_details']:12.2f}"
            f"  {row['asked_first']:11d}  {row['delegated_on_second_turn']:15d}"
        )
    print(
        f"overall {total}/{len(results)}; median {sorted(r['seconds'] for r in results)[len(results) // 2]:.2f}s per reply"
    )
    print(
        f"wording faults over {len(results)} replies and {relayed} relayed answers: "
        + (", ".join(f"{k} {v}" for k, v in sorted(faults.items())) or "none")
    )
    return {
        "profile": args.profile,
        "from_audio": args.from_audio,
        "summary": summary,
        "wording": {"replies": len(results), "relayed": relayed, **dict(faults)},
        "results": results,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("--profile-file", type=Path, required=True)
    parser.add_argument("--profile", default="cascade-cpu")
    parser.add_argument("--from-audio", action="store_true")
    parser.add_argument("--out", type=Path)
    args = parser.parse_args()
    report = asyncio.run(run(args))
    out = (
        args.out
        or testset.REPO_ROOT
        / ".cache"
        / "voice-cascade-eval"
        / datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    )
    out.mkdir(parents=True, exist_ok=True)
    (out / "report.json").write_text(json.dumps(report, indent=2, ensure_ascii=False))
    print(f"report in {out}")


if __name__ == "__main__":
    main()
