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

``--from-audio`` first transcribes each phone clip with the profile's STT,
so the score includes what STT makes of the test voice. Reports accuracy
per expected class and writes ``report.json`` to ``--out``.
"""

from __future__ import annotations

import argparse
import asyncio
import io
import json
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


#: The caller's second turn when the voice asked for details first.
FOLLOW_UP = "Mein Name ist Jana Probe, meine Nummer ist 0150 123456."


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
    print("\nexpect     n  right  with details  asked first  then delegated")
    for expect, row in summary.items():
        print(
            f"{expect:9s} {row['n']:2d}  {row['accuracy']:5.2f}  {row['with_details']:12.2f}"
            f"  {row['asked_first']:11d}  {row['delegated_on_second_turn']:15d}"
        )
    print(
        f"overall {total}/{len(results)}; median {sorted(r['seconds'] for r in results)[len(results) // 2]:.2f}s per reply"
    )
    return {
        "profile": args.profile,
        "from_audio": args.from_audio,
        "summary": summary,
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
