#!/usr/bin/env python3
"""Play one phone call against a running SRW through the fake GPT-Live server.

The local end-to-end gate for phone calls (design
knowledge-base/knowledge/features/phone_calls_as_subagents.md §7.2): it runs
``tests/_fake_gpt_live.py`` on this machine, delivers a signed
``live.transport.incoming`` webhook to the orchestrator, and then plays the
caller and the voice:

1. the orchestrator front accepts (persona + fact sheet) and its recorder
   attaches and sends the greeting instruction;
2. the voice greets, the caller asks a fact-sheet question and the voice
   answers it;
3. the job's phone subagent attaches (a second sideband);
4. the caller asks for something only the back office can do and the voice
   delegates; the script waits for the job's answer
   (``session.commentary.append`` with that delegation id);
5. the caller says goodbye and hangs up.

Prerequisites: a ``telephony_settings`` row whose ``api_base``/``ws_base``
point at ``http://<--advertise-host>:<--port>/v1`` (``ws://`` for ws_base),
whose ``webhook_secret`` is ``--webhook-secret`` and whose API key is
``--api-key`` (the fake checks it), plus an enabled default line.

Example (k3d; pods reach this host on the k3d docker network gateway)::

    scripts/telephony_fake_call.py --orchestrator http://127.0.0.1:8085 \\
        --advertise-host 172.18.0.1 --port 18440 \\
        --webhook-secret whsec_... --api-key sk-fake-gpt-live
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "src"))
sys.path.insert(0, REPO)

from tests._fake_gpt_live import FakeGptLive  # noqa: E402


def _step(message: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {message}", flush=True)


async def play(args: argparse.Namespace) -> int:
    fake = FakeGptLive(api_key=args.api_key, host="0.0.0.0", port=args.port)
    await fake.start()
    _step(
        f"fake GPT-Live on :{fake.port}; pods use http://{args.advertise_host}:{fake.port}/v1"
    )
    failures = 0
    try:
        webhook_url = args.orchestrator.rstrip("/") + "/api/telephony/openai/webhook"
        started = time.monotonic()
        session_id = await fake.incoming_call(
            webhook_url=webhook_url,
            webhook_secret=args.webhook_secret,
            to_number=args.to_number,
            from_number=args.from_number,
            verify_ssl=not args.insecure,
        )
        _step(f"webhook delivered for {session_id}: HTTP {fake.webhook_responses[-1]}")
        session = await fake.wait_until(
            session_id, lambda s: s.status != "pending", timeout=15
        )
        if session.status != "accepted":
            _step(
                f"FAIL: call was {session.status} (reject status {session.reject_status})"
            )
            return 1
        accepted_s = time.monotonic() - started
        instructions = (session.accept_config or {}).get("instructions") or ""
        _step(
            f"accepted after {accepted_s:.1f}s; voice={session.accept_config['audio']['output']['voice']}; instructions {len(instructions)} chars"
        )

        await fake.wait_until(session_id, lambda s: len(s.sockets) >= 1, timeout=10)
        greeting = await fake.wait_for_command(
            session_id, "session.instructions.append", timeout=10
        )
        _step(f"recorder attached; greeting instruction: {greeting['content'][:120]!r}")

        await fake.say(
            session_id,
            "assistant",
            "Guten Tag, hier ist der digitale Assistent der Reha-Kliniken Küppelsmühle. Ich bin eine künstliche Intelligenz. Wie kann ich Ihnen helfen?",
        )
        await fake.say(session_id, "caller", "Haben Sie ein Schwimmbad?")
        await fake.say(
            session_id, "assistant", "Ja, wir haben ein Sole-Bewegungsbad mit 32 Grad."
        )

        _step(
            f"waiting up to {args.attach_timeout}s for the job's phone subagent to attach"
        )
        try:
            await fake.wait_until(
                session_id, lambda s: s.attach_count >= 2, timeout=args.attach_timeout
            )
            _step(f"job attached after {time.monotonic() - started:.1f}s")
        except asyncio.TimeoutError:
            _step("FAIL: the job never attached")
            failures += 1

        await fake.say(
            session_id,
            "caller",
            "Ich bin krank und muss meinen Termin am Dienstag absagen. Mein Name ist Erika Muster.",
        )
        await fake.say(
            session_id, "assistant", "Einen Moment, ich gebe das an unser Büro weiter."
        )
        delegation_id = await fake.delegate(session_id)
        asked = time.monotonic()
        _step(
            f"delegation {delegation_id}: waiting up to {args.answer_timeout}s for the back office"
        )
        try:
            answer = await fake.wait_for_command(
                session_id,
                "session.commentary.append",
                lambda c: c.get("delegation_id") == delegation_id,
                timeout=args.answer_timeout,
            )
            content = str(answer.get("content") or "")
            _step(f"answer after {time.monotonic() - asked:.1f}s: {content!r}")
            if content.startswith("Sagen Sie dem Anrufer: Ich habe"):
                # The phone subagent's follow-up or the recorder's no-job
                # fallback: nobody in the back office answered in time.
                _step("FAIL: that is a fallback, not the back office's answer")
                failures += 1
        except asyncio.TimeoutError:
            _step("FAIL: no answer for the delegation")
            failures += 1

        await fake.say(
            session_id, "assistant", "Alles klar, ich habe das weitergegeben."
        )
        await fake.say(session_id, "caller", "Danke, auf Wiederhören.")
        await asyncio.sleep(1.0)
        await fake.remote_hangup(session_id)
        _step("caller hung up")
        await asyncio.sleep(2.0)
        sent = [c["type"] for c in fake.session(session_id).commands]
        _step(f"commands received: {sent}")
        return 1 if failures else 0
    finally:
        await fake.stop()


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--orchestrator", required=True, help="Base URL that reaches the orchestrator"
    )
    parser.add_argument(
        "--advertise-host",
        required=True,
        help="Host/IP the cluster uses to reach this machine",
    )
    parser.add_argument("--port", type=int, default=18440)
    parser.add_argument("--webhook-secret", required=True)
    parser.add_argument("--api-key", default="sk-fake-gpt-live")
    parser.add_argument("--to-number", default="+4960521829990")
    parser.add_argument("--from-number", default="+4917600000000")
    parser.add_argument("--attach-timeout", type=float, default=180.0)
    parser.add_argument("--answer-timeout", type=float, default=90.0)
    parser.add_argument(
        "--insecure",
        action="store_true",
        help="Skip TLS verification (k3d self-signed)",
    )
    return asyncio.run(play(parser.parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
