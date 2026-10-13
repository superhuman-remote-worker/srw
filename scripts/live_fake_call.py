#!/usr/bin/env python3
"""Play one live session against a running SRW through the fake GPT-Live server.

The end-to-end gate for live-session jobs (design
knowledge-base/knowledge/features/live_session_jobs.md §5.7, §12). It runs
``tests/_fake_gpt_live.py`` on this machine, delivers a signed
``live.transport.incoming`` webhook to the phone-line connector's public
webhook, and plays the caller and the voice:

1. the line's connector pod accepts (the expert's persona and facts), attaches
   the sideband and sends the greeting instruction;
2. the voice greets and the caller asks something;
3. the job's voice subagent attaches to the pod (``live_sessions.attached_at``);
4. the caller asks for something only the back office can do and the voice
   delegates; the script waits for the job's answer (a
   ``session.commentary.append`` for that delegation that is not the pod's
   no-job fallback);
5. the caller hangs up; the job turns strategic and finishes.

Scenarios: ``cancel`` (an appointment cancellation, A3), ``human`` (ask for a
person: transfer, A6), ``no-automation`` (no enabled live automation on the
connector: the pod rejects with SIP 480) and ``missed`` (the caller hangs up
before the job attaches: the job still turns strategic and finishes).

Prerequisites: a live_voice catalog model whose endpoint is
``http://<--advertise-host>:<--port>/v1`` with the API key ``--api-key``; a
``srw.gpt-live-sip/v1`` connector with that model and ``--webhook-secret`` in
a project on a network tier that may reach private addresses; for every
scenario but ``no-automation``, an enabled live automation on it.

Example (k3d)::

    scripts/live_fake_call.py --orchestrator https://localhost --insecure \\
        --connector-id <uuid> --advertise-host 192.168.1.20 --port 18440 \\
        --webhook-secret-file ~/.config/srw/live-fake-webhook-secret
"""

from __future__ import annotations

import argparse
import asyncio
import os
import shlex
import subprocess
import sys
import time

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "src"))
sys.path.insert(0, REPO)

from tests._fake_gpt_live import FakeGptLive  # noqa: E402

PSQL = (
    "kubectl --context k3d-srw -n srw exec srw-postgres-0 -- "
    "psql -U srw -d srw -tAc"
)


def _step(message: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {message}", flush=True)


def _sql(args: argparse.Namespace, query: str) -> str:
    result = subprocess.run(
        [*shlex.split(args.psql), query],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    return result.stdout.strip()


async def _poll(predicate, *, timeout: float, interval: float = 1.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = await asyncio.to_thread(predicate)
        if value:
            return value
        await asyncio.sleep(interval)
    return None


def _session_row(args: argparse.Namespace, provider_session_id: str) -> list[str]:
    row = _sql(
        args,
        "SELECT id, status, coalesce(job_id::text,''), "
        "coalesce(attached_at::text,''), coalesce(close_reason,''), "
        "coalesce(error,'') FROM live_sessions "
        f"WHERE provider_session_id = '{provider_session_id}'",
    )
    return row.split("|") if row else []


async def play(args: argparse.Namespace) -> int:
    secret = args.webhook_secret
    if args.webhook_secret_file:
        with open(os.path.expanduser(args.webhook_secret_file)) as handle:
            secret = handle.read().strip()
    if not secret:
        _step("FAIL: a webhook secret is required (--webhook-secret-file)")
        return 2
    fake = FakeGptLive(api_key=args.api_key, host="0.0.0.0", port=args.port)
    await fake.start()
    _step(
        f"fake GPT-Live on :{fake.port}; the line's pod uses "
        f"http://{args.advertise_host}:{fake.port}/v1"
    )
    failures = 0
    try:
        webhook_url = (
            args.orchestrator.rstrip("/") + f"/api/live/webhooks/{args.connector_id}"
        )
        started = time.monotonic()
        provider_id = await fake.incoming_call(
            webhook_url=webhook_url,
            webhook_secret=secret,
            to_number=args.to_number,
            from_number=args.from_number,
            verify_ssl=not args.insecure,
        )
        _step(f"webhook delivered for {provider_id}: HTTP {fake.webhook_responses[-1]}")
        session = await fake.wait_until(
            provider_id, lambda s: s.status != "pending", timeout=15
        )
        if args.scenario == "no-automation":
            if session.status == "rejected" and session.reject_status == 480:
                _step("rejected with SIP 480, as expected")
            else:
                _step(f"FAIL: expected a 480 reject, got {session.status}")
                failures += 1
            row = await _poll(
                lambda: (r := _session_row(args, provider_id))
                and r[1] == "rejected"
                and r,
                timeout=10,
            )
            _step(f"live_sessions row: {row}")
            if not row or row[4] != "no_automation":
                failures += 1
            return 1 if failures else 0
        if session.status != "accepted":
            _step(
                f"FAIL: session was {session.status} (reject {session.reject_status})"
            )
            return 1
        config = session.accept_config or {}
        _step(
            f"accepted after {time.monotonic() - started:.1f}s; voice="
            f"{config.get('audio', {}).get('output', {}).get('voice')}; instructions "
            f"{len(config.get('instructions') or '')} chars"
        )
        await fake.wait_until(provider_id, lambda s: len(s.sockets) >= 1, timeout=10)
        greeting = await fake.wait_for_command(
            provider_id, "session.instructions.append", timeout=10
        )
        _step(
            f"pod attached; greeting after {time.monotonic() - started:.1f}s: "
            f"{greeting['content'][:100]!r}"
        )
        await fake.say(
            provider_id,
            "assistant",
            "Hello, you are speaking with an AI assistant. How can I help?",
        )

        if args.scenario == "missed":
            await fake.say(provider_id, "caller", "Never mind, I will call back.")
            await asyncio.sleep(0.5)
            await fake.remote_hangup(provider_id)
            _step("caller hung up before the job attached")
        else:
            await fake.say(provider_id, "caller", "When are you open?")
            _step(f"waiting up to {args.attach_timeout}s for the job to attach")
            row = await _poll(
                lambda: (r := _session_row(args, provider_id)) and r[3] and r,
                timeout=args.attach_timeout,
            )
            if row:
                _step(f"job {row[2]} attached after {time.monotonic() - started:.1f}s")
            else:
                _step("FAIL: the job never attached")
                failures += 1
            request = (
                "I would like to speak to a person, please."
                if args.scenario == "human"
                else "I am ill and must cancel my appointment on Tuesday. My name is "
                "Erika Example."
            )
            await fake.say(provider_id, "caller", request)
            await fake.say(provider_id, "assistant", "One moment, I am checking.")
            delegation_id = await fake.delegate(provider_id)
            asked = time.monotonic()
            _step(f"delegation {delegation_id}: waiting for the back office")
            try:
                answer = await fake.wait_for_command(
                    provider_id,
                    "session.commentary.append",
                    lambda c: c.get("delegation_id") == delegation_id
                    and not str(c.get("event_id") or "").startswith("srw_fallback_"),
                    timeout=args.answer_timeout,
                )
                _step(
                    f"answer after {time.monotonic() - asked:.1f}s: "
                    f"{str(answer.get('content'))[:160]!r}"
                )
            except asyncio.TimeoutError:
                _step("FAIL: no answer from the back office for the delegation")
                failures += 1
            if args.scenario == "human":
                try:
                    await fake.wait_until(
                        provider_id, lambda s: s.refer_target is not None, timeout=90
                    )
                    _step(f"transferred to {fake.session(provider_id).refer_target}")
                except asyncio.TimeoutError:
                    _step("FAIL: the call was not transferred")
                    failures += 1
            await fake.say(provider_id, "caller", "Thank you, goodbye.")
            await asyncio.sleep(1.0)
            if fake.session(provider_id).status != "closed":
                await fake.remote_hangup(provider_id)
            _step("caller hung up")

        row = await _poll(
            lambda: (r := _session_row(args, provider_id))
            and r[1] in ("ended", "failed")
            and r,
            timeout=30,
        )
        _step(f"live_sessions row: {row}")
        if not row or row[1] != "ended":
            failures += 1
        job_id = row[2] if row else ""
        if job_id:
            _step(f"waiting up to {args.finish_timeout}s for job {job_id} to finish")
            status = await _poll(
                lambda: (
                    s := _sql(args, f"SELECT status FROM jobs WHERE id = '{job_id}'")
                )
                in ("completed", "pending_review", "failed", "paused")
                and s,
                timeout=args.finish_timeout,
                interval=5,
            )
            _step(f"job status: {status or 'still running'}")
            if status not in ("completed", "pending_review"):
                failures += 1
            usage = _sql(
                args,
                "SELECT usage_seconds FROM live_sessions "
                f"WHERE provider_session_id = '{provider_id}'",
            )
            _step(f"usage_seconds: {usage or 'none'}")
        sent = [c["type"] for c in fake.session(provider_id).commands]
        _step(f"commands received: {sent}")
        return 1 if failures else 0
    finally:
        await fake.stop()


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--orchestrator", required=True)
    parser.add_argument("--connector-id", required=True)
    parser.add_argument("--advertise-host", required=True)
    parser.add_argument("--port", type=int, default=18440)
    parser.add_argument(
        "--scenario",
        choices=("cancel", "human", "no-automation", "missed"),
        default="cancel",
    )
    parser.add_argument("--webhook-secret", default="")
    parser.add_argument("--webhook-secret-file", default="")
    parser.add_argument("--api-key", default="sk-fake-gpt-live")
    parser.add_argument("--to-number", default="+4960000000000")
    parser.add_argument("--from-number", default="+4917600000000")
    parser.add_argument("--attach-timeout", type=float, default=180.0)
    parser.add_argument("--answer-timeout", type=float, default=120.0)
    parser.add_argument("--finish-timeout", type=float, default=600.0)
    parser.add_argument("--psql", default=PSQL, help="Command prefix running SQL")
    parser.add_argument("--insecure", action="store_true")
    return asyncio.run(play(parser.parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
