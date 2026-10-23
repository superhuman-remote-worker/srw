"""SRW's side of a call for the WP2 bench (``scripts/voice-bench.sh run --dut srw``).

Plays what the orchestrator and the gpt-live-sip driver do with a GPT-Live
engine, against the voice gateway's real API: receives the gateway's signed
``live.transport.incoming`` webhook (verified with the orchestrator's own
code), accepts the call with the ``echo`` profile, attaches to the session's
WebSocket, sends the greeting the way the driver does, and on DTMF 9 asks
for a transfer to the bench's human extension. Every step goes to a JSON
lines log the bench reads per profile; no audio passes through here.

With ``answers`` in the config it also plays a canned back office for a
cascade profile (WP4, without k3d): each ``session.delegation.created`` is
matched against what the caller said since the last delegation; the first
entry whose keywords all appear answers with its ``answer`` as
``commentary`` after ``answer_delay`` seconds, and an entry with
``transfer: true`` then asks for the transfer to ``transfer_target``.
With ``status`` (config-wide or per entry) it first sends that line as an
``instructions`` append to say word for word, ``status_delay`` seconds after
the delegation (a back office that says what it is doing before a slow
lookup). An entry's ``follow_up`` goes out as the delegation's
``commentary`` ``follow_up_delay`` seconds in, before the answer (SRW's
live driver after ``answer_timeout_s``).
``instructions`` and ``greeting`` replace the bench's accept instructions
and greeting append. Only event kinds and the matched entry are logged,
never what was said, unless ``log_transcripts`` asks for the transcripts
with their times (bench timelines of synthetic test speech).

For the test set's call level (``scripts/voice-eval.sh run --levels call``),
entries may carry their own ``delay``; ``answer_once`` uses each entry for
one delegation only; ``default_answer`` answers a delegation no entry
matches; and ``log_content`` also logs the transcripts and each delegation's
request (what the caller said since the last one, as SRW's driver forwards
it), the synthetic test caller's words only.

    python srw_driver.py --config driver.json   # inside the bench network
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time
import uuid
from pathlib import Path

import aiohttp
from aiohttp import web

from shared.runtime.live.webhook import (
    WebhookVerificationError,
    parse_incoming_call,
    verify_webhook,
)


def match_answer(
    answers: list[dict],
    heard: str,
    used: set[int] | None = None,
    default: str | None = None,
) -> tuple[int, dict] | None:
    """The first entry whose keyword groups all appear in what the caller
    said (``a|b`` for either; no keywords match anything); entries in
    ``used`` are skipped; ``default`` (index -1) when none matches."""
    said = " ".join(heard.lower().split())
    for index, entry in enumerate(answers):
        if used is not None and index in used:
            continue
        if all(
            any(k in said for k in group.lower().split("|"))
            for group in entry.get("keywords", [])
        ):
            return index, entry
    if default:
        return -1, {"answer": default}
    return None


class Driver:
    def __init__(self, config: dict):
        self.config = config
        self.key = Path(config["api_key_file"]).read_text().strip()
        self.secrets = {
            connector: Path(path).read_text().strip()
            for connector, path in config["connectors"].items()
        }
        self.log_path = Path(config["events"])
        self.http: aiohttp.ClientSession | None = None
        self.tasks: set[asyncio.Task] = set()
        self.answers: list[dict] = config.get("answers") or []
        self.used: set[int] = set()

    def log(self, kind: str, **data) -> None:
        line = json.dumps({"t": time.time(), "kind": kind, **data})
        with self.log_path.open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")

    async def webhook(self, request: web.Request) -> web.Response:
        connector = request.match_info["connector"]
        body = await request.read()
        secret = self.secrets.get(connector)
        if secret is None:
            return web.json_response({"error": "unknown connector"}, status=404)
        try:
            payload = verify_webhook(secret, request.headers, body)
            call = parse_incoming_call(payload, request.headers["webhook-id"])
        except (WebhookVerificationError, ValueError, KeyError) as exc:
            self.log("webhook_refused", connector=connector, error=str(exc))
            return web.json_response({"error": "unverified"}, status=400)
        if call is None:
            return web.json_response({"ok": True})
        self.log(
            "webhook",
            connector=connector,
            session_id=call.session_id,
            sip_headers=[{"name": n, "value": v} for n, v in call.sip_headers],
        )
        self.spawn(self.handle(call.session_id))
        return web.json_response({"ok": True})

    def spawn(self, coroutine) -> None:
        task = asyncio.create_task(coroutine)
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)

    def match(self, heard: str) -> tuple[int, dict] | None:
        """The canned answer for what the caller said, by keywords."""
        found = match_answer(
            self.answers,
            heard,
            self.used if self.config.get("answer_once") else None,
            self.config.get("default_answer"),
        )
        if found is not None and found[0] >= 0:
            self.used.add(found[0])
        return found

    async def back_office(self, ws, sid: str, delegation_id: str, heard: str) -> None:
        found = self.match(heard)
        extra = {"request": heard} if self.config.get("log_content") else {}
        self.log(
            "delegation",
            session_id=sid,
            delegation_id=delegation_id,
            answer=None if found is None else found[0],
            **extra,
        )
        if found is None:
            return
        index, entry = found
        answer_delay = float(entry.get("delay", self.config.get("answer_delay", 4.0)))
        status = entry.get("status", self.config.get("status"))
        waited = 0.0
        if status:
            delay = entry.get("status_delay", self.config.get("status_delay", 2.0))
            waited = min(float(delay), answer_delay)
            await asyncio.sleep(waited)
            await ws.send_json(
                {
                    "type": "session.instructions.append",
                    "event_id": "evt_" + uuid.uuid4().hex,
                    "content": f'Say exactly: "{status}"',
                }
            )
            self.log("status", session_id=sid)
        if entry.get("follow_up"):
            at = max(float(entry["follow_up_delay"]), waited)
            await asyncio.sleep(at - waited)
            waited = at
            await ws.send_json(
                {
                    "type": "session.commentary.append",
                    "event_id": "evt_" + uuid.uuid4().hex,
                    "delegation_id": delegation_id,
                    "content": entry["follow_up"],
                }
            )
            self.log("follow_up", session_id=sid)
        await asyncio.sleep(answer_delay - waited)
        await ws.send_json(
            {
                "type": "session.commentary.append",
                "event_id": "evt_" + uuid.uuid4().hex,
                "delegation_id": delegation_id,
                "content": entry["answer"],
            }
        )
        self.log("answered", session_id=sid, answer=index)
        if entry.get("transfer"):
            # Let the voice say the answer before the caller leaves.
            await asyncio.sleep(float(self.config.get("transfer_delay", 4.0)))
            target = self.config["transfer_target"]
            status = await self.post(sid, "refer", {"target_uri": target})
            self.log("referred", session_id=sid, status=status)

    async def post(self, sid: str, action: str, body: dict) -> int:
        url = f"{self.config['gateway']}/v1/live/sessions/{sid}/{action}"
        async with self.http.post(url, json=body) as response:
            return response.status

    async def handle(self, sid: str) -> None:
        # accept (the bench), or for manual checks a SIP status to reject
        # with, or "ignore" to let the gateway's ringing deadline expire.
        decision = self.config.get("decision", "accept")
        if decision == "ignore":
            return
        if decision != "accept":
            status = await self.post(sid, "reject", {"status_code": int(decision)})
            self.log("rejected", session_id=sid, status=status)
            return
        status = await self.post(
            sid,
            "accept",
            {
                "session": {
                    "type": "live",
                    "model": self.config.get("model", "echo"),
                    "instructions": self.config.get(
                        "instructions", "Bench: repeat what the caller says."
                    ),
                    "audio": {"output": {"voice": "marin"}},
                    "delegation": {"type": "client"},
                }
            },
        )
        self.log("accepted", session_id=sid, status=status)
        if status != 200:
            return
        url = f"{self.config['gateway']}/v1/live/sessions/{sid}/attach"
        async with self.http.ws_connect(url.replace("http", "ws", 1)) as ws:
            self.log("attached", session_id=sid)
            # The driver sends the greeting right after accept (§4.1).
            await ws.send_json(
                {
                    "type": "session.instructions.append",
                    "event_id": "evt_" + uuid.uuid4().hex,
                    "content": self.config.get(
                        "greeting", "Guten Tag, hier ist der Testanschluss."
                    ),
                }
            )
            heard: list[str] = []
            async for message in ws:
                if message.type != aiohttp.WSMsgType.TEXT:
                    break
                event = json.loads(message.data)
                kind = event.get("type")
                entry = {"session_id": sid, "event": kind}
                if kind == "transport.dtmf.received":
                    entry["digit"] = event.get("event")
                elif kind == "session.closed":
                    entry["reason"] = event.get("reason")
                elif kind == "error":
                    entry["code"] = (event.get("error") or {}).get("code")
                if kind == "session.input_transcript.delta":
                    heard.append(event.get("delta") or "")
                if (
                    self.config.get("log_transcripts") or self.config.get("log_content")
                ) and kind.endswith("_transcript.delta"):
                    entry["text"] = event.get("delta") or ""
                    entry["start_ms"] = event.get("start_ms")
                self.log("event", **entry)
                if kind == "session.delegation.created" and self.answers:
                    delegation_id = (event.get("delegation") or {}).get("id")
                    self.spawn(
                        self.back_office(ws, sid, delegation_id, " ".join(heard))
                    )
                    heard = []
                if (
                    kind == "transport.dtmf.received"
                    and event.get("event") == self.config["transfer_digit"]
                ):
                    target = self.config["transfer_target"]
                    status = await self.post(sid, "refer", {"target_uri": target})
                    self.log("referred", session_id=sid, status=status)
                if kind == "session.closed":
                    break
        self.log("detached", session_id=sid)

    async def run(self) -> None:
        self.http = aiohttp.ClientSession(
            headers={"Authorization": f"Bearer {self.key}"},
            timeout=aiohttp.ClientTimeout(total=10),
        )
        app = web.Application()
        app.router.add_post("/api/live/webhooks/{connector}", self.webhook)
        runner = web.AppRunner(app, access_log=None)
        await runner.setup()
        host, _, port = self.config["listen"].rpartition(":")
        await web.TCPSite(runner, host, int(port)).start()
        self.log("ready")
        try:
            await asyncio.Event().wait()
        finally:
            await runner.cleanup()
            await self.http.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--config", required=True)
    args = parser.parse_args()
    config = json.loads(Path(args.config).read_text())
    asyncio.run(Driver(config).run())


if __name__ == "__main__":
    main()
