"""One-shot wire probe for the subscription proxy (CLIProxyAPI).

Run it ONCE after every ``codexProxy.image`` move, never in a loop. A 400 from
the proxy's Claude lane is charged to the credential and benches the whole
lane for ~60s, so the probe stops at the first non-200 instead of cascading
into a second bench, and it checks the credential is live before every call.

It speaks exactly the shape SRW dispatch sends — streamed Chat Completions
with ``reasoning_effort`` and SRW's own ``Anthropic-Beta`` header for Claude
rows, streamed Responses with ``reasoning.summary=auto`` for Codex rows — and
reports, per case, whether reasoning comes back readable:

  VISIBLE      reasoning text arrived
  REDACTED     reasoning deltas (or billed thinking tokens) arrived with no
               text — Anthropic's signature-only thinking block
  NO-THINKING  neither (the model skipped thinking; inconclusive, not a fail)

The proxy's Chat Completions usage carries no thinking-token breakdown, so
on the Claude lane REDACTED rests on the empty-delta signature alone.

The ``*-noheader`` case drops SRW's header, which measures whether the proxy
now produces visible thinking on its own (CLIProxyAPI v7.3.x derives
``thinking.display`` from ``reasoning_effort``; v7.2.110 needed the header).

Usage (repo root; runs inside the orchestrator pod, stdlib only):

  kubectl --context=main -n superhuman-remote-worker exec -i \\
      deploy/srw-orchestrator -c orchestrator -- \\
      python - [CASE ...] < scripts/subscription-proxy-probe.py

  No CASE runs the default sequence; ``--list`` prints the cases.
"""

from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request

BASE = (os.environ.get("CODEX_PROXY_URL") or "http://srw-codex-proxy:8317").rstrip("/")
if BASE.endswith("/v1"):
    BASE = BASE[: -len("/v1")]
KEY = os.environ.get("SUBSCRIPTION_PROXY_MANAGEMENT_KEY") or os.environ.get(
    "CODEX_MANAGEMENT_KEY", ""
)
TIMEOUT_S = 240
MAX_TOKENS = 16000

try:  # the deployed dispatch header, so the probe cannot drift from it
    from shared.subscription_routing import (
        ANTHROPIC_BETA_HEADER,
        CLAUDE_VISIBLE_THINKING_BETAS,
    )

    SRW_CLAUDE_HEADERS = {
        ANTHROPIC_BETA_HEADER: ",".join(CLAUDE_VISIBLE_THINKING_BETAS)
    }
except ImportError:  # pragma: no cover - only outside an SRW image
    SRW_CLAUDE_HEADERS = {}
    print(
        "! shared.subscription_routing not importable; Claude cases send no SRW header"
    )

THINK_PROMPT = (
    "A bat and a ball cost 1.10 in total. The bat costs 1.00 more than the ball. "
    "What does the ball cost? Then: how many primes lie strictly between 100 and "
    "150? Reply with the two numbers only."
)

TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "lookup_order",
            "description": "Look up an order's shipping status by its number.",
            "parameters": {
                "type": "object",
                "properties": {"order_id": {"type": "string"}},
                "required": ["order_id"],
            },
        },
    }
]


class ProbeFailed(Exception):
    pass


def _request(method, path, body=None, headers=None, stream=False):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(BASE + path, data=data, method=method)
    req.add_header("Authorization", f"Bearer {KEY}")
    if body is not None:
        req.add_header("Content-Type", "application/json")
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    try:
        resp = urllib.request.urlopen(req, timeout=TIMEOUT_S)
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", "replace")[:800]
        raise ProbeFailed(f"HTTP {exc.code} on {method} {path}: {detail}") from None
    if stream:
        return resp
    return json.loads(resp.read().decode() or "{}")


def _sse_events(resp):
    for raw in resp:
        line = raw.decode("utf-8", "replace").strip()
        if not line.startswith("data:"):
            continue
        payload = line[len("data:") :].strip()
        if payload == "[DONE]":
            return
        try:
            yield json.loads(payload)
        except json.JSONDecodeError:
            continue


def _find_count(obj, needles=("reasoning_tokens", "thinking_tokens")):
    """First integer under any key containing a needle, searched recursively."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            if any(n in k for n in needles) and isinstance(v, int):
                return v
            found = _find_count(v, needles)
            if found is not None:
                return found
    return None


def _verdict(r):
    if r["reasoning_chars"] > 0:
        return "VISIBLE"
    if r["reasoning_deltas"] or r["reasoning_tokens"]:
        return "REDACTED"
    return "NO-THINKING"


def credential_live(channel):
    listing = _request("GET", "/v0/management/auth-files")
    entries = [
        f
        for f in listing.get("files", [])
        if str(f.get("provider") or f.get("type") or "").lower() == channel
        and not f.get("disabled")
    ]
    if not entries:
        raise ProbeFailed(f"no {channel} credential on the proxy")
    live = [f for f in entries if not f.get("unavailable")]
    if not live:
        retry = ", ".join(str(f.get("next_retry_after")) for f in entries)
        raise ProbeFailed(
            f"{channel} credential benched until {retry}; wait, do not retry now"
        )


def chat(model, messages, effort, headers, tools=None):
    body = {
        "model": model,
        "messages": messages,
        "stream": True,
        "stream_options": {"include_usage": True},
        "temperature": 1.0,
        "max_tokens": MAX_TOKENS,
    }
    if effort:
        body["reasoning_effort"] = effort
    if tools:
        body["tools"] = tools
    started = time.monotonic()
    resp = _request("POST", "/v1/chat/completions", body, headers, stream=True)
    content, reasoning, usage, finish = [], [], {}, None
    calls: dict[int, dict] = {}
    for event in _sse_events(resp):
        if event.get("usage"):
            usage = event["usage"]
        for choice in event.get("choices") or []:
            delta = choice.get("delta") or {}
            finish = choice.get("finish_reason") or finish
            if delta.get("content"):
                content.append(delta["content"])
            for key in ("reasoning_content", "reasoning"):
                if isinstance(delta.get(key), str):
                    reasoning.append(delta[key])
            for detail in delta.get("reasoning_details") or []:
                if isinstance(detail, dict):
                    reasoning.append(detail.get("text") or detail.get("summary") or "")
            for tc in delta.get("tool_calls") or []:
                slot = calls.setdefault(
                    tc.get("index", 0), {"id": None, "name": "", "args": ""}
                )
                slot["id"] = tc.get("id") or slot["id"]
                fn = tc.get("function") or {}
                slot["name"] += fn.get("name") or ""
                slot["args"] += fn.get("arguments") or ""
    return {
        "content": "".join(content),
        "reasoning_chars": len("".join(reasoning)),
        "reasoning_deltas": len(reasoning),
        "reasoning_tokens": _find_count(usage) or 0,
        "usage": usage,
        "finish": finish,
        "tool_calls": [calls[i] for i in sorted(calls)],
        "seconds": round(time.monotonic() - started, 1),
    }


def responses(model, prompt, effort):
    body = {
        "model": model,
        "input": [{"role": "user", "content": prompt}],
        "stream": True,
        "reasoning": {"effort": effort, "summary": "auto"},
        "max_output_tokens": MAX_TOKENS,
    }
    started = time.monotonic()
    resp = _request("POST", "/v1/responses", body, stream=True)
    content, reasoning, usage = [], [], {}
    for event in _sse_events(resp):
        kind = event.get("type", "")
        if kind == "response.output_text.delta":
            content.append(event.get("delta") or "")
        elif kind == "response.reasoning_summary_text.delta":
            reasoning.append(event.get("delta") or "")
        elif kind in ("response.completed", "response.done"):
            usage = (event.get("response") or {}).get("usage") or {}
        elif kind in ("response.failed", "error"):
            raise ProbeFailed(f"stream error: {json.dumps(event)[:800]}")
    return {
        "content": "".join(content),
        "reasoning_chars": len("".join(reasoning)),
        "reasoning_deltas": len(reasoning),
        "reasoning_tokens": _find_count(usage) or 0,
        "usage": usage,
        "finish": "completed" if usage else None,
        "tool_calls": [],
        "seconds": round(time.monotonic() - started, 1),
    }


def thinking_case(model, effort, with_header):
    def run():
        credential_live("claude")
        r = chat(
            model,
            [{"role": "user", "content": THINK_PROMPT}],
            effort,
            SRW_CLAUDE_HEADERS if with_header else {},
        )
        return _verdict(r), r

    return run


def tool_loop_case(model, effort):
    """Two turns through the proxy: the model must call the tool, then answer
    from its result — SRW's main loop in miniature."""

    def run():
        credential_live("claude")
        messages = [
            {
                "role": "user",
                "content": "Use the lookup_order tool to check order 4711, "
                "then tell me its status in one sentence.",
            }
        ]
        first = chat(model, messages, effort, SRW_CLAUDE_HEADERS, tools=TOOLS)
        calls = first["tool_calls"]
        if not calls or calls[0]["name"] != "lookup_order":
            raise ProbeFailed(
                f"expected a lookup_order call, got finish={first['finish']} "
                f"calls={calls} content={first['content'][:200]!r}"
            )
        call = calls[0]
        messages.append(
            {
                "role": "assistant",
                "content": first["content"] or None,
                "tool_calls": [
                    {
                        "id": call["id"],
                        "type": "function",
                        "function": {"name": call["name"], "arguments": call["args"]},
                    }
                ],
            }
        )
        messages.append(
            {
                "role": "tool",
                "tool_call_id": call["id"],
                "content": json.dumps({"order_id": "4711", "status": "shipped"}),
            }
        )
        credential_live("claude")
        second = chat(model, messages, effort, SRW_CLAUDE_HEADERS, tools=TOOLS)
        if not second["content"].strip():
            raise ProbeFailed(
                f"no answer after the tool result (finish={second['finish']})"
            )
        grounded = "shipped" in second["content"].lower()
        second["seconds"] = round(first["seconds"] + second["seconds"], 1)
        return ("PASS" if grounded else "PASS (answer ignores tool result)"), second

    return run


def codex_case(model, effort):
    def run():
        credential_live("codex")
        r = responses(model, THINK_PROMPT, effort)
        return _verdict(r), r

    return run


CASES = {
    "opus-5-5": (thinking_case("claude-opus-5-5", "medium", True), "claude-opus-5-5"),
    "opus-5-5-noheader": (
        thinking_case("claude-opus-5-5", "medium", False),
        "claude-opus-5-5",
    ),
    "opus-5-5-tools": (tool_loop_case("claude-opus-5-5", "medium"), "claude-opus-5-5"),
    "fable-5-1": (thinking_case("claude-fable-5-1", "low", True), "claude-fable-5-1"),
    "opus-5": (thinking_case("claude-opus-5", "high", True), "claude-opus-5"),
    "opus-5-noheader": (thinking_case("claude-opus-5", "high", False), "claude-opus-5"),
    "opus-5-tools": (tool_loop_case("claude-opus-5", "high"), "claude-opus-5"),
    "codex": (codex_case("gpt-5.6-sol", "low"), "gpt-5.6-sol"),
}
DEFAULT = [
    "opus-5-5",
    "opus-5-5-noheader",
    "opus-5-5-tools",
    "fable-5-1",
    "opus-5",
    "codex",
]


def main(argv):
    if "--list" in argv:
        for name, (_, model) in CASES.items():
            print(f"{name:20s} {model}{'  (default)' if name in DEFAULT else ''}")
        return 0
    selected = [a for a in argv if not a.startswith("-")] or DEFAULT
    unknown = [a for a in selected if a not in CASES]
    if unknown:
        print(f"unknown case(s): {unknown}; see --list")
        return 2
    if not KEY:
        print(
            "no proxy key in SUBSCRIPTION_PROXY_MANAGEMENT_KEY / CODEX_MANAGEMENT_KEY"
        )
        return 2

    listed = {m.get("id") for m in _request("GET", "/v1/models").get("data", [])}
    print(f"proxy {BASE}: {len(listed)} models listed")
    rows, usages, failed = [], [], False
    for name in selected:
        run, model = CASES[name]
        if model not in listed:
            rows.append((name, "SKIPPED (not listed)", "", ""))
            continue
        try:
            verdict, r = run()
        except ProbeFailed as exc:
            rows.append((name, "FAIL", "", str(exc)))
            failed = True
            break  # one bench at most; everything after it would 503 anyway
        usages.append((name, r["usage"]))
        rows.append(
            (
                name,
                verdict,
                f"{r['seconds']}s",
                f"reasoning={r['reasoning_chars']}ch/{r['reasoning_deltas']}deltas "
                f"answer={r['content'].strip()[:60]!r}",
            )
        )
    for skipped in selected[len(rows) :]:
        rows.append((skipped, "NOT RUN", "", "stopped after the failure above"))
    print()
    for name, verdict, secs, note in rows:
        print(f"{name:20s} {verdict:34s} {secs:>7s}  {note}")
    if "--verbose" in argv:
        for name, usage in usages:
            print(f"\n{name} usage: {json.dumps(usage, sort_keys=True)}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
