"""Harness for the prompt-cache prefix-invariance gate.

Support module for tests/test_prompt_cache_prefix_invariance.py (WP0 of
knowledge-base/knowledge/plans/append_only_context_injection_plan_2026_10_05.md).
It drives a scripted conversation through SRW's real request assembly and
records the bytes each request would put on the wire.

The seam is the HTTP transport. ``FakeProvider.install`` patches
``HTTPTransport.handle_request`` and ``AsyncHTTPTransport.handle_async_request``
of ``httpx`` (and of ``httpx2`` when installed), so the provider SDKs SRW
builds (OpenAI through ``ReasoningChatOpenAI``, Anthropic, google-genai) run
unchanged up to the socket. Every transformation between graph state
``messages`` and the request body is therefore inside the measurement: the
execute node's transient tail (``graph.py``), the session loop's per-call
injection (``persistent_graph.py``), ``fold_system_messages``,
``mark_anthropic_cache_breakpoints`` and each SDK's message conversion. The
fake provider answers in the provider's own wire format (JSON for the worker's
``ainvoke``, SSE for the session's ``astream``), so the assistant messages that
re-enter history are the ones the real SDK parsed.

The open-weight families are served behind OpenAI Chat Completions (vLLM,
OpenRouter, MiniMax's API), so their check renders the captured Chat
Completions ``messages`` through the pinned chat template, the way the server
does (vLLM's preprocessing is reproduced in ``_vllm_conversation``).

Which step of the script to answer is derived from the request itself (the
number of distinct ``STEPnn`` markers it carries), so a retried request gets
the same answer instead of advancing the script.

``injection_mode`` (``context_management.injection_mode``, WP2) selects how the
context reaches the request: ``legacy`` rebuilds the per-request tail,
``append_only`` appends typed context entries to the history once and folds
them into their carrier (D27).
"""

from __future__ import annotations

import base64
import copy
import dataclasses
import difflib
import hashlib
import itertools
import json
import re
import types
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable, Dict, List, Optional, Tuple
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import httpx
from langchain_core.messages import (
    AIMessage,
    BaseMessage,
    HumanMessage,
    RemoveMessage,
    ToolMessage,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
TEMPLATE_DIR = Path(__file__).resolve().parent / "fixtures" / "chat_templates"
WORKER_CONFIG_PATH = str(REPO_ROOT / "config" / "worker_base.yaml")

SYSTEM_PROMPT = (
    "You are the SRW worker under test. This system prompt is fixed for the "
    "whole job, so it belongs to the cached prefix."
)
TASK_BRIEF = "# Task brief\n\nRead brief.md, then write summary.md."
PROXY_URL = "http://srw-codex-proxy:8317/v1"
VLLM_URL = "http://vllm-fixture.invalid/v1"


# ---------------------------------------------------------------------------
# The scripted conversation
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Step:
    """One scripted assistant turn. Its text or arguments carry ``STEPnn``."""

    calls: Tuple[Tuple[str, Dict[str, Any]], ...] = ()
    text: str = ""
    reasoning: str = ""


_MARKER = re.compile(r"STEP(\d{2})")

# Worker, tactical phase: a tool loop with one todo change in the middle
# (todo_complete on request 2) and one at the end.
WORKER_SCRIPT: Tuple[Step, ...] = (
    Step(
        calls=(("read_file", {"path": "brief.md", "why": "STEP00"}),),
        text="I will read the brief first. STEP00",
        reasoning="Reasoning STEP00: the brief comes first.",
    ),
    Step(
        calls=(("todo_complete", {"completion_note": "STEP01 brief read"}),),
        reasoning="Reasoning STEP01: the first todo is done.",
    ),
    Step(
        calls=(("write_file", {"path": "summary.md", "content": "STEP02 draft"}),),
        reasoning="Reasoning STEP02: write the summary.",
    ),
    Step(
        calls=(("read_file", {"path": "summary.md", "why": "STEP03"}),),
        reasoning="Reasoning STEP03: check what was written.",
    ),
    Step(
        calls=(("todo_complete", {"completion_note": "STEP04 summary written"}),),
        reasoning="Reasoning STEP04: the second todo is done.",
    ),
)

# Session: two user turns with tool use in between.
SESSION_USER_TURNS = (
    "Turn one: what is in notes.md?",
    "Turn two: and what does plan.md say?",
)
SESSION_SCRIPT: Tuple[Step, ...] = (
    Step(
        calls=(("read_file", {"path": "notes.md", "why": "STEP00"}),),
        reasoning="Reasoning STEP00: open notes.md.",
    ),
    Step(
        calls=(("read_product_guide", {"topic": "index", "why": "STEP01"}),),
        reasoning="Reasoning STEP01: check the product guide.",
    ),
    Step(
        text="STEP02 notes.md lists three open items.",
        reasoning="Reasoning STEP02: answer turn one.",
    ),
    Step(
        calls=(("read_file", {"path": "plan.md", "why": "STEP03"}),),
        reasoning="Reasoning STEP03: open plan.md.",
    ),
    Step(
        text="STEP04 plan.md schedules the release for Friday.",
        reasoning="Reasoning STEP04: answer turn two.",
    ),
)

# Steps that end a session turn (text only); the next request starts a new
# user turn. Used to label a failing pair.
SESSION_TURN_ENDS = {2}


def _function_tool(name: str, description: str, properties: dict) -> dict:
    return {
        "type": "function",
        "function": {
            "name": name,
            "description": description,
            "parameters": {"type": "object", "properties": properties},
        },
    }


_STR = {"type": "string"}
WORKER_TOOLS = [
    _function_tool("read_file", "Read a workspace file", {"path": _STR, "why": _STR}),
    _function_tool(
        "write_file", "Write a workspace file", {"path": _STR, "content": _STR}
    ),
    _function_tool(
        "todo_complete",
        "[tactical-phase tool] Mark the current todo complete",
        {"todo_id": _STR, "completion_note": _STR},
    ),
]
SESSION_TOOLS = [
    _function_tool("read_file", "Read a workspace file", {"path": _STR, "why": _STR}),
    _function_tool(
        "read_product_guide", "Read the SRW product guide", {"topic": _STR, "why": _STR}
    ),
]


# ---------------------------------------------------------------------------
# Families
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Family:
    """One provider renderer: an API wire format, optionally plus a template."""

    id: str
    api: str  # openai_chat | responses | anthropic | gemini
    model: str
    provider: str
    base_url: Optional[str] = None
    reasoning_level: Optional[str] = None
    # The server returns reasoning as a ``reasoning_content`` field (vLLM,
    # OpenRouter, MiniMax with reasoning_split).
    reasoning_field: bool = False
    # Key into fixtures/chat_templates/MANIFEST.json; None for API families.
    template: Optional[str] = None
    # Template families only. True: request N is rendered WITHOUT its
    # generation prompt, so the check covers the history alone (what a block
    # cache such as vLLM APC can reuse when only the last few prompt tokens
    # differ). False: the strict check, generation prompt included.
    history_only: bool = False

    def llm_config(self):
        from shared.runtime.core import loader

        settings = loader.resolve_model_settings(self.model) or {}
        return dataclasses.replace(
            loader.LLMConfig(),
            model=self.model,
            provider=self.provider,
            base_url=self.base_url,
            api_key="fixture-key",
            max_retries=0,
            reasoning_level=self.reasoning_level,
            extra_body=copy.deepcopy(settings.get("extra_body")),
            prompt_cache_key="srw-job-prefix-fixture",
        )


FAMILIES: Dict[str, Family] = {
    f.id: f
    for f in (
        # GPT on first-party OpenAI Chat Completions.
        Family("openai-chat", "openai_chat", "gpt-5.6-sol", "openai", None, "high"),
        # Claude through the subscription proxy: Chat Completions plus the
        # explicit cache_control breakpoints SRW moves every request.
        Family("openai-chat-claude-proxy", "openai_chat", "claude-opus-5-5", "openai", PROXY_URL),
        # GPT-5.6 through the Codex lane: the Responses API shape the proxy takes.
        Family("responses-codex", "responses", "codex/gpt-5.6-sol", "codex", PROXY_URL, "high"),
        Family("anthropic", "anthropic", "claude-opus-5-5", "anthropic"),
        Family("gemini", "gemini", "gemini-3.5-flash", "google"),
        # Open-weight families: Chat Completions body, rendered server-side.
        Family("qwen3.6", "openai_chat", "qwen3.6-27b", "openai", VLLM_URL, None, True, "qwen3.6"),
        Family("gemma-4", "openai_chat", "gemma-4-31b-it", "openai", VLLM_URL, None, True, "gemma-4"),
        Family("gemma-4-history", "openai_chat", "gemma-4-31b-it", "openai", VLLM_URL, None, True, "gemma-4", True),
        Family("glm-5.2", "openai_chat", "z-ai/glm-5.2", "openrouter", None, "high", True, "glm-5.2"),
        Family("minimax-m2", "openai_chat", "MiniMax-M2", "openai", VLLM_URL, None, True, "minimax-m2"),
        Family("minimax-m2-history", "openai_chat", "MiniMax-M2", "openai", VLLM_URL, None, True, "minimax-m2", True),
        Family("deepseek-v3.2", "openai_chat", "deepseek/deepseek-v3.2", "openrouter", None, "high", True, "deepseek-v3.2"),
    )
}  # fmt: skip


def bind_like_srw(llm: Any, llm_config: Any, tools: List[dict]) -> Any:
    """Bind tools the way ``UniversalAgent`` does (agent.py, ``bind_tools``)."""
    from shared.runtime.core.loader import supports_parallel_tool_calls

    kwargs: Dict[str, Any] = {}
    if supports_parallel_tool_calls(llm_config.provider, llm_config.model):
        kwargs["parallel_tool_calls"] = False
    return llm.bind_tools(copy.deepcopy(tools), **kwargs)


# ---------------------------------------------------------------------------
# The fake provider (transport seam + scripted wire responses)
# ---------------------------------------------------------------------------


@dataclass
class Captured:
    api: str
    url: str
    body: Dict[str, Any]


def _api_of(url: str) -> Optional[str]:
    if "/chat/completions" in url:
        return "openai_chat"
    if url.rstrip("/").endswith("/responses"):
        return "responses"
    if url.rstrip("/").endswith("/v1/messages"):
        return "anthropic"
    if ":generateContent" in url or ":streamGenerateContent" in url:
        return "gemini"
    return None


# A wire answer: (content type, body bytes). The transport that received the
# request wraps it in its own Response class (httpx or httpx2).
Wire = Tuple[str, bytes]


def _json_response(payload: dict) -> Wire:
    return "application/json", json.dumps(payload).encode()


def _sse_response(frames: List[str]) -> Wire:
    return "text/event-stream", "".join(frames).encode()


def _openai_chat_wire(step: Step, k: int, model: str, stream: bool, rc: bool):
    calls = [
        {
            "id": f"call_{k}_{i}",
            "type": "function",
            "function": {"name": name, "arguments": json.dumps(args)},
        }
        for i, (name, args) in enumerate(step.calls)
    ]
    finish = "tool_calls" if calls else "stop"
    usage = {"prompt_tokens": 10, "completion_tokens": 1, "total_tokens": 11}
    base = {"id": f"chatcmpl-{k}", "created": 0, "model": model}
    if not stream:
        message: Dict[str, Any] = {"role": "assistant", "content": step.text or None}
        if calls:
            message["tool_calls"] = calls
        if rc and step.reasoning:
            message["reasoning_content"] = step.reasoning
        return _json_response(
            {
                **base,
                "object": "chat.completion",
                "choices": [{"index": 0, "finish_reason": finish, "message": message}],
                "usage": usage,
            }
        )

    def chunk(delta: dict, finish_reason: Optional[str] = None) -> dict:
        return {
            **base,
            "object": "chat.completion.chunk",
            "choices": [{"index": 0, "delta": delta, "finish_reason": finish_reason}],
        }

    chunks = [chunk({"role": "assistant", "content": ""})]
    if rc and step.reasoning:
        chunks.append(chunk({"reasoning_content": step.reasoning}))
    if step.text:
        chunks.append(chunk({"content": step.text}))
    for i, call in enumerate(calls):
        chunks.append(chunk({"tool_calls": [{"index": i, **call}]}))
    chunks.append(chunk({}, finish))
    chunks.append(
        {**base, "object": "chat.completion.chunk", "choices": [], "usage": usage}
    )
    frames = [f"data: {json.dumps(c)}\n\n" for c in chunks] + ["data: [DONE]\n\n"]
    return _sse_response(frames)


def _responses_wire(step: Step, k: int, model: str, stream: bool, rc: bool):
    output: List[dict] = []
    if step.reasoning:
        output.append(
            {
                "type": "reasoning",
                "id": f"rs_{k}",
                "summary": [{"type": "summary_text", "text": step.reasoning}],
            }
        )
    if step.text:
        output.append(
            {
                "type": "message",
                "id": f"msg_{k}",
                "role": "assistant",
                "status": "completed",
                "content": [
                    {"type": "output_text", "text": step.text, "annotations": []}
                ],
            }
        )
    for i, (name, args) in enumerate(step.calls):
        output.append(
            {
                "type": "function_call",
                "id": f"fc_{k}_{i}",
                "call_id": f"call_{k}_{i}",
                "name": name,
                "arguments": json.dumps(args),
                "status": "completed",
            }
        )
    response = {
        "id": f"resp_{k}",
        "object": "response",
        "created_at": 0,
        "status": "completed",
        "model": model,
        "output": output,
        "usage": {
            "input_tokens": 10,
            "output_tokens": 1,
            "total_tokens": 11,
            "input_tokens_details": {"cached_tokens": 0},
            "output_tokens_details": {"reasoning_tokens": 0},
        },
        "error": None,
        "incomplete_details": None,
        "instructions": None,
        "metadata": {},
        "parallel_tool_calls": False,
        "temperature": None,
        "tool_choice": "auto",
        "tools": [],
        "top_p": None,
        "text": {"format": {"type": "text"}},
        "reasoning": {"effort": "high", "summary": "auto"},
    }
    if not stream:
        return _json_response(response)

    events: List[dict] = []
    seq = itertools.count()

    def ev(kind: str, **fields: Any) -> None:
        events.append({"type": kind, "sequence_number": next(seq), **fields})

    ev("response.created", response={**response, "status": "in_progress", "output": []})
    for idx, item in enumerate(output):
        if item["type"] == "reasoning":
            ev(
                "response.output_item.added",
                output_index=idx,
                item={**item, "summary": []},
            )
            for si, part in enumerate(item["summary"]):
                ids = {"item_id": item["id"], "output_index": idx, "summary_index": si}
                ev(
                    "response.reasoning_summary_part.added",
                    **ids,
                    part={"type": "summary_text", "text": ""},
                )
                ev("response.reasoning_summary_text.delta", **ids, delta=part["text"])
                ev("response.reasoning_summary_text.done", **ids, text=part["text"])
                ev("response.reasoning_summary_part.done", **ids, part=part)
        elif item["type"] == "message":
            ev(
                "response.output_item.added",
                output_index=idx,
                item={**item, "status": "in_progress", "content": []},
            )
            ids = {"item_id": item["id"], "output_index": idx, "content_index": 0}
            ev(
                "response.content_part.added",
                **ids,
                part={"type": "output_text", "text": "", "annotations": []},
            )
            ev("response.output_text.delta", **ids, delta=step.text, logprobs=[])
            ev("response.output_text.done", **ids, text=step.text, logprobs=[])
            ev("response.content_part.done", **ids, part=item["content"][0])
        else:
            ev(
                "response.output_item.added",
                output_index=idx,
                item={**item, "arguments": "", "status": "in_progress"},
            )
            ids = {"item_id": item["id"], "output_index": idx}
            ev("response.function_call_arguments.delta", **ids, delta=item["arguments"])
            ev(
                "response.function_call_arguments.done",
                **ids,
                arguments=item["arguments"],
            )
        ev("response.output_item.done", output_index=idx, item=item)
    ev("response.completed", response=response)
    return _sse_response(
        [f"event: {e['type']}\ndata: {json.dumps(e)}\n\n" for e in events]
    )


def _anthropic_wire(step: Step, k: int, model: str, stream: bool, rc: bool):
    content: List[dict] = []
    if step.text:
        content.append({"type": "text", "text": step.text})
    for i, (name, args) in enumerate(step.calls):
        content.append(
            {"type": "tool_use", "id": f"toolu_{k}_{i}", "name": name, "input": args}
        )
    stop = "tool_use" if step.calls else "end_turn"
    message = {
        "id": f"msg_{k}",
        "type": "message",
        "role": "assistant",
        "model": model,
        "content": content,
        "stop_reason": stop,
        "stop_sequence": None,
        "usage": {"input_tokens": 10, "output_tokens": 1},
    }
    if not stream:
        return _json_response(message)
    events: List[dict] = [
        {
            "type": "message_start",
            "message": {**message, "content": [], "stop_reason": None},
        }
    ]
    for idx, block in enumerate(content):
        if block["type"] == "text":
            start = {"type": "text", "text": ""}
            delta = {"type": "text_delta", "text": block["text"]}
        else:
            start = {
                "type": "tool_use",
                "id": block["id"],
                "name": block["name"],
                "input": {},
            }
            delta = {
                "type": "input_json_delta",
                "partial_json": json.dumps(block["input"]),
            }
        events.append(
            {"type": "content_block_start", "index": idx, "content_block": start}
        )
        events.append({"type": "content_block_delta", "index": idx, "delta": delta})
        events.append({"type": "content_block_stop", "index": idx})
    events.append(
        {
            "type": "message_delta",
            "delta": {"stop_reason": stop, "stop_sequence": None},
            "usage": {"output_tokens": 1},
        }
    )
    events.append({"type": "message_stop"})
    return _sse_response(
        [f"event: {e['type']}\ndata: {json.dumps(e)}\n\n" for e in events]
    )


def _gemini_wire(step: Step, k: int, model: str, stream: bool, rc: bool):
    parts: List[dict] = []
    if step.reasoning:
        parts.append({"text": step.reasoning, "thought": True})
    if step.text:
        parts.append({"text": step.text})
    for i, (name, args) in enumerate(step.calls):
        part: Dict[str, Any] = {"functionCall": {"name": name, "args": args}}
        if i == 0:
            part["thoughtSignature"] = base64.b64encode(f"sig-{k}".encode()).decode()
        parts.append(part)
    payload = {
        "candidates": [
            {
                "content": {"role": "model", "parts": parts},
                "finishReason": "STOP",
                "index": 0,
            }
        ],
        "usageMetadata": {
            "promptTokenCount": 10,
            "candidatesTokenCount": 1,
            "totalTokenCount": 11,
        },
        "modelVersion": model,
        "responseId": f"resp-{k}",
    }
    if not stream:
        return _json_response(payload)
    return _sse_response([f"data: {json.dumps(payload)}\r\n\r\n"])


_WIRE = {
    "openai_chat": _openai_chat_wire,
    "responses": _responses_wire,
    "anthropic": _anthropic_wire,
    "gemini": _gemini_wire,
}


class FakeProvider:
    """Records every LLM request body and answers from the script."""

    def __init__(self, script: Tuple[Step, ...], family: Family) -> None:
        self.script = script
        self.family = family
        self.requests: List[Captured] = []

    def respond(self, request: Any, http: types.ModuleType) -> Any:
        url = str(request.url)
        api = _api_of(url)
        if api is None:
            raise AssertionError(f"unexpected outbound request in the harness: {url}")
        raw = request.content.decode("utf-8")
        body = json.loads(raw) if raw else {}
        self.requests.append(Captured(api=api, url=url, body=body))
        step_index = len(set(_MARKER.findall(raw)))
        if step_index >= len(self.script):
            raise AssertionError(
                f"request {len(self.requests)} is past the end of the script"
            )
        stream = body.get("stream") is True or ":streamGenerateContent" in url
        content_type, data = _WIRE[api](
            self.script[step_index],
            step_index,
            self.family.model,
            stream,
            self.family.reasoning_field,
        )
        return http.Response(200, headers={"content-type": content_type}, content=data)

    def install(self, monkeypatch: Any) -> None:
        from shared.runtime.llm import reasoning_chat

        # httpx2 is the transport of openai>=3 and anthropic>=1 when they build
        # their own client (openai still takes SRW's legacy httpx clients).
        modules = [httpx]
        try:
            import httpx2

            modules.append(httpx2)
        except ImportError:
            pass
        for http in modules:
            self._patch_transports(monkeypatch, http)
        # Layer-0 token counting would load a tokenizer; the size is irrelevant.
        monkeypatch.setattr(reasoning_chat, "count_request_tokens", lambda *a, **k: 10)
        try:
            # google-genai prefers aiohttp for async calls when it is installed;
            # force its httpx path so the transport patch sees the request.
            import google.genai._api_client as genai_api_client

            monkeypatch.setattr(genai_api_client, "has_aiohttp", False, raising=False)
        except ImportError:
            pass

    def _patch_transports(self, monkeypatch: Any, http: types.ModuleType) -> None:
        provider = self

        def handle(_transport: Any, request: Any) -> Any:
            return provider.respond(request, http)

        async def ahandle(_transport: Any, request: Any) -> Any:
            return provider.respond(request, http)

        monkeypatch.setattr(http.HTTPTransport, "handle_request", handle)
        monkeypatch.setattr(http.AsyncHTTPTransport, "handle_async_request", ahandle)


# ---------------------------------------------------------------------------
# Worker scenario (graph.py execute node)
# ---------------------------------------------------------------------------

JOB_ID = "prefix-invariance-job"
MEMORY_TEXT = (
    "--- Recalled Memories ---\n"
    "- [factual] The release checklist lives in docs/release.md.\n"
    "- [procedural] Summaries go to summary.md in the workspace root."
)
KNOWLEDGE_TEXT = (
    "--- Project Knowledge ---\n"
    "## Release process\nReleases are cut on Fridays after the smoke test."
)
ACTIVE_SUBAGENTS = (
    "<active_subagents>\n"
    "- reviewer-ab12: running (background)\n"
    "Reports push automatically; do not poll.\n"
    "</active_subagents>"
)
GUIDANCE = [
    {
        "id": "g-1",
        "text": "Keep the summary under 200 words.",
        "source": "supervisor",
        "created_at": "2026-10-05T10:00:00Z",
    }
]


PROJECT_ID = "6f0c1b5e-7d64-4f43-9a52-0d6a3c3f1a11"
KNOWLEDGE_TITLE = "Release process"
KNOWLEDGE_BODY = "Releases are cut on Fridays after the smoke test."
KNOWLEDGE_BODY_REVISED = "Releases are cut on Thursdays after the smoke test."
MEMORY_FACTS = (
    ("factual", "The release checklist lives in docs/release.md."),
    ("procedural", "Summaries go to summary.md in the workspace root."),
)
# ``churn``: seven memories, so the per-entry cap (5, D29) leaves two to drip
# in on the next request; later one memory and the note change (D5).
CHURN_FACTS = MEMORY_FACTS + tuple(
    ("factual", f"Churn fact {i}: release step {i} is documented.") for i in range(3, 8)
)
MEMORY_REVISED = "The release checklist moved to docs/releasing.md."
CHURN_MEMORY_CHANGE_AT = 3  # assemble call (= worker request) of the memory change
CHURN_NOTE_CHANGE_AT = 4  # assemble call of the note change


def memory_records(*, churn: bool = False, call: int = 0) -> List[Any]:
    """The memory rows behind the payload's memory block, in rank order."""
    from shared.runtime.services.recall_store import MemoryRecord

    facts = CHURN_FACTS if churn else MEMORY_FACTS
    records = [
        MemoryRecord(
            id=UUID(int=index),
            content=content,
            memory_type=memory_type,
            importance=0.5,
            token_count=len(content) // 4,
        )
        for index, (memory_type, content) in enumerate(facts, 1)
    ]
    if churn and call >= CHURN_MEMORY_CHANGE_AT:
        records[0] = dataclasses.replace(records[0], content=MEMORY_REVISED)
    return records


def knowledge_records(*, churn: bool = False, call: int = 0) -> List[Any]:
    """The knowledge note behind the payload's knowledge block."""
    from shared.runtime.services.knowledge_store import KnowledgeRecord

    body = (
        KNOWLEDGE_BODY_REVISED
        if churn and call >= CHURN_NOTE_CHANGE_AT
        else KNOWLEDGE_BODY
    )
    return [
        KnowledgeRecord(
            note_id="release",
            project_id=UUID(PROJECT_ID),
            title=KNOWLEDGE_TITLE,
            note_type="process",
            content=body,
        )
    ]


class RecordingMemoryManager:
    """MemoryManager seam stub with a memory + knowledge payload.

    The rendered blocks (``content`` / ``messages``, what legacy mode
    injects) are fixed. ``InjectionBlock.records`` carry the store rows
    behind them (what append_only plans from, WP2 spec §C): fixed, or with
    ``churn`` seven memories (a drip-feed past the per-entry cap) and a
    memory and the note that change on later requests.
    """

    def __init__(self, *, churn: bool = False) -> None:
        self.churn = churn
        self.assemble_requests: List[Any] = []
        self.captures: List[Any] = []
        self.payload = self._payload(0)

    def _payload(self, call: int) -> Any:
        from agent.core.knowledge_injection import create_knowledge_injection_messages
        from agent.core.memory_injection import create_memory_injection_messages
        from agent.services.memory import AssembleStats, InjectionBlock, MemoryPayload

        blocks = [
            InjectionBlock(
                kind="memory",
                content=MEMORY_TEXT,
                messages=list(create_memory_injection_messages(MEMORY_TEXT)),
                token_count=40,
                items=[{"record_id": "m1", "token_count": 40}],
                records=memory_records(churn=self.churn, call=call),
            ),
            InjectionBlock(
                kind="knowledge",
                content=KNOWLEDGE_TEXT,
                messages=list(create_knowledge_injection_messages(KNOWLEDGE_TEXT)),
                token_count=20,
                items=[{"record_id": "k1", "token_count": 20}],
                records=knowledge_records(churn=self.churn, call=call),
            ),
        ]
        return MemoryPayload(blocks=blocks, stats=AssembleStats(blocks=len(blocks)))

    async def assemble(self, req: Any) -> Any:
        self.payload = self._payload(len(self.assemble_requests))
        self.assemble_requests.append(req)
        return self.payload

    async def capture(self, event: Any) -> None:
        self.captures.append(event)

    def capture_nowait(self, event: Any) -> None:
        self.captures.append(event)


class WorkerContextManager:
    """Real-typed ContextManager fake (as in test_execute_prepared_layout.py).

    ``arm_overflow`` makes the next full-request count exceed every limit, so
    the execute node takes its Layer-1 safety path: a forced
    ``ensure_within_limits`` and a rebuild of the request with the transient
    tail re-anchored. The forced compaction is a no-op here (nothing is
    evicted), which isolates the rebuild from compaction itself; a real
    compaction is an accepted, one-off cache miss.
    """

    def __init__(self) -> None:
        self.config = SimpleNamespace(
            compaction_threshold_tokens=100_000,
            summarization_threshold_tokens=100_000,
            keep_recent_messages=10,
        )
        self._state = SimpleNamespace(summaries=[])
        self.arm_overflow = False
        self.forced_rebuilds = 0

    def set_current_phase(self, phase: str, phase_key: Optional[str] = None) -> None:
        pass

    def should_summarize(self, messages: Any) -> bool:
        return False

    def get_token_count(self, messages: List[Any]) -> int:
        if self.arm_overflow and len(messages) > 1:
            return 10**9
        return sum(len(str(getattr(m, "content", ""))) // 4 for m in messages)

    async def ensure_within_limits(self, messages, *args, **kwargs):
        if kwargs.get("force"):
            self.arm_overflow = False
            self.forced_rebuilds += 1
        return messages


class OverflowOnce:
    """Raise ``ContextOverflowError`` on the first attempt of one request.

    That exception is what sends the execute node into its Layer-0 emergency
    compaction: a forced ``ensure_within_limits``, then a rebuild of the
    request with the transient tail re-anchored, then a retry. No real client
    raises it today (``ReasoningChatOpenAI`` turns a Layer-0 overflow into a
    synthetic HTTP 413), so it is raised here, at the LLM boundary, before the
    provider sees the request. Only the retried request reaches the wire.
    """

    def __init__(self, bound: Any, at_request: int) -> None:
        self._bound = bound
        self._at = at_request
        self._calls = 0
        self.raised = False
        self.kwargs = getattr(bound, "kwargs", {})

    async def ainvoke(self, messages: List[BaseMessage], *args: Any, **kwargs: Any):
        from shared.runtime.llm.exceptions import ContextOverflowError

        call = self._calls
        self._calls += 1
        if not self.raised and call == self._at:
            self.raised = True
            raise ContextOverflowError(token_count=10**9, limit=128_000)
        return await self._bound.ainvoke(messages, *args, **kwargs)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._bound, name)


def _failed_citation() -> Any:
    from shared.runtime.citation_engine.models import Citation, VerificationStatus

    return Citation(
        id=7,
        claim="The release is cut on Mondays",
        quote_context="Releases are cut on Fridays after the smoke test.",
        source_id=1,
        locator={"page": 1},
        created_at=datetime(2026, 10, 5, tzinfo=timezone.utc),
        verbatim_quote="cut on Mondays",
        verification_status=VerificationStatus.FAILED,
        verification_notes="quote not found in source",
        similarity_score=0.31,
    )


async def run_worker_scenario(
    family: Family,
    *,
    sources: frozenset,
    workdir: Path,
    monkeypatch: Any,
    safety_rebuild_at: Optional[int] = None,
    emergency_rebuild_at: Optional[int] = None,
    churn: bool = False,
    injection_mode: str = "legacy",
) -> List[Captured]:
    """Run the tactical tool loop through ``create_execute_node``.

    Between execute calls the harness does what the graph does: append the
    node's messages (the add_messages reducer) and run the tools node, here a
    stand-in that answers each call. ``check_todos`` does not touch
    ``messages``.

    ``sources`` (see ``VARIANTS``) switches the injection sources on:
    ``"todos"`` is the todo list in its post-WP1 form (D17-D19): the history
    opens with the strategic->tactical phase-start message that carries the
    list, and ``todo_complete`` is the real tool, so its results carry the
    full updated list and the todo change is real. Without it the todo list
    is empty and ``todo_complete`` is answered by the stand-in, so no todo
    text reaches any request. ``"context"`` is the rest of the per-turn tail
    (memory and knowledge, citation feedback, supervisor guidance, active
    subagents), which WP2 moves into the history.

    ``safety_rebuild_at`` makes request k take the Layer-1 safety rebuild;
    ``emergency_rebuild_at`` makes it take the Layer-0 emergency rebuild.
    ``churn`` makes the memory seam return more memories than one entry
    takes and change a memory and the note later (see
    ``RecordingMemoryManager``). ``injection_mode`` is the worker's
    ``context_management.injection_mode``. Supervisor guidance is pending on
    two consecutive requests, as the heartbeat inbox keeps an entry until
    the ack lands; ``delivered_guidance_ids`` is carried into state.
    """
    from shared.runtime.core.loader import create_llm, load_agent_config
    from shared.runtime.services.guardrails import format_nudge

    from agent.core.phase import _with_todo_list
    from agent.core.workspace import WorkspaceManager
    from agent.graph import create_execute_node
    from agent.managers import TodoManager
    from agent.tools.context import ToolContext
    from agent.tools.core.todo import create_todo_tools
    from tests._fs_backend import FilesystemTestBackend

    todos_on = "todos" in sources
    context_on = "context" in sources
    provider = FakeProvider(WORKER_SCRIPT, family)
    provider.install(monkeypatch)
    llm_config = family.llm_config()
    llm_with_tools = bind_like_srw(create_llm(llm_config), llm_config, WORKER_TOOLS)
    overflow = None
    if emergency_rebuild_at is not None:
        llm_with_tools = overflow = OverflowOnce(llm_with_tools, emergency_rebuild_at)

    workspace = WorkspaceManager(
        job_id=JOB_ID, base_path=workdir, backend=FilesystemTestBackend(workdir)
    )
    workspace.initialize()
    config = load_agent_config(WORKER_CONFIG_PATH)
    config.llm = llm_config
    config.context_management.injection_mode = injection_mode
    todo = TodoManager(workspace, model_name=llm_config.model)
    todo.is_strategic_phase = False
    todo.phase_number = 2
    history: List[BaseMessage] = [HumanMessage(content=TASK_BRIEF)]
    ctx = ToolContext(workspace_manager=workspace)
    ctx.todo_manager = todo
    todo_tools: Dict[str, Any] = {}
    if todos_on:
        todo.add("Read the brief")
        todo.add("Write the summary")
        todo_tools = {tool.name: tool for tool in create_todo_tools(ctx)}
        # What handle_transition appended when this tactical phase began.
        history.append(
            HumanMessage(
                content=_with_todo_list(
                    format_nudge(
                        "phase_transition_strategic_to_tactical",
                        model=llm_config.model,
                        phase_number=2,
                        phase_name="Summary",
                        todo_count=2,
                    ),
                    todo,
                    is_strategic=False,
                    phase_number=2,
                )
            )
        )

    memory_service = None
    guidance_turns: set[int] = set()
    if context_on:
        memory_service = RecordingMemoryManager(churn=churn)
        ctx.citation_engine = SimpleNamespace(
            list_citations=AsyncMock(return_value=[_failed_citation()])
        )
        ctx.subagent_runtime = SimpleNamespace(
            active_subagents_block=lambda: ACTIVE_SUBAGENTS
        )
        guidance_turns = {2, 3}

    context_mgr = WorkerContextManager()
    node = create_execute_node(
        llm_with_tools=llm_with_tools,
        todo_manager=todo,
        memory_manager=MagicMock(),
        workspace_manager=workspace,
        config=config,
        context_mgr=context_mgr,
        retry_manager=MagicMock(),
        auxiliary_llm=MagicMock(),
        summarization_prompt="summarize",
        tool_context=ctx,
        tool_names=None,
        memory_service=memory_service,
    )
    state: Dict[str, Any] = {
        "job_id": JOB_ID,
        "iteration": 0,
        "messages": history,
        "is_strategic_phase": False,
        "phase_number": 2,
        "turn_count": 0,
        "metadata": {},
    }
    turn = [0]

    def guidance(_job_id: str) -> List[dict]:
        return copy.deepcopy(GUIDANCE) if turn[0] in guidance_turns else []

    with (
        patch("agent.graph.get_phase_system_prompt", return_value=SYSTEM_PROMPT),
        patch("agent.graph.get_archiver", return_value=None),
        patch("agent.graph._get_pending_supervisor_guidance", side_effect=guidance),
        patch("agent.graph._ack_supervisor_guidance"),
    ):
        for index in range(len(WORKER_SCRIPT)):
            turn[0] = index
            if safety_rebuild_at == index:
                context_mgr.arm_overflow = True
            result = await node(state)
            if result.get("error"):
                raise AssertionError(f"execute node failed: {result['error']}")
            new_messages = [
                m for m in result["messages"] if not isinstance(m, RemoveMessage)
            ]
            state["messages"] = state["messages"] + new_messages
            state["iteration"] = result["iteration"]
            state["turn_count"] = result["turn_count"]
            if "delivered_guidance_ids" in result:
                state["delivered_guidance_ids"] = result["delivered_guidance_ids"]
            response = next(
                m for m in reversed(new_messages) if isinstance(m, AIMessage)
            )
            if not response.tool_calls:
                raise AssertionError(f"turn {index}: expected a tool call")
            for call in response.tool_calls:
                if call["name"] in todo_tools:
                    output = todo_tools[call["name"]].invoke(call["args"])
                else:
                    output = (
                        f"ok: {call['name']} {json.dumps(call['args'], sort_keys=True)}"
                    )
                state["messages"].append(
                    ToolMessage(
                        content=str(output), tool_call_id=call["id"], name=call["name"]
                    )
                )
    expected_forced = int(safety_rebuild_at is not None) + int(overflow is not None)
    if context_mgr.forced_rebuilds != expected_forced:
        raise AssertionError(
            f"expected {expected_forced} forced rebuild(s), saw {context_mgr.forced_rebuilds}"
        )
    if overflow is not None and not overflow.raised:
        raise AssertionError("the Layer-0 emergency rebuild did not run")
    if context_on and memory_service.assemble_requests == []:
        raise AssertionError("the memory seam was never consulted")
    if todos_on and todo.all_complete() is not True:
        raise AssertionError("the scripted todo changes did not run")
    return provider.requests


# ---------------------------------------------------------------------------
# Session scenario (persistent_graph.py run_persistent_loop)
# ---------------------------------------------------------------------------

CHARTER = {
    "title": "Project charter",
    "content": "Standing orders: ship weekly; never touch production data.",
}


def _session_config(
    *, injections: bool, model: str, injection_mode: str = "legacy"
) -> MagicMock:
    """MagicMock config in the style of the persistent-graph tests."""
    from shared.runtime.core.skill_resolution import APP_GUIDE_LOADER_TOOL

    config = MagicMock()
    config.extra = {}
    if injections:
        config.extra = {
            "_resolved_skills": {
                "menu": [
                    {
                        "name": "app-guide",
                        "system_managed": True,
                        "loader_tool": APP_GUIDE_LOADER_TOOL,
                        "bundle_digest": "a" * 64,
                    }
                ]
            }
        }
    config.llm.timeout = 600
    config.llm.model = model
    config.memory.enabled = injections
    config.memory.observer_interval = 5
    config.memory.query = None
    config.memory.project_scoped = False
    config.memory.max_memories_per_entry = 5
    config.context_management.max_summary_length = 10_000
    config.context_management.injection_mode = injection_mode
    config.officer.enabled = False
    config.officer.conference = injections  # charter injection
    config.officer.max_actions_per_wake = 100
    return config


def _session_tool(name: str) -> MagicMock:
    async def run(args: Any) -> str:
        return f"ok: {name} {json.dumps(args, sort_keys=True)}"

    tool = MagicMock()
    tool.name = name
    tool.args_schema = None
    tool.ainvoke = AsyncMock(side_effect=run)
    return tool


async def run_session_scenario(
    family: Family,
    *,
    sources: frozenset,
    monkeypatch: Any,
    injection_mode: str = "legacy",
) -> List[Captured]:
    """Two user turns through the real ``run_persistent_loop`` (astream path).

    Sessions never carried a todo list; ``"context"`` in ``sources`` switches
    on the charter, memory and knowledge, active subagents and the App Guide
    turn boundary.
    """
    injections = "context" in sources
    import asyncio

    from agent.persistent_graph import PersistentLoopCallbacks, run_persistent_loop
    from shared.runtime.core.loader import create_llm
    from shared.runtime.core.skill_resolution import APP_GUIDE_LOADER_TOOL

    assert SESSION_TOOLS[1]["function"]["name"] == APP_GUIDE_LOADER_TOOL
    provider = FakeProvider(SESSION_SCRIPT, family)
    provider.install(monkeypatch)
    llm_config = family.llm_config()
    llm_with_tools = bind_like_srw(create_llm(llm_config), llm_config, SESSION_TOOLS)

    tools = [_session_tool(t["function"]["name"]) for t in SESSION_TOOLS]
    context_manager = AsyncMock()
    context_manager.should_summarize = MagicMock(return_value=False)
    context_manager.ensure_within_limits = AsyncMock(
        side_effect=lambda messages, *_a, **_k: messages
    )
    context_manager.config.keep_recent_messages = 10
    context_manager.record_provider_usage = MagicMock()
    errors: List[Any] = []

    async def on_error(*args: Any, **kwargs: Any) -> None:
        errors.append((args, kwargs))

    callbacks = PersistentLoopCallbacks(
        get_user_input=AsyncMock(
            side_effect=[*SESSION_USER_TURNS, asyncio.CancelledError()]
        ),
        on_token=AsyncMock(),
        on_thinking=AsyncMock(),
        on_tool_start=AsyncMock(),
        on_tool_result=AsyncMock(),
        permission_check=AsyncMock(return_value=True),
        on_turn_start=AsyncMock(),
        on_turn_complete=AsyncMock(),
        on_error=on_error,
        check_interrupt=MagicMock(return_value=None),
        persist_message=AsyncMock(),
    )
    tool_context = SimpleNamespace(knowledge_bindings=[], citation_engine=None)
    kwargs: Dict[str, Any] = {}
    if injections:
        tool_context.subagent_runtime = SimpleNamespace(
            active_subagents_block=lambda: ACTIVE_SUBAGENTS
        )
        knowledge_store = MagicMock()
        knowledge_store.get_charter_note = AsyncMock(return_value=CHARTER)
        kwargs = {
            "memory_service": RecordingMemoryManager(),
            "knowledge_store": knowledge_store,
            "project_ids": [PROJECT_ID],
        }

    await run_persistent_loop(
        llm_with_tools=llm_with_tools,
        tools=tools,
        context_manager=context_manager,
        config=_session_config(
            injections=injections, model=family.model, injection_mode=injection_mode
        ),
        system_prompt="You are the SRW session assistant under test.",
        callbacks=callbacks,
        messages=[],
        tool_context=tool_context,
        **kwargs,
    )
    if errors:
        raise AssertionError(f"session turn reported errors: {errors}")
    if len(provider.requests) != len(SESSION_SCRIPT):
        raise AssertionError(
            f"expected {len(SESSION_SCRIPT)} requests, got {len(provider.requests)}"
        )
    return provider.requests


# ---------------------------------------------------------------------------
# Views: what each provider caches, normalised
# ---------------------------------------------------------------------------

# Keys whose value differs by transport mode only (worker ainvoke vs session
# astream, or a session fallback to ainvoke) and never by prompt content.
_TRANSPORT_KEYS = {"stream", "stream_options"}
_ITEM_KEYS = {
    "openai_chat": "messages",
    "responses": "input",
    "anthropic": "messages",
    "gemini": "contents",
}


def _strip_cache_markers(value: Any) -> Any:
    """Drop Anthropic ``cache_control`` markers at any depth.

    SRW moves them deliberately each request (``mark_anthropic_cache_breakpoints``
    puts one on the last stable message); they tell the provider where to write
    a cache entry and are not part of the cached text.
    """
    if isinstance(value, dict):
        return {
            k: _strip_cache_markers(v) for k, v in value.items() if k != "cache_control"
        }
    if isinstance(value, list):
        return [_strip_cache_markers(v) for v in value]
    return value


def api_view(captured: Captured) -> Tuple[Dict[str, Any], List[Any]]:
    """Split a request into (frame, items) with cache markers normalised away.

    ``frame`` is everything outside the message list (system/instructions,
    tools, model parameters); ``items`` is the message/input/contents list.
    ``prompt_cache_key`` is a routing hint, not prompt text.
    """
    body = _strip_cache_markers(copy.deepcopy(captured.body))
    items = body.pop(_ITEM_KEYS[captured.api], [])
    for key in _TRANSPORT_KEYS | {"prompt_cache_key"}:
        body.pop(key, None)
    return body, items


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def _short(text: str, limit: int = 400) -> str:
    return text if len(text) <= limit else text[: limit - 3] + "..."


def api_prefix_violation(prev: Captured, nxt: Captured) -> Optional[str]:
    """None if ``nxt`` starts with ``prev``; else where and how it diverges."""
    prev_frame, prev_items = api_view(prev)
    next_frame, next_items = api_view(nxt)
    if canonical(prev_frame) != canonical(next_frame):
        keys = sorted(
            k
            for k in set(prev_frame) | set(next_frame)
            if canonical(prev_frame.get(k)) != canonical(next_frame.get(k))
        )
        return f"non-message part changed: {keys}"
    for index, item in enumerate(prev_items):
        if index >= len(next_items):
            return (
                f"request N has {len(prev_items)} items but N+1 has only "
                f"{len(next_items)}: the history shrank"
            )
        a, b = canonical(item), canonical(next_items[index])
        if a != b:
            diff = "\n".join(
                difflib.unified_diff(
                    [_short(a)], [_short(b)], "request N", "request N+1", lineterm=""
                )
            )
            return f"first diverging item: index {index}\n{diff}"
    return None


def text_prefix_violation(prev: str, nxt: str) -> Optional[str]:
    if nxt.startswith(prev):
        return None
    offset = next(
        (i for i, (a, b) in enumerate(zip(prev, nxt)) if a != b),
        min(len(prev), len(nxt)),
    )
    lo = max(0, offset - 80)
    return (
        f"first differing character at offset {offset} of {len(prev)}\n"
        f"  request N  : {prev[lo : offset + 120]!r}\n"
        f"  request N+1: {nxt[lo : offset + 120]!r}"
    )


# ---------------------------------------------------------------------------
# Chat templates (server-side rendering of the Chat Completions body)
# ---------------------------------------------------------------------------


def load_manifest() -> Dict[str, Any]:
    return json.loads((TEMPLATE_DIR / "MANIFEST.json").read_text(encoding="utf-8"))


def template_source(key: str) -> Tuple[Dict[str, Any], str]:
    entry = load_manifest()["templates"][key]
    raw = (TEMPLATE_DIR / entry["file"]).read_bytes()
    digest = hashlib.sha256(raw).hexdigest()
    if digest != entry["sha256"]:
        raise AssertionError(
            f"{entry['file']} does not match its MANIFEST sha256; re-vendor it, never edit it"
        )
    return entry, raw.decode("utf-8")


def _vllm_conversation(messages: List[dict]) -> List[dict]:
    """The conversation vLLM hands to the template for a Chat Completions body.

    Mirrors ``vllm.entrypoints.chat_utils``: text parts are joined into one
    string ("string" content format; SRW sends text only), a null content
    becomes "", and assistant ``tool_calls[].function.arguments`` are parsed
    from the JSON string into a mapping (``_postprocess_messages``). Fields
    the server does not forward (``cache_control``) are dropped.
    """
    out = []
    for message in messages:
        msg = {
            k: copy.deepcopy(v)
            for k, v in message.items()
            if k
            in (
                "role",
                "content",
                "tool_calls",
                "tool_call_id",
                "name",
                "reasoning_content",
            )
        }
        content = msg.get("content")
        if content is None:
            msg["content"] = ""
        elif isinstance(content, list):
            msg["content"] = "\n".join(
                part.get("text", "") for part in content if isinstance(part, dict)
            )
        for call in msg.get("tool_calls") or []:
            arguments = call.get("function", {}).get("arguments")
            call["function"]["arguments"] = json.loads(arguments) if arguments else {}
        out.append(msg)
    return out


def _template_kwargs(body: Dict[str, Any]) -> Dict[str, Any]:
    kwargs = dict(body.get("chat_template_kwargs") or {})
    effort = body.get("reasoning_effort") or (body.get("reasoning") or {}).get("effort")
    if effort:
        kwargs.setdefault("reasoning_effort", effort)
    return kwargs


_DSV32_MODULE: Dict[str, types.ModuleType] = {}


def _dsv32_encoder() -> types.ModuleType:
    if "module" not in _DSV32_MODULE:
        entry, source = template_source("deepseek-v3.2")
        module = types.ModuleType("encoding_dsv32_fixture")
        exec(
            compile(source, str(TEMPLATE_DIR / entry["file"]), "exec"), module.__dict__
        )
        _DSV32_MODULE["module"] = module
    return _DSV32_MODULE["module"]


def render_template(
    key: str, body: Dict[str, Any], *, add_generation_prompt: bool = True
) -> str:
    """Render a captured Chat Completions body as the serving engine would."""
    entry, source = template_source(key)
    tools = body.get("tools") or None
    if entry["kind"] == "dsv32_encoder":
        if not add_generation_prompt:
            raise ValueError(
                "the DeepSeek V3.2 encoder has no separate generation prompt"
            )
        # vllm/tokenizers/deepseek_v32.py: the tools ride on an extra leading
        # system message, reasoning history is dropped when the last message
        # is a user turn. SRW asks for reasoning (OpenRouter
        # ``reasoning.effort``), so the request is rendered in thinking mode.
        # The pinned encoder parses arguments itself, so they stay strings.
        messages = [
            {k: v for k, v in m.items() if k != "cache_control"}
            for m in copy.deepcopy(body["messages"])
        ]
        if tools:
            messages.insert(0, {"role": "system", "tools": tools})
        return _dsv32_encoder().encode_messages(
            messages,
            thinking_mode="thinking",
            drop_thinking=messages[-1]["role"] == "user",
        )
    from transformers.utils.chat_template_utils import render_jinja_template

    rendered, _ = render_jinja_template(
        conversations=[_vllm_conversation(body["messages"])],
        tools=tools,
        chat_template=source,
        add_generation_prompt=add_generation_prompt,
        **entry.get("special_tokens", {}),
        **_template_kwargs(body),
    )
    return rendered[0] if isinstance(rendered, list) else rendered


# ---------------------------------------------------------------------------
# Running the check over a captured sequence
# ---------------------------------------------------------------------------


def prefix_violations(
    family: Family, requests: List[Captured], *, turn_ends: frozenset = frozenset()
) -> Dict[int, str]:
    """Every consecutive pair (N, N+1) where N+1 does not start with N.

    Keyed by N. For template families request N+1 is always rendered with its
    generation prompt; request N without it when ``family.history_only``.
    """
    pairs: List[Tuple[Any, Any]] = []
    check: Callable[[Any, Any], Optional[str]]
    if family.template is not None:
        full = [render_template(family.template, r.body) for r in requests]
        prev = full
        if family.history_only:
            prev = [
                render_template(family.template, r.body, add_generation_prompt=False)
                for r in requests
            ]
        pairs = [(prev[n], full[n + 1]) for n in range(len(full) - 1)]
        check = text_prefix_violation
    else:
        pairs = [(requests[n], requests[n + 1]) for n in range(len(requests) - 1)]
        check = api_prefix_violation
    found: Dict[int, str] = {}
    for n, (a, b) in enumerate(pairs):
        problem = check(a, b)
        if problem:
            boundary = " (new user turn)" if n in turn_ends else ""
            found[n] = f"request {n} -> {n + 1}{boundary}: {problem}"
    return found


#: Injection sources each variant switches on (see ``run_worker_scenario``).
VARIANTS: Dict[str, frozenset] = {
    "injected": frozenset({"todos", "context"}),
    "todos-only": frozenset({"todos"}),
    "control": frozenset(),
}

#: ``context_management.injection_mode`` values the gate runs.
INJECTION_MODES = ("legacy", "append_only")

# Request 2 is the one after the first todo change, mid-loop.
SCENARIOS: Dict[str, Dict[str, Any]] = {
    "worker-tool-loop": {"kind": "worker"},
    "worker-safety-rebuild": {"kind": "worker", "safety_rebuild_at": 2},
    "worker-emergency-rebuild": {"kind": "worker", "emergency_rebuild_at": 2},
    # The tool loop with memory and knowledge churn: a drip-feed past the
    # per-entry cap, then a changed memory and a changed note (D5, D29).
    "worker-memory-churn": {"kind": "worker", "churn": True},
    "session-two-turns": {"kind": "session"},
}


async def run_scenario(
    name: str,
    family: Family,
    *,
    sources: frozenset,
    workdir: Path,
    monkeypatch: Any,
    injection_mode: str = "legacy",
) -> Tuple[List[Captured], frozenset]:
    """Run one scenario; return the captured requests and the turn-end steps."""
    spec = dict(SCENARIOS[name])
    if spec.pop("kind") == "session":
        requests = await run_session_scenario(
            family,
            sources=sources,
            monkeypatch=monkeypatch,
            injection_mode=injection_mode,
        )
        return requests, frozenset(SESSION_TURN_ENDS)
    requests = await run_worker_scenario(
        family,
        sources=sources,
        workdir=workdir,
        monkeypatch=monkeypatch,
        injection_mode=injection_mode,
        **spec,
    )
    return requests, frozenset()


def text_occurrences(value: Any, needle: str) -> int:
    """How often ``needle`` occurs in the string leaves of a request body."""
    if isinstance(value, str):
        return value.count(needle)
    if isinstance(value, dict):
        return sum(text_occurrences(v, needle) for v in value.values())
    if isinstance(value, list):
        return sum(text_occurrences(v, needle) for v in value)
    return 0
