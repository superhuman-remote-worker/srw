"""Session restore fidelity at the wire: a restored turn sends the live bytes.

Design: knowledge-base/knowledge/features/append_only_context_injection.md
§3.6 (F7) and the F1 note of
knowledge-base/knowledge/plans/append_only_context_injection_plan_2026_10_05.md.
For a session, every turn may rebuild the model history from its stored
``thread_messages`` rows (the stateless lane does it every turn, the pinned
lane after a pod restart). The provider cache is read only when the rebuilt
request starts with exactly the bytes the live loop would have sent.

How one case runs (scenario x client):

- Live. The real session loop (``run_persistent_loop``) runs the scenario's
  turns 1..N and then a probe turn N+1, against the real client the loader
  builds for the family (``harness.FAMILIES``). The fake provider sits behind
  the HTTP transport (``harness.FakeProvider``), so the SDK parses the
  provider's own wire format into the assistant messages and serializes every
  request itself. Each message goes through the transport's two writes: the
  incremental persist (``_persist_one_message``) and the turn-end reconcile
  (``_save_turn_ai_messages``, pinned lane; the stateless lane writes the
  same rows through the same serializer and restores through the same
  reader), both through ``_serialize_message_row`` with its role overrides
  and the ``srw_injection`` kwargs of context rows.
- When the loop asks for the probe input, the stored rows are copied.
- Restored. A fresh process restores the copy with
  ``_restore_session_messages`` (the real ``PostgresDB`` history reader,
  ``_db_rows_to_lc_messages``, the pairing repair and the provider sanitize;
  Path A when a compaction checkpoint exists) and runs the same probe turn.
- Compared. The first request of the probe turn, as the client put it on the
  wire: the request body with its key order kept. Identical, or the case fails
  with every differing item and where it first differs (a character offset
  into the item's JSON). A difference that vanishes when keys are sorted is
  reported as "key order only".

Stubs and constructed placements:

- The database is ``StoredRows``. Writes mirror ``save_thread_message(s)``:
  the ``ON CONFLICT (id)`` column rules of ``_THREAD_MESSAGE_UPSERT_SQL``
  (``additional_kwargs`` COALESCE, every other column replaced, role and
  ``seq`` kept) and ``insert_if_absent``. Reads answer the real reader's
  queries (``fetch``/``fetchrow`` of ``get_thread_messages_history``,
  ``get_latest_compaction_checkpoint``, ``get_seq_for_message_id``). The
  jsonb columns come back in jsonb's key order (shorter keys first, then
  bytewise), as Postgres returns them; ``content`` is text and comes back
  as written. ``created_at`` is not modelled: a row without it counts as
  young, as these rows are. ``test_stored_rows_read_back_like_postgres``
  checks this model against a real Postgres (testcontainers; skipped
  without a container runtime).
- The provider answers its script in order (``Provider``): the harness's
  marker-based step choice cannot survive a compaction, and no request is
  retried here.
- Tools are stand-ins (``harness._session_tool``) plus ``view_image``, whose
  result carries an ``<image_data>`` tag, so the loop builds its synthetic
  image row.
- Memory is a stub manager that hands a retrieval result to a scheduled
  (turn, provider call) (``TurnMemory``); its records are rendered by the real
  planner. By default turn 1's first call takes the memory and the knowledge
  note in, with the charter, the subagent status and the App Guide turn
  boundary (``harness._session_config(injections=True)``).
- The context manager is an identity stub, except in ``compaction``, where the
  real ``ContextManager`` summarizes once at turn 3 with the auxiliary fold
  (native compaction off: no main request is recorded) and the real
  ``_persist_compaction_checkpoint`` writes the checkpoint row.
- The restored process has no context manager or auxiliary model, so the
  resume re-bound is skipped (it is a no-op at this size).
- The Anthropic wire answers with a signed thinking block when a step has
  reasoning (the harness's Anthropic wire has none).
- Placements the loop never makes today, constructed at the loop's own
  seams: ``image-turn-boundary`` holds turn 1's App Guide boundary back from
  the first call (the planner then appends it at the second call, after the
  image row); ``standalone-entry`` appends a memory entry after turn 1's
  text-only answer before the probe turn starts (the planner runs only before
  a provider call, never after a turn's final answer).
- Tool arguments are written in jsonb's key order in every scenario except
  ``tool-args-key-order``, so that one finding stays in one scenario.

Markers: no case differs today. A case in ``KNOWN_DRIFT`` is
``xfail(strict=True)`` with its causes (``CAUSES``): ``F1: ...``
(provider-native assistant structure), ``F7: ...`` (the synthetic image row)
or ``jsonb: ...`` (key order through a jsonb column). Only a ``RestoreDrift``
counts as the expected failure. ``expected_causes`` names the causes of each
case and ``observed_causes`` detects every cause found so far in the two
requests; a case fails outright when they disagree, so a case that differs
again fails naming its causes, and a fix that removes one of a listed case's
causes turns the run red until ``KNOWN_DRIFT`` is updated. A difference no
detector explains fails outright too.

F7 as the soak saw it (an empty AI message coming back as ``" "``, the image
row gaining a trailing space) came from the serializer joining every list part
with a space, empty ones included; since ``509118a3e`` it skips empty parts,
and the plain and tool-call cases here pin that. F7 at the wire was
``image-dropped`` and ``image-entry-joined``, F1 the ``responses-*`` and
``gemini-*`` causes; ``jsonb-tool-args`` went with F7's argument text, and
``anthropic-thinking-key-order`` and ``responses-item-key-order`` (a turn's
native blocks handed back by ``provider_raw`` shortest key first) went when
the blocks were stored the same way. Every case sends the live bytes.
"""

from __future__ import annotations

import asyncio
import copy
import difflib
import itertools
import json
import re
from dataclasses import dataclass, field, replace
from datetime import datetime
from types import SimpleNamespace
from typing import Any, Callable, Dict, List, Optional, Tuple
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID, uuid4

import pytest
from langchain_core.messages import AIMessage, BaseMessage

from agent.core.thread_messages import _persist_one_message
from agent.database.postgres_db import PostgresDB
from agent.services.image_content import IMAGE_DATA_TAG_TEMPLATE
from shared.row_identity import _coerce_row_id
from shared.runtime.core.context_entries import make_context_entry
from shared.runtime.core.message_markers import stamp_turn_membership
from tests import _prompt_cache_prefix_harness as harness
from tests.test_session_delegation_batch_characterization import _fresh_pool
from tests.test_subagent_thread_migration import (  # noqa: F401  (scratch_pg_dsn fixture)
    _agent_db,
    scratch_pg_dsn,
)

# langchain-openai warns about SRW's `reasoning_effort` in model_kwargs on
# every Chat Completions client; it is not under test here.
pytestmark = pytest.mark.filterwarnings(
    "ignore:Parameters .*reasoning_effort.* should be specified explicitly:UserWarning"
)

THREAD = "8d0f3c52-6b1e-4f7a-9c2d-3e5b7a9f1c04"
SYSTEM_PROMPT = "You are the SRW session assistant under test."
PROBE_INPUT = "Probe: is anything else still open?"
# A 1x1 PNG: small enough that the image tier never re-encodes it.
PNG_B64 = (
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA"
    "60e6kgAAAABJRU5ErkJggg=="
)
LATE_MEMORY = "The image renderer lives in tools/render.py."
STANDALONE_MEMORY = "[m:5d1e0b] The owner wants answers in bullet points."
SUMMARY = "## Objective\n- Answer questions about notes.md and plan.md."

_STR = {"type": "string"}
TOOL_SCHEMAS = [
    *harness.SESSION_TOOLS,
    harness._function_tool(
        "view_image", "Show an image file", {"path": _STR, "why": _STR}
    ),
]


class RestoreDrift(AssertionError):
    """The restored probe request differs from the live one on the wire."""


# ---------------------------------------------------------------------------
# Clients
# ---------------------------------------------------------------------------

#: Client label -> harness family. Each is the client the loader wires for it.
CLIENTS: Dict[str, str] = {
    # langchain-openai Chat Completions (ReasoningChatOpenAI), first party.
    "chat": "openai-chat",
    # Chat Completions with reasoning_content (OpenRouter's GLM 5.2).
    "chat-reasoning": "glm-5.2",
    # Claude through the proxy's Chat Completions route, cache_control on.
    "chat-claude-proxy": "openai-chat-claude-proxy",
    # langchain-openai Responses input, first party and the Codex lane.
    "responses": "responses-openai",
    "responses-codex": "responses-codex",
    # Native Messages API (ChatAnthropic), direct and through the proxy.
    "anthropic": "anthropic",
    "anthropic-proxy": "anthropic-claude-proxy",
    # ChatGoogleGenerativeAI.
    "gemini": "gemini",
}


# ---------------------------------------------------------------------------
# Scenarios
# ---------------------------------------------------------------------------

StepSpec = Callable[[int], harness.Step]


def _reasoning(k: int) -> str:
    return f"Reasoning STEP{k:02d}: work out what to do next."


def say(text: str, *, reasoning: bool = False) -> StepSpec:
    """A text-only answer (ends the turn)."""
    return lambda k: harness.Step(
        text=f"STEP{k:02d} {text}", reasoning=_reasoning(k) if reasoning else ""
    )


def call(
    *calls: Tuple[str, str],
    text: str = "",
    reasoning: bool = False,
    jsonb_order: bool = True,
) -> StepSpec:
    """One assistant message calling ``(tool, path)`` pairs, in order.

    The step marker rides in each call's ``why`` argument. ``jsonb_order``
    writes the arguments in jsonb's key order (``why`` before ``path``), so a
    jsonb round trip leaves them as they are.
    """

    def build(k: int) -> harness.Step:
        marker = f"STEP{k:02d}"
        args = [
            {"why": marker, "path": path}
            if jsonb_order
            else {"path": path, "why": marker}
            for _, path in calls
        ]
        return harness.Step(
            calls=tuple((name, a) for (name, _), a in zip(calls, args)),
            text=f"{marker} {text}" if text else "",
            reasoning=_reasoning(k) if reasoning else "",
        )

    return build


@dataclass(frozen=True)
class Scenario:
    """Turns 1..N of a session; the probe turn N+1 is added by the runner.

    ``memory`` maps (turn, provider call of the turn) to the memory records a
    finished retrieval hands in there: ``"base"`` (two memories and the
    knowledge note) or ``"late"`` (the same plus :data:`LATE_MEMORY`).
    """

    turns: Tuple[Tuple[Any, Tuple[StepSpec, ...]], ...]
    memory: Dict[Tuple[int, int], str] = field(default_factory=lambda: {(1, 1): "base"})
    #: Compact once at the start of this turn (real ContextManager).
    compact_at_turn: Optional[int] = None
    #: Hold this turn's App Guide boundary back from its first provider call.
    late_turn_boundary: Optional[int] = None
    #: Append a memory entry after the last answer before the probe turn.
    entry_after_last_answer: bool = False
    #: Interrupt the tool batch of this turn before its first tool runs.
    interrupt_tools_in_turn: Optional[int] = None

    def script(self) -> Tuple[harness.Step, ...]:
        specs = [spec for _, steps in self.turns for spec in steps]
        specs.append(say("Nothing else is open."))
        return tuple(spec(k) for k, spec in enumerate(specs))

    @property
    def inputs(self) -> List[Any]:
        return [turn_input for turn_input, _ in self.turns]

    @property
    def probe_step(self) -> int:
        return sum(len(steps) for _, steps in self.turns)


_NOTES = ("read_file", "notes.md")
_PLAN = ("read_file", "plan.md")
_IMAGE = ("view_image", "chart.png")
_ANSWER_ONE = say("notes.md lists three open items.")
_ANSWER_TWO = say("plan.md schedules the release for Friday.")

SCENARIOS: Dict[str, Scenario] = {
    # Two text-only turns.
    "plain-text": Scenario(
        turns=(
            ("Turn one: hello?", (say("Hello. How can I help?"),)),
            ("Turn two: what can you do?", (say("I read and write files."),)),
        )
    ),
    # A tool call without text: the assistant message's content is empty.
    "tool-call-empty-content": Scenario(
        turns=(("What is in notes.md?", (call(_NOTES), _ANSWER_ONE)),)
    ),
    # One assistant message calling two tools; two results.
    "parallel-tool-batch": Scenario(
        turns=(("Compare notes.md and plan.md.", (call(_NOTES, _PLAN), _ANSWER_TWO)),)
    ),
    # Visible text and a tool call in one assistant message.
    "text-and-tool-call": Scenario(
        turns=(
            (
                "What is in notes.md?",
                (call(_NOTES, text="Let me look at notes.md."), _ANSWER_ONE),
            ),
        )
    ),
    # The same tool call with its arguments in an order jsonb rewrites.
    "tool-args-key-order": Scenario(
        turns=(
            ("What is in notes.md?", (call(_NOTES, jsonb_order=False), _ANSWER_ONE)),
        )
    ),
    # A tool returns an image: the loop appends the synthetic image row.
    "image": Scenario(
        turns=(("Show me chart.png.", (call(_IMAGE), say("The chart rises."))),)
    ),
    # The image row carries a memory entry: the retrieval finished during
    # the tool call, so it is taken in at the call after the image (D6, D8).
    "image-memory-entry": Scenario(
        turns=(("Show me chart.png.", (call(_IMAGE), say("The chart rises."))),),
        memory={(1, 1): "base", (1, 2): "late"},
    ),
    # The image row carries the turn's App Guide boundary (constructed).
    "image-turn-boundary": Scenario(
        turns=(("Show me chart.png.", (call(_IMAGE), say("The chart rises."))),),
        late_turn_boundary=1,
    ),
    # Reasoning on a tool-call step and on the answer, in each provider's
    # shape (reasoning_content, Responses reasoning items, Anthropic thinking
    # blocks, Gemini thought parts).
    "reasoning": Scenario(
        turns=(
            (
                "What is in notes.md?",
                (
                    call(_NOTES, reasoning=True),
                    say("notes.md lists three items.", reasoning=True),
                ),
            ),
        )
    ),
    # Turn two's input is a system event (persisted as role='event').
    "event-row": Scenario(
        turns=(
            ("Start the report job.", (say("Started job 7f3a2c."),)),
            (
                {
                    "role": "event",
                    "content": "[job finished] Job 7f3a2c completed: report.md written.",
                },
                (say("The report job finished."),),
            ),
        )
    ),
    # A context entry after a text-only answer folds as a standalone user
    # message (constructed).
    "standalone-entry": Scenario(
        turns=(("Turn one: hello?", (say("Hello. How can I help?"),)),),
        entry_after_last_answer=True,
    ),
    # Turn 3 starts with a compaction; the restore takes Path A (summary +
    # the rows after its boundary).
    "compaction": Scenario(
        turns=(
            ("What is in notes.md?", (call(_NOTES), _ANSWER_ONE)),
            ("And plan.md?", (call(_PLAN), _ANSWER_TWO)),
            ("Summarize both.", (say("Three items, released Friday."),)),
        ),
        compact_at_turn=3,
    ),
    # A tool batch interrupted before it ran: the assistant call stays
    # without results and the loop appends a "[tool-call interruption]" event.
    "interrupted-tool-call": Scenario(
        turns=(
            ("What is in notes.md?", (call(_NOTES),)),
            ("Never mind. Hello?", (say("Hello."),)),
        ),
        interrupt_tools_in_turn=1,
    ),
}


# ---------------------------------------------------------------------------
# The stored rows (thread_messages, jsonb modelled)
# ---------------------------------------------------------------------------


def jsonb(value: Any) -> Any:
    """``value`` as a Postgres ``jsonb`` column hands it back.

    jsonb keeps one canonical object key order: shorter keys first, then
    bytewise. Arrays keep their order; strings and numbers are unchanged
    for the values these rows carry.
    """
    if isinstance(value, dict):
        keys = sorted(value, key=lambda key: (len(key.encode()), key.encode()))
        return {key: jsonb(value[key]) for key in keys}
    if isinstance(value, list):
        return [jsonb(item) for item in value]
    return value


_COLUMNS = (
    "content",
    "tool_calls",
    "turn_number",
    "metrics",
    "tool_call_id",
    "thinking",
    "reasoning",
    "tool_results",
    "provider",
    "provider_raw",
    "additional_kwargs",
    "response_metadata",
)
_JSONB_COLUMNS = frozenset(
    {
        "tool_calls",
        "metrics",
        "reasoning",
        "tool_results",
        "provider_raw",
        "additional_kwargs",
        "response_metadata",
    }
)


class StoredRows:
    """``thread_messages`` of one thread: the upsert and the reader's queries.

    ``writes`` logs every write call as it arrived, so a real Postgres can be
    given the same writes (``test_stored_rows_read_back_like_postgres``).
    """

    def __init__(self) -> None:
        self.rows: Dict[str, Dict[str, Any]] = {}
        self.writes: List[Tuple[str, Any]] = []
        self._seq = itertools.count(1)

    def copy(self) -> "StoredRows":
        other = StoredRows()
        other.rows = copy.deepcopy(self.rows)
        other.writes = copy.deepcopy(self.writes)
        last = max((row["seq"] for row in self.rows.values()), default=0)
        other._seq = itertools.count(last + 1)
        return other

    # --- writes (PostgresDB.save_thread_message / save_thread_messages) ----

    def _upsert(self, thread_id: str, row: Dict[str, Any]) -> Dict[str, Any]:
        assert thread_id == THREAD
        row = dict(row)
        insert_if_absent = row.pop("insert_if_absent", False)
        row_id = _coerce_row_id(row.get("id"))
        values = {column: row.get(column) for column in _COLUMNS}
        for column in _JSONB_COLUMNS:
            if values[column] is not None:
                # json.dumps on the way in, jsonb in the column.
                values[column] = jsonb(json.loads(json.dumps(values[column])))
        stored = self.rows.get(row_id)
        if stored is None:
            stored = {"id": row_id, "role": row["role"], "seq": next(self._seq)}
            stored.update(values)
            self.rows[row_id] = stored
        elif insert_if_absent:
            stored["tool_call_id"] = values["tool_call_id"]
        else:
            for column in _COLUMNS:
                if column == "additional_kwargs":
                    if values[column] is not None:
                        stored[column] = values[column]
                elif column == "metrics" and stored["role"] != "ai":
                    if values[column] is not None:
                        stored[column] = values[column]
                else:
                    stored[column] = values[column]
        return {"id": row_id, "seq": stored["seq"]}

    async def save_thread_message(self, thread_id: str, **row: Any) -> Dict[str, Any]:
        self.writes.append(("one", copy.deepcopy(row)))
        return self._upsert(thread_id, row)

    async def save_thread_messages(
        self, thread_id: str, rows: List[Dict[str, Any]], **boundary: Any
    ) -> Optional[str]:
        assert not boundary, "the pinned lane's reconcile carries no boundary"
        self.writes.append(("batch", copy.deepcopy(rows)))
        for row in rows:
            self._upsert(thread_id, row)
        return "producer-1" if boundary.get("turn_input_message_id") else None

    # --- reads (the PostgresDB reader's queries) ---------------------------

    def ordered(self) -> List[Dict[str, Any]]:
        return sorted(self.rows.values(), key=lambda row: row["seq"])

    async def fetch(self, query: str, *params: Any) -> List[Dict[str, Any]]:
        """``get_thread_messages_history``'s query over the stored rows."""
        assert "FROM thread_messages AS message" in query, query
        assert params[0] == THREAD
        assert "since_turn" not in query and "turn_number > $" not in query
        rows = [
            row for row in self.ordered() if row["role"] not in ("summary", "error")
        ]
        if "boundary.turn" in query:  # the seq_gt cursor
            boundary_seq = params[1]
            boundary = next(
                (row for row in self.rows.values() if row["seq"] == boundary_seq),
                None,
            )
            boundary_turn = boundary["turn_number"] if boundary else None
            rows = [
                row
                for row in rows
                if row["seq"] > boundary_seq
                or (
                    boundary_turn is not None
                    and row["turn_number"] is not None
                    and row["turn_number"] > boundary_turn
                )
            ]
        if "ORDER BY seq DESC" in query:
            rows.reverse()
        limit = re.search(r"LIMIT \$(\d+)", query)
        offset = re.search(r"OFFSET \$(\d+)", query)
        start = params[int(offset.group(1)) - 1] if offset else 0
        rows = rows[start:]
        if limit:
            rows = rows[: params[int(limit.group(1)) - 1]]
        with_raw = "message.provider_raw" in query

        def text(value: Any) -> Optional[str]:
            # asyncpg hands jsonb back as its JSON text.
            return json.dumps(value) if value is not None else None

        return [
            {
                "id": row["id"],
                "role": row["role"],
                "content": row["content"],
                "tool_calls": text(row["tool_calls"]),
                "tool_call_id": row["tool_call_id"],
                "turn_number": row["turn_number"],
                "admitted_turn_number": None,
                "additional_kwargs": text(row["additional_kwargs"])
                if row["role"] == "context"
                else None,
                **({"provider_raw": text(row["provider_raw"])} if with_raw else {}),
            }
            for row in rows
        ]

    async def fetchrow(self, query: str, *params: Any) -> Optional[Dict[str, Any]]:
        assert params[0] == THREAD
        if "role = 'summary'" in query:  # get_latest_compaction_checkpoint
            summaries = [row for row in self.ordered() if row["role"] == "summary"]
            if not summaries:
                return None
            latest = max(
                summaries, key=lambda row: (row["turn_number"] or 0, row["seq"])
            )
            return {
                "content": latest["content"],
                "metrics": json.dumps(latest["metrics"]),
                "turn_number": latest["turn_number"],
            }
        if query.startswith("SELECT seq FROM thread_messages"):
            row = self.rows.get(params[1])
            return {"seq": row["seq"]} if row else None
        raise AssertionError(f"unexpected query: {query}")

    def db(self) -> PostgresDB:
        """A ``PostgresDB`` whose reader runs for real over these rows."""
        db = PostgresDB.__new__(PostgresDB)
        db.fetch = self.fetch
        db.fetchrow = self.fetchrow
        db.save_thread_message = self.save_thread_message
        db.save_thread_messages = self.save_thread_messages
        return db


# ---------------------------------------------------------------------------
# Provider, memory, context manager
# ---------------------------------------------------------------------------


def _anthropic_wire(step: harness.Step, k: int, model: str, stream: bool, rc: bool):
    """The harness's Anthropic wire plus a signed thinking block for reasoning."""
    content_type, data = harness._anthropic_wire(step, k, model, stream, rc)
    if not step.reasoning:
        return content_type, data
    thinking = {"type": "thinking", "thinking": step.reasoning, "signature": f"sig-{k}"}
    if not stream:
        message = json.loads(data)
        message["content"].insert(0, thinking)
        return content_type, json.dumps(message).encode()
    frames = [frame for frame in data.decode().split("\n\n") if frame]
    events = [json.loads(frame.split("data: ", 1)[1]) for frame in frames]
    for event in events:
        if event.get("index") is not None:
            event["index"] += 1
    block = [
        {
            "type": "content_block_start",
            "index": 0,
            "content_block": {"type": "thinking", "thinking": "", "signature": ""},
        },
        {
            "type": "content_block_delta",
            "index": 0,
            "delta": {"type": "thinking_delta", "thinking": step.reasoning},
        },
        {
            "type": "content_block_delta",
            "index": 0,
            "delta": {"type": "signature_delta", "signature": f"sig-{k}"},
        },
        {"type": "content_block_stop", "index": 0},
    ]
    events = events[:1] + block + events[1:]
    return harness._sse_response(
        [f"event: {e['type']}\ndata: {json.dumps(e)}\n\n" for e in events]
    )


_WIRE = {**harness._WIRE, "anthropic": _anthropic_wire}


class Provider(harness.FakeProvider):
    """The harness's fake provider, answering its script in order.

    The harness picks the step from the request's ``STEPnn`` markers; a
    compaction evicts markers, so here each request takes the next step from
    ``start`` (no request is retried in these runs: ``max_retries=0`` and no
    empty answer). It also answers Anthropic thinking blocks and keeps each
    raw request body.
    """

    def __init__(
        self, script: Tuple[harness.Step, ...], family: Any, start: int = 0
    ) -> None:
        super().__init__(script, family)
        self.start = start
        self.raw: List[str] = []

    def respond(self, request: Any, http: Any) -> Any:
        url = str(request.url)
        api = harness._api_of(url)
        if api is None:
            raise AssertionError(f"unexpected outbound request: {url}")
        raw = request.content.decode("utf-8")
        body = json.loads(raw) if raw else {}
        self.requests.append(harness.Captured(api=api, url=url, body=body))
        self.raw.append(raw)
        index = self.start + len(self.requests) - 1
        if index >= len(self.script):
            raise AssertionError(f"request {len(self.requests)} is past the script")
        stream = body.get("stream") is True or ":streamGenerateContent" in url
        content_type, data = _WIRE[api](
            self.script[index],
            index,
            self.family.model,
            stream,
            self.family.reasoning_field,
        )
        return http.Response(200, headers={"content-type": content_type}, content=data)


class TurnMemory(harness.RecordingMemoryManager):
    """Hands a retrieval result in at scheduled (turn, provider call) points.

    ``start_retrieval`` runs once at each turn start; ``take_retrieval`` once
    before each provider call, as the loop drives them. ``first_turn`` is the
    turn id of the first turn this process runs.
    """

    def __init__(self, schedule: Dict[Tuple[int, int], str], first_turn: int) -> None:
        super().__init__()
        self.schedule = schedule
        self.turn = first_turn - 1
        self.call = 0

    def start_retrieval(self, req: Any) -> bool:
        self.turn += 1
        self.call = 0
        self.assemble_requests.append(req)
        return True

    def take_retrieval(self) -> Optional[Any]:
        from agent.services.memory import (
            AssembleStats,
            InjectionBlock,
            MemoryPayload,
            RetrievalResult,
        )
        from shared.runtime.services.recall_store import MemoryRecord

        self.call += 1
        kind = self.schedule.get((self.turn, self.call))
        if kind is None:
            return None
        memories = harness.memory_records()
        if kind == "late":
            memories.append(
                MemoryRecord(
                    id=UUID(int=42),
                    content=LATE_MEMORY,
                    memory_type="factual",
                    importance=0.5,
                    token_count=len(LATE_MEMORY) // 4,
                )
            )
        blocks = [
            InjectionBlock(kind="memory", token_count=40, records=memories),
            InjectionBlock(
                kind="knowledge", token_count=20, records=harness.knowledge_records()
            ),
        ]
        return RetrievalResult(
            payload=MemoryPayload(blocks=blocks, stats=AssembleStats(blocks=2)),
            seq=len(self.assemble_requests),
        )


def _compacting_context_manager(model: str) -> Any:
    """The session's real ``ContextManager``, compacting only when armed."""
    from agent.core.context import ContextConfig, ContextManager

    class OnceContextManager(ContextManager):
        armed = False

        def should_summarize(self, messages: List[BaseMessage]) -> bool:
            return self.armed

        async def summarize_and_compact(self, *args: Any, **kwargs: Any):
            self.armed = False
            return await super().summarize_and_compact(*args, **kwargs)

        def record_main_request(self, *args: Any, **kwargs: Any) -> None:
            # No recorded request: compaction takes the auxiliary fold.
            self._last_request = None

    return OnceContextManager(
        config=ContextConfig(
            compaction_threshold_tokens=10**9,
            summarization_threshold_tokens=10**9,
            keep_recent_messages=4,
            model_max_context_tokens=10**9,
        ),
        model=model,
        preserve_message_identity=True,
    )


def _stub_context_manager() -> Any:
    context_manager = AsyncMock()
    context_manager.should_summarize = MagicMock(return_value=False)
    context_manager.ensure_within_limits = AsyncMock(
        side_effect=lambda history, *_a, **_k: history
    )
    context_manager.config.keep_recent_messages = 10
    context_manager.record_provider_usage = MagicMock()
    context_manager.record_main_request = MagicMock()
    return context_manager


def _summarizer() -> Any:
    from shared.runtime.services.auxiliary import AuxiliaryLLM

    llm = MagicMock()
    llm.ainvoke = AsyncMock(
        return_value=AIMessage(
            content=SUMMARY, response_metadata={"finish_reason": "stop"}
        )
    )
    return AuxiliaryLLM(llm=llm, max_context_tokens=15_000)


def _config(family: Any) -> Any:
    config = harness._session_config(injections=True, model=family.model)
    config.image_quality = "standard"
    config.limits.image_tokens = {}
    return config


def _image_tool() -> MagicMock:
    async def run(args: Any) -> str:
        return f"Rendered {args.get('path')}:\n" + IMAGE_DATA_TAG_TEMPLATE.format(
            mime="image/png", b64=PNG_B64
        )

    tool = MagicMock()
    tool.name = "view_image"
    tool.args_schema = None
    tool.ainvoke = AsyncMock(side_effect=run)
    return tool


def _tools() -> List[Any]:
    tools = [
        harness._session_tool(t["function"]["name"]) for t in harness.SESSION_TOOLS
    ]
    return [*tools, _image_tool()]


def _late_boundary_planner(turn: Optional[int]) -> Callable[..., Any]:
    """The real planner, holding ``turn``'s App Guide boundary back from the
    turn's first provider call, so it is planned at the second."""
    from agent.persistent_graph import plan_context_entries

    calls: Dict[Optional[int], int] = {}

    def plan(messages: Any, sources: Any, **kwargs: Any) -> Any:
        calls[sources.turn] = calls.get(sources.turn, 0) + 1
        if sources.turn == turn and calls[sources.turn] == 1:
            sources = replace(sources, turn_boundary="")
        return plan_context_entries(messages, sources, **kwargs)

    return plan


# ---------------------------------------------------------------------------
# Running the session
# ---------------------------------------------------------------------------


@dataclass
class Session:
    """What one process of the session loop needs and records."""

    family: Any
    scenario: Scenario
    rows: StoredRows
    provider: Provider
    messages: List[BaseMessage]
    first_turn: int
    turn: int = 0
    interrupt_armed: bool = False


async def _run_session(session: Session, inputs: Callable[[], Any], monkeypatch: Any):
    """Run the real session loop until ``inputs`` raises CancelledError."""
    import agent.api.persistent_app as pa
    from agent.api.persistent_app import _save_turn_ai_messages
    from agent.persistent_graph import PersistentLoopCallbacks, run_persistent_loop
    from shared.runtime.core.loader import create_llm

    family, scenario = session.family, session.scenario
    session.provider.install(monkeypatch)
    llm_config = family.llm_config()
    llm = harness.bind_like_srw(create_llm(llm_config), llm_config, TOOL_SCHEMAS)
    db = session.rows.db()
    session.turn = session.first_turn - 1
    compacting = scenario.compact_at_turn is not None
    context_manager = (
        _compacting_context_manager(family.model)
        if compacting
        else _stub_context_manager()
    )
    errors: List[Any] = []

    async def on_turn_start(turn_id: int) -> None:
        session.turn = turn_id
        if compacting:
            context_manager.armed = turn_id == scenario.compact_at_turn

    async def persist(msg: Any) -> bool:
        await _persist_one_message(db, THREAD, msg, session.turn)
        if (
            session.turn == scenario.interrupt_tools_in_turn
            and isinstance(msg, AIMessage)
            and msg.tool_calls
        ):
            session.interrupt_armed = True
        return True

    def check_interrupt() -> Optional[bool]:
        if session.interrupt_armed:
            session.interrupt_armed = False
            return True
        return None

    async def on_turn_complete(turn_id, metrics, input_id, scope_kind, scope_id, **_):
        await _save_turn_ai_messages(db, THREAD, session.messages, turn_id, metrics)

    async def on_error(*args: Any, **kwargs: Any) -> None:
        errors.append((args, kwargs))

    async def on_context_compacted(summary_text: str, before: int, after: int) -> None:
        # The transport's checkpoint write (persistent_app._record_compaction).
        await pa._persist_compaction_checkpoint(summary_text, before, after, "auto")

    callbacks = PersistentLoopCallbacks(
        get_user_input=AsyncMock(side_effect=inputs),
        on_token=AsyncMock(),
        on_thinking=AsyncMock(),
        on_tool_start=AsyncMock(),
        on_tool_result=AsyncMock(),
        permission_check=AsyncMock(return_value=True),
        on_turn_start=on_turn_start,
        on_turn_complete=on_turn_complete,
        on_error=on_error,
        check_interrupt=check_interrupt,
        persist_message=persist,
        on_context_compacted=on_context_compacted,
    )
    knowledge_store = MagicMock()
    knowledge_store.get_charter_note = AsyncMock(return_value=harness.CHARTER)

    class Transport(SimpleNamespace):
        """``persistent_app._session`` as the checkpoint write reads it."""

        @property
        def turn_count(self) -> int:
            return session.turn

    transport = Transport(postgres_conn=db, context_manager=context_manager)
    with (
        patch.object(pa, "_session", transport),
        patch.object(pa._session_identity, "_thread_id", THREAD),
        patch(
            "agent.persistent_graph.plan_context_entries",
            _late_boundary_planner(scenario.late_turn_boundary),
        ),
    ):
        await run_persistent_loop(
            llm_with_tools=llm,
            tools=_tools(),
            context_manager=context_manager,
            config=_config(family),
            system_prompt=SYSTEM_PROMPT,
            callbacks=callbacks,
            messages=session.messages,
            auxiliary_llm=_summarizer() if compacting else None,
            knowledge_store=knowledge_store,
            project_ids=[harness.PROJECT_ID],
            tool_context=SimpleNamespace(
                knowledge_bindings=[],
                citation_engine=None,
                subagent_runtime=SimpleNamespace(
                    active_subagents_block=lambda: harness.ACTIVE_SUBAGENTS
                ),
            ),
            memory_service=TurnMemory(scenario.memory, session.first_turn),
            initial_turn_count=session.first_turn - 1,
            memory_thread_id=THREAD,
        )
    if errors:
        raise AssertionError(f"the session loop reported errors: {errors}")


@dataclass
class Probe:
    """The probe turn's first request, live and restored."""

    live: harness.Captured
    live_raw: str
    restored: harness.Captured
    restored_raw: str
    live_messages: List[BaseMessage]
    restored_messages: List[BaseMessage]
    rows: StoredRows


async def _append_standalone_entry(session: Session, db: PostgresDB) -> None:
    """A memory entry after the last answer, appended and persisted the way
    the loop appends a planned entry (stamped with its turn)."""
    entry = make_context_entry(
        "memory",
        STANDALONE_MEMORY,
        section="memory",
        items=[{"key": "memory:standalone", "hash": "b" * 16, "handle": "m:5d1e0b"}],
        turn=session.turn,
    )
    entry.id = f"msg_{'5' * 24}"
    stamp_turn_membership(entry, session.turn)
    session.messages.append(entry)
    await _persist_one_message(db, THREAD, entry, session.turn)


async def run_case(scenario_id: str, client: str, monkeypatch: Any) -> Probe:
    """Live turns 1..N+1, then a restore of turns 1..N and turn N+1 again."""
    import agent.api.persistent_app as pa

    scenario = SCENARIOS[scenario_id]
    family = harness.FAMILIES[CLIENTS[client]]
    script = scenario.script()

    # Live: turns 1..N, then the probe. The rows are copied when the loop
    # asks for the probe input (after turn N's reconcile).
    live = Session(
        family=family,
        scenario=scenario,
        rows=StoredRows(),
        provider=Provider(script, family),
        messages=[],
        first_turn=1,
    )
    taken: Dict[str, Any] = {}
    queue = list(scenario.inputs)

    async def live_inputs() -> Any:
        if queue:
            return queue.pop(0)
        if "rows" in taken:
            raise asyncio.CancelledError()
        if scenario.entry_after_last_answer:
            await _append_standalone_entry(live, live.rows.db())
        taken["rows"] = live.rows.copy()
        taken["request"] = len(live.provider.requests)
        return PROBE_INPUT

    await _run_session(live, live_inputs, monkeypatch)
    rows: StoredRows = taken["rows"]
    index = taken["request"]
    assert len(live.provider.requests) == len(script), "the live script did not run"

    # Restored: a fresh process restores the copy and runs the probe turn.
    db = rows.copy().db()
    restored_session = SimpleNamespace(
        messages=[],
        context_manager=None,
        auxiliary_llm=None,
        config=_config(family),
        turn_count=0,
    )
    with (
        patch.object(pa, "_session", restored_session),
        patch.object(pa, "_agent", SimpleNamespace(postgres_conn=db)),
        patch.object(pa._session_identity, "_thread_id", THREAD),
    ):
        await pa._restore_session_messages()
    restored = Session(
        family=family,
        scenario=replace(scenario, compact_at_turn=None, interrupt_tools_in_turn=None),
        rows=rows.copy(),
        provider=Provider(script, family, start=scenario.probe_step),
        messages=restored_session.messages,
        first_turn=restored_session.turn_count + 1,
    )
    restored_queue = [PROBE_INPUT]

    async def restored_inputs() -> Any:
        if restored_queue:
            return restored_queue.pop(0)
        raise asyncio.CancelledError()

    restored_snapshot = list(restored_session.messages)
    await _run_session(restored, restored_inputs, monkeypatch)
    assert restored.first_turn == len(scenario.turns) + 1
    return Probe(
        live=live.provider.requests[index],
        live_raw=live.provider.raw[index],
        restored=restored.provider.requests[0],
        restored_raw=restored.provider.raw[0],
        live_messages=live.messages,
        restored_messages=restored_snapshot,
        rows=rows,
    )


# ---------------------------------------------------------------------------
# Comparing at the wire
# ---------------------------------------------------------------------------


def _split(captured: harness.Captured) -> Tuple[str, List[Any]]:
    body = copy.deepcopy(captured.body)
    items = body.pop(harness._ITEM_KEYS[captured.api], [])
    return json.dumps(body, ensure_ascii=False), items


def _dump(value: Any, *, sort: bool = False) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=sort)


def _label(item: Any) -> str:
    if isinstance(item, dict):
        return str(item.get("role") or item.get("type") or "?")
    return type(item).__name__


def _first_bytes(a: str, b: str, context: int = 60) -> str:
    offset = next(
        (i for i, (x, y) in enumerate(zip(a, b)) if x != y), min(len(a), len(b))
    )
    lo = max(0, offset - context)
    return (
        f"at character {offset}\n"
        f"      live    : {a[lo : offset + context]!r}\n"
        f"      restored: {b[lo : offset + context]!r}"
    )


def wire_differences(live: harness.Captured, restored: harness.Captured) -> List[str]:
    """Every difference between the two requests, aligned item by item."""
    problems: List[str] = []
    live_frame, live_items = _split(live)
    restored_frame, restored_items = _split(restored)
    if live_frame != restored_frame:
        problems.append(
            "the request outside the message list differs "
            + _first_bytes(live_frame, restored_frame)
        )
    a = [_dump(item) for item in live_items]
    b = [_dump(item) for item in restored_items]
    matcher = difflib.SequenceMatcher(a=a, b=b, autojunk=False)
    for op, i1, i2, j1, j2 in matcher.get_opcodes():
        if op == "equal":
            continue
        if op == "replace" and i2 - i1 == j2 - j1:
            for i, j in zip(range(i1, i2), range(j1, j2)):
                same_sorted = _dump(live_items[i], sort=True) == _dump(
                    restored_items[j], sort=True
                )
                problems.append(
                    f"item {i} ({_label(live_items[i])}) differs"
                    + (" (key order only)" if same_sorted else "")
                    + " "
                    + _first_bytes(a[i], b[j])
                )
            continue
        if i2 > i1:
            problems.append(
                f"live items {i1}..{i2 - 1} "
                f"({', '.join(_label(x) for x in live_items[i1:i2])}) "
                + (
                    "are missing after the restore"
                    if op == "delete"
                    else "were replaced"
                )
                + f": {a[i1][:160]!r}"
            )
        if j2 > j1:
            problems.append(
                f"restored items {j1}..{j2 - 1} "
                f"({', '.join(_label(x) for x in restored_items[j1:j2])}) "
                + ("are new" if op == "insert" else "came instead")
                + f": {b[j1][:160]!r}"
            )
    return problems


# ---------------------------------------------------------------------------
# Causes found so far, their detectors, and the cases that differ today
# ---------------------------------------------------------------------------

#: Every cause of a restore difference found so far, with its marker text.
#: All are fixed; the detectors name them when one comes back.
CAUSES: Dict[str, str] = {
    "responses-item-ids": (
        "F1: a restored Responses assistant turn loses its item ids (message "
        "'msg_*', function_call 'fc_*'), and its message item is rebuilt with "
        "another key order"
    ),
    "responses-reasoning": (
        "F1: a restored Responses assistant turn loses its reasoning items ('rs_*')"
    ),
    "gemini-thought-signature": (
        "F1: a restored Gemini functionCall part loses its thoughtSignature"
    ),
    "gemini-thought-parts": "F1: a restored Gemini model turn loses its thought parts",
    "anthropic-thinking-key-order": (
        "jsonb: a restored Anthropic thinking block comes back from provider_raw, a "
        "jsonb column, with its keys reordered ('type' first; key order only)"
    ),
    "responses-item-key-order": (
        "jsonb: a restored Responses reasoning or function_call item comes back "
        "from provider_raw, a jsonb column, with its keys reordered ('id' "
        "first; key order only)"
    ),
    "jsonb-tool-args": (
        "jsonb: restored tool-call arguments come back in jsonb key order "
        "(thread_messages.tool_calls and provider_raw are jsonb: shorter keys "
        "first), so the arguments the provider sees are reordered"
    ),
    "image-dropped": (
        "F7: the synthetic image row is stored as its text only, so the restored "
        "row is a plain string without the image part"
    ),
    "image-entry-joined": (
        "F7: an entry folded into the image row is joined into that string with "
        "'\\n\\n' instead of riding as its own text part"
    ),
}

_IMAGE_SCENARIOS = {"image", "image-memory-entry", "image-turn-boundary"}

#: (scenario, client) -> the causes that make the case differ today: none. A
#: difference whose fix cannot land with the change that caused it is listed
#: here, and the case is a strict xfail until it is fixed.
KNOWN_DRIFT: Dict[Tuple[str, str], Tuple[str, ...]] = {}


def expected_causes(scenario_id: str, client: str) -> List[str]:
    """The causes that make ``(scenario_id, client)`` differ today."""
    return list(KNOWN_DRIFT.get((scenario_id, client), ()))


def _strings(value: Any) -> List[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, dict):
        return [s for v in value.values() for s in _strings(v)]
    if isinstance(value, list):
        return [s for v in value for s in _strings(v)]
    return []


def _blocks(items: List[Any]) -> List[Dict[str, Any]]:
    """Every content block / part dict inside the items."""
    found = []
    for item in items:
        if not isinstance(item, dict):
            continue
        for key in ("content", "parts"):
            if isinstance(item.get(key), list):
                found.extend(b for b in item[key] if isinstance(b, dict))
    return found


def _tool_arg_orders(items: List[Any]) -> List[List[str]]:
    """The key order of every replayed tool call's arguments, per client."""
    orders = []
    for item in items:
        if not isinstance(item, dict):
            continue
        for call_ in item.get("tool_calls") or []:  # Chat Completions
            orders.append(list(json.loads(call_["function"]["arguments"])))
        if item.get("type") == "function_call":  # Responses
            orders.append(list(json.loads(item["arguments"])))
    for block in _blocks(items):
        if block.get("type") == "tool_use":  # Anthropic
            orders.append(list(block.get("input") or {}))
        if isinstance(block.get("functionCall"), dict):  # Gemini
            orders.append(list(block["functionCall"].get("args") or {}))
    return orders


def observed_causes(probe: Probe) -> List[str]:
    """The causes the two probe requests show, by their signatures."""
    _, live = _split(probe.live)
    _, restored = _split(probe.restored)
    live_blocks, restored_blocks = _blocks(live), _blocks(restored)
    found = []

    def responses_ids(items: List[Any]) -> int:
        return sum(
            1
            for i in items
            if isinstance(i, dict)
            and i.get("type") in ("message", "function_call")
            and i.get("id")
        )

    if responses_ids(live) > responses_ids(restored):
        found.append("responses-item-ids")

    def count(items: List[Any], kind: str) -> int:
        return sum(1 for i in items if isinstance(i, dict) and i.get("type") == kind)

    if count(live, "reasoning") > count(restored, "reasoning"):
        found.append("responses-reasoning")

    def signed(blocks: List[Dict[str, Any]]) -> int:
        return sum(1 for b in blocks if b.get("thoughtSignature"))

    if signed(live_blocks) > signed(restored_blocks):
        found.append("gemini-thought-signature")

    def thoughts(blocks: List[Dict[str, Any]]) -> int:
        return sum(1 for b in blocks if b.get("thought") is True)

    if thoughts(live_blocks) > thoughts(restored_blocks):
        found.append("gemini-thought-parts")

    def reordered(live_: List[Any], restored_: List[Any], *kinds: str) -> bool:
        """The ``kinds`` blocks are the same but for their key order."""

        def dumps(items: List[Any], sort: bool) -> List[str]:
            return [
                _dump(i, sort=sort)
                for i in items
                if isinstance(i, dict) and i.get("type") in kinds
            ]

        return dumps(live_, True) == dumps(restored_, True) and dumps(
            live_, False
        ) != dumps(restored_, False)

    if reordered(live_blocks, restored_blocks, "thinking"):
        found.append("anthropic-thinking-key-order")
    if reordered(live, restored, "reasoning", "function_call"):
        found.append("responses-item-key-order")

    live_orders, restored_orders = _tool_arg_orders(live), _tool_arg_orders(restored)
    if live_orders != restored_orders and [sorted(o) for o in live_orders] == [
        sorted(o) for o in restored_orders
    ]:
        found.append("jsonb-tool-args")

    if PNG_B64 in probe.live_raw and PNG_B64 not in probe.restored_raw:
        found.append("image-dropped")

    def joined(items: List[Any]) -> bool:
        return any(
            "Image content from tool call" in s and ":\n\n<srw_context" in s
            for s in _strings(items)
        )

    if joined(restored) and not joined(live):
        found.append("image-entry-joined")
    return found


def _cases() -> List[Any]:
    cases = []
    for scenario_id in SCENARIOS:
        for client in CLIENTS:
            causes = expected_causes(scenario_id, client)
            marks = (
                [
                    pytest.mark.xfail(
                        strict=True,
                        raises=RestoreDrift,
                        reason="; ".join(CAUSES[c] for c in causes),
                    )
                ]
                if causes
                else []
            )
            cases.append(
                pytest.param(
                    scenario_id, client, marks=marks, id=f"{scenario_id}-{client}"
                )
            )
    return cases


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def _next_after_image(messages: List[BaseMessage]) -> Optional[BaseMessage]:
    for index, msg in enumerate(messages):
        if isinstance(msg.content, list) and "Image content from tool call" in str(
            msg.content
        ):
            return messages[index + 1] if index + 1 < len(messages) else None
    raise AssertionError("no image row in the live history")


def _check_scenario_ran(scenario_id: str, client: str, probe: Probe) -> None:
    """Non-vacuity: the scenario's feature is in the live history and reached
    the live probe request, and the restore took the path it names."""
    from shared.runtime.core.context_entries import entry_kind
    from shared.runtime.core.message_markers import is_compaction_summary

    raw = probe.live_raw
    if scenario_id in _IMAGE_SCENARIOS:
        assert PNG_B64 in raw, "the image never reached the provider"
        folded = {
            "image-memory-entry": "memory",
            "image-turn-boundary": "turn_boundary",
        }
        after = _next_after_image(probe.live_messages)
        assert entry_kind(after) == folded.get(scenario_id), (
            f"the image row is followed by {after!r}"
        )
    if scenario_id == "image-memory-entry":
        assert LATE_MEMORY in raw
    if scenario_id == "standalone-entry":
        assert STANDALONE_MEMORY in raw
    if scenario_id == "event-row":
        assert "[job finished]" in raw
        assert "[job finished]" in str(probe.restored_messages)
    if scenario_id == "compaction":
        assert "## Objective" in raw, "the compaction summary is not on the wire"
        # Path A: the restore starts with the checkpoint's summary.
        assert is_compaction_summary(probe.restored_messages[0])
    if scenario_id == "interrupted-tool-call":
        assert "[tool-call interruption]" in raw
    if scenario_id == "reasoning":
        # Each provider's reasoning shape is replayed live (Chat Completions
        # never sends reasoning_content back).
        _, items = _split(probe.live)
        replayed = {
            "responses": any(
                isinstance(i, dict) and i.get("type") == "reasoning" for i in items
            ),
            "anthropic": any(b.get("type") == "thinking" for b in _blocks(items)),
            "gemini": any(b.get("thought") is True for b in _blocks(items)),
        }
        family = next((key for key in replayed if client.startswith(key)), None)
        if family is not None:
            assert replayed[family], "no reasoning was replayed live"


@pytest.mark.asyncio
@pytest.mark.parametrize(("scenario_id", "client"), _cases())
async def test_the_restored_probe_request_is_the_live_one(
    scenario_id: str, client: str, monkeypatch: Any
) -> None:
    """The probe turn's first request is byte-identical after a restore.

    A case that differs raises ``RestoreDrift`` (its expected failure). A
    case whose causes no longer match ``expected_causes`` fails outright: a
    fix landed (remove its cause there), or a new difference appeared.
    """
    probe = await run_case(scenario_id, client, monkeypatch)
    _check_scenario_ran(scenario_id, client, probe)
    problems = wire_differences(probe.live, probe.restored)
    observed = observed_causes(probe)
    expected = expected_causes(scenario_id, client)
    details = "\n  - ".join(problems) or "(identical)"
    assert sorted(observed) == sorted(expected), (
        f"{scenario_id} on {client}: expected causes {expected}, observed "
        f"{observed}. Update expected_causes if a fix landed.\n  - {details}"
    )
    if problems and not observed:
        raise AssertionError(
            f"{scenario_id} on {client}: unexplained drift\n  - {details}"
        )
    if problems:
        raise RestoreDrift(
            f"{scenario_id} on {client}: the restored probe request differs\n  - {details}"
        )
    assert probe.live_raw == probe.restored_raw


# ---------------------------------------------------------------------------
# The StoredRows model against a real Postgres
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def pg_dsn(scratch_pg_dsn: str) -> str:  # noqa: F811 (pytest fixture param)
    """The migration test's scratch Postgres (testcontainers), reused as-is."""
    return scratch_pg_dsn


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("scenario_id", "client"),
    [
        # tool_calls jsonb with arguments jsonb reorders.
        ("tool-args-key-order", "chat"),
        # provider_raw jsonb (Anthropic thinking and tool_use blocks, and
        # Responses reasoning and function_call items, as JSON text).
        ("reasoning", "anthropic"),
        ("reasoning", "responses"),
        # The image row (role 'event') with a folded memory entry (role
        # 'context', additional_kwargs jsonb).
        ("image-memory-entry", "gemini"),
        # The compaction checkpoint row (role 'summary', metrics jsonb).
        ("compaction", "responses"),
    ],
)
async def test_stored_rows_read_back_like_postgres(
    scenario_id: str, client: str, pg_dsn: str, monkeypatch: Any
) -> None:
    """``StoredRows`` hands back what Postgres hands back.

    Every write the live run made before the probe turn is replayed, in
    order, through the real ``PostgresDB`` writer into a migrated scratch
    database. The real reader's rows and checkpoint must equal the model's,
    key order included: that is what makes the matrix above a statement
    about stored rows, not about the model.
    """
    probe = await run_case(scenario_id, client, monkeypatch)
    pool = await _fresh_pool(pg_dsn)
    try:
        async with pool.acquire() as conn:
            owner = uuid4()
            await conn.execute(
                "INSERT INTO users (id, display_name) VALUES ($1, 'owner')", owner
            )
            await conn.execute(
                "INSERT INTO threads (id, user_id, title, status, execution_lane) "
                "VALUES ($1, $2, 'Wire fidelity', 'active', 'pinned')",
                UUID(THREAD),
                owner,
            )
        real = _agent_db(pool)
        for kind, payload in probe.rows.writes:
            if kind == "one":
                await real.save_thread_message(thread_id=THREAD, **payload)
            else:
                await real.save_thread_messages(THREAD, payload)
        model = probe.rows.db()
        read = {
            "thread_id": THREAD,
            "limit": 1000,
            "newest_first": True,
            "include_provider_raw": True,
        }
        real_rows = await real.get_thread_messages_history(**read)
        assert real_rows, "nothing was written"
        if scenario_id == "tool-args-key-order":
            # The model wrote {"path", "why"}; Postgres hands back jsonb's order.
            (call_row,) = [row for row in real_rows if row["tool_calls"]]
            assert list(call_row["tool_calls"][0]["args"]) == ["why", "path"]
        model_rows = await model.get_thread_messages_history(**read)
        # The write time (the Responses retention check reads it) is the
        # one column the model leaves out.
        assert all(isinstance(row.pop("created_at"), datetime) for row in real_rows)
        assert all(row.pop("created_at") is None for row in model_rows)
        assert json.dumps(real_rows) == json.dumps(model_rows)
        assert json.dumps(await real.get_latest_compaction_checkpoint(THREAD)) == (
            json.dumps(await model.get_latest_compaction_checkpoint(THREAD))
        )
    finally:
        await pool.close()
