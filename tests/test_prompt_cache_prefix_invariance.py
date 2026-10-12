"""Prompt-cache prefix invariance: request N+1 must start with request N.

Design: knowledge-base/knowledge/features/append_only_context_injection.md
(§2.5 the cache rule, D17-D19 todos, D27 carrier fold, D31 this gate); plan:
knowledge-base/knowledge/plans/append_only_context_injection_plan_2026_10_05.md
(WP0, WP1).

- The cache rule. GPT-5.6 and later (and the Codex subscription path) reuse a
  cache entry only at "the end of the latest user message or last tool
  response", so a request is cached only if it begins with ALL of the previous
  request, byte for byte. Block caches (vLLM, MiniMax, DeepSeek) are kinder,
  but the same rule decides how much of them is reused.
- What the harness proves. tests/_prompt_cache_prefix_harness.py drives a
  scripted conversation through SRW's real request assembly with a fake
  provider behind the HTTP transport, and captures the body of every request:
  the worker execute node in a tactical tool loop with a todo change, its
  Layer-1 safety rebuild and Layer-0 emergency rebuild, and two user turns of
  the session loop. Each pair (N, N+1) is then checked per renderer:
  - API families (OpenAI Chat, Claude through the subscription proxy,
    Responses / Codex, Anthropic Messages, Gemini): the non-message part
    (system/instructions, tools, parameters) is identical, and request N's
    message list is an element-wise prefix of N+1's, each element
    byte-identical as canonical JSON. Anthropic ``cache_control`` markers and
    ``prompt_cache_key`` are normalised away first: SRW moves the markers
    deliberately each request and both are cache hints, not prompt text.
    ``stream``/``stream_options`` are transport flags and are ignored too.
  - Open-weight families: the captured Chat Completions body is rendered
    through the pinned chat template (fixtures/chat_templates, MANIFEST.json)
    the way vLLM does, with ``add_generation_prompt=True``, and the rendered
    request N must be a string prefix of request N+1. ``*-history`` families
    render request N without its generation prompt: that isolates history
    rewrites from a generation prompt the replay does not reproduce.
- Variants (``harness.VARIANTS``). ``injected`` runs with every context
  injection live (memory and knowledge, citation feedback, supervisor
  guidance, active subagents and the todo list for the worker; charter,
  memory and knowledge, active subagents and the App Guide turn boundary for
  the session). ``todos-only`` (worker only) runs with the todo list as the
  one live source, in its WP1 form: the phase-start message and the
  ``todo_complete`` results carry the full list, and nothing is re-sent per
  request (D17-D19); it is WP1's gate. ``control`` switches all of them off,
  each at its own source (no todo list, ``todo_complete`` answered like any
  other tool). A control failure is intrinsic to the template or provider; a
  ``todos-only`` or ``injected`` case counts only the pairs its control does
  not already break, so each case has one cause.
- The Layer-0 emergency rebuild is entered by raising ``ContextOverflowError``
  at the LLM boundary: no real client raises it today (``ReasoningChatOpenAI``
  answers a Layer-0 overflow with a synthetic HTTP 413 instead).
- How to read a failure: each broken pair is reported as
  ``request N -> N+1``, with the first diverging list index and a short diff
  (API), or the first differing character offset with context (template).
- Modes (``harness.INJECTION_MODES``, ``context_management.injection_mode``).
  ``injected`` runs in both: ``legacy-injected`` rebuilds the per-request
  tail and keeps its strict xfail (the rollback mode is not fixed, only kept
  byte-identical); ``append_only-injected`` appends typed context entries
  once and folds them into their carrier (WP2, D27) and must pass for every
  scenario, worker and session. ``worker-memory-churn`` runs append_only
  only: seven memories (two drip in past the per-entry cap) and a memory and
  the note that change later (an "(updated; ...)" entry). Non-vacuity: the
  last append_only worker request carries each kind the scenario injects the
  expected number of times (memory and guidance exactly once without churn);
  the session's last request holds one App Guide turn boundary per user turn
  (two), the charter, memory, knowledge and subagent status once each. Both
  bind ``memory_search`` with the context sources, so both carry the
  up-front memory summary (D35) exactly once, read once per run.
  ``todos-only`` and ``control`` carry no context sources, so they run once,
  in the default mode.
- Asynchronous retrieval (WP3, D6/D8). append_only retrieves memory off the
  request path. In the main cases each retrieval has finished before the
  request that started it takes results in, so every request plans from its
  own retrieval. ``test_memory_that_arrives_a_request_late_keeps_the_prefix``
  delivers every result one request late (the session's turn-one result in
  turn two) and checks that the prefix still holds and the memory arrives.
- Markers. Cases that fail today are ``xfail(strict=True)`` with their cause:
  ``injection: ...`` (fixed by WP2) or ``intrinsic: ...``. Strict is the
  point: when a work package fixes a case it XPASSes, which fails the run and
  forces removing the marker. Only a ``PrefixViolation`` counts as the expected
  failure; any other error in the harness fails the test.
"""

import hashlib

import pytest

from shared.runtime.core.context_entries import UPDATED_ITEM_MARKER, memory_handle
from tests import _prompt_cache_prefix_harness as harness
from tests._prompt_cache_prefix_harness import (
    FAMILIES,
    INJECTION_MODES,
    SCENARIOS,
    VARIANTS,
    Captured,
    api_prefix_violation,
    prefix_violations,
    run_scenario,
    text_occurrences,
    text_prefix_violation,
)


# langchain-openai warns about SRW's `reasoning_effort` in model_kwargs on
# every Chat Completions client this module builds; it is not under test here.
pytestmark = pytest.mark.filterwarnings(
    "ignore:Parameters .*reasoning_effort.* should be specified explicitly:UserWarning"
)


class PrefixViolation(AssertionError):
    """Request N+1 does not start with request N."""


TEMPLATE_FAMILIES = [f for f in FAMILIES if FAMILIES[f].template is not None]
WORKER_SCENARIOS = [s for s in SCENARIOS if s.startswith("worker")]
# Scenarios that only make sense with append-only entries (the churn exists in
# the store records, which legacy mode does not read).
APPEND_ONLY_SCENARIOS = {"worker-memory-churn"}

_WP2 = "fixed by WP2 (append-only carrier fold, D27)"
INJECTION_CAUSES = {
    "worker-tool-loop": (
        "injection: graph.py _inject_transient_messages rebuilds the per-turn "
        "tail on every request (memory + KB synthetic tool-call pairs, citation "
        "feedback, supervisor guidance, <active_subagents>) and re-anchors it "
        "before the new tool results (find_tail_injection_anchor), so request "
        f"N+1 writes history where request N had its tail; {_WP2} with D21 "
        "append-on-change (the todo list left the tail in WP1, D17)"
    ),
    "worker-safety-rebuild": (
        "injection: as in the tool loop, and the Layer-1 safety rebuild in "
        "graph.py re-runs _inject_transient_messages on the rebuilt request; "
        f"{_WP2}, which must route the rebuild through the carrier fold too"
    ),
    "worker-emergency-rebuild": (
        "injection: as in the tool loop, and the Layer-0 emergency rebuild "
        "(graph.py, except ContextOverflowError) re-runs "
        f"_inject_transient_messages; {_WP2}, which must route the rebuild "
        "through the carrier fold too"
    ),
    "session-two-turns": (
        "injection: persistent_graph.py _inject_context_pairs re-anchors the "
        "charter pair, the memory/KB pairs, <active_subagents> and the App "
        "Guide turn-boundary HumanMessage at the tail of every provider call "
        "(_provider_input), so each new tool result or user turn lands where "
        f"the previous call's tail was; {_WP2} with D21/D22 append-on-change"
    ),
}

_GEMMA_GEN_PROMPT = (
    "after tool results the Gemma 4 generation prompt opens a thought channel "
    "('<|channel>thought'), and the replayed model turn has none because SRW "
    "does not send reasoning_content back"
)
_GEMMA_TURN_CLOSE = (
    "Gemma 4 closes a model turn that has visible text with <turn|> only when "
    "no assistant turn follows it (forward scan continues_into_next), so a "
    "tool-call turn with text is re-rendered once the next assistant turn "
    "arrives"
)
_MINIMAX_GEN_PROMPT = (
    "intrinsic: the MiniMax-M2 generation prompt ends with '<think>\\n', and the "
    "replayed assistant turn has no think block because SRW does not send "
    "reasoning_content back; only the generation-prompt tail of request N "
    "differs (minimax-m2-history passes)"
)
INTRINSIC_CAUSES = {
    **{
        (scenario, "gemma-4"): (
            f"intrinsic: {_GEMMA_GEN_PROMPT}; and {_GEMMA_TURN_CLOSE} "
            "(gemma-4-history isolates the second)"
        )
        for scenario in WORKER_SCENARIOS
    },
    ("session-two-turns", "gemma-4"): (
        f"intrinsic: {_GEMMA_GEN_PROMPT}; only the generation-prompt tail of "
        "request N differs (gemma-4-history passes)"
    ),
    **{
        (scenario, "gemma-4-history"): f"intrinsic: {_GEMMA_TURN_CLOSE}"
        for scenario in WORKER_SCENARIOS
    },
    **{(scenario, "minimax-m2"): _MINIMAX_GEN_PROMPT for scenario in SCENARIOS},
    ("session-two-turns", "qwen3.6"): (
        "intrinsic: Qwen3.6 renders the <think></think> wrapper only on "
        "assistant turns after the latest user message (preserve_thinking "
        "off), so turn two re-renders turn one's tool loop without it; passes "
        "with chat_template_kwargs preserve_thinking=true (follow-up N5 in "
        "append_only_context_injection_research/aoci_q3_chat_templates.md)"
    ),
    ("session-two-turns", "deepseek-v3.2"): (
        "intrinsic: in thinking mode the DeepSeek V3.2 encoder ends user turns "
        "and tool results before the latest user message with </think> "
        "instead of opening <think>, so turn two re-renders turn one; the "
        "encoder has no option to keep it"
    ),
}

# The strict check of these families already breaks on (almost) every pair of
# the injection-free control, so it cannot separate injection breaks; their
# *-history family is the injection gate.
SATURATED = {"gemma-4", "minimax-m2"}


def _case(scenario: str, variant: str, family: str, mode: str = "legacy"):
    marks = []
    label = f"{mode}-{variant}" if variant == "injected" else variant
    if variant == "control" and (scenario, family) in INTRINSIC_CAUSES:
        marks.append(
            pytest.mark.xfail(
                strict=True,
                raises=PrefixViolation,
                reason=INTRINSIC_CAUSES[(scenario, family)],
            )
        )
    elif variant != "control" and family in SATURATED:
        marks.append(
            pytest.mark.skip(
                reason=(
                    f"the strict {family} check fails in the injection-free "
                    "control on nearly every pair (generation prompt; see the "
                    f"control case), so {family}-history is this family's "
                    "injection gate"
                )
            )
        )
    elif variant == "injected" and mode == "legacy":
        marks.append(
            pytest.mark.xfail(
                strict=True,
                raises=PrefixViolation,
                reason=INJECTION_CAUSES[scenario],
            )
        )
    # ``todos-only`` carries no marker: WP1's gate, it must pass. Nor does
    # ``append_only-injected``, worker or session: WP2's gate.
    return pytest.param(
        scenario,
        variant,
        mode,
        family,
        id=f"{scenario}-{label}-{family}",
        marks=marks,
    )


def _variants(scenario: str) -> tuple:
    if scenario in APPEND_ONLY_SCENARIOS:
        return ("injected",)
    if scenario in WORKER_SCENARIOS:
        return ("injected", "todos-only", "control")
    return ("injected", "control")  # sessions never carried a todo list


def _modes(scenario: str, variant: str) -> tuple:
    if variant != "injected":
        return ("legacy",)  # no context sources: both modes send the same
    if scenario in APPEND_ONLY_SCENARIOS:
        return ("append_only",)
    return INJECTION_MODES


CASES = [
    _case(scenario, variant, family, mode)
    for scenario in SCENARIOS
    for variant in _variants(scenario)
    for mode in _modes(scenario, variant)
    for family in FAMILIES
]


def _require_renderer(family_id: str) -> None:
    family = FAMILIES[family_id]
    if family.template is not None:
        pytest.importorskip("transformers.utils.chat_template_utils")
    elif family.api == "anthropic":
        pytest.importorskip("langchain_anthropic")
    elif family.api == "gemini":
        pytest.importorskip("langchain_google_genai")


async def _run(scenario, family, variant, tmp_path, monkeypatch, mode="legacy"):
    workdir = tmp_path / f"{mode}-{variant}"
    workdir.mkdir()
    requests, turn_ends = await run_scenario(
        scenario,
        family,
        sources=VARIANTS[variant],
        workdir=workdir,
        monkeypatch=monkeypatch,
        injection_mode=mode,
    )
    return requests, prefix_violations(family, requests, turn_ends=turn_ends)


async def _violations(scenario, family, variant, tmp_path, monkeypatch, mode="legacy"):
    _, found = await _run(scenario, family, variant, tmp_path, monkeypatch, mode)
    return found


def _srw(kind: str) -> str:
    return f'<srw_context kind="{kind}">'


def _assert_memory_summary_once(body: object) -> None:
    """The up-front memory summary (D35) is in the request exactly once:
    given at conversation start, never again (its read is counted by the
    runner: once per run)."""
    topics = "Frequent topics: " + ", ".join(harness.MEMORY_SUMMARY_TOPICS) + "."
    assert text_occurrences(body, topics) == 1
    assert text_occurrences(body, "call memory_search") == 1


def _assert_appended_once(scenario: str, last: Captured) -> None:
    """Non-vacuity of an append_only worker case: the context is there.

    The last request holds every entry the run appended, folded into its
    carrier, each exactly as often as it was appended: memory and guidance
    once without churn; with churn three memory entries (five memories, the
    two that dripped in, the changed one), each memory once by its handle
    except the changed one, and two knowledge entries.
    """
    body = last.body
    churn = SCENARIOS[scenario].get("churn", False)
    expected = {
        "memory_summary": 1,
        "memory": 3 if churn else 1,
        "knowledge": 2 if churn else 1,
        "citation": 1,
        "guidance": 1,
        "subagents": 1,
    }
    for kind, count in expected.items():
        assert text_occurrences(body, _srw(kind)) == count, (kind, count)
    guidance_text = harness.GUIDANCE[0]["text"]
    assert text_occurrences(body, guidance_text) == 1
    _assert_memory_summary_once(body)
    # Nothing of the legacy tail reaches an append_only request.
    assert text_occurrences(body, harness.MEMORY_TEXT) == 0
    assert text_occurrences(body, "[SUPERVISOR GUIDANCE]") == 1
    facts = harness.CHURN_FACTS if churn else harness.MEMORY_FACTS
    for index in range(1, len(facts) + 1):
        handle = f"[{memory_handle(harness.UUID(int=index))}]"
        assert text_occurrences(body, handle) == (2 if churn and index == 1 else 1)
    assert text_occurrences(body, UPDATED_ITEM_MARKER) == (2 if churn else 0)
    if churn:
        assert text_occurrences(body, harness.MEMORY_REVISED) == 1
        assert text_occurrences(body, harness.KNOWLEDGE_BODY_REVISED) == 1


def _assert_session_appended_once(last: Captured) -> None:
    """Non-vacuity of the append_only session case: the context is there.

    The last request of the second user turn holds every entry the session
    appended, folded into its carrier: the App Guide turn boundary once per
    user turn (D22), the charter, memory, knowledge and subagent status once
    each (present since turn one, so never re-sent). Nothing of the legacy
    per-call tail reaches it.
    """
    body = last.body
    expected = {
        "turn_boundary": 2,
        "charter": 1,
        "memory_summary": 1,
        "memory": 1,
        "knowledge": 1,
        "subagents": 1,
    }
    for kind, count in expected.items():
        assert text_occurrences(body, _srw(kind)) == count, (kind, count)
    assert text_occurrences(body, "<managed_product_guide_turn_boundary") == 2
    assert text_occurrences(body, harness.CHARTER["content"]) == 1
    assert text_occurrences(body, "<active_subagents>") == 1
    _assert_memory_summary_once(body)
    for _memory_type, fact in harness.MEMORY_FACTS:
        assert text_occurrences(body, fact) == 1
    # The legacy tail renders memory as one block and the charter and memory
    # as synthetic tool-call pairs; none of it reaches an append_only request.
    assert text_occurrences(body, harness.MEMORY_TEXT) == 0
    assert text_occurrences(body, "charter_inject_") == 0
    assert text_occurrences(body, "memory_inject_") == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("scenario,variant,mode,family_id", CASES)
async def test_request_starts_with_the_previous_request(
    scenario, variant, mode, family_id, tmp_path, monkeypatch
):
    _require_renderer(family_id)
    family = FAMILIES[family_id]

    control = await _violations(scenario, family, "control", tmp_path, monkeypatch)
    if variant == "control":
        if control:
            raise PrefixViolation(
                f"{scenario} / {family_id}, injections off:\n"
                + "\n".join(control.values())
            )
        return

    requests, found = await _run(scenario, family, variant, tmp_path, monkeypatch, mode)
    # A pair the control already breaks is the control case's finding.
    caused = {n: v for n, v in found.items() if n not in control}
    if caused:
        note = (
            f"\n(pairs {sorted(control)} also break without injections; see the "
            "control case)"
            if control
            else ""
        )
        raise PrefixViolation(
            f"{scenario} / {family_id}, {mode}-{variant}:\n"
            + "\n".join(caused.values())
            + note
        )
    if variant == "injected" and mode == "append_only":
        if scenario in WORKER_SCENARIOS:
            _assert_appended_once(scenario, requests[-1])
        else:
            _assert_session_appended_once(requests[-1])


#: append_only with memory retrieved a request late (WP3, D6/D8): what the
#: last request holds. The worker's first request goes out without memory
#: and request 1 takes in request 0's retrieval; churn's later results land
#: one request later too (the drip, then the changed memory; the note change
#: of the last retrieval is never taken in). The session's turn-one
#: retrieval finishes only when turn two starts, so the first request of
#: turn two (request 3) carries memory and knowledge.
LATE_EXPECTED = {
    "worker-tool-loop": {"first": 1, "memory": 1, "knowledge": 1, "updated": 0},
    "worker-memory-churn": {"first": 1, "memory": 3, "knowledge": 1, "updated": 1},
    "session-two-turns": {"first": 3, "memory": 1, "knowledge": 1, "updated": 0},
}
LATE_CASES = [
    pytest.param(scenario, family, id=f"{scenario}-append_only-late-{family}")
    for scenario in LATE_EXPECTED
    for family in FAMILIES
    if family not in SATURATED
]


@pytest.mark.asyncio
@pytest.mark.parametrize("scenario,family_id", LATE_CASES)
async def test_memory_that_arrives_a_request_late_keeps_the_prefix(
    scenario, family_id, tmp_path, monkeypatch
):
    """WP3: a retrieval that finishes after its request was sent reaches a
    later request as an appended entry; every request still starts with
    the previous one, and nothing is sent twice."""
    _require_renderer(family_id)
    family = FAMILIES[family_id]
    control = await _violations(scenario, family, "control", tmp_path, monkeypatch)

    workdir = tmp_path / "append_only-late"
    workdir.mkdir()
    requests, turn_ends = await run_scenario(
        scenario,
        family,
        sources=VARIANTS["injected"],
        workdir=workdir,
        monkeypatch=monkeypatch,
        injection_mode="append_only",
        memory_lag=1,
    )
    found = prefix_violations(family, requests, turn_ends=turn_ends)
    caused = {n: v for n, v in found.items() if n not in control}
    if caused:
        raise PrefixViolation(
            f"{scenario} / {family_id}, append_only, memory a request late:\n"
            + "\n".join(caused.values())
        )
    expected = LATE_EXPECTED[scenario]
    with_memory = [
        n for n, r in enumerate(requests) if text_occurrences(r.body, _srw("memory"))
    ]
    assert with_memory[0] == expected["first"]
    last = requests[-1].body
    assert text_occurrences(last, _srw("memory")) == expected["memory"]
    assert text_occurrences(last, _srw("knowledge")) == expected["knowledge"]
    assert text_occurrences(last, UPDATED_ITEM_MARKER) == expected["updated"]


PREFETCH_CASES = [
    pytest.param(family, id=f"session-two-turns-append_only-prefetch-{family}")
    for family in FAMILIES
    if family not in SATURATED
]


@pytest.mark.asyncio
@pytest.mark.parametrize("family_id", PREFETCH_CASES)
async def test_the_idle_time_prefetch_reaches_turn_two_and_keeps_the_prefix(
    family_id, tmp_path, monkeypatch
):
    """WP4 (D24, D32): the memory the idle-time prefetch found at the end of
    turn one goes out with the first request of turn two, as an appended
    entry; every request still starts with the previous one."""
    _require_renderer(family_id)
    family = FAMILIES[family_id]
    scenario = "session-two-turns"
    control = await _violations(scenario, family, "control", tmp_path, monkeypatch)

    workdir = tmp_path / "append_only-prefetch"
    workdir.mkdir()
    requests, turn_ends = await run_scenario(
        scenario,
        family,
        sources=VARIANTS["injected"],
        workdir=workdir,
        monkeypatch=monkeypatch,
        injection_mode="append_only",
        memory_prefetch=True,
    )
    found = prefix_violations(family, requests, turn_ends=turn_ends)
    caused = {n: v for n, v in found.items() if n not in control}
    if caused:
        raise PrefixViolation(
            f"{scenario} / {family_id}, append_only, idle-time prefetch:\n"
            + "\n".join(caused.values())
        )
    with_fact = [
        n
        for n, r in enumerate(requests)
        if text_occurrences(r.body, harness.PREFETCH_FACT)
    ]
    first_of_turn_two = max(turn_ends) + 1
    assert with_fact[0] == first_of_turn_two
    last = requests[-1].body
    assert text_occurrences(last, harness.PREFETCH_FACT) == 1
    # Turn one's memory entry and the prefetched one; the rest was present.
    assert text_occurrences(last, _srw("memory")) == 2


@pytest.mark.asyncio
async def test_append_only_breakpoints_on_real_worker_requests(tmp_path, monkeypatch):
    """§H/O4 on the requests the worker really sends to Claude via the proxy.

    Every append_only request carries folded carriers, so each one marks the
    system prompt, the newest assistant message (it reads the entry the
    previous request wrote) and the last message (it writes the next one).
    """
    family = FAMILIES["openai-chat-claude-proxy"]
    requests, found = await _run(
        "worker-tool-loop",
        family,
        "injected",
        tmp_path,
        monkeypatch,
        "append_only",
    )
    assert found == {}
    assert len(requests) == len(harness.WORKER_SCRIPT)
    for n, captured in enumerate(requests):
        messages = captured.body["messages"]
        marked = [i for i, m in enumerate(messages) if "cache_control" in m]
        assistants = [i for i, m in enumerate(messages) if m["role"] == "assistant"]
        expected = {0, len(messages) - 1}
        if assistants:
            expected.add(assistants[-1])
        assert messages[0]["role"] == "system"
        assert set(marked) == expected, (n, marked, expected)
        assert len(marked) <= 4
        # The carrier the last message is: the newest tool result, with the
        # entries of this request folded in after its own text (n >= 1).
        if n:
            assert messages[-1]["role"] == "tool"


@pytest.mark.asyncio
async def test_append_only_breakpoints_on_real_session_requests(tmp_path, monkeypatch):
    """§H/O4 on the session's two turns through the Claude proxy.

    Every append_only request carries a folded carrier (the turn's input
    holds the turn-start entries), so each one marks the system prompt, the
    newest assistant message once there is one, and the last message.
    """
    family = FAMILIES["openai-chat-claude-proxy"]
    requests, found = await _run(
        "session-two-turns",
        family,
        "injected",
        tmp_path,
        monkeypatch,
        "append_only",
    )
    assert found == {}
    assert len(requests) == len(harness.SESSION_SCRIPT)
    for n, captured in enumerate(requests):
        messages = captured.body["messages"]
        marked = [i for i, m in enumerate(messages) if "cache_control" in m]
        assistants = [i for i, m in enumerate(messages) if m["role"] == "assistant"]
        expected = {0, len(messages) - 1}
        if assistants:
            expected.add(assistants[-1])
        assert set(marked) == expected, (n, marked, expected)
        assert len(marked) <= 4


@pytest.mark.skip(
    reason=(
        "MiniMax M3(MiniMaxAI/MiniMax-M3@f0e1c1e04d40177e4673a22097036854f536e9c0) "
        "is under the MiniMax Community License, a non-commercial grant whose "
        "commercial use needs attribution plus a notice to or authorization from "
        "MiniMax; its template is not vendored into this repository. The Q3 "
        "research rendered it: all assistant turns keep their thinking, no "
        "history rewrite."
    )
)
def test_minimax_m3_template():
    """Placeholder so the family is visible in the matrix."""


class TestVendoredTemplates:
    def test_files_match_the_manifest(self):
        manifest = harness.load_manifest()["templates"]
        for key, entry in manifest.items():
            raw = (harness.TEMPLATE_DIR / entry["file"]).read_bytes()
            assert hashlib.sha256(raw).hexdigest() == entry["sha256"], key
            assert len(entry["revision"]) == 40, key
            assert entry["revision"] in entry["source_url"], key

    def test_every_template_family_has_a_vendored_template(self):
        manifest = harness.load_manifest()["templates"]
        assert {FAMILIES[f].template for f in TEMPLATE_FAMILIES} <= set(manifest)


# ---------------------------------------------------------------------------
# The checker itself (seeded regressions, so a green gate is not vacuous)
# ---------------------------------------------------------------------------


def _chat(messages, **extra) -> Captured:
    body = {"model": "m", "tools": [{"type": "function"}], "messages": messages}
    body.update(extra)
    return Captured(api="openai_chat", url="http://x/v1/chat/completions", body=body)


SYSTEM = {"role": "system", "content": "sys"}
USER = {"role": "user", "content": "task"}
REPLY = {"role": "assistant", "content": "done"}


class TestChecker:
    def test_an_appended_message_is_a_prefix_extension(self):
        assert (
            api_prefix_violation(_chat([SYSTEM, USER]), _chat([SYSTEM, USER, REPLY]))
            is None
        )

    def test_a_relocated_tail_is_reported_at_its_index(self):
        tail = {"role": "user", "content": "<active_tasks>"}
        problem = api_prefix_violation(
            _chat([SYSTEM, USER, tail]), _chat([SYSTEM, USER, REPLY, tail])
        )
        assert problem is not None and "index 2" in problem

    def test_a_volatile_byte_in_history_is_reported(self):
        edited = {"role": "user", "content": "task "}
        problem = api_prefix_violation(
            _chat([SYSTEM, USER]), _chat([SYSTEM, edited, REPLY])
        )
        assert problem is not None and "index 1" in problem

    def test_a_changed_tool_list_is_reported(self):
        problem = api_prefix_violation(
            _chat([SYSTEM, USER]), _chat([SYSTEM, USER, REPLY], tools=[])
        )
        assert problem == "non-message part changed: ['tools']"

    def test_moved_cache_markers_are_not_a_change(self):
        marked = {**USER, "cache_control": {"type": "ephemeral"}}
        assert (
            api_prefix_violation(
                _chat([SYSTEM, marked]),
                _chat(
                    [SYSTEM, USER, {**REPLY, "cache_control": {"type": "ephemeral"}}]
                ),
            )
            is None
        )

    def test_text_check_reports_the_first_differing_offset(self):
        assert text_prefix_violation("abc", "abcdef") is None
        problem = text_prefix_violation("abXc", "abcdef")
        assert problem is not None and "offset 2" in problem
