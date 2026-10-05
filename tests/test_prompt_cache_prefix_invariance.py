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
- Markers. Cases that fail today are ``xfail(strict=True)`` with their cause:
  ``injection: ...`` (fixed by WP2) or ``intrinsic: ...``. Strict is the
  point: when a work package fixes a case it XPASSes, which fails the run and
  forces removing the marker. Only a ``PrefixViolation`` counts as the expected
  failure; any other error in the harness fails the test.
"""

import hashlib

import pytest

from tests import _prompt_cache_prefix_harness as harness
from tests._prompt_cache_prefix_harness import (
    FAMILIES,
    SCENARIOS,
    VARIANTS,
    Captured,
    api_prefix_violation,
    prefix_violations,
    run_scenario,
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


def _case(scenario: str, variant: str, family: str):
    marks = []
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
    elif variant == "injected":
        marks.append(
            pytest.mark.xfail(
                strict=True,
                raises=PrefixViolation,
                reason=INJECTION_CAUSES[scenario],
            )
        )
    # ``todos-only`` carries no marker: WP1's gate, it must pass.
    return pytest.param(
        scenario, variant, family, id=f"{scenario}-{variant}-{family}", marks=marks
    )


def _variants(scenario: str) -> tuple:
    if scenario in WORKER_SCENARIOS:
        return ("injected", "todos-only", "control")
    return ("injected", "control")  # sessions never carried a todo list


CASES = [
    _case(scenario, variant, family)
    for scenario in SCENARIOS
    for variant in _variants(scenario)
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


async def _violations(scenario, family, variant, tmp_path, monkeypatch):
    workdir = tmp_path / variant
    workdir.mkdir()
    requests, turn_ends = await run_scenario(
        scenario,
        family,
        sources=VARIANTS[variant],
        workdir=workdir,
        monkeypatch=monkeypatch,
    )
    return prefix_violations(family, requests, turn_ends=turn_ends)


@pytest.mark.asyncio
@pytest.mark.parametrize("scenario,variant,family_id", CASES)
async def test_request_starts_with_the_previous_request(
    scenario, variant, family_id, tmp_path, monkeypatch
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

    found = await _violations(scenario, family, variant, tmp_path, monkeypatch)
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
            f"{scenario} / {family_id}, {variant}:\n"
            + "\n".join(caused.values())
            + note
        )


@pytest.mark.skip(
    reason=(
        "MiniMax M3 (MiniMaxAI/MiniMax-M3@f0e1c1e04d40177e4673a22097036854f536e9c0) "
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
