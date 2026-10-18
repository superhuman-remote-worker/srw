"""Relevance-gate policy (memory overhaul Phase 3, slice 4).

P4: a memory is injected because it qualifies — and nothing is injected
when nothing qualifies. Below-threshold is not "inject the best of a bad
lot".

Two modes, because the slice-4 measurement falsified the naive design:
- ``absolute``: drop items whose channel score is below ``threshold``.
  Measured on the s20 corpus this LOSES evidence — qwen3-reranker's
  absolute scale varies by orders of magnitude per query (top evidence
  0.9991 on one question, 0.0019 on another), so any absolute cutoff
  that trims one question's filler deletes another's evidence
  (complete_rerank_gate_s20: R@5 0.80 at threshold 0.05).
- ``relative`` (the measured recommendation): drop items scoring below
  ``threshold × top-score-of-this-assemble``. Evidence/distractor
  separation is consistently strong (5–800× on the probed questions)
  even where absolute confidence is tiny, so a per-assemble relative
  floor keeps evidence while shedding filler. Note: relative gating can
  never inject *nothing* (the top item always passes its own floor) —
  the probe showed score magnitude alone cannot distinguish "answerable
  but weakly phrased" from "unanswerable with a topical near-miss", so
  the P4 nothing-qualifies ideal is not reachable from rerank scores;
  it returns with calibrated/ensemble channels.

Fail-open on absence, fail-closed only on evidence: an item the channel
never scored — scorer outage (manager containment passes items through
unscored), candidates past the reranker's ``top_k`` — passes the gate
untouched. A reranker outage
therefore degrades to the legacy full dump, never to an empty injection.
Corollary (measured, affe2881 s20): when the reranker's ``top_k`` does
not cover the candidate set, the unscored tail leaks through the gate —
size the channel scorer to full coverage when gating on it, or compose
with ``bounded`` so the leak is capped.

Scope discipline (matches the bounded policy):
- Only ``kind == "memory"`` items are gated; knowledge items render to
  their own block and pass through in position. (A KB gate needs a
  scorer that scores knowledge candidates first — the reranker scorer
  deliberately does not.)
- Order-preserving: survivors keep their scored order.
- Composes with ``bounded``: ``policies: [gate, bounded]`` drops the
  non-qualifying tail first, then caps what remains.
"""

import logging
from typing import Any, List

from agent.services.memory.registry import register_memory_plugin
from agent.services.memory.types import AssembleRequest, Scored

logger = logging.getLogger(__name__)


GATE_MODES = ("absolute", "relative")


class GatePolicy:
    """Policy protocol implementation: drop memory items scored below threshold."""

    def __init__(
        self, threshold: float, channel: str = "rerank", mode: str = "absolute"
    ) -> None:
        if threshold is None:
            raise ValueError(
                "gate policy needs memory.gate.threshold — "
                "binding it without one is config theatre"
            )
        if not channel:
            raise ValueError("memory.gate.channel must be a non-empty channel name")
        if mode not in GATE_MODES:
            raise ValueError(f"memory.gate.mode '{mode}' not in {GATE_MODES}")
        self.threshold = float(threshold)
        self.channel = channel
        self.mode = mode

    def _floor(self, items: List[Scored]) -> float:
        if self.mode == "absolute":
            return self.threshold
        top = max(
            (
                item.candidate.channel_scores[self.channel]
                for item in items
                if item.candidate.kind == "memory"
                and self.channel in item.candidate.channel_scores
            ),
            default=None,
        )
        # No scored items at all (scorer outage) — a floor of +inf would
        # be irrelevant since unscored items pass anyway, but keep it 0
        # for an honest debug log.
        return self.threshold * top if top is not None else 0.0

    async def apply(self, req: AssembleRequest, items: List[Scored]) -> List[Scored]:
        floor = self._floor(items)
        kept: List[Scored] = []
        dropped = 0
        unscored = 0
        for item in items:
            if item.candidate.kind != "memory":
                kept.append(item)
                continue
            score = item.candidate.channel_scores.get(self.channel)
            if score is None:
                unscored += 1
                kept.append(item)
                continue
            if score < floor:
                dropped += 1
                continue
            kept.append(item)
        if dropped or unscored:
            logger.debug(
                "Gate (%s %s, floor %.6f): dropped %d memory items, passed %d unscored",
                self.channel,
                self.mode,
                floor,
                dropped,
                unscored,
            )
        return kept


@register_memory_plugin(
    "policy",
    "gate",
    description="Drop memory-kind items scoring below the floor on the "
    "configured channel — memory.gate.threshold directly (absolute) or "
    "threshold x the assemble's top score (relative); unscored items "
    "pass through",
)
def _build_gate(runtime: Any) -> GatePolicy:
    cfg = getattr(runtime.memory_config, "gate", None)
    if cfg is None:
        raise ValueError("memory.gate config section missing")
    if cfg.threshold is None:
        raise ValueError(
            "gate policy needs memory.gate.threshold — "
            "binding it without one is config theatre"
        )
    return GatePolicy(
        threshold=cfg.threshold,
        channel=cfg.channel,
        mode=getattr(cfg, "mode", "absolute"),
    )
