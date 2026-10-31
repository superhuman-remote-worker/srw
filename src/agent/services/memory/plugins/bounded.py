"""Bounded-injection policy (memory overhaul Phase 3, slice 3).

Caps how much of the memory channel actually gets injected, in the
post-scorer order. Motivated by the Phase-2/3 eval findings on the seam
arm: ~95% of a multi-session project corpus was TTL-pinned (cross-session
memories froze once their job scope stopped ticking; the tier is retired
since), so the two-tier read path degenerated to dump-the-store (~236
items / ~9k tokens per turn) and the reader had to find the answer
mid-dump. With a reranker
scorer placing the evidence at first-hit rank ~1.35, a small top-K is
strictly better reading material.

Placement matters: ``memory.budget_tokens`` trims inside the retriever
— in legacy hybrid order, BEFORE scorers run — so a budget cut there
drops the evidence a reranker would have surfaced. This policy is the
post-scorer cut.

Scope discipline:
- Only ``kind == "memory"`` items are capped; knowledge items render to
  their own block and pass through untouched, in position — unless
  ``include_knowledge`` is set (B5): then knowledge items count against
  the same ``max_tokens``, making it the one token budget across the
  memory + KB blocks (the legacy KB block is uncapped on every call).
  ``max_items`` stays a memory-count cap either way — a combined item
  count would starve the KB block behind hundreds of memory candidates.
- Order-preserving: items keep their scored order; the policy only
  drops the overflow.
- Token cap uses the candidate's stored token_count with a chars/4
  estimate as fallback (knowledge candidates deliberately carry 0).
"""

import logging
from typing import Any, List, Optional

from agent.services.memory.registry import register_memory_plugin
from agent.services.memory.types import AssembleRequest, Scored

logger = logging.getLogger(__name__)


def _token_count(item: Scored) -> int:
    tokens = int(getattr(item.candidate, "token_count", 0) or 0)
    if tokens > 0:
        return tokens
    return len(item.candidate.text or "") // 4


class BoundedPolicy:
    """Policy protocol implementation: cap memory items post-scoring."""

    def __init__(
        self,
        max_items: Optional[int] = None,
        max_tokens: Optional[int] = None,
        include_knowledge: bool = False,
    ) -> None:
        if max_items is None and max_tokens is None:
            raise ValueError(
                "bounded policy needs memory.bounded.max_items and/or "
                "max_tokens — binding it with neither is config theatre"
            )
        if max_items is not None and max_items < 1:
            raise ValueError(f"memory.bounded.max_items must be >= 1, got {max_items}")
        if max_tokens is not None and max_tokens < 1:
            raise ValueError(
                f"memory.bounded.max_tokens must be >= 1, got {max_tokens}"
            )
        if include_knowledge and max_tokens is None:
            raise ValueError(
                "memory.bounded.include_knowledge needs max_tokens — "
                "max_items never caps knowledge, so it would be config theatre"
            )
        self.max_items = max_items
        self.max_tokens = max_tokens
        self.include_knowledge = include_knowledge

    async def apply(self, req: AssembleRequest, items: List[Scored]) -> List[Scored]:
        kept: List[Scored] = []
        memory_items = 0
        budget_tokens = 0
        dropped = 0
        for item in items:
            kind = item.candidate.kind
            if kind != "memory" and not (
                self.include_knowledge and kind == "knowledge"
            ):
                kept.append(item)
                continue
            tokens = _token_count(item)
            over_items = (
                kind == "memory"
                and self.max_items is not None
                and memory_items >= self.max_items
            )
            over_tokens = (
                self.max_tokens is not None and budget_tokens + tokens > self.max_tokens
            )
            if over_items or over_tokens:
                dropped += 1
                continue
            kept.append(item)
            if kind == "memory":
                memory_items += 1
            budget_tokens += tokens
        if dropped:
            logger.debug(
                "Bounded injection: kept %d memory items (%d budget tokens), "
                "dropped %d",
                memory_items,
                budget_tokens,
                dropped,
            )
        return kept


@register_memory_plugin(
    "policy",
    "bounded",
    description="Cap memory-kind items of the assembled payload after "
    "scorers run (max_items and/or max_tokens; order-preserving)",
)
def _build_bounded(runtime: Any) -> BoundedPolicy:
    cfg = getattr(runtime.memory_config, "bounded", None)
    if cfg is None:
        raise ValueError("memory.bounded config section missing")
    return BoundedPolicy(
        max_items=cfg.max_items,
        max_tokens=cfg.max_tokens,
        include_knowledge=getattr(cfg, "include_knowledge", False),
    )
