"""Body for ``POST /api/jobs/{job_id}/loop-plan``.

Moved verbatim from ``orchestrator.main`` (R1.B07 lane L, census group
``J_project_loops``).
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel


# Design: knowledge-base/knowledge/features/loop_campaign_scheduling.md.
class LoopPlanRequest(BaseModel):
    """Body for ``POST /api/jobs/{job_id}/loop-plan`` — a Critic-filed campaign
    plan. Validated structurally by ``validate_loop_plan``; kept as a free dict
    here so the agent gets ONE consolidated, actionable error message from the
    domain validator instead of a pydantic field soup."""

    plan: dict[str, Any]


__all__ = ["LoopPlanRequest"]
