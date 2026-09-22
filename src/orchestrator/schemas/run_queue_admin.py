"""Operator request bodies for the run-queue and completion-command verbs.

R1.B06. Declared beside the routes that accept them so the published request
schema keeps the exact class name it had in ``orchestrator.main``.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


class ClaimantGoneAttestationRequest(BaseModel):
    """An administrator's assertion that one exact claimant process is gone."""

    pod: str = Field(min_length=1, max_length=253)
    pod_uid: str = Field(min_length=1, max_length=64)
    reason: str = Field(min_length=1, max_length=2048)


class CompletionCommandForceResolveRequest(BaseModel):
    """Explicit incident disposition for an unfinished completion command."""

    expected_state: Literal["pending", "finalizing", "parked"]
    terminal_status: Literal["completed", "failed", "cancelled"]
    reason: str = Field(min_length=1, max_length=2048)
