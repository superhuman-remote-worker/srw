"""Only an unchanged, proven gate outcome preserves completion human intent."""

from unittest.mock import AsyncMock

import pytest

from orchestrator.services.deliverable_gate import (
    run_deliverable_gate,
    DELIVERABLE_GATE_BOUNCE_CAP,
)
from tests.test_deliverable_gate import make_job, make_db, make_gitea, completion_result


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case,expected",
    [
        ("not_applicable", True),
        ("verified", True),
        ("unreadable", False),
        ("unverified", False),
        ("bounce", False),
        ("capped", False),
    ],
)
async def test_gate_returns_typed_human_wait_continuity(case, expected):
    manifest = None if case == "not_applicable" else ["answer.txt"]
    if case == "unverified":
        manifest = ["kb:answer"]
    context = (
        {"deliverable_gate": {"bounces": DELIVERABLE_GATE_BOUNCE_CAP}}
        if case == "capped"
        else {}
    )
    job = make_job(manifest=manifest, context_extra=context)
    gitea = make_gitea(
        None if case == "unreadable" else ["answer.txt"] if case == "verified" else []
    )
    result = await run_deliverable_gate(
        job,
        completion_result(False),
        "pending_review",
        db=make_db(),
        gitea=gitea,
        queue_resume=AsyncMock(),
    )
    assert result.preserves_human_wait is expected
    # The public three-item legacy unpacking contract is unchanged.
    assert tuple(result) == (result.status, result.actions, result.bounced)
