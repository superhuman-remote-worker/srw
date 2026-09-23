"""The A1 stage accepts only the exact owner public Pause hold."""

from datetime import datetime, timezone
from uuid import uuid4

import pytest


def test_owned_pause_receipt_binds_source_owner_and_exact_id():
    from orchestrator.operator_cli.vm_retained_resume_pause import (
        OwnerPauseHoldError, owned_pause_hold_id,
    )

    owner, hold_id = (str(uuid4()) for _ in range(2))
    receipt = {
        "version": 1, "source": "public_pause", "paused_by": owner,
        "hold_id": hold_id, "paused_at": datetime.now(timezone.utc).isoformat(),
    }
    context = {"_operator_pause_hold": receipt}
    assert owned_pause_hold_id(context, owner_id=owner) == hold_id
    assert owned_pause_hold_id(
        context, owner_id=owner, expected_hold_id=hold_id,
    ) == hold_id
    for changed in (
        {**receipt, "source": "internal_pause"},
        {**receipt, "paused_by": str(uuid4())},
        {**receipt, "version": 2},
        {**receipt, "hold_id": str(uuid4())},
        {**receipt, "paused_at": "yesterday"},
    ):
        with pytest.raises(OwnerPauseHoldError):
            owned_pause_hold_id(
                {"_operator_pause_hold": changed}, owner_id=owner,
                expected_hold_id=hold_id,
            )
    assert owned_pause_hold_id({}, owner_id=owner) is None
    with pytest.raises(OwnerPauseHoldError):
        owned_pause_hold_id({}, owner_id=owner, expected_hold_id=hold_id)
