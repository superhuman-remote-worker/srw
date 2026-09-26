"""Malformed pointers must not become cleanup or Resume authority."""

from dataclasses import asdict
from uuid import uuid4

import pytest

from shared.container_recovery import ContainerRecoveryCleanup


@pytest.mark.parametrize(
    "field,value",
    [
        ("phase", {}),
        ("phase", "unknown"),
        ("version", True),
        ("intent_generation", True),
        ("job_id", None),
    ],
)
def test_malformed_receipt_is_refused(field, value):
    receipt = asdict(ContainerRecoveryCleanup(*(str(uuid4()) for _ in range(5)), 1))
    receipt[field] = value
    assert ContainerRecoveryCleanup.parse(receipt) is None


def test_malformed_current_projection_has_no_cleanup_authority():
    receipt = ContainerRecoveryCleanup(*(str(uuid4()) for _ in range(5)), 1)
    assert not receipt.matches_owner(
        "paused",
        {
            "workspace_container": [1],
            "_operator_pause_hold": {
                "hold_id": receipt.hold_id,
                "source": "workspace_recovery_unavailable",
            },
        },
    )
