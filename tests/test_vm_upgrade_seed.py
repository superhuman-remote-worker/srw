"""C2's durable VM upgrade seed marker and its internal routes."""

from __future__ import annotations

import json

import pytest

from shared.workspace_upgrade import (
    UPGRADE_ALREADY_RUNNING,
    UPGRADE_CANCELLED,
    UPGRADE_FAILED,
    UPGRADE_INTERRUPTED,
    UPGRADE_REFUSED,
    WORKSPACE_UPGRADE_FAILURE_CODES,
    vm_provision_generation,
    vm_upgrade_seed,
)

THREAD_ID = "11111111-1111-4111-8111-111111111111"
REQUEST_ID = "55555555-5555-4555-8555-555555555555"
GENERATION = "66666666-6666-4666-8666-666666666666"
SOURCE = {
    "backend": "virtual",
    "prefix": f"threads/{THREAD_ID}/",
    "binding": {"kind": "virtual", "backing_id": "rclone:" + "a" * 64},
}


def _metadata(marker, **vm):
    return {"vm": {"upgrade_seed": marker, **vm}}


def test_the_failure_codes_are_the_five_typed_reasons():
    assert WORKSPACE_UPGRADE_FAILURE_CODES == {
        UPGRADE_CANCELLED,
        UPGRADE_REFUSED,
        UPGRADE_FAILED,
        UPGRADE_INTERRUPTED,
        UPGRADE_ALREADY_RUNNING,
    }
    assert UPGRADE_CANCELLED == "upgrade_cancelled"
    assert UPGRADE_INTERRUPTED == "upgrade_interrupted"


@pytest.mark.parametrize("state", ["pending", "seeded"])
def test_a_well_formed_marker_is_read_back(state):
    marker = {
        "control_request_id": REQUEST_ID.upper(),
        "state": state,
        "source": SOURCE,
    }
    assert vm_upgrade_seed(_metadata(marker)) == {
        "control_request_id": REQUEST_ID,
        "state": state,
        "source": SOURCE,
    }


def test_json_text_metadata_and_a_none_source_are_accepted():
    marker = {"control_request_id": REQUEST_ID, "state": "pending", "source": None}
    assert vm_upgrade_seed(json.dumps(_metadata(marker)))["source"] is None


@pytest.mark.parametrize(
    "metadata",
    [
        None,
        {},
        {"vm": None},
        {"vm": {"status": "ready"}},
        _metadata(None),
        _metadata(
            {"control_request_id": "not-a-uuid", "state": "pending", "source": None}
        ),
        _metadata({"control_request_id": REQUEST_ID, "state": "done", "source": None}),
        _metadata(
            {
                "control_request_id": REQUEST_ID,
                "state": "pending",
                "source": {"backend": "sandbox"},
            }
        ),
        _metadata(
            {
                "control_request_id": REQUEST_ID,
                "state": "pending",
                "source": "threads/x/",
            }
        ),
    ],
)
def test_anything_else_is_no_marker(metadata):
    assert vm_upgrade_seed(metadata) is None


def test_the_provision_generation_is_canonical_or_absent():
    assert (
        vm_provision_generation({"vm": {"provision_generation": GENERATION.upper()}})
        == GENERATION
    )
    assert vm_provision_generation({"vm": {"provision_generation": "x"}}) is None
    assert vm_provision_generation({"vm": {}}) is None
    assert vm_provision_generation({}) is None
