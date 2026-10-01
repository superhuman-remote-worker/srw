"""Closing a settled provider life retains its cumulative acceptance evidence."""

import pytest

from tests.e2e.app.deterministic_provider import test_provider as fixtures

arm = fixtures.arm
chat_request = fixtures.chat_request
control = fixtures.control
inference = fixtures.inference
store = fixtures.store

pytestmark = pytest.mark.asyncio


async def test_control_close_retains_exact_calls_counters_and_readback(
    control, inference
):
    rid = "retaining-close-proof"
    await arm(control, rid)
    assert (
        await inference.post("/v1/chat/completions", json=chat_request(rid))
    ).status_code == 200
    before = (await control.get(f"/control/scenarios/{rid}")).json()
    response = await control.post(
        f"/control/scenarios/{rid}/close", json={"expected_cancelled": 0}
    )
    assert response.status_code == 200, response.text
    closed = response.json()
    assert closed["closed"] is True
    assert closed["calls"] == before["calls"]
    assert closed["counters"] == before["counters"]
    assert (await control.get(f"/control/scenarios/{rid}")).json() == closed
    assert (
        await control.post(
            f"/control/scenarios/{rid}/close", json={"expected_cancelled": 0}
        )
    ).json() == closed
    overview = (await control.get("/control/scenarios")).json()
    assert overview["runs"] == []
    assert overview["closed_runs"] == [closed]


async def test_close_records_cancelled_work_without_consuming_or_resetting_it(
    control, store
):
    rid = "retaining-close-cancelled"
    await arm(control, rid)
    decision = await store.begin_call(
        run_id=rid,
        endpoint="chat.completions",
        model="e2e-chat",
        stream=True,
        consume_required=True,
    )
    await store.finish_call(decision, "cancelled")
    before = (await control.get(f"/control/scenarios/{rid}")).json()
    response = await control.post(
        f"/control/scenarios/{rid}/close", json={"expected_cancelled": 1}
    )
    assert response.status_code == 200, response.text
    closed = response.json()
    assert closed["closed"] is True
    assert closed["consumed_required_responses"] == 0
    assert closed["remaining_required_responses"] == 1
    assert closed["unexpected_count"] == 1
    assert closed["counters"] == before["counters"]
    assert closed["calls"] == before["calls"]
    assert closed["expected_cancelled"] == 1
