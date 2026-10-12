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


@pytest.mark.parametrize(
    "fault",
    [
        "unused-required",
        "pending",
        "cancelled-auxiliary",
        "unattributed-cancel",
        "noncancel-error",
        "counter-mismatch",
        "sequence-gap",
    ],
)
async def test_close_refuses_unsettled_or_ambiguous_accounting(control, store, fault):
    rid = "close-refusal-" + fault
    await arm(control, rid)
    expected = 0
    if fault != "unused-required":
        decision = await store.begin_call(
            run_id=rid,
            endpoint="chat.completions",
            model="e2e-chat",
            stream=True,
            consume_required=fault != "cancelled-auxiliary",
        )
        if fault != "pending":
            outcome = (
                "cancelled"
                if fault in {"cancelled-auxiliary", "unattributed-cancel"}
                else "error"
                if fault == "noncancel-error"
                else "success"
            )
            await store.finish_call(decision, outcome)
            if fault in {"cancelled-auxiliary", "noncancel-error"}:
                expected = 1
            if fault == "counter-mismatch":
                store._runs[rid].counters.clear()
            elif fault == "sequence-gap":
                store._runs[rid].calls[0]["sequence"] += 1
    before = (await control.get(f"/control/scenarios/{rid}")).json()
    response = await control.post(
        f"/control/scenarios/{rid}/close", json={"expected_cancelled": expected}
    )
    assert response.status_code == 409
    assert (await control.get(f"/control/scenarios/{rid}")).json() == before
    overview = (await control.get("/control/scenarios")).json()
    assert overview["runs"] == [before]
    assert overview["closed_runs"] == []


@pytest.mark.parametrize("operation", ["arm", "reset", "advance", "changed-close"])
async def test_closed_life_cannot_be_reused_erased_or_rebudgeted(
    control, inference, operation
):
    rid = "closed-life-" + operation
    await arm(control, rid)
    await inference.post("/v1/chat/completions", json=chat_request(rid))
    closed = (await control.post(f"/control/scenarios/{rid}/close", json={})).json()
    if operation == "reset":
        response = await control.delete(f"/control/scenarios/{rid}")
    elif operation == "changed-close":
        response = await control.post(
            f"/control/scenarios/{rid}/close", json={"expected_cancelled": 1}
        )
    else:
        response = await control.post(
            f"/control/scenarios/{rid}/{operation}",
            json={"scenario": "reply", "required_responses": 1},
        )
    assert response.status_code in {404, 409}
    assert (await control.get(f"/control/scenarios/{rid}")).json() == closed
    assert (await control.get("/control/scenarios")).json()["runs"] == []


async def test_late_closed_life_request_is_globally_accounted_without_changing_archive(
    control, inference
):
    rid = "closed-life-late-request"
    await arm(control, rid)
    await inference.post("/v1/chat/completions", json=chat_request(rid))
    closed = (await control.post(f"/control/scenarios/{rid}/close", json={})).json()
    response = await inference.post("/v1/chat/completions", json=chat_request(rid))
    assert response.status_code == 409
    assert (await control.get(f"/control/scenarios/{rid}")).json() == closed
    overview = (await control.get("/control/scenarios")).json()
    assert overview["unscoped_unexpected_calls"] == 1
    assert overview["unscoped_calls"][0]["correlation_run_ids"] == [rid]
    assert overview["closed_runs"] == [closed]


async def test_retaining_close_preserves_prior_global_and_probe_accounting(
    control, inference
):
    rid = "close-preserves-global"
    await arm(control, rid)
    await inference.post("/v1/chat/completions", json=chat_request(rid))
    await inference.post("/v1/chat/completions", json=chat_request("unarmed-life"))
    await control.post(
        "/control/probe-windows/close-probes/arm", json={"expected_probes": 1}
    )
    before = (await control.get("/control/scenarios")).json()
    assert (
        await control.post(f"/control/scenarios/{rid}/close", json={})
    ).status_code == 200
    after = (await control.get("/control/scenarios")).json()
    for key in (
        "unscoped_unexpected_calls",
        "unscoped_calls",
        "unscoped_calls_truncated",
        "probe_windows",
        "active_probe_window",
    ):
        assert after[key] == before[key]


async def test_archive_copies_cannot_mutate_retained_counters_or_calls(
    control, inference, store
):
    from tests.e2e.app.deterministic_provider.provider import CloseScenarioRequest

    rid = "close-copy-isolation"
    await arm(control, rid)
    await inference.post("/v1/chat/completions", json=chat_request(rid))
    closed = (await control.post(f"/control/scenarios/{rid}/close", json={})).json()
    exposed = await store.close(rid, CloseScenarioRequest())
    exposed["counters"][0]["count"] = 0
    exposed["calls"][0]["outcome"] = "forgotten"
    observed = await store.state(rid)
    observed["calls"].clear()
    overview = await store.overview()
    overview["closed_runs"][0]["calls"].clear()
    assert (await control.get(f"/control/scenarios/{rid}")).json() == closed


async def test_close_control_authority_and_body_contract_are_preserved(
    control, inference
):
    rid = "close-control-authority"
    await arm(control, rid)
    await inference.post("/v1/chat/completions", json=chat_request(rid))
    assert (
        await control.post(
            f"/control/scenarios/{rid}/close",
            json={},
            headers={"Authorization": "Bearer wrong-control-proof"},
        )
    ).status_code == 401
    assert (
        await control.post(
            f"/control/scenarios/{rid}/close", json={"erase_counters": True}
        )
    ).status_code == 422
    assert (
        await control.post(
            f"/control/scenarios/{rid}/close", json={"expected_cancelled": -1}
        )
    ).status_code == 422
