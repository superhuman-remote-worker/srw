"""Lifecycle fixtures keep one correlation and cumulative call accounting."""

import json

import pytest

from tests.e2e.app.deterministic_provider.test_provider import (
    arm,
    chat_request,
    sse_payloads,
)
from tests.e2e.app.deterministic_provider import test_provider as fixtures

control = fixtures.control
inference = fixtures.inference
store = fixtures.store

pytestmark = pytest.mark.asyncio


async def test_advance_keeps_every_call_and_counter(control, inference):
    rid = "lifecycle-phases"
    await arm(control, rid)
    assert (
        await inference.post("/v1/chat/completions", json=chat_request(rid))
    ).status_code == 200
    before = (await control.get(f"/control/scenarios/{rid}")).json()
    advanced = await control.post(
        f"/control/scenarios/{rid}/advance",
        json={
            "scenario": "slow-stream",
            "required_responses": 1,
            "chunk_delay_ms": 0,
        },
    )
    assert advanced.status_code == 200, advanced.text
    current = advanced.json()
    assert current["calls"] == before["calls"]
    assert current["counters"] == before["counters"]
    assert current["consumed_required_responses"] == 1
    assert current["required_responses"] == 2
    assert (
        await inference.post(
            "/v1/chat/completions", json=chat_request(rid, stream=True)
        )
    ).status_code == 200
    final = (await control.get(f"/control/scenarios/{rid}")).json()
    assert final["consumed_required_responses"] == 2
    assert [c["sequence"] for c in final["calls"]] == [1, 2]
    assert final["unexpected_count"] == 0


async def test_advance_refuses_unfinished_and_unaccounted_work(control, store):
    rid = "unfinished-phase"
    await arm(control, rid)
    body = {"scenario": "reply", "required_responses": 1}
    initial = (await control.get(f"/control/scenarios/{rid}")).json()
    assert (
        await control.post(f"/control/scenarios/{rid}/advance", json=body)
    ).status_code == 409
    assert (await control.get(f"/control/scenarios/{rid}")).json() == initial
    decision = await store.begin_call(
        run_id=rid,
        endpoint="chat.completions",
        model="e2e-chat",
        stream=True,
        consume_required=True,
    )
    assert (
        await control.post(f"/control/scenarios/{rid}/advance", json=body)
    ).status_code == 409
    await store.finish_call(decision, "cancelled")
    cancelled = (await control.get(f"/control/scenarios/{rid}")).json()
    assert (
        await control.post(f"/control/scenarios/{rid}/advance", json=body)
    ).status_code == 409
    assert (await control.get(f"/control/scenarios/{rid}")).json() == cancelled
    allowed = await control.post(
        f"/control/scenarios/{rid}/advance", json={**body, "expected_cancelled": 1}
    )
    assert allowed.status_code == 200
    assert allowed.json()["calls"] == cancelled["calls"]
    assert allowed.json()["unexpected_count"] == 1


@pytest.mark.parametrize("stream", [False, True])
async def test_delegation_recovery_fixture_emits_exact_two_calls_then_a_final(
    control, inference, stream
):
    rid = "delegation-recovery"
    armed = await control.post(
        f"/control/scenarios/{rid}/arm",
        json={
            "scenario": "delegation-batch",
            "required_responses": 1,
        },
    )
    assert armed.status_code == 201, armed.text
    tools = [
        {"type": "function", "function": {"name": "delegate_agent", "parameters": {}}}
    ]
    request = chat_request(rid, stream=stream, extra={"tools": tools})
    response = await inference.post("/v1/chat/completions", json=request)
    assert response.status_code == 200, response.text
    if stream:
        calls = [
            p["choices"][0]["delta"]["tool_calls"]
            for p in sse_payloads(response)
            if isinstance(p, dict)
            and p.get("choices")
            and "tool_calls" in p["choices"][0]["delta"]
        ][0]
    else:
        calls = response.json()["choices"][0]["message"]["tool_calls"]
    assert len(calls) == 2 and len({c["id"] for c in calls}) == 2
    assert all(c["function"]["name"] == "delegate_agent" for c in calls)
    assert all(
        json.loads(c["function"]["arguments"])["subagent_type"] == "probe"
        for c in calls
    )
    assert all(
        f"E2E-{rid}" in json.loads(c["function"]["arguments"])["prompt"] for c in calls
    )
    request["messages"].extend(
        [{"role": "assistant", "content": None, "tool_calls": calls}]
        + [
            {
                "role": "tool",
                "tool_call_id": c["id"],
                "content": "[delegate_agent: INTERRUPTED - no final report]",
            }
            for c in calls
        ]
    )
    final = await inference.post("/v1/chat/completions", json=request)
    assert final.status_code == 200, final.text
    state = (await control.get(f"/control/scenarios/{rid}")).json()
    assert state["consumed_required_responses"] == 1
    assert state["unexpected_count"] == 0


async def test_delegation_child_wait_is_correlated_and_does_not_consume_a_final(
    control, inference
):
    rid = "delegation-child"
    response = await control.post(
        f"/control/scenarios/{rid}/arm",
        json={
            "scenario": "delegation-batch",
            "required_responses": 1,
        },
    )
    assert response.status_code == 201
    payload = chat_request(
        rid,
        extra={
            "tools": [
                {
                    "type": "function",
                    "function": {"name": "shell_execute", "parameters": {}},
                }
            ]
        },
    )
    response = await inference.post("/v1/chat/completions", json=payload)
    call = response.json()["choices"][0]["message"]["tool_calls"][0]
    assert call["function"]["name"] == "shell_execute"
    args = json.loads(call["function"]["arguments"])
    assert args["command"] == "sleep 300" and args["timeout"] == 390
    state = (await control.get(f"/control/scenarios/{rid}")).json()
    assert state["consumed_required_responses"] == 0
    assert state["pending_calls"] == 0


async def test_delegation_parent_initializes_its_shell_before_fanout(
    control, inference
):
    rid = "delegation-shell-proof"
    assert (
        await control.post(
            f"/control/scenarios/{rid}/arm",
            json={"scenario": "delegation-batch", "required_responses": 1},
        )
    ).status_code == 201
    tools = [
        {"type": "function", "function": {"name": name, "parameters": {}}}
        for name in ("shell_execute", "delegate_agent")
    ]
    request = chat_request(rid, extra={"tools": tools})
    first = await inference.post("/v1/chat/completions", json=request)
    calls = first.json()["choices"][0]["message"]["tool_calls"]
    assert len(calls) == 1 and calls[0]["function"]["name"] == "shell_execute"
    request["messages"].extend(
        [
            {"role": "assistant", "content": None, "tool_calls": calls},
            {
                "role": "tool",
                "tool_call_id": calls[0]["id"],
                "content": "R33C_SHELL_READY",
            },
        ]
    )
    next_response = await inference.post("/v1/chat/completions", json=request)
    calls = next_response.json()["choices"][0]["message"]["tool_calls"]
    assert len(calls) == 2 and all(
        c["function"]["name"] == "delegate_agent" for c in calls
    )
    state = (await control.get(f"/control/scenarios/{rid}")).json()
    assert state["consumed_required_responses"] == 0 and state["unexpected_count"] == 0
