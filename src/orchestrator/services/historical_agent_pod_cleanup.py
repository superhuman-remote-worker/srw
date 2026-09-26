"""Retire historical dedicated agent Pods with immutable settlement authority."""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable, Mapping, Sequence
from typing import Any


def _json(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value


def _pod_labels(intent: Mapping[str, Any], thread_id: str) -> dict[str, str]:
    labels = {
        "srw.io/runtime-generation": str(intent["runtime_generation"]),
        "srw.io/provision-attempt": str(intent["attempt_id"]),
    }
    if intent["provisioner"] == "agent":
        labels.update(
            {
                "srw/managed-by": "agent-provisioner",
                "srw/purpose": "session",
                "srw.io/thread-id": thread_id,
            }
        )
    else:
        labels.update(
            {
                "srw/component": "persistent-agent",
                "srw/thread-id": thread_id,
            }
        )
    return labels


def _is_current_intent(
    intent: Mapping[str, Any], current_pod: Mapping[str, Any]
) -> bool:
    """Skip the captured current actor; its own stop path owns it."""

    if str(intent["attempt_id"]) != str(current_pod.get("provision_attempt") or ""):
        return False
    if not (
        str(intent["pod_uid"]) == str(current_pod.get("pod_uid") or "")
        and str(intent["pod_name"]) == str(current_pod.get("pod_name") or "")
        and str(intent["namespace"]) == str(current_pod.get("namespace") or "")
        and str(intent["runtime_generation"])
        == str(current_pod.get("runtime_generation") or "")
    ):
        raise RuntimeError("current claimant does not match its published intent")
    return True


async def _retire_exact_candidates(
    db: Any,
    *,
    thread_id: str,
    candidates: Sequence[tuple[Mapping[str, Any], str | None]],
    intents: Sequence[Mapping[str, Any]],
    pvc_name: str | None,
    assert_current: Callable[[], Awaitable[None]],
    agent_provisioner: Any,
) -> None:
    """Retire each proven candidate on its own; report what remains.

    Every candidate carries its own immutable proof, so one live or changed
    claimant never holds an exited one hostage. Authority is re-asserted per
    candidate and a change aborts at once; everything else is retried by the
    caller's durable retry after all candidates had their turn.
    """

    refusal: str | None = None
    for intent, old_agent_id in candidates:
        await assert_current()
        actors = await db.fetch(
            "SELECT id,hostname,pod_uid,thread_id,current_job_id,status FROM agents "
            "WHERE pod_uid=$1 OR id=$2::uuid",
            str(intent["pod_uid"]),
            old_agent_id,
        )
        if any(
            str(actor["id"]) != old_agent_id
            or actor["hostname"] != intent["pod_name"]
            or actor["pod_uid"] != intent["pod_uid"]
            or actor["thread_id"] is not None
            or actor["current_job_id"] is not None
            or actor["status"] != "offline"
            for actor in actors
        ):
            refusal = refusal or "historical claimant has a live or ambiguous actor"
            continue
        successors = frozenset(
            str(other["pod_uid"])
            for other in intents
            if other["pod_name"] == intent["pod_name"]
            and other["namespace"] == intent["namespace"]
            and other["attempt_id"] != intent["attempt_id"]
        )
        if not await agent_provisioner.retire_historical_claimant_pod_exact(
            pod_name=str(intent["pod_name"]),
            pod_uid=str(intent["pod_uid"]),
            namespace=str(intent["namespace"]),
            expected_labels=_pod_labels(intent, thread_id),
            pvc_name=pvc_name,
            known_successor_uids=successors,
        ):
            refusal = refusal or "historical claimant Pod retirement is retryable"
    if refusal is not None:
        raise RuntimeError(refusal)


async def retire_historical_claimant_pods(
    db: Any,
    *,
    claim: Mapping[str, str],
    current_pod: Mapping[str, Any],
    assert_current: Callable[[], Awaitable[None]],
    agent_provisioner: Any,
) -> None:
    """Remove old terminal claimant Pods before fencing their shared PVC.

    A published intent identifies a Pod, but does not prove its actor settled.
    Join it to the server-captured soft outcome or an exact recycle handoff.
    Neither a missing actor row nor a matching generation alone is authority.
    """

    await assert_current()
    intents = await db.fetch(
        "SELECT * FROM thread_agent_pod_provision_intents "
        "WHERE thread_id=$1::uuid AND workspace_claim_id=$2::uuid "
        "AND status='published' ORDER BY attempt_id",
        claim["thread_id"],
        claim["claim_id"],
    )
    outcomes = await db.fetch(
        "SELECT runtime_generation,agent_id,runtime_attach_token,retired_agent_pod "
        "FROM thread_runtime_retirement_outcomes WHERE thread_id=$1::uuid "
        "AND NOT permanent AND outcome='settled' "
        "AND retired_agent_pod->>'workspace_claim_id'=$2",
        claim["thread_id"],
        claim["claim_id"],
    )
    handoffs = await db.fetch(
        "SELECT * FROM thread_agent_pod_recycle_handoffs "
        "WHERE thread_id=$1::uuid AND workspace_claim_id=$2::uuid "
        "AND process_zero_protocol='finalized_exact_terminal_v1'",
        claim["thread_id"],
        claim["claim_id"],
    )
    # Validate the entire set before making the first historical mutation.
    candidates = []
    for intent in intents:
        attempt = str(intent["attempt_id"])
        if _is_current_intent(intent, current_pod):
            continue
        if not (
            intent["protection_protocol"] == "finalizer_v1"
            and intent["provisioner"] == claim["provisioner"]
            and intent["namespace"] == claim["namespace"]
            and intent["pod_uid"]
        ):
            raise RuntimeError("historical claimant intent is incomplete")
        matches = []
        for outcome in outcomes:
            proof = _json(outcome["retired_agent_pod"])
            if not isinstance(proof, dict):
                continue
            expected = {
                "version": 1,
                "pod_name": str(intent["pod_name"]),
                "pod_uid": str(intent["pod_uid"]),
                "namespace": claim["namespace"],
                "provisioner": claim["provisioner"],
                "provision_attempt": attempt,
                "protection_protocol": "finalizer_v1",
                "workspace_claim_id": claim["claim_id"],
                "workspace_create_attempt": claim["create_attempt"],
                "workspace_created_runtime_generation": claim[
                    "created_runtime_generation"
                ],
                "pvc_name": claim["pvc_name"],
                "pvc_uid": claim["pvc_uid"],
            }
            if (
                proof == expected
                and str(outcome["runtime_generation"])
                == str(intent["runtime_generation"])
                and outcome["agent_id"] is not None
                and outcome["runtime_attach_token"] is not None
            ):
                matches.append(str(outcome["agent_id"]))
        recycle_proof = any(
            str(handoff["predecessor_attempt_id"]) == attempt
            and handoff["predecessor_pod_uid"] == intent["pod_uid"]
            and handoff["runtime_generation"] == intent["runtime_generation"]
            and handoff["namespace"] == intent["namespace"]
            and handoff["pod_name"] == intent["pod_name"]
            for handoff in handoffs
        )
        if len(set(matches)) > 1 or not (matches or recycle_proof):
            raise RuntimeError("historical claimant lacks immutable actor settlement")
        candidates.append((intent, matches[0] if matches else None))

    await _retire_exact_candidates(
        db,
        thread_id=claim["thread_id"],
        candidates=candidates,
        intents=intents,
        pvc_name=claim["pvc_name"],
        assert_current=assert_current,
        agent_provisioner=agent_provisioner,
    )


async def retire_historical_unclaimed_agent_pods(
    db: Any,
    *,
    thread_id: str,
    current_pod: Mapping[str, Any],
    assert_current: Callable[[], Awaitable[None]],
    agent_provisioner: Any,
) -> None:
    """Retire old dedicated Pods that mounted no agent workspace claim.

    Soft settlement records such a Pod's exact identity on the append-only
    outcome (0301). An intent with no recorded relation is left untouched —
    nothing is inferred from its generation, name or an absent actor. A
    recorded relation must match the published intent exactly; a relation
    that names the Pod but disagrees with it refuses the whole set. Authority
    is asserted before each mutation; a set with nothing to retire is a no-op.
    """

    intents = await db.fetch(
        "SELECT * FROM thread_agent_pod_provision_intents "
        "WHERE thread_id=$1::uuid AND workspace_claim_id IS NULL "
        "AND status='published' ORDER BY attempt_id",
        thread_id,
    )
    outcomes = await db.fetch(
        "SELECT runtime_generation,agent_id,runtime_attach_token,retired_agent_pod "
        "FROM thread_runtime_retirement_outcomes WHERE thread_id=$1::uuid "
        "AND NOT permanent AND outcome='settled' "
        "AND retired_agent_pod IS NOT NULL "
        "AND NOT retired_agent_pod ? 'workspace_claim_id'",
        thread_id,
    )
    candidates = []
    for intent in intents:
        attempt = str(intent["attempt_id"])
        if _is_current_intent(intent, current_pod):
            continue
        related = []
        for outcome in outcomes:
            proof = _json(outcome["retired_agent_pod"])
            if isinstance(proof, dict) and (
                str(proof.get("pod_uid") or "") == str(intent["pod_uid"] or "")
                or str(proof.get("provision_attempt") or "") == attempt
            ):
                related.append((outcome, proof))
        if not related:
            continue
        if not (
            intent["protection_protocol"] == "finalizer_v1"
            and intent["provisioner"] in {"agent", "persistent"}
            and intent["namespace"]
            and intent["pod_uid"]
        ):
            raise RuntimeError("historical agent Pod intent is incomplete")
        expected = {
            "version": 1,
            "pod_name": str(intent["pod_name"]),
            "pod_uid": str(intent["pod_uid"]),
            "namespace": str(intent["namespace"]),
            "provisioner": str(intent["provisioner"]),
            "provision_attempt": attempt,
            "protection_protocol": "finalizer_v1",
        }
        matches = {
            str(outcome["agent_id"])
            for outcome, proof in related
            if proof == expected
            and str(outcome["runtime_generation"]) == str(intent["runtime_generation"])
            and outcome["agent_id"] is not None
            and outcome["runtime_attach_token"] is not None
        }
        if len(matches) != 1 or len(related) != 1:
            raise RuntimeError("historical agent Pod lacks exact actor settlement")
        candidates.append((intent, next(iter(matches))))

    if not candidates:
        return
    await _retire_exact_candidates(
        db,
        thread_id=thread_id,
        candidates=candidates,
        intents=intents,
        pvc_name=None,
        assert_current=assert_current,
        agent_provisioner=agent_provisioner,
    )
