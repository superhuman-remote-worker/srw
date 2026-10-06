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
    nonrestartable_attempts: frozenset[str] = frozenset(),
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
            **(
                {"require_nonrestartable": True}
                if str(intent["attempt_id"]) in nonrestartable_attempts
                else {}
            ),
        ):
            refusal = refusal or "historical claimant Pod retirement is retryable"
    if refusal is not None:
        raise RuntimeError(refusal)


def _exact_pre_setup_abort(
    intent: Mapping[str, Any], outcomes: Sequence[Mapping[str, Any]]
) -> str | None:
    """An immutable abort releases only its captured life, never remote writers."""

    related = [
        outcome
        for outcome in outcomes
        if str(outcome["runtime_generation"]) == str(intent["runtime_generation"])
        or str(outcome["agent_pod_uid"] or "") == str(intent["pod_uid"] or "")
    ]
    if not related:
        return None
    exact = [
        outcome
        for outcome in related
        if outcome["thread_id"] == intent["thread_id"]
        and outcome["runtime_generation"] == intent["runtime_generation"]
        and outcome["agent_pod_uid"] == intent["pod_uid"]
        and outcome["runtime_attach_token"] is not None
        and outcome["agent_id"] is not None
        and outcome["successor_generation"] != outcome["runtime_generation"]
        and outcome["release_kind"] == "process_zero"
        and outcome["quiescence_protocol"] == "agent_attach_not_started_v1"
        and intent["status"] == "published"
        and intent["workspace_claim_id"] is None
        and intent["protection_protocol"] == "finalizer_v1"
        and intent["provisioner"] in {"agent", "persistent"}
        and intent["namespace"]
        and intent["pod_uid"]
    ]
    if len(related) != 1 or len(exact) != 1:
        raise RuntimeError("aborted claimant lacks one exact pre-setup settlement")
    return str(exact[0]["agent_id"])


def _expected_soft_pod_proof(
    intent: Mapping[str, Any], claim: Mapping[str, Any] | None = None
) -> dict[str, Any]:
    """Build the exact server-captured proof for one published intent."""

    proof = {
        "version": 1,
        "pod_name": str(intent["pod_name"]),
        "pod_uid": str(intent["pod_uid"]),
        "namespace": str(
            claim["namespace"] if claim is not None else intent["namespace"]
        ),
        "provisioner": str(
            claim["provisioner"] if claim is not None else intent["provisioner"]
        ),
        "provision_attempt": str(intent["attempt_id"]),
        "protection_protocol": "finalizer_v1",
    }
    if claim is not None:
        proof.update(
            {
                "workspace_claim_id": str(claim["claim_id"]),
                "workspace_create_attempt": str(claim["create_attempt"]),
                "workspace_created_runtime_generation": str(
                    claim["created_runtime_generation"]
                ),
                "pvc_name": str(claim["pvc_name"]),
                "pvc_uid": str(claim["pvc_uid"]),
            }
        )
    return proof


def _claimant_soft_settlement(
    intent: Mapping[str, Any],
    claim: Mapping[str, Any],
    outcomes: Sequence[Mapping[str, Any]],
    *,
    strict: bool = False,
) -> str | None:
    """Match a claim-bearing Pod to an append-only soft outcome."""

    attempt = str(intent["attempt_id"])
    related = []
    for outcome in outcomes:
        proof = _json(outcome["retired_agent_pod"])
        if str(outcome["runtime_generation"]) == str(intent["runtime_generation"]) or (
            isinstance(proof, dict)
            and (
                str(proof.get("pod_uid") or "") == str(intent["pod_uid"] or "")
                or str(proof.get("provision_attempt") or "") == attempt
            )
        ):
            related.append((outcome, proof))
    if not related:
        return None
    expected = _expected_soft_pod_proof(intent, claim)
    matches = {
        str(outcome["agent_id"])
        for outcome, proof in related
        if proof == expected
        and str(outcome["runtime_generation"]) == str(intent["runtime_generation"])
        and outcome["agent_id"] is not None
        and outcome["runtime_attach_token"] is not None
    }
    if len(matches) > 1 or (strict and (len(related) != 1 or len(matches) != 1)):
        raise RuntimeError("historical claimant lacks exact actor settlement")
    return next(iter(matches)) if matches else None


def _claimless_soft_settlement(
    intent: Mapping[str, Any], outcomes: Sequence[Mapping[str, Any]]
) -> str | None:
    """Require one exact settled outcome for a claim-less dedicated Pod."""

    attempt = str(intent["attempt_id"])
    related = []
    for outcome in outcomes:
        proof = _json(outcome["retired_agent_pod"])
        if str(outcome["runtime_generation"]) == str(intent["runtime_generation"]) or (
            isinstance(proof, dict)
            and (
                str(proof.get("pod_uid") or "") == str(intent["pod_uid"] or "")
                or str(proof.get("provision_attempt") or "") == attempt
            )
        ):
            related.append((outcome, proof))
    if not related:
        return None
    expected = _expected_soft_pod_proof(intent)
    if (
        len(related) != 1
        or related[0][1] != expected
        or str(related[0][0]["runtime_generation"]) != str(intent["runtime_generation"])
        or related[0][0]["agent_id"] is None
        or related[0][0]["runtime_attach_token"] is None
    ):
        raise RuntimeError("historical agent Pod lacks exact actor settlement")
    return str(related[0][0]["agent_id"])


async def retire_soft_ended_agent_pod(
    db: Any, *, pod_name: str, pod_uid: str, namespace: str, agent_provisioner: Any
) -> bool:
    """Retry one settled dedicated Pod's terminal cleanup from the Pod reaper."""

    hints = await db.fetch(
        "SELECT * FROM thread_agent_pod_provision_intents WHERE pod_name=$1 "
        "AND pod_uid=$2 AND namespace=$3 AND status='published' LIMIT 2",
        pod_name,
        pod_uid,
        namespace,
    )
    if len(hints) != 1:
        return False
    hint = hints[0]
    thread_id = str(hint["thread_id"])
    async with db.try_thread_advisory_lock(thread_id) as acquired:
        if not acquired:
            return False
        try:

            async def assert_settled_life() -> tuple[str, Mapping[str, Any] | None]:
                fresh = await db.fetchrow(
                    "SELECT * FROM thread_agent_pod_provision_intents "
                    "WHERE attempt_id=$1::uuid",
                    hint["attempt_id"],
                )
                claim = (
                    await db.fetchrow(
                        "SELECT * FROM thread_agent_workspace_claims "
                        "WHERE claim_id=$1::uuid",
                        hint["workspace_claim_id"],
                    )
                    if hint["workspace_claim_id"] is not None
                    else None
                )
                if (
                    fresh is None
                    or dict(fresh) != dict(hint)
                    or hint["provisioner"] != "agent"
                    or hint["protection_protocol"] != "finalizer_v1"
                    or (
                        hint["workspace_claim_id"] is not None
                        and (
                            claim is None
                            or claim["thread_id"] != hint["thread_id"]
                            or claim["claim_id"] != hint["workspace_claim_id"]
                            or claim["namespace"] != hint["namespace"]
                            or claim["provisioner"] != hint["provisioner"]
                            or claim["protection_protocol"] != "finalizer_v1"
                            or claim["status"] not in {"ready", "fenced", "reclaimed"}
                            or not claim["pvc_uid"]
                        )
                    )
                ):
                    raise RuntimeError("soft claimant claim or intent changed")
                outcomes = await db.fetch(
                    "SELECT runtime_generation,agent_id,runtime_attach_token,"
                    "retired_agent_pod FROM thread_runtime_retirement_outcomes "
                    "WHERE thread_id=$1::uuid AND NOT permanent AND outcome='settled' "
                    "AND (runtime_generation=$2::uuid "
                    "OR retired_agent_pod->>'pod_uid'=$3 "
                    "OR retired_agent_pod->>'provision_attempt'=$4)",
                    thread_id,
                    hint["runtime_generation"],
                    pod_uid,
                    str(hint["attempt_id"]),
                )
                old_agent_id = (
                    _claimant_soft_settlement(hint, claim, outcomes, strict=True)
                    if claim is not None
                    else _claimless_soft_settlement(hint, outcomes)
                )
                if old_agent_id is None:
                    raise RuntimeError("soft claimant has no settled outcome")
                current = await db.get_thread(thread_id)
                if current:
                    metadata = _json(current.get("metadata")) or {}
                    current_pod = metadata.get("agent_pod") or {}
                    if (
                        str(current.get("agent_id") or "") == old_agent_id
                        or str(current.get("control_admission_agent_id") or "")
                        == old_agent_id
                        or str(current.get("runtime_attach_token") or "")
                        == str(outcomes[0]["runtime_attach_token"])
                        or str(current_pod.get("pod_uid") or "") == pod_uid
                        or str(current_pod.get("provision_attempt") or "")
                        == str(hint["attempt_id"])
                        or (
                            str(current.get("runtime_generation"))
                            == str(hint["runtime_generation"])
                            and (
                                current.get("status") != "ended"
                                or current.get("agent_id") is not None
                                or current.get("runtime_attach_token") is not None
                                or current_pod
                            )
                        )
                    ):
                        raise RuntimeError("soft claimant is current or rebound")
                return old_agent_id, claim

            old_agent_id, claim = await assert_settled_life()
            intents = await db.fetch(
                "SELECT * FROM thread_agent_pod_provision_intents "
                "WHERE thread_id=$1::uuid AND status='published' ORDER BY attempt_id",
                thread_id,
            )

            async def assert_current() -> None:
                agent_id, current_claim = await assert_settled_life()
                if agent_id != old_agent_id or current_claim != claim:
                    raise RuntimeError("soft claimant authority changed")

            await _retire_exact_candidates(
                db,
                thread_id=thread_id,
                candidates=[(hint, old_agent_id)],
                intents=intents,
                pvc_name=str(claim["pvc_name"]) if claim is not None else None,
                assert_current=assert_current,
                agent_provisioner=agent_provisioner,
                nonrestartable_attempts=(
                    frozenset({str(hint["attempt_id"])})
                    if claim is None
                    else frozenset()
                ),
            )
        except RuntimeError:
            return False
    return True


async def retire_aborted_unclaimed_agent_pod(
    db: Any, *, pod_name: str, pod_uid: str, namespace: str, agent_provisioner: Any
) -> bool:
    """Retry exact terminal abort cleanup in the existing Pod reaper.

    The published intent and append-only abort survive thread deletion. They
    remain the durable obligation; no current generation is adopted, and no
    synthetic End or process-zero receipt is written. A missing Pod is already
    settled; an unknown replacement is refused and no replacement is touched.
    """

    hints = await db.fetch(
        "SELECT * FROM thread_agent_pod_provision_intents WHERE pod_name=$1 "
        "AND pod_uid=$2 AND namespace=$3 AND status='published' "
        "AND workspace_claim_id IS NULL LIMIT 2",
        pod_name,
        pod_uid,
        namespace,
    )
    if len(hints) != 1:
        return False
    hint = hints[0]
    thread_id = str(hint["thread_id"])
    async with db.try_thread_advisory_lock(thread_id) as acquired:
        if not acquired:
            return False
        intents = await db.fetch(
            "SELECT * FROM thread_agent_pod_provision_intents WHERE thread_id=$1::uuid "
            "AND status='published' ORDER BY attempt_id",
            thread_id,
        )
        outcomes = await db.fetch(
            "SELECT * FROM thread_runtime_attach_abort_outcomes WHERE thread_id=$1::uuid",
            thread_id,
        )
        try:
            old_agent_id = _exact_pre_setup_abort(hint, outcomes)
            if old_agent_id is None:
                return False

            async def assert_released_life() -> None:
                current = await db.get_thread(thread_id)
                if current:
                    metadata = _json(current.get("metadata")) or {}
                    pod = metadata.get("agent_pod") or {}
                    if (
                        str(current.get("runtime_generation"))
                        == str(hint["runtime_generation"])
                        or str(current.get("agent_id") or "") == old_agent_id
                        or str(pod.get("pod_uid") or "") == pod_uid
                    ):
                        raise RuntimeError("aborted claimant is current or rebound")
                fresh = await db.fetchrow(
                    "SELECT * FROM thread_agent_pod_provision_intents WHERE attempt_id=$1::uuid",
                    hint["attempt_id"],
                )
                fresh_outcomes = await db.fetch(
                    "SELECT * FROM thread_runtime_attach_abort_outcomes WHERE thread_id=$1::uuid",
                    thread_id,
                )
                if (
                    fresh is None
                    or dict(fresh) != dict(hint)
                    or _exact_pre_setup_abort(fresh, fresh_outcomes) != old_agent_id
                ):
                    raise RuntimeError("aborted claimant authority changed")

            await _retire_exact_candidates(
                db,
                thread_id=thread_id,
                candidates=[(hint, old_agent_id)],
                intents=intents,
                pvc_name=None,
                assert_current=assert_released_life,
                agent_provisioner=agent_provisioner,
                nonrestartable_attempts=frozenset({str(hint["attempt_id"])}),
            )
        except RuntimeError:
            return False
    return True


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
        old_agent_id = _claimant_soft_settlement(intent, claim, outcomes)
        recycle_proof = any(
            str(handoff["predecessor_attempt_id"]) == attempt
            and handoff["predecessor_pod_uid"] == intent["pod_uid"]
            and handoff["runtime_generation"] == intent["runtime_generation"]
            and handoff["namespace"] == intent["namespace"]
            and handoff["pod_name"] == intent["pod_name"]
            for handoff in handoffs
        )
        if not (old_agent_id or recycle_proof):
            raise RuntimeError("historical claimant lacks immutable actor settlement")
        candidates.append((intent, old_agent_id))

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

    Soft settlement or exact pre-setup abort records the released Pod's life
    on append-only outcomes. An intent with no recorded relation is left untouched —
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
    aborts = await db.fetch(
        "SELECT * FROM thread_runtime_attach_abort_outcomes WHERE thread_id=$1::uuid",
        thread_id,
    )
    candidates = []
    aborted_attempts = set()
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
            aborted_agent_id = _exact_pre_setup_abort(intent, aborts)
            if aborted_agent_id is not None:
                candidates.append((intent, aborted_agent_id))
                aborted_attempts.add(attempt)
            continue
        if not (
            intent["protection_protocol"] == "finalizer_v1"
            and intent["provisioner"] in {"agent", "persistent"}
            and intent["namespace"]
            and intent["pod_uid"]
        ):
            raise RuntimeError("historical agent Pod intent is incomplete")
        expected = _expected_soft_pod_proof(intent)
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
        nonrestartable_attempts=frozenset(aborted_attempts),
    )
