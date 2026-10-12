"""Own-incarnation retirement composed with an unchanged inherited handoff."""

from uuid import UUID

from orchestrator.services.vm_creation_lineage import _object, _refuse, prove
from shared.vm_workspace_storage import storage_binding


_RETIREMENT_FIELDS = (
    "status",
    "provision_generation",
    "identity_provision_generation",
    "identity_authenticated",
    "vm_uid",
    "rootdisk_pvc_uid",
    "workspace_storage",
    "retirement_cleanup_pending",
    "preparation",
    "preparation_request",
)


async def prove_replacement(
    conn,
    *,
    job,
    binding,
    expected=None,
    cleanup_id=None,
    own_proposal=None,
    adoption=False,
):
    """Caller owns the inherited scope, queue/Job/execution/retry locks.

    Capture compares the current retired VM. Replays compare the frozen last_vm
    with the prior adopted ledger and this Job's own receipt/cleanup, then repeat
    the original handoff proof under the exact instance lock.
    """
    from orchestrator.services.vm_creation_retry_store import (
        VMCreationRetryStore,
        _record,
    )

    context = _object(job["context"])
    old = _object(context.get("last_vm"))
    facts = {key: old.get(key) for key in _RETIREMENT_FIELDS}
    try:
        if (
            storage_binding(facts["workspace_storage"]) != binding
            or facts["status"] != "deleted"
            or facts["identity_authenticated"] is not True
            or facts["identity_provision_generation"] != facts["provision_generation"]
            or facts["rootdisk_pvc_uid"] != binding["pvc_uid"]
            or facts["retirement_cleanup_pending"] is True
        ):
            _refuse()
        retired_generation = UUID(facts["provision_generation"])
    except (ValueError, KeyError, TypeError):
        _refuse()
    if expected is None:
        current = _object(context.get("vm"))
        if own_proposal is None or any(
            current.get(key) != value for key, value in facts.items()
        ):
            _refuse()
        proposal = own_proposal
    else:
        if (
            expected.get("kind") != "retained_attachment_replacement"
            or expected.get("retired_vm") != facts
        ):
            _refuse()
        proposal = {
            "predecessor_evidence": expected.get("retirement"),
            "predecessor_cleanup_admission_id": cleanup_id,
        }
    retirement, cleanup = await VMCreationRetryStore._own_predecessor(
        conn, job, UUID(binding["pvc_uid"]), proposal
    )
    previous = _record(
        await conn.fetchrow(
            "SELECT * FROM vm_creation_retries WHERE job_id=$1 AND provision_generation=$2 FOR SHARE",
            job["id"],
            retired_generation,
        )
    )
    if (
        not previous
        or previous["state"] not in {"succeeded", "settled"}
        or previous["reason"] != "creation_adopted"
        or str(previous["observed_vm_uid"]) != facts["vm_uid"]
        or str(previous["observed_pvc_uid"]) != binding["pvc_uid"]
        or previous["canonical_request"].get("workspace_storage") != binding
    ):
        _refuse()
    # The retired VM's context copy is optional convenience metadata. The
    # adopted ledger remains the execution and deadline authority even if that
    # copy disappeared. The existing scope already holds this execution lock.
    execution = await conn.fetchrow(
        "SELECT *,CASE WHEN resolved->'spec'->>'timeoutSeconds' IS NULL THEN NULL "
        "ELSE created_at+((resolved->'spec'->>'timeoutSeconds')::double precision * interval '1 second') END AS deadline "
        "FROM srw_execution_specs WHERE work_kind='Job' AND work_id=$1 FOR SHARE",
        job["id"],
    )
    if (
        not execution
        or execution["id"] != previous["execution_id"]
        or execution["revision"] != previous["execution_revision"]
        or execution["generation"] != previous["execution_generation"]
        or execution["deadline"] != previous["admission_deadline"]
    ):
        _refuse()
    historical = previous["predecessor_evidence"]
    handoff = (
        historical.get("handoff")
        if historical.get("kind") == "retained_attachment_replacement"
        else historical
    )
    if (
        not isinstance(handoff, dict)
        or handoff.get("kind") != "retained_attachment_handoff"
    ):
        _refuse()
    await prove(
        conn, job_id=job["id"], binding=binding, expected=handoff, adoption=adoption
    )
    from orchestrator.services.vm_creation_prepared_lineage import (
        prepared_origin,
        validate_prepared_request,
    )
    from shared.vm_creation_issuance import prepared_source_metadata

    origin = prepared_origin(handoff)
    validate_prepared_request(
        handoff, previous["canonical_request"], previous["controller_configuration"]
    )
    if facts["preparation"] != (
        prepared_source_metadata(origin["source"]) if origin else None
    ) or facts["preparation_request"] != previous["canonical_request"].get(
        "preparation"
    ):
        _refuse()
    attachment_rows = await conn.fetch(
        "SELECT evidence FROM vm_creation_effects WHERE request_id=$1 AND effect_kind='workspace_attach' AND state='observed'",
        previous["request_id"],
    )
    if len(attachment_rows) != 1:
        _refuse()
    attachment = _object(attachment_rows[0]["evidence"])
    if (
        attachment.get("workspace_uid") != binding["uid"]
        or attachment.get("generation") != binding["generation"]
        or attachment.get("execution_id") != str(job["id"])
    ):
        _refuse()
    proof = {
        "kind": "retained_attachment_replacement",
        "version": 1,
        "handoff": handoff,
        "retired_request_id": str(previous["request_id"]),
        "retired_vm": facts,
        "retirement": retirement,
        "cleanup_admission_id": str(cleanup),
        "attachment": attachment,
    }
    if expected is not None and proof != expected:
        _refuse()
    return proof, cleanup


def validate_replacement_claim(proof, intent):
    """The old adopted Lease, still attached to this Job at this generation."""
    observed = proof["attachment"]
    prior = {
        "uid": observed["uid"],
        "resource_version": observed["resource_version"],
        "workspace_uid": observed["workspace_uid"],
        "generation": observed["generation"],
        "execution_id": observed["execution_id"],
        "detached": False,
        "released": False,
    }
    if intent["action"] != "claim" or intent["prior"] != prior:
        _refuse()
