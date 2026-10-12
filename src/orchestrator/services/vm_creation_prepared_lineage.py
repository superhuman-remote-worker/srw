"""Select full prepared clone provenance under the existing inherited Job locks."""

from uuid import UUID

from orchestrator.services.vm_creation_lineage import _object, _refuse
from shared.vm_creation_issuance import (
    prepared_source_metadata,
    validate_rootdisk_source,
)
from shared.vm_inherited_preparation import validate_inherited_preparation


def prepared_origin(proof):
    if proof.get("kind") == "retained_attachment_replacement":
        proof = proof["handoff"]
    return proof.get("prepared_origin")


def validate_prepared_request(proof, request, configuration=None):
    origin = prepared_origin(proof)
    if origin is None:
        if request.get("preparation") is not None:
            _refuse()
        return
    try:
        validate_inherited_preparation(
            origin, request=request, configuration=configuration
        )
    except (ValueError, KeyError, TypeError):
        _refuse()


async def prove_prepared_origin(conn, scope):
    first, previous = scope["original_vm"], scope["previous_vm"]
    binding = scope["binding"]
    rows = await conn.fetch(
        "SELECT r.request_id,r.canonical_request,r.controller_configuration,e.effect_nonce,e.carrier_intent,e.evidence "
        "FROM vm_creation_retries r JOIN vm_creation_effects e ON e.request_id=r.request_id "
        "WHERE r.job_id=$1 AND r.state IN ('succeeded','settled') AND r.reason='creation_adopted' "
        "AND r.observed_pvc_uid=$2 AND e.effect_kind='rootdisk' AND e.state='observed' "
        "AND e.evidence->>'pvc_uid'=$3 AND e.carrier_intent->'rootdisk_source'->>'kind'='prepared' "
        "AND e.carrier_intent->'rootdisk_source'->>'mode'='clone'",
        UUID(binding["owner_id"]),
        UUID(binding["pvc_uid"]),
        binding["pvc_uid"],
    )
    if not rows and all(
        vm.get("preparation") is None and vm.get("preparation_request") is None
        for vm in (first, previous)
    ):
        return None
    if len(rows) != 1:
        _refuse()
    row = rows[0]
    origin = {
        "request_id": str(row["request_id"]),
        "effect_nonce": str(row["effect_nonce"]),
        "request": _object(row["canonical_request"]),
        "configuration": _object(row["controller_configuration"]),
        "source": _object(row["carrier_intent"])["rootdisk_source"],
        "root": _object(row["evidence"]),
    }
    try:
        validate_rootdisk_source(
            origin["source"],
            request=origin["request"],
            configuration=origin["configuration"],
            expected_pvc_uid=None,
        )
        for job_id, vm in (
            (binding["owner_id"], first),
            (scope["previous_job_id"], previous),
        ):
            if vm["preparation"] != prepared_source_metadata(origin["source"]):
                _refuse()
            if job_id == binding["owner_id"]:
                if vm["preparation_request"] != origin["request"]["preparation"]:
                    _refuse()
            else:
                validate_inherited_preparation(
                    origin,
                    request={
                        "job_id": job_id,
                        "workspace_storage": vm["workspace_storage"],
                        "vm_image": vm["preparation_request"]["image"],
                        "preparation": vm["preparation_request"],
                    },
                )
    except (ValueError, KeyError, TypeError):
        _refuse()
    return origin
