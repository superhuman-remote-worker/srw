"""Immutable VM create snapshots; callers authenticate controller resolution."""

from collections.abc import Mapping
from copy import deepcopy
import re
from typing import Any

from shared.vm_creation_retry import canonical_request_digest
from shared.vm_creation_issuance import canonical_configuration_digest


def build_vm_creation_request(
    *,
    job_id: str,
    agent_config: str,
    vm_image: str | None,
    cpu_cores: int,
    memory: str,
    description: str,
    network_tier: str,
    entity_type: str = "job",
    provision_generation: str | None = None,
    orchestrator_url: str | None = None,
    disk_size: str | None = None,
    initialization: dict | None = None,
    workspace_storage: dict | None = None,
    preparation: dict | None = None,
) -> dict:
    """Build the existing unsigned HTTP shape without resolving controller defaults."""
    payload = {
        "job_id": job_id,
        "entity_type": entity_type,
        "agent_config": agent_config,
        "cpu_cores": cpu_cores,
        "memory": memory,
        "description": description,
        "nats_url": "",
        "network_tier": network_tier,
    }
    for key, value in (
        ("orchestrator_url", orchestrator_url),
        ("disk_size", disk_size),
        ("provision_generation", provision_generation),
        ("vm_image", vm_image),
    ):
        if value:
            payload[key] = value
    for key, value in (
        ("workspace_storage", workspace_storage),
        ("preparation", preparation),
    ):
        if value is not None:
            payload[key] = value
    if initialization is not None:
        from shared.workspace_initialization import validate_initialization_request

        payload["initialization"] = validate_initialization_request(initialization)
    return deepcopy(payload)


async def capture_vm_creation_request(
    db: Any,
    *,
    job_id: str,
    generation: str,
    request: Mapping[str, object] | None = None,
    initial_request: bool = True,
    controller_configuration_digest: str | None = None,
    controller_configuration: dict | None = None,
    _conn: Any = None,
) -> dict | None:
    """Read or atomically freeze first inputs, returning only a validated snapshot.

    A lookup precedes option resolution on replay. The store's conditional write
    still chooses one winner if multiple first creates race; a lookup is never
    permission to overwrite. A None result cannot authorize transport.

    A supplied controller digest must come from authenticated resolution; this
    helper validates its shape, not its provenance. The optional complete
    configuration document is frozen with its digest; effect grants require it.
    Without it, controller namespace/implementation remains unproven. Snapshot
    capture alone never proves original issuance was fenced.
    """
    proposal = None
    if (
        controller_configuration is not None
        and canonical_configuration_digest(controller_configuration)
        != controller_configuration_digest
    ):
        raise ValueError("Controller configuration document does not match its digest.")
    if controller_configuration_digest is not None and (
        not isinstance(controller_configuration_digest, str)
        or re.fullmatch(r"sha256:[0-9a-f]{64}", controller_configuration_digest) is None
    ):
        raise ValueError("Controller configuration requires a canonical digest.")
    if request is not None:
        if (
            request.get("job_id") != job_id
            or request.get("entity_type") != "job"
            or request.get("provision_generation") != generation
        ):
            raise ValueError("Creation request owner or generation changed.")
        proposal = {
            "version": 1,
            "provision_generation": generation,
            "request": deepcopy(dict(request)),
            "request_digest": canonical_request_digest(request),
            "initial_request": initial_request,
            "controller_configuration_digest": controller_configuration_digest,
            "controller_configuration_authenticated": controller_configuration_digest
            is not None,
            "issuance_authority_bound": False,
        }
        if controller_configuration is not None:
            proposal["controller_configuration"] = deepcopy(controller_configuration)
    captured = await db.capture_vm_creation_request_if_generation(
        job_id, generation, proposal, **({"_conn": _conn} if _conn is not None else {})
    )
    if captured is None:
        return None
    if not isinstance(captured, dict):
        raise ValueError("Stored creation request is malformed.")
    payload = captured.get("request")
    configuration = captured.get("controller_configuration_digest")
    if (
        type(captured.get("version")) is not int
        or captured.get("version") != 1
        or captured.get("provision_generation") != generation
        or type(captured.get("initial_request")) is not bool
        or captured.get("issuance_authority_bound") is not False
        or (
            configuration is not None
            and (
                not isinstance(configuration, str)
                or re.fullmatch(r"sha256:[0-9a-f]{64}", configuration) is None
            )
        )
        or captured.get("controller_configuration_authenticated")
        is not (configuration is not None)
        or (
            controller_configuration_digest is not None
            and configuration != controller_configuration_digest
        )
        or not isinstance(payload, dict)
        or payload.get("job_id") != job_id
        or payload.get("entity_type") != "job"
        or payload.get("provision_generation") != generation
        or canonical_request_digest(payload) != captured.get("request_digest")
    ):
        raise ValueError("Stored creation request identity changed.")
    document = captured.get("controller_configuration")
    if (
        document is not None
        and canonical_configuration_digest(document) != configuration
    ):
        raise ValueError("Stored controller configuration identity changed.")
    if controller_configuration is not None and document != controller_configuration:
        raise ValueError("Stored controller configuration document changed.")
    return deepcopy(captured)
