"""Read-only identity of the controller inputs that affect a create request.

No render, credential generation, Kubernetes call or infrastructure preparation is
performed here. Actual templates and installed controller/shared Python sources
are included so a renderer change cannot masquerade as the same configuration.
"""

from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path

from shared.vm_creation_retry import canonical_request_digest, _validate_json
from shared.vm_creation_issuance import canonical_configuration_digest
from shared.workspace_preparation_settings import PreparationSettings
from shared.vm_resource_policy import (
    CompleteResourcePolicySnapshot,
    EnforcementResourcePolicySnapshot,
)


def _digest(value):
    return (
        "sha256:"
        + hashlib.sha256(
            json.dumps(
                value,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            ).encode()
        ).hexdigest()
    )


def resolve_creation_configuration(
    controller,
    request,
    *,
    _resource_policy_snapshot: CompleteResourcePolicySnapshot | None = None,
):
    from vm_controller import controller as settings
    from vm_controller import headscale_client
    import shared
    from shared.workspace_initialization import validate_initialization_request
    from shared.workspace_preparation import validate_request

    if _resource_policy_snapshot is None:
        raw_resource = os.getenv("VM_RESOURCE_ADMISSION_CONFIG", "")
        if raw_resource:
            from shared.vm_resource_policy import validate_enforcement_resource_policy

            try:
                resource_document = json.loads(raw_resource)
                flags = resource_document["policy"]
                if flags["enforcementEnabled"] is True:
                    _resource_policy_snapshot = validate_enforcement_resource_policy(
                        resource_document
                    )
                elif flags["shadowEnabled"] is True:
                    raise ValueError("Resource shadow mode is unavailable")
            except (ValueError, TypeError, KeyError, UnicodeError):
                raise ValueError("Invalid installed resource policy") from None

    # Validate the complete option vocabulary before adding resolved defaults.
    canonical_request_digest(request)
    payload = dict(request)
    if payload.get("entity_type", "job") not in {"job", "thread"}:
        raise ValueError("Creation retry owner kind is unsupported")
    from uuid import UUID

    for key in ("job_id", "provision_generation"):
        if (
            not isinstance(payload.get(key), str)
            or str(UUID(payload[key])) != payload[key]
        ):
            raise ValueError("Creation identity incomplete")
    payload["entity_type"] = payload.get("entity_type", "job")
    payload["vm_image"] = payload.get("vm_image") or settings.DEFAULT_VM_IMAGE
    payload["cpu_cores"] = payload.get("cpu_cores", settings.DEFAULT_CPU)
    payload["memory"] = payload.get("memory") or settings.DEFAULT_MEMORY
    payload["agent_config"] = payload.get("agent_config", "worker_base")
    payload["description"] = payload.get("description", "")
    payload["disk_size"] = settings.effective_disk_size(payload)
    payload["network_tier"] = (
        str(payload.get("network_tier") or "").strip()
        or settings.VM_DEFAULT_NETWORK_TIER
        or "internet-only"
    )
    if (
        not settings._NETWORK_TIER_PATTERN.fullmatch(payload["network_tier"])
        or type(payload["cpu_cores"]) is not int
        or payload["cpu_cores"] < 1
    ):
        raise ValueError("Invalid resolved create options")
    if "network_profile" in payload:
        from shared.vm_network_profile import (
            NETWORK_PROFILE,
            compatible_image,
            validate_network_profile,
        )

        validate_network_profile(payload["network_profile"])
        if (
            not compatible_image(payload["vm_image"])
            or payload.get("preparation") is not None
        ):
            raise ValueError("VM network profile source is not admitted")
    if payload.get("workspace_storage") is not None:
        from shared.vm_creation_lineage import disk_owner

        # Read-only resolution binds accounting identity; SQL handoff proof is
        # still required before any controller effect is granted.
        disk_owner(payload)
    if payload.get("initialization") is not None:
        validate_initialization_request(payload["initialization"])
    if payload.get("preparation") is not None:
        validate_request(payload["preparation"])
    template = controller.template_text
    cloud_init = controller.cloud_init_text
    if (
        not isinstance(template, str)
        or not template
        or not isinstance(cloud_init, str)
        or not cloud_init
    ):
        raise ValueError("Controller templates unavailable")
    public_key = os.environ.get("SSH_AUTHORIZED_KEY", "")
    _validate_json(public_key)
    if "${SSH_AUTHORIZED_KEY}" in cloud_init and not public_key.strip():
        raise ValueError("Controller public SSH identity unavailable")
    sources = {}
    for package, root in [
        ("vm_controller", Path(settings.__file__).parent),
        ("shared", Path(shared.__file__).parent),
    ]:
        for file in sorted(root.rglob("*.py")):
            sources[f"{package}/{file.relative_to(root)}"] = hashlib.sha256(
                file.read_bytes()
            ).hexdigest()
    preparation_service = getattr(controller, "_workspace_preparation_service", None)
    preparation_settings = (
        preparation_service.settings
        if preparation_service is not None
        else PreparationSettings.from_environment()
    )
    if payload.get("preparation") is not None:
        from shared.workspace_preparation_settings import disk_bytes

        if disk_bytes(payload["disk_size"]) < disk_bytes(
            preparation_settings.disk_size
        ):
            if request.get("disk_size") is not None:
                raise ValueError("Prepared source exceeds requested disk capacity")
            payload["disk_size"] = preparation_settings.disk_size
    configuration = {
        "version": 1,
        "namespace": settings.VM_NAMESPACE,
        "vm_template_digest": "sha256:" + hashlib.sha256(template.encode()).hexdigest(),
        "cloud_init_template_digest": "sha256:"
        + hashlib.sha256(cloud_init.encode()).hexdigest(),
        "implementation_digest": _digest(sources),
        "storage_class": settings.VM_STORAGE_CLASS,
        "persistent_rootdisk": settings.VM_PERSISTENT_ROOTDISK,
        "node_selector": settings.VM_NODE_SELECTOR,
        "tolerations": settings.VM_TOLERATIONS,
        "nats_url": settings.NATS_URL,
        "orchestrator_id": settings.ORCHESTRATOR_ID,
        "orchestrator_url": settings.ORCHESTRATOR_URL
        or str(payload.get("orchestrator_url") or "").strip(),
        "headscale_url": os.environ.get("HEADSCALE_URL", ""),
        "headscale_enabled": controller.headscale.is_available,
        # The enrollment API uses import-time client settings; cloud-init's
        # endpoint above is separately read from the rendering environment.
        # Bind both actual sources without capturing API or generated keys.
        "headscale_api_url": headscale_client.HEADSCALE_URL,
        "headscale_user": headscale_client.HEADSCALE_USER,
        "headscale_key_expiry_minutes": headscale_client.AUTH_KEY_EXPIRY_MINUTES,
        "authorized_public_key_digest": "sha256:"
        + hashlib.sha256(public_key.encode()).hexdigest(),
        "golden_enabled": settings.VM_GOLDEN_IMAGE_ENABLED,
        "golden_disk_size": settings.VM_GOLDEN_DISK_SIZE,
        "disk_size_floor": settings.VM_DISK_SIZE,
        "preparation": asdict(preparation_settings),
    }
    if "network_profile" in payload:
        configuration["network_profile_policy"] = {
            "version": 1,
            "image": payload["vm_image"],
            "profile": dict(NETWORK_PROFILE),
        }
    # JSON-normalize tuple settings before credential validation. No credential
    # values, generated guest tokens or rendered cloud-init enter this document.
    configuration = json.loads(json.dumps(configuration))
    if _resource_policy_snapshot is not None:
        from shared.vm_resource_configuration import build_resource_configuration
        from shared.vm_resource_policy import (
            validate_enforcement_resource_policy_snapshot,
            validate_resource_policy_snapshot,
        )

        _resource_policy_snapshot = (
            validate_enforcement_resource_policy_snapshot(_resource_policy_snapshot)
            if type(_resource_policy_snapshot) is EnforcementResourcePolicySnapshot
            else validate_resource_policy_snapshot(_resource_policy_snapshot)
        )
        configuration["version"] = (
            3 if _resource_policy_snapshot.inventory.protocol == 2 else 2
        )
        configuration["resource_admission"] = build_resource_configuration(
            _resource_policy_snapshot,
            template=template,
            request=payload,
            configuration=configuration,
        )
    _validate_json(configuration)
    return {
        "creation_retry_protocol": 1,
        "request": payload,
        "request_digest": canonical_request_digest(payload),
        "controller_configuration": configuration,
        "controller_configuration_digest": canonical_configuration_digest(
            configuration
        ),
    }
