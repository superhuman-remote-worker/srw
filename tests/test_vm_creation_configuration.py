from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4
import pytest
from vm_controller import controller as module
from vm_controller.creation_configuration import resolve_creation_configuration
from shared.vm_creation_retry import canonical_request_digest


def request():
    return {
        "job_id": str(uuid4()),
        "entity_type": "job",
        "provision_generation": str(uuid4()),
        "description": "example",
        "initialization": None,
    }


def controller():
    return SimpleNamespace(
        template_text="kind: VirtualMachine\nimage: ${VM_IMAGE}",
        cloud_init_text="#cloud-config\nhostname: worker",
        headscale=SimpleNamespace(is_available=True),
    )


def test_resolution_freezes_all_options_and_effective_render_inputs(monkeypatch):
    original = request()
    resolved = resolve_creation_configuration(controller(), original)
    assert resolved["request_digest"] == canonical_request_digest(resolved["request"])
    assert resolved["request"]["initialization"] is None
    assert resolved["request"]["vm_image"] == module.DEFAULT_VM_IMAGE
    assert resolved["request"]["disk_size"] == module.VM_DISK_SIZE
    monkeypatch.setattr(module, "VM_NODE_SELECTOR", {"zone": "different"})
    changed = resolve_creation_configuration(controller(), original)
    assert (
        changed["controller_configuration_digest"]
        != resolved["controller_configuration_digest"]
    )
    assert original == {**original} and "vm_image" not in original


def test_resolution_refuses_credentials_in_semantic_endpoints(monkeypatch):
    monkeypatch.setattr(module, "NATS_URL", "nats://user:password@example:4222")
    with pytest.raises(ValueError):
        resolve_creation_configuration(controller(), request())


@pytest.mark.asyncio
async def test_resolution_endpoint_requires_mac_and_does_no_mutating_io(monkeypatch):
    from shared.vm_lifecycle_auth import sign_payload, verify_payload, AUTH_FIELD

    secret = b"creation-configuration-test-secret-at-least-32"
    monkeypatch.setattr(module, "LIFECYCLE_HMAC_SECRET", secret)
    vm = module.VMController.__new__(module.VMController)
    vm.template_text = "kind: VirtualMachine\nimage: ${VM_IMAGE}"
    vm.cloud_init_text = "#cloud-config"
    vm.headscale = SimpleNamespace(is_available=True, create_auth_key=AsyncMock())
    vm._verify_lifecycle_request = AsyncMock(return_value=True)
    vm.render_template = AsyncMock(
        side_effect=AssertionError("must not render credentials")
    )
    body = sign_payload(
        {"request": request()},
        direction="request",
        operation="creation_config_resolve",
        secret=secret,
    )
    response = await vm.http_resolve_creation_config(
        SimpleNamespace(json=AsyncMock(return_value=body))
    )
    assert response.status == 200
    import json

    result = json.loads(response.text)
    assert verify_payload(
        result,
        direction="response",
        operation="creation_config_resolve",
        secret=secret,
        expected_correlation_id=body[AUTH_FIELD]["request_id"],
    )
    vm._verify_lifecycle_request.assert_awaited_once_with(
        body, "creation_config_resolve", mutating=False
    )
    vm.headscale.create_auth_key.assert_not_awaited()
    vm.render_template.assert_not_awaited()


@pytest.mark.asyncio
async def test_unsigned_resolution_is_refused_before_configuration_access(monkeypatch):
    secret = b"creation-configuration-test-secret-at-least-32"
    monkeypatch.setattr(module, "LIFECYCLE_HMAC_SECRET", secret)
    vm = module.VMController.__new__(module.VMController)
    response = await vm.http_resolve_creation_config(
        SimpleNamespace(json=AsyncMock(return_value={"request": request()}))
    )
    assert response.status == 401


def test_all_supplied_semantic_options_survive_resolution():
    original = {
        **request(),
        "cpu_cores": 9,
        "memory": "19Gi",
        "disk_size": "123Gi",
        "network_tier": "custom-tier",
        "orchestrator_url": "https://orchestrator.example",
        "nats_url": "nats://caller.example:4222",
    }
    resolved = resolve_creation_configuration(controller(), original)
    for key, value in original.items():
        assert resolved["request"][key] == value


@pytest.mark.asyncio
async def test_effective_configuration_document_is_frozen_and_cannot_be_recaptured():
    from tests.test_vm_creation_request import CreationDB, options, JOB, GENERATION
    from orchestrator.services.vm_creation_request import (
        build_vm_creation_request,
        capture_vm_creation_request,
    )

    request_payload = build_vm_creation_request(**options(), network_tier="restricted")
    resolved = resolve_creation_configuration(controller(), request_payload)
    db = CreationDB()
    captured = await capture_vm_creation_request(
        db,
        job_id=JOB,
        generation=GENERATION,
        request=resolved["request"],
        controller_configuration=resolved["controller_configuration"],
        controller_configuration_digest=resolved["controller_configuration_digest"],
    )
    assert captured["controller_configuration"] == resolved["controller_configuration"]
    changed = controller()
    changed.cloud_init_text = "#cloud-config\nhostname: changed"
    drift = resolve_creation_configuration(changed, request_payload)
    with pytest.raises(ValueError):
        await capture_vm_creation_request(
            db,
            job_id=JOB,
            generation=GENERATION,
            request=drift["request"],
            controller_configuration=drift["controller_configuration"],
            controller_configuration_digest=drift["controller_configuration_digest"],
        )
    assert db.snapshot == captured


@pytest.mark.parametrize(
    ("setting", "original", "changed", "field"),
    [
        ("HEADSCALE_USER", "original-account", "different-account", "headscale_user"),
        ("AUTH_KEY_EXPIRY_MINUTES", 10, 30, "headscale_key_expiry_minutes"),
        (
            "HEADSCALE_URL",
            "https://original-headscale.example",
            "https://different-headscale.example",
            "headscale_api_url",
        ),
    ],
)
def test_resolution_binds_actual_headscale_issuance_settings(
    monkeypatch, setting, original, changed, field
):
    from vm_controller import headscale_client

    payload = request()
    monkeypatch.setattr(headscale_client, setting, original)
    first = resolve_creation_configuration(controller(), payload)
    monkeypatch.setattr(headscale_client, setting, changed)
    second = resolve_creation_configuration(controller(), payload)
    assert first["request_digest"] == second["request_digest"]
    assert (
        first["controller_configuration_digest"]
        != second["controller_configuration_digest"]
    )
    assert first["controller_configuration"][field] == original
    assert second["controller_configuration"][field] == changed


@pytest.mark.parametrize(
    "field", ["headscale_user", "headscale_key_expiry_minutes", "headscale_api_url"]
)
def test_configuration_missing_headscale_issuance_identity_is_refused(field):
    from shared.vm_creation_issuance import canonical_configuration_digest

    configuration = resolve_creation_configuration(controller(), request())[
        "controller_configuration"
    ]
    configuration.pop(field, None)
    with pytest.raises(ValueError, match="incomplete"):
        canonical_configuration_digest(configuration)
