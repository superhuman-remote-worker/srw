"""VM lifecycle protocol compatibility, including frozen pre-extraction vectors."""

import ast
import hashlib
import hmac
import json
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import sys
from uuid import uuid4

import pytest
import yaml

from orchestrator.services import vm_lifecycle_auth as orchestrator_auth
from vm_controller import lifecycle_auth as controller_auth


SECRET = b"a-dedicated-lifecycle-secret-at-least-32-bytes"
ISSUED_AT = 1_800_000_000
REQUEST_ID = "00000000-0000-4000-8000-000000000001"
REPO = Path(__file__).resolve().parents[1]

# Frozen before the shared extraction; both original implementations emitted
# these envelopes. The MACs were independently checked against the v1 canonical
# JSON/domain specification. Never generate expected values using production code.
LEGACY_WIRES = (
    '{"_lifecycle_auth":{"correlation_id":null,"direction":"request",'
    '"issued_at":1800000000,"operation":"create",'
    '"request_id":"00000000-0000-4000-8000-000000000001",'
    '"signature":"c24af73cbf343e2cd21f12cb74a06829034202ed5eaf4bc9327c8a9b468cf32c",'
    '"version":"hmac-sha256-v1"},"cpu_cores":2,'
    '"job_id":"11111111-2222-4333-8444-555555555555",'
    '"metadata":{"display_name":"Größe 世界","enabled":true,"optional":null},'
    '"provision_generation":"aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"}',
    '{"_lifecycle_auth":{"correlation_id":"00000000-0000-4000-8000-000000000001",'
    '"direction":"response","issued_at":1800000000,"operation":"create",'
    '"request_id":"00000000-0000-4000-8000-000000000003",'
    '"signature":"9e9367888f72f2a23b56300bd0b4456223254125f7848fdbf04d05d504c2edfa",'
    '"version":"hmac-sha256-v1"},'
    '"job_id":"11111111-2222-4333-8444-555555555555",'
    '"provision_generation":"aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",'
    '"ready":false,"status":"created","vm_uid":"admitted-uid"}',
)


@pytest.fixture(
    params=[orchestrator_auth, controller_auth], ids=["orchestrator", "controller"]
)
def auth(request):
    return request.param


@pytest.mark.parametrize("wire", LEGACY_WIRES, ids=["request", "response"])
def test_verifies_frozen_legacy_envelope(auth, wire):
    payload = json.loads(wire)
    envelope = payload["_lifecycle_auth"]
    assert auth.verify_payload(
        payload,
        direction=envelope["direction"],
        operation="create",
        secret=SECRET,
        now=ISSUED_AT,
        expected_correlation_id=envelope["correlation_id"],
    )


@pytest.mark.parametrize("wire", LEGACY_WIRES, ids=["request", "response"])
def test_signer_reproduces_frozen_legacy_wire(auth, wire):
    payload = json.loads(wire)
    envelope = payload.pop("_lifecycle_auth")
    result = auth.sign_payload(
        payload,
        direction=envelope["direction"],
        operation="create",
        secret=SECRET,
        issued_at=ISSUED_AT,
        request_id=envelope["request_id"],
        correlation_id=envelope["correlation_id"],
    )
    assert "_lifecycle_auth" not in payload
    assert (
        json.dumps(result, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        == wire
    )


@pytest.mark.parametrize(
    ("offset", "valid"), [(-11, False), (-10, True), (60, True), (61, False)]
)
def test_frozen_envelope_age_boundaries(auth, offset, valid):
    assert (
        auth.verify_payload(
            json.loads(LEGACY_WIRES[0]),
            direction="request",
            operation="create",
            secret=SECRET,
            now=ISSUED_AT + offset,
        )
        is valid
    )


@pytest.mark.parametrize(
    "changes",
    [
        {"issued_at": True},
        {"issued_at": float(ISSUED_AT)},
        {"request_id": "not-a-uuid"},
        {"request_id": "11111111-AAAA-4BBB-8CCC-111111111111"},
        {"request_id": "00000000000040008000000000000001"},
        {"correlation_id": "not-a-uuid"},
        {"correlation_id": "11111111-AAAA-4BBB-8CCC-111111111111"},
        {"correlation_id": 123},
    ],
)
def test_valid_mac_does_not_admit_invalid_envelope_fields(auth, changes):
    options = {
        "direction": "request",
        "operation": "create",
        "secret": SECRET,
        "issued_at": ISSUED_AT,
        "request_id": REQUEST_ID,
        **changes,
    }
    signed = auth.sign_payload({"job_id": "job-one"}, **options)
    assert not auth.verify_payload(
        signed, direction="request", operation="create", secret=SECRET, now=ISSUED_AT
    )


@pytest.mark.parametrize(
    "changes",
    [
        {"direction": "response"},
        {"operation": "delete"},
        {"secret": b"a-different-secret-at-least-32-bytes"},
        {"expected_correlation_id": REQUEST_ID},
    ],
)
def test_frozen_envelope_rejects_wrong_binding(auth, changes):
    options = {
        "direction": "request",
        "operation": "create",
        "secret": SECRET,
        **changes,
    }
    assert not auth.verify_payload(
        json.loads(LEGACY_WIRES[0]), now=ISSUED_AT, **options
    )


def test_legacy_mode_strips_envelope_without_mutating_input(auth):
    payload = json.loads(LEGACY_WIRES[0])
    unsigned = auth.sign_payload(
        payload, direction="request", operation="create", secret=None
    )
    assert "_lifecycle_auth" in payload
    assert "_lifecycle_auth" not in unsigned
    assert unsigned == auth.unsigned_payload(payload)
    assert auth.verify_payload(
        payload, direction="request", operation="create", secret=None
    )
    assert auth.verify_payload(
        unsigned, direction="request", operation="create", secret=None
    )
    assert not auth.verify_payload(
        unsigned, direction="request", operation="create", secret=SECRET
    )


@pytest.mark.parametrize(
    ("value", "expected"), [(None, None), ("", None), ("é" * 16, ("é" * 16).encode())]
)
def test_configured_secret_uses_utf8_byte_length(auth, value, expected):
    source = {} if value is None else {"VM_LIFECYCLE_HMAC_SECRET": value}
    assert auth.configured_secret(source) == expected


@pytest.mark.parametrize("value", ["short", "a" * 31, "é" * 15])
def test_configured_short_secret_raises_same_error(auth, value):
    with pytest.raises(
        auth.LifecycleAuthConfigurationError,
        match="^VM_LIFECYCLE_HMAC_SECRET must be at least 32 bytes$",
    ):
        auth.configured_secret({"VM_LIFECYCLE_HMAC_SECRET": value})


@pytest.mark.parametrize("value", [float("nan"), float("inf")])
def test_nonfinite_payload_keeps_serialization_error(auth, value):
    with pytest.raises(ValueError, match="Out of range float values"):
        auth.sign_payload(
            {"value": value}, direction="request", operation="create", secret=SECRET
        )


def test_guest_token_matches_cross_image_formula() -> None:
    secret = b"0123456789abcdef0123456789abcdef"
    entity_type = "job"
    entity_id = "11111111-1111-4111-8111-111111111111"
    generation = "22222222-2222-4222-8222-222222222222"
    guest_key = hmac.new(secret, b"srw-kdf|vm-guest-token|v1", hashlib.sha256).digest()
    expected = hmac.new(
        guest_key,
        (f"srw.vm.guest.v1\n{entity_type}\n{entity_id}\n{generation}\n").encode(),
        hashlib.sha256,
    ).hexdigest()

    assert (
        orchestrator_auth.guest_token(secret, entity_type, entity_id, generation)
        == expected
    )


def _signed_by_orchestrator(operation: str = "create") -> dict:
    return orchestrator_auth.sign_payload(
        {
            "job_id": str(uuid4()),
            "provision_generation": "00000000-0000-4000-8000-000000000002",
        },
        direction="request",
        operation=operation,
        secret=SECRET,
        issued_at=ISSUED_AT,
        request_id=REQUEST_ID,
    )


def test_orchestrator_and_standalone_controller_envelopes_interoperate() -> None:
    request = _signed_by_orchestrator()
    assert controller_auth.verify_payload(
        request,
        direction="request",
        operation="create",
        secret=SECRET,
        now=ISSUED_AT,
    )

    response = controller_auth.sign_payload(
        {
            "job_id": request["job_id"],
            "vm_uid": "admitted-uid",
            "provision_generation": request["provision_generation"],
        },
        direction="response",
        operation="create",
        secret=SECRET,
        issued_at=ISSUED_AT,
        request_id=REQUEST_ID,
    )
    assert orchestrator_auth.verify_payload(
        response,
        direction="response",
        operation="create",
        secret=SECRET,
        now=ISSUED_AT,
    )


def test_tampered_or_cross_operation_envelope_is_rejected() -> None:
    request = _signed_by_orchestrator()
    request["job_id"] = str(uuid4())
    assert not controller_auth.verify_payload(
        request,
        direction="request",
        operation="create",
        secret=SECRET,
        now=ISSUED_AT,
    )

    untampered = _signed_by_orchestrator()
    assert not controller_auth.verify_payload(
        untampered,
        direction="request",
        operation="delete",
        secret=SECRET,
        now=ISSUED_AT,
    )


@pytest.mark.parametrize("issued_at", [ISSUED_AT - 61, ISSUED_AT + 11])
def test_expired_or_future_envelope_is_rejected(issued_at: int) -> None:
    request = orchestrator_auth.sign_payload(
        {"job_id": "job-one"},
        direction="request",
        operation="status",
        secret=SECRET,
        issued_at=issued_at,
        request_id=REQUEST_ID,
    )
    assert not controller_auth.verify_payload(
        request,
        direction="request",
        operation="status",
        secret=SECRET,
        now=ISSUED_AT,
    )


def test_unsigned_payload_is_legacy_only() -> None:
    payload = {"job_id": "job-one"}
    assert orchestrator_auth.verify_payload(
        payload,
        direction="request",
        operation="create",
        secret=None,
        now=ISSUED_AT,
    )
    assert not orchestrator_auth.verify_payload(
        payload,
        direction="request",
        operation="create",
        secret=SECRET,
        now=ISSUED_AT,
    )


def test_present_short_key_never_downgrades_to_legacy() -> None:
    with pytest.raises(
        orchestrator_auth.LifecycleAuthConfigurationError,
        match="at least 32 bytes",
    ):
        orchestrator_auth.configured_secret({"VM_LIFECYCLE_HMAC_SECRET": "short"})


def test_guest_token_known_vector(auth) -> None:
    assert (
        auth.guest_token(
            SECRET,
            "job",
            "11111111-2222-4333-8444-555555555555",
            "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
        )
        == "42e877b1219d604802f241eca001f27ec0ebc337bd8e05e9fffe22a8b46e3f33"
    )


def test_response_must_correlate_to_the_exact_request() -> None:
    correlation_id = "00000000-0000-4000-8000-000000000010"
    response = controller_auth.sign_payload(
        {"job_id": "job-one", "status": "created"},
        direction="response",
        operation="create",
        secret=SECRET,
        issued_at=ISSUED_AT,
        request_id=REQUEST_ID,
        correlation_id=correlation_id,
    )

    assert orchestrator_auth.verify_payload(
        response,
        direction="response",
        operation="create",
        secret=SECRET,
        now=ISSUED_AT,
        expected_correlation_id=correlation_id,
    )
    assert not orchestrator_auth.verify_payload(
        response,
        direction="response",
        operation="create",
        secret=SECRET,
        now=ISSUED_AT,
        expected_correlation_id="00000000-0000-4000-8000-000000000011",
    )


def _controller_source_copies():
    dockerfile = (REPO / "docker/Dockerfile.vm-controller").read_text()
    for line in dockerfile.splitlines():
        # This guard owns COPY inputs, not shell parsing of unrelated RUN
        # instructions, which may span multiple physical Dockerfile lines.
        if not line.lstrip().startswith("COPY "):
            continue
        parts = shlex.split(line)
        if parts and parts[0] == "COPY":
            for source in parts[1:-1]:
                if source.startswith("src/"):
                    yield source, parts[-1]


def test_controller_shared_inputs_trigger_tilt_and_ci_rebuilds():
    sources = {source for source, _ in _controller_source_copies()}
    shared_inputs = {"src/shared/"}
    assert {
        source for source in sources if source.startswith("src/shared/")
    } == shared_inputs

    tilt = ast.parse((REPO / "Tiltfile").read_text())
    build = next(
        node
        for node in ast.walk(tilt)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "docker_build"
        and node.args
        and isinstance(node.args[0], ast.Constant)
        and node.args[0].value == "srw-vm-controller"
    )
    watched = ast.literal_eval(
        next(keyword.value for keyword in build.keywords if keyword.arg == "only")
    )
    required = shared_inputs | {
        "src/vm_controller/",
        "pyproject.toml",
        ".dockerignore",
        "docker/Dockerfile.vm-controller",
        "scripts/check_vm_controller_imports.py",
    }
    assert required <= set(watched)

    workflow = yaml.safe_load((REPO / ".github/workflows/develop.yml").read_text())
    script = next(
        step["run"]
        for step in workflow["jobs"]["changes"]["steps"]
        if "VM_CONTROLLER_PATHS=" in step.get("run", "")
    )
    declaration = re.search(r"VM_CONTROLLER_PATHS=\((.*?)\)", script, re.DOTALL)
    assert declaration is not None
    assert required <= set(shlex.split(declaration.group(1)))
    assert 'VM_CONTROLLER_SHA=$(last_input_sha "${VM_CONTROLLER_PATHS[@]}")' in script
    assert 'VM_CONTROLLER=$(image_missing vm-controller "$VM_CONTROLLER_SHA")' in script


def test_controller_copied_protocol_imports_without_other_packages(tmp_path):
    # Exercise the Dockerfile's actual source closure without the checkout's
    # editable install or site-packages. Actual built-image smoke remains separate.
    for source, destination in _controller_source_copies():
        target = tmp_path / destination.removeprefix("./")
        if (REPO / source).is_dir():
            shutil.copytree(
                REPO / source,
                target,
                dirs_exist_ok=True,
                ignore=shutil.ignore_patterns("__pycache__"),
            )
        else:
            target.mkdir(parents=True, exist_ok=True)
            shutil.copy2(REPO / source, target)
    script = """
import json
from pathlib import Path
import sys

before = set(sys.modules)
from vm_controller import lifecycle_auth
from shared.workspace_initialization import initialization_request
from shared.vm_workspace_storage import storage_binding
from shared.workspace_preparation import preparation_request
from shared.workspace_preparation_settings import PreparationSettings
assert PreparationSettings().enabled is False
assert callable(preparation_request)
assert callable(storage_binding)
assert initialization_request([{"command": ["true"]}])["version"] == 1
assert Path(lifecycle_auth.__file__).is_relative_to(Path.cwd())
assert lifecycle_auth.verify_payload(
    json.loads(sys.argv[1]),
    direction="request",
    operation="create",
    secret=b"a-dedicated-lifecycle-secret-at-least-32-bytes",
    now=1800000000,
)
new_roots = {name.split(".")[0] for name in set(sys.modules) - before}
assert new_roots <= sys.stdlib_module_names | {"shared", "vm_controller"}
assert "shared.runtime" not in sys.modules
"""
    result = subprocess.run(
        [sys.executable, "-E", "-S", "-c", script, LEGACY_WIRES[0]],
        cwd=tmp_path / "src",
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout == ""
    assert result.stderr == ""


def test_controller_packaged_modules_and_lazy_shared_imports_load(tmp_path):
    """Import the actual copied tree, including imports deferred until startup."""
    for source, destination in _controller_source_copies():
        target = tmp_path / destination.removeprefix("./")
        if (REPO / source).is_dir():
            shutil.copytree(
                REPO / source,
                target,
                dirs_exist_ok=True,
                ignore=shutil.ignore_patterns("__pycache__"),
            )
        else:
            target.mkdir(parents=True, exist_ok=True)
            shutil.copy2(REPO / source, target)
    script = """
from pathlib import Path
import runpy
import sys
sys.path.insert(0, sys.argv[1])
import shared
assert Path(shared.__file__).is_relative_to(Path(sys.argv[1]))
runpy.run_path(sys.argv[2], run_name="__main__")
assert "shared.vm_resource_inventory_settings" in sys.modules
assert "shared.kubernetes_quantities" in sys.modules
assert "shared.runtime" not in sys.modules
"""
    result = subprocess.run(
        [
            sys.executable,
            "-I",
            "-c",
            script,
            str(tmp_path / "src"),
            str(REPO / "scripts/check_vm_controller_imports.py"),
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
