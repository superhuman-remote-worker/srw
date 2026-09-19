"""Freeze the first unsigned create before dispatch and never replay current defaults."""

import asyncio
from copy import deepcopy
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import pytest

from orchestrator.services.vm_creation_request import (
    build_vm_creation_request,
    capture_vm_creation_request,
)
from orchestrator.services.vm_provisioner import VMProvisioner
from shared.vm_lifecycle_auth import AUTH_FIELD, sign_payload, unsigned_payload
from tests.test_non_pinned_workspace_lifecycle_real_postgres import (
    _schema_applied,  # noqa: F401
    db as _postgres_db_fixture,
    pg_dsn,  # noqa: F401
)

db = _postgres_db_fixture

JOB = "00000000-0000-4000-8000-000000000041"
GENERATION = "00000000-0000-4000-8000-000000000042"
SECRET = b"creation-request-test-secret-at-least-32-bytes"


def options(**changes):
    return {
        "job_id": JOB,
        "agent_config": "worker_base",
        "vm_image": "image@sha256:original",
        "cpu_cores": 8,
        "memory": "16Gi",
        "description": "work",
        "provision_generation": GENERATION,
        **changes,
    }


def test_builder_preserves_wire_options_and_owns_nested_values():
    storage = {"pvc_uid": "pinned-disk", "owner_id": JOB}
    request = build_vm_creation_request(
        **options(),
        network_tier="restricted",
        workspace_storage=storage,
        orchestrator_url="https://orchestrator.example",
        disk_size="100Gi",
    )
    storage["pvc_uid"] = "replacement"
    assert request == {
        "job_id": JOB,
        "entity_type": "job",
        "agent_config": "worker_base",
        "vm_image": "image@sha256:original",
        "cpu_cores": 8,
        "memory": "16Gi",
        "description": "work",
        "nats_url": "",
        "network_tier": "restricted",
        "provision_generation": GENERATION,
        "orchestrator_url": "https://orchestrator.example",
        "disk_size": "100Gi",
        "workspace_storage": {"pvc_uid": "pinned-disk", "owner_id": JOB},
    }


class CreationDB:
    """Network-bound store stand-in; real SQL exclusion is tested below."""

    def __init__(self):
        self.generation = GENERATION
        self.snapshot = None
        self.updates = []
        self.get_workspace_network_tier = AsyncMock(return_value="restricted")
        self.refuse_status = False

    async def capture_vm_creation_request_if_generation(
        self, job, generation, snapshot
    ):
        if generation != self.generation:
            return None
        if self.snapshot is None:
            self.snapshot = deepcopy(snapshot)
        return deepcopy(self.snapshot)

    async def merge_vm_context_if_provision_generation(self, job, generation, updates):
        if generation != self.generation or (
            self.refuse_status and updates.get("status") == "provisioning"
        ):
            return False
        self.updates.append(deepcopy(updates))
        return True


@pytest.fixture
def creator(monkeypatch):
    monkeypatch.setenv("VM_MODE", "same-cluster")
    provisioner = VMProvisioner()
    provisioner._lifecycle_hmac_secret = SECRET
    provisioner._db = CreationDB()
    sent = []

    async def post(path, *, json):
        assert provisioner._db.snapshot is not None
        unsigned = unsigned_payload(json)
        assert unsigned == provisioner._db.snapshot["request"]
        sent.append(unsigned)
        body = sign_payload(
            {"status": "waiting_capacity", "provision_generation": GENERATION},
            direction="response",
            operation="create",
            secret=SECRET,
            correlation_id=json[AUTH_FIELD]["request_id"],
        )
        return httpx.Response(
            200, json=body, request=httpx.Request("POST", "http://controller/vms")
        )

    provisioner._http_client = SimpleNamespace(post=AsyncMock(side_effect=post))
    return provisioner, sent


@pytest.mark.asyncio
async def test_capture_precedes_post_and_remains_unproven_without_controller_config(
    creator,
):
    provisioner, sent = creator
    assert await provisioner._create_http(**options())
    snapshot = provisioner._db.snapshot
    assert sent[0]["network_tier"] == "restricted"
    assert snapshot["version"] == 1
    assert snapshot["controller_configuration_digest"] is None
    assert snapshot["controller_configuration_authenticated"] is False
    assert snapshot["issuance_authority_bound"] is False
    assert AUTH_FIELD not in snapshot["request"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "refusal", ["stale_generation", "capture_failed", "status_failed"]
)
async def test_failed_capture_or_status_cas_prevents_post(creator, refusal):
    provisioner, sent = creator
    if refusal == "stale_generation":
        provisioner._db.generation = str(uuid4())
    elif refusal == "capture_failed":
        provisioner._db.capture_vm_creation_request_if_generation = AsyncMock(
            return_value=None
        )
    else:
        provisioner._db.refuse_status = True
    assert await provisioner._create_http(**options()) is False
    assert sent == []


@pytest.mark.asyncio
async def test_deferred_replay_uses_first_snapshot_despite_current_options_drifting(
    creator, monkeypatch
):
    provisioner, sent = creator
    monkeypatch.setenv("ORCHESTRATOR_URL", "https://original.example")
    await provisioner._create_http(**options())
    original = deepcopy(provisioner._db.snapshot)
    monkeypatch.setenv("ORCHESTRATOR_URL", "https://replacement.example")
    provisioner._db.get_workspace_network_tier.return_value = "home-allowed"
    await provisioner._create_http(
        **options(vm_image="different", cpu_cores=16), set_provisioning=False
    )
    assert len(sent) == 2
    assert sent[0] == sent[1]
    assert provisioner._db.snapshot == original


@pytest.mark.asyncio
async def test_lost_response_records_ambiguity_and_preserves_first_request(creator):
    provisioner, sent = creator
    post = provisioner._http_client.post.side_effect

    async def lost(path, *, json):
        await post(path, json=json)
        raise httpx.ReadTimeout("lost reply")

    provisioner._http_client.post.side_effect = lost
    assert await provisioner._create_http(**options()) is False
    observation = provisioner._db.updates[-1]["creation_observation"]
    assert observation["outcome"] == "transport_unknown"
    assert observation["authenticated"] is False
    provisioner._http_client.post.side_effect = post
    await provisioner._create_http(
        **options(vm_image="changed"), set_provisioning=False
    )
    assert sent[0] == sent[1]


@pytest.mark.asyncio
@pytest.mark.parametrize("authenticated", [True, False])
async def test_rejection_facts_require_authenticated_same_generation_reply(
    creator, authenticated
):
    provisioner, _ = creator

    async def reject(path, *, json):
        body = {
            "status": "failed",
            "error": "409 is not authority",
            "provision_generation": GENERATION,
        }
        if authenticated:
            body = sign_payload(
                body,
                direction="response",
                operation="create",
                secret=SECRET,
                correlation_id=json[AUTH_FIELD]["request_id"],
            )
        return httpx.Response(
            409, json=body, request=httpx.Request("POST", "http://controller/vms")
        )

    provisioner._http_client.post.side_effect = reject
    assert await provisioner._create_http(**options()) is False
    observed = provisioner._db.updates[-1]["creation_observation"]
    assert observed["outcome"] == ("rejected" if authenticated else "response_unproven")
    assert observed["authenticated"] is authenticated
    assert "409 is not authority" not in json.dumps(observed)


@pytest.mark.asyncio
async def test_capture_rejects_tampered_returned_snapshot(creator):
    provisioner, _ = creator
    request = build_vm_creation_request(**options(), network_tier="restricted")
    await capture_vm_creation_request(
        provisioner._db, job_id=JOB, generation=GENERATION, request=request
    )
    provisioner._db.snapshot["request"]["memory"] = "32Gi"
    with pytest.raises(ValueError):
        await capture_vm_creation_request(
            provisioner._db, job_id=JOB, generation=GENERATION, request=request
        )


@pytest.mark.asyncio
async def test_postgres_concurrent_capture_returns_one_immutable_snapshot(db):
    job, generation = uuid4(), str(uuid4())
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO jobs(id,description,status,context) VALUES($1,'capture','paused',$2::jsonb)",
            job,
            json.dumps(
                {"vm": {"provision_generation": generation, "status": "provisioning"}}
            ),
        )
    first, second = await asyncio.gather(
        *(
            capture_vm_creation_request(
                db,
                job_id=str(job),
                generation=generation,
                request=build_vm_creation_request(
                    **options(
                        job_id=str(job), provision_generation=generation, memory=memory
                    ),
                    network_tier="restricted",
                ),
            )
            for memory in ("16Gi", "32Gi")
        )
    )
    assert first == second
    assert first["request"]["memory"] in {"16Gi", "32Gi"}
    assert (
        await db.capture_vm_creation_request_if_generation(
            str(job), str(uuid4()), first
        )
        is None
    )
    async with db.acquire() as conn:
        stored = await conn.fetchval(
            "SELECT context->'vm'->'creation_request' FROM jobs WHERE id=$1", job
        )
    assert (json.loads(stored) if isinstance(stored, str) else stored) == first


@pytest.mark.asyncio
async def test_capture_can_bind_already_authenticated_controller_configuration(creator):
    provisioner, _ = creator
    snapshot = await capture_vm_creation_request(
        provisioner._db,
        job_id=JOB,
        generation=GENERATION,
        request=build_vm_creation_request(**options(), network_tier="restricted"),
        controller_configuration_digest="sha256:" + "a" * 64,
    )
    assert snapshot["controller_configuration_digest"] == "sha256:" + "a" * 64
    assert snapshot["controller_configuration_authenticated"] is True
    assert snapshot["issuance_authority_bound"] is False


@pytest.mark.asyncio
async def test_captured_replay_does_not_validate_replacement_preparation_or_initialization(
    creator,
):
    provisioner, sent = creator
    await provisioner._create_http(**options())
    await provisioner._create_http(
        **options(),
        set_provisioning=False,
        preparation={"credential": "must-not-replace-original"},
        initialization={"not-a-valid-current-recipe": True},
    )
    assert len(sent) == 2
    assert sent[1] == sent[0]


@pytest.mark.asyncio
async def test_credential_capture_failure_cannot_fall_back_to_uncaptured_post(creator):
    provisioner, sent = creator
    assert (
        await provisioner._create_http(**options(description="PASSWORD=secret"))
        is False
    )
    assert sent == []
    assert provisioner._db.snapshot is None


@pytest.mark.asyncio
async def test_legacy_poll_capture_does_not_claim_to_be_original_request(creator):
    provisioner, _ = creator
    await provisioner._create_http(**options(), set_provisioning=False)
    assert provisioner._db.snapshot["initial_request"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        {"version": True},
        {"controller_configuration_authenticated": True},
        {"controller_configuration_digest": "not-a-digest"},
        {"initial_request": "true"},
        {"issuance_authority_bound": True},
    ],
)
async def test_capture_rejects_malformed_or_unearned_stored_evidence(creator, change):
    provisioner, _ = creator
    request = build_vm_creation_request(**options(), network_tier="restricted")
    await capture_vm_creation_request(
        provisioner._db, job_id=JOB, generation=GENERATION, request=request
    )
    provisioner._db.snapshot.update(change)
    with pytest.raises(ValueError):
        await capture_vm_creation_request(
            provisioner._db, job_id=JOB, generation=GENERATION
        )


@pytest.mark.asyncio
async def test_supplied_controller_configuration_cannot_change_existing_snapshot(
    creator,
):
    provisioner, _ = creator
    await capture_vm_creation_request(
        provisioner._db,
        job_id=JOB,
        generation=GENERATION,
        request=build_vm_creation_request(**options(), network_tier="restricted"),
        controller_configuration_digest="sha256:" + "a" * 64,
    )
    with pytest.raises(ValueError):
        await capture_vm_creation_request(
            provisioner._db,
            job_id=JOB,
            generation=GENERATION,
            controller_configuration_digest="sha256:" + "b" * 64,
        )


@pytest.mark.asyncio
async def test_authenticated_reply_with_invalid_storage_attestation_is_not_observed_success(
    creator,
):
    provisioner, _ = creator

    async def invalid_attestation(path, *, json):
        body = sign_payload(
            {"status": "created", "provision_generation": GENERATION},
            direction="response",
            operation="create",
            secret=SECRET,
            correlation_id=json[AUTH_FIELD]["request_id"],
        )
        return httpx.Response(
            200, json=body, request=httpx.Request("POST", "http://controller/vms")
        )

    provisioner._http_client.post.side_effect = invalid_attestation
    assert (
        await provisioner._create_http(
            **options(), workspace_storage={"pvc_uid": "retained"}
        )
        is False
    )
    observation = provisioner._db.updates[-1]["creation_observation"]
    assert observation["outcome"] == "response_unproven"
    assert observation["authenticated"] is True
