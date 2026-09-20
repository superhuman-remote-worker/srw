"""Golden source intent travels through actual PostgreSQL effect authority."""

from copy import deepcopy
from uuid import uuid4

import pytest

from tests.test_vm_creation_golden import (
    golden as _golden_fixture,
    setup as _setup_fixture,
)
from tests.test_vm_creation_actuation_real_postgres import (
    test_real_authority_lost_vm_reply_adoption_retains_attempts_and_hold as _exercise,
)
from tests.test_vm_creation_effects_real_postgres import reserved, SECRET
from tests.test_vm_creation_retry_real_postgres import (
    db as _db_fixture,
    postgres_db_fixture,  # noqa: F401
    pg_dsn,  # noqa: F401
    _schema_applied,  # noqa: F401
)
from orchestrator.services.vm_creation_retry_store import VMCreationRetryConflict
from shared.vm_creation_issuance import verify_creation_carrier, seal_creation_carrier
from vm_controller import controller as settings

setup = _setup_fixture
golden = _golden_fixture
db = _db_fixture


@pytest.mark.asyncio
async def test_golden_source_real_authority_survives_lost_vm_reply(db, golden):
    ctrl, api, fake, payload, name = golden
    await _exercise(db, (ctrl, api, fake, payload), False)
    async with db.acquire() as conn:
        rows = await conn.fetch(
            "SELECT carrier_intent FROM vm_creation_effects ORDER BY effect_number"
        )
    import json

    sources = [json.loads(row["carrier_intent"])["rootdisk_source"] for row in rows]
    assert len(sources) == 3 and all(source == sources[0] for source in sources)
    assert sources[0]["kind"] == "golden"
    assert sources[0]["dv_uid"] == api.objects["DataVolume", name]["metadata"]["uid"]
    # Adoption happens before CDI completion. A later observation releases the
    # pin using the same durable source/root identities, without another POST.
    rootname = "agent-vm-" + payload["job_id"] + "-rootdisk"
    api.objects["DataVolume", rootname]["status"] = {"phase": "Succeeded"}
    api.objects["PersistentVolumeClaim", rootname]["status"] = {"phase": "Bound"}
    assert (await ctrl._do_create_serialized(payload))["status"] == "created"
    from vm_controller.creation_sources import pins

    assert (
        pins(api.objects["DataVolume", name])[payload["creation_retry"]["request_id"]][
            "state"
        ]
        == "released"
    )
    assert api.writes.count("DataVolume") == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["image", "namespace", "storage", "pvc_owner_dv_uid"])
async def test_real_authority_refuses_forged_semantic_golden_source(
    db, monkeypatch, field
):
    import json

    monkeypatch.setattr(settings, "VM_GOLDEN_IMAGE_ENABLED", True)
    store, row, claim, carrier = await reserved(db, monkeypatch)
    async with db.acquire() as conn:
        request = json.loads(
            await conn.fetchval(
                "SELECT canonical_request FROM vm_creation_retries WHERE request_id=$1",
                row["request_id"],
            )
        )
    image = request["vm_image"]
    dv_uid = str(uuid4())
    values = verify_creation_carrier(carrier, secret=SECRET)
    source = {
        "kind": "golden",
        "image": image,
        "namespace": settings.VM_NAMESPACE,
        "name": settings._golden_name(image),
        "dv_uid": dv_uid,
        "pvc_uid": str(uuid4()),
        "pvc_owner_dv_uid": dv_uid,
        "image_ref": image,
        "registry_source": {"registry": {"url": "docker://" + image}},
        "storage": settings.VMController._golden_dv_manifest(
            None, settings._golden_name(image), image
        )["spec"]["storage"],
        "pvc_volume_mode": "Filesystem",
    }
    changed = deepcopy(source)
    changed[field] = str(uuid4())
    values.update(version=2, rootdisk_source=changed)

    def sealed():
        return seal_creation_carrier(
            values,
            namespace=carrier["metadata"]["namespace"],
            uid=carrier["metadata"]["uid"],
            resource_version="3",
            secret=SECRET,
        )

    with pytest.raises(
        VMCreationRetryConflict, match="creation_rootdisk_source_changed"
    ):
        await store.begin_effect(
            request_id=str(row["request_id"]),
            claim_token=str(claim["claim_token"]),
            carrier=sealed(),
        )
    values["rootdisk_source"] = source
    grant = await store.begin_effect(
        request_id=str(row["request_id"]),
        claim_token=str(claim["claim_token"]),
        carrier=sealed(),
    )
    assert grant["actuation_allowed"] is True
    # Matching configuration and valid source shape do not allow a new source
    # UID at the next stage: the first effect's immutable source is authority.
    from tests.test_vm_creation_effects_real_postgres import disk_observation

    root = disk_observation(sealed())
    root["object"]["spec"] = {
        "source": {"pvc": {"name": source["name"], "namespace": source["namespace"]}},
        "storage": {"volumeMode": "Filesystem"},
    }
    await store.observe_effect(
        request_id=str(row["request_id"]), carrier=sealed(), observation=root
    )
    values.update(
        effect_kind="cloud_init",
        effect_nonce=str(uuid4()),
        object_name="agent-vm-" + str(row["job_id"]) + "-cloudinit",
        current_dv_uid=root["object"]["metadata"]["uid"],
        current_pvc_uid=root["pvc"]["metadata"]["uid"],
    )
    changed = deepcopy(source)
    changed["dv_uid"] = changed["pvc_owner_dv_uid"] = str(uuid4())
    values["rootdisk_source"] = changed
    with pytest.raises(
        VMCreationRetryConflict, match="creation_rootdisk_source_changed"
    ):
        await store.begin_effect(
            request_id=str(row["request_id"]),
            claim_token=str(claim["claim_token"]),
            carrier=sealed(),
        )
    values["rootdisk_source"] = source
    assert (
        await store.begin_effect(
            request_id=str(row["request_id"]),
            claim_token=str(claim["claim_token"]),
            carrier=sealed(),
        )
    )["actuation_allowed"] is True
