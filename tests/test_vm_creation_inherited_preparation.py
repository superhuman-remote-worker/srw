"""Full original clone semantics must survive prepared workspace inheritance."""

from copy import deepcopy
from uuid import uuid4

import pytest

from tests.test_vm_creation_prepared_attachment import (
    setup as _setup_fixture,
    prepared as _prepared_fixture,
    workspace as _workspace_fixture,
)
from tests.test_vm_creation_prepared_actuation import finish_prepared
from shared.workspace_preparation import preparation_request

setup, prepared, workspace = _setup_fixture, _prepared_fixture, _workspace_fixture


async def origin_and_request(workspace):
    _, _, authority, payload, _ = workspace
    assert (await finish_prepared(workspace))["status"] == "created"
    effect = next(
        e
        for e in authority.row["effects"]
        if e["carrier_intent"]["effect_kind"] == "rootdisk"
    )
    origin = {
        "request_id": authority.row["request_id"],
        "effect_nonce": effect["carrier_intent"]["effect_nonce"],
        "request": deepcopy(authority.row["request"]),
        "configuration": deepcopy(authority.row["controller_configuration"]),
        "source": deepcopy(effect["carrier_intent"]["rootdisk_source"]),
        "root": deepcopy(effect["evidence"]),
    }
    request = deepcopy(origin["request"])
    request["job_id"], request["provision_generation"] = str(uuid4()), str(uuid4())
    request["workspace_storage"].update(generation=2, pvc_uid=origin["root"]["pvc_uid"])
    old = request["preparation"]
    request["preparation"] = preparation_request(
        {
            "image": old["image"],
            "prepare": old["steps"],
            "cache": old["cache"],
            "pullPolicy": old["pullPolicy"],
        },
        scope_kind=old["scope"]["kind"],
        scope_uid=old["scope"]["uid"],
        allocation_id=request["job_id"],
        owner_kind="job",
    )
    return origin, request


@pytest.mark.asyncio
async def test_inherited_preparation_preserves_original_clone_receipt(workspace):
    from shared.vm_inherited_preparation import retained_prepared_source
    from shared.vm_creation_issuance import validate_rootdisk_source

    origin, request = await origin_and_request(workspace)
    source = retained_prepared_source(origin)
    validate_rootdisk_source(
        source,
        request=request,
        configuration=origin["configuration"],
        expected_pvc_uid=origin["root"]["pvc_uid"],
    )
    assert source["receipt"] == origin["source"]["receipt"]
    assert source["retained_root"]["pvc_uid"] != source["receipt"]["pvcUid"]
    assert source["allocation"]["request"]["allocationId"] != request["job_id"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        "steps",
        "scope",
        "image",
        "cache",
        "pull",
        "builder",
        "network",
        "target_pvc",
        "source_receipt",
    ],
)
async def test_inherited_preparation_refuses_semantic_or_identity_drift(
    workspace, change
):
    from shared.vm_inherited_preparation import retained_prepared_source
    from shared.vm_creation_issuance import validate_rootdisk_source
    from shared.workspace_preparation import revision

    origin, request = await origin_and_request(workspace)
    config = deepcopy(origin["configuration"])
    if change in {"steps", "scope", "image", "cache", "pull"}:
        prep = request["preparation"]
        if change == "steps":
            prep["steps"] = []
        elif change == "scope":
            prep["scope"]["uid"] = str(uuid4())
        elif change == "image":
            prep["image"] = request["vm_image"] = "docker.io/library/other:latest"
        elif change == "cache":
            prep["cache"] = "Rebuild"
        else:
            prep["pullPolicy"] = "Always"
        prep["revision"] = revision({k: v for k, v in prep.items() if k != "revision"})
    elif change == "builder":
        config["preparation"]["builder_image"] = "docker.io/library/other:latest"
    elif change == "network":
        config["preparation"]["network_policy_revision"] = "different"
    source = retained_prepared_source(origin)
    if change == "target_pvc":
        source["retained_root"]["pvc_uid"] = str(uuid4())
    elif change == "source_receipt":
        source["receipt"]["pvcUid"] = origin["root"]["pvc_uid"]
    with pytest.raises(ValueError):
        from shared.vm_inherited_preparation import validate_inherited_preparation

        validate_inherited_preparation(origin, request=request, configuration=config)
        validate_rootdisk_source(
            source,
            request=request,
            configuration=config,
            expected_pvc_uid=origin["root"]["pvc_uid"],
        )


@pytest.mark.asyncio
async def test_carrier_does_not_duplicate_original_canonical_request(workspace):
    import json
    from shared.vm_inherited_preparation import retained_prepared_source

    origin, _ = await origin_and_request(workspace)
    before = retained_prepared_source(origin)
    origin["request"]["description"] = "d" * (256 * 1024)
    after = retained_prepared_source(origin)
    assert after == before
    assert after["inherited_origin"] == {
        "request_id": origin["request_id"],
        "effect_nonce": origin["effect_nonce"],
    }
    assert len(json.dumps(after)) < 20_000
