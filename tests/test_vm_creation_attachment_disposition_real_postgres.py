"""Exact cancellation markers fence delayed attachment writes and lost replies."""

from copy import deepcopy

import pytest

from tests.test_vm_creation_disposition_instance_guard_real_postgres import (
    db as _db_fixture,
    setup as _setup_fixture,
    attached as _attached_fixture,
    postgres_db_fixture,  # noqa: F401
    pg_dsn,  # noqa: F401
    _schema_applied,  # noqa: F401
    frozen_attachment,
)
from shared.vm_creation_disposition import disposition_identity
from shared.vm_workspace_storage import storage_name
from vm_controller.creation_disposition import CreationDisposer


db, setup, attached = _db_fixture, _setup_fixture, _attached_fixture


def complete_scans(ctrl):
    ctrl.k8s_client.list_namespaced_custom_object = lambda **_: {
        "metadata": {"resourceVersion": "1"},
        "items": [],
    }
    ctrl.core_api.list_namespaced_pod = lambda **_: {
        "metadata": {"resourceVersion": "1"},
        "items": [],
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("lost_reply", [False, True])
async def test_empty_observed_attachment_has_exact_release_tombstone_and_stale_cas_fence(
    db, attached, lost_reply
):
    ctrl, api, _, payload = attached
    service, row, _, disposition = await frozen_attachment(db, attached)
    complete_scans(ctrl)
    name = storage_name(payload["workspace_storage"])
    before = api.read("Lease", name)
    method = ctrl.coordination_api.replace_namespaced_lease

    def replace(**kwargs):
        result = method(**kwargs)
        if lost_reply and kwargs["name"] == name:
            raise TimeoutError("cancellation CAS reply lost")
        return result

    ctrl.coordination_api.replace_namespaced_lease = replace
    await CreationDisposer(ctrl)._run(disposition_identity(row), {})
    current = await service.retries.inspect(request_id=str(row["request_id"]))
    proof = current["cancellation_completion"]["workspace_attachment"]
    assert proof["outcome"] == "released"
    assert proof["lease"]["uid"] == before["metadata"]["uid"]
    after = api.read("Lease", name)
    assert after["metadata"]["annotations"]["srw.io/released"] == "true"
    assert after["metadata"]["annotations"]["srw.io/detached"] == "false"
    assert proof["lease"]["resource_version"] == after["metadata"]["resourceVersion"]
    assert current["state"] == "settled"
    writes = deepcopy(api.writes)
    await CreationDisposer(ctrl)._run(disposition_identity(row), {})
    assert api.writes == writes
    from kubernetes.client.exceptions import ApiException

    with pytest.raises(ApiException):
        method(name=name, namespace=before["metadata"]["namespace"], body=before)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["uid", "resourceVersion", "execution"])
async def test_observed_attachment_identity_change_refuses_cancellation_write(
    db, attached, change
):
    from uuid import uuid4

    ctrl, api, _, payload = attached
    service, row, _, _ = await frozen_attachment(db, attached)
    complete_scans(ctrl)
    name = storage_name(payload["workspace_storage"])
    metadata = api.objects["Lease", name]["metadata"]
    if change == "execution":
        metadata["labels"]["srw.io/workspace-execution"] = str(uuid4())
    else:
        metadata[change] = str(uuid4()) if change == "uid" else "changed-revision"
    before = api.read("Lease", name)
    result = await CreationDisposer(ctrl).run(disposition_identity(row))
    assert result["status"] != "creation_disposed"
    assert api.read("Lease", name) == before
    assert (
        "workspace_attachment"
        not in (await service.retries.inspect(request_id=str(row["request_id"])))[
            "cancellation_completion"
        ]
    )
