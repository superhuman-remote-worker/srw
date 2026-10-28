"""Installation upgrades do not invalidate Experts using the shipped harness."""

from copy import deepcopy
import json
from uuid import UUID, uuid4

from fastapi import HTTPException
import pytest

from orchestrator.services.manifest_experts import expert_manifest
from orchestrator.services.manifest_runtime_ownership import (
    custom_srw_image,
    require_srw_expert_configuration,
    require_srw_launch_configuration,
)
from orchestrator.services.manifest_session_delivery import prepare_delivery_snapshot
from shared.manifests import ManifestError, preview_documents, validate_documents
from tests import test_manifest_native_full_schema as full_schema

actor = full_schema.actor
database = full_schema.database
postgres_url = full_schema.postgres_url


def installed_expert():
    return expert_manifest(
        {
            "id": uuid4(),
            "name": "installed-helper",
            "display_name": "Installed Helper",
            "icon": "code",
            "color": "#89b4fa",
            "expert_type": "worker",
            "owner_id": uuid4(),
            "config": {"llm": {"model": "gpt-4o"}},
        }
    )


@pytest.mark.parametrize(
    "runtime",
    [
        {},
        {"config": {}},
        {"adapter": None},
        {"adapter": "unknown"},
        {"adapter": "srw/v1", "image": None},
        {"adapter": "srw/v1", "image": ""},
    ],
)
def test_only_explicit_srw_adapter_can_omit_image(runtime):
    document = installed_expert()
    document["spec"]["runtime"] = runtime
    with pytest.raises(ManifestError):
        validate_documents([document])


def test_installed_binding_survives_preview_and_legacy_editor_without_image_copy():
    document = installed_expert()
    runtime = document["spec"]["runtime"]
    assert "image" not in runtime
    assert (
        "image" not in preview_documents([document])["resolved"][0]["spec"]["runtime"]
    )
    for image in ("installed:before", "installed:after"):
        require_srw_launch_configuration(runtime, trusted_image=image)
    row = {"name": "installed-helper", "expert_type": "worker", "config": {}}
    assert "image" not in expert_manifest(row, existing=document)["spec"]["runtime"]
    document["spec"]["runtime"]["image"] = "installed:before"
    edited = expert_manifest(row, existing=document)
    assert edited["spec"]["runtime"]["image"] == "installed:before"
    with pytest.raises(HTTPException) as denied:
        require_srw_launch_configuration(
            edited["spec"]["runtime"], trusted_image="installed:after"
        )
    assert denied.value.status_code == 422


@pytest.mark.parametrize("image", [None, "", " "])
def test_installed_binding_requires_configured_runtime(image):
    with pytest.raises(HTTPException) as denied:
        require_srw_launch_configuration({"adapter": "srw/v1"}, trusted_image=image)
    assert denied.value.status_code == 503


def test_historical_session_and_roster_admission_record_current_installed_image():
    expert = {"harness_adapter": "srw/v1", "manifest": installed_expert()}
    original = deepcopy(expert)
    for image in ("installed:before", "installed:after"):
        snapshot = prepare_delivery_snapshot(
            {"id": uuid4(), "user_id": uuid4()},
            {},
            {"agent": {"workspace": {"backend": "none"}}},
            {},
            image=image,
            config_name="session_base",
            expert=expert,
            refs={"helper": expert},
        )
        runtime = snapshot["resolved"]["spec"]["execution"]["expert"]["inline"][
            "runtime"
        ]
        assert runtime["image"] == image
    assert expert == original


@pytest.mark.asyncio
async def test_native_rollout_records_new_image_without_replaying_prior_job(
    database, actor
):
    document = full_schema.assignment(adapter="srw/v1", mode="Reported")
    del document["spec"]["execution"]["expert"]["inline"]["runtime"]["image"]
    _, _, resources, _, first_id, before = await full_schema.admit(
        database, actor, document
    )
    first_image = database.manifest_runtime_image
    database.manifest_runtime_image = "test.invalid/srw:rebuilt"
    repeated = await resources.apply(json.dumps(document), actor, format="json")
    assert next(iter(repeated["executions"].values())) == first_id
    assert await full_schema.read_execution(database, "Job", first_id) == before

    new_document = deepcopy(document)
    new_document["metadata"]["name"] = "after-rollout"
    _, _, _, _, second_id, after = await full_schema.admit(
        database, actor, new_document
    )
    for snapshot, image in [
        (before, first_image),
        (after, database.manifest_runtime_image),
    ]:
        assert (
            "image"
            not in snapshot["document"]["spec"]["execution"]["expert"]["inline"][
                "runtime"
            ]
        )
        assert (
            snapshot["resolved"]["spec"]["execution"]["expert"]["inline"]["runtime"][
                "image"
            ]
            == image
        )
    assert second_id != first_id
    assert await full_schema.read_execution(database, "Job", first_id) == before

    pinned = deepcopy(document)
    pinned["metadata"]["name"] = "stale-explicit-image"
    pinned["spec"]["execution"]["expert"]["inline"]["runtime"]["image"] = first_image
    with pytest.raises(HTTPException) as denied:
        await full_schema.admit(database, actor, pinned)
    assert denied.value.status_code == 422
    assert await database.fetchval("SELECT count(*) FROM jobs") == 2
    assert await database.fetchval("SELECT count(*) FROM srw_resources") == 2
    assert await database.fetchval("SELECT count(*) FROM srw_execution_specs") == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("work_kind", ["Job", "Session"])
@pytest.mark.parametrize("in_project", [False, True])
async def test_saved_expert_admission_survives_rollout(
    database, actor, work_kind, in_project
):
    document = installed_expert()
    document["metadata"]["scope"]["name"] = str(actor["id"])
    if work_kind == "Session":
        document["metadata"]["annotations"]["srw.io/expert-type"] = "session"
        document["spec"]["runtime"]["config"]["config_name"] = "session_base"
    resources = full_schema.ManifestResourceService(database)
    result = await resources.apply(json.dumps(document), actor, format="json")
    resource_id = UUID(result["resources"][0]["uid"])
    expert_id = await database.fetchval(
        "SELECT linked_id FROM srw_resources WHERE id=$1", resource_id
    )
    project_id = None
    if in_project:
        project = {
            "apiVersion": "srw/v1alpha1",
            "kind": "Project",
            "metadata": {"name": "installed-team"},
            "spec": {
                "resources": {
                    "experts": {
                        "helper": {
                            "ref": {
                                "name": document["metadata"]["name"],
                                "scope": document["metadata"]["scope"],
                            }
                        }
                    }
                },
                "defaults": {
                    "sessionExpert" if work_kind == "Session" else "expert": "helper"
                },
            },
        }
        saved = await resources.apply(json.dumps(project), actor, format="json")
        project_resource_id = UUID(saved["resources"][0]["uid"])
        project_before = await database.fetchrow(
            "SELECT * FROM srw_resources WHERE id=$1", project_resource_id
        )
        project_id = str(project_before["linked_id"])

    async def create():
        if work_kind == "Session":
            return await database.create_thread(
                user_id=str(actor["id"]),
                authority_user_id=str(actor["id"]),
                authority_project_ids=[project_id] if project_id else [],
                project_id=project_id,
                initial_metadata={
                    "expert_id": str(expert_id),
                    "config_override": {"workspace": {"backend": "none"}},
                },
            )
        job = await database.create_job(
            description="Installed Expert rollout",
            user_id=str(actor["id"]),
            authority_user_id=str(actor["id"]),
            authority_project_ids=[project_id] if project_id else [],
            project_id=project_id,
            expert_id=str(expert_id),
            config_override={"workspace": {"backend": "none"}},
        )
        return str(job["id"])

    first_id = await create()
    before = await full_schema.read_execution(database, work_kind, first_id)
    database.manifest_runtime_image = "test.invalid/srw:rebuilt"
    second_id = await create()
    after = await full_schema.read_execution(database, work_kind, second_id)
    assert (
        after["resolved"]["spec"]["execution"]["expert"]["inline"]["runtime"]["image"]
        == database.manifest_runtime_image
    )
    assert await full_schema.read_execution(database, work_kind, first_id) == before
    if in_project:
        assert any(
            dependency.get("uid") == str(project_resource_id)
            and dependency.get("revision") == project_before["active_revision"]
            for dependency in after["dependencies"]
        )
        assert (
            await database.fetchrow(
                "SELECT * FROM srw_resources WHERE id=$1", project_resource_id
            )
            == project_before
        )
    assert (
        await database.fetchval(
            "SELECT resource_version FROM srw_resources WHERE id=$1", resource_id
        )
        == 1
    )


CUSTOM = "registry.example/cc@sha256:" + "a" * 64
APPROVED = ({"name": "cc", "image": CUSTOM},)


def test_an_approved_image_passes_the_launch_rule():
    private = require_srw_launch_configuration(
        {"adapter": "srw/v1", "image": CUSTOM},
        trusted_image="installed:1",
        approved_images=APPROVED,
    )
    assert private == {}


def test_an_unapproved_image_is_still_refused():
    with pytest.raises(HTTPException) as denied:
        require_srw_launch_configuration(
            {"adapter": "srw/v1", "image": CUSTOM}, trusted_image="installed:1"
        )
    assert (
        denied.value.status_code == 422 and "installed harness" in denied.value.detail
    )


def test_a_session_on_a_custom_image_expert_is_refused_as_jobs_only():
    expert = {
        "harness_adapter": "srw/v1",
        "manifest": {"spec": {"runtime": {"adapter": "srw/v1", "image": CUSTOM}}},
    }
    with pytest.raises(HTTPException) as denied:
        require_srw_expert_configuration(
            expert,
            interactive=True,
            trusted_image="installed:1",
            approved_images=APPROVED,
        )
    assert denied.value.status_code == 422 and "jobs only" in denied.value.detail


def test_custom_srw_image_is_none_for_the_installed_or_omitted_image():
    assert custom_srw_image({"adapter": "srw/v1"}, trusted_image="installed:1") is None
    assert (
        custom_srw_image({"image": "installed:1"}, trusted_image="installed:1") is None
    )
    assert custom_srw_image({"image": CUSTOM}, trusted_image="installed:1") == CUSTOM
