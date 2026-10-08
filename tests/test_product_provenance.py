"""Focused M2d tests for declared component provenance."""

from __future__ import annotations

import json
import re
import shutil
import subprocess
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, patch
from uuid import UUID

import pytest
import yaml

from orchestrator.services.product_capabilities import (
    ProductCapabilityService,
    ResolutionRequest,
    registered_agent_provenance,
)
from orchestrator.database.postgres import PostgresDB
from shared.runtime.core.product_capabilities import (
    ComponentProvenance,
    ProductComponent,
    ProvenanceStatus,
    REGISTRY_REVISION,
)
from shared.runtime.core.runtime_provenance import (
    build_product_provenance,
    component_provenance_from_environment,
    inherited_content_provenance,
)

_NOW = datetime(2026, 7, 27, 14, 0, tzinfo=timezone.utc)
_USER_ID = UUID("11111111-1111-1111-1111-111111111111")
_THREAD_ID = UUID("22222222-2222-2222-2222-222222222222")
_PROJECT_ID = UUID("33333333-3333-3333-3333-333333333333")
_ORCHESTRATOR_REVISION = "a" * 40
_AGENT_REVISION = "b" * 40
_SOURCE_URL = "https://github.com/superhuman-remote-worker/srw"
_DOCUMENTATION_URL = f"{_SOURCE_URL}/tree/main/docs"
_ROOT = Path(__file__).resolve().parents[1]


def _declared(revision: str) -> ComponentProvenance:
    return ComponentProvenance(
        source_revision=revision,
        provenance_status=ProvenanceStatus.DECLARED,
    )


def test_environment_declarations_are_closed_full_revision_only_and_never_verified():
    provenance = component_provenance_from_environment(
        {
            "SRW_COMPONENT": "agent",
            "SRW_SOURCE_REVISION": _AGENT_REVISION,
            "SRW_SOURCE_URL": _SOURCE_URL,
            "SRW_RELEASE_VERSION": "0.0.0-dev.sha-bbbbbbb",
            "SRW_DOCUMENTATION_URL": _DOCUMENTATION_URL,
            "SRW_AGENT_ARTIFACT_DIGEST": f"sha256:{'c' * 64}",
        },
        ProductComponent.AGENT,
        include_common=True,
    )

    assert provenance.provenance_status is ProvenanceStatus.DECLARED
    assert provenance.source_revision == _AGENT_REVISION
    assert provenance.source_url == _SOURCE_URL
    assert provenance.release_version == "0.0.0-dev.sha-bbbbbbb"
    assert provenance.documentation_url == _DOCUMENTATION_URL
    assert provenance.artifact_digest == f"sha256:{'c' * 64}"

    wrong_component = component_provenance_from_environment(
        {
            "SRW_COMPONENT": "orchestrator",
            "SRW_SOURCE_REVISION": _AGENT_REVISION,
        },
        ProductComponent.AGENT,
        include_common=True,
    )
    assert wrong_component.provenance_status is ProvenanceStatus.UNAVAILABLE

    short_tag = component_provenance_from_environment(
        {
            "SRW_AGENT_SOURCE_REVISION": "sha-bbbbbbb",
            "SRW_AGENT_ARTIFACT_DIGEST": "experimental",
        },
        ProductComponent.AGENT,
    )
    assert short_tag.provenance_status is ProvenanceStatus.UNAVAILABLE


def test_bounded_deployment_json_supplies_only_valid_component_fields():
    provenance = component_provenance_from_environment(
        {
            "SRW_DEPLOYMENT_PROVENANCE_JSON": json.dumps(
                {
                    "source_url": _SOURCE_URL,
                    "release_version": "v1.2.3",
                    "documentation_url": _DOCUMENTATION_URL,
                    "components": {
                        "workspace": {
                            "source_revision": _ORCHESTRATOR_REVISION,
                            "artifact_digest": f"sha256:{'d' * 64}",
                        }
                    },
                    "private": {"credential": "must-not-be-forwarded"},
                }
            )
        },
        ProductComponent.WORKSPACE,
    )

    dumped = provenance.model_dump_json()
    assert provenance.source_revision == _ORCHESTRATOR_REVISION
    assert provenance.artifact_digest == f"sha256:{'d' * 64}"
    assert provenance.provenance_status is ProvenanceStatus.DECLARED
    assert "credential" not in dumped
    assert "must-not-be-forwarded" not in dumped


def test_product_mixed_build_needs_two_independently_deployed_revisions():
    registry = inherited_content_provenance(
        _declared(_ORCHESTRATOR_REVISION),
        content_digest=REGISTRY_REVISION,
    )
    insufficient = build_product_provenance(
        {
            ProductComponent.ORCHESTRATOR: _declared(_ORCHESTRATOR_REVISION),
            ProductComponent.REGISTRY: registry,
        }
    )
    assert insufficient.mixed_build is None

    matching = build_product_provenance(
        {
            ProductComponent.ORCHESTRATOR: _declared(_ORCHESTRATOR_REVISION),
            ProductComponent.AGENT: _declared(_ORCHESTRATOR_REVISION),
            ProductComponent.REGISTRY: registry,
        }
    )
    assert matching.mixed_build is False

    mixed = build_product_provenance(
        {
            ProductComponent.ORCHESTRATOR: _declared(_ORCHESTRATOR_REVISION),
            ProductComponent.AGENT: _declared(_AGENT_REVISION),
            ProductComponent.REGISTRY: registry,
        }
    )
    assert mixed.mixed_build is True


@pytest.mark.parametrize(
    ("orchestrator_revision", "agent_revision"),
    [
        (_ORCHESTRATOR_REVISION, _AGENT_REVISION),
        (_AGENT_REVISION, _ORCHESTRATOR_REVISION),
    ],
)
def test_older_and_newer_agent_orchestrator_orders_both_remain_mixed(
    orchestrator_revision: str,
    agent_revision: str,
):
    product = build_product_provenance(
        {
            ProductComponent.ORCHESTRATOR: _declared(orchestrator_revision),
            ProductComponent.AGENT: _declared(agent_revision),
        }
    )

    assert product.mixed_build is True
    assert (
        product.components[ProductComponent.ORCHESTRATOR].source_revision
        == orchestrator_revision
    )
    assert product.components[ProductComponent.AGENT].source_revision == agent_revision


def test_guide_content_identity_does_not_copy_parent_artifact_digest():
    parent = ComponentProvenance(
        source_revision=_AGENT_REVISION,
        artifact_digest=f"sha256:{'c' * 64}",
        release_version="v1.2.3",
        provenance_status=ProvenanceStatus.DECLARED,
    )
    guide = inherited_content_provenance(
        parent,
        content_digest=f"sha256:{'e' * 64}",
    )

    assert guide.source_revision == _AGENT_REVISION
    assert guide.content_digest == f"sha256:{'e' * 64}"
    assert guide.artifact_digest is None
    assert guide.release_version == "v1.2.3"


@pytest.mark.asyncio
async def test_registered_agent_provenance_ignores_legacy_short_sha_and_self_verified():
    class FakeDb:
        def __init__(self, metadata: dict[str, Any]) -> None:
            self.metadata = metadata

        async def get_agent(self, _agent_id: str) -> dict[str, Any]:
            return {"status": "session", "metadata": self.metadata}

    admitted_thread = {"agent_id": "agent-1"}
    legacy = await registered_agent_provenance(
        FakeDb({"build_sha": "bbbbbbb"}),
        admitted_thread,
    )
    assert legacy.provenance_status is ProvenanceStatus.UNAVAILABLE

    verified = await registered_agent_provenance(
        FakeDb(
            {
                "product_provenance": {
                    "source_revision": _AGENT_REVISION,
                    "artifact_digest": f"sha256:{'c' * 64}",
                    "provenance_status": "verified",
                }
            }
        ),
        admitted_thread,
    )
    assert verified.provenance_status is ProvenanceStatus.UNAVAILABLE


@pytest.mark.asyncio
async def test_agent_registration_persists_full_provenance_beside_legacy_short_sha():
    with patch.dict("os.environ", {"DATABASE_URL": "postgresql://test"}):
        database = PostgresDB()
    connection = AsyncMock()
    connection.fetchrow = AsyncMock(return_value={"id": _AGENT_REVISION})

    @asynccontextmanager
    async def acquire():
        yield connection

    database.acquire = acquire
    provenance = _declared(_AGENT_REVISION).model_dump(mode="json")

    await database.register_agent(
        config_name="defaults",
        pod_ip="10.0.0.5",
        hostname=None,
        build_sha=_AGENT_REVISION[:7],
        product_provenance=provenance,
    )

    sql, *parameters = connection.fetchrow.call_args.args
    assert "INSERT INTO agents" in sql
    metadata = json.loads(parameters[7])
    process_generation = metadata.pop("dispatch_process_generation")
    assert str(UUID(process_generation)) == process_generation
    assert metadata == {
        "build_sha": _AGENT_REVISION[:7],
        "product_provenance": provenance,
    }


@pytest.mark.asyncio
async def test_agent_registration_expected_id_updates_only_that_hostname_row():
    with patch.dict("os.environ", {"DATABASE_URL": "postgresql://test"}):
        database = PostgresDB()
    expected_id = UUID("44444444-4444-4444-4444-444444444444")
    connection = AsyncMock()
    connection.fetchrow = AsyncMock(return_value={"id": expected_id})
    connection.execute = AsyncMock(side_effect=["UPDATE 0", "UPDATE 1"])

    @asynccontextmanager
    async def acquire():
        yield connection

    database.acquire = acquire

    result = await database.register_agent(
        config_name="session_base",
        pod_ip="10.0.0.5",
        hostname="agent-winner-host",
        agent_mode="persistent",
        thread_id=str(_THREAD_ID),
        expected_agent_id=str(expected_id),
    )

    select_sql, *select_parameters = connection.fetchrow.await_args.args
    assert "WHERE id = $1 AND hostname = $2" in select_sql
    assert select_parameters == [expected_id, "agent-winner-host"]
    assert connection.execute.await_count == 2
    update_sql = connection.execute.await_args_list[1].args[0]
    assert "WHERE id = $5" in update_sql
    assert connection.execute.await_args_list[1].args[5] == expected_id
    assert result["agent_id"] == str(expected_id)


@pytest.mark.asyncio
async def test_agent_registration_missing_expected_row_has_no_side_effects():
    with patch.dict("os.environ", {"DATABASE_URL": "postgresql://test"}):
        database = PostgresDB()
    connection = AsyncMock()
    connection.fetchrow = AsyncMock(return_value=None)

    @asynccontextmanager
    async def acquire():
        yield connection

    database.acquire = acquire

    with pytest.raises(
        RuntimeError,
        match="expected agent no longer matches registration hostname",
    ):
        await database.register_agent(
            config_name="session_base",
            pod_ip="10.0.0.5",
            hostname="agent-winner-host",
            agent_mode="persistent",
            thread_id=str(_THREAD_ID),
            expected_agent_id="44444444-4444-4444-4444-444444444444",
        )

    connection.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_agent_registration_lost_exact_update_fails_closed():
    with patch.dict("os.environ", {"DATABASE_URL": "postgresql://test"}):
        database = PostgresDB()
    expected_id = UUID("44444444-4444-4444-4444-444444444444")
    connection = AsyncMock()
    connection.fetchrow = AsyncMock(return_value={"id": expected_id})
    connection.execute = AsyncMock(side_effect=["UPDATE 0", "UPDATE 0"])

    @asynccontextmanager
    async def acquire():
        yield connection

    database.acquire = acquire

    with pytest.raises(
        RuntimeError,
        match="expected agent disappeared during exact registration update",
    ):
        await database.register_agent(
            config_name="session_base",
            pod_ip="10.0.0.5",
            hostname="agent-winner-host",
            agent_mode="persistent",
            thread_id=str(_THREAD_ID),
            expected_agent_id=str(expected_id),
        )


@pytest.mark.asyncio
async def test_agent_registration_insert_only_never_reuses_hostname_row():
    with patch.dict("os.environ", {"DATABASE_URL": "postgresql://test"}):
        database = PostgresDB()
    inserted_id = UUID("55555555-5555-5555-5555-555555555555")
    connection = AsyncMock()
    connection.fetchrow = AsyncMock(return_value={"id": inserted_id})

    @asynccontextmanager
    async def acquire():
        yield connection

    database.acquire = acquire

    result = await database.register_agent(
        config_name="session_base",
        pod_ip="10.0.0.5",
        hostname="non-unique-hostname",
        agent_mode="persistent",
        thread_id=str(_THREAD_ID),
        insert_only=True,
    )

    assert connection.fetchrow.await_count == 1
    sql = connection.fetchrow.await_args.args[0]
    assert "INSERT INTO agents" in sql
    assert "SELECT id FROM agents" not in sql
    connection.execute.assert_not_awaited()
    assert result["agent_id"] == str(inserted_id)


@pytest.mark.asyncio
async def test_service_reports_all_components_and_mixed_agent_orchestrator_builds():
    async def grants(_db: Any, **_kwargs: Any) -> dict[str, Any]:
        return {
            "browser": True,
            "datasource_tools": True,
            "delegation": True,
            "permission_mode": "auto_accept",
        }

    async def agent_provenance(
        _db: Any,
        _thread: dict[str, Any] | None,
    ) -> ComponentProvenance:
        return _declared(_AGENT_REVISION)

    deployment = {
        "source_url": _SOURCE_URL,
        "release_version": "v1.2.3",
        "documentation_url": _DOCUMENTATION_URL,
        "components": {
            "cockpit": {"source_revision": _ORCHESTRATOR_REVISION},
            "mcp": {"source_revision": _ORCHESTRATOR_REVISION},
            "workspace": {"source_revision": _ORCHESTRATOR_REVISION},
        },
    }
    service = ProductCapabilityService(
        object(),
        grants_resolver=grants,
        agent_provenance_resolver=agent_provenance,
        environment={
            "SRW_COMPONENT": "orchestrator",
            "SRW_SOURCE_REVISION": _ORCHESTRATOR_REVISION,
            "SRW_SOURCE_URL": _SOURCE_URL,
            "SRW_RELEASE_VERSION": "v1.2.3",
            "SRW_DOCUMENTATION_URL": _DOCUMENTATION_URL,
            "SRW_DEPLOYMENT_PROVENANCE_JSON": json.dumps(deployment),
        },
        clock=lambda: _NOW,
    )
    response = await service.resolve(
        ResolutionRequest(
            user_id=str(_USER_ID),
            is_admin=False,
            thread_id=_THREAD_ID,
            primary_project_id=_PROJECT_ID,
            capability_ids=("datasources.email",),
        ),
        admitted_thread={
            "id": str(_THREAD_ID),
            "user_id": str(_USER_ID),
            "project_id": str(_PROJECT_ID),
            "agent_id": "agent-1",
        },
    )

    assert set(response.product.components) == set(ProductComponent)
    assert response.product.release_version == "v1.2.3"
    assert response.product.mixed_build is True
    assert (
        response.product.components[ProductComponent.REGISTRY].content_digest
        == REGISTRY_REVISION
    )
    assert (
        response.product.components[ProductComponent.GUIDE].provenance_status
        is ProvenanceStatus.UNAVAILABLE
    )


def test_all_release_dockerfiles_carry_standard_declared_oci_metadata():
    dockerfiles = (
        "docker/Dockerfile.agent",
        "docker/Dockerfile.orchestrator",
        "docker/Dockerfile.cockpit",
        "docker/Dockerfile.mcp",
        "docker/Dockerfile.workspace",
        "docker/Dockerfile.vm-controller",
        "docker/agent-vm-base/Dockerfile.containerDisk",
        "docker/agent-vm-base/Dockerfile.containerDisk-stage1",
    )
    required = (
        'ARG SRW_SOURCE_REVISION=""',
        "ARG SRW_SOURCE_URL=",
        'ARG SRW_RELEASE_VERSION=""',
        "ARG SRW_DOCUMENTATION_URL=",
        'LABEL org.opencontainers.image.source="${SRW_SOURCE_URL}"',
        'LABEL org.opencontainers.image.revision="${SRW_SOURCE_REVISION}"',
        'LABEL org.opencontainers.image.version="${SRW_RELEASE_VERSION}"',
        'LABEL org.opencontainers.image.documentation="${SRW_DOCUMENTATION_URL}"',
        "LABEL io.srw.component=",
    )

    for relative_path in dockerfiles:
        text = (_ROOT / relative_path).read_text(encoding="utf-8")
        for marker in required:
            assert marker in text, f"{relative_path} lacks {marker}"


# 99223c6d moved develop.yml off push HEAD: each component is built from its own
# last-input commit, so the full-revision expression is now per-job —
# `steps.ident.outputs.full` for image labels, `COMPONENT_SHA` for the chart
# provenance block. Both carry the full 40-char sha; the 7-char image tag is
# *derived* from them (`${FULL::7}` / `${COMPONENT_SHA::7}`), never substituted
# for them. main.yml still builds everything from push HEAD and stays on
# `github.sha`. The contract this test guards is unchanged: whatever the
# dialect, the revision label gets the full sha and the short one is only a tag.
_FULL_REVISION_EXPRS = (
    "${{ github.sha }}",
    "${{ steps.ident.outputs.full }}",
)
_SHORT_SHA_EXPRS = (
    "${{ steps.short_sha.outputs.sha }}",
    "${{ steps.ident.outputs.short }}",
)


def _count_full_revision(workflow: str, key: str) -> int:
    """Occurrences of ``key=<full-revision expression>``, in either dialect."""
    return sum(workflow.count(f"{key}={expr}") for expr in _FULL_REVISION_EXPRS)


def test_image_workflows_pass_full_source_revision_separately_from_short_sha():
    main = (_ROOT / ".github/workflows/main.yml").read_text(encoding="utf-8")
    develop = (_ROOT / ".github/workflows/develop.yml").read_text(encoding="utf-8")
    stage1 = (_ROOT / ".github/workflows/stage1-rebuild.yml").read_text(
        encoding="utf-8"
    )

    assert _count_full_revision(main, "SRW_SOURCE_REVISION") >= 7
    assert _count_full_revision(develop, "SRW_SOURCE_REVISION") >= 7
    # Eight build identities, and the workspace job labels two images: the
    # full one and the minimal one it is built on.
    assert _count_full_revision(main, "org.opencontainers.image.revision") == 9
    assert _count_full_revision(develop, "org.opencontainers.image.revision") == 9
    for component in (
        "agent",
        "orchestrator",
        "cockpit",
        "mcp",
        "workspace",
        "vm-controller",
        "vm-preparer",
        "driver-shim",
    ):
        assert f"io.srw.component={component}" in main
        assert f"io.srw.component={component}" in develop
    assert '--build-arg "SRW_SOURCE_REVISION=${GITHUB_SHA}"' in main
    assert '--build-arg "SRW_SOURCE_REVISION=${GITHUB_SHA}"' in develop
    assert "REF_FULL_SHA=$(git rev-parse HEAD)" in stage1
    assert '--build-arg "SRW_SOURCE_REVISION=${REF_FULL_SHA}"' in stage1
    assert "BUILD_SHA=${{ steps.short_sha.outputs.sha }}" in main
    assert "BUILD_SHA=${{ steps.ident.outputs.short }}" in develop

    # The short sha is a tag, never a revision — in either workflow's dialect.
    for expr in _SHORT_SHA_EXPRS:
        assert f"SRW_SOURCE_REVISION={expr}" not in main
        assert f"SRW_SOURCE_REVISION={expr}" not in develop
        assert f"org.opencontainers.image.revision={expr}" not in main
        assert f"org.opencontainers.image.revision={expr}" not in develop

    # ...and it must stay *derived* from the full sha, so the two cannot drift
    # apart into an image tagged with one commit and labeled with another.
    # Eight build identities: the five services, controller, disk builder and
    # the connector driver shim.
    assert develop.count("short=${FULL::7}") == 8
    # The chart-stamping step derives every baked tag from the same identity
    # sha whose full form ships as that component's provenance revision —
    # eight including the ephemeral preparation builder and the driver shim.
    assert len(re.findall(r'="sha-\$\{SHA_[A-Z]+::7\}"', develop)) == 8

    assert (
        ".provenance.components[strenv(component)].sourceRevision = strenv(GITHUB_SHA)"
    ) in main
    assert len(re.findall(r"sourceRevision\s*= strenv\(REV_[A-Z]+\)", develop)) == 6


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm is unavailable")
def test_helm_digest_pin_drives_both_image_reference_and_artifact_declaration():
    digest = f"sha256:{'c' * 64}"
    revision = "d" * 40
    command = [
        "helm",
        "template",
        "srw-provenance-test",
        str(_ROOT / "helm"),
        "-f",
        str(_ROOT / "helm/ci/test-values.yaml"),
        "--set-string",
        f"image.agent.digest={digest}",
        "--set-string",
        f"provenance.components.agent.sourceRevision={revision}",
        "--set-string",
        "provenance.components.agent.releaseVersion=v1.2.3",
        "--show-only",
        "templates/configmap.yaml",
    ]
    rendered = subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    document = yaml.safe_load(rendered)
    data = document["data"]
    deployment = json.loads(data["SRW_DEPLOYMENT_PROVENANCE_JSON"])

    assert data["AGENT_IMAGE"].endswith(f"@{digest}")
    assert data["PERSISTENT_AGENT_IMAGE"].endswith(f"@{digest}")
    assert deployment["components"]["agent"] == {
        "artifact_digest": digest,
        "release_version": "v1.2.3",
        "source_revision": revision,
    }
    assert deployment["components"]["orchestrator"]["artifact_digest"] == ""


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm is unavailable")
def test_helm_rejects_tags_and_short_values_in_digest_and_revision_fields():
    result = subprocess.run(
        [
            "helm",
            "lint",
            str(_ROOT / "helm"),
            "-f",
            str(_ROOT / "helm/ci/test-values.yaml"),
            "--set-string",
            "image.agent.digest=sha-deadbee",
            "--set-string",
            "provenance.components.agent.sourceRevision=deadbee",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    assert "values don't meet the specifications of the schema" in (
        result.stdout + result.stderr
    )


def test_env_example_provenance_is_explicit_and_empty_by_default():
    """The public env template must document the variable and ship it EMPTY.

    Provenance is an attestation: a non-empty default would advertise a
    source revision / image digest the running artifact does not actually
    carry. The chart-side wiring is covered by the two helm tests above.
    """
    environment = (_ROOT / ".env.example").read_text(encoding="utf-8")

    assert "SRW_DEPLOYMENT_PROVENANCE_JSON=\n" in environment
    assert "leave this empty" in environment
