"""After a sidecar Pod is created: what attach, End and the owner see (D7).

* the cloud payload of a Pod with a recorded plan is the sidecar payload,
  never an in-workspace mount, whatever the thread's rows say now;
* an agent image from before the in-pod plane gets no cloud payload it
  would misread (no in-workspace mount, sync or legacy session folder, and
  no degraded flag that would stop a stateless turn), and the state says so;
* the mount state is checked against the plan and the closed sets, merged
  only into the record of the same plan, and never shows the plan's remotes
  to the owner.
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import pytest

from orchestrator.services import agent_cloud_mounts, cloud_mount_status
from orchestrator.services.cloud_mount_plan import CloudMountPlan, SidecarMount
from orchestrator.services.cloud_mount_sidecar import PLAN_CONTEXT_KEY
from orchestrator.services.thread_projection import redact_thread_metadata
from tests import test_persistent_recycler_real_postgres as authority

db = authority.db
pg_dsn = authority.pg_dsn
_schema_applied = authority._schema_applied

THREAD_ID = "88888888-8888-4888-8888-888888888888"
POD_UID = "99999999-9999-4999-8999-999999999999"


def _plan() -> CloudMountPlan:
    mount = SidecarMount(
        index=0,
        name="project",
        mount_id="row-project",
        mount_kind="project",
        source_ref="project-1",
        backend="nextcloud",
        access="read_write",
        source_type="webdav",
        source_config=(
            ("url", "http://srw-nextcloud/remote.php/dav/files/agent/project/"),
            ("user", "agent-service"),
        ),
        root="",
        flags=(),
    )
    return CloudMountPlan(
        mounts=(mount,),
        excluded=(
            {"source_ref": "row-x", "mount_kind": "project", "reason": "set_fallback"},
        ),
        drain_seconds=60,
        cache_size="10Gi",
        passwords={0: "secret"},
    )


def _metadata(status: str = "ready", incarnation: str = POD_UID) -> dict:
    return {
        "workspace_container": {
            "status": status,
            "pod_ip": "10.0.0.9",
            "_runtime_incarnation": POD_UID,
            PLAN_CONTEXT_KEY: {
                **_plan().recorded(),
                "runtime_incarnation": incarnation,
            },
        }
    }


def _deps() -> agent_cloud_mounts.AgentCloudMountDependencies:
    return agent_cloud_mounts.AgentCloudMountDependencies(
        store=SimpleNamespace(),
        cloud_router=SimpleNamespace(),
        cloud_tasks=SimpleNamespace(),
        is_protected_cloud_mode_enabled=lambda: True,
        cloud_workspace_driver=lambda: "rclone_mount",
        slugify_mount_name=lambda name: name,
    )


# --------------------------------------------------------------------------- #
# The attach payload
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_a_sidecar_pod_gets_the_sidecar_payload_whatever_the_rows_say(
    monkeypatch,
):
    rows_builder = AsyncMock()
    monkeypatch.setattr(agent_cloud_mounts, "_resolve_live_mount_set", rows_builder)
    payload = await agent_cloud_mounts._build_agent_cloud_mount(
        {"id": THREAD_ID},
        mount_rows=[{"backend_id": "nextcloud", "cloud_handle": "h"}],
        metadata=_metadata(),
        dependencies=_deps(),
    )
    assert payload["delivery"] == "sidecar"
    assert [m["workspace_name"] for m in payload["mounts"]] == ["project"]
    assert "secret" not in json.dumps(payload)
    rows_builder.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_sidecar_pod_not_ready_gets_nothing_and_another_pods_plan_is_ignored(
    monkeypatch,
):
    assert (
        await agent_cloud_mounts._build_agent_cloud_mount(
            {"id": THREAD_ID},
            mount_rows=[],
            metadata=_metadata(status="creating"),
            dependencies=_deps(),
        )
        is None
    )
    # A plan recorded for an earlier Pod: this Pod is on the in-workspace path.
    monkeypatch.setattr(
        agent_cloud_mounts,
        "_runtime_supports_rclone_mount",
        lambda metadata, dependencies: True,
    )
    monkeypatch.setattr(
        agent_cloud_mounts, "container_denies_fuse", AsyncMock(return_value=False)
    )
    live = agent_cloud_mounts.LiveMountSet(
        mounts=[{"workspace_name": "x"}], fallback=False, excluded=[]
    )
    monkeypatch.setattr(
        agent_cloud_mounts, "_resolve_live_mount_set", AsyncMock(return_value=live)
    )
    payload = await agent_cloud_mounts._build_agent_cloud_mount(
        {"id": THREAD_ID},
        mount_rows=[],
        metadata=_metadata(incarnation="00000000-0000-4000-8000-000000000000"),
        dependencies=_deps(),
    )
    assert payload["driver"] == "rclone" and "delivery" not in payload


@pytest.mark.asyncio
async def test_end_reconstructs_the_sidecar_payload_only_for_the_exact_retiring_runtime(
    monkeypatch,
):
    monkeypatch.setattr(
        agent_cloud_mounts,
        "_runtime_supports_terminal_rclone_retirement",
        lambda *args, **kwargs: False,
    )
    assert (
        await agent_cloud_mounts._build_agent_cloud_mount(
            {"id": THREAD_ID},
            mount_rows=[],
            metadata=_metadata(),
            terminal_retirement_token=7,
            dependencies=_deps(),
        )
        is None
    )
    monkeypatch.setattr(
        agent_cloud_mounts,
        "_runtime_supports_terminal_rclone_retirement",
        lambda *args, **kwargs: True,
    )
    payload = await agent_cloud_mounts._build_agent_cloud_mount(
        {"id": THREAD_ID},
        mount_rows=[],
        metadata=_metadata(),
        terminal_retirement_token=7,
        dependencies=_deps(),
    )
    assert payload["delivery"] == "sidecar"


# --------------------------------------------------------------------------- #
# A protected sidecar Pod (Phase B)
# --------------------------------------------------------------------------- #

GRANT = {
    "id": "aaaaaaaa-0000-4000-8000-000000000001",
    "runtime_generation": "66666666-6666-4666-8666-666666666666",
    "engage_attempt": "bbbbbbbb-0000-4000-8000-000000000002",
    "reader_id": "srw-reader-u",
}


def _protected_plan() -> CloudMountPlan:
    mount = SidecarMount(
        index=0,
        name="lower",
        mount_id=f"protected-{THREAD_ID}",
        mount_kind="protected_lower",
        source_ref=None,
        backend="nextcloud",
        access="read_only",
        source_type="webdav",
        source_config=(
            ("url", "https://nc.internal/remote.php/dav/files/srw-reader-u/"),
            ("user", "srw-reader-u"),
        ),
        root="",
        flags=(),
    )
    return CloudMountPlan(
        mounts=(mount,),
        excluded=(),
        drain_seconds=60,
        cache_size="10Gi",
        passwords={0: "reader-secret"},
        protected=True,
        overlay={
            "lower": "/cloud/lower",
            "upper": "/home/agent-host/.overlay/upper",
            "work": "/home/agent-host/.overlay/work",
            "merged": "/cloud/merged",
            "quota_bytes": 8 * 1024**3,
        },
        protected_grant=GRANT,
    )


def _protected_metadata() -> dict:
    return {
        "protected_cloud": True,
        "workspace_container": {
            "status": "ready",
            "pod_ip": "10.0.0.9",
            "_runtime_incarnation": POD_UID,
            PLAN_CONTEXT_KEY: {
                **_protected_plan().recorded(),
                "runtime_incarnation": POD_UID,
            },
        },
    }


@pytest.mark.asyncio
async def test_a_protected_sidecar_pod_gets_its_lower_and_overlay_without_a_credential(
    monkeypatch,
):
    resolver = AsyncMock()
    monkeypatch.setattr(agent_cloud_mounts, "_resolve_protected_grant", resolver)
    payload = await agent_cloud_mounts._build_agent_cloud_mount(
        {"id": THREAD_ID},
        mount_rows=[],
        metadata=_protected_metadata(),
        dependencies=_deps(),
    )
    assert payload["delivery"] == "sidecar" and payload["protected"] is True
    assert payload["skip_workspace_links"] is True
    assert payload["overlay"]["merged"] == "/cloud/merged"
    assert payload["mounts"] == [
        {
            "index": 0,
            "mount_id": f"protected-{THREAD_ID}",
            "mount_kind": "protected_lower",
            "target_path": "/cloud/lower",
            "workspace_name": "lower",
            "access": "read_only",
        }
    ]
    text = json.dumps(payload)
    assert "reader-secret" not in text and "remote.php" not in text
    # The Pod's plan answers; no engage or row read on this path.
    resolver.assert_not_awaited()


def test_attach_accepts_a_protected_sidecar_pod_only_with_the_grant_it_holds():
    from orchestrator.services.thread_workspace_delivery import _sidecar_holds_grant

    metadata = _protected_metadata()
    row = {**GRANT, "status": "active", "credentials": "reader-secret"}
    assert _sidecar_holds_grant(metadata, row) is True
    # A re-engaged or re-minted grant, a later runtime, another reader: the
    # sidecar's credential is not that grant's.
    for key, value in (
        ("engage_attempt", "dddddddd-0000-4000-8000-000000000004"),
        ("runtime_generation", "77777777-7777-4777-8777-777777777777"),
        ("id", "eeeeeeee-0000-4000-8000-000000000005"),
        ("reader_id", "srw-reader-v"),
    ):
        assert _sidecar_holds_grant(metadata, {**row, key: value}) is False, key
    assert _sidecar_holds_grant(metadata, None) is False
    # An unprotected sidecar Pod holds no grant at all.
    assert _sidecar_holds_grant(_metadata(), row) is False
    # Nor does a Pod whose plan belongs to another incarnation.
    stale = _protected_metadata()
    stale["workspace_container"]["_runtime_incarnation"] = (
        "00000000-0000-4000-8000-000000000000"
    )
    assert _sidecar_holds_grant(stale, row) is False


# --------------------------------------------------------------------------- #
# The workspace poll, new agent and old agent
# --------------------------------------------------------------------------- #


async def _poll(header: str | None) -> dict:
    """The real delivery under the application's composition, with the cloud
    payload of a sidecar Pod."""
    import orchestrator.main as orch_main
    from orchestrator.application import preparation as preparation_composition
    from orchestrator.services import (
        dispatch_credentials,
        session_config_resolution,
        thread_mount_rows,
        thread_project_authorization,
        thread_workspace_delivery,
        workspace_tier_policy,
    )

    sidecar = agent_cloud_mounts.agent_payload(_plan().recorded())
    thread = {
        "id": THREAD_ID,
        "user_id": None,
        "project_id": None,
        "status": "active",
        "execution_lane": "stateless",
        "runtime_generation": str(uuid4()),
        "runtime_retirement_token": None,
        "nc_session_folder": 'nextcloud:{"path": "sessions/x"}',
        "metadata": {"workspace_container": {"status": "ready", "pod_ip": "10.0.0.9"}},
    }
    resources = orch_main.app.state.resources
    with (
        patch.object(
            resources.postgres_db, "get_thread", AsyncMock(return_value=thread)
        ),
        patch.object(
            resources.postgres_db, "list_thread_mounts", AsyncMock(return_value=[])
        ),
        patch.object(
            thread_mount_rows, "thread_project_ids", AsyncMock(return_value=[])
        ),
        patch.object(
            thread_project_authorization,
            "revalidate_thread_project_ids",
            AsyncMock(return_value=[]),
        ),
        patch.object(
            thread_mount_rows,
            "resolve_thread_datasources",
            AsyncMock(return_value=None),
        ),
        patch.object(
            thread_mount_rows,
            "resolve_thread_repositories",
            AsyncMock(return_value=None),
        ),
        patch.object(
            thread_workspace_delivery,
            "agent_canvas_workspace_capabilities",
            return_value=(False, False, False),
        ),
        patch.object(
            agent_cloud_mounts,
            "_build_agent_cloud_mount",
            AsyncMock(return_value=sidecar),
        ),
        patch.object(
            agent_cloud_mounts,
            "_build_agent_cloud_sync",
            return_value={"version": 2, "mounts": []},
        ),
        patch.object(
            preparation_composition,
            "cloud_workspace_driver",
            return_value="rclone_mount",
        ),
        patch.object(
            resources,
            "main_cloud_router",
            SimpleNamespace(active=SimpleNamespace(is_initialized=True)),
        ),
        patch.object(
            session_config_resolution,
            "resolve_session_config",
            AsyncMock(return_value=None),
        ),
        patch.object(
            dispatch_credentials,
            "inject_thread_dispatch_credentials",
            AsyncMock(side_effect=lambda value, **_kwargs: value),
        ),
        patch.object(
            workspace_tier_policy,
            "inject_lite_workspace_config",
            side_effect=lambda value, **_kwargs: value,
        ),
        patch(
            "orchestrator.services.thread_workspace_delivery.thread_runtime_refusal_detail",
            return_value=None,
        ),
    ):
        return await thread_workspace_delivery.agent_get_thread_workspace_locked(
            THREAD_ID,
            presented_cloud_mount_delivery=header,
            dependencies=preparation_composition.thread_workspace_delivery_dependencies(
                resources
            ),
        )


@pytest.mark.asyncio
async def test_a_new_agent_gets_the_sidecar_payload_and_nothing_else_for_the_cloud():
    response = await _poll("sidecar")
    assert response["cloud_mount_sidecar"]["delivery"] == "sidecar"
    assert response["cloud_mount"] is None
    assert response["cloud_sync"] is None
    assert response["nc_session_folder"] is None
    assert response["cloud_sync_degraded"] is False
    assert response["cloud_mount_agent_outdated"] is False


@pytest.mark.asyncio
async def test_an_older_agent_gets_no_cloud_payload_it_would_misread():
    """What an agent image from before D7 receives. Its attach code reads
    cloud_mount, cloud_sync, nc_session_folder and cloud_sync_degraded and
    ignores unknown keys: with all four empty it builds no mount manager and
    no sync coordinator, and (no degraded flag) a stateless turn is not
    refused. The folders stay usable under /cloud; the state says why the
    agent does not manage them."""
    response = await _poll(None)
    assert response["cloud_mount"] is None
    assert response["cloud_sync"] is None
    assert response["nc_session_folder"] is None
    assert response["cloud_sync_degraded"] is False
    assert response["cloud_mount_agent_outdated"] is True


@pytest.mark.asyncio
async def test_an_older_agent_is_recorded_and_the_poll_never_fails():
    merges: list[dict] = []

    class _Store:
        async def merge_thread_cloud_mount_status(self, thread_id, **kwargs):
            merges.append(kwargs)
            raise RuntimeError("database away")

    await cloud_mount_status.record_agent_outdated(
        _Store(), THREAD_ID, {"fingerprint": "f" * 64, "runtime_incarnation": POD_UID}
    )
    assert merges[0]["notice"] == "agent_outdated" and merges[0]["mounts"] == {}
    assert merges[0]["runtime_incarnation"] == POD_UID
    await cloud_mount_status.record_agent_outdated(SimpleNamespace(), THREAD_ID, {})


async def _protected_poll(*, recorded_attempt: str, header: str | None = "sidecar"):
    """The real protected pinned delivery (the harness of
    test_session_config_plumbing's reader-attempt fence) for a Pod whose
    sidecars mount the lower from the grant its plan recorded."""
    import orchestrator.main as orch_main
    from orchestrator.services import (
        container_provisioner as container_provisioner_module,
        dispatch_credentials,
        protected_cloud_engage,
        session_config_resolution,
        thread_mount_rows,
        thread_project_authorization,
        thread_workspace_delivery,
        workspace_tier_policy,
    )
    from orchestrator.services.cloud.protected_reader_authority import (
        ProtectedNextcloudReaderGrantPlan,
    )
    from orchestrator.services.cloud_staging.source_identity import (
        ProtectedMountSourceIdentity,
    )
    from tests import _b09_control_seams as control_seams

    thread_id = "00000000-0000-4000-8000-0000000000c3"
    agent_id = "00000000-0000-4000-8000-0000000000a1"
    generation = "00000000-0000-4000-8000-0000000000d4"
    attach_token = "00000000-0000-4000-8000-0000000000e5"
    attempt = "00000000-0000-4000-8000-0000000000f1"
    backend_instance_id = "00000000-0000-4000-8000-000000000061"
    mount_rows = [
        {
            "id": "00000000-0000-4000-8000-000000000071",
            "mount_kind": "project",
            "backend_id": "nextcloud",
            "backend_instance_id": backend_instance_id,
            "source_kind": "project_folder",
            "source_ref": "00000000-0000-4000-8000-000000000062",
            "target_path": "projects/proj",
            "cloud_handle": (
                '{"backend":"nextcloud","native_id":"42",'
                '"vendor_meta":{"mountpoint":"Proj"}}'
            ),
        }
    ]
    source = ProtectedMountSourceIdentity.from_mount_row(mount_rows[0])
    grant_plan = ProtectedNextcloudReaderGrantPlan(
        engage_attempt=attempt,
        backend_instance_id=backend_instance_id,
        source=source,
    )
    ro_row = {
        "id": "00000000-0000-4000-8000-000000000081",
        "selected_mount_id": mount_rows[0]["id"],
        "thread_id": thread_id,
        "user_id": "user-1",
        "backend": "nextcloud",
        "backend_instance_id": backend_instance_id,
        "reader_id": grant_plan.reader_id,
        "grant_group_id": grant_plan.group_id,
        "grant_handle": grant_plan.grant_handle,
        "grant_handle_sha256": grant_plan.grant_handle_sha256,
        "source_binding": source.binding,
        "source_binding_sha256": source.sha256,
        "credentials": "credential-a1-sentinel",
        "webdav_url": (
            "https://nc.internal/remote.php/dav/files/"
            f"{grant_plan.reader_id}/{grant_plan.mountpoint}/"
        ),
        "auth_kind": "basic",
        "status": "active",
        "etag_baseline": {},
        "runtime_generation": generation,
        "engage_attempt": attempt,
    }
    plan = _protected_plan()
    plan = CloudMountPlan(
        **{
            **{f: getattr(plan, f) for f in plan.__dataclass_fields__},
            "protected_grant": {
                "id": ro_row["id"],
                "runtime_generation": generation,
                "engage_attempt": recorded_attempt,
                "reader_id": grant_plan.reader_id,
            },
        }
    )
    incarnation = "00000000-0000-4000-8000-000000000092"
    workspace = {
        "status": "ready",
        "provisioner": "k8s",
        "pod_ip": "10.42.0.10",
        "pod_port": 30022,
        "_canvas_workspace_generation": "00000000-0000-4000-8000-000000000091",
        "_runtime_incarnation": incarnation,
        PLAN_CONTEXT_KEY: {**plan.recorded(), "runtime_incarnation": incarnation},
    }
    attestation = container_provisioner_module.WorkspaceRuntimeAttestation(
        backing_id="k8s-pvc:agent-workspaces:pvc-uid-a1",
        workspace_generation=workspace["_canvas_workspace_generation"],
        runtime_incarnation=incarnation,
        ssh_host_key_fingerprint="SHA256:trusted-a1",
        host="workspace-session.agent-workspaces.svc.cluster.local",
        pod_ip=workspace["pod_ip"],
        port=workspace["pod_port"],
    )
    thread = {
        "id": thread_id,
        "execution_lane": "pinned",
        "status": "created",
        "agent_id": agent_id,
        "runtime_generation": generation,
        "runtime_attach_token": attach_token,
        "runtime_retirement_token": None,
        "user_id": "user-1",
        "project_id": None,
        "metadata": {
            "protected_cloud": True,
            "config_override": {"workspace": {"backend": "sandbox"}},
            "workspace_container": workspace,
            "_workspace_binding": {
                "generation": workspace["_canvas_workspace_generation"],
                "kind": "remote",
                "backing_id": "k8s-pvc:agent-workspaces:pvc-uid-a1",
                "ssh_host_key_fingerprint": "SHA256:trusted-a1",
                "runtime_incarnation": incarnation,
            },
        },
    }
    store = orch_main.app.state.resources.postgres_db
    sidecar = agent_cloud_mounts.agent_payload(plan.recorded())
    with (
        patch.object(store, "get_thread", AsyncMock(return_value=thread)),
        patch.object(
            container_provisioner_module.container_provisioner,
            "attest_workspace_runtime",
            AsyncMock(return_value=attestation),
        ),
        patch.object(
            store, "pinned_thread_agent_is_reciprocal", AsyncMock(return_value=True)
        ),
        patch.object(
            protected_cloud_engage,
            "_protected_cloud_delivery_state",
            AsyncMock(return_value=("ready", None)),
        ),
        patch.object(store, "get_ro_mount_by_thread", AsyncMock(return_value=ro_row)),
        patch.object(store, "list_thread_mounts", AsyncMock(return_value=mount_rows)),
        patch.object(
            thread_mount_rows, "thread_project_ids", AsyncMock(return_value=[])
        ),
        patch.object(
            thread_project_authorization,
            "revalidate_thread_project_ids",
            AsyncMock(return_value=[]),
        ),
        patch.object(
            thread_workspace_delivery,
            "agent_canvas_workspace_capabilities",
            return_value=(False, False, False),
        ),
        patch.object(
            thread_mount_rows,
            "resolve_thread_datasources",
            AsyncMock(return_value=None),
        ),
        patch.object(
            agent_cloud_mounts,
            "_build_agent_cloud_mount",
            AsyncMock(return_value=sidecar),
        ),
        patch.object(agent_cloud_mounts, "_build_agent_cloud_sync", return_value=None),
        patch.object(
            dispatch_credentials,
            "inject_thread_dispatch_credentials",
            AsyncMock(return_value={"workspace": {"backend": "sandbox"}}),
        ),
        patch.object(
            session_config_resolution,
            "resolve_session_config",
            AsyncMock(return_value=None),
        ),
        patch.object(
            thread_mount_rows,
            "resolve_thread_repositories",
            AsyncMock(return_value=None),
        ),
        patch.object(
            store,
            "managed_repository_authorities_are_current",
            AsyncMock(return_value=True),
        ),
        patch.object(
            workspace_tier_policy,
            "inject_lite_workspace_config",
            side_effect=lambda value, **_kwargs: value,
        ),
    ):
        return await control_seams.agent_get_thread_workspace_locked(
            thread_id,
            presented_agent_id=agent_id,
            presented_runtime_generation=generation,
            presented_attach_token=attach_token,
            presented_cloud_mount_delivery=header,
        )


@pytest.mark.asyncio
async def test_a_protected_sidecar_pod_is_ready_without_the_reader_credential():
    response = await _protected_poll(
        recorded_attempt="00000000-0000-4000-8000-0000000000f1"
    )
    assert response["protected_cloud_state"] == "ready"
    assert response["cloud_mount"] is None
    assert response["cloud_sync"] is None and response["nc_session_folder"] is None
    sidecar = response["cloud_mount_sidecar"]
    assert sidecar["protected"] is True and sidecar["delivery"] == "sidecar"
    assert "credential-a1-sentinel" not in json.dumps(response, default=str)


@pytest.mark.asyncio
async def test_a_protected_sidecar_pod_holding_a_replaced_grant_fails_closed():
    response = await _protected_poll(
        recorded_attempt="00000000-0000-4000-8000-0000000000f2"
    )
    assert response["protected_cloud_state"] == "failed"
    assert response["protected_cloud_error_code"] == "engage_refused"
    assert "credential-a1-sentinel" not in json.dumps(response, default=str)
    assert response.get("cloud_mount_sidecar") is None


@pytest.mark.asyncio
async def test_an_older_agent_on_a_protected_sidecar_pod_fails_closed_and_says_why():
    """An agent image from before D7 validates the protected ``cloud_mount``
    it expects and, finding none, refuses protected cloud with
    ProtectedCloudUnavailable (a WorkspaceNotReady): the session gets no cloud
    rather than a half-protected one, never a crash; the poll marks the
    agent outdated, which the state records for the cockpit."""
    from agent.api import session_contract, session_workspace

    response = await _protected_poll(
        recorded_attempt="00000000-0000-4000-8000-0000000000f1", header=None
    )
    assert response["cloud_mount_agent_outdated"] is True
    assert response["cloud_mount"] is None
    with pytest.raises(session_contract.ProtectedCloudUnavailable):
        # What the older agent runs on this response.
        session_workspace.validate_protected_cloud_mount(response["cloud_mount"])


# --------------------------------------------------------------------------- #
# The state
# --------------------------------------------------------------------------- #


def test_a_new_pods_state_is_pending_and_names_what_was_left_out():
    status = cloud_mount_status.initial_status(_plan().recorded(), POD_UID, now="t")
    assert status["mounts"] == {
        "project": {
            "mount_kind": "project",
            "target_path": "/cloud/project",
            "access": "read_write",
            "state": "pending",
            "reason": None,
            "reported_by": "orchestrator",
            "updated_at": "t",
        }
    }
    assert status["excluded"] == [
        {"source_ref": "row-x", "mount_kind": "project", "reason": "set_fallback"}
    ]
    assert "srw-nextcloud" not in json.dumps(status)


@pytest.mark.parametrize(
    "report",
    [
        [{"name": "other", "state": "mounted"}],
        [{"name": "project", "state": "broken"}],
        [{"name": "project", "state": "unavailable", "reason": "401 Unauthorized"}],
        [{"name": "project", "state": "mounted", "reason": "timeout"}],
        "not a list",
    ],
)
def test_a_report_outside_the_plan_or_the_closed_sets_is_refused(report):
    with pytest.raises(ValueError):
        cloud_mount_status.report_entries(_plan().recorded(), report)


def test_a_report_becomes_the_agents_entries():
    entries = cloud_mount_status.report_entries(
        _plan().recorded(),
        [{"name": "project", "state": "unavailable", "reason": "credential_rejected"}],
        now="t",
    )
    assert entries["project"]["state"] == "unavailable"
    assert entries["project"]["reason"] == "credential_rejected"
    assert entries["project"]["reported_by"] == "agent"


def test_the_owner_never_sees_the_plans_remotes():
    record = {"metadata": {**_metadata(), "cloud_mount_status": {"version": 1}}}
    redacted = redact_thread_metadata(record)["metadata"]
    assert PLAN_CONTEXT_KEY not in redacted["workspace_container"]
    assert redacted["cloud_mount_status"] == {"version": 1}
    assert "agent-service" not in json.dumps(redacted)


@pytest.mark.asyncio
async def test_the_state_merges_only_into_the_same_plan_on_postgres(db):
    thread_id = uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO threads (id,status,execution_lane,metadata) "
            "VALUES ($1,'active','stateless','{\"keep\": 1}'::jsonb)",
            thread_id,
        )
    recorded = _plan().recorded()
    assert await cloud_mount_status.record_pod(db, str(thread_id), recorded, POD_UID)
    entries = cloud_mount_status.report_entries(
        recorded, [{"name": "project", "state": "mounted"}]
    )

    async def status() -> dict:
        async with db.acquire() as conn:
            metadata = json.loads(
                await conn.fetchval(
                    "SELECT metadata FROM threads WHERE id=$1", thread_id
                )
            )
        assert metadata["keep"] == 1
        return metadata.get("cloud_mount_status")

    # An older agent leaves the notice; an up-to-date agent's report clears it.
    await cloud_mount_status.record_agent_outdated(
        db,
        str(thread_id),
        {"fingerprint": recorded["fingerprint"], "runtime_incarnation": POD_UID},
    )
    assert (await status())["notice"] == "agent_outdated"
    assert await cloud_mount_status.record_report(
        db,
        str(thread_id),
        fingerprint=recorded["fingerprint"],
        runtime_incarnation=POD_UID,
        entries=entries,
    )
    current = await status()
    assert current["notice"] is None
    assert current["mounts"]["project"]["state"] == "mounted"
    assert current["excluded"][0]["reason"] == "set_fallback"
    # Another plan, or another Pod of the same plan, changes nothing.
    other_pod = "00000000-0000-4000-8000-0000000000aa"
    unavailable = cloud_mount_status.report_entries(
        recorded, [{"name": "project", "state": "unavailable", "reason": "timeout"}]
    )
    assert not await cloud_mount_status.record_report(
        db,
        str(thread_id),
        fingerprint="0" * 64,
        runtime_incarnation=POD_UID,
        entries=unavailable,
    )
    assert not await cloud_mount_status.record_report(
        db,
        str(thread_id),
        fingerprint=recorded["fingerprint"],
        runtime_incarnation=other_pod,
        entries=unavailable,
    )
    assert (await status())["mounts"]["project"]["state"] == "mounted"
    assert await cloud_mount_status.record_pod(db, str(thread_id), None, POD_UID)
    async with db.acquire() as conn:
        metadata = json.loads(
            await conn.fetchval("SELECT metadata FROM threads WHERE id=$1", thread_id)
        )
    assert "cloud_mount_status" not in metadata and metadata["keep"] == 1


class _Request:
    def __init__(self, body, store) -> None:
        self._body = body
        self.headers = {}
        dependencies = SimpleNamespace(require_internal=AsyncMock(), store=store)
        self.app = SimpleNamespace(
            state=SimpleNamespace(
                thread_workspace_delivery_dependencies_factory=lambda: dependencies
            )
        )

    async def json(self):
        if isinstance(self._body, Exception):
            raise self._body
        return self._body


@pytest.mark.asyncio
async def test_the_agents_report_is_kept_only_for_this_pods_plan():
    from fastapi import HTTPException

    from orchestrator.routers.agent_thread_workspace import (
        agent_report_cloud_mount_status,
    )

    merges: list[dict] = []

    class _Store:
        async def get_thread(self, thread_id):
            return {"id": thread_id, "metadata": _metadata()}

        async def merge_thread_cloud_mount_status(self, thread_id, **kwargs):
            merges.append(kwargs)
            return True

    fingerprint = _plan().recorded()["fingerprint"]
    good = {
        "fingerprint": fingerprint,
        "pod_uid": POD_UID,
        "mounts": [{"name": "project", "state": "unavailable", "reason": "timeout"}],
    }
    assert await agent_report_cloud_mount_status(
        _Request(good, _Store()), THREAD_ID
    ) == {"ok": True}
    assert merges[0]["mounts"]["project"]["reason"] == "timeout"
    assert merges[0]["runtime_incarnation"] == POD_UID
    assert merges[0]["clear_notice"] is True
    for body, status in (
        ({**good, "fingerprint": "0" * 64}, 409),
        # An earlier Pod of the same plan: same fingerprint, another UID.
        ({**good, "pod_uid": "00000000-0000-4000-8000-0000000000aa"}, 409),
        ({k: v for k, v in good.items() if k != "pod_uid"}, 422),
        ({**good, "mounts": [{"name": "project", "state": "exploded"}]}, 422),
        ({"mounts": []}, 422),
        (ValueError("not json"), 422),
    ):
        with pytest.raises(HTTPException) as refused:
            await agent_report_cloud_mount_status(_Request(body, _Store()), THREAD_ID)
        assert refused.value.status_code == status
    assert len(merges) == 1


@pytest.mark.asyncio
async def test_a_sandbox_upgraded_to_a_vm_gets_the_vms_folders_not_the_pods(
    monkeypatch,
):
    metadata = _metadata()
    metadata["vm"] = {"status": "ready", "ssh_host": "vm.example"}
    monkeypatch.setattr(
        agent_cloud_mounts,
        "_runtime_supports_rclone_mount",
        lambda metadata, dependencies: True,
    )
    live = agent_cloud_mounts.LiveMountSet(
        mounts=[{"workspace_name": "project", "access": "read_only"}],
        fallback=False,
        excluded=[],
    )
    rows = AsyncMock(return_value=live)
    monkeypatch.setattr(agent_cloud_mounts, "_resolve_live_mount_set", rows)
    payload = await agent_cloud_mounts._build_agent_cloud_mount(
        {"id": THREAD_ID},
        mount_rows=[],
        metadata=metadata,
        dependencies=_deps(),
    )
    assert payload["driver"] == "rclone" and "delivery" not in payload
    assert rows.await_args.kwargs["runtime_is_vm"] is True


def test_the_payload_says_whether_the_pods_folders_already_settled():
    recorded = {**_plan().recorded(), "runtime_incarnation": POD_UID}
    status = cloud_mount_status.initial_status(recorded, POD_UID)
    assert agent_cloud_mounts.agent_payload(recorded, status=status)["settled"] is False
    status["mounts"]["project"]["state"] = "unavailable"
    payload = agent_cloud_mounts.agent_payload(recorded, status=status)
    assert payload["settled"] is True and payload["runtime_incarnation"] == POD_UID
    # Another Pod's record says nothing about this one.
    assert (
        agent_cloud_mounts.agent_payload(
            {**recorded, "runtime_incarnation": "00000000-0000-4000-8000-0000000000aa"},
            status=status,
        )["settled"]
        is False
    )
    # A protected payload is part of the attach identity: never volatile.
    protected = {**_protected_plan().recorded(), "runtime_incarnation": POD_UID}
    assert "settled" not in agent_cloud_mounts.agent_payload(protected, status=status)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("workspace", "plane", "recovers"),
    [
        (
            {"status": "ready", "provisioner": "k8s", "_runtime_incarnation": POD_UID},
            True,
            True,
        ),
        # Already recorded (a plan, or None for a Pod without one).
        (
            {
                "status": "ready",
                "provisioner": "k8s",
                "_runtime_incarnation": POD_UID,
                PLAN_CONTEXT_KEY: None,
            },
            True,
            False,
        ),
        (
            {
                "status": "creating",
                "provisioner": "k8s",
                "_runtime_incarnation": POD_UID,
            },
            True,
            False,
        ),
        (
            {"status": "ready", "provisioner": "k8s", "_runtime_incarnation": POD_UID},
            False,
            False,
        ),
    ],
)
async def test_attach_recovers_a_plan_the_pod_records_but_the_thread_lacks(
    monkeypatch, workspace, plane, recovers
):
    from orchestrator.services import thread_workspace_delivery

    if plane:
        monkeypatch.setenv("CONNECTOR_IN_POD_OPENER_IMAGE", "o:1")
        monkeypatch.setenv("CONNECTOR_IN_POD_RCLONE_IMAGE", "r:1")
    else:
        monkeypatch.delenv("CONNECTOR_IN_POD_OPENER_IMAGE", raising=False)
    recover = AsyncMock(return_value=True)
    store = SimpleNamespace(
        get_thread=AsyncMock(
            return_value={
                "id": THREAD_ID,
                "metadata": {"workspace_container": workspace},
            }
        )
    )
    monkeypatch.setattr(
        thread_workspace_delivery.connector_credential_leases,
        "prepare_thread_lease_delivery",
        AsyncMock(),
    )
    dependencies = SimpleNamespace(
        store=store,
        container_provisioner=SimpleNamespace(recover_cloud_mount_plan=recover),
    )
    await thread_workspace_delivery.prepare_agent_thread_workspace(
        THREAD_ID, dependencies=dependencies
    )
    assert recover.await_count == (1 if recovers else 0)
    if recovers:
        owner, incarnation = recover.await_args.args
        assert owner.id == THREAD_ID and incarnation == POD_UID
    # It never fails the poll.
    recover.side_effect = RuntimeError("apiserver away")
    await thread_workspace_delivery.prepare_agent_thread_workspace(
        THREAD_ID, dependencies=dependencies
    )


@pytest.mark.asyncio
async def test_the_in_flight_creation_digest_reads_the_real_tables(db):
    """The query a retry uses to replay its own plan runs against the real
    schema (both the pinned intent and the creation reservation)."""
    thread_id = str(uuid4())
    assert (
        await db.get_in_flight_workspace_creation_digest(thread_id, pinned=True) is None
    )
    assert (
        await db.get_in_flight_workspace_creation_digest(thread_id, pinned=False)
        is None
    )
    assert await db.get_in_flight_workspace_creation_digest("nope", pinned=True) is None


@pytest.mark.asyncio
async def test_a_pod_without_a_plan_is_read_once_and_a_vm_upgrade_drops_the_pods_state(
    monkeypatch,
):
    from orchestrator.services import thread_workspace_delivery

    monkeypatch.setenv("CONNECTOR_IN_POD_OPENER_IMAGE", "o:1")
    monkeypatch.setenv("CONNECTOR_IN_POD_RCLONE_IMAGE", "r:1")
    monkeypatch.setattr(
        thread_workspace_delivery.connector_credential_leases,
        "prepare_thread_lease_delivery",
        AsyncMock(),
    )
    monkeypatch.setattr(thread_workspace_delivery, "_PODS_WITHOUT_PLAN", {})
    pod = "00000000-0000-4000-8000-0000000000bb"
    workspace = {"status": "ready", "provisioner": "k8s", "_runtime_incarnation": pod}
    recover = AsyncMock(return_value=False)
    cleared: list = []

    class _Store:
        metadata: dict = {"workspace_container": workspace}

        async def get_thread(self, thread_id):
            return {"id": thread_id, "metadata": self.metadata}

        async def set_thread_cloud_mount_status(self, thread_id, status):
            cleared.append((thread_id, status))
            return True

    store = _Store()
    dependencies = SimpleNamespace(
        store=store,
        container_provisioner=SimpleNamespace(recover_cloud_mount_plan=recover),
    )
    for _ in range(3):
        await thread_workspace_delivery.prepare_agent_thread_workspace(
            THREAD_ID, dependencies=dependencies
        )
    # A Pod from before the plane: one read, then remembered.
    assert recover.await_count == 1
    # Upgraded to a VM: the sandbox Pod's folder state goes.
    store.metadata = {
        "workspace_container": workspace,
        "vm": {"status": "ready", "ssh_host": "vm.example"},
        "cloud_mount_status": {"fingerprint": "f" * 64, "mounts": {}},
    }
    await thread_workspace_delivery.prepare_agent_thread_workspace(
        THREAD_ID, dependencies=dependencies
    )
    assert cleared == [(THREAD_ID, None)] and recover.await_count == 1
