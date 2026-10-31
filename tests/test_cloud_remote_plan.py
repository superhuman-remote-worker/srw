"""A Virtual Session's cloud folders as remote descriptors, planned per claim
(features/cloud_folders_on_virtual_sessions.md § Orchestrator, § Status):

* a bound Nextcloud mount becomes one descriptor with the password folded in
  the way rclone wants it; a bearer mount of the service-account kind becomes
  an ``oauth_token`` descriptor without any credential (the executor fetches
  the token per call); an impersonated bearer is left out with a reason;
* the planner reuses the Container's binding chain on the ``virtual`` tier and
  answers ``None`` where nothing is bound and nothing was left out;
* the status record it writes is the one the agent's probe reports against,
  through the same closed states the sidecar uses.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from orchestrator.services import agent_cloud_mounts, cloud_mount_status
from orchestrator.services import cloud_remote_plan as crp
from orchestrator.services.agent_cloud_mounts import LiveMountSet
from orchestrator.services.cloud_mount_plan import rclone_reveal


def _built(name="outbox", *, auth=None, access="read_write", kind="cloud_outbox"):
    return {
        "mount_id": f"m-{name}",
        "mount_kind": kind,
        "backend": "nextcloud",
        "target_path": f"/cloud/{name}",
        "workspace_name": name,
        "access": access,
        "source_ref": f"c-{name}",
        "source": {
            "type": "webdav",
            "config": {
                "url": "https://cloud.example/remote.php/dav/files/svc/",
                "vendor": "nextcloud",
                "user": "svc",
            },
            "root": f"sessions/{name}",
        },
        "auth": auth if auth is not None else {"type": "basic", "password": "s3cret"},
        "provider_flags": [],
        "cache": {},
        "required_capabilities": ["rclone"],
    }


class TestRemoteEntry:
    def test_a_basic_webdav_mount_becomes_a_descriptor(self):
        entry = crp.remote_entry(_built())
        assert entry is not None
        assert (entry["name"], entry["path"], entry["access"]) == (
            "outbox",
            "cloud/outbox",
            "read_write",
        )
        assert (entry["mount_id"], entry["mount_kind"], entry["source_ref"]) == (
            "m-outbox",
            "cloud_outbox",
            "c-outbox",
        )
        spec = entry["rclone_spec"]
        assert spec["type"] == "webdav" and spec["root"] == "sessions/outbox"
        assert (
            spec["config"]["user"] == "svc" and spec["config"]["vendor"] == "nextcloud"
        )
        assert rclone_reveal(spec["config"]["pass"]) == "s3cret"
        assert "password" not in str(entry["auth"]) and entry["auth"] == {
            "type": "basic"
        }

    def test_read_only_is_kept_and_other_levels_normalise(self):
        assert crp.remote_entry(_built(access="read_only"))["access"] == "read_only"
        assert crp.remote_entry(_built(access="weird"))["access"] == "read_write"

    def test_a_service_account_bearer_becomes_a_token_route_entry(self):
        built = _built(
            "oc",
            kind="project",
            auth={
                "type": "keycloak_client_credentials",
                "issuer": "https://auth.example/realms/srw",
                "client_id": "opencloud-orchestrator",
                "client_secret": "very-secret",
            },
        )
        built["backend"] = "opencloud"
        built["source"]["config"] = {
            "url": "https://cloud.example/dav/spaces/drive-1/",
            "vendor": "infinitescale",
            # Never part of a token entry, whatever a source config says.
            "bearer_token": "stale",
        }
        entry = crp.remote_entry(built)
        assert entry is not None
        assert entry["auth"] == {"type": crp.OAUTH_TOKEN}
        assert entry["rclone_spec"] == {
            "type": "webdav",
            "config": {
                "url": "https://cloud.example/dav/spaces/drive-1/",
                "vendor": "infinitescale",
            },
            "root": "sessions/oc",
        }
        assert entry["backend"] == "opencloud" and entry["mount_id"] == "m-oc"
        text = str(entry)
        assert "very-secret" not in text and "stale" not in text
        assert "client_id" not in text and "issuer" not in text

    @pytest.mark.parametrize(
        "broken",
        [
            {"auth": {"type": "keycloak_user_impersonation", "target_user_sub": "u"}},
            {"auth": {"type": "basic", "password": ""}},
        ],
    )
    def test_a_mount_the_executor_cannot_serve_is_refused(self, broken):
        assert crp.remote_entry(_built(**broken)) is None
        built = _built(**broken)
        assert crp.remote_refusal(built) == {
            "source_ref": "c-outbox",
            "mount_kind": "cloud_outbox",
            "reason": "delivery_unsupported",
            "detail": "bearer_token",
            "name": "outbox",
            "level": "read_write",
        }

    def test_a_name_with_a_slash_or_no_url_is_refused(self):
        built = _built()
        built["workspace_name"] = "a/b"
        assert crp.remote_entry(built) is None
        built = _built()
        built["source"]["config"].pop("url")
        assert crp.remote_entry(built) is None


def _deps(driver="rclone_mount"):
    return SimpleNamespace(cloud_workspace_driver=lambda: driver)


def _thread(**over):
    thread = {
        "id": "t-1",
        "user_id": "u-1",
        "metadata": {},
        "main_cloud_share_handle": None,
    }
    thread.update(over)
    return thread


class TestResolvePlan:
    def test_binds_on_the_virtual_tier_and_hands_over_descriptors(self, monkeypatch):
        seen: dict = {}

        async def live(thread, *, mount_rows, runtime_is_vm, dependencies, **kw):
            seen.update(kw, runtime_is_vm=runtime_is_vm)
            return LiveMountSet(
                mounts=[
                    _built(),
                    _built("atlas", kind="project", access="read_only"),
                    _built("oc", auth={"type": "keycloak_client_credentials"}),
                    _built(
                        "home",
                        kind="user_root",
                        auth={
                            "type": "keycloak_user_impersonation",
                            "target_user_sub": "u-1",
                        },
                    ),
                ],
                excluded=[
                    {
                        "source_ref": "c-x",
                        "mount_kind": "project",
                        "reason": "not_eligible",
                        "name": "",
                        "title": "X",
                    }
                ],
            )

        monkeypatch.setattr(agent_cloud_mounts, "_resolve_live_mount_set", live)
        plan = asyncio.run(
            crp.resolve_cloud_remote_plan(
                _thread(main_cloud_share_handle="share-1"),
                mount_rows=[],
                dependencies=_deps(),
            )
        )
        assert seen == {"workspace_backend": "virtual", "runtime_is_vm": False}
        assert plan is not None
        assert [e["path"] for e in plan.entries] == [
            "cloud/outbox",
            "cloud/atlas",
            "cloud/oc",
        ]
        assert plan.entries[0]["shared"] is True and "shared" not in plan.entries[1]
        assert plan.entries[2]["auth"] == {"type": "oauth_token"}
        assert [(e["reason"], e.get("detail")) for e in plan.excluded] == [
            ("not_eligible", None),
            ("delivery_unsupported", "bearer_token"),
        ]
        record = plan.record
        assert record["fingerprint"] == plan.fingerprint
        assert [
            (m["name"], m["target_path"], m["access"]) for m in record["mounts"]
        ] == [
            ("outbox", "cloud/outbox", "read_write"),
            ("atlas", "cloud/atlas", "read_only"),
            ("oc", "cloud/oc", "read_write"),
        ]
        assert "pass" not in str(record) and "s3cret" not in str(record)
        payload = plan.agent_payload()
        assert payload["plan"] == {
            "fingerprint": plan.fingerprint,
            "incarnation": plan.incarnation,
        }
        assert len(payload["mounts"]) == 3
        # The same body fingerprints the same; each delivery is its own claim.
        again = asyncio.run(
            crp.resolve_cloud_remote_plan(
                _thread(main_cloud_share_handle="share-1"),
                mount_rows=[],
                dependencies=_deps(),
            )
        )
        assert again.fingerprint == plan.fingerprint
        assert again.incarnation != plan.incarnation

    def test_nothing_bound_and_nothing_left_out_is_no_plan(self, monkeypatch):
        async def live(*a, **k):
            return LiveMountSet(mounts=[])

        monkeypatch.setattr(agent_cloud_mounts, "_resolve_live_mount_set", live)
        assert (
            asyncio.run(
                crp.resolve_cloud_remote_plan(
                    _thread(), mount_rows=[], dependencies=_deps()
                )
            )
            is None
        )

    def test_the_sync_driver_and_an_officer_get_no_plan(self, monkeypatch):
        called = []

        async def live(*a, **k):
            called.append(1)
            return LiveMountSet(mounts=[_built()])

        monkeypatch.setattr(agent_cloud_mounts, "_resolve_live_mount_set", live)
        assert (
            asyncio.run(
                crp.resolve_cloud_remote_plan(
                    _thread(), mount_rows=[], dependencies=_deps("sync")
                )
            )
            is None
        )
        from orchestrator.services import cloud_mount_plan

        officer = _thread(metadata={"config_override": {"officer": {"enabled": True}}})
        assert cloud_mount_plan._officer(officer["metadata"]) is True
        assert (
            asyncio.run(
                crp.resolve_cloud_remote_plan(
                    officer, mount_rows=[], dependencies=_deps()
                )
            )
            is None
        )
        assert called == []


class TestStatusRecord:
    def _plan_body(self):
        return {
            "fingerprint": "fp-1",
            "mounts": [
                {
                    "name": "outbox",
                    "mount_kind": "cloud_outbox",
                    "target_path": "cloud/outbox",
                    "access": "read_write",
                    "mount_id": "m-outbox",
                    "source_ref": "c-outbox",
                }
            ],
            "excluded": [
                {"source_ref": "c-x", "mount_kind": "project", "reason": "not_eligible"}
            ],
        }

    def test_the_record_is_pending_until_the_probe_reports(self):
        status = cloud_mount_status.remote_status(
            self._plan_body(), "claim-1", now="2026-10-28T12:00:00+00:00"
        )
        assert status["delivery"] == "remote"
        assert (status["fingerprint"], status["runtime_incarnation"]) == (
            "fp-1",
            "claim-1",
        )
        assert status["mounts"]["outbox"]["state"] == "pending"
        assert status["mounts"]["outbox"]["target_path"] == "cloud/outbox"
        assert status["excluded"] == [
            {"source_ref": "c-x", "mount_kind": "project", "reason": "not_eligible"}
        ]
        recorded = cloud_mount_status.recorded_remote_plan(
            {"cloud_mount_status": status}
        )
        assert recorded == {
            "fingerprint": "fp-1",
            "runtime_incarnation": "claim-1",
            "mounts": [
                {
                    "name": "outbox",
                    "mount_kind": "cloud_outbox",
                    "target_path": "cloud/outbox",
                    "access": "read_write",
                }
            ],
        }
        # The agent's probe reports in the record's closed states.
        entries = cloud_mount_status.report_entries(
            recorded, [{"name": "outbox", "state": "mounted"}]
        )
        assert entries["outbox"]["state"] == "mounted"
        entries = cloud_mount_status.report_entries(
            recorded,
            [{"name": "outbox", "state": "unavailable", "reason": "mount_failed"}],
        )
        assert entries["outbox"]["reason"] == "mount_failed"
        with pytest.raises(ValueError):
            cloud_mount_status.report_entries(
                recorded, [{"name": "ghost", "state": "mounted"}]
            )

    def test_a_pods_record_is_not_a_remote_plan(self):
        pod = cloud_mount_status.initial_status(self._plan_body(), "pod-uid")
        assert (
            cloud_mount_status.recorded_remote_plan({"cloud_mount_status": pod}) is None
        )
        assert cloud_mount_status.recorded_remote_plan({}) is None
        broken = cloud_mount_status.remote_status(self._plan_body(), "claim-1")
        broken["mounts"] = "nope"
        assert (
            cloud_mount_status.recorded_remote_plan({"cloud_mount_status": broken})
            is None
        )

    def test_record_remote_plan_is_best_effort(self):
        class Store:
            def __init__(self):
                self.writes = []

            async def set_thread_cloud_mount_status(self, thread_id, status):
                self.writes.append((thread_id, status))
                return True

        store = Store()
        assert asyncio.run(
            cloud_mount_status.record_remote_plan(store, "t-1", {"version": 1})
        )
        assert store.writes == [("t-1", {"version": 1})]
        assert (
            asyncio.run(cloud_mount_status.record_remote_plan(object(), "t-1", {}))
            is False
        )

        class Failing(Store):
            async def set_thread_cloud_mount_status(self, thread_id, status):
                raise RuntimeError("db down")

        assert (
            asyncio.run(cloud_mount_status.record_remote_plan(Failing(), "t-1", {}))
            is False
        )


def test_the_spec_names_the_remote_form_and_its_plane():
    from shared.connectors.builtin import CLOUD_FOLDER_SPEC, CLOUD_OUTBOX_SPEC
    from shared.connectors.contract import validate_spec

    for spec in (CLOUD_FOLDER_SPEC, CLOUD_OUTBOX_SPEC):
        assert spec.delivery_forms == ("cloud_mount", "cloud_remote")
        assert spec.plane_for("cloud_mount") == "in_pod"
        assert spec.plane_for("cloud_remote") == "harness"
        assert "virtual" in spec.supported_backends
        assert validate_spec(spec) == []
    from dataclasses import replace

    bad = replace(CLOUD_FOLDER_SPEC, form_planes={"ssh_identity": "harness"})
    assert any("not one of the driver's forms" in p for p in validate_spec(bad))
    bad = replace(CLOUD_FOLDER_SPEC, form_planes={"cloud_remote": "orbit"})
    assert any("is not one of" in p for p in validate_spec(bad))


_ = AsyncMock  # the harness above fakes with plain coroutines
