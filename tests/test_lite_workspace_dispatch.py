"""S2 — orchestrator-side dispatch/validation for the lite workspace tiers.

Covers ``knowledge-base/knowledge/features/no_workspace_agent_mode.md`` §4/§11 (orchestrator side):
the pure config helpers the job-dispatch and session-attach seams share, the
skip-provisioning decision, the repository-datasource rejection, and — the
load-bearing one — that the §4 ``mounts`` payload the orchestrator emits is
exactly what the agent-side factory (``create_lite_backend``) consumes. That
roundtrip is what proves S1 and S2 agree on the contract without a cluster.

Import pattern mirrors test_dispatch_phase_credentials.py: ``orchestrator/`` on
``sys.path`` so ``import main`` resolves; conftest handles the license gate.
"""

from __future__ import annotations

import os
from types import SimpleNamespace

import pytest
from orchestrator.application import preparation as preparation_composition
from orchestrator.services import container_provisioner as container_provisioner_module
from orchestrator.services import docker_provisioner as docker_provisioner_module
from orchestrator.services import (
    job_datasource_selection as job_datasource_selection_module,
)
from orchestrator.services import job_workspace_runtime as job_workspace_runtime_module
from orchestrator.services import virtual_workspace as virtual_workspace_module
from orchestrator.services import workspace_tier_policy as workspace_tier_policy_module

os.environ.setdefault("VECTOR_DB_URL", "postgresql://test@localhost/test")

import orchestrator.main  # noqa: E402
from agent.core.backends.factory import create_lite_backend  # noqa: E402


# ---------------------------------------------------------------------------
# _backend_from_override
# ---------------------------------------------------------------------------
class TestBackendFromOverride:
    def test_dict(self):
        assert (
            workspace_tier_policy_module.backend_from_override(
                {"workspace": {"backend": "virtual"}}
            )
            == "virtual"
        )

    def test_json_string(self):
        assert (
            workspace_tier_policy_module.backend_from_override(
                '{"workspace": {"backend": "none"}}'
            )
            == "none"
        )

    def test_none(self):
        assert workspace_tier_policy_module.backend_from_override(None) is None

    def test_no_workspace_key(self):
        assert (
            workspace_tier_policy_module.backend_from_override({"llm": {"model": "x"}})
            is None
        )

    def test_bad_json_string(self):
        assert (
            workspace_tier_policy_module.backend_from_override("not json at all")
            is None
        )

    def test_non_dict_workspace(self):
        assert (
            workspace_tier_policy_module.backend_from_override({"workspace": "oops"})
            is None
        )


# ---------------------------------------------------------------------------
# _virtual_workspace_rclone_spec
# ---------------------------------------------------------------------------
class TestVirtualWorkspaceRcloneSpec:
    # s3 credentials/config now arrive as discrete env vars (mirroring the
    # snapshot S3 wiring) rather than a single JSON blob.
    S3_ENV = {
        "VIRTUAL_WORKSPACE_S3_ACCESS_KEY_ID": "AKID",
        "VIRTUAL_WORKSPACE_S3_SECRET_ACCESS_KEY": "SEKRET",
        "VIRTUAL_WORKSPACE_S3_ENDPOINT": "http://minio.minio.svc:9000",
        "VIRTUAL_WORKSPACE_S3_REGION": "eu-central-1",
        "VIRTUAL_WORKSPACE_S3_PROVIDER": "Minio",
    }

    def test_unset_type_returns_none(self, monkeypatch):
        monkeypatch.delenv("VIRTUAL_WORKSPACE_RCLONE_TYPE", raising=False)
        assert virtual_workspace_module.virtual_workspace_rclone_spec() is None

    def test_s3_builds_spec_from_discrete_env(self, monkeypatch):
        monkeypatch.setenv("VIRTUAL_WORKSPACE_RCLONE_TYPE", "s3")
        monkeypatch.setenv("VIRTUAL_WORKSPACE_RCLONE_ROOT", "srw-workspaces")
        for key, value in self.S3_ENV.items():
            monkeypatch.setenv(key, value)
        spec = virtual_workspace_module.virtual_workspace_rclone_spec()
        assert spec["type"] == "s3"
        assert spec["root"] == "srw-workspaces"
        # no_check_bucket is baked in (scoped key can't create the bucket).
        assert spec["config"] == {
            "provider": "Minio",
            "access_key_id": "AKID",
            "secret_access_key": "SEKRET",
            "endpoint": "http://minio.minio.svc:9000",
            "region": "eu-central-1",
            "no_check_bucket": "true",
        }

    def test_s3_defaults_provider_and_region(self, monkeypatch):
        monkeypatch.setenv("VIRTUAL_WORKSPACE_RCLONE_TYPE", "s3")
        monkeypatch.setenv("VIRTUAL_WORKSPACE_S3_ACCESS_KEY_ID", "K")
        monkeypatch.setenv("VIRTUAL_WORKSPACE_S3_SECRET_ACCESS_KEY", "S")
        for key in ("VIRTUAL_WORKSPACE_S3_PROVIDER", "VIRTUAL_WORKSPACE_S3_REGION"):
            monkeypatch.delenv(key, raising=False)
        config = virtual_workspace_module.virtual_workspace_rclone_spec()["config"]
        assert config["provider"] == "Minio"
        assert config["region"] == "us-east-1"
        assert config["no_check_bucket"] == "true"

    def test_memory_type_needs_no_config(self, monkeypatch):
        monkeypatch.setenv("VIRTUAL_WORKSPACE_RCLONE_TYPE", "memory")
        monkeypatch.delenv("VIRTUAL_WORKSPACE_RCLONE_ROOT", raising=False)
        # s3 creds are ignored for non-s3 types — config stays empty.
        for key in self.S3_ENV:
            monkeypatch.setenv(key, "leaked")
        assert virtual_workspace_module.virtual_workspace_rclone_spec() == {
            "type": "memory",
            "config": {},
            "root": "",
        }


# ---------------------------------------------------------------------------
# _inject_lite_workspace_config
# ---------------------------------------------------------------------------
class TestInjectLiteWorkspaceConfig:
    def test_non_lite_returns_unchanged(self):
        co = {"workspace": {"backend": "sandbox"}}
        assert (
            workspace_tier_policy_module.inject_lite_workspace_config(
                co, prefix="jobs/j/"
            )
            is co
        )

    def test_none_override_stays_none(self):
        # backend is None -> not lite -> no enrichment, no crash.
        assert (
            workspace_tier_policy_module.inject_lite_workspace_config(
                None, prefix="jobs/j/"
            )
            is None
        )

    def test_none_backend_git_off_no_mounts(self):
        co = workspace_tier_policy_module.inject_lite_workspace_config(
            {"workspace": {"backend": "none"}}, prefix="jobs/j/"
        )
        ws = co["workspace"]
        assert ws["backend"] == "none"
        assert ws["git_versioning"] is False
        assert "mounts" not in ws

    def test_none_backend_strips_stray_mounts(self):
        co = workspace_tier_policy_module.inject_lite_workspace_config(
            {"workspace": {"backend": "none", "mounts": [{"x": 1}]}},
            prefix="jobs/j/",
        )
        assert "mounts" not in co["workspace"]

    def test_virtual_builds_mount(self, monkeypatch):
        monkeypatch.setenv("VIRTUAL_WORKSPACE_RCLONE_TYPE", "memory")
        monkeypatch.delenv("VIRTUAL_WORKSPACE_S3_ACCESS_KEY_ID", raising=False)
        monkeypatch.delenv("VIRTUAL_WORKSPACE_RCLONE_ROOT", raising=False)
        co = workspace_tier_policy_module.inject_lite_workspace_config(
            {"workspace": {"backend": "virtual"}}, prefix="jobs/j7/"
        )
        ws = co["workspace"]
        assert ws["backend"] == "virtual"
        assert ws["git_versioning"] is False
        assert len(ws["mounts"]) == 1
        mount = ws["mounts"][0]
        assert mount["name"] == "workspace"
        assert mount["prefix"] == "jobs/j7/"
        assert mount["access"] == "read_write"
        assert mount["rclone_spec"]["type"] == "memory"

    def test_virtual_without_objectstore_raises(self, monkeypatch):
        monkeypatch.delenv("VIRTUAL_WORKSPACE_RCLONE_TYPE", raising=False)
        with pytest.raises(workspace_tier_policy_module.LiteWorkspaceConfigError):
            workspace_tier_policy_module.inject_lite_workspace_config(
                {"workspace": {"backend": "virtual"}}, prefix="jobs/j/"
            )


# ---------------------------------------------------------------------------
# _repository_datasource_names
# ---------------------------------------------------------------------------
class TestRepositoryDatasourceNames:
    def test_filters_repository(self):
        ds = [
            {"type": "postgresql", "name": "pg"},
            {"type": "repository", "name": "repo1"},
            {"type": "credentials", "name": "api-key"},
        ]
        assert job_datasource_selection_module.repository_datasource_names(ds) == [
            "repo1",
            "api-key",
        ]

    def test_case_insensitive(self):
        assert job_datasource_selection_module.repository_datasource_names(
            [{"type": "Repository", "name": "r"}]
        ) == ["r"]

    def test_id_fallback_when_no_name(self):
        assert job_datasource_selection_module.repository_datasource_names(
            [{"type": "repository", "id": "abc"}]
        ) == ["abc"]

    def test_empty_and_none(self):
        assert job_datasource_selection_module.repository_datasource_names(None) == []
        assert job_datasource_selection_module.repository_datasource_names([]) == []

    def test_skips_non_dict_entries(self):
        assert job_datasource_selection_module.repository_datasource_names(
            ["junk", {"type": "repository", "name": "r"}]
        ) == ["r"]


# ---------------------------------------------------------------------------
# _job_needs_sandbox / _job_needs_vm: lite tiers never provision
# ---------------------------------------------------------------------------
class TestLiteNeverProvisions:
    def _force_provisioners_available(self, monkeypatch):
        # is_available is a read-only property on the real singletons, so swap
        # the module references for stand-ins that report available. This is
        # the worst case for the lite short-circuit: a provisioner *is* ready.
        monkeypatch.setattr(
            container_provisioner_module,
            "container_provisioner",
            SimpleNamespace(is_available=True),
        )
        monkeypatch.setattr(
            docker_provisioner_module,
            "docker_provisioner",
            SimpleNamespace(is_available=True),
        )

    def test_virtual_does_not_need_sandbox(self, monkeypatch):
        self._force_provisioners_available(monkeypatch)
        job = {"config_override": {"workspace": {"backend": "virtual"}}}
        assert (
            job_workspace_runtime_module.job_needs_sandbox(
                job,
                dependencies=preparation_composition.job_workspace_runtime_dependencies(
                    orchestrator.main.app.state.resources
                ),
            )
            is False
        )

    def test_none_does_not_need_sandbox(self, monkeypatch):
        self._force_provisioners_available(monkeypatch)
        job = {"config_override": {"workspace": {"backend": "none"}}}
        assert (
            job_workspace_runtime_module.job_needs_sandbox(
                job,
                dependencies=preparation_composition.job_workspace_runtime_dependencies(
                    orchestrator.main.app.state.resources
                ),
            )
            is False
        )

    def test_unset_backend_still_needs_sandbox(self, monkeypatch):
        # Sanity: the stand-ins really do report available, so the lite=False
        # results above are the short-circuit, not an inert provisioner.
        self._force_provisioners_available(monkeypatch)
        assert (
            job_workspace_runtime_module.job_needs_sandbox(
                {"config_override": {}},
                dependencies=preparation_composition.job_workspace_runtime_dependencies(
                    orchestrator.main.app.state.resources
                ),
            )
            is True
        )

    def test_virtual_does_not_need_vm(self):
        job = {"config_override": {"workspace": {"backend": "virtual"}}}
        assert job_workspace_runtime_module.job_needs_vm(job) is False

    def test_none_does_not_need_vm(self):
        job = {"config_override": {"workspace": {"backend": "none"}}}
        assert job_workspace_runtime_module.job_needs_vm(job) is False


# ---------------------------------------------------------------------------
# §4 payload contract roundtrip — orchestrator output -> agent factory
# ---------------------------------------------------------------------------
class TestPayloadContractRoundtrip:
    """The single most important S2 test: whatever the orchestrator emits in
    ``config_override.workspace`` must be directly consumable by the agent-side
    ``create_lite_backend`` (no translation in between). Uses the dev ``memory``
    object store so the whole path runs in-process."""

    def test_virtual_payload_builds_a_working_backend(self, monkeypatch):
        monkeypatch.setenv("VIRTUAL_WORKSPACE_RCLONE_TYPE", "memory")
        monkeypatch.delenv("VIRTUAL_WORKSPACE_S3_ACCESS_KEY_ID", raising=False)
        monkeypatch.delenv("VIRTUAL_WORKSPACE_RCLONE_ROOT", raising=False)

        co = workspace_tier_policy_module.inject_lite_workspace_config(
            {"workspace": {"backend": "virtual"}}, prefix="jobs/roundtrip/"
        )
        ws = co["workspace"]

        # Exactly the shape the agent loader hands the factory (backend + mounts).
        cfg = SimpleNamespace(backend=ws["backend"], mounts=ws["mounts"])
        backend = create_lite_backend(cfg, job_id="roundtrip")
        backend.connect()

        # The orchestrator's prefix flowed through to the backend's virtual root.
        assert backend.root == "jobs/roundtrip/"

        # Full write -> read -> exists -> list roundtrip over the emitted payload.
        backend.write_file("notes/plan.md", "hello virtual")
        assert backend.read_file("notes/plan.md") == "hello virtual"
        assert backend.exists("notes/plan.md") is True
        # Directories list with a trailing slash in the virtual backend.
        assert "notes/" in backend.list_dir("")

    def test_none_payload_builds_scratch_backend(self):
        co = workspace_tier_policy_module.inject_lite_workspace_config(
            {"workspace": {"backend": "none"}}, prefix="jobs/n/"
        )
        cfg = SimpleNamespace(backend=co["workspace"]["backend"], mounts=None)
        backend = create_lite_backend(cfg, job_id="n1")
        try:
            backend.connect()
            # ScratchBackend is a real disposable FS backend (no file tools are
            # registered over it in `none` mode, but the object itself works).
            backend.write_file("scratch.txt", "x")
            assert backend.read_file("scratch.txt") == "x"
        finally:
            backend.disconnect()


# ---------------------------------------------------------------------------
# _is_lite_config_override — gates scholar/critic/curator subjobs off for lite
# ---------------------------------------------------------------------------
class TestLiteSubjobGating:
    """The git-graft lifecycle subjobs (scholar/critic/curator) are skipped for
    lite jobs; all three guards key on ``_is_lite_config_override``."""

    def test_lite_backends_detected(self):
        assert workspace_tier_policy_module.is_lite_config_override(
            {"workspace": {"backend": "virtual"}}
        )
        assert workspace_tier_policy_module.is_lite_config_override(
            {"workspace": {"backend": "none"}}
        )

    def test_full_backends_not_lite(self):
        assert not workspace_tier_policy_module.is_lite_config_override(
            {"workspace": {"backend": "sandbox"}}
        )
        assert not workspace_tier_policy_module.is_lite_config_override(
            {"workspace": {"backend": "vm"}}
        )

    def test_missing_or_none_not_lite(self):
        assert not workspace_tier_policy_module.is_lite_config_override(None)
        assert not workspace_tier_policy_module.is_lite_config_override({})
        assert not workspace_tier_policy_module.is_lite_config_override(
            {"llm": {"model": "x"}}
        )

    def test_json_string_override(self):
        assert workspace_tier_policy_module.is_lite_config_override(
            '{"workspace": {"backend": "none"}}'
        )
        assert not workspace_tier_policy_module.is_lite_config_override(
            "not json at all"
        )


# ---------------------------------------------------------------------------
# _object_store_startup_warning — one loud, early signal when the deployment
# has NO object store at all (knowledge-history/done/s3_object_store_bundled_fallback.md
# item 3). Reads the same env the features read; returns the message (or None)
# so it is testable without a live lifespan.
# ---------------------------------------------------------------------------
class TestObjectStoreStartupWarning:
    """Warn if EITHER seam is unconfigured; the two almost always point at the
    same store, so a half-config is usually a mistake and (virtual being the
    default session backend) a snapshots-only config silently breaks the
    default UX. Silent only when BOTH have a durable store."""

    def test_silent_only_when_both_seams_configured(self):
        assert (
            virtual_workspace_module.object_store_startup_warning(
                {
                    "S3_ENDPOINT": "http://minio.minio.svc:9000",
                    "VIRTUAL_WORKSPACE_RCLONE_TYPE": "s3",
                }
            )
            is None
        )

    def test_warns_when_no_store_at_all(self):
        msg = virtual_workspace_module.object_store_startup_warning({})
        assert msg is not None
        assert "LiteWorkspaceConfigError" in msg  # virtual bullet
        assert "snapshots" in msg.lower()  # snapshot bullet
        assert "garage.enabled=true" in msg
        assert "knowledge-history/done/s3_object_store_bundled_fallback.md" in msg

    def test_warns_about_virtual_when_only_snapshots_configured(self):
        # S3_ENDPOINT set (snapshots OK) but the virtual tier is unconfigured —
        # warn about the virtual tier ONLY, don't claim snapshots are disabled.
        msg = virtual_workspace_module.object_store_startup_warning(
            {"S3_ENDPOINT": "http://minio:9000"}
        )
        assert msg is not None
        assert "LiteWorkspaceConfigError" in msg
        assert "snapshots" not in msg.lower()

    def test_warns_about_snapshots_when_only_virtual_configured(self):
        # Durable virtual store but no S3_ENDPOINT — warn about snapshots ONLY,
        # don't claim virtual sessions fail.
        msg = virtual_workspace_module.object_store_startup_warning(
            {"VIRTUAL_WORKSPACE_RCLONE_TYPE": "s3"}
        )
        assert msg is not None
        assert "snapshots" in msg.lower()
        assert "LiteWorkspaceConfigError" not in msg

    def test_memory_store_flagged_non_durable(self):
        msg = virtual_workspace_module.object_store_startup_warning(
            {"VIRTUAL_WORKSPACE_RCLONE_TYPE": "memory"}
        )
        assert msg is not None
        assert "non-durable" in msg.lower()
        assert "snapshots" in msg.lower()  # snapshots still unconfigured here

    def test_memory_with_snapshots_flags_only_non_durable(self):
        # Snapshots configured + memory virtual — warn only about the non-durable
        # virtual store, not about snapshots.
        msg = virtual_workspace_module.object_store_startup_warning(
            {
                "S3_ENDPOINT": "http://minio:9000",
                "VIRTUAL_WORKSPACE_RCLONE_TYPE": "memory",
            }
        )
        assert msg is not None
        assert "non-durable" in msg.lower()
        assert "snapshots" not in msg.lower()

    def test_whitespace_values_treated_as_empty(self):
        assert (
            virtual_workspace_module.object_store_startup_warning(
                {"S3_ENDPOINT": "   ", "VIRTUAL_WORKSPACE_RCLONE_TYPE": "  "}
            )
            is not None
        )


class TestObjectStoreRequiredEnforcement:
    """Opt-in fail-closed: OBJECT_STORE_REQUIRED makes an incomplete object-store
    config a startup error (crash-loop until fixed) instead of a warning. Default
    (unset/falsey) stays warn-only. Truthy per the repo convention: true/1/yes."""

    def test_warns_not_raises_when_flag_unset(self):
        msg = virtual_workspace_module.check_object_store_config({})
        assert msg is not None
        assert "Object store not fully configured" in msg

    def test_raises_when_required_and_store_missing(self):
        with pytest.raises(RuntimeError, match="refuses to start"):
            virtual_workspace_module.check_object_store_config(
                {"OBJECT_STORE_REQUIRED": "true"}
            )

    def test_raises_on_partial_config_when_required(self):
        # snapshots set but virtual tier missing + required -> still fail-closed
        with pytest.raises(RuntimeError):
            virtual_workspace_module.check_object_store_config(
                {"S3_ENDPOINT": "http://minio:9000", "OBJECT_STORE_REQUIRED": "1"}
            )

    def test_no_raise_when_required_and_both_configured(self):
        assert (
            virtual_workspace_module.check_object_store_config(
                {
                    "S3_ENDPOINT": "http://minio:9000",
                    "VIRTUAL_WORKSPACE_RCLONE_TYPE": "s3",
                    "OBJECT_STORE_REQUIRED": "true",
                }
            )
            is None
        )

    def test_falsey_flag_values_warn_not_raise(self):
        for val in ("false", "0", "no", "", "  "):
            msg = virtual_workspace_module.check_object_store_config(
                {"OBJECT_STORE_REQUIRED": val}
            )
            assert msg is not None, f"{val!r} should warn, not raise"

    def test_truthy_flag_variants_raise(self):
        for val in ("true", "TRUE", "1", "yes", " Yes "):
            with pytest.raises(RuntimeError):
                virtual_workspace_module.check_object_store_config(
                    {"OBJECT_STORE_REQUIRED": val}
                )
