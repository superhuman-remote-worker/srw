"""Characterization for the shared policy core extracted in R1.B05 lane P.

Two kinds of assertion, on purpose:

* **Behavioural**, with the expected value written out. These survive the
  extraction and are what pins the behaviour afterwards.
* **Differential**, comparing the moved function against the copy still in
  ``orchestrator.main``. These are what makes the move *provably*
  behaviour-preserving while both exist; they degenerate into "main exports the
  moved symbol" once ``main`` becomes a set of aliases, which is still a real
  contract.

Every dependency-injected function is additionally driven through a fake
dependency object whose fields RECORD that they were called, so a field that
silently stopped being consulted fails here rather than passing inertly (B05
port contract §P3).
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException

from orchestrator.schemas.job_create import JobCreate
from orchestrator.services import (
    agent_toolset_probe as toolset,
    config_overrides,
    datasource_config,
    deployment_gates as gates,
    grant_enforcement as grants,
    job_create_ingress as ingress,
    session_class_policy as classes,
    session_config_resolution as sessioncfg,
    session_create_overrides as overrides,
    session_tool_policy as toolpolicy,
    session_tool_view,
    session_workspace_policy as wspolicy,
    thread_config_update as config_update,
    virtual_workspace,
    vm_workspace_policy as vmpolicy,
    workspace_tier_policy as tier,
)
from orchestrator.services import agent_toolset_probe as agent_toolset_probe_module
from orchestrator.services import config_overrides as config_overrides_module
from orchestrator.services import datasource_config as datasource_config_module
from orchestrator.services import deployment_gates as deployment_gates_module
from orchestrator.services import grant_enforcement as grant_enforcement_module
from orchestrator.services import job_create_ingress as job_create_ingress_module
from orchestrator.services import session_class_policy as session_class_policy_module
from orchestrator.services import (
    session_create_overrides as session_create_overrides_module,
)
from orchestrator.services import session_tool_policy as session_tool_policy_module
from orchestrator.services import (
    session_workspace_policy as session_workspace_policy_module,
)
from orchestrator.services import virtual_workspace as virtual_workspace_module
from orchestrator.services import vm_workspace_policy as vm_workspace_policy_module
from orchestrator.services import workspace_tier_policy as workspace_tier_policy_module

# ---------------------------------------------------------------------------
# Feature gates
# ---------------------------------------------------------------------------

#: ``(function, env var, default when unset)``. The default is per-gate and the
#: whole point of the table: a uniform default would flip four deployments.
_GATES = [
    (
        gates.is_experts_db_enabled,
        deployment_gates_module.is_experts_db_enabled,
        "EXPERTS_DB_ENABLED",
        True,
    ),
    (
        gates.is_skills_db_enabled,
        deployment_gates_module.is_skills_db_enabled,
        "SKILLS_DB_ENABLED",
        False,
    ),
    (
        gates.mcp_datasources_enabled,
        deployment_gates_module.mcp_datasources_enabled,
        "MCP_DATASOURCES_ENABLED",
        False,
    ),
    (
        gates.datasource_defaults_on_omission,
        deployment_gates_module.datasource_defaults_on_omission,
        "DATASOURCE_DEFAULTS_ON_OMISSION",
        False,
    ),
    (
        gates.datasource_scope_auto_attach_v1_enabled,
        deployment_gates_module.datasource_scope_auto_attach_v1_enabled,
        "DATASOURCE_SCOPE_AUTO_ATTACH_V1_ENABLED",
        False,
    ),
    (
        gates.mcp_stdio_enabled,
        deployment_gates_module.mcp_stdio_enabled,
        "MCP_STDIO_ENABLED",
        False,
    ),
    (
        gates.is_protected_cloud_mode_enabled,
        deployment_gates_module.is_protected_cloud_mode_enabled,
        "PROTECTED_CLOUD_MODE_ENABLED",
        False,
    ),
    (
        gates.require_pinned_status_identity,
        deployment_gates_module.require_pinned_status_identity,
        "REQUIRE_PINNED_STATUS_IDENTITY",
        True,
    ),
]

#: The truthy vocabulary, and the near-misses that must stay off. ``"on"`` is
#: deliberately NOT truthy here.
_GATE_VALUES = ["true", "TRUE", " yes ", "1", "yes", "false", "0", "no", "on", "", "  "]


@pytest.mark.parametrize(("moved", "original", "env", "default"), _GATES)
def test_gate_default_when_unset(monkeypatch, moved, original, env, default):
    monkeypatch.delenv(env, raising=False)
    assert moved() is default
    assert moved() == original()


@pytest.mark.parametrize(("moved", "original", "env", "default"), _GATES)
@pytest.mark.parametrize("value", _GATE_VALUES)
def test_gate_vocabulary_matches_main(
    monkeypatch, moved, original, env, default, value
):
    monkeypatch.setenv(env, value)
    expected = value.lower().strip() in ("true", "1", "yes")
    assert moved() is expected
    assert moved() == original()


def test_gates_are_read_live_not_cached(monkeypatch):
    monkeypatch.setenv("MCP_STDIO_ENABLED", "true")
    assert gates.mcp_stdio_enabled() is True
    monkeypatch.setenv("MCP_STDIO_ENABLED", "false")
    assert gates.mcp_stdio_enabled() is False


# ---------------------------------------------------------------------------
# MCP datasource validation
# ---------------------------------------------------------------------------

_MCP_CASES = [
    ("https://mcp.example/sse", {"transport": "sse"}, None),
    ("https://mcp.example", {}, None),
    (None, {"transport": "http"}, "require connection_url"),
    ("file:///tmp/sock", {"transport": "http"}, "must be an HTTP(S) URL"),
    ("https://u:p@mcp.example", {"transport": "http"}, "must not embed credentials"),
    ("https://mcp.example", {"transport": "ftp"}, "Invalid MCP transport"),
    ("https://mcp.example", {"transport": 7}, "must be a string"),
    ("https://mcp.example", {"transport": "http", "nope": 1}, "Unknown remote MCP"),
    (
        "https://mcp.example",
        {"transport": "http", "auth": {"type": "bearer"}},
        "requires a token",
    ),
    (
        "https://mcp.example",
        {"transport": "http", "auth": {"type": "bearer", "token": "t", "x": 1}},
        "Unknown MCP bearer auth",
    ),
    (
        "https://mcp.example",
        {"transport": "http", "auth": {"type": "headers", "headers": {"A": "b\nc"}}},
        "custom headers must map",
    ),
    (
        "https://mcp.example",
        {"transport": "http", "auth": {"type": "none", "extra": 1}},
        "Unknown MCP no-auth",
    ),
    (
        "https://mcp.example",
        {"transport": "http", "auth": {"type": "x"}},
        "Invalid MCP auth type",
    ),
    # ``auth`` is read as ``credentials.get("auth") or {}``, so a FALSY non-dict
    # (``[]``) is the same as "no auth" and only a truthy one is rejected.
    ("https://mcp.example", {"transport": "http", "auth": []}, None),
    ("https://mcp.example", {"transport": "http", "auth": 5}, "auth must be an object"),
]


@pytest.mark.parametrize(("url", "creds", "fragment"), _MCP_CASES)
def test_validate_mcp_datasource_matches_main(monkeypatch, url, creds, fragment):
    monkeypatch.delenv("MCP_STDIO_ENABLED", raising=False)

    def run(fn):
        try:
            fn(url, creds)
            return None
        except HTTPException as exc:
            return (exc.status_code, str(exc.detail))

    moved = run(datasource_config.validate_mcp_datasource)
    original = run(datasource_config_module.validate_mcp_datasource)
    assert moved == original
    if fragment is None:
        assert moved is None
    else:
        assert moved is not None and fragment in moved[1]
        assert moved[0] == 400


def test_validate_mcp_datasource_reads_the_stdio_gate_from_the_new_module(monkeypatch):
    """The moved body consults ``deployment_gates`` on every call, so env still steers."""
    monkeypatch.delenv("MCP_STDIO_ENABLED", raising=False)
    with pytest.raises(HTTPException) as off:
        datasource_config.validate_mcp_datasource(None, {"transport": "stdio"})
    assert "disabled on this deployment" in str(off.value.detail)

    monkeypatch.setenv("MCP_STDIO_ENABLED", "true")
    with pytest.raises(HTTPException) as on:
        datasource_config.validate_mcp_datasource(None, {"transport": "stdio"})
    assert "credentials.command" in str(on.value.detail)

    datasource_config.validate_mcp_datasource(
        None,
        {"transport": "stdio", "command": "srv", "args": ["-x"], "env": {"A": "b"}},
    )


def test_validate_mcp_datasource_rejects_null_bytes(monkeypatch):
    monkeypatch.setenv("MCP_STDIO_ENABLED", "true")
    for creds in (
        {"transport": "stdio", "command": "sr\x00v"},
        {"transport": "stdio", "command": "srv", "args": ["a\x00"]},
        {"transport": "stdio", "command": "srv", "env": {"A=B": "c"}},
        {"transport": "stdio", "command": "srv", "env": {"A": 1}},
        {"transport": "stdio", "command": "srv", "unknown": 1},
    ):
        with pytest.raises(HTTPException):
            datasource_config.validate_mcp_datasource(None, creds)


# ---------------------------------------------------------------------------
# Workspace tier reading + lite enrichment (copy semantics)
# ---------------------------------------------------------------------------


class TestWorkspaceTierPolicy:
    @pytest.mark.parametrize(
        ("override", "expected"),
        [
            ({"workspace": {"backend": "virtual"}}, "virtual"),
            (json.dumps({"workspace": {"backend": "none"}}), "none"),
            (None, None),
            ({"llm": {"model": "x"}}, None),
            ("not json at all", None),
            ({"workspace": "oops"}, None),
        ],
    )
    def test_backend_from_override_matches_main(self, override, expected):
        assert tier.backend_from_override(override) == expected
        assert tier.backend_from_override(
            override
        ) == workspace_tier_policy_module.backend_from_override(override)

    def test_non_lite_returns_the_caller_object_itself(self):
        co = {"workspace": {"backend": "sandbox"}}
        assert tier.inject_lite_workspace_config(co, prefix="jobs/j/") is co
        assert co == {"workspace": {"backend": "sandbox"}}

    def test_none_override_stays_none(self):
        assert tier.inject_lite_workspace_config(None, prefix="jobs/j/") is None

    def test_lite_mutates_the_caller_object_in_place(self):
        co = {"workspace": {"backend": "none", "mounts": [{"x": 1}]}}
        out = tier.inject_lite_workspace_config(co, prefix="jobs/j/")
        assert out is co, (
            "lite enrichment is in-place; a copy would drop the caller's view"
        )
        assert co["workspace"] == {"backend": "none", "git_versioning": False}

    def test_virtual_builds_the_mount(self, monkeypatch):
        monkeypatch.setenv("VIRTUAL_WORKSPACE_RCLONE_TYPE", "memory")
        monkeypatch.delenv("VIRTUAL_WORKSPACE_RCLONE_ROOT", raising=False)
        co = tier.inject_lite_workspace_config(
            {"workspace": {"backend": "virtual"}}, prefix="threads/t7/"
        )
        assert co["workspace"]["git_versioning"] is False
        assert co["workspace"]["mounts"] == [
            {
                "name": "workspace",
                "rclone_spec": {"type": "memory", "config": {}, "root": ""},
                "prefix": "threads/t7/",
                "access": "read_write",
            }
        ]

    def test_virtual_without_object_store_refuses(self, monkeypatch):
        monkeypatch.delenv("VIRTUAL_WORKSPACE_RCLONE_TYPE", raising=False)
        with pytest.raises(tier.LiteWorkspaceConfigError) as exc:
            tier.inject_lite_workspace_config(
                {"workspace": {"backend": "virtual"}}, prefix="jobs/j/"
            )
        assert "needs an object store" in str(exc.value)

    def test_object_store_is_reached_through_the_virtual_workspace_module(
        self, monkeypatch
    ):
        """One patch point steers every consumer of the rclone spec."""
        monkeypatch.setattr(
            virtual_workspace,
            "virtual_workspace_rclone_spec",
            lambda: {"type": "s3", "config": {}, "root": "b"},
        )
        co = tier.inject_lite_workspace_config(
            {"workspace": {"backend": "virtual"}}, prefix="p/"
        )
        assert co["workspace"]["mounts"][0]["rclone_spec"]["root"] == "b"

    @pytest.mark.parametrize(
        ("override", "expected"),
        [
            ({"workspace": {"backend": "virtual"}}, True),
            ({"workspace": {"backend": "none"}}, True),
            ({"workspace": {"backend": "sandbox"}}, False),
            (None, False),
        ],
    )
    def test_is_lite_config_override(self, override, expected):
        assert tier.is_lite_config_override(override) is expected
        assert tier.is_lite_config_override(
            override
        ) == workspace_tier_policy_module.is_lite_config_override(override)

    def test_thread_workspace_backend_reads_json_metadata(self):
        thread = {
            "metadata": json.dumps(
                {"config_override": {"workspace": {"backend": "vm"}}}
            )
        }
        assert tier.thread_workspace_backend(thread) == "vm"
        assert tier.thread_workspace_backend({"metadata": {}}) is None


# ---------------------------------------------------------------------------
# Object-store startup checks
# ---------------------------------------------------------------------------


class TestObjectStoreStartupChecks:
    def test_both_seams_durable_is_silent(self):
        env = {"S3_ENDPOINT": "https://s3", "VIRTUAL_WORKSPACE_RCLONE_TYPE": "s3"}
        assert virtual_workspace.object_store_startup_warning(env) is None
        assert virtual_workspace.check_object_store_config(env) is None

    def test_memory_store_is_reported_as_non_durable(self):
        env = {"S3_ENDPOINT": "https://s3", "VIRTUAL_WORKSPACE_RCLONE_TYPE": "memory"}
        msg = virtual_workspace.object_store_startup_warning(env)
        assert "NON-DURABLE" in msg
        assert msg == virtual_workspace_module.object_store_startup_warning(env)

    def test_unset_seams_name_both_failures(self):
        msg = virtual_workspace.object_store_startup_warning({})
        assert "S3_ENDPOINT unset" in msg and "LiteWorkspaceConfigError" in msg
        assert msg == virtual_workspace_module.object_store_startup_warning({})

    def test_warn_only_by_default(self):
        assert virtual_workspace.check_object_store_config({}) is not None

    @pytest.mark.parametrize("flag", ["true", "1", "YES", " yes "])
    def test_required_flag_refuses_to_start(self, flag):
        with pytest.raises(RuntimeError) as exc:
            virtual_workspace.check_object_store_config({"OBJECT_STORE_REQUIRED": flag})
        assert "refuses to start" in str(exc.value)

    def test_required_flag_is_silent_when_the_store_is_durable(self):
        assert (
            virtual_workspace.check_object_store_config(
                {
                    "S3_ENDPOINT": "https://s3",
                    "VIRTUAL_WORKSPACE_RCLONE_TYPE": "s3",
                    "OBJECT_STORE_REQUIRED": "true",
                }
            )
            is None
        )


# ---------------------------------------------------------------------------
# Session workspace tiers
# ---------------------------------------------------------------------------


class TestSessionWorkspacePolicy:
    def test_vm_is_creatable_but_never_a_saved_default(self):
        assert "vm" in wspolicy.SESSION_CREATE_WORKSPACE_BACKENDS
        assert "vm" not in wspolicy.SESSION_WORKSPACE_BACKENDS
        assert wspolicy.default_session_workspace_backend(
            {"workspace_backend": "vm"}
        ) == (wspolicy.SESSION_DEFAULT_WORKSPACE_BACKEND)

    @pytest.mark.parametrize(
        ("settings", "expected"),
        [
            ({"workspace_backend": "sandbox"}, "sandbox"),
            ({"workspace_backend": "bogus"}, "virtual"),
            ({}, "virtual"),
            (None, "virtual"),
        ],
    )
    def test_default_backend_chain(self, settings, expected):
        assert wspolicy.default_session_workspace_backend(settings) == expected
        assert wspolicy.default_session_workspace_backend(
            settings
        ) == session_workspace_policy_module.default_session_workspace_backend(settings)

    def test_workspace_override_accepts_vm_and_rejects_unknown(self):
        assert wspolicy.validated_session_workspace_override(
            {"workspace": {"backend": "vm"}}
        ) == {"backend": "vm"}
        assert wspolicy.validated_session_workspace_override({"workspace": {}}) is None
        assert wspolicy.validated_session_workspace_override(None) is None
        assert wspolicy.validated_session_workspace_override(
            {"workspace": {"word_limit": 5}}
        ) == {"word_limit": 5}
        with pytest.raises(HTTPException) as exc:
            wspolicy.validated_session_workspace_override(
                {"workspace": {"backend": "docker"}}
            )
        assert exc.value.status_code == 400

    def test_ready_timeout_is_per_tier_and_env_tunable(self, monkeypatch):
        monkeypatch.delenv("VM_WS_READY_TIMEOUT_S", raising=False)
        monkeypatch.delenv("WS_READY_TIMEOUT_S", raising=False)
        assert wspolicy.session_ready_timeout_s("vm") == 960
        assert wspolicy.session_ready_timeout_s("sandbox") == 180
        assert wspolicy.session_ready_timeout_s(None) == 180
        monkeypatch.setenv("VM_WS_READY_TIMEOUT_S", "1200")
        assert wspolicy.session_ready_timeout_s("vm") == 1200


# ---------------------------------------------------------------------------
# Session class + execution lane
# ---------------------------------------------------------------------------


class TestSessionClassPolicy:
    @pytest.mark.parametrize(
        ("config", "expected"),
        [
            ({}, None),
            ({"officer": None}, None),
            ({"officer": {}}, None),
            ({"officer": {"enabled": False, "conference": False}}, None),
            (
                {"officer": {"enabled": True}},
                "officer sessions still use the pinned watchdog and wake drain",
            ),
            (
                {"officer": {"conference": True}},
                "conference sessions still use pinned lifecycle wakes",
            ),
            ({"officer": {"enabled": 1}}, "session class configuration is malformed"),
            ({"officer": []}, "session class configuration is malformed"),
            (None, "session class configuration is malformed"),
        ],
    )
    def test_pinned_refusal_is_a_three_state_table(self, config, expected):
        assert classes.session_class_pinned_refusal(config) == expected
        assert classes.session_class_pinned_refusal(
            config
        ) == config_update.session_class_pinned_refusal(config)

    def test_conference_is_checked_before_enabled(self):
        both = {"officer": {"enabled": True, "conference": True}}
        assert classes.session_class_pinned_refusal(both) == (
            "conference sessions still use pinned lifecycle wakes"
        )

    def test_materialized_class_rejects_non_bool_without_coercion(self):
        assert classes.materialized_session_class_override({}) == {
            "enabled": False,
            "conference": False,
        }
        assert classes.materialized_session_class_override(
            {"officer": {"enabled": True}}
        ) == {"enabled": True, "conference": False}
        for bad in ({"officer": {"enabled": 1}}, {"officer": {"conference": "true"}}):
            with pytest.raises(HTTPException) as exc:
                classes.materialized_session_class_override(bad)
            assert exc.value.status_code == 400
        with pytest.raises(HTTPException):
            classes.materialized_session_class_override("nope")

    @pytest.mark.parametrize(
        ("config", "expected"),
        [
            ({}, False),
            ({"officer": {"enabled": False}}, False),
            ({"officer": {"conference": True}}, False),
            ({"officer": {"enabled": True}}, True),
            ({"agent": {"officer": {"enabled": True}}}, True),
            ({"agent": {"officer": {}}}, False),
        ],
    )
    def test_protected_cloud_officer_active_accepts_both_shapes(self, config, expected):
        assert classes.protected_cloud_officer_active(config) is expected
        assert classes.protected_cloud_officer_active(
            config
        ) == session_class_policy_module.protected_cloud_officer_active(config)

    @pytest.mark.parametrize(
        "config", [None, {"agent": 3}, {"officer": 5}, {"officer": {"enabled": 1}}]
    )
    def test_protected_cloud_officer_active_raises_on_malformed(self, config):
        with pytest.raises(ValueError):
            classes.protected_cloud_officer_active(config)

    # -- execution lane ---------------------------------------------------

    @staticmethod
    def _lane_deps(*, enabled=True, in_cluster=True, spec=None, calls=None):
        calls = calls if calls is not None else []

        def _enabled():
            calls.append("enabled")
            return enabled

        def _spec():
            calls.append("spec")
            return spec

        return classes.ExecutionLaneDependencies(
            stateless_session_enabled=_enabled,
            container_provisioner=SimpleNamespace(
                is_available=True, in_cluster=in_cluster
            ),
            virtual_workspace_rclone_spec=_spec,
        )

    def test_pinned_class_short_circuits_before_the_flag_is_read(self):
        calls: list[str] = []
        deps = self._lane_deps(calls=calls)
        assert (
            classes.resolve_thread_execution_lane(
                workspace_backend="sandbox",
                effective_config={"officer": {"enabled": True}},
                dependencies=deps,
            )
            == "pinned"
        )
        assert calls == [], "a pinned-only class must not depend on the pool gate"

    def test_flag_is_consulted_as_a_callable(self):
        calls: list[str] = []
        deps = self._lane_deps(enabled=False, calls=calls)
        assert (
            classes.resolve_thread_execution_lane(
                workspace_backend="none", effective_config={}, dependencies=deps
            )
            == "pinned"
        )
        assert calls == ["enabled"], "the flag must be READ per call, not captured"

    @pytest.mark.parametrize(
        ("backend", "in_cluster", "spec", "expected"),
        [
            ("sandbox", True, None, "stateless"),
            ("sandbox", False, None, "pinned"),
            ("virtual", True, {"type": "s3"}, "stateless"),
            ("virtual", True, {"type": "memory"}, "pinned"),
            ("virtual", True, None, "pinned"),
            ("none", True, None, "stateless"),
            ("vm", True, None, "pinned"),
            (None, True, None, "pinned"),
        ],
    )
    def test_lane_matrix(self, backend, in_cluster, spec, expected):
        deps = self._lane_deps(in_cluster=in_cluster, spec=spec)
        assert (
            classes.resolve_thread_execution_lane(
                workspace_backend=backend, effective_config={}, dependencies=deps
            )
            == expected
        )

    # -- stateless workspace gate ----------------------------------------

    def test_require_stateless_workspace_refusal_carries_the_reason(self):
        thread = {
            "id": "t",
            "metadata": {"config_override": {"workspace": {"backend": "docker"}}},
        }
        with pytest.raises(HTTPException) as exc:
            classes.require_stateless_workspace(thread)
        assert exc.value.status_code == 409
        assert "declared_backend_unsupported" in str(exc.value.detail)

    def test_require_stateless_workspace_admits_a_clean_lite_row(self):
        thread = {
            "id": "t",
            "metadata": {"config_override": {"workspace": {"backend": "none"}}},
        }
        assert classes.require_stateless_workspace(thread) == "none"

    def test_end_gate_admits_only_the_exact_retiring_authority(self):
        base_meta = {
            "config_override": {"workspace": {"backend": "sandbox"}},
            "workspace_container": {
                "provisioner": "k8s",
                "status": "retiring_process_zero",
            },
        }
        ended = {"id": "t", "status": "ended", "metadata": base_meta}
        # No retirement authority in metadata -> falls back to the ordinary gate.
        with pytest.raises(HTTPException) as exc:
            classes.require_stateless_end_workspace(ended)
        assert exc.value.status_code == 409

        # A live (non-ended) thread in the same status is never admitted.
        live = {"id": "t", "status": "active", "metadata": base_meta}
        with pytest.raises(HTTPException):
            classes.require_stateless_end_workspace(live)


# ---------------------------------------------------------------------------
# Session tool policy
# ---------------------------------------------------------------------------


class TestDelegationGate:
    def test_names_without_the_gate_are_emptied(self):
        merged = {"delegation": ["delegate_agent"]}
        toolpolicy.apply_delegation_gate(merged, {"enabled": False})
        assert merged["delegation"] == []

    def test_absent_gate_block_is_off(self):
        merged = {"delegation": ["delegate_agent"]}
        toolpolicy.apply_delegation_gate(merged, None)
        assert merged["delegation"] == []

    @pytest.mark.parametrize("enabled", [1, "true", "yes", [1]])
    def test_only_the_boolean_true_enables(self, enabled):
        merged = {"delegation": ["delegate_agent"]}
        toolpolicy.apply_delegation_gate(merged, {"enabled": enabled})
        assert merged["delegation"] == [], "truthiness must not stand in for the gate"

    def test_gate_and_names_together_bind(self):
        merged = {"delegation": ["delegate_agent"]}
        toolpolicy.apply_delegation_gate(merged, {"enabled": True})
        assert merged["delegation"] == ["delegate_agent"]

    def test_no_names_is_a_no_op_and_creates_no_key(self):
        merged: dict = {}
        toolpolicy.apply_delegation_gate(merged, {"enabled": True})
        assert merged == {}


class TestToolOverrideBoundary:
    def test_with_validated_returns_the_caller_object_when_there_is_no_tools_key(self):
        co = {"llm": {"model": "x"}}
        assert toolpolicy.with_validated_tool_overrides(co) is co
        assert toolpolicy.with_validated_tool_overrides(None) is None

    def test_with_validated_returns_a_new_dict_and_never_mutates(self):
        co = {"llm": {"model": "x"}, "tools": {"research": ["web_search"]}}
        out = toolpolicy.with_validated_tool_overrides(co)
        assert out is not co
        assert co["tools"] == {"research": ["web_search"]}
        assert out["llm"] is co["llm"]

    def test_unknown_category_is_rejected_not_dropped(self):
        with pytest.raises(HTTPException) as exc:
            toolpolicy.validated_tool_overrides({"tools": {"not_a_category": []}})
        assert exc.value.status_code == 400

    @pytest.mark.parametrize(
        ("fn", "group"),
        [
            (toolpolicy.fleet_management_explicitly_disabled, "orchestrator"),
            (toolpolicy.agent_catalog_explicitly_disabled, "agent_catalog"),
            (toolpolicy.workflows_explicitly_disabled, "workflows"),
        ],
    )
    def test_explicitly_disabled_keys_on_the_empty_list_only(self, fn, group):
        assert fn({"tools": {group: []}}) is True
        assert fn({"tools": {group: ["x"]}}) is False
        assert fn({"tools": {}}) is False
        assert fn({}) is False
        assert fn(None) is False

    def test_disabled_markers_are_derived_from_explicit_empty_lists(self):
        markers = toolpolicy.session_tool_group_disabled_markers(
            {
                "tools": {
                    "orchestrator": [],
                    "canvas": [],
                    "workflows": ["x"],
                    "research": [],
                }
            }
        )
        assert markers == {"_fleet_management_disabled": True, "_canvas_disabled": True}
        assert toolpolicy.session_tool_group_disabled_markers({"tools": "nope"}) == {}
        assert toolpolicy.session_tool_group_disabled_markers(None) == {}

    def test_fleet_tools_override_reads_the_orchestrator_category(self):
        assert (
            toolpolicy.validated_session_fleet_tools_override(
                {"tools": {"orchestrator": []}}
            )
            == []
        )
        assert toolpolicy.validated_session_fleet_tools_override({}) is None


class TestToolPolicyPrediction:
    """The resolved and legacy predictions, compared against their consumers.

    The differential cases originally compared the moved functions with the
    copies still in ``main``. Since R1.B10 their only consumer is
    ``services.session_tool_view`` (the tool-groups read and the creation
    preview), so the contract is now that the name *that* module looks up at
    call time is the canonical policy.
    """

    CASES = [
        None,
        {"tools": {"orchestrator": []}},
        {"tools": {"delegation": ["delegate_agent"]}},
        {"tools": {"delegation": ["delegate_agent"]}, "delegation": {"enabled": True}},
        {"tools": {"canvas": []}},
    ]

    @pytest.mark.parametrize("override", CASES)
    def test_merged_policy_matches_its_consumer(self, override):
        kwargs = dict(
            base_config_name="session_base",
            expert_row=None,
            project_overrides=None,
            request_override=override,
        )
        assert toolpolicy.merged_session_tool_policy(**kwargs) == (
            session_tool_view.merged_session_tool_policy(**kwargs)
        )

    @pytest.mark.parametrize("override", CASES)
    def test_legacy_policy_matches_its_consumer(self, override):
        assert toolpolicy.legacy_session_tool_policy("session_base", override) == (
            session_tool_view.legacy_session_tool_policy("session_base", override)
        )

    @pytest.mark.parametrize("override", CASES)
    def test_merged_groups_match_main(self, override):
        kwargs = dict(
            base_config_name="session_base",
            expert_row=None,
            project_overrides=None,
            request_override=override,
        )
        assert toolpolicy.merged_session_tool_groups(**kwargs) == (
            session_tool_policy_module.merged_session_tool_groups(**kwargs)
        )

    @pytest.mark.parametrize("override", CASES)
    def test_legacy_groups_match_main(self, override):
        assert toolpolicy.legacy_session_tool_groups("session_base", override) == (
            session_tool_policy_module.legacy_session_tool_groups(
                "session_base", override
            )
        )

    def test_delegation_gate_applies_to_both_predictions(self):
        named = {"tools": {"delegation": ["delegate_agent"]}}
        merged, _ = toolpolicy.merged_session_tool_policy(
            base_config_name="session_base",
            expert_row=None,
            project_overrides=None,
            request_override=named,
        )
        assert merged["delegation"] == []
        legacy, _ = toolpolicy.legacy_session_tool_policy("session_base", named)
        assert legacy["delegation"] == []

        gated = {**named, "delegation": {"enabled": True}}
        merged_on, _ = toolpolicy.merged_session_tool_policy(
            base_config_name="session_base",
            expert_row=None,
            project_overrides=None,
            request_override=gated,
        )
        assert merged_on["delegation"] == ["delegate_agent"]
        legacy_on, _ = toolpolicy.legacy_session_tool_policy("session_base", gated)
        assert legacy_on["delegation"] == ["delegate_agent"]

    def test_a_malformed_stored_expert_fragment_raises_exactly_as_its_consumer(self):
        """Pinned, not endorsed.

        ``resolve_config`` json-decodes the expert row BEFORE the provenance
        block's own guarded decode, so a corrupt stored fragment escapes as a
        ``JSONDecodeError`` rather than degrading. That is main's behaviour
        today and it moved unchanged; the equivalence is what this asserts.
        """
        kwargs = dict(
            base_config_name="session_base",
            expert_row={"config": "{not json"},
            project_overrides=None,
            request_override=None,
        )
        with pytest.raises(json.JSONDecodeError):
            toolpolicy.merged_session_tool_policy(**kwargs)
        with pytest.raises(json.JSONDecodeError):
            session_tool_view.merged_session_tool_policy(**kwargs)


# ---------------------------------------------------------------------------
# Agent toolset probing
# ---------------------------------------------------------------------------


class TestAgentToolsetProbe:
    def test_origin_fields_distinguish_three_provenances(self):
        prediction = toolset.origin_fields(toolset.unmeasured("no agent"))
        assert prediction["origin"] == "prediction"
        assert prediction["prediction_reason"] == "no agent"
        assert prediction["degraded_reason"] is None
        assert prediction["backend"] is None

        full = toolset.origin_fields(
            toolset.Measurement(
                {"core": ["a"]}, "2026-01-01T00:00:00Z", {"tier": "k8s"}, None
            )
        )
        assert full["origin"] == "agent"
        assert full["degraded_reason"] is None
        assert full["backend"] == {"tier": "k8s"}

        partial = toolset.origin_fields(
            toolset.Measurement({"core": ["a"]}, None, None, "thin", partial=True)
        )
        assert partial["origin"] == "agent_partial"
        assert partial["degraded_reason"] == "thin"
        assert partial["prediction_reason"] is None

    def test_origin_fields_match_the_tool_view_consumer(self):
        for m in (
            toolset.unmeasured("x"),
            toolset.Measurement({"core": []}, "t", {"a": 1}, None),
            toolset.Measurement({"core": []}, None, None, "thin", partial=True),
        ):
            twin = agent_toolset_probe_module.Measurement(
                m.categories, m.observed_at, m.backend, m.reason, m.partial
            )
            assert toolset.origin_fields(m) == session_tool_view.origin_fields(twin)

    @pytest.mark.asyncio
    async def test_no_agent_is_unmeasured_without_touching_the_store(self):
        store = MagicMock()
        store.get_agent = AsyncMock(side_effect=AssertionError("must not be called"))
        m = await toolset.agent_toolset_measurement(
            {}, dependencies=toolset.AgentToolsetDependencies(store=store)
        )
        assert m.categories is None
        assert m.reason == "no agent is attached to this session"

    @pytest.mark.asyncio
    async def test_store_lookup_failure_is_reported_as_unmeasured(self):
        store = MagicMock()
        store.get_agent = AsyncMock(side_effect=RuntimeError("boom"))
        m = await toolset.agent_toolset_measurement(
            {"agent_id": "a"},
            dependencies=toolset.AgentToolsetDependencies(store=store),
        )
        assert m.reason == "the bound agent could not be looked up"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("agent", "reason"),
        [
            (None, "the bound agent has no reachable address"),
            ({"pod_ip": None}, "the bound agent has no reachable address"),
            ({"pod_ip": "1.2.3.4", "status": "offline"}, "the bound agent is offline"),
            ({"pod_ip": "1.2.3.4", "status": "failed"}, "the bound agent is failed"),
            (
                {"pod_ip": "1.2.3.4", "status": "completed"},
                "the bound agent is completed",
            ),
            (
                {"pod_ip": "1.2.3.4", "status": "booting"},
                "the bound agent is still booting",
            ),
        ],
    )
    async def test_skip_reasons_are_specific(self, agent, reason):
        store = MagicMock()
        store.get_agent = AsyncMock(return_value=agent)
        m = await toolset.agent_toolset_measurement(
            {"agent_id": "a"},
            dependencies=toolset.AgentToolsetDependencies(store=store),
        )
        assert m.categories is None and m.reason == reason

    @pytest.mark.asyncio
    async def test_ready_is_deliberately_probed(self, monkeypatch):
        store = MagicMock()
        store.get_agent = AsyncMock(
            return_value={"pod_ip": "1.2.3.4", "status": "ready"}
        )
        probed: list[dict] = []

        async def fake_probe(agent):
            probed.append(agent)
            return toolset.unmeasured("probed")

        monkeypatch.setattr(toolset, "agent_toolset_probe", fake_probe)
        m = await toolset.agent_toolset_measurement(
            {"agent_id": "a"},
            dependencies=toolset.AgentToolsetDependencies(store=store),
        )
        assert probed and m.reason == "probed"

    @pytest.mark.asyncio
    async def test_one_deadline_covers_the_whole_probe(self, monkeypatch):
        import asyncio

        store = MagicMock()
        store.get_agent = AsyncMock(
            return_value={"pod_ip": "1.2.3.4", "status": "session"}
        )

        async def slow(agent):
            await asyncio.sleep(5)

        monkeypatch.setattr(toolset, "agent_toolset_probe", slow)
        monkeypatch.setattr(toolset, "AGENT_TOOLSET_BUDGET_S", 0.01)
        m = await toolset.agent_toolset_measurement(
            {"agent_id": "a"},
            dependencies=toolset.AgentToolsetDependencies(store=store),
        )
        assert m.reason == "the agent did not answer within the probe budget"


# ---------------------------------------------------------------------------
# Grant enforcement
# ---------------------------------------------------------------------------


def _grant_deps(
    *,
    store=None,
    user_experts=True,
    runner_grants=None,
    dispatch=None,
    vm=None,
    calls=None,
):
    calls = calls if calls is not None else []

    async def _user_experts():
        calls.append("user_experts_enabled")
        return user_experts

    async def _runner(**kwargs):
        calls.append(("resolve_runner_grants", kwargs))
        return runner_grants

    async def _dispatch(*args, **kwargs):
        calls.append(("enforce_dispatch_grants", args, kwargs))
        if dispatch is not None:
            await dispatch(*args, **kwargs)

    async def _vm(user, *, job_needs_vm):
        calls.append(("check_vm_permission", job_needs_vm))
        if vm is not None:
            await vm(user, job_needs_vm=job_needs_vm)

    return grants.GrantEnforcementDependencies(
        store=store if store is not None else MagicMock(),
        user_experts_enabled=_user_experts,
        resolve_runner_grants=_runner,
        enforce_dispatch_grants=_dispatch,
        check_vm_permission=_vm,
    )


class TestGrantRefusalShapes:
    def test_grant_denied_carries_a_plain_list(self):
        exc = grants.GrantDenied(["a:1", "b:2"])
        assert exc.violations == ["a:1", "b:2"]
        assert str(exc) == "a:1; b:2"

    def test_violations_detail_is_the_rendered_string(self):
        detail = grants.grant_violations_detail(["shell_tools", "vm_workspace"])
        assert (
            detail == "config exceeds your capability grants: shell_tools; vm_workspace"
        )
        assert detail == grant_enforcement_module.grant_violations_detail(
            ["shell_tools", "vm_workspace"]
        )

    def test_endpoint_violations_detail_is_the_rendered_string(self):
        detail = sessioncfg.endpoint_violations_detail(["llm: no url"])
        assert detail == "session cannot start — unusable model transport: llm: no url"


class TestStripAcknowledgedGrants:
    def test_no_violations_returns_the_caller_fragment_itself(self):
        fragment = {"tools": {"shell": True}}
        out = grants.strip_acknowledged_grants(
            fragment, {"shell_tools": True}, {"shell_tools"}
        )
        assert out is fragment

    def test_unacknowledged_drift_leaves_everything_in_place(self):
        fragment = {"tools": {"shell": True}, "autonomy": "full"}
        out = grants.strip_acknowledged_grants(fragment, {"shell_tools": False}, set())
        assert out is fragment, "the dispatch PEP must still deny on all of it"

    def test_acknowledged_violation_is_stripped_into_a_new_fragment(self):
        fragment = {"tools": {"shell": True}}
        out = grants.strip_acknowledged_grants(
            fragment, {"shell_tools": False}, {"shell_tools"}
        )
        assert out == grant_enforcement_module.strip_acknowledged_grants(
            {"tools": {"shell": True}}, {"shell_tools": False}, {"shell_tools"}
        )
        assert not out.get("tools", {}).get("shell")


class TestGrantEnforcementSeams:
    @pytest.mark.asyncio
    async def test_dispatch_grants_reach_the_injected_resolver(self):
        calls: list = []
        deps = _grant_deps(runner_grants=None, calls=calls)
        await grants.enforce_dispatch_grants(
            {"tools": {"shell": True}},
            runner_user_id="u",
            project_ids=[],
            dependencies=deps,
        )
        assert calls[0][0] == "resolve_runner_grants"
        assert calls[0][1]["runner_kind"] == "user"

    @pytest.mark.asyncio
    async def test_admin_bypass_short_circuits_before_evaluate(self):
        deps = _grant_deps(runner_grants=None)
        await grants.enforce_dispatch_grants(
            {"autonomy": "full"},
            runner_user_id="admin",
            project_ids=[],
            dependencies=deps,
        )

    @pytest.mark.asyncio
    async def test_dispatch_grants_raise_grant_denied(self):
        deps = _grant_deps(runner_grants={"shell_tools": False})
        with pytest.raises(grants.GrantDenied) as exc:
            await grants.enforce_dispatch_grants(
                {"tools": {"shell": True}},
                runner_user_id="u",
                project_ids=[],
                dependencies=deps,
            )
        assert exc.value.violations

    @pytest.mark.asyncio
    async def test_lifecycle_runner_gets_the_raised_autonomy_ceiling(self, monkeypatch):
        """The elevated ceiling is a COPY; the resolver's own dict is untouched."""
        from orchestrator.services import grants_service

        resolved = {"autonomy_ceiling": "review", "shell_tools": False}
        monkeypatch.setattr(
            grants_service, "resolve_grants_for", AsyncMock(return_value=resolved)
        )
        store = MagicMock()
        store.get_user = AsyncMock(return_value={"id": "u", "is_admin": False})
        deps = _grant_deps(store=store)

        out = await grants.resolve_runner_grants(
            runner_user_id="u",
            project_ids=[],
            runner_kind="lifecycle",
            dependencies=deps,
        )
        assert out["autonomy_ceiling"] == "full"
        assert out["shell_tools"] is False
        assert resolved["autonomy_ceiling"] == "review"

        plain = await grants.resolve_runner_grants(
            runner_user_id="u", project_ids=[], dependencies=deps
        )
        assert plain is resolved

    @pytest.mark.asyncio
    async def test_admin_resolver_returns_none(self):
        store = MagicMock()
        store.get_user = AsyncMock(return_value={"id": "u", "is_admin": True})
        deps = _grant_deps(store=store)
        assert (
            await grants.resolve_runner_grants(
                runner_user_id="u", project_ids=[], dependencies=deps
            )
            is None
        )

    @pytest.mark.asyncio
    async def test_session_create_translates_denial_to_422(self):
        async def deny(*_a, **_k):
            raise grants.GrantDenied(["shell_tools: denied"])

        deps = _grant_deps(dispatch=deny)
        with pytest.raises(HTTPException) as exc:
            await grants.enforce_session_create_grants(
                {"tools": {"shell": True}},
                user_id="u",
                project_ids=[],
                dependencies=deps,
            )
        assert exc.value.status_code == 422
        assert exc.value.detail == grants.grant_violations_detail(
            ["shell_tools: denied"]
        )

    @pytest.mark.asyncio
    async def test_job_create_no_ops_without_a_principal_or_an_override(self):
        calls: list = []
        deps = _grant_deps(calls=calls)
        await grants.enforce_job_create_grants(
            {"tools": {}}, user_id=None, project_ids=[], dependencies=deps
        )
        await grants.enforce_job_create_grants(
            None, user_id="u", project_ids=[], dependencies=deps
        )
        await grants.enforce_job_create_grants(
            {}, user_id="u", project_ids=[], dependencies=deps
        )
        assert calls == [], "no principal or no override means nothing to evaluate"

    @pytest.mark.asyncio
    async def test_expert_save_prelude_reaches_the_injected_kill_switch(self):
        calls: list = []
        deps = _grant_deps(user_experts=False, calls=calls)
        with pytest.raises(HTTPException) as exc:
            await grants.enforce_expert_save_prelude(MagicMock(), dependencies=deps)
        assert exc.value.status_code == 403
        assert calls == ["user_experts_enabled"]

    @pytest.mark.asyncio
    async def test_user_experts_enabled_fails_open(self):
        store = MagicMock()
        store.get_system_setting = AsyncMock(side_effect=RuntimeError("db down"))
        deps = _grant_deps(store=store)
        assert await grants.user_experts_enabled(dependencies=deps) is True

        store.get_system_setting = AsyncMock(return_value={"value": {"enabled": False}})
        assert await grants.user_experts_enabled(dependencies=deps) is False

        store.get_system_setting = AsyncMock(return_value=None)
        assert await grants.user_experts_enabled(dependencies=deps) is True

    @pytest.mark.asyncio
    async def test_save_grants_admin_bypass_leaves_the_config_untouched(self):
        deps = _grant_deps()
        config = {"tools": {"shell": True}}
        await grants.enforce_save_grants(
            config, user={"is_admin": True}, dependencies=deps
        )
        stripped, dropped = await grants.strip_save_grants(
            config, user={"is_admin": True}, dependencies=deps
        )
        assert stripped is config and dropped == []


class TestWorkspaceUpgradeGate:
    @pytest.mark.asyncio
    async def test_vm_target_runs_the_operator_gate_and_the_pdp(self):
        calls: list = []
        store = MagicMock()
        store.get_user = AsyncMock(return_value={"id": "u", "is_admin": False})
        store.get_projects_for_user = AsyncMock(return_value=[])
        deps = _grant_deps(store=store, calls=calls)
        await grants.enforce_workspace_upgrade_grants_for_config(
            owner_id="u",
            config_override={"workspace": {"backend": "virtual"}, "tools": {}},
            target_tier="vm",
            dependencies=deps,
        )
        kinds = [c[0] if isinstance(c, tuple) else c for c in calls]
        assert "check_vm_permission" in kinds
        dispatch = next(c for c in calls if c[0] == "enforce_dispatch_grants")
        assert dispatch[1][0]["workspace"] == {"backend": "vm"}

    @pytest.mark.asyncio
    async def test_sandbox_target_skips_the_operator_gate(self):
        calls: list = []
        store = MagicMock()
        store.get_user = AsyncMock(return_value=None)
        deps = _grant_deps(store=store, calls=calls)
        await grants.enforce_workspace_upgrade_grants_for_config(
            owner_id=None,
            config_override=None,
            target_tier="sandbox",
            dependencies=deps,
        )
        kinds = [c[0] if isinstance(c, tuple) else c for c in calls]
        assert "check_vm_permission" not in kinds

    @pytest.mark.asyncio
    async def test_denial_becomes_403(self):
        async def deny(*_a, **_k):
            raise grants.GrantDenied(["vm_workspace: denied"])

        store = MagicMock()
        store.get_user = AsyncMock(return_value={"id": "u", "is_admin": False})
        store.get_projects_for_user = AsyncMock(return_value=[])
        deps = _grant_deps(store=store, dispatch=deny)
        with pytest.raises(HTTPException) as exc:
            await grants.enforce_workspace_upgrade_grants_for_config(
                owner_id="u",
                config_override={},
                target_tier="sandbox",
                dependencies=deps,
            )
        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_thread_and_job_wrappers_extract_their_own_override(self):
        seen: list = []

        store = MagicMock()
        store.get_user = AsyncMock(return_value=None)
        deps = _grant_deps(store=store, calls=seen)

        await grants.enforce_workspace_upgrade_grants(
            {
                "user_id": None,
                "metadata": json.dumps({"config_override": {"tools": {"shell": True}}}),
            },
            target_tier="sandbox",
            dependencies=deps,
        )
        await grants.enforce_job_workspace_upgrade_grants(
            {
                "user_id": None,
                "config_override": json.dumps({"tools": {"shell": True}}),
            },
            target_tier="sandbox",
            dependencies=deps,
        )
        posts = [c[1][0] for c in seen if c[0] == "enforce_dispatch_grants"]
        assert len(posts) == 2
        assert all(p["tools"] == {"shell": True} for p in posts)
        assert all(p["workspace"] == {"backend": "sandbox"} for p in posts)

    @pytest.mark.asyncio
    async def test_malformed_stored_json_degrades_to_an_empty_override(self):
        seen: list = []
        store = MagicMock()
        store.get_user = AsyncMock(return_value=None)
        deps = _grant_deps(store=store, calls=seen)
        await grants.enforce_workspace_upgrade_grants(
            {"user_id": None, "metadata": "{not json"},
            target_tier="sandbox",
            dependencies=deps,
        )
        post = next(c[1][0] for c in seen if c[0] == "enforce_dispatch_grants")
        assert post == {"workspace": {"backend": "sandbox"}}


# ---------------------------------------------------------------------------
# VM operator gate
# ---------------------------------------------------------------------------


class TestVmWorkspacePolicy:
    @pytest.mark.asyncio
    async def test_no_op_when_the_job_does_not_need_a_vm(self):
        store = MagicMock()
        store.get_system_setting = AsyncMock(
            side_effect=AssertionError("must not read")
        )
        await vmpolicy.check_vm_permission(
            None,
            job_needs_vm=False,
            dependencies=vmpolicy.VmPermissionDependencies(store=store),
        )

    @pytest.mark.asyncio
    async def test_kill_switch_blocks_admins_too(self):
        store = MagicMock()
        store.get_system_setting = AsyncMock(return_value={"value": {"enabled": False}})
        with pytest.raises(HTTPException) as exc:
            await vmpolicy.check_vm_permission(
                {"is_admin": True},
                job_needs_vm=True,
                dependencies=vmpolicy.VmPermissionDependencies(store=store),
            )
        assert exc.value.status_code == 403
        assert "globally disabled" in str(exc.value.detail)

    @pytest.mark.asyncio
    async def test_setting_read_uses_the_shared_key(self):
        store = MagicMock()
        store.get_system_setting = AsyncMock(return_value=None)
        store.user_can_use_vm = AsyncMock(return_value=True)
        await vmpolicy.check_vm_permission(
            {"is_admin": False},
            job_needs_vm=True,
            dependencies=vmpolicy.VmPermissionDependencies(store=store),
        )
        store.get_system_setting.assert_awaited_once_with(
            vmpolicy.VM_WORKSPACES_SETTING_KEY
        )
        assert vmpolicy.VM_WORKSPACES_SETTING_KEY == "vm_workspaces"

    @pytest.mark.asyncio
    async def test_db_failure_falls_through_to_the_per_user_check(self):
        store = MagicMock()
        store.get_system_setting = AsyncMock(side_effect=RuntimeError("db down"))
        store.user_can_use_vm = AsyncMock(return_value=False)
        with pytest.raises(HTTPException) as exc:
            await vmpolicy.check_vm_permission(
                {"is_admin": False},
                job_needs_vm=True,
                dependencies=vmpolicy.VmPermissionDependencies(store=store),
            )
        assert "not permitted" in str(exc.value.detail)

    @pytest.mark.asyncio
    async def test_anonymous_caller_is_refused(self):
        store = MagicMock()
        store.get_system_setting = AsyncMock(return_value=None)
        store.user_can_use_vm = AsyncMock(
            side_effect=AssertionError("no user to check")
        )
        with pytest.raises(HTTPException):
            await vmpolicy.check_vm_permission(
                None,
                job_needs_vm=True,
                dependencies=vmpolicy.VmPermissionDependencies(store=store),
            )

    @pytest.mark.parametrize(
        ("ctx", "expected"),
        [
            (None, False),
            ({}, False),
            ({"status": "deleted"}, False),
            ({"status": "deleting"}, False),
            ({"status": "ready"}, True),
            ({"status": "suspended"}, True),
            ({"status": None}, True),
        ],
    )
    def test_vm_needs_release_matches_main(self, ctx, expected):
        assert vmpolicy.vm_needs_release(ctx) is expected
        assert vmpolicy.vm_needs_release(
            ctx
        ) == vm_workspace_policy_module.vm_needs_release(ctx)


# ---------------------------------------------------------------------------
# Public job create ingress
# ---------------------------------------------------------------------------


class TestPublicJobIngress:
    @staticmethod
    def _job(**kw):
        base = dict(
            parent_job_id="p",
            creation_order=3,
            worktree_path="/tmp/x",
            delegation_context={"a": 1},
            thread_id="t",
            context={"snapshot": 1, "user_note": "keep", "verification_rounds": []},
            config_override={"runner_kind": "system", "llm": {"model": "m"}},
        )
        base.update(kw)
        return SimpleNamespace(**base)

    def test_public_markers_are_stripped(self):
        job = self._job()
        ingress.strip_public_job_reserved_markers(job)
        assert job.parent_job_id is None
        assert job.creation_order is None
        assert job.worktree_path is None
        assert job.delegation_context is None
        assert job.thread_id is None
        assert job.context == {"user_note": "keep"}
        assert job.config_override == {"llm": {"model": "m"}}

    def test_stripping_matches_main(self):
        moved, original = self._job(), self._job()
        ingress.strip_public_job_reserved_markers(moved)
        job_create_ingress_module.strip_public_job_reserved_markers(original)
        assert moved.context == original.context
        assert moved.config_override == original.config_override

    def test_raw_officer_claim_context_is_stripped_for_internal_bodies_too(self):
        job = SimpleNamespace(
            context={"officer_admission": 1, "evidence_manifest": {}, "kept": True}
        )
        ingress.strip_raw_officer_claim_context(job)
        assert job.context == {"kept": True}

    def test_operator_pause_hold_cannot_be_seeded_at_creation(self):
        seeded = {
            "_operator_pause_hold": {"version": 1, "hold_id": "forged"},
            "last_operator_pause_hold": {"hold_id": "forged"},
            "kept": True,
        }
        internal = SimpleNamespace(context=dict(seeded))
        ingress.strip_raw_officer_claim_context(internal)
        public = self._job(context=dict(seeded))
        ingress.strip_public_job_reserved_markers(public)
        body = JobCreate(description="seeded hold", context=dict(seeded))

        assert internal.context == {"kept": True}
        assert public.context == {"kept": True}
        assert body.context == {"kept": True}

    def test_non_dict_context_is_left_alone(self):
        job = self._job(context=None, config_override=None)
        ingress.strip_public_job_reserved_markers(job)
        assert job.context is None and job.config_override is None

    def test_the_two_reserved_sets_are_distinct(self):
        assert (
            ingress.PUBLIC_JOB_CONFIG_RESERVED_KEYS
            < ingress.PUBLIC_JOB_CONTEXT_RESERVED_KEYS
        )
        assert (
            ingress.PUBLIC_JOB_CONTEXT_RESERVED_KEYS
            == job_create_ingress_module.PUBLIC_JOB_CONTEXT_RESERVED_KEYS
        )
        assert (
            ingress.PUBLIC_JOB_CONFIG_RESERVED_KEYS
            == job_create_ingress_module.PUBLIC_JOB_CONFIG_RESERVED_KEYS
        )


# ---------------------------------------------------------------------------
# Session create request validators
# ---------------------------------------------------------------------------


class TestSessionCreateValidators:
    @pytest.mark.parametrize(
        "value", ["none", "LOW", " medium ", "high", "xhigh", "max"]
    )
    def test_reasoning_level_vocabulary(self, value):
        # This used to assert parity against ``main._validated_reasoning_level``.
        # R1.B07 moved the last in-``main`` consumer of that alias out to
        # ``services.officer_post_policy``, so the alias is gone and the second
        # half compared the service against itself. The vocabulary is the thing
        # under test, and ``overrides`` is now its only owner.
        assert overrides.validated_reasoning_level(value) == value.strip().lower()

    @pytest.mark.parametrize("value", ["", None, "ultra", 5])
    def test_reasoning_level_fails_loud(self, value):
        with pytest.raises(HTTPException) as exc:
            overrides.validated_reasoning_level(value)
        assert exc.value.status_code == 400

    def test_officer_override_admits_only_known_keys(self):
        with pytest.raises(HTTPException) as exc:
            overrides.validated_session_officer_override({"officer": {"nope": 1}})
        assert "Unknown officer override keys" in str(exc.value.detail)

    def test_officer_enabled_and_conference_use_the_same_coercion_vocabulary(self):
        out = overrides.validated_session_officer_override(
            {"officer": {"enabled": "true", "conference": 1}}
        )
        assert out == {"enabled": True, "conference": True}
        assert overrides.validated_session_officer_override(
            {"officer": {"enabled": "maybe"}}
        ) == {"enabled": False}

    def test_officer_numeric_bounds_are_clamped_at_zero_and_fail_loud(self):
        assert overrides.validated_session_officer_override(
            {"officer": {"sleep_min_minutes": -4, "daily_token_ceiling": "12"}}
        ) == {"daily_token_ceiling": 12, "sleep_min_minutes": 0}
        with pytest.raises(HTTPException):
            overrides.validated_session_officer_override(
                {"officer": {"max_actions_per_wake": "abc"}}
            )

    def test_slot_spend_ceilings_are_post_owned_and_refused(self):
        with pytest.raises(HTTPException) as exc:
            overrides.validated_session_officer_override(
                {"officer": {"slots": {"a": {"spend_ceiling_daily": 5}}}}
            )
        assert "owned by the Officer Post" in str(exc.value.detail)

    def test_empty_and_absent_officer_blocks_are_none(self):
        assert overrides.validated_session_officer_override({"officer": {}}) is None
        assert overrides.validated_session_officer_override({}) is None
        assert overrides.validated_session_officer_override(None) is None

    def test_officer_override_matches_the_config_commit_core(self):
        for co in (
            {"officer": {"enabled": True, "sleep_min_minutes": 5}},
            {"officer": {"conference": True}},
            {},
        ):
            assert overrides.validated_session_officer_override(
                co
            ) == config_update.validated_session_officer_override(co)

    def test_post_owned_fragment_materializes_safe_absent_values(self):
        seen: list = []

        def fake_patch(body):
            seen.append(body)
            return ({"officer": {"auto_pull": True}}, None, {})

        out = overrides.validated_post_owned_officer_create_fragment(
            {"officer": {"auto_pull": True, "ignored": 1}},
            validated_officer_post_patch=fake_patch,
        )
        assert seen == [{"auto_pull": True}], (
            "only Post-owned keys reach the B07 validator"
        )
        assert out == {
            "auto_pull": True,
            "worker_spend_ceiling_daily": None,
            "slots": None,
        }

    def test_post_owned_fragment_rejects_a_non_object_snapshot(self):
        assert (
            overrides.validated_post_owned_officer_create_fragment(
                None, validated_officer_post_patch=MagicMock()
            )
            is None
        )
        with pytest.raises(RuntimeError):
            overrides.validated_post_owned_officer_create_fragment(
                [], validated_officer_post_patch=MagicMock()
            )
        with pytest.raises(HTTPException):
            overrides.validated_post_owned_officer_create_fragment(
                {"officer": 5}, validated_officer_post_patch=MagicMock()
            )

    @pytest.mark.parametrize(
        ("config", "expected"),
        [
            ({}, None),
            ({"officer": {}}, None),
            ({"officer": {"auto_pull": False}}, None),
            ({"officer": {"auto_pull": True}}, "auto_pull"),
            ({"officer": {"auto_pull": 1}}, "auto_pull"),
            (
                {"officer": {"worker_spend_ceiling_daily": 5}},
                "worker_spend_ceiling_daily",
            ),
            (
                {"officer": {"slots": {"a": {"spend_ceiling_daily": 1}}}},
                "slots.*.spend_ceiling_daily",
            ),
        ],
    )
    def test_effective_post_owned_refusal_names_the_field(self, config, expected):
        assert overrides.effective_officer_post_owned_refusal(config) == expected
        assert overrides.effective_officer_post_owned_refusal(
            config
        ) == session_create_overrides_module.effective_officer_post_owned_refusal(
            config
        )


# ---------------------------------------------------------------------------
# Session config resolution (the PDP)
# ---------------------------------------------------------------------------


def _session_deps(**overrides_):
    """A dependency object whose every field records that it was reached."""
    calls: list[str] = []

    def rec(name, value):
        async def _async(*_a, **_k):
            calls.append(name)
            return value

        return _async

    store = MagicMock()
    store.fetchrow = AsyncMock(return_value=None)
    store.get_user_settings = AsyncMock(return_value={})
    store.resolve_default_for_capability = AsyncMock(return_value=None)
    store.get_expert_by_id = AsyncMock(return_value=None)
    store.get_expert_visible_by_id = AsyncMock(return_value=None)
    store.get_user = AsyncMock(return_value=None)
    store.get_project_expert_link = AsyncMock(return_value=None)

    def _experts_gate():
        calls.append("is_experts_db_enabled")
        return True

    fields = dict(
        store=store,
        is_experts_db_enabled=_experts_gate,
        user_experts_enabled=rec("user_experts_enabled", True),
        resolve_runner_grants=rec("resolve_runner_grants", None),
        enforce_dispatch_grants=rec("enforce_dispatch_grants", None),
        gather_in_scope_skills=rec("gather_in_scope_skills", {}),
        seed_registry_model_overrides=rec("seed_registry_model_overrides", None),
        inject_thread_dispatch_credentials=rec(
            "inject_thread_dispatch_credentials", {}
        ),
        thread_project_ids=rec("thread_project_ids", []),
        thread_has_knowledge_scope=rec("thread_has_knowledge_scope", False),
    )
    fields.update(overrides_)
    return sessioncfg.SessionConfigDependencies(**fields), calls


class TestSessionConfigResolution:
    @pytest.mark.asyncio
    async def test_disabled_experts_short_circuit_with_a_status(self):
        deps, calls = _session_deps()
        deps = sessioncfg.SessionConfigDependencies(
            **{**deps.__dict__, "is_experts_db_enabled": lambda: False}
        )
        status: dict = {}
        assert (
            await sessioncfg.resolve_session_config(
                {"id": "22222222-2222-4222-8222-222222222222"},
                {},
                status=status,
                dependencies=deps,
            )
            is None
        )
        assert status == {"state": "disabled"}

    @pytest.mark.asyncio
    async def test_outbox_opt_in_resolves_the_bundled_base_with_experts_off(self):
        deps, calls = _session_deps()
        deps = sessioncfg.SessionConfigDependencies(
            **{**deps.__dict__, "is_experts_db_enabled": lambda: False}
        )
        status: dict = {}
        out = await sessioncfg.resolve_session_config(
            {"id": "22222222-2222-4222-8222-222222222222", "user_id": None},
            {"expert_id": "should-be-ignored"},
            status=status,
            resolve_base_when_experts_disabled=True,
            dependencies=deps,
        )
        assert status["state"] == "ok" and out is not None
        deps.store.get_expert_by_id.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_every_injected_collaborator_is_reached_on_the_happy_path(self):
        deps, calls = _session_deps()
        out = await sessioncfg.resolve_session_config(
            {"id": "11111111-1111-4111-8111-111111111111", "user_id": "u"},
            {},
            dependencies=deps,
        )
        assert out is not None
        for name in (
            "is_experts_db_enabled",
            "user_experts_enabled",
            "gather_in_scope_skills",
            "seed_registry_model_overrides",
            "enforce_dispatch_grants",
            "thread_project_ids",
            "thread_has_knowledge_scope",
            "inject_thread_dispatch_credentials",
        ):
            assert name in calls, f"{name} was never consulted"

    @pytest.mark.asyncio
    async def test_the_same_module_sibling_patch_point_is_reached(self, monkeypatch):
        """The re-point target for the three suites that stubbed main's copy.

        ``resolve_session_config`` resolves ``resolve_session_account_defaults``
        in THIS module's namespace, so patching it on ``orchestrator.main``
        after the extraction would be green-but-inert. Patching it here is what
        steers, and the sentinel below is a value only the stub can produce.
        """
        seen: dict = {}

        async def stub(user_id, all_user_settings=None, *, dependencies):
            seen["called"] = True
            return {"llm": {"model": "sentinel-account-model"}}

        def spy(**kwargs):
            seen["base_defaults"] = kwargs["base_defaults"]
            kwargs["capture"]["merged_fragment"] = {}
            return {"agent": {}}

        monkeypatch.setattr(sessioncfg, "resolve_session_account_defaults", stub)
        monkeypatch.setattr(sessioncfg, "resolve_config", spy)
        deps, _ = _session_deps()
        await sessioncfg.resolve_session_config(
            {"id": "22222222-2222-4222-8222-222222222222", "user_id": "u"},
            {},
            dependencies=deps,
        )
        assert seen["called"] is True
        assert seen["base_defaults"] == {"llm": {"model": "sentinel-account-model"}}

    @pytest.mark.asyncio
    async def test_a_project_scoped_thread_skips_the_mount_lookup(self):
        deps, calls = _session_deps()
        await sessioncfg.resolve_session_config(
            {
                "id": "22222222-2222-4222-8222-222222222222",
                "user_id": "u",
                "project_id": "p",
            },
            {},
            dependencies=deps,
        )
        assert "thread_project_ids" not in calls

    @pytest.mark.asyncio
    async def test_grant_denied_propagates_and_records_the_violations(self):
        async def deny(*_a, **_k):
            raise grants.GrantDenied(["shell_tools: denied"])

        deps, _ = _session_deps(enforce_dispatch_grants=deny)
        status: dict = {}
        with pytest.raises(grants.GrantDenied):
            await sessioncfg.resolve_session_config(
                {"id": "22222222-2222-4222-8222-222222222222", "user_id": "u"},
                {},
                status=status,
                dependencies=deps,
            )
        assert status["state"] == "denied"
        assert status["grant_violations"] == ["shell_tools: denied"]

    @pytest.mark.asyncio
    async def test_any_other_failure_falls_back_to_config_name(self):
        async def boom(*_a, **_k):
            raise RuntimeError("resolve exploded")

        deps, _ = _session_deps(gather_in_scope_skills=boom)
        status: dict = {}
        assert (
            await sessioncfg.resolve_session_config(
                {"id": "22222222-2222-4222-8222-222222222222", "user_id": "u"},
                {},
                status=status,
                dependencies=deps,
            )
            is None
        )
        assert status["state"] == "error"

    @pytest.mark.asyncio
    async def test_a_uuid_config_name_resolves_onto_the_session_base(self, monkeypatch):
        seen: dict = {}

        def spy(**kwargs):
            seen.update(kwargs)
            kwargs["capture"]["merged_fragment"] = {}
            return {"agent": {}}

        monkeypatch.setattr(sessioncfg, "resolve_config", spy)
        deps, _ = _session_deps()
        await sessioncfg.resolve_session_config(
            {
                "id": "22222222-2222-4222-8222-222222222222",
                "user_id": "u",
                "config_name": "11111111-1111-4111-8111-111111111111",
            },
            {},
            dependencies=deps,
        )
        assert seen["base_config_name"] == "session_base"

    @pytest.mark.asyncio
    async def test_attach_time_override_wins_over_the_stored_one(self, monkeypatch):
        seen: dict = {}

        def spy(**kwargs):
            seen.update(kwargs)
            kwargs["capture"]["merged_fragment"] = {}
            return {"agent": {}}

        monkeypatch.setattr(sessioncfg, "resolve_config", spy)

        async def passthrough(request_override, **_k):
            return request_override

        deps, _ = _session_deps(seed_registry_model_overrides=passthrough)
        await sessioncfg.resolve_session_config(
            {"id": "22222222-2222-4222-8222-222222222222", "user_id": "u"},
            {"config_override": {"stored": True}},
            config_override={"attach": True},
            dependencies=deps,
        )
        assert seen["request_override"] == {"attach": True}

    @pytest.mark.asyncio
    async def test_disabled_tool_markers_are_derived_from_the_merged_fragment(
        self, monkeypatch
    ):
        def spy(**kwargs):
            kwargs["capture"]["merged_fragment"] = {"tools": {"orchestrator": []}}
            return {"agent": {}}

        monkeypatch.setattr(sessioncfg, "resolve_config", spy)

        async def echo(blob, callback):
            await callback({})
            return blob

        monkeypatch.setattr(sessioncfg, "inject_blob_credentials", echo)
        deps, _ = _session_deps()
        out = await sessioncfg.resolve_session_config(
            {"id": "22222222-2222-4222-8222-222222222222", "user_id": "u"},
            {},
            dependencies=deps,
        )
        assert out["agent"]["_fleet_management_disabled"] is True


class TestAcknowledgedGrantStrip:
    @pytest.mark.asyncio
    async def test_no_acknowledgement_means_no_hook(self):
        deps, calls = _session_deps()
        assert (
            await sessioncfg.acknowledged_grant_strip(
                {}, user_id="u", project_id=None, dependencies=deps
            )
            is None
        )
        assert "resolve_runner_grants" not in calls

    @pytest.mark.asyncio
    async def test_admin_bypass_means_no_hook(self):
        deps, calls = _session_deps()
        assert (
            await sessioncfg.acknowledged_grant_strip(
                {"config_drift_ack": {"grant:shell_tools": "revoked"}},
                user_id="u",
                project_id=None,
                dependencies=deps,
            )
            is None
        )
        assert "resolve_runner_grants" in calls

    @pytest.mark.asyncio
    async def test_hook_strips_only_the_acknowledged_grant(self):
        async def runner(**_k):
            return {"shell_tools": False}

        deps, _ = _session_deps(resolve_runner_grants=runner)
        hook = await sessioncfg.acknowledged_grant_strip(
            {"config_drift_ack": {"grant:shell_tools": "revoked"}},
            user_id="u",
            project_id=None,
            dependencies=deps,
        )
        assert hook is not None
        assert not hook({"tools": {"shell": True}}).get("tools", {}).get("shell")


class TestAccountDefaults:
    @pytest.mark.asyncio
    async def test_anonymous_caller_gets_an_empty_layer(self):
        deps, _ = _session_deps()
        assert (
            await sessioncfg.account_defaults_layer(None, "session", dependencies=deps)
            == {}
        )
        assert (
            await sessioncfg.resolve_session_account_defaults(None, dependencies=deps)
            == {}
        )

    @pytest.mark.asyncio
    async def test_worker_layer_is_only_the_model_floor(self):
        store = MagicMock()
        store.get_user_settings = AsyncMock(
            return_value={
                "default_model": "m",
                "default_reasoning_level": "high",
                "persistent_agent": {"model": "p", "workspace_backend": "sandbox"},
            }
        )
        store.resolve_default_for_capability = AsyncMock(return_value=None)
        deps, _ = _session_deps(store=store)
        worker = await sessioncfg.account_defaults_layer(
            "u", "worker", dependencies=deps
        )
        assert worker == {"llm": {"model": "m", "reasoning_level": "high"}}
        assert "workspace" not in worker

    @pytest.mark.asyncio
    async def test_session_layer_carries_the_workspace_backend(self):
        store = MagicMock()
        store.get_user_settings = AsyncMock(
            return_value={
                "default_model": "m",
                "persistent_agent": {
                    "model": "p",
                    "permission_mode": "supervised",
                    "headless_mode": "attentive",
                    "headless_attention_sleep_minutes": "7",
                    "workspace_backend": "sandbox",
                },
            }
        )
        store.resolve_default_for_capability = AsyncMock(return_value=None)
        deps, _ = _session_deps(store=store)
        layer = await sessioncfg.account_defaults_layer(
            "u", "session", dependencies=deps
        )
        assert layer["workspace"] == {"backend": "sandbox"}
        assert layer["llm"]["model"] == "p", (
            "persistent_agent.model wins over default_model"
        )
        assert layer["interactive"] == {"permission_mode": "supervised"}
        assert layer["headless"] == {"mode": "attentive", "attention_sleep_minutes": 7}

    @pytest.mark.asyncio
    async def test_registry_default_fills_the_gap(self):
        store = MagicMock()
        store.get_user_settings = AsyncMock(return_value={})
        store.resolve_default_for_capability = AsyncMock(
            side_effect=lambda cap: {"chat": "c", "auxiliary": "a"}[cap]
        )
        deps, _ = _session_deps(store=store)
        assert await sessioncfg.resolve_default_models("u", dependencies=deps) == {
            "llm": {"model": "c"},
            "auxiliary": {"model": "a"},
        }


class TestPrefetchRosterRefs:
    @pytest.mark.asyncio
    async def test_no_refs_means_no_database_call(self):
        store = MagicMock()
        store.get_expert_by_id = AsyncMock(
            side_effect=AssertionError("must not be called")
        )
        store.get_user = AsyncMock(side_effect=AssertionError("must not be called"))
        deps, _ = _session_deps(store=store)
        assert (
            await sessioncfg.prefetch_roster_refs(
                expert_row={"config": {"llm": {"model": "x"}}}, dependencies=deps
            )
            == {}
        )

    @pytest.mark.asyncio
    async def test_expert_refs_are_fetched_by_id_and_overrides_by_visibility(self):
        ref_a = "11111111-1111-4111-8111-111111111111"
        ref_b = "22222222-2222-4222-8222-222222222222"
        store = MagicMock()
        store.get_expert_by_id = AsyncMock(return_value={"id": ref_a})
        store.get_user = AsyncMock(return_value={"is_admin": False})
        store.get_expert_visible_by_id = AsyncMock(return_value={"id": ref_b})
        deps, _ = _session_deps(store=store)
        out = await sessioncfg.prefetch_roster_refs(
            expert_row={"config": {"subagents": {"roster": {"a": {"$ref": ref_a}}}}},
            overrides=({"subagents": {"roster": {"b": {"$ref": ref_b}}}},),
            user_id="u",
            dependencies=deps,
        )
        assert out == {ref_a: {"id": ref_a}, ref_b: {"id": ref_b}}
        store.get_expert_visible_by_id.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_a_json_string_expert_fragment_is_parsed(self):
        ref = "11111111-1111-4111-8111-111111111111"
        store = MagicMock()
        store.get_expert_by_id = AsyncMock(return_value={"id": ref})
        deps, _ = _session_deps(store=store)
        out = await sessioncfg.prefetch_roster_refs(
            expert_row={
                "config": json.dumps({"subagents": {"roster": {"a": {"$ref": ref}}}})
            },
            dependencies=deps,
        )
        assert out == {ref: {"id": ref}}

    @pytest.mark.asyncio
    async def test_an_invisible_ref_is_simply_absent(self):
        ref = "11111111-1111-4111-8111-111111111111"
        store = MagicMock()
        store.get_expert_by_id = AsyncMock(return_value=None)
        store.get_user = AsyncMock(return_value=None)
        store.get_expert_visible_by_id = AsyncMock(return_value=None)
        deps, _ = _session_deps(store=store)
        assert (
            await sessioncfg.prefetch_roster_refs(
                overrides=({"subagents": {"roster": {"a": {"$ref": ref}}}},),
                user_id="u",
                dependencies=deps,
            )
            == {}
        )


class TestSessionPreflights:
    @staticmethod
    def _thread(metadata):
        return {
            "id": "22222222-2222-4222-8222-222222222222",
            "user_id": "u",
            "metadata": metadata,
        }

    @pytest.mark.asyncio
    async def test_malformed_metadata_json_is_a_409(self):
        deps, _ = _session_deps()
        with pytest.raises(HTTPException) as exc:
            await sessioncfg.session_grant_violations(
                self._thread("{not json"), dependencies=deps
            )
        assert exc.value.status_code == 409
        assert exc.value.detail["code"] == "session_metadata_malformed"

    @pytest.mark.asyncio
    async def test_a_malformed_protected_marker_is_its_own_refusal(self):
        deps, _ = _session_deps()
        with pytest.raises(HTTPException) as exc:
            await sessioncfg.session_grant_violations(
                self._thread({"protected_cloud": "yes"}), dependencies=deps
            )
        assert exc.value.detail["code"] == "protected_cloud_malformed"

    @pytest.mark.asyncio
    async def test_protected_cloud_requires_the_container_tier(self):
        deps, _ = _session_deps()
        thread = self._thread(
            {
                "protected_cloud": True,
                "config_override": {"workspace": {"backend": "virtual"}},
            }
        )
        with pytest.raises(HTTPException) as exc:
            await sessioncfg.session_grant_violations(thread, dependencies=deps)
        assert exc.value.detail["code"] == "protected_cloud_unsupported_workspace"

    @pytest.mark.asyncio
    async def test_grant_preflight_returns_the_violations(self):
        async def deny(*_a, **_k):
            raise grants.GrantDenied(["shell_tools: denied"])

        deps, _ = _session_deps(enforce_dispatch_grants=deny)
        assert await sessioncfg.session_grant_violations(
            self._thread({}), dependencies=deps
        ) == ["shell_tools: denied"]

    @pytest.mark.asyncio
    async def test_endpoint_preflight_defers_to_the_grant_preflight(self):
        async def deny(*_a, **_k):
            raise grants.GrantDenied(["shell_tools: denied"])

        deps, _ = _session_deps(enforce_dispatch_grants=deny)
        assert (
            await sessioncfg.session_endpoint_violations(
                self._thread({}), dependencies=deps
            )
            == []
        )

    @pytest.mark.asyncio
    async def test_endpoint_preflight_fails_open_on_a_resolve_error(self):
        async def boom(*_a, **_k):
            raise RuntimeError("nope")

        deps, _ = _session_deps(gather_in_scope_skills=boom)
        assert (
            await sessioncfg.session_endpoint_violations(
                self._thread({}), dependencies=deps
            )
            == []
        )

    @pytest.mark.asyncio
    async def test_officer_class_is_refused_for_protected_cloud(self, monkeypatch):
        def spy(**kwargs):
            kwargs["capture"]["merged_fragment"] = {}
            return {"agent": {"officer": {"enabled": True}}}

        monkeypatch.setattr(sessioncfg, "resolve_config", spy)

        async def echo(blob, callback):
            await callback({})
            return blob

        monkeypatch.setattr(sessioncfg, "inject_blob_credentials", echo)
        deps, _ = _session_deps()
        with pytest.raises(HTTPException) as exc:
            await sessioncfg.require_supported_protected_session_class(
                {"id": "22222222-2222-4222-8222-222222222222", "user_id": "u"},
                {"protected_cloud": True},
                dependencies=deps,
            )
        assert exc.value.detail["code"] == "protected_cloud_unsupported_session_class"

    @pytest.mark.asyncio
    async def test_an_unverifiable_class_is_refused_rather_than_admitted(self):
        async def boom(*_a, **_k):
            raise RuntimeError("nope")

        deps, _ = _session_deps(gather_in_scope_skills=boom)
        with pytest.raises(HTTPException) as exc:
            await sessioncfg.require_supported_protected_session_class(
                {"id": "22222222-2222-4222-8222-222222222222", "user_id": "u"},
                {"protected_cloud": True},
                dependencies=deps,
            )
        assert exc.value.detail["code"] == "protected_cloud_unsupported_session_class"

    @pytest.mark.asyncio
    async def test_a_thread_without_the_marker_is_not_class_checked(self):
        deps, calls = _session_deps()
        await sessioncfg.require_supported_protected_session_class(
            {"id": "22222222-2222-4222-8222-222222222222"}, {}, dependencies=deps
        )
        assert calls == []


# ---------------------------------------------------------------------------
# looks_like_uuid
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("11111111-1111-4111-8111-111111111111", True),
        ("11111111111141118111111111111111", True),
        ("session_base", False),
        ("", False),
        (None, False),
        (5, False),
    ],
)
def test_looks_like_uuid_matches_main(value, expected):
    assert config_overrides.looks_like_uuid(value) is expected
    assert config_overrides.looks_like_uuid(
        value
    ) == config_overrides_module.looks_like_uuid(value)
