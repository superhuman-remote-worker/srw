"""The pure workspace-defaults resolver (Slice A2b)."""

import json

import pytest

from shared.workspace_defaults import (
    CATALOG_SHARED,
    InstallationDefaults,
    InvalidWorkspaceDefaults,
    ProjectDefaults,
    Upgrade,
    UpgradeRefused,
    backend_mode,
    default_backend,
    installation_defaults,
    next_mode,
    resolve_defaults,
)

BUILTINS = frozenset({"virtual", "container-minimal", "container-full", "vm-full"})
SHIPPED = InstallationDefaults()
PROJECT_CONTAINER = {
    "inline": {"backend": "sandbox", "environment": {"image": "r.example/site:1"}}
}
PROJECT_VM = {"inline": {"backend": "vm"}}


def ref(name):
    return {"ref": {"name": name, "scope": CATALOG_SHARED}}


def test_shipped_values_keep_todays_behaviour():
    job = resolve_defaults(
        "worker", project=None, installation=SHIPPED, builtins=BUILTINS
    )
    session = resolve_defaults(
        "session", project=None, installation=SHIPPED, builtins=BUILTINS
    )
    assert (job.mode, job.template, job.sources()) == (
        "container",
        ref("container-full"),
        {"tier": "installation", "template": "builtin"},
    )
    assert job.binding() == {"template": ref("container-full")}
    assert (session.mode, session.template, session.sources()) == (
        "virtual",
        None,
        {"tier": "installation", "template": None},
    )
    assert session.binding() == {"template": {"inline": {"backend": "virtual"}}}


@pytest.mark.parametrize(
    ("project", "role", "expected"),
    [
        (
            ProjectDefaults(jobs="vm"),
            "worker",
            ("vm", ref("vm-full"), "project", "builtin"),
        ),
        (
            ProjectDefaults(sessions="container"),
            "session",
            ("container", ref("container-full"), "project", "builtin"),
        ),
        (ProjectDefaults(jobs="none"), "worker", ("none", None, "project", None)),
        (
            ProjectDefaults(sessions="vm"),
            "worker",
            ("container", ref("container-full"), "installation", "builtin"),
        ),
        (
            ProjectDefaults(jobs="container", container=PROJECT_CONTAINER),
            "worker",
            ("container", PROJECT_CONTAINER, "project", "project"),
        ),
    ],
)
def test_the_project_mode_for_the_role_wins(project, role, expected):
    got = resolve_defaults(
        role, project=project, installation=SHIPPED, builtins=BUILTINS
    )
    assert (got.mode, got.template, got.tier_source, got.template_source) == expected


def test_project_template_applies_when_the_tier_came_from_the_installation():
    got = resolve_defaults(
        "worker",
        project=ProjectDefaults(container=PROJECT_CONTAINER),
        installation=SHIPPED,
        builtins=BUILTINS,
    )
    assert got.sources() == {"tier": "installation", "template": "project"}
    assert got.template == PROJECT_CONTAINER


def test_the_installation_template_name_beats_the_builtin():
    installation = InstallationDefaults(container="company-image")
    got = resolve_defaults(
        "worker", project=None, installation=installation, builtins=BUILTINS
    )
    assert (got.template, got.template_source) == (ref("company-image"), "installation")
    assert got.template_name() == "company-image"


def test_builtins_off_with_empty_names_uses_the_bare_backend():
    got = resolve_defaults(
        "worker", project=None, installation=SHIPPED, builtins=frozenset()
    )
    assert got.template is None
    assert got.binding() == {"template": {"inline": {"backend": "sandbox"}}}
    assert got.sources() == {"tier": "installation", "template": "builtin"}


def test_a_manifest_row_carries_its_revision_only_when_it_supplied_something():
    row = ProjectDefaults(
        jobs="container", source="manifest", manifest_revision="sha256:abc"
    )
    used = resolve_defaults(
        "worker", project=row, installation=SHIPPED, builtins=BUILTINS
    )
    unused = resolve_defaults(
        "session", project=row, installation=SHIPPED, builtins=BUILTINS
    )
    assert used.project_revision == "sha256:abc"
    assert unused.project_revision is None


@pytest.mark.parametrize(
    ("current", "requested", "mode"),
    [
        ("virtual", None, "container"),
        ("none", None, "container"),
        ("container", None, "vm"),
        ("virtual", "vm", "vm"),
    ],
)
def test_upgrades_take_the_requested_or_next_tier(current, requested, mode):
    got = resolve_defaults(
        "session",
        project=ProjectDefaults(vm=PROJECT_VM),
        installation=SHIPPED,
        builtins=BUILTINS,
        upgrade=Upgrade(current=current, requested=requested),
    )
    assert got.mode == mode
    assert got.tier_source == "upgrade"
    if mode == "vm":
        assert (got.template, got.template_source) == (PROJECT_VM, "project")


@pytest.mark.parametrize(
    ("current", "requested"),
    [
        ("vm", None),
        ("container", "container"),
        ("vm", "container"),
        ("virtual", "none"),
    ],
)
def test_an_upgrade_never_stays_or_goes_down(current, requested):
    with pytest.raises(
        UpgradeRefused,
        match="An upgrade must move to a higher tier than the current one.",
    ):
        resolve_defaults(
            "session",
            project=None,
            installation=SHIPPED,
            builtins=BUILTINS,
            upgrade=Upgrade(current=current, requested=requested),
        )


def test_installation_values_parse_and_fall_back_per_field():
    environ = {
        "WORKSPACE_DEFAULTS": json.dumps(
            {"jobs": "vm", "sessions": "", "container": "", "vm": "big-vm"}
        )
    }
    assert installation_defaults(environ) == InstallationDefaults(
        jobs="vm", sessions="virtual", container=None, vm="big-vm"
    )
    assert installation_defaults({}) == SHIPPED


@pytest.mark.parametrize(
    "raw",
    [
        "not json",
        "[]",
        json.dumps({"jobs": "sandbox"}),
        json.dumps({"extra": 1}),
        json.dumps({"vm": 3}),
    ],
)
def test_malformed_installation_values_raise(raw):
    with pytest.raises(InvalidWorkspaceDefaults):
        installation_defaults({"WORKSPACE_DEFAULTS": raw})


def test_default_backend_never_raises():
    assert default_backend("worker", {"WORKSPACE_DEFAULTS": "not json"}) == "sandbox"
    assert (
        default_backend(
            "session", {"WORKSPACE_DEFAULTS": json.dumps({"sessions": "none"})}
        )
        == "none"
    )


def test_backend_and_mode_names():
    assert backend_mode("sandbox") == backend_mode("container") == "container"
    assert backend_mode("remote") == "vm"
    assert next_mode("virtual") == "container"
    assert next_mode("container") == "vm"


def test_the_contract_floor_follows_the_installation(monkeypatch):
    from shared.workspace_contract import build_workspace_contract
    from shared.runtime.core.workspace_selection import execution_workspace_config

    monkeypatch.setenv(
        "WORKSPACE_DEFAULTS", json.dumps({"jobs": "vm", "sessions": "none"})
    )
    assert build_workspace_contract({}).assigned_backend == "vm"
    assert execution_workspace_config(role="session") == {"backend": "none"}
    assert execution_workspace_config(role="worker") == {"backend": "vm"}
