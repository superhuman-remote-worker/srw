"""Frozen container settings become one deterministic pod profile."""

import pytest

from orchestrator.services.sandbox_workspace_settings import (
    SandboxImagePolicy,
    SandboxSettings,
    image_repository,
    resolve_sandbox_settings,
    sandbox_pod_profile,
)

DEFAULT = "ghcr.io/superhuman-remote-worker/srw-workspace:sha-abc123"
CUSTOM = "registry.example/team/workspace:1"


def policy(**changes) -> SandboxImagePolicy:
    values = {
        "default_image": DEFAULT,
        "trusted_repositories": frozenset(
            {"ghcr.io/superhuman-remote-worker/srw-workspace"}
        ),
        "custom_images_privileged": False,
        "fuse_enabled": True,
        "fuse_privileged": True,
        "pull_timeout_seconds": 600,
    }
    values.update(changes)
    return SandboxImagePolicy(**values)


def test_empty_settings_keep_todays_pod():
    profile = sandbox_pod_profile(SandboxSettings(), policy())
    assert (profile.image, profile.cpu, profile.memory) == (DEFAULT, "500m", "1Gi")
    assert (profile.cpu_limit, profile.memory_limit) == ("2000m", "4Gi")
    assert profile.pull_policy is None and profile.storage is None
    assert profile.fuse_enabled and profile.fuse_privileged
    assert profile.templated is False
    assert profile.plan_extension() is None


def test_memory_is_reserved_and_cpu_is_shared():
    profile = sandbox_pod_profile(SandboxSettings(cpu=1, memory="3Gi"), policy())
    assert (profile.cpu, profile.cpu_limit) == ("250m", "1000m")
    assert (profile.memory, profile.memory_limit) == ("3Gi", "3Gi")


def test_fractional_cpu_rounds_up_to_whole_millicores():
    profile = sandbox_pod_profile(SandboxSettings(cpu=0.3), policy())
    assert (profile.cpu, profile.cpu_limit) == ("75m", "300m")


@pytest.mark.parametrize("cores,limit", [(0.7, "700m"), (1.1, "1100m"), (2.5, "2500m")])
def test_decimal_cpu_has_no_float_noise(cores, limit):
    assert sandbox_pod_profile(SandboxSettings(cpu=cores), policy()).cpu_limit == limit


def test_tiny_cpu_rounds_up_to_one_millicore():
    profile = sandbox_pod_profile(SandboxSettings(cpu=0.001), policy())
    assert (profile.cpu, profile.cpu_limit) == ("1m", "1m")


def test_unset_dimension_keeps_its_default():
    profile = sandbox_pod_profile(SandboxSettings(memory="6Gi"), policy())
    assert (profile.cpu, profile.cpu_limit) == ("500m", "2000m")


def test_custom_image_is_unprivileged_by_default():
    profile = sandbox_pod_profile(
        SandboxSettings(image=CUSTOM, pull_policy="Always", storage="15Gi"), policy()
    )
    assert profile.image == CUSTOM
    assert profile.fuse_enabled is False and profile.fuse_privileged is False
    assert profile.plan_extension() == {"pull_policy": "Always", "storage": "15Gi"}


def test_switch_gives_custom_images_the_installation_profile():
    profile = sandbox_pod_profile(
        SandboxSettings(image=CUSTOM), policy(custom_images_privileged=True)
    )
    assert profile.fuse_enabled and profile.fuse_privileged


def test_custom_image_never_exceeds_the_installation_fuse_settings():
    profile = sandbox_pod_profile(
        SandboxSettings(image=CUSTOM),
        policy(custom_images_privileged=True, fuse_privileged=False),
    )
    assert profile.fuse_enabled is True and profile.fuse_privileged is False


def test_trusted_repository_matches_any_tag_or_digest():
    trusted = "ghcr.io/superhuman-remote-worker/srw-workspace@sha256:" + "d" * 64
    assert sandbox_pod_profile(SandboxSettings(image=trusted), policy()).fuse_privileged


@pytest.mark.parametrize(
    "reference,repository",
    [
        ("ghcr.io/org/ws:1", "ghcr.io/org/ws"),
        ("ghcr.io/org/ws@sha256:" + "e" * 64, "ghcr.io/org/ws"),
        ("localhost:5005/srw-workspace:tilt-1", "localhost:5005/srw-workspace"),
        ("srw-registry:5000/x", "srw-registry:5000/x"),
        ("ubuntu", "ubuntu"),
    ],
)
def test_image_repository_strips_tag_and_digest_only(reference, repository):
    assert image_repository(reference) == repository


def test_policy_from_env_always_trusts_the_installation_repository(monkeypatch):
    monkeypatch.setenv("WORKSPACE_IMAGE", "localhost:5005/srw-workspace:tilt-9")
    monkeypatch.setenv(
        "WORKSPACE_TRUSTED_IMAGE_REPOSITORIES", '["ghcr.io/org/srw-workspace-minimal"]'
    )
    monkeypatch.setenv("WORKSPACE_CUSTOM_IMAGES_PRIVILEGED", "true")
    monkeypatch.setenv("WORKSPACE_IMAGE_PULL_TIMEOUT_SECONDS", "900")
    loaded = SandboxImagePolicy.from_env()
    assert loaded.trusted_repositories == frozenset(
        {"localhost:5005/srw-workspace", "ghcr.io/org/srw-workspace-minimal"}
    )
    assert loaded.custom_images_privileged is True
    assert loaded.pull_timeout_seconds == 900


def test_policy_from_env_rejects_a_non_list(monkeypatch):
    monkeypatch.setenv("WORKSPACE_TRUSTED_IMAGE_REPOSITORIES", '"ghcr.io/org/x"')
    with pytest.raises(ValueError):
        SandboxImagePolicy.from_env()


def test_fuse_enabled_uses_the_provisioners_deny_list(monkeypatch):
    # WORKSPACE_FUSE_ENABLED must parse exactly like
    # container_provisioner._env_flag: unset defaults on, a recognized falsy
    # value turns it off, and anything else (even a typo) stays on.
    monkeypatch.delenv("WORKSPACE_FUSE_ENABLED", raising=False)
    assert SandboxImagePolicy.from_env().fuse_enabled is True

    monkeypatch.setenv("WORKSPACE_FUSE_ENABLED", "false")
    loaded = SandboxImagePolicy.from_env()
    assert loaded.fuse_enabled is False and loaded.fuse_privileged is False

    monkeypatch.setenv("WORKSPACE_FUSE_ENABLED", "enabled")
    assert SandboxImagePolicy.from_env().fuse_enabled is True


def test_fuse_privileged_off_disables_privilege_but_not_fuse(monkeypatch):
    monkeypatch.setenv("WORKSPACE_FUSE_ENABLED", "true")
    monkeypatch.setenv("WORKSPACE_FUSE_PRIVILEGED", "off")
    loaded = SandboxImagePolicy.from_env()
    assert loaded.fuse_enabled is True
    assert loaded.fuse_privileged is False


@pytest.mark.parametrize(
    "value,expected",
    [("yes", True), ("enabled", False), (None, False)],
)
def test_custom_images_privileged_is_fail_closed(monkeypatch, value, expected):
    # Unlike the FUSE flags, a garbage WORKSPACE_CUSTOM_IMAGES_PRIVILEGED must
    # never grant privilege: only a recognized truthy value opts in.
    if value is None:
        monkeypatch.delenv("WORKSPACE_CUSTOM_IMAGES_PRIVILEGED", raising=False)
    else:
        monkeypatch.setenv("WORKSPACE_CUSTOM_IMAGES_PRIVILEGED", value)
    assert SandboxImagePolicy.from_env().custom_images_privileged is expected


@pytest.mark.parametrize(
    "fuse_enabled,fuse_privileged",
    [("true", "true"), ("false", "true"), ("true", "off"), ("enabled", "true")],
)
def test_from_env_fuse_matches_the_container_provisioner(
    monkeypatch, fuse_enabled, fuse_privileged
):
    from orchestrator.services.container_provisioner import ContainerProvisioner

    monkeypatch.setenv("WORKSPACE_FUSE_ENABLED", fuse_enabled)
    monkeypatch.setenv("WORKSPACE_FUSE_PRIVILEGED", fuse_privileged)
    loaded = SandboxImagePolicy.from_env()
    provisioner = ContainerProvisioner()
    assert loaded.fuse_enabled == provisioner._fuse_enabled
    assert loaded.fuse_privileged == provisioner._fuse_privileged


@pytest.mark.asyncio
async def test_non_uuid_owner_has_no_snapshot():
    assert await resolve_sandbox_settings(object(), "job", "not-a-uuid") == (
        SandboxSettings()
    )
