"""The agent's shell gets the workspace image's settings and browsers.

knowledge-base/knowledge/issues/workspace_shell_misses_image_environment.md:
sshd starts every session with a fresh environment, so the image's Dockerfile
ENV never reached the agent. write-agent-environment copies the settings into
/etc/environment, which pam_env loads into every SSH session, and
link-playwright-browsers puts the image's browsers where the agent's own
Playwright looks. Both workspace images use both scripts.
"""

import importlib.machinery
import importlib.util
import os
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
WRITER = REPO / "docker/write-agent-environment"
LINKER = REPO / "docker/link-playwright-browsers"
DOCKERFILE = (REPO / "docker/Dockerfile.workspace").read_text()
ENTRYPOINT = (REPO / "docker/workspace-entrypoint.sh").read_text()
STAGE1 = (REPO / "docker/agent-vm-base/scripts/provision-stage1.sh").read_text()
STAGE2 = (REPO / "docker/agent-vm-base/scripts/provision-stage2.sh").read_text()
SCRIPTS = ("link-playwright-browsers", "write-agent-environment")


# ── write-agent-environment ───────────────────────────────────────────────────
AGENT_VALUES = {
    "PIP_BREAK_SYSTEM_PACKAGES": "1",
    "npm_config_prefix": "/home/agent-host/.npm-global",
}


def _write(tmp_path, *names, existing=None, **values):
    target = tmp_path / "environment"
    if existing is not None:
        target.write_text(existing)
    env = {
        "PATH": f"/opt/tool/bin:{os.environ['PATH']}",
        "AGENT_ENVIRONMENT_FILE": str(target),
        **AGENT_VALUES,
        **values,
    }
    result = subprocess.run(
        ["bash", str(WRITER), *names], env=env, capture_output=True, text=True
    )
    return result, target, env


def test_writer_puts_path_and_the_install_settings_in_the_file(tmp_path):
    result, target, env = _write(tmp_path)
    assert result.returncode == 0, result.stderr
    assert target.read_text().splitlines() == [
        f'PATH="{env["PATH"]}"',
        'PIP_BREAK_SYSTEM_PACKAGES="1"',
        'npm_config_prefix="/home/agent-host/.npm-global"',
    ]


def test_writer_replaces_its_own_lines_and_keeps_the_rest(tmp_path):
    existing = (
        'LANG="C.UTF-8"\n'
        'PATH="/usr/bin:/bin"\n'
        "export npm_config_prefix=/old\n"
        "PATHS_ELSEWHERE=/keep\n"
    )
    result, target, env = _write(tmp_path, existing=existing)
    assert result.returncode == 0, result.stderr
    lines = target.read_text().splitlines()
    assert lines[:2] == ['LANG="C.UTF-8"', "PATHS_ELSEWHERE=/keep"]
    assert f'PATH="{env["PATH"]}"' in lines
    assert 'npm_config_prefix="/home/agent-host/.npm-global"' in lines
    assert len(lines) == 5


def test_writer_takes_extra_names_from_a_derived_image(tmp_path):
    result, target, _ = _write(tmp_path, "GOPATH", GOPATH="/opt/go")
    assert result.returncode == 0, result.stderr
    assert target.read_text().splitlines()[-1] == 'GOPATH="/opt/go"'


def test_writer_refuses_an_unset_value_and_leaves_the_file(tmp_path):
    result, target, _ = _write(tmp_path, "GOPATH", existing="LANG=C\n")
    assert result.returncode == 1
    assert "GOPATH is not set" in result.stderr
    assert target.read_text() == "LANG=C\n"


def test_writer_refuses_what_pam_env_cannot_read(tmp_path):
    bad_name, _, _ = _write(tmp_path, "NOT-A-NAME")
    assert bad_name.returncode == 2
    quoted, _, _ = _write(tmp_path, "GOPATH", GOPATH='/opt/"go"')
    assert quoted.returncode == 1


# ── link-playwright-browsers ──────────────────────────────────────────────────
def _image_browsers(tmp_path):
    source = tmp_path / "opt-playwright"
    for name in ("chromium-1217", "chromium_headless_shell-1217", "ffmpeg-1011"):
        (source / name).mkdir(parents=True)
        (source / name / "INSTALLATION_COMPLETE").write_text("")
    (source / ".links").mkdir()
    (source / ".links" / "abc123").write_text("/usr/lib/python3/playwright/package")
    return source


def _link(home, source, **env):
    return subprocess.run(
        ["bash", str(LINKER), str(source)],
        env={"HOME": str(home), "PATH": os.environ["PATH"], **env},
        capture_output=True,
        text=True,
    )


def test_linker_links_every_browser_and_registers_the_image_playwright(tmp_path):
    source, home = _image_browsers(tmp_path), tmp_path / "home"
    home.mkdir()
    assert _link(home, source).returncode == 0
    cache = home / ".cache/ms-playwright"
    for name in ("chromium-1217", "chromium_headless_shell-1217", "ffmpeg-1011"):
        assert (cache / name).is_symlink()
        assert os.readlink(cache / name) == str(source / name)
        assert (cache / name / "INSTALLATION_COMPLETE").exists()
    # Without this registration, an install by another Playwright version
    # deletes the links as unused browsers.
    assert (cache / ".links/abc123").read_text() == (
        "/usr/lib/python3/playwright/package"
    )
    assert _link(home, source).returncode == 0  # repeatable


def test_linker_keeps_downloads_and_drops_links_an_upgrade_left_dangling(tmp_path):
    source, home = _image_browsers(tmp_path), tmp_path / "home"
    cache = home / ".cache/ms-playwright"
    (cache / "chromium-1217").mkdir(parents=True)  # the agent downloaded it
    (cache / "chromium-1217" / "own").write_text("")
    (cache / "chromium-1100").symlink_to(source / "chromium-1100")  # old image
    (cache / "elsewhere").symlink_to(tmp_path / "missing")  # not ours
    assert _link(home, source).returncode == 0
    assert not (cache / "chromium-1217").is_symlink()
    assert (cache / "chromium-1217" / "own").exists()
    assert not (cache / "chromium-1100").is_symlink()
    assert (cache / "elsewhere").is_symlink()
    assert (cache / "ffmpeg-1011").is_symlink()


def test_linker_follows_xdg_cache_home_and_skips_an_image_without_browsers(tmp_path):
    source, home, xdg = _image_browsers(tmp_path), tmp_path / "home", tmp_path / "xdg"
    assert _link(home, source, XDG_CACHE_HOME=str(xdg)).returncode == 0
    assert (xdg / "ms-playwright/ffmpeg-1011").is_symlink()

    assert _link(home, tmp_path / "absent").returncode == 0
    assert not (home / ".cache").exists()


# ── container image ───────────────────────────────────────────────────────────
def test_container_sets_no_playwright_path_and_no_pip_target():
    assert "ENV PLAYWRIGHT_BROWSERS_PATH" not in DOCKERFILE
    assert "PIP_TARGET" not in DOCKERFILE
    assert "ENV PIP_BREAK_SYSTEM_PACKAGES=1 \\" in DOCKERFILE


def test_container_writes_the_environment_after_its_env_and_links_before_the_gate():
    env_block = DOCKERFILE.index("ENV PIP_BREAK_SYSTEM_PACKAGES=1")
    assert (
        DOCKERFILE.index("npm_config_prefix=/home/agent-host/.npm-global") > env_block
    )
    writer = DOCKERFILE.index("RUN /usr/local/bin/write-agent-environment")
    assert writer > DOCKERFILE.index("npm_config_prefix=/home/agent-host/.npm-global")
    assert DOCKERFILE.rfind("\nENV ") < writer  # no ENV after the copy
    linker = DOCKERFILE.index(
        "COPY --chmod=0755 docker/link-playwright-browsers "
        "/usr/local/bin/link-playwright-browsers"
    )
    assert linker < DOCKERFILE.index("RUN /usr/local/bin/assert-browser-stack")


def test_entrypoint_links_the_browsers_as_agent_host_after_seeding_home():
    link = ENTRYPOINT.index(
        "su -s /bin/sh agent-host -c /usr/local/bin/link-playwright-browsers"
    )
    assert link > ENTRYPOINT.index("cp -rn /etc/skel.agent-host/.")
    assert "(non-fatal)" in ENTRYPOINT[link : link + 200]


def test_gates_check_the_agent_shell_and_playwright_default_folder():
    contract = (REPO / "docker/assert-workspace-contract.sh").read_text()
    assert "su -l agent-host -s /usr/bin/env" in contract
    assert '_check "agent shell: PATH"' in contract
    for name in ("PIP_BREAK_SYSTEM_PACKAGES", "npm_config_prefix"):
        assert f"_agent_has_image_value {name}" in contract
    browser = (REPO / "docker/assert-browser-stack.sh").read_text()
    assert "/usr/local/bin/link-playwright-browsers" in browser
    assert '_check "Playwright finds the image\'s browsers"' in browser


# ── VM image ──────────────────────────────────────────────────────────────────
def test_vm_installs_the_same_scripts_and_runs_them():
    packer = (REPO / "docker/agent-vm-base/stage2.pkr.hcl").read_text()
    for script in SCRIPTS:
        assert f'"../{script}"' in packer
        assert (
            f"install -o root -g root -m 0755 /tmp/{script} /usr/local/bin/{script}"
            in STAGE2
        )
    assert "PIP_BREAK_SYSTEM_PACKAGES=1 \\" in STAGE2
    assert "npm_config_prefix=/home/agent-host/.npm-global \\" in STAGE2
    assert "sudo sed -i '/^PLAYWRIGHT_BROWSERS_PATH=/d' /etc/environment" in STAGE2
    link = STAGE2.index("sudo -u agent-host -H /usr/local/bin/link-playwright-browsers")
    assert link < STAGE2.index("sudo -u agent-host /usr/local/bin/assert-browser-stack")


def test_vm_no_longer_points_playwright_at_opt_or_deletes_the_links():
    assert 'PLAYWRIGHT_BROWSERS_PATH=/opt/playwright" | sudo tee' not in STAGE1
    cleanup = (REPO / "docker/agent-vm-base/scripts/cleanup.sh").read_text()
    assert "/home/*/.cache/ms-playwright" not in cleanup


def test_ci_and_tilt_rebuild_the_images_when_the_scripts_change():
    workflow = (REPO / ".github/workflows/develop.yml").read_text()
    for detector in ("WORKSPACE_PATHS=(", "VM_BASE_PATHS=("):
        paths = workflow.split(detector, 1)[1].split(")", 1)[0]
        for script in SCRIPTS:
            assert f"docker/{script}" in paths, (detector, script)
    tilt = (REPO / "Tiltfile").read_text()
    for script in SCRIPTS:
        assert tilt.count(f"'docker/{script}',") == 2  # full and minimal


# ── browser-exec stays on the image's packages ────────────────────────────────
def test_browser_exec_ignores_the_agents_own_pip_installs(monkeypatch):
    loader = importlib.machinery.SourceFileLoader(
        "agent_environment_browser_exec_test", str(REPO / "docker/browser-exec")
    )
    spec = importlib.util.spec_from_loader(loader.name, loader)
    module = importlib.util.module_from_spec(spec)
    loader.exec_module(module)

    import site

    user_site = site.getusersitepackages()
    monkeypatch.setattr(
        sys, "path", ["/usr/lib/python3/dist-packages", user_site, f"{user_site}/x"]
    )
    monkeypatch.delenv("PYTHONNOUSERSITE", raising=False)
    module._ignore_user_site_packages()
    assert sys.path == ["/usr/lib/python3/dist-packages"]
    assert os.environ["PYTHONNOUSERSITE"] == "1"
