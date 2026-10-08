"""The workspace Dockerfile builds a minimal image and a full image on top."""

from pathlib import Path
import re

REPO = Path(__file__).resolve().parents[1]
DOCKERFILE = (REPO / "docker/Dockerfile.workspace").read_text()
CONTRACT = REPO / "docker/assert-workspace-contract.sh"

MINIMAL = {
    "openssh-server",
    "tmux",
    "git",
    "python3",
    "python3-venv",
    "python3-pip",
    "zstd",
    "fuse3",
    "fuse-overlayfs",
    "ca-certificates",
    "procps",
    "curl",
    "wget",
    "jq",
    "less",
    "vim-tiny",
    "nano",
    "ripgrep",
    "zip",
    "unzip",
}
FULL_ONLY = {
    "build-essential",
    "cmake",
    "pkg-config",
    "python3-dev",
    "libssl-dev",
    "libffi-dev",
    "libpq-dev",
    "zlib1g-dev",
    "libbz2-dev",
    "libreadline-dev",
    "libsqlite3-dev",
    "nodejs",
    "postgresql-client",
    "mongodb-mongosh",
    "openjdk-17-jre-headless",
    "cypher-shell",
    "pandoc",
    "poppler-utils",
    "ffmpeg",
    "fd-find",
    "tree",
    "htop",
    "net-tools",
    "iproute2",
    "dnsutils",
    "iputils-ping",
    "lsb-release",
    "gnupg",
    "fonts-dejavu-core",
    "fonts-dejavu-mono",
}
CONTRACT_BINARIES = (
    "tmux",
    "flock",
    "timeout",
    "mktemp",
    "python3",
    "tar",
    "zstd",
    "git",
    "ssh-agent",
    "ssh-add",
    "ssh-keygen",
    "code-server",
    "rclone",
    "fusermount3",
    "mountpoint",
    "fuse-overlayfs",
)


def stages() -> dict[str, str]:
    parts = re.split(r"(?m)^FROM ", DOCKERFILE)[1:]
    return {part.splitlines()[0].split(" AS ")[-1].strip(): part for part in parts}


def tokens(stage: str) -> set[str]:
    """Standalone words of a stage's instruction lines, comments left out."""
    found: set[str] = set()
    for line in stage.splitlines():
        stripped = line.strip()
        if not stripped.startswith("#"):
            found.update(stripped.replace("\\", " ").split())
    return found


def test_full_is_built_on_minimal_and_is_the_default_target():
    assert re.findall(r"(?m)^FROM (.+)$", DOCKERFILE) == [
        "ubuntu:24.04 AS minimal",
        "minimal AS full",
    ]


def test_minimal_carries_the_contract_and_no_toolchain():
    found = tokens(stages()["minimal"])
    assert MINIMAL <= found, MINIMAL - found
    assert not FULL_ONLY & found, FULL_ONLY & found


def test_full_adds_the_toolchains():
    found = tokens(stages()["full"])
    assert FULL_ONLY <= found, FULL_ONLY - found


def test_the_browser_stack_and_the_ide_are_in_minimal():
    minimal = stages()["minimal"]
    assert "playwright install --with-deps chromium" in minimal
    assert "docker/browser-exec /usr/local/bin/browser-exec" in minimal
    assert "code-server_${CODE_SERVER_VERSION}_amd64.deb" in minimal
    assert "rclone-v${SRW_RCLONE_VERSION}-linux-amd64.deb" in minimal


def last_install(stage: str) -> int:
    return max(
        stage.rfind("apt-get install"),
        stage.rfind("dpkg -i"),
        stage.rfind("npm install"),
    )


def test_both_targets_run_both_gates():
    for name, stage in stages().items():
        assert "RUN /usr/local/bin/assert-browser-stack" in stage, name
        contract = stage.rfind("RUN /usr/local/bin/assert-workspace-contract")
        assert contract > last_install(stage), name
    full = stages()["full"]
    browser = full.rfind("RUN /usr/local/bin/assert-browser-stack")
    assert browser > last_install(full)


def test_full_carries_the_pinned_kubectl_kubeconfig_connectors_need():
    """Kubeconfig connectors land in the workspace home (D1d); the shell
    needs kubectl. The pin matches the VM base image's."""
    minimal, full = stages()["minimal"], stages()["full"]
    pin = dict(re.findall(r"(?m)^ARG (SRW_KUBECTL_\w+)=(\S+)$", full))
    vm = (REPO / "docker/agent-vm-base/scripts/provision-stage1.sh").read_text()
    assert pin == dict(re.findall(r"(?m)^(SRW_KUBECTL_\w+)=(\S+)$", vm))
    assert set(pin) == {"SRW_KUBECTL_VERSION", "SRW_KUBECTL_SHA256"}
    assert '"${SRW_KUBECTL_SHA256}  /tmp/kubectl" | sha256sum -c -' in full
    assert "install -m 0755 /tmp/kubectl /usr/local/bin/kubectl" in full
    assert full.index("/usr/local/bin/kubectl") < full.rfind(
        "RUN /usr/local/bin/assert-workspace-contract"
    )
    assert "kubectl" not in minimal


def test_full_installs_node_packages_system_wide():
    # minimal sets npm_config_prefix to the home directory, which the workspace
    # volume hides. A global install in full must not land there.
    full = stages()["full"]
    assert "env -u npm_config_prefix npm install -g" in full
    assert "env -u npm_config_prefix corepack enable" in full


def test_the_entrypoint_and_user_are_set_once_in_minimal():
    minimal, full = stages()["minimal"], stages()["full"]
    assert minimal.count("ENTRYPOINT") == 1 and "ENTRYPOINT" not in full
    assert minimal.count("useradd") == 1 and "useradd" not in full
    assert minimal.count("cp -a /home/agent-host /etc/skel.agent-host") == 1
    # full must not write into the home directory: the skeleton is already taken.
    assert "/home/agent-host" not in full


def test_each_target_names_its_variant():
    assert 'LABEL io.srw.workspace.variant="minimal"' in stages()["minimal"]
    assert 'LABEL io.srw.workspace.variant="full"' in stages()["full"]


def test_the_contract_script_is_installed_and_checks_the_contract():
    assert (
        DOCKERFILE.count(
            "COPY --chmod=0755 docker/assert-workspace-contract.sh "
            "/usr/local/bin/assert-workspace-contract"
        )
        == 1
    )
    script = CONTRACT.read_text()
    assert script.startswith("#!/usr/bin/env bash\n")
    for binary in CONTRACT_BINARIES:
        assert f'"{binary}"' in script, binary
    for setting in (
        "Port 30022",
        "AuthorizedKeysFile /etc/ssh/authorized_keys/%u",
        "TrustedUserCAKeys /etc/ssh/srw_user_ca.pub",
        "AuthorizedPrincipalsFile /etc/ssh/principals/%u",
    ):
        assert setting in script, setting
    assert "1.70.0" in script and "1.13" in script


def test_tilt_sends_the_contract_script_to_the_build():
    tiltfile = (REPO / "Tiltfile").read_text()
    assert "'docker/assert-workspace-contract.sh'," in tiltfile
