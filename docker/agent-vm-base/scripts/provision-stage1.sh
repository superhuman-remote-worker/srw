#!/usr/bin/env bash
# =============================================================================
# Agent VM Base Image — Stage 1 Provisioning
# =============================================================================
#
# Heavy, stable bits that go into the published `agent-vm-base-stage1` qcow2:
#   - System packages (dev tools, build essentials, networking)
#   - Datasource CLI clients (psql, mongosh, cypher-shell + JRE)
#   - Tailscale (mesh VPN client for Headscale connectivity)
#   - Python 3 + pip + management-daemon deps (nats-py, psutil)
#   - Playwright Chromium (the slowest single step)
#   - Node.js 22 + minimal global npm packages
#
# Rerun only when stage1 inputs change: this script, .playwright-version,
# cloud-init/, or stage1.pkr.hcl. Stage 2 (provision-stage2.sh) layers
# user/daemon/sudo-gate config on top per commit.
# =============================================================================

set -euxo pipefail

echo "=== Stage 1: heavy provisioning ==="

# -----------------------------------------------------------------------------
# Profiling helper — prints per-section elapsed time so CI logs show which
# step dominates wall-clock. Grep "[PROFILE]" in the build log.
# -----------------------------------------------------------------------------
__SECTION_START=$SECONDS
__PREV_SECTION=""
_section() {
    if [ -n "${__PREV_SECTION}" ]; then
        echo ">>> [PROFILE] '${__PREV_SECTION}' took $((SECONDS - __SECTION_START))s"
    fi
    __PREV_SECTION="$1"
    __SECTION_START=$SECONDS
    echo "--- ${1} ---"
}
_section_end() {
    if [ -n "${__PREV_SECTION}" ]; then
        echo ">>> [PROFILE] '${__PREV_SECTION}' took $((SECONDS - __SECTION_START))s"
    fi
    echo ">>> [PROFILE] stage1 total: ${SECONDS}s"
}

# -----------------------------------------------------------------------------
# 1. APT configuration: Azure mirror + tuning + global no-recommends
# -----------------------------------------------------------------------------

_section "Configuring APT"

# Prefer the Azure mirror (faster from GHA Azure-hosted runners) but keep
# archive.ubuntu.com as a fallback so apt rotates to it during Azure outages.
# We ate a 23-min hang on 2026-05-05 when Azure stopped responding mid-build.
# Short timeouts make those failures fast instead of stalling.
sudo sed -i 's|http://archive\.ubuntu\.com/ubuntu|http://azure.archive.ubuntu.com/ubuntu|g; s|http://security\.ubuntu\.com/ubuntu|http://azure.archive.ubuntu.com/ubuntu|g' \
    /etc/apt/sources.list.d/ubuntu.sources 2>/dev/null || \
    sudo sed -i 's|http://archive\.ubuntu\.com/ubuntu|http://azure.archive.ubuntu.com/ubuntu|g; s|http://security\.ubuntu\.com/ubuntu|http://azure.archive.ubuntu.com/ubuntu|g' \
    /etc/apt/sources.list

sudo tee /etc/apt/sources.list.d/ubuntu-fallback.list > /dev/null <<'EOF'
deb http://archive.ubuntu.com/ubuntu noble main restricted universe multiverse
deb http://archive.ubuntu.com/ubuntu noble-updates main restricted universe multiverse
deb http://security.ubuntu.com/ubuntu noble-security main restricted universe multiverse
EOF

sudo tee /etc/apt/apt.conf.d/99-build-tuning > /dev/null <<'EOF'
Acquire::Retries "5";
Acquire::Retries::Delay::Maximum "10";
Acquire::http::Timeout "15";
Acquire::https::Timeout "15";
Acquire::http::ConnectionAttemptDelayMsec "500";
Acquire::http::Pipeline-Depth "10";
APT::Install-Recommends "false";
EOF

# -----------------------------------------------------------------------------
# 2. System packages
# -----------------------------------------------------------------------------

_section "Installing system packages"

sudo apt-get update -y

# eatmydata: LD_PRELOADs libeatmydata.so so dpkg's fsync() calls become no-ops.
# Trades durability for speed (~30-50% faster installs); fine for image builds
# since the disk is discarded on failure. Install it first, then wrap every
# subsequent apt-get with `sudo eatmydata`.
sudo apt-get install -y eatmydata

sudo eatmydata apt-get install -y \
    openssh-server \
    tmux \
    git \
    curl \
    wget \
    jq \
    vim \
    nano \
    less \
    tree \
    htop \
    procps \
    net-tools \
    iproute2 \
    dnsutils \
    iputils-ping \
    zip \
    unzip \
    sudo \
    ca-certificates \
    gnupg \
    lsb-release \
    build-essential \
    cmake \
    pkg-config \
    libssl-dev \
    libffi-dev \
    libpq-dev \
    zlib1g-dev \
    libbz2-dev \
    libreadline-dev \
    libsqlite3-dev \
    ripgrep \
    fd-find \
    fuse3 \
    fuse-overlayfs \
    poppler-utils \
    pandoc \
    python3 \
    python3-venv \
    python3-dev \
    python3-pip \
    podman \
    podman-compose \
    uidmap \
    slirp4netns \
    passt \
    netavark \
    aardvark-dns \
    catatonit \
    dbus-user-session

# Podman: the VM tier's reason for existing is that the agent has real sudo
# and a real kernel, so containerised work (a postgres beside the app, a
# compose stack, an image build) happens here rather than in the sandbox tier
# where it is impossible. Rootless by default for agent-host (see stage 2);
# `sudo podman` remains available for the rare privileged case.
#
# Every dependency above is listed EXPLICITLY because this image sets
# `APT::Install-Recommends "false"` — podman's Recommends would otherwise be
# skipped and the failures are non-obvious:
#   uidmap        newuidmap/newgidmap; without it rootless refuses to start
#   slirp4netns   } rootless networking; passt/pasta is the newer path and
#   passt         } podman picks whichever it finds
#   netavark      the network backend podman 4.x actually uses
#   aardvark-dns  container-to-container NAME RESOLUTION. Without it a compose
#                 stack starts but services cannot resolve each other, which
#                 looks like an application bug rather than a missing package
#   catatonit     init process for `--init` / PID-1 reaping
#   dbus-user-session  user D-Bus, needed for the lingering systemd user
#                 session that keeps the rootless podman socket alive
# fuse-overlayfs is already installed above and is what keeps rootless storage
# off the slow vfs driver.

# Short-name resolution: Ubuntu ships registries.conf with
# `unqualified-search-registries` commented out, so `podman run postgres:16`
# fails with a short-name prompt that never gets answered in a non-interactive
# agent shell. Set it explicitly — this is the single most common way a
# baked-in podman looks broken to a worker.
sudo mkdir -p /etc/containers
if ! grep -qs '^unqualified-search-registries' /etc/containers/registries.conf 2>/dev/null; then
    echo 'unqualified-search-registries = ["docker.io"]' \
        | sudo tee -a /etc/containers/registries.conf > /dev/null
fi

# NOTE: `podman-docker` (the /usr/bin/docker shim) is deliberately NOT
# installed: /usr/bin/docker is the real Docker CLI from section 2b below.
# An agent following the public k3d guide expects Docker Engine, and the shim
# plus a DOCKER_HOST export pointing at podman cost probe 1 (2026-09-04) twenty
# minutes and a root-owned profile edit. Rootless podman stays available as
# `podman` / `podman-compose`.

# -----------------------------------------------------------------------------
# 2b. Docker Engine + local-Kubernetes tooling
#     Baked in so a worker can run the repository's own k3d guide, a k3d/k3s
#     cluster, Tilt, or any customer's compose/k8s workflow without the
#     install-from-scratch step every job. Explicitly a stopgap until VM
#     software profiles exist (knowledge-base: vm_fleet_software_profiles);
#     versions are pinned with checksums so image builds are reproducible.
# -----------------------------------------------------------------------------

_section "Installing Docker Engine + kubectl/helm/k3d/tilt/mkcert"

sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
    | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" \
    | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update -y
sudo eatmydata apt-get install -y \
    docker-ce \
    docker-ce-cli \
    containerd.io \
    docker-buildx-plugin \
    docker-compose-plugin
# Rootful daemon on boot; agent-host joins the `docker` group in stage 2.
sudo systemctl enable docker.service containerd.service
# Docker's default FORWARD DROP policy is fine for k3d (it adds its own rules)
# but keep the socket group-readable rather than world-writable.
sudo mkdir -p /etc/docker
[ -f /etc/docker/daemon.json ] || echo '{}' | sudo tee /etc/docker/daemon.json > /dev/null

# kubectl — pinned upstream binary + published checksum (dl.k8s.io).
SRW_KUBECTL_VERSION=v1.33.13
SRW_KUBECTL_SHA256=316d712726d857c20744c57cb36aa47cfc79bc0af7e82cebf3780244b654c073
curl -fsSL "https://dl.k8s.io/release/${SRW_KUBECTL_VERSION}/bin/linux/amd64/kubectl" -o /tmp/kubectl
echo "${SRW_KUBECTL_SHA256}  /tmp/kubectl" | sha256sum -c -
sudo install -m 0755 /tmp/kubectl /usr/local/bin/kubectl && rm -f /tmp/kubectl

# helm — Helm 3 line (the guide requires 3.12+; Helm 4 is not yet validated
# against this chart). Checksum from get.helm.sh/<asset>.sha256sum.
SRW_HELM_VERSION=v3.21.4
SRW_HELM_SHA256=61f88ab166748cb19604d7884cb100ae9ccb13804ddeb98e08af167eacbb6a14
curl -fsSL "https://get.helm.sh/helm-${SRW_HELM_VERSION}-linux-amd64.tar.gz" -o /tmp/helm.tgz
echo "${SRW_HELM_SHA256}  /tmp/helm.tgz" | sha256sum -c -
tar -xzf /tmp/helm.tgz -C /tmp linux-amd64/helm
sudo install -m 0755 /tmp/linux-amd64/helm /usr/local/bin/helm && rm -rf /tmp/helm.tgz /tmp/linux-amd64

# k3d — pinned GitHub release binary.
SRW_K3D_VERSION=v5.9.0
SRW_K3D_SHA256=06d8f25bc3a971c4eb29e0ff08429b180402db0f4dec838c9eac427e296800a0
curl -fsSL "https://github.com/k3d-io/k3d/releases/download/${SRW_K3D_VERSION}/k3d-linux-amd64" -o /tmp/k3d
echo "${SRW_K3D_SHA256}  /tmp/k3d" | sha256sum -c -
sudo install -m 0755 /tmp/k3d /usr/local/bin/k3d && rm -f /tmp/k3d

# tilt — pinned GitHub release tarball.
SRW_TILT_VERSION=0.37.7
SRW_TILT_SHA256=b695193fab68def8310cb971fa60bbe47ba0a782e24f54ebad287c13316a61b0
curl -fsSL "https://github.com/tilt-dev/tilt/releases/download/v${SRW_TILT_VERSION}/tilt.${SRW_TILT_VERSION}.linux.x86_64.tar.gz" -o /tmp/tilt.tgz
echo "${SRW_TILT_SHA256}  /tmp/tilt.tgz" | sha256sum -c -
tar -xzf /tmp/tilt.tgz -C /tmp tilt
sudo install -m 0755 /tmp/tilt /usr/local/bin/tilt && rm -f /tmp/tilt.tgz /tmp/tilt

# mkcert — pinned GitHub release binary (the guide's local CA). `mkcert
# -install` is per-user and runs at job time; libnss3-tools lets it reach
# Chromium's trust store.
SRW_MKCERT_VERSION=v1.4.4
SRW_MKCERT_SHA256=6d31c65b03972c6dc4a14ab429f2928300518b26503f58723e532d1b0a3bbb52
curl -fsSL "https://github.com/FiloSottile/mkcert/releases/download/${SRW_MKCERT_VERSION}/mkcert-${SRW_MKCERT_VERSION}-linux-amd64" -o /tmp/mkcert
echo "${SRW_MKCERT_SHA256}  /tmp/mkcert" | sha256sum -c -
sudo install -m 0755 /tmp/mkcert /usr/local/bin/mkcert && rm -f /tmp/mkcert
sudo eatmydata apt-get install -y libnss3-tools

for bin in docker kubectl helm k3d tilt mkcert; do
    command -v "$bin" >/dev/null || { echo "ERROR: $bin missing after install" >&2; exit 1; }
done
docker --version; kubectl version --client; helm version --short; k3d version; tilt version; mkcert -version

# -----------------------------------------------------------------------------
# 3. Datasource CLI clients (psql, mongosh, cypher-shell)
#     Agent uses these via run_command for read/write datasource queries.
#     Mirrors docker/Dockerfile.workspace datasource section.
# -----------------------------------------------------------------------------

_section "Installing datasource CLI clients"

# Add MongoDB and Neo4j APT repos first, then a single apt-get update + install
# for all three clients (postgresql-client comes from Ubuntu's default repos so
# no custom source is needed). One update instead of three saves ~30-60s on
# slow links since each apt-get update fetches ~100 MB of metadata.
curl -fsSL https://www.mongodb.org/static/pgp/server-8.0.asc \
    | sudo gpg --dearmor -o /usr/share/keyrings/mongodb-server-8.0.gpg
echo "deb [signed-by=/usr/share/keyrings/mongodb-server-8.0.gpg] https://repo.mongodb.org/apt/ubuntu noble/mongodb-org/8.0 multiverse" \
    | sudo tee /etc/apt/sources.list.d/mongodb-org-8.0.list

curl -fsSL https://debian.neo4j.com/neotechnology.gpg.key \
    | sudo gpg --dearmor -o /usr/share/keyrings/neo4j-archive-keyring.gpg
echo "deb [signed-by=/usr/share/keyrings/neo4j-archive-keyring.gpg] https://debian.neo4j.com stable latest" \
    | sudo tee /etc/apt/sources.list.d/neo4j.list

sudo apt-get update -y
sudo eatmydata apt-get install -y \
    postgresql-client \
    mongodb-mongosh \
    openjdk-17-jre-headless \
    cypher-shell

# -----------------------------------------------------------------------------
# 3b. rclone — pinned upstream release with checksum verification (not apt:
#     Noble ships 1.60.1-DEV, too old for OpenCloud's webdav vendor). Keep
#     version and checksum in sync with docker/Dockerfile.workspace.
# -----------------------------------------------------------------------------

_section "Installing rclone (pinned)"

# SRW_ prefix matters: rclone parses RCLONE_* env vars as flags, so a stray
# exported RCLONE_VERSION would crash every rclone invocation.
SRW_RCLONE_VERSION=1.75.2
SRW_RCLONE_SHA256=efbfe852181f7191eb3c9043ed1ab49c9b2d0ba045c61c926a5da111c939ec5c
curl -fsSL "https://downloads.rclone.org/v${SRW_RCLONE_VERSION}/rclone-v${SRW_RCLONE_VERSION}-linux-amd64.deb" \
    -o /tmp/rclone.deb
echo "${SRW_RCLONE_SHA256}  /tmp/rclone.deb" | sha256sum -c -
sudo dpkg -i /tmp/rclone.deb
rm /tmp/rclone.deb
rclone version

# fuse-overlayfs version gate — the overlay-over-rclone spike (see
# knowledge-base/knowledge/superpowers/plans/2026-07-09-protected-cloud-mode-phase0-spike.md)
# needs the big-dir/readdir fixes that landed in 1.13. Fail the build loudly
# rather than silently shipping a too-old binary.
#
# The sed must be anchored to the tool's own "fuse-overlayfs: version X"
# line: `--version` also prints the FUSE library version and the FUSE
# kernel interface version, each on their own "... version N.N" line, so
# an unanchored `.*version \(...\)` pattern matches all three lines and
# `$v` becomes a multi-line string that makes `dpkg --compare-versions`
# print "bad syntax" on every build.
fuse-overlayfs --version
v="$(fuse-overlayfs --version | sed -n 's/^fuse-overlayfs: version \([0-9.]*\).*/\1/p')"
dpkg --compare-versions "$v" ge 1.13 \
    || { echo "fuse-overlayfs $v < 1.13"; exit 1; }

# -----------------------------------------------------------------------------
# 4. Tailscale (mesh VPN — joins Headscale tailnet at VM boot via cloud-init)
# -----------------------------------------------------------------------------

_section "Installing Tailscale"
curl -fsSL https://tailscale.com/install.sh | sh
sudo systemctl enable tailscaled

# -----------------------------------------------------------------------------
# 5. Python setup (Ubuntu 24.04 ships Python 3.12)
# -----------------------------------------------------------------------------

_section "Configuring Python"

# Ensure python/python3 point to the system python
sudo update-alternatives --install /usr/bin/python python /usr/bin/python3 1 || true

# Management daemon dependencies (system-wide so they survive packer user deletion)
sudo python3 -m pip install --break-system-packages nats-py psutil

# -----------------------------------------------------------------------------
# 6. Playwright Chromium + browser-use (replaces apt chromium-browser, which on
#    Ubuntu Noble is a snap shim that hangs the build with no snap-store
#    connectivity). Pin propagated from .playwright-version via the Packer var
#    of the same name; symlink is provisioned at image build time only —
#    runtime upgrades would dangle the symlink and are not supported.
#
#    Keep in lockstep with docker/Dockerfile.workspace: the container workspace
#    installs this same trio (playwright + browser-use + chromium) and both
#    images are gated by docker/assert-browser-stack.sh. browser-exec itself is
#    per-commit source and is installed in stage2, not here.
# -----------------------------------------------------------------------------

_section "Installing Playwright Chromium + browser-use"
: "${PLAYWRIGHT_VERSION:?PLAYWRIGHT_VERSION must be set by Packer (see .playwright-version)}"

# browser-use is CAPPED below 0.13.0: 0.13 (2026-06-08) rewrote the browser
# layer to pure CDP and dropped Playwright, so it no longer finds the
# Playwright-installed Chromium under PLAYWRIGHT_BROWSERS_PATH and falls back to
# auto-installing one via `uvx playwright install` — which fails on this image
# ([Errno 2] No such file or directory: 'uvx'), breaking every browser_* tool.
# 0.12.x (validated 2026-05-27) is the last Playwright-compatible line the
# browser-exec daemon is written against. Do NOT loosen this cap without
# upgrading docker/browser-exec to the 0.13 API.
#
# Because this browser_use requirement currently lives on develop AHEAD of main,
# develop publishes its OWN stage1 base under the :experimental tag while main
# keeps :latest (see .github/workflows/stage1-rebuild.yml + develop.yml). A
# shared :latest would let a main-built base — lacking browser_use — fail
# develop's stage2 browser-stack gate on every build.
#
# --ignore-installed is required HERE but not in docker/Dockerfile.workspace,
# and the difference is the base image. The container starts from a minimal
# ubuntu:24.04; this VM starts from the Ubuntu *cloud image*, which preinstalls
# apt-managed Python packages for cloud-init (python3-typing-extensions,
# python3-jwt, ...). Those ship no RECORD file, so when browser-use's dependency
# resolver tries to upgrade them pip aborts with:
#     ERROR: Cannot uninstall typing_extensions 4.10.0, RECORD file not found.
#            Hint: The package was installed by debian.
# Purging them is not an option — cloud-init depends on them. --ignore-installed
# instead lets pip install its own copies under /usr/local/lib/python3.12/
# dist-packages, which precedes /usr/lib/python3/dist-packages on sys.path, so
# pip's versions win at import time while apt's stay intact for cloud-init.
#
# Keep playwright's pin NAMED in this same command: --ignore-installed
# reinstalls the whole dependency tree, and browser-use depends on playwright,
# so an unnamed playwright would be silently resolved to whatever browser-use
# prefers instead of .playwright-version. (Verified: this command yields
# browser-use 0.12.9 + playwright 1.59.0 on the cloud image.)
sudo python3 -m pip install --break-system-packages --ignore-installed \
    "playwright==${PLAYWRIGHT_VERSION}" "browser-use>=0.12.9,<0.13.0"
# fonts-noto-core: extended Unicode/CJK coverage that --with-deps does not pull
sudo eatmydata apt-get install -y --no-install-recommends fonts-noto-core
sudo PLAYWRIGHT_BROWSERS_PATH=/opt/playwright eatmydata playwright install --with-deps chromium

# Playwright's directory layout varies by version: older revs use chrome-linux/,
# newer (>=1.49) use chrome-linux64/. Match both, exclude headless-shell builds.
CHROMIUM_BIN=$(sudo find /opt/playwright -maxdepth 4 -type f \( -path '*/chrome-linux/chrome' -o -path '*/chrome-linux64/chrome' \) ! -path '*headless_shell*' | head -1)
test -n "$CHROMIUM_BIN" || { echo "ERROR: chromium binary not found under /opt/playwright"; sudo find /opt/playwright -maxdepth 4 -type d; exit 1; }
sudo ln -sf "$CHROMIUM_BIN" /usr/local/bin/agent-chromium
# No PLAYWRIGHT_BROWSERS_PATH: it would send every Playwright version, including
# a project's own, to this read-only folder. The agent's Playwright finds these
# browsers through links in ~/.cache/ms-playwright, made in stage2.
echo "Chromium installed at $CHROMIUM_BIN (symlinked /usr/local/bin/agent-chromium)"

# Assert stage1's half of the browser contract here rather than deferring it all
# to the stage2 gate: stage1 is the expensive rebuild, so its own breakage should
# fail in its own layer. The full contract (incl. browser-exec) is asserted by
# docker/assert-browser-stack.sh at the end of stage2.
python3 -c "import browser_use" \
    || { echo "ERROR: browser-use installed but not importable — see cap note above"; exit 1; }
/usr/local/bin/agent-chromium --headless --no-sandbox --version \
    || { echo "ERROR: agent-chromium is present but will not run"; exit 1; }

# -----------------------------------------------------------------------------
# 7. Node.js 22 + minimal global npm packages
#    Trimmed from the prior list (typescript, ts-node, @angular/cli, eslint,
#    prettier, yarn). Only typescript + prettier are commonly invoked by
#    agent jobs; ts-node and @angular/cli have no recorded usage in src/
#    or config/, and yarn/pnpm are now provided on-demand via corepack.
# -----------------------------------------------------------------------------

_section "Installing Node.js 22"
curl -fsSL https://deb.nodesource.com/setup_22.x | sudo -E bash -
sudo eatmydata apt-get install -y nodejs

_section "Installing global npm packages"
sudo npm install -g \
    typescript \
    prettier
# Enable corepack so `yarn`/`pnpm` install on-demand via shims.
sudo corepack enable

# -----------------------------------------------------------------------------
# 8. code-server (Web IDE for live-VM IDE sessions)
#     Pinned upstream .deb + SHA256 (same pattern as rclone in section 3b).
#     Reached only through the agent's authenticated SSH direct-tcpip channel
#     to guest loopback — see knowledge-base/knowledge/features/vm_snapshots_and_ide.md
#     "Live-VM IDE Access via the Agent" — so it binds 127.0.0.1 with auth
#     disabled; that config + a (disabled) systemd unit land in stage2. The
#     .deb bundles its own Node runtime and does NOT use the system Node.js
#     installed in section 7.
#
#     Version + checksum come from the GitHub release asset digest
#     (https://api.github.com/repos/coder/code-server/releases). Bump both
#     together; a stale checksum fails the build loudly at `sha256sum -c`.
# -----------------------------------------------------------------------------

_section "Installing code-server (pinned)"

SRW_CODE_SERVER_VERSION=4.128.0
SRW_CODE_SERVER_SHA256=c0f6e3706c4285f06bb2350274576f289262e6a9962a70e3c31c6d3ea17a29d2
curl -fsSL "https://github.com/coder/code-server/releases/download/v${SRW_CODE_SERVER_VERSION}/code-server_${SRW_CODE_SERVER_VERSION}_amd64.deb" \
    -o /tmp/code-server.deb
echo "${SRW_CODE_SERVER_SHA256}  /tmp/code-server.deb" | sha256sum -c -
# apt-get (not bare `dpkg -i`) so any .deb dependencies resolve automatically.
sudo eatmydata apt-get install -y /tmp/code-server.deb
rm /tmp/code-server.deb
code-server --version

_section_end
echo "=== Stage 1 complete ==="
