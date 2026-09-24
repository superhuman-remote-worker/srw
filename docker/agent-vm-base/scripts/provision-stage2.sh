#!/usr/bin/env bash
# =============================================================================
# Agent VM Base Image — Stage 2 Provisioning
# =============================================================================
#
# Light, per-commit bits applied on top of the stage1 qcow2:
#   - User setup (agent-host for SSH + workspace)
#   - SSH server config tuned for RemoteBackend
#   - Management daemon (NATS bridge to orchestrator)
#   - Sudo approval gate (plugin .so + Go daemon)
#   - tmux + git config
#
# Files (daemon binaries, sudo-gate artifacts, configs) are uploaded to /tmp/
# by Packer file provisioners before this script runs. Most stage2 changes
# are: new sudo-gate binary version, updated daemon Python, config tweaks.
# =============================================================================

set -euxo pipefail

echo "=== Stage 2: light provisioning ==="

# -----------------------------------------------------------------------------
# Profiling helper — same shape as stage1 for grep-friendly post-run analysis.
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
    echo ">>> [PROFILE] stage2 total: ${SECONDS}s"
}

# -----------------------------------------------------------------------------
# 1. Users and directories
# -----------------------------------------------------------------------------

_section "Setting up users"

# agent-host: the SSH user that RemoteBackend connects as. Skip if stage1
# already created it (defensive — currently stage1 doesn't, but lets us
# safely re-run stage2 against an already-stage2'd image during local dev).
if ! id agent-host >/dev/null 2>&1; then
    sudo useradd -m -s /bin/bash agent-host
fi
echo "agent-host ALL=(ALL) NOPASSWD:ALL" | sudo tee /etc/sudoers.d/agent-host
sudo chmod 0440 /etc/sudoers.d/agent-host
# Allow agent-host to read systemd journal (for debugging daemon issues)
sudo usermod -aG systemd-journal agent-host
# Docker Engine (stage 1) is rootful; group membership is what lets agent-host
# drive it without sudo — and it must be baked in here, because a group added
# at job time only reaches NEW login shells (probe 1, 2026-09-04, obstacle 5).
sudo usermod -aG docker agent-host

# Workspace lives in a dedicated subdirectory of home — keeps dotfiles
# separate and provides a clean target for git clone.
sudo mkdir -p /home/agent-host/workspace
sudo chown agent-host:agent-host /home/agent-host/workspace

# SSH authorized_keys outside home dir — keeps ~ clean for workspace use.
# Keys are injected at runtime by cloud-init.
sudo mkdir -p /etc/ssh/authorized_keys
sudo chmod 755 /etc/ssh/authorized_keys

# ---------------------------------------------------------------------------
# Rootless podman for agent-host (engine + deps installed in stage 1).
# ---------------------------------------------------------------------------
# Rootless needs a subordinate uid/gid range; without one podman exits with
# "cannot find UID/GID for user agent-host". useradd only writes these when
# the distro default is configured, so set them explicitly and idempotently.
if ! grep -q '^agent-host:' /etc/subuid; then
    echo 'agent-host:100000:65536' | sudo tee -a /etc/subuid > /dev/null
fi
if ! grep -q '^agent-host:' /etc/subgid; then
    echo 'agent-host:100000:65536' | sudo tee -a /etc/subgid > /dev/null
fi

# Keep a systemd user session alive without an interactive login. RemoteBackend
# connects over SSH and sessions come and go; without lingering, /run/user/<uid>
# disappears between them and long-running containers die with the session.
sudo loginctl enable-linger agent-host || true

# Enable the rootless podman API socket by writing the symlink `systemctl
# --user enable` would create. Done by hand because there is no user D-Bus
# during the image build. The socket is what Docker-API clients (compose v2,
# testcontainers) talk to; the `docker` CLI shim does not need it.
sudo -u agent-host mkdir -p /home/agent-host/.config/systemd/user/sockets.target.wants
sudo -u agent-host ln -sf /usr/lib/systemd/user/podman.socket \
    /home/agent-host/.config/systemd/user/sockets.target.wants/podman.socket

# DOCKER_HOST is deliberately NOT exported. /usr/bin/docker is Docker Engine's
# own CLI and talks to /var/run/docker.sock (agent-host is in `docker`). The
# rootless podman socket stays reachable for anything that asks for it
# explicitly (CONTAINER_HOST / `podman --remote`); exporting it as DOCKER_HOST
# silently redirected the Docker CLI to podman even after Docker Engine was
# installed (probe 1, 2026-09-04, obstacle 5). Remove any snippet a previous
# image revision left behind so a re-run of stage 2 converges.
sudo rm -f /etc/profile.d/podman-docker-host.sh

# Agent runtime directory
sudo mkdir -p /run/agent
sudo chmod 755 /run/agent

# Ensure /run/agent survives reboots via tmpfiles.d
echo "d /run/agent 0755 root root -" | sudo tee /etc/tmpfiles.d/agent.conf

# -----------------------------------------------------------------------------
# 2. SSH server configuration
# -----------------------------------------------------------------------------

_section "Configuring SSH"
sudo tee /etc/ssh/sshd_config.d/agent.conf > /dev/null <<'SSHEOF'
# Agent VM SSH config — optimized for RemoteBackend
PermitRootLogin no
PasswordAuthentication no
PubkeyAuthentication yes
AuthorizedKeysFile /etc/ssh/authorized_keys/%u
X11Forwarding no
PrintMotd no
AcceptEnv LANG LC_*
Subsystem sftp /usr/lib/openssh/sftp-server
# Canvas reaches declared guest HTTP ports only through request-scoped
# direct-tcpip channels on this authenticated SSH transport. Keep forwarding
# local to the SSH client, restrict its destination to guest loopback, and
# disable the unrelated forwarding/tunnel surfaces explicitly.
AllowTcpForwarding local
PermitOpen 127.0.0.1:*
GatewayPorts no
AllowAgentForwarding no
PermitTunnel no
# Keep connections alive (agent may have idle periods between tool calls)
ClientAliveInterval 60
ClientAliveCountMax 720
MaxStartups 10:30:100
# Above the OpenSSH default (10): the agent multiplexes parallel tool execs
# plus persistent SFTP/shell channels over ONE transport; mirrors
# docker/Dockerfile.workspace. See
# knowledge-base/knowledge/issues/maxsessions_parallel_tools_false_workspace_death.md
MaxSessions 16
SSHEOF

sudo systemctl enable ssh

# -----------------------------------------------------------------------------
# 3. Management daemon
# -----------------------------------------------------------------------------

_section "Installing management daemon"
sudo mkdir -p /opt/srw
sudo cp /tmp/management-daemon.py /opt/srw/management-daemon.py
sudo chmod 755 /opt/srw/management-daemon.py

sudo cp /tmp/management-daemon.service /etc/systemd/system/management-daemon.service
sudo systemctl daemon-reload
# Don't enable here — cloud-init runcmd starts it with the correct env vars.
# The daemon's _wait_for_cloud_init() method also ensures SSH keys are in
# place before registering, as a safety net.

# Create default env file (overwritten by cloud-init at VM creation)
sudo tee /etc/default/management-daemon > /dev/null <<'EOF'
# Overwritten by cloud-init at VM creation time
NATS_URL=
JOB_ID=
ORCHESTRATOR_ID=
EOF

# -----------------------------------------------------------------------------
# 3b. code-server (Web IDE)
#
# The binary is installed in stage1. Here we place the loopback / auth-none
# config and a DISABLED systemd unit. The orchestrator starts and stops it over
# SSH for IDE sessions (see knowledge-base/knowledge/features/
# vm_snapshots_and_ide.md, "Live-VM IDE Access via the Agent"); we deliberately
# do NOT enable it, so it stays dormant during normal headless job runs.
# -----------------------------------------------------------------------------

_section "Installing code-server config + unit"

sudo mkdir -p /etc/code-server
sudo install -o root -g root -m 0644 /tmp/code-server-config.yaml /etc/code-server/config.yaml

sudo install -o root -g root -m 0644 /tmp/code-server.service /etc/systemd/system/code-server.service
sudo systemctl daemon-reload
# Intentionally NOT `systemctl enable`d — the orchestrator manages it over SSH.

# The owner-authorized live IDE starts an agent-host user unit.  A lingering
# user manager survives the SSH channel used to request startup; the unit is
# deliberately not enabled, so no code-server process starts at guest boot.
sudo install -d -o agent-host -g agent-host -m 0755 /home/agent-host/.config/systemd/user
sudo install -o agent-host -g agent-host -m 0644 \
  /tmp/srw-code-server-user.service \
  /home/agent-host/.config/systemd/user/srw-code-server-user.service
sudo loginctl enable-linger agent-host

# user-data-dir / extensions-dir live outside $HOME (see config). code-server
# runs as agent-host, so agent-host must own this tree.
sudo mkdir -p /var/lib/code-server/extensions
sudo chown -R agent-host:agent-host /var/lib/code-server

# -----------------------------------------------------------------------------
# 4. Sudo approval gate
# -----------------------------------------------------------------------------
#
# The sudo approval gate intercepts every sudo invocation and forwards it
# to the orchestrator for human approval. Components:
#   - sudo_gate.so    — C plugin loaded by sudo (compiled from vm/sudo-plugin/)
#   - sudo-gated      — Go daemon bridging plugin to orchestrator (vm/sudo-daemon/)
#
# Compiled binaries are expected at /tmp/ (placed by Packer file provisioner
# from CI artifacts, or compiled during an earlier build step).
# Both binaries are required; an image without the gate must not be published.

_section "Setting up sudo approval gate"

if [ -s /tmp/sudo_gate.so ] && [ -s /tmp/sudo-gated ]; then
    echo "Installing plugin .so..."
    sudo install -o root -g root -m 0644 /tmp/sudo_gate.so /usr/libexec/sudo/

    echo "Installing daemon binary..."
    sudo install -o root -g root -m 0755 /tmp/sudo-gated /usr/local/bin/

    echo "Creating daemon user..."
    if ! getent group sudo-gated >/dev/null 2>&1; then
        sudo groupadd -r sudo-gated
    fi
    if ! id sudo-gated >/dev/null 2>&1; then
        sudo useradd -r -g sudo-gated -s /usr/sbin/nologin -d /nonexistent sudo-gated
    fi

    echo "Installing systemd units..."
    sudo install -o root -g root -m 0644 /tmp/sudo-gated.service /etc/systemd/system/
    sudo install -o root -g root -m 0644 /tmp/sudo-gated.socket /etc/systemd/system/

    echo "Installing daemon config..."
    sudo mkdir -p /etc/sudo-gate
    sudo install -o root -g root -m 0644 /tmp/sudo-gated-config.yaml /etc/sudo-gate/config.yaml

    echo "Setting up tmpfiles.d..."
    sudo mkdir -p /etc/tmpfiles.d
    sudo sh -c 'echo "d /run/sudo-gated 0775 root sudo-gated -" > /etc/tmpfiles.d/sudo-gated.conf'

    echo "Enabling socket activation..."
    sudo systemctl daemon-reload
    sudo systemctl enable sudo-gated.socket

    echo "Applying immutable flags..."
    sudo chattr +i /usr/libexec/sudo/sudo_gate.so 2>/dev/null || echo "  chattr skipped (unsupported fs)"

    # Register plugin in sudo.conf LAST — once registered, the plugin runs on
    # every sudo invocation. Since the daemon isn't running during provisioning,
    # fail_mode=deny would break all subsequent sudo commands in this script
    # and in later Packer provisioners (tmux, git config, cleanup).
    # We use fail_mode=open here; cleanup.sh seals it to fail_mode=deny after
    # every remaining provisioning command has completed.
    echo "Registering plugin in sudo.conf..."
    # Strip the immutable flag if a prior stage2 already set it (idempotent re-run)
    sudo chattr -i /etc/sudo.conf 2>/dev/null || true
    if ! grep -q "sudo_gate_approval" /etc/sudo.conf; then
        sudo sh -c 'echo "Plugin sudo_gate_approval sudo_gate.so socket_path=/run/sudo-gated/sudo-gated.sock timeout=305 fail_mode=open" >> /etc/sudo.conf'
    fi
    sudo chattr +i /etc/sudo.conf 2>/dev/null || echo "  chattr skipped (unsupported fs)"

    echo "Sudo approval gate installed"
else
    echo "ERROR: sudo gate binaries are missing or empty" >&2
    exit 1
fi

# Default env file for sudo-gated (always created, overwritten by cloud-init).
# Placed outside the if-block because heredocs inside conditional blocks
# fail under Packer's SSH script provisioner.
sudo tee /etc/default/sudo-gated > /dev/null <<'SGEOF'
# Overwritten by cloud-init at VM creation time
NATS_URL=
JOB_ID=
ORCHESTRATOR_ID=
VM_ID=
SGEOF

# -----------------------------------------------------------------------------
# 5. tmux configuration
# -----------------------------------------------------------------------------

_section "Configuring tmux"
sudo tee /home/agent-host/.tmux.conf > /dev/null <<'TMUXEOF'
# Increase scrollback buffer
set-option -g history-limit 50000
# Mouse support
set -g mouse on
# 256 colors
set -g default-terminal "screen-256color"
TMUXEOF
sudo chown agent-host:agent-host /home/agent-host/.tmux.conf

# -----------------------------------------------------------------------------
# 6. Git configuration
# -----------------------------------------------------------------------------

_section "Configuring git"
sudo -u agent-host git config --global init.defaultBranch main
sudo -u agent-host git config --global user.name "Agent Worker"
sudo -u agent-host git config --global user.email "agent@srw.local"
sudo -u agent-host git config --global core.editor vim
sudo -u agent-host git config --global core.pager cat

# -----------------------------------------------------------------------------
# 7. browser-exec — workspace-side browser executor
# -----------------------------------------------------------------------------
#
# The agent drives this over SSH (src/agent/tools/context.py) so Chrome's CDP stays on
# the workspace loopback and never crosses the network. It is the agent's ONLY
# browser path — the in-pod fallback was removed deliberately
# (knowledge-base/knowledge/issues/remove_local_browser_fallback.md) — so a workspace without
# browser-exec cannot render at all. It fails opaquely, too: the tool returns
# "browser-exec returned no output" and the agent concludes no renderer exists
# anywhere, then remembers that conclusion.
#
# This is the same file the container workspace ships (docker/browser-exec),
# uploaded from ../ by the Packer file provisioner rather than copied into
# files/. It lives in stage2 because it is per-commit source, not a heavy stable
# dep: editing it must not force the slow stage1 rebuild. Its browser-use and
# chromium dependencies come from stage1.

_section "Installing browser-exec"
sudo install -o root -g root -m 0755 /tmp/browser-exec /usr/local/bin/browser-exec
sudo install -o root -g root -m 0755 /tmp/check-browser-stream.py /usr/local/bin/check-browser-stream

# -----------------------------------------------------------------------------
# 8. Browser stack conformance gate
# -----------------------------------------------------------------------------
#
# Shared with docker/Dockerfile.workspace — see docker/assert-browser-stack.sh
# for why this is one shared file rather than a per-image check. Runs last so it
# sees the finished image, and runs as agent-host (not root) because that is the
# user the agent actually SSHes in as: it must pass on *that* PATH.
#
# Also installed permanently, so "can this workspace render?" is answerable in
# five seconds on any live box instead of five weeks.

_section "Asserting browser stack"
sudo install -o root -g root -m 0755 /tmp/assert-browser-stack.sh /usr/local/bin/assert-browser-stack
sudo -u agent-host /usr/local/bin/assert-browser-stack

# -----------------------------------------------------------------------------
# 9. Container stack conformance gate (VM-only)
# -----------------------------------------------------------------------------
#
# Same reasoning as the browser gate, same failure history: a capability that
# is present but unreachable is worse than one that is absent, because nothing
# reports it. Runs as agent-host so it checks the subuid/subgid ranges and PATH
# of the user the agent actually SSHes in as.
#
# Static contract only — a real `podman run` needs a user session that does not
# exist during the image build. On a live VM, `assert-container-stack --run`
# completes the proof in five seconds.

_section "Asserting container stack"
sudo install -o root -g root -m 0755 /tmp/assert-container-stack.sh /usr/local/bin/assert-container-stack
sudo -u agent-host /usr/local/bin/assert-container-stack

_section_end
echo "=== Stage 2 complete ==="
