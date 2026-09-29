#!/usr/bin/env bash
# =============================================================================
# Build-time gate for the SRW workspace contract.
# =============================================================================
#
# The orchestrator and the agent reach a container workspace over SSH and rely
# on the programs below for readiness, shell sessions, End and cleanup proofs,
# snapshots, git versioning, the IDE and the cloud mount. An image that lacks
# one of them still becomes Ready and then fails later, with a message that
# names none of this. This gate fails the image build instead.
#
# It runs at the end of both targets of docker/Dockerfile.workspace and stays in
# the image, so a live workspace can be checked in seconds.
#
# Usage: assert-workspace-contract   (exits non-zero with a diagnostic)
# =============================================================================
set -uo pipefail

cd /

fail=0

_check() {
    local label="$1"
    shift
    local out
    if out=$("$@" 2>&1); then
        printf '  ok       %s\n' "$label"
    else
        printf '  MISSING  %s\n' "$label"
        if [ -n "$out" ]; then
            printf '%s\n' "$out" | sed 's/^/           | /'
        fi
        fail=1
    fi
}

_on_path() {
    command -v "$1"
}

_sshd_setting() {
    grep -Eqsh "^[[:space:]]*$1[[:space:]]*\$" \
        /etc/ssh/sshd_config /etc/ssh/sshd_config.d/*.conf
}

_at_least() {
    # _at_least <found> <required>
    [ -n "$1" ] && [ "$(printf '%s\n%s\n' "$2" "$1" | sort -V | head -n 1)" = "$2" ]
}

_rclone_version() {
    rclone version 2>/dev/null | sed -n '1s/^rclone v\([0-9.]*\).*/\1/p'
}

_fuse_overlayfs_version() {
    fuse-overlayfs --version 2>/dev/null | sed -n 's/^fuse-overlayfs: version \([0-9.]*\).*/\1/p'
}

_owner_is_agent_host() {
    [ "$(stat -c %U "$1")" = "agent-host" ]
}

_no_sudo() {
    ! command -v sudo
}

echo "Asserting SRW workspace contract:"

# Readiness: the pod is Ready when sshd answers on 30022 and agent-host can
# log in with the key the entrypoint installs.
_check "sshd"                      test -x /usr/sbin/sshd
_check "ssh-keygen"                _on_path "ssh-keygen"
_check "sftp-server"               test -x /usr/lib/openssh/sftp-server
_check "sshd: Port 30022"          _sshd_setting "Port 30022"
_check "sshd: AuthorizedKeysFile"  _sshd_setting "AuthorizedKeysFile /etc/ssh/authorized_keys/%u"
_check "sshd: TrustedUserCAKeys"   _sshd_setting "TrustedUserCAKeys /etc/ssh/srw_user_ca.pub"
_check "sshd: principals file"     _sshd_setting "AuthorizedPrincipalsFile /etc/ssh/principals/%u"
_check "sshd: sftp subsystem"      _sshd_setting "Subsystem sftp /usr/lib/openssh/sftp-server"
_check "authorized_keys directory" test -d /etc/ssh/authorized_keys
_check "user agent-host is uid 1000" test "$(id -u agent-host 2>/dev/null)" = "1000"
_check "agent-host shell is bash"  test "$(getent passwd agent-host | cut -d: -f7)" = "/bin/bash"
_check "home skeleton"             test -d /etc/skel.agent-host
_check "entrypoint"                test -x /usr/local/bin/entrypoint.sh

# Shell sessions, attach and bounded mutations.
for binary in "tmux" "bash" "flock" "timeout" "mktemp" "grep" "awk" "sed"; do
    _check "$binary"               _on_path "$binary"
done

# End and cleanup proofs, credential install, snapshot restore.
_check "python3"                   _on_path "python3"

# Suspend, restore and IDE state.
_check "tar"                       _on_path "tar"
_check "zstd"                      _on_path "zstd"

# Git versioning and managed repositories.
_check "git"                       _on_path "git"
_check "ssh-agent"                 _on_path "ssh-agent"
_check "ssh-add"                   _on_path "ssh-add"

# Session IDE, and recovery after an aborted attach.
_check "code-server"               _on_path "code-server"

# Cloud mounts.
_check "rclone"                    _on_path "rclone"
_check "rclone >= 1.70.0"          _at_least "$(_rclone_version)" "1.70.0"
_check "fusermount3"               _on_path "fusermount3"
_check "mountpoint"                _on_path "mountpoint"
_check "fuse-overlayfs"            _on_path "fuse-overlayfs"
_check "fuse-overlayfs >= 1.13"    _at_least "$(_fuse_overlayfs_version)" "1.13"
_check "/cloud owned by agent-host" _owner_is_agent_host /cloud

# The sudo policy: agent-host must not be able to escalate.
_check "no sudo binary"            _no_sudo

if [ "$fail" -ne 0 ]; then
    cat >&2 <<'EOF'

ERROR: SRW workspace contract incomplete (see MISSING above).

A workspace image must provide everything listed in
examples/manifests/container-workspace-templates.md, section "Images".
The simplest way is to build FROM an SRW base image:
  ghcr.io/superhuman-remote-worker/srw-workspace-minimal
  ghcr.io/superhuman-remote-worker/srw-workspace
EOF
    exit 1
fi

echo "SRW workspace contract OK."
