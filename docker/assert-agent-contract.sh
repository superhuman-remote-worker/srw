#!/usr/bin/env bash
# =============================================================================
# Build-time gate for the SRW agent image contract.
# =============================================================================
#
# The agent process shells out to a handful of programs: rclone for the
# `virtual` tier's object store and its cloud folders, node/npx and uvx for
# MCP stdio servers, git and ripgrep for the tools. An image that lacks one
# of them, or carries a too-old rclone, still starts and fails later with a
# message that names none of this. This gate fails the image build instead,
# the way docker/assert-workspace-contract.sh does for the workspace image.
#
# It runs at the end of docker/Dockerfile.agent and docker/Dockerfile.agent.dev
# and stays in the image, so a live Pod can be checked in seconds.
#
# Usage: assert-agent-contract   (exits non-zero with a diagnostic)
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

_at_least() {
    # _at_least <found> <required>
    [ -n "$1" ] && [ "$(printf '%s\n%s\n' "$2" "$1" | sort -V | head -n 1)" = "$2" ]
}

_rclone_version() {
    rclone version 2>/dev/null | sed -n '1s/^rclone v\([0-9.]*\).*/\1/p'
}

echo "agent contract:"

# The virtual tier's object store and its cloud folders: upstream rclone,
# pinned in the Dockerfile (the distribution package is 1.60.1). 1.75.1 is the
# floor: rcat --size, the WebDAV vendors a cloud folder needs, the Nextcloud
# modtime and hash fixes, and the rc CVEs closed in 1.73.5 and 1.74.3.
_check "rclone"                    _on_path "rclone"
_check "rclone >= 1.75.1"          _at_least "$(_rclone_version)" "1.75.1"

# MCP stdio runtimes (node servers through npx, Python servers through uvx).
_check "node"                      _on_path "node"
_check "npx"                       _on_path "npx"
_check "uvx"                       _on_path "uvx"

# Tools that shell out.
_check "git"                       _on_path "git"
_check "rg"                        _on_path "rg"
_check "jq"                        _on_path "jq"

if [ "$fail" -ne 0 ]; then
    echo "agent contract: FAILED" >&2
    exit 1
fi
echo "agent contract: ok"
