#!/usr/bin/env bash
# =============================================================================
# Build-time conformance gate for the workspace browser stack.
# =============================================================================
#
# Both workspace images must satisfy this identical contract:
#   - docker/Dockerfile.workspace   (container workspace)
#   - docker/agent-vm-base/         (VM golden image, asserted in stage2)
#
# This file is SHARED rather than duplicated on purpose. The two images are
# hand-maintained twins, and the VM shipped for ~5 weeks with a working Chromium
# that no agent could reach: browser-exec and browser-use were added to the
# container only. Nothing asserted the images agreed, so every VM-backed job
# silently degraded to "No renderer available" static-only verification.
# See knowledge-base/knowledge/issues/vm_workspace_missing_browser_exec.md.
#
# A per-image assertion would not have caught that — adding a capability to one
# image and its own private assert leaves the other image silently short. A
# shared gate fails the build of whichever image lacks the capability. When you
# add to the browser stack, add it HERE, and expect both builds to fail until
# both images install it. That failure is the feature.
#
# Usage: assert-browser-stack.sh   (exits non-zero with a diagnostic on failure)
# =============================================================================
set -uo pipefail

# Run from a directory every user can traverse. browser_use reads `./.env` at
# import time (pydantic-settings), and stat'ing it from an unreadable CWD
# raises EACCES — which reads as "browser_use not importable". Exactly that
# failed every stage2 VM build: provision-stage2.sh invokes this gate as
# `sudo -u agent-host` from /home/packer, which is 0750 on Ubuntu 24.04
# (Noble made home dirs private), so agent-host could not stat `./.env`.
cd /

fail=0

_check() {
    local label="$1"
    shift
    local out
    # Capture rather than discard: "MISSING" with no diagnostic once cost a
    # multi-hour investigation — an import can fail for reasons other than
    # absence (dependency conflict, permission error), and only the output
    # distinguishes them.
    if out=$("$@" 2>&1); then
        printf '  ok       %s\n' "$label"
        if [ "$label" = "shared-browser stream conformance" ] && [ -n "$out" ]; then
            printf '%s\n' "$out"
        fi
    else
        printf '  MISSING  %s\n' "$label"
        if [ -n "$out" ]; then
            printf '%s\n' "$out" | sed 's/^/           | /'
        fi
        fail=1
    fi
}

echo "Asserting workspace browser stack:"

# The only sanctioned browser entry point. The agent invokes it over SSH
# (src/agent/tools/context.py) and has no local fallback by design — the in-pod
# browser was removed deliberately (knowledge-base/knowledge/issues/remove_local_browser_fallback.md).
# If browser-exec is absent the tool returns an opaque empty-stdout error and the
# agent concludes no renderer exists anywhere.
_check "browser-exec on PATH"      command -v browser-exec

# browser-exec imports browser_use at runtime; a missing import makes the daemon
# unspawnable in exactly the same silent way as a missing browser-exec.
_check "browser_use importable"    python3 -c "import browser_use"
_check "playwright importable"     python3 -c "import playwright"

# browser-exec launches this exact path (BROWSER_EXEC_CHROMIUM default). `test -x`
# follows symlinks, so this also catches a dangling agent-chromium — the symlink
# target is provisioned at build time only and runtime upgrades would dangle it.
_check "agent-chromium executable" test -x /usr/local/bin/agent-chromium

# Proves the whole stack, not just its file layout: a present-but-unrunnable
# Chromium fails here rather than on the first agent screenshot.
_check "agent-chromium runs"       /usr/local/bin/agent-chromium --headless --no-sandbox --version

# browser-exec renders headful under Xvfb by default (BROWSER_EXEC_HEADFUL), so
# a missing Xvfb silently downgrades every workspace to the headless fingerprint
# — coherent-geometry and WebGL checks start failing on sites that gate on them,
# with nothing in the job output saying why. Xvfb arrives via Playwright's
# `install --with-deps`, i.e. as a transitive dependency nobody declared: exactly
# the kind of thing that vanishes on an upstream dependency-list change. Assert
# it like any other load-bearing binary.
_check "Xvfb present"              command -v Xvfb

# The agent's own Playwright looks in ~/.cache/ms-playwright, not in
# /opt/playwright where the image installs the browsers. link-playwright-browsers
# links them there (at every container start, once in the VM build). Link into
# an empty home and launch Playwright's default browser from it: without the
# links Playwright downloads its own copy, or fails without network.
_playwright_default_folder() {
    local home status
    home=$(mktemp -d)
    env -u PLAYWRIGHT_BROWSERS_PATH -u XDG_CACHE_HOME HOME="$home" \
        /usr/local/bin/link-playwright-browsers \
        && env -u PLAYWRIGHT_BROWSERS_PATH -u XDG_CACHE_HOME HOME="$home" \
            python3 -c 'from playwright.sync_api import sync_playwright
with sync_playwright() as p:
    p.chromium.launch().close()'
    status=$?
    rm -rf "$home"
    return "$status"
}
_check "Playwright finds the image's browsers" _playwright_default_folder

# Exercises the actual browser-use/CDP stream seam, including loading, active
# target handoff, viewer limits, and a zero-viewer restart. The program owns and
# removes all of its temporary state so this is safe inside an image-build RUN.
_check "shared-browser stream conformance" \
    /usr/local/bin/check-browser-stream

if [ "$fail" -ne 0 ]; then
    cat >&2 <<'EOF'

ERROR: workspace browser stack incomplete (see MISSING above).

Every workspace image must install all of:
  - playwright (pin from .playwright-version) + chromium in /opt/playwright,
    symlinked to /usr/local/bin/agent-chromium
  - docker/link-playwright-browsers -> /usr/local/bin/link-playwright-browsers,
    chmod +x, run as the workspace user (container entrypoint, VM stage2)
  - browser-use, CAPPED <0.13.0 (0.13 dropped Playwright for pure CDP and
    breaks the browser-exec daemon)
  - docker/browser-exec -> /usr/local/bin/browser-exec, chmod +x
  - docker/check-browser-stream.py -> /usr/local/bin/check-browser-stream

Container: docker/Dockerfile.workspace
VM:        docker/agent-vm-base/scripts/provision-stage1.sh (pip deps, chromium)
           docker/agent-vm-base/scripts/provision-stage2.sh (browser-exec, browser links)
EOF
    exit 1
fi

echo "Workspace browser stack OK."
