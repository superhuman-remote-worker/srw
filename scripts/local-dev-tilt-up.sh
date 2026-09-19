#!/usr/bin/env bash
# =============================================================================
# local-dev-tilt-up.sh — Tilt-aware bootstrap.
#
# Builds on top of scripts/local-dev-up.sh:
#   1. Runs the base bootstrap (cluster, local DNS/TLS, namespace, Secrets,
#      and vendored chart dependencies).
#   2. Runs `tilt up` (foreground, ^C to stop).
#
# Idempotent: re-runs are safe. Use it to bring a stopped cluster back up
# or after `k3d cluster delete`.
#
# Prereq: `tilt` binary on PATH. See docs/development.md.
# =============================================================================
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)
cd "$REPO_ROOT"

VALUES_LOCAL="$REPO_ROOT/deployment/values-local.yaml"

log()  { printf '\033[1;34m[tilt-bootstrap]\033[0m %s\n' "$*"; }
die()  { printf '\033[1;31m[error]\033[0m %s\n' "$*" >&2; exit 1; }

# --- 0. Tilt-specific prereq -------------------------------------------------
command -v tilt >/dev/null \
  || die "tilt not on PATH — install it per docs/development.md"
helm upgrade --help 2>/dev/null | grep -F -- '--take-ownership' >/dev/null \
  || die "helm lacks --take-ownership — install a current Helm 3 release before using Tilt Force Update"
[[ -f "$VALUES_LOCAL" ]] \
  || die "deployment/values-local.yaml is missing — copy the example and add an LLM key first"

# --- 1. Base bootstrap -------------------------------------------------------
EXPOSURE_MODE="${SRW_EXPOSURE_MODE:-multi-host}"
log "running base bootstrap (exposure: $EXPOSURE_MODE; cluster + namespace + Secrets)"
"$SCRIPT_DIR/local-dev-up.sh"

# --- 2. Run Tilt -------------------------------------------------------------
cat <<EOF

$(printf '\033[1;32m✓ Cluster ready. Starting Tilt.\033[0m')

Tilt UI:      https://localhost:10350
Cockpit:      $([ "$EXPOSURE_MODE" = single-origin ] && printf 'https://localhost:8443' || printf 'https://localhost')   (test/srw-k3d-dev-test, after first build completes)

Press Ctrl-C to stop Tilt (cluster keeps running). To stop the cluster too:
  k3d cluster stop srw

EOF

exec tilt up
