#!/usr/bin/env bash
# =============================================================================
# Local development bootstrap: k3d cluster + KEDA + namespace + local runtime
# Secrets + vendored Helm chart dependencies. Multi-host mode additionally
# installs cert-manager and the host's mkcert CA; single-origin mode uses the
# chart-owned self-signed gateway at https://localhost:8443.
#
# Idempotent: re-runs are safe. Skips anything that already exists.
#
# After this, copy the values template and `helm install`:
#   cp deployment/values-local.yaml.example deployment/values-local.yaml
#   $EDITOR deployment/values-local.yaml      # paste at least one LLM key
#   helm install srw ./helm -n srw --kube-context k3d-srw \
#     -f deployment/values-local.yaml -f deployment/values-local-images.yaml
#
# Prerequisites (must be done once on the host BEFORE running this):
#   - docker + k3d + kubectl + helm + openssl installed
#   - multi-host only: mkcert installed, plus `mkcert -install`
# =============================================================================
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)

CLUSTER_NAME="${CLUSTER_NAME:-srw}"
NAMESPACE="${NAMESPACE:-srw}"
KUBE_CONTEXT="k3d-${CLUSTER_NAME}"
MKCERT_CAROOT="${MKCERT_CAROOT:-$HOME/.local/share/mkcert}"
CERT_MANAGER_VERSION="${CERT_MANAGER_VERSION:-v1.16.2}"
KEDA_VERSION="${KEDA_VERSION:-2.20.2}"
SRW_EXPOSURE_MODE="${SRW_EXPOSURE_MODE:-multi-host}"

log()  { printf '\033[1;34m[bootstrap]\033[0m %s\n' "$*"; }
ok()   { printf '\033[1;32m[ok]\033[0m %s\n' "$*"; }
skip() { printf '\033[1;33m[skip]\033[0m %s\n' "$*"; }
die()  { printf '\033[1;31m[error]\033[0m %s\n' "$*" >&2; exit 1; }

case "$SRW_EXPOSURE_MODE" in
  multi-host|single-origin) ;;
  *) die "SRW_EXPOSURE_MODE must be 'multi-host' or 'single-origin' (got '$SRW_EXPOSURE_MODE')" ;;
esac

# --- Prereq checks ----------------------------------------------------------
for bin in docker k3d kubectl helm openssl ssh-keygen git curl; do
  command -v "$bin" >/dev/null || die "missing required binary: $bin"
done

if [ "$SRW_EXPOSURE_MODE" = "multi-host" ]; then
  command -v mkcert >/dev/null || die "missing required binary: mkcert"
  [[ -f "$MKCERT_CAROOT/rootCA.pem" && -f "$MKCERT_CAROOT/rootCA-key.pem" ]] \
    || die "mkcert CA not found at $MKCERT_CAROOT — run 'mkcert -install' first"
fi

# --- 1. k3d cluster ---------------------------------------------------------
if k3d cluster list "$CLUSTER_NAME" >/dev/null 2>&1; then
  skip "k3d cluster '$CLUSTER_NAME' already exists"
  if [ "$SRW_EXPOSURE_MODE" = "single-origin" ]; then
    EXISTING_MAPPING=$(docker port "k3d-${CLUSTER_NAME}-server-0" 30443/tcp 2>/dev/null || true)
    if [ "$EXISTING_MAPPING" != "127.0.0.1:8443" ]; then
      die "existing cluster '$CLUSTER_NAME' does not map 127.0.0.1:8443 to server-0:30443 (found '${EXISTING_MAPPING:-no mapping}'). A k3d port mapping cannot be changed in place. Back up any existing cluster data before deliberately running 'k3d cluster delete $CLUSTER_NAME', then recreate it with SRW_EXPOSURE_MODE=single-origin."
    fi
  else
    EXISTING_HTTP_MAPPING=$(docker port "k3d-${CLUSTER_NAME}-serverlb" 80/tcp 2>/dev/null || true)
    EXISTING_HTTPS_MAPPING=$(docker port "k3d-${CLUSTER_NAME}-serverlb" 443/tcp 2>/dev/null || true)
    if ! grep -Eq '(^|:)80$' <<<"$EXISTING_HTTP_MAPPING" \
      || ! grep -Eq '(^|:)443$' <<<"$EXISTING_HTTPS_MAPPING"; then
      die "existing cluster '$CLUSTER_NAME' does not expose load-balancer ports 80 and 443 (found 80/tcp='${EXISTING_HTTP_MAPPING:-no mapping}', 443/tcp='${EXISTING_HTTPS_MAPPING:-no mapping}'). A k3d port mapping cannot be changed in place. Back up any existing cluster data before deliberately running 'k3d cluster delete $CLUSTER_NAME', then recreate it with SRW_EXPOSURE_MODE=multi-host."
    fi
  fi
else
  log "creating k3d cluster '$CLUSTER_NAME'"
  if [ "$SRW_EXPOSURE_MODE" = "single-origin" ]; then
    if command -v ss >/dev/null && ss -H -ltn '( sport = :8443 )' 2>/dev/null | grep -q .; then
      die "TCP port 127.0.0.1:8443 is already in use; stop that listener or choose a different local mapping before creating the cluster"
    fi
    k3d cluster create "$CLUSTER_NAME" \
      --servers 1 \
      --port "127.0.0.1:8443:30443@server:0" \
      --registry-create "${CLUSTER_NAME}-registry:0.0.0.0:5005"
  else
    k3d cluster create "$CLUSTER_NAME" \
      --servers 1 \
      --port "80:80@loadbalancer" \
      --port "443:443@loadbalancer" \
      --registry-create "${CLUSTER_NAME}-registry:0.0.0.0:5005"
  fi
  ok "cluster created"
fi

# Ensure kubectl is pointed at it for this script's commands
KCTL="kubectl --context=$KUBE_CONTEXT"

# --- 2. cert-manager (multi-host only) --------------------------------------
if [ "$SRW_EXPOSURE_MODE" = "multi-host" ]; then
  if $KCTL -n cert-manager get deploy cert-manager >/dev/null 2>&1; then
    skip "cert-manager already installed"
  else
    log "installing cert-manager $CERT_MANAGER_VERSION"
    helm repo add jetstack https://charts.jetstack.io --force-update >/dev/null
    helm repo update >/dev/null
    helm install cert-manager jetstack/cert-manager \
      --kube-context "$KUBE_CONTEXT" \
      --namespace cert-manager --create-namespace \
      --version "$CERT_MANAGER_VERSION" --set crds.enabled=true >/dev/null
    log "waiting for cert-manager to be ready"
    $KCTL -n cert-manager rollout status deploy/cert-manager --timeout=180s
    $KCTL -n cert-manager rollout status deploy/cert-manager-webhook --timeout=180s
    $KCTL -n cert-manager rollout status deploy/cert-manager-cainjector --timeout=180s
    ok "cert-manager ready"
  fi
else
  skip "single-origin mode uses the chart-owned self-signed certificate"
fi

# --- 2b. KEDA (queue-driven autoscaling of the stateless agent pool) --------
# Cluster-level prerequisite for `agent.stateless.autoscaling.enabled`
# (helm/templates/agent/stateless-scaledobject.yaml). Its own release in its
# own namespace, deliberately outside the srw chart and outside Tilt's `srw`
# resource — same footing as cert-manager above. Dev gets it from the HomeLab
# Fleet bundle deployments_managed/keda/.
if $KCTL -n keda get deploy keda-operator >/dev/null 2>&1; then
  skip "KEDA already installed"
else
  log "installing KEDA $KEDA_VERSION"
  helm repo add kedacore https://kedacore.github.io/charts --force-update >/dev/null
  helm repo update >/dev/null
  helm upgrade --install keda kedacore/keda \
    --kube-context "$KUBE_CONTEXT" \
    --namespace keda --create-namespace \
    --version "$KEDA_VERSION" >/dev/null
  log "waiting for KEDA to be ready"
  $KCTL -n keda rollout status deploy/keda-operator --timeout=180s
  $KCTL -n keda rollout status deploy/keda-operator-metrics-apiserver --timeout=180s
  $KCTL -n keda rollout status deploy/keda-admission-webhooks --timeout=180s
  ok "KEDA ready"
fi

# --- 3. mkcert CA Secret + ClusterIssuer (multi-host only) ------------------
if [ "$SRW_EXPOSURE_MODE" = "multi-host" ]; then
  if $KCTL -n cert-manager get secret mkcert-ca-key-pair >/dev/null 2>&1; then
    skip "secret mkcert-ca-key-pair already exists"
  else
    log "uploading mkcert CA to cert-manager namespace"
    $KCTL -n cert-manager create secret tls mkcert-ca-key-pair \
      --cert="$MKCERT_CAROOT/rootCA.pem" --key="$MKCERT_CAROOT/rootCA-key.pem"
    ok "CA secret created"
  fi

  if $KCTL get clusterissuer mkcert-issuer >/dev/null 2>&1; then
    skip "ClusterIssuer mkcert-issuer already exists"
  else
    log "creating mkcert ClusterIssuer"
    $KCTL apply -f - <<'EOF'
apiVersion: cert-manager.io/v1
kind: ClusterIssuer
metadata:
  name: mkcert-issuer
spec:
  ca:
    secretName: mkcert-ca-key-pair
EOF
    ok "ClusterIssuer created"
  fi
fi

# --- 4. SRW namespace + local runtime Secrets -------------------------------
$KCTL create namespace "$NAMESPACE" --dry-run=client -o yaml | $KCTL apply -f - >/dev/null

if $KCTL -n "$NAMESPACE" get secret srw-session-jwt >/dev/null 2>&1; then
  skip "secret srw-session-jwt already exists in namespace $NAMESPACE"
else
  log "minting session-router JWT secret"
  JWT_SECRET=$(openssl rand -base64 48 | tr -d '\n' | head -c 64)
  $KCTL -n "$NAMESPACE" create secret generic srw-session-jwt \
    --from-literal=jwt-secret="$JWT_SECRET"
  ok "session-jwt Secret created"
fi

if $KCTL -n "$NAMESPACE" get secret srw-vm-ssh-key >/dev/null 2>&1; then
  skip "secret srw-vm-ssh-key already exists in namespace $NAMESPACE"
else
  log "generating dummy VM SSH keypair (not used locally — orchestrator + workspace pods mount this Secret)"
  TMPDIR=$(mktemp -d)
  trap 'rm -rf "$TMPDIR"' EXIT
  ssh-keygen -t ed25519 -f "$TMPDIR/key" -N "" -C "srw-local@dev" -q
  # Key names must match what the chart expects: ssh-privatekey (orchestrator,
  # mounted via subPath) + ssh-publickey (workspace pod authorized_keys seed).
  $KCTL -n "$NAMESPACE" create secret generic srw-vm-ssh-key \
    --from-file=ssh-privatekey="$TMPDIR/key" \
    --from-file=ssh-publickey="$TMPDIR/key.pub"
  ok "vm-ssh-key Secret created"
fi

# --- 5. In-cluster DNS for *.localhost ingress hosts -------------------------
# Pods cannot resolve cloud.localhost / auth.localhost / git.localhost (no
# ordinary DNS exists for .localhost), but cloud, git, OIDC, and workspace
# flows must reach those ingress hostnames from inside the cluster. Map them
# to Traefik's ClusterIP through the k3s coredns-custom hook so every pod gets
# the same answer. Idempotent; the ClusterIP is stable for the life of the
# cluster (re-run this script after `k3d cluster delete && create`).
if [ "$SRW_EXPOSURE_MODE" = "multi-host" ]; then
  TRAEFIK_IP=$($KCTL -n kube-system get svc traefik -o jsonpath='{.spec.clusterIP}' 2>/dev/null || true)
  if [ -z "$TRAEFIK_IP" ]; then
    skip "traefik svc not up yet — re-run this script later to install the *.localhost DNS override"
  else
    log "installing coredns-custom override: *.localhost ingress hosts -> $TRAEFIK_IP"
    $KCTL apply -f - <<COREDNS_EOF >/dev/null
apiVersion: v1
kind: ConfigMap
metadata:
  name: coredns-custom
  namespace: kube-system
data:
  srw-localhost.server: |
    cloud.localhost auth.localhost git.localhost:53 {
        hosts {
            $TRAEFIK_IP cloud.localhost
            $TRAEFIK_IP auth.localhost
            $TRAEFIK_IP git.localhost
            fallthrough
        }
    }
COREDNS_EOF
    $KCTL -n kube-system rollout restart deploy/coredns >/dev/null
    ok "coredns-custom override applied (cloud/auth/git.localhost -> $TRAEFIK_IP)"
  fi
fi

# --- 6. Vendored Helm chart dependencies ------------------------------------
# `helm/charts/` is gitignored (*.tgz), so a fresh clone has no
# collabora-online tarball and every `helm install|upgrade` refuses to run:
# the dependency-presence check fires before `collabora.enabled` is evaluated,
# so it blocks the whole release even though Collabora is off locally. CI does
# the same repo-add + build before each helm invocation. `dependency build`
# (not `update`) installs the Chart.lock pins, so local matches CI exactly.
#
# `dependency list` is offline and instant, so re-runs cost nothing.
CHART_DEPS=$(helm dependency list "$REPO_ROOT/helm" 2>/dev/null || true)
if [[ "$CHART_DEPS" == *missing* ]]; then
  log "vendoring Helm chart dependencies into helm/charts/"
  helm repo add collabora https://collaboraonline.github.io/online --force-update >/dev/null
  helm dependency build "$REPO_ROOT/helm" >/dev/null
  ok "chart dependencies vendored"
else
  skip "Helm chart dependencies already vendored"
fi

# --- 7. Pin component images to this checkout ------------------------------
# The chart defaults every component to `:latest`, which is published by the
# MAIN pipeline. A checkout of `develop` (the default branch) therefore installs
# develop's chart against main's images, and the two disagree whenever develop
# has changed a chart<->image contract since the last release — the research
# seed hook's `--research-providers-only` broke exactly this way on 2026-09-04.
# Develop CI publishes `sha-<7>` tags for every component it builds, keyed by
# the newest commit that touched that component's build inputs. Walk this
# checkout's history and take, per component, the newest commit whose image
# exists on GHCR; write the result to a gitignored overlay that the install
# command passes after values-local.yaml. Disable with SRW_IMAGE_PIN=0.
IMAGES_FILE="$REPO_ROOT/deployment/values-local-images.yaml"
if [ "${SRW_IMAGE_PIN:-1}" = "0" ]; then
  skip "image pinning disabled (SRW_IMAGE_PIN=0); the chart's :latest tags apply"
elif ! git -C "$REPO_ROOT" rev-parse --verify HEAD >/dev/null 2>&1; then
  skip "not a git checkout; cannot pin images to a commit — the chart's :latest tags apply"
else
  log "pinning component images to this checkout's history"
  GHCR_NS="ghcr.io/superhuman-remote-worker/srw"
  PIN_DEPTH="${SRW_IMAGE_PIN_DEPTH:-300}"
  ghcr_token() {
    curl -fsS "https://ghcr.io/token?scope=repository:superhuman-remote-worker/srw-$1:pull" \
      | sed -n 's/.*"token":"\([^"]*\)".*/\1/p'
  }
  ghcr_has_tag() {  # token component tag
    local code
    code=$(curl -s -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $1" \
      -H 'Accept: application/vnd.oci.image.index.v1+json, application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.list.v2+json, application/vnd.docker.distribution.manifest.v2+json' \
      "https://ghcr.io/v2/superhuman-remote-worker/srw-$2/manifests/$3")
    [ "$code" = "200" ]
  }
  resolve_tag() {  # component -> prints "sha-xxxxxxx depth" or nothing (one anonymous pull token per component)
    local component="$1" depth=0 sha tok
    tok=$(ghcr_token "$component") || return 1
    [ -n "$tok" ] || return 1
    while read -r sha; do
      if ghcr_has_tag "$tok" "$component" "sha-${sha:0:7}"; then
        printf '%s %s\n' "sha-${sha:0:7}" "$depth"; return 0
      fi
      depth=$((depth+1))
    done < <(git -C "$REPO_ROOT" rev-list --max-count="$PIN_DEPTH" HEAD)
    return 1
  }
  # Resolve all components in parallel; each writes its answer to a temp file.
  PIN_TMP=$(mktemp -d)
  for component in orchestrator agent cockpit mcp workspace vm-controller; do
    ( resolve_tag "$component" > "$PIN_TMP/$component" 2>/dev/null || : ) &
  done
  wait
  {
    echo "# Generated by scripts/local-dev-up.sh — component image tags resolved from"
    echo "# this checkout's git history ($(git -C "$REPO_ROOT" rev-parse --short=7 HEAD),"
    echo "# $(date -u +%Y-%m-%dT%H:%M:%SZ)). Re-run the script after pulling to refresh."
    echo "# Pass it AFTER values-local.yaml so it wins: helm ... -f deployment/values-local.yaml -f deployment/values-local-images.yaml"
    echo "image:"
  } > "$IMAGES_FILE"
  PIN_FAILED=""
  for component in orchestrator agent cockpit mcp workspace; do
    resolved=$(cat "$PIN_TMP/$component")
    if [ -n "$resolved" ]; then
      tag="${resolved%% *}"; depth="${resolved##* }"
      printf '  %s:\n    tag: %s\n' "$component" "$tag" >> "$IMAGES_FILE"
      if [ "$depth" = "0" ]; then ok "$component → $tag (HEAD)"; else ok "$component → $tag (newest commit with a published image; $depth commit(s) behind HEAD)"; fi
    else
      PIN_FAILED="$PIN_FAILED $component"
      printf '  %s:\n    tag: latest\n' "$component" >> "$IMAGES_FILE"
    fi
  done
  resolved=$(cat "$PIN_TMP/vm-controller")
  if [ -n "$resolved" ]; then
    tag="${resolved%% *}"
    printf 'vmController:\n  image:\n    tag: %s\n' "$tag" >> "$IMAGES_FILE"
    ok "vm-controller → $tag"
  else
    PIN_FAILED="$PIN_FAILED vm-controller"
    printf 'vmController:\n  image:\n    tag: latest\n' >> "$IMAGES_FILE"
  fi
  rm -rf "$PIN_TMP"
  if [ -n "$PIN_FAILED" ]; then
    printf '\033[1;33m[warn]\033[0m no sha-tagged image within %s commits for:%s — left on :latest, which may be OLDER than this chart\n' "$PIN_DEPTH" "$PIN_FAILED"
  fi
  ok "wrote $IMAGES_FILE"
fi

# --- Done -------------------------------------------------------------------
if [ "$SRW_EXPOSURE_MODE" = "single-origin" ]; then
  EXTRA_VALUES='  -f deployment/values-local-single-origin.yaml \'
  COCKPIT_URL='https://localhost:8443/'
else
  EXTRA_VALUES=''
  COCKPIT_URL='https://localhost/'
fi

cat <<EOF

$(printf '\033[1;32m✓ Local cluster ready.\033[0m')

Next:
  cp deployment/values-local.yaml.example deployment/values-local.yaml
  \$EDITOR deployment/values-local.yaml      # paste at least one LLM key
  helm install srw ./helm -n $NAMESPACE --kube-context $KUBE_CONTEXT \
    -f deployment/values-local.yaml \
${EXTRA_VALUES}
    -f deployment/values-local-images.yaml   # images pinned to this checkout

Then open $COCKPIT_URL and log in as test / srw-k3d-dev-test.

Cluster lifecycle:
  k3d cluster stop  $CLUSTER_NAME
  k3d cluster start $CLUSTER_NAME
EOF
