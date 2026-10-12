#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# Self-healing Helm apply for the Tilt inner loop.
#
# Replaces the `helm_resource` extension's helm-apply-helper.py. Same contract
# (Tilt passes TILT_IMAGE_<i> plus the RELEASE_NAME/CHART/NAMESPACE env), same
# resulting `helm upgrade --install` argv — with one addition: a preflight that
# clears a stale `pending-*` release before invoking Helm.
#
# Why the preflight exists
# ------------------------
# Tilt kills the helm subprocess whenever it cancels an in-flight deploy — a
# superseding build, a Ctrl-C, or the k8s_upsert_timeout_secs deadline. Helm
# writes its release secret as `pending-upgrade` *before* applying and flips it
# to `deployed` only at the end, so a killed helm leaves that secret pending
# forever. Every later `helm upgrade` then refuses with:
#
#     Error: UPGRADE FAILED: another operation (install/upgrade/rollback) is in progress
#
# and the inner loop is wedged until someone clears it by hand. Restarting Tilt
# does not help — the lock lives in the cluster, not the process. Diagnosed
# 2026-07-29; see knowledge-base/knowledge/features/tilt_inner_loop_dev.md "Risks and known
# gotchas".
#
# The recovery is to drop the pending revision's secret. The previous revision
# stays `deployed` and becomes the head again, and because we always pass
# `--take-ownership`, the next upgrade re-adopts any object the killed run had
# already applied. Nothing is uninstalled and no data is touched — unlike
# `tilt trigger srw`, which runs the delete helper (`helm uninstall`) first.
#
# The preflight only fires when the pending revision has been untouched for
# SRW_HELM_STALE_AFTER seconds, so it cannot stomp a genuinely running helm.
# -----------------------------------------------------------------------------
set -euo pipefail

RELEASE="${RELEASE_NAME:?RELEASE_NAME not set (Tilt supplies this)}"
CHART="${CHART:?CHART not set (Tilt supplies this)}"
NS="${NAMESPACE:-}"
STALE_AFTER="${SRW_HELM_STALE_AFTER:-60}"
# helm/kubectl below must each name the target cluster explicitly via
# EXPECT_CONTEXT. Ambient kubeconfig is not sufficient: a --context/
# --kube-context flag on a parent command does not propagate to these child
# invocations, and the preflight *deletes* a Secret, so every operation
# carries its own context flag. Never point these at `main`.
EXPECT_CONTEXT="${SRW_HELM_EXPECT_CONTEXT:-k3d-srw}"

# --- cluster-target guard: fail before any Helm/kubectl invocation ---------
# EXPECT_CONTEXT used to gate only the preflight delete; it now selects every
# operation, so a disallowed value (e.g. SRW_HELM_EXPECT_CONTEXT=main) would
# redirect upgrades at another cluster. Reject anything outside the allowlist
# here, before the first cluster command runs.
case "$EXPECT_CONTEXT" in
k3d-srw) ;;
*)
    echo "srw-preflight: refusing disallowed SRW_HELM_EXPECT_CONTEXT value (only 'k3d-srw' is permitted)." >&2
    exit 1
    ;;
esac

# Inherited Helm connection overrides: reject, do not sanitize. Installed
# `helm --help` documents HELM_KUBEAPISERVER/HELM_KUBECAFILE/HELM_KUBEASGROUPS/
# HELM_KUBEASUSER/HELM_KUBECONTEXT/HELM_KUBETOKEN/
# HELM_KUBEINSECURE_SKIP_TLS_VERIFY/HELM_KUBETLS_SERVER_NAME (endpoint, auth,
# TLS) plus HELM_NAMESPACE (namespace): any of them set would fight the
# explicit --kube-context/--namespace flags below or, when NAMESPACE is
# empty, silently redirect release state. KUBECONFIG is the deliberate
# exception — the script needs it for credential chaining and always selects
# within it explicitly, failing loudly if k3d-srw is absent. Only the
# variable NAME is reported here, never its value.
for _srw_helm_override in HELM_KUBECONTEXT HELM_KUBEAPISERVER HELM_KUBECAFILE HELM_KUBEASGROUPS HELM_KUBEASUSER HELM_KUBETOKEN HELM_KUBEINSECURE_SKIP_TLS_VERIFY HELM_KUBETLS_SERVER_NAME HELM_NAMESPACE; do
    if [[ -n "${!_srw_helm_override:-}" ]]; then
        echo "srw-preflight: refusing environment override '$_srw_helm_override' (connection targeting is fixed to k3d-srw via explicit flags)." >&2
        exit 1
    fi
done
unset _srw_helm_override

# Forwarded Tilt/CLI arguments (Tilt passes --take-ownership, --wait,
# --timeout, --values, --set/--set-string) must not smuggle cluster
# selectors: pflag lets a later duplicate flag win, so an inherited
# --kube-context/--context/--namespace would silently override the explicit
# targeting below. Cluster selectors travel via SRW_HELM_EXPECT_CONTEXT and
# NAMESPACE env, never argv; reject them outright (even redundant ones, since
# this script appends its own).
for _srw_forwarded_arg in "$@"; do
    # Strip any =value before matching or reporting: a rejected token like
    # --kube-token=<secret> must never have its value printed, stderr
    # included. Only the bare option name is ever reported below.
    _srw_forwarded_opt="${_srw_forwarded_arg%%=*}"
    case "$_srw_forwarded_opt" in
    --kube-* | --kubeconfig | --context | --cluster* | --server | -s | \
    --namespace | -n | --as* | --token* | --username* | --password* | \
    --client-certificate* | --client-key* | --certificate-authority* | \
    --insecure-skip-tls-verify* | --tls-server-name*)
        echo "srw-preflight: refusing cluster-selecting argument '$_srw_forwarded_opt'." >&2
        echo "srw-preflight: cluster targeting is fixed to '$EXPECT_CONTEXT' via env, not argv." >&2
        exit 1
        ;;
    esac
done
unset _srw_forwarded_arg _srw_forwarded_opt

ns_args=()
if [[ -n "$NS" ]]; then
    ns_args=(--namespace "$NS")
fi
# Per-operation cluster targeting (see above): helm takes --kube-context,
# kubectl takes --context.
helm_ctx_args=(--kube-context "$EXPECT_CONTEXT")
kctx_args=(--context "$EXPECT_CONTEXT")

# --- preflight: clear a stale pending-* revision -----------------------------
unstick_pending_release() {
    local status age pending_revs
    status="$(helm "${helm_ctx_args[@]}" status "$RELEASE" "${ns_args[@]}" -o json 2>/dev/null |
        python3 -c 'import json,sys; print(json.load(sys.stdin)["info"]["status"])' 2>/dev/null || true)"

    case "$status" in
    pending-install | pending-upgrade | pending-rollback) ;;
    *) return 0 ;;
    esac

    # How long has it sat there? A live helm run is not ours to interrupt.
    age="$(helm "${helm_ctx_args[@]}" status "$RELEASE" "${ns_args[@]}" -o json 2>/dev/null | python3 -c '
import datetime, json, sys
ts = json.load(sys.stdin)["info"]["last_deployed"]
# Helm emits RFC3339 with nanosecond precision; trim to microseconds for fromisoformat.
import re
ts = re.sub(r"\.(\d{6})\d+", r".\1", ts)
delta = datetime.datetime.now(datetime.timezone.utc) - datetime.datetime.fromisoformat(ts)
print(int(delta.total_seconds()))
' 2>/dev/null || echo 0)"

    if ((age < STALE_AFTER)); then
        echo "srw-preflight: release '$RELEASE' is $status but only ${age}s old — assuming a live helm run, not touching it." >&2
        return 0
    fi

    pending_revs="$(helm "${helm_ctx_args[@]}" history "$RELEASE" "${ns_args[@]}" -o json 2>/dev/null | python3 -c '
import json, sys
hist = json.load(sys.stdin)
print(" ".join(str(h["revision"]) for h in hist if h["status"].startswith("pending")))
' 2>/dev/null || true)"

    if [[ -z "$pending_revs" ]]; then
        return 0
    fi

    # Validate the EXPLICITLY selected context, not the ambient one:
    # `kubectl config current-context` reports ambient current-context even
    # when --context names another entry, so consulting it here can refuse
    # recovery of the intended local release (or approve the wrong one).
    # `config view --minify` honors --context, so the round-tripped name
    # proves the selected entry resolves. This reads local kubeconfig files
    # only: no cluster contact, no change to the user's global kubeconfig.
    local selected
    selected="$(kubectl "${kctx_args[@]}" config view --minify -o json 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin).get("current-context", ""))' 2>/dev/null || true)"
    if [[ "$selected" != "$EXPECT_CONTEXT" ]]; then
        echo "srw-preflight: refusing to clear the lock — explicitly selected context resolves to '$selected', expected '$EXPECT_CONTEXT'." >&2
        echo "srw-preflight: ambient current-context is not consulted. Letting helm fail loudly instead." >&2
        return 0
    fi

    echo "srw-preflight: release '$RELEASE' stuck in $status for ${age}s (revisions: $pending_revs)." >&2
    echo "srw-preflight: dropping the pending revision secret(s); --take-ownership re-adopts any applied object." >&2

    for rev in $pending_revs; do
        kubectl "${kctx_args[@]}" delete secret "sh.helm.release.v1.${RELEASE}.v${rev}" \
            "${ns_args[@]}" --ignore-not-found >&2
    done

    echo "srw-preflight: cleared. Head is now $(helm "${helm_ctx_args[@]}" list "${ns_args[@]}" --all -o json 2>/dev/null | python3 -c '
import json, sys
rels = json.load(sys.stdin)
print(next((f'"'"'rev {r["revision"]} ({r["status"]})'"'"' for r in rels), "none"))
' 2>/dev/null || echo "unknown")." >&2
}

unstick_pending_release

# --- build the image --set flags --------------------------------------------
# Tilt sets TILT_IMAGE_<i> to the fully-tagged ref it just built/pushed, in the
# order of the Tiltfile's image_deps. TILT_IMAGE_KEY_REPO_<i>/_TAG_<i> carry the
# chart keys those halves map to. Splitting on the LAST colon keeps a registry
# port in the repository half: srw-registry:5000/srw-agent:tilt-abc splits into
# `srw-registry:5000/srw-agent` + `tilt-abc`.
flags=("$@")

image_count="${TILT_IMAGE_COUNT:-0}"
for ((i = 0; i < image_count; i++)); do
    img_var="TILT_IMAGE_${i}"
    repo_key_var="TILT_IMAGE_KEY_REPO_${i}"
    tag_key_var="TILT_IMAGE_KEY_TAG_${i}"
    digest_key_var="TILT_IMAGE_KEY_DIGEST_${i}"

    img="${!img_var:-}"
    repo_key="${!repo_key_var:-}"
    tag_key="${!tag_key_var:-}"
    digest_key="${!digest_key_var:-}"

    if [[ -z "$img" || -z "$repo_key" || -z "$tag_key" ]]; then
        echo "srw-preflight: image slot $i is incompletely wired (img='$img' repo_key='$repo_key' tag_key='$tag_key')" >&2
        exit 1
    fi

    flags+=(--set "${repo_key}=${img%:*}" --set "${tag_key}=${img##*:}")
    # An overlay's old digest wins over a fresh tag in these chart helpers.
    # Replace it with the exact image Tilt pushed. The controller cannot assume
    # that k3d's node-side registry hostname also resolves inside a Pod.
    if [[ -n "$digest_key" ]]; then
        image_map_var="TILT_IMAGE_MAP_${i}"
        image_map="${!image_map_var:?Tilt image map is required for a digest pin}"
        digest="$(python3 "$(dirname -- "${BASH_SOURCE[0]}")/tilt-image-digest.py" "$image_map" "$img")"
        flags+=(--set-string "${digest_key}=${digest}")
    fi
done

# --- apply -------------------------------------------------------------------
install_cmd=(helm "${helm_ctx_args[@]}" upgrade --install "${flags[@]}" "${ns_args[@]}" "$RELEASE" "$CHART")
echo "Running cmd: ${install_cmd[*]}" >&2
"${install_cmd[@]}" >&2

# Hand Tilt the object set the release owns so it can track pod status. A
# --namespace flag would also reject resources explicitly placed in the native
# hosting namespace. Supply the release namespace as a temporary context default
# instead: omitted namespaces resolve correctly and explicit namespaces survive.
# The overlay contains context names only; credentials stay in the existing
# kubeconfig files, which are never changed.
read_release_resources() (
    if [[ -z "$NS" ]]; then
        helm "${helm_ctx_args[@]}" get manifest "$RELEASE" | kubectl "${kctx_args[@]}" get -oyaml -f -
        exit
    fi
    srw_read_context_file="$(mktemp "${TMPDIR:-/tmp}/srw-tilt-context.XXXXXX")"
    trap 'rm -f -- "$srw_read_context_file"' EXIT
    # Select EXPECT_CONTEXT explicitly: ambient current-context is not proof
    # of the right cluster (verified: config view --minify honors --context).
    kubectl --context "$EXPECT_CONTEXT" config view --minify -o json | python3 -c '
import json, sys
config = json.load(sys.stdin)
name = config["current-context"]
current = next(item["context"] for item in config["contexts"] if item["name"] == name)
context = {key: current[key] for key in ("cluster", "user") if key in current}
context["namespace"] = sys.argv[1]
json.dump({"apiVersion": "v1", "kind": "Config", "current-context": name,
           "contexts": [{"name": name, "context": context}]}, sys.stdout)
    ' "$NS" > "$srw_read_context_file"
    helm "${helm_ctx_args[@]}" get manifest "$RELEASE" "${ns_args[@]}" |
        KUBECONFIG="$srw_read_context_file:${KUBECONFIG:-$HOME/.kube/config}" \
        kubectl --context "$EXPECT_CONTEXT" get -oyaml -f -
)
echo "Running cmd: helm get manifest $RELEASE | kubectl get -f - -oyaml" >&2
read_release_resources
