#!/usr/bin/env bash
# =============================================================================
# Workspace container entrypoint
# Seeds dotfiles, installs SSH key, starts code-server + SSHD.
# =============================================================================

set -e

# Cooperative process-retirement identity.  Code-server terminals and other
# workspace residents inherit this exact owner+Pod-UID tag; public stateless
# End scans it after stopping the known parents so nohup/setsid descendants
# cannot keep mutating a snapshot. Static/pinned Docker workspaces predate this
# authority and provide none of the three fields; retain that legacy untagged
# mode, but refuse any partial or malformed authority set.
if [ -z "${SRW_WORKSPACE_OWNER_KIND:-}" ] \
    && [ -z "${SRW_WORKSPACE_OWNER_ID:-}" ] \
    && [ -z "${SRW_WORKSPACE_RUNTIME_UID:-}" ]; then
    unset SRW_WORKSPACE_PROCESS_TAG
else
    case "${SRW_WORKSPACE_OWNER_KIND:-}" in job|session) ;;
        *) echo "workspace owner kind is unavailable" >&2; exit 78 ;;
    esac
    canonical_uuid_re='^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    if ! printf '%s\n' "${SRW_WORKSPACE_OWNER_ID:-}" | grep -Eq "$canonical_uuid_re"; then
        echo "workspace owner identity is unavailable" >&2
        exit 78
    fi
    if ! printf '%s\n' "${SRW_WORKSPACE_RUNTIME_UID:-}" | grep -Eq "$canonical_uuid_re"; then
        echo "workspace runtime identity is unavailable" >&2
        exit 78
    fi
    export SRW_WORKSPACE_PROCESS_TAG="v1:${SRW_WORKSPACE_OWNER_KIND}:${SRW_WORKSPACE_OWNER_ID}:${SRW_WORKSPACE_RUNTIME_UID}"
fi

# ---------------------------------------------------------------------------
# 1. Seed dotfiles from skeleton (idempotent)
#    Volume mounts shadow everything baked into /home/agent-host by the
#    Dockerfile. The skeleton restores dotfiles (.bashrc, .gitconfig, etc.)
#    on first boot without overwriting files that already exist (-n).
# ---------------------------------------------------------------------------
chown agent-host:agent-host /home/agent-host
if [ ! -f /home/agent-host/.workspace-initialized ]; then
    su -s /bin/sh agent-host -c \
        'cp -rn /etc/skel.agent-host/. /home/agent-host/ && touch /home/agent-host/.workspace-initialized'
fi

# ---------------------------------------------------------------------------
# 1b. Ensure workspace directory exists (clean target for git clone)
# ---------------------------------------------------------------------------
mkdir -p /home/agent-host/workspace
chown agent-host:agent-host /home/agent-host/workspace

# ---------------------------------------------------------------------------
# 2. Install SSH public key (outside home dir)
#    Key is mounted from a K8s secret at /tmp/ssh-pubkey/. Written to
#    /etc/ssh/authorized_keys/agent-host which sshd finds via the
#    AuthorizedKeysFile /etc/ssh/authorized_keys/%u directive.
#    Root ownership satisfies StrictModes.
# ---------------------------------------------------------------------------
if [ -f /tmp/ssh-pubkey/ssh-publickey ]; then
    cp /tmp/ssh-pubkey/ssh-publickey /etc/ssh/authorized_keys/agent-host
    chmod 644 /etc/ssh/authorized_keys/agent-host
fi

# Gateway user CA. Absent on deployments without the ssh-gateway, in which case
# sshd simply has no CA to trust and certificate auth is unavailable -- the
# agent's own key path is unaffected.
if [ -f /tmp/ssh-pubkey/user-ca.pub ]; then
    cp /tmp/ssh-pubkey/user-ca.pub /etc/ssh/srw_user_ca.pub
    chmod 644 /etc/ssh/srw_user_ca.pub

    # Scope certificate auth to THIS workspace. Every workspace image bakes
    # in the same Unix user and trusts the same CA, so without this file a
    # certificate minted for any workspace would authenticate to all of
    # them for its whole validity window (sshd_config's
    # AuthorizedPrincipalsFile /etc/ssh/principals/%u reads this, keyed by
    # login user -- always "agent-host" here). SRW_WORKSPACE_OWNER_ID is the
    # same thread id the gateway resolves for this workspace's ssh handle,
    # and connect_upstream() mints each certificate's principal from that
    # same value, so the two agree without either side hard-coding the
    # other. No identity, no file: sshd then finds AuthorizedPrincipalsFile
    # pointing at a missing file and refuses every certificate, which is
    # the safe default for a workspace with nothing to scope to.
    if [ -n "${SRW_WORKSPACE_OWNER_ID:-}" ]; then
        install -d -o root -g root -m 0755 /etc/ssh/principals
        printf '%s\n' "$SRW_WORKSPACE_OWNER_ID" > /etc/ssh/principals/agent-host
        chmod 644 /etc/ssh/principals/agent-host
    fi
fi

# ---------------------------------------------------------------------------
# 2a. Install a per-workspace SSH host identity.
#
# Host identity must not live below the agent-owned home directory. Kubernetes
# mounts a pod-private emptyDir here so keys survive a container restart; a new
# pod/container receives a new key and the provisioner rotates the paired
# Canvas generation. Never fall back to image-baked host keys.
# ---------------------------------------------------------------------------
HOST_KEY_DIR=/var/lib/srw-system/ssh
install -d -o root -g root -m 0700 /var/lib/srw-system
install -d -o root -g root -m 0700 "$HOST_KEY_DIR"
# Native manifest workspaces can pin the SSH host identity before the Pod
# starts. This Secret belongs to one attachment; no shared host/client key is
# accepted from the workspace PVC. Older provisioners omit the mount and keep
# the existing generation-on-start behavior below.
if [ -d /tmp/ssh-hostkey ]; then
    if [ ! -s /tmp/ssh-hostkey/ssh_host_ed25519_key ] \
        || [ ! -s /tmp/ssh-hostkey/ssh_host_ed25519_key.pub ]; then
        echo "workspace SSH host identity is incomplete" >&2
        exit 78
    fi
    install -o root -g root -m 0600 /tmp/ssh-hostkey/ssh_host_ed25519_key \
        "$HOST_KEY_DIR/ssh_host_ed25519_key"
    install -o root -g root -m 0644 /tmp/ssh-hostkey/ssh_host_ed25519_key.pub \
        "$HOST_KEY_DIR/ssh_host_ed25519_key.pub"
fi
if [ ! -s "$HOST_KEY_DIR/ssh_host_ed25519_key" ]; then
    ssh-keygen -q -t ed25519 -N '' -f "$HOST_KEY_DIR/ssh_host_ed25519_key"
fi
if [ ! -s "$HOST_KEY_DIR/ssh_host_rsa_key" ]; then
    ssh-keygen -q -t rsa -b 3072 -N '' -f "$HOST_KEY_DIR/ssh_host_rsa_key"
fi
# Reassert the trust boundary on every start, including a restart after the
# agent deletes its home marker and the skeleton is seeded again. Nothing in
# the home-seeding path recursively changes ownership, and persisted host keys
# remain root-owned with exact OpenSSH modes.
chown root:root "$HOST_KEY_DIR"/ssh_host_*_key "$HOST_KEY_DIR"/ssh_host_*_key.pub
chmod 0600 "$HOST_KEY_DIR"/ssh_host_*_key
chmod 0644 "$HOST_KEY_DIR"/ssh_host_*_key.pub
for key in "$HOST_KEY_DIR"/ssh_host_*_key "$HOST_KEY_DIR"/ssh_host_*_key.pub; do
    ln -sfn "$key" "/etc/ssh/$(basename "$key")"
done

# ---------------------------------------------------------------------------
# 2a'. Trust the operator's extra CA (chart value trust.extraCaCertificates).
#
# The provisioner mounts it and sets SRW_EXTRA_CA_FILE. Adding it to the
# system store covers curl, git, wget and OpenSSL; the profile script points
# Python's requests/httpx (which bundle their own roots) and Node at the same
# store for login shells. Best-effort: a failure leaves the public roots in
# place and the private endpoint failing its handshake, never unverified.
# ---------------------------------------------------------------------------
if [ -n "${SRW_EXTRA_CA_FILE:-}" ] && [ -s "$SRW_EXTRA_CA_FILE" ]; then
    if command -v update-ca-certificates >/dev/null 2>&1 \
        && install -D -o root -g root -m 0644 "$SRW_EXTRA_CA_FILE" \
            /usr/local/share/ca-certificates/srw-extra-ca.crt \
        && update-ca-certificates >/dev/null 2>&1; then
        cat > /etc/profile.d/srw-extra-ca.sh <<EOF
export SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt
export REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt
export NODE_EXTRA_CA_CERTS=/usr/local/share/ca-certificates/srw-extra-ca.crt
EOF
        chmod 644 /etc/profile.d/srw-extra-ca.sh
    else
        echo "extra CA from $SRW_EXTRA_CA_FILE not installed (non-fatal)" >&2
    fi
fi

# ---------------------------------------------------------------------------
# 2b. Seed per-user code-server config (theme / keybindings / snippets)
#     The orchestrator mounts a ConfigMap carrying a self-contained seed.sh
#     that writes the user's stored config into /var/lib/code-server/User and
#     sets each file's mtime (so the settings sweeper won't re-pull an
#     untouched seed). Runs as root before the su to agent-host below, so its
#     chown succeeds. Best-effort — a failure must not stop the workspace.
# ---------------------------------------------------------------------------
if [ -f /mnt/code-server-config/seed.sh ]; then
    sh /mnt/code-server-config/seed.sh || echo "code-server seed failed (non-fatal)" >&2
fi

# ---------------------------------------------------------------------------
# 2c. Start SSHD (background) BEFORE the state wait.
#     The orchestrator delivers license/globalStorage state over SSH, and the
#     pod's readiness probe is on :30022 — the orchestrator only pushes state
#     once the pod reports Ready. So sshd MUST be listening before we wait on
#     the sentinel; otherwise the pod never goes Ready, the push never fires,
#     and the wait below always times out. sshd is the container's lifecycle
#     anchor (we `wait` on it at the end instead of `exec`-ing it).
# ---------------------------------------------------------------------------
/usr/sbin/sshd -D -e &
SSHD_PID=$!

# ---------------------------------------------------------------------------
# 2d. Wait (bounded) for the orchestrator to deliver license/globalStorage
#     state. Only when the seed ConfigMap signalled state is expected. The
#     orchestrator streams the bundle in over SSH (now reachable, see 2c) then
#     touches the sentinel. Bounded so a slow/absent orchestrator can't wedge
#     startup — on timeout we start code-server anyway (state lands late).
# ---------------------------------------------------------------------------
if [ -f /mnt/code-server-config/expect-state ]; then
    i=0
    while [ ! -f /var/lib/code-server/.ide-seed-state-done ] && [ "$i" -lt 30 ]; do
        sleep 1; i=$((i+1))
    done
    [ -f /var/lib/code-server/.ide-seed-state-done ] || \
        echo "ide state seed sentinel timed out after ${i}s (non-fatal)" >&2
fi

# ---------------------------------------------------------------------------
# 3. Start code-server as agent-host (background). Starts AFTER the state wait
#    so globalStorage (paid-theme license etc.) is already in place on first
#    paint. --user-data-dir and --extensions-dir outside home keep the IDE
#    file explorer clean. Opens /home/agent-host/workspace as the root.
# ---------------------------------------------------------------------------
if [ -n "${SRW_WORKSPACE_PROCESS_TAG:-}" ]; then
    CODE_SERVER_PROCESS_PREFIX="exec env SRW_WORKSPACE_PROCESS_TAG=$SRW_WORKSPACE_PROCESS_TAG"
else
    CODE_SERVER_PROCESS_PREFIX="exec"
fi

# 3a. Recipient binding. The orchestrator derives one credential per workspace
#     runtime (src/orchestrator/services/ide_credentials.py) and presents it on
#     every proxied request, so a proxy that dialled a reused Pod IP meets a
#     credential this code-server does not accept and is refused HERE — the
#     one check the control plane cannot get wrong. Without the credential we
#     do NOT fall back to `auth: none`: an unauthenticated IDE on the Pod
#     network is the hole this closes, so code-server simply does not start.
#     The workspace itself is unaffected — SSH is the primary transport and is
#     already listening.
#
#     Written to a file rather than passed in argv or through `su`: the value
#     would otherwise sit in `ps` output, and env does not reliably survive
#     `su` (which is why the process tag above is re-exported explicitly).
#     /var/lib/code-server is container-local, not the workspace PVC, so the
#     credential dies with the Pod and never lands in a snapshot.
CODE_SERVER_CONFIG=/var/lib/code-server/.srw-auth.yaml
if [ -z "${HASHED_PASSWORD:-}" ]; then
    echo "code-server NOT started: no workspace credential was injected" >&2
    echo "  (set IDE_CREDENTIAL_KEY on the orchestrator; see ide_credentials.py)" >&2
else
    printf 'auth: password\nhashed-password: "%s"\n' "$HASHED_PASSWORD" \
        > "$CODE_SERVER_CONFIG"
    chown agent-host:agent-host "$CODE_SERVER_CONFIG"
    chmod 0600 "$CODE_SERVER_CONFIG"
    # Drop it from this process's environment so it is not inherited by the
    # agent's own shells further down the tree.
    unset HASHED_PASSWORD
    su -s /bin/sh agent-host -c "$CODE_SERVER_PROCESS_PREFIX code-server \
        --config $CODE_SERVER_CONFIG \
        --bind-addr 0.0.0.0:38080 \
        --user-data-dir /var/lib/code-server \
        --extensions-dir /var/lib/code-server/extensions \
        /home/agent-host/workspace" &
fi

# ---------------------------------------------------------------------------
# 4. Keep the container alive, anchored to SSHD (PID exits → container exits,
#    same lifecycle as the previous `exec sshd`).
# ---------------------------------------------------------------------------
wait "$SSHD_PID"
