#!/bin/sh
# =============================================================================
# OpenCloud pre-start hook
# =============================================================================
# Runs before `opencloud server` on every container start. Fully idempotent.
#
# Unlike Nextcloud, OpenCloud does not have an `occ`-style admin CLI for
# day-2 operations: user provisioning happens via OIDC auto-provisioning,
# groups and Spaces are created via the LibreGraph API, and service-account
# auth is handled by Keycloak's client_credentials flow. So this hook is
# deliberately minimal — it only verifies the environment is sane before
# handing control back to the server entrypoint.
#
# What this checks:
#   1. `PROXY_USER_OIDC_CLAIM=sub` is set. This MUST be configured on day 0;
#      switching the claim after users exist orphans all Spaces / groups /
#      shares (see owncloud/ocis#6664 and knowledge-base/knowledge/operations/opencloud-bootstrap.md).
#   2. An OIDC issuer is configured.
#   3. Prints a reminder that the operator needs to seed the
#      `opencloud-orchestrator` Keycloak client out-of-band.
#
# Non-fatal — failures must not prevent the server from starting. The
# orchestrator's ensure_initialized() will emit its own diagnostic if
# auth fails at runtime.
# =============================================================================
set +e

log() {
    printf "[oc-setup] %s\n" "$*"
}

log "Running OpenCloud pre-start hook"

# ----- Sub-claim guard -------------------------------------------------------
if [ "${PROXY_USER_OIDC_CLAIM:-}" != "sub" ]; then
    log "WARNING: PROXY_USER_OIDC_CLAIM is '${PROXY_USER_OIDC_CLAIM:-<unset>}'," \
        "expected 'sub'."
fi

# ----- OIDC issuer guard -----------------------------------------------------
if [ -z "${OC_OIDC_ISSUER:-}" ]; then
    log "WARNING: OC_OIDC_ISSUER is not set; OpenCloud will fail login flows."
fi

# ----- Keycloak client reminder ----------------------------------------------
log "Reminder: the orchestrator expects a Keycloak client named"
log "  '${OPENCLOUD_KEYCLOAK_CLIENT_ID:-opencloud-orchestrator}'"
log "with serviceAccountsEnabled=true and the 'opencloud-admin' role assigned."
log "The chart's Keycloak bootstrap hook seeds this; otherwise create it in the"
log "Keycloak admin UI."

log "Pre-start hook complete"
exit 0
