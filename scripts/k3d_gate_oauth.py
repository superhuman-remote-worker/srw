"""The Keycloak client the local k3d gates log their accounts in through.

An ID token of the srw realm's ``admin-cli`` client carries no ``email`` and
no ``realm_access.roles``, and the orchestrator recomputes ``is_admin`` from
the token at every login (``_resolve_user_from_claims``): a gate that logs an
administrator in through ``admin-cli`` demotes that account (``test``) until
its next cockpit login. The gates log in through :data:`GATE_CLIENT`
instead: a public client of the srw realm with direct access grants and the
profile, email and roles scopes, the client of
scripts/k3d-user-root-connector-gate.py's ``create-client`` action, so its
ID token carries what a cockpit login's does.

Unlike that gate's ``<gate id>-oauth`` it is one standing fixture of the
local realm, not one per run: the first login that finds the realm without
it creates it, with the orchestrator's own Keycloak admin credentials (they
stay in the pod), and no run's cleanup removes it (its ``srw-gate``
attribute is no run's id). A gate's in-pod program includes
:data:`LOGIN_SOURCE` and calls ``gate_id_token``.

A gate loads it by path, as the gates load one another: a gate is run as a
script and imported by its tests, so this directory is not always on the
import path.
"""

from __future__ import annotations

#: The srw realm's client the gates log in through.
GATE_CLIENT = "srw-k3d-gate"
#: Its ``srw-gate`` attribute: no run's id, so no run's sweep matches it.
GATE_MARKER = "k3d-gates"

#: Python source for a gate's in-pod program, after its ``cap_memory()``
#: definition: ``gate_id_token(opener, token_url, username, password)``
#: returns the account's ID token from :data:`GATE_CLIENT`, creating the
#: client first when the realm does not know it (``invalid_client``).
LOGIN_SOURCE = (
    r"""
def _srw_gate_client_create(opener):
    import json as _json, os as _os, urllib.error as _error
    import urllib.parse as _parse, urllib.request as _request
    base = _os.environ.get("KEYCLOAK_URL", "").rstrip("/")
    realm = _os.environ.get("KEYCLOAK_REALM", "") or "srw"
    admin = _os.environ.get("KEYCLOAK_ADMIN_USER", "")
    admin_password = _os.environ.get("KEYCLOAK_ADMIN_PASSWORD", "")
    if not (base and admin and admin_password):
        raise SystemExit("the orchestrator has no Keycloak admin credentials")
    form = _parse.urlencode({
        "grant_type": "password", "client_id": "admin-cli",
        "username": admin, "password": admin_password,
    }).encode()
    with opener.open(
        base + "/realms/master/protocol/openid-connect/token", data=form, timeout=30
    ) as response:
        token = _json.load(response)["access_token"]
    message = _request.Request(
        base + "/admin/realms/" + _parse.quote(realm, safe="") + "/clients",
        data=_json.dumps({
            "clientId": _SRW_GATE_CLIENT, "name": _SRW_GATE_CLIENT, "enabled": True,
            "protocol": "openid-connect", "publicClient": True,
            "standardFlowEnabled": False, "directAccessGrantsEnabled": True,
            "serviceAccountsEnabled": False, "fullScopeAllowed": True,
            "defaultClientScopes": ["profile", "email", "roles"],
            "optionalClientScopes": [],
            "attributes": {"srw-gate": _SRW_GATE_MARKER},
        }).encode(),
        method="POST",
        headers={"Authorization": "Bearer " + token, "Content-Type": "application/json"},
    )
    try:
        opener.open(message, timeout=30).close()
    except _error.HTTPError as error:
        if error.code != 409:  # 409: another gate created it meanwhile
            raise SystemExit("creating the gate's Keycloak client answered HTTP %d"
                             % error.code)

def gate_id_token(opener, token_url, username, password):
    import json as _json, urllib.error as _error, urllib.parse as _parse
    form = _parse.urlencode({
        "grant_type": "password", "client_id": _SRW_GATE_CLIENT, "scope": "openid",
        "username": username, "password": password,
    }).encode()
    for attempt in (0, 1):
        try:
            with opener.open(token_url, data=form, timeout=30) as response:
                return _json.load(response)["id_token"]
        except _error.HTTPError as error:
            unknown = error.code == 401 and b'"invalid_client"' in error.read()
            if attempt or not unknown:
                raise
        _srw_gate_client_create(opener)
"""
    + f"\n_SRW_GATE_CLIENT = {GATE_CLIENT!r}\n_SRW_GATE_MARKER = {GATE_MARKER!r}\n"
)


__all__ = ["GATE_CLIENT", "GATE_MARKER", "LOGIN_SOURCE"]
