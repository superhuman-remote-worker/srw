"""The Keycloak client the local k3d gates log their accounts in through.

The orchestrator takes a Keycloak token as a Bearer credential only when it
is an *access* token bound to the API (``src/orchestrator/security/oidc.py``):
issued to an allowed client (``azp``) or for an allowed audience (``aud``).
It refuses every ID token. admin-cli's access token is refused too, and is
no use anyway: it is a lightweight token with no ``sub``, e-mail or realm
roles, and since the orchestrator recomputes ``is_admin`` from the token at
every login (``_resolve_user_from_claims``), a login without the roles would
demote an administrator (``test``).

So every gate client carries :data:`GATE_CLIENT_MAPPERS`: ``sub`` in the
access token (the imported local realm has no ``basic`` scope) and the
``orchestrator-api`` audience (:data:`GATE_AUDIENCE`), which the chart's
default ``auth.oidc.allowedAudiences`` accepts. The binding comes from the
gate client's own tokens, the way the chart documents it for any first-party
client. ``deployment/values-local.yaml`` needs no allowance for it, and no
chart default is widened.

Most gates log in through :data:`GATE_CLIENT`, a public client of the srw
realm with direct access grants and the profile, email and roles scopes, so
its tokens carry what a cockpit login's do. It is one standing fixture of the
local realm, not one per run. The first login that finds the realm without
it creates it, with the orchestrator's own Keycloak admin credentials (they
stay in the pod). The first login whose token still lacks the claims (a
client made before the mappers existed) adds the missing mappers. No run's
cleanup removes it: its ``srw-gate`` attribute is no run's id. A gate that
makes an OAuth client of its own per run (``<gate id>-oauth``) creates it
with the same mappers: its Keycloak program includes :data:`MAPPERS_SOURCE`.

A gate's in-pod program includes :data:`LOGIN_SOURCE` and calls
``gate_access_token``. A host-side script calls :func:`access_token`, which
runs that program in the orchestrator pod and returns the token; a shell
script runs this file (see :func:`main`).

A gate loads it by path, as the gates load one another: a gate is run as a
script and imported by its tests, so this directory is not always on the
import path.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys

#: The srw realm's client the gates log in through.
GATE_CLIENT = "srw-k3d-gate"
#: Its ``srw-gate`` attribute: no run's id, so no run's sweep matches it.
GATE_MARKER = "k3d-gates"
#: The audience a gate's access token carries: the bundled realm's client for
#: the orchestrator API, the chart's default ``auth.oidc.allowedAudiences``.
GATE_AUDIENCE = "orchestrator-api"

#: The protocol mappers every gate client is created with.
GATE_CLIENT_MAPPERS: tuple[dict, ...] = (
    {
        "name": "subject",
        "protocol": "openid-connect",
        "protocolMapper": "oidc-sub-mapper",
        "config": {"access.token.claim": "true", "introspection.token.claim": "true"},
    },
    {
        "name": "orchestrator-api audience",
        "protocol": "openid-connect",
        "protocolMapper": "oidc-audience-mapper",
        "config": {
            "included.custom.audience": GATE_AUDIENCE,
            "access.token.claim": "true",
            "id.token.claim": "false",
            "introspection.token.claim": "true",
        },
    },
)

#: Python source defining ``_SRW_GATE_MAPPERS`` and ``_SRW_GATE_AUDIENCE`` for
#: an in-pod program that creates a gate client: its create payload sets
#: ``"protocolMappers": _SRW_GATE_MAPPERS``.
MAPPERS_SOURCE = (
    f"\n_SRW_GATE_MAPPERS = {list(GATE_CLIENT_MAPPERS)!r}\n"
    f"_SRW_GATE_AUDIENCE = {GATE_AUDIENCE!r}\n"
)

#: Python source for a gate's in-pod program, after its ``cap_memory()``
#: definition: ``gate_access_token(opener, token_url, username, password)``
#: returns the account's access token from :data:`GATE_CLIENT`, first creating
#: the client when the realm does not know it (``invalid_client``), or adding
#: its missing mappers when its token lacks ``sub`` or the audience.
LOGIN_SOURCE = (
    r"""
def _srw_gate_admin(opener):
    import json as _json, os as _os, urllib.parse as _parse
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
    return base + "/admin/realms/" + _parse.quote(realm, safe=""), token

def _srw_gate_call(opener, token, method, url, body=None):
    import json as _json, urllib.error as _error, urllib.request as _request
    message = _request.Request(
        url,
        data=None if body is None else _json.dumps(body).encode(),
        method=method,
        headers={"Authorization": "Bearer " + token, "Content-Type": "application/json"},
    )
    try:
        with opener.open(message, timeout=30) as response:
            text = response.read().decode("utf-8", "replace")
            return response.status, (_json.loads(text) if text.strip() else None)
    except _error.HTTPError as error:
        return error.code, None

def _srw_gate_client_ensure(opener):
    import urllib.parse as _parse
    admin_api, token = _srw_gate_admin(opener)
    status, found = _srw_gate_call(
        opener, token, "GET",
        admin_api + "/clients?" + _parse.urlencode({"clientId": _SRW_GATE_CLIENT}),
    )
    if status != 200:
        raise SystemExit("looking the gate's Keycloak client up answered HTTP %d" % status)
    found = [c for c in found or [] if c.get("clientId") == _SRW_GATE_CLIENT]
    if not found:
        status, _ = _srw_gate_call(opener, token, "POST", admin_api + "/clients", {
            "clientId": _SRW_GATE_CLIENT, "name": _SRW_GATE_CLIENT, "enabled": True,
            "protocol": "openid-connect", "publicClient": True,
            "standardFlowEnabled": False, "directAccessGrantsEnabled": True,
            "serviceAccountsEnabled": False, "fullScopeAllowed": True,
            "defaultClientScopes": ["profile", "email", "roles"],
            "optionalClientScopes": [],
            "attributes": {"srw-gate": _SRW_GATE_MARKER},
            "protocolMappers": _SRW_GATE_MAPPERS,
        })
        if status not in (201, 409):  # 409: another gate created it meanwhile
            raise SystemExit(
                "creating the gate's Keycloak client answered HTTP %d" % status
            )
        return
    client = found[0]
    if (client.get("attributes") or {}).get("srw-gate") != _SRW_GATE_MARKER:
        raise SystemExit(
            "the realm's %s client is not the gates' own; not changing it"
            % _SRW_GATE_CLIENT
        )
    mappers = (
        admin_api + "/clients/" + _parse.quote(client["id"], safe="")
        + "/protocol-mappers/models"
    )
    status, present = _srw_gate_call(opener, token, "GET", mappers)
    if status != 200:
        raise SystemExit("listing the gate client's mappers answered HTTP %d" % status)
    names = {mapper.get("name") for mapper in present or []}
    for mapper in _SRW_GATE_MAPPERS:
        if mapper["name"] not in names:
            status, _ = _srw_gate_call(opener, token, "POST", mappers, mapper)
            if status not in (201, 409):
                raise SystemExit(
                    "adding the gate client's %s mapper answered HTTP %d"
                    % (mapper["name"], status)
                )

def _srw_gate_bound(token):
    import base64 as _base64, json as _json
    try:
        part = token.split(".")[1]
        claims = _json.loads(_base64.urlsafe_b64decode(part + "=" * (-len(part) % 4)))
    except (IndexError, ValueError):
        return False
    aud = claims.get("aud")
    audiences = [aud] if isinstance(aud, str) else aud if isinstance(aud, list) else []
    return bool(claims.get("sub")) and _SRW_GATE_AUDIENCE in audiences

def gate_access_token(opener, token_url, username, password):
    import json as _json, urllib.error as _error, urllib.parse as _parse
    form = _parse.urlencode({
        "grant_type": "password", "client_id": _SRW_GATE_CLIENT, "scope": "openid",
        "username": username, "password": password,
    }).encode()
    ensured = False
    while True:
        try:
            with opener.open(token_url, data=form, timeout=30) as response:
                token = _json.load(response)["access_token"]
        except _error.HTTPError as error:
            unknown = error.code == 401 and b'"invalid_client"' in error.read()
            if ensured or not unknown:
                raise
        else:
            if _srw_gate_bound(token):
                return token
            if ensured:
                raise SystemExit(
                    "the gate client's access token still lacks sub or the %s "
                    "audience" % _SRW_GATE_AUDIENCE
                )
        _srw_gate_client_ensure(opener)
        ensured = True
"""
    + MAPPERS_SOURCE
    + f"_SRW_GATE_CLIENT = {GATE_CLIENT!r}\n_SRW_GATE_MARKER = {GATE_MARKER!r}\n"
)

#: The orchestrator Deployment and container the login runs in.
ORCHESTRATOR = "deploy/srw-orchestrator"
ORCHESTRATOR_CONTAINER = "orchestrator"
DEFAULT_KUBECTL = ("kubectl", "--context", "k3d-srw", "--namespace", "srw")

#: The in-pod program of :func:`access_token`: the envelope (account, and the
#: client to log in through) arrives on stdin, the token is the last line out.
_HOST_LOGIN_PROGRAM = (
    LOGIN_SOURCE
    + r"""
import json as _json, os as _os, sys as _sys, urllib.request as _request
_envelope = _json.loads(_sys.stdin.readline())
_SRW_GATE_CLIENT = _envelope.get("client") or _SRW_GATE_CLIENT
_SRW_GATE_MARKER = _envelope.get("marker") or _SRW_GATE_MARKER
_token_url = (
    _os.environ.get("KEYCLOAK_URL", "").rstrip("/") + "/realms/"
    + (_os.environ.get("KEYCLOAK_REALM", "") or "srw")
    + "/protocol/openid-connect/token"
)
print(gate_access_token(
    _request.build_opener(_request.ProxyHandler({})), _token_url,
    _envelope["username"], _envelope["password"],
))
"""
)


def access_token(
    username: str,
    password: str,
    *,
    kubectl: list[str] | tuple[str, ...] = DEFAULT_KUBECTL,
    client: str = GATE_CLIENT,
    marker: str = GATE_MARKER,
    timeout: float = 120,
) -> str:
    """The account's access token from the gate client, minted in the
    orchestrator pod (where the Keycloak admin credentials are); the password
    travels on stdin, never on a command line."""
    result = subprocess.run(
        [*kubectl, "exec", "-i", ORCHESTRATOR, "-c", ORCHESTRATOR_CONTAINER, "--"]
        + ["python", "-c", _HOST_LOGIN_PROGRAM],
        input=json.dumps(
            {
                "username": username,
                "password": password,
                "client": client,
                "marker": marker,
            }
        )
        + "\n",
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    lines = result.stdout.strip().splitlines()
    if result.returncode != 0 or not lines:
        reason = (result.stderr.strip().splitlines() or ["no output"])[-1]
        raise RuntimeError(f"the gate login in the orchestrator pod failed: {reason}")
    token = lines[-1].strip()
    if token.count(".") != 2 or any(char.isspace() for char in token):
        raise RuntimeError("the gate login returned no access token")
    return token


def main() -> int:
    """Print an access token for ``$SRW_K3D_TEST_USER`` (default ``test``,
    password ``$SRW_K3D_TEST_PASSWORD``, default the k3d dev password) on the
    cluster ``$KUBE_CONTEXT`` / ``$KUBE_NAMESPACE`` (default k3d-srw / srw)."""
    kubectl = (
        "kubectl",
        "--context",
        os.environ.get("KUBE_CONTEXT", "k3d-srw"),
        "--namespace",
        os.environ.get("KUBE_NAMESPACE", "srw"),
    )
    try:
        token = access_token(
            os.environ.get("SRW_K3D_TEST_USER", "test"),
            os.environ.get("SRW_K3D_TEST_PASSWORD", "srw-k3d-dev-test"),
            kubectl=kubectl,
        )
    except (RuntimeError, subprocess.TimeoutExpired) as error:
        print(error, file=sys.stderr)
        return 1
    print(token)
    return 0


__all__ = [
    "GATE_AUDIENCE",
    "GATE_CLIENT",
    "GATE_CLIENT_MAPPERS",
    "GATE_MARKER",
    "LOGIN_SOURCE",
    "MAPPERS_SOURCE",
    "access_token",
    "main",
]


if __name__ == "__main__":
    raise SystemExit(main())
