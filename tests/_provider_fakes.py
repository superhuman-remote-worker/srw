"""Fake provider APIs for the provider-minted connector drivers (C5).

``FakeKubeApi`` answers the three calls SRW makes with a minting credential
(create a Secret, TokenRequest bound to it, delete it) and keeps the tokens
it minted, so a test can ask whether one still authenticates (the API
server's rule: unexpired, and its bound Secret exists with the same uid).
``FakeGitHubApi`` verifies the App JWT with the App's public key, mints
installation tokens with the requested repositories and permissions
checked against what the installation allows, and revokes them.

``ProviderRouter`` routes an ``httpx.MockTransport`` to them by host;
``install`` puts it behind ``provider_http.configure_provider_http``.
"""

from __future__ import annotations

import base64
import json
import secrets
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlsplit

import httpx

KUBE_SERVER = "https://kube.test:6443"
GITHUB_API = "https://api.github.com"


def _self_signed_ca() -> str:
    """A real (self-signed) CA certificate, so an SSL context loads it."""
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import NameOID

    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "fake-kube-ca")])
    now = datetime.now(timezone.utc)
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(days=1))
        .not_valid_after(now + timedelta(days=30))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(key, hashes.SHA256())
    )
    return cert.public_bytes(serialization.Encoding.PEM).decode()


FAKE_CA = _self_signed_ca()


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: datetime) -> str:
    return value.strftime("%Y-%m-%dT%H:%M:%SZ")


def _json(request: httpx.Request) -> Any:
    return json.loads(request.content or b"null")


def minting_kubeconfig_yaml(
    token: str,
    *,
    server: str = KUBE_SERVER,
    namespace: str | None = "work",
    user_extra: dict[str, Any] | None = None,
    cluster_extra: dict[str, Any] | None = None,
) -> str:
    import yaml

    cluster: dict[str, Any] = {
        "server": server,
        "certificate-authority-data": base64.b64encode(FAKE_CA.encode()).decode(),
        **(cluster_extra or {}),
    }
    user: dict[str, Any] = {"token": token, **(user_extra or {})}
    context: dict[str, Any] = {"cluster": "fake", "user": "minter"}
    if namespace:
        context["namespace"] = namespace
    return yaml.safe_dump(
        {
            "apiVersion": "v1",
            "kind": "Config",
            "clusters": [{"name": "fake", "cluster": cluster}],
            "users": [{"name": "minter", "user": user}],
            "contexts": [{"name": "fake", "context": context}],
            "current-context": "fake",
        }
    )


class FakeKubeApi:
    """The API server, as a minting credential with the documented Role
    sees it."""

    def __init__(
        self,
        *,
        minting_token: str = "minting-token-0123456789",
        namespace: str = "srw-identities",
        service_account: str = "agent",
    ) -> None:
        self.minting_token = minting_token
        self.namespace = namespace
        self.service_account = service_account
        self.secrets: dict[str, dict[str, Any]] = {}
        self.tokens: dict[str, dict[str, Any]] = {}
        self.requests: list[tuple[str, str, Any]] = []
        #: Verbs the minting credential lacks ("create-secret",
        #: "delete-secret", "token"): a 403.
        self.forbidden: set[str] = set()
        #: Status to answer the next N calls of a verb with.
        self.fail: dict[str, list[int]] = {}

    def authenticates(self, token: str) -> bool:
        """The bound-token rule: unexpired, its Secret exists, same uid."""
        found = self.tokens.get(token)
        if found is None or found["expires_at"] <= _now():
            return False
        secret = self.secrets.get(found["secret"])
        return secret is not None and secret["uid"] == found["uid"]

    def _failure(self, verb: str) -> httpx.Response | None:
        queued = self.fail.get(verb) or []
        if queued:
            status = queued.pop(0)
            return httpx.Response(status, json={"message": f"injected {status}"})
        if verb in self.forbidden:
            return httpx.Response(
                403, json={"kind": "Status", "message": f"forbidden: {verb}"}
            )
        return None

    def handle(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        body = _json(request) if request.content else None
        self.requests.append((request.method, path, body))
        if request.headers.get("authorization") != f"Bearer {self.minting_token}":
            return httpx.Response(401, json={"message": "Unauthorized"})
        parts = path.strip("/").split("/")
        # /api/v1/namespaces/<ns>/secrets[/<name>]
        if parts[:3] != ["api", "v1", "namespaces"] or len(parts) < 5:
            return httpx.Response(404, json={"message": "not found"})
        namespace = parts[3]
        if namespace != self.namespace:
            return httpx.Response(403, json={"message": "forbidden: namespace"})
        if parts[4] == "secrets" and request.method == "POST" and len(parts) == 5:
            if failure := self._failure("create-secret"):
                return failure
            name = body["metadata"]["name"]
            if name in self.secrets:
                return httpx.Response(409, json={"message": "already exists"})
            uid = secrets.token_hex(8)
            self.secrets[name] = {"uid": uid, "manifest": body}
            return httpx.Response(
                201, json={**body, "metadata": {**body["metadata"], "uid": uid}}
            )
        if parts[4] == "secrets" and request.method == "DELETE" and len(parts) == 6:
            if failure := self._failure("delete-secret"):
                return failure
            name = parts[5]
            found = self.secrets.get(name)
            if found is None:
                return httpx.Response(404, json={"message": "not found"})
            wanted = ((body or {}).get("preconditions") or {}).get("uid")
            if wanted and wanted != found["uid"]:
                return httpx.Response(409, json={"message": "uid precondition"})
            del self.secrets[name]
            return httpx.Response(200, json={"kind": "Status", "status": "Success"})
        if (
            parts[4] == "serviceaccounts"
            and len(parts) == 7
            and parts[6] == "token"
            and request.method == "POST"
        ):
            if parts[5] != self.service_account:
                return httpx.Response(403, json={"message": "forbidden: account"})
            if failure := self._failure("token"):
                return failure
            spec = body["spec"]
            ref = spec["boundObjectRef"]
            secret = self.secrets.get(ref["name"])
            if (
                ref["kind"] != "Secret"
                or secret is None
                or secret["uid"] != ref.get("uid")
            ):
                return httpx.Response(422, json={"message": "bound object missing"})
            seconds = int(spec.get("expirationSeconds") or 3600)
            if seconds < 600:
                return httpx.Response(422, json={"message": "too short"})
            token = "kube-" + secrets.token_urlsafe(24)
            expires = _now() + timedelta(seconds=seconds)
            self.tokens[token] = {
                "secret": ref["name"],
                "uid": ref["uid"],
                "expires_at": expires,
                "audiences": spec.get("audiences"),
            }
            return httpx.Response(
                201,
                json={
                    "kind": "TokenRequest",
                    "status": {"token": token, "expirationTimestamp": _iso(expires)},
                },
            )
        return httpx.Response(404, json={"message": "not found"})


class FakeGitHubApi:
    """GitHub's App endpoints, for one App and one installation."""

    def __init__(
        self,
        *,
        public_key_pem: str,
        app_id: str = "4242",
        installation_id: str = "9090",
        repositories: tuple[str, ...] = ("repo",),
        app_permissions: dict[str, str] | None = None,
        owner: str = "acme",
    ) -> None:
        self.public_key_pem = public_key_pem
        self.app_id = app_id
        self.installation_id = installation_id
        self.repositories = set(repositories)
        self.owner = owner
        self.app_permissions = app_permissions or {"contents": "write"}
        self.tokens: dict[str, dict[str, Any]] = {}
        self.requests: list[tuple[str, str, Any]] = []
        #: Extra permissions to grant beyond the request (a misbehaving
        #: answer SRW must refuse).
        self.grant_extra: dict[str, str] = {}
        self.fail: list[int] = []

    def live(self, token: str) -> dict[str, Any] | None:
        found = self.tokens.get(token)
        if found is None or found["revoked"] or found["expires_at"] <= _now():
            return None
        return found

    def _jwt_ok(self, header: str | None) -> bool:
        import jwt

        if not header or not header.startswith("Bearer "):
            return False
        try:
            claims = jwt.decode(
                header.removeprefix("Bearer "),
                self.public_key_pem,
                algorithms=["RS256"],
                options={"require": ["iat", "exp", "iss"]},
                leeway=0,
            )
        except jwt.PyJWTError:
            return False
        lifetime = int(claims["exp"]) - int(claims["iat"])
        return str(claims["iss"]) == self.app_id and 0 < lifetime <= 600

    def handle(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        body = _json(request) if request.content else None
        self.requests.append((request.method, path, body))
        if self.fail:
            return httpx.Response(self.fail.pop(0), json={"message": "injected"})
        if (
            request.method == "POST"
            and path == f"/app/installations/{self.installation_id}/access_tokens"
        ):
            if not self._jwt_ok(request.headers.get("authorization")):
                return httpx.Response(401, json={"message": "Bad credentials"})
            repositories = body.get("repositories") or []
            permissions = body.get("permissions") or {}
            if not set(repositories) <= self.repositories:
                return httpx.Response(
                    422, json={"message": "There is at least one repository ..."}
                )
            rank = {"read": 0, "write": 1}
            for name, level in permissions.items():
                allowed = self.app_permissions.get(name)
                if allowed is None or rank[level] > rank[allowed]:
                    return httpx.Response(
                        422,
                        json={"message": "The permissions requested are not granted"},
                    )
            token = "ghs_" + secrets.token_urlsafe(27)
            expires = _now() + timedelta(hours=1)
            granted = {**permissions, "metadata": "read", **self.grant_extra}
            self.tokens[token] = {
                "repositories": list(repositories),
                "permissions": granted,
                "expires_at": expires,
                "revoked": False,
            }
            return httpx.Response(
                201,
                json={
                    "token": token,
                    "expires_at": _iso(expires),
                    "permissions": granted,
                    "repository_selection": "selected",
                    "repositories": [{"name": name} for name in repositories],
                },
            )
        if request.method == "DELETE" and path == "/installation/token":
            token = (request.headers.get("authorization") or "").removeprefix("Bearer ")
            found = self.live(token)
            if found is None:
                return httpx.Response(401, json={"message": "Bad credentials"})
            found["revoked"] = True
            return httpx.Response(204)
        if request.method == "GET" and path.startswith("/repos/"):
            token = (request.headers.get("authorization") or "").removeprefix("Bearer ")
            found = self.live(token)
            _, owner, name = path.strip("/").split("/", 2)
            if found is None:
                return httpx.Response(401, json={"message": "Bad credentials"})
            if owner != self.owner or name not in found["repositories"]:
                return httpx.Response(404, json={"message": "Not Found"})
            return httpx.Response(
                200,
                json={
                    "full_name": f"{owner}/{name}",
                    "default_branch": "main",
                    "private": True,
                },
            )
        return httpx.Response(404, json={"message": "Not Found"})


class ProviderRouter:
    """One MockTransport for both fakes, by host."""

    def __init__(
        self, kube: FakeKubeApi | None = None, github: FakeGitHubApi | None = None
    ):
        self.kube = kube
        self.github = github
        self.hosts: list[str] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        host = request.url.host
        self.hosts.append(host)
        if self.kube is not None and host == urlsplit(KUBE_SERVER).hostname:
            return self.kube.handle(request)
        if self.github is not None and host == urlsplit(GITHUB_API).hostname:
            return self.github.handle(request)
        raise httpx.ConnectError(f"no route to {host}", request=request)

    def factory(self):
        transport = httpx.MockTransport(self)

        def make(*, verify: Any = True, timeout: float = 10.0) -> httpx.AsyncClient:
            return httpx.AsyncClient(
                transport=transport, timeout=timeout, follow_redirects=False
            )

        return make


def install(monkeypatch: Any, router: ProviderRouter) -> ProviderRouter:
    """Route every provider call of the C5 drivers to ``router``."""
    from orchestrator.services.connector_drivers import provider_http

    monkeypatch.setitem(provider_http._state, "factory", router.factory())
    return router


def rsa_key_pair() -> tuple[str, str]:
    """``(private PEM, public PEM)`` of a fresh 2048-bit RSA key."""
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    private = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.TraditionalOpenSSL,
        serialization.NoEncryption(),
    ).decode()
    public = (
        key.public_key()
        .public_bytes(
            serialization.Encoding.PEM,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        .decode()
    )
    return private, public
