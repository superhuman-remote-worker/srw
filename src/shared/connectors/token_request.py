"""Kubernetes TokenRequest minting for kubeconfig connectors (connector drivers C5).

A kubeconfig connector may name a ``token_request`` in its config. Its stored
kubeconfig is then a *minting* credential that SRW keeps: at each bind SRW
creates a Secret for the binding in the target ServiceAccount's namespace and
mints a short-lived token for that ServiceAccount through the TokenRequest
API, bound to that Secret (``boundObjectRef``). The workspace receives a
kubeconfig with the cluster's server, its CA and the minted token, nothing
else: no ``exec`` plugin, no client certificate, never the minting
credential. Deleting the Secret revokes the token at the API server, so SRW
deletes it when the execution ends (and when the token expires, so nothing
is left behind).

**The minting credential** is a bearer token, typically a ServiceAccount's
own. SRW refuses a kubeconfig whose user runs an ``exec`` plugin or an
``auth-provider`` (SRW runs no plugins), reads a token file, sends basic
auth or a client certificate, skips TLS verification, goes through a proxy
or reads its CA from a file: none of that can be held as data.

**The minimal RBAC** for the minting credential is a Role in the target
ServiceAccount's own namespace (the API server looks a bound Secret up
there)::

    rules:
    - apiGroups: [""]
      resources: ["serviceaccounts/token"]
      resourceNames: ["<target service account>"]
      verbs: ["create"]
    - apiGroups: [""]
      resources: ["secrets"]
      verbs: ["create", "delete"]

``create`` and ``delete`` on Secrets cannot be narrowed by name (SRW names
each Secret when it creates it), so keep the target ServiceAccount in a
namespace of its own that holds nothing but it and SRW's bound Secrets, and
grant the ServiceAccount its permissions with RoleBindings in the namespaces
it works in (a RoleBinding may name a ServiceAccount of another namespace).
Grant the ServiceAccount nothing in its own namespace. The minting
credential never needs ``get``, ``list`` or ``watch``: it cannot read a
Secret, so a service-account token Secret it could create stays unreadable
to it.

**Access levels.** SRW cannot narrow a token TokenRequest mints: whatever
the target ServiceAccount's RBAC allows, the workspace can do, at either
access level. A read-only connector needs a read-only ServiceAccount.

**Lifetime.** ``expiration_seconds`` is at least 600 (the API server's
minimum) and at most :data:`MAX_EXPIRATION_SECONDS`; the API server may
shorten it (``--service-account-max-token-expiration``). A delivery mints
afresh once less than half of the current token's lifetime is left
(``orchestrator.services.connector_minted_credentials``).

Design: knowledge-base/knowledge/features/connector_drivers.md, "Three ways
to give an agent ephemeral authority" and "Today's types as drivers"; slice
C5.
"""

from __future__ import annotations

import base64
import binascii
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

#: The API server refuses a shorter one (``MinTokenAgeSec``).
MIN_EXPIRATION_SECONDS = 600
DEFAULT_EXPIRATION_SECONDS = 3600
#: SRW's own ceiling: a day.
MAX_EXPIRATION_SECONDS = 86400
MAX_AUDIENCES = 10
MAX_AUDIENCE_LENGTH = 256
#: The config key that turns minting on.
CONFIG_KEY = "token_request"
#: The labels SRW's bound Secrets carry.
MANAGED_BY_LABEL = "app.kubernetes.io/managed-by"
MANAGED_BY = "srw"
CREDENTIAL_LABEL = "srw.io/minted-credential"
#: Every bound Secret's name: this prefix and the credential row's id.
SECRET_PREFIX = "srw-mint-"

_DNS_LABEL = re.compile(r"[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?\Z")
_DNS_SUBDOMAIN = re.compile(
    r"(?=.{1,253}\Z)[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*\Z"
)
_AUDIENCE = re.compile(r"[\x21-\x7e]+\Z")
_TOKEN = re.compile(r"[\x21-\x7e]{16,8192}\Z")
#: A user's fields SRW cannot hold as data, and why.
_REFUSED_USER_FIELDS: dict[str, str] = {
    "exec": "runs an exec plugin",
    "auth-provider": "uses an auth-provider plugin",
    "tokenFile": "reads its token from a file",
    "username": "uses basic auth",
    "password": "uses basic auth",
    "client-certificate": "reads a client certificate from a file",
    "client-key": "reads a client key from a file",
    "client-certificate-data": "authenticates with a client certificate",
    "client-key-data": "authenticates with a client certificate",
    "as": "impersonates another user",
    "as-groups": "impersonates another user",
    "as-uid": "impersonates another user",
    "as-user-extra": "impersonates another user",
}
#: A cluster's fields SRW cannot hold as data, and why.
_REFUSED_CLUSTER_FIELDS: dict[str, str] = {
    "insecure-skip-tls-verify": "skips TLS verification",
    "certificate-authority": "reads its CA from a file",
    "proxy-url": "goes through a proxy",
}


class TokenRequestConfigError(ValueError):
    """A ``token_request`` config or a minting kubeconfig SRW refuses; the
    message says why, in words a connector's owner understands."""


@dataclass(frozen=True)
class TokenRequestOptions:
    """What the connector's ``token_request`` config names."""

    namespace: str
    service_account: str
    expiration_seconds: int = DEFAULT_EXPIRATION_SECONDS
    audiences: tuple[str, ...] = ()

    def as_config(self) -> dict[str, Any]:
        config: dict[str, Any] = {
            "namespace": self.namespace,
            "service_account": self.service_account,
            "expiration_seconds": self.expiration_seconds,
        }
        if self.audiences:
            config["audiences"] = list(self.audiences)
        return config


@dataclass(frozen=True)
class MintingKubeconfig:
    """The parts of a minting kubeconfig SRW uses: where the cluster is, how
    to trust it, the bearer token it mints with, and the namespace the
    workspace's context defaults to. ``token`` is secret."""

    server: str
    ca_pem: str | None
    tls_server_name: str | None
    token: str
    context_namespace: str | None
    cluster_name: str

    def __repr__(self) -> str:  # never the token
        return f"MintingKubeconfig(server={self.server!r})"


def token_request_options(config: Any) -> TokenRequestOptions | None:
    """The connector's ``token_request``, or ``None`` when it names none.

    Raises :class:`TokenRequestConfigError` for one SRW refuses.
    """
    if not isinstance(config, Mapping) or config.get(CONFIG_KEY) is None:
        return None
    return parse_token_request(config.get(CONFIG_KEY))


def parse_token_request(value: Any) -> TokenRequestOptions:
    """A ``token_request`` config, checked."""
    if not isinstance(value, Mapping):
        raise TokenRequestConfigError("token_request must be an object")
    unknown = sorted(
        set(value) - {"namespace", "service_account", "expiration_seconds", "audiences"}
    )
    if unknown:
        raise TokenRequestConfigError(
            f"token_request has fields SRW does not know: {', '.join(unknown)}"
        )
    namespace = value.get("namespace")
    if not isinstance(namespace, str) or not _DNS_LABEL.fullmatch(namespace):
        raise TokenRequestConfigError(
            "token_request.namespace must be the target ServiceAccount's "
            "namespace (a DNS label)"
        )
    account = value.get("service_account")
    if not isinstance(account, str) or not _DNS_SUBDOMAIN.fullmatch(account):
        raise TokenRequestConfigError(
            "token_request.service_account must name the target ServiceAccount"
        )
    seconds = value.get("expiration_seconds", DEFAULT_EXPIRATION_SECONDS)
    if (
        isinstance(seconds, bool)
        or not isinstance(seconds, int)
        or not MIN_EXPIRATION_SECONDS <= seconds <= MAX_EXPIRATION_SECONDS
    ):
        raise TokenRequestConfigError(
            "token_request.expiration_seconds must be a whole number from "
            f"{MIN_EXPIRATION_SECONDS} (the API server's minimum) to "
            f"{MAX_EXPIRATION_SECONDS}"
        )
    raw_audiences = value.get("audiences") or []
    if not isinstance(raw_audiences, list) or len(raw_audiences) > MAX_AUDIENCES:
        raise TokenRequestConfigError(
            f"token_request.audiences must be a list of at most {MAX_AUDIENCES}"
        )
    audiences: list[str] = []
    for audience in raw_audiences:
        if (
            not isinstance(audience, str)
            or len(audience) > MAX_AUDIENCE_LENGTH
            or not _AUDIENCE.fullmatch(audience)
        ):
            raise TokenRequestConfigError(
                "token_request.audiences holds a value that is no audience"
            )
        if audience not in audiences:
            audiences.append(audience)
    return TokenRequestOptions(namespace, account, seconds, tuple(audiences))


def _named(items: Any, name: Any, key: str) -> Mapping[str, Any] | None:
    for item in items if isinstance(items, list) else ():
        if isinstance(item, Mapping) and item.get("name") == name:
            inner = item.get(key)
            return inner if isinstance(inner, Mapping) else None
    return None


def _ca_pem(data: Any) -> str:
    try:
        raw = base64.b64decode(str(data), validate=True)
        text = raw.decode("ascii")
    except (binascii.Error, ValueError, UnicodeDecodeError):
        raise TokenRequestConfigError(
            "the minting kubeconfig's certificate-authority-data is not base64 "
            "PEM certificates"
        ) from None
    if "-----BEGIN CERTIFICATE-----" not in text:
        raise TokenRequestConfigError(
            "the minting kubeconfig's certificate-authority-data holds no certificate"
        )
    return text


def minting_kubeconfig(doc: Any) -> MintingKubeconfig:
    """The current context's cluster and user of a minting kubeconfig (the
    parsed document; the orchestrator reads the YAML).

    Raises :class:`TokenRequestConfigError` for anything SRW cannot hold as
    data or use from its own process (see the module docstring).
    """
    if not isinstance(doc, Mapping):
        raise TokenRequestConfigError("the minting kubeconfig is not a kubeconfig")
    current = doc.get("current-context")
    context = _named(doc.get("contexts"), current, "context")
    if not current or context is None:
        raise TokenRequestConfigError(
            "the minting kubeconfig names no current context SRW can find"
        )
    cluster_name = context.get("cluster")
    cluster = _named(doc.get("clusters"), cluster_name, "cluster")
    user = _named(doc.get("users"), context.get("user"), "user")
    if cluster is None or user is None:
        raise TokenRequestConfigError(
            "the minting kubeconfig's current context names a cluster or a "
            "user it does not define"
        )
    for field_name, why in _REFUSED_USER_FIELDS.items():
        if user.get(field_name):
            raise TokenRequestConfigError(
                f"the minting kubeconfig's user {why}; SRW mints with a bearer "
                "token only (a ServiceAccount's token)"
            )
    for field_name, why in _REFUSED_CLUSTER_FIELDS.items():
        if cluster.get(field_name):
            raise TokenRequestConfigError(
                f"the minting kubeconfig's cluster {why}; SRW refuses it"
            )
    token = user.get("token")
    if not isinstance(token, str) or not _TOKEN.fullmatch(token.strip()):
        raise TokenRequestConfigError(
            "the minting kubeconfig's user holds no bearer token"
        )
    server = cluster.get("server")
    parts = urlsplit(server if isinstance(server, str) else "")
    try:
        port = parts.port
    except ValueError:
        port = -1
    if (
        parts.scheme != "https"
        or not parts.hostname
        or "@" in parts.netloc
        or parts.query
        or parts.fragment
        or port == -1
    ):
        raise TokenRequestConfigError(
            "the minting kubeconfig's server must be an https URL"
        )
    ca = cluster.get("certificate-authority-data")
    tls_name = cluster.get("tls-server-name")
    if tls_name is not None and (
        not isinstance(tls_name, str) or not _DNS_SUBDOMAIN.fullmatch(tls_name)
    ):
        raise TokenRequestConfigError(
            "the minting kubeconfig's tls-server-name is no host name"
        )
    namespace = context.get("namespace")
    if namespace is not None and (
        not isinstance(namespace, str) or not _DNS_LABEL.fullmatch(namespace)
    ):
        namespace = None
    return MintingKubeconfig(
        server=str(server).rstrip("/"),
        ca_pem=_ca_pem(ca) if ca else None,
        tls_server_name=tls_name or None,
        token=token.strip(),
        context_namespace=namespace,
        cluster_name=str(cluster_name or "cluster"),
    )


def delivered_kubeconfig(
    *,
    server: str,
    ca_pem: str | None,
    tls_server_name: str | None,
    token: str,
    context_namespace: str | None,
    name: str,
) -> dict[str, Any]:
    """The kubeconfig the workspace receives (as a document): one cluster,
    one user with the minted token only, one context (no exec, no client
    certificate, no minting credential)."""
    cluster: dict[str, Any] = {"server": server}
    if ca_pem:
        cluster["certificate-authority-data"] = base64.b64encode(
            ca_pem.encode("ascii")
        ).decode("ascii")
    if tls_server_name:
        cluster["tls-server-name"] = tls_server_name
    context: dict[str, Any] = {"cluster": name, "user": name}
    if context_namespace:
        context["namespace"] = context_namespace
    return {
        "apiVersion": "v1",
        "kind": "Config",
        "clusters": [{"name": name, "cluster": cluster}],
        "users": [{"name": name, "user": {"token": token}}],
        "contexts": [{"name": name, "context": context}],
        "current-context": name,
    }


def secret_name(credential_id: Any) -> str:
    """The bound Secret of one minted credential (its row's id)."""
    return SECRET_PREFIX + str(credential_id).replace("-", "")


def bound_secret(
    *, name: str, credential_id: Any, annotations: Mapping[str, str]
) -> dict[str, Any]:
    """The Secret a token is bound to: no data, immutable, labelled SRW's."""
    return {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": name,
            "labels": {
                MANAGED_BY_LABEL: MANAGED_BY,
                CREDENTIAL_LABEL: str(credential_id),
            },
            "annotations": dict(annotations),
        },
        "type": "Opaque",
        "immutable": True,
    }


def token_request(
    options: TokenRequestOptions, *, secret: str, secret_uid: str
) -> dict[str, Any]:
    """The TokenRequest body: the configured lifetime and audiences, bound to
    the Secret by name and uid."""
    spec: dict[str, Any] = {
        "expirationSeconds": options.expiration_seconds,
        "boundObjectRef": {
            "kind": "Secret",
            "apiVersion": "v1",
            "name": secret,
            "uid": secret_uid,
        },
    }
    if options.audiences:
        spec["audiences"] = list(options.audiences)
    return {
        "apiVersion": "authentication.k8s.io/v1",
        "kind": "TokenRequest",
        "spec": spec,
    }


__all__ = [
    "CONFIG_KEY",
    "DEFAULT_EXPIRATION_SECONDS",
    "MAX_EXPIRATION_SECONDS",
    "MIN_EXPIRATION_SECONDS",
    "MintingKubeconfig",
    "TokenRequestConfigError",
    "TokenRequestOptions",
    "bound_secret",
    "delivered_kubeconfig",
    "minting_kubeconfig",
    "parse_token_request",
    "secret_name",
    "token_request",
    "token_request_options",
]
