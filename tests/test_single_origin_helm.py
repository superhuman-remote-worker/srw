"""Rendered contracts for the opt-in single-origin HTTPS gateway."""

from __future__ import annotations

import shutil
import subprocess
import tempfile
from base64 import b64decode
from datetime import timedelta
from hashlib import sha256
from ipaddress import IPv4Address
from pathlib import Path

import pytest
import yaml
from cryptography import x509
from cryptography.hazmat.primitives import serialization


ROOT = Path(__file__).resolve().parents[1]
CHART = ROOT / "helm"

pytestmark = pytest.mark.skipif(
    shutil.which("helm") is None, reason="Helm is not installed"
)


def _run_render(
    values: dict,
    *,
    release_name: str = "single-origin-proof",
    namespace: str = "single-origin-proof",
) -> subprocess.CompletedProcess[str]:
    """Render real chart output with the minimum ordinary chart prerequisites."""

    merged = {
        "license": {"acceptTerms": True},
        "global": {"domain": "example.test"},
        # OpenCloud has no sub-path contract in the approved v1 design. The
        # supported bundled cloud for this profile is Nextcloud (covered below).
        "opencloud": {"enabled": False},
        **values,
    }
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml") as handle:
        yaml.safe_dump(merged, handle)
        handle.flush()
        return subprocess.run(
            [
                "helm",
                "template",
                release_name,
                str(CHART),
                "--namespace",
                namespace,
                "--values",
                handle.name,
            ],
            check=False,
            capture_output=True,
            text=True,
        )


def render(
    values: dict,
    *,
    release_name: str = "single-origin-proof",
    namespace: str = "single-origin-proof",
) -> list[dict]:
    result = _run_render(values, release_name=release_name, namespace=namespace)
    assert result.returncode == 0, result.stderr
    return [document for document in yaml.safe_load_all(result.stdout) if document]


def _single_origin_values(address: str = "192.0.2.10") -> dict:
    return {
        "exposure": {
            "mode": "single-origin",
            "singleOrigin": {"address": address},
        }
    }


def _one(docs: list[dict], kind: str, name_suffix: str) -> dict:
    matches = [
        document
        for document in docs
        if document.get("kind") == kind
        and document.get("metadata", {}).get("name", "").endswith(name_suffix)
    ]
    assert len(matches) == 1, f"expected one {kind} ending in {name_suffix}: {matches}"
    return matches[0]


def _orchestrator_environment(docs: list[dict]) -> dict[str, str]:
    deployment = _one(docs, "Deployment", "-orchestrator")
    return {
        entry["name"]: entry.get("value", "")
        for entry in deployment["spec"]["template"]["spec"]["containers"][0]["env"]
    }


def _application_config(docs: list[dict]) -> dict[str, str]:
    return next(
        document["data"]
        for document in docs
        if document.get("kind") == "ConfigMap"
        and "COCKPIT_EXTERNAL_URL" in document.get("data", {})
    )


def test_single_origin_gateway_listens_on_unprivileged_https_port() -> None:
    docs = render(
        {
            **_single_origin_values(),
        }
    )
    gateway = next(
        document
        for document in docs
        if document["kind"] == "Deployment"
        and document["metadata"]["name"].endswith("single-origin")
    )
    assert (
        gateway["spec"]["template"]["spec"]["containers"][0]["ports"][0][
            "containerPort"
        ]
        == 8443
    )


def test_gateway_is_a_namespaced_annotation_only_traefik_controller() -> None:
    docs = render(_single_origin_values())
    deployment = _one(docs, "Deployment", "-single-origin")
    assert deployment["spec"]["template"]["spec"]["securityContext"] == {
        "runAsNonRoot": True,
        "runAsUser": 65532,
        "runAsGroup": 65532,
        "seccompProfile": {"type": "RuntimeDefault"},
    }
    container = deployment["spec"]["template"]["spec"]["containers"][0]

    assert container["image"] == "docker.io/library/traefik:3.7.13"
    assert container["image"].split(":")[-1] != "latest"
    assert container["args"] == [
        "--entrypoints.websecure.address=:8443",
        "--entrypoints.websecure.http.tls=true",
        "--entrypoints.traefik.address=:9000",
        "--ping=true",
        "--ping.entrypoint=traefik",
        "--providers.kubernetesingress=true",
        "--providers.kubernetesingress.namespaces=single-origin-proof",
        next(
            argument
            for argument in container["args"]
            if argument.startswith("--providers.kubernetesingress.ingressclass=")
        ),
        "--providers.kubernetesingress.disableclusterscoperesources=true",
        "--providers.file.directory=/etc/traefik/dynamic",
        "--providers.file.watch=true",
    ]
    assert not any(
        "ingressendpoint" in argument.lower() for argument in container["args"]
    )

    role = _one(docs, "Role", "-single-origin")
    granted = {
        (tuple(rule.get("apiGroups", [])), tuple(rule["resources"]))
        for rule in role["rules"]
    }
    assert granted == {
        (("",), ("services", "endpoints", "secrets")),
        (("discovery.k8s.io",), ("endpointslices",)),
        (("networking.k8s.io",), ("ingresses",)),
    }
    assert all(rule["verbs"] == ["get", "list", "watch"] for rule in role["rules"])

    forbidden_gateway_kinds = {
        "ClusterRole",
        "ClusterRoleBinding",
        "IngressClass",
    }
    gateway_documents = [
        document
        for document in docs
        if document.get("metadata", {})
        .get("labels", {})
        .get("app.kubernetes.io/component")
        == "single-origin-gateway"
    ]
    assert not (
        {document["kind"] for document in gateway_documents} & forbidden_gateway_kinds
    )
    forbidden_extension_kinds = {
        "Certificate",
        "Issuer",
        "ClusterIssuer",
        "Middleware",
        "TLSStore",
        "IngressRoute",
    }
    assert not ({document["kind"] for document in docs} & forbidden_extension_kinds)


def test_gateway_image_digest_override_takes_precedence_over_tag() -> None:
    values = _single_origin_values()
    values["exposure"]["singleOrigin"]["gateway"] = {
        "image": {"digest": f"sha256:{'a' * 64}"}
    }
    docs = render(values)
    container = _one(docs, "Deployment", "-single-origin")["spec"]["template"]["spec"][
        "containers"
    ][0]
    assert container["image"] == f"docker.io/library/traefik@sha256:{'a' * 64}"


def test_gateway_service_and_mounts_preserve_nodeport_and_projected_updates() -> None:
    docs = render(_single_origin_values())
    service = _one(docs, "Service", "-single-origin")
    assert service["spec"]["type"] == "NodePort"
    assert service["spec"]["ports"] == [
        {
            "name": "https",
            "protocol": "TCP",
            "port": 443,
            "targetPort": 8443,
            "nodePort": 30443,
        }
    ]

    deployment = _one(docs, "Deployment", "-single-origin")
    pod = deployment["spec"]["template"]["spec"]
    container = pod["containers"][0]
    assert {mount["mountPath"] for mount in container["volumeMounts"]} == {
        "/etc/traefik/dynamic",
        "/etc/traefik/tls",
    }
    assert all("subPath" not in mount for mount in container["volumeMounts"])
    assert {next(iter(volume.keys() - {"name"})) for volume in pod["volumes"]} == {
        "configMap",
        "secret",
    }


@pytest.mark.parametrize(
    ("address", "san_type", "san_value"),
    [
        ("192.0.2.10", x509.IPAddress, IPv4Address("192.0.2.10")),
        ("localhost", x509.DNSName, "localhost"),
    ],
)
def test_generated_certificate_has_matching_san_key_checksum_and_validity(
    address: str, san_type: type[x509.GeneralName], san_value: object
) -> None:
    docs = render(_single_origin_values(address))
    secret = _one(docs, "Secret", "-single-origin-tls")
    cert_pem = b64decode(secret["data"]["tls.crt"])
    key_pem = b64decode(secret["data"]["tls.key"])
    cert = x509.load_pem_x509_certificate(cert_pem)
    key = serialization.load_pem_private_key(key_pem, password=None)

    sans = cert.extensions.get_extension_for_class(x509.SubjectAlternativeName).value
    assert sans.get_values_for_type(san_type) == [san_value]
    assert cert.public_key().public_bytes(
        serialization.Encoding.DER,
        serialization.PublicFormat.SubjectPublicKeyInfo,
    ) == key.public_key().public_bytes(
        serialization.Encoding.DER,
        serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    validity = cert.not_valid_after_utc - cert.not_valid_before_utc
    assert timedelta(days=364) <= validity <= timedelta(days=366)

    deployment = _one(docs, "Deployment", "-single-origin")
    assert (
        deployment["spec"]["template"]["metadata"]["annotations"][
            "checksum/tls-certificate"
        ]
        == sha256(cert_pem).hexdigest()
    )


def test_certificate_honors_configured_validity() -> None:
    values = _single_origin_values()
    values["exposure"]["singleOrigin"]["tls"] = {"validityDays": 30}
    cert_pem = b64decode(
        _one(render(values), "Secret", "-single-origin-tls")["data"]["tls.crt"]
    )
    cert = x509.load_pem_x509_certificate(cert_pem)
    validity = cert.not_valid_after_utc - cert.not_valid_before_utc
    assert timedelta(days=29) <= validity <= timedelta(days=31)


def test_file_provider_owns_default_certificate_and_path_middlewares() -> None:
    docs = render(
        {
            **_single_origin_values(),
            "nextcloud": {"enabled": True, "internal": True},
        }
    )
    config = yaml.safe_load(
        _one(docs, "ConfigMap", "-single-origin")["data"]["dynamic.yaml"]
    )
    assert config["tls"]["stores"]["default"]["defaultCertificate"] == {
        "certFile": "/etc/traefik/tls/tls.crt",
        "keyFile": "/etc/traefik/tls/tls.key",
    }
    middlewares = config["http"]["middlewares"]
    assert middlewares["strip-git"]["stripPrefix"]["prefixes"] == ["/git"]
    assert middlewares["strip-cloud"]["stripPrefix"]["prefixes"] == ["/cloud"]
    assert middlewares["redirect-git-slash"]["redirectRegex"] == {
        "regex": r"^https://([^/]+)/git(\?.*)?$",
        "replacement": "https://${1}/git/${2}",
        "permanent": True,
    }
    assert middlewares["redirect-cloud-slash"]["redirectRegex"] == {
        "regex": r"^https://([^/]+)/cloud(\?.*)?$",
        "replacement": "https://${1}/cloud/${2}",
        "permanent": True,
    }


def test_static_routes_are_hostless_class_isolated_and_do_not_claim_sessions() -> None:
    docs = render(
        {
            **_single_origin_values(),
            "nextcloud": {"enabled": True, "internal": True},
        }
    )
    ingresses = [document for document in docs if document["kind"] == "Ingress"]
    assert len(ingresses) == 7

    annotations = [ingress["metadata"]["annotations"] for ingress in ingresses]
    classes = {annotation["kubernetes.io/ingress.class"] for annotation in annotations}
    assert len(classes) == 1
    assert all(
        annotation["traefik.ingress.kubernetes.io/router.entrypoints"] == "websecure"
        for annotation in annotations
    )
    assert all("ingressClassName" not in ingress["spec"] for ingress in ingresses)
    assert all("tls" not in ingress["spec"] for ingress in ingresses)
    assert all("host" not in ingress["spec"]["rules"][0] for ingress in ingresses)

    paths = {
        ingress["metadata"]["name"].rsplit("-single-origin-", 1)[-1]: [
            (path["path"], path["pathType"])
            for path in ingress["spec"]["rules"][0]["http"]["paths"]
        ]
        for ingress in ingresses
    }
    assert paths == {
        "orchestrator": [
            ("/api", "Prefix"),
            ("/auth", "Prefix"),
            ("/ws", "Prefix"),
        ],
        "identity": [("/identity", "Prefix")],
        "git": [("/git/", "Prefix")],
        "git-redirect": [("/git", "Exact")],
        "cloud": [("/cloud/", "Prefix")],
        "cloud-redirect": [("/cloud", "Exact")],
        "cockpit": [("/", "Prefix")],
    }
    assert not any(
        path.startswith("/p")
        for ingress in ingresses
        for path in (
            item["path"] for item in ingress["spec"]["rules"][0]["http"]["paths"]
        )
    )

    by_suffix = {
        ingress["metadata"]["name"].rsplit("-single-origin-", 1)[-1]: ingress
        for ingress in ingresses
    }
    assert (
        by_suffix["git"]["metadata"]["annotations"][
            "traefik.ingress.kubernetes.io/router.middlewares"
        ]
        == "strip-git@file"
    )
    assert (
        by_suffix["cloud"]["metadata"]["annotations"][
            "traefik.ingress.kubernetes.io/router.middlewares"
        ]
        == "strip-cloud@file"
    )
    assert (
        by_suffix["git-redirect"]["metadata"]["annotations"][
            "traefik.ingress.kubernetes.io/router.middlewares"
        ]
        == "redirect-git-slash@file"
    )
    assert (
        by_suffix["cloud-redirect"]["metadata"]["annotations"][
            "traefik.ingress.kubernetes.io/router.middlewares"
        ]
        == "redirect-cloud-slash@file"
    )


def test_canonical_urls_bff_policy_and_session_router_contract() -> None:
    docs = render(
        {
            **_single_origin_values(),
            "sessionRouter": {
                "annotations": {
                    "example.test/retained": "yes",
                    "kubernetes.io/ingress.class": "must-not-win",
                }
            },
        }
    )
    config = _application_config(docs)
    origin = "https://192.0.2.10:30443"
    assert config["COCKPIT_EXTERNAL_URL"] == origin
    assert config["GITEA_URL"] == f"{origin}/git"
    assert config["GITEA_INTERNAL_URL"].endswith("-gitea:3000")
    assert config["KEYCLOAK_URL"].endswith("-keycloak:8080/identity")
    assert config["KEYCLOAK_ISSUER_URL"] == f"{origin}/identity"
    assert config["SRW_BFF_REDIRECT_URI"] == f"{origin}/auth/callback"
    assert config["SRW_SPA_BASE_URL"] == origin
    assert config["SRW_COOKIE_DOMAIN"] == ""
    assert config["SRW_COOKIE_SECURE"] == "1"
    assert config["SRW_COOKIE_SAMESITE"] == "lax"
    assert config["CORS_ORIGINS"] == origin
    assert config["IDE_PROXY_BASE_URL"] == origin

    environment = _orchestrator_environment(docs)
    assert environment["SESSION_PUBLIC_ORIGIN"] == origin
    assert environment["SESSION_INGRESS_HOST"] == ""
    assert environment["SESSION_INGRESS_CLASS"]
    assert environment["SESSION_INGRESS_SINGLE_ORIGIN"] == "1"
    assert environment["SESSION_INGRESS_TLS_SECRET"] == ""
    annotations = yaml.safe_load(environment["SESSION_INGRESS_ANNOTATIONS"])
    assert annotations == {
        "kubernetes.io/ingress.class": environment["SESSION_INGRESS_CLASS"],
        "traefik.ingress.kubernetes.io/router.entrypoints": "websecure",
        "example.test/retained": "yes",
    }


def test_public_origin_normalizes_default_https_port() -> None:
    values = _single_origin_values()
    values["exposure"]["singleOrigin"]["publicPort"] = 443
    docs = render(values)
    config = _application_config(docs)
    assert config["COCKPIT_EXTERNAL_URL"] == "https://192.0.2.10"
    assert config["CORS_ORIGINS"] == "https://192.0.2.10"
    assert config["KEYCLOAK_ISSUER_URL"] == "https://192.0.2.10/identity"


def test_single_origin_does_not_require_a_domain_or_derive_ip_subdomains() -> None:
    values = _single_origin_values()
    values["global"] = {"domain": ""}
    docs = render(values)
    config = _application_config(docs)
    origin = "https://192.0.2.10:30443"
    assert config["COCKPIT_EXTERNAL_URL"] == origin
    assert config["KEYCLOAK_ISSUER_URL"] == f"{origin}/identity"
    assert config["GITEA_URL"] == f"{origin}/git"
    assert "api.192.0.2.10" not in yaml.safe_dump(docs)


def test_gateway_class_is_unique_for_release_and_namespace_and_dns_bounded() -> None:
    first = render(_single_origin_values(), release_name="same", namespace="first")
    second = render(_single_origin_values(), release_name="same", namespace="second")
    third = render(_single_origin_values(), release_name="r" * 53, namespace="n" * 63)

    def ingress_class(documents: list[dict]) -> str:
        ingress = next(
            document for document in documents if document["kind"] == "Ingress"
        )
        return ingress["metadata"]["annotations"]["kubernetes.io/ingress.class"]

    classes = {ingress_class(first), ingress_class(second), ingress_class(third)}
    assert len(classes) == 3
    assert all(
        len(value) <= 63 and value.endswith("-srw-single-origin") for value in classes
    )
    assert len(_one(third, "Deployment", "-single-origin")["metadata"]["name"]) <= 63


def test_gateway_resource_names_do_not_collide_for_long_releases() -> None:
    def gateway_names(release: str) -> set[tuple[str, str]]:
        return {
            (doc["kind"], doc["metadata"]["name"])
            for doc in render(_single_origin_values(), release_name=release)
            if doc["metadata"].get("labels", {}).get("app.kubernetes.io/component")
            == "single-origin-gateway"
        }

    first = gateway_names("r" * 49 + "a")
    second = gateway_names("r" * 49 + "b")
    assert first and second
    assert not first.intersection(second)
    assert all(len(name) <= 63 for _, name in first | second)


def test_gateway_class_identity_has_unambiguous_namespace_release_boundary() -> None:
    classes = []
    for namespace, release in [("a-b", "c"), ("a", "b-c")]:
        docs = render(
            _single_origin_values(), namespace=namespace, release_name=release
        )
        ingress = next(doc for doc in docs if doc["kind"] == "Ingress")
        classes.append(
            ingress["metadata"]["annotations"]["kubernetes.io/ingress.class"]
        )
    assert classes[0] != classes[1]


@pytest.mark.parametrize(
    ("single_origin", "message"),
    [
        ({"address": "example.com"}, "localhost or a valid IPv4"),
        ({"address": "999.1.1.1"}, "localhost or a valid IPv4"),
        ({"address": "192.0.2.10", "publicPort": 0}, "publicPort"),
        ({"address": "192.0.2.10", "publicPort": 65536}, "publicPort"),
        (
            {"address": "192.0.2.10", "service": {"nodePort": 29999}},
            "nodePort",
        ),
        (
            {"address": "192.0.2.10", "service": {"nodePort": 32768}},
            "nodePort",
        ),
        (
            {"address": "192.0.2.10", "tls": {"validityDays": 0}},
            "validityDays",
        ),
        (
            {"address": "192.0.2.10", "tls": {"mode": "cert-manager"}},
            "self-signed",
        ),
    ],
)
def test_invalid_single_origin_values_fail_render(
    single_origin: dict, message: str
) -> None:
    result = _run_render(
        {"exposure": {"mode": "single-origin", "singleOrigin": single_origin}}
    )
    assert result.returncode != 0
    assert message in result.stderr


def test_invalid_exposure_mode_fails_schema_validation() -> None:
    result = _run_render({"exposure": {"mode": "public-ip"}})
    assert result.returncode != 0
    assert "multi-host" in result.stderr
    assert "single-origin" in result.stderr


@pytest.mark.parametrize(
    ("extra", "message"),
    [
        (
            {"agent": {"pinnedLegacyNamespaces": ["agents-old"]}},
            "pinnedLegacyNamespaces",
        ),
        (
            {"keycloak": {"enabled": True, "internal": False}},
            "bundled keycloak.internal=true",
        ),
        (
            {"gitea": {"enabled": True, "internal": False}},
            "bundled gitea.internal=true",
        ),
        (
            {"nextcloud": {"enabled": True, "internal": False}},
            "bundled nextcloud.internal=true",
        ),
        ({"opencloud": {"enabled": True}}, "opencloud.enabled=false"),
        ({"sshGateway": {"enabled": True}}, "sshGateway.enabled"),
        (
            {"databases": {"neo4j": {"boltTls": {"enabled": True}}}},
            "boltTls.enabled=false",
        ),
    ],
)
def test_unsupported_single_origin_integrations_fail_actionably(
    extra: dict, message: str
) -> None:
    result = _run_render({**_single_origin_values(), **extra})
    assert result.returncode != 0
    assert message in result.stderr


def test_multi_host_remains_the_default_and_keeps_existing_urls() -> None:
    docs = render({})
    assert not any(
        document["kind"] == "Deployment"
        and document["metadata"]["name"].endswith("-single-origin")
        for document in docs
    )
    assert not any(
        document["kind"] == "Secret"
        and document["metadata"]["name"].endswith("-single-origin-tls")
        for document in docs
    )
    config = _application_config(docs)
    assert config["COCKPIT_EXTERNAL_URL"] == "https://example.test"
    assert config["GITEA_URL"] == "https://git.example.test"
    assert config["KEYCLOAK_URL"].endswith("-keycloak:8080")
    assert config["KEYCLOAK_ISSUER_URL"] == "https://auth.example.test"
    assert config["SRW_COOKIE_DOMAIN"] == ".example.test"

    ingresses = [document for document in docs if document["kind"] == "Ingress"]
    assert ingresses
    assert all("host" in ingress["spec"]["rules"][0] for ingress in ingresses)


@pytest.mark.parametrize("single_origin", [False, True])
def test_cockpit_capabilities_follow_exposure_profile(single_origin: bool) -> None:
    docs = render(_single_origin_values() if single_origin else {})
    env_js = _one(docs, "ConfigMap", "-cockpit-env")["data"]["env.js"]
    deployment = _one(docs, "Deployment", "-cockpit")
    assert (
        deployment["spec"]["template"]["metadata"]["annotations"][
            "checksum/environment"
        ]
        == sha256(env_js.encode()).hexdigest()
    )
    flag = "false" if single_origin else "true"
    for capability in [
        "serviceWorkerEnabled",
        "externalClientsEnabled",
        "adminToolsEnabled",
    ]:
        assert f"window['env']['{capability}'] = {flag};" in env_js
