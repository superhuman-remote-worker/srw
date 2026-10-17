#!/usr/bin/env python3
"""Exercise the manifest API through local k3d ingress and real Keycloak auth.

Uses the documented local development identity. Credentials stay in memory;
no Jobs, workspace instances, or Project activations are created. The cluster
must already contain the implementation; this script does not deploy it.
"""

from copy import deepcopy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import ssl
import subprocess
import sys

import httpx

from shared.manifests import parse_documents

ROOT = Path(__file__).resolve().parents[1]
KUBECTL = ["kubectl", "--context", "k3d-srw", "--namespace", "srw"]
API = "https://api.localhost"

# Logins go through the gates' Keycloak client (scripts/k3d_gate_oauth.py), in
# the orchestrator pod: its access token carries `sub`, the realm roles and
# the audience the API's Bearer check accepts. admin-cli's ID token is no
# Bearer credential.
_OAUTH_SPEC = importlib.util.spec_from_file_location(
    "_k3d_gate_oauth", ROOT / "scripts" / "k3d_gate_oauth.py"
)
assert _OAUTH_SPEC is not None and _OAUTH_SPEC.loader is not None
gate_oauth = importlib.util.module_from_spec(_OAUTH_SPEC)
_OAUTH_SPEC.loader.exec_module(gate_oauth)


class GateFailure(Exception):
    """A diagnostic which never embeds credentials or arbitrary API responses."""


def require(condition, message):
    if not condition:
        raise GateFailure(message)


def command(args):
    result = subprocess.run(args, text=True, capture_output=True, timeout=30)
    require(
        result.returncode == 0, "A cluster inspection failed; check k3d-srw readiness."
    )
    return result.stdout


def deployed_identity():
    paths = [
        str(path.relative_to(ROOT))
        for path in sorted((ROOT / "src/shared/manifests").glob("*"))
        if path.suffix in {".py", ".json"}
    ] + [
        "src/orchestrator/routers/manifests.py",
        "src/orchestrator/schemas/manifests.py",
        "src/orchestrator/services/manifests.py",
        "src/orchestrator/services/manifest_legacy.py",
    ]
    code = (
        "import hashlib,json; from pathlib import Path; "
        f"paths={paths!r}; "
        'print(json.dumps({p:hashlib.sha256(Path("/app",p).read_bytes()).hexdigest() for p in paths}))'
    )
    remote = json.loads(
        command(
            KUBECTL
            + [
                "exec",
                "deployment/srw-orchestrator",
                "-c",
                "orchestrator",
                "--",
                "python",
                "-I",
                "-c",
                code,
            ]
        )
    )
    local = {
        path: hashlib.sha256((ROOT / path).read_bytes()).hexdigest() for path in paths
    }
    require(
        remote == local,
        "Deployed manifest source differs from this checkout; reconcile through Helm/Tilt first.",
    )
    pods = json.loads(
        command(
            KUBECTL
            + [
                "get",
                "pods",
                "-l",
                "app.kubernetes.io/component=orchestrator,app.kubernetes.io/instance=srw",
                "-o",
                "json",
            ]
        )
    )
    ready = [
        pod
        for pod in pods["items"]
        if any(
            condition["type"] == "Ready" and condition["status"] == "True"
            for condition in pod["status"].get("conditions", [])
        )
    ]
    require(len(ready) == 1, "Run this gate with one ready local orchestrator replica.")
    container = next(
        item
        for item in ready[0]["status"]["containerStatuses"]
        if item["name"] == "orchestrator"
    )
    return {
        "pod": ready[0]["metadata"]["name"],
        "image": container["image"],
        "imageID": container["imageID"],
        "sourceFilesMatched": len(paths),
        "sourceDigest": hashlib.sha256(
            json.dumps(local, sort_keys=True).encode()
        ).hexdigest(),
    }


def run():
    identity = deployed_identity()
    documents = [
        doc
        for path in sorted((ROOT / "examples/manifests").glob("*.yaml"))
        for doc in parse_documents(path.read_text())
    ]
    body = {"source": json.dumps(documents), "format": "json"}
    passed = []

    # Use the system trust store, including the local mkcert CA. No TLS bypass.
    with httpx.Client(
        verify=ssl.create_default_context(), trust_env=False, timeout=30
    ) as client:

        def request(operation, payload=None, *, headers=None, status=200):
            response = client.request(
                "GET" if operation == "schema" else "POST",
                f"{API}/api/manifests/{operation}",
                json=payload,
                headers=headers,
            )
            require(
                response.status_code == status,
                f"{operation}: expected HTTP {status}, received {response.status_code}.",
            )
            return response.json()

        for headers in ({}, {"Authorization": "Bearer invalid-test-token"}):
            for operation in ("schema", "validate", "preview", "export"):
                request(
                    operation,
                    None if operation == "schema" else body,
                    headers=headers,
                    status=401,
                )
        passed.append("all four endpoints reject missing and invalid authentication")

        try:
            token = gate_oauth.access_token(
                os.environ.get("SRW_K3D_TEST_USER", "test"),
                os.environ.get("SRW_K3D_TEST_PASSWORD", "srw-k3d-dev-test"),
                kubectl=KUBECTL,
            )
        except (RuntimeError, subprocess.TimeoutExpired):
            raise GateFailure("Local Keycloak login failed.") from None
        headers = {"Authorization": f"Bearer {token}"}

        require(
            request("schema", headers=headers)["properties"]["apiVersion"]["const"]
            == "srw/v1alpha1",
            "Packaged schema version differs.",
        )
        validation = request("validate", body, headers=headers)
        require(
            validation["valid"] and len(validation["documents"]) == len(documents),
            "Public examples did not all validate.",
        )
        preview = request("preview", body, headers=headers)
        require(
            preview["admissionReady"] is False and preview["effects"] == [],
            "Preview must remain separate from admission.",
        )
        require(
            {
                "resourceAuthorization",
                "credentialDelivery",
                "workspaceInstances",
                "projectActivation",
            }
            <= set(preview["pendingChecks"]),
            "Preview omitted pending live checks.",
        )
        require(
            preview["documents"] == documents, "Preview changed authored definitions."
        )
        jobs = {
            doc["metadata"]["name"]: doc["spec"]
            for doc in preview["resolved"]
            if doc["kind"] == "Job"
        }
        inline = jobs["zero-integration-example"]["execution"]["expert"]["inline"][
            "runtime"
        ]
        require(
            inline["config"]["arbitraryOption"]["keepLiteralNull"] is None,
            "Private null was lost.",
        )
        require(
            inline["config"]["tools"] == ["a_tool_this_image_does_not_have"],
            "Private tools were normalized.",
        )
        source = jobs["implement-cpp-feature"]["execution"]["connectors"]["source"][
            "inline"
        ]
        require(
            source["credentials"]["token"]["secretRef"]["scope"]
            == {"kind": "Account", "name": "personal"},
            "Referenced credential scope changed.",
        )
        passed.append(
            f"real Keycloak authentication, packaged schema, and all {len(documents)} example resources"
        )
        passed.append(
            "reference resolution, opaque harness settings, secret references, and pending admission checks"
        )

        for format in ("yaml", "json"):
            exported = request(
                "export", {**body, "output_format": format}, headers=headers
            )
            again = request(
                "preview",
                {"source": exported["source"], "format": format},
                headers=headers,
            )
            require(
                again == preview,
                f"{format} export/preview round trip changed the result.",
            )
        passed.append(
            "YAML and JSON export/preview round trips preserve references and ownership"
        )

        default_job = {
            "apiVersion": "srw/v1alpha1",
            "kind": "Job",
            "metadata": {
                "name": "manifest-k3d-defaults",
                "scope": {"kind": "Project", "name": "cpp-product"},
            },
            "spec": {"execution": {}},
        }
        default_body = {
            "source": json.dumps([*documents, default_job]),
            "format": "json",
        }
        defaults = request("preview", default_body, headers=headers)
        execution = defaults["resolved"][-1]["spec"]["execution"]
        require(
            len(defaults["defaults"]) == 3
            and execution["workspace"] is not None
            and "source" in execution["connectors"],
            "Project selection defaults were not applied.",
        )
        default_job["spec"]["execution"].update(workspace=None, connectors={})
        disabled = request(
            "preview",
            {"source": json.dumps([*documents, default_job]), "format": "json"},
            headers=headers,
        )
        execution = disabled["resolved"][-1]["spec"]["execution"]
        require(
            len(disabled["defaults"]) == 1
            and execution["workspace"] is None
            and execution["connectors"] == {},
            "Explicit empty selections did not suppress defaults.",
        )
        passed.append("project defaults and explicit workspace/connector opt-outs")

        duplicate = request(
            "validate",
            {"source": "kind: Expert\nkind: PRIVATE-SENTINEL"},
            headers=headers,
            status=422,
        )
        require(
            duplicate["detail"]["code"] == "DuplicateKey"
            and "PRIVATE-SENTINEL" not in json.dumps(duplicate),
            "Duplicate-key diagnostics exposed source or used the wrong code.",
        )
        missing = deepcopy(
            next(
                doc
                for doc in documents
                if doc["metadata"]["name"] == "implement-cpp-feature"
            )
        )
        missing["spec"]["execution"]["expert"]["ref"]["name"] = "not-supplied"
        missing_response = request(
            "preview",
            {"source": json.dumps([missing]), "format": "json"},
            headers=headers,
            status=422,
        )
        require(
            missing_response["detail"]["code"] == "UnresolvedReference",
            "Missing reference did not produce the expected diagnostic.",
        )
        request(
            "validate", {"source": " " * (1024 * 1024 + 1)}, headers=headers, status=422
        )
        passed.append(
            "malformed/oversized input and unresolved references fail with HTTP 422"
        )

    after = deployed_identity()
    require(
        after == identity,
        "The deployed replica changed during the gate; rerun against a stable rollout.",
    )
    print(
        json.dumps(
            {
                "passed": True,
                "context": "k3d-srw",
                "namespace": "srw",
                "api": API,
                **identity,
                "checks": passed,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    try:
        run()
    except (
        GateFailure,
        httpx.HTTPError,
        OSError,
        subprocess.TimeoutExpired,
        ValueError,
        KeyError,
    ) as exc:
        # HTTP/parser exceptions may carry arbitrary response bodies or credentials.
        message = (
            str(exc)
            if isinstance(exc, GateFailure)
            else "Gate failed during transport, cluster inspection, or response validation."
        )
        print(json.dumps({"passed": False, "error": message}), file=sys.stderr)
        raise SystemExit(1)
