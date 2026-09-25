#!/usr/bin/env python3
"""Verify a deployed manifest cutover through real local ingress and auth.

Run explicitly after deploying this checkout to k3d-srw. This script does not
build, deploy, enable native hosting, or alter existing definitions. It writes
uniquely named Account resources and an inactive disposable Project, then retires
them through the public API. A generic Job must be rejected before admission on
the current unverified CNI profile. Read-only SQL establishes rollback evidence.
Credentials, arbitrary HTTP bodies, and SQL connection details are never printed.
"""

from __future__ import annotations

import argparse
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import ssl
import subprocess
from urllib.parse import urlparse
from uuid import UUID, uuid4

import httpx

from shared.manifests import parse_documents, validate_documents

ROOT = Path(__file__).resolve().parents[1]
KUBECTL = ["kubectl", "--context", "k3d-srw", "--namespace", "srw"]
API = "https://api.localhost"
AUTH = "https://auth.localhost/realms/srw/protocol/openid-connect/token"
OWNER_LABEL = "srw.io/cutover-gate"


class GateFailure(Exception):
    """Only fixed diagnostics belong here; never interpolate server bodies."""


def require(condition, message):
    if not condition:
        raise GateFailure(message)


def command(args):
    try:
        result = subprocess.run(args, text=True, capture_output=True, timeout=45)
    except (OSError, subprocess.TimeoutExpired):
        raise GateFailure("Local cluster inspection did not complete.") from None
    require(result.returncode == 0, "Local cluster inspection failed.")
    return result.stdout


def remote_json(code):
    raw = command(
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
    try:
        return json.loads(raw)
    except ValueError:
        raise GateFailure("Cluster inspection returned an invalid result.") from None


def source_paths():
    paths = {
        "src/orchestrator/main.py",
        "src/orchestrator/database/postgres.py",
        "src/orchestrator/routers/expert_catalog.py",
        "src/orchestrator/routers/projects.py",
        "src/orchestrator/services/expert_catalog.py",
        "src/orchestrator/services/expert_authoring.py",
        "src/orchestrator/services/projects.py",
        "src/orchestrator/services/container_provisioner.py",
        "src/orchestrator/services/stateless_workspace_history_cleanup.py",
        "src/shared/runtime/core/srw_manifest_config.py",
        "src/shared/runtime/core/expert_resolution.py",
        "src/shared/runtime/core/workspace_selection.py",
        "src/shared/runtime/core/tool_report.py",
        "src/orchestrator/services/config_resolver.py",
        "src/orchestrator/services/agent_registration.py",
        "src/orchestrator/services/job_admission.py",
        "src/orchestrator/services/job_admission_config.py",
        "src/orchestrator/services/thread_admission.py",
        "src/orchestrator/schemas/job_create.py",
        "src/orchestrator/schemas/thread_admission.py",
    }
    for pattern in (
        "src/orchestrator/application/*.py",
        "src/shared/manifests/*.py",
        "src/shared/manifests/*.json",
        "src/orchestrator/routers/manifests.py",
        "src/orchestrator/schemas/manifests.py",
        "src/orchestrator/services/manifest*.py",
        "src/orchestrator/services/generic_harness_runtime.py",
        "src/orchestrator/database/migrations/app/*manifest*.sql",
        "src/orchestrator/database/migrations/app/0236_*.sql",
        "src/orchestrator/database/migrations/app/0237_*.sql",
        "src/orchestrator/database/migrations/app/0238_*.sql",
        "config/experts/*/config.yaml",
        "config/subagents/*/config.yaml",
    ):
        paths.update(str(path.relative_to(ROOT)) for path in ROOT.glob(pattern))
    return sorted(paths)


def deployed_identity():
    # Explicit context and localhost API are checked before credentials or writes.
    kubeconfig = json.loads(
        command(KUBECTL + ["config", "view", "--minify", "-o", "json"])
    )
    server = kubeconfig["clusters"][0]["cluster"]["server"]
    require(
        urlparse(server).hostname in {"localhost", "127.0.0.1", "0.0.0.0", "::1"},
        "k3d-srw must identify a local Kubernetes API endpoint.",
    )
    nodes = json.loads(command(KUBECTL + ["get", "nodes", "-o", "json"]))
    require(
        bool(nodes["items"])
        and all(
            item["metadata"]["name"].startswith("k3d-srw-") for item in nodes["items"]
        ),
        "The selected cluster does not contain the expected k3d-srw nodes.",
    )
    paths = source_paths()
    code = (
        "import hashlib,json,os; from pathlib import Path; "
        f"paths={paths!r}; "
        "print(json.dumps({'files':{p:hashlib.sha256(Path('/app',p).read_bytes()).hexdigest() for p in paths},"
        "'nativeHostingEnabled':os.getenv('MANIFEST_NETWORK_ISOLATION_VERIFIED','false').lower()=='true',"
        "'nativeNamespace':os.getenv('MANIFEST_NAMESPACE','srw-native')}))"
    )
    remote = remote_json(code)
    local = {
        path: hashlib.sha256((ROOT / path).read_bytes()).hexdigest() for path in paths
    }
    require(
        remote["files"] == local,
        "Deployed cutover source differs from this checkout; deploy coherently first.",
    )
    require(
        remote["nativeHostingEnabled"] is False,
        "This gate requires generic hosting disabled on the unverified k3d-srw profile.",
    )
    require(
        re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", remote["nativeNamespace"])
        and remote["nativeNamespace"] != "srw",
        "Native hosting must use a distinct valid namespace.",
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
            item["type"] == "Ready" and item["status"] == "True"
            for item in pod["status"].get("conditions", [])
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
        "imageID": container["imageID"],
        "sourceFilesMatched": len(paths),
        "sourceDigest": hashlib.sha256(
            json.dumps(local, sort_keys=True).encode()
        ).hexdigest(),
        "nativeHostingEnabled": False,
        "nativeNamespace": remote["nativeNamespace"],
    }


def database_evidence(prefix, rejected_names):
    """Read aggregate facts in one repeatable, read-only transaction inside the pod."""
    require(re.fullmatch(r"cutover-[0-9a-f]{12}-", prefix), "Invalid gate identity.")
    require(
        all(
            re.fullmatch(re.escape(prefix) + r"[a-z-]+", name)
            for name in rejected_names
        ),
        "Invalid rejected resource identity.",
    )
    code = """
import asyncio,json
import asyncpg
from shared.db_url import build_postgres_url

async def inspect():
    conn=await asyncpg.connect(build_postgres_url('POSTGRES',fallback_env='DATABASE_URL'),timeout=15)
    try:
        async with conn.transaction(isolation='repeatable_read',readonly=True):
            prefix,names=PARAMETERS
            result={}
            result['activeOwnedResources']=await conn.fetchval("SELECT count(*) FROM srw_resources WHERE name LIKE $1 AND deleted_at IS NULL",prefix+'%')
            result['ownedRevisions']=await conn.fetchval("SELECT count(*) FROM srw_resource_revisions v JOIN srw_resources r ON r.id=v.resource_id WHERE r.name LIKE $1",prefix+'%')
            result['ownedProjects']=await conn.fetchval("SELECT count(*) FROM projects WHERE name LIKE $1",prefix+'%')
            result['legacyExpertPayloads']=await conn.fetchval("SELECT count(*) FROM experts e JOIN srw_resources r ON r.id=e.manifest_resource_id WHERE r.name LIKE $1 AND (e.config<>'{}'::jsonb OR e.prompts<>'{}'::jsonb)",prefix+'%')
            result['legacyProjectPayloads']=await conn.fetchval("SELECT count(*) FROM projects p JOIN srw_resources r ON r.id=p.manifest_resource_id WHERE r.name LIKE $1 AND (p.default_config_name IS NOT NULL OR p.default_config_override IS NOT NULL)",prefix+'%')
            result['rejectedResources']=await conn.fetchval("SELECT count(*) FROM srw_resources WHERE name=ANY($1::text[])",names)
            result['rejectedOperations']=await conn.fetchval("SELECT count(*) FROM srw_manifest_operations WHERE idempotency_key=$1",prefix+'rejected-apply')
            result['rejectedJobs']=await conn.fetchval("SELECT count(*) FROM jobs WHERE description=$1",prefix+'job')
            result['rejectedExecutions']=await conn.fetchval("SELECT count(*) FROM srw_execution_specs WHERE document->'metadata'->>'name'=$1",prefix+'job')
            result['rejectedAttempts']=await conn.fetchval("SELECT count(*) FROM srw_execution_attempts a JOIN srw_execution_specs s ON s.id=a.execution_id WHERE s.document->'metadata'->>'name'=$1",prefix+'job')
            result['rejectedWorkspaceBindings']=await conn.fetchval("SELECT count(*) FROM srw_execution_workspace_bindings b JOIN srw_execution_specs s ON s.id=b.execution_id WHERE s.document->'metadata'->>'name'=$1",prefix+'job')
            print(json.dumps(result))
    finally:
        await conn.close()

try:
    asyncio.run(inspect())
except Exception:
    raise SystemExit('Read-only cutover database inspection failed.') from None
""".replace("PARAMETERS", repr((prefix, rejected_names)))
    result = remote_json(code)
    require(
        isinstance(result, dict)
        and all(type(value) is int and value >= 0 for value in result.values()),
        "Database inspection returned invalid counts.",
    )
    return result


def native_inventory(namespace):
    result = command(
        ["kubectl", "--context", "k3d-srw", "--namespace", namespace]
        + [
            "get",
            "pods,secrets,pvc,networkpolicies",
            "-o",
            "custom-columns=KIND:.kind,UID:.metadata.uid",
            "--no-headers",
        ]
    )
    # kubectl prints metadata columns only, including for Secret objects.
    return sorted(tuple(line.split()) for line in result.splitlines() if line.strip())


def resource_key(document):
    metadata = document["metadata"]
    scope = metadata["scope"]
    return f"{document['kind']}/{scope['kind']}/{scope['name']}/{metadata['name']}"


def authored_expert(prefix, suffix="expert", *, image="busybox:1.36", private=None):
    return {
        "apiVersion": "srw/v1alpha1",
        "kind": "Expert",
        "metadata": {
            "name": prefix + suffix,
            "scope": {"kind": "Account", "name": "me"},
            "annotations": {OWNER_LABEL: prefix},
            "tags": ["worker"],
        },
        "spec": {
            "runtime": {
                "image": image,
                "config": private
                if private is not None
                else {"literalNull": None, "tools": ["unimplemented-gate-tool"]},
            }
        },
    }


def authored_project(prefix):
    return {
        "apiVersion": "srw/v1alpha1",
        "kind": "Project",
        "metadata": {
            "name": prefix + "project",
            "scope": {"kind": "Account", "name": "me"},
            "annotations": {OWNER_LABEL: prefix},
        },
        "spec": {
            "description": "Disposable manifest cutover verification; no team controller.",
            "resources": {
                "experts": {
                    prefix + alias: {"inline": authored_expert(prefix)["spec"]}
                    for alias in ("worker", "spare")
                }
            },
            "defaults": {
                "expert": prefix + "worker",
                "workspace": None,
                "connectors": [],
            },
        },
    }


def authored_job(prefix):
    return {
        "apiVersion": "srw/v1alpha1",
        "kind": "Job",
        "metadata": {
            "name": prefix + "job",
            "scope": {"kind": "Account", "name": "me"},
            "annotations": {OWNER_LABEL: prefix},
        },
        "spec": {
            "task": {"text": prefix + "job"},
            "execution": {
                "expert": {"ref": {"name": prefix + "rollback"}},
                "workspace": None,
                "connectors": {},
            },
            "completion": {"mode": "ProcessExit"},
            "retry": {"maxAttempts": 1},
            "timeoutSeconds": 10,
        },
    }


class CutoverGate:
    def __init__(self, client, *, prefix, inspect_db, inspect_native):
        self.client, self.prefix = client, prefix
        self.inspect_db, self.inspect_native = inspect_db, inspect_native
        self.headers = {}
        self.cleanup_intents = set()
        self.evidence = {
            "checks": [],
            "observations": {},
            "cleanup": {"complete": False},
        }

    def request(
        self, method, path, *, payload=None, params=None, status=200, authenticated=True
    ):
        try:
            response = self.client.request(
                method,
                API + path,
                json=payload,
                params=params,
                headers=self.headers if authenticated else {},
            )
        except httpx.HTTPError:
            suffix = (
                " Mutation outcome is unknown; no automatic replay was attempted."
                if method in {"POST", "PUT", "DELETE"}
                else ""
            )
            raise GateFailure("An HTTP operation did not complete." + suffix) from None
        require(
            response.status_code == status,
            f"{method}: expected HTTP {status}, received {response.status_code}.",
        )
        try:
            return response.json()
        except ValueError:
            raise GateFailure("An HTTP operation returned invalid JSON.") from None

    def login(self):
        try:
            response = self.client.post(
                AUTH,
                data={
                    "grant_type": "password",
                    "client_id": "admin-cli",
                    "scope": "openid",
                    "username": os.getenv("SRW_K3D_TEST_USER", "test"),
                    "password": os.getenv("SRW_K3D_TEST_PASSWORD", "srw-k3d-dev-test"),
                },
            )
            require(response.status_code == 200, "Local Keycloak login failed.")
            token = response.json().get("id_token")
        except (httpx.HTTPError, ValueError):
            raise GateFailure("Local Keycloak login did not complete.") from None
        require(
            isinstance(token, str) and bool(token),
            "Local Keycloak returned no identity token.",
        )
        require(
            not any(char.isspace() for char in token),
            "Local Keycloak returned an invalid identity token.",
        )
        self.headers = {"Authorization": "Bearer " + token}

    def body(self, documents, **options):
        return {"source": json.dumps(documents), "format": "json", **options}

    def preview(self, documents):
        return self.request(
            "POST",
            "/api/manifests/preview",
            payload=self.body(documents, resolution="stored"),
        )

    def apply(self, documents, *, versions=None, plan=None, key=None, status=200):
        docs = documents if isinstance(documents, list) else [documents]
        for document in docs:
            metadata = document["metadata"]
            require(
                metadata.get("annotations", {}).get(OWNER_LABEL) == self.prefix
                and metadata["name"].startswith(self.prefix),
                "The gate refuses to mutate a definition outside its owned identities.",
            )
            # Register intent before the request, including unknown transport outcomes.
            self.cleanup_intents.add((document["kind"], metadata["name"]))
        return self.request(
            "POST",
            "/api/manifests/apply",
            payload=self.body(
                documents,
                expected_versions=versions or {},
                plan_revision=plan,
                idempotency_key=key,
            ),
            status=status,
        )

    def items(self, *, kind=None, project=None):
        params = {
            "scope_kind": "Project" if project else "Account",
            "scope_name": project or "me",
        }
        if kind:
            params["kind"] = kind
        result = self.request("GET", "/api/resources", params=params)
        require(
            isinstance(result.get("resources"), list), "Resource list shape differs."
        )
        return result["resources"]

    def current(self, item):
        return self.request("GET", "/api/resources/" + item["uid"])

    def catalog_identity(self, item):
        matches = [
            row
            for row in self.request("GET", "/api/experts")
            if row.get("manifest_uid") == item["uid"]
        ]
        require(
            len(matches) == 1,
            "The Expert catalog does not expose one canonical identity.",
        )
        return str(UUID(matches[0]["id"]))

    def check(self, message):
        self.evidence["checks"].append(message)

    def exercise_expert(self):
        document = authored_expert(self.prefix)
        preview = self.preview(document)
        require(
            preview["effects"] == [] and preview["admissionReady"] is False,
            "Stored preview performed admission effects.",
        )
        require(
            not any(
                item["resource"]["metadata"]["name"] == document["metadata"]["name"]
                for item in self.items(kind="Expert")
            ),
            "Preview persisted an Expert.",
        )
        initial = self.apply(
            document, plan=preview["planRevision"], key=self.prefix + "expert-create"
        )
        first = initial["resources"][0]
        require(
            first["resourceVersion"] == 1 and first["changed"],
            "Initial Expert version differs.",
        )
        require(
            first["resource"]["spec"] == document["spec"],
            "Opaque Expert settings changed.",
        )
        replay = self.apply(
            document, plan=preview["planRevision"], key=self.prefix + "expert-create"
        )
        require(
            replay == initial, "Idempotent Expert replay changed the operation result."
        )
        repeated = self.apply(document)["resources"][0]
        require(
            repeated["uid"] == first["uid"]
            and repeated["resourceVersion"] == 1
            and repeated["changed"] is False,
            "Identical apply changed Expert identity or version.",
        )
        require(
            self.current(first)["resource"] == first["resource"],
            "Resource get differs from apply.",
        )
        for output_format in ("json", "yaml"):
            exported = self.request(
                "POST",
                "/api/manifests/export",
                payload=self.body(first["resource"], output_format=output_format),
            )
            require(
                parse_documents(exported["source"], format=output_format)
                == [first["resource"]],
                "Stored Expert export changed its authored manifest.",
            )
        self.check(
            "Expert stored preview, list/get, JSON/YAML export, identical apply and idempotency replay"
        )

        changed = deepcopy(document)
        changed["spec"]["runtime"]["config"]["generation"] = 2
        self.apply(changed, key=self.prefix + "expert-create", status=409)
        self.apply(changed, status=409)
        updated = self.apply(changed, versions={resource_key(first["resource"]): 1})[
            "resources"
        ][0]
        require(
            updated["uid"] == first["uid"] and updated["resourceVersion"] == 2,
            "Expert CAS update did not preserve identity and increment once.",
        )
        self.apply(document, versions={resource_key(first["resource"]): 1}, status=409)
        self.request(
            "DELETE",
            "/api/resources/" + first["uid"],
            params={"expected_version": 1},
            status=409,
        )
        require(
            self.current(updated)["revision"] == updated["revision"],
            "A rejected Expert write changed the resource.",
        )
        catalog_id = self.catalog_identity(updated)
        detail = self.request("GET", "/api/experts/" + catalog_id)
        require(
            detail["manifest"] == updated["resource"] and detail["config"] == {},
            "Generic private configuration entered the SRW editor projection.",
        )
        self.request(
            "PUT",
            "/api/experts/" + catalog_id,
            payload={"description": "must not rewrite a generic harness"},
            status=409,
        )
        self.check(
            "Expert version/CAS conflicts and generic editor boundary preserve canonical content"
        )
        return updated

    def exercise_editor(self):
        document = authored_expert(
            self.prefix,
            "editor",
            image="srw-agent:latest",
            private={
                "config_name": "worker_base",
                "config": {"llm": {"temperature": 0.17}},
                "prompts": {
                    "instructions": "Disposable cutover instructions, first revision."
                },
            },
        )
        document["spec"]["runtime"]["adapter"] = "srw/v1"
        saved = self.apply(document)["resources"][0]
        identity = self.catalog_identity(saved)
        detail = self.request("GET", "/api/experts/" + identity)
        require(
            detail["manifest"] == saved["resource"]
            and detail["config"]["llm"]["temperature"] == 0.17
            and detail["instructions"]
            == document["spec"]["runtime"]["config"]["prompts"]["instructions"],
            "The existing Expert editor did not project canonical SRW settings.",
        )
        next_instructions = "Disposable cutover instructions, editor revision."
        self.request(
            "PUT",
            "/api/experts/" + identity,
            payload={"prompts": {"instructions": next_instructions}},
        )
        current = self.current(saved)
        require(
            current["resourceVersion"] == saved["resourceVersion"] + 1
            and current["resource"]["spec"]["runtime"]["config"]["prompts"][
                "instructions"
            ]
            == next_instructions,
            "Existing editor write did not update the same canonical Expert.",
        )
        bundle = self.request("GET", "/api/experts/" + identity + "/export")
        require(
            bundle["prompts"]["instructions"] == next_instructions,
            "Existing editor export did not project current canonical prompts.",
        )
        require(
            self.inspect_db()["legacyExpertPayloads"] == 0,
            "Legacy Expert config/prompt payload columns remain populated.",
        )
        self.check(
            "Existing SRW Expert editor reads/writes/exports the same canonical resource; legacy payload columns are empty"
        )

    def exercise_project(self):
        document = authored_project(self.prefix)
        initial = self.apply(document, key=self.prefix + "project-create")
        parent = next(
            item
            for item in initial["resources"]
            if item["resource"]["kind"] == "Project"
        )
        children = [
            item
            for item in initial["resources"]
            if item["resource"]["kind"] == "Expert"
        ]
        require(
            len(children) == 2 and parent["activeRevision"] == parent["revision"],
            "Project did not activate one complete composition.",
        )
        project_id = str(UUID(children[0]["resource"]["metadata"]["scope"]["name"]))
        require(
            {item["uid"] for item in self.items(project=project_id)}
            == {item["uid"] for item in children},
            "Project resource list does not contain its managed children.",
        )
        project_rows = [
            row
            for row in self.request("GET", "/api/projects")
            if row.get("manifest_uid") == parent["uid"]
        ]
        require(
            len(project_rows) == 1 and project_rows[0]["id"] == project_id,
            "Project list did not expose its canonical composition.",
        )
        # The legacy Project detail GET schedules background cloud provisioning.
        # Use its list projection plus canonical get to keep this gate's effects
        # confined to desired state and inactive domain identities.
        require(
            project_rows[0]["manifest"] == parent["resource"],
            "Project list projection differs from its resource manifest.",
        )

        before = {item["uid"]: self.current(item) for item in initial["resources"]}
        before_db = self.inspect_db()
        invalid = deepcopy(document)
        invalid["spec"]["defaults"]["sessionExpert"] = self.prefix + "worker"
        invalid["spec"]["resources"]["experts"][self.prefix + "worker"]["inline"][
            "runtime"
        ]["config"]["generation"] = "must-roll-back"
        # Valid manifest shape, invalid domain default-slot combination. The
        # identity synchronization fails after candidate resource writes.
        validate_documents([invalid])
        self.apply(
            invalid,
            versions={resource_key(parent["resource"]): parent["resourceVersion"]},
            status=422,
        )
        require(
            {item["uid"]: self.current(item) for item in initial["resources"]}
            == before,
            "A failed Project activation changed a parent or child revision.",
        )
        require(
            self.inspect_db()["ownedRevisions"] == before_db["ownedRevisions"],
            "A failed Project activation left immutable revisions behind.",
        )

        updated_document = deepcopy(document)
        updated_document["spec"]["resources"]["experts"].pop(self.prefix + "spare")
        updated_document["spec"]["resources"]["experts"][self.prefix + "worker"][
            "inline"
        ]["runtime"]["config"]["generation"] = 2
        self.apply(updated_document, status=409)
        updated = self.apply(
            updated_document,
            versions={resource_key(parent["resource"]): parent["resourceVersion"]},
        )
        next_parent = next(
            item
            for item in updated["resources"]
            if item["resource"]["kind"] == "Project"
        )
        require(
            next_parent["resourceVersion"] == 2
            and next_parent["activeRevision"] == next_parent["revision"],
            "Project upgrade did not activate exactly one next generation.",
        )
        current_children = self.items(project=project_id)
        require(
            len(current_children) == 1 and current_children[0]["resourceVersion"] == 2,
            "Project upgrade did not update and retire managed children together.",
        )
        removed = next(
            item
            for item in children
            if item["resource"]["metadata"]["name"].endswith("spare")
        )
        self.request("GET", "/api/resources/" + removed["uid"], status=404)
        self.apply(document, versions={resource_key(parent["resource"]): 1}, status=409)
        default_job = authored_job(self.prefix)
        default_job["metadata"]["scope"] = {"kind": "Project", "name": project_id}
        default_job["spec"]["execution"] = {}
        resolved = self.preview(default_job)["resolved"][0]["spec"]["execution"]
        require(
            resolved["expert"]["inline"]["runtime"]["config"]["generation"] == 2
            and resolved["workspace"] is None
            and resolved["connectors"] == {},
            "Stored default resolution did not use the active Project generation.",
        )
        require(
            self.inspect_db()["legacyProjectPayloads"] == 0,
            "Legacy Project default payload columns remain populated.",
        )
        self.check(
            "Project list projection, atomic failed activation, versioned upgrade, managed-child retirement and active defaults"
        )

        self.request(
            "DELETE",
            "/api/resources/" + parent["uid"],
            params={"expected_version": 1},
            status=409,
        )
        self.request(
            "DELETE",
            "/api/resources/" + parent["uid"],
            params={"expected_version": next_parent["resourceVersion"]},
        )
        for item in [next_parent, *current_children]:
            self.request("GET", "/api/resources/" + item["uid"], status=404)
        self.request("GET", "/api/projects/" + project_id, status=404)
        require(
            not any(
                row.get("manifest_uid") == parent["uid"]
                for row in self.request("GET", "/api/projects")
            ),
            "Retired Project remains in the domain list.",
        )
        self.check(
            "Exact-version Project deletion retires its managed definitions and domain identity"
        )

    def exercise_disabled_hosting(self):
        rollback = authored_expert(self.prefix, "rollback")
        # Harmless even if a regression unexpectedly admits it; no external I/O.
        rollback["spec"]["runtime"]["command"] = ["/bin/true"]
        job = authored_job(self.prefix)
        before_db, before_kube = self.inspect_db(), self.inspect_native()
        require(
            all(
                value == 0
                for key, value in before_db.items()
                if key.startswith("rejected")
            ),
            "Rejected-Job probe identities already exist.",
        )
        result = self.apply(
            [rollback, job], key=self.prefix + "rejected-apply", status=503
        )
        require(
            result.get("detail", {}).get("code") == "HostingCapabilityUnavailable",
            "Generic Job rejection did not identify the unverified hosting capability.",
        )
        after_db, after_kube = self.inspect_db(), self.inspect_native()
        require(
            all(
                value == 0
                for key, value in after_db.items()
                if key.startswith("rejected")
            ),
            "Rejected generic apply persisted resource, operation, Job, execution, attempt or workspace records.",
        )
        require(
            after_db["ownedRevisions"] == before_db["ownedRevisions"],
            "Rejected generic apply left resource revisions behind.",
        )
        require(
            before_kube == after_kube,
            "Native Kubernetes objects changed across rejected admission.",
        )
        require(
            not any(
                item["resource"]["metadata"]["name"]
                in {job["metadata"]["name"], rollback["metadata"]["name"]}
                for item in self.items()
            ),
            "Rejected generic bundle is visible in the resource list.",
        )
        self.evidence["observations"]["rejectedAdmission"] = {
            key: value for key, value in after_db.items() if key.startswith("rejected")
        }
        self.evidence["observations"]["nativeObjectInventoryUnchanged"] = True
        self.check(
            "Unverified hosting returns HostingCapabilityUnavailable 503 and rolls the entire generic Job bundle back without execution effects"
        )

    def cleanup(self):
        """Discover uncertain creates; delete only exact owned names and annotations."""
        if not self.cleanup_intents:
            self.evidence["cleanup"] = {"complete": True, "activeOwnedResources": 0}
            return
        failures = 0
        try:
            candidates = [
                item
                for item in self.items()
                if (item["resource"]["kind"], item["resource"]["metadata"]["name"])
                in self.cleanup_intents
            ]
            for item in sorted(
                candidates,
                key=lambda row: {"Job": 0, "Project": 1}.get(
                    row["resource"]["kind"], 2
                ),
            ):
                if (
                    item["resource"]["metadata"].get("annotations", {}).get(OWNER_LABEL)
                    != self.prefix
                ):
                    failures += 1
                    continue
                try:
                    current = self.current(item)
                    self.request(
                        "DELETE",
                        "/api/resources/" + item["uid"],
                        params={"expected_version": current["resourceVersion"]},
                    )
                    self.request("GET", "/api/resources/" + item["uid"], status=404)
                except GateFailure:
                    failures += 1
            counts = self.inspect_db()
            self.evidence["cleanup"] = {
                "complete": failures == 0
                and counts["activeOwnedResources"] == 0
                and counts["ownedProjects"] == 0,
                "activeOwnedResources": counts["activeOwnedResources"],
                "ownedProjects": counts["ownedProjects"],
                "failedOperations": failures,
                "historicalRevisionsRetained": True,
            }
        except Exception:
            self.evidence["cleanup"] = {"complete": False, "inspectionFailed": True}

    def run(self):
        self.login()
        try:
            self.request("GET", "/api/resources", authenticated=False, status=401)
            anonymous_probe = authored_expert(self.prefix)
            self.cleanup_intents.add(("Expert", anonymous_probe["metadata"]["name"]))
            self.request(
                "POST",
                "/api/manifests/apply",
                payload=self.body(anonymous_probe),
                authenticated=False,
                status=401,
            )
            self.check(
                "Real Keycloak authentication; resource list and apply reject anonymous callers"
            )
            self.exercise_expert()
            self.exercise_editor()
            self.exercise_project()
            self.exercise_disabled_hosting()
        finally:
            self.cleanup()
        require(
            self.evidence["cleanup"]["complete"],
            "Disposable cutover resources were not fully retired; inspect this gate identity.",
        )
        self.check(
            "All disposable Account and Project resources retired; immutable audit history retained"
        )


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args(argv)
    prefix = "cutover-" + uuid4().hex[:12] + "-"
    evidence = {
        "gate": "manifest-http-cutover",
        "context": "k3d-srw",
        "gateIdentity": prefix,
        "startedAt": datetime.now(timezone.utc).isoformat(),
        "status": "failed",
        "scope": "Deployed HTTP/auth/desired-state/PostgreSQL cutover; no admitted job or session execution",
    }
    gate = None
    try:
        identity = deployed_identity()
        evidence["deployment"] = identity
        with httpx.Client(
            verify=ssl.create_default_context(),
            trust_env=False,
            follow_redirects=False,
            timeout=30,
        ) as client:
            gate = CutoverGate(
                client,
                prefix=prefix,
                inspect_db=lambda: database_evidence(
                    prefix, [prefix + "rollback", prefix + "job"]
                ),
                inspect_native=lambda: native_inventory(identity["nativeNamespace"]),
            )
            gate.run()
        evidence["status"] = "passed"
    except GateFailure as exc:
        evidence["failure"] = str(exc)
    except Exception as exc:
        evidence["failure"] = (
            "Unexpected gate failure ("
            + type(exc).__name__
            + "); response details suppressed."
        )
    if gate is not None:
        evidence.update(gate.evidence)
    evidence["finishedAt"] = datetime.now(timezone.utc).isoformat()
    print(json.dumps(evidence, indent=2))
    return 0 if evidence["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
