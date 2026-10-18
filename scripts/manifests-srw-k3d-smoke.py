#!/usr/bin/env python3
"""Run an owned deterministic SRW adapter Job/Session smoke on local k3d-srw.

Run only after a coherent orchestrator and agent rollout. This is a live test:
it creates a temporary Keycloak administrator, a fixture namespace and model,
and test-owned work. It never edits an existing user or a default model. Its
normal cleanup uses public lifecycle APIs, then revokes the exact identity.
All emitted evidence is allowlisted; credentials and response bodies stay in
memory. The fixture image remains in the local registry as a reusable cache.
"""

from __future__ import annotations

import argparse
import base64
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import importlib.util
import inspect
import json
import os
from pathlib import Path
import secrets
import socket
import ssl
import subprocess
import sys
import threading
import time
from uuid import UUID, uuid4
from urllib.parse import urlparse

import httpx
from kubernetes import client as kube, config as kube_config
from kubernetes.client.exceptions import ApiException
import yaml


ROOT = Path(__file__).resolve().parents[1]
_spec = importlib.util.spec_from_file_location(
    "manifest_cutover_gate", ROOT / "scripts/manifests-cutover-k3d-gate.py"
)
cutover = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(cutover)
GateFailure, require = cutover.GateFailure, cutover.require
OWNER_LABEL = "srw.io/adapter-smoke"
CAPABILITIES = ("chat", "auxiliary", "embedding", "vision", "tts", "search", "fetch")


def progress(message):
    print(message, file=sys.stderr, flush=True)


def wait_for(check, message, *, timeout=300, interval=2):
    deadline, next_update = time.monotonic() + timeout, time.monotonic() + 30
    while time.monotonic() < deadline:
        result = check()
        if result:
            return result
        if time.monotonic() >= next_update:
            progress(message)
            next_update = time.monotonic() + 30
        time.sleep(interval)
    raise GateFailure(message + " Timed out.")


def request(
    client,
    method,
    url,
    *,
    headers=None,
    payload=None,
    data=None,
    params=None,
    status=200,
    capture_location=False,
):
    try:
        response = client.request(
            method, url, headers=headers, json=payload, data=data, params=params
        )
    except httpx.HTTPError:
        raise GateFailure(
            "An authenticated smoke request did not complete; mutations are not replayed."
        ) from None
    allowed = {status} if isinstance(status, int) else set(status)
    require(
        response.status_code in allowed,
        f"{method}: unexpected HTTP {response.status_code}.",
    )
    if capture_location:
        return response.headers.get("Location")
    if response.status_code in {204, 404} or not response.content:
        return None
    try:
        return response.json()
    except ValueError:
        raise GateFailure("An authenticated smoke response was invalid JSON.") from None


def cleanup_owned_workload(
    client,
    *,
    kind,
    work_id,
    owner_id,
    prefix,
    headers,
    evidence,
    timeout=120,
    interval=2,
):
    """Retry explicit retirement-pending responses after exact owner readback.

    An SSE reply can precede claim drain. A received 503 is a known retryable
    result; transport ambiguity and every other HTTP failure stay non-replayed.
    """
    work_id, owner_id = str(UUID(work_id)), str(UUID(owner_id))
    require(kind in {"Job", "Session"}, "Unknown cleanup workload kind.")
    path, field, suffix = (
        ("/api/jobs/", "description", "job")
        if kind == "Job"
        else ("/api/persistent/threads/", "title", "session")
    )
    url = cutover.API + path + work_id
    params = {"permanent": "true", "force": "true"} if kind == "Session" else None
    item = {"kind": kind, "workID": work_id, "statuses": [], "absent": False}
    evidence.append(item)
    deadline = time.monotonic() + timeout

    def call(method, *, params=None):
        try:
            response = client.request(method, url, headers=headers, params=params)
        except httpx.HTTPError:
            item["transportAmbiguous"] = True
            raise GateFailure(
                "Owned cleanup transport failed; mutation is not replayed."
            ) from None
        item["statuses"].append({"method": method, "status": response.status_code})
        return response

    def read_exact():
        response = call("GET")
        if response.status_code == 404:
            item["absent"] = True
            return False
        require(response.status_code == 200, "Owned cleanup readback failed.")
        try:
            row = response.json()
        except ValueError:
            raise GateFailure("Owned cleanup readback returned invalid JSON.") from None
        require(
            isinstance(row, dict)
            and row.get("id") == work_id
            and row.get("user_id") == owner_id
            and row.get(field) == "E2E-" + prefix + suffix,
            "Owned cleanup UUID, owner or exact title changed; deletion refused.",
        )
        return True

    while read_exact():
        response = call("DELETE", params=params)
        if response.status_code in {200, 404}:
            require(not read_exact(), "Acknowledged owned deletion is still present.")
            return
        require(
            response.status_code == 503,
            f"Owned {kind} cleanup failed with HTTP {response.status_code}; not replayed.",
        )
        if time.monotonic() >= deadline:
            raise GateFailure("Owned cleanup remained HTTP 503 until its deadline.")
        time.sleep(min(interval, max(0, deadline - time.monotonic())))


def login(client, username, password, *, master=False, oauth_client="admin-cli"):
    token = request(
        client,
        "POST",
        "https://auth.localhost/realms/"
        + ("master" if master else "srw")
        + "/protocol/openid-connect/token",
        data={
            "grant_type": "password",
            "client_id": "admin-cli" if master else oauth_client,
            "scope": "openid",
            "username": username,
            "password": password,
        },
    ).get("access_token")
    require(
        isinstance(token, str) and token and not any(c.isspace() for c in token),
        "Local Keycloak returned no valid token.",
    )
    return {"Authorization": "Bearer " + token}


def expires_soon(headers):
    """Use the token's untrusted expiry only to renew; the server verifies auth."""
    try:
        payload = headers["Authorization"].split(".")[1]
        claims = json.loads(
            base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4))
        )
        return float(claims["exp"]) < time.time() + 90
    except (ValueError, KeyError, IndexError, TypeError):
        return True


class TemporaryAdmin:
    """Exact run-owned Keycloak identity; bootstrap authority never reaches SRW."""

    def __init__(self, client, name):
        self.client, self.name = client, name
        self.uid = self.app_uid = None
        self.oauth_uid = None
        self.oauth_client = name + "-oauth"
        self.oauth_intent = False
        self.project_receipt = None
        self.headers = self.bootstrap = {}
        self.password = self.bootstrap_user = self.bootstrap_password = None
        self.created_intent = False
        self.cleanup_evidence = {
            "identityRevoked": False,
            "identityDeleted": False,
            "applicationUserDeleted": False,
            "oauthClientDeleted": False,
        }

    def keycloak(self, method, suffix, **kwargs):
        if expires_soon(self.bootstrap) and self.bootstrap_user:
            self.bootstrap = login(
                self.client, self.bootstrap_user, self.bootstrap_password, master=True
            )
        return request(
            self.client,
            method,
            "https://auth.localhost/admin/realms/srw" + suffix,
            headers=self.bootstrap,
            **kwargs,
        )

    def create(self):
        public = yaml.safe_load(
            (ROOT / "deployment/values-local.yaml.example").read_text()
        )
        values = public["secrets"]["values"]
        self.bootstrap_user = os.getenv(
            "SRW_K3D_BOOTSTRAP_USER", values["KC_ADMIN_USER"]
        )
        self.bootstrap_password = os.getenv(
            "SRW_K3D_BOOTSTRAP_PASSWORD", values["KC_ADMIN_PASSWORD"]
        )
        self.bootstrap = login(
            self.client, self.bootstrap_user, self.bootstrap_password, master=True
        )
        self.create_oauth_client()
        require(
            not self.keycloak(
                "GET", "/users", params={"username": self.name, "exact": "true"}
            ),
            "The temporary identity already exists; refusing adoption.",
        )
        password = self.password = secrets.token_urlsafe(40)
        self.created_intent = True
        location = self.keycloak(
            "POST",
            "/users",
            status=201,
            capture_location=True,
            payload={
                "username": self.name,
                "enabled": True,
                "firstName": "Manifest",
                "lastName": "Smoke",
                "email": self.name + "@example.invalid",
                "emailVerified": True,
                "attributes": {OWNER_LABEL: [self.name]},
                "credentials": [
                    {"type": "password", "value": password, "temporary": False}
                ],
            },
        )
        path = urlparse(location or "").path
        require(
            path.startswith("/admin/realms/srw/users/"),
            "Keycloak returned no immutable identity creation receipt.",
        )
        self.uid = str(UUID(path.rsplit("/", 1)[1]))
        self.recover_identity()
        role = self.keycloak("GET", "/roles/admin")
        require(
            role.get("name") == "admin",
            "The local administrator realm role is unavailable.",
        )
        self.keycloak(
            "POST", f"/users/{self.uid}/role-mappings/realm", payload=[role], status=204
        )
        self.headers = login(
            self.client, self.name, password, oauth_client=self.oauth_client
        )
        me = request(
            self.client, "GET", cutover.API + "/api/auth/me", headers=self.headers
        )["user"]
        self.app_uid = str(UUID(me["id"]))
        require(
            me.get("is_admin") is True and me.get("is_approved") is True,
            "Temporary fixture identity lacks the normal administrator role.",
        )

    def recover_identity(self):
        if not self.created_intent:
            return
        matches = self.keycloak(
            "GET", "/users", params={"username": self.name, "exact": "true"}
        )
        require(len(matches) <= 1, "Temporary identity lookup was ambiguous.")
        if not matches:
            return
        user = matches[0]
        marker = user.get("attributes", {}).get(OWNER_LABEL)
        if self.uid is None and marker is None and self.password:
            # A lost POST response may leave no Location receipt. Prove the
            # exact new account using its in-memory random password, without
            # making an SRW login or provisioning an application profile.
            headers = login(
                self.client, self.name, self.password, oauth_client=self.oauth_client
            )
            payload = headers["Authorization"].split(".")[1]
            claims = json.loads(
                base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4))
            )
            self.uid = str(UUID(claims["sub"]))
        require(
            user.get("username") == self.name
            and marker in (None, [self.name])
            and (
                (
                    self.uid == user.get("id")
                    and user.get("email") == self.name + "@example.invalid"
                )
                or (self.uid is None and marker == [self.name])
            ),
            "Refusing cleanup of a Keycloak identity without its ownership marker.",
        )
        self.uid = str(UUID(user["id"]))

    def delete_app_user(self):
        if self.uid and not self.app_uid:
            self.app_uid = application_identity(self.uid)
        if self.app_uid:
            self.project_receipt = default_project_receipt(self.app_uid)
            self.refresh()
            request(
                self.client,
                "DELETE",
                cutover.API + "/api/users/" + self.app_uid,
                headers=self.headers,
                status=(200, 404),
            )
            self.cleanup_evidence["applicationUserDeleted"] = True
            require(
                application_identity(self.uid) is None,
                "The temporary application user still exists after deletion.",
            )
            if self.project_receipt:
                result = default_project_receipt(self.app_uid, self.project_receipt)
                require(
                    result.get("removed") is True,
                    "The owned default Project cleanup did not finish.",
                )
            self.cleanup_evidence["defaultProjectRemoved"] = True

    def refresh(self):
        if self.headers and expires_soon(self.headers):
            self.headers = login(
                self.client, self.name, self.password, oauth_client=self.oauth_client
            )

    def create_oauth_client(self):
        require(
            not self.keycloak(
                "GET", "/clients", params={"clientId": self.oauth_client}
            ),
            "The temporary OAuth client already exists; refusing adoption.",
        )
        self.oauth_intent = True
        location = self.keycloak(
            "POST",
            "/clients",
            status=201,
            capture_location=True,
            payload={
                "clientId": self.oauth_client,
                "name": self.oauth_client,
                "enabled": True,
                "protocol": "openid-connect",
                "publicClient": True,
                "standardFlowEnabled": False,
                "directAccessGrantsEnabled": True,
                "serviceAccountsEnabled": False,
                "fullScopeAllowed": True,
                "defaultClientScopes": ["profile", "email", "roles"],
                "optionalClientScopes": [],
                "attributes": {OWNER_LABEL: self.name},
                # The gate clients' mappers (scripts/k3d_gate_oauth.py): `sub`
                # (the imported local realm has no basic scope) and the
                # audience the API's Bearer check accepts.
                "protocolMappers": list(cutover.gate_oauth.GATE_CLIENT_MAPPERS),
            },
        )
        path = urlparse(location or "").path
        require(
            path.startswith("/admin/realms/srw/clients/"),
            "Keycloak returned no immutable OAuth client receipt.",
        )
        self.oauth_uid = str(UUID(path.rsplit("/", 1)[1]))

    def remove_oauth_client(self):
        if self.oauth_intent and not self.oauth_uid:
            matches = self.keycloak(
                "GET", "/clients", params={"clientId": self.oauth_client}
            )
            require(len(matches) <= 1, "Temporary OAuth client lookup was ambiguous.")
            if matches:
                row = matches[0]
                require(
                    row.get("clientId") == self.oauth_client
                    and row.get("attributes", {}).get(OWNER_LABEL) == self.name,
                    "The uncertain OAuth client has a different owner.",
                )
                self.oauth_uid = str(UUID(row["id"]))
        if self.oauth_uid:
            self.keycloak("DELETE", "/clients/" + self.oauth_uid, status=(204, 404))
            self.cleanup_evidence["oauthClientDeleted"] = True

    def revoke(self):
        # This finally block also runs after an unknown create/app-login result.
        try:
            if self.uid is None:
                self.recover_identity()
            if self.uid:
                try:
                    self.keycloak(
                        "PUT",
                        "/users/" + self.uid,
                        payload={"enabled": False},
                        status=204,
                    )
                    self.keycloak("POST", f"/users/{self.uid}/logout", status=204)
                    self.cleanup_evidence["identityRevoked"] = True
                finally:
                    self.keycloak("DELETE", "/users/" + self.uid, status=(204, 404))
                    self.cleanup_evidence["identityDeleted"] = True
        finally:
            self.remove_oauth_client()
            self.headers = {}
            self.password = self.bootstrap_password = None


def application_identity(keycloak_uid):
    keycloak_uid = str(UUID(keycloak_uid))
    return cutover.remote_json(
        """
import asyncio,json,asyncpg
from shared.db_url import build_postgres_url
async def inspect():
    conn=await asyncpg.connect(build_postgres_url('POSTGRES',fallback_env='DATABASE_URL'),server_settings={'default_transaction_read_only':'on'})
    try:
        value=await conn.fetchval('SELECT id::text FROM users WHERE keycloak_sub=$1',IDENTITY)
        print(json.dumps(value))
    finally:
        await conn.close()
try:
    asyncio.run(inspect())
except Exception:
    raise SystemExit('Read-only fixture identity inspection failed.') from None
""".replace("IDENTITY", repr(keycloak_uid))
    )


async def owned_default_project(db, user_id, receipt=None):
    """Test-only exact cleanup; the public default-Project guard stays intact."""
    user = UUID(user_id)
    async with db.transaction_scope() as conn:
        await conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended('srw-resource-catalog',0))"
        )
        account = await conn.fetchrow(
            "SELECT default_project_id FROM users WHERE id=$1 FOR UPDATE", user
        )
        if receipt:
            if account is not None or receipt.get("ownerID") != user_id:
                raise RuntimeError(
                    "Default Project cleanup requires the captured, removed fixture account."
                )
            project_id = UUID(receipt["projectID"])
        else:
            if not account or not account["default_project_id"]:
                return None
            project_id = account["default_project_id"]
        row = await conn.fetchrow(
            "SELECT id,is_default,created_at,manifest_resource_id FROM projects WHERE id=$1 FOR UPDATE",
            project_id,
        )
        if row is None:
            if receipt:
                return {"removed": True}
            raise RuntimeError("Fixture default Project is missing.")
        current = {
            "projectID": str(project_id),
            "ownerID": user_id,
            "createdAt": row["created_at"].isoformat(),
            "manifestID": str(row["manifest_resource_id"]),
        }
        if not row["is_default"] or (receipt and current != receipt):
            raise RuntimeError("Default Project identity changed.")
        members = await conn.fetch(
            "SELECT user_id,role FROM project_members WHERE project_id=$1 FOR UPDATE",
            project_id,
        )
        if receipt:
            if members:
                raise RuntimeError("The orphaned default Project acquired a member.")
        elif (
            len(members) != 1
            or members[0]["user_id"] != user
            or members[0]["role"] != "owner"
        ):
            raise RuntimeError(
                "The fixture does not exclusively own its default Project."
            )
        resource = await conn.fetchrow(
            "SELECT id,owner_id FROM srw_resources WHERE id=$1 FOR UPDATE",
            row["manifest_resource_id"],
        )
        if resource is None or resource["owner_id"] != (None if receipt else user):
            raise RuntimeError(
                "The default Project's canonical resource authority changed."
            )
        checks = (
            (
                "SELECT EXISTS(SELECT 1 FROM users WHERE default_project_id=$1 AND id<>$2)",
                (project_id, user),
            ),
            (
                "SELECT EXISTS(SELECT 1 FROM jobs WHERE project_id=$1 OR user_id=$2)",
                (project_id, user),
            ),
            ("SELECT EXISTS(SELECT 1 FROM threads WHERE user_id=$1)", (user,)),
            (
                "SELECT EXISTS(SELECT 1 FROM thread_mounts WHERE source_kind='project_folder' AND source_ref=$1)",
                (project_id,),
            ),
            (
                "SELECT EXISTS(SELECT 1 FROM project_repositories WHERE project_id=$1)",
                (project_id,),
            ),
            (
                "SELECT EXISTS(SELECT 1 FROM project_officers WHERE project_id=$1 AND thread_id IS NOT NULL)",
                (project_id,),
            ),
            (
                "SELECT EXISTS(SELECT 1 FROM srw_resources r LEFT JOIN datasources d ON r.kind='Connector' AND r.linked_id=d.id WHERE r.project_id=$1 AND r.id<>$2 AND r.deleted_at IS NULL AND NOT(COALESCE(d.type='kb' AND d.config->>'native_project_id'=$3,FALSE)))",
                (project_id, row["manifest_resource_id"], str(project_id)),
            ),
            (
                "SELECT EXISTS(SELECT 1 FROM project_datasources p JOIN datasources d ON d.id=p.datasource_id WHERE p.project_id=$1 AND (d.type<>'kb' OR d.config->>'native_project_id' IS DISTINCT FROM $2 OR EXISTS(SELECT 1 FROM project_datasources p2 WHERE p2.datasource_id=d.id AND p2.project_id<>$1)))",
                (project_id, str(project_id)),
            ),
        )
        for query, values in checks:
            if await conn.fetchval(query, *values):
                raise RuntimeError(
                    "The temporary default Project contains outside membership, work or resources."
                )
        if receipt:
            if not await db.delete_project(str(project_id)):
                raise RuntimeError("Owned default Project deletion failed.")
            return {"removed": True}
        return current


def default_project_receipt(user_id, receipt=None):
    user_id = str(UUID(user_id))
    code = "import asyncio,json\nfrom uuid import UUID\nfrom orchestrator.database.postgres import PostgresDB\n"
    code += inspect.getsource(owned_default_project)
    code += """
async def run():
    db=PostgresDB(min_connections=1,max_connections=1)
    await db.connect()
    try:
        print(json.dumps(await owned_default_project(db,USER,RECEIPT)))
    finally:
        await db.close()
try:
    asyncio.run(run())
except Exception:
    raise SystemExit('Exact default Project fixture cleanup failed.') from None
""".replace("USER", repr(user_id)).replace("RECEIPT", repr(receipt))
    return cutover.remote_json(code)


def readonly_evidence(*, kind=None, work_id=None, prefix=None):
    if work_id is not None:
        work_id = str(UUID(work_id))
        require(kind in {"Job", "Session"}, "Invalid snapshot work kind.")
    code = """
import asyncio,hashlib,json
from uuid import UUID
from orchestrator.database.postgres import PostgresDB
from orchestrator.services.manifest_store import ManifestStore
from orchestrator.services.manifest_execution_snapshot import srw_snapshot_config

async def inspect():
    kind,work_id,prefix=PARAMETERS
    db=PostgresDB(min_connections=1,max_connections=1,server_settings={'default_transaction_read_only':'on'})
    await db.connect()
    try:
        if work_id:
            snapshot=await ManifestStore(db).execution(kind,work_id)
            if not snapshot:
                print('null'); return
            blob,policy=srw_snapshot_config(snapshot)
            agent=blob['agent']; runtime=snapshot['resolved']['spec']['execution']['expert']['inline']['runtime']
            source={key:blob.get(key) for key in ('prompts','instructions','skills')}
            result={'id':str(snapshot['id']),'generation':snapshot['generation'],'revision':snapshot['revision'],
              'adapter':snapshot['harness_adapter'],'image':runtime['image'],'model':agent.get('llm',{}).get('model'),
              'temperature':agent.get('llm',{}).get('temperature'),
              'workspaceBackend':agent.get('workspace',{}).get('backend'),
              'workspace':snapshot['document']['spec']['execution'].get('workspace'),
              'resolvedWorkspace':snapshot['resolved']['spec']['execution'].get('workspace'),
              'sourceDigest':hashlib.sha256(json.dumps(source,sort_keys=True).encode()).hexdigest()}
            if kind=='Job':
                result['job']=dict(await db.fetchrow("SELECT execution_lane,status,COALESCE(context ? 'workspace_container',FALSE) AS has_workspace_container FROM jobs WHERE id=$1",UUID(work_id)))
            if kind=='Session':
                row=await db.fetchrow("SELECT execution_lane,status,runtime_generation::text AS generation,total_turns FROM threads WHERE id=$1",UUID(work_id))
                result['thread']=dict(row) if row else None
            print(json.dumps(result)); return
        result={'effective':{},'fallback':{}}
        for capability in CAPABILITIES:
            result['effective'][capability]=await db.resolve_default_for_capability(capability)
            rows=await db.list_models_by_capability_alphabetical(capability)
            result['fallback'][capability]=rows[0]['display_label'] if rows else None
        if prefix:
            result['jobs']=[str(r['id']) for r in await db.fetch("SELECT id FROM jobs WHERE description=$1",'E2E-'+prefix+'job')]
            result['threads']=[str(r['id']) for r in await db.fetch("SELECT id FROM threads WHERE title=$1",'E2E-'+prefix+'session')]
        print(json.dumps(result))
    finally:
        await db.close()
try:
    asyncio.run(inspect())
except Exception:
    raise SystemExit('Read-only smoke evidence inspection failed.') from None
""".replace("PARAMETERS", repr((kind, work_id, prefix))).replace(
        "CAPABILITIES", repr(CAPABILITIES)
    )
    return cutover.remote_json(code)


def publish_fixture():
    progress("Building the owned deterministic provider image.")
    fixture = ROOT / "tests/e2e/app/deterministic_provider"
    tag = (
        "localhost:5005/srw-manifest-fixture:"
        + hashlib.sha256(
            b"".join(path.read_bytes() for path in sorted(fixture.glob("*.py")))
            + (fixture / "Dockerfile").read_bytes()
            + (fixture / "requirements.txt").read_bytes()
        ).hexdigest()[:20]
    )
    # Build/push output is captured, since future Dockerfiles may emit config.
    for command in (
        ["docker", "build", "--tag", tag, str(fixture)],
        ["docker", "push", tag],
    ):
        process = subprocess.Popen(
            command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        try:
            wait_for(
                lambda: process.poll() is not None,
                "Waiting for the fixture image publication.",
                timeout=600,
            )
            require(
                process.returncode == 0, "Fixture image build or publication failed."
            )
        finally:
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=10)
    accept = {
        "Accept": ",".join(
            (
                "application/vnd.oci.image.index.v1+json",
                "application/vnd.oci.image.manifest.v1+json",
                "application/vnd.docker.distribution.manifest.v2+json",
            )
        )
    }
    with httpx.Client(trust_env=False, timeout=15) as client:
        url = "http://localhost:5005/v2/srw-manifest-fixture/manifests/"
        response = client.get(url + tag.rsplit(":", 1)[1], headers=accept)
        require(
            response.status_code == 200, "Published fixture manifest is unavailable."
        )
        digest = "sha256:" + hashlib.sha256(response.content).hexdigest()
        require(
            response.headers.get("Docker-Content-Digest") == digest
            and client.get(url + digest, headers=accept).status_code == 200,
            "The fixture registry digest could not be verified.",
        )
    return "srw-registry:5000/srw-manifest-fixture@" + digest


def deployed_agents():
    paths = [
        "src/agent/api/persistent_app.py",
        "src/agent/agent.py",
        "src/agent/core/workspace.py",
        "src/shared/runtime/core/session_config_patch.py",
        "src/shared/runtime/core/tool_report.py",
        "src/shared/runtime/core/srw_manifest_config.py",
    ]
    local = {
        path: hashlib.sha256((ROOT / path).read_bytes()).hexdigest() for path in paths
    }
    pods = json.loads(
        cutover.command(
            cutover.KUBECTL
            + [
                "get",
                "pods",
                "-l",
                "app.kubernetes.io/component=agent-stateless",
                "-o",
                "json",
            ]
        )
    )["items"]
    require(
        pods
        and all(
            not pod["metadata"].get("deletionTimestamp")
            and any(
                value["type"] == "Ready" and value["status"] == "True"
                for value in pod["status"].get("conditions", [])
            )
            for pod in pods
        ),
        "Wait for a completely ready stateless agent rollout before the smoke.",
    )
    evidence = []
    for pod in pods:
        container = next(
            value for value in pod["spec"]["containers"] if value["name"] == "agent"
        )
        remote = json.loads(
            cutover.command(
                cutover.KUBECTL
                + [
                    "exec",
                    pod["metadata"]["name"],
                    "-c",
                    container["name"],
                    "--",
                    "python",
                    "-I",
                    "-c",
                    "import json,hashlib; from pathlib import Path; "
                    f"print(json.dumps({{p:hashlib.sha256(Path('/app',p).read_bytes()).hexdigest() for p in {paths!r}}}))",
                ]
            )
        )
        require(
            remote == local,
            "A ready stateless agent differs from this checkout's snapshot implementation.",
        )
        evidence.append(
            {
                "pod": pod["metadata"]["name"],
                "uid": pod["metadata"]["uid"],
                "image": container["image"],
                "sourceFilesMatched": len(paths),
            }
        )
    return evidence


class Fixture:
    def __init__(self, client, name, model):
        self.client, self.name, self.model = client, name, model
        self.core = kube.CoreV1Api(client)
        self.network = kube.NetworkingV1Api(client)
        self.uid = None
        self.create_intent = False
        self.forward = None
        self.control = None
        self.control_token, self.inference_key = (
            secrets.token_urlsafe(40),
            secrets.token_urlsafe(40),
        )

    def create(self, image):
        labels = {OWNER_LABEL: self.name}
        try:
            self.core.read_namespace(self.name)
        except ApiException as exc:
            if exc.status != 404:
                raise
        else:
            raise GateFailure(
                "The fixture namespace already exists; refusing adoption."
            )
        self.create_intent = True
        namespace = self.core.create_namespace(
            {"metadata": {"name": self.name, "labels": labels}}
        )
        self.uid = namespace.metadata.uid
        self.network.create_namespaced_network_policy(
            self.name,
            {
                "metadata": {"name": "fixture", "labels": labels},
                "spec": {
                    "podSelector": {},
                    "policyTypes": ["Ingress", "Egress"],
                    "egress": [],
                    "ingress": [
                        {
                            "from": [
                                {
                                    "namespaceSelector": {
                                        "matchLabels": {
                                            "kubernetes.io/metadata.name": "srw"
                                        }
                                    }
                                }
                            ],
                            "ports": [{"protocol": "TCP", "port": 8000}],
                        }
                    ],
                },
            },
        )
        self.core.create_namespaced_secret(
            self.name,
            {
                "metadata": {"name": "fixture", "labels": labels},
                "stringData": {
                    "control": self.control_token,
                    "inference": self.inference_key,
                },
            },
        )
        self.core.create_namespaced_pod(
            self.name,
            {
                "metadata": {"name": "fixture", "labels": labels},
                "spec": {
                    "automountServiceAccountToken": False,
                    "restartPolicy": "Never",
                    "securityContext": {
                        "runAsNonRoot": True,
                        "runAsUser": 65532,
                        "runAsGroup": 65532,
                        "seccompProfile": {"type": "RuntimeDefault"},
                    },
                    "containers": [
                        {
                            "name": "fixture",
                            "image": image,
                            "imagePullPolicy": "IfNotPresent",
                            "env": [{"name": "E2E_CHAT_MODEL_ID", "value": self.model}]
                            + [
                                {
                                    "name": env,
                                    "valueFrom": {
                                        "secretKeyRef": {"name": "fixture", "key": key}
                                    },
                                }
                                for env, key in (
                                    ("E2E_CONTROL_TOKEN", "control"),
                                    ("E2E_INFERENCE_API_KEY", "inference"),
                                )
                            ],
                            "ports": [{"containerPort": 8000}],
                            "resources": {
                                "requests": {"cpu": "100m", "memory": "128Mi"},
                                "limits": {"cpu": "1", "memory": "512Mi"},
                            },
                            "securityContext": {
                                "allowPrivilegeEscalation": False,
                                "readOnlyRootFilesystem": True,
                                "capabilities": {"drop": ["ALL"]},
                            },
                            "readinessProbe": {
                                "httpGet": {"path": "/health", "port": 8000},
                                "periodSeconds": 2,
                            },
                        }
                    ],
                },
            },
        )
        self.core.create_namespaced_service(
            self.name,
            {
                "metadata": {"name": "fixture", "labels": labels},
                "spec": {
                    "selector": labels,
                    "ports": [{"port": 8000, "targetPort": 8000}],
                },
            },
        )
        wait_for(
            lambda: any(
                c.type == "Ready" and c.status == "True"
                for c in (
                    self.core.read_namespaced_pod(
                        "fixture", self.name
                    ).status.conditions
                    or []
                )
            ),
            "Waiting for the deterministic provider readiness.",
        )
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]
        self.forward = subprocess.Popen(
            [
                "kubectl",
                "--context",
                "k3d-srw",
                "-n",
                self.name,
                "port-forward",
                "--address",
                "127.0.0.1",
                "pod/fixture",
                f"{port}:8001",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        self.control = httpx.Client(
            base_url=f"http://127.0.0.1:{port}",
            trust_env=False,
            timeout=5,
            headers={"Authorization": "Bearer " + self.control_token},
        )

        def reachable():
            require(
                self.forward.poll() is None, "Fixture control port-forward stopped."
            )
            try:
                return self.control.get("/control/health").status_code == 200
            except httpx.HTTPError:
                return False

        wait_for(
            reachable,
            "Waiting for the private fixture control channel.",
            timeout=30,
            interval=0.2,
        )

    def arm(self, run_id, scenario, required):
        request(
            self.control,
            "POST",
            f"/control/scenarios/{run_id}/arm",
            status=201,
            payload={
                "scenario": scenario,
                "required_responses": required,
                "chunk_delay_ms": 0,
            },
        )

    def state(self, run_id):
        row = request(self.control, "GET", f"/control/scenarios/{run_id}")
        return {
            key: row[key]
            for key in (
                "consumed_required_responses",
                "worker_job_tool_steps",
                "unexpected_count",
                "pending_calls",
            )
        }

    def reset(self, run_id):
        request(self.control, "DELETE", f"/control/scenarios/{run_id}")

    def close(self):
        if self.control:
            self.control.close()
        if self.forward:
            self.forward.terminate()
            self.forward.wait(timeout=10)
        if self.create_intent and not self.uid:
            try:
                current = self.core.read_namespace(self.name)
            except ApiException as exc:
                if exc.status != 404:
                    raise
            else:
                require(
                    current.metadata.labels.get(OWNER_LABEL) == self.name,
                    "The uncertain namespace creation has a different owner.",
                )
                self.uid = current.metadata.uid
        if self.uid:
            current = self.core.read_namespace(self.name)
            require(
                current.metadata.uid == self.uid
                and current.metadata.labels.get(OWNER_LABEL) == self.name,
                "Refusing cleanup of a replaced fixture namespace.",
            )
            self.core.delete_namespace(
                self.name,
                body=kube.V1DeleteOptions(
                    preconditions=kube.V1Preconditions(uid=self.uid)
                ),
            )

            def removed():
                try:
                    self.core.read_namespace(self.name)
                except ApiException as exc:
                    if exc.status == 404:
                        return True
                    raise
                return False

            wait_for(
                removed,
                "Waiting for the owned provider namespace cleanup.",
                timeout=120,
            )


class Presence:
    """Hold the real owner SSE stream; never retain or print event payloads."""

    def __init__(self, headers, thread_id):
        self.client = httpx.Client(
            verify=ssl.create_default_context(),
            trust_env=False,
            headers=headers,
            timeout=httpx.Timeout(15, read=None),
        )
        self.url = cutover.API + f"/api/persistent/threads/{thread_id}/stream"
        self.ready, self.stop = threading.Event(), threading.Event()
        self.ok = False
        self.response = None
        self.thread = threading.Thread(target=self.run, daemon=True)

    def run(self):
        try:
            with self.client.stream("GET", self.url) as response:
                self.response = response
                self.ok = response.status_code == 200
                self.ready.set()
                if self.ok:
                    for _ in response.iter_lines():
                        if self.stop.is_set():
                            break
        except httpx.HTTPError:
            self.ready.set()

    def open(self):
        self.thread.start()
        require(
            self.ready.wait(20) and self.ok,
            "The owner Session SSE stream did not open.",
        )

    def close(self):
        self.stop.set()
        if self.response:
            self.response.close()
        self.client.close()
        self.thread.join(timeout=5)


def authored_expert(prefix, image, model, *, session=False):
    groups = set()
    for path in (
        "config/expert_base.yaml",
        "config/overlays/worker.yaml",
        "config/overlays/session.yaml",
    ):
        groups.update(yaml.safe_load((ROOT / path).read_text()).get("tools", {}))
    tools = {name: [] for name in groups}
    if not session:
        tools.update(
            workspace=["read_file"],
            core=["next_phase_todos", "todo_complete", "job_complete"],
        )
    private = {
        "config_name": "session_base" if session else "worker_base",
        "config": {
            "llm": {"model": model, "temperature": 0.17},
            "auxiliary": {"model": model, "enabled": False},
            "memory": {"enabled": False},
            "tools": tools,
            "limits": {"max_tool_calls_per_job": 50},
        },
        "prompts": {
            "persona": "Use only the configured deterministic test provider. Captured manifest persona."
        },
    }
    if not session:
        private["config"].update(
            autonomy="full",
            instruction_files=[],
            verification={"enabled": False},
            curator={"enabled": False},
            scholar={"enabled": False},
            delegation={"enabled": False},
        )
    document = cutover.authored_expert(
        prefix, "session" if session else "worker", image=image, private=private
    )
    document["spec"]["workspacePreference"] = {"backend": "sandbox"}
    document["spec"]["runtime"]["adapter"] = "srw/v1"
    document["spec"]["runtime"].pop("image")
    if session:
        document["metadata"]["annotations"]["srw.io/expert-type"] = "session"
        document["metadata"]["tags"] = ["session"]
    return document


def authored_job(prefix, expert):
    document = cutover.authored_job(prefix)
    document["spec"].update(
        task={"text": "E2E-" + prefix + "job"},
        completion={"mode": "Reported"},
        timeoutSeconds=600,
    )
    document["spec"]["execution"].update(
        expert={"inline": expert["spec"]},
        workspace={
            "template": {"inline": {"backend": "sandbox", "retention": "Delete"}}
        },
    )
    return document


class OwnedGate(cutover.CutoverGate):
    def request(self, *args, **kwargs):
        if kwargs.get("authenticated", True) and expires_soon(self.headers):
            self.login()
        return super().request(*args, **kwargs)


class Smoke:
    def __init__(self, client, prefix, image, fixture, admin):
        self.client, self.prefix, self.image = client, prefix, image
        self.fixture, self.admin = fixture, admin
        self.gate = OwnedGate(
            client,
            prefix=prefix,
            inspect_db=lambda: cutover.database_evidence(prefix, []),
            inspect_native=None,
        )
        self.model = fixture.model
        self.endpoint_uid = self.model_uid = self.thread_id = self.job_id = None
        self.presence = None
        self.defaults = self.pins = None
        self.evidence = {"checks": [], "cleanup": {"complete": False}}

    def admin_request(self, method, path, **kwargs):
        self.admin.refresh()
        return request(
            self.client,
            method,
            cutover.API + path,
            headers=self.admin.headers,
            **kwargs,
        )

    def check(self, message):
        self.evidence["checks"].append(message)
        progress(message)

    def register_model(self):
        self.defaults = readonly_evidence()["effective"]
        before = readonly_evidence()
        # The resolver uses display_label sorting for fallback, not wire IDs.
        for capability in ("chat", "auxiliary"):
            require(
                before["effective"][capability]
                and before["fallback"][capability]
                and before["fallback"][capability] < self.model,
                "The unique fixture label could change an effective fallback; refusing registration.",
            )
        self.pins = self.admin_request("GET", "/api/admin/providers/defaults")
        existing = self.admin_request("GET", "/api/admin/providers/models")
        require(
            not any(row["model_id"] == self.model for row in existing),
            "The unique test model already exists.",
        )
        endpoint = self.admin_request(
            "POST",
            "/api/admin/providers/endpoints",
            payload={
                "label": self.prefix + "fixture",
                "base_url": f"http://fixture.{self.fixture.name}.svc.cluster.local:8000/v1",
                "api_key": self.fixture.inference_key,
                "allow_insecure": True,
            },
        )
        self.endpoint_uid = str(UUID(endpoint["id"]))
        catalog = self.admin_request(
            "POST",
            "/api/admin/providers/models",
            payload={
                "provider_kind": "endpoint",
                "provider_ref": self.endpoint_uid,
                "model_id": self.model,
                "display_label": self.model,
                "capabilities": ["chat", "auxiliary"],
                "family": "e2e",
                "context_window": 128000,
                "enabled": True,
                "notes": self.prefix + "fixture",
            },
        )
        self.model_uid = str(UUID(catalog["id"]))
        self.assert_defaults()
        self.check(
            "Role-authenticated owned provider/model registration preserves configured and effective defaults"
        )

    def assert_defaults(self):
        require(
            readonly_evidence()["effective"] == self.defaults
            and self.admin_request("GET", "/api/admin/providers/defaults") == self.pins,
            "Model defaults changed during the smoke; no automatic restoration attempted.",
        )

    def snapshot(self, kind, work_id):
        result = readonly_evidence(kind=kind, work_id=work_id)
        require(
            result
            and result["adapter"] == "srw/v1"
            and result["model"] == self.model
            and result["image"] == self.image,
            "The execution does not contain the admitted SRW adapter/model snapshot.",
        )
        return result

    def workspace_binding(self, backend):
        if backend == "none":
            return None
        name = self.prefix + backend
        self.gate.apply(
            {
                "apiVersion": "srw/v1alpha1",
                "kind": "WorkspaceTemplate",
                "metadata": {
                    "name": name,
                    "scope": {"kind": "Account", "name": "me"},
                    "annotations": {cutover.OWNER_LABEL: self.prefix},
                },
                "spec": {"backend": backend},
            }
        )
        return {"template": {"ref": {"name": name}}}

    def exercise_job(self, backend="sandbox", *, compatibility=False):
        run_id = self.prefix + "job"
        self.fixture.arm(run_id, "worker-job", 100)
        document = authored_job(
            self.prefix, authored_expert(self.prefix, self.image, self.model)
        )
        document["metadata"]["name"] += "-" + backend
        worker = authored_expert(self.prefix, self.image, self.model)
        worker_item = self.gate.apply(worker)["resources"][0]
        document["spec"]["execution"]["expert"] = {
            "ref": {"name": worker["metadata"]["name"]}
        }
        selected = self.workspace_binding(backend)
        document["spec"]["execution"]["workspace"] = selected
        if compatibility:
            # The compatibility Job API supplies a default Project. Explicitly
            # name the Account scope of this independently owned template.
            selected["template"]["ref"]["scope"] = worker_item["resource"]["metadata"][
                "scope"
            ]
            result = self.gate.request(
                "POST",
                "/api/jobs",
                payload={
                    "description": "E2E-" + run_id,
                    "expert_id": self.gate.catalog_identity(worker_item),
                    "workspace": selected,
                    "datasource_ids": [],
                },
            )
            self.job_id = str(UUID(result["id"]))
            item = None
        else:
            result = self.gate.apply(
                document, key=self.prefix + "job-create-" + backend
            )
            require(
                len(result["executions"]) == 1,
                "Job admission did not return one work identity.",
            )
            self.job_id = str(UUID(next(iter(result["executions"].values()))))
            item = result["resources"][0]

        def finished():
            current = (
                self.gate.request("GET", "/api/jobs/" + self.job_id)
                if compatibility
                else self.gate.current(item)
            )
            phase = (
                current.get("status")
                if compatibility
                else current.get("status", {}).get("phase")
            )
            require(
                phase not in {"failed", "paused", "pending_review"},
                "The deterministic Job did not complete successfully.",
            )
            return current if phase == "completed" else None

        wait_for(
            finished, "Waiting for the real SRW Job to report completion.", timeout=600
        )
        snapshot = self.snapshot("Job", self.job_id)
        require(
            snapshot["workspaceBackend"] == backend
            and snapshot["workspace"] == selected,
            "The Job did not bind its referenced workspace independently of the Expert.",
        )
        if backend in {"virtual", "none"}:
            require(
                not snapshot["job"]["has_workspace_container"],
                "A workspace without a container acquired a synthetic infrastructure projection.",
            )
        if not compatibility:
            repeated = self.gate.apply(document)
            require(
                next(iter(repeated["executions"].values())) == self.job_id
                and repeated["resources"][0]["uid"] == item["uid"]
                and not repeated["resources"][0]["changed"],
                "Reapply replayed a completed Job.",
            )
        state = self.fixture.state(run_id)
        require(
            state["worker_job_tool_steps"] >= 10
            and state["unexpected_count"] == 0
            and state["pending_calls"] == 0,
            "The Job fixture observed missing or unexpected inference.",
        )
        result = {
            "workID": self.job_id,
            "snapshot": snapshot,
            "provider": state,
            "ingress": "job-api" if compatibility else "manifest-api",
        }
        self.evidence.setdefault("workspaceJobs", []).append(result)
        if backend == "sandbox":
            self.evidence["job"] = result
        self.fixture.reset(run_id)
        self.check(
            f"Existing Job API selects a referenced {backend} workspace independently of its Expert"
            if compatibility
            else f"Native SRW manifest Job on {backend} completes through the same Expert and reapply preserves its execution identity"
        )

    def reply(self, number):
        self.gate.request(
            "POST",
            f"/api/persistent/threads/{self.thread_id}/input",
            payload={"content": "E2E-" + self.prefix + f"session message {number}"},
        )

        def replied():
            rows = self.gate.request(
                "GET", f"/api/persistent/threads/{self.thread_id}/messages"
            )["messages"]
            count = sum(
                row.get("role") == "ai"
                and row.get("content") == "E2E_REPLY:" + self.prefix + "session"
                for row in rows
            )
            return count >= number

        wait_for(replied, "Waiting for the deterministic Session reply.", timeout=240)

        def settled():
            connection = self.gate.request(
                "GET", f"/api/sessions/{self.thread_id}/connection"
            )
            queue = connection.get("queue") or {}
            require(
                queue.get("state") != "parked",
                "The Session claim parked after its reply.",
            )
            return queue.get("state") == "done" and not queue.get("pending_input")

        wait_for(
            settled, "Waiting for the Session turn's durable release.", timeout=120
        )

    def open_presence(self):
        self.gate.login()
        self.presence = Presence(self.gate.headers, self.thread_id)
        self.presence.open()

    def exercise_session(self):
        run_id = self.prefix + "session"
        self.fixture.arm(run_id, "reply", 3)
        document = authored_expert(self.prefix, self.image, self.model, session=True)
        item = self.gate.apply(document)["resources"][0]
        expert_id = self.gate.catalog_identity(item)
        selected = self.workspace_binding("sandbox")
        thread = self.gate.request(
            "POST",
            "/api/persistent/threads",
            payload={
                "title": "E2E-" + run_id,
                "config_name": "session_base",
                "expert_id": expert_id,
                "model": self.model,
                "project_ids": [],
                "datasource_ids": [],
                "use_datasource_defaults": False,
                "workspace": selected,
            },
        )
        self.thread_id = str(UUID(thread["thread_id"]))
        connection = self.gate.request(
            "GET", f"/api/sessions/{self.thread_id}/connection"
        )
        require(
            connection.get("controls", {}).get("config.update") == "rest",
            "This smoke requires the deployed stateless Session REST control lane.",
        )
        self.open_presence()
        self.reply(1)
        original = self.snapshot("Session", self.thread_id)
        require(
            original["thread"]["execution_lane"] == "stateless"
            and original["workspaceBackend"] == "sandbox"
            and original["workspace"] == selected,
            "The Session execution lane differs.",
        )

        changed = deepcopy(document)
        changed["spec"]["runtime"]["config"]["prompts"]["persona"] = (
            "A later saved Expert persona must not replace captured instructions."
        )
        changed["spec"]["runtime"]["config"]["config"]["llm"]["temperature"] = 0.91
        self.gate.apply(
            changed,
            versions={cutover.resource_key(item["resource"]): item["resourceVersion"]},
        )
        unchanged = self.snapshot("Session", self.thread_id)
        require(
            all(
                unchanged[key] == original[key]
                for key in (
                    "id",
                    "generation",
                    "revision",
                    "sourceDigest",
                    "temperature",
                    "workspace",
                    "resolvedWorkspace",
                    "workspaceBackend",
                )
            ),
            "Editing the saved Expert changed a Session's captured configuration.",
        )
        updated = self.gate.request(
            "PATCH",
            f"/api/persistent/threads/{self.thread_id}/config",
            payload={"config_override": {"llm": {"temperature": 0.42}}},
        )
        require(
            updated["effective"] == "next_turn",
            "Stateless PATCH did not declare its turn boundary.",
        )
        patched = self.snapshot("Session", self.thread_id)
        require(
            patched["id"] == original["id"]
            and patched["generation"] == original["generation"] + 1
            and patched["temperature"] == 0.42
            and patched["sourceDigest"] == original["sourceDigest"]
            and patched["workspace"] == original["workspace"]
            and patched["resolvedWorkspace"] == original["resolvedWorkspace"],
            "The Session PATCH did not create the expected isolated generation.",
        )
        self.reply(2)
        self.gate.request("DELETE", f"/api/persistent/threads/{self.thread_id}")
        self.presence.close()
        self.presence = None
        self.gate.request(
            "POST", f"/api/persistent/threads/{self.thread_id}/resume", payload={}
        )
        self.open_presence()
        self.reply(3)
        resumed = self.snapshot("Session", self.thread_id)
        require(
            all(
                resumed[key] == patched[key]
                for key in (
                    "id",
                    "generation",
                    "revision",
                    "sourceDigest",
                    "temperature",
                    "workspace",
                    "resolvedWorkspace",
                    "workspaceBackend",
                )
            ),
            "End/Resume re-resolved or replaced the edited Session snapshot.",
        )
        state = self.fixture.state(run_id)
        require(
            state["consumed_required_responses"] == 3
            and state["unexpected_count"] == 0
            and state["pending_calls"] == 0,
            "The Session fixture observed missing or unexpected inference.",
        )
        self.evidence["session"] = {
            "workID": self.thread_id,
            "original": original,
            "patched": patched,
            "resumed": resumed,
            "provider": state,
        }
        self.check(
            "Stateless Session first attach, frozen source edit, next-turn PATCH and End/Resume preserve the admitted contract"
        )

    def exercise_workspace_sessions(self):
        """One immutable Expert, with explicit shell/file tools, on three tiers."""
        if self.presence:
            self.presence.close()
            self.presence = None
        document = authored_expert(
            self.prefix + "workspace-", self.image, self.model, session=True
        )
        document["metadata"]["annotations"][cutover.OWNER_LABEL] = self.prefix
        tools = document["spec"]["runtime"]["config"]["config"]["tools"]
        tools.update(workspace=["read_file"], shell=["run_command"])
        item = self.gate.apply(document)["resources"][0]
        expert_id = self.gate.catalog_identity(item)
        run_id = self.prefix + "session"
        results = self.evidence["workspaceSessions"] = []
        for backend in ("sandbox", "virtual", "none"):
            progress(f"Exercising the unchanged Session Expert on {backend}.")
            self.fixture.reset(run_id)
            self.fixture.arm(run_id, "reply", 1)
            selected = self.workspace_binding(backend)
            preview = self.gate.request(
                "POST",
                "/api/persistent/tool-groups/preview",
                payload={
                    "expert_id": expert_id,
                    "workspace": selected,
                    "workspace_preference": "sandbox",
                },
            )
            require(
                preview["workspace"]["backend"] == backend
                and preview["workspace"]["source"] == "request",
                "An Expert recommendation replaced the explicit workspace preview.",
            )
            if backend != "sandbox":
                require(
                    preview["categories"]["shell"]["state"] == "unavailable",
                    "The preview did not explain unavailable shell tools.",
                )
            thread = self.gate.request(
                "POST",
                "/api/persistent/threads",
                payload={
                    "title": "E2E-" + run_id,
                    "expert_id": expert_id,
                    "config_name": "session_base",
                    "model": self.model,
                    "project_ids": [],
                    "datasource_ids": [],
                    "use_datasource_defaults": False,
                    "workspace": selected,
                },
            )
            self.thread_id = str(UUID(thread["thread_id"]))
            self.open_presence()
            self.reply(1)
            snapshot = self.snapshot("Session", self.thread_id)
            require(
                snapshot["workspaceBackend"] == backend
                and snapshot["workspace"] == selected,
                "A Session did not bind the requested workspace.",
            )
            report = self.gate.request(
                "GET", f"/api/persistent/threads/{self.thread_id}/tool-groups"
            )
            state = self.fixture.state(run_id)
            require(
                state["consumed_required_responses"] == 1
                and state["unexpected_count"] == 0
                and state["pending_calls"] == 0,
                "The workspace Session used unexpected inference.",
            )
            results.append(
                {
                    "backend": backend,
                    "workID": self.thread_id,
                    "snapshot": snapshot,
                    "toolReportOrigin": report["origin"],
                    "provider": state,
                    "shellPreview": preview["categories"]["shell"]["state"],
                }
            )
            self.presence.close()
            self.presence = None
        require(
            self.gate.current(item)["revision"] == item["revision"],
            "Workspace selection unexpectedly changed the Expert revision.",
        )
        self.fixture.reset(run_id)
        self.check(
            "The same immutable Session Expert replies on sandbox, virtual and explicit none; previews respect the selected tier"
        )

    def cleanup(self):
        progress("Cleaning only the owned smoke workloads and fixture registrations.")
        cleanup = self.evidence["cleanup"]
        cleanup["stage"] = "owner-authentication"
        if self.presence:
            self.presence.close()
            self.presence = None
        self.gate.login()
        identity = request(
            self.client,
            "GET",
            cutover.API + "/api/auth/me",
            headers=self.gate.headers,
        )["user"]
        owner_id = str(UUID(identity["id"]))
        # Recover IDs after an unknown mutation response, through read-only exact
        # names. The names are random and every workload is owned by the test user.
        owned = readonly_evidence(prefix=self.prefix)
        for kind, ids in (("Session", owned["threads"]), ("Job", owned["jobs"])):
            cleanup["stage"] = kind.lower() + "-retirement"
            for work_id in ids:
                cleanup_owned_workload(
                    self.client,
                    kind=kind,
                    work_id=work_id,
                    owner_id=owner_id,
                    prefix=self.prefix,
                    headers=self.gate.headers,
                    evidence=cleanup.setdefault("workloads", []),
                )
        cleanup["stage"] = "manifest-retirement"
        self.gate.cleanup()
        require(
            self.gate.evidence["cleanup"]["complete"],
            "Owned manifest definitions remain after cleanup.",
        )
        cleanup["stage"] = "provider-retirement"
        endpoints = (
            self.admin_request("GET", "/api/admin/providers/endpoints")
            if self.admin.headers
            else []
        )
        endpoints = [
            row
            for row in endpoints
            if row["label"] == self.prefix + "fixture"
            and row["base_url"]
            == f"http://fixture.{self.fixture.name}.svc.cluster.local:8000/v1"
        ]
        require(len(endpoints) <= 1, "Fixture endpoint ownership is ambiguous.")
        for endpoint in endpoints:
            endpoint_id = str(UUID(endpoint["id"]))
            rows = self.admin_request(
                "GET",
                "/api/admin/providers/models",
                params={"provider_kind": "endpoint", "provider_ref": endpoint_id},
            )
            require(
                all(
                    row["model_id"] == self.model
                    and row["notes"] == self.prefix + "fixture"
                    for row in rows
                ),
                "The owned provider acquired unrelated models; refusing cleanup.",
            )
            for row in rows:
                self.admin_request(
                    "DELETE", "/api/admin/providers/models/" + str(UUID(row["id"]))
                )
            self.admin_request(
                "DELETE", "/api/admin/providers/endpoints/" + endpoint_id
            )
        if self.defaults is not None:
            cleanup["stage"] = "defaults-verification"
            self.assert_defaults()
        remaining = readonly_evidence(prefix=self.prefix)
        require(
            not remaining["jobs"] and not remaining["threads"],
            "Owned work remains after lifecycle cleanup.",
        )
        self.evidence["cleanup"]["workloadsAndCatalogRemoved"] = True
        cleanup["stage"] = "workloads-and-catalog-removed"


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--fixture-image",
        help="Previously published immutable srw-registry:5000 fixture image; omit to build current source",
    )
    parser.add_argument(
        "--compatibility-job-only",
        action="store_true",
        help="Exercise only workspace selection through the existing Job API",
    )
    args = parser.parse_args(argv)
    prefix = "cutover-" + uuid4().hex[:12] + "-"
    evidence = {
        "gate": "manifest-srw-adapter-smoke",
        "context": "k3d-srw",
        "gateIdentity": prefix,
        "startedAt": datetime.now(timezone.utc).isoformat(),
        "status": "failed",
        "scope": "Real SRW Jobs on sandbox/virtual and stateless Sessions on sandbox/virtual/none over HTTP, SSE and referenced workspace snapshots",
    }
    admin = fixture = smoke = None
    succeeded = False
    with httpx.Client(
        verify=ssl.create_default_context(),
        trust_env=False,
        follow_redirects=False,
        timeout=60,
    ) as client:
        try:
            evidence["deployment"] = cutover.deployed_identity()
            evidence["agents"] = deployed_agents()
            image = cutover.remote_json(
                "import json; from orchestrator.services.manifest_experts import installed_srw_image; print(json.dumps(installed_srw_image()))"
            )
            require(
                isinstance(image, str) and image,
                "The trusted installation agent image is unavailable.",
            )
            # Validate the authored contracts locally before creating a test identity.
            model = "zz-srw-manifest-" + prefix.removeprefix("cutover-").removesuffix(
                "-"
            )
            from shared.manifests import validate_documents

            validate_documents(
                [
                    authored_job(prefix, authored_expert(prefix, image, model)),
                    authored_expert(prefix, image, model, session=True),
                ]
            )
            fixture_image = args.fixture_image or publish_fixture()
            require(
                fixture_image.startswith(
                    "srw-registry:5000/srw-manifest-fixture@sha256:"
                )
                and len(fixture_image.rsplit(":", 1)[1]) == 64,
                "Fixture image must use a verified local registry digest.",
            )
            evidence["fixtureImage"] = fixture_image
            admin = TemporaryAdmin(client, "srw-manifest-admin-" + uuid4().hex[:12])
            admin.create()
            fixture = Fixture(
                kube_config.new_client_from_config(context="k3d-srw"),
                "srw-adapter-gate-" + uuid4().hex[:12],
                model,
            )
            smoke = Smoke(client, prefix, image, fixture, admin)
            smoke.gate.login()
            fixture.create(fixture_image)
            smoke.register_model()
            if args.compatibility_job_only:
                evidence["scope"] = (
                    "Real existing Job API with a referenced virtual workspace and independently selected Expert"
                )
                smoke.exercise_job("virtual", compatibility=True)
            else:
                smoke.exercise_job()
                smoke.exercise_job("virtual")
                smoke.exercise_job("virtual", compatibility=True)
                smoke.exercise_session()
                smoke.exercise_workspace_sessions()
            succeeded = True
        except GateFailure as exc:
            evidence["failure"] = str(exc)
        except Exception as exc:
            evidence["failure"] = (
                "Unexpected smoke failure ("
                + type(exc).__name__
                + "); details suppressed."
            )
        finally:
            cleanup_ok = True
            try:
                if smoke:
                    smoke.cleanup()
                if fixture:
                    if smoke:
                        smoke.evidence["cleanup"]["stage"] = "fixture-namespace"
                    fixture.close()
                if admin:
                    if smoke:
                        smoke.evidence["cleanup"]["stage"] = "application-identity"
                    admin.delete_app_user()
            except Exception as exc:
                cleanup_ok = False
                evidence["cleanupFailure"] = (
                    "Owned cleanup did not finish ("
                    + type(exc).__name__
                    + "); exact fixture identities retained in evidence."
                )
                if isinstance(exc, GateFailure):
                    evidence["cleanupSafeError"] = str(exc)
            finally:
                if admin:
                    try:
                        admin.revoke()
                    except Exception as exc:
                        cleanup_ok = False
                        evidence["identityCleanupFailure"] = (
                            "Exact Keycloak cleanup did not finish ("
                            + type(exc).__name__
                            + ")."
                        )
                    evidence["temporaryAdmin"] = {
                        "username": admin.name,
                        "oauthClientID": admin.oauth_uid,
                        "keycloakID": admin.uid,
                        "applicationID": admin.app_uid,
                        **admin.cleanup_evidence,
                        "externalProfileResidue": "Normal JIT cloud/Gitea profiles may outlive the application user; no external account deletion API is assumed.",
                    }
            if smoke:
                evidence.update(smoke.evidence)
                evidence["ownedWorkIDs"] = {
                    "job": smoke.job_id,
                    "session": smoke.thread_id,
                }
            evidence.setdefault("cleanup", {})["complete"] = cleanup_ok
            if cleanup_ok:
                evidence["cleanup"]["stage"] = "complete"
            if fixture:
                evidence["fixtureNamespace"] = {
                    "name": fixture.name,
                    "uid": fixture.uid,
                }
            if succeeded and cleanup_ok:
                evidence["status"] = "passed"
    evidence["finishedAt"] = datetime.now(timezone.utc).isoformat()
    print(json.dumps(evidence, indent=2))
    return 0 if evidence["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
