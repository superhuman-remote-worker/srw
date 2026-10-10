#!/usr/bin/env python3
"""Local k3d gate for the job outbox and the project folder copy (main cloud
slices 6.2 and 6.3).

Design: knowledge-base/knowledge/features/main_cloud_as_connectors.md,
"Overnight run: slices 3 to 8" (job outbox; Mode A becomes
``cloud_folder_checkout``), and the slice 6/8 plan's §8 items 6 to 10.
Envelope: scripts/k3d-main-cloud-gate.py's. Dry-run by default, the exact
k3d-srw/srw context, every secret on ``kubectl exec -i`` stdin and scrubbed
from every printed line, ``cap_memory()`` in every program the gate runs in
a pod, and a cleanup in ``finally`` that touches only what this run created,
followed by a residue check by gate id (``--sweep <gate id>`` repeats it
for a run that died). No LLM runs: jobs are created held (paused, with a
gate freeze), so the dispatcher never starts an agent; the gate plays the
agent's part (commits to the job's repository) and hands its completion
report to the orchestrator's own completion handler, in a program in the
orchestrator pod with the application's resources. (The HTTP endpoint fences
a pinned report by its assigned agent, and a held job was never claimed by
one.)

Fixtures (all disposable, all named after the gate id):

  account     an SRW account ``<gate id>@example.invalid``, approved, with an
              MCP token minted for this run (the REST calls' Bearer), and a
              Nextcloud account of the same email (the outbox is shared with
              it; no k3d account has one before its first cloud login)
  project     ``<gate id>``, created over REST by the account, provisioned on
              the main cloud (its folder and its two native connectors are
              waited for); its folder holds ``gate.md`` and ``gate.bin``
  jobs        ``copy``: the default selection (the folder's copy and the
              owner's outbox), its copy chosen read-only as a creator would on
              its chip; ``write-back``: the default selection at the level
              admission resolves; ``empty``: an explicit empty selection

Checks (each printed PASS/FAIL; the exit status is 0 only if all pass):

  preflight   the orchestrator serves this checkout's slice 6 modules byte
              for byte; the project's checkout and outbox rows exist
  copy        (§8 item 8) a copy chosen read-only is stamped read_only, the
              seed holds dispatch and then copies both files (binary too),
              they are in the job's repository under projects/<slug>/, and
              the task brief names the path and says edits go nowhere; a
              commit there is not captured, the job completes, its review
              mode is open_folder, and the cloud file is unchanged
  outbox      (§8 item 6) the completion copied the declared deliverable into
              a folder shared with the owner: context.job_outbox delivered
              and shared, exported_at stamped, the owner reads the file over
              their own WebDAV
  resync      (§8 item 7) the re-sync endpoint answers 200 for the cloud
              project's job, twice, without sharing again
  write-back  (§8 item 9) admission stamps reviewed_write_back by default
              (the highest level the matrix offers); the commit is captured
              (diff_status pending, capture captured, pending_review, review
              mode diff), the owner's accept answers 200, and the cloud file
              holds the edit (read back through the main cloud)
  empty       (§8 item 10) an explicit empty selection gets no copy: no
              cloud_checkout, no cloud_baseline, no projects/ in its repository
  cleanup     nothing this run created is left: the jobs and their
              repositories, the outbox folders, the project (its folder and
              connectors), the Nextcloud account, the SRW account and token

Run with the repository venv on the k3d-srw cluster, alone: this is a
mutating gate.

  .venv/bin/python scripts/k3d-cloud-outbox-checkout-gate.py           # plan
  .venv/bin/python scripts/k3d-cloud-outbox-checkout-gate.py \\
      --run --confirm LOCAL-K3D-DISPOSABLE
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import re
import secrets
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[1]
LOCAL_CONTEXT = "k3d-srw"
LOCAL_NAMESPACE = "srw"
LOCAL_CONFIRMATION = "LOCAL-K3D-DISPOSABLE"
K = ["kubectl", f"--context={LOCAL_CONTEXT}", "-n", LOCAL_NAMESPACE]
ORCHESTRATOR = "deploy/srw-orchestrator"
ORCHESTRATOR_CONTAINER = "orchestrator"
POSTGRES_POD = "srw-postgres-0"
POD_ROOT = "/app"
ACCOUNT_DOMAIN = "example.invalid"
_GATE_ID_RE = re.compile(r"oc-[0-9a-f]{10}\Z")
_UUID_RE = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\Z")

#: What the orchestrator must serve byte for byte for slices 6.2 and 6.3.
SERVED = (
    "src/orchestrator/services/job_cloud_checkout.py",
    "src/orchestrator/services/job_cloud_baseline.py",
    "src/orchestrator/services/job_export.py",
    "src/orchestrator/services/job_provisioning.py",
    "src/orchestrator/services/job_projection.py",
    "src/orchestrator/services/legacy_job_completion.py",
    "src/orchestrator/services/project_cloud_connectors.py",
    "src/orchestrator/services/connector_drivers/cloud_folder_checkout.py",
    "src/shared/job_cloud_connectors.py",
)
#: The project folder's files: a text file the jobs edit and a binary one
#: only a complete copy holds.
TEXT_FILE = "gate.md"
TEXT_BODY = b"# Gate\n\nThe cloud original.\n"
BINARY_FILE = "gate.bin"
BINARY_BODY = bytes(range(256)) * 4
EDITED_BODY = "# Gate\n\nEdited by the job.\n"
DELIVERABLE = "output/report.md"
_SECRETS: list[str] = []


class GateError(RuntimeError):
    """Infrastructure trouble: the gate could not observe the product."""


class SafetyError(RuntimeError):
    """The requested run is outside the local disposable boundary."""


def _scrub(text: str) -> str:
    for value in _SECRETS:
        if value:
            text = text.replace(value, "<redacted>")
    return text


def secret(value: str) -> str:
    _SECRETS.append(value)
    return value


def run(
    args: list[str], *, data: str | None = None, timeout: int = 240
) -> tuple[int, str, str]:
    """Run argv; never echo argv or stdin. Output comes back scrubbed."""
    label = " ".join(args[4:7]) if args[:1] == ["kubectl"] else " ".join(args[:2])
    try:
        result = subprocess.run(
            args, input=data, text=True, capture_output=True, timeout=timeout
        )
    except subprocess.TimeoutExpired:
        raise GateError(f"{label[:80]} timed out after {timeout}s") from None
    return result.returncode, _scrub(result.stdout.strip()), _scrub(result.stderr)


def command(args: list[str], *, data: str | None = None, timeout: int = 240) -> str:
    rc, out, err = run(args, data=data, timeout=timeout)
    if rc:
        label = " ".join(args[4:7]) if args[:1] == ["kubectl"] else " ".join(args[:2])
        raise GateError(f"{label[:80]} failed (exit {rc}): {err.strip()[-600:]}")
    return out


def sql(query: str) -> str:
    """One statement on the app database (no secret may be in ``query``)."""
    return command(
        K
        + ["exec", POSTGRES_POD, "--", "psql", "-U", "srw", "-d", "srw"]
        + ["-v", "ON_ERROR_STOP=1", "-tAc", query]
    )


def lit(text: str) -> str:
    return "'" + str(text).replace("'", "''") + "'"


def wait_for(
    label: str, probe: Callable[[], Any], *, timeout: int, interval: float = 3.0
):
    deadline = time.monotonic() + timeout
    while True:
        value = probe()
        if value:
            return value
        if time.monotonic() >= deadline:
            raise GateError(f"timed out: {label}")
        time.sleep(interval)


# ---------------------------------------------------------------------------
# The program the gate runs in the orchestrator pod (stdin carries the
# request, secrets included; it prints one JSON line)
# ---------------------------------------------------------------------------

# Every program the gate runs inside a pod starts with this and calls
# cap_memory() once its imports are done: a gate program that grows must fail
# the gate with a MemoryError, never take the pod to the OOM killer. (The
# same helper as scripts/k3d-connector-drivers-gate.py.)
POD_MEMORY_BUDGET = 384 << 20
_POD_MEMORY_CAP = (
    r"""
import resource as _srw_resource
def cap_memory(budget=%d):
    with open("/proc/self/status") as status:
        data = next(
            int(line.split()[1]) * 1024
            for line in status
            if line.startswith("VmData:")
        )
    limit = data + budget
    _soft, hard = _srw_resource.getrlimit(_srw_resource.RLIMIT_DATA)
    if hard != _srw_resource.RLIM_INFINITY:
        limit = min(limit, hard)  # an inherited cap is only ever tightened
    _srw_resource.setrlimit(_srw_resource.RLIMIT_DATA, (limit, limit))
"""
    % POD_MEMORY_BUDGET
)

_RUNNER = (
    _POD_MEMORY_CAP
    + r"""
import asyncio, base64, hashlib, json, os, sys, urllib.error, urllib.request
from uuid import UUID
from orchestrator.database.postgres import PostgresDB
from orchestrator.services.cloud import (
    MainCloudRouter, ProjectFolderHandle, SessionFolderHandle, build_backend,
)
from orchestrator.services.gitea import GiteaClient
cap_memory()
request = json.loads(sys.stdin.readline())
_opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def http(method, path, *, body=None, headers=None):
    request_ = urllib.request.Request(
        "http://localhost:8085" + path,
        data=None if body is None else json.dumps(body).encode(),
        method=method,
        headers={"Content-Type": "application/json", **(headers or {})},
    )
    try:
        with _opener.open(request_, timeout=240) as response:
            return response.status, response.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as error:
        return error.code, error.read().decode("utf-8", "replace")


def _context(row):
    value = (row or {}).get("context")
    if isinstance(value, str):
        value = json.loads(value)
    return value or {}


async def _wait_tasks(timeout=60):
    pending = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    if pending:
        await asyncio.wait(pending, timeout=timeout)


async def main():
    from orchestrator.services.cloud.instance_registry import (
        reload_active_main_cloud_instance,
    )
    action = request["action"]
    if action == "complete":
        # The orchestrator's own completion, in this process: resources as
        # the application builds them, connected and bound as at startup.
        from orchestrator.application import build_application_resources
        resources = build_application_resources()
        db, router, gitea = (
            resources.postgres_db, resources.main_cloud_router, resources.gitea_client
        )
    else:
        resources = None
        db = PostgresDB(min_connections=1, max_connections=3)
        router = MainCloudRouter(build_backend())
        gitea = GiteaClient()
    await db.connect()
    # The active installation's authority, read and attested as at startup.
    if not await reload_active_main_cloud_instance(db, router):
        raise SystemExit("no active main cloud installation")
    backend = router.active
    if not backend.is_initialized:
        raise SystemExit("the main cloud backend is not initialized")
    if not await gitea.ensure_initialized():
        raise SystemExit("gitea is not initialized")
    out = {}
    try:
        if action == "account":
            # The Nextcloud account first (admin OCS), then the SRW row and
            # its token, which the gate's REST calls carry.
            status, text = None, ""
            resp = await backend._client.post(
                "/ocs/v1.php/cloud/users", params={"format": "json"},
                data={"userid": request["nc_user"], "password": request["nc_password"],
                      "email": request["email"], "displayName": request["name"]},
            )
            out["nc_status"] = resp.status_code
            out["nc_code"] = resp.json().get("ocs", {}).get("meta", {}).get("statuscode")
            row = await db.fetchrow(
                "INSERT INTO users (display_name, email, preferred_username, "
                "keycloak_sub, is_approved, approved_at) "
                "VALUES ($1, $2, $3, $4, TRUE, now()) RETURNING id",
                request["name"], request["email"], request["username"],
                request["sub"],
            )
            out["user_id"] = str(row["id"])
            await db.execute(
                "INSERT INTO auth_tokens (user_id, name, token_hash, token_prefix, "
                "kind, scope, expires_at) VALUES ($1, $2, $3, $4, 'mcp', 'user', "
                "now() + interval '2 hours')",
                row["id"], request["token_name"], request["token_hash"],
                request["token_prefix"],
            )
            # What SRW does when it admits a user while a main cloud is
            # configured (security.auth._ensure_user_outbox).
            from orchestrator.services.cloud_outbox_connectors import (
                ensure_user_outbox_connector,
            )
            out["outbox"] = await ensure_user_outbox_connector(db, str(row["id"]))
        elif action == "api":
            status, text = http(
                request["method"], request["path"], body=request.get("body"),
                headers={"Authorization": "Bearer " + request["token"]},
            )
            if request.get("pick"):
                # Only the fields a check reads: a job's JSON carries its
                # whole resolved config.
                try:
                    decoded = json.loads(text)
                except ValueError:
                    decoded = None
                if isinstance(decoded, dict):
                    text = json.dumps({key: decoded.get(key) for key in request["pick"]})
            out.update(status=status, body=text[:20000])
        elif action == "project_state":
            project = await db.get_project(request["project_id"])
            out["handle"] = bool(project and project.get("main_cloud_folder_handle"))
            out["rows"] = {
                row["managed_key"].split(":")[0]: str(row["id"])
                for row in await db.fetch(
                    "SELECT id, managed_key FROM datasources WHERE managed_key = ANY($1)",
                    [f"{kind}:{request['project_id']}" for kind in
                     ("project-cloud", "project-cloud-checkout")]
                    + [f"cloud-outbox:{request['user_id']}"],
                )
            }
            out["outbox"] = out["rows"].pop("cloud-outbox", None)
        elif action == "put_files":
            project = await db.get_project(request["project_id"])
            handle = ProjectFolderHandle.from_db(
                project["main_cloud_folder_handle"], backend=project["main_cloud_backend"]
            )
            for path, b64 in request["files"].items():
                await backend.put_project_folder_file_bytes(
                    handle, path=path, content=base64.b64decode(b64)
                )
            out["listed"] = sorted(
                e.path for e in await backend.list_project_folder(handle) if not e.is_dir
            )
        elif action == "read_cloud":
            project = await db.get_project(request["project_id"])
            handle = ProjectFolderHandle.from_db(
                project["main_cloud_folder_handle"], backend=project["main_cloud_backend"]
            )
            blob = await backend.get_project_folder_file_bytes(handle, path=request["path"])
            out["sha256"] = hashlib.sha256(blob).hexdigest()
            out["text"] = blob.decode("utf-8", "replace")[:2000]
        elif action == "create_job":
            from orchestrator.services.datasource_policy import (
                default_datasource_selection,
            )
            from orchestrator.services.job_cloud_checkout import admission_stamp
            from orchestrator.services.job_provisioning import provision_job_repo
            user, project = request["user_id"], request["project_id"]
            if request.get("datasource_ids") is None:
                ids, revisions = await default_datasource_selection(
                    db, user, [project], "sandbox", execution_kind="job"
                )
            else:
                ids, revisions = list(request["datasource_ids"]), {}
            # Admission's decision before the row exists, as
            # job_admission_creation makes it.
            stamp = await admission_stamp(
                db, datasource_ids=ids, project_id=project, user_id=user,
                workspace_backend="sandbox", outbox_allowed=True,
            )
            out["admitted_access"] = (stamp.get("cloud_checkout") or {}).get("access")
            if request.get("choose_access") and "cloud_checkout" in stamp:
                # The level a creator picks on the copy's chip (an execution's
                # access choice), within what is offered.
                level = request["choose_access"]
                stamp["cloud_checkout"]["access"] = level
                for entry in stamp.get("cloud_connectors") or []:
                    if entry.get("kind") == "cloud_folder_checkout":
                        entry.update(
                            access=level,
                            edits="discarded" if level == "read_only" else "reviewed",
                        )
            out["stamp"] = stamp
            out["selected"] = ids
            job = await db.create_job(
                description=request["description"],
                config_name="general-worker",
                context={**stamp, "gate_id": request["gate_id"]},
                user_id=user, project_id=project, priority=1,
                # Held: paused with a freeze, which the dispatcher never picks.
                status="paused",
                freeze_data={"freeze_type": "gate_hold", "gate_id": request["gate_id"]},
                datasource_ids=ids,
                datasource_selection_provenance={
                    "origin": "default" if request.get("datasource_ids") is None
                    else "explicit",
                    "creation_path": "gate", "effective_work_owner_id": user,
                    "target_project_ids": [project], "datasource_ids": ids,
                    "policy_revisions": revisions,
                },
                datasource_policy_revisions=revisions,
                authority_user_id=user, authority_project_ids=[project],
            )
            out["job_id"] = str(job["id"])
            out["seeding_at_insert"] = (
                _context(await db.get_job(str(job["id"]))).get("cloud_baseline") or {}
            ).get("state")
            await provision_job_repo(
                job_row=job, gitea_client=gitea, postgres_db=db, main_cloud_router=router
            )
            await _wait_tasks()
            fresh = await db.get_job(str(job["id"]))
            out["repo_name"] = fresh.get("repo_name")
        elif action == "job_state":
            job = await db.get_job(request["job_id"])
            context = _context(job)
            baseline = dict(context.get("cloud_baseline") or {})
            baseline["entries"] = sorted((baseline.get("entries") or {}).keys())
            out.update(
                status=job.get("status"), diff_status=job.get("diff_status"),
                exported_at=str(job.get("exported_at") or ""),
                exported_folder_handle=job.get("exported_folder_handle"),
                repo_name=job.get("repo_name"),
                baseline_commit=job.get("cloud_diff_baseline_commit"),
                cloud_checkout=context.get("cloud_checkout"),
                cloud_connectors=context.get("cloud_connectors"),
                cloud_baseline=baseline if context.get("cloud_baseline") else None,
                job_outbox=context.get("job_outbox"),
            )
        elif action == "repo_files":
            found = []
            for path in request["paths"]:
                blob = await gitea.get_file_bytes(request["repo"], path, ref="main")
                found.append(None if blob is None else hashlib.sha256(blob).hexdigest())
            out["files"] = found
            entries = await gitea.list_contents(request["repo"], "", ref="main")
            out["top"] = sorted(entry.get("name", "") for entry in entries or [])
        elif action == "commit":
            for path, content in request["files"].items():
                ok = await gitea.create_or_update_file(
                    request["repo"], path, content, "gate: the agent's commit"
                )
                if not ok:
                    raise SystemExit(f"commit of {path} failed")
            out["committed"] = sorted(request["files"])
        elif action == "complete":
            # The agent's completion report, handled by the orchestrator's
            # completion (the legacy path every completion runs) in this
            # process: a report from a running job. The job stays frozen, so
            # nothing dispatches it between the two statements; no agent is
            # assigned, so no dispatch claim is made.
            from orchestrator.application import completion as completion_composition
            from orchestrator.schemas.job_runtime import JobCompleteRequest
            from orchestrator.services import legacy_job_completion
            out["flipped"] = await db.execute(
                "UPDATE jobs SET status='processing' WHERE id=$1 AND status='paused'",
                UUID(request["job_id"]),
            )
            body = JobCompleteRequest(
                should_stop=True, goal_achieved=True,
                freeze_data={
                    "status": "job_completed", "summary": "gate completion",
                    "deliverables": request["deliverables"],
                },
            )
            try:
                result = await legacy_job_completion.complete_job_legacy(
                    None, request["job_id"], body,
                    dependencies=completion_composition.legacy_completion_dependencies(
                        resources
                    ),
                    _authorized=True,
                )
                out.update(status=200, body=json.dumps(result, default=str)[:4000])
            except Exception as exc:
                out.update(status=getattr(exc, "status_code", 500), body=str(exc)[:2000])
            await _wait_tasks()
        elif action == "read_shared":
            # What the owner sees: their own WebDAV, their own password.
            import httpx
            base = backend._base_url
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.get(
                    f"{base}/remote.php/dav/files/{request['nc_user']}/"
                    f"{request['folder']}/{request['path']}",
                    auth=(request["nc_user"], request["nc_password"]),
                )
            out["status"] = resp.status_code
            out["sha256"] = (
                hashlib.sha256(resp.content).hexdigest() if resp.status_code == 200
                else None
            )
        elif action == "discover":
            gate = request["gate_id"]
            user = await db.fetchrow(
                "SELECT id FROM users WHERE email = $1", request["email"]
            )
            project = await db.fetchrow(
                "SELECT id FROM projects WHERE name = $1", gate
            )
            jobs = await db.fetch(
                "SELECT id, repo_name, exported_folder_handle FROM jobs "
                "WHERE context->>'gate_id' = $1", gate
            )
            out.update(
                user_id=str(user["id"]) if user else None,
                project_id=str(project["id"]) if project else None,
                jobs=[str(row["id"]) for row in jobs],
                repos=[row["repo_name"] for row in jobs if row["repo_name"]],
                outbox_handles=[
                    row["exported_folder_handle"] for row in jobs
                    if row["exported_folder_handle"]
                ],
            )
            if user and request.get("token_hash"):
                await db.execute(
                    "INSERT INTO auth_tokens (user_id, name, token_hash, "
                    "token_prefix, kind, scope, expires_at) VALUES ($1, $2, $3, $4, "
                    "'mcp', 'user', now() + interval '1 hour')",
                    user["id"], request["token_name"], request["token_hash"],
                    request["token_prefix"],
                )
        elif action == "cleanup":
            done = {}
            for handle in request.get("outbox_handles") or []:
                try:
                    await backend.delete_session_folder(
                        SessionFolderHandle.from_db(handle, backend=backend.backend_id),
                        if_exists=True,
                    )
                    done[handle] = "deleted"
                except Exception as exc:
                    done[handle] = f"failed: {type(exc).__name__}"
            out["outbox"] = done
            if request.get("user_id"):
                await db.execute(
                    "UPDATE auth_tokens SET revoked_at=now() WHERE user_id=$1",
                    UUID(request["user_id"]),
                )
                out["user_deleted"] = await db.delete_user(request["user_id"])
            if request.get("nc_user"):
                resp = await backend._client.delete(
                    f"/ocs/v1.php/cloud/users/{request['nc_user']}",
                    params={"format": "json"},
                )
                out["nc_user"] = resp.status_code
            if request.get("gitea_user"):
                http_client, base = gitea._get_client(), gitea._url
                resp = await http_client.delete(
                    f"{base}/api/v1/admin/users/{request['gitea_user']}",
                    params={"purge": "true"},
                )
                out["gitea_user"] = resp.status_code
        elif action == "residue":
            gate = request["gate_id"]
            out["jobs"] = await db.fetchval(
                "SELECT count(*) FROM jobs WHERE context->>'gate_id' = $1", gate
            )
            out["projects"] = await db.fetchval(
                "SELECT count(*) FROM projects WHERE name = $1", gate
            )
            out["users"] = await db.fetchval(
                "SELECT count(*) FROM users WHERE email = $1", request["email"]
            )
            out["agents"] = await db.fetchval(
                "SELECT count(*) FROM agents WHERE hostname = $1", gate
            )
            out["datasources"] = await db.fetchval(
                "SELECT count(*) FROM datasources WHERE managed_key = ANY($1)",
                request["managed_keys"],
            )
            out["repos"] = [
                repo for repo in request["repos"]
                if await gitea.get_branch_head_sha(repo, "main")
            ]
            folders = await backend._client.get(
                "/index.php/apps/groupfolders/folders", params={"format": "json"}
            )
            data = folders.json().get("ocs", {}).get("data", {}) or {}
            values = data.values() if isinstance(data, dict) else data
            out["group_folders"] = [
                folder.get("mount_point") for folder in values
                if gate in str(folder.get("mount_point", ""))
            ]
            resp = await backend._client.get(
                f"/ocs/v1.php/cloud/users/{request['nc_user']}",
                params={"format": "json"},
            )
            out["nc_user"] = resp.json().get("ocs", {}).get("meta", {}).get(
                "statuscode"
            ) == 100
        else:
            raise SystemExit(f"unknown action {action!r}")
    finally:
        await db.close()
    print(json.dumps(out, default=str))


asyncio.run(main())
"""
)


#: The runner's actions a retry repeats harmlessly: reads, idempotent writes
#: (a commit of the same content, a re-sync) and the cleanup.
_RETRY_SAFE = frozenset(
    {
        "project_state",
        "put_files",
        "read_cloud",
        "job_state",
        "repo_files",
        "commit",
        "read_shared",
        "discover",
        "cleanup",
        "residue",
    }
)
#: How a reload of the orchestrator (Tilt syncing another builder's edit
#: restarts uvicorn) shows on a ``kubectl exec`` into it.
_TRANSIENT = re.compile(
    r"container not found|unable to upgrade connection|task [0-9a-f]+ not found"
    r"|exit 137|Connection reset by peer|Connection refused|failed to exec"
)


def _wait_for_orchestrator() -> None:
    wait_for(
        "the orchestrator to accept execs again",
        lambda: run(
            K
            + ["exec", ORCHESTRATOR, "-c", ORCHESTRATOR_CONTAINER, "--"]
            + ["curl", "-s", "-o", "/dev/null", "http://localhost:8085/api/health"]
        )[0]
        == 0,
        timeout=240,
        interval=5,
    )


def pod(action: str, **payload: Any) -> dict[str, Any]:
    """One action of the runner, in the orchestrator pod.

    A retry-safe action survives a reload of the orchestrator mid-call; any
    other fails the gate, saying so, rather than running twice.
    """
    retry_safe = action in _RETRY_SAFE or (
        action == "api"
        and (
            payload.get("method") in {"GET", "DELETE"}
            or str(payload.get("path", "")).endswith("/export-to-shared-folder")
        )
    )
    attempts = 4 if retry_safe else 1
    for attempt in range(1, attempts + 1):
        try:
            out = command(
                K
                + ["exec", "-i", ORCHESTRATOR, "-c", ORCHESTRATOR_CONTAINER, "--"]
                + ["python", "-c", _RUNNER],
                data=json.dumps({"action": action, **payload}) + "\n",
                timeout=300,
            )
            break
        except GateError as exc:
            if not _TRANSIENT.search(str(exc)):
                raise
            if attempt == attempts:
                raise GateError(
                    f"{action}: the orchestrator restarted mid-call"
                    + ("" if retry_safe else " (not retried: not idempotent)")
                ) from exc
            print(f"retry: {action} after an orchestrator reload", flush=True)
            _wait_for_orchestrator()
    last = out.strip().splitlines()[-1] if out.strip() else "{}"
    try:
        return json.loads(last)
    except json.JSONDecodeError as exc:
        raise GateError(f"{action}: unreadable output: {last[-300:]}") from exc


# ---------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------


@dataclass
class Results:
    passed: list[str] = field(default_factory=list)
    failed: list[str] = field(default_factory=list)

    def check(self, name: str, ok: bool, detail: str = "") -> bool:
        line = f"{'PASS' if ok else 'FAIL'}  {name}" + (f": {detail}" if detail else "")
        print(_scrub(line), flush=True)
        (self.passed if ok else self.failed).append(name)
        return ok


@dataclass
class Fixtures:
    gate_id: str
    token: str = ""
    nc_password: str = ""
    user_id: str | None = None
    project_id: str | None = None
    jobs: dict[str, str] = field(default_factory=dict)
    repos: list[str] = field(default_factory=list)
    outbox_handles: list[str] = field(default_factory=list)

    @property
    def email(self) -> str:
        return f"{self.gate_id}@{ACCOUNT_DOMAIN}"


def api(
    fx: Fixtures,
    method: str,
    path: str,
    body: Any = None,
    *,
    pick: list[str] | None = None,
) -> tuple[int, Any]:
    result = pod("api", method=method, path=path, body=body, token=fx.token, pick=pick)
    text = result.get("body") or ""
    try:
        payload = json.loads(text) if text else None
    except json.JSONDecodeError:
        payload = text
    return int(result.get("status") or 0), payload


def served_matches() -> list[str]:
    """The files the orchestrator pod does not serve byte for byte."""
    stale = []
    for rel in SERVED:
        local = hashlib.sha256((ROOT / rel).read_bytes()).hexdigest()
        rc, out, _ = run(
            K
            + ["exec", ORCHESTRATOR, "-c", ORCHESTRATOR_CONTAINER, "--"]
            + ["sha256sum", f"{POD_ROOT}/{rel}"]
        )
        if rc or not out.startswith(local):
            stale.append(rel)
    return stale


def render_brief(state: dict[str, Any]) -> str:
    """The task brief's cloud block, rendered by this checkout's agent code
    from what the job carries (the agent image is not this gate's)."""
    sys.path.insert(0, str(ROOT / "src"))
    from agent.core.deliverables import format_cloud_connectors_block

    return format_cloud_connectors_block(
        state.get("cloud_connectors"), state.get("cloud_baseline")
    )


def setup(fx: Fixtures, results: Results) -> str:
    """The account, the project and its folder's files; returns the slug."""
    fx.token = secret("srw_" + secrets.token_urlsafe(32))
    fx.nc_password = secret("Gate-" + secrets.token_urlsafe(24))
    account = pod(
        "account",
        nc_user=fx.gate_id,
        nc_password=fx.nc_password,
        email=fx.email,
        name=f"Gate {fx.gate_id}",
        username=fx.gate_id,
        sub=f"gate-{fx.gate_id}",
        token_name=fx.gate_id,
        token_hash=hashlib.sha256(fx.token.encode()).hexdigest(),
        token_prefix=fx.token[:12],
    )
    fx.user_id = account["user_id"]
    if account.get("nc_code") != 100:
        raise GateError(f"the Nextcloud account was not created ({account})")

    status, project = api(
        fx, "POST", "/api/projects", {"name": fx.gate_id, "user_id": fx.user_id}
    )
    if status != 200 or not isinstance(project, dict):
        raise GateError(f"project create answered {status}: {project}")
    fx.project_id = str(project["id"])
    state = wait_for(
        "the project's folder and its native connectors",
        lambda: (
            s
            if (
                s := pod("project_state", project_id=fx.project_id, user_id=fx.user_id)
            )["handle"]
            and {"project-cloud", "project-cloud-checkout"} <= set(s["rows"])
            else None
        ),
        timeout=180,
        interval=5,
    )
    results.check(
        "preflight: the project has its folder copy connector",
        "project-cloud-checkout" in state["rows"],
        f"rows {sorted(state['rows'])}",
    )
    results.check(
        "preflight: the owner has an outbox connector", bool(state.get("outbox"))
    )
    put = pod(
        "put_files",
        project_id=fx.project_id,
        files={
            TEXT_FILE: base64.b64encode(TEXT_BODY).decode(),
            BINARY_FILE: base64.b64encode(BINARY_BODY).decode(),
        },
    )
    if put["listed"] != sorted([BINARY_FILE, TEXT_FILE]):
        raise GateError(f"the project folder holds {put['listed']}")
    return re.sub(r"[^a-z0-9]", "_", fx.gate_id.lower()).strip("_")


def create_job(fx: Fixtures, name: str, **payload: Any) -> dict[str, Any]:
    created = pod(
        "create_job",
        gate_id=fx.gate_id,
        user_id=fx.user_id,
        project_id=fx.project_id,
        description=f"{fx.gate_id} {name}",
        **payload,
    )
    fx.jobs[name] = created["job_id"]
    if created.get("repo_name"):
        fx.repos.append(created["repo_name"])
    return created


def job_state(job_id: str) -> dict[str, Any]:
    return pod("job_state", job_id=job_id)


def check_copy(fx: Fixtures, results: Results, slug: str) -> None:
    created = create_job(fx, "copy", choose_access="read_only")
    stamp = created["stamp"]
    results.check(
        "copy: a copy chosen read-only is stamped read-only",
        (stamp.get("cloud_checkout") or {}).get("access") == "read_only"
        and (stamp.get("cloud_connectors") or [{}])[0].get("edits") == "discarded",
        json.dumps(stamp.get("cloud_checkout")),
    )
    results.check(
        "copy: the row holds dispatch from its insert (seeding)",
        created.get("seeding_at_insert") == "seeding",
    )
    state = job_state(created["job_id"])
    baseline = state.get("cloud_baseline") or {}
    results.check(
        "copy: the seed copied both files, the binary too",
        baseline.get("state") == "ready"
        and baseline.get("entries") == sorted([BINARY_FILE, TEXT_FILE])
        and (baseline.get("seeded"), baseline.get("skipped_binary")) == (2, 0)
        and baseline.get("failed") == 0,
        json.dumps(baseline),
    )
    files = pod(
        "repo_files",
        repo=state["repo_name"],
        paths=[f"projects/{slug}/{TEXT_FILE}", f"projects/{slug}/{BINARY_FILE}"],
    )
    results.check(
        "copy: the job's repository holds projects/<slug>/",
        files["files"]
        == [
            hashlib.sha256(TEXT_BODY).hexdigest(),
            hashlib.sha256(BINARY_BODY).hexdigest(),
        ],
        json.dumps(files["files"]),
    )
    brief = render_brief(state)
    results.check(
        "copy: the task brief names the copy and says edits go nowhere",
        f"`projects/{slug}/` in this workspace is a copy" in brief
        and "2 file(s) were copied." in brief
        and "SRW never applies the copy's changes" in brief
        and "## Cloud Outbox" in brief,
        brief.strip().splitlines()[2][:160] if brief.strip() else "empty",
    )
    pod(
        "commit",
        repo=state["repo_name"],
        files={
            f"projects/{slug}/{TEXT_FILE}": EDITED_BODY,
            DELIVERABLE: f"# Report of {fx.gate_id}\n",
        },
    )
    completed = pod("complete", job_id=created["job_id"], deliverables=[DELIVERABLE])
    results.check(
        "copy: the completion report is accepted",
        completed.get("status") == 200,
        f"{completed.get('status')} {str(completed.get('body'))[:300]}",
    )
    state = job_state(created["job_id"])
    results.check(
        "copy: a read-only copy's commit is not captured and the job completes",
        state["status"] == "completed"
        and state["diff_status"] is None
        and "capture" not in (state.get("cloud_checkout") or {}),
        f"status {state['status']}, diff {state['diff_status']}",
    )
    status, job = api(
        fx,
        "GET",
        f"/api/jobs/{created['job_id']}",
        pick=["status", "diff_status", "cloud_review_mode"],
    )
    results.check(
        "copy: its review mode is open_folder",
        status == 200
        and isinstance(job, dict)
        and job.get("cloud_review_mode") == "open_folder",
        f"{status} {job.get('cloud_review_mode') if isinstance(job, dict) else ''}",
    )
    cloud = pod("read_cloud", project_id=fx.project_id, path=TEXT_FILE)
    results.check(
        "copy: the cloud folder is unchanged",
        cloud["sha256"] == hashlib.sha256(TEXT_BODY).hexdigest(),
    )

    # §8 item 6: the outbox, delivered by that completion.
    outbox = state.get("job_outbox") or {}
    if state.get("exported_folder_handle"):
        fx.outbox_handles.append(state["exported_folder_handle"])
    results.check(
        "outbox: the completion copied the deliverable and shared the folder",
        outbox.get("state") == "delivered"
        and outbox.get("trigger") == "completion"
        and outbox.get("files") == 1
        and outbox.get("shared") is True
        and bool(state.get("exported_at")),
        json.dumps(outbox),
    )
    shared = pod(
        "read_shared",
        nc_user=fx.gate_id,
        nc_password=fx.nc_password,
        folder=outbox.get("folder") or "",
        path="report.md",
    )
    results.check(
        "outbox: the owner reads the file in their own cloud",
        shared.get("status") == 200
        and shared.get("sha256")
        == hashlib.sha256(f"# Report of {fx.gate_id}\n".encode()).hexdigest(),
        f"WebDAV {shared.get('status')}",
    )

    # §8 item 7: the re-sync endpoint, for a cloud project's job.
    for attempt in (1, 2):
        status, body = api(
            fx, "POST", f"/api/jobs/{created['job_id']}/export-to-shared-folder"
        )
        results.check(
            f"resync: the cloud project's job re-syncs (attempt {attempt})",
            status == 200
            and isinstance(body, dict)
            and body.get("files_copied") == 1
            and body.get("shared") is True,
            f"{status} {str(body)[:200]}",
        )


def check_write_back(fx: Fixtures, results: Results, slug: str) -> None:
    created = create_job(fx, "write-back")
    state = job_state(created["job_id"])
    results.check(
        "write-back: admission stamps reviewed_write_back (the highest offered) "
        "and the copy is seeded",
        created.get("admitted_access") == "reviewed_write_back"
        and (state.get("cloud_checkout") or {}).get("access") == "reviewed_write_back"
        and (state.get("cloud_baseline") or {}).get("state") == "ready",
        f"admitted {created.get('admitted_access')}",
    )
    pod(
        "commit",
        repo=state["repo_name"],
        files={
            f"projects/{slug}/{TEXT_FILE}": EDITED_BODY,
            DELIVERABLE: f"# Report of {fx.gate_id}\n",
        },
    )
    completed = pod("complete", job_id=created["job_id"], deliverables=[DELIVERABLE])
    state = job_state(created["job_id"])
    if state.get("exported_folder_handle"):
        fx.outbox_handles.append(state["exported_folder_handle"])
    capture = (state.get("cloud_checkout") or {}).get("capture") or {}
    results.check(
        "write-back: the commit is captured for review",
        completed.get("status") == 200
        and state["diff_status"] == "pending"
        and capture.get("state") == "captured"
        and capture.get("files") == 1
        and state["status"] == "pending_review",
        f"status {state['status']}, diff {state['diff_status']}, capture {capture}",
    )
    status, job = api(
        fx,
        "GET",
        f"/api/jobs/{created['job_id']}",
        pick=["status", "diff_status", "cloud_review_mode"],
    )
    results.check(
        "write-back: its review mode is diff",
        status == 200
        and isinstance(job, dict)
        and job.get("cloud_review_mode") == "diff",
    )
    status, body = api(fx, "POST", f"/api/jobs/{created['job_id']}/accept")
    results.check(
        "write-back: the owner's accept answers 200",
        status == 200,
        f"{status} {str(body)[:300]}",
    )
    cloud = pod("read_cloud", project_id=fx.project_id, path=TEXT_FILE)
    state = job_state(created["job_id"])
    results.check(
        "write-back: the cloud folder holds the job's edit",
        cloud["sha256"] == hashlib.sha256(EDITED_BODY.encode()).hexdigest()
        and state["diff_status"] == "accepted"
        and state["status"] == "completed",
        f"cloud {cloud['text'][:60]!r}, diff {state['diff_status']}, status {state['status']}",
    )


def check_empty(fx: Fixtures, results: Results) -> None:
    created = create_job(fx, "empty", datasource_ids=[])
    state = job_state(created["job_id"])
    top = pod("repo_files", repo=state["repo_name"], paths=[])["top"]
    results.check(
        "empty: an explicit empty selection gets no copy",
        created["selected"] == []
        and created["stamp"] == {}
        and state.get("cloud_checkout") is None
        and state.get("cloud_baseline") is None
        and state.get("cloud_connectors") is None
        and "projects" not in top,
        f"stamp {created['stamp']}, top {top}",
    )


def cleanup(fx: Fixtures) -> None:
    """Remove what this gate id created, found by the gate id (so a run that
    died, or ``--sweep``, cleans up the same way): the jobs and the project
    over REST as their owner (a fresh token), then the outbox folders and the
    accounts."""
    token = secret("srw_" + secrets.token_urlsafe(32))
    found = pod(
        "discover",
        gate_id=fx.gate_id,
        email=fx.email,
        token_name=f"{fx.gate_id}-cleanup",
        token_hash=hashlib.sha256(token.encode()).hexdigest(),
        token_prefix=token[:12],
    )
    fx.user_id = fx.user_id or found["user_id"]
    fx.project_id = fx.project_id or found["project_id"]
    fx.repos = sorted(set(fx.repos) | set(found["repos"]))
    owner = Fixtures(gate_id=fx.gate_id, token=token)
    for job_id in found["jobs"]:
        try:
            status, _ = api(owner, "DELETE", f"/api/jobs/{job_id}")
            print(f"cleanup: job {job_id[:8]} -> {status}", flush=True)
        except GateError as exc:
            print(f"cleanup: job {job_id[:8]} failed: {exc}", flush=True)
    if found["project_id"]:
        try:
            status, _ = api(owner, "DELETE", f"/api/projects/{found['project_id']}")
            print(f"cleanup: project -> {status}", flush=True)
        except GateError as exc:
            print(f"cleanup: project failed: {exc}", flush=True)
    try:
        out = pod(
            "cleanup",
            outbox_handles=sorted(
                set(fx.outbox_handles) | set(found["outbox_handles"])
            ),
            user_id=found["user_id"],
            nc_user=fx.gate_id,
            gitea_user=fx.gate_id,
        )
        print(f"cleanup: {json.dumps(out)}", flush=True)
    except GateError as exc:
        print(f"cleanup: accounts failed: {exc}", flush=True)


def residue(fx: Fixtures, results: Results) -> None:
    keys = [f"cloud-outbox:{fx.user_id}"] if fx.user_id else []
    if fx.project_id:
        keys += [
            f"project-cloud:{fx.project_id}",
            f"project-cloud-checkout:{fx.project_id}",
        ]
    left = pod(
        "residue",
        gate_id=fx.gate_id,
        email=fx.email,
        managed_keys=keys,
        repos=fx.repos,
        nc_user=fx.gate_id,
    )
    clean = (
        left["jobs"] == 0
        and left["projects"] == 0
        and left["users"] == 0
        and left["agents"] == 0
        and left["datasources"] == 0
        and not left["repos"]
        and not left["group_folders"]
        and not left["nc_user"]
    )
    results.check("cleanup: nothing this run created is left", clean, json.dumps(left))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--run", action="store_true", help="run (default: plan)")
    parser.add_argument("--confirm", default="", help=f"must be {LOCAL_CONFIRMATION}")
    parser.add_argument("--gate-id", default="", help="reuse a gate id (oc-<10 hex>)")
    parser.add_argument(
        "--sweep",
        default="",
        metavar="GATE_ID",
        help="only remove what an earlier run of GATE_ID left, then check",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the gate; 2 for a run outside the local disposable boundary or
    a cluster the gate could not observe."""
    try:
        return _main(build_parser().parse_args(argv))
    except (GateError, SafetyError) as exc:
        print(f"ERROR: {_scrub(str(exc))}", file=sys.stderr)
        return 2


def _main(args: argparse.Namespace) -> int:
    gate_id = args.sweep or args.gate_id or f"oc-{secrets.token_hex(5)}"
    if not _GATE_ID_RE.fullmatch(gate_id):
        raise SafetyError(f"gate id {gate_id!r} is not oc-<10 hex>")
    print(f"gate id {gate_id}; context {LOCAL_CONTEXT}/{LOCAL_NAMESPACE}")
    print(
        "plan: account + Nextcloud account, project with two files, jobs copy / "
        "write-back / empty (held, never dispatched), the orchestrator's own "
        "completion in-process, re-sync and accept over REST, then cleanup and "
        "a residue check"
    )
    if not args.run:
        print("dry run: pass --run --confirm " + LOCAL_CONFIRMATION)
        return 0
    if args.confirm != LOCAL_CONFIRMATION:
        raise SafetyError(f"--run needs --confirm {LOCAL_CONFIRMATION}")
    context = command(["kubectl", "config", "get-contexts", "-o", "name"])
    if LOCAL_CONTEXT not in context.split():
        raise SafetyError(f"no {LOCAL_CONTEXT} context")

    results = Results()
    fx = Fixtures(gate_id=gate_id)
    if args.sweep:
        cleanup(fx)
        residue(fx, results)
        return 0 if not results.failed else 1
    stale = served_matches()
    if not results.check(
        "preflight: the orchestrator serves this checkout's slice 6 modules",
        not stale,
        ", ".join(stale),
    ):
        return 1
    try:
        slug = setup(fx, results)
        check_copy(fx, results, slug)
        check_write_back(fx, results, slug)
        check_empty(fx, results)
    except GateError as exc:
        results.check("gate", False, str(exc))
    finally:
        cleanup(fx)
        try:
            residue(fx, results)
        except GateError as exc:
            results.check("cleanup: residue check", False, str(exc))
    print(f"\n{len(results.passed)} passed, {len(results.failed)} failed", flush=True)
    return 0 if not results.failed else 1


if __name__ == "__main__":
    sys.exit(main())
