#!/usr/bin/env python3
"""Local k3d gate for main cloud slice 3.5 (and 6.1): cloud folders as connectors.

Design: knowledge-base/knowledge/features/main_cloud_as_connectors.md, "Slice 3
plan (2026-10-10)" 3.3 to 3.6 and "Overnight run: slices 3 to 8" (the session
outbox). A session's cloud folders are its attached cloud connectors, each
bound on its own: its project's own ``cloud_folder`` (``project-cloud:``) at
``cloud/<project>``, the owner's ``cloud_outbox`` (``cloud-outbox:``) at
``cloud/outbox``. One that cannot be mounted is named with its reason and
nothing takes its place; an explicit empty list mounts nothing.

Template: scripts/k3d-cloud-mount-sidecar-gate.py (D7), whose envelope and
helpers this gate reuses (loaded from that file): dry-run by default, the
exact k3d-srw/srw context, passwords only on ``kubectl exec -i`` stdin and
scrubbed from every printed line, ``cap_memory()`` in every program run in a
pod, and a cleanup in ``finally`` that touches only what this run created,
followed by a residue check by gate id. It needs what the D7 gate needs (the
in-pod plane with both images, the bundled Nextcloud with its files on the
PVC); the protected phase needs ``agent.protectedCloudModeEnabled: "true"``.

Fixtures (all disposable, all named after the gate id): projects ``main``,
``broken`` and ``prot``; sandbox sessions ``omitted``, ``empty`` and ``nochip``
in main, ``broken`` and ``foreign`` in broken, ``prot`` in prot. Only the
omitted session gets a turn ("Reply with the single word ready."), so its
system prompt can be read from the audit store.

Checks (each printed PASS/FAIL; the exit status is 0 only if all pass):

  preflight   Tilt reports srw ``ok``; every orchestrator pod serves this
              checkout's mount-rule modules and every stateless agent pod its
              prompt block, byte for byte; the in-pod plane is on
  backfill    every cloud project of the active installation has exactly one
              ``project-cloud:`` row linked to it, each approved user exactly
              one ``cloud-outbox:`` row (``Session outbox``, scope all, auto
              attach, created by the user), and the orchestrator logged both
              startup count lines; each gate project gets its row
  omitted     a session in main with ``datasource_ids`` omitted attaches main's
              cloud folder and the owner's outbox; SRW created its session
              folder and recorded it; its Pod's plan mounts the project folder
              at ``cloud/<project>`` and the outbox at ``cloud/outbox`` (mount
              ids are the connector ids, nothing excluded) and both are mounted
              in the workspace; a write as agent-host into the outbox reaches
              agent-service's ``sessions/<id>``; after one turn its system
              prompt's ``<cloud_folders>`` block names both, the outbox as not
              the project folder
  empty       ``datasource_ids: []``: nothing attached, no session folder, no
              outbox record, a Pod without sidecars or plan
  nochip      only the outbox attached: the plan mounts the outbox alone and
              names nothing as excluded, so no project folder takes its place
  broken      broken's folder handle pointed at a folder that does not exist:
              the plan still mounts it and the outbox, and nothing else; the
              project folder reads unavailable (not_found) in its status file
              while the outbox is mounted
  foreign     broken's cloud folder row naming another project's folder:
              excluded as config_invalid, the outbox mounted, nothing else
  protected   (protected mode on) a legacy ``protected_cloud: true`` session in
              prot: a protected plan (the reader lower alone, from this
              runtime's grant) that names the outbox ``beside_protected``; no
              session folder is created for it
  cleanup     nothing this run created is left: sessions (and their pods),
              projects (and their cloud folder rows), plan ConfigMaps and
              credential Secrets; a repointed handle is restored first

Run with the repository venv on the k3d-srw cluster, alone: this is a mutating
gate.

  .venv/bin/python scripts/k3d-cloud-folder-connector-gate.py          # plan
  .venv/bin/python scripts/k3d-cloud-folder-connector-gate.py \\
      --run --confirm LOCAL-K3D-DISPOSABLE [--require-protected] [--json out.json]

A run that was interrupted is swept by its id:

  .venv/bin/python scripts/k3d-cloud-folder-connector-gate.py \\
      --run --confirm LOCAL-K3D-DISPOSABLE --sweep --gate-id cfc-0123456789
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import re
import secrets
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[1]
_D7_GATE = ROOT / "scripts" / "k3d-cloud-mount-sidecar-gate.py"


def _load_d7():
    name = "_k3d_cloud_mount_sidecar_gate"
    if name in sys.modules:
        return sys.modules[name]
    spec = importlib.util.spec_from_file_location(name, _D7_GATE)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


d7 = _load_d7()
GateError = d7.GateError
SafetyError = d7.SafetyError
K = d7.K
LOCAL_CONTEXT = d7.LOCAL_CONTEXT
LOCAL_NAMESPACE = d7.LOCAL_NAMESPACE
LOCAL_CONFIRMATION = d7.LOCAL_CONFIRMATION
ORCHESTRATOR = d7.ORCHESTRATOR
ORCHESTRATOR_CONTAINER = d7.ORCHESTRATOR_CONTAINER
AGENT_CONTAINER = d7.AGENT_CONTAINER
NEXTCLOUD = d7.NEXTCLOUD
NEXTCLOUD_CONTAINER = d7.NEXTCLOUD_CONTAINER
SUPERVISOR_CONTAINER = d7.SUPERVISOR_CONTAINER
OPENER_CONTAINER = d7.OPENER_CONTAINER
DEFAULT_MODEL = d7.DEFAULT_MODEL
TURN_PROMPT = d7.TURN_PROMPT
AUDIT_POD = "srw-auditdb-0"
run, command, sql, sql_json, lit, wait_for = (
    d7.run,
    d7.command,
    d7.sql,
    d7.sql_json,
    d7.lit,
    d7.wait_for,
)
container, recorded_plan, credential_secret_name = (
    d7.container,
    d7.recorded_plan,
    d7.credential_secret_name,
)

_GATE_ID_RE = re.compile(r"cfc-[0-9a-f]{10}\Z")
#: The closed reasons an attached cloud folder may be excluded with
#: (``cloud_connector_mounts.EXCLUSION_REASONS`` and the per-folder grants').
EXCLUSION_REASONS = frozenset(
    {
        "access_unavailable",
        "config_invalid",
        "not_eligible",
        "no_session_folder",
        "backend_unavailable",
        "unbuildable",
        "beside_protected",
        "too_many_mounts",
        "grant_unavailable",
        "reader_unavailable",
        "cloud_not_connected",
        "cloud_grant_revoked",
    }
)
_UUID_RE = d7._UUID_RE

#: What every orchestrator pod must serve byte for byte.
SERVED = (
    "src/orchestrator/services/cloud_connector_mounts.py",
    "src/orchestrator/services/session_cloud_preparation.py",
    "src/orchestrator/services/cloud_outbox_connectors.py",
    "src/orchestrator/services/agent_cloud_mounts.py",
    "src/orchestrator/services/cloud_mount_plan.py",
    "src/orchestrator/services/thread_admission.py",
    "src/orchestrator/services/connector_drivers/cloud_folder.py",
    "src/orchestrator/services/connector_drivers/cloud_outbox.py",
    "src/orchestrator/services/project_cloud_connectors.py",
    "src/orchestrator/application/workspace.py",
    "src/orchestrator/application/lifecycle.py",
)
#: What every stateless agent pod must serve: this slice's prompt block, by
#: a marker per file (the agent image is rebuilt for every agent-side edit of
#: the shared tree, so its bytes trail other work by minutes).
AGENT_SERVED = {
    "src/shared/runtime/services/cloud_mount/sidecar.py": "def describe_mounted(",
    "src/shared/runtime/core/loader.py": "Cloud folders in this session's workspace:",
    "src/agent/api/persistent_session.py": "def _config_with_cloud_folders(",
}

#: An existing file's name in a session folder, by the folder's id: ``$1``.
_NEXTCLOUD_SESSION_FILE = r"""
data=$(php /var/www/html/occ config:system:get datadirectory)
find "$data"/*/files/sessions/"$1" -maxdepth 1 -type f -name "$2" 2>/dev/null \
  | while read -r path; do sha256sum "$path" | cut -d' ' -f1; done
"""


def audit_sql(query: str) -> str:
    """One read on the audit store (no secret in ``query``)."""
    return command(
        K
        + ["exec", AUDIT_POD, "--", "psql", "-U", "srw", "-d", "srw_audit"]
        + ["-v", "ON_ERROR_STOP=1", "-tAc", query]
    )


def committed_bytes(paths: tuple[str, ...]) -> dict[str, str]:
    """``expected_bytes`` of the committed files (``git show HEAD:<path>``)."""
    return {
        path: hashlib.sha256(
            subprocess.run(
                ["git", "-C", str(ROOT), "show", f"HEAD:{path}"],
                capture_output=True,
                check=True,
                timeout=60,
            ).stdout
        ).hexdigest()
        for path in paths
    }


def plan_mounts(plan: dict[str, Any]) -> list[tuple[str, str, str]]:
    """``(name, mount_kind, mount_id)`` of a recorded plan's mounts."""
    return sorted(
        (
            str(m.get("name") or ""),
            str(m.get("mount_kind") or ""),
            str(m.get("mount_id") or ""),
        )
        for m in plan.get("mounts") or []
    )


def plan_exclusions(plan: dict[str, Any]) -> list[tuple[str, str, str]]:
    """``(source_ref, mount_kind, reason)`` of a recorded plan's exclusions."""
    return sorted(
        (
            str(e.get("source_ref") or ""),
            str(e.get("mount_kind") or ""),
            str(e.get("reason") or ""),
        )
        for e in plan.get("excluded") or []
    )


def slug(name: str) -> str:
    """``thread_mount_rows.slugify_mount_name``, as the planner names a
    project's folder."""
    out = "".join(ch.lower() if ch.isalnum() else "_" for ch in name).strip("_")
    return out or "project"


def prompt_block(request_text: str) -> str:
    """The ``<cloud_folders>`` block of an archived LLM request, unescaped
    enough to read (the request is JSON text)."""
    match = re.search(r"<cloud_folders>(.*?)</cloud_folders>", request_text, re.S)
    return match.group(1).replace("\\n", "\n") if match else ""


PLAN = [
    "preflight: Tilt srw ok; orchestrator pods serve this checkout's mount rule "
    "and stateless agent pods its prompt block; the in-pod plane is on",
    "backfill: one project-cloud row per cloud project, one cloud-outbox row "
    "per approved user, both startup count lines logged; each gate project "
    "gets its row",
    "omitted: list omitted -> main's folder and the outbox attached, the "
    "session folder created and recorded, the plan mounts cloud/<project> and "
    "cloud/outbox (connector ids, nothing excluded), both mounted; a write into "
    "the outbox reaches agent-service's sessions/<id>; the prompt block names "
    "both, the outbox as not the project folder",
    "empty: datasource_ids [] -> nothing attached, no session folder, a Pod "
    "without sidecars or plan",
    "nochip: only the outbox -> the outbox mounts alone, nothing substituted",
    "broken: the project's handle points at a missing folder -> both still "
    "planned, the project folder unavailable (not_found), the outbox mounted",
    "foreign: the cloud folder row names another project -> excluded "
    "config_invalid, the outbox mounted, nothing else",
    "protected: a legacy protected_cloud session -> a protected plan naming the "
    "outbox beside_protected, no session folder",
    "cleanup: sessions, projects and their rows, pods, plan ConfigMaps and "
    "Secrets; a repointed handle restored; residue check by gate id",
]


#: The session phases, in their order (``--phases`` picks some).
PHASES = ("omitted", "empty", "nochip", "broken", "foreign", "protected")


@dataclass
class Session:
    label: str
    project_label: str
    thread: str = ""
    pod: str = ""
    plan: dict[str, Any] | None = None


class CloudFolderConnectorGate:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.gate_id = args.gate_id or f"cfc-{secrets.token_hex(5)}"
        self.report = d7.Report(self.gate_id)
        self.api = d7.Api(args.user, args.password)
        self.started = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        self.user_id = ""
        self.outbox_id = ""
        self.protected_mode = False
        self.projects: dict[str, str] = {}  # label -> project id
        self.folders: dict[str, str] = {}  # label -> project-cloud row id
        self.sessions: dict[str, Session] = {}
        self.objects: set[str] = set()
        self.restore: list[str] = []  # SQL that undoes a repointed handle
        self.swept_threads: list[str] = []

    # -- naming --------------------------------------------------------------
    def name(self, label: str) -> str:
        return f"{self.gate_id} {label}"

    def title(self, label: str) -> str:
        return f"Cloud folder connector gate {self.gate_id} {label}"

    # -- cluster reads ---------------------------------------------------------
    def served(self) -> list[str]:
        """Every pod that is not serving this checkout's bytes of a file:
        neither the working tree's nor, while another edit of the shared tree
        is still rolling out, the committed ones."""
        problems: list[str] = []
        for component, container_name, paths in (
            ("orchestrator", ORCHESTRATOR_CONTAINER, SERVED),
            ("agent-stateless", AGENT_CONTAINER, None),
        ):
            pods = [
                pod
                for pod in json.loads(
                    command(
                        K
                        + ["get", "pods", "-l"]
                        + [f"{d7._SELECTOR},app.kubernetes.io/component={component}"]
                        + ["-o", "json"]
                    )
                )["items"]
                if not pod["metadata"].get("deletionTimestamp")
            ]
            if not pods:
                problems.append(f"no {component} pod")
            for pod in pods:
                if paths is None:
                    missing = [
                        path
                        for path, marker in AGENT_SERVED.items()
                        if run(
                            K
                            + ["exec", pod["metadata"]["name"], "-c", container_name]
                            + ["--", "grep", "-qF", marker, f"{d7.POD_ROOT}/{path}"],
                            timeout=60,
                        )[0]
                    ]
                    if missing:
                        problems.append(
                            f"{pod['metadata']['name']} lacks this slice in {missing}"
                        )
                    continue
                stale = None
                for expected in (d7.expected_bytes(paths), committed_bytes(paths)):
                    found = json.loads(
                        command(
                            K
                            + ["exec", "-i", pod["metadata"]["name"]]
                            + ["-c", container_name, "--"]
                            + ["python", "-c", d7._HASH_PROGRAM, d7.POD_ROOT],
                            data=json.dumps(expected),
                        ).splitlines()[-1]
                    )
                    files = set(found.get("stale") or [])
                    stale = files if stale is None else stale & files
                if stale:
                    problems.append(f"{pod['metadata']['name']} stale: {sorted(stale)}")
        return problems

    def orchestrator_env(self, name: str) -> str:
        rc, out, _err = run(
            K
            + ["exec", ORCHESTRATOR, "-c", ORCHESTRATOR_CONTAINER, "--"]
            + ["printenv", name],
            timeout=60,
        )
        return out.strip() if rc == 0 else ""

    def workspace_pod(self, thread: str) -> dict:
        def probe() -> dict | None:
            items = json.loads(
                command(
                    K + ["get", "pods", "-l", f"srw/thread-id={thread}", "-o", "json"]
                )
            )["items"]
            ready = [
                pod
                for pod in items
                if pod["metadata"]["name"].startswith("ws-thread-")
                and not pod["metadata"].get("deletionTimestamp")
                and pod.get("status", {}).get("phase") == "Running"
                and any(
                    status.get("name") == "workspace" and status.get("ready")
                    for status in pod.get("status", {}).get("containerStatuses") or []
                )
            ]
            return ready[0] if len(ready) == 1 else None

        return wait_for(
            f"workspace pod of {thread}", probe, timeout=self.args.pod_timeout
        )

    def ws(self, pod: str, script: str, *args: str) -> str:
        rc, out, err = run(
            K
            + ["exec", "-i", pod, "-c", "workspace", "--"]
            + ["sh", "-c", script, "gate", *args],
            timeout=300,
        )
        if rc:
            raise GateError(f"workspace command failed on {pod}: {err.strip()[-300:]}")
        return out

    def status_by_index(self, pod: str) -> dict[int, dict[str, Any]]:
        return d7.status_files(
            d7.sections(self.ws(pod, d7._INSPECT_SCRIPT, d7.HOME)).get("status", "")
        )

    def index_of(self, plan: dict[str, Any], kind: str) -> int:
        for mount in plan.get("mounts") or []:
            if mount.get("mount_kind") == kind:
                return int(mount["index"])
        raise GateError(f"the plan mounts no {kind}")

    def settled(self, pod: str, plan: dict[str, Any]) -> dict[int, dict[str, Any]]:
        indices = [int(m["index"]) for m in plan.get("mounts") or []]
        return wait_for(
            f"every folder of {pod} settles",
            lambda: (
                found
                if all(
                    (found := self.status_by_index(pod)).get(i, {}).get("state")
                    in ("mounted", "unavailable")
                    for i in indices
                )
                else None
            ),
            timeout=240,
            interval=4,
        )

    def thread_row(self, thread: str) -> dict[str, Any]:
        return (
            sql_json(
                "SELECT json_build_object('ids', metadata->'datasource_ids', "
                "'rule', metadata->>'cloud_mount_rule', 'outbox', "
                "metadata->'cloud_outbox', 'handle', main_cloud_session_handle, "
                "'share', main_cloud_share_handle) FROM threads WHERE id = "
                f"{lit(thread)}"
            )
            or {}
        )

    def native_row(self, project: str) -> str:
        return wait_for(
            f"project {project}'s cloud folder connector",
            lambda: sql(
                "SELECT id FROM datasources WHERE managed_key = "
                f"{lit('project-cloud:' + project)}"
            ),
            timeout=180,
            interval=5,
        )

    # -- fixtures ------------------------------------------------------------
    def create(self, path: str, body: dict[str, Any], existing: Callable[[], str]):
        """A create that survives the orchestrator reloading under it (a
        shared tree under edit restarts uvicorn): a 5xx or an exec that
        could not reach the pod is retried, after looking for what an
        earlier attempt may already have made (``existing``: its id)."""
        deadline = time.monotonic() + 600
        while True:
            try:
                status, parsed = self.api.call("POST", path, body)
            except GateError as exc:
                status, parsed = 0, {"error": str(exc)}
            if status in (200, 201, 202):
                return parsed
            if status and status < 500:
                raise GateError(f"POST {path} -> HTTP {status}: {str(parsed)[:300]}")
            found = existing()
            if found:
                return {"id": found}
            if time.monotonic() > deadline:
                raise GateError(f"POST {path} -> HTTP {status}: {str(parsed)[:300]}")
            self.report.note(f"POST {path} -> {status or 'no answer'}; retrying")
            time.sleep(15)

    def create_project(self, label: str) -> str:
        created = self.create(
            "/api/projects",
            {
                "name": self.name(label),
                "description": "Cloud folder connector gate (disposable)",
                "user_id": self.user_id,
            },
            lambda: sql(
                f"SELECT id FROM projects WHERE name = {lit(self.name(label))} LIMIT 1"
            ),
        )
        project = self.projects[label] = str(created["id"])
        wait_for(
            f"project {label}'s main-cloud folder",
            lambda: sql(
                "SELECT main_cloud_folder_handle FROM projects WHERE id = "
                f"{lit(project)} AND main_cloud_folder_handle IS NOT NULL"
            ),
            timeout=180,
            interval=5,
        )
        self.folders[label] = self.native_row(project)
        return project

    def start_session(
        self,
        label: str,
        project_label: str,
        *,
        datasource_ids: list[str] | None = None,
        protected: bool = False,
        wait_pod: bool = True,
    ) -> Session:
        session = self.sessions[label] = Session(label, project_label)
        body: dict[str, Any] = {
            "title": self.title(label),
            "permission_mode": "autonomous",
            "project_id": self.projects[project_label],
            "config_override": {"workspace": {"backend": "sandbox"}},
            "model": self.args.model,
        }
        if datasource_ids is not None:
            body["datasource_ids"] = datasource_ids
        if protected:
            body["protected_cloud"] = True
        created = self.create(
            "/api/persistent/threads",
            body,
            lambda: sql(
                f"SELECT id FROM threads WHERE title = {lit(self.title(label))} LIMIT 1"
            ),
        )
        session.thread = str(created.get("thread_id") or created["id"])
        print(f"session {label} {session.thread}", flush=True)
        if wait_pod:
            pod = self.workspace_pod(session.thread)
            session.pod = pod["metadata"]["name"]
            session.plan = recorded_plan(pod)
            objects = credential_secret_name(pod)
            if objects:
                self.objects.add(objects)
        return session

    def turn(self, session: Session) -> None:
        deadline = time.monotonic() + self.args.turn_timeout
        while True:
            try:
                status, body = self.api.call(
                    "POST",
                    f"/api/persistent/threads/{session.thread}/input",
                    {"content": TURN_PROMPT},
                )
            except GateError as exc:  # the orchestrator reloading
                status, body = 503, {"error": str(exc)}
            if status in (200, 201, 202, 204):
                return
            retryable = d7.input_retryable(status, body) or status >= 500
            if not retryable or time.monotonic() > deadline:
                raise GateError(
                    f"input to {session.label} -> HTTP {status}: {str(body)[:300]}"
                )
            time.sleep(5)

    def prompt_of(self, session: Session) -> str:
        def probe() -> str:
            text = audit_sql(
                "SELECT request::text FROM llm_requests WHERE job_id::text = "
                f"{lit(session.thread)} AND call_type = 'main' "
                "ORDER BY timestamp DESC LIMIT 1"
            )
            return text if "<cloud_folders>" in text else ""

        return prompt_block(
            wait_for(
                f"the {session.label} session's first LLM request",
                probe,
                timeout=self.args.turn_timeout,
                interval=10,
            )
        )

    # -- phases ------------------------------------------------------------
    def preflight(self) -> None:
        if shutil.which("tilt"):
            # A shared tree under edit keeps Tilt busy; wait for a quiet
            # moment (the served-bytes check below is what proves the code).
            def tilt_ok() -> bool:
                rc, status, _err = run(
                    ["tilt", "get", "uiresource", "srw", "-o"]
                    + ["jsonpath={.status.updateStatus}"],
                    timeout=30,
                )
                return rc == 0 and status == "ok"

            wait_for("Tilt srw ok", tilt_ok, timeout=900, interval=10)
        else:
            self.report.note("tilt not on PATH; rollout state not read")
        problems: list[str] = []

        def current() -> bool:
            # Another edit in the shared tree may still be rolling out (the
            # agent image takes about a minute): wait, bounded, for it.
            nonlocal problems
            problems = self.served()
            return not problems

        try:
            wait_for(
                "the deployment serves this checkout", current, timeout=900, interval=30
            )
        except GateError:
            pass
        self.report.check(
            "preflight: every orchestrator and stateless agent pod serves this "
            "checkout's cloud folder connector code",
            not problems,
            "; ".join(problems)[:600],
        )
        if problems:
            raise GateError("the deployment does not serve this checkout")
        plane = bool(
            self.orchestrator_env("CONNECTOR_IN_POD_OPENER_IMAGE")
            and self.orchestrator_env("CONNECTOR_IN_POD_RCLONE_IMAGE")
        )
        self.report.check("preflight: the in-pod plane is on", plane)
        if not plane:
            raise GateError("connectors.inPodPlane is off")
        self.protected_mode = self.orchestrator_env(
            "PROTECTED_CLOUD_MODE_ENABLED"
        ).lower() in ("true", "1", "yes")
        if self.args.require_protected:
            self.report.check(
                "preflight: protected cloud mode is on", self.protected_mode
            )
        elif not self.protected_mode:
            self.report.note("protected cloud mode is off: its phase is skipped")
        self.user_id = sql(
            f"SELECT id FROM users WHERE preferred_username = {lit(self.args.user)}"
        )
        if not _UUID_RE.fullmatch(self.user_id):
            raise GateError(f"no app user {self.args.user!r}")

    def backfill(self) -> None:
        missing = sql(
            "SELECT count(*) FROM projects p "
            "JOIN main_cloud_active_backend a ON a.backend_instance_id = "
            "p.main_cloud_backend_instance_id "
            "WHERE NOT p.is_default AND p.main_cloud_folder_handle IS NOT NULL "
            "AND (SELECT count(*) FROM datasources d JOIN project_datasources pd "
            "ON pd.datasource_id = d.id AND pd.project_id = p.id "
            "WHERE d.managed_key = 'project-cloud:' || p.id::text) <> 1"
        )
        self.report.check(
            "backfill: every cloud project has exactly one project-cloud row linked "
            "to it",
            missing == "0",
            f"{missing} without",
        )
        rc, logs, _err = run(
            K + ["logs", ORCHESTRATOR, "-c", ORCHESTRATOR_CONTAINER, "--tail=-1"],
            timeout=180,
        )
        lines = logs.splitlines() if rc == 0 else []
        passes = [line for line in lines if "Session outbox connectors:" in line]
        self.report.check(
            "backfill: the orchestrator logged both startup count lines",
            bool(passes)
            and any("Project cloud folder connectors:" in x for x in lines),
        )
        # The backfill's promise covers the users admitted before its last
        # pass; one admitted since by a path outside SRW (a gate's SQL) gets
        # theirs at the next start.
        last = (
            re.match(r"(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d)", passes[-1])
            if passes
            else None
        )
        before = (
            "AND coalesce(u.approved_at, u.created_at) < "
            f"{lit(last.group(1) + '+00')}::timestamptz "
            if last
            else ""
        )
        outboxes = sql(
            "SELECT count(*) FROM users u WHERE u.is_approved "
            + before
            + "AND (SELECT count(*) FROM datasources d WHERE d.managed_key = "
            "'cloud-outbox:' || u.id::text AND d.type = 'cloud_outbox' AND "
            "d.name = 'Session outbox' AND d.scope_mode = 'all' AND d.auto_attach "
            "AND d.created_by = u.id) <> 1"
        )
        self.report.check(
            "backfill: every user approved before its last pass has exactly one "
            "session outbox row",
            outboxes == "0",
            f"{outboxes} without",
        )
        self.outbox_id = sql(
            "SELECT id FROM datasources WHERE managed_key = "
            f"{lit('cloud-outbox:' + self.user_id)}"
        )
        for label in ("main", "broken", "prot"):
            self.create_project(label)
        self.report.check(
            "backfill: each gate project has its own cloud folder row",
            all(_UUID_RE.fullmatch(self.folders[label]) for label in self.folders),
            json.dumps(self.folders),
        )

    def omitted(self) -> None:
        if not _UUID_RE.fullmatch(self.outbox_id):
            raise GateError("the gate user has no outbox")
        session = self.start_session("omitted", "main")
        row = self.thread_row(session.thread)
        folder = self.folders["main"]
        self.report.check(
            "omitted: main's cloud folder and the owner's outbox are attached",
            {folder, self.outbox_id} <= set(row.get("ids") or []),
            json.dumps(row.get("ids")),
        )
        outbox = row.get("outbox") or {}
        self.report.check(
            "omitted: SRW created the session folder and recorded the outbox",
            bool(row.get("handle"))
            and outbox.get("connector_id") == self.outbox_id
            and outbox.get("state") == "ready"
            and row.get("rule") == "connectors",
            json.dumps({k: row.get(k) for k in ("handle", "outbox", "rule")}),
        )
        plan = session.plan or {}
        name = slug(self.name("main"))
        expected = sorted(
            [(name, "project", folder), ("outbox", "cloud_outbox", self.outbox_id)]
        )
        self.report.check(
            "omitted: the plan mounts the project folder at cloud/<project> and "
            "the outbox at cloud/outbox, nothing excluded",
            plan_mounts(plan) == expected and not plan.get("excluded"),
            json.dumps({"mounts": plan_mounts(plan), "excluded": plan.get("excluded")}),
        )
        states = self.settled(session.pod, plan)
        self.report.check(
            "omitted: both folders are mounted in the workspace",
            all(entry.get("state") == "mounted" for entry in states.values()),
            json.dumps(states),
        )
        marker = secrets.token_hex(16)
        filename = f"{self.gate_id}-outbox.txt"
        rc, _out, err = run(
            K
            + ["exec", session.pod, "-c", "workspace", "--"]
            + ["sh", "-c", d7._AS_AGENT, "gate"]
            + [f"printf %s {marker} > /cloud/outbox/{filename}"],
            timeout=120,
        )
        if rc:
            raise GateError(f"the outbox write failed: {err.strip()[-200:]}")
        folder_id = str(row.get("handle") or "").rsplit("/", 1)[-1].strip('"}')
        digest = hashlib.sha256(marker.encode()).hexdigest()
        try:
            reached = wait_for(
                "the outbox write reaches Nextcloud",
                lambda: command(
                    K
                    + ["exec", NEXTCLOUD, "-c", NEXTCLOUD_CONTAINER, "--"]
                    + ["su", "-s", "/bin/sh", "www-data", "-c"]
                    + [_NEXTCLOUD_SESSION_FILE, "gate", folder_id, filename]
                ).strip()
                == digest,
                timeout=180,
                interval=5,
            )
        except GateError:
            reached = False
        self.report.check(
            "omitted: a write into the outbox reaches agent-service's session folder",
            bool(reached),
            f"sessions/{folder_id}/{filename}",
        )
        self.turn(session)
        block = self.prompt_of(session)
        self.report.check(
            "omitted: the prompt's cloud folder block names both folders, the "
            "outbox as not the project folder",
            f"workspace/cloud/{name}:" in block
            and "workspace/cloud/outbox: a folder SRW created for this session" in block
            and "not the project folder" in block,
            block.strip()[:500],
        )

    def empty(self) -> None:
        session = self.start_session("empty", "main", datasource_ids=[])
        row = self.thread_row(session.thread)
        pod = self.workspace_pod(session.thread)
        self.report.check(
            "empty: nothing attached, no session folder, no outbox record",
            (row.get("ids") or []) == []
            and not row.get("handle")
            and not row.get("outbox"),
            json.dumps(row),
        )
        self.report.check(
            "empty: the Pod has no sidecars and no plan",
            container(pod, SUPERVISOR_CONTAINER) is None
            and container(pod, OPENER_CONTAINER) is None
            and recorded_plan(pod) is None,
            pod["metadata"]["name"],
        )

    def nochip(self) -> None:
        session = self.start_session("nochip", "main", datasource_ids=[self.outbox_id])
        plan = session.plan or {}
        self.report.check(
            "nochip: without the project folder the outbox mounts alone, nothing "
            "in the project folder's place",
            plan_mounts(plan) == [("outbox", "cloud_outbox", self.outbox_id)]
            and not plan.get("excluded"),
            json.dumps({"mounts": plan_mounts(plan), "excluded": plan.get("excluded")}),
        )

    def _repoint(self, label: str, column_sql: str, config_sql: str) -> None:
        project, folder = self.projects[label], self.folders[label]
        before_handle = sql(
            f"SELECT main_cloud_folder_handle FROM projects WHERE id = {lit(project)}"
        )
        before_config = sql(
            f"SELECT config::text FROM datasources WHERE id = {lit(folder)}"
        )
        self.restore.append(
            f"UPDATE projects SET main_cloud_folder_handle = {lit(before_handle)} "
            f"WHERE id = {lit(project)}"
        )
        self.restore.append(
            f"UPDATE datasources SET config = {lit(before_config)}::jsonb "
            f"WHERE id = {lit(folder)}"
        )
        if column_sql:
            sql(column_sql.format(project=lit(project)))
        sql(config_sql.format(folder=lit(folder), project=lit(project)))

    def _undo(self) -> None:
        while self.restore:
            sql(self.restore.pop())

    def broken(self) -> None:
        missing = f"{self.gate_id}-missing"
        # The project's own columns name the folder (the connector's config is
        # SRW's copy, kept in step): point both at a folder that is not there.
        handle = (
            "jsonb_set(main_cloud_folder_handle::jsonb, '{{vendor_meta,mountpoint}}', "
            f"to_jsonb({lit(missing)}::text))::text"
        )
        self._repoint(
            "broken",
            "UPDATE projects SET main_cloud_folder_handle = "
            + handle
            + " WHERE id = {project}",
            "UPDATE datasources d SET config = jsonb_set(d.config, '{{folder_handle}}', "
            "to_jsonb(p.main_cloud_folder_handle)) FROM projects p "
            "WHERE d.id = {folder} AND p.id = {project}",
        )
        try:
            session = self.start_session("broken", "broken")
        finally:
            self._undo()
        plan = session.plan or {}
        folder = self.folders["broken"]
        name = slug(self.name("broken"))
        mounts, excluded = plan_mounts(plan), plan_exclusions(plan)
        outbox = ("outbox", "cloud_outbox", self.outbox_id)
        detail = json.dumps({"mounts": mounts, "excluded": plan.get("excluded")})
        if (name, "project", folder) in mounts:
            # Planned (its credential needs nothing of the folder): the mount
            # itself says the folder is not there.
            self.report.check(
                "broken: the plan holds the broken folder and the outbox, nothing else",
                mounts == sorted([(name, "project", folder), outbox]) and not excluded,
                detail,
            )
            states = self.settled(session.pod, plan)
            project = states.get(self.index_of(plan, "project")) or {}
            mounted = states.get(self.index_of(plan, "cloud_outbox")) or {}
            self.report.check(
                "broken: the project folder reads unavailable (not_found), the "
                "outbox is mounted",
                project.get("state") == "unavailable"
                and project.get("reason") == "not_found"
                and mounted.get("state") == "mounted",
                json.dumps({"project": project, "outbox": mounted}),
            )
        else:
            # Or refused before the Pod (a per-folder grant that cannot be made
            # for a folder that is not there): named, with its reason.
            reasons = [reason for ref, kind, reason in excluded if ref == folder]
            self.report.check(
                "broken: the broken folder is excluded with its reason and the "
                "outbox mounts alone, nothing in its place",
                mounts == [outbox]
                and len(reasons) == 1
                and reasons[0] in EXCLUSION_REASONS,
                detail,
            )

    def foreign(self) -> None:
        self._repoint(
            "broken",
            "",
            "UPDATE datasources SET config = jsonb_set(config, '{{project_id}}', "
            f"to_jsonb({lit(self.projects['main'])}::text)) WHERE id = {{folder}}",
        )
        try:
            session = self.start_session("foreign", "broken")
        finally:
            self._undo()
        plan = session.plan or {}
        self.report.check(
            "foreign: a cloud folder row naming another project is excluded "
            "config_invalid; the outbox mounts alone",
            plan_mounts(plan) == [("outbox", "cloud_outbox", self.outbox_id)]
            and plan_exclusions(plan)
            == [(self.folders["broken"], "project", "config_invalid")],
            json.dumps({"mounts": plan_mounts(plan), "excluded": plan.get("excluded")}),
        )

    def protected(self) -> None:
        if not self.protected_mode:
            return
        session = self.start_session("prot", "prot", protected=True)
        plan = session.plan or {}
        row = self.thread_row(session.thread)
        self.report.check(
            "protected: a protected plan mounts the reader lower alone",
            plan.get("protected") is True
            and [m[1] for m in plan_mounts(plan)] == ["protected_lower"],
            json.dumps(plan_mounts(plan)),
        )
        self.report.check(
            "protected: the outbox is named beside_protected, not dropped",
            plan_exclusions(plan)
            == [(self.outbox_id, "cloud_outbox", "beside_protected")],
            json.dumps(plan.get("excluded")),
        )
        self.report.check(
            "protected: no session folder is created for a protected session",
            not row.get("handle"),
            json.dumps(row),
        )

    # -- cleanup -------------------------------------------------------------
    def titled_threads(self) -> list[str]:
        rows = sql(
            "SELECT id FROM threads WHERE "
            f"position({lit(self.gate_id)} in coalesce(title, '')) > 0"
        )
        return [row for row in rows.splitlines() if _UUID_RE.fullmatch(row)]

    def named_projects(self) -> list[str]:
        rows = sql(
            f"SELECT id FROM projects WHERE name LIKE {lit(self.gate_id + ' %')}"
        )
        return [row for row in rows.splitlines() if _UUID_RE.fullmatch(row)]

    def delete_thread(self, thread: str) -> bool:
        def gone() -> bool:
            try:
                status, _body = self.api.call(
                    "DELETE",
                    f"/api/persistent/threads/{thread}?force=true&permanent=true",
                )
            except GateError:  # the orchestrator reloading: ask again
                return False
            if status == 404:
                return True
            return sql(f"SELECT count(*) FROM threads WHERE id = {lit(thread)}") == "0"

        try:
            wait_for("session deleted", gone, timeout=900, interval=10)
        except GateError:
            return False
        return True

    def cleanup(self) -> list[str]:
        problems: list[str] = []

        def step(label: str, action: Callable[[], Any]) -> None:
            try:
                if action() is False:
                    problems.append(label)
            except GateError as exc:
                problems.append(f"{label} ({exc})")

        step("restore a repointed handle", self._undo)
        threads = [s.thread for s in self.sessions.values() if s.thread]
        for thread in dict.fromkeys([*threads, *self.titled_threads()]):
            step(f"delete session {thread}", lambda t=thread: self.delete_thread(t))
        for project in dict.fromkeys([*self.projects.values(), *self.named_projects()]):

            def project_deleted(project=project) -> bool:
                try:
                    status, _body = self.api.call("DELETE", f"/api/projects/{project}")
                except GateError:  # the orchestrator reloading: ask again
                    return False
                return status in (200, 204, 404)

            step(
                f"delete project {project}",
                lambda p=project_deleted: bool(
                    wait_for("project deleted", p, timeout=600, interval=10)
                ),
            )
        for problem in problems:
            print(f"cleanup: {problem} failed", flush=True)
        return problems

    def residue(self) -> list[str]:
        left: list[str] = []
        titled = self.titled_threads()
        if titled:
            left.append(f"sessions titled with the gate id: {titled}")
        named = self.named_projects()
        if named:
            left.append(f"projects: {named}")
        rows = sql(
            "SELECT count(*) FROM datasources WHERE managed_key = ANY(ARRAY["
            + ",".join(lit("project-cloud:" + p) for p in self.projects.values())
            + "]::text[])"
            if self.projects
            else "SELECT 0"
        )
        if rows != "0":
            left.append(f"{rows} project cloud folder rows")
        threads = [s.thread for s in self.sessions.values() if s.thread]
        for thread in [*threads, *(t for t in self.swept_threads if t not in threads)]:
            selector = f"srw/thread-id={thread}"
            try:
                wait_for(
                    f"pods {selector} gone",
                    lambda selector=selector: not json.loads(
                        command(K + ["get", "pods", "-l", selector, "-o", "json"])
                    )["items"],
                    timeout=180,
                    interval=10,
                )
            except GateError:
                left.append(f"pods {selector}")
        for name in sorted(self.objects):
            for kind in ("configmap", "secret"):
                rc, _out, _err = run(K + ["get", kind, name], timeout=60)
                if rc == 0:
                    left.append(f"{kind} {name}")
        if self.restore:
            left.append("a repointed handle was not restored")
        return left

    def sweep(self) -> int:
        self.swept_threads = self.titled_threads()
        problems = self.cleanup()
        try:
            left = self.residue()
        except GateError as exc:
            left = [f"residue check failed: {exc}"]
        ok = self.report.check(
            f"sweep {self.gate_id}: nothing is left",
            not problems and not left,
            "; ".join([*problems, *left]),
        )
        self.write_json()
        return 0 if ok else 1

    def write_json(self) -> None:
        if not self.args.json:
            return
        Path(self.args.json).write_text(
            json.dumps(
                {
                    "gate_id": self.gate_id,
                    "passed": self.report.passed,
                    "checks": [
                        {"name": name, "ok": ok, "detail": detail}
                        for name, ok, detail in self.report.results
                    ],
                },
                indent=2,
            )
            + "\n"
        )

    # -- run -----------------------------------------------------------------
    def run(self) -> int:
        try:
            self.preflight()
            self.backfill()
            for phase in (
                self.omitted,
                self.empty,
                self.nochip,
                self.broken,
                self.foreign,
                self.protected,
            ):
                if self.args.phases and phase.__name__ not in self.args.phases:
                    continue
                try:
                    phase()
                except GateError as exc:
                    self.report.check(
                        f"{phase.__name__}: infrastructure", False, str(exc)
                    )
        except GateError as exc:
            self.report.check("gate infrastructure", False, str(exc))
        finally:
            if self.args.keep:
                self._undo()
                print(
                    "kept: "
                    + json.dumps(
                        {
                            "projects": self.projects,
                            "sessions": {k: v.thread for k, v in self.sessions.items()},
                        }
                    )
                )
            else:
                self.cleanup()
                try:
                    left = self.residue()
                except GateError as exc:
                    left = [f"residue check failed: {exc}"]
                self.report.check(
                    "cleanup: nothing this run created is left",
                    not left,
                    "; ".join(left),
                )
        verdict = "PASS" if self.report.passed else "FAIL"
        failed = [name for name, ok, _ in self.report.results if not ok]
        print(
            f"{verdict} {self.gate_id}: {len(self.report.results)} checks, "
            f"{len(failed)} failed {failed if failed else ''}".rstrip()
        )
        self.write_json()
        return 0 if self.report.passed else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--context", default=LOCAL_CONTEXT)
    parser.add_argument("--namespace", default=LOCAL_NAMESPACE)
    parser.add_argument("--run", action="store_true")
    parser.add_argument("--confirm")
    parser.add_argument("--gate-id")
    parser.add_argument("--user", default="test")
    parser.add_argument("--password", default="srw-k3d-dev-test")
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--pod-timeout", type=int, default=420)
    parser.add_argument("--turn-timeout", type=int, default=420)
    parser.add_argument("--json", help="also write the results to this file")
    parser.add_argument(
        "--phases",
        nargs="+",
        choices=PHASES,
        help="run only these session phases (preflight and backfill always run)",
    )
    parser.add_argument(
        "--require-protected",
        action="store_true",
        help="fail when protected cloud mode is off instead of skipping",
    )
    parser.add_argument("--keep", action="store_true", help="skip cleanup")
    parser.add_argument(
        "--sweep",
        action="store_true",
        help="only remove what an earlier run of --gate-id left behind",
    )
    return parser


def validate(args: argparse.Namespace) -> None:
    if args.context != LOCAL_CONTEXT or args.namespace != LOCAL_NAMESPACE:
        raise SafetyError("this gate is restricted to k3d-srw/srw")
    if args.run and args.confirm != LOCAL_CONFIRMATION:
        raise SafetyError(f"--run requires --confirm {LOCAL_CONFIRMATION}")
    if not args.run and args.confirm is not None:
        raise SafetyError("--confirm is accepted only with --run")
    if args.gate_id is not None and not _GATE_ID_RE.fullmatch(args.gate_id):
        raise SafetyError("--gate-id must be cfc- followed by 10 hex digits")
    if args.sweep and (not args.run or args.gate_id is None or args.keep):
        raise SafetyError("--sweep needs --run, --confirm and --gate-id, not --keep")
    if not d7._MODEL_RE.fullmatch(args.model):
        raise SafetyError("model id is malformed")
    if not d7._USER_RE.fullmatch(args.user):
        raise SafetyError("user name is malformed")
    if not 60 <= args.pod_timeout <= 1800:
        raise SafetyError("--pod-timeout must be between 60 and 1800 seconds")
    if not 60 <= args.turn_timeout <= 1800:
        raise SafetyError("--turn-timeout must be between 60 and 1800 seconds")


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        validate(args)
    except SafetyError as exc:
        print(f"refused: {exc}", file=sys.stderr)
        return 2
    if not args.run:
        print("plan (dry run; pass --run --confirm LOCAL-K3D-DISPOSABLE to execute):")
        for step in PLAN:
            print(f"  - {step}")
        return 0
    gate = CloudFolderConnectorGate(args)
    return gate.sweep() if args.sweep else gate.run()


if __name__ == "__main__":
    raise SystemExit(main())
