#!/usr/bin/env python3
"""Local k3d gate for connector access levels (main cloud slice 5).

Design: knowledge-base/knowledge/features/main_cloud_as_connectors.md
("Access levels", "Overnight run: slices 3 to 8": protected is the
connector's level, per-link default access, cross-project folders). It
reuses scripts/k3d-cloud-mount-sidecar-gate.py (sessions, their workspace
Pods, the overlay and lower checks) and scripts/k3d-main-cloud-gate.py (the
disposable second account and its OAuth client), with their safety envelope:
dry-run by default, the exact k3d-srw/srw context, secrets only on stdin and
scrubbed from every printed line, and a cleanup in ``finally`` that touches
only what this run created.

It needs protected cloud mode on (``agent.protectedCloudModeEnabled``) and
the in-pod plane, as the D7 gate does with ``--require-protected``.

Fixtures (all disposable, named after the gate id): projects ``p`` and
``q`` of the owner; project ``b`` of the owner with the second account as
editor; project ``a2`` of the second account.

Checks (each printed PASS/FAIL; the exit status is 0 only if all pass):

  chip        a session with p's folder at ``datasource_access: protected``:
              the server writes the marker, the lane is pinned, a
              thread_protected_sources row names the connector and no
              thread_mounts row is its source; cloud_ro_mounts is active and
              its selected_mount_id is that row's id; /cloud/lower is a
              read-only sidecar mount (EROFS) under the capture overlay; a
              write is staged at turn end and listed by cloud-diff; apply puts
              it in Nextcloud; End revokes the grant and Resume re-engages
              under the same selected id
  default     PATCH p's link {access: protected}: a session that names no
              level is protected (origin link_default); one with an explicit
              read_write is not; the link is reset
  exclusion   p protected beside q's folder: the plan excludes q
              ``beside_protected``; two folders at protected is 422
              protected_cloud_single_folder
  cross       the second account (editor of b) lists b's folder in a2, not
              preselected, with every offered level; a session of a2 binding
              b's folder at protected engages b's folder; as a viewer, b's
              read_write is 422 (above_ceiling) and the resumed protected
              session's engage is refused (engage_refused); removed from b,
              the folder is not listed and naming it is 403; b's knowledge
              base is never eligible in a2
  alias       the legacy ``protected_cloud: true`` still engages the
              project's own folder; with that folder not attached it is 422
              protected_cloud_requires_folder, and with a conflicting level
              422 protected_cloud_conflict

Not here: the deployment switch off (unit tests: the cluster runs with it on)
and the read-only reader account (scripts/k3d-folder-grants-gate.py).

  .venv/bin/python scripts/k3d-cloud-access-gate.py           # plan
  .venv/bin/python scripts/k3d-cloud-access-gate.py \\
      --run --confirm LOCAL-K3D-DISPOSABLE
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import re
import secrets
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[1]
_D7_GATE = ROOT / "scripts" / "k3d-cloud-mount-sidecar-gate.py"
_D4_GATE = ROOT / "scripts" / "k3d-main-cloud-gate.py"


def _load(name: str, path: Path):
    if name in sys.modules:
        return sys.modules[name]
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


d7 = _load("_k3d_cloud_mount_sidecar_gate", _D7_GATE)
d4 = _load("_k3d_main_cloud_gate", _D4_GATE)

#: What a call meets while the orchestrator restarts (another checkout's
#: reload, a rebuild): retried, never reported as the product's answer.
_TRANSIENT = (
    "Connection refused",
    "Connection reset",
    "failed to create exec",
    "container not found",
    "RemoteDisconnected",
)


def _retrying(call):
    def wrapped(self, *args, **kwargs):
        deadline = d7.time.monotonic() + 300
        while True:
            try:
                return call(self, *args, **kwargs)
            except (d7.GateError, d4.GateError) as exc:
                if d7.time.monotonic() > deadline or not any(
                    marker in str(exc) for marker in _TRANSIENT
                ):
                    raise
                d7.time.sleep(5)

    return wrapped


# Both gates' API clients ride out an orchestrator restart. A retried call
# that already took effect answers as a repeat would (409/404), which every
# step here tolerates.
d4.Api.call = _retrying(d4.Api.call)
d7.Api.call = _retrying(d7.Api.call)

#: Either gate's infrastructure error (they are separate classes).
GateError = (d7.GateError, d4.GateError)
SafetyError = d7.SafetyError
sql = d7.sql
sql_json = d7.sql_json
lit = d7.lit
wait_for = d7.wait_for

_GATE_ID_RE = re.compile(r"ca-[0-9a-f]{10}\Z")
_UUID_RE = d7._UUID_RE
#: What every orchestrator pod must serve byte for byte.
SERVED = (
    "src/shared/connectors/contract.py",
    "src/orchestrator/services/connector_access.py",
    "src/orchestrator/services/datasource_policy.py",
    "src/orchestrator/services/thread_admission.py",
    "src/orchestrator/services/protected_cloud_engage.py",
    "src/orchestrator/services/projects.py",
    "src/orchestrator/services/datasources.py",
    "src/orchestrator/database/postgres.py",
)
PLAN = (
    "preflight: Tilt ok, the orchestrator serves this checkout's access code, "
    "protected mode and the in-pod plane are on",
    "accounts: a second account and an OAuth client (scripts/k3d-main-cloud-gate.py)",
    "chip: protected from datasource_access, through staging, apply, End and Resume",
    "default: a project link's protected default, and an explicit level over it",
    "exclusion: a second folder beside a protected one; two protected folders",
    "cross: another project's folder for its editor, viewer and a non-member",
    "alias: protected_cloud as an alias, and its refusals",
    "cleanup: sessions, projects, the second account and the client",
)


class _OwnerApi:
    """The D7 gate's API surface over the run's own OAuth client.

    The orchestrator recomputes ``is_admin`` from each login's token, so the
    owner logs in only through clients whose tokens carry the realm roles:
    the run's client here, and the gates' client (scripts/k3d_gate_oauth.py)
    in the D7 gate's own calls; ``admin-cli``'s would demote it."""

    def __init__(self, api) -> None:
        self.api = api

    def call(self, method: str, path: str, body: Any = None, *, timeout: int = 180):
        return self.api.call(method, path, body)

    def ok(self, method: str, path: str, body: Any = None) -> Any:
        return self.api.ok(method, path, body)


class AccessGate:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        digits = (args.gate_id or f"ca-{secrets.token_hex(5)}")[3:]
        self.gate_id = f"ca-{digits}"
        self.report = d7.Report(self.gate_id)
        self.d7 = d7.CloudMountSidecarGate(
            argparse.Namespace(**{**vars(args), "gate_id": f"d7-{digits}"})
        )
        self.d4 = d4.MainCloudGate(
            argparse.Namespace(
                **{
                    **vars(args),
                    "gate_id": f"d4-{digits}",
                    "expect_provider": None,
                    "base_url": "https://localhost",
                }
            )
        )
        self.projects: dict[str, tuple[str, str]] = {}  # label -> (id, who)
        self.threads: dict[str, tuple[str, str]] = {}  # label -> (id, who)

    # -- helpers -----------------------------------------------------------
    def name(self, label: str) -> str:
        return f"{self.gate_id} {label}"

    def resume(self, api, thread: str, *, timeout: int = 900) -> tuple[int, Any]:
        """Resume as a client must: an End that landed during the workspace
        create holds Resume for the create-fence horizon (409
        ``workspace_predecessor_cleanup_pending`` with ``retry_after``)."""

        deadline = time.monotonic() + timeout
        while True:
            status, body = api.call(
                "POST", f"/api/persistent/threads/{thread}/resume", {}
            )
            detail = body.get("detail") if isinstance(body, dict) else None
            if not (
                status == 409
                and isinstance(detail, dict)
                and detail.get("code") == "workspace_predecessor_cleanup_pending"
            ):
                return status, body
            try:
                wait = (
                    datetime.fromisoformat(str(detail.get("retry_after")))
                    - datetime.now(timezone.utc)
                ).total_seconds() + 5
            except (TypeError, ValueError):
                wait = 30
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return status, body
            self.report.note(
                f"resume of {thread} held until {detail.get('retry_after')}; retrying"
            )
            time.sleep(min(max(wait, 5), remaining))

    def apply(self, thread: str, filename: str) -> tuple[int, Any]:
        """Apply the staged diff once more if an end-of-turn stage moved the
        epoch during the first apply: apply then refuses its baseline
        publication (fail closed, retryable), and its writes are idempotent."""

        status, applied = 0, None
        for _attempt in range(2):
            status, diff = self.d7.api.call(
                "GET", f"/api/agents/threads/{thread}/cloud-diff"
            )
            if status != 200 or not any(
                f.get("path") == filename for f in (diff.get("files") or [])
            ):
                return status, diff
            status, applied = self.d7.api.call(
                "POST",
                f"/api/agents/threads/{thread}/cloud-diff/apply",
                {"epoch": diff.get("epoch")},
            )
            if not (
                status == 502
                and "staging authority changed before baseline publication"
                in str(applied)
            ):
                return status, applied
            self.report.note("apply raced an end-of-turn stage; applying again")
        return status, applied

    def api(self, who: str):
        return self.d4.owner if who == "owner" else self.d4.other

    def project(self, label: str, who: str = "owner") -> str:
        created = self.api(who).ok(
            "POST",
            "/api/projects",
            {
                "name": self.name(label),
                "description": "access gate (disposable)",
                "user_id": self.d4.owner_id if who == "owner" else self.d4.other_id,
            },
        )
        project = str(created["id"])
        self.projects[label] = (project, who)
        wait_for(
            f"project {label}'s main-cloud folder",
            lambda: sql(
                "SELECT main_cloud_folder_handle FROM projects WHERE id = "
                f"{lit(project)} AND main_cloud_folder_handle IS NOT NULL"
            ),
            timeout=180,
            interval=5,
        )
        return project

    def folder(self, project: str) -> str:
        return wait_for(
            "the project's cloud folder connector",
            lambda: sql(
                "SELECT id FROM datasources WHERE managed_key = "
                f"{lit('project-cloud:' + project)}"
            ),
            timeout=180,
            interval=5,
        )

    def create(self, label: str, body: dict[str, Any], who: str = "owner"):
        payload = {
            "title": f"access gate {self.gate_id} {label}",
            # The second account's grants stop below autonomous.
            "permission_mode": "autonomous" if who == "owner" else "auto_accept",
            "config_override": {"workspace": {"backend": "sandbox"}},
            "model": self.args.model,
            **body,
        }
        status, created = self.api(who).call("POST", "/api/persistent/threads", payload)
        if status in (200, 201) and created.get("thread_id"):
            self.threads[label] = (str(created["thread_id"]), who)
        return status, created

    def thread_row(self, thread: str) -> dict[str, Any]:
        return sql_json(
            "SELECT row_to_json(t) FROM (SELECT execution_lane, metadata "
            f"FROM threads WHERE id = {lit(thread)}) t"
        )

    def grant(self, thread: str) -> dict[str, Any] | None:
        return sql_json(
            "SELECT row_to_json(r) FROM (SELECT id, status, selected_mount_id, "
            "engage_attempt FROM cloud_ro_mounts WHERE thread_id = "
            f"{lit(thread)}) r"
        )

    def source(self, thread: str) -> dict[str, Any] | None:
        return sql_json(
            "SELECT row_to_json(s) FROM (SELECT id, datasource_id, source_ref, "
            "target_path, cloud_handle FROM thread_protected_sources WHERE "
            f"thread_id = {lit(thread)}) s"
        )

    def wait_grant(self, thread: str, status: str, timeout: int = 480) -> dict:
        return wait_for(
            f"the grant {status}",
            lambda: (
                row
                if (row := self.grant(thread) or {}).get("status") == status
                else None
            ),
            timeout=timeout,
            interval=5,
        )

    def session(self, label: str, thread: str):
        session = d7.Session(label, self.projects.get(label, ("", ""))[0], "")
        session.thread = thread
        session.lane = self.thread_row(thread)["execution_lane"]
        pod = self.d7.workspace_pod(
            f"srw/thread-id={thread}", "ws-thread-", timeout=self.args.pod_timeout
        )
        session.pod = pod["metadata"]["name"]
        return session

    # -- phases --------------------------------------------------------------
    def preflight(self) -> None:
        # Other work may be syncing: wait for Tilt to settle before the D7
        # preflight reads it once.
        if shutil.which("tilt"):
            wait_for(
                "Tilt srw ok",
                lambda: d7.run(
                    ["tilt", "get", "uiresource", "srw", "-o"]
                    + ["jsonpath={.status.updateStatus}"],
                    timeout=30,
                )[1]
                == "ok",
                timeout=900,
                interval=10,
            )
        self.d7.preflight()
        problems = self.d7.served_problems(
            "orchestrator", d7.ORCHESTRATOR_CONTAINER, SERVED
        )
        self.report.check(
            "preflight: every orchestrator pod serves this checkout's access code",
            not problems,
            "; ".join(problems)[:600],
        )
        if problems or not self.d7.protected_mode:
            raise d7.GateError(
                "the deployment does not serve this checkout, protected on"
            )

    def chip(self) -> None:
        p = self.projects["p"][0] if "p" in self.projects else self.project("p")
        folder = self.folder(p)
        status, created = self.create(
            "chip",
            {
                "project_id": p,
                "datasource_ids": [folder],
                "datasource_access": {folder: "protected"},
            },
        )
        if status not in (200, 201):
            raise d7.GateError(f"chip session -> HTTP {status}: {str(created)[:300]}")
        thread = self.threads["chip"][0]
        row = self.thread_row(thread)
        metadata = row["metadata"]
        source = self.source(thread) or {}
        self.report.check(
            "chip: the server writes the marker from the level, the lane is pinned, "
            "and the protected source names the connector",
            metadata.get("protected_cloud") is True
            and row["execution_lane"] == "pinned"
            and metadata.get("datasource_access") == {folder: "protected"}
            and source.get("datasource_id") == folder
            and source.get("source_ref") == p,
            f"{row['execution_lane']} {metadata.get('datasource_access')} {source}",
        )
        grant = self.wait_grant(thread, "active")
        self.report.check(
            "chip: the grant is active and selects the protected source's id",
            grant.get("selected_mount_id") == source.get("id"),
            f"{grant.get('selected_mount_id')} vs {source.get('id')}",
        )
        session = self.session("chip", thread)
        entry = self.d7.wait_status_file(session.pod, 0, "mounted")
        tops = d7.top_mounts(self.d7.inspect(session.pod).get("mountinfo", ""))
        fstype, options = tops.get("/cloud/lower", ("", ""))
        rc, _out, err = self.d7.as_agent(session.pod, "touch /cloud/lower/gate-probe")
        self.report.check(
            "chip: /cloud/lower is a read-only sidecar mount and a write fails "
            "with EROFS",
            entry.get("state") == "mounted"
            and fstype == "fuse.rclone"
            and "ro" in options.split(",")
            and rc != 0
            and "Read-only file system" in err,
            f"{fstype} {options} rc={rc}",
        )
        self.d7.turn(session)
        wait_for(
            "the overlay at /cloud/merged",
            lambda: d7.top_mounts(
                self.d7.inspect(session.pod).get("mountinfo", "")
            ).get("/cloud/merged", ("",))[0]
            == "fuse.fuse-overlayfs",
            timeout=self.args.turn_timeout,
            interval=5,
        )
        filename = f"{self.gate_id}-chip.txt"
        rc, _out, _err = self.d7.as_agent(
            session.pod, f"printf staged > /cloud/merged/{filename}"
        )
        self.d7.turn(session)

        def staged() -> dict[str, Any] | None:
            status, body = self.d7.api.call(
                "GET", f"/api/agents/threads/{thread}/cloud-diff"
            )
            files = (body.get("files") or []) if status == 200 else []
            return body if any(f.get("path") == filename for f in files) else None

        diff = wait_for(
            "the staged diff", staged, timeout=self.args.turn_timeout, interval=10
        )
        self.report.check(
            "chip: a write through the overlay is staged and listed by cloud-diff",
            rc == 0 and bool(diff),
            f"rc={rc}",
        )
        status, applied = self.apply(thread, filename)
        native = json.loads(source["cloud_handle"])["native_id"]
        landed = self.d7.nextcloud_file(native, filename)
        self.report.check(
            "chip: apply writes the staged file into the folder in Nextcloud",
            status == 200 and landed is not None,
            f"HTTP {status} {str(applied)[:160]} file={landed}",
        )
        status, _body = self.d7.api.call(
            "DELETE", f"/api/persistent/threads/{thread}?force=true", timeout=400
        )
        revoked = self.wait_grant(thread, "revoked", timeout=600)
        status_resume, _body = self.resume(self.d7.api, thread)
        resumed = self.wait_grant(thread, "active", timeout=600)
        self.report.check(
            "chip: End revokes the grant and Resume re-engages under the same "
            "selected id with a new attempt",
            status in (200, 202, 204)
            and status_resume == 200
            and revoked.get("selected_mount_id") == source.get("id")
            and resumed.get("selected_mount_id") == source.get("id")
            and resumed.get("engage_attempt") != grant.get("engage_attempt"),
            f"end {status} resume {status_resume}",
        )

    def default(self) -> None:
        p = self.projects["p"][0] if "p" in self.projects else self.project("p")
        folder = self.folder(p)
        self.d4.owner.ok(
            "PATCH", f"/api/projects/{p}/datasources/{folder}", {"access": "protected"}
        )
        try:
            status, created = self.create(
                "default", {"project_id": p, "datasource_ids": [folder]}
            )
            metadata = (
                self.thread_row(self.threads["default"][0])["metadata"]
                if status in (200, 201)
                else {}
            )
            origin = (
                (metadata.get("datasource_selection") or {}).get("access") or {}
            ).get(folder) or {}
            self.report.check(
                "default: a session naming no level gets the link's protected default",
                metadata.get("protected_cloud") is True
                and origin.get("origin") == "link_default",
                f"HTTP {status} {origin}",
            )
            status, created = self.create(
                "explicit",
                {
                    "project_id": p,
                    "datasource_ids": [folder],
                    "datasource_access": {folder: "read_write"},
                },
            )
            metadata = (
                self.thread_row(self.threads["explicit"][0])["metadata"]
                if status in (200, 201)
                else {}
            )
            self.report.check(
                "default: an explicit read_write wins over the link's default",
                "protected_cloud" not in metadata
                and metadata.get("datasource_access") == {folder: "read_write"},
                f"HTTP {status} {metadata.get('datasource_access')}",
            )
        finally:
            self.d4.owner.call(
                "PATCH", f"/api/projects/{p}/datasources/{folder}", {"access": None}
            )
        for label in ("default", "explicit"):
            self.delete_thread(label)

    def exclusion(self) -> None:
        p = self.projects["p"][0] if "p" in self.projects else self.project("p")
        q = self.project("q")
        p_folder, q_folder = self.folder(p), self.folder(q)
        status, body = self.create(
            "two",
            {
                "project_id": p,
                "datasource_ids": [p_folder, q_folder],
                "datasource_access": {p_folder: "protected", q_folder: "protected"},
            },
        )
        self.report.check(
            "exclusion: two folders at protected is 422 protected_cloud_single_folder",
            status == 422
            and (body.get("detail") or {}).get("code")
            == "protected_cloud_single_folder",
            f"HTTP {status} {str(body)[:160]}",
        )
        status, body = self.create(
            "beside",
            {
                "project_id": p,
                "datasource_ids": [p_folder, q_folder],
                "datasource_access": {p_folder: "protected"},
            },
        )
        if status not in (200, 201):
            raise d7.GateError(f"beside session -> HTTP {status}: {str(body)[:300]}")
        thread = self.threads["beside"][0]
        self.wait_grant(thread, "active")
        session = self.session("beside", thread)
        pod = self.d7.pod_json(session.pod) or {}
        plan = d7.recorded_plan(pod) or {}
        excluded = {
            (entry.get("source_ref"), entry.get("reason"))
            for entry in plan.get("excluded") or []
        }
        self.report.check(
            "exclusion: the protected session's plan mounts p alone and excludes "
            "q beside_protected",
            plan.get("protected") is True
            and len(plan.get("mounts") or []) == 1
            and (q_folder, "beside_protected") in excluded,
            f"excluded {sorted(excluded)}",
        )
        self.delete_thread("beside")

    def cross(self) -> None:
        other_id = self.d4.other_id
        b = self.project("b")
        self.d4.owner.ok(
            "POST",
            f"/api/projects/{b}/members",
            {"user_id": other_id, "role": "editor"},
        )
        a2 = self.project("a2", who="other")
        b_folder = self.folder(b)
        b_kb = sql(
            f"SELECT id FROM datasources WHERE managed_key = {lit('project-kb:' + b)}"
        )

        def eligible() -> dict[str, dict]:
            rows = self.d4.other.ok(
                "GET",
                f"/api/datasources/eligible?project_id={a2}&execution=session"
                "&workspace_backend=sandbox",
            )
            return {str(row["id"]): row for row in rows}

        listed = eligible()
        row = listed.get(b_folder) or {}
        levels = {
            level["id"]: level
            for level in (row.get("access") or {}).get("levels") or []
        }
        self.report.check(
            "cross: an editor of b lists b's folder in a2, not preselected, with "
            "protected and read_write offered and no ceiling",
            bool(row)
            and row.get("default_selected") is False
            and levels.get("protected", {}).get("offered") is True
            and levels.get("read_write", {}).get("offered") is True
            and (row.get("access") or {}).get("ceiling") is None,
            f"{sorted(levels)} {row.get('default_selected')}",
        )
        self.report.check(
            "cross: b's knowledge base is never eligible in a2",
            not b_kb or b_kb not in listed,
            b_kb,
        )
        status, body = self.create(
            "cross",
            {
                "project_id": a2,
                "datasource_ids": [b_folder],
                "datasource_access": {b_folder: "protected"},
            },
            who="other",
        )
        if status not in (200, 201):
            raise d7.GateError(f"cross session -> HTTP {status}: {str(body)[:300]}")
        thread = self.threads["cross"][0]
        grant = self.wait_grant(thread, "active")
        source = self.source(thread) or {}
        self.report.check(
            "cross: a session of a2 protects b's folder, engaged from b",
            source.get("source_ref") == b
            and source.get("datasource_id") == b_folder
            and grant.get("selected_mount_id") == source.get("id"),
            str(source),
        )
        self.d4.owner.ok(
            "PATCH", f"/api/projects/{b}/members/{other_id}", {"role": "viewer"}
        )
        status, body = self.create(
            "viewer",
            {
                "project_id": a2,
                "datasource_ids": [b_folder],
                "datasource_access": {b_folder: "read_write"},
            },
            who="other",
        )
        detail = body.get("detail") if isinstance(body, dict) else {}
        self.report.check(
            "cross: as a viewer of b, read_write is 422 above_ceiling",
            status == 422
            and (detail or {}).get("code") == "connector_access_unavailable"
            and (detail or {}).get("reason") == "above_ceiling",
            f"HTTP {status} {str(body)[:200]}",
        )
        ceiling = ((eligible().get(b_folder) or {}).get("access") or {}).get("ceiling")
        self.report.check(
            "cross: the eligible list gives a viewer the read_only ceiling",
            ceiling == "read_only",
            str(ceiling),
        )
        # End must settle (status ended) before Resume re-engages; a session
        # whose End does not finish is reported, and the checks after it run.
        try:
            self.d4.other.call("DELETE", f"/api/persistent/threads/{thread}?force=true")
            wait_for(
                "the cross session ended",
                lambda: sql(f"SELECT status FROM threads WHERE id = {lit(thread)}")
                == "ended",
                timeout=600,
                interval=10,
            )
            status_resume, body = self.resume(self.d4.other, thread)
            if status_resume != 200:
                raise d7.GateError(f"resume answered HTTP {status_resume} {body}")
            code = wait_for(
                "the resumed engage refused",
                lambda: (
                    self.thread_row(thread)["metadata"].get(
                        "protected_cloud_error_code"
                    )
                    or None
                ),
                timeout=480,
                interval=5,
            )
        except GateError as exc:
            code = f"not observed: {exc}"
        self.report.check(
            "cross: demoted to viewer, the resumed session's engage is refused",
            code == "engage_refused"
            and (self.grant(thread) or {}).get("status") != "active",
            str(code),
        )
        self.d4.owner.ok("DELETE", f"/api/projects/{b}/members/{other_id}")
        listed = eligible()
        status, body = self.create(
            "outsider",
            {"project_id": a2, "datasource_ids": [b_folder]},
            who="other",
        )
        self.report.check(
            "cross: removed from b, its folder is not listed and naming it is 403",
            b_folder not in listed and status == 403,
            f"HTTP {status}",
        )
        self.delete_thread("cross")

    def alias(self) -> None:
        p = self.projects["p"][0] if "p" in self.projects else self.project("p")
        folder = self.folder(p)
        status, body = self.create(
            "alias-none",
            {"project_id": p, "datasource_ids": [], "protected_cloud": True},
        )
        self.report.check(
            "alias: protected_cloud without the folder attached is 422 "
            "protected_cloud_requires_folder",
            status == 422
            and (body.get("detail") or {}).get("code")
            == "protected_cloud_requires_folder",
            f"HTTP {status}",
        )
        status, body = self.create(
            "alias-conflict",
            {
                "project_id": p,
                "datasource_ids": [folder],
                "protected_cloud": True,
                "datasource_access": {folder: "read_write"},
            },
        )
        self.report.check(
            "alias: protected_cloud with a conflicting level is 422 "
            "protected_cloud_conflict",
            status == 422
            and (body.get("detail") or {}).get("code") == "protected_cloud_conflict",
            f"HTTP {status}",
        )
        status, body = self.create(
            "alias",
            {"project_id": p, "datasource_ids": [folder], "protected_cloud": True},
        )
        thread = self.threads.get("alias", ("", ""))[0]
        grant = self.wait_grant(thread, "active") if thread else {}
        source = self.source(thread) if thread else {}
        self.report.check(
            "alias: protected_cloud still engages the project's own folder",
            status in (200, 201)
            and (source or {}).get("datasource_id") == folder
            and grant.get("selected_mount_id") == (source or {}).get("id"),
            f"HTTP {status}",
        )
        self.delete_thread("alias")

    # -- cleanup ---------------------------------------------------------------
    def delete_thread(self, label: str) -> bool:
        thread, who = self.threads.get(label, ("", ""))
        if not thread:
            return True

        def gone() -> bool:
            status, _body = self.api(who).call(
                "DELETE", f"/api/persistent/threads/{thread}?force=true&permanent=true"
            )
            if status == 404:
                return True
            return sql(f"SELECT count(*) FROM threads WHERE id = {lit(thread)}") == "0"

        try:
            wait_for(f"session {label} deleted", gone, timeout=300, interval=5)
        except GateError:
            return False
        self.threads.pop(label, None)
        return True

    def sweep(self) -> int:
        """Remove what an earlier run of ``--gate-id`` left behind: its
        sessions, projects, second account and OAuth client."""
        self.d4.client_started = self.d4.account_started = True
        if not self.d4.keycloak("count").get("clients"):
            # The run's client went with its cleanup: log in through a new
            # one of this gate id, which the sweep then removes.
            created = self.d4.keycloak("create-client")
            self.d4.client_uuid = self.d4.receipt(created, "OAuth client")
        self.d4.owner.client_id = self.d4.other.client_id = self.d4.oauth_client
        self.d7.api = _OwnerApi(self.d4.owner)
        prefix = self.gate_id.replace("_", "\\_")
        for thread in filter(
            None,
            sql(
                "SELECT id FROM threads WHERE title LIKE "
                f"{lit('access gate ' + prefix + ' %')}"
            ).splitlines(),
        ):
            self.threads[thread] = (thread, "owner")
        for project in filter(
            None,
            sql(
                f"SELECT id FROM projects WHERE name LIKE {lit(prefix + ' %')}"
            ).splitlines(),
        ):
            self.projects[project] = (project, "owner")
        other = sql(
            "SELECT id FROM users WHERE preferred_username = "
            f"{lit(self.d4.other.username)}"
        )
        if _UUID_RE.fullmatch(other or ""):
            self.d4.other_id, self.d4.account_row = other, True
        problems = self.cleanup()
        return 1 if problems else 0

    def cleanup(self) -> list[str]:
        problems: list[str] = []

        def step(label: str, action: Callable[[], Any]) -> None:
            try:
                if action() is False:
                    problems.append(label)
            except GateError as exc:
                problems.append(f"{label} ({exc})")

        for label in list(self.threads):
            step(
                f"delete session {label}", lambda label=label: self.delete_thread(label)
            )
        for label, (project, _who) in list(self.projects.items()):

            def deleted(project=project) -> bool:
                status, _body = self.d4.owner.call("DELETE", f"/api/projects/{project}")
                return status in (200, 204, 404)

            step(
                f"delete project {label}",
                lambda deleted=deleted: bool(
                    wait_for("project deleted", deleted, timeout=180, interval=10)
                ),
            )
        problems += self.d4.cleanup()
        for problem in problems:
            print(f"cleanup: {problem} failed", flush=True)
        return problems

    def run(self) -> int:
        every = (self.chip, self.default, self.exclusion, self.cross, self.alias)
        phases = tuple(
            phase
            for phase in every
            if not self.args.phases or phase.__name__ in self.args.phases
        )
        try:
            self.preflight()
            # Both accounts log in with the run's own OAuth client.
            self.d4.accounts()
            self.d7.api = _OwnerApi(self.d4.owner)
            for phase in phases:
                try:
                    phase()
                except GateError as exc:
                    self.report.check(f"{phase.__name__}: completed", False, str(exc))
        finally:
            if not self.args.keep:
                self.cleanup()
        return 0 if self.report.passed else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--context", default=d7.LOCAL_CONTEXT)
    parser.add_argument("--namespace", default=d7.LOCAL_NAMESPACE)
    parser.add_argument("--run", action="store_true")
    parser.add_argument("--confirm")
    parser.add_argument("--gate-id")
    parser.add_argument("--user", default="test")
    parser.add_argument("--password", default="srw-k3d-dev-test")
    parser.add_argument("--model", default=d7.DEFAULT_MODEL)
    parser.add_argument("--pod-timeout", type=int, default=420)
    parser.add_argument("--turn-timeout", type=int, default=420)
    parser.add_argument("--keep", action="store_true", help="skip cleanup")
    parser.add_argument(
        "--phases",
        nargs="*",
        choices=("chip", "default", "exclusion", "cross", "alias"),
        help="run only these phases (exclusion needs p, made by chip or default)",
    )
    parser.add_argument(
        "--sweep",
        action="store_true",
        help="only remove what an earlier run of --gate-id left behind",
    )
    return parser


def validate(args: argparse.Namespace) -> None:
    if args.context != d7.LOCAL_CONTEXT or args.namespace != d7.LOCAL_NAMESPACE:
        raise SafetyError("this gate is restricted to k3d-srw/srw")
    if args.run and args.confirm != d7.LOCAL_CONFIRMATION:
        raise SafetyError(f"--run requires --confirm {d7.LOCAL_CONFIRMATION}")
    if not args.run and args.confirm is not None:
        raise SafetyError("--confirm is accepted only with --run")
    if args.gate_id is not None and not _GATE_ID_RE.fullmatch(args.gate_id):
        raise SafetyError("--gate-id must be ca- followed by 10 hex digits")
    if not d7._MODEL_RE.fullmatch(args.model):
        raise SafetyError("model id is malformed")
    if not d7._USER_RE.fullmatch(args.user):
        raise SafetyError("user name is malformed")
    if args.sweep and not args.gate_id:
        raise SafetyError("--sweep needs the --gate-id of the run to remove")
    args.require_protected = True


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
    gate = AccessGate(args)
    return gate.sweep() if args.sweep else gate.run()


if __name__ == "__main__":
    raise SystemExit(main())
