#!/usr/bin/env python3
"""Local k3d gate for cloud folders on Virtual Sessions (the ``cloud_remote`` form).

Design: knowledge-base/knowledge/features/cloud_folders_on_virtual_sessions.md,
"Acceptance (the k3d gate)". A Virtual Session's attached cloud connectors
reach its executor as remote descriptors; the Virtual backend serves them at
``cloud/<name>`` through explicit rclone operations, with no workspace Pod, no
FUSE and no copy. The folder set, its names and the prompt block are the
Container's (scripts/k3d-cloud-folder-connector-gate.py), which this gate
subclasses for its fixtures, turns, audit reads and cleanup.

Fixtures (disposable, named after the gate id): project ``main``; Virtual
sessions ``attached`` (ids omitted: the project folder and the owner's outbox),
``empty`` (``datasource_ids: []``) and ``readonly`` (the project folder at the
``read_only`` level) in it. Each session gets turns whose tool calls are what
the checks read: the file they write in Nextcloud, the tool results in the
next archived LLM request.

Checks (each printed PASS/FAIL; the exit status is 0 only if all pass):

  preflight   Tilt reports srw ``ok``; every orchestrator pod serves this
              checkout's Virtual planner and admission, every stateless agent
              pod the multi-remote backend, by marker
  attached    (cases 1, 2, 3, 6, 13) the session is admitted as Virtual with
              both connectors attached and its outbox folder created; no
              ``ws-thread`` Pod exists for it at any point; its record says
              ``delivery: remote`` and both folders report mounted after the
              probe; the prompt's <cloud_folders> block names both at
              cloud/<name> and never a tool the session lacks; a write to
              cloud/outbox/<file> is in agent-service's sessions/<id> when the
              turn is over; a file placed in the project folder is listed by
              the agent's next call with no restart; a root-wide search is
              refused with the guidance; per-turn wall time is recorded
  empty       (case 12) ``datasource_ids: []``: Virtual, nothing attached, no
              session folder, no remote record, and a turn answers
  readonly    (case 4) the project folder at read_only: a write under it is
              refused naming the folder and the level, nothing written
  cleanup     nothing this run created is left (sessions, project, rows); the
              permanent delete removes the outbox folder (case 10)

Not automated here (unit-tested, or manual): the edit conflict (case 5), the
50 MiB read refusal (7), the upgrade to a Container (8), the second executor
and the eviction scrub (9), the unshared outbox (11).

Run with the repository venv on the k3d-srw cluster, alone: a mutating gate.

  .venv/bin/python scripts/k3d-virtual-cloud-folders-gate.py          # plan
  .venv/bin/python scripts/k3d-virtual-cloud-folders-gate.py \\
      --run --confirm LOCAL-K3D-DISPOSABLE [--model gpt-5-mini] [--json out.json]
  .venv/bin/python scripts/k3d-virtual-cloud-folders-gate.py \\
      --run --confirm LOCAL-K3D-DISPOSABLE --sweep --gate-id vcf-0123456789
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import re
import secrets
import shutil
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
_CFC_GATE = ROOT / "scripts" / "k3d-cloud-folder-connector-gate.py"


def _load_cfc():
    name = "_k3d_cloud_folder_connector_gate"
    if name in sys.modules:
        return sys.modules[name]
    spec = importlib.util.spec_from_file_location(name, _CFC_GATE)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


cfc = _load_cfc()
d7 = cfc.d7
GateError = cfc.GateError
K = cfc.K
ORCHESTRATOR = cfc.ORCHESTRATOR
ORCHESTRATOR_CONTAINER = cfc.ORCHESTRATOR_CONTAINER
NEXTCLOUD = cfc.NEXTCLOUD
NEXTCLOUD_CONTAINER = cfc.NEXTCLOUD_CONTAINER
run, command, sql, sql_json, lit, wait_for = (
    cfc.run,
    cfc.command,
    cfc.sql,
    cfc.sql_json,
    cfc.lit,
    cfc.wait_for,
)
audit_sql, prompt_block, slug = cfc.audit_sql, cfc.prompt_block, cfc.slug
_UUID_RE = cfc._UUID_RE
_GATE_ID_RE = re.compile(r"vcf-[0-9a-f]{10}\Z")

#: What every orchestrator pod must serve byte for byte (this slice's files).
SERVED = (
    "src/orchestrator/services/cloud_remote_plan.py",
    "src/orchestrator/services/cloud_mount_status.py",
    "src/orchestrator/services/thread_workspace_delivery.py",
    "src/orchestrator/services/thread_admission.py",
    "src/orchestrator/services/agent_cloud_mounts.py",
    "src/orchestrator/services/cloud/nextcloud.py",
    "src/orchestrator/application/workspace.py",
    "src/orchestrator/application/preparation.py",
    "src/shared/connectors/builtin.py",
    "src/shared/connectors/contract.py",
)
#: What every stateless agent pod must serve: this slice, by a marker per file.
AGENT_SERVED = {
    "src/agent/core/backends/virtual.py": "class CloudRemote:",
    "src/agent/core/backends/factory.py": "def cloud_remote_from_entry(",
    "src/agent/api/persistent_session.py": "def _remote_cloud_folders(",
    "src/agent/api/session_attach.py": "_probe_and_report_remote_cloud_folders",
    "src/shared/runtime/core/backends/rclone.py": "def cloud_object_store_from_spec(",
}

#: A file placed into a Group Folder's data on disk, then scanned: ``$1`` the
#: folder id, ``$2`` the file name, ``$3`` its content.
_NEXTCLOUD_PUT_GROUP_FILE = r"""
data=$(php /var/www/html/occ config:system:get datadirectory)
dir="$data/__groupfolders/$1"
mkdir -p "$dir" && printf %s "$3" > "$dir/$2" && \
  php /var/www/html/occ groupfolders:scan "$1" >/dev/null 2>&1 && echo placed
"""

#: An existing file's content hash in a session folder: ``$1`` the folder id,
#: ``$2`` the file name (the connector gate's, reused).
_NEXTCLOUD_SESSION_FILE = cfc._NEXTCLOUD_SESSION_FILE

#: Whether a session folder still exists on disk: ``$1`` the folder id.
_NEXTCLOUD_SESSION_DIR = r"""
data=$(php /var/www/html/occ config:system:get datadirectory)
ls -d "$data"/*/files/sessions/"$1" 2>/dev/null | head -1
"""

WRITE_PROMPT = (
    "Use write_file to create the file {path} with exactly the single word "
    "ready as its content. Then reply with the single word done."
)
LIST_PROMPT = (
    "Use list_files on the folder {path} and reply with the names of the files "
    "it contains, one per line, nothing else."
)
ROOT_SEARCH_PROMPT = (
    "Call search_files with the query ready and no path (leave path empty). "
    "Reply with the single word done, whatever the tool returns."
)
READ_ONLY_PROMPT = (
    "Use write_file to create the file {path} with the single word ready as its "
    "content. If the tool refuses, reply with the single word refused and stop; "
    "otherwise reply with the single word written."
)


def _committed_or_working_bytes(paths: tuple[str, ...]) -> dict[str, str]:
    """The connector gate's ``committed_bytes`` for the files HEAD has; a file
    this slice adds (not committed yet) is compared against the working tree
    alone."""
    out: dict[str, str] = {}
    for path in paths:
        try:
            out.update(cfc.committed_bytes((path,)))
        except Exception:
            out.update(d7.expected_bytes((path,)))
    return out


PLAN = [
    "preflight: Tilt srw ok; orchestrator pods serve the Virtual planner and "
    "admission, stateless agent pods the multi-remote backend",
    "attached: ids omitted on Virtual -> admitted Virtual, both connectors "
    "attached, the outbox folder created; never a ws-thread Pod; the record "
    "says delivery remote and both folders mounted; the prompt block names "
    "cloud/<project> and cloud/outbox; a write lands in sessions/<id>; a file "
    "placed in the project folder is listed next call; a root-wide search is "
    "refused; turn wall times recorded",
    "empty: datasource_ids [] -> Virtual, nothing attached, no session folder, "
    "no remote record, a turn answers",
    "readonly: the project folder at read_only -> a write under it is refused "
    "naming the folder and level; nothing written",
    "cleanup: sessions and the project with its rows; the outbox folder gone "
    "after the permanent delete; residue check by gate id",
]

PHASES = ("attached", "empty", "readonly")


class VirtualCloudFoldersGate(cfc.CloudFolderConnectorGate):
    def __init__(self, args: argparse.Namespace) -> None:
        super().__init__(args)
        self.gate_id = args.gate_id or f"vcf-{secrets.token_hex(5)}"
        self.report = d7.Report(self.gate_id)
        self.defaults_virtual = False
        self.latency: dict[str, float] = {}
        self.folder_ids: dict[str, str] = {}  # session label -> session folder id

    # -- cluster reads -------------------------------------------------------
    def served(self) -> list[str]:
        saved = cfc.SERVED, cfc.AGENT_SERVED, cfc.committed_bytes
        cfc.SERVED, cfc.AGENT_SERVED = SERVED, AGENT_SERVED
        cfc.committed_bytes = _committed_or_working_bytes
        try:
            return super().served()
        finally:
            cfc.SERVED, cfc.AGENT_SERVED, cfc.committed_bytes = saved

    def thread_state(self, thread: str) -> dict[str, Any]:
        return (
            sql_json(
                "SELECT json_build_object("
                "'ids', metadata->'datasource_ids', "
                "'backend', metadata->'config_override'->'workspace'->>'backend', "
                "'selection', metadata->'workspace_selection', "
                "'outbox', metadata->'cloud_outbox', "
                "'status', metadata->'cloud_mount_status', "
                "'access', metadata->'datasource_access', "
                "'handle', main_cloud_session_handle, "
                "'share', main_cloud_share_handle, "
                "'lifecycle', status) FROM threads WHERE id = "
                f"{lit(thread)}"
            )
            or {}
        )

    def thread_pods(self, thread: str) -> list[str]:
        items = json.loads(
            command(K + ["get", "pods", "-l", f"srw/thread-id={thread}", "-o", "json"])
        )["items"]
        return [pod["metadata"]["name"] for pod in items]

    def folder_id_of(self, state: dict[str, Any]) -> str:
        handle = str(state.get("handle") or "")
        match = re.search(r'"native_id"\s*:\s*"?([0-9A-Za-z._-]+)"?', handle)
        return match.group(1) if match else handle.rsplit("/", 1)[-1].strip('"}')

    def group_folder_id(self, project_label: str) -> str:
        handle = sql(
            "SELECT main_cloud_folder_handle FROM projects WHERE id = "
            f"{lit(self.projects[project_label])}"
        )
        match = re.search(r'"native_id"\s*:\s*"?([0-9]+)"?', handle)
        if not match:
            raise GateError(f"project {project_label} has no Group Folder id")
        return match.group(1)

    def nextcloud(self, script: str, *args: str) -> str:
        return command(
            K
            + ["exec", NEXTCLOUD, "-c", NEXTCLOUD_CONTAINER, "--"]
            + ["su", "-s", "/bin/sh", "www-data", "-c", script, "gate", *args],
            timeout=180,
        ).strip()

    def requests_of(self, thread: str) -> int:
        return int(
            audit_sql(
                "SELECT count(*) FROM llm_requests WHERE job_id::text = "
                f"{lit(thread)} AND call_type = 'main'"
            )
            or "0"
        )

    def request_text_with(self, thread: str, needle: str, *, after: int) -> str:
        """The first archived main request past the ``after``-th that holds
        ``needle`` (a tool result lands in the request that follows it)."""

        def probe() -> str:
            text = audit_sql(
                "SELECT request::text FROM llm_requests WHERE job_id::text = "
                f"{lit(thread)} AND call_type = 'main' ORDER BY timestamp ASC "
                f"OFFSET {int(after)}"
            )
            return text if needle in text else ""

        return wait_for(
            f"an LLM request holding {needle[:40]!r}",
            probe,
            timeout=self.args.turn_timeout,
            interval=10,
        )

    # -- fixtures ------------------------------------------------------------
    def start_virtual_session(
        self,
        label: str,
        project_label: str,
        *,
        datasource_ids: list[str] | None = None,
        datasource_access: dict[str, str] | None = None,
        explicit: bool | None = None,
    ) -> cfc.Session:
        session = self.sessions[label] = cfc.Session(label, project_label)
        body: dict[str, Any] = {
            "title": self.title(label),
            "permission_mode": "autonomous",
            "project_id": self.projects[project_label],
            "model": self.args.model,
        }
        if explicit is None:
            explicit = not self.defaults_virtual
        if explicit:
            body["config_override"] = {"workspace": {"backend": "virtual"}}
        if datasource_ids is not None:
            body["datasource_ids"] = datasource_ids
        if datasource_access:
            body["datasource_access"] = datasource_access
        created = self.create(
            "/api/persistent/threads",
            body,
            lambda: sql(
                f"SELECT id FROM threads WHERE title = {lit(self.title(label))} LIMIT 1"
            ),
        )
        session.thread = str(created.get("thread_id") or created["id"])
        print(f"session {label} {session.thread}", flush=True)
        return session

    def send(self, session: cfc.Session, text: str) -> float:
        """One turn; the wall time until its first archived LLM request grows
        by one is what the latency note records."""
        before = self.requests_of(session.thread)
        started = time.monotonic()
        deadline = started + self.args.turn_timeout
        while True:
            try:
                status, body = self.api.call(
                    "POST",
                    f"/api/persistent/threads/{session.thread}/input",
                    {"content": text},
                )
            except GateError as exc:  # the orchestrator reloading
                status, body = 503, {"error": str(exc)}
            if status in (200, 201, 202, 204):
                break
            retryable = d7.input_retryable(status, body) or status >= 500
            if not retryable or time.monotonic() > deadline:
                raise GateError(
                    f"input to {session.label} -> HTTP {status}: {str(body)[:300]}"
                )
            time.sleep(5)
        wait_for(
            f"the {session.label} session's turn reaches the LLM",
            lambda: self.requests_of(session.thread) > before,
            timeout=self.args.turn_timeout,
            interval=5,
        )
        return time.monotonic() - started

    # -- phases ------------------------------------------------------------
    def preflight(self) -> None:
        if shutil.which("tilt"):

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
            "checkout's cloud folders on Virtual",
            not problems,
            "; ".join(problems)[:600],
        )
        if problems:
            raise GateError("the deployment does not serve this checkout")
        driver = self.orchestrator_env("CLOUD_WORKSPACE_DRIVER") or "rclone_mount"
        self.report.check(
            "preflight: the cloud workspace driver is rclone_mount",
            driver == "rclone_mount",
            driver,
        )
        defaults = self.orchestrator_env("WORKSPACE_DEFAULTS")
        try:
            self.defaults_virtual = (json.loads(defaults) if defaults else {}).get(
                "sessions"
            ) == "virtual"
        except ValueError:
            self.defaults_virtual = False
        self.report.note(
            "sessions default to Virtual: the tier is left to the defaults"
            if self.defaults_virtual
            else f"WORKSPACE_DEFAULTS={defaults!r}: Virtual is chosen explicitly"
        )
        self.user_id = sql(
            f"SELECT id FROM users WHERE preferred_username = {lit(self.args.user)}"
        )
        if not _UUID_RE.fullmatch(self.user_id):
            raise GateError(f"no app user {self.args.user!r}")
        self.outbox_id = sql(
            "SELECT id FROM datasources WHERE managed_key = "
            f"{lit('cloud-outbox:' + self.user_id)}"
        )
        if not _UUID_RE.fullmatch(self.outbox_id):
            raise GateError("the gate user has no outbox connector")
        self.create_project("main")

    def attached(self) -> None:
        session = self.start_virtual_session("attached", "main")
        state = self.thread_state(session.thread)
        folder = self.folders["main"]
        name = slug(self.name("main"))
        tier = ((state.get("selection") or {}).get("sources") or {}).get("tier")
        self.report.check(
            "folders: the session is admitted as Virtual with the project folder "
            "and the outbox attached (no cloud_connectors upgrade)",
            state.get("backend") == "virtual"
            and {folder, self.outbox_id} <= set(state.get("ids") or [])
            and tier != "cloud_connectors",
            json.dumps(
                {"backend": state.get("backend"), "tier": tier, "ids": state.get("ids")}
            ),
        )
        outbox = state.get("outbox") or {}
        self.report.check(
            "folders: SRW created the session's outbox folder and recorded it",
            bool(state.get("handle"))
            and outbox.get("connector_id") == self.outbox_id
            and outbox.get("state") == "ready",
            json.dumps({"handle": bool(state.get("handle")), "outbox": outbox}),
        )
        self.folder_ids["attached"] = self.folder_id_of(state)
        pods_before = self.thread_pods(session.thread)
        self.report.check(
            "folders: no workspace Pod exists for the session",
            not any(p.startswith("ws-thread-") for p in pods_before),
            json.dumps(pods_before),
        )

        report_name = f"{self.gate_id}-report.md"
        self.latency["attached:write"] = self.send(
            session, WRITE_PROMPT.format(path=f"cloud/outbox/{report_name}")
        )
        block = prompt_block(
            audit_sql(
                "SELECT request::text FROM llm_requests WHERE job_id::text = "
                f"{lit(session.thread)} AND call_type = 'main' "
                "ORDER BY timestamp ASC LIMIT 1"
            )
        )
        self.report.check(
            "folders: the prompt's cloud folder block names cloud/<project> and "
            "cloud/outbox, the outbox as not the project folder, and no tool the "
            "session lacks",
            f"- cloud/{name}:" in block
            and "- cloud/outbox: a folder SRW created for this session" in block
            and "not the project folder" in block
            and "srw_cloud_status" not in block,
            block.strip()[:500],
        )

        def remote_record() -> dict | None:
            status = (self.thread_state(session.thread).get("status") or {}) or {}
            mounts = status.get("mounts") or {}
            if status.get("delivery") != "remote" or not mounts:
                return None
            if all(
                m.get("state") in ("mounted", "unavailable") for m in mounts.values()
            ):
                return status
            return None

        try:
            status = wait_for(
                "the remote record settles", remote_record, timeout=180, interval=5
            )
        except GateError:
            status = self.thread_state(session.thread).get("status") or {}
        mounts = (status or {}).get("mounts") or {}
        self.report.check(
            "folders: the record says delivery remote and both folders report "
            "mounted after the probe",
            (status or {}).get("delivery") == "remote"
            and {m for m in mounts} == {name, "outbox"}
            and all(m.get("state") == "mounted" for m in mounts.values()),
            json.dumps({k: v.get("state") for k, v in mounts.items()})
            + f" delivery={(status or {}).get('delivery')!r}",
        )

        folder_id = self.folder_ids["attached"]
        digests = {
            hashlib.sha256(b"ready").hexdigest(),
            hashlib.sha256(b"ready\n").hexdigest(),
        }
        try:
            reached = wait_for(
                "the agent's write reaches agent-service's session folder",
                lambda: self.nextcloud(_NEXTCLOUD_SESSION_FILE, folder_id, report_name)
                in digests,
                timeout=self.args.turn_timeout,
                interval=5,
            )
        except GateError:
            reached = False
        self.report.check(
            "folders: write_file cloud/outbox/<file> lands in sessions/<id>/<file> "
            "with the written content",
            bool(reached),
            f"sessions/{folder_id}/{report_name}",
        )

        placed_name = f"{self.gate_id}-human.txt"
        placed = self.nextcloud(
            _NEXTCLOUD_PUT_GROUP_FILE,
            self.group_folder_id("main"),
            placed_name,
            "placed by the gate",
        )
        if placed != "placed":
            raise GateError("could not place a file in the project folder")
        before = self.requests_of(session.thread)
        self.latency["attached:list"] = self.send(
            session, LIST_PROMPT.format(path=f"cloud/{name}")
        )
        try:
            listed = bool(
                self.request_text_with(session.thread, placed_name, after=before)
            )
        except GateError:
            listed = False
        self.report.check(
            "folders: a file placed in the project folder is listed by the agent's "
            "next call, with no restart",
            listed,
            placed_name,
        )

        before = self.requests_of(session.thread)
        self.latency["attached:search"] = self.send(session, ROOT_SEARCH_PROMPT)
        try:
            refused = bool(
                self.request_text_with(
                    session.thread, "search_files needs a path", after=before
                )
            )
        except GateError:
            refused = False
        self.report.check(
            "folders: a root-wide search_files is refused with the guidance to "
            "name a folder",
            refused,
        )
        pods_after = self.thread_pods(session.thread)
        self.report.check(
            "folders: still no workspace Pod after three turns",
            not any(p.startswith("ws-thread-") for p in pods_after),
            json.dumps(pods_after),
        )
        self.report.note(
            "latency (seconds from input to the turn's LLM request): "
            + json.dumps({k: round(v, 1) for k, v in self.latency.items()})
        )

    def empty(self) -> None:
        session = self.start_virtual_session("empty", "main", datasource_ids=[])
        state = self.thread_state(session.thread)
        self.report.check(
            "empty: Virtual, nothing attached, no session folder, no remote record",
            state.get("backend") == "virtual"
            and (state.get("ids") or []) == []
            and not state.get("handle")
            and not state.get("status"),
            json.dumps(
                {k: state.get(k) for k in ("backend", "ids", "handle", "status")}
            ),
        )
        self.latency["empty:turn"] = self.send(session, d7.TURN_PROMPT)
        text = audit_sql(
            "SELECT request::text FROM llm_requests WHERE job_id::text = "
            f"{lit(session.thread)} AND call_type = 'main' ORDER BY timestamp ASC LIMIT 1"
        )
        self.report.check(
            "empty: the turn reaches the LLM with no cloud folder block",
            bool(text) and "<cloud_folders>" not in text,
        )

    def readonly(self) -> None:
        folder = self.folders["main"]
        session = self.start_virtual_session(
            "readonly",
            "main",
            datasource_ids=[folder, self.outbox_id],
            datasource_access={folder: "read_only"},
        )
        state = self.thread_state(session.thread)
        name = slug(self.name("main"))
        self.report.check(
            "readonly: the project folder binds at read_only on Virtual",
            state.get("backend") == "virtual"
            and (state.get("access") or {}).get(folder) == "read_only",
            json.dumps(
                {"backend": state.get("backend"), "access": state.get("access")}
            ),
        )
        target = f"cloud/{name}/{self.gate_id}-ro.txt"
        before = self.requests_of(session.thread)
        self.latency["readonly:write"] = self.send(
            session, READ_ONLY_PROMPT.format(path=target)
        )
        try:
            refused = bool(
                self.request_text_with(
                    session.thread,
                    "read-only cloud folder (level read_only)",
                    after=before,
                )
            )
        except GateError:
            refused = False
        on_disk = self.nextcloud(
            r"""
data=$(php /var/www/html/occ config:system:get datadirectory)
[ -e "$data/__groupfolders/$1/$2" ] && echo present || true
""",
            self.group_folder_id("main"),
            f"{self.gate_id}-ro.txt",
        )
        self.report.check(
            "readonly: write_file under the read-only folder is refused naming the "
            "folder and the level, and nothing is written",
            refused and not on_disk,
            f"refused={refused} on_disk={on_disk!r}",
        )

    # -- cleanup -------------------------------------------------------------
    def residue(self) -> list[str]:
        left = super().residue()
        folder_id = self.folder_ids.get("attached")
        if folder_id:
            try:
                present = wait_for(
                    "the outbox folder is gone after the permanent delete",
                    lambda: ""
                    if self.nextcloud(_NEXTCLOUD_SESSION_DIR, folder_id)
                    else "gone",
                    timeout=180,
                    interval=10,
                )
            except GateError:
                present = ""
            if present != "gone":
                left.append(f"the outbox folder sessions/{folder_id} still exists")
        return left

    def run(self) -> int:
        try:
            self.preflight()
            for phase in (self.attached, self.empty, self.readonly):
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
                    "cleanup: nothing this run created is left; the outbox folder is "
                    "gone after the permanent delete",
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
    parser = cfc.build_parser()
    parser.description = "k3d gate: cloud folders on Virtual Sessions (cloud_remote)"
    for action in parser._actions:
        if action.dest == "phases":
            action.choices = list(PHASES)
            action.help = f"run only these session phases (of {', '.join(PHASES)})"
    return parser


def validate(args: argparse.Namespace) -> None:
    """The connector gate's safety contract, with this gate's own id pattern."""
    if args.context != cfc.LOCAL_CONTEXT or args.namespace != cfc.LOCAL_NAMESPACE:
        raise cfc.SafetyError("this gate is restricted to k3d-srw/srw")
    if args.run and args.confirm != cfc.LOCAL_CONFIRMATION:
        raise cfc.SafetyError(f"--run requires --confirm {cfc.LOCAL_CONFIRMATION}")
    if not args.run and args.confirm is not None:
        raise cfc.SafetyError("--confirm is accepted only with --run")
    if args.gate_id is not None and not _GATE_ID_RE.fullmatch(args.gate_id):
        raise cfc.SafetyError("--gate-id must be vcf- followed by 10 hex digits")
    if args.sweep and (not args.run or args.gate_id is None or args.keep):
        raise cfc.SafetyError(
            "--sweep needs --run, --confirm and --gate-id, not --keep"
        )
    if not d7._MODEL_RE.fullmatch(args.model):
        raise cfc.SafetyError("model id is malformed")
    if not d7._USER_RE.fullmatch(args.user):
        raise cfc.SafetyError("user name is malformed")
    if not 60 <= args.pod_timeout <= 1800:
        raise cfc.SafetyError("--pod-timeout must be between 60 and 1800 seconds")
    if not 60 <= args.turn_timeout <= 1800:
        raise cfc.SafetyError("--turn-timeout must be between 60 and 1800 seconds")


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        validate(args)
    except cfc.SafetyError as exc:
        print(f"refused: {exc}", file=sys.stderr)
        return 2
    if not args.run:
        print(f"plan (dry run; add --run --confirm {cfc.LOCAL_CONFIRMATION}):")
        for line in PLAN:
            print(f"  - {line}")
        return 0
    gate = VirtualCloudFoldersGate(args)
    if args.sweep:
        return gate.sweep()
    return gate.run()


if __name__ == "__main__":
    sys.exit(main())
