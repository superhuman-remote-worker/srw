#!/usr/bin/env python3
"""Local k3d gate for srw, the command-line client (cli/).

Design: knowledge-base/knowledge/features/srw_cli.md (sections 15 and 16,
tier 5). The client is written against an API without typed responses, so
only a run against a real installation shows that the two still fit. This
gate runs the built binary against the local k3d cluster, as an admin and as
an ordinary user, and checks what a person or a pipeline would see: exit
codes, tables and messages.

Safety envelope, as for the other k3d gates: dry-run by default; only the
exact context k3d-srw and namespace srw; the server must be a localhost
address; tokens are generated here, only their hashes reach the database,
and they are scrubbed from every printed line; cleanup runs in ``finally``
and touches only what this run created (named after the gate id).

Fixtures: six personal access tokens and one MCP token, written into
``auth_tokens`` by SQL with a three-hour expiry and revoked at the end; an
Expert, a WorkspaceTemplate and a Project applied from manifests; three
small jobs on a cheap model.

Phases (each check is printed PASS or FAIL; the exit status is 0 only if
every check passed):

  identity   whoami and version, for both users
  scopes     a jobs:read token validates and previews and cannot apply; an
             ordinary user applies into the own account and not into the
             shared catalog; a Job needs jobs:write and a Project
             projects:write; an MCP token is not scope-checked
  objects    a Project is created and deleted from a manifest; the lists of
             experts, projects, connectors and sessions; a stored lookup by
             name; export, and apply of the export without a change
  job-admin  run --autonomy full --wait (0); get, describe, logs, files, cp
  job-user   run with the applied expert and default autonomy; logs -f; wait
             (10); approve; wait (0)
  lifecycle  pause (wait 10), resume, cancel (wait 8), delete job
  cleanup    delete what the run created; revoke its tokens

`wait` and `logs -f` ride out a restart of the orchestrator, so a Tilt
rebuild in the middle slows a run down and should not fail it. A run that
was interrupted leaves its objects behind; every job carries the gate id in
its task, so ``--sweep`` finds and removes them later.

  .venv/bin/python scripts/k3d-cli-gate.py                    # the plan
  .venv/bin/python scripts/k3d-cli-gate.py --run --confirm LOCAL-K3D-DISPOSABLE
  .venv/bin/python scripts/k3d-cli-gate.py --run --confirm LOCAL-K3D-DISPOSABLE \\
      --gate-id clig-0123abcd --sweep                         # only clean up
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import subprocess
import sys
import tempfile
import time
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[1]
CONTEXT = "k3d-srw"
NAMESPACE = "srw"
LOCAL_CONFIRMATION = "LOCAL-K3D-DISPOSABLE"
PHASES = {
    "identity": "whoami and version, as the admin and as the ordinary user",
    "scopes": "what each token scope may and may not do with manifests",
    "objects": "a project from a manifest, the lists, lookups by name, export",
    "job-admin": "run --wait with full autonomy, then read the job back",
    "job-user": "run with default autonomy, follow, wait, approve",
    "lifecycle": "pause, resume, cancel and delete a job",
    "cleanup": "delete what the run created and revoke its tokens",
}
_EMAIL = re.compile(r"^[A-Za-z0-9._+-]+@[A-Za-z0-9.-]+$")
_MODEL = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/:-]{0,99}$")
_GATE_ID = re.compile(r"^clig-[0-9a-f]{8}$")
_UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")

#: name -> (which user, token kind, scopes)
TOKENS = {
    "u_read": ("user", "api", ("jobs:read",)),
    "u_write": ("user", "api", ("jobs:read", "resources:write")),
    "u_full": (
        "user",
        "api",
        ("jobs:read", "jobs:write", "resources:write", "projects:write"),
    ),
    "a_admin": ("admin", "api", ("admin",)),
    "a_scoped": ("admin", "api", ("jobs:read", "resources:write")),
    "u_mcp": ("user", "mcp", ()),
}


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--context", default=CONTEXT)
    p.add_argument("--namespace", default=NAMESPACE)
    p.add_argument("--run", action="store_true", help="touch the cluster")
    p.add_argument("--confirm", default="", help=f"must be {LOCAL_CONFIRMATION}")
    p.add_argument("--server", default="https://api.localhost")
    p.add_argument("--binary", default="", help="a built srw; built from cli/ if empty")
    p.add_argument("--model", default="deepseek-flash", help="a cheap enabled model")
    p.add_argument("--user", default="kbsmoke-owner@example.invalid")
    p.add_argument("--admin", default="admin@localhost")
    p.add_argument("--gate-id", default="")
    p.add_argument("--wait", default="8m", help="how long one job may take")
    p.add_argument(
        "--sweep",
        action="store_true",
        help="only remove what the run named by --gate-id left behind",
    )
    return p


def refusal(args: argparse.Namespace) -> str | None:
    """Why this invocation is outside the local, disposable boundary."""
    if args.context != CONTEXT or args.namespace != NAMESPACE:
        return f"only the context {CONTEXT} and the namespace {NAMESPACE} are allowed"
    if args.run != (args.confirm == LOCAL_CONFIRMATION):
        return f"--run needs --confirm {LOCAL_CONFIRMATION}, and only together"
    host = (urlsplit(args.server).hostname or "").lower()
    if urlsplit(args.server).scheme not in ("http", "https") or not (
        host == "localhost" or host.endswith(".localhost") or host == "127.0.0.1"
    ):
        return "--server must be a localhost address of the k3d cluster"
    if not _MODEL.match(args.model):
        return "--model is not a model id"
    if not _EMAIL.match(args.user) or not _EMAIL.match(args.admin):
        return "--user and --admin are e-mail addresses of existing accounts"
    if args.gate_id and not _GATE_ID.match(args.gate_id):
        return "--gate-id looks like clig-<8 hex digits>"
    if not re.fullmatch(r"[1-9][0-9]?m", args.wait):
        return "--wait is a number of minutes, for example 8m"
    if args.sweep and not (args.run and args.gate_id):
        return "--sweep removes things: it needs --run, --confirm and --gate-id"
    return None


def mint_sql(gate_id: str, emails: dict[str, str], digests: dict[str, str]) -> str:
    """The INSERTs for the run's tokens. Only hashes go to the database."""
    lines = []
    for name, (who, kind, scopes) in TOKENS.items():
        scope_list = (
            "ARRAY[" + ",".join(f"'{s}'" for s in scopes) + "]::text[]"
            if scopes
            else "NULL"
        )
        lines.append(
            "INSERT INTO auth_tokens (user_id, name, token_hash, token_prefix, "
            "kind, scope, scopes, expires_at) "
            f"SELECT id, '{gate_id}-{name}', '{digests[name]}', "
            f"'{'ak_' if kind == 'api' else 'srw_'}gate', '{kind}', 'user', "
            f"{scope_list}, now() + interval '3 hours' "
            f"FROM users WHERE email = '{emails[who]}';"
        )
    return "\n".join(lines) + "\n"


def revoke_sql(gate_id: str) -> str:
    """Revoke the run's tokens. Nothing else is written by SQL: what the run
    stored is removed through the API, as a user would remove it."""
    return (
        "UPDATE auth_tokens SET revoked_at = now() "
        f"WHERE name LIKE '{gate_id}-%' AND revoked_at IS NULL;\n"
    )


def leftover_sql(gate_id: str) -> str:
    """Counts what a finished run must not leave: stored resources and jobs."""
    return (
        "SELECT (SELECT count(*) FROM srw_resources "
        f"WHERE name LIKE '{gate_id}-%' AND deleted_at IS NULL) + "
        f"(SELECT count(*) FROM jobs WHERE description LIKE '%{job_tag(gate_id)}%');\n"
    )


def job_tag(gate_id: str) -> str:
    """What every task of a run ends with, so its jobs can be found again."""
    return f"(gate {gate_id})"


def leftover_jobs_sql(gate_id: str) -> str:
    return f"SELECT id FROM jobs WHERE description LIKE '%{job_tag(gate_id)}%';\n"


def manifests(gate_id: str, model: str) -> dict[str, str]:
    """The run's manifest files, by file name."""
    return {
        "expert.yaml": (
            "apiVersion: srw/v1alpha1\nkind: Expert\n"
            f"metadata:\n  name: {gate_id}-helper\n"
            "spec:\n  runtime:\n    adapter: srw/v1\n    config:\n"
            "      config_name: worker_base\n      prompts:\n"
            "        persona: Answer briefly and finish.\n"
        ),
        "template.json": json.dumps(
            {
                "apiVersion": "srw/v1alpha1",
                "kind": "WorkspaceTemplate",
                "metadata": {"name": f"{gate_id}-ws"},
                "spec": {
                    "backend": "sandbox",
                    "resources": {"cpu": 1, "memory": "1Gi", "storage": "5Gi"},
                    "environment": {
                        "image": "registry.example/team/workspace@sha256:"
                        + "0123456789abcdef" * 4,
                        "pullPolicy": "IfNotPresent",
                    },
                },
            }
        ),
        "job.yaml": (
            "apiVersion: srw/v1alpha1\nkind: Job\n"
            f"metadata:\n  name: {gate_id}-job\n"
            "spec:\n  task:\n    text: Reply with the single word OK and finish.\n"
            "  execution:\n    expert:\n      inline:\n        runtime:\n"
            "          adapter: srw/v1\n"
            f"          config: {{config_name: worker_base, llm: {{model: {model}}}}}\n"
            "    connectors: {}\n"
        ),
        "project.yaml": (
            "apiVersion: srw/v1alpha1\nkind: Project\n"
            f"metadata:\n  name: {gate_id}-project\nspec:\n  resources: {{}}\n"
        ),
        "broken.yaml": "apiVersion: srw/v1alpha1\nkind: Expert\nkind: Twice\n",
    }


class Gate:
    """One run: the binary, the tokens and the checks."""

    def __init__(self, args: argparse.Namespace, binary: Path, work: Path):
        self.args, self.binary, self.work = args, binary, work
        self.gate_id = args.gate_id or "clig-" + secrets.token_hex(4)
        self.tokens = {
            name: ("ak_" if kind == "api" else "srw_") + secrets.token_urlsafe(32)
            for name, (_, kind, _) in TOKENS.items()
        }
        self.failed: list[str] = []
        self.jobs: list[tuple[str, str]] = []
        self.files = {}
        for name, text in manifests(self.gate_id, args.model).items():
            path = work / name
            path.write_text(text)
            self.files[name] = str(path)

    # -- plumbing -----------------------------------------------------------

    def scrub(self, text: str) -> str:
        for token in self.tokens.values():
            text = text.replace(token, "<token>")
        return text

    def psql(self, sql: str) -> str:
        done = subprocess.run(
            [
                "kubectl",
                f"--context={CONTEXT}",
                "-n",
                NAMESPACE,
                "exec",
                "-i",
                "srw-postgres-0",
                "-c",
                "postgres",
                "--",
                "psql",
                "-U",
                "postgres",
                "-d",
                "srw",
                "-At",
                "-v",
                "ON_ERROR_STOP=1",
            ],
            input=sql,
            capture_output=True,
            text=True,
            timeout=60,
        )
        if done.returncode != 0:
            raise RuntimeError("psql failed: " + self.scrub(done.stderr.strip()))
        return done.stdout

    def srw(self, token: str, *argv: str, stdin: str = "", timeout: int = 900):
        """Run the binary in environment mode. Returns (code, stdout, stderr)."""
        done = subprocess.run(
            [str(self.binary), *argv],
            input=stdin,
            capture_output=True,
            text=True,
            timeout=timeout,
            env={
                "HOME": str(self.work / "home"),
                "PATH": os.environ.get("PATH", ""),
                "SRW_API_URL": self.args.server,
                "SRW_TOKEN": self.tokens[token],
            },
        )
        return done.returncode, done.stdout, done.stderr

    def check(self, label: str, result, code: int, *needles: str) -> str:
        """Record one check: the exit code, and texts the output must hold."""
        got, out, err = result
        text = out + err
        problem = verdict(got, text, code, needles)
        print(f"  {'FAIL' if problem else 'PASS'}  {label}")
        if problem:
            self.failed.append(label)
            print("        " + problem)
            print(
                "        " + self.scrub(text.strip()[-400:]).replace("\n", "\n        ")
            )
        return out

    # -- phases -------------------------------------------------------------

    def identity(self) -> None:
        s, c = self.srw, self.check
        c(
            "whoami as the ordinary user",
            s("u_write", "whoami"),
            0,
            "Scopes:",
            "jobs:read, resources:write",
            "no (this token has no admin scope)",
        )
        c("whoami as the admin", s("a_admin", "whoami"), 0, "Admin:", "yes")
        c("version names the manifest API", s("u_read", "version"), 0, "srw/v1alpha1")

    def scopes(self) -> None:
        f, s, c = self.files, self.srw, self.check
        c("jobs:read validates", s("u_read", "validate", "-f", f["expert.yaml"]), 0)
        c("jobs:read previews", s("u_read", "preview", "-f", f["expert.yaml"]), 0)
        c(
            "jobs:read cannot apply",
            s("u_read", "apply", "-f", f["expert.yaml"]),
            3,
            "requires resources:write",
        )
        c(
            "an ordinary user applies into the own account",
            s("u_write", "apply", "-f", f["expert.yaml"]),
            0,
            "changed (version 1)",
        )
        c(
            "the shared catalog is refused to an ordinary user",
            s("u_write", "apply", "-f", f["expert.yaml"], "--catalog"),
            3,
        )
        c(
            "and to an admin's token without the admin scope",
            s("a_scoped", "apply", "-f", f["expert.yaml"], "--catalog"),
            3,
        )
        c(
            "a Job needs jobs:write",
            s("u_write", "apply", "-f", f["job.yaml"]),
            3,
            "requires jobs:write",
        )
        c(
            "a Project needs projects:write",
            s("u_write", "apply", "-f", f["project.yaml"]),
            3,
            "requires projects:write",
        )
        c(
            "an error names its file",
            s("u_read", "validate", "-f", f["expert.yaml"], "-f", f["broken.yaml"]),
            1,
            "DuplicateKey",
            f["broken.yaml"],
        )
        c(
            "an MCP token is not scope-checked",
            s("u_mcp", "validate", "-f", f["expert.yaml"]),
            0,
        )
        events = self.psql(
            "SELECT count(*) FROM security_events WHERE event_type='token_scope_denied' "
            f"AND created_at > now() - interval '10 minutes' AND user_id = "
            f"(SELECT id FROM users WHERE email='{self.args.user}');"
        ).strip()
        c(
            "each refusal left a security event",
            (0 if int(events or 0) >= 3 else 1, events, ""),
            0,
        )

    def objects(self) -> None:
        f, s, c, gid = self.files, self.srw, self.check, self.gate_id
        c(
            "a Project with projects:write",
            s("u_full", "apply", "-f", f["project.yaml"]),
            0,
        )
        c(
            "it is in the project list",
            s("u_full", "get", "projects"),
            0,
            f"{gid}-project",
        )
        c(
            "delete project needs projects:write",
            s("u_write", "delete", "project", f"{gid}-project", "--yes"),
            3,
        )
        c(
            "delete project",
            s("u_full", "delete", "project", f"{gid}-project", "--yes"),
            0,
        )
        c(
            "the expert is listed under its own name",
            s("u_read", "get", "experts"),
            0,
            f"{gid}-helper ",
        )
        c("connectors list", s("u_read", "get", "connectors"), 0)
        c("sessions list", s("a_admin", "get", "sessions"), 0)
        c(
            "apply a workspace template",
            s("u_write", "apply", "-f", f["template.json"]),
            0,
        )
        c(
            "find it by name",
            s("u_read", "get", "wt", f"{gid}-ws", "-o", "name"),
            0,
            "workspacetemplate/",
        )
        c(
            "an unknown kind is refused by the server",
            s("u_read", "api", "/api/resources?kind=Nope"),
            1,
            "Unknown resource kind",
        )
        exported = c(
            "export by kind and name, in reading order",
            s("u_read", "export", "expert", f"{gid}-helper"),
            0,
            f"apiVersion: srw/v1alpha1\nkind: Expert\nmetadata:\n  name: {gid}-helper\n",
        )
        c(
            "the export applies back without a change",
            s("u_write", "apply", "-f", "-", stdin=exported),
            0,
            "unchanged (version 1)",
        )
        c(
            "delete the template by name",
            s("u_write", "delete", "wt", f"{gid}-ws", "--account", "--yes"),
            0,
        )

    def run_job(self, token: str, task: str, *extra: str) -> str:
        code, out, err = self.srw(
            token,
            "run",
            f"{task} {job_tag(self.gate_id)}",
            "--model",
            self.args.model,
            "--no-connectors",
            "-o",
            "name",
            *extra,
        )
        found = _UUID.search(out)
        self.check("the job starts", (code, out, err), 0, "job/")
        if not found:
            raise RuntimeError("no job id; the remaining job checks cannot run")
        self.jobs.append((token, found.group(0)))
        return found.group(0)

    def approve(self, token: str, job: str, tries: int = 4, pause: float = 5.0):
        """approve, again while the server answers a conflict.

        A job that has just stopped for review can refuse its approval for a
        moment: its agent is being deregistered (409, "pinned approval agent
        changed"). A script is fast enough to meet that; a person is not.
        The run says when it happened.
        """
        refused = 0
        result = self.srw(token, "approve", job)
        while result[0] == 5 and refused < tries - 1:
            refused += 1
            time.sleep(pause)
            result = self.srw(token, "approve", job)
        if refused:
            print(f"  NOTE  approve answered a conflict {refused} time(s) first")
        return result

    def job_admin(self) -> None:
        s, c, wait = self.srw, self.check, self.args.wait
        job = self.run_job(
            "a_admin",
            "Write the single word OK into output/answer.txt, then finish.",
            "--autonomy",
            "full",
        )
        c(
            "wait: completed",
            s("a_admin", "wait", job, "--timeout", wait),
            0,
            "completed",
        )
        c("get jobs", s("a_admin", "get", "jobs", "--limit", "5"), 0, job)
        c("get job by an id prefix", s("a_admin", "get", "job", job[:8]), 0, job)
        c(
            "describe job",
            s("a_admin", "describe", "job", job),
            0,
            "Status:",
            "completed",
        )
        c("logs", s("a_admin", "logs", job), 0, "tool", "write_file")
        c("files", s("a_admin", "files", job, "--recursive"), 0, "output/answer.txt")
        c("cp", s("a_admin", "cp", f"{job}:output/answer.txt", "-"), 0, "OK")

    def job_user(self) -> None:
        s, c, wait = self.srw, self.check, self.args.wait
        c("a jobs:read token cannot start a job", s("u_read", "run", "x"), 3)
        job = self.run_job(
            "u_full",
            "Write the single word DONE into output/result.txt, then finish.",
            "--expert",
            f"{self.gate_id}-helper",
        )
        c(
            "logs -f ends when the job stops",
            s("u_full", "logs", job, "-f"),
            0,
            "write_file",
        )
        c(
            "wait: stopped for review",
            s("u_full", "wait", job, "--timeout", wait),
            10,
            "pending_review",
        )
        c("approve", self.approve("u_full", job), 0)
        c(
            "wait: completed",
            s("u_full", "wait", job, "--timeout", wait),
            0,
            "completed",
        )

    def lifecycle(self) -> None:
        s, c, wait = self.srw, self.check, self.args.wait
        job = self.run_job(
            "a_admin",
            "Count from 1 to 40 into output/count.txt, ten numbers per write, "
            "read the file back, then finish.",
            "--autonomy",
            "full",
        )
        c(
            "wait until it runs",
            s("a_admin", "wait", job, "--for", "status=processing", "--timeout", wait),
            0,
        )
        c("pause", s("a_admin", "pause", job), 0, "paused")
        c("wait: paused by a person", s("a_admin", "wait", job, "--timeout", "2m"), 10)
        c(
            "describe says who holds it",
            s("a_admin", "describe", "job", job),
            0,
            "Held by:",
        )
        c("resume", s("a_admin", "resume", job), 0)
        c(
            "delete of a job that has not ended is refused",
            s("a_admin", "delete", "job", job, "--yes"),
            5,
        )
        c("cancel", s("a_admin", "cancel", job), 0, "cancelled")
        c("wait: cancelled", s("a_admin", "wait", job, "--timeout", "2m"), 8)

    def settle(self, tries: int = 60, pause: float = 5.0) -> bool:
        """Wait until the API answers; a rebuild may be replacing it."""
        for _ in range(tries):
            if self.srw("a_admin", "whoami")[0] == 0:
                return True
            time.sleep(pause)
        return False

    def cleanup(self) -> None:
        if not self.settle():
            self.failed.append("cleanup: the API does not answer")
            print("  FAIL  the API does not answer; nothing was removed.")
            print(f"        Later: --gate-id {self.gate_id} --sweep")
            return
        # This run's jobs, and any an interrupted run with this id left.
        jobs = {job: token for token, job in self.jobs}
        for job in self.psql(leftover_jobs_sql(self.gate_id)).split():
            jobs.setdefault(job, "a_admin")
        for job, token in jobs.items():
            self.srw(token, "cancel", job)
            self.srw(token, "wait", job, "--timeout", "3m")
            self.check(
                f"delete job {job[:8]}",
                self.srw(token, "delete", "job", job, "--yes"),
                0,
            )
        for kind, name, extra in (
            ("expert", f"{self.gate_id}-helper", ()),
            ("wt", f"{self.gate_id}-ws", ("--account",)),
            ("project", f"{self.gate_id}-project", ()),
        ):
            # Deleted already when the run went well; 4 is "not there".
            code, _, _ = self.srw("u_full", "delete", kind, name, "--yes", *extra)
            if code not in (0, 4):
                self.failed.append(f"cleanup of {kind} {name}")
                print(f"  FAIL  cleanup of {kind} {name} (exit {code})")
        left = self.psql(leftover_sql(self.gate_id)).strip()
        self.check("nothing of the run is left", (0 if left == "0" else 1, left, ""), 0)


def verdict(got: int, text: str, code: int, needles: tuple[str, ...]) -> str:
    """Empty when a result is what a check expects, else what is wrong."""
    if got != code:
        return f"exit {got}, expected {code}"
    missing = [needle for needle in needles if needle not in text]
    return f"missing in the output: {missing}" if missing else ""


def build(work: Path) -> Path:
    binary = work / "srw"
    subprocess.run(
        ["go", "build", "-o", str(binary), "."],
        cwd=ROOT / "cli",
        check=True,
        env={**os.environ, "CGO_ENABLED": "0"},
    )
    return binary


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    problem = refusal(args)
    if problem:
        print("refused: " + problem, file=sys.stderr)
        return 2
    if not args.run:
        print(
            f"srw k3d gate (dry run; nothing is touched). Context {CONTEXT}, namespace {NAMESPACE}."
        )
        for name, what in PHASES.items():
            print(f"  - {name}: {what}")
        print(f"Run it with: --run --confirm {LOCAL_CONFIRMATION}")
        return 0
    # Each check is printed when it is done, also into a file or a pipe.
    sys.stdout.reconfigure(line_buffering=True)
    with tempfile.TemporaryDirectory(prefix="srw-cli-gate-") as directory:
        work = Path(directory)
        (work / "home").mkdir()
        binary = Path(args.binary) if args.binary else build(work)
        gate = Gate(args, binary, work)
        print(f"srw k3d gate {gate.gate_id} against {args.server}")
        digests = {
            name: hashlib.sha256(token.encode()).hexdigest()
            for name, token in gate.tokens.items()
        }
        emails = {"user": args.user, "admin": args.admin}
        try:
            gate.psql(mint_sql(gate.gate_id, emails, digests))
            for name in (
                "identity",
                "scopes",
                "objects",
                "job-admin",
                "job-user",
                "lifecycle",
            ):
                if args.sweep:
                    break
                print(f"{name}:")
                getattr(gate, name.replace("-", "_"))()
        except Exception as error:  # noqa: BLE001 - reported, then cleaned up
            gate.failed.append(f"the run stopped: {gate.scrub(str(error))}")
            print(f"  FAIL  the run stopped: {gate.scrub(str(error))}")
        finally:
            print("cleanup:")
            try:
                gate.cleanup()
            finally:
                gate.psql(revoke_sql(gate.gate_id))
        if gate.failed:
            print(f"FAILED: {len(gate.failed)} check(s): " + "; ".join(gate.failed))
            return 1
        print("PASSED")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
