#!/usr/bin/env python3
"""Bounded local-k3d gate: connector credentials never reach checkpoints or snapshots.

Slice C0 of the connector drivers feature. Dry-run by default; ``--run`` with
``--confirm LOCAL-K3D-DISPOSABLE`` creates, through the orchestrator REST API
and as the disposable ``test`` account, four connectors carrying recognisable
fake secrets and one job (model ``--model``, default the Meta model k3d smokes
use) that attaches all of them:

- a token repository connector (``https://c0-gate.invalid/...``; the clone
  fails on DNS, so the token never lands in a ``.git/config``, but it is
  typed into the agent's git tab),
- an SSH repository connector (also on ``c0-gate.invalid``),
- an ``ssh_key`` connector whose host is the in-cluster Gitea SSH endpoint,
  so it is delivered (a host-less one is not, since C1),
- an env (``generic``) connector (written to ``~/.srw-credentials/``).

Since C1 both SSH keys are loaded into per-identity ``ssh-agent`` processes
(``~/.ssh/srw-managed/sockets/<32hex>.sock``) and never written to disk. The
gate hashes the code of the pod that runs the job before any scan, proves the
runtime received the credentials (the env file holds the env value, and an
agent socket holds each SSH key's fingerprint, one key per socket), checks
the C1 promise that no ``~/.ssh/repo_*`` file and no ``PRIVATE KEY`` exists
under the workspace home, then fails if any secret appears in:

- the LangGraph checkpoint tables in the app database (``checkpoints``,
  ``checkpoint_blobs``, ``checkpoint_writes``; every row, not only this job's),
  while the job runs, once it settles and after it completes,
- the pod-local SQLite checkpoint files of the pinned agent pod,
- a non-strict and a strict-terminal snapshot of the workspace home, captured
  by the real ``SnapshotService.capture_vm_snapshot`` from the orchestrator pod
  (the upload is intercepted) while the job runs and again once it settles,
- ``~/.bash_history`` on the workspace once the agent's shells have ended,
- the job's real S3 snapshot, taken by the completion teardown after the gate
  approves the job; it is reported as skipped only if it never appears.

A job that fails, or pauses on an LLM outage, before the scans can run fails
the gate as "LLM unavailable". The gate then deletes the job and the
connectors. Secrets travel only on ``kubectl exec -i`` stdin, never in an
argument, and the report names markers by label and files by path only.
Child-process output is never printed.
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
import tempfile
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Sequence
from urllib.parse import urlencode
from uuid import UUID


LOCAL_CONTEXT = "k3d-srw"
LOCAL_NAMESPACE = "srw"
LOCAL_CONFIRMATION = "LOCAL-K3D-DISPOSABLE"
ORCHESTRATOR = "deploy/srw-orchestrator"
STATELESS_AGENT = "deploy/srw-agent-stateless"
POSTGRES_POD = "srw-postgres-0"
ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MODEL = "muse-spark-1.3-contributor"
# Files whose deployed copy must match this checkout before the gate means
# anything: the strip, the shell history fix and the snapshot excludes.
AGENT_FILES = (
    "src/agent/core/state.py",
    "src/shared/runtime/core/shell_protocol.py",
    "src/shared/runtime/core/backends/remote.py",
)
ORCHESTRATOR_FILES = ("src/orchestrator/services/snapshot_service.py",)
# Archive members the snapshot excludes must have dropped.
EXCLUDED_MEMBER_PATTERN = (
    r"(?:^|/)(?:\.srw-credentials(?:/|$)|\.ssh/srw-managed(?:/|$)|\.ssh/repo_"
    r"|\.cache/srw/rclone/.+/(?:rclone\.conf$|\.?bearer\.token))"
)
_GATE_ID_RE = re.compile(r"srw-c0-[0-9a-f]{12}\Z")
_MARKER_RE = re.compile(r"[A-Za-z0-9+/=]{20,}\Z")
_POD_RE = re.compile(r"[a-z0-9](?:[-a-z0-9.]{0,251}[a-z0-9])?\Z")
_HOST_RE = re.compile(r"[A-Za-z0-9.:-]{1,253}\Z")
_MODEL_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/@-]{0,199}\Z")
_PARK_REASON_RE = re.compile(r"[a-z0-9_]{1,64}\Z")


class SafetyError(RuntimeError):
    """The requested run is outside the hard-coded disposable boundary."""


class GateFailure(RuntimeError):
    """A redacted gate failure; the text never carries child output."""


@dataclass(frozen=True)
class GateConfig:
    gate_id: str
    lane: str
    model: str
    run: bool
    timeout_seconds: int
    settle_seconds: int
    snapshot_wait_seconds: int
    keep: bool
    skip_code_check: bool


@dataclass
class SafeReport:
    gate_id: str
    mode: str
    lane: str
    job_id: str | None = None
    phases: list[dict[str, Any]] = field(default_factory=list)
    cleanup: str = "not_started"

    def record(self, name: str, result: str, **detail: Any) -> None:
        self.phases.append({"name": name, "result": result, **detail})

    def as_dict(self) -> dict[str, Any]:
        return {
            "gate_id": self.gate_id,
            "mode": self.mode,
            "lane": self.lane,
            "job_id": self.job_id,
            "phases": self.phases,
            "cleanup": self.cleanup,
        }


# ---------------------------------------------------------------------------
# Secrets and markers
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class GateSecrets:
    token: str
    env_name: str
    env_value: str
    ssh_repo_key: str
    ssh_file_key: str
    # Public SHA256 fingerprints: what ``ssh-add -l`` reports for each key.
    ssh_repo_fingerprint: str
    ssh_file_fingerprint: str

    def markers(self) -> dict[str, str]:
        """Label -> needle. Labels are safe to print; needles are not."""
        markers = {"token": self.token, "env": self.env_value}
        markers.update(_key_markers("ssh_repo_key", self.ssh_repo_key))
        markers.update(_key_markers("ssh_key_connector", self.ssh_file_key))
        for needle in markers.values():
            if not _MARKER_RE.fullmatch(needle):
                raise GateFailure("generated marker is malformed")
        return markers


def _key_markers(label: str, private_key: str) -> dict[str, str]:
    """One needle per base64 body line of an OpenSSH key.

    The first body line is the same for every ed25519 key, so it is skipped;
    every later line is unique to this key. Lines are matched whole, which a
    LF-normalising store cannot split.
    """
    body = [
        line.strip()
        for line in private_key.strip().splitlines()
        if line.strip() and not line.startswith("-----")
    ]
    lines = [line for line in body[1:] if len(line) >= 20]
    if not lines:
        raise GateFailure("generated SSH key has no usable body")
    return {f"{label}:{index}": line for index, line in enumerate(lines, 1)}


def ssh_fingerprint(public_key: str) -> str:
    """The ``SHA256:`` fingerprint ``ssh-add -l`` prints for a public key."""
    blob = base64.b64decode(public_key.split()[1])
    digest = base64.b64encode(hashlib.sha256(blob).digest()).decode()
    return "SHA256:" + digest.rstrip("=")


def _generate_ssh_key() -> tuple[str, str]:
    """A fresh unencrypted ed25519 key and its public fingerprint."""
    with tempfile.TemporaryDirectory(prefix="srw-c0-gate-") as directory:
        path = Path(directory) / "key"
        completed = subprocess.run(
            ["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-C", "", "-f", str(path)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        if completed.returncode != 0:
            raise GateFailure("ssh-keygen failed")
        return (
            path.read_text(encoding="utf-8"),
            ssh_fingerprint(path.with_suffix(".pub").read_text(encoding="utf-8")),
        )


def generate_secrets(gate_id: str) -> GateSecrets:
    suffix = gate_id.rsplit("-", 1)[-1]
    repo_key, repo_fingerprint = _generate_ssh_key()
    file_key, file_fingerprint = _generate_ssh_key()
    return GateSecrets(
        token=f"srwc0tok{secrets.token_hex(12)}",
        env_name=f"C0_GATE_{suffix.upper()}",
        env_value=f"srwc0env{secrets.token_hex(12)}",
        ssh_repo_key=repo_key,
        ssh_file_key=file_key,
        ssh_repo_fingerprint=repo_fingerprint,
        ssh_file_fingerprint=file_fingerprint,
    )


def connector_bodies(
    gate_id: str, values: GateSecrets, *, ssh_host: str, ssh_port: int
) -> list[dict[str, Any]]:
    """The four connectors; the repository hosts never resolve.

    The ``ssh_key`` connector names a host (the in-cluster Gitea SSH endpoint,
    as the C1 gate uses) because since C1 only an ``ssh_key`` with a host is
    delivered to the workspace ``ssh-agent``; without one it is unsupported.
    """
    if not _HOST_RE.fullmatch(ssh_host) or not 0 < int(ssh_port) < 65536:
        raise GateFailure("Gitea SSH endpoint is malformed")
    return [
        {
            "name": f"{gate_id}-token",
            "type": "repository",
            "connection_url": "https://c0-gate.invalid/srw-c0/token.git",
            "credentials": {"auth_method": "token", "token": values.token},
            "config": {"forge": "gitea"},
            "description": "C0 gate token repository (disposable)",
        },
        {
            "name": f"{gate_id}-ssh-repo",
            "type": "repository",
            "connection_url": "ssh://git@c0-gate.invalid/srw-c0/ssh.git",
            "credentials": {"auth_method": "ssh", "ssh_key": values.ssh_repo_key},
            "config": {"forge": "gitea"},
            "description": "C0 gate SSH repository (disposable)",
        },
        {
            "name": f"{gate_id}-ssh-key",
            "type": "ssh_key",
            "config": {"host": ssh_host, "user": "git", "port": int(ssh_port)},
            "credentials": {"files": [{"contents": values.ssh_file_key}]},
            "description": "C0 gate SSH key file (disposable)",
        },
        {
            "name": f"{gate_id}-env",
            "type": "generic",
            "credentials": {"env_vars": {values.env_name: values.env_value}},
            "cli_hint": "C0 gate environment",
            "description": "C0 gate env connector (disposable)",
        },
    ]


# ---------------------------------------------------------------------------
# Scanners. Each one is a script sent on stdin with the markers embedded, so
# no needle is ever an argument. They print labels and paths only.
# ---------------------------------------------------------------------------

# Every program the gate runs inside a pod calls cap_memory() once its
# imports are done. The orchestrator pod has a 1 GiB limit and serves the
# product meanwhile: a gate program that grows must fail the gate with a
# MemoryError, never take the pod to the OOM killer. RLIMIT_DATA counts heap,
# anonymous maps and thread stacks (inherited by its children), not the
# shared libraries mapped in, so the budget is what the program adds.
POD_MEMORY_BUDGET = 256 << 20
_POD_MEMORY_CAP = (
    """
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

_SCAN_HELPERS = r"""
import json, os, sys
MARKERS = {markers}
NEEDLES = {{label: value.encode() for label, value in MARKERS.items()}}
OVERLAP = max(len(n) for n in NEEDLES.values())

def scan_stream(read, found, where, extra=None):
    # 1 MiB at a time, windows overlapping by the longest needle: nothing
    # larger than that is ever held. ``extra`` ({{name: bytes}}) are looked
    # for too; the names seen are returned.
    seen, tail = set(), b""
    longest = max([OVERLAP] + [len(value) for value in (extra or {{}}).values()])
    while True:
        chunk = read(1 << 20)
        if not chunk:
            return seen
        window = tail + chunk
        for label, needle in NEEDLES.items():
            if needle in window:
                found.setdefault(label, set()).add(where)
        for name, value in (extra or {{}}).items():
            if value in window:
                seen.add(name)
        tail = window[-longest:]

def report(found, **extra):
    extra["hits"] = {{label: sorted(paths) for label, paths in sorted(found.items())}}
    print(json.dumps(extra, sort_keys=True))
"""


def _markers_literal(markers: dict[str, str]) -> str:
    for label, needle in markers.items():
        if not re.fullmatch(r"[a-z_]+(?::[0-9]+)?", label) or not _MARKER_RE.fullmatch(
            needle
        ):
            raise GateFailure("marker is malformed")
    return repr(dict(markers))


# The in-cluster Gitea SSH endpoint, as the orchestrator proves deploy keys
# against it (and as scripts/k3d-ssh-agent-connectors-gate.py uses it).
GITEA_SSH_ENDPOINT_PROGRAM = _POD_MEMORY_CAP + (
    "import json\n"
    "from orchestrator.services.gitea import GiteaClient\n"
    "cap_memory()\n"
    "host, port = GiteaClient()._ssh_internal_endpoint()\n"
    'print(json.dumps({"host": host, "port": port}))\n'
)

# Run as agent-host in the workspace: one line per agent socket, its name and
# the fingerprints ssh-add lists for it.
AGENT_SOCKETS_SCRIPT = (
    "for socket in ~/.ssh/srw-managed/sockets/*.sock; do\n"
    '  test -S "$socket" || continue\n'
    "  printf '%s %s\\n' \"${socket##*/}\" "
    '"$(SSH_AUTH_SOCK="$socket" ssh-add -l 2>/dev/null | '
    'awk \'NF { printf "%s,", $2 }\')"\n'
    "done\n"
)


def parse_agent_sockets(listing: str) -> dict[str, list[str]]:
    """``{socket: [fingerprint, ...]}`` from :data:`AGENT_SOCKETS_SCRIPT`."""
    held: dict[str, list[str]] = {}
    for line in listing.splitlines():
        name, _, prints = line.strip().partition(" ")
        if name:
            held[name] = [value for value in prints.split(",") if value]
    return held


def file_scan_script(markers: dict[str, str], roots: Sequence[str]) -> str:
    """Scan every regular file under ``roots`` (pinned SQLite checkpoints)."""
    return (
        _POD_MEMORY_CAP
        + _SCAN_HELPERS.format(markers=_markers_literal(markers))
        + f"""
cap_memory()
found, files = {{}}, 0
for root in {list(roots)!r}:
    for directory, _dirs, names in os.walk(root):
        for name in names:
            path = os.path.join(directory, name)
            if not os.path.isfile(path) or os.path.islink(path):
                continue
            files += 1
            with open(path, "rb") as handle:
                scan_stream(handle.read, found, path)
report(found, files=files)
"""
    )


# The tar-aware scan. It runs in the orchestrator pod for the live
# captures (scan_archive, from a capture file) and in the gate's own process
# for the product's S3 snapshot (scan_archive_stream, from a local zstd fed
# by the pod's pass-through): the same text either way.
_ARCHIVE_SCANNER = r"""
import asyncio, base64, hashlib, re, shutil, subprocess, tarfile, tempfile

JOB = {job_id!r}
EXCLUDED = re.compile({excluded!r})

def _reader(data):
    chunks = [data]
    return lambda _size: chunks.pop() if chunks else b""

def scan_archive_stream(stream):
    # Stream the tar once: member names, pax headers and file bodies, each
    # body 1 MiB at a time; no member is ever held whole.
    found, members, excluded = {{}}, 0, set()
    ssh_config = history = False
    with tarfile.open(fileobj=stream, mode="r|") as archive:
        for member in archive:
            members += 1
            name = member.name
            header = (name + "\\0" + member.linkname + "\\0"
                      + json.dumps(member.pax_headers, sort_keys=True))
            scan_stream(_reader(header.encode()), found, name)
            if EXCLUDED.search(name):
                excluded.add(name)
            if not member.isfile():
                continue
            history = history or name.endswith("agent-host/.bash_history")
            data = archive.extractfile(member)
            if name.endswith("agent-host/.ssh/config"):
                # Since C1 the user's own ~/.ssh/config carries the managed
                # Include line; the config.d it names is excluded.
                seen = scan_stream(
                    data.read, found, name,
                    extra={{"kept": b"srw-managed/config.d/*.conf"}},
                )
                ssh_config = "kept" in seen
            else:
                scan_stream(data.read, found, name)
    return dict(members=members, excluded_paths=sorted(excluded),
                ssh_config_kept=ssh_config, bash_history_present=history,
                hits={{k: sorted(v) for k, v in sorted(found.items())}})

def scan_archive(path):
    unzstd = subprocess.Popen(["zstd", "-dc", "--", path], stdout=subprocess.PIPE)
    try:
        result = scan_archive_stream(unzstd.stdout)
    finally:
        unzstd.stdout.close()
    if unzstd.wait() != 0:
        raise RuntimeError("zstd failed")
    return result
"""


def _archive_scanner(markers: dict[str, str], job_id: str) -> str:
    return _SCAN_HELPERS.format(
        markers=_markers_literal(markers)
    ) + _ARCHIVE_SCANNER.format(
        job_id=str(UUID(job_id)), excluded=EXCLUDED_MEMBER_PATTERN
    )


def snapshot_capture_script(
    markers: dict[str, str], *, job_id: str, ssh_host: str, ssh_port: int
) -> str:
    """Capture both snapshot modes with the real service, then scan them.

    The upload is intercepted, so nothing reaches S3 and the job's own
    snapshot history is untouched.
    """
    if not _HOST_RE.fullmatch(ssh_host) or not 0 < int(ssh_port) < 65536:
        raise GateFailure("workspace endpoint is malformed")
    return (
        _POD_MEMORY_CAP
        + _archive_scanner(markers, job_id)
        + f"""
from orchestrator.services.snapshot_service import SnapshotService
cap_memory()
HOST, PORT = {ssh_host!r}, {int(ssh_port)}

def fingerprint():
    try:
        out = subprocess.run(["ssh-keyscan", "-T", "10", "-p", str(PORT), HOST],
                             capture_output=True, timeout=20).stdout
    except (OSError, subprocess.SubprocessError):
        return None
    for line in out.splitlines():
        fields = line.split()
        if len(fields) >= 3 and not line.lstrip().startswith(b"#"):
            raw = base64.b64decode(fields[2])
            return "SHA256:" + base64.b64encode(
                hashlib.sha256(raw).digest()).decode().rstrip("=")
    return None

async def capture(strict, workdir):
    out = os.path.join(workdir, ("strict" if strict else "nonstrict") + ".tar.zst")
    service = SnapshotService()
    service._available = True
    service._db = None
    async def keep(job_id, tar_path, manifest, **_kwargs):
        shutil.copyfile(tar_path, out)
        return True
    async def no_environment(*_args, **_kwargs):
        return {{}}
    service.upload_snapshot = keep
    service._collect_environment_info = no_environment
    ok = await service.capture_vm_snapshot(
        job_id=JOB, ssh_host=HOST, ssh_port=PORT, source_type="pod",
        entity_type="jobs", strict_terminal=strict,
        expected_host_key_fingerprint=fingerprint() if strict else None,
    )
    if not ok or not os.path.exists(out):
        return dict(captured=False)
    return dict(captured=True, **scan_archive(out))

async def main():
    with tempfile.TemporaryDirectory(prefix="srw-c0-gate-") as workdir:
        result = dict(non_strict=await capture(False, workdir),
                      strict=await capture(True, workdir))
    print(json.dumps(result, sort_keys=True))

asyncio.run(main())
"""
    )


#: ``s3_snapshot_stream_script`` exit codes: no object store, no snapshot yet.
S3_NO_STORE, S3_NO_SNAPSHOT = 3, 4


def s3_snapshot_stream_script(*, job_id: str) -> str:
    """Pass the job's real S3 snapshot through to stdout, 1 MiB at a time.

    No marker is in it and nothing is read whole, decompressed or scanned in
    the pod: the gate scans the stream in its own process
    (:func:`scan_s3_snapshot_stream`). Exits 3 without an object store and 4
    while the job has no snapshot.
    """
    key = f"jobs/{UUID(job_id)}/env.tar.zst"
    return (
        _POD_MEMORY_CAP
        + f"""
import asyncio, sys
from orchestrator.services.snapshot_service import SnapshotService
KEY = {key!r}
async def connect():
    service = SnapshotService()
    await service.connect(None)
    return service
service = asyncio.run(connect())
cap_memory()
s3 = getattr(service, "_s3", None)
if not service.is_available or s3 is None:
    raise SystemExit({S3_NO_STORE})
try:
    body = s3.get_object(Bucket=service._bucket, Key=KEY)["Body"]
except Exception as error:
    code = getattr(error, "response", {{}}).get("Error", {{}}).get("Code", "")
    if code in ("404", "NoSuchKey"):
        raise SystemExit({S3_NO_SNAPSHOT})
    raise
out = sys.stdout.buffer
while True:
    chunk = body.read(1 << 20)
    if not chunk:
        break
    out.write(chunk)
out.flush()
"""
    )


def s3_snapshot_producer() -> list[str]:
    """The pod side of the S3 scan: its python reads the script on stdin."""
    return [
        "kubectl",
        f"--context={LOCAL_CONTEXT}",
        "-n",
        LOCAL_NAMESPACE,
        "exec",
        "-i",
        ORCHESTRATOR,
        "-c",
        "orchestrator",
        "--",
        "python",
        "-",
    ]


def scan_s3_snapshot_stream(
    markers: dict[str, str], *, job_id: str, timeout: int = 900
) -> dict[str, Any] | None:
    """Scan the job's S3 snapshot here, streamed out of the pod.

    The pod passes the object's bytes through; a local ``zstd -dc`` reads
    them straight from the producer and the tar-aware scanner (the same text
    the pod runs for captures) reads zstd's output, 1 MiB at a time. Returns
    None while there is no snapshot; raises GateFailure when the transfer or
    the archive is incomplete.
    """
    namespace: dict[str, Any] = {}
    exec(_archive_scanner(markers, job_id), namespace)  # the gate's own text
    with tempfile.TemporaryFile() as errors:
        producer = subprocess.Popen(
            s3_snapshot_producer(),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=errors,
        )
        processes = [producer]
        try:
            assert producer.stdin is not None and producer.stdout is not None
            producer.stdin.write(s3_snapshot_stream_script(job_id=job_id).encode())
            producer.stdin.close()
            try:
                decoder = subprocess.Popen(
                    ["zstd", "-dcq"],
                    stdin=producer.stdout,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.DEVNULL,
                )
            except FileNotFoundError:
                raise GateFailure("zstd is not installed where the gate runs") from None
            processes.append(decoder)
            producer.stdout.close()
            watchdog = threading.Timer(
                timeout, lambda: [process.kill() for process in processes]
            )
            watchdog.start()
            try:
                result = namespace["scan_archive_stream"](decoder.stdout)
                failure = None
            except Exception as error:  # a short or broken stream
                result, failure = None, type(error).__name__
            finally:
                watchdog.cancel()
            codes = [process.wait(timeout=60) for process in processes]
        finally:
            for process in processes:
                if process.poll() is None:
                    process.kill()
                    process.wait()
    if codes[0] in (S3_NO_STORE, S3_NO_SNAPSHOT):
        return None
    if failure or any(codes):
        raise GateFailure(
            f"S3 snapshot stream incomplete (exit {codes}, {failure or 'no error'})"
        )
    return result


def checkpoint_scan_sql(markers: dict[str, str], *, job_id: str) -> str:
    """One query: every checkpoint row holding a marker, plus a control.

    The control proves this job's ``metadata`` channel exists and holds the
    connector names the strip keeps, so an empty result means "scanned and
    clean", not "nothing to scan".
    """
    job_id = str(UUID(job_id))
    _markers_literal(markers)
    rows = ", ".join(f"('{label}', '{needle}')" for label, needle in markers.items())
    return f"""
SET statement_timeout = '300s';
WITH m(label, needle) AS (VALUES {rows})
SELECT json_build_object(
  'hits', COALESCE((SELECT json_agg(DISTINCT h) FROM (
      SELECT m.label || ' checkpoint_blobs:' || b.channel AS h
        FROM m JOIN checkpoint_blobs b
          ON position(convert_to(m.needle, 'UTF8') IN b.blob) > 0
      UNION ALL
      SELECT m.label || ' checkpoint_writes:' || w.channel
        FROM m JOIN checkpoint_writes w
          ON position(convert_to(m.needle, 'UTF8') IN w.blob) > 0
      UNION ALL
      SELECT m.label || ' checkpoints'
        FROM m JOIN checkpoints c
          ON strpos(c.checkpoint::text, m.needle) > 0
          OR strpos(c.metadata::text, m.needle) > 0
  ) found), '[]'::json),
  'job_rows', (SELECT count(*) FROM checkpoints WHERE thread_id = '{job_id}'),
  'control', (
      SELECT count(*) FROM checkpoint_blobs
       WHERE thread_id = '{job_id}' AND channel = 'metadata'
         AND position(convert_to('-token', 'UTF8') IN blob) > 0
  ) + (
      SELECT count(*) FROM checkpoints
       WHERE thread_id = '{job_id}' AND strpos(checkpoint::text, '-token') > 0
  )
);
"""


# ---------------------------------------------------------------------------
# Cluster access
# ---------------------------------------------------------------------------


class Kube:
    """Exact-context kubectl; failures name the operation, never the output."""

    def __init__(self, timeout: int) -> None:
        self.timeout = timeout

    def run(
        self,
        arguments: Sequence[str],
        *,
        operation: str,
        data: str | None = None,
        timeout: int | None = None,
        ok_codes: Sequence[int] = (0,),
    ) -> str:
        try:
            completed = subprocess.run(
                [
                    "kubectl",
                    f"--context={LOCAL_CONTEXT}",
                    "-n",
                    LOCAL_NAMESPACE,
                    *arguments,
                ],
                input=data,
                capture_output=True,
                text=True,
                check=False,
                timeout=timeout or self.timeout,
            )
        except subprocess.TimeoutExpired:
            raise GateFailure(f"{operation} timed out") from None
        if completed.returncode not in ok_codes:
            raise GateFailure(f"{operation} failed (rc={completed.returncode})")
        return completed.stdout.strip()

    def sql(self, query: str, *, operation: str) -> str:
        return self.run(
            [
                "exec",
                "-i",
                POSTGRES_POD,
                "--",
                "psql",
                "-U",
                "srw",
                "-d",
                "srw",
                "-v",
                "ON_ERROR_STOP=1",
                "-tAq",
                "-f",
                "-",
            ],
            operation=operation,
            data=query,
        )

    def python(self, target: str, container: str, script: str, *, operation: str):
        output = self.run(
            ["exec", "-i", target, "-c", container, "--", "python", "-"],
            operation=operation,
            data=script,
            timeout=900,
        )
        try:
            return json.loads(output.splitlines()[-1])
        except (IndexError, ValueError):
            raise GateFailure(f"{operation} returned no report") from None


class Api:
    """REST through the orchestrator pod as the disposable ``test`` account."""

    def __init__(self, kube: Kube, password: str) -> None:
        self.kube = kube
        body = urlencode(
            {
                "grant_type": "password",
                "client_id": "admin-cli",
                "username": "test",
                "scope": "openid",
                "password": password,
            }
        )
        raw = kube.run(
            [
                "exec",
                "-i",
                ORCHESTRATOR,
                "-c",
                "orchestrator",
                "--",
                "curl",
                "-fsS",
                "-X",
                "POST",
                "http://srw-keycloak:8080/realms/srw/protocol/openid-connect/token",
                "--data-binary",
                "@-",
            ],
            operation="keycloak login",
            data=body,
        )
        try:
            self._token = json.loads(raw)["id_token"]
        except (KeyError, ValueError):
            raise GateFailure("keycloak login returned no token") from None

    def call(self, method: str, path: str, body: Any = None, *, operation: str) -> Any:
        # The bearer and the body (which carries the fake secrets) both go on
        # stdin as a curl config, never as an argument.
        config = (
            f'header = "Authorization: Bearer {self._token}"\n'
            'header = "Content-Type: application/json"\n'
        )
        if body is not None:
            config += "data-binary = " + json.dumps(json.dumps(body)) + "\n"
        raw = self.kube.run(
            [
                "exec",
                "-i",
                ORCHESTRATOR,
                "-c",
                "orchestrator",
                "--",
                "curl",
                "-sS",
                "-X",
                method,
                "--config",
                "-",
                "-w",
                "\\n%{http_code}",
                f"http://localhost:8085{path}",
            ],
            operation=operation,
            data=config,
        )
        body, _, code = raw.rpartition("\n")
        if not code.isdigit() or not 200 <= int(code) < 300:
            raise GateFailure(f"{operation}: HTTP {code[-3:]}")
        try:
            return json.loads(body) if body.strip() else None
        except ValueError:
            raise GateFailure(f"{operation}: response was not JSON") from None


# ---------------------------------------------------------------------------
# The gate
# ---------------------------------------------------------------------------


def wait_for(check, *, timeout: int, operation: str, interval: float = 3.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = check()
        if value:
            return value
        time.sleep(interval)
    raise GateFailure(f"timed out: {operation}")


_LLM_FAILURE_WORDS = (
    "llm",
    "model",
    "provider",
    "quota",
    "credit",
    "rate limit",
    "401",
    "402",
    "429",
)


class Gate:
    def __init__(self, config: GateConfig, kube: Kube, password: str) -> None:
        self.config = config
        self.kube = kube
        self.password = password
        self.report = SafeReport(config.gate_id, "run", config.lane)
        self.datasource_ids: list[str] = []
        self.job_id: str | None = None
        self.scans_started = False

    # -- preflight -------------------------------------------------------

    def code_check(self, target: str, container: str, paths: Sequence[str]) -> None:
        expected = {
            f"/app/{path}": hashlib.sha256((ROOT / path).read_bytes()).hexdigest()
            for path in paths
        }
        probe = _POD_MEMORY_CAP + (
            "import hashlib, json\n"
            "cap_memory()\n"
            f"expected = {expected!r}\n"
            "stale = [p for p, h in expected.items() if hashlib.sha256("
            "open(p, 'rb').read()).hexdigest() != h]\n"
            "print(json.dumps({'stale': stale}))\n"
        )
        result = self.kube.python(target, container, probe, operation="code check")
        if result.get("stale"):
            raise GateFailure(
                f"{target} does not run this checkout ({', '.join(result['stale'])})"
            )

    # -- setup -----------------------------------------------------------

    def gitea_ssh_endpoint(self) -> tuple[str, int]:
        result = self.kube.python(
            ORCHESTRATOR,
            "orchestrator",
            GITEA_SSH_ENDPOINT_PROGRAM,
            operation="Gitea SSH endpoint",
        )
        return str(result["host"]), int(result["port"])

    def create(self, api: Api, values: GateSecrets) -> None:
        ssh_host, ssh_port = self.gitea_ssh_endpoint()
        for body in connector_bodies(
            self.config.gate_id, values, ssh_host=ssh_host, ssh_port=ssh_port
        ):
            created = api.call(
                "POST", "/api/datasources", body, operation="create connector"
            )
            self.datasource_ids.append(str(UUID(str(created["id"]))))
        self.report.record("connectors_created", "pass", count=4)
        job = {
            "description": (
                "C0 credential residue gate. Write the word DONE to "
                "output/done.txt, then complete the job."
            ),
            "datasource_ids": list(self.datasource_ids),
            "execution_lane": self.config.lane,
            "config_override": {"llm": {"model": self.config.model}},
        }
        created = api.call("POST", "/api/jobs", job, operation="create job")
        self.job_id = str(UUID(str(created.get("id") or created.get("job_id"))))
        self.report.job_id = self.job_id
        self.report.record("job_created", "pass", model=self.config.model)

    def job_row(self) -> dict[str, Any]:
        raw = self.kube.sql(
            "SELECT json_build_object('status', status, 'container', "
            "context->'workspace_container', 'agent', (SELECT a.hostname FROM "
            "agents a WHERE a.id = j.assigned_agent_id), 'leased_by', (SELECT "
            "coalesce(q.leased_by, q.last_leased_by) FROM run_queue q WHERE "
            "q.unit_id = j.id), 'parked', (SELECT coalesce(q.park_reason, "
            "'parked') FROM run_queue q WHERE q.unit_id = j.id AND q.state = "
            "'parked'), 'llm_failure', lower(concat_ws(' ', "
            "j.error_message, j.freeze_data::text, j.context->>'llm_outage')) "
            f"~ '{'|'.join(_LLM_FAILURE_WORDS)}') FROM jobs j "
            f"WHERE j.id = '{self.job_id}';",
            operation="read job",
        )
        return json.loads(raw) if raw else {}

    def alive(self, row: dict[str, Any] | None = None) -> dict[str, Any]:
        """Fail clearly when the job stops before the scans could run.

        A paused job whose run-queue unit is parked (a stateless worker's
        lost lease, say) never runs again without an operator, so it stops
        the gate too, naming the park reason, instead of a scan timing out.
        """
        row = row if row is not None else self.job_row()
        status = row.get("status")
        parked = row.get("parked")
        stopped = status in {"failed", "cancelled"} or (
            status == "paused" and (row.get("llm_failure") or parked)
        )
        if stopped and not self.scans_started:
            if row.get("llm_failure"):
                cause = "LLM unavailable"
            elif status == "paused" and parked:
                reason = parked if _PARK_REASON_RE.fullmatch(str(parked)) else "?"
                cause = f"job stopped (its unit parked: {reason})"
            else:
                cause = "job stopped"
            raise GateFailure(
                f"{cause}: job {status} before the scans could run "
                f"(model {self.config.model})"
            )
        return row

    def workspace(self) -> dict[str, Any]:
        def ready():
            container = self.alive().get("container") or {}
            host = container.get("host") or container.get("pod_ip")
            if (
                container.get("status") == "ready"
                and host
                and container.get("pod_name")
            ):
                return container
            return None

        container = wait_for(
            ready, timeout=self.config.timeout_seconds, operation="workspace ready"
        )
        if not _POD_RE.fullmatch(str(container["pod_name"])):
            raise GateFailure("workspace pod name is malformed")
        self.report.record("workspace_ready", "pass")
        return container

    def agent_pod(self) -> str:
        """The pod that runs this job, pinned or stateless, checked first.

        A pinned pod idle since an older Tilt build keeps its old image, so
        its own code is hashed before any scan; finding none fails the gate.
        """
        key = "agent" if self.config.lane == "pinned" else "leased_by"

        def found():
            pod = self.alive().get(key)
            if not pod or not _POD_RE.fullmatch(str(pod)):
                return None
            exists = self.kube.run(
                ["get", "pod", str(pod), "-o", "name", "--ignore-not-found"],
                operation="find agent pod",
            )
            return str(pod) if exists else None

        try:
            pod = wait_for(
                found,
                timeout=self.config.timeout_seconds,
                operation=f"{self.config.lane} agent pod",
            )
        except GateFailure:
            raise GateFailure(
                f"no {self.config.lane} agent pod found for the job"
            ) from None
        if not self.config.skip_code_check:
            self.code_check(pod, "agent", AGENT_FILES)
        self.report.record("agent_pod_code_matches", "pass", pod=pod)
        return pod

    def runtime_received_credentials(
        self, pod: str, markers: dict[str, str], values: GateSecrets
    ) -> None:
        """The env file holds the env value, and an agent socket each SSH key.

        Since C1 an SSH key is never written to the workspace: it is loaded
        into its own ``ssh-agent``. The proof is its fingerprint in
        ``ssh-add -l`` of one socket, which holds that key alone.
        """
        wanted = {
            "ssh_repo_key": values.ssh_repo_fingerprint,
            "ssh_key_connector": values.ssh_file_fingerprint,
        }

        def materialized():
            self.alive()
            env = self.workspace_grep(
                pod, [markers["env"]], ["/home/agent-host/.srw-credentials"]
            )
            held = parse_agent_sockets(self.as_agent_host(pod, AGENT_SOCKETS_SCRIPT))
            holding = {
                prints[0]: name for name, prints in held.items() if len(prints) == 1
            }
            if any("/.srw-credentials/" in p for p in env) and set(
                wanted.values()
            ) <= set(holding):
                return env, held
            return None

        env, held = wait_for(
            materialized,
            timeout=self.config.timeout_seconds,
            operation="credentials materialized on the workspace",
        )
        shared = sorted(name for name, prints in held.items() if len(prints) != 1)
        self.report.record(
            "runtime_received_credentials",
            "fail" if shared else "pass",
            env_files=sorted(p.replace("/home/agent-host/", "~/") for p in env),
            agent_sockets=len(held),
            ssh_keys_held=sorted(wanted),
            sockets_not_holding_one_key=shared,
        )
        if shared:
            raise GateFailure("an ssh-agent socket does not hold exactly one key")

    def no_key_on_disk(self, pod: str, markers: dict[str, str]) -> None:
        """C1: no ``~/.ssh/repo_*`` file and no ``PRIVATE KEY`` under the home."""
        needles = ["PRIVATE KEY"] + [
            needle for label, needle in markers.items() if label.startswith("ssh_")
        ]
        found = self.workspace_grep(pod, needles, ["/home/agent-host"])
        legacy = self.kube.run(
            ["exec", pod, "-c", "workspace", "--", "find", "/home/agent-host/.ssh"]
            + ["-maxdepth", "1", "-name", "repo_*"],
            operation="legacy key probe",
            ok_codes=(0, 1),
        ).splitlines()
        paths = sorted(
            {p.replace("/home/agent-host/", "~/") for p in [*found, *legacy] if p}
        )
        self.report.record(
            "workspace_holds_no_key_file", "fail" if paths else "pass", paths=paths
        )
        if paths:
            raise GateFailure("a private key or ~/.ssh/repo_* file is on the workspace")

    def as_agent_host(self, pod: str, script: str) -> str:
        """Run ``script`` as agent-host in the workspace (stdin, never argv)."""
        return self.kube.run(
            ["exec", "-i", pod, "-c", "workspace", "--"]
            + ["su", "-s", "/bin/bash", "agent-host", "-c", "bash -s"],
            operation="workspace shell",
            data="set -u\ncd ~\n" + script,
        )

    def workspace_grep(
        self, pod: str, needles: Sequence[str], paths: Sequence[str]
    ) -> list[str]:
        """Files under ``paths`` on the workspace holding any needle."""
        output = self.kube.run(
            ["exec", "-i", pod, "-c", "workspace", "--", "grep", "-rlF"]
            + ["-D", "skip", "--binary-files=text", "-f", "-"]
            + list(paths),
            operation="workspace grep",
            data="\n".join(needles) + "\n",
            ok_codes=(0, 1, 2),
        )
        return [line for line in output.splitlines() if line]

    # -- scans -----------------------------------------------------------

    def scan_checkpoints(
        self, markers: dict[str, str], *, phase: str, require_rows: bool = True
    ) -> None:
        """Scan every checkpoint row for a marker.

        ``require_rows`` waits until this job's rows exist and its metadata
        channel holds the kept connector names. A terminal job has had its
        rows pruned, so a later scan only checks that nothing anywhere holds
        a marker.
        """
        query = checkpoint_scan_sql(markers, job_id=self.job_id)

        def present():
            self.alive()
            result = json.loads(self.kube.sql(query, operation="checkpoint scan"))
            if not require_rows or (result["job_rows"] and result["control"]):
                return result
            return None

        result = wait_for(
            present,
            timeout=self.config.timeout_seconds,
            operation="checkpoint rows with the metadata channel",
        )
        self.scans_started = True
        if result["hits"]:
            self.report.record(phase, "fail", hits=sorted(result["hits"]))
            raise GateFailure(f"{phase}: credential found in checkpoint rows")
        self.report.record(phase, "pass", job_rows=result["job_rows"])

    def scan_pinned_sqlite(self, pod: str, markers: dict[str, str]) -> None:
        # WORKSPACE_PATH is /workspace on agent pods (helm configmap).
        script = file_scan_script(
            markers, ["/workspace/checkpoints", "/workspace/phase_snapshots"]
        )
        result = self.kube.python(pod, "agent", script, operation="sqlite scan")
        if result["hits"]:
            self.report.record("pinned_sqlite_scan", "fail", hits=result["hits"])
            raise GateFailure("credential found in pinned checkpoint files")
        self.report.record("pinned_sqlite_scan", "pass", files=result["files"])

    def _archive_verdict(self, name: str, archive: dict[str, Any]) -> bool:
        bad = bool(
            archive["hits"]
            or archive["excluded_paths"]
            or not archive["ssh_config_kept"]
        )
        self.report.record(
            name,
            "fail" if bad else "pass",
            members=archive["members"],
            hits=archive["hits"],
            excluded_paths=archive["excluded_paths"][:20],
            ssh_config_kept=archive["ssh_config_kept"],
            bash_history_present=archive["bash_history_present"],
        )
        return bad

    def scan_live_snapshots(
        self, markers: dict[str, str], container: dict, *, phase: str
    ) -> None:
        host = str(container.get("host") or container.get("pod_ip"))
        port = int(container.get("port") or 22)
        script = snapshot_capture_script(
            markers, job_id=self.job_id, ssh_host=host, ssh_port=port
        )
        result = self.kube.python(
            ORCHESTRATOR, "orchestrator", script, operation="snapshot capture"
        )
        failed = False
        for mode in ("non_strict", "strict"):
            capture = result.get(mode) or {}
            name = f"snapshot_{phase}_{mode}"
            if not capture.get("captured"):
                self.report.record(name, "fail", reason="no capture")
                failed = True
            else:
                failed = self._archive_verdict(name, capture) or failed
        if failed:
            raise GateFailure(f"credential residue in a {phase} workspace snapshot")

    def scan_bash_history(self, pod: str, markers: dict[str, str]) -> None:
        """Once the job's shells are gone, ~/.bash_history holds no secret."""

        def shells_gone():
            output = self.kube.run(
                ["exec", pod, "-c", "workspace", "--", "pgrep", "-u", "agent-host"]
                + ["-x", "tmux"],
                operation="tmux probe",
                ok_codes=(0, 1, 126, 127),
            )
            return not output

        try:
            wait_for(shells_gone, timeout=180, operation="agent shells ended")
            shells = "ended"
        except GateFailure:
            shells = "still_running"
        hits = self.workspace_grep(
            pod, list(markers.values()), ["/home/agent-host/.bash_history"]
        )
        self.report.record(
            "bash_history_scan",
            "fail" if hits else "pass",
            agent_shells=shells,
            hits=[p.replace("/home/agent-host/", "~/") for p in hits],
        )
        if hits:
            raise GateFailure("credential found in ~/.bash_history")

    def scan_s3_snapshot(self, markers: dict[str, str]) -> None:
        """Wait (bounded) for the job's real S3 snapshot, then scan it."""
        broken: list[str] = []

        def available():
            try:
                return scan_s3_snapshot_stream(markers, job_id=self.job_id)
            except GateFailure as error:  # appeared, but did not read whole
                broken.append(str(error))
                return {"broken": True}

        try:
            result = wait_for(
                available,
                timeout=self.config.snapshot_wait_seconds,
                operation="job S3 snapshot",
                interval=15,
            )
        except GateFailure:
            self.report.record(
                "snapshot_s3_product", "skipped", reason="never appeared"
            )
            return
        if broken:
            self.report.record("snapshot_s3_product", "fail", reason=broken[-1])
            raise GateFailure("the job's S3 snapshot could not be read whole")
        if self._archive_verdict("snapshot_s3_product", result):
            raise GateFailure("credential residue in the job's S3 snapshot")

    def settle(self) -> str:
        """Let the job reach a resting status and return it."""
        resting = {"pending_review", "paused", "completed", "failed", "waiting"}

        def rested():
            status = self.job_row().get("status")
            return status if status in resting else None

        status = wait_for(
            rested, timeout=self.config.settle_seconds, operation="job settle"
        )
        row = self.job_row()
        if status in {"paused", "failed"}:
            cause = "LLM unavailable" if row.get("llm_failure") else "job stopped"
            self.report.record("job_settled", "fail", status=status)
            raise GateFailure(
                f"{cause}: job {status} before it completed, so the settled "
                f"snapshot and history checks could not run (model {self.config.model})"
            )
        self.report.record("job_settled", "pass", status=status)
        return status

    def workspace_still_up(self, container: dict) -> bool:
        current = self.job_row().get("container") or {}
        if current.get("status") != "ready" or current.get("pod_name") != container.get(
            "pod_name"
        ):
            return False
        return bool(
            self.kube.run(
                ["get", "pod", str(container["pod_name"]), "-o", "name"]
                + ["--ignore-not-found"],
                operation="find workspace pod",
            )
        )

    # -- cleanup ---------------------------------------------------------

    def cleanup(self) -> None:
        if self.config.keep:
            self.report.cleanup = "kept"
            return
        try:
            api = Api(self.kube, self.password)
        except GateFailure:
            self.report.cleanup = "login_failed"
            return
        problems = []
        if self.job_id:
            try:
                api.call("PUT", f"/api/jobs/{self.job_id}/cancel", operation="cancel")
            except GateFailure:
                pass  # already resting or terminal; delete decides
            try:
                wait_for(
                    lambda: self.job_row().get("status")
                    not in {"created", "processing"},
                    timeout=180,
                    operation="job stopped",
                )
            except GateFailure:
                problems.append("job_still_running")

            def deleted():
                try:
                    api.call("DELETE", f"/api/jobs/{self.job_id}", operation="delete")
                except GateFailure:
                    return False
                return True

            try:
                wait_for(deleted, timeout=180, operation="job delete", interval=10)
            except GateFailure:
                problems.append("delete_job")
        for datasource_id in self.datasource_ids:
            try:
                api.call(
                    "DELETE",
                    f"/api/datasources/{datasource_id}",
                    operation="delete connector",
                )
            except GateFailure:
                problems.append(f"delete_connector:{datasource_id}")
        self.report.cleanup = "complete" if not problems else ",".join(problems)

    # -- run -------------------------------------------------------------

    def run(self) -> SafeReport:
        if not self.config.skip_code_check:
            self.code_check(ORCHESTRATOR, "orchestrator", ORCHESTRATOR_FILES)
            self.code_check(STATELESS_AGENT, "agent", AGENT_FILES)
            self.report.record("deployed_code_matches", "pass")
        values = generate_secrets(self.config.gate_id)
        markers = values.markers()
        api = Api(self.kube, self.password)
        try:
            self.create(api, values)
            container = self.workspace()
            agent_pod = self.agent_pod()
            workspace_pod = str(container["pod_name"])
            self.runtime_received_credentials(workspace_pod, markers, values)
            self.no_key_on_disk(workspace_pod, markers)
            self.scan_checkpoints(markers, phase="checkpoint_scan_running")
            if self.config.lane == "pinned":
                self.scan_pinned_sqlite(agent_pod, markers)
            self.scan_live_snapshots(markers, container, phase="running")

            status = self.settle()
            if self.workspace_still_up(container):
                self.scan_bash_history(workspace_pod, markers)
                self.scan_live_snapshots(markers, container, phase="settled")
            else:
                self.report.record(
                    "snapshot_settled", "skipped", reason="workspace released"
                )
            self.scan_checkpoints(
                markers,
                phase="checkpoint_scan_settled",
                require_rows=status not in {"completed", "failed"},
            )
            if status == "pending_review":
                # Approval completes the job; its teardown takes the strict
                # terminal snapshot the S3 scan below reads. Log in again: the
                # first token has expired by now.
                Api(self.kube, self.password).call(
                    "POST", f"/api/jobs/{self.job_id}/approve", {}, operation="approve"
                )
                self.report.record("job_approved", "pass")
            self.scan_s3_snapshot(markers)
            self.scan_checkpoints(
                markers, phase="checkpoint_scan_final", require_rows=False
            )
        finally:
            self.cleanup()
        return self.report


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--context", default=LOCAL_CONTEXT)
    parser.add_argument("--namespace", default=LOCAL_NAMESPACE)
    parser.add_argument("--lane", choices=("pinned", "stateless"), default="pinned")
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--gate-id")
    parser.add_argument("--timeout-seconds", type=int, default=900)
    parser.add_argument("--settle-seconds", type=int, default=900)
    parser.add_argument("--snapshot-wait-seconds", type=int, default=600)
    parser.add_argument("--keep", action="store_true")
    parser.add_argument("--skip-code-check", action="store_true")
    parser.add_argument("--run", action="store_true")
    parser.add_argument("--confirm")
    return parser


def validate_config(args: argparse.Namespace) -> GateConfig:
    if args.context != LOCAL_CONTEXT or args.namespace != LOCAL_NAMESPACE:
        raise SafetyError("this harness is restricted to k3d-srw/srw")
    if not 120 <= args.timeout_seconds <= 3600:
        raise SafetyError("timeout must be between 120 and 3600 seconds")
    if not 60 <= args.settle_seconds <= 3600:
        raise SafetyError("settle time must be between 60 and 3600 seconds")
    if not 0 <= args.snapshot_wait_seconds <= 3600:
        raise SafetyError("snapshot wait must be between 0 and 3600 seconds")
    if not _MODEL_RE.fullmatch(args.model):
        raise SafetyError("model id is malformed")
    if args.run and args.confirm != LOCAL_CONFIRMATION:
        raise SafetyError(f"--run requires --confirm {LOCAL_CONFIRMATION}")
    if not args.run and args.confirm is not None:
        raise SafetyError("--confirm is accepted only with --run")
    gate_id = args.gate_id or f"srw-c0-{secrets.token_hex(6)}"
    if not _GATE_ID_RE.fullmatch(gate_id):
        raise SafetyError("gate id must be srw-c0- followed by 12 hex digits")
    return GateConfig(
        gate_id=gate_id,
        lane=args.lane,
        model=args.model,
        run=bool(args.run),
        timeout_seconds=args.timeout_seconds,
        settle_seconds=args.settle_seconds,
        snapshot_wait_seconds=args.snapshot_wait_seconds,
        keep=bool(args.keep),
        skip_code_check=bool(args.skip_code_check),
    )


def plan_report(config: GateConfig) -> SafeReport:
    report = SafeReport(config.gate_id, "plan", config.lane)
    phases = [
        "deployed_code_matches",
        "connectors_created",
        "job_created",
        "workspace_ready",
        "agent_pod_code_matches",
        "runtime_received_credentials",
        "workspace_holds_no_key_file",
        "checkpoint_scan_running",
        *(["pinned_sqlite_scan"] if config.lane == "pinned" else []),
        "snapshot_running_non_strict",
        "snapshot_running_strict",
        "job_settled",
        "bash_history_scan",
        "snapshot_settled_non_strict",
        "snapshot_settled_strict",
        "checkpoint_scan_settled",
        "job_approved",
        "snapshot_s3_product",
        "checkpoint_scan_final",
    ]
    for phase in phases:
        report.record(phase, "planned")
    report.cleanup = "planned"
    return report


def main(argv: Sequence[str] | None = None) -> int:
    import os

    try:
        config = validate_config(build_parser().parse_args(argv))
        if not config.run:
            print(json.dumps(plan_report(config).as_dict(), sort_keys=True))
            return 0
        password = os.environ.get("SRW_K3D_TEST_PASSWORD", "srw-k3d-dev-test")
        gate = Gate(config, Kube(config.timeout_seconds), password)
        try:
            report = gate.run()
        except GateFailure as exc:
            payload = gate.report.as_dict()
            payload.update(mode="failed", error=str(exc))
            print(json.dumps(payload, sort_keys=True))
            return 1
        print(json.dumps(report.as_dict(), sort_keys=True))
        return 0
    except SafetyError as exc:
        print(json.dumps({"mode": "refused", "error": str(exc)}, sort_keys=True))
        return 2
    except GateFailure as exc:
        print(json.dumps({"mode": "failed", "error": str(exc)}, sort_keys=True))
        return 1
    except Exception as exc:  # never echo a message that could carry output
        print(
            json.dumps(
                {"mode": "failed", "error": f"unexpected {type(exc).__name__}"},
                sort_keys=True,
            )
        )
        return 1


if __name__ == "__main__":
    sys.exit(main())
