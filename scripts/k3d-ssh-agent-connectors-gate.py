#!/usr/bin/env python3
"""Local k3d gate for connector drivers C1: connector SSH keys in ssh-agents.

Design: knowledge-base/knowledge/features/connector_drivers.md, Track C, C1.
Template: scripts/k3d-managed-repository-residue-gate.py (safety envelope,
dry-run by default, secrets only on stdin) and the REST helpers of
scripts/parallel-subagents-k3d-gate.py.

The in-cluster Gitea plays the external SSH host. The gate creates three
disposable repositories owned by the Gitea service user, four ed25519 keys and
four connectors that point at Gitea's SSH endpoint as if it were GitHub:

  A  repository, deploy key A (write) on repo a, host key PINNED via Test
  B  repository, deploy key B (write) on repo b, same host, NOT pinned
  C  ssh_key with host/user/port = Gitea SSH, key C (read) on repo a
  D  repository on repo d with a WRONG pinned host key

Checks (each printed PASS/FAIL; the exit status is 0 only if all pass):

  validation  an injected host, an identity-alias host (srw-repo-...) and a
              passphrase key are refused with 400; anything created anyway
              is recorded first, so cleanup deletes it
  test-pin    Test on A reports Gitea's host key; pinning it is accepted
  no-key      no "PRIVATE KEY" and no gate key body anywhere under the
              workspace home; no legacy ~/.ssh/repo_* file
  one-key     every socket in ~/.ssh/srw-managed/sockets holds exactly one
              key, and A, B, C, D are each held by their own socket
  clone-push  repos a and b are cloned through srw-repo-<slug> aliases; a
              fetch works in each and a push from a lands in Gitea
  same-host   two deploy keys on one host both work, and alias A cannot read
              repo b (each alias offers only its own key)
  ssh-key     a plain `git ls-remote ssh://git@<gitea-ssh>/...` uses C
  wrong-pin   D is not cloned and its alias fails host-key verification
  include     ~/.ssh/config ends the managed Include behind `Match all`
  transcript  no gate key body in thread messages, events, or job rows
  checkpoint  no gate key body in the LangGraph checkpoint tables (the job)
  detach      after B is detached (stateless: applied at the next attach)
              B's agent and config are gone and A still fetches
  job-snap    on k3d a stateless job's S3 snapshot comes from a cancel, not
              from completion (or an approval), and only once the job is at
              work: a cancel during its first seconds takes none. So once the
              job's workspace checks pass and its graph has taken a few steps
              (checkpoint rows), while it still runs, the gate cancels it,
              waits (bounded) for the cancel to finish and for the jobs/<id>/
              objects to settle, and scans them member by member for gate key
              bodies and for any PEM private key. A cancel that leaves no
              object FAILS. A job that settled before the cancel is reported
              SKIP (k3d completion takes no snapshot), after one listing.
  end         after End the workspace holds no ssh-agent process (counted
              from /proc; a failed exec or no answer is a FAIL, not zero)
  snapshot    a threads/<id>/ snapshot is scanned if present (a stateless
              sandbox End writes none) and reported as absent otherwise.
              --allow-no-snapshot passes only when no object store is
              configured at all

A pass is every check line PASS (SKIP lines are allowed) and a final
``PASS c1-<id>: N checks, 0 failed`` with exit status 0.

Run with the repository venv on the k3d-srw cluster, alone (no other gate or
full test run): this is a mutating gate.

  .venv/bin/python scripts/k3d-ssh-agent-connectors-gate.py           # plan
  .venv/bin/python scripts/k3d-ssh-agent-connectors-gate.py \\
      --run --confirm LOCAL-K3D-DISPOSABLE

Private keys travel only on ``kubectl exec -i`` stdin and are scrubbed from
every message this script prints. Cleanup (delete the session permanently,
cancel and delete the job, delete the connectors and the Gitea repositories)
runs in ``finally`` unless ``--keep``: nothing named with the gate id stays.
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
import tarfile
import tempfile
import threading
import time
import uuid
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
STATELESS_AGENT = "deploy/srw-agent-stateless"
AGENT_CONTAINER = "agent"
POSTGRES_POD = "srw-postgres-0"
WORKSPACE_CONTAINER = "workspace"
HOME = "/home/agent-host"
KEYCLOAK_TOKEN_URL = "http://srw-keycloak:8080/realms/srw/protocol/openid-connect/token"
POD_ROOT = "/app"

# The files whose served bytes must be this checkout's (a green Tilt resource
# can still run an old image).
ORCHESTRATOR_FILES = [
    "src/orchestrator/services/workspace_ssh_connector.py",
    "src/orchestrator/services/connector_drivers/workspace_ssh.py",
    "src/orchestrator/services/connector_drivers/repository.py",
    "src/orchestrator/services/connector_drivers/ssh_key.py",
    "src/orchestrator/services/agent_datasource_payload.py",
    "src/orchestrator/services/thread_mount_rows.py",
    "src/orchestrator/services/thread_workspace_delivery.py",
    "src/orchestrator/services/datasources.py",
    "src/shared/runtime/core/workspace_ssh_identity.py",
    "src/shared/runtime/core/managed_repository.py",
]
AGENT_FILES = [
    "src/agent/core/datasource_setup.py",
    "src/agent/api/persistent_session.py",
    "src/agent/agent.py",
    "src/shared/runtime/core/workspace_ssh_identity.py",
    "src/shared/runtime/core/managed_repository.py",
]

_SECRETS: list[str] = []


class GateError(RuntimeError):
    """Infrastructure trouble: the gate could not observe the product."""


class SafetyError(RuntimeError):
    """The requested run is outside the local disposable boundary."""


def _scrub(text: str) -> str:
    for secret in _SECRETS:
        if secret:
            text = text.replace(secret, "<redacted>")
    return text


def command(args: list[str], *, data: str | None = None, timeout: int = 180) -> str:
    """Run argv; never echo argv or stdin, only a short scrubbed label."""

    label = " ".join(args[4:7]) if args[:1] == ["kubectl"] else " ".join(args[:2])
    try:
        result = subprocess.run(
            args, input=data, text=True, capture_output=True, timeout=timeout
        )
    except subprocess.TimeoutExpired:
        raise GateError(f"{label[:80]} timed out after {timeout}s") from None
    if result.returncode:
        tail = _scrub(result.stderr.strip()[-400:])
        raise GateError(f"{label[:80]} failed (exit {result.returncode}): {tail}")
    return result.stdout.strip()


def sql(query: str) -> str:
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
# Programs run inside the orchestrator container (stdin carries every secret)
# ---------------------------------------------------------------------------

# Every program the gate runs inside a pod starts with this and calls
# cap_memory() once its imports are done. The orchestrator pod has a 1 GiB
# limit and serves the product meanwhile: a gate program that grows must
# fail the gate with a MemoryError, never take the pod to the OOM killer.
# RLIMIT_DATA counts heap, anonymous maps and thread stacks, not the shared
# libraries mapped in, so the budget is what the program adds after import.
POD_MEMORY_BUDGET = 256 << 20
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

_API_PROGRAM = (
    _POD_MEMORY_CAP
    + r"""
import json, sys, urllib.error, urllib.parse, urllib.request
cap_memory()
envelope = json.loads(sys.stdin.readline())
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
form = urllib.parse.urlencode({
    "grant_type": "password", "client_id": "admin-cli", "scope": "openid",
    "username": envelope["username"], "password": envelope["password"],
}).encode()
with opener.open(envelope["token_url"], data=form, timeout=30) as response:
    token = json.load(response)["id_token"]
body = envelope.get("body")
request = urllib.request.Request(
    "http://localhost:8085" + envelope["path"],
    data=None if body is None else json.dumps(body).encode(),
    method=envelope["method"],
    headers={"Authorization": "Bearer " + token, "Content-Type": "application/json"},
)
try:
    with opener.open(request, timeout=180) as response:
        status, text = response.status, response.read().decode("utf-8", "replace")
except urllib.error.HTTPError as error:
    status, text = error.code, error.read().decode("utf-8", "replace")
print(json.dumps({"status": status, "body": text}))
"""
)

_GITEA_PROGRAM = (
    _POD_MEMORY_CAP
    + r"""
import asyncio, json, sys
from orchestrator.services.gitea import GiteaClient
cap_memory()
request = json.loads(sys.stdin.readline())
async def main():
    client = GiteaClient()
    if not await client.ensure_initialized():
        raise SystemExit("gitea is not initialized")
    out = {}
    try:
        if request["action"] == "setup":
            host, port = client._ssh_internal_endpoint()
            out.update(owner=client.repository_owner, ssh_host=host, ssh_port=port)
            for repo, marker in request["repos"].items():
                if not await client.create_repo(repo, intent_marker=marker):
                    raise SystemExit("repository creation failed")
            for key in request["deploy_keys"]:
                key_id = await client.ensure_repo_deploy_key(
                    key["repo"], title=key["title"], public_key=key["public_key"],
                    access_mode=key["access_mode"],
                )
                if key_id is None:
                    raise SystemExit("deploy key registration failed")
        elif request["action"] == "branch_head":
            out["sha"] = await client.get_branch_head_sha(
                request["repo"], request["branch"]
            )
        elif request["action"] == "cleanup":
            out["deleted"] = [
                repo for repo, marker in request["repos"].items()
                if await client.delete_repo(repo, intent_marker=marker)
            ]
    finally:
        await client.close()
    print(json.dumps(out))
asyncio.run(main())
"""
)

# Lists snapshot objects, or passes ONE object's bytes through to stdout,
# 1 MiB at a time. Nothing is read whole, decompressed or scanned in the
# pod: the gate does that in its own process (scan_snapshot_object).
# ``stream`` exits 3 when no object store is configured.
_SNAPSHOT_PROGRAM = (
    _POD_MEMORY_CAP
    + r"""
import asyncio, json, sys
from orchestrator.services.snapshot_service import SnapshotService
request = json.loads(sys.stdin.readline())
async def connect():
    service = SnapshotService()
    await service.connect(None)
    return service
service = asyncio.run(connect())
s3 = getattr(service, "_s3", None)
cap_memory()
if request["mode"] == "list":
    if s3 is None:
        print(json.dumps({"configured": False}))
        raise SystemExit(0)
    keys = []
    for prefix in request["prefixes"]:
        token = None
        while True:
            kwargs = {"Bucket": service._bucket, "Prefix": prefix}
            if token:
                kwargs["ContinuationToken"] = token
            page = s3.list_objects_v2(**kwargs)
            keys += [item["Key"] for item in page.get("Contents", [])]
            if not page.get("IsTruncated"):
                break
            token = page.get("NextContinuationToken")
    print(json.dumps({"configured": True, "objects": keys}))
elif request["mode"] == "stream":
    if s3 is None:
        raise SystemExit(3)
    body = s3.get_object(Bucket=service._bucket, Key=request["key"])["Body"]
    out = sys.stdout.buffer
    while True:
        chunk = body.read(1 << 20)
        if not chunk:
            break
        out.write(chunk)
    out.flush()
else:
    raise SystemExit("unknown mode")
"""
)

_HASH_PROGRAM = (
    _POD_MEMORY_CAP
    + r"""
import hashlib, json, sys
from pathlib import Path
cap_memory()
root = sys.argv[1]
expected = json.loads(sys.stdin.read())
print(json.dumps(sorted(
    path for path, digest in expected.items()
    if not Path(root, path).is_file()
    or hashlib.sha256(Path(root, path).read_bytes()).hexdigest() != digest
)))
"""
)


ZSTD_MAGIC = b"\x28\xb5\x2f\xfd"
#: A private key the gate does not know about: a PEM private-key header
#: line, up to two RFC 1421 header lines and a blank line, then a base64 body
#: line. The bare words "PRIVATE KEY" are not enough: the k3d run of
#: 2026-10-08 found them only in library code under /usr/local
#: (cryptography's ``_SK_START = b"-----BEGIN OPENSSH PRIVATE KEY-----"``,
#: PyJWT's PEM type list, rsa and google-auth docstrings, Node's string
#: table), never followed by a key body.
GENERIC_KEY_PATTERN = re.compile(
    rb"-----BEGIN [A-Z0-9 ]{0,40}PRIVATE KEY-----\r?\n"
    rb"(?:[A-Za-z][A-Za-z0-9-]{0,40}: [^\r\n]{0,100}\r?\n){0,2}(?:\r?\n)?"
    rb"[A-Za-z0-9+/]{40}"
)
#: The longest text GENERIC_KEY_PATTERN can match; consecutive scan windows
#: overlap by at least this much less one byte.
GENERIC_KEY_SPAN = 512
STREAM_CHUNK = 1 << 20


def _needle_span(needle: Any) -> int:
    return len(needle) if isinstance(needle, bytes) else GENERIC_KEY_SPAN


def _needle_in(needle: Any, window: bytes) -> bool:
    if isinstance(needle, bytes):
        return needle in window
    return needle.search(window) is not None


def scan_stream(
    read: Callable[[int], bytes],
    needles: list[Any],
    *,
    chunk_size: int = STREAM_CHUNK,
) -> tuple[set[int], int, bytes]:
    """Scan a byte stream chunk by chunk: ``(needle indexes found, size, head)``.

    A needle is literal bytes or a compiled pattern no longer than
    GENERIC_KEY_SPAN. Consecutive windows overlap by the longest needle less
    one byte, so a needle split across two reads is still found; nothing
    larger than one chunk plus that overlap is ever held.
    """

    overlap = max((_needle_span(needle) for needle in needles), default=1) - 1
    found: set[int] = set()
    size, head, tail = 0, b"", b""
    while True:
        chunk = read(chunk_size)
        if not chunk:
            return found, size, head
        if len(head) < 4:
            head = (head + chunk)[:4]
        size += len(chunk)
        window = tail + chunk
        found.update(
            index for index, needle in enumerate(needles) if _needle_in(needle, window)
        )
        tail = window[-overlap:] if overlap > 0 else b""


@dataclass
class ObjectScan:
    complete: bool
    #: needle label -> where it was found: tar member paths, or "(object)"
    #: for an object that is not a tar archive. Never the needle itself.
    hits: dict[str, list[str]]
    size: int
    members: int = 0
    detail: str = ""

    @property
    def hit(self) -> bool:
        return bool(self.hits)


#: At most this many member paths are kept per needle label.
HIT_PATHS_KEPT = 20


class _Counted:
    """A read-through wrapper that counts the bytes read."""

    def __init__(self, stream: Any) -> None:
        self._stream, self.size = stream, 0

    def read(self, size: int = -1) -> bytes:
        chunk = self._stream.read(size)
        self.size += len(chunk)
        return chunk


def scan_tar_stream(
    stream: Any, needles: list[Any]
) -> tuple[dict[int, set[str]], int, int]:
    """Scan a tar stream member by member: ``(hits, members, size)``.

    ``hits`` maps a needle's index to the member paths it was found in: a
    member's name, link name and pax headers, then its body 1 MiB at a time
    (:func:`scan_stream`), so no member is ever held whole. Whatever follows
    the end-of-archive marker is drained, so the decoder feeding the stream
    finishes cleanly.
    """

    hits: dict[int, set[str]] = {}
    members = 0
    counted = _Counted(stream)
    with tarfile.open(fileobj=counted, mode="r|") as archive:
        for member in archive:
            members += 1
            header = "\0".join(
                [
                    member.name,
                    member.linkname,
                    json.dumps(member.pax_headers, sort_keys=True),
                ]
            ).encode("utf-8", "surrogateescape")
            found = {
                index
                for index, needle in enumerate(needles)
                if _needle_in(needle, header)
            }
            body = archive.extractfile(member) if member.isfile() else None
            if body is not None:
                found |= scan_stream(body.read, needles)[0]
            for index in found:
                hits.setdefault(index, set()).add(member.name)
    while counted.read(STREAM_CHUNK):
        pass
    return hits, members, counted.size


def snapshot_stream_command() -> list[str]:
    """The producer: the pod passes the object's bytes through to stdout."""
    return (
        K
        + ["exec", "-i", ORCHESTRATOR, "-c", ORCHESTRATOR_CONTAINER, "--"]
        + ["python", "-c", _SNAPSHOT_PROGRAM]
    )


def scan_snapshot_object(
    key: str,
    needles: list[Any],
    labels: list[str] | None = None,
    *,
    timeout: int = 900,
) -> ObjectScan:
    """Stream one snapshot object out of the pod and scan it here.

    A ``.zst`` object is decompressed by a local ``zstd -dc`` reading the
    producer's stdout directly; both exit codes must be 0, so a truncated
    transfer or archive is never read as clean. A raw object that turns out
    to be zstd-compressed is not complete either: its bytes were never
    looked at decompressed. A ``.tar`` object is scanned member by member
    (:func:`scan_tar_stream`), so a hit names the member it is in; any other
    object as one byte stream. Hits are reported by ``labels`` (parallel to
    ``needles``), never by needle. Raises FileNotFoundError when zstd is
    missing.
    """

    labels = labels or [f"needle {index}" for index in range(len(needles))]
    name = key.rsplit("/", 1)[-1]
    compressed = ".zst" in name
    archive = ".tar" in name
    members = 0
    failure = ""
    request = (json.dumps({"mode": "stream", "key": key}) + "\n").encode()
    with tempfile.TemporaryFile() as errors:
        producer = subprocess.Popen(
            snapshot_stream_command(),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=errors,
        )
        processes = [producer]
        try:
            assert producer.stdin is not None and producer.stdout is not None
            producer.stdin.write(request)
            producer.stdin.close()
            source = producer.stdout
            if compressed:
                decoder = subprocess.Popen(
                    ["zstd", "-dcq"],
                    stdin=producer.stdout,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.DEVNULL,
                )
                processes.append(decoder)
                producer.stdout.close()
                assert decoder.stdout is not None
                source = decoder.stdout
            watchdog = threading.Timer(
                timeout, lambda: [process.kill() for process in processes]
            )
            watchdog.start()
            try:
                if archive:
                    by_member, members, size = scan_tar_stream(source, needles)
                    head = b""
                else:
                    found, size, head = scan_stream(source.read, needles)
                    by_member = {index: {"(object)"} for index in found}
            except (tarfile.TarError, OSError) as error:
                by_member, size, head = {}, 0, b""
                failure = type(error).__name__
            finally:
                watchdog.cancel()
            codes = [process.wait(timeout=60) for process in processes]
        finally:
            for process in processes:
                if process.poll() is None:
                    process.kill()
                    process.wait()
        errors.seek(0)
        stderr = _scrub(errors.read()[-300:].decode("utf-8", "replace").strip())
    complete = (
        not failure
        and all(code == 0 for code in codes)
        and size > 0
        and (compressed or not head.startswith(ZSTD_MAGIC))
        # Zeros read as an empty tar; a snapshot never is one.
        and (not archive or members > 0)
    )
    detail = (
        ""
        if complete
        else f"exit {codes}, {size} bytes; {failure} {stderr}".rstrip("; ")
    )
    hits: dict[str, list[str]] = {}
    for index, paths in by_member.items():
        merged = hits.setdefault(labels[index], [])
        merged.extend(path for path in sorted(paths) if path not in merged)
    return ObjectScan(
        complete=complete,
        hits={label: paths[:HIT_PATHS_KEPT] for label, paths in sorted(hits.items())},
        size=size,
        members=members,
        detail=detail,
    )


def in_orchestrator(
    program: str, payload: dict[str, Any], *, timeout: int = 180
) -> dict:
    out = command(
        K
        + ["exec", "-i", ORCHESTRATOR, "-c", ORCHESTRATOR_CONTAINER, "--"]
        + ["python", "-c", program],
        data=json.dumps(payload) + "\n",
        timeout=timeout,
    )
    return json.loads(out.splitlines()[-1])


class Api:
    def __init__(self, username: str, password: str) -> None:
        self.username = username
        self.password = password
        _SECRETS.append(password)

    def call(self, method: str, path: str, body: Any = None) -> tuple[int, Any]:
        result = in_orchestrator(
            _API_PROGRAM,
            {
                "username": self.username,
                "password": self.password,
                "token_url": KEYCLOAK_TOKEN_URL,
                "method": method,
                "path": path,
                "body": body,
            },
        )
        text = _scrub(result.get("body") or "")
        try:
            parsed = json.loads(text) if text.strip() else {}
        except json.JSONDecodeError:
            parsed = {"raw": text[:400]}
        return int(result["status"]), parsed

    def ok(self, method: str, path: str, body: Any = None) -> Any:
        status, parsed = self.call(method, path, body)
        if status not in (200, 201, 202, 204):
            raise GateError(f"{method} {path} -> HTTP {status}: {str(parsed)[:300]}")
        return parsed


# ---------------------------------------------------------------------------
# Keys
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class GateKey:
    label: str
    private_key: str
    public_key: str
    fingerprint: str

    @property
    def needles(self) -> list[str]:
        """Body lines unique to this key: what any copy of it would carry.

        The first body line is the format header every OpenSSH key shares, so
        it identifies nothing; every later full-length line does.
        """
        body = self.private_key.splitlines()[2:-1]
        return [line for line in body if len(line) >= 40]


def make_key(label: str, *, passphrase: bytes | None = None) -> GateKey:
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
    from cryptography.hazmat.primitives.serialization import (
        BestAvailableEncryption,
        Encoding,
        NoEncryption,
        PrivateFormat,
        PublicFormat,
    )

    private = Ed25519PrivateKey.generate()
    encryption = BestAvailableEncryption(passphrase) if passphrase else NoEncryption()
    private_text = private.private_bytes(
        Encoding.PEM, PrivateFormat.OpenSSH, encryption
    ).decode()
    public_text = (
        private.public_key()
        .public_bytes(Encoding.OpenSSH, PublicFormat.OpenSSH)
        .decode()
    )
    digest = hashlib.sha256(base64.b64decode(public_text.split()[1])).digest()
    fingerprint = "SHA256:" + base64.b64encode(digest).decode().rstrip("=")
    _SECRETS.append(private_text)
    _SECRETS.extend(line for line in private_text.splitlines()[1:-1] if len(line) > 20)
    return GateKey(
        label, private_text, f"{public_text} srw-c1-gate-{label}", fingerprint
    )


def random_host_key() -> str:
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
    from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

    return (
        Ed25519PrivateKey.generate()
        .public_key()
        .public_bytes(Encoding.OpenSSH, PublicFormat.OpenSSH)
        .decode()
    )


# ---------------------------------------------------------------------------
# The gate
# ---------------------------------------------------------------------------


@dataclass
class Report:
    gate_id: str
    results: list[tuple[str, bool, str]] = field(default_factory=list)
    skipped: list[tuple[str, str]] = field(default_factory=list)

    def check(self, name: str, ok: bool, detail: str = "") -> bool:
        self.results.append((name, bool(ok), _scrub(detail)))
        print(
            f"{'PASS' if ok else 'FAIL'} {name}{': ' + _scrub(detail) if detail else ''}",
            flush=True,
        )
        return bool(ok)

    def skip(self, name: str, reason: str) -> None:
        """A check that could not run here; it neither passes nor fails."""
        self.skipped.append((name, _scrub(reason)))
        print(f"SKIP {name}: {_scrub(reason)}", flush=True)

    @property
    def passed(self) -> bool:
        return bool(self.results) and all(ok for _, ok, _ in self.results)


PLAN = [
    "preflight: k3d-srw context, served C1 bytes, Gitea and Keycloak reachable",
    "fixture: 3 Gitea repos, 4 ed25519 deploy/connector keys (stdin only)",
    "validation: injected host, alias host and passphrase key are refused (400)",
    "connectors: A (pinned via Test), B (unpinned, same host), C (ssh_key), D (wrong pin)",
    "session: stateless sandbox session with A, B, C, D; one turn",
    "workspace: no-key, one-key, clone-push, same-host, ssh-key, wrong-pin, include",
    "transcript: no key body in thread rows",
    "detach: drop B, one more turn; B retired, A still works",
    "job: stateless job with A and C; no key in workspace, checkpoint or job rows",
    "job-snap: cancel the still-running job; wait for jobs/<id>/ objects; scan them",
    "end: End the session; no ssh-agent left",
    "snapshot: scan threads/<id>/ objects if any (a sandbox End writes none)",
    "cleanup: delete session, cancel+delete job, delete connectors and repos",
]


# Counts from /proc, so a missing ps cannot read as "zero agents"; the
# trailing marker line is the only output that counts as an answer.
COUNT_SSH_AGENTS = (
    "n=0\n"
    "for f in /proc/[0-9]*/comm; do\n"
    '  IFS= read -r c < "$f" 2>/dev/null || continue\n'
    '  [ "$c" = ssh-agent ] && n=$((n + 1))\n'
    "done\n"
    'echo "ssh-agents=$n"\n'
)


def ssh_agent_count(rc: int, output: str) -> int | None:
    """The count ``COUNT_SSH_AGENTS`` printed, or None when it did not answer."""
    if rc != 0:
        return None
    lines = output.strip().splitlines()
    match = re.fullmatch(r"ssh-agents=([0-9]+)", lines[-1]) if lines else None
    return int(match.group(1)) if match else None


def origin_alias(origin: str, *, owner: str, repo: str) -> str | None:
    """The identity alias in a clone origin, either URL form, or None."""
    alias = r"(srw-repo-[0-9a-f]{32})"
    path = rf"{re.escape(owner)}/{re.escape(repo)}\.git"
    match = re.fullmatch(rf"ssh://{alias}/{path}|{alias}:{path}", origin)
    return (match.group(1) or match.group(2)) if match else None


# A job in these statuses is still running; a cancel ends it (and on k3d is
# what uploads its jobs/<id>/ snapshot).
JOB_RUNNING = frozenset({"created", "processing"})
# The job's graph has taken this many steps (checkpoint rows) once its agent
# is at work: a cancel before that takes no snapshot.
JOB_AT_WORK_CHECKPOINTS = 4


class SshAgentConnectorsGate:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.gate_id = f"c1-{secrets.token_hex(5)}"
        self.report = Report(self.gate_id)
        self.api = Api(args.user, args.password)
        self.keys: dict[str, GateKey] = {}
        self.repos: dict[str, str] = {}  # repo name -> creation intent marker
        self.connectors: dict[str, str] = {}  # label -> datasource id
        self.thread: str | None = None
        self.job: str | None = None
        self._snapshot_listing: list[str] = []
        self._store: bool | None = None
        self.gitea: dict[str, Any] = {}

    # -- helpers -----------------------------------------------------------
    def repo(self, label: str) -> str:
        return f"srw-{self.gate_id}-{label}"

    def ssh_url(self, label: str) -> str:
        host, port = self.gitea["ssh_host"], self.gitea["ssh_port"]
        return f"ssh://git@{host}:{port}/{self.gitea['owner']}/{self.repo(label)}.git"

    def needles(self) -> list[str]:
        return [needle for key in self.keys.values() for needle in key.needles]

    def aliases(self, pod: str) -> dict[str, str]:
        """``{connector label: srw-repo-<slug>}`` from the README facts block."""
        _rc, readme = self.ws(pod, "cat ~/workspace/README.md\n")
        found: dict[str, str] = {}
        for label in self.connectors:
            for line in readme.splitlines():
                if f"**{self.gate_id} {label}**" not in line:
                    continue
                match = re.search(r"srw-repo-[0-9a-f]{32}", line)
                if match:
                    found[label] = match.group(0)
        return found

    def workspace_pod(self, selector: str) -> str:
        def probe() -> str | None:
            pods = json.loads(
                command(K + ["get", "pods", "-l", selector, "-o", "json"])
            )["items"]
            running = [
                pod["metadata"]["name"]
                for pod in pods
                if pod.get("status", {}).get("phase") == "Running"
                and not pod["metadata"].get("deletionTimestamp")
            ]
            return running[0] if len(running) == 1 else None

        return wait_for(f"workspace pod {selector}", probe, timeout=300)

    def ws(self, pod: str, script: str, *, check: bool = True) -> tuple[int, str]:
        """Run ``script`` as agent-host in the workspace (stdin, never argv)."""
        result = subprocess.run(
            K
            + ["exec", "-i", pod, "-c", WORKSPACE_CONTAINER, "--"]
            + ["su", "-s", "/bin/bash", "agent-host", "-c", "bash -s"],
            input="set -u\ncd ~\n" + script,
            text=True,
            capture_output=True,
            timeout=120,
        )
        output = _scrub(result.stdout.strip())
        if check and result.returncode:
            raise GateError(
                f"workspace command failed (exit {result.returncode}): "
                + _scrub(result.stderr.strip()[-300:])
            )
        return result.returncode, output

    def queue(self, thread: str) -> tuple[str, int, int] | None:
        row = sql(
            "SELECT state || ' ' || coalesce(input_seq, 0) || ' ' || "
            f"coalesce(consumed_seq, 0) FROM run_queue WHERE unit_id = {lit(thread)}"
        )
        if not row:
            return None
        state, input_seq, consumed_seq = row.split()
        if state == "parked":
            raise GateError("the session unit parked")
        return state, int(input_seq), int(consumed_seq)

    def turn(self, text: str, step: int) -> None:
        before = self.queue(self.thread)
        previous = before[1] if before else 0
        self.api.ok(
            "POST", f"/api/persistent/threads/{self.thread}/input", {"content": text}
        )

        def answered() -> bool:
            current = self.queue(self.thread)
            return bool(
                current
                and current[0] == "done"
                and current[1] > previous
                and current[1] == current[2]
            )

        wait_for(f"turn {step} answered", answered, timeout=self.args.turn_timeout)

    # -- phases ------------------------------------------------------------
    def preflight(self) -> None:
        for target, container, paths in (
            (ORCHESTRATOR, ORCHESTRATOR_CONTAINER, ORCHESTRATOR_FILES),
            (STATELESS_AGENT, AGENT_CONTAINER, AGENT_FILES),
        ):
            expected = {
                path: hashlib.sha256((ROOT / path).read_bytes()).hexdigest()
                for path in paths
            }
            stale = json.loads(
                command(
                    K
                    + ["exec", "-i", target, "-c", container, "--"]
                    + ["python", "-c", _HASH_PROGRAM, POD_ROOT],
                    data=json.dumps(expected),
                ).splitlines()[-1]
            )
            if stale:
                raise GateError(f"{target} does not serve this checkout: {stale}")
        print("PASS preflight: orchestrator and stateless agent serve this checkout")

    def fixture(self) -> None:
        for label in ("a", "b", "c", "d"):
            self.keys[label] = make_key(label)
        self.repos = {self.repo(label): str(uuid.uuid4()) for label in ("a", "b", "d")}
        self.gitea = in_orchestrator(
            _GITEA_PROGRAM,
            {
                "action": "setup",
                "repos": self.repos,
                "deploy_keys": [
                    {
                        "repo": self.repo("a"),
                        "title": f"{self.gate_id}-a",
                        "public_key": self.keys["a"].public_key,
                        "access_mode": "write",
                    },
                    {
                        "repo": self.repo("b"),
                        "title": f"{self.gate_id}-b",
                        "public_key": self.keys["b"].public_key,
                        "access_mode": "write",
                    },
                    {
                        "repo": self.repo("a"),
                        "title": f"{self.gate_id}-c",
                        "public_key": self.keys["c"].public_key,
                        "access_mode": "read",
                    },
                ],
            },
        )
        print(
            f"fixture: Gitea SSH {self.gitea['ssh_host']}:{self.gitea['ssh_port']} "
            f"owner {self.gitea['owner']}, repos {sorted(self.repos)}"
        )

    def create_connector(self, label: str, body: dict[str, Any]) -> str:
        created = self.api.ok(
            "POST",
            "/api/datasources",
            {"name": f"{self.gate_id} {label}", "scope_mode": "all", **body},
        )
        self.connectors[label] = str(created["id"])
        return self.connectors[label]

    def attempt_connector(self, label: str, body: dict[str, Any]) -> tuple[int, Any]:
        """POST a connector that should be refused; record it if it was not.

        The id is recorded before anyone looks at the status code, so a
        regression that answers 201 still leaves a connector cleanup deletes.
        """
        status, parsed = self.api.call(
            "POST",
            "/api/datasources",
            {"name": f"{self.gate_id} {label}", "scope_mode": "all", **body},
        )
        if isinstance(parsed, dict) and parsed.get("id"):
            self.connectors[label] = str(parsed["id"])
        return status, parsed

    def validation(self) -> None:
        encrypted = make_key("encrypted", passphrase=b"gate-passphrase")
        key = {"files": [{"contents": self.keys["c"].private_key}]}
        status, _body = self.attempt_connector(
            "refused-host",
            {
                "type": "ssh_key",
                "config": {"host": "gitea\n  ProxyCommand sh -c id"},
                "credentials": key,
            },
        )
        self.report.check(
            "validation: injected host refused", status == 400, f"HTTP {status}"
        )
        status, _body = self.attempt_connector(
            "refused-alias",
            {
                "type": "ssh_key",
                "config": {"host": "SRW-REPO-" + "0" * 32},
                "credentials": key,
            },
        )
        self.report.check(
            "validation: identity-alias host refused", status == 400, f"HTTP {status}"
        )
        status, body = self.attempt_connector(
            "refused-passphrase",
            {
                "type": "ssh_key",
                "credentials": {"files": [{"contents": encrypted.private_key}]},
            },
        )
        self.report.check(
            "validation: passphrase key refused",
            status == 400 and "passphrase" in json.dumps(body),
            f"HTTP {status}",
        )

    def connectors_setup(self) -> None:
        def repository(label: str, key: str, config: dict[str, Any]) -> dict:
            return {
                "type": "repository",
                "connection_url": self.ssh_url(label),
                "config": {"forge": "gitea", **config},
                "credentials": {
                    "auth_method": "ssh",
                    "ssh_key": self.keys[key].private_key,
                },
            }

        a = self.create_connector("A", repository("a", "a", {}))
        tested = self.api.ok("POST", f"/api/datasources/{a}/test")
        host_key = (tested.get("details") or {}).get("host_key")
        self.report.check(
            "test-pin: Test reports the host key",
            tested.get("status") == "ok" and bool(host_key),
            tested.get("message", ""),
        )
        self.api.ok(
            "PUT",
            f"/api/datasources/{a}",
            {"config": {"forge": "gitea", "known_hosts": host_key}},
        )
        retested = self.api.ok("POST", f"/api/datasources/{a}/test")
        self.report.check(
            "test-pin: pinned key matches",
            retested.get("status") == "ok"
            and (retested.get("details") or {}).get("host_key_matches_pin") is True,
            retested.get("message", ""),
        )
        self.create_connector("B", repository("b", "b", {}))
        self.create_connector(
            "C",
            {
                "type": "ssh_key",
                "config": {
                    "host": self.gitea["ssh_host"],
                    "user": "git",
                    "port": int(self.gitea["ssh_port"]),
                },
                "credentials": {"files": [{"contents": self.keys["c"].private_key}]},
            },
        )
        self.create_connector(
            "D", repository("d", "d", {"known_hosts": random_host_key()})
        )

    def session(self) -> None:
        body: dict[str, Any] = {
            "title": f"C1 ssh-agent gate {self.gate_id}",
            "permission_mode": "autonomous",
            "datasource_ids": [self.connectors[label] for label in "ABCD"],
            "config_override": {"workspace": {"backend": "sandbox"}},
        }
        if self.args.model:
            body["model"] = self.args.model
        created = self.api.ok("POST", "/api/persistent/threads", body)
        self.thread = str(created.get("thread_id") or created["id"])
        print(f"session {self.thread}")
        lane = sql(f"SELECT execution_lane FROM threads WHERE id = {lit(self.thread)}")
        if lane != "stateless":
            raise GateError(f"session lane is {lane!r}, not stateless")
        self.turn("Reply with the single word READY.", 1)

    def workspace_checks(self, pod: str, *, labels: str) -> None:
        # The needles reach grep through a pipe from this script's stdin; they
        # are never written to a file in the workspace (where grep would then
        # find them).
        needles = "\n".join(["PRIVATE KEY", *self.needles()])
        _rc, found = self.ws(
            pod,
            f"grep -rlF -D skip --binary-files=text "
            f"-f <(cat <<'C1_NEEDLES'\n{needles}\nC1_NEEDLES\n"
            f") {HOME} 2>/dev/null || true\n"
            "ls -1 ~/.ssh/repo_* 2>/dev/null || true\n",
        )
        self.report.check(f"no-key ({labels})", not found, found[:300])

        _rc, listing = self.ws(
            pod,
            "for socket in ~/.ssh/srw-managed/sockets/*.sock; do\n"
            '  test -S "$socket" || continue\n'
            "  printf '%s %s\\n' \"${socket##*/}\" "
            '"$(SSH_AUTH_SOCK="$socket" ssh-add -l 2>/dev/null | '
            'awk \'NF { printf "%s,", $2 }\')"\n'
            "done\n",
        )
        held: dict[str, list[str]] = {}
        for line in listing.splitlines():
            name, _, prints = line.partition(" ")
            held[name] = [value for value in prints.split(",") if value]
        one_each = bool(held) and all(len(value) == 1 for value in held.values())
        wanted = {self.keys[label.lower()].fingerprint for label in labels}
        holding = {value[0] for value in held.values() if len(value) == 1}
        self.report.check(
            f"one-key ({labels})",
            one_each and wanted <= holding,
            f"{len(held)} sockets; missing {sorted(wanted - holding)}",
        )

    def session_checks(self) -> None:
        pod = self.workspace_pod(f"app=srw-workspace,srw/thread-id={self.thread}")
        self.workspace_checks(pod, labels="ABCD")
        owner = self.gitea["owner"]
        a, b, d = self.repo("a"), self.repo("b"), self.repo("d")
        branch = f"{self.gate_id}-push"

        rc, origins = self.ws(
            pod,
            f"git -C ~/workspace/repos/{a} remote get-url origin\n"
            f"git -C ~/workspace/repos/{b} remote get-url origin\n",
            check=False,
        )
        lines = origins.splitlines()
        matches = [
            origin_alias(line, owner=owner, repo=name)
            for line, name in zip(lines, (a, b))
        ]
        cloned = rc == 0 and len(lines) == 2 and all(matches)
        self.report.check("clone-push: cloned through aliases", cloned, origins)
        alias_a = matches[0] if cloned else "srw-repo-unknown"

        rc, _out = self.ws(
            pod,
            f"cd ~/workspace/repos/{a} && git fetch -q origin\n"
            f"cd ~/workspace/repos/{b} && git fetch -q origin\n",
            check=False,
        )
        self.report.check("same-host: both deploy keys fetch", rc == 0)
        rc, _out = self.ws(
            pod,
            f"cd ~/workspace/repos/{a} && "
            "git -c user.email=c1-gate@srw.invalid -c user.name=c1-gate "
            "commit -q --allow-empty -m 'C1 gate push' && "
            f"git push -q origin HEAD:refs/heads/{branch} && git rev-parse HEAD\n",
            check=False,
        )
        local_head = _out.splitlines()[-1] if rc == 0 and _out else ""
        remote_head = (
            in_orchestrator(
                _GITEA_PROGRAM, {"action": "branch_head", "repo": a, "branch": branch}
            ).get("sha")
            if local_head
            else None
        )
        self.report.check(
            "clone-push: push lands in Gitea",
            bool(local_head) and remote_head == local_head,
        )
        rc, _out = self.ws(
            pod,
            f"GIT_TERMINAL_PROMPT=0 git ls-remote {alias_a}:{owner}/{b}.git "
            ">/dev/null 2>&1\n",
            check=False,
        )
        self.report.check("same-host: alias A cannot read repo b", rc != 0)
        rc, _out = self.ws(
            pod,
            "GIT_TERMINAL_PROMPT=0 git ls-remote "
            f"{self.ssh_url('a')} HEAD >/dev/null\n",
            check=False,
        )
        self.report.check("ssh-key: plain host uses connector C", rc == 0)

        aliases = self.aliases(pod)
        alias_d = aliases.get("D", "")
        rc, out = self.ws(
            pod,
            f"test ! -e ~/workspace/repos/{d} || echo cloned\n"
            f"ssh -o BatchMode=yes -T {alias_d or 'srw-repo-missing'} true 2>&1 | "
            "grep -c 'Host key verification failed' || true\n",
            check=False,
        )
        self.report.check(
            "wrong-pin: D is not cloned and fails host-key verification",
            bool(alias_d) and "cloned" not in out and out.strip().endswith("1"),
            f"alias {'found' if alias_d else 'missing'}; {out[-120:]}",
        )
        rc, _out = self.ws(
            pod,
            'awk \'prev == "Match all" && '
            '$0 == "Include /home/agent-host/.ssh/srw-managed/config.d/*.conf" '
            "{ found = 1 } { prev = $0 } END { exit !found }' ~/.ssh/config\n",
            check=False,
        )
        self.report.check("include: Match all before the managed Include", rc == 0)
        self.transcript_checks()

    def transcript_checks(self) -> None:
        clauses = (
            " OR ".join(
                f"position({lit(needle)} in row_text) > 0" for needle in self.needles()
            )
            + " OR position('PRIVATE KEY' in row_text) > 0"
        )
        hits = sql(
            "SELECT count(*) FROM ("
            f"SELECT content AS row_text FROM thread_messages WHERE thread_id = {lit(self.thread)} "
            f"UNION ALL SELECT payload::text FROM thread_events WHERE thread_id = {lit(self.thread)} "
            f"UNION ALL SELECT metadata::text FROM threads WHERE id = {lit(self.thread)}"
            f") rows WHERE {clauses}"
        )
        self.report.check(
            "transcript: no key in thread rows", hits == "0", f"{hits} rows"
        )

    def detach(self) -> None:
        keep = [self.connectors[label] for label in "ACD"]
        self.api.ok(
            "PATCH",
            f"/api/persistent/threads/{self.thread}/config",
            {"datasource_ids": keep},
        )
        self.turn("Reply with the single word AGAIN.", 2)
        pod = self.workspace_pod(f"app=srw-workspace,srw/thread-id={self.thread}")
        _rc, listing = self.ws(
            pod,
            'for socket in ~/.ssh/srw-managed/sockets/*.sock; do test -S "$socket" '
            '|| continue; SSH_AUTH_SOCK="$socket" ssh-add -l 2>/dev/null | '
            "awk 'NF { print $2 }'; done\n",
        )
        held = set(listing.split())
        self.report.check(
            "detach: B's agent is gone, A's remains",
            self.keys["b"].fingerprint not in held
            and self.keys["a"].fingerprint in held,
        )
        rc, _out = self.ws(
            pod,
            f"cd ~/workspace/repos/{self.repo('a')} && git fetch -q origin\n",
            check=False,
        )
        self.report.check("detach: A still fetches", rc == 0)
        rc, _out = self.ws(
            pod,
            f"cd ~/workspace/repos/{self.repo('b')} && "
            "GIT_TERMINAL_PROMPT=0 git fetch -q origin 2>/dev/null\n",
            check=False,
        )
        self.report.check("detach: B no longer fetches", rc != 0)
        aliases = self.aliases(pod)
        self.report.check(
            "detach: README lists A, C and D but not B",
            set(aliases) == {"A", "C", "D"},
            f"aliases for {sorted(aliases)}",
        )

    def end(self) -> None:
        selector = f"app=srw-workspace,srw/thread-id={self.thread}"
        self.api.ok("DELETE", f"/api/persistent/threads/{self.thread}?force=true")
        wait_for(
            "session unit released",
            lambda: sql(
                f"SELECT coalesce(state, 'none') FROM run_queue WHERE unit_id = {lit(self.thread)}"
            )
            not in ("queued", "leased"),
            timeout=180,
        )
        pods = json.loads(command(K + ["get", "pods", "-l", selector, "-o", "json"]))[
            "items"
        ]
        if not pods:
            self.report.check("end: no ssh-agent left (workspace deleted)", True)
            return
        time.sleep(10)
        pod = pods[0]["metadata"]["name"]
        rc, out = self.ws(pod, COUNT_SSH_AGENTS, check=False)
        if rc != 0:
            remaining = json.loads(
                command(K + ["get", "pods", "-l", selector, "-o", "json"])
            )["items"]
            if not remaining:
                self.report.check("end: no ssh-agent left (workspace deleted)", True)
                return
        count = ssh_agent_count(rc, out)
        self.report.check(
            "end: no ssh-agent left",
            count == 0,
            f"exit {rc}; {'unreadable' if count is None else count} agents",
        )

    def job_status(self) -> str:
        return sql(f"SELECT coalesce(status, '') FROM jobs WHERE id = {lit(self.job)}")

    def store_configured(self) -> bool:
        """Whether an object store is configured; reported once if not."""

        if self._store is None:
            listed = self.snapshot_objects(f"threads/{self.thread}/")
            self._store = bool(listed.get("configured"))
            if not self._store:
                self.report.check(
                    "snapshot: object store configured",
                    self.args.allow_no_snapshot,
                    "none"
                    + (" (--allow-no-snapshot)" if self.args.allow_no_snapshot else ""),
                )
        return self._store

    def job_checkpoints(self) -> int:
        return int(
            sql(f"SELECT count(*) FROM checkpoints WHERE thread_id = {lit(self.job)}")
            or 0
        )

    def wait_for_job_at_work(self) -> None:
        """Wait (bounded) until the job's graph has taken a few steps.

        A cancel that lands while a stateless job is still starting deletes
        its workspace without the archive snapshot (k3d, 2026-10-08: a job
        cancelled 16 s after dispatch, just past its first checkpoint, left
        no jobs/<id>/ object; one cancelled minutes in, its agent working,
        left a 99.9 MB snapshot). The job expert has no shell, so progress
        is read from its checkpoint rows, not from the workspace.
        """

        def at_work() -> bool:
            if self.job_status() not in JOB_RUNNING:
                return True
            return self.job_checkpoints() >= JOB_AT_WORK_CHECKPOINTS

        try:
            wait_for(
                "job agent at work",
                at_work,
                timeout=self.args.turn_timeout,
                interval=5,
            )
        except GateError:
            print(
                "note: the job never got past its first steps; cancelling anyway",
                flush=True,
            )

    def job_snapshot(self) -> None:
        """Cancel the running job, then scan the snapshot the cancel uploads.

        On k3d a stateless job's ``jobs/<id>/`` snapshot comes from a cancel:
        neither completion nor an approval takes one. So this runs once the
        job's workspace checks pass and its agent is at work, while it still
        runs, and before End and cleanup (whose delete removes the objects).
        A job that settled first is reported SKIP after one listing.
        """

        if not self.job or not self.store_configured():
            return
        prefix = f"jobs/{self.job}/"
        self.wait_for_job_at_work()
        status = self.job_status()
        if status not in JOB_RUNNING:
            if self.snapshot_objects(prefix).get("objects"):
                self.scan_snapshot("snapshot (job)", prefix)
            else:
                self.report.skip(
                    "snapshot (job)",
                    f"job settled ({status or 'missing'}) before cancel; "
                    "k3d completion takes no snapshot",
                )
            return

        def stopped_status() -> str | None:
            current = self.job_status()
            return current if current and current not in JOB_RUNNING else None

        self.api.ok("PUT", f"/api/jobs/{self.job}/cancel")
        print(f"job {self.job} cancelled to take its snapshot", flush=True)
        try:
            stopped = wait_for(
                "cancelled job stops",
                stopped_status,
                timeout=self.args.job_timeout,
                interval=5,
            )
            self.report.check("job-snap: the cancel finished", True, f"{stopped}")
        except GateError:
            self.report.check(
                "job-snap: the cancel finished",
                False,
                f"still {self.job_status() or 'missing'} after "
                f"{self.args.job_timeout}s",
            )
        self._snapshot_listing = []
        try:
            wait_for(
                "job snapshot objects",
                lambda: self.snapshot_settled(prefix),
                timeout=self.args.snapshot_timeout,
                interval=15,
            )
        except GateError:
            self.report.check(
                "snapshot (job): the cancel took a snapshot",
                False,
                f"no settled {prefix} objects after {self.args.snapshot_timeout}s",
            )
            return
        self.scan_snapshot("snapshot (job)", prefix)

    def snapshot_objects(self, prefix: str) -> dict:
        return in_orchestrator(
            _SNAPSHOT_PROGRAM, {"mode": "list", "prefixes": [prefix]}, timeout=120
        )

    def scan_snapshot(self, name: str, prefix: str) -> None:
        """Stream every object under ``prefix`` out of the pod and scan it here."""

        objects = self.snapshot_objects(prefix).get("objects") or []
        needles, labels = self.labelled_needles()
        hits: list[str] = []
        incomplete: list[str] = []
        for key in objects:
            try:
                scanned = scan_snapshot_object(key, needles, labels)
            except FileNotFoundError:
                self.report.check(f"{name}: zstd decoder available", False)
                return
            for label, paths in scanned.hits.items():
                hits.append(f"{key}: {label} in {paths}")
            if not scanned.complete:
                incomplete.append(f"{key} ({scanned.detail})")
        self.report.check(
            f"{name}: no key in snapshot objects",
            bool(objects) and not hits and not incomplete,
            f"{len(objects)} objects; hits {hits}"
            + (f"; incomplete {incomplete}" if incomplete else ""),
        )

    def labelled_needles(self) -> tuple[list[Any], list[str]]:
        """Every needle with the label a report may print instead of it."""

        needles: list[bytes] = []
        labels: list[str] = []
        for label, key in sorted(self.keys.items()):
            for needle in key.needles:
                needles.append(needle.encode())
                labels.append(f"key {label.upper()}")
        needles.append(GENERIC_KEY_PATTERN)
        labels.append("generic private key (PEM header and body)")
        return needles, labels

    def snapshot_settled(self, prefix: str) -> bool:
        """True once objects exist and the listing held still for one poll."""

        listed = self.snapshot_objects(prefix).get("objects") or []
        previous, self._snapshot_listing = self._snapshot_listing, sorted(listed)
        return bool(listed) and self._snapshot_listing == previous

    def snapshot(self) -> None:
        """Scan the thread's S3 snapshot after End, before cleanup.

        ``--allow-no-snapshot`` covers only a deployment without an object
        store. A stateless sandbox session's End writes no ``threads/<id>/``
        snapshot, so the thread's is scanned when present and reported as
        absent otherwise. (The job's is scanned by :meth:`job_snapshot`.)
        """

        thread_prefix = f"threads/{self.thread}/"
        if not self.store_configured():
            return
        if self.snapshot_objects(thread_prefix).get("objects"):
            self.scan_snapshot("snapshot (thread)", thread_prefix)
        else:
            self.report.check(
                "snapshot (thread): none to scan", True, f"{thread_prefix} absent"
            )

    def job_run(self) -> None:
        created = self.api.ok(
            "POST",
            "/api/jobs",
            {
                # The job runs for minutes either way (its strategic phase
                # alone is several LLM turns), long enough for the workspace
                # checks and the cancel that takes its snapshot.
                "description": (
                    "C1 ssh-agent gate: write the word ok to output/c1.txt, "
                    "then complete the job."
                ),
                "datasource_ids": [self.connectors["A"], self.connectors["C"]],
                "config_override": {"workspace": {"backend": "sandbox"}},
                "execution_lane": "stateless",
            },
        )
        self.job = str(created.get("job_id") or created["id"])
        print(f"job {self.job}")
        wait_for(
            "job checkpoint written",
            lambda: sql(
                "SELECT count(*) > 0 FROM checkpoints WHERE thread_id = "
                f"{lit(self.job)}"
            )
            == "t",
            timeout=self.args.turn_timeout,
        )
        pod = self.workspace_pod(f"app=srw-workspace,srw/job-id={self.job}")
        self.workspace_checks(pod, labels="AC")
        clauses = " OR ".join(
            f"position(convert_to({lit(needle)}, 'UTF8') in blob) > 0"
            for needle in self.needles()
        )
        hits = sql(
            "SELECT count(*) FROM ("
            "SELECT blob FROM checkpoint_blobs UNION ALL "
            "SELECT blob FROM checkpoint_writes UNION ALL "
            "SELECT convert_to(checkpoint::text || metadata::text, 'UTF8') FROM checkpoints"
            f") rows WHERE {clauses}"
        )
        self.report.check(
            "checkpoint: no key in checkpoint tables", hits == "0", f"{hits} rows"
        )
        text_clauses = " OR ".join(
            f"position({lit(needle)} in row_text) > 0" for needle in self.needles()
        )
        hits = sql(
            "SELECT count(*) FROM (SELECT concat_ws(' ', context::text, "
            "config_override::text, resolved_config::text) AS row_text FROM jobs "
            f"WHERE id = {lit(self.job)}) rows WHERE {text_clauses}"
        )
        self.report.check(
            "transcript: no key in the job row", hits == "0", f"{hits} rows"
        )

    def cleanup(self) -> None:
        steps: list[tuple[str, Callable[[], Any]]] = []
        if self.thread:
            steps.append(
                (
                    "delete session",
                    # permanent: an ended thread row (titled with the gate
                    # id) would otherwise stay behind after every run.
                    lambda: self.api.call(
                        "DELETE",
                        f"/api/persistent/threads/{self.thread}"
                        "?force=true&permanent=true",
                    ),
                )
            )
        if self.job:
            steps.append(
                (
                    "cancel job",
                    lambda: self.api.call("PUT", f"/api/jobs/{self.job}/cancel"),
                )
            )
            steps.append(
                ("delete job", lambda: self.api.call("DELETE", f"/api/jobs/{self.job}"))
            )
        for label, datasource_id in self.connectors.items():
            steps.append(
                (
                    f"delete connector {label}",
                    lambda datasource_id=datasource_id: self.api.call(
                        "DELETE", f"/api/datasources/{datasource_id}"
                    ),
                )
            )
        if self.repos:
            steps.append(
                (
                    "delete Gitea repositories",
                    lambda: in_orchestrator(
                        _GITEA_PROGRAM, {"action": "cleanup", "repos": self.repos}
                    ),
                )
            )
        for label, step in steps:
            try:
                step()
            except GateError as exc:
                print(f"cleanup: {label} failed: {exc}", flush=True)

    def run(self) -> int:
        try:
            self.preflight()
            self.fixture()
            self.validation()
            self.connectors_setup()
            self.session()
            self.session_checks()
            self.detach()
            if not self.args.skip_job:
                self.job_run()
                # While the job still runs, and long before cleanup deletes
                # it (and with it every jobs/<id>/ object).
                self.job_snapshot()
            self.end()
            self.snapshot()
        except GateError as exc:
            self.report.check("gate infrastructure", False, str(exc))
        finally:
            if self.args.keep:
                print(
                    "kept: "
                    + json.dumps(
                        {
                            "thread": self.thread,
                            "job": self.job,
                            "connectors": self.connectors,
                            # name -> creation intent marker: what a later
                            # cleanup needs to delete each repository.
                            "repos": self.repos,
                        }
                    )
                )
            else:
                self.cleanup()
        verdict = "PASS" if self.report.passed else "FAIL"
        failed = [name for name, ok, _ in self.report.results if not ok]
        skipped = [name for name, _ in self.report.skipped]
        print(
            f"{verdict} {self.gate_id}: {len(self.report.results)} checks, "
            f"{len(failed)} failed {failed if failed else ''}".rstrip()
            + (f", {len(skipped)} skipped {skipped}" if skipped else "")
        )
        return 0 if self.report.passed else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--context", default=LOCAL_CONTEXT)
    parser.add_argument("--namespace", default=LOCAL_NAMESPACE)
    parser.add_argument("--run", action="store_true")
    parser.add_argument("--confirm")
    parser.add_argument("--user", default="test")
    parser.add_argument("--password", default="srw-k3d-dev-test")
    parser.add_argument("--model", help="session model (default: account default)")
    parser.add_argument("--turn-timeout", type=int, default=420)
    parser.add_argument("--skip-job", action="store_true")
    parser.add_argument(
        "--job-timeout",
        type=int,
        default=900,
        help="seconds a cancelled job may take to stop",
    )
    parser.add_argument(
        "--snapshot-timeout",
        type=int,
        default=300,
        help="seconds to wait for the job's jobs/<id>/ snapshot objects",
    )
    parser.add_argument(
        "--allow-no-snapshot",
        action="store_true",
        help="pass the snapshot check when no object store is configured",
    )
    parser.add_argument("--keep", action="store_true", help="skip cleanup")
    return parser


def validate(args: argparse.Namespace) -> None:
    if args.context != LOCAL_CONTEXT or args.namespace != LOCAL_NAMESPACE:
        raise SafetyError("this gate is restricted to k3d-srw/srw")
    if args.run and args.confirm != LOCAL_CONFIRMATION:
        raise SafetyError(f"--run requires --confirm {LOCAL_CONFIRMATION}")
    if not args.run and args.confirm is not None:
        raise SafetyError("--confirm is accepted only with --run")
    if not 60 <= args.turn_timeout <= 1800:
        raise SafetyError("--turn-timeout must be between 60 and 1800 seconds")
    if not 60 <= args.job_timeout <= 3600:
        raise SafetyError("--job-timeout must be between 60 and 3600 seconds")
    if not 30 <= args.snapshot_timeout <= 1800:
        raise SafetyError("--snapshot-timeout must be between 30 and 1800 seconds")


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
    return SshAgentConnectorsGate(args).run()


if __name__ == "__main__":
    raise SystemExit(main())
