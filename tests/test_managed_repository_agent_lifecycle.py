"""Real-process acceptance for managed-repository ssh-agent ownership."""

from __future__ import annotations

import json
import os
import shlex
import shutil
import signal
import subprocess
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

from orchestrator.services.managed_repository_authority import _deploy_keypair
from orchestrator.services.stateless_session_retirement import (
    ShellRetirementUnavailable,
    resolve_shell_retirement_authority,
    retire_stateless_workspace_residents,
    verify_stateless_workspace_residents_retired,
)
from shared.runtime.core.managed_repository import (
    _SSH_AGENT_RETIRE_PROGRAM,
    managed_repository_agent_launch_command,
    managed_repository_agent_retirement_command,
    managed_repository_agent_zero_command,
    managed_ssh_config_include_command,
    managed_ssh_namespace_setup_command,
    materialize_managed_repository_credentials,
    render_ssh_identity_config,
)


def _run(command: str, *, secret: bytes = b"") -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(
        ["bash", "-c", command],
        input=secret,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
        timeout=20,
    )


def _state(home: Path, authority_id: str) -> tuple[Path, dict[str, str]]:
    slug = authority_id.replace("-", "")
    path = home / ".ssh" / "srw-managed" / "agents" / f"{slug}.state"
    values = dict(line.split("=", 1) for line in path.read_text().splitlines())
    return path, values


def _process_is_generation(pid: int, starttime: str) -> bool:
    stat_path = Path(f"/proc/{pid}/stat")
    try:
        observed = stat_path.read_text().rsplit(")", 1)[1].split()[19]
    except (FileNotFoundError, IndexError, OSError):
        return False
    return observed == starttime


def _runtime_scope() -> str:
    return (
        Path("/proc/sys/kernel/random/boot_id").read_text().strip()
        + ":"
        + str(os.stat("/proc/self/ns/pid").st_ino)
    )


def _receipt_lines(
    *,
    slug: str,
    generation: int,
    pid: int,
    starttime: str,
    socket_path: Path,
    runtime_scope: str | None = None,
    workspace_generation: str = "-",
    runtime_incarnation: str = "-",
) -> str:
    return "\n".join(
        [
            "version=2",
            f"authority_id={slug}",
            f"generation={generation}",
            f"workspace_generation={workspace_generation}",
            f"runtime_incarnation={runtime_incarnation}",
            f"runtime_scope={runtime_scope or _runtime_scope()}",
            f"pid={pid}",
            f"starttime={starttime}",
            f"socket={socket_path}",
            "",
        ]
    )


def _stop(pid: int) -> None:
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline and Path(f"/proc/{pid}").exists():
        time.sleep(0.02)
    if Path(f"/proc/{pid}").exists():
        os.kill(pid, signal.SIGKILL)


def test_proc_identity_race_retries_then_fails_closed() -> None:
    definitions, separator, _program = _SSH_AGENT_RETIRE_PROGRAM.partition(
        "\nmode = sys.argv[1]"
    )
    assert separator
    probe = r"""
class FakeTime:
    def __init__(self):
        self.sleeps = []

    def sleep(self, value):
        self.sleeps.append(value)


time = FakeTime()
calls = 0


def transient(_pid):
    global calls
    calls += 1
    if calls == 1:
        raise SystemExit(86)
    return ("ssh-agent", ["ssh-agent", "-a", "/tmp/agent.sock", "-s"], "123")


_identity_once = transient
assert identity(42)[2] == "123"
assert calls == 2
assert time.sleeps == [0.01]


def persistent(_pid):
    global calls
    calls += 1
    raise SystemExit(86)


calls = 0
time.sleeps = []
_identity_once = persistent
try:
    identity(42)
except SystemExit as exc:
    assert exc.code == 86
else:
    raise AssertionError("persistent ambiguity did not fail closed")
assert calls == 5
assert time.sleeps == [0.01, 0.01, 0.01, 0.01]
"""
    result = subprocess.run(
        ["python3", "-c", definitions + probe],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
        timeout=10,
    )
    assert result.returncode == 0, result.stderr.decode(errors="replace")


class _LocalShellBackend:
    supports_shell = True

    def __init__(self, home: Path, *, shared: bool = False) -> None:
        self._remote_root = str(home / "workspace")
        self._shared_workspace = shared

    def resolve_home_path(self, relative_path: str) -> str:
        return str(Path(self._remote_root).parent / relative_path)

    def execute_with_secret_stdin(
        self,
        command: str,
        secret: bytes | bytearray | str,
        *,
        timeout: int = 30,
    ) -> bool:
        payload = secret.encode() if isinstance(secret, str) else bytes(secret)
        return (
            subprocess.run(
                ["bash", "-c", command],
                input=payload,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
                timeout=timeout,
            ).returncode
            == 0
        )

    def exec_claim_resource(
        self, command: str, timeout: int = 30, *, operation: str = ""
    ) -> str:
        del operation
        result = _run(command)
        if result.returncode:
            raise RuntimeError(f"claim resource failed: {result.returncode}")
        return ""


@pytest.fixture
def short_home() -> Path:
    # Linux AF_UNIX paths are limited to roughly 108 bytes. Keep the real
    # ssh-agent socket root representative of the short in-container home.
    with tempfile.TemporaryDirectory(prefix="srw-ma-", dir="/tmp") as value:
        yield Path(value)


def test_real_agent_launch_reuse_generation_replace_and_retire(
    short_home: Path,
) -> None:
    tmp_path = short_home
    authority_id = str(uuid4())
    private_key, _public_key, _fingerprint = _deploy_keypair()

    first = _run(
        managed_repository_agent_launch_command(
            home_path=str(tmp_path), authority_id=authority_id, generation=1
        ),
        secret=private_key.encode(),
    )
    assert first.returncode == 0, first.stderr.decode(errors="replace")
    state_path, first_state = _state(tmp_path, authority_id)
    first_pid = int(first_state["pid"])
    assert _process_is_generation(first_pid, first_state["starttime"])
    assert Path(f"/proc/{first_pid}/comm").read_text().strip() == "ssh-agent"
    assert b"ssh-agent\0-a\0" in Path(f"/proc/{first_pid}/cmdline").read_bytes()
    assert private_key not in state_path.read_text()

    launch = managed_repository_agent_launch_command(
        home_path=str(tmp_path), authority_id=authority_id, generation=1
    )
    assert "ssh-agent -a " in launch
    assert " -s 9>&-" in launch

    reused = _run(
        managed_repository_agent_launch_command(
            home_path=str(tmp_path),
            authority_id=authority_id,
            generation=1,
            preserve_existing=True,
        ),
        secret=b"unused-on-exact-reuse",
    )
    assert reused.returncode == 0, reused.stderr.decode(errors="replace")
    assert int(_state(tmp_path, authority_id)[1]["pid"]) == first_pid

    replaced = _run(
        managed_repository_agent_launch_command(
            home_path=str(tmp_path),
            authority_id=authority_id,
            generation=2,
            preserve_existing=True,
        ),
        secret=private_key.encode(),
    )
    assert replaced.returncode == 0, replaced.stderr.decode(errors="replace")
    second_state = _state(tmp_path, authority_id)[1]
    second_pid = int(second_state["pid"])
    assert second_pid != first_pid
    assert second_state["generation"] == "2"
    assert not _process_is_generation(first_pid, first_state["starttime"])
    assert (
        _run(managed_repository_agent_zero_command(home_path=str(tmp_path))).returncode
        == 85
    )

    retired = _run(managed_repository_agent_retirement_command(home_path=str(tmp_path)))
    assert retired.returncode == 0, retired.stderr.decode(errors="replace")
    assert not _process_is_generation(second_pid, second_state["starttime"])
    assert (
        _run(managed_repository_agent_zero_command(home_path=str(tmp_path))).returncode
        == 0
    )


def test_repository_agent_does_not_inherit_ide_setup_lock(
    short_home: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Older OpenSSH retains inherited fds and would wedge the next IDE setup."""
    real_agent = shutil.which("ssh-agent")
    assert real_agent
    bin_dir = short_home / "bin"
    bin_dir.mkdir()
    observed = short_home / "inherited-locks.json"
    wrapper = bin_dir / "ssh-agent"
    wrapper.write_text(
        "#!/usr/bin/env python3\n"
        "import json, os, sys\n"
        "from pathlib import Path\n"
        "inherited = []\n"
        "for fd in (8, 9):\n"
        "    try:\n"
        "        os.fstat(fd)\n"
        "    except OSError:\n"
        "        continue\n"
        "    inherited.append(fd)\n"
        f"Path({str(observed)!r}).write_text(json.dumps(inherited))\n"
        f"os.execv({real_agent!r}, [{real_agent!r}, *sys.argv[1:]])\n"
    )
    wrapper.chmod(0o755)
    monkeypatch.setenv("PATH", f"{bin_dir}:{os.environ['PATH']}")
    authority_id = str(uuid4())
    key, _, _ = _deploy_keypair()
    launch = managed_repository_agent_launch_command(
        home_path=str(short_home), authority_id=authority_id, generation=1
    )
    command = (
        f"exec 8>{shlex.quote(str(short_home / 'setup.lock'))}; flock -x 8; " + launch
    )
    try:
        result = _run(command, secret=key.encode())
        assert result.returncode == 0, result.stderr.decode(errors="replace")
        assert json.loads(observed.read_text()) == []
    finally:
        retired = _run(
            managed_repository_agent_retirement_command(
                home_path=str(short_home), authority_ids=[authority_id]
            )
        )
        assert retired.returncode == 0
        assert (
            _run(
                managed_repository_agent_zero_command(home_path=str(short_home))
            ).returncode
            == 0
        )


def test_failed_key_or_proof_rolls_back_real_spawn(short_home: Path) -> None:
    tmp_path = short_home
    authority_id = str(uuid4())
    bad_key = _run(
        managed_repository_agent_launch_command(
            home_path=str(tmp_path), authority_id=authority_id, generation=1
        ),
        secret=b"not-a-private-key",
    )
    assert bad_key.returncode != 0
    assert (
        _run(managed_repository_agent_zero_command(home_path=str(tmp_path))).returncode
        == 0
    )
    assert not _state_path(tmp_path, authority_id).exists()

    private_key, _public_key, _fingerprint = _deploy_keypair()
    bad_proof = _run(
        managed_repository_agent_launch_command(
            home_path=str(tmp_path),
            authority_id=authority_id,
            generation=1,
            expected_fingerprint="SHA256:" + "A" * 43,
            probe_url="ssh://example.invalid/srw/nope.git",
        ),
        secret=private_key.encode(),
    )
    assert bad_proof.returncode != 0
    assert (
        _run(managed_repository_agent_zero_command(home_path=str(tmp_path))).returncode
        == 0
    )
    assert not _state_path(tmp_path, authority_id).exists()


def _state_path(home: Path, authority_id: str) -> Path:
    return (
        home
        / ".ssh"
        / "srw-managed"
        / "agents"
        / f"{authority_id.replace('-', '')}.state"
    )


def test_stale_receipt_and_socket_converge_before_relaunch(short_home: Path) -> None:
    tmp_path = short_home
    authority_id = str(uuid4())
    private_key, _public_key, _fingerprint = _deploy_keypair()
    command = managed_repository_agent_launch_command(
        home_path=str(tmp_path), authority_id=authority_id, generation=1
    )
    assert _run(command, secret=private_key.encode()).returncode == 0
    _path, stale = _state(tmp_path, authority_id)
    stale_pid = int(stale["pid"])
    _stop(stale_pid)

    relaunched = _run(
        managed_repository_agent_launch_command(
            home_path=str(tmp_path),
            authority_id=authority_id,
            generation=1,
            preserve_existing=True,
        ),
        secret=private_key.encode(),
    )
    assert relaunched.returncode == 0, relaunched.stderr.decode(errors="replace")
    current = _state(tmp_path, authority_id)[1]
    assert int(current["pid"]) != stale_pid
    assert (
        _run(
            managed_repository_agent_retirement_command(home_path=str(tmp_path))
        ).returncode
        == 0
    )


def test_same_generation_invalid_key_state_self_heals_but_forge_outage_preserves(
    short_home: Path,
) -> None:
    authority_id = str(uuid4())
    private_key, _public_key, fingerprint = _deploy_keypair()
    other_key, _other_public, _other_fingerprint = _deploy_keypair()
    bare_repo = short_home / "probe.git"
    subprocess.run(["git", "init", "--bare", str(bare_repo)], check=True)
    launch = lambda: managed_repository_agent_launch_command(  # noqa: E731
        home_path=str(short_home),
        authority_id=authority_id,
        generation=1,
        preserve_existing=True,
        expected_fingerprint=fingerprint,
        probe_url=str(bare_repo),
    )
    assert _run(launch(), secret=private_key.encode()).returncode == 0
    first = _state(short_home, authority_id)[1]
    socket_path = first["socket"]

    # A removed key is local resident corruption. The exact authority is
    # replaced and loaded from the supplied expected key under one lock.
    subprocess.run(
        ["ssh-add", "-D"],
        env={**os.environ, "SSH_AUTH_SOCK": socket_path},
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    healed = _run(launch(), secret=private_key.encode())
    assert healed.returncode == 0, healed.stderr.decode(errors="replace")
    second = _state(short_home, authority_id)[1]
    assert second["pid"] != first["pid"]

    # An extra identity violates the dedicated-agent invariant and converges
    # the same way; it is never accepted merely because the expected key exists.
    subprocess.run(
        ["ssh-add", "-"],
        input=other_key.encode(),
        env={**os.environ, "SSH_AUTH_SOCK": second["socket"]},
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    assert _run(launch(), secret=private_key.encode()).returncode == 0
    third = _state(short_home, authority_id)[1]
    assert third["pid"] != second["pid"]

    # The remote forge probe is deliberately outside local self-heal. A
    # transient outage fails this attach but preserves the proven resident.
    unavailable = managed_repository_agent_launch_command(
        home_path=str(short_home),
        authority_id=authority_id,
        generation=1,
        preserve_existing=True,
        expected_fingerprint=fingerprint,
        probe_url=str(short_home / "missing.git"),
    )
    assert _run(unavailable, secret=private_key.encode()).returncode != 0
    assert _state(short_home, authority_id)[1]["pid"] == third["pid"]
    assert (
        _run(
            managed_repository_agent_retirement_command(home_path=str(short_home))
        ).returncode
        == 0
    )


def test_prior_runtime_receipt_does_not_confuse_successor_pid_reuse(
    short_home: Path,
) -> None:
    authority_id = str(uuid4())
    slug = authority_id.replace("-", "")
    socket_path = short_home / ".ssh" / "srw-managed" / "sockets" / f"{slug}.sock"
    state_path = _state_path(short_home, authority_id)
    socket_path.parent.mkdir(parents=True)
    state_path.parent.mkdir(parents=True)
    unrelated = subprocess.Popen(["sleep", "30"])
    try:
        unrelated_start = (
            Path(f"/proc/{unrelated.pid}/stat")
            .read_text()
            .rsplit(")", 1)[1]
            .split()[19]
        )
        state_path.write_text(
            _receipt_lines(
                slug=slug,
                generation=1,
                pid=unrelated.pid,
                starttime=unrelated_start,
                socket_path=socket_path,
                runtime_scope="00000000-0000-0000-0000-000000000001:1",
            )
        )
        state_path.chmod(0o600)
        private_key, _public_key, _fingerprint = _deploy_keypair()
        launched = _run(
            managed_repository_agent_launch_command(
                home_path=str(short_home),
                authority_id=authority_id,
                generation=1,
                preserve_existing=True,
            ),
            secret=private_key.encode(),
        )
        assert launched.returncode == 0, launched.stderr.decode(errors="replace")
        current = _state(short_home, authority_id)[1]
        assert int(current["pid"]) != unrelated.pid
        assert unrelated.poll() is None
        assert current["runtime_scope"] == _runtime_scope()
        assert (
            _run(
                managed_repository_agent_retirement_command(home_path=str(short_home))
            ).returncode
            == 0
        )
    finally:
        unrelated.terminate()
        unrelated.wait(timeout=5)


def test_server_runtime_incarnation_change_replaces_exact_resident(
    short_home: Path,
) -> None:
    authority_id = str(uuid4())
    workspace_generation = str(uuid4())
    first_runtime = str(uuid4())
    second_runtime = str(uuid4())
    private_key, _public_key, _fingerprint = _deploy_keypair()
    first = _run(
        managed_repository_agent_launch_command(
            home_path=str(short_home),
            authority_id=authority_id,
            generation=1,
            preserve_existing=True,
            workspace_generation=workspace_generation,
            runtime_incarnation=first_runtime,
        ),
        secret=private_key.encode(),
    )
    assert first.returncode == 0, first.stderr.decode(errors="replace")
    first_state = _state(short_home, authority_id)[1]

    second = _run(
        managed_repository_agent_launch_command(
            home_path=str(short_home),
            authority_id=authority_id,
            generation=1,
            preserve_existing=True,
            workspace_generation=workspace_generation,
            runtime_incarnation=second_runtime,
        ),
        secret=private_key.encode(),
    )
    assert second.returncode == 0, second.stderr.decode(errors="replace")
    second_state = _state(short_home, authority_id)[1]
    assert second_state["pid"] != first_state["pid"]
    assert second_state["workspace_generation"] == workspace_generation
    assert second_state["runtime_incarnation"] == second_runtime
    assert (
        _run(
            managed_repository_agent_retirement_command(home_path=str(short_home))
        ).returncode
        == 0
    )


def test_delayed_lower_generation_cannot_downgrade_live_or_dead_successor(
    short_home: Path,
) -> None:
    authority_id = str(uuid4())
    current_key, _current_public, _current_fingerprint = _deploy_keypair()
    stale_key, _stale_public, _stale_fingerprint = _deploy_keypair()
    current_config = "Host current-generation\n"
    stale_config = "Host stale-generation\n"
    current = managed_repository_agent_launch_command(
        home_path=str(short_home),
        authority_id=authority_id,
        generation=2,
        preserve_existing=True,
        config_content=current_config,
    )
    assert _run(current, secret=current_key.encode()).returncode == 0
    state_path, current_state = _state(short_home, authority_id)
    config_path = (
        short_home
        / ".ssh"
        / "srw-managed"
        / "config.d"
        / f"{authority_id.replace('-', '')}.conf"
    )

    stale = managed_repository_agent_launch_command(
        home_path=str(short_home),
        authority_id=authority_id,
        generation=1,
        preserve_existing=True,
        config_content=stale_config,
    )
    refused_live = _run(stale, secret=stale_key.encode())
    assert refused_live.returncode == 5
    assert config_path.read_text() == current_config
    assert _state(short_home, authority_id)[1] == current_state
    forced_stale = managed_repository_agent_launch_command(
        home_path=str(short_home),
        authority_id=authority_id,
        generation=1,
        preserve_existing=False,
        config_content=stale_config,
    )
    assert _run(forced_stale, secret=stale_key.encode()).returncode == 5
    assert config_path.read_text() == current_config
    assert _state(short_home, authority_id)[1] == current_state

    # Lost process after N+1 does not make its durable generation disappear.
    _stop(int(current_state["pid"]))
    refused_dead = _run(stale, secret=stale_key.encode())
    assert refused_dead.returncode == 5
    assert config_path.read_text() == current_config
    assert state_path.exists()
    assert _state(short_home, authority_id)[1] == current_state

    relaunched = _run(current, secret=current_key.encode())
    assert relaunched.returncode == 0, relaunched.stderr.decode(errors="replace")
    assert int(_state(short_home, authority_id)[1]["pid"]) != int(current_state["pid"])
    assert (
        _run(
            managed_repository_agent_retirement_command(home_path=str(short_home))
        ).returncode
        == 0
    )


def test_concurrent_generations_converge_monotonically(short_home: Path) -> None:
    authority_id = str(uuid4())
    first_key, _first_public, _first_fingerprint = _deploy_keypair()
    second_key, _second_public, _second_fingerprint = _deploy_keypair()
    assert (
        _run(
            managed_repository_agent_launch_command(
                home_path=str(short_home),
                authority_id=authority_id,
                generation=1,
                preserve_existing=True,
                config_content="Host generation-one\n",
            ),
            secret=first_key.encode(),
        ).returncode
        == 0
    )

    def invoke(generation: int) -> int:
        return _run(
            managed_repository_agent_launch_command(
                home_path=str(short_home),
                authority_id=authority_id,
                generation=generation,
                preserve_existing=True,
                config_content=f"Host generation-{generation}\n",
            ),
            secret=(second_key if generation == 2 else first_key).encode(),
        ).returncode

    with ThreadPoolExecutor(max_workers=2) as pool:
        higher = pool.submit(invoke, 2)
        delayed_lower = pool.submit(invoke, 1)
        results = {2: higher.result(timeout=20), 1: delayed_lower.result(timeout=20)}
    assert results[2] == 0
    assert results[1] in {0, 5}
    final = _state(short_home, authority_id)[1]
    assert final["generation"] == "2"
    config_path = (
        short_home
        / ".ssh"
        / "srw-managed"
        / "config.d"
        / f"{authority_id.replace('-', '')}.conf"
    )
    assert config_path.read_text() == "Host generation-2\n"
    assert (
        _run(
            managed_repository_agent_retirement_command(home_path=str(short_home))
        ).returncode
        == 0
    )


def test_trusted_new_runtime_discards_old_receipt_before_reused_pid_check(
    short_home: Path,
) -> None:
    authority_id = str(uuid4())
    slug = authority_id.replace("-", "")
    workspace_generation = str(uuid4())
    old_runtime = str(uuid4())
    new_runtime = str(uuid4())
    socket_path = short_home / ".ssh" / "srw-managed" / "sockets" / f"{slug}.sock"
    state_path = _state_path(short_home, authority_id)
    socket_path.parent.mkdir(parents=True)
    state_path.parent.mkdir(parents=True)
    unrelated = subprocess.Popen(["sleep", "30"])
    try:
        unrelated_start = (
            Path(f"/proc/{unrelated.pid}/stat")
            .read_text()
            .rsplit(")", 1)[1]
            .split()[19]
        )
        state_path.write_text(
            _receipt_lines(
                slug=slug,
                generation=1,
                pid=unrelated.pid,
                starttime=unrelated_start,
                socket_path=socket_path,
                workspace_generation=workspace_generation,
                runtime_incarnation=old_runtime,
            )
        )
        state_path.chmod(0o600)
        private_key, _public_key, _fingerprint = _deploy_keypair()
        successor = _run(
            managed_repository_agent_launch_command(
                home_path=str(short_home),
                authority_id=authority_id,
                generation=1,
                preserve_existing=True,
                workspace_generation=workspace_generation,
                runtime_incarnation=new_runtime,
            ),
            secret=private_key.encode(),
        )
        assert successor.returncode == 0, successor.stderr.decode(errors="replace")
        assert unrelated.poll() is None
        assert _state(short_home, authority_id)[1]["runtime_incarnation"] == new_runtime
        assert (
            _run(
                managed_repository_agent_retirement_command(home_path=str(short_home))
            ).returncode
            == 0
        )
    finally:
        unrelated.terminate()
        unrelated.wait(timeout=5)


def test_receipt_pid_reuse_or_extra_process_fails_closed(short_home: Path) -> None:
    tmp_path = short_home
    authority_id = str(uuid4())
    slug = authority_id.replace("-", "")
    socket_path = tmp_path / ".ssh" / "srw-managed" / "sockets" / f"{slug}.sock"
    state_path = _state_path(tmp_path, authority_id)
    state_path.parent.mkdir(parents=True)
    socket_path.parent.mkdir(parents=True)

    foreign = subprocess.Popen(["sleep", "30"])
    try:
        foreign_start = (
            Path(f"/proc/{foreign.pid}/stat").read_text().rsplit(")", 1)[1].split()[19]
        )
        state_path.write_text(
            _receipt_lines(
                slug=slug,
                generation=1,
                pid=foreign.pid,
                starttime=foreign_start,
                socket_path=socket_path,
            )
        )
        state_path.chmod(0o600)
        refused = _run(
            managed_repository_agent_retirement_command(home_path=str(tmp_path))
        )
        assert refused.returncode == 86
        assert foreign.poll() is None
    finally:
        foreign.terminate()
        foreign.wait(timeout=5)


def test_two_exact_processes_on_one_socket_are_ambiguous(short_home: Path) -> None:
    authority_id = str(uuid4())
    private_key, _public_key, _fingerprint = _deploy_keypair()
    assert (
        _run(
            managed_repository_agent_launch_command(
                home_path=str(short_home), authority_id=authority_id, generation=1
            ),
            secret=private_key.encode(),
        ).returncode
        == 0
    )
    _path, receipt = _state(short_home, authority_id)
    first_pid = int(receipt["pid"])
    socket_path = Path(receipt["socket"])
    socket_path.unlink()
    output = subprocess.check_output(["ssh-agent", "-a", str(socket_path), "-s"])
    second_pid = int(
        next(
            line.split("=", 1)[1].split(";", 1)[0]
            for line in output.decode().splitlines()
            if line.startswith("SSH_AGENT_PID=")
        )
    )
    try:
        refused = _run(
            managed_repository_agent_retirement_command(home_path=str(short_home))
        )
        assert refused.returncode == 86
        assert Path(f"/proc/{first_pid}").exists()
        assert Path(f"/proc/{second_pid}").exists()
    finally:
        _stop(first_pid)
        _stop(second_pid)
        settled = _run(
            managed_repository_agent_retirement_command(home_path=str(short_home))
        )
        assert settled.returncode == 0


def test_exact_receipt_zombie_is_process_zero_and_retryable(short_home: Path) -> None:
    authority_id = str(uuid4())
    slug = authority_id.replace("-", "")
    socket_path = short_home / ".ssh" / "srw-managed" / "sockets" / f"{slug}.sock"
    state_path = _state_path(short_home, authority_id)
    state_path.parent.mkdir(parents=True)
    socket_path.parent.mkdir(parents=True)
    zombie = subprocess.Popen(["true"])
    try:
        deadline = time.monotonic() + 2
        observed: list[str] = []
        while time.monotonic() < deadline:
            observed = (
                Path(f"/proc/{zombie.pid}/stat").read_text().rsplit(")", 1)[1].split()
            )
            if observed[0] == "Z":
                break
            time.sleep(0.01)
        assert observed[0] == "Z"
        state_path.write_text(
            _receipt_lines(
                slug=slug,
                generation=1,
                pid=zombie.pid,
                starttime=observed[19],
                socket_path=socket_path,
            )
        )
        state_path.chmod(0o600)
        settled = _run(
            managed_repository_agent_retirement_command(home_path=str(short_home))
        )
        assert settled.returncode == 0, settled.stderr.decode(errors="replace")
        assert not state_path.exists()
        assert (
            _run(
                managed_repository_agent_zero_command(home_path=str(short_home))
            ).returncode
            == 0
        )
    finally:
        zombie.wait(timeout=5)


def test_private_namespace_legacy_adoption_requires_exact_config(
    short_home: Path,
) -> None:
    tmp_path = short_home
    authority_id = str(uuid4())
    slug = authority_id.replace("-", "")
    root = tmp_path / ".ssh" / "srw-managed"
    socket_path = root / "sockets" / f"{slug}.sock"
    config_path = root / "config.d" / f"{slug}.conf"
    socket_path.parent.mkdir(parents=True)
    config_path.parent.mkdir(parents=True)
    config_path.write_text(f"Host srw-repo-{slug}\n  IdentityAgent {socket_path}\n")
    config_path.chmod(0o600)
    output = subprocess.check_output(["ssh-agent", "-a", str(socket_path), "-s"])
    pid = int(
        next(
            line.split("=", 1)[1].split(";", 1)[0]
            for line in output.decode().splitlines()
            if line.startswith("SSH_AGENT_PID=")
        )
    )
    try:
        assert (
            _run(
                managed_repository_agent_retirement_command(home_path=str(tmp_path))
            ).returncode
            == 0
        )
        assert not Path(f"/proc/{pid}").exists()
    finally:
        _stop(pid)


def test_root_and_shared_handoffs_preserve_sibling_until_terminal_teardown(
    short_home: Path,
) -> None:
    tmp_path = short_home
    authority_id = str(uuid4())
    private_key, _public_key, _fingerprint = _deploy_keypair()
    assert (
        _run(
            managed_repository_agent_launch_command(
                home_path=str(tmp_path), authority_id=authority_id, generation=1
            ),
            secret=private_key.encode(),
        ).returncode
        == 0
    )
    _path, state = _state(tmp_path, authority_id)
    pid = int(state["pid"])

    shared_child = _LocalShellBackend(tmp_path, shared=True)
    materialize_managed_repository_credentials([], shared_child)
    assert _process_is_generation(pid, state["starttime"])

    fresh_owner = _LocalShellBackend(tmp_path)
    materialize_managed_repository_credentials([], fresh_owner)
    assert _process_is_generation(pid, state["starttime"])
    # Only exact terminal workspace authority performs namespace-wide cleanup.
    assert (
        _run(
            managed_repository_agent_retirement_command(home_path=str(tmp_path))
        ).returncode
        == 0
    )
    assert not _process_is_generation(pid, state["starttime"])
    assert (
        _run(managed_repository_agent_zero_command(home_path=str(tmp_path))).returncode
        == 0
    )


def _terminal_thread() -> dict:
    generation = str(uuid4())
    runtime = str(uuid4())
    return {
        "id": str(uuid4()),
        "status": "ended",
        "metadata": {
            "workspace_container": {
                "status": "ready",
                "provisioner": "k8s",
                "pod_ip": "10.0.0.8",
                "port": 30022,
                "_canvas_workspace_generation": generation,
                "_runtime_incarnation": runtime,
            },
            "_workspace_binding": {
                "kind": "remote",
                "generation": generation,
                "backing_id": f"k8s-pod:agent-workspaces:{runtime}",
                "ssh_host_key_fingerprint": "SHA256:" + "A" * 43,
            },
        },
    }


def _retiring_terminal_thread() -> dict:
    thread = _terminal_thread()
    metadata = thread["metadata"]
    workspace = metadata["workspace_container"]
    binding = metadata["_workspace_binding"]
    workspace["status"] = "retiring_process_zero"
    metadata["_stateless_workspace_retirement_pending"] = True
    metadata["_stateless_claim_retirement"] = {
        "terminal_token": 9,
        "claimant_quiesced": True,
        "shell_retirement_required": True,
        "resident_cleanup_required": True,
        "residents_retired": False,
        "remote_retired": False,
        "permanent": True,
        "workspace_absence_proven": False,
        "workspace_generation": binding["generation"],
        "endpoint_generation": workspace["_canvas_workspace_generation"],
        "runtime_incarnation": workspace["_runtime_incarnation"],
        "host_key_fingerprint": binding["ssh_host_key_fingerprint"],
    }
    return thread


def test_terminal_endpoint_accepts_exact_process_zero_retirement_fence() -> None:
    thread = _retiring_terminal_thread()

    authority = resolve_shell_retirement_authority(thread, terminal_token=9)

    assert authority.thread_id == thread["id"]
    assert (
        authority.runtime_incarnation
        == thread["metadata"]["workspace_container"]["_runtime_incarnation"]
    )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("terminal_token", 10),
        ("workspace_generation", "11111111-1111-4111-8111-111111111111"),
        ("endpoint_generation", "22222222-2222-4222-8222-222222222222"),
        ("runtime_incarnation", "33333333-3333-4333-8333-333333333333"),
        ("host_key_fingerprint", "SHA256:" + "B" * 43),
    ],
)
def test_terminal_endpoint_rejects_drifted_process_zero_authority(
    field: str,
    value: object,
) -> None:
    thread = _retiring_terminal_thread()
    thread["metadata"]["_stateless_claim_retirement"][field] = value

    with pytest.raises(ShellRetirementUnavailable):
        resolve_shell_retirement_authority(thread, terminal_token=9)


def test_terminal_endpoint_rejects_unowned_process_zero_state() -> None:
    thread = _retiring_terminal_thread()
    thread["metadata"].pop("_stateless_workspace_retirement_pending")
    thread["metadata"].pop("_stateless_claim_retirement")

    with pytest.raises(ShellRetirementUnavailable):
        resolve_shell_retirement_authority(thread, terminal_token=9)


@pytest.mark.asyncio
async def test_terminal_end_retires_then_reproves_all_credential_agents() -> None:
    backend = MagicMock()
    backend.exec_terminal_claim_resource.return_value = ""
    backend.verify_terminal_claim_resources_retired.return_value = ""
    thread = _terminal_thread()
    with patch(
        "orchestrator.services.stateless_session_retirement._build_terminal_backend",
        return_value=backend,
    ):
        await retire_stateless_workspace_residents(
            thread, terminal_token=9, cloud_mount_cfg=None
        )
        await verify_stateless_workspace_residents_retired(
            thread, terminal_token=9, cloud_mount_cfg=None
        )

    commands = [
        call.args[0] for call in backend.exec_terminal_claim_resource.call_args_list
    ]
    assert " all " in commands[0]
    assert "/home/agent-host/.ssh/srw-managed/sockets" in commands[0]
    assert " zero " in commands[-1]
    verify_command = backend.verify_terminal_claim_resources_retired.call_args.args[0]
    assert " zero " in verify_command
    assert "ssh-agent" in verify_command


# ---------------------------------------------------------------------------
# Shared runtime for connector identities (connector drivers C1)
# ---------------------------------------------------------------------------


def _agent_key_count(socket_path: str) -> int:
    listed = subprocess.run(
        ["ssh-add", "-l"],
        env={**os.environ, "SSH_AUTH_SOCK": socket_path},
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    if listed.returncode == 1:
        return 0
    assert listed.returncode == 0
    return len([line for line in listed.stdout.decode().splitlines() if line])


def test_fingerprint_proof_without_forge_probe(short_home: Path) -> None:
    """An identity with no repository proves its key locally and nothing else."""

    authority_id = str(uuid4())
    private_key, _public_key, fingerprint = _deploy_keypair()
    launched = _run(
        managed_repository_agent_launch_command(
            home_path=str(short_home),
            authority_id=authority_id,
            generation=1,
            preserve_existing=True,
            expected_fingerprint=fingerprint,
        ),
        secret=private_key.encode(),
    )
    assert launched.returncode == 0, launched.stderr.decode(errors="replace")
    state = _state(short_home, authority_id)[1]
    assert _agent_key_count(state["socket"]) == 1

    other_key = _deploy_keypair()[0]
    wrong = _run(
        managed_repository_agent_launch_command(
            home_path=str(short_home),
            authority_id=str(uuid4()),
            generation=1,
            expected_fingerprint=fingerprint,
        ),
        secret=other_key.encode(),
    )
    assert wrong.returncode != 0
    with pytest.raises(Exception):
        managed_repository_agent_launch_command(
            home_path=str(short_home),
            authority_id=authority_id,
            generation=1,
            probe_url="ssh://srw-repo-x/o/r.git",
        )
    assert (
        _run(
            managed_repository_agent_retirement_command(home_path=str(short_home))
        ).returncode
        == 0
    )
    assert (
        _run(
            managed_repository_agent_zero_command(home_path=str(short_home))
        ).returncode
        == 0
    )


def test_key_lifetime_expires_and_the_next_materialization_self_heals(
    short_home: Path,
) -> None:
    authority_id = str(uuid4())
    private_key, _public_key, fingerprint = _deploy_keypair()

    def launch(lifetime: int) -> subprocess.CompletedProcess[bytes]:
        return _run(
            managed_repository_agent_launch_command(
                home_path=str(short_home),
                authority_id=authority_id,
                generation=1,
                preserve_existing=True,
                expected_fingerprint=fingerprint,
                key_lifetime_seconds=lifetime,
            ),
            secret=private_key.encode(),
        )

    assert "ssh-add -t 3 -" in managed_repository_agent_launch_command(
        home_path=str(short_home),
        authority_id=authority_id,
        generation=1,
        key_lifetime_seconds=3,
    )
    lifetime = 5

    def sleep_until(deadline: float) -> None:
        time.sleep(max(0.0, deadline - time.monotonic()))

    assert launch(lifetime).returncode == 0
    loaded_by = time.monotonic()
    first = _state(short_home, authority_id)[1]
    # A proven reuse re-adds the key, restarting its lifetime.
    sleep_until(loaded_by + 2)
    refresh_started = time.monotonic()
    assert launch(lifetime).returncode == 0
    refreshed_by = time.monotonic()
    assert _state(short_home, authority_id)[1]["pid"] == first["pid"]
    # Past the first load's expiry, inside the refreshed one.
    sleep_until(loaded_by + lifetime + 0.5)
    assert time.monotonic() < refresh_started + lifetime
    assert _agent_key_count(first["socket"]) == 1
    # Unrefreshed, the agent forgets the key on its own ...
    sleep_until(refreshed_by + lifetime + 0.5)
    assert _agent_key_count(first["socket"]) == 0
    # ... and the next materialization replaces the keyless resident.
    assert launch(lifetime).returncode == 0
    healed = _state(short_home, authority_id)[1]
    assert healed["pid"] != first["pid"]
    assert _agent_key_count(healed["socket"]) == 1
    assert (
        _run(
            managed_repository_agent_retirement_command(home_path=str(short_home))
        ).returncode
        == 0
    )


def test_rendered_config_carries_the_legacy_adoption_lines(short_home: Path) -> None:
    """A crash between spawn and receipt must not wedge terminal teardown."""

    authority_id = str(uuid4())
    slug = authority_id.replace("-", "")
    root = short_home / ".ssh" / "srw-managed"
    socket_path = root / "sockets" / f"{slug}.sock"
    config = render_ssh_identity_config(
        alias=f"srw-repo-{slug}",
        socket_path=str(socket_path),
        known_hosts_path=str(root / "known_hosts.d" / slug),
        host="github.com",
        port=22,
        user="git",
        strict_host_key_checking=True,
        host_key_alias=f"srw-repo-{slug}",
        extra_hosts=["bastion.example.com"],
    )
    lines = config.splitlines()
    assert lines[:2] == [f"Host srw-repo-{slug}", f"  IdentityAgent {socket_path}"]
    assert "  StrictHostKeyChecking yes" in lines
    assert lines.count(f"  IdentityAgent {socket_path}") == 2

    assert (
        _run(managed_ssh_namespace_setup_command(home_path=str(short_home))).returncode
        == 0
    )
    config_path = root / "config.d" / f"{slug}.conf"
    config_path.write_text(config)
    config_path.chmod(0o600)
    output = subprocess.check_output(["ssh-agent", "-a", str(socket_path), "-s"])
    pid = int(
        next(
            line.split("=", 1)[1].split(";", 1)[0]
            for line in output.decode().splitlines()
            if line.startswith("SSH_AGENT_PID=")
        )
    )
    try:
        assert (
            _run(
                managed_repository_agent_retirement_command(home_path=str(short_home))
            ).returncode
            == 0
        )
        assert not Path(f"/proc/{pid}").exists()
    finally:
        _stop(pid)


@pytest.mark.parametrize(
    "value",
    ["git\n  ProxyCommand sh -c id", "a b", "%d", "#x"],
)
def test_rendered_config_refuses_values_that_start_a_directive(value: str) -> None:
    slug = uuid4().hex
    with pytest.raises(Exception):
        render_ssh_identity_config(
            alias=f"srw-repo-{slug}",
            socket_path=f"/home/agent-host/.ssh/srw-managed/sockets/{slug}.sock",
            known_hosts_path="/home/agent-host/.ssh/srw-managed/known_hosts",
            host="github.com",
            user=value,
        )


def test_include_behind_match_all_survives_a_trailing_host_block(
    short_home: Path,
) -> None:
    slug = uuid4().hex
    root = short_home / ".ssh" / "srw-managed"
    socket_path = root / "sockets" / f"{slug}.sock"
    user_config = short_home / ".ssh" / "config"
    user_config.parent.mkdir(parents=True)
    # What a pre-C1 connector clone (or a user) leaves at the end of the file,
    # followed by the legacy bare Include that therefore only applies to it.
    user_config.write_text(
        "Host github.com\n  IdentityFile ~/.ssh/repo_widget\n"
        f"Include {root}/config.d/*.conf\n"
    )
    (root / "config.d").mkdir(parents=True)
    (root / "config.d" / f"{slug}.conf").write_text(
        render_ssh_identity_config(
            alias=f"srw-repo-{slug}",
            socket_path=str(socket_path),
            known_hosts_path=str(root / "known_hosts"),
            host="github.com",
            port=22,
            user="git",
        )
    )

    def resolved_agent() -> str:
        evaluated = subprocess.run(
            ["ssh", "-G", "-F", str(user_config), f"srw-repo-{slug}"],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            check=True,
        ).stdout.decode()
        return next(
            (
                line.split(" ", 1)[1]
                for line in evaluated.splitlines()
                if line.startswith("identityagent ")
            ),
            "",
        )

    # The bare Include sits inside the trailing Host block: the alias is dead.
    assert resolved_agent() == ""
    assert (
        _run(managed_ssh_namespace_setup_command(home_path=str(short_home))).returncode
        == 0
    )
    assert resolved_agent() == str(socket_path)
    # Idempotent: a second setup appends nothing.
    before = user_config.read_text()
    assert before.count("Match all") == 1
    assert (
        _run(managed_ssh_config_include_command(home_path=str(short_home))).returncode
        == 0
    )
    assert user_config.read_text() == before
    assert oct(user_config.stat().st_mode & 0o777) == "0o600"
