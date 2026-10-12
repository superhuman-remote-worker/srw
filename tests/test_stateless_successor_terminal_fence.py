"""Terminal resident cleanup on a successor that reuses its predecessor's PVC.

A soft End retires runtime A and keeps the PVC; ``shell_cleanup`` leaves A's
``retired`` ownership tombstone in the PVC-resident ``$HOME/.srw/tmux``.  Resume
starts runtime B on the same volume.  Until a turn creates B's tmux session the
tombstone still names A, so the first terminal cleanup for B (End or permanent
Delete right after Resume) meets a foreign tombstone and no tmux session.

These tests run the generated, flock-wrapped shell for real (bash + flock)
against a stateful fake tmux, so each decision is proven where it executes.
The workspace-wide process-zero scan is replaced by a no-op: it inspects this
host's ``/proc`` rather than the fenced state, and it has its own tests.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

import pytest

from shared.runtime.core.backends.remote import RemoteBackend
from shared.runtime.core.workspace_backend import WorkspaceUnavailableError


_THREAD_ID = "5e7a1c2d-3b4f-4a6e-8c9d-0e1f2a3b4c5d"
# Token (75) or incarnation (80) fence: either refusal leaves the record intact.
_FENCE_REFUSAL = r"exit code (75|80)$"

_FAKE_TMUX = r"""#!/usr/bin/env python3
import os
import sys

state = os.environ["FAKE_TMUX_STATE"]
args = sys.argv[1:]
session_file = os.path.join(state, "session")


def option_path(name):
    return os.path.join(state, "opt_" + name.lstrip("@"))


if not args:
    sys.exit(0)
cmd, rest = args[0], args[1:]
if cmd == "has-session":
    sys.exit(0 if os.path.exists(session_file) else 1)
if cmd == "new-session":
    open(session_file, "w").close()
    sys.exit(0)
if cmd == "kill-session":
    if not os.path.exists(session_file):
        sys.exit(1)
    os.remove(session_file)
    for name in os.listdir(state):
        if name.startswith("opt_") or name.startswith("env_"):
            os.remove(os.path.join(state, name))
    sys.exit(0)
if not os.path.exists(session_file):
    sys.exit(1)
if cmd == "display-message":
    fmt = rest[-1]
    name = fmt[fmt.index("{") + 1 : fmt.rindex("}")]
    try:
        with open(option_path(name)) as fh:
            print(fh.read())
    except FileNotFoundError:
        print("")
    sys.exit(0)
if cmd == "set-option":
    positional = [a for a in rest if not a.startswith("-")]
    # drop the value following -t
    if "-t" in rest:
        positional.remove(rest[rest.index("-t") + 1])
    with open(option_path(positional[0]), "w") as fh:
        fh.write(positional[1] if len(positional) > 1 else "")
    sys.exit(0)
if cmd == "set-environment":
    positional = [a for a in rest if not a.startswith("-")]
    if "-t" in rest:
        positional.remove(rest[rest.index("-t") + 1])
    with open(os.path.join(state, "env_" + positional[0]), "w") as fh:
        fh.write(positional[1])
    sys.exit(0)
if cmd == "show-environment":
    name = rest[-1]
    try:
        with open(os.path.join(state, "env_" + name)) as fh:
            print(name + "=" + fh.read())
    except FileNotFoundError:
        sys.exit(1)
    sys.exit(0)
sys.exit(0)
"""


class _Workspace:
    """One PVC-resident ``$HOME`` shared by successive runtimes."""

    def __init__(self, tmp_path: Path) -> None:
        self.home = tmp_path / "home"
        self.home.mkdir()
        fake_bin = tmp_path / "bin"
        fake_bin.mkdir()
        tmux = fake_bin / "tmux"
        tmux.write_text(_FAKE_TMUX)
        tmux.chmod(0o755)
        # A tmux server belongs to one Pod: each runtime gets its own state.
        self._tmux_root = tmp_path / "tmux"
        self._tmux_root.mkdir()
        self._path = f"{fake_bin}:{os.environ['PATH']}"

    def tmux_state(self, runtime: str) -> Path:
        state = self._tmux_root / runtime
        state.mkdir(exist_ok=True)
        return state

    def runner(self, runtime: str):
        env = dict(os.environ)
        env["HOME"] = str(self.home)
        env["PATH"] = self._path
        env["FAKE_TMUX_STATE"] = str(self.tmux_state(runtime))

        def run(command, timeout=30, *, retain_tail=False):
            completed = subprocess.run(
                ["bash", "-c", command],
                env=env,
                text=True,
                capture_output=True,
                timeout=15,
                check=False,
            )
            assert completed.returncode != 127, completed.stderr
            return completed.stdout, completed.returncode

        return run

    def tombstone(self, backend: RemoteBackend) -> list[str]:
        path = self.home / ".srw" / "tmux" / backend._tmux_state_filename
        return path.read_text().strip().split("|")


def _runtime_backend(
    *, token: int, workspace_generation: str, runtime_incarnation: str
) -> RemoteBackend:
    backend = RemoteBackend(
        host="workspace.test",
        workspace_path="/home/agent-host/workspace",
        job_id=_THREAD_ID,
        workspace_generation=workspace_generation,
        runtime_incarnation=runtime_incarnation,
    )
    backend.set_shell_owner_token(token)
    return backend


def _run(workspace: _Workspace, backend: RemoteBackend, runtime: str, action):
    with (
        patch.object(
            backend, "_exec_with_status", side_effect=workspace.runner(runtime)
        ),
        patch.object(backend, "_ensure_connected"),
        patch.object(
            backend,
            "_stateless_terminal_process_zero_shell",
            return_value=":\n",
        ),
    ):
        return action()


def _retired_predecessor(workspace: _Workspace, *, token: int):
    """Runtime A's soft End: the ordinary strict shell retirement."""

    generation, runtime = str(uuid4()), str(uuid4())
    predecessor = _runtime_backend(
        token=token, workspace_generation=generation, runtime_incarnation=runtime
    )
    _run(workspace, predecessor, runtime, predecessor.shell_cleanup)
    fields = workspace.tombstone(predecessor)
    assert fields[2:6] == [generation, runtime, "retired", str(token)]
    return predecessor, generation, runtime


class TestSuccessorTerminalCleanupAfterResume:
    def test_first_terminal_cleanup_restamps_the_predecessor_tombstone(self, tmp_path):
        """End/Delete right after Resume must be able to drain runtime B."""

        workspace = _Workspace(tmp_path)
        _retired_predecessor(workspace, token=5)
        generation, runtime = str(uuid4()), str(uuid4())
        successor = _runtime_backend(
            token=9, workspace_generation=generation, runtime_incarnation=runtime
        )

        output = _run(
            workspace,
            successor,
            runtime,
            lambda: successor.exec_terminal_claim_resource(
                "printf __SRW_RESIDENT_CLEANUP__", 30, operation="resident cleanup"
            ),
        )

        assert output == "__SRW_RESIDENT_CLEANUP__"
        fields = workspace.tombstone(successor)
        assert fields[2:6] == [generation, runtime, "active", "9"]

        # The ordinary shell retirement and post-kill proof then settle B.
        _run(workspace, successor, runtime, successor.shell_cleanup)
        assert workspace.tombstone(successor)[2:6] == [
            generation,
            runtime,
            "retired",
            "9",
        ]
        _run(
            workspace,
            successor,
            runtime,
            lambda: successor.verify_terminal_claim_resources_retired(":", 30),
        )

    def test_a_live_same_name_tmux_behind_a_foreign_tombstone_still_fails_closed(
        self, tmp_path
    ):
        """A tmux session the stale record cannot explain is never adopted."""

        workspace = _Workspace(tmp_path)
        predecessor, generation_a, runtime_a = _retired_predecessor(workspace, token=5)
        generation, runtime = str(uuid4()), str(uuid4())
        successor = _runtime_backend(
            token=9, workspace_generation=generation, runtime_incarnation=runtime
        )
        (workspace.tmux_state(runtime) / "session").touch()

        with pytest.raises(WorkspaceUnavailableError, match="exit code 80"):
            _run(
                workspace,
                successor,
                runtime,
                lambda: successor.exec_terminal_claim_resource(
                    "printf __SHOULD_NOT_RUN__", 30
                ),
            )

        assert workspace.tombstone(predecessor)[2:6] == [
            generation_a,
            runtime_a,
            "retired",
            "5",
        ]

    def test_a_foreign_tombstone_ahead_of_the_terminal_token_fails_closed(
        self, tmp_path
    ):
        """End's token must be monotonic over any record it replaces."""

        workspace = _Workspace(tmp_path)
        predecessor, generation_a, runtime_a = _retired_predecessor(workspace, token=12)
        generation, runtime = str(uuid4()), str(uuid4())
        successor = _runtime_backend(
            token=9, workspace_generation=generation, runtime_incarnation=runtime
        )

        with pytest.raises(WorkspaceUnavailableError, match=_FENCE_REFUSAL):
            _run(
                workspace,
                successor,
                runtime,
                lambda: successor.exec_terminal_claim_resource(
                    "printf __SHOULD_NOT_RUN__", 30
                ),
            )

        assert workspace.tombstone(predecessor)[2:6] == [
            generation_a,
            runtime_a,
            "retired",
            "12",
        ]

    @pytest.mark.parametrize("successor_state", ["active", "retired"])
    def test_a_late_predecessor_retirement_cannot_restamp_the_successor(
        self, tmp_path, successor_state
    ):
        """A's delayed terminal work carries A's older token and is refused.

        Queue tokens are monotonic per thread and Resume serializes behind
        the retirement lock, so a late predecessor request always carries a
        token below any record B has written.  Reaching B's volume through
        the shared Service name must not let it restamp B's record.
        """

        workspace = _Workspace(tmp_path)
        predecessor, _, runtime_a = _retired_predecessor(workspace, token=5)
        generation, runtime = str(uuid4()), str(uuid4())
        successor = _runtime_backend(
            token=9, workspace_generation=generation, runtime_incarnation=runtime
        )
        _run(
            workspace,
            successor,
            runtime,
            lambda: successor.exec_terminal_claim_resource(":", 30),
        )
        if successor_state == "retired":
            _run(workspace, successor, runtime, successor.shell_cleanup)
        late = _runtime_backend(
            token=6,
            workspace_generation=predecessor._workspace_generation,
            runtime_incarnation=runtime_a,
        )

        with pytest.raises(WorkspaceUnavailableError, match=_FENCE_REFUSAL):
            _run(
                workspace,
                late,
                runtime,
                lambda: late.exec_terminal_claim_resource(
                    "printf __SHOULD_NOT_RUN__", 30
                ),
            )

        assert workspace.tombstone(successor)[2:6] == [
            generation,
            runtime,
            successor_state,
            "9",
        ]


class TestRetiredResourceVerificationHonoursTheProcessZeroScan:
    """The post-shell re-proof must fail when its own zero scan refuses.

    ``verify_terminal_claim_resources_retired`` runs the retired-record fence,
    whose last step is the read-only tagged process-zero scan, and then the
    caller's resident-zero command. The scan's refusal must end the script;
    otherwise the resident command's own success is reported instead.
    """

    def test_a_live_tagged_process_fails_the_post_shell_verification(self, tmp_path):
        workspace = _Workspace(tmp_path)
        generation, runtime = str(uuid4()), str(uuid4())
        backend = _runtime_backend(
            token=9, workspace_generation=generation, runtime_incarnation=runtime
        )
        _run(workspace, backend, runtime, backend.shell_cleanup)
        tombstone = workspace.tombstone(backend)
        assert tombstone[2:6] == [generation, runtime, "retired", "9"]
        # Verify mode reads /proc and never signals, so a real host process
        # is a safe residual: it carries this runtime's exact workspace tag.
        survivor = subprocess.Popen(
            ["sleep", "60"],
            env={
                **os.environ,
                "SRW_WORKSPACE_PROCESS_TAG": backend._workspace_process_tag(),
            },
            start_new_session=True,
        )
        try:
            with (
                patch.object(
                    backend,
                    "_exec_with_status",
                    side_effect=workspace.runner(runtime),
                ),
                patch.object(backend, "_ensure_connected"),
                pytest.raises(WorkspaceUnavailableError, match=r"exit code 85$"),
            ):
                backend.verify_terminal_claim_resources_retired(
                    "printf __SRW_TERMINAL_RESIDENTS_ZERO__", 30
                )
            assert survivor.poll() is None
        finally:
            survivor.kill()
            survivor.wait(timeout=5)
        assert workspace.tombstone(backend) == tombstone
