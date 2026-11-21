"""The workspace-side foreground check, run as written against a real terminal.

``RemoteBackend._shell_foreground_check_command`` decides whether a keys-mode
Ctrl+C may type a completion probe into a tab: only when the tab's shell is
alone in the terminal's foreground. A wrong ``shell`` would type the probe
into a REPL or an ssh session, so the script runs here against an interactive
bash on a pseudo-terminal, with a stand-in ``tmux`` that names that bash as the
pane's process. No tmux and no SSH are needed; Linux ``/proc`` is.
"""

from __future__ import annotations

import os
import shutil
import signal
import subprocess
import sys
import time

import pytest

from shared.runtime.core.backends.remote import RemoteBackend

pytestmark = pytest.mark.skipif(
    not sys.platform.startswith("linux") or shutil.which("bash") is None,
    reason="needs Linux /proc and bash",
)

# The child makes the pty its controlling terminal, so the shell owns the
# terminal's foreground group with job control on, as in a tmux pane.
_ATTACH_AND_EXEC = (
    "import os, sys\n"
    "os.setsid()\n"
    "fd = os.open(sys.argv[1], os.O_RDWR)\n"
    "for n in (0, 1, 2):\n"
    "    os.dup2(fd, n)\n"
    "os.close(fd)\n"
    "os.execvp(sys.argv[2], sys.argv[2:])\n"
)


@pytest.fixture
def pane(tmp_path):
    """An interactive bash on a pty, plus a way to type and to run the check."""
    master, slave = os.openpty()
    slave_name = os.ttyname(slave)
    os.close(slave)
    os.set_blocking(master, False)
    env = dict(os.environ, PS1="$ ", HOME=str(tmp_path))
    shell = subprocess.Popen(
        [
            sys.executable,
            "-c",
            _ATTACH_AND_EXEC,
            slave_name,
            "bash",
            "--norc",
            "--noprofile",
            "-i",
        ],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env=env,
    )
    fake_tmux = tmp_path / "bin" / "tmux"
    fake_tmux.parent.mkdir()
    fake_tmux.write_text(f"#!/bin/sh\necho {shell.pid}\n")
    fake_tmux.chmod(0o755)
    check_env = dict(os.environ, PATH=f"{fake_tmux.parent}:{os.environ['PATH']}")
    command = RemoteBackend._shell_foreground_check_command("%1")

    def check() -> str:
        return subprocess.run(
            ["sh", "-c", command],
            env=check_env,
            capture_output=True,
            text=True,
            timeout=30,
        ).stdout.strip()

    def type_(text: str) -> None:
        os.write(master, text.encode())

    def wait_for(expected: str) -> str:
        deadline = time.monotonic() + 10
        seen = check()
        while seen != expected and time.monotonic() < deadline:
            time.sleep(0.05)
            # Keep the pty drained so the shell never blocks on output.
            try:
                os.read(master, 65536)
            except (BlockingIOError, OSError):
                pass
            seen = check()
        return seen

    try:
        yield type_, wait_for, check
    finally:
        try:
            os.killpg(shell.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        shell.wait(timeout=10)
        os.close(master)


def test_idle_shell_then_foreground_job_then_interrupted(pane):
    type_, wait_for, _ = pane
    assert wait_for("shell") == "shell"

    type_("sleep 30\n")
    assert wait_for("busy") == "busy"

    type_("\x03")
    assert wait_for("shell") == "shell"


def test_child_shell_named_bash_is_busy(pane):
    """A multi-line command runs in a child bash: same name, other process."""
    type_, wait_for, _ = pane
    assert wait_for("shell") == "shell"
    type_("bash --norc --noprofile -c 'sleep 30; true'\n")
    assert wait_for("busy") == "busy"


def test_child_in_the_shells_own_group_is_busy(pane):
    """Without job control a child shares the shell's foreground group."""
    type_, wait_for, _ = pane
    assert wait_for("shell") == "shell"
    type_("set +m; sleep 30\n")
    assert wait_for("busy") == "busy"


def test_background_job_leaves_the_shell_in_front(pane):
    type_, wait_for, _ = pane
    assert wait_for("shell") == "shell"
    type_("sleep 30 &\n")
    time.sleep(0.3)
    assert wait_for("shell") == "shell"


def test_unreadable_pane_is_unknown(tmp_path):
    fake_tmux = tmp_path / "tmux"
    fake_tmux.write_text("#!/bin/sh\nexit 1\n")
    fake_tmux.chmod(0o755)
    result = subprocess.run(
        ["sh", "-c", RemoteBackend._shell_foreground_check_command("%1")],
        env=dict(os.environ, PATH=f"{tmp_path}:{os.environ['PATH']}"),
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0
    assert result.stdout.strip() == "unknown"
