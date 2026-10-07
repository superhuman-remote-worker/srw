"""Agent tmux shells never write a history file into the workspace home.

The git tab types ``git clone 'https://oauth2:<token>@…'``. Killing the tmux
session SIGHUPs bash, which then writes ``~/.bash_history`` into the home that
every workspace snapshot and the PVC keep. This drives a real tmux and bash
with the exact setup the remote backend uses: the shell preamble
(``NONINTERACTIVE_ENV_EXPORT``) for shell tabs and the session environment
for every later window. A control server without either proves bash had the
time to write its history.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import time
import uuid
from pathlib import Path

import pytest

from shared.runtime.core.shell_protocol import NONINTERACTIVE_ENV_EXPORT

pytestmark = pytest.mark.skipif(
    shutil.which("tmux") is None or shutil.which("bash") is None,
    reason="tmux and bash are required",
)


class _Server:
    def __init__(self, home: Path) -> None:
        home.mkdir()
        (home / ".bashrc").write_text("PS1='$ '\n")
        self.home = home
        self.socket = f"srw-history-{uuid.uuid4().hex[:12]}"
        self.env = {
            key: value
            for key, value in os.environ.items()
            if key not in {"HISTFILE", "TMUX", "PROMPT_COMMAND"}
        }
        self.env.update(HOME=str(home), SHELL=shutil.which("bash"))

    def tmux(self, *args: str) -> str:
        return subprocess.run(
            ["tmux", "-L", self.socket, "-f", os.devnull, *args],
            env=self.env,
            capture_output=True,
            text=True,
            timeout=10,
        ).stdout

    def run(self, window: str, command: str, marker: str) -> None:
        self.tmux("send-keys", "-t", f"s:{window}", command, "Enter")
        self.tmux("send-keys", "-t", f"s:{window}", f"echo {marker}-$((40+2))", "Enter")
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if f"{marker}-42" in self.tmux("capture-pane", "-p", "-t", f"s:{window}"):
                return
            time.sleep(0.05)
        raise AssertionError(f"shell in {window} did not run {marker}")

    def history(self) -> str:
        path = self.home / ".bash_history"
        return path.read_text() if path.exists() else ""

    def close(self) -> None:
        self.tmux("kill-server")


def test_killed_agent_shells_leave_no_history(tmp_path) -> None:
    secret = "c0-history-token-" + uuid.uuid4().hex[:8]
    control = _Server(tmp_path / "control")
    agent = _Server(tmp_path / "agent")
    try:
        for server in (control, agent):
            server.tmux("new-session", "-d", "-s", "s", "-n", "default", "bash -i")
        control.run("default", f"echo {secret}", "control")

        # What _init_shell and shell_open_tab do: the session environment for
        # every later window, and the preamble in each shell tab.
        agent.tmux("set-environment", "-t", "s", "HISTFILE", "/dev/null")
        agent.run("default", NONINTERACTIVE_ENV_EXPORT, "preamble")
        agent.run("default", f"echo {secret}", "shell")
        agent.tmux("new-window", "-d", "-t", "s", "-n", "git", "bash -i")
        agent.run("git", NONINTERACTIVE_ENV_EXPORT, "git-preamble")
        agent.run("git", f"echo {secret}", "git")
        agent.tmux("new-window", "-d", "-t", "s", "-n", "repl", "bash -i")
        agent.run("repl", f"echo {secret}", "repl")  # no preamble on repl tabs

        for server in (control, agent):
            server.tmux("kill-session", "-t", "s")
        deadline = time.monotonic() + 10
        while secret not in control.history() and time.monotonic() < deadline:
            time.sleep(0.05)
    finally:
        control.close()
        agent.close()

    assert secret in control.history(), "control bash never wrote its history"
    assert secret not in agent.history()
