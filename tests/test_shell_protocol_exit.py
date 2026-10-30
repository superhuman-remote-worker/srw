"""A command that would end a tab's shell runs in a subshell instead.

Dev jobs 19837f4b and 1dc0fac1 (2026-10-30) typed
``python3 -m unittest ... ; exit "$result"`` into a tab, as a model trained on
``bash -c`` harnesses writes it. The tab's shell exited, the session with it,
and the job paused and later failed on resume. ``confine_shell_exit`` runs
such a command in a subshell, where ``exit`` only sets its exit code.
"""

import subprocess

import pytest

from shared.runtime.core.shell_protocol import (
    command_shell_ended_message,
    confine_shell_exit,
    ends_typing_shell,
)

ENDS_THE_SHELL = (
    "python3 -m unittest discover -s tests -v > /tmp/r.txt 2>&1; result=$?; "
    'cat /tmp/r.txt; echo "Exit code: $result"; exit "$result"',
    "PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tests -v "
    "> /tmp/r.txt 2>&1; result=$?; tail -55 /tmp/r.txt; echo RED:$result; exit 0",
    "exit",
    "exit 3",
    "cd /x || exit 1; rm -rf build",
    "[ -f a ] && exit 0",
    "logout",
    "exec bash",
    "exec 2>&1",
    "FOO=1 exit 2",
    "command exit",
    "if true; then exit 1; fi",
    "case $x in a) exit 1;; esac",
    "{ exit 1; }",
    "grep -q x f || { echo missing; exit 1; }",
    'die() { echo "$1"; exit 1; }; [ -f x ] || die missing',
    "function die { exit 1; }; die",
    "set -e",
    "set -euo pipefail; pytest",
    "set -Eeuo pipefail",
    "set -o errexit",
    "set -o nounset -o errexit",
    "cat <<EOF\nbody\nEOF\nexit 4",
)

LEAVES_THE_SHELL = (
    "echo exit",
    'git commit -m "exit early"',
    "bash -c 'exit 1'",
    'python3 -c "import sys; sys.exit(1)"',
    "(cd x; exit 1)",
    "echo $(exit 1)",
    "x=`exit 1`",
    'echo "$(echo "exit")"',
    "cat <(exit 1)",
    "echo ${#arr[@]}",
    "pytest -x; echo rc=$?",
    "ls # exit",
    "echo a#exit",
    "make 2>&1 | tee log",
    "npm test &>/dev/null; echo $?",
    "((i++))",
    'trap "exit 1" INT',
    "exit_code=1; echo $exit_code",
    "myexit 1",
    "nohup ./server &",
    "source venv/bin/activate && pytest",
    "set -x",
    "set +e",
    "set -u",
    "set -o pipefail",
    "set -- -e",
    "cat > run.sh <<'EOF'\nset -e\nexit 1\nEOF\nchmod +x run.sh",
    "python3 - <<'PY'\nimport sys\nsys.exit(0)\nPY",
    "cat <<-EOF\n\texit 1\n\tEOF\necho ok",
    'echo "unterminated',
    "",
)


@pytest.mark.parametrize("command", ENDS_THE_SHELL)
def test_top_level_shell_ending_commands_are_seen(command):
    assert ends_typing_shell(command)


@pytest.mark.parametrize("command", LEAVES_THE_SHELL)
def test_commands_that_cannot_end_the_shell_are_left_alone(command):
    assert not ends_typing_shell(command)
    assert confine_shell_exit(command) == command


def test_single_line_command_stays_on_one_line():
    assert confine_shell_exit('pytest; exit "$rc"') == '( pytest; exit "$rc" )'


@pytest.mark.parametrize("command", ("make || exit 1 # build first", "echo a\nexit 2"))
def test_comment_or_newline_puts_the_parenthesis_on_its_own_line(command):
    assert confine_shell_exit(command) == f"(\n{command}\n)"


@pytest.mark.parametrize(
    ("command", "status", "printed"),
    (
        ("cd /tmp || exit 1; echo inside; exit 3", 3, "inside"),
        ("set -euo pipefail; false; echo not-reached", 1, ""),
        ('die() { echo "dying: $1"; exit 7; }; false || die x; echo no', 7, "dying: x"),
        ("make test || exit 1 # with a comment", 1, ""),
    ),
)
def test_confined_command_keeps_the_typing_shell_alive(command, status, printed):
    """Typed into one shell, as a tab is: the status survives, the shell too."""
    script = (
        f'{confine_shell_exit(command)}\nprintf "STATUS %s\\n" "$?"\necho SHELL-ALIVE\n'
    )
    result = subprocess.run(
        ["bash", "--norc", "-s"],
        input=script,
        capture_output=True,
        text=True,
        timeout=10,
        cwd="/",
    )
    assert f"STATUS {status}" in result.stdout
    assert "SHELL-ALIVE" in result.stdout
    assert "not-reached" not in result.stdout
    if printed:
        assert printed in result.stdout


def test_command_shell_ended_message_names_the_reset():
    tab = command_shell_ended_message("git", session_reset=False)
    session = command_shell_ended_message("git", session_reset=True)
    assert "The next command on 'git' opens a fresh shell" in tab
    assert "whole shell session ended" in session
    for message in (tab, session):
        assert message.startswith("The shell in tab 'git' exited")
        assert "`exit`" in message
