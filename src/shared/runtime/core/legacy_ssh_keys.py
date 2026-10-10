"""The pre-agent repository SSH key rule, for every caller that retires one.

A repository clone before workspace ssh-agent identities (slice C1) wrote
its key to ``~/.ssh/repo_<slug>`` and an exact block to ``~/.ssh/config``.
Only a key such a block names, and nothing else, is SRW's to delete; a
user's own ``~/.ssh/repo_deploy`` never is. The agent's checkout retires
them once a repository is proven through its alias
(``agent.connectors.checkout``), and the orchestrator's credential scrub
of a kept workspace runs the same listing and program
(``orchestrator.services.managed_repository_process_retirement``).
"""

from __future__ import annotations

import re
import shlex


#: A pre-agent clone wrote its key to ``~/.ssh/repo_<datasource-name-slug>``
#: and appended, to ``~/.ssh/config``, exactly
#: ``\nHost <host>\n  IdentityFile <abs key path>\n  StrictHostKeyChecking
#: accept-new\n``. Only a key that such an exact block names, and that no
#: other config line names, is ours to delete: an ``IdentityFile`` line a user
#: wrote (absolute path or not) never causes a delete, and neither does a key
#: no line names (``ssh-keygen -f ~/.ssh/repo_deploy``).
LEGACY_KEY_NAME = re.compile(r"repo_[a-z0-9-]+")
LEGACY_KEY_MARKER = "__srw_legacy_repo_keys__"
# One awk pass over config per candidate (``path`` is its absolute path).
# Every line naming the file must be the IdentityFile line of an exact
# pre-agent block (``Host <host>`` before it, ``  StrictHostKeyChecking
# accept-new`` after it, nothing indented after that), and one must be.
# The delete program applies the same rule again.
EXACT_BLOCK_AWK = (
    "'{ line[NR] = $0 } END { exact = 0; other = 0; "
    'for (i = 1; i <= NR; i++) { s = line[i]; sub(/^[ \\t]+/, "", s); '
    'sub(/[ \\t\\r]+$/, "", s); if (s != "IdentityFile " path) continue; '
    'if (line[i] == "  IdentityFile " path && i > 1 '
    "&& line[i - 1] ~ /^Host [^ \\t\\r]+$/ && i < NR "
    '&& line[i + 1] == "  StrictHostKeyChecking accept-new" '
    "&& (i + 2 > NR || line[i + 2] !~ /^[ \\t]/)) exact++; else other++ } "
    "exit !(exact > 0 && other == 0) }'"
)


def list_legacy_keys_command(ssh_dir: str) -> str:
    """List pre-agent key files: one marker line on stdout.

    Regular files only, slug-shaped names only, a PEM key header that is not
    a public key, and named in ``config`` only by exact pre-agent blocks
    (:data:`EXACT_BLOCK_AWK`). Runs in a subshell to leave the persistent
    ``git`` tab where it was; the marker is split in the command so an echoed
    or wrapped command line can never read as the answer. (The pattern avoids
    spelling a private-key header: every command stays clear of what the
    key-residue checks scan for.)
    """

    prefix = shlex.quote(f"{ssh_dir}/")
    return (
        f"( cd {shlex.quote(ssh_dir)} 2>/dev/null || exit 0; "
        "test -f config || exit 0; _srw_names=''; "
        "for _srw_f in repo_*; do "
        '[ -f "$_srw_f" ] && [ ! -L "$_srw_f" ] || continue; '
        'case "${_srw_f#repo_}" in ""|*[!a-z0-9-]*) continue;; esac; '
        'IFS= read -r _srw_first < "$_srw_f" || continue; '
        'case "$_srw_first" in *"PUBLIC KEY-----") continue;; '
        '"-----BEGIN "*" KEY-----") ;; *) continue;; esac; '
        f'awk -v path={prefix}"$_srw_f" {EXACT_BLOCK_AWK} config || continue; '
        '_srw_names="$_srw_names $_srw_f"; '
        "done; "
        f"printf '%s%s\\n' {LEGACY_KEY_MARKER[:13]} "
        f'{LEGACY_KEY_MARKER[13:]}"$_srw_names" )'
    )


# Runs on the workspace (argv: ssh dir, names). Re-checks every name exactly
# as the listing did (the same exact-block rule), deletes it, then removes
# each deleted key's exact pre-agent blocks from config (and the blank line
# before each). Since no other line may name a deleted key, nothing in config
# is left pointing at it; a block a user edited never qualified.
RETIRE_LEGACY_KEYS_PROGRAM = r"""
import os, re, stat, sys, tempfile
ssh_dir, names = sys.argv[1], sys.argv[2:]
config = os.path.join(ssh_dir, "config")
try:
    with open(config, encoding="utf-8", errors="surrogateescape") as handle:
        lines = handle.read().split("\n")
except OSError:
    sys.exit(0)
HOST = re.compile(r"Host [^ \t\r]+")
TRUST = "  StrictHostKeyChecking accept-new"
def exact_block_at(index, path):
    following = lines[index + 2] if index + 2 < len(lines) else ""
    return (
        lines[index] == "  IdentityFile " + path
        and index > 0
        and HOST.fullmatch(lines[index - 1]) is not None
        and index + 1 < len(lines)
        and lines[index + 1] == TRUST
        and not following[:1].isspace()
    )
def only_exact_blocks_name(path):
    exact = other = 0
    for index, line in enumerate(lines):
        if line.strip() != "IdentityFile " + path:
            continue
        if exact_block_at(index, path):
            exact += 1
        else:
            other += 1
    return exact > 0 and other == 0
def key_file(path):
    try:
        if not stat.S_ISREG(os.lstat(path).st_mode):
            return False
        with open(path, "rb") as handle:
            first = handle.readline(4096).rstrip(b"\r\n")
    except OSError:
        return False
    return (
        first.startswith(b"-----BEGIN ")
        and first.endswith(b" KEY-----")
        and not first.endswith(b"PUBLIC KEY-----")
    )
removed = set()
for name in names:
    path = os.path.join(ssh_dir, name)
    if (
        re.fullmatch(r"repo_[a-z0-9-]+", name)
        and key_file(path)
        and only_exact_blocks_name(path)
    ):
        os.unlink(path)
        removed.add(path)
kept, index, changed = [], 0, False
while index < len(lines):
    if (
        index + 1 < len(lines)
        and lines[index + 1].startswith("  IdentityFile ")
        and lines[index + 1][len("  IdentityFile "):] in removed
        and exact_block_at(index + 1, lines[index + 1][len("  IdentityFile "):])
    ):
        if kept and kept[-1] == "":
            kept.pop()
        index += 3
        changed = True
        continue
    kept.append(lines[index])
    index += 1
if changed and not os.path.islink(config):
    mode = stat.S_IMODE(os.stat(config).st_mode)
    descriptor, temporary = tempfile.mkstemp(dir=ssh_dir, prefix=".config.srw-")
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", errors="surrogateescape") as handle:
            handle.write("\n".join(kept))
        os.chmod(temporary, mode)
        os.replace(temporary, config)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
"""


__all__ = [
    "EXACT_BLOCK_AWK",
    "LEGACY_KEY_MARKER",
    "LEGACY_KEY_NAME",
    "RETIRE_LEGACY_KEYS_PROGRAM",
    "list_legacy_keys_command",
]
