#!/usr/bin/env python3
"""Generate/check per-role dependency locks. See requirements/README.md."""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
ROLES = {
    "orchestrator": ("orchestrator", "3.11"),
    "vm-controller": ("vm_controller", "3.12"),
    "voice-gateway": ("voice_gateway", "3.12"),
}
#: Distributions a role's resolution needs but its image never installs. Each
#: ships only a source distribution, which a hash-checked install could only
#: build with unpinned build tools (pip does not hash-check build
#: dependencies). The resolver reads it from source, the generator leaves it
#: out of the lock, the role installs its lock with --no-deps, and
#: ``pip-check`` tolerates exactly these missing requirements.
EXCLUDED: dict[str, dict[str, str]] = {
    "voice-gateway": {
        "docopt": "num2words' command-line script only; Pipecat imports the "
        "num2words library, which never imports docopt",
    },
}
COMMON_INPUTS = (
    "requirements/constraints.txt",
    "requirements/lock-tools.txt",
    "scripts/lock_dependencies.py",
)
BOOTSTRAP = """
import subprocess, sys
subprocess.run([sys.executable, '-m', 'venv', '/tmp/resolver'], check=True)
python = '/tmp/resolver/bin/python'
subprocess.run([python, '-m', 'pip', '--isolated', 'install', '--disable-pip-version-check',
               '--index-url=https://pypi.org/simple', '-r', 'requirements/lock-tools.txt'], check=True)
subprocess.run([python, 'scripts/lock_dependencies.py', '_compile', *sys.argv[1:]], check=True)
"""


def paths(role: str) -> tuple[str, str, str]:
    package, version = ROLES[role]
    lock = f"requirements/locks/{role}-py{version.replace('.', '')}.txt"
    return f"src/{package}/requirements.txt", lock, lock.removesuffix(".txt") + ".json"


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def inputs(role: str, root: Path) -> dict[str, str]:
    names = set((*COMMON_INPUTS, paths(role)[0]))
    pending = [name for name in names if name.endswith(".txt")]
    while pending:
        name = pending.pop()
        for line in (root / name).read_text().splitlines():
            include = re.fullmatch(
                r"(?:-r|--requirement|-c|--constraint)[ =]*(\S+)",
                line.split("#", 1)[0].strip(),
            )
            if include:
                target = (root / name).parent / include[1]
                relative = target.resolve().relative_to(root.resolve()).as_posix()
                if relative not in names:
                    names.add(relative)
                    pending.append(relative)
    return {name: digest(root / name) for name in sorted(names)}


def pinned_versions(lock: Path) -> dict[str, str]:
    pins = {}
    for line in lock.read_text().splitlines():
        if not line or line.startswith(("#", " ")) or line == "--only-binary :all:":
            continue
        match = re.fullmatch(r"([A-Za-z0-9_.-]+)==([^\s;]+)(?: \\)?", line)
        if not match:
            raise ValueError(
                f"{lock.name}: expected a fully pinned, marker-free role lock"
            )
        name, version = match.groups()
        name = re.sub(r"[-_.]+", "-", name).lower()
        if name in pins:
            raise ValueError(f"{lock.name}: duplicate pin {name}")
        pins[name] = version
    if not pins:
        raise ValueError(f"{lock.name}: empty lock")
    return pins


def check(role: str, root: Path = ROOT, installed: bool = False) -> dict:
    _, lock, manifest = paths(role)
    recorded = json.loads((root / manifest).read_text())
    expected = {
        "schema": 1,
        "role": role,
        "python": ROLES[role][1],
        "platform": "linux/amd64",
        "inputs": inputs(role, root),
        "lock_sha256": digest(root / lock),
    }
    if recorded != expected:
        raise ValueError(
            f"{role}: stale lock; run python scripts/lock_dependencies.py compile {role}"
        )
    pins = pinned_versions(root / lock)
    if installed:
        check_platform(role)
        for name, expected_version in pins.items():
            actual = importlib.metadata.version(name)
            if actual != expected_version:
                raise ValueError(
                    f"{role}: installed {name}=={actual}, expected {expected_version}"
                )
    return {
        "role": role,
        "python": ROLES[role][1],
        "packages": len(pins),
        "installed": installed,
    }


def check_platform(role: str) -> None:
    version = ".".join(map(str, sys.version_info[:2]))
    if (version, sys.platform, platform.machine()) != (
        ROLES[role][1],
        "linux",
        "x86_64",
    ):
        raise ValueError(f"{role}: requires Linux amd64 Python {ROLES[role][1]}")


def strip_excluded(role: str, lock: Path) -> None:
    """Leave the role's :data:`EXCLUDED` distributions out of its lock: their
    pin with its hash and ``via`` lines, and the format option naming them."""
    excluded = set(EXCLUDED.get(role, ()))
    if not excluded:
        return
    out, skipping = [], False
    for line in lock.read_text().splitlines():
        if line.startswith((" ", "\t")):
            if not skipping:
                out.append(line)
            continue
        name = re.split(r"==|\s", line, maxsplit=1)[0].lower()
        skipping = bool(line) and name in excluded
        option = re.fullmatch(r"--no-binary[ =](\S+)", line)
        if option and set(option[1].split(",")) <= excluded:
            continue
        if not skipping:
            out.append(line)
    lock.write_text("\n".join(out) + "\n")
    left = excluded & set(pinned_versions(lock))
    if left:
        raise ValueError(f"{role}: excluded distributions still pinned: {left}")


def pip_check(role: str) -> list[str]:
    """``pip check`` of the installed role: the problems other than a
    requirement on one of its :data:`EXCLUDED` distributions."""
    result = subprocess.run(
        [sys.executable, "-m", "pip", "check"],
        capture_output=True,
        text=True,
        check=False,
    )
    excluded = set(EXCLUDED.get(role, ()))
    problems = []
    for line in result.stdout.splitlines():
        missing = re.fullmatch(
            r"\S+ \S+ requires (\S+), which is not installed\.", line
        )
        if line.startswith("No broken requirements") or (
            missing and missing[1].lower() in excluded
        ):
            continue
        problems.append(line)
    if result.returncode not in (0, 1) or (result.returncode and not result.stdout):
        problems.append(
            result.stderr.strip() or f"pip check exited {result.returncode}"
        )
    return problems


def compile_inside(role: str, upgrades: list[str]) -> None:
    check_platform(role)
    for name, version in pinned_versions(ROOT / "requirements/lock-tools.txt").items():
        if importlib.metadata.version(name) != version:
            raise ValueError(f"resolver requires {name}=={version}")
    requirement, lock, manifest = paths(role)
    command = [
        sys.executable,
        "-m",
        "piptools",
        "compile",
        requirement,
        "--output-file",
        lock,
        "--resolver=backtracking",
        "--generate-hashes",
        "--strip-extras",
        "--allow-unsafe",
        "--no-emit-index-url",
        "--no-emit-trusted-host",
        "--index-url=https://pypi.org/simple",
        "--pip-args=--only-binary=:all:"
        + "".join(f" --no-binary={name}" for name in sorted(EXCLUDED.get(role, ()))),
        "--cache-dir=/tmp/pip-tools-cache",
        *upgrades,
    ]
    env = dict(
        os.environ,
        CUSTOM_COMPILE_COMMAND=f"python scripts/lock_dependencies.py compile {role}",
    )
    subprocess.run(command, cwd=ROOT, env=env, check=True)
    strip_excluded(role, ROOT / lock)
    pinned_versions(ROOT / lock)
    (ROOT / manifest).write_text(
        json.dumps(
            {
                "schema": 1,
                "role": role,
                "python": ROLES[role][1],
                "platform": "linux/amd64",
                "inputs": inputs(role, ROOT),
                "lock_sha256": digest(ROOT / lock),
            },
            indent=2,
        )
        + "\n"
    )


def compile_container(role: str, upgrades: list[str]) -> None:
    requirement, lock, manifest = paths(role)
    before = inputs(role, ROOT)
    with tempfile.TemporaryDirectory(prefix="srw-dependency-lock-") as directory:
        stage = Path(directory)
        for name in (*before, lock):
            target = stage / name
            target.parent.mkdir(parents=True, exist_ok=True)
            if (ROOT / name).exists():
                shutil.copyfile(ROOT / name, target)
        subprocess.run(
            [
                "docker",
                "run",
                "--rm",
                "--platform=linux/amd64",
                "--user",
                f"{os.getuid()}:{os.getgid()}",
                "--mount",
                f"type=bind,source={stage},target=/work",
                "--workdir",
                "/work",
                f"python:{ROLES[role][1]}-slim",
                "python",
                "-c",
                BOOTSTRAP,
                role,
                *upgrades,
            ],
            check=True,
        )
        check(role, stage)
        if before != inputs(role, ROOT):
            raise ValueError(
                "inputs changed during resolution; rerun without concurrent edits"
            )
        (ROOT / lock).parent.mkdir(parents=True, exist_ok=True)
        for name in (lock, manifest):
            shutil.copyfile(stage / name, ROOT / name)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    verify = commands.add_parser(
        "check", help="offline input/output drift and optional installed-version check"
    )
    verify.add_argument("role", choices=ROLES, nargs="?")
    verify.add_argument("--installed", action="store_true")
    consistency = commands.add_parser(
        "pip-check",
        help="pip check of an installed role, tolerating only its excluded distributions",
    )
    consistency.add_argument("role", choices=ROLES)
    for command in ("compile", "_compile"):
        generate = commands.add_parser(command)
        generate.add_argument("role", choices=ROLES)
        generate.add_argument("--upgrade", action="store_true")
        generate.add_argument("--upgrade-package", action="append", default=[])
    args = parser.parse_args()
    try:
        if args.command == "pip-check":
            problems = pip_check(args.role)
            for line in problems:
                print(line)
            if problems:
                raise ValueError(f"{args.role}: broken requirements")
        elif args.command == "check":
            if args.installed and not args.role:
                parser.error("--installed requires a role")
            for role in [args.role] if args.role else ROLES:
                print(json.dumps(check(role, installed=args.installed)))
        else:
            upgrades = ["--upgrade"] if args.upgrade else []
            for name in args.upgrade_package:
                upgrades.extend(("--upgrade-package", name))
            if args.command == "compile":
                compile_container(args.role, upgrades)
            else:
                compile_inside(args.role, upgrades)
    except (ValueError, OSError, subprocess.CalledProcessError) as exc:
        parser.exit(1, f"{exc}\n")


if __name__ == "__main__":
    main()
