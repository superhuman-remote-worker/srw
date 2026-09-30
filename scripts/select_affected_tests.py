#!/usr/bin/env python3
"""Select the test files a change can affect, by static import graph.

Why not the obvious thing
-------------------------
Mapping ``src/agent/core/foo.py`` to ``tests/test_foo.py`` by name looks sufficient and
is not. Measured on a real change (2026-08-03, commit a1d92680): nine test files
failed and a naming rule selects **three** of them. The other six —
``test_app_guide_content``, ``test_persistent_session``,
``test_orchestrator_catalog_tool``, ``test_orchestrator_workflows_tool``,
``test_tool_grant_classification``, ``test_session_tool_groups_endpoint`` — share
no name with anything the commit touched. Many tests here are deliberately global
tripwires ("every capability grant must appear in the app guide", "this fixture
must not move", "these three vocabularies must agree"), so they break from edits
anywhere. All nine DO reach the changed modules through imports, which is why the
graph is the right instrument and the filename is not.

Two edge types
--------------
1. **Import edges** — AST-parsed, resolved against the editable ``src/`` package
   root and the repo root (for tests and retained entry scripts), then
   closed transitively. Resolution yields the leaf module, so the package
   ``__init__.py`` files Python runs on the way there are added as edges too:
   ``from tests.e2e.app import harness`` executes three package bodies before
   ``harness``, and a break in any of them fails the importing test.
2. **Data edges** — many tests assert on files rather than code: YAML under
   ``config/``, the app-guide markdown, ``cockpit/angular.json``, Helm templates.
   String literals that look like repo paths become dependencies too, so editing
   a config file selects the tests that read it.

Targets vs dependencies
-----------------------
What this prints is a pytest argument list, so only ``tests/**/test_*.py`` may
appear in it. Every other file — source, config, and the helpers, package inits
and fixture entrypoints that also live under ``tests/`` — is a dependency:
changing it selects the tests that reach it. Naming a non-test module as a
target makes pytest import a file nobody wrote to be collected, which is a
collection error rather than a test failure and takes the whole run with it.

Bias
----
Every ambiguity resolves toward running MORE tests. Unparseable file, unresolvable
import, changed conftest, changed dependency pin, changed selector — all print
``ALL``. A wrong "skip" is a shipped bug; a wrong "run" costs minutes.
"""

from __future__ import annotations

import argparse
import ast
import subprocess
import sys
from collections.abc import Iterable
from functools import lru_cache
from pathlib import Path

#: Printed instead of a file list when the whole suite must run.
ALL = "ALL"

#: Directories searched for first-party modules, in ``sys.path`` order.
#: Application modules resolve as agent.*, orchestrator.*, mcp_server.*,
#: vm_controller.*, and shared.*; tests/helpers retain their repository root.
IMPORT_ROOTS = ("src", "")

#: Trees scanned for first-party modules.
SOURCE_TREES = ("src", "tests")

#: Changing any of these invalidates the whole graph or the environment the
#: suite runs in, so selection is not safe. Matched as path prefixes.
FULL_SUITE_TRIGGERS = (
    "scripts/pytest_shard.py",
    "scripts/pytest_file_timings.py",
    "policy/pytest_file_timings.json",
    "tests/conftest.py",
    "conftest.py",
    "pytest.ini",
    "pyproject.toml",
    "setup.cfg",
    "tox.ini",
    "requirements.txt",
    "requirements-dev.txt",
    "requirements/",
    "scripts/lock_dependencies.py",
    "src/orchestrator/requirements.txt",
    "src/mcp_server/requirements.txt",
    "src/vm_controller/requirements.txt",
    ".github/workflows/",
    ".squawk.toml",
    "scripts/select_affected_tests.py",
    "tests/select_affected_tests_data",
)

#: Path prefixes whose literals count as data dependencies. A literal must
#: either contain "/" or name one of these, so a bare "src" in an unrelated
#: string does not wire a test to the entire source tree.
DATA_PREFIXES = (
    "config",
    "policy",
    "helm",
    "cockpit",
    "deployment",
    "website",
    "scripts",
    "vm",
    "docker",
)

#: Tests that assert a repo-wide invariant without naming a path a literal scan
#: can see — they glob, or read through a helper. Kept short and explicit: each
#: entry is a claim that the test can fail from an edit it does not import.
#: Cheap to run and they are the tripwires most worth never skipping.
ALWAYS_RUN = (
    "tests/test_config_tool_grants_snapshot.py",
    "tests/test_tool_policy.py",
    "tests/test_tool_grant_classification.py",
    "tests/test_config_tool_names_are_registered.py",
    # These scanners read the source tree through computed paths, so their
    # security inventories are invisible to ordinary import/data edges.
    "tests/test_endpoint_inventory.py",
    "tests/test_notification_producer_manifest.py",
    "tests/test_runtime_coordinate_inventory.py",
)


def _is_test_module(rel: str) -> bool:
    """True for a path pytest collects on its own: ``tests/**/test_*.py``."""
    return rel.startswith("tests/") and Path(rel).name.startswith("test_")


def _module_candidates(path: Path, repo: Path) -> list[str]:
    """Dotted names ``path`` is importable as, one per matching import root."""
    rel = path.relative_to(repo)
    names: list[str] = []
    for root in IMPORT_ROOTS:
        root_parts = tuple(p for p in root.split("/") if p)
        if rel.parts[: len(root_parts)] != root_parts:
            continue
        sub = rel.parts[len(root_parts) :]
        if not sub:
            continue
        stem = list(sub)
        stem[-1] = Path(stem[-1]).stem
        if stem[-1] == "__init__":
            stem.pop()
        if stem:
            names.append(".".join(stem))
    return names


def _python_files(repo: Path) -> list[Path]:
    files: list[Path] = []
    for tree in SOURCE_TREES:
        base = repo / tree
        if base.is_dir():
            files.extend(p for p in base.rglob("*.py") if "__pycache__" not in p.parts)
    files.extend(p for p in repo.glob("*.py"))
    return sorted(set(files))


def build_module_index(repo: Path) -> dict[str, Path]:
    """Dotted module name -> file. Earlier import roots win, as ``sys.path`` does."""
    index: dict[str, Path] = {}
    for path in _python_files(repo):
        for name in _module_candidates(path, repo):
            index.setdefault(name, path)
    return index


def _imported_names(
    tree: ast.AST, own_module: str | None, *, is_package: bool = False
) -> set[str]:
    """Dotted names imported by one parsed module, relative imports resolved."""
    out: set[str] = set()
    if is_package:
        package = own_module or ""
    else:
        package = (
            own_module.rsplit(".", 1)[0] if own_module and "." in own_module else ""
        )
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                out.add(alias.name)
        elif isinstance(node, ast.ImportFrom):
            if node.level:
                base = package.split(".") if package else []
                # level 1 = current package; each extra level walks up one.
                trimmed = (
                    base[: len(base) - (node.level - 1)] if node.level > 1 else base
                )
                parts = [*trimmed, *(node.module.split(".") if node.module else [])]
                prefix = ".".join(p for p in parts if p)
            else:
                prefix = node.module or ""
            if prefix:
                out.add(prefix)
                for alias in node.names:
                    # ``from pkg.mod import thing`` — `thing` may itself be a
                    # submodule, so record both readings and let resolution decide.
                    out.add(f"{prefix}.{alias.name}")
    return out


def _path_literals(tree: ast.AST) -> set[str]:
    """String constants that plausibly name a repo path."""
    out: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Constant) or not isinstance(node.value, str):
            continue
        raw = node.value.strip().strip("/")
        if not raw or len(raw) < 3 or raw.startswith(("http", "postgres", "redis")):
            continue
        head = raw.split("/", 1)[0]
        if "/" in raw or head in DATA_PREFIXES:
            out.add(raw)
    return out


class Graph:
    """First-party import edges plus data-file edges, both direction-agnostic."""

    def __init__(self, repo: Path) -> None:
        self.repo = repo
        self.module_index = build_module_index(repo)
        self.imports: dict[Path, set[Path]] = {}
        self.literals: dict[Path, set[str]] = {}
        self.unparseable: list[Path] = []
        self._build()

    def _resolve(self, dotted: str) -> Path | None:
        """Longest first-party prefix of ``dotted``, as a file."""
        parts = dotted.split(".")
        for stop in range(len(parts), 0, -1):
            hit = self.module_index.get(".".join(parts[:stop]))
            if hit is not None:
                return hit
        return None

    @lru_cache(maxsize=None)
    def _package_inits(self, path: Path) -> frozenset[Path]:
        """``__init__.py`` files Python executes when it imports ``path``.

        ``from tests.e2e.app import harness`` runs three package bodies before
        ``harness`` itself. Resolution only ever yields the leaf module, so
        without this edge a broken package init selects nothing.
        """
        out: set[Path] = set()
        for parent in path.parents:
            if parent == self.repo or not parent.is_relative_to(self.repo):
                break
            init = parent / "__init__.py"
            if init != path and init.is_file():
                out.add(init)
        return frozenset(out)

    def _build(self) -> None:
        for path in _python_files(self.repo):
            try:
                tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            except (SyntaxError, UnicodeDecodeError, OSError):
                self.unparseable.append(path)
                continue
            own = next(iter(_module_candidates(path, self.repo)), None)
            deps = {
                target
                for name in _imported_names(
                    tree, own, is_package=path.name == "__init__.py"
                )
                if (target := self._resolve(name)) is not None and target != path
            }
            # Importing the module runs its own package chain, and each import
            # it makes runs that target's chain.
            for anchor in (path, *tuple(deps)):
                deps |= self._package_inits(anchor)
            deps.discard(path)
            self.imports[path] = deps
            self.literals[path] = _path_literals(tree)

    @lru_cache(maxsize=None)
    def _closure(self, path: Path) -> frozenset[Path]:
        seen: set[Path] = set()
        stack = [path]
        while stack:
            cur = stack.pop()
            for dep in self.imports.get(cur, ()):
                if dep not in seen:
                    seen.add(dep)
                    stack.append(dep)
        return frozenset(seen)

    def test_files(self) -> list[Path]:
        return sorted(
            p
            for p in self.imports
            if p.is_relative_to(self.repo)
            and _is_test_module(p.relative_to(self.repo).as_posix())
        )

    def reaches(self, test: Path, target: Path) -> bool:
        return target in self._closure(test)

    def reads(self, test: Path, changed_rel: str) -> bool:
        """True when a literal in ``test`` names ``changed_rel`` or a parent of it."""
        for lit in self.literals.get(test, ()):
            if changed_rel == lit or changed_rel.startswith(lit.rstrip("/") + "/"):
                return True
        return False


def changed_files_from_git(base: str, head: str, repo: Path) -> list[str]:
    out = subprocess.run(
        ["git", "diff", "--name-only", base, head],
        cwd=repo,
        capture_output=True,
        text=True,
        check=True,
    )
    return [line for line in out.stdout.splitlines() if line.strip()]


def select(changed: Iterable[str], repo: Path) -> list[str] | str:
    """Test files to run, or :data:`ALL`."""
    changed = [c.strip() for c in changed if c.strip()]
    if not changed:
        return []

    for path in changed:
        if any(path == t or path.startswith(t) for t in FULL_SUITE_TRIGGERS):
            return ALL

    graph = Graph(repo)
    if graph.unparseable:
        return ALL

    selected: set[Path] = {repo / rel for rel in ALWAYS_RUN if (repo / rel).exists()}
    tests = graph.test_files()
    if not tests:
        return ALL

    for rel in changed:
        abs_path = repo / rel
        if rel.endswith(".py"):
            if _is_test_module(rel):
                # A changed test runs itself even if nothing imports it.
                if abs_path.exists():
                    selected.add(abs_path)
                continue
            # Anything else under tests/ — helpers, fixtures, package inits,
            # container entrypoints — is a dependency, not a target. Handing it
            # to pytest imports a module that was never written to be collected
            # (tests/e2e/app/deterministic_provider/run.py imports `provider`
            # the way its own image lays it out) and the run dies at collection.
            if not abs_path.exists():
                # Deleted or moved source: the graph cannot say who used it.
                return ALL
            if abs_path not in graph.imports:
                # A .py outside the scanned trees (scripts/, vm/, generators).
                # Nothing imports it as a module, so fall back rather than guess.
                return ALL
            consumers = {t for t in tests if graph.reaches(t, abs_path)}
            if rel.startswith("src/") and not consumers:
                # Dynamic imports/entry points can escape the static graph.
                # Always-run tripwires must not disguise zero source coverage.
                return ALL
            selected.update(consumers)
        else:
            selected.update(t for t in tests if graph.reads(t, rel))

    return sorted(str(p.relative_to(repo)) for p in selected)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--repo", default=".", help="repository root")
    ap.add_argument("--base", help="git base ref for the diff")
    ap.add_argument("--head", default="HEAD", help="git head ref for the diff")
    ap.add_argument(
        "--changed",
        action="append",
        default=[],
        help="repo-relative changed path (repeatable); skips git",
    )
    args = ap.parse_args(argv)
    repo = Path(args.repo).resolve()

    if args.changed:
        changed = args.changed
    elif args.base:
        try:
            changed = changed_files_from_git(args.base, args.head, repo)
        except subprocess.CalledProcessError:
            print(ALL)
            return 0
    else:
        changed = [line for line in sys.stdin.read().splitlines() if line.strip()]

    result = select(changed, repo)
    if result == ALL:
        print(ALL)
    else:
        for path in result:
            print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
