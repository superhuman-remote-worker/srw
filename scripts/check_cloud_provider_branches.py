#!/usr/bin/env python3
"""Inventory code outside the main-cloud adapters that branches on a provider.

What a main-cloud provider can do is its adapter's declaration, the provider
support matrix (``orchestrator.services.cloud.capabilities``): code outside
the adapters asks ``provider_offers(...)`` or calls the adapter, never
compares a ``backend_id`` with ``"nextcloud"`` (main_cloud_as_connectors.md,
slice 2). This scanner finds the places that still name a provider, so the
count can only go down. Every site carries a reviewed classification in
``policy/cloud_provider_branches.txt``; a new site is rendered
``unclassified`` and fails ``tests/test_cloud_provider_branches.py``.
Modelled on ``scripts/check_connector_type_branches.py``.

**Provider ids come from the adapters' settings**: every ``backend_id:
Literal["..."]`` in ``orchestrator/services/cloud/config.py``, read from its
syntax tree, so a new provider extends the gate without an edit here.

**What it finds**, in every Python module under ``src/``:

* a comparison (``==``, ``!=``, ``in``, ``not in``) with a provider id or a
  literal collection holding one, whatever the other side is
  (``backend.backend_id == "nextcloud"``, ``row.get("backend") != "nextcloud"``,
  ``source_config.get("vendor") == "nextcloud"``);
* a ``match`` case naming a provider id;
* a keyword argument whose name mentions ``backend``, ``provider`` or
  ``vendor`` set to a provider id (``expected_backend_id="nextcloud"``);
* a set, list or tuple literal naming two or more provider ids;
* a dict literal with two or more provider-id keys (per-provider tables);
* a Python string holding SQL that tests a ``backend``/``backend_id``/
  ``main_cloud_backend`` column against a provider id.

A provider id used as data (``"backend": "nextcloud"`` in a payload, a
``BACKEND_ID`` constant) is not a branch and is not reported.

**Allowlisted**: the adapters, where deciding by provider is the point:
``src/orchestrator/services/cloud/`` (the orchestrator-side adapters, their
settings and contracts) and ``src/agent/services/cloud_sync/`` (the
agent-side halves: per-provider sync and the protected reader transport).

**Outside the gate, by design**: SQL files (migrations), the cockpit
(TypeScript) and prompts.

**Classifications** (there is no "to convert" class: a branch is converted
to a declaration query or an adapter method, or it is one of these):

* ``sql``: a provider test in SQL text. The protected tables are
  Nextcloud-only by trigger (migration 0186), so their SQL names the provider
  until a second protected provider brings a migration;
* ``protected-record``: a check of a protected-cloud record whose format only
  the Nextcloud adapter writes (its canonical source identity, the signed
  effect intent it re-reads). Frozen at the reviewed sites
  (:data:`PROTECTED_RECORD_SITES`): no new site may take it;
* ``legacy-column``: the pre-abstraction Nextcloud columns
  (``nc_session_folder``, ``nc_share_id``), written for Nextcloud only until
  they are dropped. Frozen at the reviewed sites (:data:`LEGACY_COLUMN_SITES`).

A site is identified by what it is, not where: the enclosing qualname, the
kind, the provider ids and a fingerprint of the node's structure. An ordinal
only tells identical duplicates in one scope apart.
"""

from __future__ import annotations

import argparse
import ast
import hashlib
import re
import sys
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SRC = REPO_ROOT / "src"
MANIFEST = REPO_ROOT / "policy" / "cloud_provider_branches.txt"
SETTINGS = SRC / "orchestrator" / "services" / "cloud" / "config.py"

#: The adapters: deciding by provider inside them is their job.
ALLOWLIST: tuple[str, ...] = (
    "src/orchestrator/services/cloud/",
    "src/agent/services/cloud_sync/",
)

UNCLASSIFIED = "unclassified"
PROTECTED_RECORD = "protected-record"
LEGACY_COLUMN = "legacy-column"
ALLOWED_CLASSIFICATIONS = frozenset({"sql", PROTECTED_RECORD, LEGACY_COLUMN})

Key = tuple[str, str, str, str, str, int]

#: The reviewed ``protected-record`` sites (canonical source identity and the
#: signed effect intent): frozen, as the connector gate froze pending-d3-d4.
PROTECTED_RECORD_SITES: frozenset[Key] = frozenset(
    {
        (
            "src/orchestrator/database/postgres.py",
            "PostgresDB.install_cloud_ro_effect_intent",
            "compare",
            "nextcloud",
            "57f72ffa720c",
            1,
        ),
        (
            "src/orchestrator/services/cloud_staging/source_identity.py",
            "ProtectedMountSourceIdentity.__post_init__",
            "compare",
            "nextcloud",
            "b1e17f0a2f6d",
            1,
        ),
        (
            "src/orchestrator/services/cloud_staging/source_identity.py",
            "ProtectedMountSourceIdentity.from_mount_row",
            "compare",
            "nextcloud",
            "1b692dcecbfb",
            1,
        ),
        (
            "src/orchestrator/services/cloud_staging/source_identity.py",
            "ProtectedMountSourceIdentity.from_mount_row",
            "compare",
            "nextcloud",
            "d2f0e7772cb5",
            1,
        ),
    }
)
#: The reviewed ``legacy-column`` sites.
LEGACY_COLUMN_SITES: frozenset[Key] = frozenset(
    {
        (
            "src/orchestrator/database/postgres.py",
            "PostgresDB.update_thread_main_cloud",
            "compare",
            "nextcloud",
            "792c497b2a90",
            ordinal,
        )
        for ordinal in (1, 2)
    }
)
FROZEN_SITES: dict[str, frozenset[Key]] = {
    PROTECTED_RECORD: PROTECTED_RECORD_SITES,
    LEGACY_COLUMN: LEGACY_COLUMN_SITES,
}

#: Keyword names that carry a provider (``expected_backend_id=...``).
_PROVIDER_KEYWORD = re.compile(r"backend|provider|vendor")
_SQL_COLUMN = r"(?:\w+\.)?(?:main_cloud_backend|backend_id|backend|provider)"
_SQL_EQUALS = re.compile(
    rf"\b{_SQL_COLUMN}\s*(?:=|<>|!=|IS\s+(?:NOT\s+)?DISTINCT\s+FROM)\s*"
    r"'([A-Za-z0-9_.-]+)'",
    re.IGNORECASE,
)
_SQL_IN = re.compile(rf"\b{_SQL_COLUMN}\s+(?:NOT\s+)?IN\s*\(([^)]*)\)", re.IGNORECASE)
_SQL_QUOTED = re.compile(r"'([A-Za-z0-9_.-]+)'")

_MAX_SKELETON_DEPTH = 6
_MAX_CONSTANT_CHARS = 48


def provider_ids(settings: Path = SETTINGS) -> frozenset[str]:
    """Every provider an adapter's settings class names (``backend_id``)."""
    ids: set[str] = set()
    for node in ast.walk(ast.parse(settings.read_text(), filename=str(settings))):
        if (
            isinstance(node, ast.AnnAssign)
            and isinstance(node.target, ast.Name)
            and node.target.id == "backend_id"
            and isinstance(node.annotation, ast.Subscript)
            and isinstance(node.annotation.value, ast.Name)
            and node.annotation.value.id == "Literal"
        ):
            for value in ast.walk(node.annotation.slice):
                if isinstance(value, ast.Constant) and isinstance(value.value, str):
                    ids.add(value.value)
    if not ids:
        raise RuntimeError(f"no provider ids found in {settings}")
    return frozenset(ids)


def _skeleton(node: ast.AST | None, depth: int = 0) -> str:
    """A bounded, position-free structural sketch (stable across Pythons)."""
    if node is None:
        return ""
    if depth > _MAX_SKELETON_DEPTH:
        return "..."
    nxt = depth + 1
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return f"{_skeleton(node.value, nxt)}.{node.attr}"
    if isinstance(node, ast.Call):
        args = [_skeleton(arg, nxt) for arg in node.args]
        args += [f"{kw.arg or '**'}={_skeleton(kw.value, nxt)}" for kw in node.keywords]
        return f"{_skeleton(node.func, nxt)}({', '.join(args)})"
    if isinstance(node, ast.Constant):
        text = str(node.value)
        if len(text) > _MAX_CONSTANT_CHARS:
            text = text[:_MAX_CONSTANT_CHARS] + "..."
        return f"'{text}'" if isinstance(node.value, str) else text
    if isinstance(node, ast.Subscript):
        return f"{_skeleton(node.value, nxt)}[{_skeleton(node.slice, nxt)}]"
    if isinstance(node, (ast.List, ast.Tuple, ast.Set)):
        brackets = {ast.List: "[]", ast.Tuple: "()", ast.Set: "{}"}[type(node)]
        inner = ", ".join(_skeleton(e, nxt) for e in node.elts)
        return f"{brackets[0]}{inner}{brackets[1]}"
    if isinstance(node, ast.Compare):
        ops = " ".join(type(op).__name__ for op in node.ops)
        rest = " ".join(_skeleton(c, nxt) for c in node.comparators)
        return f"{_skeleton(node.left, nxt)} {ops} {rest}"
    if isinstance(node, ast.BoolOp):
        values = ", ".join(_skeleton(v, nxt) for v in node.values)
        return f"{type(node.op).__name__}({values})"
    if isinstance(node, ast.UnaryOp):
        return f"{type(node.op).__name__} {_skeleton(node.operand, nxt)}"
    if isinstance(node, ast.MatchValue):
        return f"case {_skeleton(node.value, nxt)}"
    if isinstance(node, ast.MatchOr):
        return " | ".join(_skeleton(p, nxt) for p in node.patterns)
    return type(node).__name__


def _digest(text: str) -> str:
    return hashlib.blake2s(text.encode("utf-8"), digest_size=6).hexdigest()


def _string_literals(node: ast.AST) -> list[ast.Constant]:
    """The string constants of a literal or a literal collection."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return [node]
    if isinstance(node, (ast.Set, ast.Tuple, ast.List)):
        return [
            e
            for e in node.elts
            if isinstance(e, ast.Constant) and isinstance(e.value, str)
        ]
    if (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id in {"frozenset", "set", "tuple", "list"}
        and len(node.args) == 1
    ):
        return _string_literals(node.args[0])
    return []


def sql_provider_literals(text: str, ids: frozenset[str]) -> list[str]:
    """Provider predicates in SQL text, normalized."""
    found: list[str] = []
    for match in _SQL_EQUALS.finditer(text):
        if match.group(1) in ids:
            found.append(" ".join(match.group(0).split()))
    for match in _SQL_IN.finditer(text):
        if set(_SQL_QUOTED.findall(match.group(1))) & ids:
            found.append(" ".join(match.group(0).split()))
    return found


@dataclass(frozen=True)
class Site:
    file: str
    qualname: str
    kind: str
    ids: str
    fingerprint: str
    ordinal: int

    @property
    def key(self) -> Key:
        return (
            self.file,
            self.qualname,
            self.kind,
            self.ids,
            self.fingerprint,
            self.ordinal,
        )

    def render(self, classification: str, reason: str = "") -> str:
        line = (
            f"{self.file}  {self.qualname}  {self.kind}  {self.ids}  "
            f"{self.fingerprint}  #{self.ordinal}  {classification}"
        )
        return f"{line}  {reason}" if reason else line


class _Visitor(ast.NodeVisitor):
    def __init__(self, ids: frozenset[str]) -> None:
        self.ids = ids
        self.stack: list[str] = []
        self.raw: list[tuple[str, str, str, str]] = []
        self.consumed: set[int] = set()
        self.docstrings: set[int] = set()

    def _record(self, kind: str, ids: set[str], shape: str) -> None:
        qualname = ".".join(self.stack) or "<module>"
        self.raw.append((qualname, kind, ",".join(sorted(ids)), _digest(shape)))

    def _ids(self, nodes: list[ast.Constant]) -> set[str]:
        return {node.value for node in nodes if node.value in self.ids}

    def _scope(self, node: ast.AST) -> None:
        body = getattr(node, "body", None)
        if (
            isinstance(body, list)
            and body
            and isinstance(body[0], ast.Expr)
            and isinstance(body[0].value, ast.Constant)
        ):
            self.docstrings.add(id(body[0].value))

    def visit_Module(self, node: ast.Module) -> None:
        self._scope(node)
        self.generic_visit(node)

    def _enter(self, node: ast.AST) -> None:
        self._scope(node)
        self.stack.append(getattr(node, "name", "?"))
        self.generic_visit(node)
        self.stack.pop()

    visit_FunctionDef = _enter
    visit_AsyncFunctionDef = _enter
    visit_ClassDef = _enter

    def visit_Compare(self, node: ast.Compare) -> None:
        operands = [node.left, *node.comparators]
        ids: set[str] = set()
        for operand in operands:
            literals = _string_literals(operand)
            ids |= self._ids(literals)
            if isinstance(operand, (ast.Set, ast.Tuple, ast.List, ast.Call)):
                self.consumed.add(id(operand))
                if isinstance(operand, ast.Call) and operand.args:
                    self.consumed.add(id(operand.args[0]))
        if ids:
            self._record("compare", ids, _skeleton(node))
        self.generic_visit(node)

    def visit_Match(self, node: ast.Match) -> None:
        for case in node.cases:
            patterns = (
                case.pattern.patterns
                if isinstance(case.pattern, ast.MatchOr)
                else [case.pattern]
            )
            values = [
                p.value
                for p in patterns
                if isinstance(p, ast.MatchValue) and isinstance(p.value, ast.Constant)
            ]
            ids = self._ids(values)
            if ids:
                shape = f"{_skeleton(node.subject)} {_skeleton(case.pattern)}"
                self._record("match", ids, shape)
        self.generic_visit(node)

    def _collection(self, node: ast.Set | ast.Tuple | ast.List) -> None:
        if id(node) not in self.consumed:
            ids = self._ids(_string_literals(node))
            if len(ids) >= 2:
                self._record("collection", ids, _skeleton(node))
        self.generic_visit(node)

    visit_Set = _collection
    visit_Tuple = _collection
    visit_List = _collection

    def visit_Dict(self, node: ast.Dict) -> None:
        keys = [
            k
            for k in node.keys
            if isinstance(k, ast.Constant) and isinstance(k.value, str)
        ]
        ids = self._ids(keys)
        if len(ids) >= 2:
            self._record(
                "provider-keyed-dict", ids, ", ".join(sorted(k.value for k in keys))
            )
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        for keyword in node.keywords:
            if keyword.arg and _PROVIDER_KEYWORD.search(keyword.arg):
                ids = self._ids(_string_literals(keyword.value))
                if ids:
                    shape = (
                        f"{_skeleton(node.func)}({keyword.arg}="
                        f"{_skeleton(keyword.value)})"
                    )
                    self._record("provider-keyword", ids, shape)
        self.generic_visit(node)

    def visit_Constant(self, node: ast.Constant) -> None:
        if isinstance(node.value, str) and id(node) not in self.docstrings:
            predicates = sql_provider_literals(node.value, self.ids)
            if predicates:
                ids: set[str] = set()
                for predicate in predicates:
                    ids |= set(_SQL_QUOTED.findall(predicate)) & self.ids
                self._record("sql-literal", ids, " | ".join(predicates))


def sites_for_source(
    rel_path: str, source: str, ids: frozenset[str] | None = None
) -> list[Site]:
    visitor = _Visitor(ids if ids is not None else provider_ids())
    visitor.visit(ast.parse(source, filename=rel_path))
    counters: Counter[tuple[str, str, str, str]] = Counter()
    sites: list[Site] = []
    for raw in visitor.raw:
        counters[raw] += 1
        sites.append(Site(rel_path, *raw, counters[raw]))
    return sites


def is_allowlisted(rel_path: str) -> bool:
    return rel_path.startswith(ALLOWLIST)


def source_files() -> list[Path]:
    return sorted(
        path
        for path in SRC.rglob("*.py")
        if "__pycache__" not in path.parts
        and not is_allowlisted(path.relative_to(REPO_ROOT).as_posix())
    )


def collect_sites() -> list[Site]:
    paths = source_files()
    if not paths:
        raise RuntimeError("No Python sources discovered for the provider inventory")
    ids = provider_ids()
    sites: list[Site] = []
    for path in paths:
        rel = path.relative_to(REPO_ROOT).as_posix()
        sites.extend(sites_for_source(rel, path.read_text(), ids))
    sites.sort(key=lambda site: site.key)
    return sites


def read_classifications(text: str | None = None) -> dict[Key, tuple[str, str]]:
    """Each manifest site's ``(classification, reason)``."""
    if text is None:
        if not MANIFEST.exists():
            return {}
        text = MANIFEST.read_text()
    result: dict[Key, tuple[str, str]] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        parts = re.split(r"\s{2,}", line, maxsplit=7)
        if len(parts) < 7 or not parts[5].startswith("#"):
            raise ValueError(f"malformed cloud-provider manifest line: {raw_line}")
        key = (*parts[:5], int(parts[5][1:]))
        if key in result:
            raise ValueError(f"duplicate cloud-provider manifest site: {raw_line}")
        result[key] = (parts[6], parts[7] if len(parts) == 8 else "")
    return result


HEADER = """\
# Main-cloud provider branches outside the adapters: maintained by
# scripts/check_cloud_provider_branches.py (its docstring says what it finds).
# Regenerate with `python scripts/check_cloud_provider_branches.py --write`;
# a new site is marked `unclassified` until reviewed, and fails the tests.
#
# Ask the provider support matrix (`provider_offers`) or call the adapter
# rather than classifying a site. Every classification needs a reason:
#   sql               a provider test in SQL text (the protected tables are
#                     Nextcloud-only by trigger, migration 0186)
#   protected-record  a protected record only the Nextcloud adapter writes;
#                     frozen at its reviewed sites
#   legacy-column     the pre-abstraction Nextcloud columns; frozen
# SQL files, the cockpit and prompts are outside this gate.
#
# <file>  <qualname>  <kind>  <provider ids>  <fingerprint>  #<ordinal>  <classification>  [<reason>]

"""


def render_manifest(
    sites: list[Site], classifications: dict[Key, tuple[str, str]] | None = None
) -> str:
    classifications = classifications or {}
    lines = []
    for site in sites:
        classification, reason = classifications.get(site.key, (UNCLASSIFIED, ""))
        lines.append(site.render(classification, " ".join(reason.split())))
    return HEADER + "\n".join(lines) + ("\n" if lines else "")


def problems(
    sites: list[Site], classifications: dict[Key, tuple[str, str]]
) -> list[str]:
    """Why the inventory fails review, one line per problem."""
    found: list[str] = []
    for site in sites:
        classification, reason = classifications.get(site.key, (UNCLASSIFIED, ""))
        where = f"{site.file} {site.qualname} ({site.kind} {site.ids})"
        if classification == UNCLASSIFIED:
            found.append(f"unclassified: {where}")
        elif classification not in ALLOWED_CLASSIFICATIONS:
            found.append(f"unknown classification {classification!r}: {where}")
        elif (
            classification in FROZEN_SITES
            and site.key not in FROZEN_SITES[classification]
        ):
            found.append(
                f"{classification} is frozen at its reviewed sites (ask the "
                f"provider support matrix instead): {where}"
            )
        elif classification == "sql" and site.kind != "sql-literal":
            found.append(f"sql classifies SQL text only: {where}")
        elif not reason:
            found.append(f"{classification} without a reason: {where}")
    return found


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    sites = collect_sites()
    classifications = read_classifications()
    rendered = render_manifest(sites, classifications)
    if args.write:
        MANIFEST.write_text(rendered)
        print(f"wrote {len(sites)} cloud-provider sites")
        return 0
    if args.check:
        if not MANIFEST.exists() or MANIFEST.read_text() != rendered:
            print("ERROR: cloud-provider manifest is stale", file=sys.stderr)
            return 1
        failures = problems(sites, classifications)
        for failure in failures:
            print(f"ERROR: {failure}", file=sys.stderr)
        if failures:
            return 1
        counts = Counter(classifications[site.key][0] for site in sites)
        summary = ", ".join(f"{n} {name}" for name, n in sorted(counts.items()))
        print(f"OK: {len(sites)} cloud-provider sites ({summary})")
        return 0
    sys.stdout.write(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
