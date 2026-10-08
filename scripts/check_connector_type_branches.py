#!/usr/bin/env python3
"""Inventory code outside the connector drivers that branches on a connector type.

A connector's behaviour belongs to its driver: its spec's flags and its
optional capabilities answer what other code used to ask by comparing
``datasources.type`` against a literal. This scanner finds the places that
still ask, so the count can only go down. Every site carries a reviewed
classification in ``policy/connector_type_branches.txt``; a new site is
rendered ``unclassified`` and fails ``tests/test_connector_type_branches.py``.

**Type ids come from the driver registry**: every built-in spec's
``legacy_type`` and driver name (``shared.connectors``). A new driver extends
the gate without an edit here.

**What it finds**, in every Python module under ``src/``:

* a comparison of a *type expression* with a type id or a literal collection
  holding one. A type expression is ``x["type"]``, ``x.get("type")``,
  ``x.type``, ``x.type_id``, ``x.legacy_type``, ``x.kind``, ``x["driver"]``
  or a name such as
  ``ds_type``, ``datasource_type`` or ``kind``, also through ``str()``,
  ``.lower()``, ``.strip()`` and ``x or ""``;
* a ``match`` on a type expression with a type-id case;
* a set, list or tuple literal naming two or more type ids;
* a dict literal with three or more type-id keys;
* a harness slot read by a type id: ``x.datasources.get("<id>")``,
  ``connections["<id>"]`` and the like, on a receiver named like a slot
  registry (``datasources``, ``connections``, ``datasources_dict``...);
  ``has_datasource("<id>")`` / ``get_datasource("<id>")`` calls;
* ``spec_for_type("<id>")`` with a literal id;
* a type id tested ``in`` a name ending in ``datasource_types``
  (``"email" in facts.attached_datasource_types``);
* ``ds_type="<id>"`` keywords;
* a Python string holding SQL that tests a ``type`` column against a type id
  (``d.type = 'kb'``, ``type IN ('a', 'b')``).

**Allowlisted**: the driver packages, where deciding by type is the point:
``src/shared/connectors/``, ``src/orchestrator/services/connector_drivers/``
and ``src/agent/connectors/``.

**Outside the gate, by design**: SQL files (``migrations/``), the cockpit
(TypeScript) and prompts (Jinja templates under ``config/prompts/``). The AST
cannot see them; D2 replaces the cockpit's copies with the server's answer.

**Classifications** (``legacy-pending``, "still to convert", reached zero
with slice D1 and is retired: a branch is converted to a spec flag or a
driver capability, or it is one of these, with a reason):

* ``not-a-connector-type``: the literal names something else (a cloud mount's
  ``source.type``, an MCP token kind, a tool category);
* ``sql``: a type test in SQL text, listed for D3's platform-owned marker;
* ``kb-domain``: OKF knowledge-base indexing, which stays outside the driver;
* ``platform-owned``: a connector SRW provisions itself (seeded defaults,
  the personal cloud storage, the native KB row and marker), which D3's
  ``managed_key`` marker identifies;
* ``pending-d3-d4``: a branch D1 cannot convert, because the marker it needs
  arrives with D3 (``managed_key``) or D4 (the ``cloud_folder`` driver).
  Frozen at the reviewed sites (:data:`PENDING_D3_D4_SITES`): no new site
  may take it;
* ``driver-internal``: a helper module only drivers call, with their own type
  (import-linter keeps everything else out of it).

A site is identified by what it is, not where: the enclosing qualname, the
kind, the type ids and a fingerprint of the node's structure. An ordinal
only tells identical duplicates in one scope apart. Reordering code within a
function keeps every reviewed classification; reshaping the branch, or
moving it to another function, mints a new site.
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
MANIFEST = REPO_ROOT / "policy" / "connector_type_branches.txt"

#: Driver packages: deciding by type inside them is their job.
ALLOWLIST: tuple[str, ...] = (
    "src/shared/connectors/",
    "src/orchestrator/services/connector_drivers/",
    "src/agent/connectors/",
)

#: Retired with slice D1: no site may be left "to convert" again.
LEGACY_PENDING = "legacy-pending"
#: The reviewed ``pending-d3-d4`` sites, frozen as the legacy-pending
#: baseline was: the class cannot take a new one.
PENDING_D3_D4 = "pending-d3-d4"
PENDING_D3_D4_SITES: frozenset[tuple[str, str, str, str, str, int]] = frozenset(
    {
        (
            "src/orchestrator/services/agent_datasource_payload.py",
            "apply_cloud_storage_override",
            "compare",
            "webdav",
            "31c49729257b",
            1,
        ),
        (
            "src/orchestrator/services/projects.py",
            "update_project",
            "compare",
            "webdav",
            "31c49729257b",
            1,
        ),
    }
)
UNCLASSIFIED = "unclassified"
ALLOWED_CLASSIFICATIONS = frozenset(
    {
        "not-a-connector-type",
        "sql",
        "kb-domain",
        "platform-owned",
        "pending-d3-d4",
        "driver-internal",
    }
)

#: Names that hold a connector type.  ``kind`` is how an identity or a
#: binding often carries one (``identity.kind == "ssh_key"``); it also names
#: unrelated vocabularies (an MCP token's kind), which are classified.
TYPE_NAMES = frozenset(
    {"ds_type", "datasource_type", "connector_type", "type_id", "driver_name", "kind"}
)
#: Attributes that hold one (``row.type``, ``driver.type_id``,
#: ``spec.legacy_type``).
TYPE_ATTRIBUTES = frozenset(
    {"type", "type_id", "legacy_type", "ds_type", "datasource_type", "driver", "kind"}
)
#: Mapping keys that hold one (``row["type"]``, ``row.get("type")``).
TYPE_KEYS = frozenset({"type", "ds_type", "datasource_type", "driver", "kind"})
_UNWRAP_METHODS = frozenset({"lower", "strip", "casefold"})
_SLOT_CALLS = frozenset({"has_datasource", "get_datasource"})
#: Receivers that hold the harness slots (``ToolContext.datasources``, a
#: materializer's ``connections``...): reading one by a type id is a branch.
_SLOT_RECEIVERS = re.compile(
    r"(?:^|_)(?:datasources|connections|datasources_dict|clients)$"
)
#: Calls that look a driver up by its type.
_SPEC_LOOKUPS = frozenset({"spec_for_type"})
#: Collections of attached connector types (``attached_datasource_types``).
_TYPE_COLLECTION_SUFFIX = "datasource_types"
_COLLECTION_CALLS = frozenset({"frozenset", "set", "tuple", "list"})

_SQL_EQUALS = re.compile(
    r"(?:\b|')type'?\s*(?:=|<>|!=)\s*'([A-Za-z0-9_./-]+)'", re.IGNORECASE
)
_SQL_IN = re.compile(r"(?:\b|')type'?\s+(?:NOT\s+)?IN\s*\(([^)]*)\)", re.IGNORECASE)
_SQL_QUOTED = re.compile(r"'([A-Za-z0-9_./-]+)'")

_MAX_SKELETON_DEPTH = 6
_MAX_CONSTANT_CHARS = 48


def registry_type_ids() -> frozenset[str]:
    """Every built-in spec's stored type and driver name."""
    if str(SRC) not in sys.path:
        sys.path.insert(0, str(SRC))
    from shared.connectors.builtin import BUILTIN_SPECS

    ids: set[str] = set()
    for spec in BUILTIN_SPECS:
        ids.add(spec.name)
        if spec.legacy_type:
            ids.add(spec.legacy_type)
    return frozenset(ids)


# ---------------------------------------------------------------------------
# Fingerprints
# ---------------------------------------------------------------------------


def _skeleton(node: ast.AST | None, depth: int = 0) -> str:
    """A bounded, position-free structural sketch of one expression.

    Hand-rolled rather than ``ast.dump`` because ``ast.dump``'s output
    changes between Python versions, and the fingerprint must not.
    """
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
    if isinstance(node, ast.Dict):
        items = ", ".join(
            f"{_skeleton(k, nxt)}: {_skeleton(v, nxt)}"
            for k, v in zip(node.keys, node.values)
        )
        return "{" + items + "}"
    if isinstance(node, ast.Compare):
        ops = " ".join(type(op).__name__ for op in node.ops)
        rest = " ".join(_skeleton(c, nxt) for c in node.comparators)
        return f"{_skeleton(node.left, nxt)} {ops} {rest}"
    if isinstance(node, ast.BoolOp):
        values = ", ".join(_skeleton(v, nxt) for v in node.values)
        return f"{type(node.op).__name__}({values})"
    if isinstance(node, ast.UnaryOp):
        return f"{type(node.op).__name__} {_skeleton(node.operand, nxt)}"
    if isinstance(node, ast.BinOp):
        op = type(node.op).__name__
        return f"{_skeleton(node.left, nxt)} {op} {_skeleton(node.right, nxt)}"
    if isinstance(node, ast.Starred):
        return f"*{_skeleton(node.value, nxt)}"
    if isinstance(node, ast.MatchValue):
        return f"case {_skeleton(node.value, nxt)}"
    if isinstance(node, ast.MatchOr):
        return " | ".join(_skeleton(p, nxt) for p in node.patterns)
    return type(node).__name__


def _digest(text: str) -> str:
    return hashlib.blake2s(text.encode("utf-8"), digest_size=6).hexdigest()


# ---------------------------------------------------------------------------
# Detection
# ---------------------------------------------------------------------------


def _unwrap(node: ast.AST) -> ast.AST:
    """``str(x)``, ``x.lower()``, ``x.strip()`` and ``x or ""`` are still ``x``."""
    while True:
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "str"
            and len(node.args) == 1
        ):
            node = node.args[0]
        elif (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr in _UNWRAP_METHODS
            and not node.args
        ):
            node = node.func.value
        elif isinstance(node, ast.BoolOp) and isinstance(node.op, ast.Or):
            node = node.values[0]
        else:
            return node


def is_type_expression(node: ast.AST) -> bool:
    node = _unwrap(node)
    if isinstance(node, ast.Name):
        return node.id in TYPE_NAMES
    if isinstance(node, ast.Attribute):
        return node.attr in TYPE_ATTRIBUTES
    if isinstance(node, ast.Subscript):
        return isinstance(node.slice, ast.Constant) and node.slice.value in TYPE_KEYS
    if (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "get"
        and node.args
    ):
        key = node.args[0]
        return isinstance(key, ast.Constant) and key.value in TYPE_KEYS
    return False


def _terminal_name(node: ast.AST) -> str:
    """The last identifier of a name or attribute chain (``a.b.c`` -> ``c``)."""
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return ""


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
        and node.func.id in _COLLECTION_CALLS
        and len(node.args) == 1
    ):
        return _string_literals(node.args[0])
    return []


def _collection_node(node: ast.AST) -> ast.AST | None:
    """The literal collection a comparison operand carries, if any."""
    if isinstance(node, (ast.Set, ast.Tuple, ast.List)):
        return node
    if (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id in _COLLECTION_CALLS
        and len(node.args) == 1
        and isinstance(node.args[0], (ast.Set, ast.Tuple, ast.List))
    ):
        return node.args[0]
    return None


def sql_type_literals(text: str, type_ids: frozenset[str]) -> list[str]:
    """``type`` predicates in SQL text that name a type id, normalized."""
    found: list[str] = []
    for match in _SQL_EQUALS.finditer(text):
        if match.group(1) in type_ids:
            found.append(" ".join(match.group(0).split()))
    for match in _SQL_IN.finditer(text):
        if set(_SQL_QUOTED.findall(match.group(1))) & type_ids:
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
    def key(self) -> tuple[str, str, str, str, str, int]:
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
    def __init__(self, type_ids: frozenset[str]) -> None:
        self.type_ids = type_ids
        self.stack: list[str] = []
        self.raw: list[tuple[str, str, str, str]] = []
        #: Collections already reported as part of a comparison.
        self.consumed: set[int] = set()
        self.docstrings: set[int] = set()

    def _record(self, kind: str, ids: set[str], shape: str) -> None:
        qualname = ".".join(self.stack) or "<module>"
        self.raw.append((qualname, kind, ",".join(sorted(ids)), _digest(shape)))

    def _ids(self, nodes: list[ast.Constant]) -> set[str]:
        return {node.value for node in nodes if node.value in self.type_ids}

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
            ids |= self._ids(_string_literals(operand))
        if (
            ids
            and any(isinstance(op, (ast.In, ast.NotIn)) for op in node.ops)
            and any(
                _terminal_name(operand).endswith(_TYPE_COLLECTION_SUFFIX)
                for operand in node.comparators
            )
        ):
            self._record("type-membership", ids, _skeleton(node))
        elif ids and any(is_type_expression(operand) for operand in operands):
            self._record("compare", ids, _skeleton(node))
            for operand in operands:
                collection = _collection_node(operand)
                if collection is not None:
                    self.consumed.add(id(collection))
        self.generic_visit(node)

    def visit_Match(self, node: ast.Match) -> None:
        if is_type_expression(node.subject):
            for case in node.cases:
                patterns = (
                    case.pattern.patterns
                    if isinstance(case.pattern, ast.MatchOr)
                    else [case.pattern]
                )
                values = [
                    p.value
                    for p in patterns
                    if isinstance(p, ast.MatchValue)
                    and isinstance(p.value, ast.Constant)
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

    def visit_Subscript(self, node: ast.Subscript) -> None:
        if _SLOT_RECEIVERS.search(_terminal_name(node.value)) and isinstance(
            node.slice, ast.Constant
        ):
            ids = self._ids(_string_literals(node.slice))
            if ids:
                self._record("registry-slot", ids, _skeleton(node))
        self.generic_visit(node)

    def visit_Dict(self, node: ast.Dict) -> None:
        keys = [
            k
            for k in node.keys
            if isinstance(k, ast.Constant) and isinstance(k.value, str)
        ]
        ids = self._ids(keys)
        if len(ids) >= 3:
            # The keys are the type-keyed shape; editing a value keeps the site.
            self._record(
                "type-keyed-dict", ids, ", ".join(sorted(k.value for k in keys))
            )
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        func = node.func
        name = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", "")
        if name in _SLOT_CALLS and node.args:
            ids = self._ids(_string_literals(node.args[0]))
            if ids:
                self._record("registry-slot", ids, _skeleton(node))
        if (
            name in ("get", "pop", "setdefault")
            and isinstance(func, ast.Attribute)
            and _SLOT_RECEIVERS.search(_terminal_name(func.value))
            and node.args
        ):
            ids = self._ids(_string_literals(node.args[0]))
            if ids:
                self._record("registry-slot", ids, _skeleton(node))
        if name in _SPEC_LOOKUPS and node.args:
            ids = self._ids(_string_literals(node.args[0]))
            if ids:
                self._record("spec-lookup", ids, _skeleton(node))
        for keyword in node.keywords:
            if keyword.arg in TYPE_NAMES:
                ids = self._ids(_string_literals(keyword.value))
                if ids:
                    shape = (
                        f"{_skeleton(func)}({keyword.arg}={_skeleton(keyword.value)})"
                    )
                    self._record("type-keyword", ids, shape)
        self.generic_visit(node)

    def visit_Constant(self, node: ast.Constant) -> None:
        if isinstance(node.value, str) and id(node) not in self.docstrings:
            predicates = sql_type_literals(node.value, self.type_ids)
            if predicates:
                ids: set[str] = set()
                for predicate in predicates:
                    ids |= set(_SQL_QUOTED.findall(predicate)) & self.type_ids
                self._record("sql-literal", ids, " | ".join(predicates))


def sites_for_source(
    rel_path: str, source: str, type_ids: frozenset[str] | None = None
) -> list[Site]:
    visitor = _Visitor(type_ids if type_ids is not None else registry_type_ids())
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
        raise RuntimeError("No Python sources discovered for the connector inventory")
    type_ids = registry_type_ids()
    sites: list[Site] = []
    for path in paths:
        rel = path.relative_to(REPO_ROOT).as_posix()
        sites.extend(sites_for_source(rel, path.read_text(), type_ids))
    sites.sort(key=lambda site: site.key)
    return sites


# ---------------------------------------------------------------------------
# Manifest
# ---------------------------------------------------------------------------

Key = tuple[str, str, str, str, str, int]


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
            raise ValueError(f"malformed connector-type manifest line: {raw_line}")
        key = (*parts[:5], int(parts[5][1:]))
        if key in result:
            raise ValueError(f"duplicate connector-type manifest site: {raw_line}")
        result[key] = (parts[6], parts[7] if len(parts) == 8 else "")
    return result


HEADER = """\
# Connector-type branches outside the drivers: maintained by
# scripts/check_connector_type_branches.py (its docstring says what it finds).
# Regenerate with `python scripts/check_connector_type_branches.py --write`;
# a new site is marked `unclassified` until reviewed, and fails the tests.
#
# Convert a branch to a spec flag or a driver capability rather than
# classifying it. `legacy-pending` reached zero with slice D1 and is retired:
# no site may carry it again. Every classification needs a reason:
#   not-a-connector-type  the literal names something else
#   sql                   a type test in SQL text (D3's platform-owned marker)
#   kb-domain             OKF knowledge-base indexing, outside the driver
#   platform-owned        a connector SRW provisions itself (D3's managed_key)
#   pending-d3-d4         needs a marker D3 or D4 brings; frozen at its reviewed sites
#   driver-internal       a helper only drivers call, with their own type
# SQL files, the cockpit and prompts are outside this gate.
#
# A site is identified by what it is, not where it is: reordering code keeps
# a classification; reshaping the branch mints a new `unclassified` site.
#
# <file>  <qualname>  <kind>  <type ids>  <fingerprint>  #<ordinal>  <classification>  [<reason>]

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
    """Why the inventory fails review, one line per problem.

    ``legacy-pending`` is retired: a site that carries it is a problem, as
    an unclassified one is. Convert the branch instead.
    """
    found: list[str] = []
    for site in sites:
        classification, reason = classifications.get(site.key, (UNCLASSIFIED, ""))
        where = f"{site.file} {site.qualname} ({site.kind} {site.ids})"
        if classification == UNCLASSIFIED:
            found.append(f"unclassified: {where}")
        elif classification == LEGACY_PENDING:
            found.append(f"legacy-pending is retired (convert the branch): {where}")
        elif classification not in ALLOWED_CLASSIFICATIONS:
            found.append(f"unknown classification {classification!r}: {where}")
        elif classification == PENDING_D3_D4 and site.key not in PENDING_D3_D4_SITES:
            found.append(
                f"pending-d3-d4 is frozen at its reviewed sites (convert the "
                f"branch): {where}"
            )
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
        print(f"wrote {len(sites)} connector-type sites")
        return 0
    if args.check:
        if not MANIFEST.exists() or MANIFEST.read_text() != rendered:
            print("ERROR: connector-type manifest is stale", file=sys.stderr)
            return 1
        failures = problems(sites, classifications)
        for failure in failures:
            print(f"ERROR: {failure}", file=sys.stderr)
        if failures:
            return 1
        counts = Counter(classifications[site.key][0] for site in sites)
        summary = ", ".join(f"{n} {name}" for name, n in sorted(counts.items()))
        print(f"OK: {len(sites)} connector-type sites ({summary})")
        return 0
    sys.stdout.write(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
