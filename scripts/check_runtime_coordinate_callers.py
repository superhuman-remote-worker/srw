#!/usr/bin/env python3
"""Inventory control-plane calls that consume raw runtime coordinates.

Pod IPs, SSH hosts and derived endpoint URLs are routing hints, not authority.
This scanner finds network/effect calls derived from those coordinates. The
policy manifest assigns every site an explicit reviewed trust classification;
a newly discovered site is rendered ``unclassified`` and fails CI.

A site's identity is its *call*, not its position: the enclosing qualname, the
call target, and a normalized fingerprint of the call's argument structure. An
ordinal only discriminates between genuinely identical duplicate calls in the
same function. Reordering unrelated calls therefore keeps every reviewed
classification, while swapping the target or reshaping the arguments mints a
new, deliberately ``unclassified`` site.
"""

from __future__ import annotations

import argparse
import ast
import hashlib
import re
import sys
from dataclasses import dataclass
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
ORCHESTRATOR = REPO_ROOT / "src" / "orchestrator"
MANIFEST = REPO_ROOT / "policy" / "runtime_coordinate_callers.txt"

ALLOWED_CLASSIFICATIONS = frozenset(
    {
        "exact-runtime-recipient",
        "fresh-k8s-attestation",
        "pinned-host-key",
        "vm-authority",
        "local-only",
        "read-only-probe",
        "contained-k8s",
    }
)

_COORDINATE_NAME = re.compile(
    r"(^|_)(pod_ip|pod_port|ssh_host|ssh_port|agent_url|workspace_url|"
    r"endpoint_url|ws_host|target_host|target_port|remote_host|remote_port)($|_)"
)
_COORDINATE_KEYS = frozenset(
    {
        "pod_ip",
        "pod_port",
        "ssh_host",
        "ssh_port",
        "agent_url",
        "workspace_url",
        "endpoint_url",
    }
)
_HTTP_METHODS = frozenset(
    {"get", "post", "put", "patch", "delete", "request", "stream"}
)
_SOCKET_METHODS = frozenset(
    {"connect", "connect_ex", "open_connection", "create_connection"}
)
_REMOTE_EFFECT_HINTS = (
    "remote",
    "ssh",
    "upload",
    "download",
    "snapshot",
    "archive",
    "seed",
    "stage",
    "mount",
    "exec",
    "proxy",
)


def _roots() -> list[Path]:
    # main.py is only the entrypoint once the application composition package
    # (orchestrator/application/) owns what used to live in it; scan both.
    roots = [ORCHESTRATOR / "main.py"]
    for subdir in ("application", "routers", "security", "services"):
        roots.extend(sorted((ORCHESTRATOR / subdir).rglob("*.py")))
    return roots


def _terminal_name(node: ast.expr | None) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return ""


def _qualified_name(node: ast.expr | None) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        prefix = _qualified_name(node.value)
        return f"{prefix}.{node.attr}" if prefix else node.attr
    return ""


_MAX_SKELETON_DEPTH = 4
_MAX_CONSTANT_CHARS = 48


def _skeleton(node: ast.AST | None, depth: int = 0) -> str:
    """Render one expression as a bounded, position-free structural sketch.

    This is deliberately *not* a general static-analysis IR. It captures just
    enough of a call argument to notice that the call changed shape — names,
    attribute paths, literal text, f-string structure, nesting arity — and
    stops at a fixed depth so a large expression cannot make the fingerprint
    unstable or unbounded.
    """

    if node is None:
        return ""
    if depth > _MAX_SKELETON_DEPTH:
        return "..."
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return f"{_skeleton(node.value, depth + 1)}.{node.attr}"
    if isinstance(node, ast.Call):
        arity = len(node.args) + len(node.keywords)
        return f"{_skeleton(node.func, depth + 1)}(/{arity})"
    if isinstance(node, ast.Await):
        return f"await {_skeleton(node.value, depth + 1)}"
    if isinstance(node, ast.Starred):
        return f"*{_skeleton(node.value, depth + 1)}"
    if isinstance(node, ast.Constant):
        text = node.value if isinstance(node.value, str) else repr(node.value)
        text = str(text)
        if len(text) > _MAX_CONSTANT_CHARS:
            text = text[:_MAX_CONSTANT_CHARS] + "..."
        return f"'{text}'" if isinstance(node.value, str) else text
    if isinstance(node, ast.FormattedValue):
        return "{" + _skeleton(node.value, depth + 1) + "}"
    if isinstance(node, ast.JoinedStr):
        return "f'" + "".join(_skeleton(v, depth + 1) for v in node.values) + "'"
    if isinstance(node, ast.Subscript):
        return f"{_skeleton(node.value, depth + 1)}[{_skeleton(node.slice, depth + 1)}]"
    if isinstance(node, ast.Dict):
        items = ", ".join(
            f"{_skeleton(k, depth + 1)}: {_skeleton(v, depth + 1)}"
            for k, v in zip(node.keys, node.values)
        )
        return "{" + items + "}"
    if isinstance(node, (ast.List, ast.Tuple, ast.Set)):
        open_, close = {
            ast.List: ("[", "]"),
            ast.Tuple: ("(", ")"),
            ast.Set: ("{", "}"),
        }[type(node)]
        return open_ + ", ".join(_skeleton(e, depth + 1) for e in node.elts) + close
    if isinstance(node, ast.BinOp):
        return (
            f"{_skeleton(node.left, depth + 1)} "
            f"{type(node.op).__name__} "
            f"{_skeleton(node.right, depth + 1)}"
        )
    if isinstance(node, ast.UnaryOp):
        return f"{type(node.op).__name__} {_skeleton(node.operand, depth + 1)}"
    if isinstance(node, ast.IfExp):
        return (
            f"{_skeleton(node.body, depth + 1)} if "
            f"{_skeleton(node.test, depth + 1)} else "
            f"{_skeleton(node.orelse, depth + 1)}"
        )
    if isinstance(node, ast.BoolOp):
        return (
            f"{type(node.op).__name__}("
            + ", ".join(_skeleton(v, depth + 1) for v in node.values)
            + ")"
        )
    if isinstance(node, ast.Compare):
        return (
            f"{_skeleton(node.left, depth + 1)} "
            + " ".join(type(op).__name__ for op in node.ops)
            + " "
            + " ".join(_skeleton(c, depth + 1) for c in node.comparators)
        )
    return type(node).__name__


def call_signature(call: ast.Call) -> str:
    """Canonical, position-free rendering of a call target and its arguments."""

    parts = [_skeleton(arg) for arg in call.args]
    parts.extend(
        f"{keyword.arg}={_skeleton(keyword.value)}"
        if keyword.arg
        else f"**{_skeleton(keyword.value)}"
        for keyword in call.keywords
    )
    return f"{callee_name(call.func)}({', '.join(parts)})"


def callee_name(func: ast.expr) -> str:
    """The dotted call target, or a stable placeholder for a computed one."""

    return _qualified_name(func) or _terminal_name(func) or "<computed>"


def fingerprint(call: ast.Call) -> str:
    return hashlib.blake2s(
        call_signature(call).encode("utf-8"), digest_size=6
    ).hexdigest()


@dataclass(frozen=True)
class Site:
    file: str
    qualname: str
    kind: str
    callee: str
    fingerprint: str
    ordinal: int

    @property
    def key(self) -> tuple[str, str, str, str, str, int]:
        return (
            self.file,
            self.qualname,
            self.kind,
            self.callee,
            self.fingerprint,
            self.ordinal,
        )

    def render(self, classification: str) -> str:
        return (
            f"{self.file}  {self.qualname}  {self.kind}  {self.callee}  "
            f"{self.fingerprint}  #{self.ordinal}  {classification}"
        )


class _Visitor(ast.NodeVisitor):
    def __init__(self, rel_path: str) -> None:
        self.rel_path = rel_path
        self.stack: list[str] = []
        self.tainted: list[set[str]] = [set()]
        self.raw_sites: list[tuple[str, str, str, str]] = []

    def _qualname(self) -> str:
        return ".".join(self.stack) or "<module>"

    def _enter_scope(self, node: ast.AST) -> None:
        self.stack.append(getattr(node, "name", "?"))
        self.tainted.append(set())
        self.generic_visit(node)
        self.tainted.pop()
        self.stack.pop()

    visit_FunctionDef = _enter_scope
    visit_AsyncFunctionDef = _enter_scope
    visit_ClassDef = _enter_scope

    def _name_is_coordinate(self, value: str) -> bool:
        return bool(_COORDINATE_NAME.search(value.lower()))

    def _expr_is_coordinate(self, node: ast.AST | None) -> bool:
        if node is None:
            return False
        for child in ast.walk(node):
            if isinstance(child, ast.Name) and (
                child.id in self.tainted[-1] or self._name_is_coordinate(child.id)
            ):
                return True
            if isinstance(child, ast.Attribute) and self._name_is_coordinate(
                child.attr
            ):
                return True
            if isinstance(child, ast.Constant) and (
                isinstance(child.value, str) and child.value in _COORDINATE_KEYS
            ):
                return True
        return False

    def _record_assignment(self, targets: list[ast.expr], value: ast.AST) -> None:
        if not self._expr_is_coordinate(value):
            return
        for target in targets:
            if isinstance(target, ast.Name):
                self.tainted[-1].add(target.id)
            elif isinstance(target, (ast.Tuple, ast.List)):
                self._record_assignment(list(target.elts), value)

    def visit_Assign(self, node: ast.Assign) -> None:
        self.visit(node.value)
        self._record_assignment(node.targets, node.value)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        if node.value is not None:
            self.visit(node.value)
            self._record_assignment([node.target], node.value)

    def visit_NamedExpr(self, node: ast.NamedExpr) -> None:
        self.visit(node.value)
        self._record_assignment([node.target], node.value)

    def _call_has_coordinate(self, call: ast.Call) -> bool:
        return any(self._expr_is_coordinate(arg) for arg in call.args) or any(
            self._expr_is_coordinate(keyword.value) for keyword in call.keywords
        )

    def _classify_call(self, call: ast.Call) -> str | None:
        if not self._call_has_coordinate(call):
            return None
        terminal = _terminal_name(call.func).lower()
        qualified = _qualified_name(call.func).lower()
        receiver = (
            _qualified_name(call.func.value).lower()
            if isinstance(call.func, ast.Attribute)
            else ""
        )
        if terminal in _HTTP_METHODS and (
            "http" in receiver
            or "client" in receiver
            or receiver in {"session", "requests", "websocket", "websockets"}
        ):
            return "http"
        if terminal in _SOCKET_METHODS:
            return "socket"
        if "ssh" in qualified or "paramiko" in qualified:
            return "ssh"
        if terminal == "create_subprocess_exec" and any(
            isinstance(arg, ast.Constant)
            and isinstance(arg.value, str)
            and arg.value in {"ssh", "scp", "sftp"}
            for arg in call.args
        ):
            return "ssh"
        if any(hint in terminal for hint in _REMOTE_EFFECT_HINTS):
            return "remote-effect"
        return None

    def visit_Call(self, node: ast.Call) -> None:
        kind = self._classify_call(node)
        if kind is not None:
            self.raw_sites.append(
                (
                    self._qualname(),
                    kind,
                    callee_name(node.func),
                    fingerprint(node),
                )
            )
        self.generic_visit(node)


def sites_for_source(rel_path: str, source: str) -> list[Site]:
    visitor = _Visitor(rel_path)
    visitor.visit(ast.parse(source, filename=rel_path))
    # The ordinal is only a duplicate discriminator: two calls share it solely
    # when their function, kind, target and argument shape are all identical,
    # so it cannot carry a classification across a reorder.
    counters: dict[tuple[str, str, str, str], int] = {}
    sites: list[Site] = []
    for qualname, kind, callee, digest in visitor.raw_sites:
        key = qualname, kind, callee, digest
        counters[key] = counters.get(key, 0) + 1
        sites.append(Site(rel_path, qualname, kind, callee, digest, counters[key]))
    return sites


def collect_sites() -> list[Site]:
    sites: list[Site] = []
    paths = _roots()
    if not paths:
        raise RuntimeError("No Python sources discovered for the policy inventory")
    for path in paths:
        sites.extend(
            sites_for_source(str(path.relative_to(REPO_ROOT)), path.read_text())
        )
    sites.sort(key=lambda item: item.key)
    return sites


def read_classifications() -> dict[tuple[str, str, str, str, str, int], str]:
    if not MANIFEST.exists():
        return {}
    result: dict[tuple[str, str, str, str, str, int], str] = {}
    for raw_line in MANIFEST.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        parts = re.split(r"\s{2,}", line)
        if len(parts) != 7 or not parts[5].startswith("#"):
            raise ValueError(f"malformed runtime-coordinate manifest line: {raw_line}")
        key = parts[0], parts[1], parts[2], parts[3], parts[4], int(parts[5][1:])
        if key in result:
            raise ValueError(f"duplicate runtime-coordinate manifest site: {raw_line}")
        result[key] = parts[6]
    return result


def render_manifest(
    sites: list[Site],
    classifications: dict[tuple[str, str, str, str, str, int], str] | None = None,
) -> str:
    classifications = classifications or {}
    header = (
        "# runtime-coordinate callers — maintained by "
        "scripts/check_runtime_coordinate_callers.py\n"
        "# DO NOT ADD A SITE WITHOUT REVIEWING ITS RECIPIENT/RESOURCE AUTHORITY.\n"
        "# Regenerate with `python scripts/check_runtime_coordinate_callers.py --write`;\n"
        "# new sites are deliberately marked `unclassified` until reviewed.\n"
        "# Classifications identify the reviewed boundary: exact runtime recipient,\n"
        "# fresh Kubernetes attestation, pinned host key, VM authority, local-only,\n"
        "# read-only probe, or a Kubernetes path contained before network I/O.\n"
        "#\n"
        "# A site is identified by its call, not by its position: <callee> is\n"
        "# the call target and <fingerprint> hashes the normalized argument\n"
        "# structure. Reordering calls preserves classifications; changing a\n"
        "# target or an argument shape mints a new `unclassified` site.\n"
        "#\n"
        "# <file>  <enclosing qualname>  <kind>  <callee>  <fingerprint>  "
        "#<ordinal>  <classification>\n\n"
    )
    body = "\n".join(
        site.render(classifications.get(site.key, "unclassified")) for site in sites
    )
    return header + body + ("\n" if body else "")


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
        print(f"wrote {len(sites)} runtime-coordinate sites")
        return 0
    if args.check:
        if not MANIFEST.exists() or MANIFEST.read_text() != rendered:
            print("ERROR: runtime-coordinate manifest is stale", file=sys.stderr)
            return 1
        invalid = {
            value
            for value in classifications.values()
            if value not in ALLOWED_CLASSIFICATIONS
        }
        if invalid:
            print(
                "ERROR: unreviewed classifications: " + ", ".join(sorted(invalid)),
                file=sys.stderr,
            )
            return 1
        print(f"OK: {len(sites)} runtime-coordinate sites classified")
        return 0
    sys.stdout.write(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
