#!/usr/bin/env python3
"""Walk the orchestrator's routes and classify every endpoint by access gate.

Output: one line per endpoint in stable sort order:

    METHOD  /api/path/{param}  pat=<decision>  classification[:detail]

``pat=`` is the personal-access-token scope the route needs, read from the
runtime table itself (``orchestrator.security.token_scopes.classify_route``) on
the composed template: a scope, ``any`` (any scoped token), ``refused``, or
``unmapped`` (PATs refused until the route is classified).

Classifications:
  gated:<gate>     — at least one known gate call in the body or signature
  admin:<gate>     — admin-only (`_require_admin`, or the fleet-scoped
                     `_require_infrastructure_fleet_admin` wrapper)
  internal:<gate>  — authenticated non-user service boundary
  public:<reason>  — opt-out via `# nosec: public <reason>` comment on the
                     line immediately above the decorator
  unscoped         — no gate found; would fail the C2 snapshot test

The CI snapshot lives at policy/endpoint_inventory.txt; a mismatch
fails the regression test and forces a manual review of any new endpoint.

Discovery follows named FastAPI/APIRouter declarations and include_router
edges without importing the application. Constructor and include prefixes are
composed, literal api_route methods are expanded, and WebSockets use METHOD WS.
Unincluded routers are excluded. Unsupported dynamic composition is an error.

The policy inventory covers declared /api, /auth and /wopi routes; other mounted
identities are reported separately. Framework-generated docs/OpenAPI routes are
outside this source inventory. Gate-name classification is separate from route
discovery and is not a proof of authorization behavior or control flow.
"""

from __future__ import annotations

import argparse
import ast
import re
import sys
from dataclasses import dataclass, replace
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
ORCHESTRATOR = REPO_ROOT / "src" / "orchestrator"
MAIN_PY = ORCHESTRATOR / "main.py"
MANIFEST = REPO_ROOT / "policy" / "endpoint_inventory.txt"

HTTP_METHODS = {"get", "post", "put", "patch", "delete", "head", "options", "trace"}
INVENTORY_PREFIXES = ("/api", "/auth", "/wopi")
ROUTE_DECORATORS = HTTP_METHODS | {"api_route", "websocket"}
UNSUPPORTED_REGISTRATIONS = {
    "add_api_route",
    "add_api_websocket_route",
    "add_route",
    "add_websocket_route",
    "mount",
    "route",
    "websocket_route",
    "append",
    "extend",
    "insert",
    "clear",
    "pop",
    "remove",
}

# Functions that gate access. `_require_admin` and `require_approved_user`
# are also gates — even an auth-only gate beats no gate. P4b added
# ``require_internal`` (shared-secret bootstrap for agent ↔ orchestrator)
# and ``require_internal_or_job_access`` (hybrid: agent key bypasses user
# auth, otherwise normal job access check). Infrastructure-metering routes use
# a separate HMAC-signed collector credential; their shared dispatch boundary
# authenticates the exact method, path, body digest, timestamp, and nonce before
# invoking an ingestion operation. The metering *admin* routes wrap
# ``_require_admin`` in ``_require_infrastructure_fleet_admin``, which
# additionally rejects project-scoped MCP admins (403) so activation boundaries
# can only be moved by a real fleet admin; main.py does not follow local
# helpers (see classify_routes), so that wrapper has to be named here.
GATE_NAMES = {
    # Audited WebSocket boundary: services/browser_stream_broker.py validates
    # origin, approved identity and thread ownership before viewer reservation
    # or accept, then rechecks after startup. Behavioral authorization coverage
    # remains in tests/test_shared_browser_broker.py; this is a source label.
    "relay_browser_stream",
    "_dispatch_infrastructure_ingestion",
    "_require_admin",
    "require_admin",
    "_require_infrastructure_fleet_admin",
    "is_internal_call",
    "require_approved_user",
    "require_internal",
    "require_internal_or_job_access",
    "require_job_access",
    "require_project_member",
    "require_project_owner",
    "require_thread_owner",
    "require_datasource_access",
    "require_datasource_owner",
    "require_sudo_request_authority",
    # Per-VM bearer + provision-generation check for guest->orchestrator calls
    # (orchestrator/security/vm_guest.py). Authenticated service boundary, no
    # user identity involved, so it classifies alongside require_internal.
    "require_vm_guest",
    # Controller -> orchestrator lifecycle-HMAC boundary. The signature binds
    # operation, body, timestamp and request/correlation IDs.
    "require_vm_cleanup_authority",
    # Bounded inventory parser precedes this fixed-purpose lifecycle HMAC gate;
    # no inventory is published before it succeeds. HTTP tests prove behavior.
    "require_vm_inventory_authority",
    "user_can_access_any_job",
    "user_can_access_job",
    # job-first then thread-owner resolver — gates session citations whose
    # ``job_id`` is actually a thread id with no ``jobs`` row (citation panel).
    "user_can_access_job_or_thread",
    "user_can_access_datasource",
    "user_can_access_ide_entity",
    # BFF cookie-session resolver — raises 401 when there is no valid session.
    # Auth-only, same tier as require_approved_user.
    "get_current_user",
}


# main.py does not follow local helpers in general (see classify_routes for
# why). These names are the audited exception: each is a thin boundary shared
# by a small family of routes, each calls its gate as the first statement, and
# none reaches a second gate — so following it reports that family's real gate
# rather than conflating it with something else downstream. Following only
# these — never every module-level def — keeps the conflation risk the blanket
# rule guards against, because a bare call to anything not listed here is still
# not followed.
#
# Membership rule, when you are tempted to add one: the helper must call
# exactly one GATE_NAMES function, unconditionally, before any other work.
FOLLOW_LOCAL_MAIN = {
    "_enter_infrastructure_storage_source_shadow",
    "_schedule_infrastructure_storage_source_activation",
    # Officer message actions (officer-reply/-escalate/-ack): `await
    # require_internal(request)` is its first statement, and every later check
    # is officer-identity, not a second gate.
    "_require_officer_route_actor",
}


@dataclass(frozen=True)
class Endpoint:
    method: str
    path: str
    func_name: str
    classification: (
        str  # "gated:<name>" | "admin:_require_admin" | "public:<reason>" | "unscoped"
    )

    @property
    def pat_scope(self) -> str:
        from orchestrator.security.token_scopes import classify_route

        return classify_route(self.method, self.path)

    def render(self) -> str:
        pat = f"pat={self.pat_scope}"
        return (
            f"{self.method.upper():<6} {self.path:<80} {pat:<19} {self.classification}"
        )


@dataclass(frozen=True)
class DiscoveredRoute:
    """A mounted identity and its source declaration; no auth-policy inference."""

    method: str
    path: str
    func_name: str
    source_path: Path
    function_lineno: int
    decorator_lineno: int


class UnsupportedRouteError(ValueError):
    """Source composition cannot be enumerated completely by this static gate."""


@dataclass(frozen=True)
class _QualifiedName:
    name: str


@dataclass(frozen=True)
class _Router:
    source_path: Path
    variable: str
    prefix: str
    is_app: bool


@dataclass(frozen=True)
class _RouteOperation:
    router: _Router
    name: str


@dataclass
class _Source:
    path: Path
    name: str
    tree: ast.Module
    lines: list[str]
    statements: list[ast.stmt]
    bindings: dict[str, list[ast.expr | _QualifiedName]]


def _statements(nodes: list[ast.stmt]) -> list[ast.stmt]:
    """Only literal branches are statically decidable; never evaluate app code."""
    result = []
    for node in nodes:
        if isinstance(node, ast.If) and isinstance(node.test, ast.Constant):
            if type(node.test.value) is bool:
                result.extend(
                    _statements(node.body if node.test.value else node.orelse)
                )
                continue
        result.append(node)
    return result


def _binding_statements(source: _Source) -> list[ast.stmt]:
    """Include explicitly declared lazy package exports as import bindings.

    A package with module __getattr__ may declare its public export targets
    under TYPE_CHECKING. Only literal __all__ names in an import-only block
    participate; conditional route registrations remain unsupported. Runtime
    identity tests must keep these declarations aligned with the lazy exports.
    A present _EXPORTS table must be a literal mapping of public names to
    (relative submodule, attribute) pairs matching those declarations. Other
    dynamic __getattr__ implementations are not evaluated by this scanner.
    """
    statements = source.statements
    if source.path.name != "__init__.py" or not any(
        isinstance(node, ast.FunctionDef) and node.name == "__getattr__"
        for node in statements
    ):
        return statements
    exports = [
        node.value
        for node in statements
        if isinstance(node, ast.Assign)
        and len(node.targets) == 1
        and isinstance(node.targets[0], ast.Name)
        and node.targets[0].id == "__all__"
    ]
    if len(exports) != 1 or not isinstance(exports[0], (ast.List, ast.Tuple)):
        return statements
    if not all(
        isinstance(item, ast.Constant) and isinstance(item.value, str)
        for item in exports[0].elts
    ):
        return statements
    public = {item.value for item in exports[0].elts}
    type_checking = {
        alias.asname or alias.name
        for node in statements
        if isinstance(node, ast.ImportFrom)
        and node.module == "typing"
        and node.level == 0
        for alias in node.names
        if alias.name == "TYPE_CHECKING"
    }
    for node in statements:
        if isinstance(node, (ast.Assign, ast.AnnAssign, ast.AugAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            type_checking.difference_update(
                target.id for target in targets if isinstance(target, ast.Name)
            )
    extra = []
    for node in statements:
        if not (
            isinstance(node, ast.If)
            and isinstance(node.test, ast.Name)
            and node.test.id in type_checking
            and not node.orelse
            and all(isinstance(item, ast.ImportFrom) for item in node.body)
        ):
            continue
        for imported in node.body:
            names = [
                alias
                for alias in imported.names
                if alias.name != "*" and (alias.asname or alias.name) in public
            ]
            if names:
                extra.append(
                    ast.copy_location(
                        ast.ImportFrom(
                            module=imported.module, names=names, level=imported.level
                        ),
                        imported,
                    )
                )
    maps = [
        node.value
        for node in statements
        if isinstance(node, ast.Assign)
        and any(
            isinstance(target, ast.Name) and target.id == "_EXPORTS"
            for target in node.targets
        )
    ]
    if maps and extra:
        error = f"{source.path}:1: lazy _EXPORTS must be a literal mapping matching TYPE_CHECKING exports"
        if len(maps) != 1 or not isinstance(maps[0], ast.Dict):
            raise UnsupportedRouteError(error)
        try:
            mapping = ast.literal_eval(maps[0])
        except (ValueError, TypeError, SyntaxError):
            raise UnsupportedRouteError(error) from None
        if len(mapping) != len(maps[0].keys) or set(mapping) != public:
            raise UnsupportedRouteError(error)
        declared = {}
        for imported in extra:
            module = imported.module or ""
            if imported.level:
                parents = source.name.split(".")
                if imported.level > len(parents):
                    raise UnsupportedRouteError(error)
                module = ".".join(
                    parents[: len(parents) - imported.level + 1]
                    + ([module] if module else [])
                )
            for alias in imported.names:
                name = alias.asname or alias.name
                target = f"{module}.{alias.name}"
                if name in declared and declared[name] != target:
                    raise UnsupportedRouteError(error)
                declared[name] = target
        for name, target in mapping.items():
            if (
                not isinstance(target, tuple)
                or len(target) != 2
                or not all(isinstance(item, str) and item for item in target)
            ):
                raise UnsupportedRouteError(error)
            if declared.get(name) != f"{source.name}.{target[0]}.{target[1]}":
                raise UnsupportedRouteError(error)
    return [*statements, *extra]


class _RouteDiscovery:
    """Resolve named, declarative routers and their include graph, without imports.

    This is intentionally not a Python evaluator. Factories, dynamic paths or
    methods, conditional registration and mutation of imported routers require
    an explicit supported form before this gate can claim a complete inventory.
    """

    def __init__(self, main_path: Path):
        self.main_path = main_path.resolve()
        self.source_root = self.main_path.parent
        while (self.source_root / "__init__.py").is_file():
            self.source_root = self.source_root.parent
        self.sources: dict[Path, _Source] = {}
        self.resolving: set[tuple[Path, str]] = set()
        self.resolved: dict[
            tuple[Path, str], _Router | _RouteOperation | _QualifiedName | None
        ] = {}

    def fail(self, source: _Source, node: ast.AST, message: str):
        raise UnsupportedRouteError(
            f"{source.path}:{getattr(node, 'lineno', 1)}: {message}"
        )

    def source(self, path: Path) -> _Source:
        path = path.resolve()
        if path in self.sources:
            return self.sources[path]
        text = path.read_text()
        tree = ast.parse(text, filename=str(path))
        parts = list(path.relative_to(self.source_root).with_suffix("").parts)
        if parts[-1] == "__init__":
            parts.pop()
        name = ".".join(parts)
        source = _Source(
            path, name, tree, text.splitlines(), _statements(tree.body), {}
        )
        self.sources[path] = source
        package = name if path.name == "__init__.py" else name.rpartition(".")[0]
        for node in _binding_statements(source):
            entries = []
            if isinstance(node, ast.ImportFrom):
                module = node.module or ""
                if node.level:
                    parents = package.split(".") if package else []
                    if node.level > len(parents):
                        self.fail(
                            source, node, "relative import escapes the source package"
                        )
                    module = ".".join(
                        parents[: len(parents) - node.level + 1]
                        + ([module] if module else [])
                    )
                entries = [
                    (
                        alias.asname or alias.name,
                        _QualifiedName(f"{module}.{alias.name}"),
                    )
                    for alias in node.names
                    if alias.name != "*"
                ]
            elif isinstance(node, ast.Import):
                entries = [
                    (
                        alias.asname or alias.name.split(".")[0],
                        _QualifiedName(
                            alias.name if alias.asname else alias.name.split(".")[0]
                        ),
                    )
                    for alias in node.names
                ]
            elif isinstance(node, ast.Assign):
                entries = [
                    (target.id, node.value)
                    for target in node.targets
                    if isinstance(target, ast.Name)
                ]
            elif (
                isinstance(node, ast.AnnAssign)
                and isinstance(node.target, ast.Name)
                and node.value is not None
            ):
                entries = [(node.target.id, node.value)]
            for variable, value in entries:
                source.bindings.setdefault(variable, []).append(value)
        return source

    def qualified(self, name: str):
        parts = name.split(".")
        for length in range(len(parts), 0, -1):
            candidate = self.source_root.joinpath(*parts[:length])
            paths = (candidate.with_suffix(".py"), candidate / "__init__.py")
            path = next((path for path in paths if path.is_file()), None)
            if path is None:
                continue
            if length == len(parts):
                return _QualifiedName(name)
            value = self.binding(self.source(path), parts[length])
            for attribute in parts[length + 1 :]:
                value = self.attribute(value, attribute)
            return value
        return _QualifiedName(name)

    def attribute(self, value, attribute: str):
        if isinstance(value, _QualifiedName):
            return self.qualified(f"{value.name}.{attribute}")
        if isinstance(value, _Router) and value.is_app and attribute == "router":
            return value
        if isinstance(
            value, _Router
        ) and attribute in ROUTE_DECORATORS | UNSUPPORTED_REGISTRATIONS | {
            "include_router"
        }:
            return _RouteOperation(value, attribute)
        return None

    def expression(self, source: _Source, node: ast.expr):
        if isinstance(node, ast.Name):
            return self.binding(source, node.id)
        if isinstance(node, ast.Attribute):
            return self.attribute(self.expression(source, node.value), node.attr)
        return None

    def literal(self, source: _Source, node: ast.expr, label: str) -> str:
        if not isinstance(node, ast.Constant) or not isinstance(node.value, str):
            self.fail(source, node, f"{label} must be a literal string")
        return node.value

    def prefix(self, source: _Source, call: ast.Call) -> str:
        if any(keyword.arg is None for keyword in call.keywords):
            self.fail(
                source, call, "expanded keyword arguments can hide route composition"
            )
        node = next(
            (item.value for item in call.keywords if item.arg == "prefix"), None
        )
        prefix = "" if node is None else self.literal(source, node, "router prefix")
        if prefix and (not prefix.startswith("/") or prefix.endswith("/")):
            self.fail(
                source, call, "router prefix must start with '/' and not end with '/'"
            )
        return prefix

    def binding(self, source: _Source, variable: str):
        key = (source.path, variable)
        if key in self.resolved:
            return self.resolved[key]
        values = source.bindings.get(variable, [])
        if not values:
            return None
        if len(values) != 1:
            self.fail(
                source,
                source.tree,
                f"route-relevant binding {variable!r} is reassigned",
            )
        if key in self.resolving:
            self.fail(
                source, source.tree, f"cyclic route reference through {variable!r}"
            )
        self.resolving.add(key)
        try:
            value = values[0]
            if isinstance(value, _QualifiedName):
                result = self.qualified(value.name)
            elif isinstance(value, ast.Call):
                callee = self.expression(source, value.func)
                name = callee.name if isinstance(callee, _QualifiedName) else ""
                if name in {
                    "fastapi.APIRouter",
                    "fastapi.routing.APIRouter",
                    "fastapi.FastAPI",
                    "fastapi.applications.FastAPI",
                }:
                    if any(
                        item.arg in {"routes", "route_class"} for item in value.keywords
                    ):
                        self.fail(
                            source,
                            value,
                            "custom route objects/classes are not supported",
                        )
                    result = _Router(
                        source.path,
                        variable,
                        self.prefix(source, value),
                        name.endswith(".FastAPI"),
                    )
                else:
                    result = None
            else:
                result = self.expression(source, value)
            self.resolved[key] = result
            return result
        finally:
            self.resolving.remove(key)

    def mentions(self, source: _Source, node: ast.AST, router: _Router) -> bool:
        for item in ast.walk(node):
            if isinstance(item, (ast.Name, ast.Attribute)):
                value = self.expression(source, item)
                if (
                    value == router
                    or isinstance(value, _RouteOperation)
                    and value.router == router
                ):
                    return True
        return False

    def events(self, router: _Router):
        source = self.source(router.source_path)
        for node in source.statements:
            if isinstance(node, (ast.Assign, ast.AnnAssign, ast.AugAssign)):
                for call in ast.walk(node):
                    if isinstance(call, ast.Call) and isinstance(
                        call.func, ast.Attribute
                    ):
                        if (
                            call.func.attr
                            in ROUTE_DECORATORS
                            | UNSUPPORTED_REGISTRATIONS
                            | {"include_router"}
                            and self.mentions(source, call.func.value, router)
                        ):
                            self.fail(
                                source,
                                call,
                                "registration inside an assignment is not supported",
                            )
                targets = (
                    node.targets if isinstance(node, ast.Assign) else [node.target]
                )
                if any(
                    isinstance(attribute, ast.Attribute)
                    and attribute.attr in {"routes", "router", "prefix", "route_class"}
                    and self.expression(source, attribute.value) == router
                    for target in targets
                    for attribute in ast.walk(target)
                ):
                    self.fail(
                        source, node, "assignment to router state is not supported"
                    )
            elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                for decorator in reversed(node.decorator_list):
                    if not isinstance(decorator, ast.Call):
                        continue
                    if not isinstance(decorator.func, ast.Attribute):
                        if self.mentions(source, decorator, router):
                            self.fail(
                                source,
                                decorator,
                                "dynamic route decorator is not supported",
                            )
                        continue
                    owner = self.expression(source, decorator.func.value)
                    method = decorator.func.attr
                    if method not in ROUTE_DECORATORS | UNSUPPORTED_REGISTRATIONS:
                        continue
                    if isinstance(owner, _Router) and owner.source_path != source.path:
                        self.fail(
                            source,
                            decorator,
                            "registration on an imported router is not supported",
                        )
                    if owner != router:
                        continue
                    if method not in ROUTE_DECORATORS:
                        self.fail(
                            source, decorator, f"{method} registration is not supported"
                        )
                    yield node.lineno, (node, decorator)
            elif isinstance(node, ast.Expr) and isinstance(node.value, ast.Call):
                call = node.value
                if not isinstance(call.func, ast.Attribute):
                    if self.mentions(source, call, router):
                        self.fail(
                            source,
                            call,
                            "passing a router to a registration helper is not supported",
                        )
                    continue
                owner = self.expression(source, call.func.value)
                if owner == router and call.func.attr == "include_router":
                    yield node.lineno, call
                elif call.func.attr in UNSUPPORTED_REGISTRATIONS and self.mentions(
                    source, call.func.value, router
                ):
                    self.fail(
                        source,
                        call,
                        f"{call.func.attr} route mutation is not supported",
                    )
                elif (
                    owner != router
                    and not (
                        isinstance(owner, _Router)
                        and call.func.attr == "include_router"
                    )
                    and any(
                        self.mentions(source, argument, router)
                        for argument in [
                            *call.args,
                            *(item.value for item in call.keywords),
                        ]
                    )
                ):
                    self.fail(
                        source,
                        call,
                        "passing a router to a registration helper is not supported",
                    )
            elif isinstance(
                node,
                (
                    ast.If,
                    ast.For,
                    ast.AsyncFor,
                    ast.While,
                    ast.With,
                    ast.AsyncWith,
                    ast.Try,
                    ast.ClassDef,
                ),
            ):
                for call in ast.walk(node):
                    if isinstance(call, ast.Call) and isinstance(
                        call.func, ast.Attribute
                    ):
                        if (
                            call.func.attr
                            in ROUTE_DECORATORS
                            | UNSUPPORTED_REGISTRATIONS
                            | {"include_router"}
                            and self.mentions(source, call.func.value, router)
                        ):
                            self.fail(
                                source,
                                call,
                                "conditional, looped or nested route registration is not supported",
                            )

    def declared(self, source: _Source, router: _Router, function, call: ast.Call):
        if any(item.arg is None for item in call.keywords):
            self.fail(source, call, "expanded decorator arguments are not supported")
        path_node = (
            call.args[0]
            if call.args
            else next(
                (item.value for item in call.keywords if item.arg == "path"), None
            )
        )
        if path_node is None:
            self.fail(source, call, "route path is missing")
        path = router.prefix + self.literal(source, path_node, "route path")
        method = call.func.attr
        if method == "websocket":
            methods = ["WS"]
        elif method == "api_route":
            node = next(
                (item.value for item in call.keywords if item.arg == "methods"), None
            )
            if node is None or (isinstance(node, ast.Constant) and node.value is None):
                methods = ["GET"]
            elif isinstance(node, (ast.List, ast.Tuple, ast.Set)) and node.elts:
                methods = [
                    self.literal(source, item, "HTTP method").upper()
                    for item in node.elts
                ]
                if any(
                    not re.fullmatch(r"[!#$%&'*+.^_`|~0-9A-Z-]+", method)
                    for method in methods
                ):
                    self.fail(
                        source,
                        node,
                        "HTTP methods must be nonempty literal method names",
                    )
            else:
                self.fail(
                    source,
                    node,
                    "api_route methods must be a nonempty literal sequence",
                )
        else:
            methods = [method.upper()]
        return [
            DiscoveredRoute(
                method, path, function.name, source.path, function.lineno, call.lineno
            )
            for method in sorted(set(methods))
        ]

    def expand(
        self, router: _Router, stack: tuple[_Router, ...] = (), included_at=None
    ):
        source = self.source(router.source_path)
        if router in stack:
            self.fail(source, source.tree, "cyclic include_router graph")
        events = list(self.events(router))
        if included_at and included_at[0] == source.path:
            if any(line > included_at[1] for line, _event in events):
                self.fail(
                    source,
                    source.tree,
                    "route registration after include_router is version-dependent; declare routes before inclusion",
                )
        result = []
        for _line, event in events:
            if isinstance(event, tuple):
                function, call = event
                result.extend(self.declared(source, router, function, call))
                continue
            prefix = router.prefix + self.prefix(source, event)
            child_node = (
                event.args[0]
                if event.args
                else next(
                    (item.value for item in event.keywords if item.arg == "router"),
                    None,
                )
            )
            child = self.expression(source, child_node) if child_node else None
            if not isinstance(child, _Router) or child.is_app:
                self.fail(
                    source,
                    event,
                    "include_router requires a named APIRouter from a resolvable source module",
                )
            result.extend(
                replace(route, path=prefix + route.path)
                for route in self.expand(
                    child, (*stack, router), (source.path, event.lineno)
                )
            )
        return result


def discover_routes(
    main_path: Path = MAIN_PY, *, app_var: str = "app"
) -> list[DiscoveredRoute]:
    """Enumerate declared mounted identities, including WS and out-of-policy paths.

    Framework-generated docs/OpenAPI routes and ASGI mounts are not inferred.
    Unsupported source registration forms fail explicitly rather than producing
    a misleading partial inventory.
    """
    if not main_path.is_file():
        raise FileNotFoundError(f"Orchestrator route source is missing: {main_path}")
    discovery = _RouteDiscovery(main_path)
    source = discovery.source(main_path)
    app = discovery.binding(source, app_var)
    if app is None:
        if any(
            isinstance(node, ast.Name)
            and isinstance(node.ctx, ast.Load)
            and node.id == app_var
            for node in ast.walk(source.tree)
        ):
            discovery.fail(
                source, source.tree, f"{app_var!r} must be a named FastAPI instance"
            )
        return []
    if not isinstance(app, _Router) or not app.is_app:
        discovery.fail(
            source, source.tree, f"{app_var!r} must be a named FastAPI instance"
        )
    return sorted(discovery.expand(app), key=lambda route: (route.path, route.method))


def in_inventory_scope(route: DiscoveredRoute) -> bool:
    return any(
        route.path == prefix or route.path.startswith(prefix + "/")
        for prefix in INVENTORY_PREFIXES
    )


def _local_functions(
    tree: ast.Module,
) -> dict[str, ast.FunctionDef | ast.AsyncFunctionDef]:
    """Module-level defs, so `_gate_calls` can follow a handler into its helper."""
    return {
        node.name: node
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }


def _gate_calls(
    func: ast.FunctionDef | ast.AsyncFunctionDef,
    local_funcs: dict[str, ast.FunctionDef | ast.AsyncFunctionDef] | None = None,
    _seen: set[str] | None = None,
) -> list[str]:
    """Names of gate functions invoked in the body or as Depends() in signature.

    Calls into a module-local helper are followed (cycle-guarded), because the
    routers routinely wrap their gate in a private one — contacts'
    ``_owned_contact``, canvases' ``_require_delegated_owner`` — and reporting
    those handlers as ungated would be a false positive, not a finding.
    """
    local_funcs = local_funcs or {}
    _seen = _seen if _seen is not None else set()
    found: list[str] = []

    def _resolve(name: str | None, *, bare: bool) -> None:
        """``bare`` = called as `helper(...)`, not `something.helper(...)`.

        Only bare names may be followed into a local def. `db.get_project_contacts()`
        is a database method that happens to share a name with the route handler
        of the same resource — recursing on attribute calls picks up that
        handler's gate and reports it as this endpoint's.
        """
        if name is None:
            return
        if name in GATE_NAMES:
            found.append(name)
        elif bare and name in local_funcs and name not in _seen:
            _seen.add(name)
            found.extend(_gate_calls(local_funcs[name], local_funcs, _seen))

    # Body: look for Call nodes whose func name is a gate
    for node in ast.walk(func):
        if isinstance(node, ast.Call):
            target = node.func
            name: str | None = None
            bare = False
            if isinstance(target, ast.Name):
                name, bare = target.id, True
            elif isinstance(target, ast.Attribute):
                name = target.attr
            _resolve(name, bare=bare)
    # Signature: Depends(gate) markers in default values
    for default in func.args.defaults + func.args.kw_defaults:
        if not isinstance(default, ast.Call):
            continue
        target = default.func
        if not (isinstance(target, ast.Name) and target.id == "Depends"):
            continue
        if not default.args:
            continue
        dep_target = default.args[0]
        dep_name: str | None = None
        dep_bare = False
        if isinstance(dep_target, ast.Name):
            dep_name, dep_bare = dep_target.id, True
        elif isinstance(dep_target, ast.Attribute):
            dep_name = dep_target.attr
        _resolve(dep_name, bare=dep_bare)
    return found


def _classify(
    func: ast.FunctionDef | ast.AsyncFunctionDef,
    public_reason: str | None,
    local_funcs: dict[str, ast.FunctionDef | ast.AsyncFunctionDef] | None = None,
) -> str:
    if public_reason is not None:
        return f"public:{public_reason}" if public_reason else "public"
    gates = _gate_calls(func, local_funcs)
    if not gates:
        return "unscoped"
    # Pick the label that describes the typical-caller path. Resource-bound
    # gates beat the admin gate because admin is usually a fallback when the
    # endpoint also exposes a per-resource path. Endpoints that ONLY check
    # admin still get labeled "admin:_require_admin".
    priority = {
        "require_internal_or_job_access": 0,
        "require_job_access": 0,
        "require_project_owner": 0,
        "require_project_member": 0,
        "require_thread_owner": 0,
        "require_datasource_owner": 0,
        "require_datasource_access": 0,
        "require_sudo_request_authority": 0,
        "user_can_access_any_job": 1,
        "user_can_access_job": 1,
        "user_can_access_job_or_thread": 1,
        "user_can_access_datasource": 1,
        "user_can_access_ide_entity": 1,
        "_require_admin": 2,
        "require_admin": 2,
        "_require_infrastructure_fleet_admin": 2,
        "_dispatch_infrastructure_ingestion": 2,
        "require_internal": 2,
        "require_vm_guest": 2,
        "require_vm_cleanup_authority": 2,
        "require_vm_inventory_authority": 2,
        "is_internal_call": 2,
        "require_approved_user": 3,
    }
    primary = sorted(gates, key=lambda g: priority.get(g, 99))[0]
    if primary == "require_admin":
        # The extracted gate preserves the existing audited policy identity.
        return "admin:_require_admin"
    if primary in ("_require_admin", "_require_infrastructure_fleet_admin"):
        return f"admin:{primary}"
    if primary in (
        "_dispatch_infrastructure_ingestion",
        "require_internal",
        "require_vm_guest",
        "require_vm_cleanup_authority",
        "require_vm_inventory_authority",
        "is_internal_call",
    ):
        return f"internal:{primary}"
    return f"gated:{primary}"


def _public_reason(source_lines: list[str], decorator_lineno: int) -> str | None:
    """Read `# nosec: public <reason>` on the line directly above the decorator.

    The decorator's lineno is 1-based; the comment, if present, is on the
    previous source line. Stops at the first non-comment/blank line.
    """
    idx = decorator_lineno - 2  # line above decorator (0-based)
    while idx >= 0:
        line = source_lines[idx].strip()
        if not line:
            idx -= 1
            continue
        if not line.startswith("#"):
            return None
        if line.startswith("# nosec: public"):
            reason = line[len("# nosec: public") :].strip(": ").strip()
            return reason
        idx -= 1
    return None


def classify_routes(
    routes: list[DiscoveredRoute], *, main_path: Path = MAIN_PY
) -> list[Endpoint]:
    """Apply the existing static gate-name policy after identity discovery.

    A gate name is source evidence, not a control-flow or authorization proof.
    Preserve the main-module helper allowlist and router-local helper behavior.
    Router-level/include dependencies are deliberately not reclassified here.
    """
    sources = {}
    endpoints = []
    for route in routes:
        if route.source_path not in sources:
            text = route.source_path.read_text()
            tree = ast.parse(text, filename=str(route.source_path))
            functions = _local_functions(tree)
            local = functions
            if route.source_path == main_path.resolve():
                local = {
                    name: node
                    for name, node in functions.items()
                    if name in FOLLOW_LOCAL_MAIN
                }
            sources[route.source_path] = (
                text.splitlines(),
                {
                    node.lineno: node
                    for node in ast.walk(tree)
                    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                },
                local,
            )
        lines, functions, local = sources[route.source_path]
        function = functions[route.function_lineno]
        endpoints.append(
            Endpoint(
                route.method,
                route.path,
                route.func_name,
                _classify(
                    function, _public_reason(lines, route.decorator_lineno), local
                ),
            )
        )
    return endpoints


def collect_endpoints(main_path: Path = MAIN_PY) -> list[Endpoint]:
    return classify_routes(
        [route for route in discover_routes(main_path) if in_inventory_scope(route)],
        main_path=main_path,
    )


def render_manifest(endpoints: list[Endpoint]) -> str:
    header = (
        "# orchestrator endpoint inventory — generated by scripts/check_endpoint_auth.py\n"
        "# DO NOT EDIT BY HAND. Regenerate with `python scripts/check_endpoint_auth.py --write`.\n"
        "#\n"
        "# SCOPE: declared mounted /api, /auth and /wopi routes, including all literal\n"
        "# HTTP methods and WS. Router constructor/include prefixes are composed;\n"
        "# unmounted routers and framework-generated docs/OpenAPI routes are excluded.\n"
        "# Other mounted paths are reported separately; unsupported composition fails.\n"
        "# Gate labels are static source evidence, not authorization/control-flow proof.\n"
        "#\n"
        "# pat=<decision> — personal access token scope the route needs, from\n"
        "#   orchestrator/security/token_scopes.py: a scope, `any` (any scoped token),\n"
        "#   `refused`, or `unmapped` (refused until classified; a test fails).\n"
        "#\n"
        "# Classifications:\n"
        "#   gated:<gate>           — protected by a require_* / user_can_access_* helper\n"
        "#   admin:<gate>           — admin-only; the fleet-scoped variant also\n"
        "#                            rejects project-scoped MCP admins\n"
        "#   internal:<helper>      — authenticated non-user service boundary\n"
        "#   public:<reason>        — opt-out via `# nosec: public <reason>` on line above decorator\n"
        "#   unscoped               — no gate detected; CI snapshot test will fail\n"
        "\n"
    )
    body = "\n".join(e.render() for e in endpoints)
    return header + body + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--write",
        action="store_true",
        help="Write the manifest to policy/endpoint_inventory.txt",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Exit non-zero if the committed manifest is stale or unscoped endpoints exist",
    )
    args = parser.parse_args()

    try:
        routes = discover_routes()
    except UnsupportedRouteError as exc:
        print(f"ERROR: unsupported route composition: {exc}", file=sys.stderr)
        return 2
    excluded = [route for route in routes if not in_inventory_scope(route)]
    if excluded:
        print(
            f"# {len(excluded)} declared mounted route(s) outside inventory scope:",
            file=sys.stderr,
        )
        for route in excluded:
            print(f"#   {route.method} {route.path}", file=sys.stderr)
    endpoints = classify_routes(
        [route for route in routes if in_inventory_scope(route)]
    )
    rendered = render_manifest(endpoints)

    unscoped = [e for e in endpoints if e.classification == "unscoped"]

    if args.write:
        MANIFEST.parent.mkdir(parents=True, exist_ok=True)
        MANIFEST.write_text(rendered)
        print(f"wrote {len(endpoints)} endpoints to {MANIFEST.relative_to(REPO_ROOT)}")
        if unscoped:
            print(f"WARNING: {len(unscoped)} unscoped endpoints in inventory:")
            for e in unscoped:
                print(f"  {e.render()}")
        return 0

    if args.check:
        if not MANIFEST.exists():
            print(f"ERROR: {MANIFEST} missing — run with --write", file=sys.stderr)
            return 2
        on_disk = MANIFEST.read_text()
        if on_disk != rendered:
            print(
                f"ERROR: {MANIFEST.relative_to(REPO_ROOT)} is stale.\n"
                "Run `python scripts/check_endpoint_auth.py --write` and commit.",
                file=sys.stderr,
            )
            return 1
        if unscoped:
            print(
                f"ERROR: {len(unscoped)} unscoped endpoints detected:",
                file=sys.stderr,
            )
            for e in unscoped:
                print(f"  {e.render()}", file=sys.stderr)
            print(
                "\nAdd a `Depends(require_*)` / in-body gate, or mark the endpoint\n"
                "`# nosec: public <reason>` on the line above the decorator.",
                file=sys.stderr,
            )
            return 1
        print(f"OK: {len(endpoints)} endpoints, all gated.")
        return 0

    # Default: print to stdout
    sys.stdout.write(rendered)
    if unscoped:
        print(
            f"\n# {len(unscoped)} unscoped endpoint(s) above — would fail --check",
            file=sys.stderr,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
