"""Deterministic bundle resolution, deliberately independent of live admission.

Only caller-supplied definitions are inspected. A successful preview is not a
claim that scopes, secret references, images or retained instances are accessible.
"""

from copy import deepcopy
import hashlib
import json

import yaml

from .errors import fail, pointer
from .parsing import ManifestLoader
from .validation import API_VERSION, MAX_NODES, check_json_value, validate_documents
from .workspace_defaults import project_workspace_defaults

_RESOURCE_MAPS = {
    "experts": "Expert",
    "workspaces": "WorkspaceTemplate",
    "connectors": "Connector",
}
_PENDING_CHECKS = [
    "resourceAuthorization",
    "backendAvailability",
    "imageResolution",
    "credentialDelivery",
    "workspaceInstances",
    "projectActivation",
]


class _ManifestDumper(yaml.SafeDumper):
    # Quote strings such as "1e3" that our JSON-compatible parser would treat
    # as numbers, even though PyYAML's default YAML 1.1 resolver would not.
    yaml_implicit_resolvers = ManifestLoader.yaml_implicit_resolvers


def _key(kind, scope, name):
    return (kind, scope["kind"], scope["name"], name)


def _identity(document):
    meta = document["metadata"]
    return {
        "kind": document["kind"],
        "scope": deepcopy(meta["scope"]),
        "name": meta["name"],
    }


def content_revision(spec: dict) -> str:
    """A local content digest, never a database resourceVersion or grant."""
    data = json.dumps(
        spec, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False
    )
    return "sha256:" + hashlib.sha256(data.encode()).hexdigest()


def _quantity(value, *, number, path):
    try:
        return int(value[:-2]) * {"Mi": 2**20, "Gi": 2**30, "Ti": 2**40}[value[-2:]]
    except ValueError:
        fail(
            "InputLimitExceeded",
            "Resource quantity exceeds the integer parsing limit.",
            document=number,
            path=pointer(path),
        )


class _Bundle:
    def __init__(self, documents, default_scope):
        self.documents = validate_documents(documents)
        self.catalog = {}
        self.projects = {}
        self.resolved = {}
        self.dependencies = []
        self.defaults = []
        self.expansion_budget = [MAX_NODES]
        if default_scope is not None:
            # Reuse the canonical metadata schema for caller context.
            validate_documents(
                [
                    {
                        "apiVersion": API_VERSION,
                        "kind": "Expert",
                        "metadata": {"name": "context", "scope": default_scope},
                        "spec": {"runtime": {"image": "context"}},
                    }
                ]
            )
        for number, doc in enumerate(self.documents, 1):
            if "scope" not in doc["metadata"]:
                if default_scope is None:
                    fail(
                        "MissingScope",
                        "Supply metadata.scope or a default scope.",
                        document=number,
                        path="/metadata/scope",
                    )
                doc["metadata"]["scope"] = deepcopy(default_scope)
            if (
                doc["kind"] == "Project"
                and doc["metadata"]["scope"]["kind"] != "Account"
            ):
                fail(
                    "InvalidScope",
                    "Projects require Account scope.",
                    document=number,
                    path="/metadata/scope",
                )
            self._add(doc, number)
            if doc["kind"] == "Project":
                name = doc["metadata"]["name"]
                if name in self.projects:
                    fail(
                        "AmbiguousProject",
                        "A bundle cannot address two projects with the same project scope name.",
                        document=number,
                        path="/metadata/name",
                    )
                self.projects[name] = doc
        # Inline children have project scope, not the parent object's Account scope.
        for project in self.projects.values():
            number = self.catalog[self._doc_key(project)][1]
            scope = {"kind": "Project", "name": project["metadata"]["name"]}
            for map_name, kind in _RESOURCE_MAPS.items():
                for alias, selection in (
                    project["spec"]["resources"].get(map_name, {}).items()
                ):
                    if "inline" in selection:
                        child = {
                            "apiVersion": API_VERSION,
                            "kind": kind,
                            "metadata": {"name": alias, "scope": scope},
                            "spec": selection["inline"],
                        }
                        self._add(child, number)

    @staticmethod
    def _doc_key(doc):
        return _key(doc["kind"], doc["metadata"]["scope"], doc["metadata"]["name"])

    def _add(self, doc, number):
        key = self._doc_key(doc)
        if key in self.catalog:
            fail(
                "DuplicateResource",
                "Resource identity collides with another bundle definition.",
                document=number,
                path="/metadata",
            )
        self.catalog[key] = (doc, number)

    def _selection(self, kind, selection, scope, number, path):
        if "inline" in selection:
            check_json_value(
                selection["inline"], document=number, budget=self.expansion_budget
            )
            spec = self._spec(
                kind, deepcopy(selection["inline"]), scope, number, (*path, "inline")
            )
            return {"inline": spec}
        ref = selection["ref"]
        target_scope = ref.get("scope", scope)
        key = _key(kind, target_scope, ref["name"])
        if key not in self.catalog:
            fail(
                "UnresolvedReference",
                "Referenced definition is not supplied in this bundle.",
                document=number,
                path=pointer((*path, "ref")),
            )
        resolved = self._resource(key)
        check_json_value(
            resolved["spec"], document=number, budget=self.expansion_budget
        )
        revision = content_revision(resolved["spec"])
        if "revision" in ref and ref["revision"] != revision:
            fail(
                "RevisionMismatch",
                "Requested revision does not match the supplied definition's resolved content digest.",
                document=number,
                path=pointer((*path, "ref", "revision")),
            )
        self.dependencies.append(
            {
                "document": number,
                "path": pointer(path),
                **_identity(resolved),
                "revision": revision,
            }
        )
        return {"inline": resolved["spec"]}

    @staticmethod
    def _secrets(values, scope):
        for value in values.values():
            if isinstance(value, dict) and "secretRef" in value:
                value["secretRef"].setdefault("scope", deepcopy(scope))

    def _spec(self, kind, spec, scope, number, path):
        if kind == "Expert":
            runtime = spec["runtime"]
            runtime.setdefault("pullPolicy", "IfNotPresent")
            self._secrets(runtime.get("env", {}), scope)
            resources = runtime.get("resources", {})
            requests, limits = (
                resources.get("requests", {}),
                resources.get("limits", {}),
            )
            for field in ("cpu", "memory"):
                if field in requests and field in limits:
                    request, limit = requests[field], limits[field]
                    if field == "memory":
                        request = _quantity(
                            request,
                            number=number,
                            path=(*path, "runtime", "resources", "requests", field),
                        )
                        limit = _quantity(
                            limit,
                            number=number,
                            path=(*path, "runtime", "resources", "limits", field),
                        )
                    if request > limit:
                        fail(
                            "InvalidResources",
                            "Resource request exceeds its limit.",
                            document=number,
                            path=pointer(
                                (*path, "runtime", "resources", "requests", field)
                            ),
                        )
        elif kind == "WorkspaceTemplate":
            spec.setdefault("retention", "Delete")
            resources = spec.get("resources", {})
            requests = resources.get("requests", {})
            if requests and spec["backend"] != "sandbox":
                fail(
                    "UnsupportedWorkspace",
                    "Only container workspaces support resources.requests.",
                    document=number,
                    path=pointer((*path, "resources", "requests")),
                )
            for field in ("cpu", "memory"):
                if field not in requests:
                    continue
                request_path = (*path, "resources", "requests", field)
                if field not in resources:
                    fail(
                        "InvalidResources",
                        "A resource request needs its maximum in the same template.",
                        document=number,
                        path=pointer(request_path),
                    )
                request, limit = requests[field], resources[field]
                if field == "memory":
                    request = _quantity(request, number=number, path=request_path)
                    limit = _quantity(
                        limit, number=number, path=(*path, "resources", field)
                    )
                if request > limit:
                    fail(
                        "InvalidResources",
                        "Resource request exceeds its limit.",
                        document=number,
                        path=pointer(request_path),
                    )
            if spec["backend"] == "virtual" and (
                spec.get("environment")
                or spec.get("initialize")
                or spec.get("resources", {}).get("cpu")
                or spec.get("resources", {}).get("memory")
            ):
                fail(
                    "UnsupportedWorkspace",
                    "Virtual workspaces do not support OS images, initialization or allocated compute.",
                    document=number,
                    path=pointer(path),
                )
            if "environment" in spec:
                spec["environment"].setdefault("pullPolicy", "IfNotPresent")
                spec["environment"].setdefault("cache", "Reuse")
            if "network" in spec:
                spec["network"]["profileRef"].setdefault("scope", deepcopy(scope))
        elif kind == "Connector":
            self._secrets(spec.get("credentials", {}), scope)
        elif kind == "Project":
            self._project(spec, scope, number, path)
        elif kind == "Job":
            self._job(spec, scope, number, path)
        return spec

    def _project(self, spec, scope, number, path):
        # The project name comes from this resource, not its account scope name.
        source = self.documents[number - 1]
        child_scope = {"kind": "Project", "name": source["metadata"]["name"]}
        for map_name, kind in _RESOURCE_MAPS.items():
            selections = spec["resources"].get(map_name, {})
            for alias, selection in list(selections.items()):
                selections[alias] = self._selection(
                    kind,
                    selection,
                    child_scope,
                    number,
                    (*path, "resources", map_name, alias),
                )
        policy = spec.get("team", {}).get("jobPolicy")
        if policy is not None:
            policy.setdefault("retry", {"maxAttempts": 1})
        try:
            project_workspace_defaults(spec)
        except ValueError as exc:
            fail(
                "WorkspaceDefaultBackendMismatch",
                str(exc),
                document=number,
                path=pointer((*path, "defaults", "workspace")),
            )

    def _job(self, spec, scope, number, path):
        execution = spec["execution"]
        project = (
            self.projects.get(scope["name"]) if scope["kind"] == "Project" else None
        )
        if (
            scope["kind"] == "Project"
            and project is None
            and any(
                field not in execution
                for field in ("expert", "workspace", "connectors")
            )
        ):
            fail(
                "UnresolvedProjectDefaults",
                "Supply the Project definition or explicitly select expert, workspace and connectors.",
                document=number,
                path=pointer((*path, "execution")),
            )
        defaults = project["spec"].get("defaults", {}) if project else {}
        resources = project["spec"]["resources"] if project else {}
        for field in ("expert", "workspace", "connectors"):
            if field in execution or field not in defaults:
                continue
            value = defaults[field]
            if field == "expert":
                execution[field] = resources["experts"][value]
            elif field == "workspace":
                execution[field] = (
                    None
                    if value is None
                    else {"template": resources["workspaces"][value]}
                )
            else:
                execution[field] = {
                    alias: resources["connectors"][alias] for alias in value
                }
            self.defaults.append(
                {
                    "document": number,
                    "path": pointer((*path, "execution", field)),
                    "project": _identity(project),
                }
            )
        if "expert" not in execution:
            fail(
                "MissingExpert",
                "Select an expert or supply a project expert default.",
                document=number,
                path=pointer((*path, "execution", "expert")),
            )
        execution["expert"] = self._selection(
            "Expert", execution["expert"], scope, number, (*path, "execution", "expert")
        )
        execution.setdefault("workspace", None)
        workspace = execution["workspace"]
        if workspace is not None and "template" in workspace:
            workspace["template"] = self._selection(
                "WorkspaceTemplate",
                workspace["template"],
                scope,
                number,
                (*path, "execution", "workspace", "template"),
            )
        execution.setdefault("connectors", {})
        for alias, selection in list(execution["connectors"].items()):
            execution["connectors"][alias] = self._selection(
                "Connector",
                selection,
                scope,
                number,
                (*path, "execution", "connectors", alias),
            )
        spec.setdefault("completion", {"mode": "ProcessExit"})
        spec.setdefault("retry", {"maxAttempts": 1})

    def _resource(self, key):
        if key not in self.resolved:
            doc, number = self.catalog[key]
            result = deepcopy(doc)
            result["spec"] = self._spec(
                doc["kind"], result["spec"], doc["metadata"]["scope"], number, ("spec",)
            )
            self.resolved[key] = result
        return self.resolved[key]

    def preview(self):
        resolved = [self._resource(self._doc_key(doc)) for doc in self.documents]
        # Cached definitions may be referenced repeatedly; bound expanded output
        # before serialization, without first deep-copying the expansion.
        result = {
            "apiVersion": API_VERSION,
            "operation": "preview",
            "resolution": "bundle",
            "admissionReady": False,
            "documents": self.documents,
            "resolved": resolved,
            "dependencies": self.dependencies,
            "defaults": self.defaults,
            "pendingChecks": _PENDING_CHECKS,
            "effects": [],
        }
        check_json_value(result)
        return deepcopy(result)


def preview_documents(
    documents: list[dict], *, default_scope: dict | None = None
) -> dict:
    return _Bundle(documents, default_scope).preview()


def export_documents(
    documents: list[dict], *, default_scope: dict | None = None, format: str = "yaml"
) -> str:
    """Export authored ownership/ref choices, never the expanded preview specs."""
    if format not in ("yaml", "json"):
        fail("InvalidFormat", "Choose json or yaml.")
    # Export must also work for a referenced definition supplied in a different
    # installation. Resolve scope/identity, but do not require its dependencies.
    authored = _Bundle(documents, default_scope).documents
    if format == "json":
        return (
            json.dumps(
                authored[0] if len(authored) == 1 else authored,
                indent=2,
                ensure_ascii=True,
                allow_nan=False,
            )
            + "\n"
        )
    return yaml.dump_all(
        authored, Dumper=_ManifestDumper, sort_keys=False, allow_unicode=True
    )
