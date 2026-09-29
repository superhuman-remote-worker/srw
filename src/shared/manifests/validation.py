"""Offline JSON Schema validation; no runtime, catalog or credential imports."""

from copy import deepcopy
from functools import lru_cache
from importlib.resources import files
import json
import math

from jsonschema import Draft202012Validator
from jsonschema.exceptions import best_match

from .errors import fail, pointer

API_VERSION = "srw/v1alpha1"
MAX_SOURCE_BYTES = 1024 * 1024
MAX_DOCUMENTS = 100
MAX_DEPTH = 64
MAX_NODES = 100_000
MAX_EXPANDED_BYTES = 8 * 1024 * 1024


@lru_cache(maxsize=1)
def _schema() -> dict:
    schema = json.loads(files(__package__).joinpath("schema.json").read_text())
    Draft202012Validator.check_schema(schema)
    return schema


def load_schema() -> dict:
    return deepcopy(_schema())


def check_json_value(value, *, document: int = 1, budget: list | None = None):
    """Bound alias expansion/depth before copying or serializing user input."""
    remaining = budget if budget is not None else [MAX_NODES]
    if len(remaining) == 1:
        remaining.append(MAX_EXPANDED_BYTES)
    ancestors = set()

    def text_size(text, path):
        try:
            return len(text.encode("utf-8"))
        except UnicodeError:
            fail(
                "InvalidJSONValue",
                "Text must contain valid Unicode characters.",
                document=document,
                path=pointer(path),
            )

    def visit(item, path, depth):
        remaining[0] -= 1
        if type(item) is str:
            remaining[1] -= text_size(item, path)
        if remaining[0] < 0 or remaining[1] < 0 or depth > MAX_DEPTH:
            fail(
                "InputLimitExceeded",
                "Manifest structure exceeds its limit.",
                document=document,
                path=pointer(path),
            )
        if item is None or type(item) in (str, bool, int):
            return
        if type(item) is float and math.isfinite(item):
            return
        if type(item) not in (dict, list):
            fail(
                "InvalidJSONValue",
                "Use finite JSON values; quote dates and other text.",
                document=document,
                path=pointer(path),
            )
        if id(item) in ancestors:
            fail(
                "RecursiveAlias",
                "Recursive aliases are not supported.",
                document=document,
                path=pointer(path),
            )
        ancestors.add(id(item))
        try:
            children = item.items() if type(item) is dict else enumerate(item)
            for key, child in children:
                if type(item) is dict and type(key) is not str:
                    fail(
                        "InvalidObjectKey",
                        "Object keys must be strings.",
                        document=document,
                        path=pointer(path),
                    )
                if type(item) is dict:
                    remaining[1] -= text_size(key, path)
                visit(child, (*path, key), depth + 1)
        finally:
            ancestors.remove(id(item))

    visit(value, (), 0)


def _schema_issue(error, document):
    # Do not expose jsonschema's default messages: many include input values.
    rule = error.validator
    if rule == "additionalProperties":
        code, message = "UnknownField", "Unknown platform field."
    elif rule == "required":
        missing = [key for key in error.validator_value if key not in error.instance]
        code, message = "MissingField", "Required fields: " + ", ".join(missing)
    elif rule == "type":
        code, message = "InvalidType", "Expected type: " + str(error.validator_value)
    else:
        code, message = "InvalidManifest", f"Field violates the {rule} constraint."
    fail(code, message, document=document, path=pointer(error.absolute_path))


def _project_aliases(doc, number):
    if doc["kind"] != "Project":
        return
    spec = doc["spec"]
    resources = spec["resources"]
    team = spec.get("team", {})
    bindings = [(("spec", "defaults"), spec.get("defaults", {}))]
    bindings.append((("spec", "team", "officer"), team.get("officer", {})))
    bindings.extend(
        (("spec", "team", "slots", name), slot)
        for name, slot in team.get("slots", {}).items()
    )
    for path, binding in bindings:
        for field, kind in (
            ("expert", "experts"),
            ("sessionExpert", "experts"),
            ("workspace", "workspaces"),
        ):
            value = binding.get(field)
            if field == "workspace" and isinstance(value, dict):
                aliases = [
                    ((*path, field, tier), value[tier])
                    for tier in ("container", "vm")
                    if tier in value
                ]
            else:
                aliases = [((*path, field), value)] if value is not None else []
            for alias_path, alias in aliases:
                if alias not in resources.get(kind, {}):
                    fail(
                        "UnknownAlias",
                        "Binding names an undeclared project resource.",
                        document=number,
                        path=pointer(alias_path),
                    )
        if any(
            alias not in resources.get("connectors", {})
            for alias in binding.get("connectors", [])
        ):
            fail(
                "UnknownAlias",
                "Binding names an undeclared project connector.",
                document=number,
                path=pointer((*path, "connectors")),
            )


def validate_documents(documents: list[dict]) -> list[dict]:
    if type(documents) is not list or not 1 <= len(documents) <= MAX_DOCUMENTS:
        fail(
            "DocumentLimit", f"Supply between 1 and {MAX_DOCUMENTS} resource documents."
        )
    budget = [MAX_NODES]
    validator = Draft202012Validator(_schema())
    for number, document in enumerate(documents, 1):
        check_json_value(document, document=number, budget=budget)
        if (
            isinstance(document, dict)
            and "apiVersion" in document
            and document["apiVersion"] != API_VERSION
        ):
            fail(
                "UnsupportedAPIVersion",
                f"Only {API_VERSION} resource manifests are supported.",
                document=number,
                path="/apiVersion",
            )
        error = best_match(validator.iter_errors(document))
        if error is not None:
            _schema_issue(error, number)
        _project_aliases(document, number)
    return deepcopy(documents)
