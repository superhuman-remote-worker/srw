"""Bounded JSON and YAML inputs using one JSON-compatible value model."""

from copy import deepcopy
from dataclasses import dataclass
import json
import re

import yaml

from .errors import ManifestError, fail
from .validation import MAX_DOCUMENTS, MAX_SOURCE_BYTES, validate_documents

#: Sources one request may carry (files given to one command).
MAX_SOURCES = 100


class ManifestLoader(yaml.SafeLoader):
    def construct_mapping(self, node, deep=False):
        result = {}
        for key_node, value_node in node.value:
            if key_node.tag == "tag:yaml.org,2002:merge":
                fail("YAMLMergeKey", "YAML merge keys are not supported.")
            key = self.construct_object(key_node, deep=deep)
            if type(key) is not str:
                fail("InvalidObjectKey", "Object keys must be strings.")
            if key in result:
                fail("DuplicateKey", "Duplicate object key.")
            result[key] = self.construct_object(value_node, deep=deep)
        return result


_JSON_NUMBER = re.compile(r"-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?\Z")
_NUMERIC = re.compile(
    r"[+-]?(?:[0-9][0-9_]*(?:\.[0-9_]*)?(?:[eE][+-]?[0-9_]+)?|0[xXoObB][0-9a-fA-F_]+|[0-9_]+(?::[0-9_.]+)+)\Z"
)


def _scalar(loader, node):
    value = loader.construct_scalar(node)
    kind = node.tag.rsplit(":", 1)[-1]
    accepted = (
        (kind == "bool" and value in ("true", "false"))
        or (kind == "null" and value == "null")
        or (kind in ("int", "float") and _JSON_NUMBER.fullmatch(value))
    )
    if not accepted:
        fail("AmbiguousScalar", "Use JSON scalar spellings or quote the value as text.")
    return json.loads(value)


for _kind in ("bool", "null", "int", "float"):
    ManifestLoader.add_constructor(f"tag:yaml.org,2002:{_kind}", _scalar)
ManifestLoader.add_implicit_resolver(
    "tag:yaml.org,2002:float", _NUMERIC, list("-+0123456789")
)


def _json_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            fail("DuplicateKey", "Duplicate object key.")
        result[key] = value
    return result


def _constant(value):
    fail("InvalidJSONValue", "JSON numbers must be finite.")


def _load(source, format) -> tuple[list, int]:
    """One source's documents, not yet validated, and its size in bytes."""
    if not isinstance(source, str):
        fail("InvalidSource", "Manifest source must be text.")
    try:
        size = len(source.encode("utf-8"))
    except UnicodeError:
        fail("InvalidSource", "Manifest source must be valid UTF-8 text.")
    if size > MAX_SOURCE_BYTES:
        fail("InputLimitExceeded", "Manifest text exceeds the 1 MiB limit.")
    if format not in ("json", "yaml"):
        fail("InvalidFormat", "Choose json or yaml.")
    documents = []
    try:
        if format == "json":
            value = json.loads(
                source, object_pairs_hook=_json_object, parse_constant=_constant
            )
            documents = value if type(value) is list else [value]
        else:
            for document in yaml.load_all(source, Loader=ManifestLoader):
                if len(documents) == MAX_DOCUMENTS:
                    fail("DocumentLimit", "Too many manifest documents.")
                documents.append(document)
    except ManifestError as exc:
        if format == "yaml":
            fail(
                exc.issue.code,
                exc.issue.message,
                document=len(documents) + 1,
                path=exc.issue.path,
            )
        raise
    except (ValueError, yaml.YAMLError, RecursionError, OverflowError):
        # Parser exceptions may contain whole source lines, including secrets.
        fail(
            "InvalidSyntax",
            "Unable to parse manifest source.",
            document=len(documents) + 1,
        )
    return documents, size


@dataclass(frozen=True)
class ParsedSources:
    """One bundle read from several named sources (:func:`parse_sources`).

    ``origins`` has one entry per document: the name of its source and the
    document's number inside that source. Later stages count documents across
    the bundle; ``ManifestError.located(origins)`` turns such a number back
    into the pair a person can find in their files.
    """

    documents: list[dict]
    origins: tuple[tuple[str, int], ...]


def parse_sources(sources: list[dict]) -> ParsedSources:
    """Read several sources, each ``{"name", "source", "format"}``, as one bundle.

    Each source is parsed by itself, so YAML and JSON can be mixed and a
    syntax error names its source. The limits of a single source hold for the
    bundle as a whole: its total size, its number of documents and its number
    of values. An error names the source and counts documents inside it.
    """
    if type(sources) is not list or not 1 <= len(sources) <= MAX_SOURCES:
        fail("SourceLimit", f"Supply between 1 and {MAX_SOURCES} manifest sources.")
    documents: list = []
    origins: list[tuple[str, int]] = []
    seen: set[str] = set()
    total = 0
    for entry in sources:
        name = entry["name"]
        try:
            if name in seen:
                fail("DuplicateSource", "Two manifest sources have the same name.")
            seen.add(name)
            loaded, size = _load(entry["source"], entry.get("format", "yaml"))
            if not loaded:
                fail("EmptySource", "The source holds no resource document.")
            total += size
            if total > MAX_SOURCE_BYTES:
                fail(
                    "InputLimitExceeded",
                    "The manifest sources together exceed the 1 MiB limit.",
                )
        except ManifestError as exc:
            raise exc.in_source(name) from None
        documents.extend(loaded)
        origins.extend((name, number) for number in range(1, len(loaded) + 1))
    try:
        validated = validate_documents(documents)
    except ManifestError as exc:
        raise exc.located(origins) from None
    return ParsedSources(validated, tuple(origins))


def parse_documents(source: str | ParsedSources, *, format: str = "yaml") -> list[dict]:
    """The validated documents of one source text.

    A bundle already read from several sources (:class:`ParsedSources`) is
    accepted in place of the text, so every caller that takes a source takes
    either; ``format`` is not used then.
    """
    if isinstance(source, ParsedSources):
        return deepcopy(source.documents)
    documents, _ = _load(source, format)
    return validate_documents(documents)
