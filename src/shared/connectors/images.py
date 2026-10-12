"""Driver images: pin or follow, and the compatibility check at bind (D5).

The image reference lives on the driver registration; SRW never rewrites it.
Each bind resolves it to a digest, so the reference decides the behaviour:

* a digest (``repo@sha256:…``, or ``repo:tag@sha256:…``, where the digest
  wins) is exact and needs no lookup;
* any tag is looked up at each bind. A version tag (``:1.4.2``) stays put as
  long as its author does not push it again; a moving tag (``:latest``,
  ``:prod``) follows the author's releases. SRW cannot tell the two apart by
  name and treats every tag alike: only a digest is a hard guarantee.

A digest that is new for a connector gets a compatibility check at bind,
against the image's own spec (the ``io.srw.driver.spec`` label) when it
carries one: the driver name and protocol major must stay, no credential slot
may disappear, and the connector's stored config must still validate. A bind
that fails it is refused: the author changed the contract under a tag, and the
fix is to pin a digest or register ``/v2``.

A shared service pod is keyed by connector, image digest and credential
generation (:func:`service_pod_key`), so a moved tag or a changed credential
starts a new pod while the old one drains.

Design: knowledge-base/knowledge/features/connector_drivers.md, "Driver
versions".
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from .contract import DriverSpec, protocol_major, protocol_supported

#: The image label carrying a driver's spec as JSON (read without running it).
SPEC_LABEL = "io.srw.driver.spec"
DIGEST_PATTERN = r"sha256:[0-9a-f]{64}"
_DIGEST = re.compile(DIGEST_PATTERN)
_HOST = re.compile(r"[a-z0-9]+(?:[.-][a-z0-9]+)*(?::[0-9]{1,5})?")
_REPOSITORY = re.compile(r"[a-z0-9]+(?:(?:[._-]+|/)[a-z0-9]+)*")
_TAG = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}")
_MAX_REFERENCE = 512
#: The longest spec label SRW reads.
MAX_SPEC_LABEL_BYTES = 64 * 1024


@dataclass(frozen=True, slots=True)
class ImageReference:
    """A parsed OCI image reference; ``tag`` and ``digest`` may both be set."""

    host: str
    repository: str
    tag: str | None
    digest: str | None

    @classmethod
    def parse(cls, value: Any) -> ImageReference:
        """Parse ``[host/]repository[:tag][@sha256:…]``; no URL syntax."""
        if not isinstance(value, str) or not 1 <= len(value) <= _MAX_REFERENCE:
            raise ValueError("an image reference is a string of 1 to 512 characters")
        name, separator, digest = value.partition("@")
        if separator and not _DIGEST.fullmatch(digest):
            raise ValueError("an image digest is sha256:<64 hex>")
        first, slash, rest = name.partition("/")
        if slash and ("." in first or ":" in first or first == "localhost"):
            host, repository = first, rest
        else:
            host, repository = "docker.io", name
        if not _HOST.fullmatch(host):
            raise ValueError(f"invalid registry host {host!r}")
        repository, tag_separator, tag = repository.partition(":")
        if host == "docker.io" and "/" not in repository:
            repository = "library/" + repository
        if not _REPOSITORY.fullmatch(repository):
            raise ValueError(f"invalid repository path {repository!r}")
        if tag_separator and not _TAG.fullmatch(tag):
            raise ValueError(f"invalid image tag {tag!r}")
        return cls(
            host=host,
            repository=repository,
            tag=tag if tag_separator else None,
            digest=digest if separator else None,
        )

    @property
    def name(self) -> str:
        return f"{self.host}/{self.repository}"

    @property
    def pinned(self) -> bool:
        """A digest is exact: it needs no lookup and never moves."""
        return self.digest is not None

    def at(self, digest: str) -> str:
        """What a pod launches: the repository at one digest."""
        if not _DIGEST.fullmatch(digest or ""):
            raise ValueError("an image digest is sha256:<64 hex>")
        return f"{self.name}@{digest}"

    def lookup(self) -> str:
        """What the registry is asked for: the digest when pinned, else the
        tag (``latest`` when none is given)."""
        if self.digest:
            return f"{self.name}@{self.digest}"
        return f"{self.name}:{self.tag or 'latest'}"

    def __str__(self) -> str:
        return (
            self.name
            + (f":{self.tag}" if self.tag else "")
            + (f"@{self.digest}" if self.digest else "")
        )


def label_spec(labels: Mapping[str, Any] | None) -> dict[str, Any] | None:
    """The spec an image declares in its ``io.srw.driver.spec`` label.

    ``None`` when the image carries none (an empty label is none: a build
    that sets it from an unset variable); ``ValueError`` when it carries one
    SRW cannot read.
    """
    raw = (labels or {}).get(SPEC_LABEL)
    if raw is None or (isinstance(raw, str) and not raw.strip()):
        return None
    if not isinstance(raw, str) or len(raw.encode("utf-8")) > MAX_SPEC_LABEL_BYTES:
        raise ValueError(f"the {SPEC_LABEL} label is not a bounded string")
    try:
        spec = json.loads(raw)
    except ValueError as exc:
        raise ValueError(f"the {SPEC_LABEL} label is not JSON") from exc
    if not isinstance(spec, dict):
        raise ValueError(f"the {SPEC_LABEL} label is not a JSON object")
    return spec


def spec_hash(spec: Mapping[str, Any] | None) -> str | None:
    """A stable digest of a label spec, recorded with each bind."""
    if spec is None:
        return None
    canonical = json.dumps(spec, sort_keys=True, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class SpecContract:
    """The parts of a spec a moved tag must keep."""

    name: str
    protocol_version: str
    config_schema: Mapping[str, Any]
    slot_names: frozenset[str]

    @classmethod
    def of_driver(cls, spec: DriverSpec) -> SpecContract:
        return cls(
            name=spec.name,
            protocol_version=spec.protocol_version,
            config_schema=spec.config_schema,
            slot_names=frozenset(slot.name for slot in spec.credential_slots),
        )

    @classmethod
    def of_label(cls, label: Mapping[str, Any]) -> SpecContract:
        """From an image's label spec; ``ValueError`` when it is malformed."""
        name = label.get("name")
        version = label.get("protocol_version")
        schema = label.get("config_schema", {"type": "object"})
        slots = label.get("credential_slots", [])
        if not isinstance(name, str) or not isinstance(version, str):
            raise ValueError("a label spec needs a name and a protocol_version")
        if not isinstance(schema, Mapping):
            raise ValueError("a label spec's config_schema is an object")
        if not isinstance(slots, list) or not all(
            isinstance(slot, Mapping) and isinstance(slot.get("name"), str)
            for slot in slots
        ):
            raise ValueError("a label spec's credential_slots are named objects")
        return cls(
            name=name,
            protocol_version=version,
            config_schema=schema,
            slot_names=frozenset(str(slot["name"]) for slot in slots),
        )


def compatibility_problems(
    previous: SpecContract,
    new: SpecContract,
    *,
    config_errors: Iterable[str] = (),
) -> list[str]:
    """Why ``new`` cannot replace ``previous`` under one connector.

    ``config_errors`` are the stored config's validation errors against
    ``new.config_schema`` (the caller owns the JSON Schema validator: this
    module is standard library only). Empty means compatible.
    """
    problems: list[str] = []
    if new.name != previous.name:
        problems.append(f"the image declares driver {new.name}, not {previous.name}")
    if not protocol_supported(new.protocol_version):
        problems.append(f"protocol {new.protocol_version} is not supported")
    elif protocol_major(new.protocol_version) != protocol_major(
        previous.protocol_version
    ):
        problems.append(
            f"the protocol major changed ({previous.protocol_version} to "
            f"{new.protocol_version})"
        )
    removed = sorted(previous.slot_names - new.slot_names)
    if removed:
        problems.append(f"credential slots disappeared: {', '.join(removed)}")
    problems += [f"the stored config no longer validates: {e}" for e in config_errors]
    return problems


def refusal_message(reference: str, problems: Iterable[str]) -> str:
    """The bind refusal a connector shows for an incompatible moved tag."""
    return (
        f"The image behind {reference} changed its contract "
        f"({'; '.join(problems)}): pin a digest or register a new driver major."
    )


def service_pod_key(connector_id: str, digest: str, generation: str) -> str:
    """One shared service pod per connector, image digest and credential
    generation."""
    if not _DIGEST.fullmatch(digest or ""):
        raise ValueError("an image digest is sha256:<64 hex>")
    if not generation:
        raise ValueError("a credential generation is required")
    return f"{connector_id}/{digest}/{generation}"


__all__ = [
    "DIGEST_PATTERN",
    "MAX_SPEC_LABEL_BYTES",
    "SPEC_LABEL",
    "ImageReference",
    "SpecContract",
    "compatibility_problems",
    "label_spec",
    "refusal_message",
    "service_pod_key",
    "spec_hash",
]
