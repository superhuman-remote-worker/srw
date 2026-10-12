"""Driver image digests: pin or follow, and the moved-tag check (D5 item 7).

The pure rules live in ``shared.connectors.images``; resolution at bind, the
brief cache and the stale fallback in
``orchestrator.services.connector_service_images``. The SQL and the refusal
end to end run against PostgreSQL in
tests/test_connector_service_hosting_real_postgres.py.
"""

from __future__ import annotations

import contextlib
import json
from dataclasses import replace
from datetime import datetime, timezone
from typing import Any

import pytest

from orchestrator.services import connector_service_images as images
from shared.connectors.contract import (
    AccessLevel,
    CredentialSlot,
    DriverSpec,
    ServiceSpec,
)
from shared.connectors.images import (
    SPEC_LABEL,
    ImageReference,
    SpecContract,
    compatibility_problems,
    label_spec,
    refusal_message,
    service_pod_key,
    spec_hash,
)
from shared.oci_registry import RegistryResolutionError, ResolvedImage

D1 = "sha256:" + "1" * 64
D2 = "sha256:" + "2" * 64

SERVICE = DriverSpec(
    name="srw.test-service/v1",
    title="Test service",
    plane="service",
    delivery_forms=("lease_token",),
    config_schema={
        "type": "object",
        "additionalProperties": False,
        "properties": {"host": {"type": "string"}},
    },
    credential_slots=(
        CredentialSlot("secret", "secret_string", {"type": "object"}, required=True),
    ),
    access_levels=(AccessLevel("ReadWrite", 1, "the lease exchange"),),
    supported_backends=frozenset({"sandbox"}),
    workspace_requirements="none",
    credential_delivery="lease",
    service=ServiceSpec(),
)


# =============================================================================
# References: pin or follow
# =============================================================================


class TestImageReference:
    @pytest.mark.parametrize(
        ("value", "name", "tag", "digest"),
        [
            ("ghcr.io/org/echo:1.4.2", "ghcr.io/org/echo", "1.4.2", None),
            ("ghcr.io/org/echo", "ghcr.io/org/echo", None, None),
            ("echo:latest", "docker.io/library/echo", "latest", None),
            (
                "srw-registry:5000/srw-driver-echo:tilt-1",
                "srw-registry:5000/srw-driver-echo",
                "tilt-1",
                None,
            ),
            (f"ghcr.io/org/echo@{D1}", "ghcr.io/org/echo", None, D1),
            (f"ghcr.io/org/echo:1@{D1}", "ghcr.io/org/echo", "1", D1),
        ],
    )
    def test_parse(self, value, name, tag, digest):
        reference = ImageReference.parse(value)
        assert (reference.name, reference.tag, reference.digest) == (name, tag, digest)
        assert reference.pinned is (digest is not None)

    def test_a_digest_wins_over_its_tag(self):
        reference = ImageReference.parse(f"ghcr.io/org/echo:1@{D1}")
        assert reference.lookup() == f"ghcr.io/org/echo@{D1}"
        assert str(reference) == f"ghcr.io/org/echo:1@{D1}"

    def test_every_tag_is_looked_up_alike(self):
        # SRW cannot tell a version tag from a moving one by name.
        for tag in ("1.4.2", "latest", "prod"):
            reference = ImageReference.parse(f"ghcr.io/org/echo:{tag}")
            assert not reference.pinned
            assert reference.lookup() == f"ghcr.io/org/echo:{tag}"
        assert ImageReference.parse("ghcr.io/org/echo").lookup().endswith(":latest")

    def test_a_pod_launches_the_repository_at_one_digest(self):
        reference = ImageReference.parse("ghcr.io/org/echo:latest")
        assert reference.at(D2) == f"ghcr.io/org/echo@{D2}"
        with pytest.raises(ValueError):
            reference.at("sha256:short")

    @pytest.mark.parametrize(
        "value",
        [
            "",
            "https://ghcr.io/org/echo",
            "ghcr.io/org/echo@sha256:XYZ",
            "ghcr.io/Org/echo",
            "ghcr.io/org/echo:-bad",
            "user:pass@ghcr.io/org/echo",
            "x" * 513,
            None,
        ],
    )
    def test_malformed_references_are_refused(self, value):
        with pytest.raises(ValueError):
            ImageReference.parse(value)


def test_the_pod_key_is_connector_digest_and_generation():
    assert service_pod_key("c", D1, "hmac-sha256:ab") == f"c/{D1}/hmac-sha256:ab"
    assert service_pod_key("c", D1, "g1") != service_pod_key("c", D2, "g1")
    assert service_pod_key("c", D1, "g1") != service_pod_key("c", D1, "g2")
    with pytest.raises(ValueError):
        service_pod_key("c", "latest", "g1")


# =============================================================================
# The spec label and the compatibility check
# =============================================================================


def _label(**over: Any) -> dict[str, Any]:
    label = {
        "name": SERVICE.name,
        "protocol_version": "1.0",
        "config_schema": SERVICE.config_schema,
        "credential_slots": [{"name": "secret"}],
    }
    label.update(over)
    return label


class TestLabelSpec:
    def test_absent_label_is_none(self):
        assert label_spec({}) is None
        assert label_spec(None) is None

    def test_label_is_json_object(self):
        assert label_spec({SPEC_LABEL: json.dumps(_label())}) == _label()
        for bad in ("not json", "[1]", "x" * (64 * 1024 + 1)):
            with pytest.raises(ValueError):
                label_spec({SPEC_LABEL: bad})

    def test_spec_hash_is_canonical(self):
        assert spec_hash(None) is None
        assert spec_hash({"b": 1, "a": 2}) == spec_hash({"a": 2, "b": 1})
        assert spec_hash({"a": 1}) != spec_hash({"a": 2})


class TestCompatibility:
    def test_the_same_contract_is_compatible(self):
        installed = SpecContract.of_driver(SERVICE)
        assert compatibility_problems(installed, SpecContract.of_label(_label())) == []
        # A new minor protocol and an added slot keep the contract.
        newer = SpecContract.of_label(
            _label(
                protocol_version="1.3",
                credential_slots=[{"name": "secret"}, {"name": "extra"}],
            )
        )
        assert compatibility_problems(installed, newer) == []

    @pytest.mark.parametrize(
        ("label", "fragment"),
        [
            (_label(protocol_version="2.0"), "protocol 2.0 is not supported"),
            (_label(name="srw.other/v1"), "declares driver srw.other/v1"),
            (_label(credential_slots=[]), "credential slots disappeared: secret"),
        ],
    )
    def test_a_broken_contract_is_named(self, label, fragment):
        problems = compatibility_problems(
            SpecContract.of_driver(SERVICE), SpecContract.of_label(label)
        )
        assert any(fragment in problem for problem in problems), problems

    def test_a_protocol_major_change_is_refused_when_both_are_supported(self):
        previous = replace(SpecContract.of_driver(SERVICE), protocol_version="0.9")
        problems = compatibility_problems(previous, SpecContract.of_label(_label()))
        assert problems == ["the protocol major changed (0.9 to 1.0)"]

    def test_config_errors_are_problems(self):
        problems = compatibility_problems(
            SpecContract.of_driver(SERVICE),
            SpecContract.of_label(_label()),
            config_errors=["/host: 1 is not of type 'string'"],
        )
        assert problems == [
            "the stored config no longer validates: /host: 1 is not of type 'string'"
        ]

    def test_malformed_labels_are_refused(self):
        for bad in (
            {},
            {"name": "x"},
            _label(config_schema=[]),
            _label(credential_slots=[1]),
        ):
            with pytest.raises(ValueError):
                SpecContract.of_label(bad)

    def test_the_refusal_says_what_to_do(self):
        message = refusal_message("ghcr.io/org/echo:latest", ["a", "b"])
        assert "ghcr.io/org/echo:latest changed its contract (a; b)" in message
        assert "pin a digest" in message

    def test_config_errors_use_json_schema_2020_12(self):
        schema = {
            "type": "object",
            "required": ["region"],
            "properties": {"region": {"type": "string"}},
        }
        assert images.config_errors(schema, {"host": "x"}) == [
            "'region' is a required property"
        ]
        assert images.config_errors({"type": 12}, {})[0].startswith(
            "the image's config schema is invalid"
        )


# =============================================================================
# Resolution at bind: the cache and the stale fallback
# =============================================================================


class _Conn:
    """The two queries resolution runs: the upsert and the last resolution.

    It is its own store: resolution writes on a connection it acquires.
    """

    def __init__(self) -> None:
        self.rows: dict[tuple[str, str, str], dict[str, Any]] = {}
        self.writes = 0

    @contextlib.asynccontextmanager
    async def acquire(self):
        yield self

    async def fetchrow(self, query: str, *args: Any):
        if query.lstrip().startswith("INSERT INTO connector_driver_images"):
            driver, reference, digest, entrypoint, cmd, spec, spec_h, protocol = args
            self.writes += 1
            row = {
                "driver": driver,
                "reference": reference,
                "digest": digest,
                "entrypoint": entrypoint,
                "cmd": cmd,
                "spec": spec,
                "spec_hash": spec_h,
                "protocol_version": protocol,
                "resolved_at": datetime(2026, 10, 8, tzinfo=timezone.utc),
            }
            self.rows[(driver, reference, digest)] = row
            return row
        if "ORDER BY resolved_at DESC" in query:
            driver, reference = args
            found = [
                row
                for (d, r, _), row in self.rows.items()
                if (d, r) == (driver, reference)
            ]
            return found[-1] if found else None
        driver, reference, digest = args
        return self.rows.get((driver, reference, digest))


class _Resolver:
    def __init__(self, *answers: Any) -> None:
        self.answers = list(answers)
        self.calls: list[str] = []

    async def resolve_image(self, image: str):
        self.calls.append(image)
        answer = self.answers.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return answer


def _image(digest: str, **labels: str) -> ResolvedImage:
    return ResolvedImage(
        reference=f"ghcr.io/org/echo@{digest}",
        digest=digest,
        entrypoint=("/echo",),
        cmd=("--serve",),
        labels=dict(labels),
    )


@pytest.fixture
def configure():
    def _configure(resolver, *, cache_seconds: float = 60.0) -> None:
        images.configure_service_images(
            images.ServiceImageSettings(
                references={SERVICE.name: "ghcr.io/org/echo:latest"},
                resolver=resolver,
                cache_seconds=cache_seconds,
                timeout_seconds=1.0,
            )
        )

    yield _configure
    images.configure_service_images(images.ServiceImageSettings())


@pytest.mark.asyncio
async def test_a_tag_is_resolved_recorded_and_cached_briefly(configure):
    resolver = _Resolver(_image(D1), _image(D2))
    configure(resolver, cache_seconds=30)
    conn = _Conn()
    clock = iter([100.0, 110.0, 140.0]).__next__
    reference = "ghcr.io/org/echo:latest"
    first = await images.resolve_driver_image(
        conn, driver=SERVICE.name, reference=reference, clock=clock
    )
    assert (first.digest, first.entrypoint, first.cmd) == (D1, ("/echo",), ("--serve",))
    assert first.protocol_version == "1.0" and first.spec is None
    cached = await images.resolve_driver_image(
        conn, driver=SERVICE.name, reference=reference, clock=clock
    )
    assert cached.digest == D1 and resolver.calls == ["ghcr.io/org/echo:latest"]
    # Past the cache: the tag is looked up again and follows the author.
    moved = await images.resolve_driver_image(
        conn, driver=SERVICE.name, reference=reference, clock=clock
    )
    assert moved.digest == D2 and len(resolver.calls) == 2
    assert first.record()["reference"] == reference
    assert set(first.record()) == {
        "reference",
        "digest",
        "resolved_at",
        "spec_hash",
        "protocol_version",
    }


@pytest.mark.asyncio
async def test_an_unreachable_registry_reuses_the_last_digest_as_stale(configure):
    resolver = _Resolver(_image(D1), RegistryResolutionError("down"))
    configure(resolver, cache_seconds=0)
    conn = _Conn()
    reference = "ghcr.io/org/echo:latest"
    await images.resolve_driver_image(conn, driver=SERVICE.name, reference=reference)
    stale = await images.resolve_driver_image(
        conn, driver=SERVICE.name, reference=reference
    )
    assert stale.digest == D1 and stale.stale is True


@pytest.mark.asyncio
async def test_a_reference_that_never_resolved_fails_the_bind(configure):
    configure(_Resolver(RegistryResolutionError("down")))
    with pytest.raises(images.ServiceImageUnavailable, match="cannot be resolved"):
        await images.resolve_driver_image(
            _Conn(), driver=SERVICE.name, reference="ghcr.io/org/echo:latest"
        )
    configure(None)
    with pytest.raises(images.ServiceImageUnavailable, match="cannot be resolved"):
        await images.resolve_driver_image(
            _Conn(), driver=SERVICE.name, reference="ghcr.io/org/echo:latest"
        )


@pytest.mark.asyncio
async def test_a_failure_is_remembered_for_the_window_and_says_nothing_internal(
    configure,
):
    resolver = _Resolver(RegistryResolutionError("10.0.0.7:5000 answered HTTP 418"))
    configure(resolver, cache_seconds=30)
    clock = iter([100.0, 110.0]).__next__
    for _ in range(2):
        with pytest.raises(images.ServiceImageUnavailable) as failed:
            await images.resolve_driver_image(
                _Conn(),
                driver=SERVICE.name,
                reference="ghcr.io/org/echo:latest",
                clock=clock,
            )
        assert "418" not in str(failed.value) and "10.0.0.7" not in str(failed.value)
    # The registry was asked once in the window.
    assert len(resolver.calls) == 1


@pytest.mark.asyncio
async def test_a_digest_reference_is_asked_for_by_digest(configure):
    resolver = _Resolver(_image(D1))
    configure(resolver)
    await images.resolve_driver_image(
        _Conn(), driver=SERVICE.name, reference=f"ghcr.io/org/echo:1@{D1}"
    )
    assert resolver.calls == [f"ghcr.io/org/echo@{D1}"]


@pytest.mark.asyncio
async def test_an_unreadable_spec_label_refuses_the_bind(configure):
    configure(_Resolver(_image(D1, **{SPEC_LABEL: "{not json"})))
    with pytest.raises(images.ServiceImageRefused, match="spec label is unreadable"):
        await images.resolve_driver_image(
            _Conn(), driver=SERVICE.name, reference="ghcr.io/org/echo:latest"
        )
    configure(
        _Resolver(_image(D1, **{SPEC_LABEL: json.dumps(_label(protocol_version="x"))}))
    )
    with pytest.raises(images.ServiceImageRefused, match="MAJOR.MINOR"):
        await images.resolve_driver_image(
            _Conn(), driver=SERVICE.name, reference="ghcr.io/org/echo:latest"
        )


@pytest.mark.asyncio
async def test_a_label_spec_is_recorded_with_its_hash(configure):
    label = _label(protocol_version="1.2")
    configure(_Resolver(_image(D1, **{SPEC_LABEL: json.dumps(label)})))
    bound = await images.resolve_driver_image(
        _Conn(), driver=SERVICE.name, reference="ghcr.io/org/echo:latest"
    )
    assert bound.spec == label
    assert bound.spec_hash == spec_hash(label)
    assert bound.protocol_version == "1.2"


def test_a_driver_without_a_configured_image_has_no_reference(configure):
    configure(None)
    assert images.image_reference_for(SERVICE.name) == "ghcr.io/org/echo:latest"
    assert images.image_reference_for("srw.other/v1") is None
